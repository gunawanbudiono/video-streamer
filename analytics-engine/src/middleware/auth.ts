// @ts-nocheck
import type { FastifyRequest, FastifyReply } from 'fastify';
import { getTenantByApiKey, getTenantById, type TenantConfig } from '../config/tenants.js';
import { config } from '../config/index.js';
import { getDefaultClient, getClickHouseClient } from '../config/clickhouse.js';
import { createHash } from 'crypto';

/** Access roles for the 3-level auth hierarchy */
export type AuthRole = 'super_admin' | 'org_admin' | 'cms';

/** Extend FastifyRequest to include tenant + org context */
declare module 'fastify' {
    interface FastifyRequest {
        tenant?: TenantConfig;
        isAdmin?: boolean;
        /** Auth role: super_admin | org_admin | cms */
        authRole?: AuthRole;
        /** Org ID (set for org_admin and sometimes cms) */
        orgId?: string;
    }
}

/**
 * Resolve org key from ClickHouse org_registry.
 * Returns org_id if key matches, null otherwise.
 */
async function resolveOrgKey(apiKey: string): Promise<string | null> {
    try {
        const keyHash = createHash('sha256').update(apiKey).digest('hex');
        const client = getDefaultClient();
        const result = await client.query({
            query: `SELECT org_id FROM org_registry FINAL WHERE org_key_hash = {hash: String} AND is_active = 1`,
            query_params: { hash: keyHash },
            format: 'JSONEachRow',
        });
        const rows = await result.json<Array<{ org_id: string }>>();
        if (rows.length > 0 && rows[0]) {
            return rows[0].org_id;
        }
        return null;
    } catch {
        return null;
    }
}

/**
 * Auth middleware: validates X-API-Key header and injects tenant/org context.
 * 
 * Access hierarchy:
 * 1. ADMIN_MASTER_KEY → super_admin (all orgs, all CMS)
 * 2. Org key (from org_registry) → org_admin (only CMS within org)
 * 3. CMS API key (from tenants) → cms (single CMS only)
 */
export async function authMiddleware(
    request: FastifyRequest,
    reply: FastifyReply
): Promise<void> {
    const apiKey = request.headers['x-api-key'] as string | undefined;

    if (!apiKey) {
        reply.code(401).send({ error: 'Missing X-API-Key header' });
        return;
    }

    // Level 1: Super Admin
    if (apiKey === config.adminMasterKey) {
        request.isAdmin = true;
        request.authRole = 'super_admin';

        // SEC-24: Resolve tenant if X-CMS-ID header is present
        const targetCmsId = request.headers['x-cms-id'] as string | undefined;
        if (targetCmsId) {
            let tenant = getTenantById(targetCmsId);
            if (!tenant) {
                const dbName = `db_${targetCmsId.replace(/-/g, '_')}`;
                tenant = {
                    cmsId: targetCmsId,
                    dbName,
                    apiKey: '',
                    readClient: getClickHouseClient({ database: dbName }),
                    ingestClient: getClickHouseClient({ database: dbName }),
                };
            }
            request.tenant = tenant;
        }
        return;
    }

    // Level 2: Org Admin key
    const orgId = await resolveOrgKey(apiKey);
    if (orgId) {
        request.isAdmin = false;
        request.authRole = 'org_admin';
        request.orgId = orgId;

        // SEC-24: Resolve tenant if X-CMS-ID header is present
        const targetCmsId = request.headers['x-cms-id'] as string | undefined;
        if (targetCmsId) {
            // Verify that this CMS partner belongs to the organization
            const defaultClient = getDefaultClient();
            if (!defaultClient) {
                reply.code(500).send({ error: 'Default clickhouse database client not initialized' });
                return;
            }
            const rs = await defaultClient.query({
                query: `SELECT org_id FROM cms_registry FINAL WHERE cms_id = {cmsId: String} AND org_id = {orgId: String} AND is_active = 1`,
                query_params: { cmsId: targetCmsId, orgId },
                format: 'JSONEachRow'
            });
            const rows = await rs.json<Array<{ org_id: string }>>();
            if (rows.length === 0) {
                reply.code(403).send({ error: 'Access Denied: Target CMS Partner does not belong to your organization.' });
                return;
            }

            let tenant = getTenantById(targetCmsId);
            if (!tenant) {
                const dbName = `db_${targetCmsId.replace(/-/g, '_')}`;
                tenant = {
                    cmsId: targetCmsId,
                    dbName,
                    apiKey: '',
                    readClient: getClickHouseClient({ database: dbName }),
                    ingestClient: getClickHouseClient({ database: dbName }),
                };
            }
            request.tenant = tenant;
        }
        return;
    }

    // Level 3: CMS-level API key
    const tenant = getTenantByApiKey(apiKey);
    if (!tenant) {
        reply.code(403).send({ error: 'Invalid API key' });
        return;
    }

    request.tenant = tenant;
    request.authRole = 'cms';
}

/**
 * Admin auth middleware: validates ADMIN_MASTER_KEY or org key for admin endpoints.
 * Super admin: full access. Org admin: scoped access.
 */
export async function adminAuthMiddleware(
    request: FastifyRequest,
    reply: FastifyReply
): Promise<void> {
    const masterKey = request.headers['x-admin-key'] as string | undefined;
    const orgKey = request.headers['x-api-key'] as string | undefined;

    // Check super admin key first
    if (masterKey && masterKey === config.adminMasterKey) {
        request.isAdmin = true;
        request.authRole = 'super_admin';
        return;
    }

    // Check org admin key
    if (orgKey) {
        const orgId = await resolveOrgKey(orgKey);
        if (orgId) {
            request.authRole = 'org_admin';
            request.orgId = orgId;
            return;
        }
    }

    reply.code(403).send({ error: 'Invalid or missing admin/org key' });
}
