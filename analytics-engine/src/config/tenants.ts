import { getCmsEnvConfig } from './index.js';
import { getClickHouseClient } from './clickhouse.js';
import type { ClickHouseClient } from '@clickhouse/client';

export interface TenantConfig {
    cmsId: string;
    dbName: string;
    apiKey: string;
    readClient: ClickHouseClient;
    ingestClient: ClickHouseClient;
}

/** Tenant registry — loaded from env on startup */
const tenants = new Map<string, TenantConfig>();

/** Initialize tenants from environment variables */
export function initTenants(): void {
    const cmsEnv = getCmsEnvConfig();

    for (const [cmsId, creds] of Object.entries(cmsEnv)) {
        if (!creds.apiKey) continue;
        const dbName = `db_${cmsId}`;
        tenants.set(cmsId, {
            cmsId,
            dbName,
            apiKey: creds.apiKey,
            readClient: getClickHouseClient({
                database: dbName,
                username: creds.chUser,
                password: creds.chPass,
            }),
            ingestClient: getClickHouseClient({
                database: dbName,
                username: creds.chIngestUser,
                password: creds.chIngestPass,
            }),
        });
    }

    console.log(`[Tenants] Loaded ${tenants.size} CMS tenant(s): ${[...tenants.keys()].join(', ')}`);
}

/** Resolve tenant by API key */
export function getTenantByApiKey(apiKey: string): TenantConfig | undefined {
    for (const tenant of tenants.values()) {
        if (tenant.apiKey === apiKey) return tenant;
    }
    return undefined;
}

/** Resolve tenant by CMS ID */
export function getTenantById(cmsId: string): TenantConfig | undefined {
    return tenants.get(cmsId);
}

/** Get all tenants */
export function getAllTenants(): TenantConfig[] {
    return [...tenants.values()];
}

export function sanitizeDbName(rawId: string | null | undefined): string {
    if (!rawId) return "";
    const clean = rawId.replace(/[^a-zA-Z0-9_]/g, "_");
    return clean.startsWith("db_") ? clean : `db_${clean}`;
}
