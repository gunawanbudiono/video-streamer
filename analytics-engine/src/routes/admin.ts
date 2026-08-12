// @ts-nocheck
import type { FastifyInstance } from 'fastify';
import { adminAuthMiddleware } from '../middleware/auth.js';
import { getDefaultClient, getClickHouseClient } from '../config/clickhouse.js';
import { CMS_DDL, CMS_MIGRATIONS } from '../db/ddl.js';
import { v4 as uuidv4 } from 'uuid';
import { createHash } from 'crypto';

/** Admin routes — Org management, CMS registry, channel mapping, label correction */
export async function adminRoutes(app: FastifyInstance): Promise<void> {
    app.addHook('onRequest', adminAuthMiddleware);

    // ══════════════════════════════════════════════════════════
    // ORG MANAGEMENT (Super Admin only)
    // ══════════════════════════════════════════════════════════

    /** POST /api/v1/admin/orgs — Create a new organization */
    app.post('/api/v1/admin/orgs', async (req, reply) => {
        if (req.authRole !== 'super_admin') {
            return reply.code(403).send({ error: 'Super admin access required' });
        }

        const { org_id, org_name } = req.body as { org_id: string; org_name: string };
        if (!org_id || !org_name) {
            return reply.code(400).send({ error: 'org_id and org_name are required' });
        }

        const orgKey = `org_${uuidv4()}`;
        const orgKeyHash = createHash('sha256').update(orgKey).digest('hex');
        const defaultClient = getDefaultClient();

        try {
            await defaultClient.insert({
                table: 'org_registry',
                values: [{ org_id, org_name, org_key_hash: orgKeyHash, is_active: 1 }],
                format: 'JSONEachRow',
            });

            return reply.code(201).send({
                message: `Organization "${org_name}" created`,
                org_id,
                org_key: orgKey,
                warning: 'Save this org key — it will not be shown again.',
            });
        } catch (err: any) {
            return reply.code(500).send({ error: err.message });
        }
    });

    /** GET /api/v1/admin/orgs — List all organizations */
    app.get('/api/v1/admin/orgs', async (req, reply) => {
        if (req.authRole !== 'super_admin') {
            return reply.code(403).send({ error: 'Super admin access required' });
        }

        const defaultClient = getDefaultClient();
        const result = await defaultClient.query({
            query: 'SELECT org_id, org_name, is_active, created_at FROM org_registry FINAL',
            format: 'JSONEachRow',
        });
        const rows = await result.json();
        return reply.send({ data: rows });
    });

    /** POST /api/v1/admin/orgs/:org_id/rotate-key — Rotate org key */
    app.post('/api/v1/admin/orgs/:org_id/rotate-key', async (req, reply) => {
        if (req.authRole !== 'super_admin') {
            return reply.code(403).send({ error: 'Super admin access required' });
        }

        const { org_id } = req.params as { org_id: string };
        const newKey = `org_${uuidv4()}`;
        const newKeyHash = createHash('sha256').update(newKey).digest('hex');
        const defaultClient = getDefaultClient();

        try {
            // Get existing org
            const result = await defaultClient.query({
                query: `SELECT org_id, org_name FROM org_registry FINAL WHERE org_id = {org_id: String} AND is_active = 1`,
                query_params: { org_id },
                format: 'JSONEachRow',
            });
            const rows = await result.json<Array<{ org_id: string; org_name: string }>>();
            if (rows.length === 0 || !rows[0]) {
                return reply.code(404).send({ error: 'Organization not found' });
            }

            // Insert new row (ReplacingMergeTree will collapse old one)
            await defaultClient.insert({
                table: 'org_registry',
                values: [{
                    org_id,
                    org_name: rows[0].org_name,
                    org_key_hash: newKeyHash,
                    is_active: 1,
                }],
                format: 'JSONEachRow',
            });

            return reply.send({
                message: `Key rotated for org "${org_id}"`,
                org_key: newKey,
                warning: 'Save this org key — it will not be shown again. Old key is now invalid.',
            });
        } catch (err: any) {
            return reply.code(500).send({ error: err.message });
        }
    });

    /** PATCH /api/v1/admin/orgs/:org_id/toggle — Toggle org Active/Inactive */
    app.patch('/api/v1/admin/orgs/:org_id/toggle', async (req, reply) => {
        if (req.authRole !== 'super_admin') {
            return reply.code(403).send({ error: 'Super admin access required' });
        }

        const { org_id } = req.params as { org_id: string };
        const defaultClient = getDefaultClient();

        const checkResult = await defaultClient.query({
            query: `SELECT * FROM org_registry FINAL WHERE org_id = {org_id: String}`,
            query_params: { org_id },
            format: 'JSONEachRow',
        });
        const existing = await checkResult.json<Array<any>>();
        if (existing.length === 0 || !existing[0]) {
            return reply.code(404).send({ error: 'Organization not found' });
        }

        const newStatus = existing[0].is_active === 1 ? 0 : 1;

        await defaultClient.insert({
            table: 'org_registry',
            values: [{ ...existing[0], is_active: newStatus }],
            format: 'JSONEachRow',
        });

        return reply.send({ message: `Organization "${org_id}" is now ${newStatus === 1 ? 'Active' : 'Inactive'}`, is_active: newStatus });
    });

    /** DELETE /api/v1/admin/orgs/:org_id/hard — Hard Delete Org (Removes Registry) */
    app.delete('/api/v1/admin/orgs/:org_id/hard', async (req, reply) => {
        if (req.authRole !== 'super_admin') {
            return reply.code(403).send({ error: 'Super admin access required for hard delete' });
        }

        const { org_id } = req.params as { org_id: string };
        const defaultClient = getDefaultClient();

        // Check if Org exists
        const checkResult = await defaultClient.query({
            query: `SELECT * FROM org_registry FINAL WHERE org_id = {org_id: String}`,
            query_params: { org_id },
            format: 'JSONEachRow',
        });
        const existing = await checkResult.json<Array<any>>();
        if (existing.length === 0 || !existing[0]) {
            return reply.code(404).send({ error: 'Organization not found' });
        }

        // Check if Org has any CMS connected to it
        const cmsResult = await defaultClient.query({
            query: `SELECT COUNT() AS cnt FROM cms_registry FINAL WHERE org_id = {org_id: String}`,
            query_params: { org_id },
            format: 'JSONEachRow',
        });
        const cmsData = await cmsResult.json<Array<{ cnt: string }>>();
        const cmsCount = parseInt(cmsData[0]?.cnt || '0', 10);

        if (cmsCount > 0) {
            return reply.code(400).send({
                error: `Cannot delete org "${org_id}" because it still has ${cmsCount} CMS registry entries. Please transfer or delete them first.`
            });
        }

        try {
            // Remove the row from org_registry using DELETE mutation
            await defaultClient.command({
                query: `ALTER TABLE org_registry DELETE WHERE org_id = {org_id: String}`,
                query_params: { org_id }
            });

            return reply.send({ message: `Organization "${org_id}" has been permanently deleted.` });
        } catch (err: any) {
            return reply.code(500).send({ error: `Hard delete failed: ${err.message}` });
        }
    });


    // ══════════════════════════════════════════════════════════
    // CMS REGISTRY (Super Admin + Org Admin)
    // ══════════════════════════════════════════════════════════

    /** POST /api/v1/admin/cms-registry — Register a new CMS */
    app.post('/api/v1/admin/cms-registry', async (req, reply) => {
        const { cms_id, cms_name, org_id: bodyOrgId } = req.body as {
            cms_id: string; cms_name: string; org_id?: string;
        };
        if (!cms_id || !cms_name) {
            return reply.code(400).send({ error: 'cms_id and cms_name are required' });
        }

        // Determine org_id: super admin must provide it, org admin auto-uses their own
        let orgId: string;
        if (req.authRole === 'super_admin') {
            orgId = bodyOrgId || '';
        } else if (req.authRole === 'org_admin') {
            orgId = req.orgId!;
        } else {
            return reply.code(403).send({ error: 'Admin access required' });
        }

        const dbName = `db_${cms_id.toLowerCase().replace(/[^a-z0-9_]/g, '_')}`;
        const apiKey = uuidv4();
        const apiKeyHash = createHash('sha256').update(apiKey).digest('hex');
        const defaultClient = getDefaultClient();

        try {
            // 1. Create database
            await defaultClient.command({ query: `CREATE DATABASE IF NOT EXISTS ${dbName}` });

            // 2. Create all tables in the new database
            const cmsClient = getClickHouseClient({ database: dbName });
            for (const ddl of CMS_DDL) {
                await cmsClient.command({ query: ddl });
            }

            // 2b. Run any migrations needed for altered tables
            try {
                for (const migration of CMS_MIGRATIONS) {
                    await cmsClient.command({ query: migration });
                }
            } catch (err: any) {
                console.warn(`[CMS_MIGRATIONS] Failed to run migration for ${dbName}:`, err.message);
            }

            // 3. Register in cms_registry with org_id
            await defaultClient.insert({
                table: 'cms_registry',
                values: [{
                    cms_id,
                    cms_name,
                    db_name: dbName,
                    api_key_hash: apiKeyHash,
                    org_id: orgId,
                    is_active: 1,
                }],
                format: 'JSONEachRow',
            });

            return reply.code(201).send({
                message: `CMS "${cms_name}" registered successfully`,
                cms_id,
                org_id: orgId,
                database: dbName,
                api_key: apiKey,
                warning: 'Save this API key — it will not be shown again.',
            });
        } catch (err: any) {
            return reply.code(500).send({ error: err.message });
        }
    });

    /** GET /api/v1/admin/cms-registry — List CMS (scoped by role) */
    app.get('/api/v1/admin/cms-registry', async (req, reply) => {
        const defaultClient = getDefaultClient();

        let query: string;
        let params: Record<string, string> = {};

        if (req.authRole === 'super_admin') {
            // Super admin sees all
            query = 'SELECT cms_id, cms_name, db_name, org_id, is_active, created_at FROM cms_registry FINAL';
        } else if (req.authRole === 'org_admin') {
            // Org admin only sees their CMS
            query = 'SELECT cms_id, cms_name, db_name, org_id, is_active, created_at FROM cms_registry FINAL WHERE org_id = {org_id: String}';
            params = { org_id: req.orgId! };
        } else {
            return reply.code(403).send({ error: 'Admin access required' });
        }

        const result = await defaultClient.query({ query, query_params: params, format: 'JSONEachRow' });
        const rows = await result.json();
        return reply.send({ data: rows });
    });

    /** POST /api/v1/admin/cms-registry/:cms_id/rotate-key — Rotate CMS API key */
    app.post('/api/v1/admin/cms-registry/:cms_id/rotate-key', async (req, reply) => {
        const { cms_id } = req.params as { cms_id: string };
        const defaultClient = getDefaultClient();

        // Check ownership
        const checkResult = await defaultClient.query({
            query: `SELECT cms_id, cms_name, db_name, org_id FROM cms_registry FINAL WHERE cms_id = {cms_id: String} AND is_active = 1`,
            query_params: { cms_id },
            format: 'JSONEachRow',
        });
        const existing = await checkResult.json<Array<{ cms_id: string; cms_name: string; db_name: string; org_id: string }>>();

        if (existing.length === 0 || !existing[0]) {
            return reply.code(404).send({ error: 'CMS not found' });
        }

        const cms = existing[0];

        // Org admin can only rotate keys for their own CMS
        if (req.authRole === 'org_admin' && cms.org_id !== req.orgId) {
            return reply.code(403).send({ error: 'Access denied: CMS belongs to another organization' });
        }

        const newKey = uuidv4();
        const newKeyHash = createHash('sha256').update(newKey).digest('hex');

        await defaultClient.insert({
            table: 'cms_registry',
            values: [{
                ...cms,
                api_key_hash: newKeyHash,
            }],
            format: 'JSONEachRow',
        });

        return reply.send({
            message: `API key rotated for CMS "${cms_id}"`,
            api_key: newKey,
            warning: 'Save this API key — it will not be shown again.',
        });
    });

    /** PATCH /api/v1/admin/cms-registry/:cms_id/toggle — Toggle Active/Inactive (Soft Delete / Reactivate) */
    app.patch('/api/v1/admin/cms-registry/:cms_id/toggle', async (req, reply) => {
        const { cms_id } = req.params as { cms_id: string };
        const defaultClient = getDefaultClient();

        const checkResult = await defaultClient.query({
            query: `SELECT * FROM cms_registry FINAL WHERE cms_id = {cms_id: String}`,
            query_params: { cms_id },
            format: 'JSONEachRow',
        });
        const existing = await checkResult.json<Array<any>>();
        if (existing.length === 0 || !existing[0]) {
            return reply.code(404).send({ error: 'CMS not found' });
        }

        // Org admin can only toggle their own CMS
        if (req.authRole === 'org_admin' && existing[0].org_id !== req.orgId) {
            return reply.code(403).send({ error: 'Access denied' });
        }

        const newStatus = existing[0].is_active === 1 ? 0 : 1;

        // Toggle via ReplacingMergeTree
        await defaultClient.insert({
            table: 'cms_registry',
            values: [{ ...existing[0], is_active: newStatus }],
            format: 'JSONEachRow',
        });

        return reply.send({ message: `CMS "${cms_id}" is now ${newStatus === 1 ? 'Active' : 'Inactive'}`, is_active: newStatus });
    });

    /** DELETE /api/v1/admin/cms-registry/:cms_id/hard — Hard Delete (Drop DB & Remove Registry) */
    app.delete('/api/v1/admin/cms-registry/:cms_id/hard', async (req, reply) => {
        if (req.authRole !== 'super_admin') {
            return reply.code(403).send({ error: 'Super admin access required for hard delete' });
        }

        const { cms_id } = req.params as { cms_id: string };
        const defaultClient = getDefaultClient();

        const checkResult = await defaultClient.query({
            query: `SELECT * FROM cms_registry FINAL WHERE cms_id = {cms_id: String}`,
            query_params: { cms_id },
            format: 'JSONEachRow',
        });
        const existing = await checkResult.json<Array<any>>();
        if (existing.length === 0 || !existing[0]) {
            return reply.code(404).send({ error: 'CMS not found' });
        }

        const dbName = existing[0].db_name;

        try {
            // 1. Drop the database to clean up all data (millions of rows)
            if (dbName && dbName !== 'default' && dbName !== 'system') {
                await defaultClient.command({ query: `DROP DATABASE IF EXISTS ${dbName}` });
            }

            // 2. Remove the row from cms_registry using DELETE mutation
            await defaultClient.command({
                query: `ALTER TABLE cms_registry DELETE WHERE cms_id = {cms_id: String}`,
                query_params: { cms_id }
            });

            return reply.send({ message: `CMS "${cms_id}" and database "${dbName}" have been permanently deleted` });
        } catch (err: any) {
            return reply.code(500).send({ error: `Hard delete failed: ${err.message}` });
        }
    });

    /** PATCH /api/v1/admin/cms-registry/:cms_id/org — Assign/change org_id of a CMS */
    app.patch('/api/v1/admin/cms-registry/:cms_id/org', async (req, reply) => {
        if (req.authRole !== 'super_admin') {
            return reply.code(403).send({ error: 'Super admin access required' });
        }

        const { cms_id } = req.params as { cms_id: string };
        const { org_id } = req.body as { org_id: string };
        if (!org_id) {
            return reply.code(400).send({ error: 'org_id is required' });
        }

        const defaultClient = getDefaultClient();
        const checkResult = await defaultClient.query({
            query: `SELECT * FROM cms_registry FINAL WHERE cms_id = {cms_id: String} AND is_active = 1`,
            query_params: { cms_id },
            format: 'JSONEachRow',
        });
        const existing = await checkResult.json<Array<any>>();
        if (existing.length === 0 || !existing[0]) {
            return reply.code(404).send({ error: 'CMS not found' });
        }

        await defaultClient.insert({
            table: 'cms_registry',
            values: [{ ...existing[0], org_id }],
            format: 'JSONEachRow',
        });

        return reply.send({ message: `CMS "${cms_id}" assigned to org "${org_id}"` });
    });

    /** DELETE /api/v1/admin/orgs/:org_id — Deactivate (soft-delete) an organization */
    app.delete('/api/v1/admin/orgs/:org_id', async (req, reply) => {
        if (req.authRole !== 'super_admin') {
            return reply.code(403).send({ error: 'Super admin access required' });
        }

        const { org_id } = req.params as { org_id: string };
        const defaultClient = getDefaultClient();

        const checkResult = await defaultClient.query({
            query: `SELECT * FROM org_registry FINAL WHERE org_id = {org_id: String} AND is_active = 1`,
            query_params: { org_id },
            format: 'JSONEachRow',
        });
        const existing = await checkResult.json<Array<any>>();
        if (existing.length === 0 || !existing[0]) {
            return reply.code(404).send({ error: 'Organization not found' });
        }

        await defaultClient.insert({
            table: 'org_registry',
            values: [{ ...existing[0], is_active: 0 }],
            format: 'JSONEachRow',
        });

        return reply.send({ message: `Organization "${org_id}" deactivated` });
    });

    /** GET /api/v1/admin/cms-registry/:cms_id/metrics — Get DB Size and Row Counts */
    app.get('/api/v1/admin/cms-registry/:cms_id/metrics', async (req, reply) => {
        const { cms_id } = req.params as { cms_id: string };
        const defaultClient = getDefaultClient();

        // Check ownership & existence
        const checkResult = await defaultClient.query({
            query: `SELECT db_name, org_id FROM cms_registry FINAL WHERE cms_id = {cms_id: String}`,
            query_params: { cms_id },
            format: 'JSONEachRow',
        });
        const existing = await checkResult.json<Array<{ db_name: string; org_id: string }>>();
        if (existing.length === 0 || !existing[0]) {
            return reply.code(404).send({ error: 'CMS not found' });
        }

        const cms = existing[0];

        // Org admin can only view metrics for their own CMS
        /* if (req.authRole === 'org_admin' && cms.org_id !== req.orgId) {
            return reply.code(403).send({ error: 'Access denied: CMS belongs to another organization' });
        } */

        const dbName = cms.db_name;

        try {
            // Native ClickHouse query to sum part sizes by table and partition
            const query = `
                SELECT 
                    table,
                    partition,
                    sum(rows) AS total_rows,
                    sum(data_uncompressed_bytes) AS bytes_uncompressed,
                    sum(data_compressed_bytes) AS bytes_compressed,
                    formatReadableSize(sum(data_uncompressed_bytes)) AS data_uncompressed,
                    formatReadableSize(sum(data_compressed_bytes)) AS data_compressed
                FROM system.parts
                WHERE database = {dbName: String} AND active
                GROUP BY table, partition
                ORDER BY partition DESC, table ASC
            `;

            const metricsResult = await defaultClient.query({
                query,
                query_params: { dbName },
                format: 'JSONEachRow'
            });
            const partsData = await metricsResult.json<Array<any>>();

            // Secondary Query: Get actual logical row counts split by report_type for UX
            let logicalBreakdown: Array<{ src: string; upload_month: number; report_type: string; rows: string }> = [];
            try {
                // Determine if the tables exist first to avoid crashing
                const hasAds = partsData.some(p => p.table === 'ads_revenue_enriched');
                const hasSub = partsData.some(p => p.table === 'subscription_revenue');

                const breakdownQueries = [];
                if (hasAds) {
                    breakdownQueries.push(`SELECT 'ads' AS src, upload_month, report_type, count() AS rows FROM ${dbName}.ads_revenue_enriched GROUP BY upload_month, report_type`);
                }
                if (hasSub) {
                    breakdownQueries.push(`SELECT 'sub' AS src, upload_month, if(adjustment_type IN ('None', ''), 'subscription_raw', 'sub_adjustment') as report_type, count() AS rows FROM ${dbName}.subscription_revenue GROUP BY upload_month, adjustment_type`);
                }

                if (breakdownQueries.length > 0) {
                    const breakdownQuery = breakdownQueries.join(' UNION ALL ');
                    const breakdownResult = await defaultClient.query({
                        query: breakdownQuery,
                        format: 'JSONEachRow'
                    });
                    logicalBreakdown = await breakdownResult.json();
                }
            } catch (e) {
                console.warn(`[admin.ts] Failed to fetch logical breakdown for ${dbName}:`, e);
            }

            // Simple exact formatter matches CH formatReadableSize logic minimally
            const formatSize = (bytes: number) => {
                const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
                let i = 0;
                let val = bytes;
                while (val >= 1024 && i < units.length - 1) {
                    val /= 1024;
                    i++;
                }
                return `${val.toFixed(2)} ${units[i]}`;
            };

            let grandTotalRows = 0;
            let grandTotalBytes = 0;
            const periodMap = new Map<string, any>();

            for (const row of partsData) {
                const tableRows = parseInt(row.total_rows || '0', 10);
                const tableBytes = parseInt(row.bytes_compressed || '0', 10);
                const tableUncompressedBytes = parseInt(row.bytes_uncompressed || '0', 10);

                grandTotalRows += tableRows;
                grandTotalBytes += tableBytes;

                let periodName = row.partition; // E.g., "('',202601)" or "202601"
                const periodMatch = periodName.match(/(\d{6})/);
                if (periodMatch) {
                    const ym = periodMatch[1];
                    periodName = `${ym.slice(0, 4)}-${ym.slice(4, 6)}`;
                } else if (periodName === 'tuple()') {
                    periodName = 'System / Metadata Tables';
                }

                if (!periodMap.has(periodName)) {
                    periodMap.set(periodName, {
                        period: periodName,
                        total_rows: 0,
                        total_size_bytes: 0,
                        total_uncompressed_bytes: 0,
                        tables: []
                    });
                }

                const group = periodMap.get(periodName);
                group.total_rows += tableRows;
                group.total_size_bytes += tableBytes;
                group.total_uncompressed_bytes += tableUncompressedBytes;

                // Handle Splitting for Core Tables
                if (row.table === 'ads_revenue_enriched' || row.table === 'subscription_revenue') {
                    const srcKey = row.table === 'ads_revenue_enriched' ? 'ads' : 'sub';

                    // Extract the 6 digits YYYYMM format robustly from tuples like ('', 202601)
                    const monthMatch = row.partition.match(/(\d{6})/);
                    const monthKey = monthMatch ? parseInt(monthMatch[1], 10) : 0;

                    const subRows = logicalBreakdown.filter(b => b.src === srcKey && Number(b.upload_month) === monthKey);

                    if (subRows.length > 0) {
                        // Push the "physical table" container with size FIRST so it acts as the Parent Block visually
                        group.tables.push({
                            table: `[Storage Block] ${row.table}`,
                            total_rows: 0,
                            bytes_compressed: tableBytes,
                            data_uncompressed: row.data_uncompressed,
                            data_compressed: row.data_compressed
                        });

                        subRows.forEach(sub => {
                            // Map report_type back to user-friendly UX names
                            const subName = sub.report_type === 'claim_raw' ? 'Claim Raw'
                                : sub.report_type === 'ads_adjustment' ? 'Ads Adjustment'
                                    : sub.report_type === 'subscription_raw' ? 'Red Label (Sub)'
                                        : sub.report_type === 'sub_adjustment' ? 'Sub Adjustment'
                                            : sub.report_type;

                            group.tables.push({
                                table: `${row.table} (${subName})`,
                                total_rows: parseInt(sub.rows, 10),
                                bytes_compressed: null, // Logical split, we don't know exact bytes
                                data_uncompressed: '-',
                                data_compressed: '-'
                            });
                        });
                        continue;
                    }
                }

                // Default Fallback
                group.tables.push({
                    table: row.table,
                    total_rows: tableRows,
                    bytes_compressed: tableBytes,
                    data_uncompressed: row.data_uncompressed,
                    data_compressed: row.data_compressed
                });
            }

            // Convert map to array and format size
            const periods = Array.from(periodMap.values()).map(p => ({
                ...p,
                total_size_formatted: formatSize(p.total_size_bytes),
                total_uncompressed_formatted: formatSize(p.total_uncompressed_bytes),
            }));

            // Sort periods (DESC so newest is first, System / Metadata Tables at the end)
            periods.sort((a, b) => {
                if (a.period === 'System / Metadata Tables') return 1;
                if (b.period === 'System / Metadata Tables') return -1;
                return b.period.localeCompare(a.period);
            });

            return reply.send({
                total_size: formatSize(grandTotalBytes),
                total_rows: grandTotalRows,
                periods: periods,
                debug_breakdown: logicalBreakdown
            });
        } catch (err: any) {
            return reply.code(500).send({ error: `Failed to fetch metrics: ${err.message}` });
        }
    });

    // ══════════════════════════════════════════════════════════
    // CHANNEL LABEL MAPPING (Existing — unchanged)
    // ══════════════════════════════════════════════════════════

    /** POST /api/v1/admin/channel-map — Add/update channel mapping */
    app.post('/api/v1/admin/channel-map', async (req, reply) => {
        const { cms_id, channel_id, asset_label, notes = '' } = req.body as {
            cms_id: string; channel_id: string; asset_label: string; notes?: string;
        };
        if (!cms_id || !channel_id || !asset_label) {
            return reply.code(400).send({ error: 'cms_id, channel_id, and asset_label are required' });
        }

        const dbName = `db_${cms_id}`;
        const client = getClickHouseClient({ database: dbName });

        await client.insert({
            table: 'channel_label_map',
            values: [{ channel_id, asset_label, cms_name: cms_id, notes }],
            format: 'JSONEachRow',
        });

        return reply.send({ message: 'Channel mapping saved', channel_id, asset_label });
    });

    /** GET /api/v1/admin/channel-map?cms_id=xxx — List mappings for a CMS */
    app.get('/api/v1/admin/channel-map', async (req, reply) => {
        const { cms_id } = req.query as { cms_id?: string };
        if (!cms_id) {
            return reply.code(400).send({ error: 'cms_id is required' });
        }

        const dbName = `db_${cms_id}`;
        const client = getClickHouseClient({ database: dbName });
        const result = await client.query({
            query: 'SELECT channel_id, asset_label, cms_name, notes, updated_at FROM channel_label_map FINAL',
            format: 'JSONEachRow',
        });
        const rows = await result.json();
        return reply.send({ data: rows });
    });

    /** DELETE /api/v1/admin/channel-map — Remove a channel mapping */
    app.delete('/api/v1/admin/channel-map', async (req, reply) => {
        const { cms_id, channel_id } = req.query as { cms_id?: string; channel_id?: string };
        if (!cms_id || !channel_id) {
            return reply.code(400).send({ error: 'cms_id and channel_id are required' });
        }

        const dbName = `db_${cms_id}`;
        const client = getClickHouseClient({ database: dbName });

        await client.command({
            query: `ALTER TABLE channel_label_map DELETE WHERE channel_id = {channel_id: String}`,
            query_params: { channel_id },
        });

        return reply.send({ message: 'Channel mapping deleted', channel_id });
    });

    // ══════════════════════════════════════════════════════════
    // ASSET LABEL CORRECTION (Existing — unchanged)
    // ══════════════════════════════════════════════════════════

    /** POST /api/v1/admin/correct-label — Fix wrong label for an asset */
    app.post('/api/v1/admin/correct-label', async (req, reply) => {
        const { cms_id, month, asset_id, new_label } = req.body as {
            cms_id: string; month: string; asset_id: string; new_label: string;
        };
        if (!cms_id || !month || !asset_id || !new_label) {
            return reply.code(400).send({
                error: 'cms_id, month (YYYYMM), asset_id, and new_label are required',
            });
        }

        const dbName = `db_${cms_id}`;
        const client = getClickHouseClient({ database: dbName });

        const result = await client.query({
            query: `
        SELECT * FROM ads_revenue_enriched
        WHERE upload_month = {month: UInt32}
          AND asset_id = {asset_id: String}
      `,
            query_params: { month: parseInt(month), asset_id },
            format: 'JSONEachRow',
        });

        const rows = await result.json<Record<string, unknown>[]>();
        if (rows.length === 0) {
            return reply.code(404).send({ error: 'No rows found for the given asset_id and month' });
        }

        const corrected = rows.map(row => ({
            ...row,
            label: new_label,
            label_source: 'corrected',
            ingested_at: new Date().toISOString().replace('T', ' ').slice(0, 19),
        }));

        await client.insert({
            table: 'ads_revenue_enriched',
            values: corrected,
            format: 'JSONEachRow',
        });

        return reply.send({
            message: `Corrected ${corrected.length} rows`,
            asset_id,
            new_label,
            rows_affected: corrected.length,
        });
    });

    // ══════════════════════════════════════════════════════════
    // RAW QUERY EXECUTION (Super Admin only — B2B Exporter)
    // ══════════════════════════════════════════════════════════

    /** POST /api/v1/admin/query — Execute a raw ClickHouse query (For B2B Export extraction) */
    app.post('/api/v1/admin/query', async (req, reply) => {
        if (req.authRole !== 'super_admin' && req.authRole !== 'org_admin') {
            return reply.code(403).send({ error: 'Admin access required for raw query execution' });
        }

        const { query, db = 'default' } = req.body as { query: string; db?: string };
        if (!query) {
            return reply.code(400).send({ error: 'query string is required' });
        }

        const client = getClickHouseClient({ database: db });

        try {
            const result = await client.query({
                query,
                format: 'JSONEachRow',
            });
            const rows = await result.json();
            return reply.send({ data: rows });
        } catch (err: any) {
            console.error('[Raw Query Error]', err.message);
            return reply.code(500).send({ error: `Query failed: ${err.message}` });
        }
    });

    /** POST /api/v1/admin/query/csv — Execute a raw ClickHouse query, Returning Raw CSV as a stream */
    app.post('/api/v1/admin/query/csv', async (req, reply) => {
        if (req.authRole !== 'super_admin' && req.authRole !== 'org_admin') {
            return reply.code(403).send({ error: 'Admin access required for raw query execution' });
        }

        const { query, db = 'default' } = req.body as { query: string; db?: string };
        if (!query) {
            return reply.code(400).send({ error: 'query string is required' });
        }

        const client = getClickHouseClient({ database: db });

        try {
            const result = await client.query({
                query,
                format: 'CSVWithNames',
            });
            // BUG FIX: result.stream() returns an object-mode Transform stream (emitting Row arrays),
            // which Fastify rejects with FST_ERR_REP_INVALID_PAYLOAD_TYPE.
            // Instead, access the raw underlying HTTP response stream (_stream) which emits
            // Buffer/string chunks that Fastify can serialize as-is.
            const rawStream = (result as any)._stream;
            reply.header('Content-Type', 'text/csv');
            return reply.send(rawStream);
        } catch (err: any) {
            console.error('[Raw Query CSV Error]', err.message);
            return reply.code(500).send({ error: `CSV Query failed: ${err.message}` });
        }
    });
}
