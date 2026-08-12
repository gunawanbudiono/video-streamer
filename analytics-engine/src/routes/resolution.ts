import type { FastifyInstance } from 'fastify';
import { authMiddleware } from '../middleware/auth.js';
import { getClickHouseClient, getDefaultClient } from '../config/clickhouse.js';
import { getTenantById } from '../config/tenants.js';

/** Helper: resolve CMS client with cms_registry fallback (same as ingest routes) */
async function resolveCmsClient(reqCmsId: string): Promise<{ cms_id: string; client: any } | null> {
    console.log(`[Resolution] resolveCmsClient called with: "${reqCmsId}"`);
    const memTenant = getTenantById(reqCmsId);
    if (memTenant) {
        console.log(`[Resolution] Found in-memory tenant: ${memTenant.cmsId}`);
        return { cms_id: memTenant.cmsId, client: memTenant.ingestClient };
    }
    console.log(`[Resolution] Not in memory, checking cms_registry...`);
    // Fallback: check cms_registry in ClickHouse
    try {
        const defaultClient = getDefaultClient();
        const res = await defaultClient.query({
            query: `SELECT cms_id, db_name FROM cms_registry FINAL WHERE cms_id = {cms_id: String} AND is_active = 1`,
            query_params: { cms_id: reqCmsId },
            format: 'JSONEachRow'
        });
        const rows = await res.json() as any[];
        console.log(`[Resolution] cms_registry returned ${rows.length} rows:`, JSON.stringify(rows));
        if (rows.length > 0) {
            const dbName = rows[0].db_name || `db_${reqCmsId.replace(/-/g, '_')}`;
            const client = getClickHouseClient({ database: dbName });
            return { cms_id: reqCmsId, client };
        }
    } catch (err) {
        console.error(`[Resolution] cms_registry lookup failed for ${reqCmsId}:`, err);
    }
    console.log(`[Resolution] CMS "${reqCmsId}" NOT FOUND anywhere!`);
    return null;
}

export async function resolutionRoutes(app: FastifyInstance): Promise<void> {
    app.addHook('onRequest', authMiddleware);

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/resolution/sync
    // Trigger bulk mutation on ClickHouse historical data to fill blank asset labels
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/resolution/sync', async (req, reply) => {
        const body = req.body as { mappings?: { resolve_id: string, asset_label: string, asset_title?: string, artist?: string, isrc?: string, upc?: string, record_label?: string, channel_name?: string }[], cms_id?: string };
        const mappings = body.mappings;

        if (!mappings || mappings.length === 0) {
            return reply.code(400).send({ error: 'mappings payload is required and cannot be empty array' });
        }

        let cms_id: string;
        let client: any;

        if (req.isAdmin || req.authRole === 'org_admin') {
            const reqCmsId = body.cms_id;
            if (!reqCmsId) return reply.code(400).send({ error: 'cms_id is required for Admin Master Key multi-CMS uploads' });
            const resolved = await resolveCmsClient(reqCmsId);
            if (!resolved) {
                return reply.code(404).send({ error: `CMS ${reqCmsId} database not active or found in tenant config` });
            }
            cms_id = resolved.cms_id;
            client = resolved.client;
        } else {
            if (!req.tenant) return reply.code(401).send({ error: 'Unauthorized' });
            cms_id = req.tenant.cmsId;
            client = req.tenant.ingestClient;
        }

        try {
            const chunkSize = 50;
            for (let i = 0; i < mappings.length; i += chunkSize) {
                const chunk = mappings.slice(i, i + chunkSize);

                const condAssetLabels = [];
                const condAssetTitle = [];
                const condArtist = [];
                const condIsrc = [];
                const condUpc = [];
                const condLabel = [];

                chunk.forEach(m => {
                    const idSafe = m.resolve_id.replace(/'/g, "''");
                    const checkId = `(asset_id = '${idSafe}' OR (video_id = '${idSafe}' AND asset_id = ''))`;

                    condAssetLabels.push(checkId, `'${(m.asset_label || '').replace(/'/g, "''")}'`);
                    if (m.asset_title) condAssetTitle.push(checkId, `'${m.asset_title.replace(/'/g, "''")}'`);
                    if (m.artist) condArtist.push(checkId, `'${m.artist.replace(/'/g, "''")}'`);
                    if (m.isrc) condIsrc.push(checkId, `'${m.isrc.replace(/'/g, "''")}'`);
                    if (m.upc) condUpc.push(checkId, `'${m.upc.replace(/'/g, "''")}'`);
                    if (m.record_label) condLabel.push(checkId, `'${m.record_label.replace(/'/g, "''")}'`);
                });

                condAssetLabels.push('asset_labels');
                condAssetTitle.push('asset_title');
                condArtist.push('artist');
                condIsrc.push('isrc');
                condUpc.push('upc');
                condLabel.push('label'); // using label column name from ClickHouse

                const updatesAds = [`asset_labels = multiIf(${condAssetLabels.join(', ')})`];
                if (condAssetTitle.length > 1) updatesAds.push(`asset_title = multiIf(${condAssetTitle.join(', ')})`);
                if (condArtist.length > 1) updatesAds.push(`artist = multiIf(${condArtist.join(', ')})`);
                if (condIsrc.length > 1) updatesAds.push(`isrc = multiIf(${condIsrc.join(', ')})`);
                if (condUpc.length > 1) updatesAds.push(`upc = multiIf(${condUpc.join(', ')})`);
                if (condLabel.length > 1) updatesAds.push(`label = multiIf(${condLabel.join(', ')})`);

                const resolveIds = chunk.map(m => `'${m.resolve_id.replace(/'/g, "''")}'`).join(',');
                const qAdsFallback = `ALTER TABLE ads_revenue_enriched
                              UPDATE ${updatesAds.join(', ')}
                              WHERE (asset_id IN (${resolveIds}) OR video_id IN (${resolveIds}))`;

                try {
                    await client.command({ query: qAdsFallback });
                } catch (e: any) {
                    console.error("[Resolution Sync] Failed mutation on ads_revenue_enriched", e.message);
                }

                const qSubFallback = `ALTER TABLE subscription_revenue
                              UPDATE ${updatesAds.join(', ')}
                              WHERE (asset_id IN (${resolveIds}) OR video_id IN (${resolveIds}))`;

                try {
                    await client.command({ query: qSubFallback });
                } catch (e: any) {
                    console.error("[Resolution Sync] Failed mutation on subscription_revenue", e.message);
                }
            }

            return reply.send({ success: true, processed_mappings: mappings.length, message: "Retroactive resolution mutations dispatched successfully" });

        } catch (err: any) {
            console.error('[Resolution Sync] Fatal error', err);
            return reply.code(500).send({ error: err.message });
        }
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/resolution/unresolved_assets
    // Query ClickHouse for blank asset labels, filtered by mapped channel IDs
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/resolution/unresolved_assets', async (req, reply) => {
        const body = req.body as { channel_ids?: string[], cms_id?: string };
        const channelIds = body.channel_ids || [];

        if (channelIds.length === 0) {
            return reply.send({ success: true, data: [] });
        }

        let cms_id: string;
        let client: any;

        if (req.isAdmin || req.authRole === 'org_admin') {
            const reqCmsId = body.cms_id;
            if (!reqCmsId) return reply.code(400).send({ error: 'cms_id is required for Admin Master Key queries' });
            const resolved = await resolveCmsClient(reqCmsId);
            if (!resolved) return reply.code(404).send({ error: `CMS ${reqCmsId} not found` });
            cms_id = resolved.cms_id;
            client = resolved.client;
        } else {
            if (!req.tenant) return reply.code(401).send({ error: 'Unauthorized' });
            cms_id = req.tenant.cmsId;
            client = req.tenant.readClient;
        }

        try {
            const formattedChannelIds = channelIds.map((id) => `'${id}'`).join(',');

            // Union Ads and Subs to properly track stuck revenue per asset/video
            const query = `
                SELECT resolve_id, any(title) as title, any(channel_id) as channel_id, 
                       any(artist) as artist, any(isrc) as isrc, any(upc) as upc, any(label) as record_label,
                       sum(revenue) as stuck_revenue, sum(views) as stuck_views
                FROM (
                    SELECT if(asset_id='', video_id, asset_id) as resolve_id, 
                           if(asset_id='', video_title, asset_title) as title, 
                           channel_id, artist, isrc, upc, label,
                           yt_rev_total + partner_rev_total as revenue, owned_views as views
                    FROM ads_revenue_enriched
                    WHERE asset_labels = '' AND channel_id IN (${formattedChannelIds})
                    
                    UNION ALL
                    
                    SELECT if(asset_id='', video_id, asset_id) as resolve_id, 
                           asset_title as title, 
                           if(asset_channel_id='', channel_id, asset_channel_id) as channel_id, 
                           artist, isrc, upc, label,
                           yt_rev_total + partner_rev_total as revenue, owned_views as views
                    FROM subscription_revenue
                    WHERE asset_labels = '' AND (asset_channel_id IN (${formattedChannelIds}) OR channel_id IN (${formattedChannelIds}))
                )
                GROUP BY resolve_id
                ORDER BY stuck_revenue DESC
                LIMIT 5000
            `;

            const resultSet = await client.query({
                query,
                format: 'JSONEachRow'
            });

            const rows = await resultSet.json();
            return reply.send({ success: true, data: rows });

        } catch (err: any) {
            console.error('[Unresolved Assets] Query error', err);
            return reply.code(500).send({ error: err.message });
        }
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/resolution/all_assets
    // Returns ALL unique assets (with or without labels) from ClickHouse
    // Used by Asset Catalog "Sync from Analytics Engine" button
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/resolution/all_assets', async (req, reply) => {
        const body = req.body as { cms_id?: string, limit?: number };

        let cms_id: string;
        let client: any;

        // HARDBLOCK: Endpoint `all_assets` sangat berbahaya karena mengekspos semua raw data CMS.
        // Sesuai permintaan User, hanya Super Admin ("org_admin" / master key) yang berhak mengeksekusi ini.
        if (!req.isAdmin && req.authRole !== 'org_admin') {
            return reply.code(403).send({ error: 'Forbidden: all_assets endpoint is highly restricted to Admin only. Use unresolved_assets instead.' });
        }

        const reqCmsId = body.cms_id;
        if (!reqCmsId) return reply.code(400).send({ error: 'cms_id is required' });
        
        const resolved = await resolveCmsClient(reqCmsId);
        if (!resolved) return reply.code(404).send({ error: `CMS ${reqCmsId} not found` });
        
        cms_id = resolved.cms_id;
        client = resolved.client;

        const limit = Math.min(body.limit || 10000, 50000);

        try {
            const query = `
                SELECT 
                    resolve_id as asset_id,
                    anyIf(asset_label, asset_label != '') as asset_label,
                    anyIf(asset_title, asset_title != '') as asset_title,
                    anyIf(artist, artist != '') as artist,
                    anyIf(isrc, isrc != '') as isrc,
                    anyIf(upc, upc != '') as upc,
                    anyIf(record_label, record_label != '') as record_label,
                    anyIf(channel_id, channel_id != '') as channel_id,
                    anyIf(channel_name, channel_name != '') as channel_name,
                    anyIf(asset_type, asset_type != '') as asset_type,
                    sum(total_yt_revenue) as total_yt_revenue,
                    sum(total_partner_revenue) as total_partner_revenue,
                    sum(total_views) as total_views
                FROM (
                    SELECT 
                        if(asset_id = '', video_id, asset_id) as resolve_id,
                        asset_labels as asset_label,
                        asset_title,
                        artist,
                        isrc,
                        upc,
                        label as record_label,
                        channel_id,
                        channel_display_name as channel_name,
                        asset_type,
                        yt_rev_total as total_yt_revenue,
                        partner_rev_total as total_partner_revenue,
                        owned_views as total_views
                    FROM ads_revenue_enriched
                    WHERE asset_id != '' OR video_id != ''

                    UNION ALL

                    SELECT 
                        if(asset_id = '', video_id, asset_id) as resolve_id,
                        asset_labels as asset_label,
                        asset_title,
                        artist,
                        isrc,
                        upc,
                        label as record_label,
                        if(asset_channel_id = '', channel_id, asset_channel_id) as channel_id,
                        '' as channel_name,
                        asset_type,
                        yt_rev_total as total_yt_revenue,
                        partner_rev_total as total_partner_revenue,
                        owned_views as total_views
                    FROM subscription_revenue
                    WHERE asset_id != '' OR video_id != ''
                )
                GROUP BY resolve_id
                ORDER BY total_partner_revenue DESC
                LIMIT ${limit}
            `;

            const resultSet = await client.query({
                query,
                format: 'JSONEachRow'
            });

            const rows = await resultSet.json();
            return reply.send({ success: true, data: rows, total: rows.length });

        } catch (err: any) {
            console.error('[All Assets] Query error', err);
            return reply.code(500).send({ error: err.message });
        }
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/resolution/assets_by_upc
    // Returns (asset_id, asset_type, revenue) grouped by UPC
    // Used by Split Manager to get asset breakdown per release
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/resolution/assets_by_upc', async (req, reply) => {
        const body = req.body as { upcs?: string[]; cms_id?: string };
        const upcs = body.upcs;

        if (!upcs || !Array.isArray(upcs) || upcs.length === 0) {
            return reply.code(400).send({ error: 'upcs is required and must be a non-empty array' });
        }

        if (upcs.length > 500) {
            return reply.code(400).send({ error: 'Maximum 500 UPCs per request' });
        }

        try {
            let cmsClients: { cms_id: string; client: any }[] = [];

            if (body.cms_id) {
                const resolved = await resolveCmsClient(body.cms_id);
                if (resolved) cmsClients.push(resolved);
            } else {
                const defaultClient = getDefaultClient();
                if (defaultClient) {
                    try {
                        let registryQuery = `SELECT cms_id FROM cms_registry FINAL WHERE is_active = 1`;
                        let registryParams: Record<string, string> = {};

                        if (req.authRole === 'org_admin' && req.orgId) {
                            registryQuery += ` AND org_id = {org_id: String}`;
                            registryParams = { org_id: req.orgId };
                        }

                        const rs = await defaultClient.query({ query: registryQuery, query_params: registryParams, format: 'JSONEachRow' });
                        const registeredCms = await rs.json() as { cms_id: string }[];
                        const { getAllTenants } = await import('../config/tenants.js');
                        const allTenants = getAllTenants();

                        for (const reg of registeredCms) {
                            const tenant = allTenants.find(t => t.cmsId === reg.cms_id);
                            if (tenant) {
                                cmsClients.push({ cms_id: reg.cms_id, client: tenant.readClient });
                            } else {
                                const client = getClickHouseClient({ database: `db_${reg.cms_id}` });
                                cmsClients.push({ cms_id: reg.cms_id, client });
                            }
                        }
                    } catch (e) {
                        const { getAllTenants } = await import('../config/tenants.js');
                        for (const tenant of getAllTenants()) {
                            cmsClients.push({ cms_id: tenant.cmsId, client: tenant.readClient });
                        }
                    }
                } else {
                    const { getAllTenants } = await import('../config/tenants.js');
                    for (const tenant of getAllTenants()) {
                        if (!cmsClients.find(c => c.cms_id === tenant.cmsId)) {
                            cmsClients.push({ cms_id: tenant.cmsId, client: tenant.readClient });
                        }
                    }
                }
            }

            console.log(`[AssetsByUPC] Querying ${cmsClients.length} CMS database(s) for ${upcs.length} UPCs`);

            // Accumulate results: { [upc]: { [asset_id]: { asset_type, ads_rev, sub_rev, total_views } } }
            const resultMap: Record<string, Record<string, { asset_type: string; ads_rev: number; sub_rev: number; total_views: number }>> = {};

            for (const { cms_id, client } of cmsClients) {
                try {
                    // Query 1: Asset identity from ALL periods (ensures all assets show up)
                    const identityQuery = `
                        SELECT
                            upc,
                            asset_id,
                            anyIf(asset_type, asset_type != '') as asset_type
                        FROM (
                            SELECT upc, asset_id, asset_type FROM ads_revenue_enriched
                            WHERE upc IN ({upcs: Array(String)}) AND asset_id != ''
                            UNION ALL
                            SELECT upc, asset_id, asset_type FROM subscription_revenue
                            WHERE upc IN ({upcs: Array(String)}) AND asset_id != ''
                        )
                        GROUP BY upc, asset_id
                    `;

                    // Query 2: Revenue from latest period only
                    const revenueQuery = `
                        SELECT
                            upc,
                            asset_id,
                            sum(ads_rev) as ads_rev,
                            sum(sub_rev) as sub_rev,
                            sum(views) as total_views
                        FROM (
                            SELECT upc, asset_id,
                                partner_rev_total as ads_rev,
                                0 as sub_rev,
                                owned_views as views
                            FROM ads_revenue_enriched
                            WHERE upc IN ({upcs: Array(String)}) AND asset_id != ''
                              AND upload_month = (SELECT max(upload_month) FROM ads_revenue_enriched WHERE upload_month > 0)
                            UNION ALL
                            SELECT upc, asset_id,
                                0 as ads_rev,
                                partner_rev_total as sub_rev,
                                owned_views as views
                            FROM subscription_revenue
                            WHERE upc IN ({upcs: Array(String)}) AND asset_id != ''
                              AND upload_month = (SELECT max(upload_month) FROM subscription_revenue WHERE upload_month > 0)
                        )
                        GROUP BY upc, asset_id
                    `;

                    // Execute identity query (all periods)
                    const identityResult = await client.query({
                        query: identityQuery,
                        query_params: { upcs },
                        format: 'JSONEachRow'
                    });
                    const identityRows = await identityResult.json() as { upc: string; asset_id: string; asset_type: string }[];

                    // Populate asset entries with type
                    for (const row of identityRows) {
                        if (!resultMap[row.upc]) resultMap[row.upc] = {};
                        if (!resultMap[row.upc][row.asset_id]) {
                            resultMap[row.upc][row.asset_id] = { asset_type: '', ads_rev: 0, sub_rev: 0, total_views: 0 };
                        }
                        if (row.asset_type && !resultMap[row.upc][row.asset_id].asset_type) {
                            resultMap[row.upc][row.asset_id].asset_type = row.asset_type;
                        }
                    }

                    // Execute revenue query (latest period only)
                    const revenueResult = await client.query({
                        query: revenueQuery,
                        query_params: { upcs },
                        format: 'JSONEachRow'
                    });
                    const revenueRows = await revenueResult.json() as { upc: string; asset_id: string; ads_rev: string; sub_rev: string; total_views: string }[];

                    // Merge revenue into existing entries
                    for (const row of revenueRows) {
                        if (!resultMap[row.upc]) resultMap[row.upc] = {};
                        if (!resultMap[row.upc][row.asset_id]) {
                            resultMap[row.upc][row.asset_id] = { asset_type: '', ads_rev: 0, sub_rev: 0, total_views: 0 };
                        }
                        const entry = resultMap[row.upc][row.asset_id];
                        entry.ads_rev += parseFloat(row.ads_rev) || 0;
                        entry.sub_rev += parseFloat(row.sub_rev) || 0;
                        entry.total_views += parseInt(row.total_views) || 0;
                    }
                } catch (cmsErr: any) {
                    console.warn(`[AssetsByUPC] Error querying CMS ${cms_id}:`, cmsErr.message);
                }
            }

            // Convert to array format per UPC
            const data: Record<string, { asset_id: string; asset_type: string; ads_rev: number; sub_rev: number; total_views: number }[]> = {};
            for (const [upc, assets] of Object.entries(resultMap)) {
                data[upc] = Object.entries(assets).map(([asset_id, info]) => ({
                    asset_id,
                    ...info,
                }));
            }

            console.log(`[AssetsByUPC] Found data for ${Object.keys(data).length} UPCs`);

            return reply.send({ success: true, data });
        } catch (err: any) {
            console.error('[AssetsByUPC] Error:', err);
            return reply.code(500).send({ error: err.message });
        }
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/resolution/revenue_by_isrc
    // Batch check revenue per ISRC across all CMS databases
    // Optional: month (YYYYMM) filter, per_cms breakdown
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/resolution/revenue_by_isrc', async (req, reply) => {
        const body = req.body as { isrcs?: string[]; cms_id?: string; month?: string; per_cms?: boolean };
        const isrcs = body.isrcs;

        if (!isrcs || !Array.isArray(isrcs) || isrcs.length === 0) {
            return reply.code(400).send({ error: 'isrcs is required and must be a non-empty array' });
        }

        if (isrcs.length > 1000) {
            return reply.code(400).send({ error: 'Maximum 1000 ISRCs per request' });
        }

        // Validate month format if provided (YYYYMM)
        if (body.month && !/^\d{6}$/.test(body.month)) {
            return reply.code(400).send({ error: 'month must be in YYYYMM format (e.g. 202601)' });
        }

        try {
            // Aggregated revenue map (backward compatible)
            const revenueMap: Record<string, { ads_rev: number; adj_ads_rev: number; sub_rev: number; adj_sub_rev: number; total_views: number }> = {};
            // Per-CMS revenue map (new: for settlement exchange rate conversion)
            const perCmsMap: Record<string, Record<string, { ads_rev: number; adj_ads_rev: number; sub_rev: number; adj_sub_rev: number; total_views: number }>> = {};

            // If cms_id is provided, query that specific CMS database
            // Otherwise, query all registered CMS databases
            let cmsClients: { cms_id: string; client: any }[] = [];

            if (body.cms_id) {
                const resolved = await resolveCmsClient(body.cms_id);
                if (resolved) cmsClients.push(resolved);
            } else {
                // Query CMS databases — scoped by org if caller is org_admin
                const defaultClient = getDefaultClient();
                if (defaultClient) {
                    try {
                        // Build org-scoped query
                        let registryQuery = `SELECT cms_id FROM cms_registry FINAL WHERE is_active = 1`;
                        let registryParams: Record<string, string> = {};

                        if (req.authRole === 'org_admin' && req.orgId) {
                            registryQuery += ` AND org_id = {org_id: String}`;
                            registryParams = { org_id: req.orgId };
                        }
                        // super_admin and cms-level keys query all (cms key already scoped by tenant)

                        const rs = await defaultClient.query({ query: registryQuery, query_params: registryParams, format: 'JSONEachRow' });
                        const registeredCms = await rs.json() as { cms_id: string }[];

                        const { getAllTenants } = await import('../config/tenants.js');
                        const allTenants = getAllTenants();

                        // Add CMS clients from registry (prefer in-memory tenant if available)
                        for (const reg of registeredCms) {
                            const tenant = allTenants.find(t => t.cmsId === reg.cms_id);
                            if (tenant) {
                                cmsClients.push({ cms_id: reg.cms_id, client: tenant.readClient });
                            } else {
                                // Fallback: create client from db name
                                const client = getClickHouseClient({ database: `db_${reg.cms_id}` });
                                cmsClients.push({ cms_id: reg.cms_id, client });
                            }
                        }
                    } catch (e) {
                        // Fallback to in-memory tenants only
                        const { getAllTenants } = await import('../config/tenants.js');
                        for (const tenant of getAllTenants()) {
                            cmsClients.push({ cms_id: tenant.cmsId, client: tenant.readClient });
                        }
                    }
                } else {
                    // Also check in-memory tenants
                    const { getAllTenants } = await import('../config/tenants.js');
                    for (const tenant of getAllTenants()) {
                        if (!cmsClients.find(c => c.cms_id === tenant.cmsId)) {
                            cmsClients.push({ cms_id: tenant.cmsId, client: tenant.readClient });
                        }
                    }
                }
            }

            console.log(`[RevenueByISRC] Querying ${cmsClients.length} CMS database(s) for ${isrcs.length} ISRCs${body.month ? ` (month: ${body.month})` : ''}`);

            // Build month filter clause if month is provided
            const monthFilter = body.month ? `AND upload_month = {month: UInt32}` : '';
            const queryParams: any = { isrcs };
            if (body.month) queryParams.month = parseInt(body.month);

            for (const { cms_id, client } of cmsClients) {
                try {
                    // Query with 4-way revenue breakdown: ads, adj_ads, sub, adj_sub
                    const query = `
                        SELECT
                            isrc,
                            sum(ads_rev) as ads_rev,
                            sum(adj_ads_rev) as adj_ads_rev,
                            sum(sub_rev) as sub_rev,
                            sum(adj_sub_rev) as adj_sub_rev,
                            sum(views) as total_views
                        FROM (
                            SELECT isrc,
                                CASE WHEN adjustment_type IN ('', 'None') THEN partner_rev_total ELSE 0 END as ads_rev,
                                CASE WHEN adjustment_type NOT IN ('', 'None') THEN partner_rev_total ELSE 0 END as adj_ads_rev,
                                0 as sub_rev,
                                0 as adj_sub_rev,
                                owned_views as views
                            FROM ads_revenue_enriched
                            WHERE isrc IN ({isrcs: Array(String)}) AND isrc != '' ${monthFilter}
                            UNION ALL
                            SELECT isrc,
                                0 as ads_rev,
                                0 as adj_ads_rev,
                                CASE WHEN adjustment_type IN ('', 'None') THEN partner_rev_total ELSE 0 END as sub_rev,
                                CASE WHEN adjustment_type NOT IN ('', 'None') THEN partner_rev_total ELSE 0 END as adj_sub_rev,
                                0 as views
                            FROM subscription_revenue
                            WHERE isrc IN ({isrcs: Array(String)}) AND isrc != '' ${monthFilter}
                        )
                        GROUP BY isrc
                    `;

                    const resultSet = await client.query({
                        query,
                        query_params: queryParams,
                        format: 'JSONEachRow'
                    });

                    const rows = await resultSet.json() as { isrc: string; ads_rev: string; adj_ads_rev: string; sub_rev: string; adj_sub_rev: string; total_views: string }[];

                    for (const row of rows) {
                        const ads = parseFloat(row.ads_rev) || 0;
                        const adjAds = parseFloat(row.adj_ads_rev) || 0;
                        const sub = parseFloat(row.sub_rev) || 0;
                        const adjSub = parseFloat(row.adj_sub_rev) || 0;
                        const views = parseInt(row.total_views) || 0;

                        // Aggregated (backward compatible)
                        if (!revenueMap[row.isrc]) {
                            revenueMap[row.isrc] = { ads_rev: 0, adj_ads_rev: 0, sub_rev: 0, adj_sub_rev: 0, total_views: 0 };
                        }
                        revenueMap[row.isrc].ads_rev += ads;
                        revenueMap[row.isrc].adj_ads_rev += adjAds;
                        revenueMap[row.isrc].sub_rev += sub;
                        revenueMap[row.isrc].adj_sub_rev += adjSub;
                        revenueMap[row.isrc].total_views += views;

                        // Per-CMS breakdown (for settlement)
                        if (body.per_cms) {
                            if (!perCmsMap[row.isrc]) perCmsMap[row.isrc] = {};
                            if (!perCmsMap[row.isrc][cms_id]) {
                                perCmsMap[row.isrc][cms_id] = { ads_rev: 0, adj_ads_rev: 0, sub_rev: 0, adj_sub_rev: 0, total_views: 0 };
                            }
                            perCmsMap[row.isrc][cms_id].ads_rev += ads;
                            perCmsMap[row.isrc][cms_id].adj_ads_rev += adjAds;
                            perCmsMap[row.isrc][cms_id].sub_rev += sub;
                            perCmsMap[row.isrc][cms_id].adj_sub_rev += adjSub;
                            perCmsMap[row.isrc][cms_id].total_views += views;
                        }
                    }
                } catch (cmsErr: any) {
                    console.warn(`[RevenueByISRC] Error querying CMS ${cms_id}:`, cmsErr.message);
                    // Continue with other CMS databases
                }
            }

            console.log(`[RevenueByISRC] Found revenue for ${Object.keys(revenueMap).length} ISRCs`);

            const response: any = {
                success: true,
                data: revenueMap,
                total: Object.keys(revenueMap).length,
            };

            // Include per-CMS breakdown if requested
            if (body.per_cms) {
                response.per_cms = perCmsMap;
            }

            return reply.send(response);
        } catch (err: any) {
            console.error('[RevenueByISRC] Error:', err);
            return reply.code(500).send({ error: err.message });
        }
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/resolution/export_csv
    // Export raw data to CSV for a specific list of ISRCs
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/resolution/export_csv', async (req, reply) => {
        const body = req.body as {
            isrcs: string[];
            month: string;
            type: 'ads' | 'adj_ads' | 'sub' | 'adj_sub';
            cms_id?: string;
        };

        if (!body.isrcs || !Array.isArray(body.isrcs) || body.isrcs.length === 0) {
            return reply.code(400).send({ error: 'isrcs array is required' });
        }
        if (!body.month || !/^\d{6}$/.test(body.month)) {
            return reply.code(400).send({ error: 'month must be in YYYYMM format (e.g. 202601)' });
        }
        if (!['ads', 'adj_ads', 'sub', 'adj_sub'].includes(body.type)) {
            return reply.code(400).send({ error: 'Invalid type. Allowed: ads, adj_ads, sub, adj_sub' });
        }

        try {
            let cmsClients: { cms_id: string; client: any }[] = [];

            if (body.cms_id) {
                const resolved = await resolveCmsClient(body.cms_id);
                if (resolved) cmsClients.push(resolved);
            } else {
                const defaultClient = getDefaultClient();
                if (defaultClient) {
                    try {
                        let registryQuery = `SELECT cms_id FROM cms_registry FINAL WHERE is_active = 1`;
                        let registryParams: Record<string, string> = {};

                        if (req.authRole === 'org_admin' && req.orgId) {
                            registryQuery += ` AND org_id = {org_id: String}`;
                            registryParams = { org_id: req.orgId };
                        }

                        const rs = await defaultClient.query({ query: registryQuery, query_params: registryParams, format: 'JSONEachRow' });
                        const registeredCms = await rs.json() as { cms_id: string }[];
                        const { getAllTenants } = await import('../config/tenants.js');
                        const allTenants = getAllTenants();

                        for (const reg of registeredCms) {
                            const tenant = allTenants.find(t => t.cmsId === reg.cms_id);
                            if (tenant) {
                                cmsClients.push({ cms_id: reg.cms_id, client: tenant.readClient });
                            } else {
                                const client = getClickHouseClient({ database: `db_${reg.cms_id}` });
                                cmsClients.push({ cms_id: reg.cms_id, client });
                            }
                        }
                    } catch (e) {
                        const { getAllTenants } = await import('../config/tenants.js');
                        for (const tenant of getAllTenants()) {
                            cmsClients.push({ cms_id: tenant.cmsId, client: tenant.readClient });
                        }
                    }
                } else {
                    const { getAllTenants } = await import('../config/tenants.js');
                    for (const tenant of getAllTenants()) {
                        if (!cmsClients.find(c => c.cms_id === tenant.cmsId)) {
                            cmsClients.push({ cms_id: tenant.cmsId, client: tenant.readClient });
                        }
                    }
                }
            }

            if (cmsClients.length === 0) {
                return reply.code(404).send({ error: 'No active CMS database found' });
            }

            let table = '';
            let cond = '';

            if (body.type === 'ads') {
                table = 'ads_revenue_enriched';
                cond = `adjustment_type IN ('', 'None')`;
            } else if (body.type === 'adj_ads') {
                table = 'ads_revenue_enriched';
                cond = `adjustment_type NOT IN ('', 'None')`;
            } else if (body.type === 'sub') {
                table = 'subscription_revenue';
                cond = `adjustment_type IN ('', 'None')`;
            } else if (body.type === 'adj_sub') {
                table = 'subscription_revenue';
                cond = `adjustment_type NOT IN ('', 'None')`;
            }

            // We must union all clients if there are multiple, or just iterate and append CSV strings.
            // Iterating and appending is easier since ClickHouse returns the CSV string.
            let fullCsv = "";
            let isFirst = true;

            const queryParams = {
                isrcs: body.isrcs,
                month: parseInt(body.month)
            };

            for (const { client, cms_id } of cmsClients) {
                try {
                    const query = `
                        SELECT *
                        FROM ${table}
                        WHERE isrc IN ({isrcs: Array(String)}) 
                          AND upload_month = {month: UInt32}
                          AND ${cond}
                        FORMAT CSVWithNames
                    `;

                    const resultSet = await client.query({
                        query,
                        query_params: queryParams,
                    });

                    const csvText = await resultSet.text();

                    if (csvText.trim()) {
                        if (isFirst) {
                            fullCsv += csvText; // Includes headers
                            isFirst = false;
                        } else {
                            // Strip the first line (headers) if not the first file
                            const lines = csvText.split('\n');
                            if (lines.length > 1) {
                                fullCsv += lines.slice(1).join('\n');
                            }
                        }
                    }
                } catch (cmsErr: any) {
                    console.warn(`[ExportCSV] Error querying CMS ${cms_id}: ${cmsErr.message}`);
                }
            }

            reply.header('Content-Type', 'text/csv');
            reply.header('Content-Disposition', `attachment; filename="export_${body.type}_${body.month}.csv"`);
            return reply.send(fullCsv);

        } catch (err: any) {
            console.error('[ExportCSV] Error:', err);
            return reply.code(500).send({ error: err.message });
        }
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/resolution/revenue_by_asset_id
    // Batch check revenue per Asset ID across all CMS databases
    // Mirror of revenue_by_isrc but keyed on asset_id
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/resolution/revenue_by_asset_id', async (req, reply) => {
        const body = req.body as { asset_ids?: string[]; cms_id?: string; month?: string };
        const assetIds = body.asset_ids;

        if (!assetIds || !Array.isArray(assetIds) || assetIds.length === 0) {
            return reply.code(400).send({ error: 'asset_ids is required and must be a non-empty array' });
        }

        if (assetIds.length > 1000) {
            return reply.code(400).send({ error: 'Maximum 1000 asset IDs per request' });
        }

        if (body.month && !/^\d{6}$/.test(body.month)) {
            return reply.code(400).send({ error: 'month must be in YYYYMM format (e.g. 202601)' });
        }

        try {
            const revenueMap: Record<string, { ads_rev: number; adj_ads_rev: number; sub_rev: number; adj_sub_rev: number; total_views: number }> = {};

            let cmsClients: { cms_id: string; client: any }[] = [];

            if (body.cms_id) {
                const resolved = await resolveCmsClient(body.cms_id);
                if (resolved) cmsClients.push(resolved);
            } else {
                const defaultClient = getDefaultClient();
                if (defaultClient) {
                    try {
                        let registryQuery = `SELECT cms_id FROM cms_registry FINAL WHERE is_active = 1`;
                        let registryParams: Record<string, string> = {};

                        if (req.authRole === 'org_admin' && req.orgId) {
                            registryQuery += ` AND org_id = {org_id: String}`;
                            registryParams = { org_id: req.orgId };
                        }

                        const rs = await defaultClient.query({ query: registryQuery, query_params: registryParams, format: 'JSONEachRow' });
                        const registeredCms = await rs.json() as { cms_id: string }[];
                        const { getAllTenants } = await import('../config/tenants.js');
                        const allTenants = getAllTenants();

                        for (const reg of registeredCms) {
                            const tenant = allTenants.find(t => t.cmsId === reg.cms_id);
                            if (tenant) {
                                cmsClients.push({ cms_id: reg.cms_id, client: tenant.readClient });
                            } else {
                                const client = getClickHouseClient({ database: `db_${reg.cms_id}` });
                                cmsClients.push({ cms_id: reg.cms_id, client });
                            }
                        }
                    } catch (e) {
                        const { getAllTenants } = await import('../config/tenants.js');
                        for (const tenant of getAllTenants()) {
                            cmsClients.push({ cms_id: tenant.cmsId, client: tenant.readClient });
                        }
                    }
                } else {
                    const { getAllTenants } = await import('../config/tenants.js');
                    for (const tenant of getAllTenants()) {
                        if (!cmsClients.find(c => c.cms_id === tenant.cmsId)) {
                            cmsClients.push({ cms_id: tenant.cmsId, client: tenant.readClient });
                        }
                    }
                }
            }

            console.log(`[RevenueByAssetID] Querying ${cmsClients.length} CMS database(s) for ${assetIds.length} asset IDs${body.month ? ` (month: ${body.month})` : ''}`);

            const monthFilter = body.month ? `AND upload_month = {month: UInt32}` : '';
            const queryParams: any = { asset_ids: assetIds };
            if (body.month) queryParams.month = parseInt(body.month);

            for (const { cms_id, client } of cmsClients) {
                try {
                    const query = `
                        SELECT
                            asset_id,
                            sum(ads_rev) as ads_rev,
                            sum(adj_ads_rev) as adj_ads_rev,
                            sum(sub_rev) as sub_rev,
                            sum(adj_sub_rev) as adj_sub_rev,
                            sum(views) as total_views
                        FROM (
                            SELECT asset_id,
                                CASE WHEN adjustment_type IN ('', 'None') THEN partner_rev_total ELSE 0 END as ads_rev,
                                CASE WHEN adjustment_type NOT IN ('', 'None') THEN partner_rev_total ELSE 0 END as adj_ads_rev,
                                0 as sub_rev,
                                0 as adj_sub_rev,
                                owned_views as views
                            FROM ads_revenue_enriched
                            WHERE asset_id IN ({asset_ids: Array(String)}) AND asset_id != '' ${monthFilter}
                            UNION ALL
                            SELECT asset_id,
                                0 as ads_rev,
                                0 as adj_ads_rev,
                                CASE WHEN adjustment_type IN ('', 'None') THEN partner_rev_total ELSE 0 END as sub_rev,
                                CASE WHEN adjustment_type NOT IN ('', 'None') THEN partner_rev_total ELSE 0 END as adj_sub_rev,
                                0 as views
                            FROM subscription_revenue
                            WHERE asset_id IN ({asset_ids: Array(String)}) AND asset_id != '' ${monthFilter}
                        )
                        GROUP BY asset_id
                    `;

                    const resultSet = await client.query({
                        query,
                        query_params: queryParams,
                        format: 'JSONEachRow'
                    });

                    const rows = await resultSet.json() as { asset_id: string; ads_rev: string; adj_ads_rev: string; sub_rev: string; adj_sub_rev: string; total_views: string }[];

                    for (const row of rows) {
                        const ads = parseFloat(row.ads_rev) || 0;
                        const adjAds = parseFloat(row.adj_ads_rev) || 0;
                        const sub = parseFloat(row.sub_rev) || 0;
                        const adjSub = parseFloat(row.adj_sub_rev) || 0;
                        const views = parseInt(row.total_views) || 0;

                        if (!revenueMap[row.asset_id]) {
                            revenueMap[row.asset_id] = { ads_rev: 0, adj_ads_rev: 0, sub_rev: 0, adj_sub_rev: 0, total_views: 0 };
                        }
                        revenueMap[row.asset_id].ads_rev += ads;
                        revenueMap[row.asset_id].adj_ads_rev += adjAds;
                        revenueMap[row.asset_id].sub_rev += sub;
                        revenueMap[row.asset_id].adj_sub_rev += adjSub;
                        revenueMap[row.asset_id].total_views += views;
                    }
                } catch (cmsErr: any) {
                    console.warn(`[RevenueByAssetID] Error querying CMS ${cms_id}:`, cmsErr.message);
                }
            }

            console.log(`[RevenueByAssetID] Found revenue for ${Object.keys(revenueMap).length} asset IDs`);

            return reply.send({
                success: true,
                data: revenueMap,
                total: Object.keys(revenueMap).length,
            });
        } catch (err: any) {
            console.error('[RevenueByAssetID] Error:', err);
            return reply.code(500).send({ error: err.message });
        }
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/resolution/revenue_by_isrc_country
    // Per-country revenue breakdown for a set of ISRCs + asset_ids
    // Includes ALL revenue types: ads, adj_ads, sub, adj_sub
    // Used by Earnings page Sales Region pivot tab
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/resolution/revenue_by_isrc_country', async (req, reply) => {
        const body = req.body as {
            isrcs?: string[];
            asset_ids?: string[];
            month?: string;
            per_isrc?: boolean;
        };

        const isrcs = body.isrcs || [];
        const assetIds = body.asset_ids || [];

        if (isrcs.length === 0 && assetIds.length === 0) {
            return reply.code(400).send({ error: 'At least one of isrcs or asset_ids is required' });
        }

        if (body.month && !/^\d{6}$/.test(body.month)) {
            return reply.code(400).send({ error: 'month must be in YYYYMM format (e.g. 202601)' });
        }

        try {
            // Accumulate per-country revenue
            const countryMap: Record<string, {
                ads_rev: number; adj_ads_rev: number;
                sub_rev: number; adj_sub_rev: number;
                total_views: number;
            }> = {};

            // Resolve CMS clients (same pattern as revenue_by_isrc)
            let cmsClients: { cms_id: string; client: any }[] = [];

            const defaultClient = getDefaultClient();
            if (defaultClient) {
                try {
                    let registryQuery = `SELECT cms_id FROM cms_registry FINAL WHERE is_active = 1`;
                    let registryParams: Record<string, string> = {};

                    if (req.authRole === 'org_admin' && req.orgId) {
                        registryQuery += ` AND org_id = {org_id: String}`;
                        registryParams = { org_id: req.orgId };
                    }

                    const rs = await defaultClient.query({ query: registryQuery, query_params: registryParams, format: 'JSONEachRow' });
                    const registeredCms = await rs.json() as { cms_id: string }[];
                    const { getAllTenants } = await import('../config/tenants.js');
                    const allTenants = getAllTenants();

                    for (const reg of registeredCms) {
                        const tenant = allTenants.find(t => t.cmsId === reg.cms_id);
                        if (tenant) {
                            cmsClients.push({ cms_id: reg.cms_id, client: tenant.readClient });
                        } else {
                            const client = getClickHouseClient({ database: `db_${reg.cms_id}` });
                            cmsClients.push({ cms_id: reg.cms_id, client });
                        }
                    }
                } catch (e) {
                    const { getAllTenants } = await import('../config/tenants.js');
                    for (const tenant of getAllTenants()) {
                        cmsClients.push({ cms_id: tenant.cmsId, client: tenant.readClient });
                    }
                }
            } else {
                const { getAllTenants } = await import('../config/tenants.js');
                for (const tenant of getAllTenants()) {
                    if (!cmsClients.find(c => c.cms_id === tenant.cmsId)) {
                        cmsClients.push({ cms_id: tenant.cmsId, client: tenant.readClient });
                    }
                }
            }

            console.log(`[RevenueByISRCCountry] Querying ${cmsClients.length} CMS database(s) for ${isrcs.length} ISRCs + ${assetIds.length} asset_ids${body.month ? ` (month: ${body.month})` : ''}`);

            const monthFilter = body.month ? `AND upload_month = {month: UInt32}` : '';
            const baseParams: any = {};
            if (body.month) baseParams.month = parseInt(body.month);

            for (const { cms_id, client } of cmsClients) {
                try {
                    // Build ISRC filter
                    const isrcFilter = isrcs.length > 0 ? `isrc IN ({isrcs: Array(String)}) AND isrc != ''` : '1=0';
                    const assetFilter = assetIds.length > 0 ? `asset_id IN ({asset_ids: Array(String)}) AND asset_id != ''` : '1=0';
                    const whereClause = `(${isrcFilter} OR ${assetFilter})`;

                    const queryParams: any = { ...baseParams };
                    if (isrcs.length > 0) queryParams.isrcs = isrcs;
                    if (assetIds.length > 0) queryParams.asset_ids = assetIds;

                    const query = `
                        SELECT
                            ${body.per_isrc ? 'isrc, asset_id,' : ''}
                            country,
                            sum(ads_rev) as ads_rev,
                            sum(adj_ads_rev) as adj_ads_rev,
                            sum(sub_rev) as sub_rev,
                            sum(adj_sub_rev) as adj_sub_rev,
                            sum(views) as total_views
                        FROM (
                            SELECT ${body.per_isrc ? 'isrc, asset_id,' : ''} country,
                                CASE WHEN adjustment_type IN ('', 'None') THEN partner_rev_total ELSE 0 END as ads_rev,
                                CASE WHEN adjustment_type NOT IN ('', 'None') THEN partner_rev_total ELSE 0 END as adj_ads_rev,
                                0 as sub_rev,
                                0 as adj_sub_rev,
                                owned_views as views
                            FROM ads_revenue_enriched
                            WHERE ${whereClause} ${monthFilter}
                            UNION ALL
                            SELECT ${body.per_isrc ? 'isrc, if(asset_id=\'\', video_id, asset_id) as asset_id,' : ''} country,
                                0 as ads_rev,
                                0 as adj_ads_rev,
                                CASE WHEN adjustment_type IN ('', 'None') THEN partner_rev_total ELSE 0 END as sub_rev,
                                CASE WHEN adjustment_type NOT IN ('', 'None') THEN partner_rev_total ELSE 0 END as adj_sub_rev,
                                0 as views
                            FROM subscription_revenue
                            WHERE ${whereClause} ${monthFilter}
                        )
                        GROUP BY ${body.per_isrc ? 'isrc, asset_id, country' : 'country'}
                        ORDER BY ads_rev + adj_ads_rev + sub_rev + adj_sub_rev DESC
                    `;

                    const resultSet = await client.query({
                        query,
                        query_params: queryParams,
                        format: 'JSONEachRow'
                    });

                    const rows = await resultSet.json() as any[];

                    for (const row of rows) {
                        const ads = parseFloat(row.ads_rev) || 0;
                        const adjAds = parseFloat(row.adj_ads_rev) || 0;
                        const sub = parseFloat(row.sub_rev) || 0;
                        const adjSub = parseFloat(row.adj_sub_rev) || 0;
                        const views = parseInt(row.total_views) || 0;
                        const c = row.country || 'Unknown';
                        const i = body.per_isrc ? (row.isrc || 'Unknown') : '';
                        const a = body.per_isrc ? (row.asset_id || 'Unknown') : '';

                        const mapKey = body.per_isrc ? `${i}|${a}|${c}` : c;

                        if (!countryMap[mapKey]) {
                            countryMap[mapKey] = { ads_rev: 0, adj_ads_rev: 0, sub_rev: 0, adj_sub_rev: 0, total_views: 0 } as any;
                            if (body.per_isrc) {
                                (countryMap[mapKey] as any).isrc = i;
                                (countryMap[mapKey] as any).asset_id = a;
                            }
                        }
                        countryMap[mapKey].ads_rev += ads;
                        countryMap[mapKey].adj_ads_rev += adjAds;
                        countryMap[mapKey].sub_rev += sub;
                        countryMap[mapKey].adj_sub_rev += adjSub;
                        countryMap[mapKey].total_views += views;
                    }
                } catch (cmsErr: any) {
                    console.warn(`[RevenueByISRCCountry] Error querying CMS ${cms_id}:`, cmsErr.message);
                }
            }

            // Convert to sorted array
            const data = Object.entries(countryMap)
                .map(([mapKey, rev]: [string, any]) => {
                    const base = {
                        total_revenue: rev.ads_rev + rev.adj_ads_rev + rev.sub_rev + rev.adj_sub_rev,
                        ads_rev: rev.ads_rev,
                        adj_ads_rev: rev.adj_ads_rev,
                        sub_rev: rev.sub_rev,
                        adj_sub_rev: rev.adj_sub_rev,
                        total_views: rev.total_views,
                    } as any;
                    if (body.per_isrc) {
                        base.country = mapKey.split('|')[2];
                        base.isrc = rev.isrc;
                        base.asset_id = rev.asset_id;
                    } else {
                        base.country = mapKey;
                    }
                    return base;
                })
                .sort((a, b) => (b.total_revenue as number) - (a.total_revenue as number));

            console.log(`[RevenueByISRCCountry] Found revenue for ${data.length} keys`);

            return reply.send({ success: true, data, total: data.length });
        } catch (err: any) {
            console.error('[RevenueByISRCCountry] Error:', err);
            return reply.code(500).send({ error: err.message });
        }
    });

}
