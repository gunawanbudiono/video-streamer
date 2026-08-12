import type { FastifyInstance } from 'fastify';
import { authMiddleware } from '../middleware/auth.js';
import { getDefaultClient, getClickHouseClient } from '../config/clickhouse.js';

/** Analytics routes — top assets, videos, channels, country, label */
export async function analyticsRoutes(app: FastifyInstance): Promise<void> {
  app.addHook('onRequest', authMiddleware);

  /**
   * GET /api/v1/analytics/available-months
   * Fetch all distinct upload months available in the Clickhouse ads_revenue_enriched table across all active CMS databases.
   */
  app.get('/api/v1/analytics/available-months', async (req, reply) => {
    try {
      const allMonths = new Set<string>();
      
      const defaultClient = getDefaultClient();
      if (!defaultClient) return reply.code(500).send({ error: 'Default Database client not initialized' });

      // Find all registered active CMS databases
      let cmsIds: string[] = [];
      try {
        let registryQuery = `SELECT cms_id FROM cms_registry FINAL WHERE is_active = 1`;
        let queryParams: Record<string, unknown> = {};

        // Scope to Organization if user is an Org Admin (not super_admin)
        if (req.orgId && req.authRole !== 'super_admin') {
           registryQuery += ` AND org_id = {orgId: String}`;
           queryParams.orgId = req.orgId;
        }

        const rs = await defaultClient.query({
          query: registryQuery,
          query_params: queryParams,
          format: 'JSONEachRow'
        });
        const registry = await rs.json() as { cms_id: string }[];
        cmsIds = registry.map(reg => reg.cms_id);
      } catch (e: any) {
        console.warn("[Available Months] Failed to query cms_registry:", e.message);
      }

      // If we have a specific tenant bound to the request, limit it to that tenant
      if (req.tenant) {
        cmsIds = [req.tenant.cmsId];
      }

      const isEst = (req.query as any).estimated === 'true';
      const jobType = isEst ? 'estimated_ads' : 'ads_revenue';

      if (cmsIds.length > 0) {
        try {
          const sql = `
            SELECT DISTINCT month
            FROM ingestion_jobs
            WHERE status = 'completed'
              AND job_type = {jobType: String}
              AND cms_id IN {cmsIds: Array(String)}
          `;
          const result = await defaultClient.query({
            query: sql,
            query_params: { jobType, cmsIds },
            format: 'JSONEachRow',
          });
          const rows = await result.json() as { month: number }[];
          for (const r of rows) {
            allMonths.add(String(r.month));
          }
        } catch (jobErr: any) {
          console.warn("[Available Months] Error querying ingestion_jobs, falling back to direct DB checks:", jobErr.message);
        }
      }

      // Fallback: If no completed jobs are found in ingestion_jobs, run parallel database queries
      if (allMonths.size === 0) {
        let cmsDbs: string[] = [];
        if (req.tenant) {
          cmsDbs = [`db_${req.tenant.cmsId.replace(/-/g, '_')}`];
        } else if (cmsIds.length > 0) {
          cmsDbs = cmsIds.map(id => `db_${id.replace(/-/g, '_')}`);
        }

        await Promise.all(cmsDbs.map(async (dbName) => {
          try {
            const client = getClickHouseClient({ database: dbName });
            const sql = isEst ? `
              SELECT DISTINCT toYYYYMM(day) AS month
              FROM estimated_revenue_daily
              WHERE day > '1970-01-01'
            ` : `
              SELECT DISTINCT upload_month AS month
              FROM ads_revenue_enriched
              WHERE upload_month > 0
            `;
            const result = await client.query({
              query: sql,
              format: 'JSONEachRow',
            });
            const rows = await result.json() as { month: number }[];
            for (const r of rows) {
              allMonths.add(String(r.month));
            }
          } catch (dbErr: any) {
            // ignore if table missing in one client
            console.warn(`[Available Months Fallback] Error in DB ${dbName}:`, dbErr.message);
          }
        }));
      }
      
      // Sort descending (newest first)
      const sorted = Array.from(allMonths).sort().reverse();
      return reply.send({ data: sorted });

    } catch (err: any) {
      return reply.code(500).send({ error: err.message });
    }
  });

  /**
   * GET /api/v1/analytics/top-assets?month=202601&limit=20
   */
  app.get('/api/v1/analytics/top-assets', async (req, reply) => {
    if (!req.tenant) return reply.code(400).send({ error: 'Tenant context required' });
    const { month, limit = '20' } = req.query as { month?: string; limit?: string };
    if (!month || !/^\d{6}$/.test(month)) {
      return reply.code(400).send({ error: 'month is required (format: YYYYMM)' });
    }

    const isEst = (req.query as any).estimated === 'true';
    const tableName = isEst ? 'estimated_revenue_daily' : 'ads_revenue_enriched';
    const whereClause = isEst 
      ? 'cms_id = {cms_id: String} AND toYYYYMM(day) = {month: UInt32}' 
      : 'cms_id = {cms_id: String} AND upload_month = {month: UInt32} AND report_type = \'claim_raw\'';

    try {
      const result = await req.tenant!.readClient.query({
        query: `
          SELECT 
            asset_id,
            any(isrc) AS isrc,
            any(upc) AS upc,
            any(artist) AS artist,
            any(asset_title) AS asset_title,
            any(album) AS album,
            any(label) AS label,
            any(asset_labels) AS asset_labels,
            any(asset_type) AS asset_type,
            sum(owned_views) AS total_views,
            sum(partner_rev_total) AS total_partner_revenue,
            sum(yt_rev_total) AS total_yt_revenue
          FROM ${tableName}
          WHERE ${whereClause}
          GROUP BY asset_id
          ORDER BY total_partner_revenue DESC
          LIMIT {limit: UInt32}
        `,
        query_params: { cms_id: req.tenant.cmsId, month: parseInt(month), limit: parseInt(limit) },
        format: 'JSONEachRow',
      });

      const rows = await result.json();
      return reply.send({ data: rows, month });
    } catch (err: any) {
      if (err.message?.includes('UNKNOWN_TABLE') || err.message?.includes('does not exist') || err.message?.includes('Table')) {
        return reply.send({ data: [], month });
      }
      return reply.code(500).send({ error: err.message });
    }
  });

  /**
   * GET /api/v1/analytics/top-videos?month=202601&limit=20
   */
  app.get('/api/v1/analytics/top-videos', async (req, reply) => {
    if (!req.tenant) return reply.code(400).send({ error: 'Tenant context required' });
    const { month, limit = '20' } = req.query as { month?: string; limit?: string };
    if (!month || !/^\d{6}$/.test(month)) {
      return reply.code(400).send({ error: 'month is required (format: YYYYMM)' });
    }

    const isEst = (req.query as any).estimated === 'true';
    const tableName = isEst ? 'estimated_revenue_daily' : 'ads_revenue_enriched';
    const whereClause = isEst 
      ? 'cms_id = {cms_id: String} AND toYYYYMM(day) = {month: UInt32}' 
      : 'cms_id = {cms_id: String} AND upload_month = {month: UInt32} AND report_type = \'claim_raw\'';

    try {
      const result = await req.tenant!.readClient.query({
        query: `
          SELECT 
            video_id,
            any(video_title) AS video_title,
            any(channel_display_name) AS channel_name,
            any(channel_id) AS channel_id,
            any(category) AS category,
            any(asset_id) AS asset_id,
            any(isrc) AS isrc,
            sum(owned_views) AS total_views,
            sum(partner_rev_total) AS total_partner_revenue,
            sum(yt_rev_total) AS total_yt_revenue
          FROM ${tableName}
          WHERE ${whereClause}
          GROUP BY video_id
          ORDER BY total_partner_revenue DESC
          LIMIT {limit: UInt32}
        `,
        query_params: { cms_id: req.tenant.cmsId, month: parseInt(month), limit: parseInt(limit) },
        format: 'JSONEachRow',
      });

      const rows = await result.json();
      return reply.send({ data: rows, month });
    } catch (err: any) {
      if (err.message?.includes('UNKNOWN_TABLE') || err.message?.includes('does not exist') || err.message?.includes('Table')) {
        return reply.send({ data: [], month });
      }
      return reply.code(500).send({ error: err.message });
    }
  });

  /**
   * GET /api/v1/analytics/top-channels?month=202601&limit=20
   */
  app.get('/api/v1/analytics/top-channels', async (req, reply) => {
    if (!req.tenant) return reply.code(400).send({ error: 'Tenant context required' });
    const { month, limit = '20' } = req.query as { month?: string; limit?: string };
    if (!month || !/^\d{6}$/.test(month)) {
      return reply.code(400).send({ error: 'month is required (format: YYYYMM)' });
    }

    const isEst = (req.query as any).estimated === 'true';
    const tableName = isEst ? 'estimated_revenue_daily' : 'ads_revenue_enriched';
    const channelColumn = isEst ? 'owner_channel_id' : 'coalesce(nullIf(asset_channel_id, \'\'), channel_id)';
    const whereClause = isEst 
      ? 'cms_id = {cms_id: String} AND toYYYYMM(day) = {month: UInt32}' 
      : 'cms_id = {cms_id: String} AND upload_month = {month: UInt32} AND report_type = \'claim_raw\'';

    try {
      const result = await req.tenant!.readClient.query({
        query: `
          SELECT 
            ${channelColumn} AS channel_id,
            any(channel_display_name) AS channel_name,
            count(DISTINCT video_id) AS video_count,
            count(DISTINCT asset_id) AS asset_count,
            sum(owned_views) AS total_views,
            sum(partner_rev_total) AS total_partner_revenue,
            sum(yt_rev_total) AS total_yt_revenue
          FROM ${tableName}
          WHERE ${whereClause}
          GROUP BY channel_id
          ORDER BY total_partner_revenue DESC
          LIMIT {limit: UInt32}
        `,
        query_params: { cms_id: req.tenant.cmsId, month: parseInt(month), limit: parseInt(limit) },
        format: 'JSONEachRow',
      });

      const rows = await result.json();
      return reply.send({ data: rows, month });
    } catch (err: any) {
      if (err.message?.includes('UNKNOWN_TABLE') || err.message?.includes('does not exist') || err.message?.includes('Table')) {
        return reply.send({ data: [], month });
      }
      return reply.code(500).send({ error: err.message });
    }
  });

  /**
   * GET /api/v1/analytics/by-country?month=202601&limit=50
   */
  app.get('/api/v1/analytics/by-country', async (req, reply) => {
    if (!req.tenant) return reply.code(400).send({ error: 'Tenant context required' });
    const { month, limit = '50' } = req.query as { month?: string; limit?: string };
    if (!month || !/^\d{6}$/.test(month)) {
      return reply.code(400).send({ error: 'month is required (format: YYYYMM)' });
    }

    const isEst = (req.query as any).estimated === 'true';
    const tableName = isEst ? 'estimated_revenue_daily' : 'ads_revenue_enriched';
    const whereClause = isEst 
      ? 'cms_id = {cms_id: String} AND toYYYYMM(day) = {month: UInt32}' 
      : 'cms_id = {cms_id: String} AND upload_month = {month: UInt32}';

    try {
      const result = await req.tenant!.readClient.query({
        query: `
          SELECT 
            country,
            sum(owned_views) AS total_views,
            sum(partner_rev_total) AS total_partner_revenue,
            sum(yt_rev_total) AS total_yt_revenue,
            count(DISTINCT asset_id) AS asset_count
          FROM ${tableName}
          WHERE ${whereClause}
          GROUP BY country
          ORDER BY total_partner_revenue DESC
          LIMIT {limit: UInt32}
        `,
        query_params: { cms_id: req.tenant.cmsId, month: parseInt(month), limit: parseInt(limit) },
        format: 'JSONEachRow',
      });

      const rows = await result.json();
      return reply.send({ data: rows, month });
    } catch (err: any) {
      if (err.message?.includes('UNKNOWN_TABLE') || err.message?.includes('does not exist') || err.message?.includes('Table')) {
        return reply.send({ data: [], month });
      }
      return reply.code(500).send({ error: err.message });
    }
  });

  /**
   * GET /api/v1/analytics/countries-views?month=202601
   */
  app.get('/api/v1/analytics/countries-views', async (req, reply) => {
    if (!req.tenant) return reply.code(400).send({ error: 'Tenant context required' });
    const { month } = req.query as { month?: string };
    if (!month || !/^\d{6}$/.test(month)) {
      return reply.code(400).send({ error: 'month is required (format: YYYYMM)' });
    }

    try {
      const result = await req.tenant!.readClient.query({
        query: `
          SELECT 
            country,
            sum(views) AS total_views,
            sum(watch_time_sec) AS total_watch_time
          FROM video_countries_daily
          WHERE cms_id = {cms_id: String} AND toYYYYMM(day) = {month: UInt32}
          GROUP BY country
          ORDER BY total_views DESC
        `,
        query_params: { cms_id: req.tenant.cmsId, month: parseInt(month) },
        format: 'JSONEachRow',
      });

      const rows = await result.json();
      return reply.send({ data: rows, month });
    } catch (err: any) {
      if (err.message?.includes('UNKNOWN_TABLE') || err.message?.includes('does not exist') || err.message?.includes('Table')) {
        return reply.send({ data: [], month });
      }
      return reply.code(500).send({ error: err.message });
    }
  });

  /**
   * GET /api/v1/analytics/by-label?month=202601
   */
  app.get('/api/v1/analytics/by-label', async (req, reply) => {
    if (!req.tenant) return reply.code(400).send({ error: 'Tenant context required' });
    const { month } = req.query as { month?: string };
    if (!month || !/^\d{6}$/.test(month)) {
      return reply.code(400).send({ error: 'month is required (format: YYYYMM)' });
    }

    const isEst = (req.query as any).estimated === 'true';
    const tableName = isEst ? 'estimated_revenue_daily' : 'ads_revenue_enriched';
    const whereClause = isEst 
      ? 'cms_id = {cms_id: String} AND toYYYYMM(day) = {month: UInt32}' 
      : 'cms_id = {cms_id: String} AND upload_month = {month: UInt32} AND report_type = \'claim_raw\'';

    try {
      const result = await req.tenant!.readClient.query({
        query: `
          SELECT 
            label,
            asset_labels,
            sum(owned_views) AS total_views,
            sum(partner_rev_total) AS total_partner_revenue,
            sum(yt_rev_total) AS total_yt_revenue,
            count(DISTINCT asset_id) AS asset_count,
            count(DISTINCT video_id) AS video_count
          FROM ${tableName}
          WHERE ${whereClause}
          GROUP BY label, asset_labels
          ORDER BY total_partner_revenue DESC
        `,
        query_params: { cms_id: req.tenant.cmsId, month: parseInt(month) },
        format: 'JSONEachRow',
      });

      const rows = await result.json();
      return reply.send({ data: rows, month });
    } catch (err: any) {
      if (err.message?.includes('UNKNOWN_TABLE') || err.message?.includes('does not exist') || err.message?.includes('Table')) {
        return reply.send({ data: [], month });
      }
      return reply.code(500).send({ error: err.message });
    }
  });

  /**
   * GET /api/v1/analytics/by-artist?month=202601&limit=20
   * Revenue grouped by artist
   */
  app.get('/api/v1/analytics/by-artist', async (req, reply) => {
    if (!req.tenant) return reply.code(400).send({ error: 'Tenant context required' });
    const { month, limit = '20' } = req.query as { month?: string; limit?: string };
    if (!month || !/^\d{6}$/.test(month)) {
      return reply.code(400).send({ error: 'month is required (format: YYYYMM)' });
    }

    const isEst = (req.query as any).estimated === 'true';
    const tableName = isEst ? 'estimated_revenue_daily' : 'ads_revenue_enriched';
    const whereClause = isEst 
      ? 'cms_id = {cms_id: String} AND toYYYYMM(day) = {month: UInt32} AND artist != \'\'' 
      : 'cms_id = {cms_id: String} AND upload_month = {month: UInt32} AND report_type = \'claim_raw\' AND artist != \'\'';

    try {
      const result = await req.tenant!.readClient.query({
        query: `
          SELECT 
            artist,
            count(DISTINCT asset_id) AS asset_count,
            count(DISTINCT video_id) AS video_count,
            sum(owned_views) AS total_views,
            sum(partner_rev_total) AS total_partner_revenue,
            sum(yt_rev_total) AS total_yt_revenue
          FROM ${tableName}
          WHERE ${whereClause}
          GROUP BY artist
          ORDER BY total_partner_revenue DESC
          LIMIT {limit: UInt32}
        `,
        query_params: { cms_id: req.tenant.cmsId, month: parseInt(month), limit: parseInt(limit) },
        format: 'JSONEachRow',
      });

      const rows = await result.json();
      return reply.send({ data: rows, month });
    } catch (err: any) {
      if (err.message?.includes('UNKNOWN_TABLE') || err.message?.includes('does not exist') || err.message?.includes('Table')) {
        return reply.send({ data: [], month });
      }
      return reply.code(500).send({ error: err.message });
    }
  });

  /**
   * GET /api/v1/analytics/revenue-split?month=202601
   * Comparison: Ads vs Subscription revenue
   */
  app.get('/api/v1/analytics/revenue-split', async (req, reply) => {
    if (!req.tenant) return reply.code(400).send({ error: 'Tenant context required' });
    const { month } = req.query as { month?: string };
    if (!month || !/^\d{6}$/.test(month)) {
      return reply.code(400).send({ error: 'month is required (format: YYYYMM)' });
    }

    const isEst = (req.query as any).estimated === 'true';
    const tableName = isEst ? 'estimated_revenue_daily' : 'ads_revenue_enriched';
    const whereClause = isEst 
      ? 'cms_id = {cms_id: String} AND toYYYYMM(day) = {month: UInt32}' 
      : 'cms_id = {cms_id: String} AND upload_month = {month: UInt32} AND report_type = \'claim_raw\'';

    try {
      const adsResult = await req.tenant!.readClient.query({
        query: `
          SELECT 
            'ads' AS source,
            count() AS total_rows,
            sum(owned_views) AS total_views,
            sum(yt_rev_total) AS total_yt_revenue,
            sum(partner_rev_total) AS total_partner_revenue
          FROM ${tableName}
          WHERE ${whereClause}
        `,
        query_params: { cms_id: req.tenant.cmsId, month: parseInt(month) },
        format: 'JSONEachRow',
      });
      const ads = await adsResult.json();

      const subsQuery = isEst ? `
          SELECT 
            'subscription' AS source,
            CAST(0, 'UInt64') AS total_rows,
            CAST(0, 'UInt64') AS total_views,
            CAST(0, 'Decimal64(10)') AS total_yt_revenue,
            CAST(0, 'Decimal64(10)') AS total_partner_revenue
      ` : `
          SELECT 
            'subscription' AS source,
            count() AS total_rows,
            sum(owned_views) AS total_views,
            sum(yt_rev_total) AS total_yt_revenue,
            sum(partner_rev_total) AS total_partner_revenue
          FROM subscription_revenue
          WHERE cms_id = {cms_id: String} AND upload_month = {month: UInt32}
      `;

      let subs = [];
      try {
        const subsResult = await req.tenant!.readClient.query({
          query: subsQuery,
          query_params: { cms_id: req.tenant.cmsId, month: parseInt(month) },
          format: 'JSONEachRow',
        });
        subs = await subsResult.json();
      } catch (subsErr: any) {
        if (subsErr.message?.includes('UNKNOWN_TABLE') || subsErr.message?.includes('does not exist') || subsErr.message?.includes('Table')) {
          subs = [{ source: 'subscription', total_rows: 0, total_views: 0, total_yt_revenue: 0, total_partner_revenue: 0 }];
        } else {
          throw subsErr;
        }
      }

      return reply.send({ data: { ads, subscription: subs }, month });
    } catch (err: any) {
      if (err.message?.includes('UNKNOWN_TABLE') || err.message?.includes('does not exist') || err.message?.includes('Table')) {
        return reply.send({ data: { ads: [], subscription: [] }, month });
      }
      return reply.code(500).send({ error: err.message });
    }
  });

  /**
   * GET /api/v1/analytics/summary?month=202601
   * Monthly overview: total revenue, views, asset count, adjustment totals
   */
  app.get('/api/v1/analytics/summary', async (req, reply) => {
    if (!req.tenant) return reply.code(400).send({ error: 'Tenant context required' });
    const { month } = req.query as { month?: string };
    if (!month || !/^\d{6}$/.test(month)) {
      return reply.code(400).send({ error: 'month is required (format: YYYYMM)' });
    }

    const isEst = (req.query as any).estimated === 'true';
    const summaryQuery = isEst ? `
        SELECT 
          'estimated_ads' AS report_type,
          count() AS total_rows,
          count(DISTINCT asset_id) AS unique_assets,
          count(DISTINCT video_id) AS unique_videos,
          count(DISTINCT owner_channel_id) AS unique_channels,
          count(DISTINCT country) AS unique_countries,
          sum(owned_views) AS total_views,
          sum(yt_rev_total) AS total_yt_revenue,
          sum(partner_rev_total) AS total_partner_revenue
        FROM estimated_revenue_daily
        WHERE cms_id = {cms_id: String} AND toYYYYMM(day) = {month: UInt32}
    ` : `
        SELECT 
          report_type,
          count() AS total_rows,
          count(DISTINCT asset_id) AS unique_assets,
          count(DISTINCT video_id) AS unique_videos,
          count(DISTINCT coalesce(nullIf(asset_channel_id, ''), channel_id)) AS unique_channels,
          count(DISTINCT country) AS unique_countries,
          sum(owned_views) AS total_views,
          sum(yt_rev_total) AS total_yt_revenue,
          sum(partner_rev_total) AS total_partner_revenue
        FROM ads_revenue_enriched
        WHERE cms_id = {cms_id: String} AND upload_month = {month: UInt32}
        GROUP BY report_type
        ORDER BY report_type
    `;

    try {
      const result = await req.tenant!.readClient.query({
        query: summaryQuery,
        query_params: { cms_id: req.tenant.cmsId, month: parseInt(month) },
        format: 'JSONEachRow',
      });

      const rows = await result.json();
      return reply.send({ data: rows, month });
    } catch (err: any) {
      if (err.message?.includes('UNKNOWN_TABLE') || err.message?.includes('does not exist') || err.message?.includes('Table')) {
        return reply.send({ data: [], month });
      }
      return reply.code(500).send({ error: err.message });
    }
  });

  /**
   * GET /api/v1/analytics/coverage?month=202605
   * Fetch dates and row counts for a given month from ads_revenue_estimated table.
   */
  app.get('/api/v1/analytics/coverage', async (req, reply) => {
    if (!req.tenant) return reply.code(400).send({ error: 'Tenant context required (pass X-CMS-ID header)' });
    const { month } = req.query as { month?: string };
    if (!month || !/^\d{6}$/.test(month)) {
      return reply.code(400).send({ error: 'month query param is required (format: YYYYMM)' });
    }

    try {
      const result = await req.tenant.readClient.query({
        query: `
          SELECT 
            toString(erd.day) AS date,
            count() AS row_count,
            sum(erd.partner_rev_total) AS partner_revenue,
            sum(erd.yt_rev_total) AS yt_revenue,
            sum(if(erd.creator_content_type != 'shorts', erd.partner_rev_auction + erd.partner_rev_reserved + erd.partner_rev_partner_sold_yt_served + erd.partner_rev_partner_sold_p_served, 0)) AS partner_revenue_ads,
            sum(erd.partner_rev_red) AS partner_revenue_premium,
            
            -- Calculate VOD transactions sum
            sum(if(erd.creator_content_type != 'shorts' AND erd.claim_origin != 'Channel Bonus' AND erd.claim_origin != 'Channel Adjustment', erd.partner_rev_total - (erd.partner_rev_auction + erd.partner_rev_reserved + erd.partner_rev_partner_sold_yt_served + erd.partner_rev_partner_sold_p_served + erd.partner_rev_red), 0)) AS calculated_vod_tx,
            
            -- GCS transaction sum (Memberships)
            coalesce(any(gcs.gcs_transaction_revenue), 0) AS gcs_tx,
            
            -- Separate Memberships (Transaction) and Affiliate (Other)
            if(gcs_tx > 0, gcs_tx, 0) AS partner_revenue_transaction,
            if(gcs_tx > 0, calculated_vod_tx - gcs_tx, calculated_vod_tx) AS partner_revenue_other,
            
            sum(if(erd.claim_origin = 'Channel Bonus' OR erd.claim_origin = 'Channel Adjustment', erd.partner_rev_total, 0)) AS partner_revenue_channel_bonus,
            sum(if(erd.creator_content_type = 'shorts', erd.partner_rev_total - erd.partner_rev_red, 0)) AS partner_revenue_shorts_feed_ads,
            sum(erd.partner_rev_total) - sum(
                if(erd.creator_content_type != 'shorts', erd.partner_rev_auction + erd.partner_rev_reserved + erd.partner_rev_partner_sold_yt_served + erd.partner_rev_partner_sold_p_served, 0) + 
                if(erd.creator_content_type = 'shorts', erd.partner_rev_total - erd.partner_rev_red, 0) + 
                erd.partner_rev_red + 
                if(erd.creator_content_type != 'shorts' AND erd.claim_origin != 'Channel Bonus' AND erd.claim_origin != 'Channel Adjustment', erd.partner_rev_total - (erd.partner_rev_auction + erd.partner_rev_reserved + erd.partner_rev_partner_sold_yt_served + erd.partner_rev_partner_sold_p_served + erd.partner_rev_red), 0) + 
                if(erd.claim_origin = 'Channel Bonus' OR erd.claim_origin = 'Channel Adjustment', erd.partner_rev_total, 0)
            ) AS partner_revenue_other_discrepancy,
            sum(if(erd.video_duration_sec > 0 AND erd.video_duration_sec <= 60, erd.partner_rev_total, 0)) AS partner_revenue_shorts,
            uniq(if(erd.video_duration_sec > 0 AND erd.video_duration_sec <= 60, erd.video_id, NULL)) AS shorts_count,
            sum(erd.owned_views) AS total_views,
            count(DISTINCT erd.asset_id) AS active_assets
          FROM estimated_revenue_daily AS erd
          LEFT JOIN (
            SELECT day, sum(estimated_partner_transaction_revenue) AS gcs_transaction_revenue
            FROM youtube_raw_asset_estimated_revenue
            WHERE cms_id = {cms_id: String} AND toYYYYMM(day) = {month: UInt32}
            GROUP BY day
          ) AS gcs ON erd.day = gcs.day
          WHERE erd.cms_id = {cms_id: String} AND toYYYYMM(erd.day) = {month: UInt32}
          GROUP BY erd.day
          ORDER BY erd.day ASC
        `,
        query_params: { cms_id: req.tenant.cmsId, month: parseInt(month, 10) },
        format: 'JSONEachRow',
      });
      const rows = await result.json() as any[];

      // Fetch job details for this CMS and month (completed, processing, pending, failed)
      const defaultCh = getDefaultClient();
      let rowsWithJobId = rows;

      if (defaultCh) {
        const jobsResult = await defaultCh.query({
          query: `
            SELECT 
              job_id, 
              status, 
              error_message, 
              toString(updated_at) AS updated_at, 
              toString(started_at) AS started_at, 
              toString(completed_at) AS completed_at,
              total_rows,
              claims_rows,
              ads_rows,
              reach_rows,
              demo_rows,
              traffic_rows,
              device_rows
            FROM ingestion_jobs
            WHERE cms_id = {cms_id: String}
              AND job_type = 'estimated_ads'
              AND status IN ('completed', 'processing', 'pending', 'failed', 'checking')
              AND month = {month: UInt32}
            ORDER BY updated_at DESC, indexOf(['pending', 'checking', 'processing', 'failed', 'completed'], status) DESC, length(detail_logs) DESC
            LIMIT 1 BY job_id
          `,
          query_params: { cms_id: req.tenant.cmsId, month: parseInt(month, 10) },
          format: 'JSONEachRow',
        });
        const jobs = await jobsResult.json() as any[];

        const dateJobMap: Record<string, { 
          job_id: string; 
          status: string; 
          error_message: string;
          updated_at: number; 
          started_at: string; 
          completed_at: string;
          total_rows: number;
          claims_rows: number;
          ads_rows: number;
          reach_rows: number;
          demo_rows: number;
          traffic_rows: number;
          device_rows: number;
        }> = {};
        for (const j of jobs) {
          if (j.error_message) {
            const match = j.error_message.match(/Sync range:\s*(\d{4}-\d{2}-\d{2})\s*to\s*(\d{4}-\d{2}-\d{2})/i);
            if (match && match[1] === match[2]) {
              const dateStr = match[1];
              
              // Do not skip failed daily jobs that were canceled in queue so we preserve the job_id
              const isAborted = j.status === 'failed' && (
                j.error_message?.includes('Penarikan dibatalkan karena proses induk') ||
                j.error_message?.includes('Job was canceled while waiting in queue') ||
                j.error_message?.includes('aborted/cancelled by user')
              );
              
              const existingMap = dateJobMap[dateStr];
              
              const currentUpdatedAt = new Date(j.updated_at ? j.updated_at + (j.updated_at.includes('Z') ? '' : 'Z') : 0).getTime();
              
              if (!existingMap || currentUpdatedAt > existingMap.updated_at) {
                dateJobMap[dateStr] = { 
                  job_id: j.job_id, 
                  status: j.status,
                  error_message: j.error_message || '',
                  updated_at: currentUpdatedAt,
                  started_at: j.started_at || '',
                  completed_at: j.completed_at || '',
                  total_rows: Number(j.total_rows) || 0,
                  claims_rows: Number(j.claims_rows) || 0,
                  ads_rows: Number(j.ads_rows) || 0,
                  reach_rows: Number(j.reach_rows) || 0,
                  demo_rows: Number(j.demo_rows) || 0,
                  traffic_rows: Number(j.traffic_rows) || 0,
                  device_rows: Number(j.device_rows) || 0
                };
              }
            }
          }
        }

        const datesWithData = new Set<string>();
        rowsWithJobId = rows.map(r => {
          datesWithData.add(r.date);
          const jobInfo = dateJobMap[r.date];
          let status = jobInfo ? jobInfo.status : 'completed';
          
          // If the latest job failed/aborted but we have ClickHouse data for this day,
          // override status to completed so the daily card displays as green (completed)
          // and stays clickable to inspect the canceled/failed run's logs.
          if (status === 'failed' && (Number(r.row_count) > 0 || Number(r.partner_revenue) > 0)) {
            status = 'completed';
          }

          return {
            ...r,
            partner_revenue: Number(r.partner_revenue) || 0,
            yt_revenue: Number(r.yt_revenue) || 0,
            partner_revenue_ads: Number(r.partner_revenue_ads) || 0,
            partner_revenue_shorts_feed_ads: Number(r.partner_revenue_shorts_feed_ads) || 0,
            partner_revenue_premium: Number(r.partner_revenue_premium) || 0,
            partner_revenue_transaction: Number(r.partner_revenue_transaction) || 0,
            partner_revenue_channel_bonus: Number(r.partner_revenue_channel_bonus) || 0,
            partner_revenue_other: Number(r.partner_revenue_other) || 0,
            partner_revenue_shorts: Number(r.partner_revenue_shorts) || 0,
            shorts_count: Number(r.shorts_count) || 0,
            job_id: jobInfo ? jobInfo.job_id : null,
            status,
            error_message: jobInfo ? jobInfo.error_message : null,
            started_at: jobInfo ? jobInfo.started_at : null,
            completed_at: jobInfo ? jobInfo.completed_at : null,
            total_rows: jobInfo ? jobInfo.total_rows : 0,
            claims_rows: jobInfo ? jobInfo.claims_rows : 0,
            ads_rows: jobInfo ? jobInfo.ads_rows : 0,
            reach_rows: jobInfo ? jobInfo.reach_rows : 0,
            demo_rows: jobInfo ? jobInfo.demo_rows : 0,
            traffic_rows: jobInfo ? jobInfo.traffic_rows : 0,
            device_rows: jobInfo ? jobInfo.device_rows : 0
          };
        });

        // Add any jobs that don't have rows in estimated_revenue_daily yet (processing, pending, failed)
        for (const [dateStr, jobInfo] of Object.entries(dateJobMap)) {
          if (!datesWithData.has(dateStr)) {
            rowsWithJobId.push({
              date: dateStr,
              row_count: 0,
              partner_revenue: 0,
              yt_revenue: 0,
              job_id: jobInfo.job_id,
              status: jobInfo.status,
              started_at: jobInfo.started_at,
              completed_at: jobInfo.completed_at,
              total_rows: jobInfo.total_rows,
              claims_rows: jobInfo.claims_rows,
              ads_rows: jobInfo.ads_rows,
              reach_rows: jobInfo.reach_rows,
              demo_rows: jobInfo.demo_rows,
              traffic_rows: jobInfo.traffic_rows,
              device_rows: jobInfo.device_rows
            });
          }
        }
      } else {
        // Fallback mapping if defaultCh is not available
        rowsWithJobId = rows.map(r => ({
          ...r,
          partner_revenue: Number(r.partner_revenue) || 0,
          yt_revenue: Number(r.yt_revenue) || 0,
          job_id: null,
          status: 'completed',
          started_at: null,
          completed_at: null,
          total_rows: 0,
          claims_rows: 0,
          ads_rows: 0,
          reach_rows: 0,
          demo_rows: 0,
          traffic_rows: 0,
          device_rows: 0
        }));
      }

      return reply.send({ data: rowsWithJobId, month });
    } catch (err: any) {
      if (err.message?.includes('UNKNOWN_TABLE') || err.message?.includes('does not exist') || err.message?.includes('Table')) {
        return reply.send({ data: [], month });
      }
      return reply.code(500).send({ error: err.message });
    }
  });

  /**
   * GET /api/v1/analytics/video-reach
   */
  app.get('/api/v1/analytics/video-reach', async (req, reply) => {
    if (!req.tenant) return reply.code(400).send({ error: 'Tenant context required' });
    const { video_ids, startDate, endDate } = req.query as { video_ids?: string; startDate?: string; endDate?: string };
    if (!video_ids) return reply.code(400).send({ error: 'video_ids query parameter is required (comma-separated)' });
    if (!startDate || !endDate) return reply.code(400).send({ error: 'startDate and endDate parameters are required' });

    const videoIdArray = video_ids.split(',').map(id => id.trim()).filter(id => id.length > 0);
    if (videoIdArray.length === 0) return reply.send({ data: [] });

    try {
      const result = await req.tenant.readClient.query({
        query: `
          SELECT 
            toString(day) AS date,
            sum(impressions) AS impressions,
            sum(views) AS views,
            sum(watch_time_sec) AS watch_time_sec,
            CASE WHEN sum(impressions) > 0 
                 THEN CAST(sum(views) * 100.0 / sum(impressions), 'Decimal64(4)') 
                 ELSE CAST(0, 'Decimal64(4)') 
            END AS impressions_ctr
          FROM video_reach_performance_daily
          WHERE cms_id = {cms_id: String}
            AND video_id IN {video_ids: Array(String)} 
            AND day >= {startDate: Date} 
            AND day <= {endDate: Date}
          GROUP BY day
          ORDER BY day ASC
        `,
        query_params: { cms_id: req.tenant.cmsId, video_ids: videoIdArray, startDate, endDate },
        format: 'JSONEachRow',
      });
      const rows = await result.json();
      return reply.send({ data: rows });
    } catch (err: any) {
      if (err.message?.includes('UNKNOWN_TABLE') || err.message?.includes('does not exist') || err.message?.includes('Table')) {
        return reply.send({ data: [] });
      }
      return reply.code(500).send({ error: err.message });
    }
  });

  /**
   * GET /api/v1/analytics/video-demographics
   */
  app.get('/api/v1/analytics/video-demographics', async (req, reply) => {
    if (!req.tenant) return reply.code(400).send({ error: 'Tenant context required' });
    const { video_ids, startDate, endDate } = req.query as { video_ids?: string; startDate?: string; endDate?: string };
    if (!video_ids) return reply.code(400).send({ error: 'video_ids query parameter is required (comma-separated)' });

    const videoIdArray = video_ids.split(',').map(id => id.trim()).filter(id => id.length > 0);
    if (videoIdArray.length === 0) return reply.send({ data: [] });

    let whereClause = 'cms_id = {cms_id: String} AND video_id IN {video_ids: Array(String)}';
    const params: Record<string, any> = { cms_id: req.tenant.cmsId, video_ids: videoIdArray };

    if (startDate && endDate) {
      whereClause += ' AND day >= {startDate: Date} AND day <= {endDate: Date}';
      params.startDate = startDate;
      params.endDate = endDate;
    }

    try {
      const result = await req.tenant.readClient.query({
        query: `
          SELECT 
            age_group,
            gender,
            sum(views) AS total_views,
            sum(watch_time_sec) AS total_watch_time_sec
          FROM video_demographics_daily
          WHERE ${whereClause}
          GROUP BY age_group, gender
          ORDER BY total_views DESC
        `,
        query_params: params,
        format: 'JSONEachRow',
      });
      const rows = await result.json();
      return reply.send({ data: rows });
    } catch (err: any) {
      if (err.message?.includes('UNKNOWN_TABLE') || err.message?.includes('does not exist') || err.message?.includes('Table')) {
        return reply.send({ data: [] });
      }
      return reply.code(500).send({ error: err.message });
    }
  });

  /**
   * GET /api/v1/analytics/video-traffic-sources
   */
  app.get('/api/v1/analytics/video-traffic-sources', async (req, reply) => {
    if (!req.tenant) return reply.code(400).send({ error: 'Tenant context required' });
    const { video_ids, startDate, endDate } = req.query as { video_ids?: string; startDate?: string; endDate?: string };
    if (!video_ids) return reply.code(400).send({ error: 'video_ids query parameter is required (comma-separated)' });

    const videoIdArray = video_ids.split(',').map(id => id.trim()).filter(id => id.length > 0);
    if (videoIdArray.length === 0) return reply.send({ data: [] });

    let whereClause = 'cms_id = {cms_id: String} AND video_id IN {video_ids: Array(String)}';
    const params: Record<string, any> = { cms_id: req.tenant.cmsId, video_ids: videoIdArray };

    if (startDate && endDate) {
      whereClause += ' AND day >= {startDate: Date} AND day <= {endDate: Date}';
      params.startDate = startDate;
      params.endDate = endDate;
    }

    try {
      const result = await req.tenant.readClient.query({
        query: `
          SELECT 
            traffic_source_type,
            sum(views) AS total_views,
            sum(watch_time_sec) AS total_watch_time_sec
          FROM video_traffic_sources_daily
          WHERE ${whereClause}
          GROUP BY traffic_source_type
          ORDER BY total_views DESC
        `,
        query_params: params,
        format: 'JSONEachRow',
      });
      const rows = await result.json();
      return reply.send({ data: rows });
    } catch (err: any) {
      if (err.message?.includes('UNKNOWN_TABLE') || err.message?.includes('does not exist') || err.message?.includes('Table')) {
        return reply.send({ data: [] });
      }
      return reply.code(500).send({ error: err.message });
    }
  });

  /**
   * GET /api/v1/analytics/video-devices
   */
  app.get('/api/v1/analytics/video-devices', async (req, reply) => {
    if (!req.tenant) return reply.code(400).send({ error: 'Tenant context required' });
    const { video_ids, startDate, endDate } = req.query as { video_ids?: string; startDate?: string; endDate?: string };
    if (!video_ids) return reply.code(400).send({ error: 'video_ids query parameter is required (comma-separated)' });

    const videoIdArray = video_ids.split(',').map(id => id.trim()).filter(id => id.length > 0);
    if (videoIdArray.length === 0) return reply.send({ data: [] });

    let whereClause = 'cms_id = {cms_id: String} AND video_id IN {video_ids: Array(String)}';
    const params: Record<string, any> = { cms_id: req.tenant.cmsId, video_ids: videoIdArray };

    if (startDate && endDate) {
      whereClause += ' AND day >= {startDate: Date} AND day <= {endDate: Date}';
      params.startDate = startDate;
      params.endDate = endDate;
    }

    try {
      const result = await req.tenant.readClient.query({
        query: `
          SELECT 
            device_type,
            operating_system,
            sum(views) AS total_views,
            sum(watch_time_sec) AS total_watch_time_sec
          FROM video_devices_daily
          WHERE ${whereClause}
          GROUP BY device_type, operating_system
          ORDER BY total_views DESC
        `,
        query_params: params,
        format: 'JSONEachRow',
      });
      const rows = await result.json();
      return reply.send({ data: rows });
    } catch (err: any) {
      if (err.message?.includes('UNKNOWN_TABLE') || err.message?.includes('does not exist') || err.message?.includes('Table')) {
        return reply.send({ data: [] });
      }
      return reply.code(500).send({ error: err.message });
    }
  });

  /**
   * GET /api/v1/analytics/asset-videos
   */
  app.get('/api/v1/analytics/asset-videos', async (req, reply) => {
    if (!req.tenant) return reply.code(400).send({ error: 'Tenant context required' });
    const { asset_id, month } = req.query as { asset_id?: string; month?: string };
    if (!asset_id) return reply.code(400).send({ error: 'asset_id query parameter is required' });
    if (!month || !/^\d{6}$/.test(month)) return reply.code(400).send({ error: 'month parameter is required (format: YYYYMM)' });

    try {
      const result = await req.tenant.readClient.query({
        query: `
          SELECT 
            video_id,
            any(video_title) AS video_title,
            any(channel_display_name) AS channel_name,
            any(channel_id) AS channel_id,
            sum(owned_views) AS total_views,
            sum(partner_rev_total) AS total_partner_revenue
          FROM estimated_revenue_daily
          WHERE cms_id = {cms_id: String}
            AND asset_id = {asset_id: String} 
            AND toYYYYMM(day) = {month: UInt32}
          GROUP BY video_id
          ORDER BY total_partner_revenue DESC
        `,
        query_params: { cms_id: req.tenant.cmsId, asset_id, month: parseInt(month, 10) },
        format: 'JSONEachRow',
      });
      const rows = await result.json();
      return reply.send({ data: rows });
    } catch (err: any) {
      if (err.message?.includes('UNKNOWN_TABLE') || err.message?.includes('does not exist') || err.message?.includes('Table')) {
        return reply.send({ data: [] });
      }
      return reply.code(500).send({ error: err.message });
    }
  });

  /**
   * GET /api/v1/analytics/subscribers
   */
  app.get('/api/v1/analytics/subscribers', async (req, reply) => {
    if (!req.tenant) return reply.code(400).send({ error: 'Tenant context required' });
    const { month } = req.query as { month?: string };
    if (!month || !/^\d{6}$/.test(month)) return reply.code(400).send({ error: 'month parameter is required (format: YYYYMM)' });

    try {
      const result = await req.tenant.readClient.query({
        query: `
          SELECT 
            toString(day) AS date,
            sum(CAST(t.subscribers_gained, 'Int64')) AS subscribers_gained,
            sum(CAST(t.subscribers_lost, 'Int64')) AS subscribers_lost,
            sum(CAST(t.subscribers_gained, 'Int64')) - sum(CAST(t.subscribers_lost, 'Int64')) AS net_subscribers
          FROM channel_subscribers_daily t
          WHERE cms_id = {cms_id: String}
            AND toYYYYMM(day) = {month: UInt32}
          GROUP BY day
          ORDER BY day ASC
        `,
        query_params: { 
          cms_id: req.tenant.cmsId, 
          month: parseInt(month, 10) 
        },
        format: 'JSONEachRow',
      });
      const rows = await result.json();
      return reply.send({ data: rows });
    } catch (err: any) {
      if (
        err.message?.includes('UNKNOWN_TABLE') ||
        err.message?.includes('does not exist') ||
        err.message?.includes('Table')
      ) {
        req.log.warn(`[Analytics Subscribers] Table not found for tenant ${req.tenant.cmsId}, returning empty array`);
        return reply.send({ data: [] });
      }
      return reply.code(500).send({ error: err.message });
    }
  });

  /**
   * GET /api/v1/analytics/video-monetization-audit
   */
  app.get('/api/v1/analytics/video-monetization-audit', async (req, reply) => {
    if (!req.tenant) return reply.code(400).send({ error: 'Tenant context required' });
    const { month } = req.query as { month?: string };
    if (!month || !/^\d{6}$/.test(month)) return reply.code(400).send({ error: 'month parameter is required (format: YYYYMM)' });

    try {
      const result = await req.tenant.readClient.query({
        query: `
          WITH video_summary AS (
            SELECT
              video_id,
              any(policy) AS policy,
              sum(owned_views) AS views,
              sum(yt_rev_total) AS revenue,
              sum(partner_rev_total) AS partner_revenue
            FROM estimated_revenue_daily
            WHERE cms_id = {cms_id: String}
              AND toYYYYMM(day) = {month: UInt32}
            GROUP BY video_id
          )
          SELECT
            multiIf(
              policy = 'Block' OR policy = 'Track' OR policy = '', 'demonetized',
              views >= 100 AND (revenue / views * 1000) < 0.5, 'limited_ads',
              'fully_monetized'
            ) AS monetization_status,
            count(DISTINCT video_id) AS video_count,
            sum(views) AS total_views,
            sum(partner_revenue) AS total_partner_revenue
          FROM video_summary
          GROUP BY monetization_status
        `,
        query_params: { 
          cms_id: req.tenant.cmsId,
          month: parseInt(month, 10) 
        },
        format: 'JSONEachRow',
      });
      const rows = await result.json();
      return reply.send({ data: rows });
    } catch (err: any) {
      if (
        err.message?.includes('UNKNOWN_TABLE') ||
        err.message?.includes('does not exist') ||
        err.message?.includes('Table')
      ) {
        req.log.warn(`[Analytics Video Monetization Audit] Table not found for tenant ${req.tenant.cmsId}, returning empty array`);
        return reply.send({ data: [] });
      }
      return reply.code(500).send({ error: err.message });
    }
  });

  app.get('/api/v1/analytics/video-interactions', async (req, reply) => {
    if (!req.tenant) return reply.code(400).send({ error: 'Tenant context required' });
    const { month } = req.query as { month?: string };
    if (!month || !/^\d{6}$/.test(month)) return reply.code(400).send({ error: 'month parameter is required (format: YYYYMM)' });

    try {
      const result = await req.tenant.readClient.query({
        query: `
          SELECT
            toString(day) AS date,
            sum(likes) AS likes,
            sum(dislikes) AS dislikes,
            sum(comments) AS comments,
            sum(shares) AS shares
          FROM video_interactions_daily
          WHERE cms_id = {cms_id: String}
            AND toYYYYMM(day) = {month: UInt32}
          GROUP BY day
          ORDER BY day ASC
        `,
        query_params: { 
          cms_id: req.tenant.cmsId, 
          month: parseInt(month, 10) 
        },
        format: 'JSONEachRow',
      });
      const rows = await result.json();
      return reply.send({ data: rows });
    } catch (err: any) {
      if (
        err.message?.includes('UNKNOWN_TABLE') ||
        err.message?.includes('does not exist') ||
        err.message?.includes('Table')
      ) {
        req.log.warn(`[Analytics Video Interactions] Table not found for tenant ${req.tenant.cmsId}, returning empty array`);
        return reply.send({ data: [] });
      }
      return reply.code(500).send({ error: err.message });
    }
  });
}
