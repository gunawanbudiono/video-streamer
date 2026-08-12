import type { FastifyInstance } from 'fastify';
import { authMiddleware } from '../middleware/auth.js';

/** Revenue analytics routes — requires CMS API key */
export async function revenueRoutes(app: FastifyInstance): Promise<void> {
    // All routes in this plugin require auth
    app.addHook('onRequest', authMiddleware);

    /**
     * GET /api/v1/revenue/daily?month=202601
     * Returns daily revenue totals for the given month
     */
    app.get('/api/v1/revenue/daily', async (req, reply) => {
        const { month } = req.query as { month?: string };
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
              day,
              sum(owned_views)       AS total_views,
              sum(yt_rev_total)      AS total_yt_revenue,
              sum(partner_rev_total) AS total_partner_revenue
            FROM ${tableName}
            WHERE ${whereClause}
            GROUP BY day
            ORDER BY day
          `,
                query_params: { 
                    cms_id: req.tenant!.cmsId,
                    month: parseInt(month, 10) 
                },
                format: 'JSONEachRow',
            });

            const rows = await result.json();
            return reply.send({ data: rows, month });
        } catch (err: any) {
            if (
                err.message?.includes('UNKNOWN_TABLE') ||
                err.message?.includes('does not exist') ||
                err.message?.includes('Table')
            ) {
                return reply.send({ data: [], month });
            }
            return reply.code(500).send({ error: err.message });
        }
    });

    /**
     * GET /api/v1/views/daily?month=202601
     * Returns daily views totals for the given month
     */
    app.get('/api/v1/views/daily', async (req, reply) => {
        const { month } = req.query as { month?: string };
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
              day,
              sum(owned_views) AS total_views,
              ${isEst ? 'sum(owned_views)' : "sumIf(owned_views, report_type = 'claim_raw')"} AS ads_views,
              ${isEst ? "CAST(0, 'UInt64')" : "sumIf(owned_views, report_type IN ('ads_adjustment', 'sub_adjustment'))"} AS adjustment_views
            FROM ${tableName}
            WHERE ${whereClause}
            GROUP BY day
            ORDER BY day
          `,
                query_params: { 
                    cms_id: req.tenant!.cmsId,
                    month: parseInt(month, 10) 
                },
                format: 'JSONEachRow',
            });

            const rows = await result.json();
            return reply.send({ data: rows, month });
        } catch (err: any) {
            if (
                err.message?.includes('UNKNOWN_TABLE') ||
                err.message?.includes('does not exist') ||
                err.message?.includes('Table')
            ) {
                return reply.send({ data: [], month });
            }
            return reply.code(500).send({ error: err.message });
        }
    });
}
