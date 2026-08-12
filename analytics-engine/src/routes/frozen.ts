import { FastifyPluginAsync } from 'fastify';
import { getDefaultClient } from '../config/clickhouse.js';
import { authMiddleware } from '../middleware/auth.js';

export const frozenRoutes: FastifyPluginAsync = async (fastify) => {
    fastify.addHook('onRequest', authMiddleware);


    // 1. FREEZE SETTLEMENT STATEMENTS
    fastify.post('/api/v1/settlements/freeze', async (request, reply) => {
        const body = request.body as any;
        const statements = body?.statements || [];

        if (!Array.isArray(statements) || statements.length === 0) {
            return reply.code(400).send({ error: 'No statements provided' });
        }

        const client = getDefaultClient();
        console.log(`[Analytics] Freezing ${statements.length} lines for ${statements[0]?.period}...`);

        try {
            // Bulk insert into ClickHouse
            await client.insert({
                table: 'settlement_statement_lines',
                values: statements.map((row: any) => ({
                    settlement_id: row.settlement_id,
                    period: row.period,
                    recipient_id: row.recipient_id,
                    channel_id: row.channel_id || '',
                    track_title: row.track_title || 'Unknown',
                    track_artist: row.track_artist || 'Unknown',
                    isrc: row.isrc || '',
                    upc: row.upc || '',
                    track_type: row.track_type || 'Original',
                    views: Number(row.views) || 0,
                    gross_revenue_usd: Number(row.gross_revenue_usd) || 0,
                    share_pct: Number(row.share_pct) || 0,
                    net_revenue_usd: Number(row.net_revenue_usd) || 0,
                    net_revenue_idr: Number(row.net_revenue_idr) || 0,
                    exchange_rate: Number(row.exchange_rate) || 0,
                    creator_name: row.creator_name || ''
                })),
                format: 'JSONEachRow'
            });

            return { success: true, message: `Successfully froze ${statements.length} lines in ClickHouse` };
        } catch (error) {
            request.log.error(error);
            return reply.code(500).send({ error: 'Failed to freeze settlement statements' });
        }
    });

    // 2. RETRIEVE FROZEN STATEMENTS
    fastify.get('/api/v1/settlements/frozen', async (request, reply) => {
        const query = (request.query as any);
        const settlementId = query.settlement_id;
        const recipientId = query.recipient_id;

        if (!settlementId || !recipientId) {
            return reply.code(400).send({ error: 'Missing settlement_id or recipient_id param' });
        }

        const client = getDefaultClient();

        try {
            const sql = `
                SELECT 
                    track_title as trackTitle,
                    track_artist as trackArtist,
                    isrc,
                    upc,
                    track_type as trackType,
                    views,
                    gross_revenue_usd as grossRevenueUsd,
                    share_pct as sharePct,
                    net_revenue_usd as netRevenueUsd,
                    net_revenue_idr as netRevenueIdr,
                    exchange_rate as exchangeRate,
                    creator_name as creatorName
                FROM settlement_statement_lines
                WHERE settlement_id = {settlementId: String}
                  AND recipient_id = {recipientId: String}
                ORDER BY net_revenue_idr DESC
            `;

            const resultSet = await client.query({
                query: sql,
                query_params: {
                    settlementId,
                    recipientId
                },
                format: 'JSONEachRow'
            });

            const data = await resultSet.json();
            return { success: true, data };

        } catch (error) {
            request.log.error(error);
            return reply.code(500).send({ error: 'Failed to retrieve frozen settlement statements' });
        }
    });
};
