import type { ClickHouseClient } from '@clickhouse/client';
import { getClickHouseClient } from '../config/clickhouse.js';

/**
 * Post-ingestion reconciliation: fix null/empty labels using channel_label_map.
 * Finds rows where label is empty but channel_id exists in the mapping table,
 * then re-inserts corrected rows (ReplacingMergeTree overwrites old ones).
 */
export async function reconcileNullLabels(opts: {
    dbName: string;
    month: number; // YYYYMM format
    cmsId: string;
}): Promise<{ corrected: number }> {
    const { dbName, month, cmsId } = opts;
    const client = getClickHouseClient({ database: dbName });

    console.log(`[Reconcile] Searching for rows with empty labels in ${dbName}, month ${month}...`);

    // Find rows that can be fixed
    const result = await client.query({
        query: `
      SELECT 
        r.*,
        m.asset_label AS correct_label
      FROM ads_revenue_enriched r
      INNER JOIN channel_label_map m 
        ON r.channel_id = m.channel_id
      WHERE r.upload_month = {month: UInt32}
        AND (r.label = '' OR r.asset_labels = '')
        AND r.label_source = 'report'
    `,
        query_params: { month },
        format: 'JSONEachRow',
    });

    const rows = await result.json() as Record<string, unknown>[];

    if (rows.length === 0) {
        console.log(`[Reconcile] No rows to fix.`);
        return { corrected: 0 };
    }

    console.log(`[Reconcile] Found ${rows.length.toLocaleString()} rows to fix.`);

    // Correct and re-insert in batches
    const BATCH = 50_000;
    for (let i = 0; i < rows.length; i += BATCH) {
        const batch = rows.slice(i, i + BATCH).map(row => {
            const correctLabel = row['correct_label'] as string | undefined;
            const { correct_label: _unused, ...rest } = row;
            return {
                ...rest,
                asset_labels: correctLabel || (rest['asset_labels'] as string) || '',
                label_source: 'reconciled',
            };
        });

        await client.insert({
            table: 'ads_revenue_enriched',
            values: batch,
            format: 'JSONEachRow',
        });
    }

    // Force merge to deduplicate using composite partition key
    await client.command({
        query: `OPTIMIZE TABLE ads_revenue_enriched PARTITION ({cmsId: String}, {month: UInt32}) FINAL`,
        query_params: { cmsId, month }
    });

    console.log(`[Reconcile] ✅ ${rows.length} rows corrected.`);
    return { corrected: rows.length };
}
