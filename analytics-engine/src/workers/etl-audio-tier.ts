import { ClickHouseClient } from '@clickhouse/client';
import { loadStagingTable } from './etl-ads.js';

function getAudioTierInsertSelectQuery(
    tempAudioTable: string,
    cmsId: string,
    month: string,
    usTaxRate: number
): string {
    const cleanCmsId = cmsId.replace(/-/g, '_');
    return `
        INSERT INTO audio_tier_revenue (
            cms_id, upload_month, day, country, video_id, asset_id, channel_id,
            asset_title, asset_labels, custom_id, isrc, upc, grid, artist, album, label,
            adjustment_type,
            owned_views, yt_rev_total, partner_rev_pro_rata, partner_rev_per_play_min, partner_rev_total,
            us_tax, net_revenue
        )
        SELECT
            {cmsId: String} AS cms_id,
            {uploadMonth: UInt32} AS upload_month,
            toDateOrZero(
                if(length(coalesce(nullIf(s."Date", ''), nullIf(s.date, ''), nullIf(s."Day", ''), nullIf(s.day, ''), '')) = 8, 
                    concat(
                        substring(coalesce(nullIf(s."Date", ''), nullIf(s.date, ''), nullIf(s."Day", ''), nullIf(s.day, ''), ''), 1, 4), '-', 
                        substring(coalesce(nullIf(s."Date", ''), nullIf(s.date, ''), nullIf(s."Day", ''), nullIf(s.day, ''), ''), 5, 2), '-', 
                        substring(coalesce(nullIf(s."Date", ''), nullIf(s.date, ''), nullIf(s."Day", ''), nullIf(s.day, ''), ''), 7, 2)
                    ), 
                    coalesce(nullIf(s."Date", ''), nullIf(s.date, ''), nullIf(s."Day", ''), nullIf(s.day, ''), '')
                )
            ) AS day,
            coalesce(nullIf(s."Country", ''), nullIf(s.country, ''), '') AS country,
            coalesce(nullIf(s."Video ID", ''), nullIf(s.video_id, ''), '') AS video_id,
            coalesce(nullIf(s."Asset ID", ''), nullIf(s.asset_id, ''), '') AS asset_id,
            coalesce(nullIf(v_chan.matched_channel_id, ''), nullIf(g_chan.matched_channel_id, ''), nullIf(a_chan.matched_channel_id, ''), '') AS channel_id,
            coalesce(nullIf(s."Asset Title", ''), nullIf(s.asset_title, ''), nullIf(a_detail.asset_title, ''), nullIf(g_am.asset_title, ''), '') AS asset_title,
            coalesce(nullIf(s."Asset Labels", ''), nullIf(s.asset_labels, ''), nullIf(g_am.asset_labels, ''), '') AS asset_labels,
            coalesce(nullIf(s."Custom ID", ''), nullIf(s.custom_id, ''), nullIf(g_am.custom_id, ''), '') AS custom_id,
            coalesce(nullIf(s."ISRC", ''), nullIf(s.isrc, ''), nullIf(a_detail.isrc, ''), nullIf(g_am.isrc, ''), '') AS isrc,
            coalesce(nullIf(s."UPC", ''), nullIf(s.upc, ''), nullIf(a_detail.upc, ''), nullIf(g_am.upc, ''), '') AS upc,
            coalesce(nullIf(s."GRid", ''), nullIf(s.grid, ''), nullIf(g_am.grid, ''), '') AS grid,
            coalesce(nullIf(s."Artist", ''), nullIf(s.artist, ''), nullIf(a_detail.artist, ''), nullIf(g_am.artist, ''), '') AS artist,
            coalesce(nullIf(s."Album", ''), nullIf(s.album, ''), nullIf(a_detail.album, ''), nullIf(g_am.album, ''), '') AS album,
            coalesce(nullIf(s."Label", ''), nullIf(s.label, ''), nullIf(s."Record Label", ''), nullIf(s.record_label, ''), nullIf(a_detail.label, ''), nullIf(g_am.label, ''), '') AS label,
            coalesce(nullIf(s."Adjustment Type", ''), nullIf(s.adjustment_type, ''), '') AS adjustment_type,
            
            toUInt64OrZero(coalesce(nullIf(s."Owned Views", ''), nullIf(s.owned_views, ''), nullIf(s.views, ''), '0')) AS owned_views,
            CAST(coalesce(nullIf(s."YouTube Revenue Split", ''), nullIf(s.youtube_revenue_split, ''), '0') AS Decimal64(10)) AS yt_rev_total,
            CAST(coalesce(nullIf(s."Partner Revenue Pro Rata", ''), nullIf(s.partner_revenue_pro_rata, ''), '0') AS Decimal64(10)) AS partner_rev_pro_rata,
            CAST(coalesce(nullIf(s."Partner Revenue Per Play Min", ''), nullIf(s.partner_revenue_per_play_min, ''), '0') AS Decimal64(10)) AS partner_rev_per_play_min,
            CAST(coalesce(nullIf(s."Partner Revenue", ''), nullIf(s.partner_revenue, ''), nullIf(s.partner_revenue_usd, ''), '0') AS Decimal64(10)) AS partner_rev_total,
            
            -- US Tax calculation
            CAST(if(coalesce(nullIf(s."Country", ''), nullIf(s.country, ''), '') = 'US', partner_rev_total * toDecimal64({usTaxRate: Float64} / 100.0, 4), 0) AS Decimal64(10)) AS us_tax,
            CAST(partner_rev_total - us_tax AS Decimal64(10)) AS net_revenue
        FROM ${tempAudioTable} s
        LEFT JOIN (
            SELECT asset_id, any(isrc) as isrc, any(upc) as upc, any(artist) as artist, any(asset_title) as asset_title, any(album) as album, any(label) as label
            FROM db_${cleanCmsId}.ads_revenue_enriched
            WHERE upload_month = {uploadMonth: UInt32}
            GROUP BY asset_id
        ) a_detail ON coalesce(nullIf(s."Asset ID", ''), nullIf(s.asset_id, '')) = a_detail.asset_id
        LEFT JOIN (
            SELECT
                asset_id,
                argMax(asset_title, day) AS asset_title,
                argMax(artist, day) AS artist,
                argMax(album, day) AS album,
                argMax(label, day) AS label,
                argMax(isrc, day) AS isrc,
                argMax(upc, day) AS upc,
                argMax(grid, day) AS grid,
                argMax(custom_id, day) AS custom_id,
                argMax(asset_labels, day) AS asset_labels
            FROM db_${cleanCmsId}.youtube_asset_metadata
            GROUP BY asset_id
        ) g_am ON coalesce(nullIf(s."Asset ID", ''), nullIf(s.asset_id, '')) = g_am.asset_id
        LEFT JOIN (
            SELECT video_id, any(channel_id) as matched_channel_id
            FROM db_${cleanCmsId}.ads_revenue_enriched
            WHERE upload_month = {uploadMonth: UInt32} AND channel_id != ''
            GROUP BY video_id
        ) v_chan ON coalesce(nullIf(s."Video ID", ''), nullIf(s.video_id, '')) = v_chan.video_id
        LEFT JOIN (
            SELECT video_id, argMax(channel_id, day) as matched_channel_id
            FROM db_${cleanCmsId}.youtube_video_metadata
            WHERE channel_id != ''
            GROUP BY video_id
        ) g_chan ON coalesce(nullIf(s."Video ID", ''), nullIf(s.video_id, '')) = g_chan.video_id
        LEFT JOIN (
            SELECT asset_id, any(channel_id) as matched_channel_id
            FROM db_${cleanCmsId}.ads_revenue_enriched
            WHERE upload_month = {uploadMonth: UInt32} AND channel_id != ''
            GROUP BY asset_id
        ) a_chan ON coalesce(nullIf(s."Asset ID", ''), nullIf(s.asset_id, '')) = a_chan.asset_id
    `;
}

export async function runAudioTierIngestionDirect(opts: {
    jobId: string;
    month: string;
    cmsId: string;
    usTaxRate: number;
    filePath: string;
    client: ClickHouseClient;
    isAborted: () => boolean;
    log: (msg: string) => void;
}): Promise<{ audioTierRows: number; audioTierRevenue: number }> {
    const { jobId, month, cmsId, filePath, client, isAborted, log, usTaxRate } = opts;

    const cleanCmsId = cmsId.replace(/-/g, '_');
    const cleanJobId = jobId.replace(/-/g, '_');
    const tempAudioTable = `temp_monthly_audio_tier_${cleanCmsId}_${cleanJobId}`;

    try {
        if (isAborted()) throw new Error("Job aborted by user");

        // 1. Create Staging Table
        log(`[Audio Tier] Creating ClickHouse staging table...`);
        await client.command({
            query: `CREATE TABLE IF NOT EXISTS ${tempAudioTable} (
                "Date" String DEFAULT '',
                "date" String DEFAULT '',
                "Day" String DEFAULT '',
                "day" String DEFAULT '',
                "Country" String DEFAULT '',
                "country" String DEFAULT '',
                "Video ID" String DEFAULT '',
                "video_id" String DEFAULT '',
                "Asset ID" String DEFAULT '',
                "asset_id" String DEFAULT '',
                "Asset Title" String DEFAULT '',
                "asset_title" String DEFAULT '',
                "Asset Labels" String DEFAULT '',
                "asset_labels" String DEFAULT '',
                "Custom ID" String DEFAULT '',
                "custom_id" String DEFAULT '',
                "ISRC" String DEFAULT '',
                "isrc" String DEFAULT '',
                "UPC" String DEFAULT '',
                "upc" String DEFAULT '',
                "GRid" String DEFAULT '',
                "grid" String DEFAULT '',
                "Artist" String DEFAULT '',
                "artist" String DEFAULT '',
                "Album" String DEFAULT '',
                "album" String DEFAULT '',
                "Label" String DEFAULT '',
                "label" String DEFAULT '',
                "Record Label" String DEFAULT '',
                "record_label" String DEFAULT '',
                "Adjustment Type" String DEFAULT '',
                "adjustment_type" String DEFAULT '',
                "Owned Views" String DEFAULT '',
                "owned_views" String DEFAULT '',
                "views" String DEFAULT '',
                "YouTube Revenue Split" String DEFAULT '',
                "youtube_revenue_split" String DEFAULT '',
                "Partner Revenue Pro Rata" String DEFAULT '',
                "partner_revenue_pro_rata" String DEFAULT '',
                "Partner Revenue Per Play Min" String DEFAULT '',
                "partner_revenue_per_play_min" String DEFAULT '',
                "Partner Revenue" String DEFAULT '',
                "partner_revenue" String DEFAULT '',
                "partner_revenue_usd" String DEFAULT ''
            ) ENGINE = StripeLog()`
        });

        if (isAborted()) throw new Error("Job aborted by user");

        // 2. Load Audio Tier Raw file
        log(`[Audio Tier] Loading Audio Tier raw file into ClickHouse staging...`);
        await loadStagingTable(client, tempAudioTable, filePath, isAborted);

        // 3. Query row count
        const countRes = await client.query({
            query: `SELECT count() as cnt FROM ${tempAudioTable}`,
            format: 'JSONEachRow'
        });
        const countRows = await countRes.json() as any[];
        const audioTierRows = countRows[0]?.cnt ? parseInt(countRows[0].cnt, 10) : 0;
        log(`[Audio Tier] Ingesting ${audioTierRows.toLocaleString()} audio tier rows using ClickHouse SQL Join...`);

        if (isAborted()) throw new Error("Job aborted by user");

        // 4. Execute INSERT SELECT for Audio Tier
        const insertQuery = getAudioTierInsertSelectQuery(tempAudioTable, cmsId, month, usTaxRate);
        await client.command({
            query: insertQuery,
            query_params: { cmsId, uploadMonth: parseInt(month, 10), usTaxRate }
        });

        // 5. Calculate total partner revenue for ingestion_jobs stats
        const revRes = await client.query({
            query: `SELECT sum(partner_rev_total) as total_rev FROM db_${cleanCmsId}.audio_tier_revenue WHERE upload_month = {uploadMonth: UInt32} AND cms_id = {cmsId: String}`,
            query_params: { cmsId, uploadMonth: parseInt(month, 10) },
            format: 'JSONEachRow'
        });
        const revRows = await revRes.json() as any[];
        const audioTierRevenue = revRows[0]?.total_rev ? parseFloat(revRows[0].total_rev) : 0;

        return { audioTierRows, audioTierRevenue };

    } finally {
        log(`[Audio Tier] Cleaning up staging table...`);
        await client.command({ query: `DROP TABLE IF EXISTS ${tempAudioTable}` }).catch(() => {});
    }
}
