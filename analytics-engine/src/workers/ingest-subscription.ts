import { ClickHouseClient } from '@clickhouse/client';
import { extractFileStream, loadStagingTable, ingestAffiliateTaxTable } from './etl-ads.js';

/** Parse Day column (YYYYMMDD) to YYYY-MM-DD */
function parseDay(val: string): string {
    if (val.length === 8) return `${val.slice(0, 4)}-${val.slice(4, 6)}-${val.slice(6, 8)}`;
    return val;
}

// ── Direct-to-ClickHouse Ingestion ──────────────────────────

function getSubInsertSelectQuery(
    tempSubTable: string,
    tempChannelMapTable: string,
    cmsId: string,
    month: string,
    usTaxRate: number,
    reportType: string
): string {
    const cleanCmsId = cmsId.replace(/-/g, '_');
    const rawVideoChan = `coalesce(nullIf(s."Video Channel ID", ''), nullIf(s.video_channel_id, ''), nullIf(s."Channel ID", ''), nullIf(s.channel_id, ''), '')`;
    const normVideoChan = `if(${rawVideoChan} = '' OR startsWith(${rawVideoChan}, 'UC'), ${rawVideoChan}, concat('UC', ${rawVideoChan}))`;
    const rawAssetChan = `coalesce(nullIf(s."Asset Channel ID", ''), nullIf(s.asset_channel_id, ''), '')`;
    const normAssetChan = `if(${rawAssetChan} = '' OR startsWith(${rawAssetChan}, 'UC'), ${rawAssetChan}, concat('UC', ${rawAssetChan}))`;

    return `
        INSERT INTO subscription_revenue (
            cms_id, upload_month, report_type, day, country, video_id, channel_id, asset_id, asset_channel_id,
            asset_title, asset_labels, asset_type, custom_id, isrc, upc, grid, artist, album, label,
            claim_type, content_type, offer, adjustment_type,
            video_title, video_duration_sec, uploader, channel_display_name,
            owned_views, monetized_views_audio, monetized_views_audiovisual, monetized_views_total, yt_rev_total,
            partner_rev_pro_rata, partner_rev_per_sub_min, partner_rev_total,
            us_tax, net_revenue
        )
        SELECT
            {cmsId: String} AS cms_id,
            {uploadMonth: UInt32} AS upload_month,
            {reportType: String} AS report_type,
            toDateOrZero(
                if(length(coalesce(nullIf(s."Day", ''), nullIf(s."Date", ''), nullIf(s."Report Date", ''), nullIf(s."Report Data", ''), nullIf(s.day, ''), nullIf(s.date, ''), '')) = 8, 
                concat(
                    substring(coalesce(nullIf(s."Day", ''), nullIf(s."Date", ''), nullIf(s."Report Date", ''), nullIf(s."Report Data", ''), nullIf(s.day, ''), nullIf(s.date, ''), ''), 1, 4), '-', 
                    substring(coalesce(nullIf(s."Day", ''), nullIf(s."Date", ''), nullIf(s."Report Date", ''), nullIf(s."Report Data", ''), nullIf(s.day, ''), nullIf(s.date, ''), ''), 5, 2), '-', 
                    substring(coalesce(nullIf(s."Day", ''), nullIf(s."Date", ''), nullIf(s."Report Date", ''), nullIf(s."Report Data", ''), nullIf(s.day, ''), nullIf(s.date, ''), ''), 7, 2)
                ), 
                coalesce(nullIf(s."Day", ''), nullIf(s."Date", ''), nullIf(s."Report Date", ''), nullIf(s."Report Data", ''), nullIf(s.day, ''), nullIf(s.date, ''), '')
            )) AS day,
            coalesce(nullIf(s."Country", ''), nullIf(s.country, ''), '') AS country,
            coalesce(nullIf(s."Video ID", ''), nullIf(s.video_id, ''), '') AS video_id,
            ${normVideoChan} AS channel_id,
            coalesce(nullIf(s."Asset ID", ''), nullIf(s.asset_id, ''), '') AS asset_id,
            ${normAssetChan} AS asset_channel_id,
            coalesce(nullIf(s."Asset Title", ''), nullIf(s.asset_title, ''), nullIf(a_detail.asset_title, ''), nullIf(g_am.asset_title, ''), '') AS asset_title,
            coalesce(
                nullIf(s."Asset Labels", ''), 
                nullIf(s.asset_labels, ''), 
                nullIf(cm_v.mapped_name, ''), 
                nullIf(cm_a.mapped_name, ''), 
                nullIf(g_am.asset_labels, ''),
                ''
            ) AS asset_labels,
            coalesce(nullIf(s."Asset Type", ''), nullIf(s.asset_type, ''), '') AS asset_type,
            coalesce(nullIf(s."Custom ID", ''), nullIf(s.custom_id, ''), nullIf(g_am.custom_id, ''), '') AS custom_id,
            coalesce(nullIf(s."ISRC", ''), nullIf(s.isrc, ''), nullIf(a_detail.isrc, ''), nullIf(g_am.isrc, ''), '') AS isrc,
            coalesce(nullIf(s."UPC", ''), nullIf(s.upc, ''), nullIf(a_detail.upc, ''), nullIf(g_am.upc, ''), '') AS upc,
            coalesce(nullIf(s."GRid", ''), nullIf(s.grid, ''), nullIf(g_am.grid, ''), '') AS grid,
            coalesce(nullIf(s."Artist", ''), nullIf(s.artist, ''), nullIf(a_detail.artist, ''), nullIf(g_am.artist, ''), '') AS artist,
            coalesce(nullIf(s."Album", ''), nullIf(s.album, ''), nullIf(a_detail.album, ''), nullIf(g_am.album, ''), '') AS album,
            coalesce(nullIf(s."Label", ''), nullIf(s.label, ''), nullIf(s."Record Label", ''), nullIf(s.record_label, ''), nullIf(a_detail.label, ''), nullIf(g_am.label, ''), '') AS label,
            coalesce(nullIf(s."Claim Type", ''), nullIf(s.claim_type, ''), '') AS claim_type,
            coalesce(nullIf(s."Content Type", ''), nullIf(s.content_type, ''), '') AS content_type,
            coalesce(nullIf(s."Offer", ''), nullIf(s.offer, ''), '') AS offer,
            coalesce(nullIf(s."Adjustment Type", ''), nullIf(s.adjustment_type, ''), '') AS adjustment_type,
            
            -- Video cross-reference details
            coalesce(nullIf(trim(s."Video Title"), ''), nullIf(trim(s.video_title), ''), nullIf(trim(v_detail.video_title), ''), nullIf(trim(g_vm.video_title), ''), '') AS video_title,
            coalesce(toUInt32OrZero(s."Video Duration (sec)"), v_detail.video_duration_sec, g_vm.video_length_sec, 0) AS video_duration_sec,
            coalesce(nullIf(trim(s."Username"), ''), nullIf(trim(s.username), ''), nullIf(trim(v_detail.uploader), ''), '') AS uploader,
            coalesce(nullIf(trim(s."Uploader"), ''), nullIf(trim(s.uploader), ''), nullIf(trim(c_detail.channel_display_name), ''), nullIf(trim(g_vm.channel_display_name), ''), '') AS channel_display_name,

            toUInt64OrZero(coalesce(nullIf(s."Owned Views", ''), nullIf(s.owned_views, ''), nullIf(s.views, ''), nullIf(s."Owned Subscription Views", ''), nullIf(s.owned_subscription_views, ''), '0')) AS owned_views,
            toUInt64OrZero(coalesce(nullIf(s."Monetized Views : Audio", ''), nullIf(s.monetized_views_audio, ''), '0')) AS monetized_views_audio,
            toUInt64OrZero(coalesce(nullIf(s."Monetized Views : Audiovisual", ''), nullIf(s.monetized_views_audiovisual, ''), '0')) AS monetized_views_audiovisual,
            toUInt64OrZero(coalesce(nullIf(s."Monetized Views", ''), nullIf(s.monetized_views, ''), '0')) AS monetized_views_total,
            CAST(coalesce(nullIf(s."YouTube Revenue Split", ''), nullIf(s.youtube_revenue_split, ''), '0') AS Decimal64(10)) AS yt_rev_total,
            CAST(coalesce(nullIf(s."Partner Revenue : Pro Rata", ''), nullIf(s.partner_revenue_pro_rata, ''), '0') AS Decimal64(10)) AS partner_rev_pro_rata,
            CAST(coalesce(nullIf(s."Partner Revenue : Per Subscriber Min", ''), nullIf(s.partner_revenue_per_sub_min, ''), '0') AS Decimal64(10)) AS partner_rev_per_sub_min,
            CAST(coalesce(nullIf(s."Partner Revenue", ''), nullIf(s.partner_revenue, ''), nullIf(s.partner_revenue_usd, ''), '0') AS Decimal64(10)) AS partner_rev_total,
            
            -- US Tax calculation
            CAST(if(coalesce(nullIf(s."Country", ''), nullIf(s.country, ''), '') = 'US', partner_rev_total * toDecimal64(if(tax_map.channel_id != '', tax_map.tax_rate, {usTaxRate: Float64}) / 100.0, 4), 0) AS Decimal64(10)) AS us_tax,
            CAST(partner_rev_total - us_tax AS Decimal64(10)) AS net_revenue
        FROM ${tempSubTable} s
        LEFT JOIN ${tempChannelMapTable} cm_v ON ${normVideoChan} = cm_v.channel_id
        LEFT JOIN ${tempChannelMapTable} cm_a ON ${normAssetChan} = cm_a.channel_id
        LEFT JOIN (
            SELECT channel_id, argMax(tax_rate, upload_month) as tax_rate
            FROM db_${cleanCmsId}.youtube_affiliate_tax_rates
            WHERE upload_month = {uploadMonth: UInt32} AND revenue_source LIKE '%Subscription%'
            GROUP BY channel_id
        ) tax_map ON ${normVideoChan} = tax_map.channel_id
        LEFT JOIN (
            SELECT video_id, any(video_title) as video_title, any(video_duration_sec) as video_duration_sec, any(uploader) as uploader
            FROM db_${cleanCmsId}.ads_revenue_enriched
            WHERE upload_month = {uploadMonth: UInt32}
            GROUP BY video_id
        ) v_detail ON coalesce(nullIf(s."Video ID", ''), nullIf(s.video_id, '')) = v_detail.video_id
        LEFT JOIN (
            SELECT
                video_id,
                argMax(video_title, day) AS video_title,
                argMax(channel_display_name, day) AS channel_display_name,
                argMax(video_length_sec, day) AS video_length_sec
            FROM db_${cleanCmsId}.youtube_video_metadata
            GROUP BY video_id
        ) g_vm ON coalesce(nullIf(s."Video ID", ''), nullIf(s.video_id, '')) = g_vm.video_id
        LEFT JOIN (
            SELECT channel_id, max(nullIf(trim(channel_display_name), '')) as channel_display_name
            FROM db_${cleanCmsId}.ads_revenue_enriched
            WHERE upload_month = {uploadMonth: UInt32}
            GROUP BY channel_id
            HAVING channel_display_name != ''
        ) c_detail ON ${normVideoChan} = c_detail.channel_id
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
    `;
}

export async function runSubscriptionIngestionDirect(opts: {
    jobId: string;
    month: string;
    cmsId: string;
    usTaxRate: number;
    files: {
        subscription: string;
        adj_red_label?: string;
        shorts_subs?: string;
        affiliate_tax?: string;
    };
    channelMap: Map<string, string>;
    client: ClickHouseClient;
    isAborted: () => boolean;
    log: (msg: string) => void;
}): Promise<{ subRows: number; adjSubRows: number; shortsSubRows: number; totalAllRows: number }> {
    const { jobId, month, cmsId, files, channelMap, client, isAborted, log, usTaxRate } = opts;

    const cleanCmsId = cmsId.replace(/-/g, '_');
    const cleanJobId = jobId.replace(/-/g, '_');
    const tempSubTable = `temp_monthly_subscription_${cleanCmsId}_${cleanJobId}`;
    const tempChannelMapTable = `temp_monthly_sub_channel_map_${cleanCmsId}_${cleanJobId}`;
    const tempAffTaxTable = `temp_affiliate_tax_${cleanCmsId}_${cleanJobId}`;

    try {
        if (isAborted()) throw new Error("Job aborted by user");

        // Ensure youtube_affiliate_tax_rates table exists and process Affiliate Tax Summary if provided
        if (files.affiliate_tax) {
            await ingestAffiliateTaxTable({
                client,
                cmsId,
                month,
                jobId,
                affiliateTaxPath: files.affiliate_tax,
                isAborted,
                log
            });
        }

        // 1. Create Staging Tables
        log(`[Step 2/2] Creating ClickHouse staging tables...`);
        await client.command({
            query: `CREATE TABLE IF NOT EXISTS ${tempSubTable} (
                "Day" String DEFAULT '',
                "Date" String DEFAULT '',
                "Report Date" String DEFAULT '',
                "Report Data" String DEFAULT '',
                "day" String DEFAULT '',
                "date" String DEFAULT '',
                "Country" String DEFAULT '',
                "country" String DEFAULT '',
                "Video ID" String DEFAULT '',
                "video_id" String DEFAULT '',
                "Video Channel ID" String DEFAULT '',
                "video_channel_id" String DEFAULT '',
                "Asset ID" String DEFAULT '',
                "asset_id" String DEFAULT '',
                "Asset Channel ID" String DEFAULT '',
                "asset_channel_id" String DEFAULT '',
                "Asset Title" String DEFAULT '',
                "asset_title" String DEFAULT '',
                "Asset Labels" String DEFAULT '',
                "asset_labels" String DEFAULT '',
                "Asset Type" String DEFAULT '',
                "asset_type" String DEFAULT '',
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
                "Claim Type" String DEFAULT '',
                "claim_type" String DEFAULT '',
                "Content Type" String DEFAULT '',
                "content_type" String DEFAULT '',
                "Offer" String DEFAULT '',
                "offer" String DEFAULT '',
                "Owned Views" String DEFAULT '',
                "owned_views" String DEFAULT '',
                "views" String DEFAULT '',
                "Monetized Views : Audio" String DEFAULT '',
                "monetized_views_audio" String DEFAULT '',
                "Monetized Views : Audiovisual" String DEFAULT '',
                "monetized_views_audiovisual" String DEFAULT '',
                "Monetized Views" String DEFAULT '',
                "monetized_views" String DEFAULT '',
                "YouTube Revenue Split" String DEFAULT '',
                "youtube_revenue_split" String DEFAULT '',
                "Partner Revenue : Pro Rata" String DEFAULT '',
                "partner_revenue_pro_rata" String DEFAULT '',
                "Partner Revenue : Per Subscriber Min" String DEFAULT '',
                "partner_revenue_per_sub_min" String DEFAULT '',
                "Partner Revenue" String DEFAULT '',
                "partner_revenue" String DEFAULT '',
                "partner_revenue_usd" String DEFAULT '',
                "Adjustment Type" String DEFAULT '',
                "adjustment_type" String DEFAULT '',
                "Channel ID" String DEFAULT '',
                "channel_id" String DEFAULT '',
                "Owned Subscription Views" String DEFAULT '',
                "owned_subscription_views" String DEFAULT '',
                "Username" String DEFAULT '',
                "username" String DEFAULT '',
                "Uploader" String DEFAULT '',
                "uploader" String DEFAULT '',
                "Video Title" String DEFAULT '',
                "video_title" String DEFAULT '',
                "Video Duration (sec)" String DEFAULT '',
                "Composition Asset ID" String DEFAULT '',
                "composition_asset_id" String DEFAULT '',
                "ISWC" String DEFAULT '',
                "iswc" String DEFAULT '',
                "Writers" String DEFAULT '',
                "writers" String DEFAULT '',
                "Copyright Type" String DEFAULT '',
                "copyright_type" String DEFAULT '',
                "Content Category" String DEFAULT '',
                "content_category" String DEFAULT '',
                "Ownership Percentage" String DEFAULT '',
                "ownership_percentage" String DEFAULT ''
            ) ENGINE = StripeLog()`
        });

        await client.command({
            query: `CREATE TABLE IF NOT EXISTS ${tempChannelMapTable} (
                channel_id String,
                mapped_name String
            ) ENGINE = StripeLog()`
        });

        if (isAborted()) throw new Error("Job aborted by user");

        // 2. Load Channel Mappings
        log(`[Step 2/2] Loading Channel Mappings into ClickHouse...`);
        const channelMapRows = Array.from(channelMap.entries()).map(([channelId, mappedName]) => ({
            channel_id: channelId,
            mapped_name: mappedName
        }));
        if (channelMapRows.length > 0) {
            await client.insert({
                table: tempChannelMapTable,
                values: channelMapRows,
                format: 'JSONEachRow'
            });
        }

        if (isAborted()) throw new Error("Job aborted by user");

        // 3. Load Subscription File
        log(`[Step 2/2] Loading Subscription Raw into ClickHouse staging...`);
        await loadStagingTable(client, tempSubTable, files.subscription, isAborted);

        // 4. Query count
        const countRes = await client.query({
            query: `SELECT count() as cnt FROM ${tempSubTable}`,
            format: 'JSONEachRow'
        });
        const countRows = await countRes.json() as any[];
        const subRows = countRows[0]?.cnt ? parseInt(countRows[0].cnt, 10) : 0;
        log(`[Step 2/2] Ingesting ${subRows.toLocaleString()} subscription rows using ClickHouse SQL Join...`);

        // 5. Execute INSERT SELECT for Subscription
        const insertQuery = getSubInsertSelectQuery(tempSubTable, tempChannelMapTable, cmsId, month, usTaxRate, 'subscription');
        await client.command({
            query: insertQuery,
            query_params: { cmsId, uploadMonth: parseInt(month, 10), usTaxRate, reportType: 'subscription' }
        });

        let adjSubRows = 0;
        if (files.adj_red_label) {
            if (isAborted()) throw new Error("Job aborted by user");
            log(`[Adjustment] Loading Subscription Adjustments into ClickHouse staging...`);
            await client.command({ query: `TRUNCATE TABLE ${tempSubTable}` });
            await loadStagingTable(client, tempSubTable, files.adj_red_label, isAborted);

            const countAdjRes = await client.query({
                query: `SELECT count() as cnt FROM ${tempSubTable}`,
                format: 'JSONEachRow'
            });
            const countAdjRows = await countAdjRes.json() as any[];
            adjSubRows = countAdjRows[0]?.cnt ? parseInt(countAdjRows[0].cnt, 10) : 0;
            log(`[Adjustment] Ingesting ${adjSubRows.toLocaleString()} adjustment rows using ClickHouse SQL Join...`);

            const insertAdjQuery = getSubInsertSelectQuery(tempSubTable, tempChannelMapTable, cmsId, month, usTaxRate, 'sub_adjustment');
            await client.command({
                query: insertAdjQuery,
                query_params: { cmsId, uploadMonth: parseInt(month, 10), usTaxRate, reportType: 'sub_adjustment' }
            });
        }

        let shortsSubRows = 0;
        if (files.shorts_subs) {
            if (isAborted()) throw new Error("Job aborted by user");
            log(`[Shorts Subs] Loading Shorts Subscription into ClickHouse staging...`);
            await client.command({ query: `TRUNCATE TABLE ${tempSubTable}` });
            await loadStagingTable(client, tempSubTable, files.shorts_subs, isAborted);

            const countShortsRes = await client.query({
                query: `SELECT count() as cnt FROM ${tempSubTable}`,
                format: 'JSONEachRow'
            });
            const countShortsRows = await countShortsRes.json() as any[];
            shortsSubRows = countShortsRows[0]?.cnt ? parseInt(countShortsRows[0].cnt, 10) : 0;
            log(`[Shorts Subs] Ingesting ${shortsSubRows.toLocaleString()} Shorts subscription rows using ClickHouse SQL Join...`);

            const insertShortsQuery = getSubInsertSelectQuery(tempSubTable, tempChannelMapTable, cmsId, month, usTaxRate, 'shorts_subs');
            await client.command({
                query: insertShortsQuery,
                query_params: { cmsId, uploadMonth: parseInt(month, 10), usTaxRate, reportType: 'shorts_subs' }
            });
        }

        const totalAllRows = subRows + adjSubRows + shortsSubRows;
        return { subRows, adjSubRows, shortsSubRows, totalAllRows };

    } finally {
        log(`[Step 2/2] Cleaning up staging tables...`);
        await client.command({ query: `DROP TABLE IF EXISTS ${tempSubTable}` }).catch(() => {});
        await client.command({ query: `DROP TABLE IF EXISTS ${tempChannelMapTable}` }).catch(() => {});
        await client.command({ query: `DROP TABLE IF EXISTS ${tempAffTaxTable}` }).catch(() => {});
    }
}
