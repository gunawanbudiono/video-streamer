import { getDefaultClient } from '../config/clickhouse.js';

async function main() {
    const defaultClient = getDefaultClient();
    console.log("Running ClickHouse mutation to map historical empty asset_ids to metadata via INSERT+DELETE...");
    
    const db = "db_MCmf4OH49HkyPyuNAA91Ew";
    const columns = [
        'cms_id', 'day', 'country', 'video_id', 'channel_id', 'owner_channel_id',
        'asset_type', 'content_type', 'creator_content_type',
        'claim_type', 'policy', 'claim_origin', 'isrc', 'upc', 'grid',
        'video_title', 'username', 'uploader', 'video_duration_sec',
        'channel_display_name', 'multiple_claims', 'category', 'asset_labels',
        'artist', 'asset_title', 'album', 'label', 'owned_views',
        'yt_rev_auction', 'yt_rev_reserved', 'yt_rev_partner_sold_yt_served',
        'yt_rev_partner_sold_p_served', 'yt_rev_red', 'yt_rev_total',
        'partner_rev_auction', 'partner_rev_reserved', 'partner_rev_partner_sold_yt_served',
        'partner_rev_partner_sold_p_served', 'partner_rev_red', 'partner_rev_total',
        'monetized_playbacks', 'ad_impressions', 'partner_rev_transaction',
        'claimed_status', 'claim_status', 'uploader_type', 'video_upload_date',
        'genre', 'estimated_cpm', 'estimated_playback_based_cpm', 'likes',
        'comments', 'shares', 'dislikes', 'watch_time_minutes',
        'average_view_duration_seconds', 'average_view_duration_percentage',
        'subscribers_gained', 'subscribers_lost', 'custom_id'
    ];

    const insertCols = ['asset_id', 'ingested_at', ...columns].join(', ');
    const qualifiedCols = columns.map(col => `estimated_revenue_daily.${col}`);
    const selectCols = [
        "coalesce(nullIf(V.mapped_asset_id, ''), 'UNCLAIMED_VIDEO') AS asset_id",
        "now() as ingested_at",
        ...qualifiedCols
    ].join(', ');

    const insertQuery = `
        INSERT INTO ${db}.estimated_revenue_daily (${insertCols})
        SELECT ${selectCols}
        FROM ${db}.estimated_revenue_daily
        LEFT JOIN (
            SELECT 
                video_id,
                argMax(asset_id, day) AS mapped_asset_id
            FROM ${db}.youtube_video_metadata
            WHERE cms_id = 'MCmf4OH49HkyPyuNAA91Ew' AND asset_id != ''
            GROUP BY video_id
        ) V ON estimated_revenue_daily.video_id = V.video_id
        WHERE estimated_revenue_daily.cms_id = 'MCmf4OH49HkyPyuNAA91Ew'
          AND estimated_revenue_daily.video_id != ''
          AND estimated_revenue_daily.asset_id = ''
    `;

    const deleteQuery = `
        ALTER TABLE ${db}.estimated_revenue_daily
        DELETE
        WHERE cms_id = 'MCmf4OH49HkyPyuNAA91Ew'
          AND video_id != ''
          AND asset_id = ''
        SETTINGS mutations_sync = 1
    `;

    try {
        console.log("Step 1: Inserting mapped rows back into estimated_revenue_daily...");
        await defaultClient.command({ query: insertQuery });
        console.log("Step 1 complete! Mapped rows inserted.");

        console.log("Step 2: Deleting old unmapped rows...");
        await defaultClient.command({ query: deleteQuery });
        console.log("Step 2 complete! Old unmapped rows deleted.");
        
        console.log("ClickHouse migration finished successfully!");
    } catch (err: any) {
        console.error("ClickHouse migration failed:", err.message);
    }
}

main();
