const { createClient } = require('@clickhouse/client');

async function testQuery() {
    const client = createClient({
        url: 'http://localhost:8123',
        username: 'default',
        password: '',
        database: 'db_LY9bm_1SvREOP8jt9cdkk'
    });

    const queryStr = `
            SELECT 
                video_id AS "Video", content_type AS "Content_type", policy AS "Policy", video_title AS "Video_Title", MAX(video_duration_sec) AS "Video_Duration", username AS "Username", uploader AS "Uploader",
                channel_display_name AS "Channel_Display_Name", channel_id AS "Channel_ID", claim_type AS "Claim_Type", claim_origin AS "Claim_Origin", multiple_claims AS "Multiple_Claims", category AS "Category",
                asset_id AS "Asset_ID", asset_labels AS "Asset_Labels", asset_channel_id AS "Asset_Channel_ID", custom_id AS "Custom_ID", isrc AS "ISRC", grid AS "GRid", upc AS "UPC", artist AS "Artist",
                asset_title AS "Asset_Title", album AS "Album", label AS "Label", SUM(owned_views) AS "Owned_Views", SUM(yt_rev_auction) AS "YouTube_Revenue_Split_Auction", SUM(yt_rev_reserved) AS "YouTube_Revenue_Split_Reserved", 
                SUM(yt_rev_partner_sold_yt_served) AS "YouTube_Revenue_Split_Partner_Sold_YouTube_Served", SUM(yt_rev_partner_sold_p_served) AS "YouTube_Revenue_Split_Partner_Sold_Partner_Served", 
                SUM(yt_rev_total) AS "YouTube_Revenue_Split", SUM(partner_rev_auction) AS "Partner_Revenue_Auction", SUM(partner_rev_reserved) AS "Partner_Revenue_Reserved", 
                SUM(partner_rev_partner_sold_yt_served) AS "Partner_Revenue_Partner_Sold_YouTube_Served", SUM(partner_rev_partner_sold_p_served) AS "Partner_Revenue_Partner_Sold_Partner_Served", 
                SUM(partner_rev_total) AS "partner_revenue"
            FROM ads_revenue_enriched
            WHERE upload_month = '202602' AND (adjustment_type = '' OR lower(adjustment_type) = 'none' OR adjustment_type IS NULL)
            GROUP BY video_id, content_type, policy, video_title, username, uploader, channel_display_name, channel_id, claim_type, claim_origin, multiple_claims, category, asset_id, asset_labels, asset_channel_id, custom_id, isrc, grid, upc, artist, asset_title, album, label
        `;

    try {
        console.log("Executing query...");
        const result = await client.query({
            query: queryStr,
            format: 'CSVWithNames'
        });
        
        const rawStream = result.stream();
        let bytes = 0;
        rawStream.on('data', (chunk) => {
            bytes += chunk.length;
        });
        rawStream.on('end', () => {
             console.log("Success! Total bytes stream:", bytes);
        });
        rawStream.on('error', (err) => {
             console.error("Stream ERROR:", err);
        });
    } catch (e) {
        console.error("Failed to query ClickHouse:", e);
    }
}
testQuery();
