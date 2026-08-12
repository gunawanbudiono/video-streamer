const { createClient } = require('@clickhouse/client');

async function main() {
    const client = createClient({
        url: 'http://localhost:8123',
        username: 'default',
        password: '',
        database: 'db_w3UR4sfdICvy3h75hKBGk'
    });

    try {
        console.log("=========================================");
        console.log("1. ADS REVENUE SAMPLE ROWS (ads_revenue_enriched)");
        console.log("=========================================");

        // Fetch non-shorts ads sample
        const adsRegularRes = await client.query({
            query: "SELECT day, country, video_id, asset_id, partner_rev_total, report_type FROM ads_revenue_enriched WHERE upload_month = 202606 AND report_type = 'claim_raw' LIMIT 2",
            format: 'JSONEachRow'
        });
        const adsRegular = await adsRegularRes.json();
        console.log("\n>>> Non-Shorts Ads (Regular Video Claims):");
        console.log(JSON.stringify(adsRegular, null, 2));

        // Fetch shorts ads sample
        const adsShortsRes = await client.query({
            query: "SELECT day, country, video_id, asset_id, partner_rev_total, report_type FROM ads_revenue_enriched WHERE upload_month = 202606 AND report_type = 'shorts_ads' LIMIT 2",
            format: 'JSONEachRow'
        });
        const adsShorts = await adsShortsRes.json();
        console.log("\n>>> Shorts Ads:");
        console.log(JSON.stringify(adsShorts, null, 2));

        console.log("\n=========================================");
        console.log("2. SUBSCRIPTION REVENUE SAMPLE ROWS (subscription_revenue)");
        console.log("=========================================");

        // Fetch non-shorts sub sample
        const subRegularRes = await client.query({
            query: "SELECT day, country, video_id, asset_id, partner_rev_total, claim_type FROM subscription_revenue WHERE upload_month = 202606 AND claim_type != '' LIMIT 2",
            format: 'JSONEachRow'
        });
        const subRegular = await subRegularRes.json();
        console.log("\n>>> Non-Shorts Subscription (Regular Premium):");
        console.log(JSON.stringify(subRegular, null, 2));

        // Fetch shorts sub sample
        const subShortsRes = await client.query({
            query: "SELECT day, country, video_id, asset_id, partner_rev_total, claim_type FROM subscription_revenue WHERE upload_month = 202606 AND claim_type = '' LIMIT 2",
            format: 'JSONEachRow'
        });
        const subShorts = await subShortsRes.json();
        console.log("\n>>> Shorts Subscription (Shorts Premium):");
        console.log(JSON.stringify(subShorts, null, 2));

    } catch (e) {
        console.error("Error querying ClickHouse:", e);
    } finally {
        await client.close();
    }
}

main();
