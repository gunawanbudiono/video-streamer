const { createClient } = require('@clickhouse/client');

async function main() {
    const client = createClient({
        url: 'http://localhost:8123',
        username: 'default',
        password: '',
        database: 'db_w3UR4sfdICvy3h75hKBGk'
    });

    try {
        console.log("=== SUBSCRIPTION REVENUE BY CONTENT_TYPE ===");
        const res = await client.query({
            query: "SELECT content_type, sum(partner_rev_total) as raw_rev, sum(net_revenue) as net_rev, count() as rows FROM subscription_revenue WHERE upload_month = 202606 GROUP BY content_type",
            format: 'JSONEachRow'
        });
        const rows = await res.json();
        console.log(JSON.stringify(rows, null, 2));

        console.log("\n=== SUBSCRIPTION REVENUE BY CLAIM_TYPE ===");
        const res2 = await client.query({
            query: "SELECT claim_type, sum(partner_rev_total) as raw_rev, count() as rows FROM subscription_revenue WHERE upload_month = 202606 GROUP BY claim_type",
            format: 'JSONEachRow'
        });
        const rows2 = await res2.json();
        console.log(JSON.stringify(rows2, null, 2));
    } catch (e) {
        console.error("Error querying ClickHouse:", e);
    } finally {
        await client.close();
    }
}

main();
