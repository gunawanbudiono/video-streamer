const { createClient } = require('@clickhouse/client');
const client = createClient({ host: 'http://localhost:8123' });
async function run() {
    const res = await client.query({
        query: "SELECT sum(partner_rev_total) as sum_partner, sum(yt_rev_total) as sum_yt, count() as cnt FROM db_DtIzPW10SINp5maPSwiuV.ads_revenue_enriched WHERE upload_month = 202606 GROUP BY report_type",
        format: 'JSONEachRow'
    });
    const rows = await res.json();
    console.log(JSON.stringify(rows, null, 2));
}
run().catch(err => console.error(err));
