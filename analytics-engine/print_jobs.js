import { getClickHouseClient } from './dist/config/clickhouse.js';

async function run() {
    const client = getClickHouseClient({ database: 'default' });
    const res = await client.query({
        query: "SELECT job_id, month, ads_revenue, adj_ads_revenue, sub_revenue, adj_sub_revenue, total_rows, status, completed_at FROM ingestion_jobs ORDER BY completed_at DESC LIMIT 5",
        format: 'JSONEachRow'
    });
    const rows = await res.json();
    console.log(JSON.stringify(rows, null, 2));
    await client.close();
}

run().catch(err => console.error(err));
