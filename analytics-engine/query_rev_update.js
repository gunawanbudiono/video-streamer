const { createClient } = require('@clickhouse/client');
const client = createClient({ host: 'http://localhost:8123' });
async function run() {
    console.log("Updating job metadata in ClickHouse...");
    await client.command({
        query: "ALTER TABLE default.ingestion_jobs UPDATE ads_revenue = 467003.4577233657, adj_ads_revenue = 1198.5706370862, sub_revenue = 22926.6857680226, adj_sub_revenue = 119.8989323609 WHERE job_id = '3111f8f5-bc11-40a6-817c-7eee861e5b9e'",
        clickhouse_settings: {
            mutations_sync: '1'
        }
    });
    console.log("Job metadata updated successfully!");
}
run().catch(err => console.error(err));
