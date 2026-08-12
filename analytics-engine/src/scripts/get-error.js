const { createClient } = require('@clickhouse/client');

async function main() {
    const client = createClient({
        url: 'http://localhost:8123',
        username: 'default',
        password: '',
        database: 'default'
    });

    try {
        const res = await client.query({
            query: "SELECT error_message FROM default.ingestion_jobs WHERE status = 'failed' ORDER BY updated_at DESC LIMIT 1",
            format: 'JSONEachRow'
        });
        const rows = await res.json();
        if (rows.length > 0) {
            console.log("=== LATEST FAILED JOB ERROR ===");
            console.log(rows[0].error_message);
        } else {
            console.log("No failed jobs found in default.ingestion_jobs.");
        }
    } catch (e) {
        console.error("Error querying ClickHouse:", e);
    } finally {
        await client.close();
    }
}

main();
