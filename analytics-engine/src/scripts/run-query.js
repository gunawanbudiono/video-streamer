const { createClient } = require('@clickhouse/client');

async function main() {
    const client = createClient({
        url: 'http://localhost:8123',
        username: 'default',
        password: '',
        database: 'default'
    });

    try {
        console.log("=== INGESTION JOBS SCHEMA ===");
        const res = await client.query({
            query: "DESCRIBE TABLE ingestion_jobs",
            format: 'JSONEachRow'
        });
        const rows = await res.json();
        console.log(JSON.stringify(rows, null, 2));
    } catch (e) {
        console.error("Error:", e);
    } finally {
        await client.close();
    }
}

main();
