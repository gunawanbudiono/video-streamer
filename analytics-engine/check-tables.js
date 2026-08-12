const { createClient } = require('@clickhouse/client');

async function run() {
    const client = createClient({
        url: 'http://localhost:8123',
        username: 'default',
        password: '',
        database: 'default'
    });

    try {
        console.log("=== DATABASES ===");
        const dbRes = await client.query({
            query: 'SHOW DATABASES',
            format: 'JSONEachRow'
        });
        const dbs = await dbRes.json();
        console.log(dbs.map(d => d.name));

        for (const db of dbs) {
            const name = db.name;
            if (name === 'system' || name === 'INFORMATION_SCHEMA' || name === 'information_schema') continue;
            
            console.log(`\n=== TABLES IN ${name} ===`);
            const tableRes = await client.query({
                query: `SHOW TABLES FROM ${name}`,
                format: 'JSONEachRow'
            });
            const tables = await tableRes.json();
            console.log(tables.map(t => t.name));
        }
    } catch (err) {
        console.error("Error checking ClickHouse:", err);
    } finally {
        await client.close();
    }
}

run();
