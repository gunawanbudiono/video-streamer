const { createClient } = require('@clickhouse/client');

async function main() {
    const ch = createClient({
        url: 'http://localhost:8123',
        username: 'default',
        password: '',
        database: 'default'
    });

    try {
        console.log("Checking ingested_files_archive table entries:");
        const r1 = await ch.query({
            query: `
                SELECT cms_id, upload_month, file_type, file_name, file_size
                FROM ingested_files_archive
                LIMIT 50
            `,
            format: 'JSONEachRow'
        });
        const rows1 = await r1.json();
        console.log("ingested_files_archive:", JSON.stringify(rows1, null, 2));

    } catch (e) {
        console.error("Error running script:", e);
    } finally {
        await ch.close();
    }
}

main();
