import { getDefaultClient } from '../config/clickhouse.js';

async function main() {
    const defaultClient = getDefaultClient();
    const dbName = 'db_cms_demo'; // Change to any existing DB

    const query = `
        SELECT 
            table,
            sum(rows) AS total_rows,
            formatReadableSize(sum(data_uncompressed_bytes)) AS data_uncompressed,
            formatReadableSize(sum(data_compressed_bytes)) AS data_compressed
        FROM system.parts
        WHERE database = '${dbName}' AND active
        GROUP BY table
        ORDER BY table
    `;

    try {
        const result = await defaultClient.query({
            query,
            format: 'JSONEachRow'
        });
        const rows = await result.json();
        console.log(`Metrics for ${dbName}:`);
        console.table(rows);
    } catch (err) {
        console.error("Error:", err);
    }
}

main();
