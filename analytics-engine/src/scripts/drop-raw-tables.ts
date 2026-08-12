import { getDefaultClient } from '../config/clickhouse.js';

async function main() {
    const defaultClient = getDefaultClient();

    try {
        // 1. Get all tenant databases
        const dbRes = await defaultClient.query({
            query: `SELECT name FROM system.databases WHERE name LIKE 'db_%'`,
            format: 'JSONEachRow'
        });
        const dbs = (await dbRes.json()) as { name: string }[];
        console.log(`Found ${dbs.length} tenant databases.`);

        let totalSavedBytes = 0;

        for (const db of dbs) {
            const dbName = db.name;
            console.log(`\n--- Processing database: ${dbName} ---`);

            // 2. Query size of tables to be dropped
            const sizeRes = await defaultClient.query({
                query: `
                    SELECT 
                        table,
                        sum(data_compressed_bytes) AS compressed_bytes
                    FROM system.parts
                    WHERE database = {dbName: String} 
                      AND table IN ('youtube_raw_estimated_revenue', 'youtube_raw_channel_estimated_revenue')
                      AND active
                    GROUP BY table
                `,
                query_params: { dbName },
                format: 'JSONEachRow'
            });
            const sizes = (await sizeRes.json()) as { table: string; compressed_bytes: string }[];
            
            for (const s of sizes) {
                const bytes = parseInt(s.compressed_bytes, 10) || 0;
                totalSavedBytes += bytes;
                console.log(`Table ${s.table}: ${(bytes / 1024 / 1024).toFixed(2)} MB (compressed)`);
            }

            // 3. Drop tables
            console.log(`Dropping youtube_raw_estimated_revenue...`);
            await defaultClient.command({
                query: `DROP TABLE IF EXISTS ${dbName}.youtube_raw_estimated_revenue`
            });

            console.log(`Dropping youtube_raw_channel_estimated_revenue...`);
            await defaultClient.command({
                query: `DROP TABLE IF EXISTS ${dbName}.youtube_raw_channel_estimated_revenue`
            });

            console.log(`✓ Raw tables dropped successfully for ${dbName}`);
        }

        console.log(`\n=========================================`);
        console.log(`⚡ Cleanup complete!`);
        console.log(`Total disk space reclaimed: ${(totalSavedBytes / 1024 / 1024 / 1024).toFixed(2)} GB`);
        console.log(`=========================================`);

    } catch (err) {
        console.error("Error during cleanup:", err);
    }
}

main();
