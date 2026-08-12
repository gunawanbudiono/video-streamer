import { getDefaultClient } from './src/config/clickhouse';

async function run() {
    console.log("Adding column detail_logs to ingestion_jobs...");
    const client = getDefaultClient();
    try {
        await client.command({
            query: `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS detail_logs String DEFAULT '[]'`
        });
        console.log("✅ Column added successfully.");
    } catch (e) {
        console.error("❌ Failed to add column:", e);
    }
    process.exit(0);
}

run();
