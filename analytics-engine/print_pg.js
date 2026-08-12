import postgres from 'postgres';
import { config } from './dist/config/index.js';

async function run() {
    const sql = postgres(config.postgresUrl);
    try {
        const rows = await sql`SELECT * FROM youtube_channel_mappings`;
        console.log("ROWS:");
        console.log(rows);
    } catch (e) {
        console.error("Error:", e.message);
    }
    await sql.end();
}

run().catch(err => console.error(err));
