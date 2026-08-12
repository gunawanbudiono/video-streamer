const postgres = require('postgres');

const sql = postgres('postgresql://neonvault:neonvault_secret@localhost:5432/neonvault_db');

async function run() {
    try {
        const partners = await sql`SELECT id, name, clickhouse_cms_id, youtube_content_owner_id FROM cms_partners`;
        console.log("CMS Partners in PG:", partners);
    } catch (err) {
        console.error("PG Query Error:", err);
    } finally {
        await sql.end();
    }
}

run();
