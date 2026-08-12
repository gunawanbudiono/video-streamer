const { Client } = require('pg');

async function main() {
    const pg = new Client({
        connectionString: 'postgresql://neondb_owner:npg_8VjJdrK6szvi@ep-polished-band-a1qgq5bg-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require'
    });

    try {
        await pg.connect();
        const res = await pg.query('SELECT id, name, cms_id FROM partners');
        console.log("Partners in Postgres:", JSON.stringify(res.rows, null, 2));
    } catch (e) {
        console.error("Error connecting to pg:", e);
    } finally {
        await pg.end();
    }
}

main();
