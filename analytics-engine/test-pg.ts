const { Client } = require('pg');

const client = new Client({
    connectionString: 'postgres://postgres:postgres@127.0.0.1:5432/neonvault'
});

async function run() {
    await client.connect();
    const res = await client.query(`SELECT month, status, type, total_rows, created_at FROM ingestion_jobs WHERE month = '202601' ORDER BY created_at DESC`);
    console.log(res.rows);
    await client.end();
}

run().catch(console.error);
