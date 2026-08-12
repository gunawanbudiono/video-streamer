const { createClient } = require('@clickhouse/client');
const client = createClient({ url: 'http://localhost:8123', database: 'default', username: 'default', password: 'neonvault_ch_admin_2026' });
async function main() {
  const sql = `SELECT DISTINCT month FROM ingestion_jobs WHERE status = 'completed' AND job_type = {jobType: String} AND cms_id IN {cmsIds: Array(String)}`;
  const result = await client.query({ query: sql, query_params: { jobType: 'ads_revenue', cmsIds: ['_O7DiMnAv0nm5wJP4LKUS'] }, format: 'JSONEachRow' });
  const rows = await result.json();
  console.log(rows);
}
main();
