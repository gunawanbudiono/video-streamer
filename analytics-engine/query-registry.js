import { getClickHouseClient } from './dist/config/clickhouse.js';

async function run() {
    const client = getClickHouseClient({ database: 'default' });
    try {
        const res1 = await client.query({
            query: "SELECT * FROM org_registry FINAL",
            format: 'JSONEachRow'
        });
        const orgs = await res1.json();
        console.log("ORGS:", orgs);

        const res2 = await client.query({
            query: "SELECT * FROM cms_registry FINAL",
            format: 'JSONEachRow'
        });
        const cms = await res2.json();
        console.log("CMS_REGISTRY:", cms);
    } catch (err) {
        console.error("ERROR:", err);
    } finally {
        await client.close();
    }
}
run();
