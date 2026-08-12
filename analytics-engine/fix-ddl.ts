import { createClient } from '@clickhouse/client';
import { CMS_DDL, CMS_MIGRATIONS } from './src/db/ddl.js';

async function run() {
    const defaultHost = process.env.CLICKHOUSE_HOST || 'http://127.0.0.1:8123';
    
    // Connect to default database
    const defaultClient = createClient({
        url: defaultHost,
        database: 'default'
    });

    let cmsIds: string[] = [];
    try {
        console.log("Fetching active CMS databases from cms_registry...");
        const result = await defaultClient.query({
            query: 'SELECT cms_id FROM cms_registry WHERE is_active = 1',
            format: 'JSONEachRow'
        });
        const rows = await result.json<{ cms_id: string }>();
        cmsIds = rows.map(r => r.cms_id);
        if (cmsIds.length === 0) {
            console.log("Registry is empty, using fallback static list.");
            cmsIds = ['LY9bm_1SvREOP8jt9cdkk', 'fZ__ofqGWo6WYQ5MV4yzv', 'asad'];
        } else {
            console.log(`Found active CMS databases from registry: ${cmsIds.join(', ')}`);
        }
    } catch (err: any) {
        console.warn(`⚠️ Failed to fetch CMS registry, falling back to static list. Error:`, err.message);
        cmsIds = ['LY9bm_1SvREOP8jt9cdkk', 'fZ__ofqGWo6WYQ5MV4yzv', 'asad']; // Fallback to active production databases
    } finally {
        await defaultClient.close();
    }

    for (const cms of cmsIds) {
        const dbName = `db_${cms}`;
        console.log(`Fixing database: ${dbName}`);

        // Connect to default to create database if it doesn't exist
        const dbClient = createClient({
            url: defaultHost,
            database: 'default'
        });
        await dbClient.command({ query: `CREATE DATABASE IF NOT EXISTS ${dbName}` });
        await dbClient.close();

        const client = createClient({
            url: defaultHost,
            database: dbName
        });

        // Migration logic: Rename ads_revenue_estimated to estimated_revenue_daily
        try {
            const checkOld = await client.query({
                query: `EXISTS TABLE ads_revenue_estimated`,
                format: 'JSONEachRow'
            });
            const oldExists = (await checkOld.json<any>())[0]?.result === 1;

            const checkNew = await client.query({
                query: `EXISTS TABLE estimated_revenue_daily`,
                format: 'JSONEachRow'
            });
            const newExists = (await checkNew.json<any>())[0]?.result === 1;

            if (oldExists && !newExists) {
                console.log(`🔄 [Migration] Renaming ads_revenue_estimated to estimated_revenue_daily in ${dbName}...`);
                // Drop Materialized View first
                await client.command({ query: `DROP VIEW IF EXISTS v_mv_asset_performance_daily` });
                // Rename table
                await client.command({ query: `RENAME TABLE ads_revenue_estimated TO estimated_revenue_daily` });
                console.log(`✅ [Migration] Successfully renamed table to estimated_revenue_daily.`);
            } else if (oldExists && newExists) {
                console.warn(`⚠️ Both ads_revenue_estimated and estimated_revenue_daily exist in ${dbName}. Dropping old table.`);
                await client.command({ query: `DROP VIEW IF EXISTS v_mv_asset_performance_daily` });
                await client.command({ query: `DROP TABLE IF EXISTS ads_revenue_estimated` });
            } else {
                // Drop MV unconditionally so that DDL execution recreates it pointing to estimated_revenue_daily
                await client.command({ query: `DROP VIEW IF EXISTS v_mv_asset_performance_daily` });
            }
        } catch (err: any) {
            console.warn(`⚠️ Migration checks or MV drop failed for ${dbName}: ${err.message}`);
        }

        for (const ddl of CMS_DDL) {
            try {
                await client.command({ query: ddl });
                console.log(`✅ Applied DDL successfully.`);
            } catch (err: any) {
                console.error(`❌ Failed DDL:`, err.message);
            }
        }
        for (const migration of CMS_MIGRATIONS) {
            try {
                await client.command({ query: migration });
                console.log(`✅ Applied migration successfully.`);
            } catch (err: any) {
                console.error(`❌ Failed migration:`, err.message);
            }
        }
        await client.close();
    }
    console.log("Done.");
}

run().catch(console.error);
