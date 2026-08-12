import Fastify from 'fastify';
import cors from '@fastify/cors';
import multipart from '@fastify/multipart';
import { config } from './config/index.js';
import { initTenants } from './config/tenants.js';
import { closeAllClients, getDefaultClient, getClickHouseClient } from './config/clickhouse.js';
import { GLOBAL_DDL, GLOBAL_MIGRATIONS, CMS_DDL, CMS_MIGRATIONS } from './db/ddl.js';
import { healthRoutes } from './routes/health.js';
import { revenueRoutes } from './routes/revenue.js';
import { analyticsRoutes } from './routes/analytics.js';
import { adminRoutes } from './routes/admin.js';
import { ingestRoutes } from './routes/ingest.js';
import { resolutionRoutes } from './routes/resolution.js';
import { frozenRoutes } from './routes/frozen.js';
import { readFileSync } from 'fs';
import { resolve } from 'path';
import { createHash } from 'crypto';

async function main() {
    const app = Fastify({
        bodyLimit: 4 * 1024 * 1024 * 1024, // 4 GB
        logger: {
            level: config.logLevel,
            transport: {
                target: 'pino-pretty',
                options: { colorize: true },
            },
        },
    });

    // ── Plugins ─────────────────────────────────────────────
    await app.register(cors, {
        origin: true,
    });
    await app.register(multipart, {
        limits: {
            fileSize: 4 * 1024 * 1024 * 1024, // 4 GB per file
            files: 10,
        },
    });

    // ── Global Error Handler ────────────────────────────────
    app.setErrorHandler((error: Error & { statusCode?: number }, _request, reply) => {
        const statusCode = error.statusCode || 500;
        app.log.error({
            err: error,
            msg: error.message,
            stack: error.stack,
        });

        // ClickHouse errors
        if (error.message?.includes('UNKNOWN_TABLE') || error.message?.includes('Table')) {
            return reply.code(500).send({
                error: 'Database table not found. CMS may not be initialized.',
                detail: error.message,
            });
        }

        return reply.code(statusCode).send({
            error: statusCode >= 500 ? 'Internal server error' : error.message,
            ...(config.logLevel === 'debug' ? { detail: error.message } : {}),
        });
    });

    // ── Routes ──────────────────────────────────────────────
    await app.register(healthRoutes);
    await app.register(revenueRoutes);
    await app.register(analyticsRoutes);
    await app.register(adminRoutes);
    await app.register(ingestRoutes);
    await app.register(resolutionRoutes);
    await app.register(frozenRoutes);

    // ── Admin UI (static) ───────────────────────────────────
    const adminHtmlPath = resolve(process.cwd(), 'src', 'admin-ui', 'index.html');
    app.get('/admin', async (_req, reply) => {
        try {
            const html = readFileSync(adminHtmlPath, 'utf-8');
            reply.type('text/html').send(html);
        } catch {
            reply.code(404).send({ error: 'Admin UI not found' });
        }
    });

    // ── Initialize DB & tenants ─────────────────────────────
    try {
        const defaultClient = getDefaultClient();
        for (const query of GLOBAL_DDL) {
            await defaultClient.command({ query });
        }
        console.log('✅ Global DDL applied');
        // Run migrations for existing tables
        for (const migration of GLOBAL_MIGRATIONS) {
            await defaultClient.command({ query: migration });
        }
        console.log('✅ Global migrations applied');

        // ── Seed / Sync Organization API Key ───────────────────
        try {
            const envOrgKey = process.env.ANALYTICS_ORG_KEY || 'org_f813e59c-4427-4da0-9f07-f6864ee64342';
            const envOrgKeyHash = createHash('sha256').update(envOrgKey).digest('hex');
            
            // Check if this specific key hash is already registered
            const orgCheck = await defaultClient.query({
                query: `SELECT 1 FROM org_registry FINAL WHERE org_key_hash = {hash: String} AND is_active = 1`,
                query_params: { hash: envOrgKeyHash },
                format: 'JSONEachRow'
            });
            const orgRows = await orgCheck.json<Array<any>>();
            
            if (orgRows.length === 0) {
                // Register/Seed this key
                await defaultClient.insert({
                    table: 'org_registry',
                    values: [{
                        org_id: 'default_org',
                        org_name: 'Default Organization',
                        org_key_hash: envOrgKeyHash,
                        is_active: 1
                    }],
                    format: 'JSONEachRow'
                });
                console.log(`✅ Seeded environment organization key into ClickHouse (Hash: ${envOrgKeyHash.slice(0, 10)}...)`);
            }
        } catch (orgErr: any) {
            console.warn('⚠️ Warning: Failed to seed default organization key into ClickHouse:', orgErr.message);
        }

        // ── Seed / Sync default CMS registry ─────────────────────
        try {
            const defaultCmsList = ['LY9bm_1SvREOP8jt9cdkk', 'fZ__ofqGWo6WYQ5MV4yzv', 'asad'];
            for (const cmsId of defaultCmsList) {
                const cmsCheck = await defaultClient.query({
                    query: `SELECT 1 FROM cms_registry FINAL WHERE cms_id = {cmsId: String} AND is_active = 1`,
                    query_params: { cmsId },
                    format: 'JSONEachRow'
                });
                const cmsRows = await cmsCheck.json<Array<any>>();
                if (cmsRows.length === 0) {
                    const dbName = `db_${cmsId.replace(/-/g, '_')}`;
                    const apiKeyHash = createHash('sha256').update(cmsId).digest('hex'); // simple fallback hash
                    await defaultClient.insert({
                        table: 'cms_registry',
                        values: [{
                            cms_id: cmsId,
                            cms_name: `CMS ${cmsId}`,
                            db_name: dbName,
                            api_key_hash: apiKeyHash,
                            org_id: 'default_org',
                            is_active: 1
                        }],
                        format: 'JSONEachRow'
                    });
                    console.log(`✅ Seeded default CMS registry for ${cmsId}`);
                    
                    // Also make sure the database and tables are created!
                    await defaultClient.command({ query: `CREATE DATABASE IF NOT EXISTS ${dbName}` });
                    const cmsClient = getClickHouseClient({ database: dbName });
                    for (const ddl of CMS_DDL) {
                        await cmsClient.command({ query: ddl });
                    }
                    console.log(`✅ Provisioned DB and DDL for CMS ${cmsId}`);
                }
            }
        } catch (cmsErr: any) {
            console.warn('⚠️ Warning: Failed to seed default CMS registry into ClickHouse:', cmsErr.message);
        }

        // ── Auto-provision DDL updates to ALL registered CMS databases ─────────────────
        try {
            const registryRes = await defaultClient.query({
                query: `SELECT DISTINCT db_name FROM cms_registry FINAL WHERE is_active = 1`,
                format: 'JSONEachRow'
            });
            const registry = await registryRes.json() as { db_name: string }[];
            for (const reg of registry) {
                if (!reg.db_name) continue;
                await defaultClient.command({ query: `CREATE DATABASE IF NOT EXISTS ${reg.db_name}` });
                const cmsClient = getClickHouseClient({ database: reg.db_name });
                for (const ddl of CMS_DDL) {
                    await cmsClient.command({ query: ddl }).catch((ddlErr) => {
                        console.warn(`[CMS DDL Startup] Warning running DDL on ${reg.db_name}:`, ddlErr.message);
                    });
                }
                
                // Run CMS migrations for altered tables
                for (const migration of CMS_MIGRATIONS) {
                    await cmsClient.command({ query: migration }).catch((migrationErr) => {
                        console.warn(`[CMS Migration Startup] Warning running migration on ${reg.db_name}:`, migrationErr.message);
                    });
                }
            }
            console.log('✅ Auto-applied CMS DDL and migrations to all registered tenant databases');

            // ── Clean up any leaked temporary tables (Startup & Every 12 Hours) ────────────
            const cleanLeakedTempTables = async () => {
                try {
                    console.log('🧹 Scanning for leaked temporary tables (older than 2 hours) to clean up...');
                    for (const reg of registry) {
                        if (!reg.db_name) continue;
                        // Hanya hapus tabel temp yang usianya > 2 jam (menghindari menghapus tabel dari job yang sedang berjalan)
                        const tempTablesRes = await defaultClient.query({
                            query: `
                                SELECT name 
                                FROM system.tables 
                                WHERE database = {db: String} 
                                  AND name LIKE 'temp_%' 
                                  AND metadata_modification_time < subtractHours(now(), 2)
                            `,
                            query_params: { db: reg.db_name },
                            format: 'JSONEachRow'
                        });
                        const tempTables = await tempTablesRes.json() as { name: string }[];
                        for (const tempTable of tempTables) {
                            await defaultClient.command({ query: `DROP TABLE IF EXISTS ${reg.db_name}.${tempTable.name}` });
                            console.log(`🧹 Cleaned up leaked temporary table: ${reg.db_name}.${tempTable.name}`);
                        }
                    }
                } catch (cleanupErr: any) {
                    console.warn('⚠️ Warning: Failed to clean up leaked temporary tables:', cleanupErr.message);
                }
            };

            // Jalankan saat startup
            await cleanLeakedTempTables();

            // Jalankan secara berkala setiap 12 jam
            setInterval(cleanLeakedTempTables, 12 * 60 * 60 * 1000);

        } catch (syncDdlErr: any) {
            console.warn('⚠️ Warning: Failed to sync CMS DDL updates:', syncDdlErr.message);
        }
    } catch (err) {
        app.log.error(err, 'Failed to apply Global DDL');
    }
    initTenants();

    // ── Start server ────────────────────────────────────────
    try {
        await app.listen({ port: config.port, host: config.host });
        console.log(`\n🚀 Analytics Engine running at http://${config.host}:${config.port}`);
        console.log(`📊 Health check: http://${config.host}:${config.port}/health`);
        console.log(`⚙️  Admin UI: http://${config.host}:${config.port}/admin`);
        console.log(`📋 Endpoints: revenue, views, analytics, admin, ingest\n`);
    } catch (err) {
        app.log.error(err);
        process.exit(1);
    }

    // ── Graceful shutdown ───────────────────────────────────
    const shutdown = async (signal: string) => {
        console.log(`\n${signal} received. Shutting down...`);
        await app.close();
        await closeAllClients();
        process.exit(0);
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));

    // ── Catch unhandled errors ───────────────────────────────
    process.on('uncaughtException', (err) => {
        console.error('[FATAL] Uncaught exception:', err);
        shutdown('uncaughtException');
    });
    process.on('unhandledRejection', (reason) => {
        console.error('[FATAL] Unhandled rejection:', reason);
    });
}

main();
