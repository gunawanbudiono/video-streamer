import type { FastifyInstance } from 'fastify';
import { authMiddleware } from '../middleware/auth.js';
import { parseClaimSummary, parseAssetSummary, extractFileStream, detectDelimiterFromFile, runAdsIngestionDirect, ingestAffiliateTaxTable } from '../workers/etl-ads.js';
import { runSubscriptionIngestionDirect } from '../workers/ingest-subscription.js';
import { runPaidFeaturesIngestionDirect } from '../workers/etl-paid.js';
import { runAudioTierIngestionDirect } from '../workers/etl-audio-tier.js';
import { runPublisherUsageIngestionDirect } from '../workers/etl-publisher-usage.js';
import { processEstimatedAds, processVideoReach, processVideoDemographics, processVideoTrafficSources, processVideoDevices, processDailySubscribers, processDailyInteractions, processVideoCountry } from '../workers/etl-estimated.js';
import { reconcileNullLabels } from '../workers/reconcile.js';
import { getClickHouseClient, getDefaultClient } from '../config/clickhouse.js';
import { getTenantById, sanitizeDbName } from '../config/tenants.js';
import { initJobLog, emitLog, completeJobLog, subscribeJobLog, getJobLogs } from '../utils/job-logger.js';
import { createHash } from 'crypto';
import { CMS_DDL } from '../db/ddl.js';
import { v4 as uuidv4 } from 'uuid';
import { mkdirSync, existsSync, unlinkSync, statSync, copyFileSync, readFileSync, readdirSync } from 'fs';
import { join } from 'path';
import { tmpdir } from 'os';

// Temp directory for uploaded files
const UPLOAD_DIR = join(tmpdir(), 'analytics-uploads');
if (!existsSync(UPLOAD_DIR)) mkdirSync(UPLOAD_DIR, { recursive: true });

/** Global map to track aborted job streams */
const activeJobTokens = new Set<string>();
/** Global set to track currently running jobs (to detect orphans after restart) */
const runningJobs = new Set<string>();
/** Global map to track active concurrency locks (key: lockKey -> value: jobId) */
const activeLocks = new Map<string, string>();

/** Clean up temp files */
function cleanupFiles(paths: string[]) {
    for (const p of paths) {
        try { unlinkSync(p); } catch { /* ignore */ }
    }
}

// Permanent archive directory for uploaded raw files
const ARCHIVE_DIR = join(process.cwd(), 'storage', 'raw-reports-archive');
if (!existsSync(ARCHIVE_DIR)) mkdirSync(ARCHIVE_DIR, { recursive: true });

/** Helper to archive uploaded file and save path to ClickHouse */
async function archiveUploadedFile(opts: {
    cmsId: string;
    month: string;
    fileType: string;
    tempPath: string;
    originalFilename: string;
    log: (msg: string) => void;
}) {
    const { cmsId, month, fileType, tempPath, originalFilename, log } = opts;
    try {
        const archiveSubDir = join(ARCHIVE_DIR, `cms_${cmsId.replace(/-/g, '_')}`, String(month));
        if (!existsSync(archiveSubDir)) mkdirSync(archiveSubDir, { recursive: true });

        const destPath = join(archiveSubDir, `${fileType}_${originalFilename}`);
        copyFileSync(tempPath, destPath);

        const defaultCh = getDefaultClient();
        await defaultCh.insert({
            table: 'default.ingested_files_archive',
            values: [{
                cms_id: cmsId,
                upload_month: parseInt(month, 10),
                file_type: fileType,
                file_name: originalFilename,
                file_path: destPath,
                file_size: statSync(destPath).size,
                uploaded_at: new Date().toISOString().slice(0, 19).replace('T', ' ')
            }],
            format: 'JSONEachRow'
        });

        log(`[Archive] Successfully archived ${fileType} to ${destPath}`);
    } catch (err: any) {
        log(`[Archive Warning] Failed to archive ${fileType}: ${err.message}`);
        console.error('[Archive Error]', err);
    }
}


/** Update job status in global ClickHouse (Fetch latest, merge, insert) */
async function updateJob(jobId: string, updates: Record<string, unknown>) {
    const client = getDefaultClient();

    // Fetch existing latest state to avoid clearing out fields not present in 'updates'
    let existing = {};
    try {
        const res = await client.query({
            query: `SELECT * FROM ingestion_jobs WHERE job_id = {jobId: String} ORDER BY updated_at DESC, indexOf(['pending', 'checking', 'processing', 'failed', 'completed'], status) DESC, length(detail_logs) DESC LIMIT 1`,
            query_params: { jobId },
            format: 'JSONEachRow'
        });
        const rows = await res.json();
        if ((rows as any[]).length > 0) {
            existing = (rows as any[])[0];
            // Remove the auto-assigned fields from existing that shouldn't just mindlessly override if we want fresh timestamps, 
            // but we DO want to preserve started_at. completed_at should be preserved unless overwritten.
        }
    } catch (err) {
        // Ignores if table doesn't exist yet or similar
    }

    const existingRow = existing as Record<string, any>;

    // Automatically set completed_at if status becomes completed/failed and it hasn't been set yet
    if ((updates.status === 'completed' || updates.status === 'failed') && !updates.completed_at && !existingRow.completed_at) {
        updates.completed_at = new Date().toISOString();
    }

    // Clear old error message if we are restarting/rerunning a completed/failed job
    if (updates.status === 'processing' && (existingRow.status === 'completed' || existingRow.status === 'failed')) {
        updates.error_message = updates.error_message || '';
    }

    const msgUpdate = updates.error_message as string | undefined;
    const msgExisting = existingRow.error_message as string | undefined;

    if (msgUpdate) {
        const rangeLine = msgUpdate.split('\n').find(line => line.startsWith("Sync range:")) ||
                          (msgExisting ? msgExisting.split('\n').find(line => line.startsWith("Sync range:")) : undefined);

        if (rangeLine && !msgUpdate.includes("Sync range:")) {
            updates.error_message = `${rangeLine}\n${msgUpdate}`;
        }
    }

    // Clean up undefined updates to avoid destroying existing values
    const cleanUpdates: Record<string, unknown> = {};
    for (const [key, val] of Object.entries(updates)) {
        if (val !== undefined) {
            cleanUpdates[key] = val;
        }
    }

    const merged = { ...existingRow, ...cleanUpdates };

    // Set updated_at and calculate total_rows dynamically to prevent destructive overwriting
    merged.updated_at = new Date().toISOString().replace('T', ' ').slice(0, 19);

    // Format started_at and completed_at to ClickHouse expected YYYY-MM-DD HH:mm:ss format
    if (merged.started_at) {
        try {
            const cleanStr = merged.started_at.includes('T') || merged.started_at.includes('Z')
                ? merged.started_at
                : merged.started_at.replace(' ', 'T') + 'Z';
            const d = new Date(cleanStr);
            if (!isNaN(d.getTime())) {
                merged.started_at = d.toISOString().replace('T', ' ').slice(0, 19);
            }
        } catch (e) {}
    }
    if (merged.completed_at !== undefined && merged.completed_at !== null) {
        try {
            const cleanStr = merged.completed_at.includes('T') || merged.completed_at.includes('Z')
                ? merged.completed_at
                : merged.completed_at.replace(' ', 'T') + 'Z';
            const d = new Date(cleanStr);
            if (!isNaN(d.getTime())) {
                merged.completed_at = d.toISOString().replace('T', ' ').slice(0, 19);
            } else {
                merged.completed_at = null;
            }
        } catch (e) {
            merged.completed_at = null;
        }
    } else {
        merged.completed_at = null;
    }

    // Explicitly parse counts since they might be strings out of ClickHouse JSON
    const sumAds = parseInt((merged.ads_rows || 0).toString(), 10);
    const sumAdjAds = parseInt((merged.adj_ads_rows || 0).toString(), 10);
    const sumSub = parseInt((merged.sub_rows || 0).toString(), 10);
    const sumAdjSub = parseInt((merged.adj_sub_rows || 0).toString(), 10);
    const sumShortsAds = parseInt((merged.shorts_ads_rows || 0).toString(), 10);
    const sumShortsSub = parseInt((merged.shorts_sub_rows || 0).toString(), 10);
    const sumReach = parseInt((merged.reach_rows || 0).toString(), 10);
    const sumDemo = parseInt((merged.demo_rows || 0).toString(), 10);
    const sumTraffic = parseInt((merged.traffic_rows || 0).toString(), 10);
    const sumDevice = parseInt((merged.device_rows || 0).toString(), 10);

    const calculatedSum = sumAds + sumAdjAds + sumSub + sumAdjSub + sumReach + sumDemo + sumTraffic + sumDevice + sumShortsAds + sumShortsSub;
    merged.total_rows = calculatedSum > 0 ? calculatedSum : (parseInt((merged.total_rows || 0).toString(), 10) || 0);
    merged.processed_rows = merged.total_rows;

    merged.claims_rows = parseInt((merged.claims_rows || 0).toString(), 10);
    merged.reach_rows = sumReach;
    merged.demo_rows = sumDemo;
    merged.traffic_rows = sumTraffic;
    merged.device_rows = sumDevice;
    merged.shorts_ads_rows = sumShortsAds;
    merged.shorts_sub_rows = sumShortsSub;
    merged.ads_revenue = parseFloat((merged.ads_revenue || 0.0).toString());
    merged.sub_revenue = parseFloat((merged.sub_revenue || 0.0).toString());
    merged.adj_ads_revenue = parseFloat((merged.adj_ads_revenue || 0.0).toString());
    merged.adj_sub_revenue = parseFloat((merged.adj_sub_revenue || 0.0).toString());
    merged.shorts_ads_revenue = parseFloat((merged.shorts_ads_revenue || 0.0).toString());
    merged.shorts_sub_revenue = parseFloat((merged.shorts_sub_revenue || 0.0).toString());

    await client.insert({
        table: 'ingestion_jobs',
        values: [{ job_id: jobId, ...merged }],
        format: 'JSONEachRow',
    });
}

export async function ingestRoutes(app: FastifyInstance): Promise<void> {
    app.addHook('onRequest', authMiddleware);

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/ingest/ads-revenue
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/ingest/ads-revenue', async (req, reply) => {
        const query = req.query as { month?: string; cms_id?: string; batchSize?: string; jobId?: string; us_tax_rate?: string };
        const month = query.month;
        const parsedBatchSize = query.batchSize ? parseInt(query.batchSize, 10) : undefined;
        if (!month || !/^\d{6}$/.test(month)) {
            return reply.code(400).send({ error: 'month query param is required (format: YYYYMM)' });
        }

        let cms_id: string;
        let dbName: string;
        let client: any;

        if (req.isAdmin || req.authRole === 'org_admin') {
            const reqCmsId = query.cms_id || (req.body as any)?.cms_id;
            if (!reqCmsId) return reply.code(400).send({ error: 'cms_id is required for Admin Master Key multi-CMS uploads' });

            const memTenant = getTenantById(reqCmsId);
            if (memTenant) {
                cms_id = memTenant.cmsId;
                dbName = memTenant.dbName;
                client = memTenant.ingestClient;
            } else {
                // Dynamically check cms_registry via ClickHouse
                try {
                    const defaultClient = getDefaultClient();
                    const res = await defaultClient.query({
                        query: `SELECT 1 FROM cms_registry WHERE cms_id = {cms_id: String} AND is_active = 1`,
                        query_params: { cms_id: reqCmsId },
                        format: 'JSONEachRow'
                    });
                    const rows = await res.json();
                    if ((rows as any[]).length === 0) {
                        try {
                            const newDbName = `db_${reqCmsId.replace(/-/g, '_')}`;
                            const apiKeyHash = createHash('sha256').update(uuidv4()).digest('hex');

                            await defaultClient.command({ query: `CREATE DATABASE IF NOT EXISTS ${newDbName}` });
                            const cmsClient = getClickHouseClient({ database: newDbName });

                            for (const ddl of CMS_DDL) {
                                await cmsClient.command({ query: ddl });
                            }

                            await defaultClient.insert({
                                table: 'cms_registry',
                                values: [{
                                    cms_id: reqCmsId,
                                    cms_name: reqCmsId,
                                    db_name: newDbName,
                                    api_key_hash: apiKeyHash,
                                    is_active: 1,
                                    org_id: (req as any).orgId || 'default_org'
                                }],
                                format: 'JSONEachRow',
                            });
                            console.log(`[Auto-Provision] Created ClickHouse DB and tables for Ads CMS: ${reqCmsId}`);
                        } catch (createErr: any) {
                            return reply.code(500).send({ error: `Auto-provisioning failed: ${createErr.message}` });
                        }
                    }

                    cms_id = reqCmsId;
                    dbName = `db_${reqCmsId.replace(/-/g, '_')}`;
                    client = getClickHouseClient({ database: dbName });
                } catch (err: any) {
                    return reply.code(500).send({ error: `DB check failed: ${err.message}` });
                }
            }
        } else {
            if (!req.tenant) return reply.code(401).send({ error: 'Unauthorized' });
            cms_id = req.tenant.cmsId;
            dbName = req.tenant.dbName;
            client = req.tenant.ingestClient;
        }

        const jobId = query.jobId || uuidv4();

        const lockKey = `ads-revenue:${cms_id}:${month}`;
        if (activeLocks.has(lockKey)) {
            return reply.code(409).send({ error: `Sync for CMS ${cms_id} and Month ${month} is already running (Job ID: ${activeLocks.get(lockKey)}).` });
        }
        activeLocks.set(lockKey, jobId);

        let tempFiles: string[] = [];
        let files: Record<string, string> = {};
        let savedFilesList: any[] = [];

        try {
            const savedFiles = await req.saveRequestFiles({ tmpdir: UPLOAD_DIR });
            savedFilesList = savedFiles;
            for (const f of savedFiles) {
                const dest = `${f.filepath}_${f.filename}`;
                copyFileSync(f.filepath, dest);
                files[f.fieldname] = dest;
                tempFiles.push(dest);
                tempFiles.push(f.filepath);
                const size = statSync(dest).size;
                console.log(`  [Upload] ${f.fieldname}: ${f.filename} (${(size / 1024 / 1024).toFixed(1)} MB)`);
            }

            // Check archive fallback for missing files
            const requiredFields = ['claim_raw', 'videoclaim', 'asset_summary'];
            const defaultCh = getDefaultClient();
            
            for (const field of requiredFields) {
                if (!files[field]) {
                    const archiveRes = await defaultCh.query({
                        query: `SELECT file_path FROM default.ingested_files_archive WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND file_type = {fileType: String}`,
                        query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10), fileType: field },
                        format: 'JSONEachRow'
                    });
                    const rows = await archiveRes.json() as { file_path: string }[];
                    if (rows.length > 0 && existsSync(rows[0].file_path)) {
                        files[field] = rows[0].file_path;
                        console.log(`  [Fallback] Field ${field} falling back to archived file: ${rows[0].file_path}`);
                    }
                }
            }

            if (!files.ads_adjustment) {
                const archiveRes = await defaultCh.query({
                    query: `SELECT file_path FROM default.ingested_files_archive WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND file_type = 'ads_adjustment'`,
                    query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                    format: 'JSONEachRow'
                });
                const rows = await archiveRes.json() as { file_path: string }[];
                if (rows.length > 0 && existsSync(rows[0].file_path)) {
                    files.ads_adjustment = rows[0].file_path;
                }
            }

            if (!files.shorts_ads) {
                const archiveRes = await defaultCh.query({
                    query: `SELECT file_path FROM default.ingested_files_archive WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND file_type = 'shorts_ads'`,
                    query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                    format: 'JSONEachRow'
                });
                const rows = await archiveRes.json() as { file_path: string }[];
                if (rows.length > 0 && existsSync(rows[0].file_path)) {
                    files.shorts_ads = rows[0].file_path;
                }
            }

            if (!files.affiliate_tax) {
                const archiveRes = await defaultCh.query({
                    query: `SELECT file_path FROM default.ingested_files_archive WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND file_type = 'affiliate_tax'`,
                    query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                    format: 'JSONEachRow'
                });
                const rows = await archiveRes.json() as { file_path: string }[];
                if (rows.length > 0 && existsSync(rows[0].file_path)) {
                    files.affiliate_tax = rows[0].file_path;
                }
            }

            // Validate required files
            if (!files.claim_raw || !files.videoclaim || !files.asset_summary) {
                cleanupFiles(tempFiles);
                activeLocks.delete(lockKey);
                return reply.code(400).send({ error: 'Required files: claim_raw, videoclaim, asset_summary' });
            }

            // Create job globally
            await updateJob(jobId, {
                job_type: 'ads_revenue', cms_id, status: 'processing', month: parseInt(month),
            });
        } catch (err: any) {
            cleanupFiles(tempFiles);
            activeLocks.delete(lockKey);
            return reply.code(500).send({ error: `Failed to initialize ingestion job: ${err.message}` });
        }

        // Respond immediately — processing continues in background
        reply.code(202).send({ job_id: jobId, status: 'processing', month });

        // Safely parse clearLog query parameter (handles string "false" or boolean false)
        const qClearLog = (req.query as any).clearLog;
        const clearLog = qClearLog !== 'false' && qClearLog !== false && qClearLog !== 0;
        initJobLog(jobId, clearLog);

        // Register job globally before yielding the event loop
        runningJobs.add(jobId);

        // Background ETL processing
        setImmediate(async () => {
            const log = (msg: string) => emitLog(jobId, msg);
            let finalStatus = 'processing';
            try {
                // Archive files before processing
                const fileFields = ['claim_raw', 'videoclaim', 'asset_summary', 'adj_claim_raw', 'shorts_ads', 'affiliate_tax'];
                for (const field of fileFields) {
                    if (files[field]) {
                        const orig = savedFilesList.find(sf => sf.fieldname === field);
                        if (orig) {
                            await archiveUploadedFile({
                                cmsId: cms_id,
                                month,
                                fileType: field === 'adj_claim_raw' 
                                    ? 'ads_adjustment' 
                                    : field === 'affiliate_tax'
                                        ? 'affiliate_tax'
                                        : field,
                                tempPath: files[field],
                                originalFilename: orig.filename,
                                log
                            });
                        }
                    }
                }

                const startTime = Date.now();
                log(`🚀 Starting Ads Revenue ETL for month ${month}`);

                // Step 0: Ingest Affiliate Tax rates if provided
                if (files.affiliate_tax) {
                    await ingestAffiliateTaxTable({
                        client,
                        cmsId: cms_id,
                        month,
                        jobId,
                        affiliateTaxPath: files.affiliate_tax,
                        isAborted: () => activeJobTokens.has(jobId),
                        log
                    });
                }

                // Step 1: Drop existing partition specifically for this CMS
                log(`[Step 1/5] Dropping partition ${month} for CMS ${cms_id}...`);
                await client.command({
                    query: `ALTER TABLE ads_revenue_enriched DROP PARTITION ({cmsId: String}, {partitionMonth: UInt32})`,
                    query_params: { cmsId: cms_id, partitionMonth: parseInt(month, 10) }
                }).catch((e: any) => log(`[Step 1/5] Partition empty or not found. Continuing.`));
                log(`[Step 1/5] ✓ Partition dropped`);

                // Load channel mappings from uploaded file (if provided)
                const channelMap = new Map<string, string>();
                if (files.channelMappings && existsSync(files.channelMappings)) {
                    try {
                        const raw = readFileSync(files.channelMappings, 'utf-8');
                        const parsed = JSON.parse(raw);
                        for (const [k, v] of Object.entries(parsed)) channelMap.set(k, v as string);
                        log(`[Step 2/5] Loaded ${channelMap.size} Channel Mappings from uploaded file`);
                    } catch (e: any) { log(`[Step 2/5] Error loading mappings file: ${e.message}`); }
                } else {
                    log(`[Step 2/5] No mappings file uploaded. Mappings will remain empty.`);
                }

                // Ingest files directly into ClickHouse Staging Tables and run SQL Join
                const usTaxRate = parseFloat(query.us_tax_rate || '10') || 10.0;
                const ingestResult = await runAdsIngestionDirect({
                    jobId,
                    month,
                    cmsId: cms_id,
                    usTaxRate,
                    files: {
                        claim_raw: files.claim_raw,
                        videoclaim: files.videoclaim,
                        asset_summary: files.asset_summary,
                        adj_claim_raw: files.adj_claim_raw,
                        shorts_ads: files.shorts_ads
                    },
                    channelMap,
                    client,
                    isAborted: () => activeJobTokens.has(jobId),
                    log
                });

                const totalAllRows = ingestResult.totalAllRows;
                const breakdown: string[] = [];
                breakdown.push(`Ads Revenue: ${ingestResult.adsRows.toLocaleString()}`);
                if (files.adj_claim_raw) {
                    breakdown.push(`Ads Adjustment: ${ingestResult.adjAdsRows.toLocaleString()}`);
                }
                if (ingestResult.shortsAdsRows) {
                    breakdown.push(`Shorts Ads: ${ingestResult.shortsAdsRows.toLocaleString()}`);
                }

                // Step 5: Reconciliation
                log(`[Step 5/5] Running reconciliation...`);
                const reconResult = await reconcileNullLabels({ dbName, month: parseInt(month), cmsId: cms_id });
                if (reconResult.corrected > 0) {
                    breakdown.push(`Reconciled: ${reconResult.corrected.toLocaleString()}`);
                }
                log(`[Step 5/5] ✓ Reconciled: ${reconResult.corrected.toLocaleString()} labels`);

                // Query total ads revenues from clickhouse enriched table
                log(`[Step 5/5] Querying calculated revenue totals from ClickHouse...`);
                const revRes = await client.query({
                    query: `SELECT sum(partner_rev_total) as total_rev FROM ads_revenue_enriched WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND report_type = 'claim_raw'`,
                    query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                    format: 'JSONEachRow'
                });
                const revRows = await revRes.json() as any[];
                const adsRevenue = revRows[0]?.total_rev ? parseFloat(revRows[0].total_rev) : 0.0;

                let adjAdsRevenue = 0.0;
                if (files.adj_claim_raw) {
                    const adjRevRes = await client.query({
                        query: `SELECT sum(partner_rev_total) as total_rev FROM ads_revenue_enriched WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND report_type = 'ads_adjustment'`,
                        query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                        format: 'JSONEachRow'
                    });
                    const adjRevRows = await adjRevRes.json() as any[];
                    adjAdsRevenue = adjRevRows[0]?.total_rev ? parseFloat(adjRevRows[0].total_rev) : 0.0;
                }

                let shortsAdsRevenue = 0.0;
                if (ingestResult.shortsAdsRows) {
                    const shortsAdsRevRes = await client.query({
                        query: `SELECT sum(partner_rev_total) as total_rev FROM ads_revenue_enriched WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND report_type = 'shorts_ads'`,
                        query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                        format: 'JSONEachRow'
                    });
                    const shortsAdsRevRows = await shortsAdsRevRes.json() as any[];
                    shortsAdsRevenue = shortsAdsRevRows[0]?.total_rev ? parseFloat(shortsAdsRevRows[0].total_rev) : 0.0;
                }

                // Query total us tax and net revenue
                const taxAndNetRes = await client.query({
                    query: `SELECT sum(us_tax) as tax_total, sum(net_revenue) as net_total FROM ads_revenue_enriched WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32}`,
                    query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                    format: 'JSONEachRow'
                });
                const taxAndNetRows = await taxAndNetRes.json() as any[];
                const adsUsTax = taxAndNetRows[0]?.tax_total ? parseFloat(taxAndNetRows[0].tax_total) : 0.0;
                const adsNetRevenue = taxAndNetRows[0]?.net_total ? parseFloat(taxAndNetRows[0].net_total) : 0.0;

                // Done
                const elapsed = Math.round((Date.now() - startTime) / 1000);
                const durStr = elapsed >= 60 ? `${Math.floor(elapsed / 60)}m ${elapsed % 60}s` : `${elapsed}s`;
                breakdown.push(`Duration: ${durStr}`);
                const detailStr = breakdown.join(' | ');
                finalStatus = 'completed';
                await updateJob(jobId, {
                    job_type: 'ads_revenue', cms_id, status: 'completed',
                    month: parseInt(month),
                    total_rows: totalAllRows, processed_rows: totalAllRows,
                    ads_rows: ingestResult.adsRows,
                    adj_ads_rows: ingestResult.adjAdsRows,
                    shorts_ads_rows: ingestResult.shortsAdsRows || 0,
                    ads_revenue: adsRevenue,
                    adj_ads_revenue: adjAdsRevenue,
                    shorts_ads_revenue: shortsAdsRevenue,
                    us_tax: adsUsTax,
                    net_revenue: adsNetRevenue,
                    error_message: `[Ads] ✅ ${totalAllRows.toLocaleString()} rows in ${durStr}`,
                    completed_at: new Date().toISOString().replace('T', ' ').slice(0, 19),
                });

                log(`✅ Completed! ${totalAllRows.toLocaleString()} total rows in ${durStr}`);
            } catch (err: any) {
                finalStatus = 'failed';
                emitLog(jobId, `❌ Ads Revenue Failed: ${err.message}`);
                console.error(err.stack || err);
                await client.command({
                    query: `ALTER TABLE ads_revenue_enriched DROP PARTITION ({cmsId: String}, {partitionMonth: UInt32})`,
                    query_params: { cmsId: cms_id, partitionMonth: parseInt(month, 10) }
                }).catch(() => { });
                await updateJob(jobId, {
                    job_type: 'ads_revenue', cms_id, status: 'failed',
                    month: parseInt(month), error_message: err.message,
                }).catch(() => { });
            } finally {
                activeLocks.delete(lockKey);
                runningJobs.delete(jobId);
                cleanupFiles(tempFiles);
                const finalLogs = getJobLogs(jobId);

                // Overwrite detail_logs entirely with the accumulated events from Memory and preserve final status
                await updateJob(jobId, { status: finalStatus, detail_logs: JSON.stringify(finalLogs) }).catch(() => { });

                // Wait 10s before closing terminal so Frontend finishes catching up
                setTimeout(() => completeJobLog(jobId), 10000);
            }
        });
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/ingest/estimated-ads
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/ingest/estimated-ads', async (req, reply) => {
        const query = req.query as { day?: string; cms_id?: string; batchSize?: string; jobId?: string; total_cms_revenue?: string; low_priority_count?: string };
        const day = query.day; // Format: YYYY-MM-DD
        const parsedBatchSize = query.batchSize ? parseInt(query.batchSize, 10) : undefined;
        const totalCmsRevenue = query.total_cms_revenue ? parseFloat(query.total_cms_revenue) : undefined;
        const lowPriorityCount = query.low_priority_count ? parseInt(query.low_priority_count, 10) : undefined;
        if (!day || !/^\d{4}-\d{2}-\d{2}$/.test(day)) {
            return reply.code(400).send({ error: 'day query param is required (format: YYYY-MM-DD)' });
        }

        let cms_id: string;
        let dbName: string;
        let client: any;

        if (req.isAdmin || req.authRole === 'org_admin') {
            const reqCmsId = query.cms_id || (req.body as any)?.cms_id;
            if (!reqCmsId) return reply.code(400).send({ error: 'cms_id is required for Admin Master Key multi-CMS uploads' });

            const memTenant = getTenantById(reqCmsId);
            if (memTenant) {
                cms_id = memTenant.cmsId;
                dbName = memTenant.dbName;
                client = memTenant.ingestClient;
            } else {
                try {
                    const defaultClient = getDefaultClient();
                    const res = await defaultClient.query({
                        query: `SELECT 1 FROM cms_registry WHERE cms_id = {cms_id: String} AND is_active = 1`,
                        query_params: { cms_id: reqCmsId },
                        format: 'JSONEachRow'
                    });
                    const rows = await res.json();
                    if ((rows as any[]).length === 0) {
                        try {
                            const newDbName = `db_${reqCmsId.replace(/-/g, '_')}`;
                            const apiKeyHash = createHash('sha256').update(uuidv4()).digest('hex');

                            await defaultClient.command({ query: `CREATE DATABASE IF NOT EXISTS ${newDbName}` });
                            const cmsClient = getClickHouseClient({ database: newDbName });

                            for (const ddl of CMS_DDL) {
                                await cmsClient.command({ query: ddl });
                            }

                            await defaultClient.insert({
                                table: 'cms_registry',
                                values: [{
                                    cms_id: reqCmsId,
                                    cms_name: reqCmsId,
                                    db_name: newDbName,
                                    api_key_hash: apiKeyHash,
                                    is_active: 1,
                                    org_id: (req as any).orgId || 'default_org'
                                }],
                                format: 'JSONEachRow',
                            });
                            console.log(`[Auto-Provision] Created ClickHouse DB and tables for Estimated Ads CMS: ${reqCmsId}`);
                        } catch (createErr: any) {
                            return reply.code(500).send({ error: `Auto-provisioning failed: ${createErr.message}` });
                        }
                    }

                    cms_id = reqCmsId;
                    dbName = `db_${reqCmsId.replace(/-/g, '_')}`;
                    client = getClickHouseClient({ database: dbName });
                } catch (err: any) {
                    return reply.code(500).send({ error: `DB check failed: ${err.message}` });
                }
            }
        } else {
            if (!req.tenant) return reply.code(401).send({ error: 'Unauthorized' });
            cms_id = req.tenant.cmsId;
            dbName = req.tenant.dbName;
            client = req.tenant.ingestClient;
        }

        let jobId = query.jobId;
        if (!jobId) {
            jobId = uuidv4();
        }

        const lockKey = `estimated-ads:${cms_id}:${day}`;
        if (activeLocks.has(lockKey)) {
            return reply.code(409).send({ error: `Sync for CMS ${cms_id} and Date ${day} is already running (Job ID: ${activeLocks.get(lockKey)}).` });
        }
        activeLocks.set(lockKey, jobId);

        const monthNum = parseInt(day.replace(/-/g, '').substring(0, 6), 10);
        const isFallback = (req.query as any).is_fallback === '1' || (req.query as any).is_fallback === 1 || (req.query as any).is_fallback === 'true' ? 1 : 0;
        const fallbackDate = (req.query as any).fallback_date || '';

        let tempFiles: string[] = [];
        const files: Record<string, string> = {};
        try {
            const savedFiles = await req.saveRequestFiles({ tmpdir: UPLOAD_DIR });
            for (const f of savedFiles) {
                const dest = `${f.filepath}_${f.filename}`;
                copyFileSync(f.filepath, dest);
                files[f.fieldname] = dest;
                tempFiles.push(dest);
                tempFiles.push(f.filepath);
                const size = statSync(dest).size;
                console.log(`  [Upload Estimated] ${f.fieldname}: ${f.filename} (${(size / 1024 / 1024).toFixed(3)} MB)`);
            }

            if (!files.claim_raw || !files.estimated_revenue) {
                cleanupFiles(tempFiles);
                activeLocks.delete(lockKey);
                return reply.code(400).send({ error: 'Required files: claim_raw, estimated_revenue' });
            }

            // Create job globally
            await updateJob(jobId, {
                job_type: 'estimated_ads',
                cms_id,
                status: 'processing',
                month: monthNum,
                is_fallback: isFallback,
                fallback_date: fallbackDate,
                error_message: `Sync range: ${day} to ${day}`
            });
        } catch (err: any) {
            cleanupFiles(tempFiles);
            activeLocks.delete(lockKey);
            return reply.code(500).send({ error: `Failed to initialize ingestion job: ${err.message}` });
        }

        reply.code(202).send({ job_id: jobId, status: 'processing', day });

        const qClearLog = (req.query as any).clearLog;
        const clearLog = qClearLog !== 'false' && qClearLog !== false && qClearLog !== 0;
        initJobLog(jobId, clearLog);

        runningJobs.add(jobId);
        activeJobTokens.delete(jobId); // Clean stale abort token for daily child job

        setImmediate(async () => {
            const log = (msg: string) => emitLog(jobId, msg);
            let finalStatus = 'processing';
            try {
                const startTime = Date.now();
                log(`🚀 Starting Daily Estimated Ads ETL for ${day}`);

                // Step 1: Idempotency deletion
                log(`[Step 1/3] Deleting existing estimated records for ${day} and CMS ${cms_id}...`);
                const subscribersSourceFile = files.subscribers || files.interactions;
                try {
                    await client.command({
                        query: `ALTER TABLE estimated_revenue_daily DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                        query_params: { cms_id, day }
                    });
                    await client.command({
                        query: `ALTER TABLE mv_asset_performance_daily DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                        query_params: { cms_id, day }
                    });

                    if (files.video_reach) {
                        await client.command({
                            query: `ALTER TABLE video_reach_performance_daily DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                            query_params: { cms_id, day }
                        });
                    }

                    if (files.video_demographics) {
                        await client.command({
                            query: `ALTER TABLE video_demographics_daily DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                            query_params: { cms_id, day }
                        });
                    }

                    if (files.video_traffic_sources) {
                        await client.command({
                            query: `ALTER TABLE video_traffic_sources_daily DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                            query_params: { cms_id, day }
                        });
                    }

                    if (files.video_devices) {
                        await client.command({
                            query: `ALTER TABLE video_devices_daily DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                            query_params: { cms_id, day }
                        });
                    }

                    if (files.video_country) {
                        await client.command({
                            query: `ALTER TABLE video_countries_daily DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                            query_params: { cms_id, day }
                        });
                    }

                    if (subscribersSourceFile) {
                        await client.command({
                            query: `ALTER TABLE channel_subscribers_daily DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                            query_params: { cms_id, day }
                        });
                    }

                    if (files.interactions) {
                        await client.command({
                            query: `ALTER TABLE video_interactions_daily DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                            query_params: { cms_id, day }
                        });
                    }
                } catch (e: any) {
                    throw new Error(`Gagal menghapus data harian lama: ${e.message}`);
                }

                log(`[Step 1/3] ✓ Existing estimated records cleared`);

                // Step 2: Ingest daily additional analytics (reach, demographics, traffic, devices)
                log(`[Step 2/3] Processing additional analytics files in parallel...`);
                const tasks: { name: string; promise: Promise<any> }[] = [];

                if (files.video_reach) {
                    tasks.push({
                        name: 'reach',
                        promise: processVideoReach({
                            filePath: files.video_reach,
                            cmsId: cms_id,
                            client,
                            batchSize: parsedBatchSize,
                            isAborted: () => activeJobTokens.has(jobId),
                            jobId,
                        }),
                    });
                }
                if (files.video_demographics) {
                    tasks.push({
                        name: 'demographics',
                        promise: processVideoDemographics({
                            filePath: files.video_demographics,
                            cmsId: cms_id,
                            client,
                            batchSize: parsedBatchSize,
                            isAborted: () => activeJobTokens.has(jobId),
                            jobId,
                        }),
                    });
                }
                if (files.video_traffic_sources) {
                    tasks.push({
                        name: 'traffic_sources',
                        promise: processVideoTrafficSources({
                            filePath: files.video_traffic_sources,
                            cmsId: cms_id,
                            client,
                            batchSize: parsedBatchSize,
                            isAborted: () => activeJobTokens.has(jobId),
                            jobId,
                        }),
                    });
                }
                if (files.video_devices) {
                    tasks.push({
                        name: 'devices',
                        promise: processVideoDevices({
                            filePath: files.video_devices,
                            cmsId: cms_id,
                            client,
                            batchSize: parsedBatchSize,
                            isAborted: () => activeJobTokens.has(jobId),
                            jobId,
                        }),
                    });
                }
                if (files.video_country) {
                    tasks.push({
                        name: 'countries',
                        promise: processVideoCountry({
                            filePath: files.video_country,
                            cmsId: cms_id,
                            client,
                            batchSize: parsedBatchSize,
                            isAborted: () => activeJobTokens.has(jobId),
                            jobId,
                        }),
                    });
                }
                if (subscribersSourceFile) {
                    tasks.push({
                        name: 'subscribers',
                        promise: processDailySubscribers({
                            filePath: subscribersSourceFile,
                            cmsId: cms_id,
                            client,
                            batchSize: parsedBatchSize,
                            isAborted: () => activeJobTokens.has(jobId),
                            jobId,
                        }),
                    });
                }
                if (files.interactions) {
                    tasks.push({
                        name: 'interactions',
                        promise: processDailyInteractions({
                            filePath: files.interactions,
                            cmsId: cms_id,
                            client,
                            batchSize: parsedBatchSize,
                            isAborted: () => activeJobTokens.has(jobId),
                            jobId,
                        }),
                    });
                }

                let reachResultRows = 0;
                let demographicsRows = 0;
                let trafficRows = 0;
                let deviceRows = 0;
                let subscriberRows = 0;
                let interactionRows = 0;

                if (tasks.length > 0) {
                    const results = await Promise.allSettled(tasks.map(t => t.promise));
                    for (let i = 0; i < tasks.length; i++) {
                        const taskName = tasks[i].name;
                        const res = results[i];
                        if (res.status === 'fulfilled') {
                            const rowCount = res.value.totalRows;
                            log(`[Step 2/3] ✓ ${taskName} ingestion completed: ${rowCount.toLocaleString()} rows`);
                            if (taskName === 'reach') reachResultRows = rowCount;
                            else if (taskName === 'demographics') demographicsRows = rowCount;
                            else if (taskName === 'traffic_sources') trafficRows = rowCount;
                            else if (taskName === 'devices') deviceRows = rowCount;
                            else if (taskName === 'subscribers') subscriberRows = rowCount;
                            else if (taskName === 'interactions') interactionRows = rowCount;
                        } else {
                            log(`[Step 2/3] ❌ ${taskName} ingestion failed: ${res.reason.message}`);
                            throw res.reason;
                        }
                    }
                } else {
                    log(`[Step 2/3] Skipped additional analytics metrics (no files provided)`);
                }

                // Step 3: Ingest daily estimated ads (estimated revenue & claims)
                log(`[Step 3/3] Processing daily estimated ads...`);
                let lastAdsUpdate = 0;
                
                let channelTransactions: any[] = [];
                if (files.channel_transactions) {
                    try {
                        const fileContent = readFileSync(files.channel_transactions, 'utf8');
                        channelTransactions = JSON.parse(fileContent);
                        log(`[Step 3/3] Loaded ${channelTransactions.length} channel transaction adjustments from uploaded JSON.`);
                    } catch (e: any) {
                        log(`[Step 3/3] ⚠️ Gagal membaca channel_transactions: ${e.message}`);
                    }
                }

                const claimResult = await processEstimatedAds({
                    filePath: files.estimated_revenue,
                    claimsFilePath: files.claim_raw,
                    assetRevenueFilePath: files.estimated_asset_revenue,
                    channelRevenueFilePath: files.estimated_channel_revenue,
                    channelTransactions,
                    cmsId: cms_id,
                    day,
                    client,
                    batchSize: parsedBatchSize,
                    log,
                    isAborted: () => activeJobTokens.has(jobId),
                    jobId,
                    totalCmsRevenue,
                    lowPriorityCount,
                    onProgress: async (progress) => {
                        if (progress.totalRows - lastAdsUpdate >= 5000) {
                            lastAdsUpdate = progress.totalRows;
                            log(`[Step 3/3] Ingested ${progress.totalRows.toLocaleString()} estimated rows`);
                            await updateJob(jobId, {
                                total_rows: progress.totalRows,
                                processed_rows: progress.processedRows,
                                ads_rows: progress.adsRows,
                                sub_rows: progress.subRows,
                                claims_rows: progress.claimsRows,
                                ads_revenue: progress.adsRevenue,
                                sub_revenue: progress.subRevenue,
                            }).catch(() => { });
                        }
                    }
                });
                log(`[Step 3/3] ✓ Daily estimated ads done: ${claimResult.totalRows.toLocaleString()} rows`);

                // Done
                const elapsed = Math.round((Date.now() - startTime) / 1000);
                const durStr = elapsed >= 60 ? `${Math.floor(elapsed / 60)}m ${elapsed % 60}s` : `${elapsed}s`;
                
                const formattedAdsRev = `$${(claimResult.adsRevenue || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
                const formattedSubRev = `$${(claimResult.subRevenue || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
                const totalRev = (claimResult.adsRevenue || 0) + (claimResult.subRevenue || 0);
                const formattedTotalRev = `$${totalRev.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

                finalStatus = 'completed';
                await updateJob(jobId, {
                    status: 'completed',
                    total_rows: claimResult.totalRows,
                    processed_rows: claimResult.totalRows,
                    ads_rows: claimResult.adsRows,
                    sub_rows: claimResult.subRows,
                    claims_rows: claimResult.claimsRows,
                    reach_rows: reachResultRows,
                    demo_rows: demographicsRows,
                    traffic_rows: trafficRows,
                    device_rows: deviceRows,
                    ads_revenue: claimResult.adsRevenue,
                    sub_revenue: claimResult.subRevenue,
                    is_fallback: isFallback,
                    fallback_date: fallbackDate,
                    error_message: `${claimResult.auditWarning ? '[Audit: Warning] ' : ''}[Daily] ✅ Ingested ${claimResult.totalRows.toLocaleString()} rows (Subs: ${subscriberRows.toLocaleString()} | Inters: ${interactionRows.toLocaleString()}) | API Sync Revenue: ${formattedTotalRev} (Ads: ${formattedAdsRev}, Sub: ${formattedSubRev}) in ${durStr}${claimResult.auditWarning ? ` | ${claimResult.auditMessage}` : ''}`,
                    completed_at: new Date().toISOString().replace('T', ' ').slice(0, 19),
                });

                log(`✅ Completed! Ingested ${claimResult.totalRows.toLocaleString()} rows | API Sync Revenue: ${formattedTotalRev} (Ads: ${formattedAdsRev}, Sub: ${formattedSubRev}) in ${durStr}`);

            } catch (err: any) {
                finalStatus = 'failed';
                emitLog(jobId, `❌ Daily Estimated ETL Failed: ${err.message}`);
                console.error(err.stack || err);
                
                // Cleanup partially ingested records on failure/abort to prevent partial data junk
                log(`[Cleanup] Cleaning up partial ClickHouse data for day ${day}...`);
                await client.command({
                    query: `ALTER TABLE estimated_revenue_daily DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                    query_params: { cms_id, day }
                }).catch(() => { });
                await client.command({
                    query: `ALTER TABLE mv_asset_performance_daily DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                    query_params: { cms_id, day }
                }).catch(() => { });
                await client.command({
                    query: `ALTER TABLE youtube_raw_claims DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                    query_params: { cms_id, day }
                }).catch(() => { });
                await client.command({
                    query: `ALTER TABLE youtube_raw_estimated_revenue DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                    query_params: { cms_id, day }
                }).catch(() => { });
                await client.command({
                    query: `ALTER TABLE youtube_raw_asset_estimated_revenue DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                    query_params: { cms_id, day }
                }).catch(() => { });
                await client.command({
                    query: `ALTER TABLE youtube_raw_channel_estimated_revenue DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                    query_params: { cms_id, day }
                }).catch(() => { });
                if (files.video_reach) {
                    await client.command({
                        query: `ALTER TABLE video_reach_performance_daily DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                        query_params: { cms_id, day }
                    }).catch(() => { });
                }
                if (files.video_demographics) {
                    await client.command({
                        query: `ALTER TABLE video_demographics_daily DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                        query_params: { cms_id, day }
                    }).catch(() => { });
                }
                if (files.video_traffic_sources) {
                    await client.command({
                        query: `ALTER TABLE video_traffic_sources_daily DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                        query_params: { cms_id, day }
                    }).catch(() => { });
                }
                if (files.video_devices) {
                    await client.command({
                        query: `ALTER TABLE video_devices_daily DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                        query_params: { cms_id, day }
                    }).catch(() => { });
                }

                await updateJob(jobId, {
                    status: 'failed',
                    error_message: err.message,
                }).catch(() => { });
            } finally {
                activeLocks.delete(lockKey);
                runningJobs.delete(jobId);
                activeJobTokens.delete(jobId);
                cleanupFiles(tempFiles);
                const finalLogs = getJobLogs(jobId);

                // Overwrite detail_logs entirely with the accumulated events from Memory and preserve final status
                await updateJob(jobId, { status: finalStatus, detail_logs: JSON.stringify(finalLogs) }).catch(() => { });
                setTimeout(() => completeJobLog(jobId), 10000);
            }
        });
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/ingest/analytics-speed-layer
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/ingest/analytics-speed-layer', async (req, reply) => {
        const body = req.body as {
            day: string;
            cms_id: string;
            jobId: string;
            columns: string[];
            rows: any[][];
        };

        if (!body.day || !body.cms_id || !body.jobId || !body.columns || !body.rows) {
            return reply.code(400).send({ error: 'Missing required parameters in body' });
        }

        const { day, cms_id, jobId, columns, rows } = body;

        let dbName = `db_${cms_id.replace(/-/g, '_')}`;
        let client = getClickHouseClient({ database: dbName });

        const lockKey = `estimated-ads:${cms_id}:${day}`;
        if (activeLocks.has(lockKey)) {
            return reply.code(409).send({ error: `Sync for CMS ${cms_id} and Date ${day} is already running (Job ID: ${activeLocks.get(lockKey)}).` });
        }
        activeLocks.set(lockKey, jobId);
        runningJobs.add(jobId);

        // Run in the background
        (async () => {
            let finalStatus = 'completed';
            initJobLog(jobId, false);
            const log = (msg: string) => emitLog(jobId, msg);
            const logError = (msg: string) => emitLog(jobId, `❌ ${msg}`);

            try {
                log(`[Speed Layer] Memulai pemrosesan data harian untuk tanggal ${day}...`);
                const { processSpeedLayerData } = await import('../workers/etl-estimated.js');
                const speedResult = await processSpeedLayerData({
                    client,
                    cmsId: cms_id,
                    day,
                    columns,
                    rows,
                    channelTransactions: (body as any).channelTransactions || [],
                    lowPriorityCount: (body as any).lowPriorityCount || 0,
                    totalCmsRevenue: (body as any).totalCmsRevenue || 0,
                    log
                });
                log(`[Speed Layer] ✓ Pemrosesan data berhasil diselesaikan!`);
                const pendingSuffix = speedResult.pendingSuffix || "";
                await updateJob(jobId, {
                    status: 'completed',
                    total_rows: speedResult.totalRows,
                    ads_revenue: speedResult.adsRevenue,
                    sub_revenue: speedResult.subRevenue,
                    error_message: speedResult.auditWarning 
                        ? `[Audit: Warning] ${speedResult.auditMessage}${pendingSuffix}` 
                        : `[Speed Layer] ✅ Ingested ${speedResult.totalRows.toLocaleString()} rows${pendingSuffix}`,
                }).catch(() => { });
            } catch (err: any) {
                finalStatus = 'failed';
                logError(`[Speed Layer] 🚨 Gagal memproses data: ${err.message}`);
                await updateJob(jobId, {
                    status: 'failed',
                    error_message: err.message,
                }).catch(() => { });
            } finally {
                activeLocks.delete(lockKey);
                runningJobs.delete(jobId);
                activeJobTokens.delete(jobId);
                const finalLogs = getJobLogs(jobId);
                await updateJob(jobId, { status: finalStatus, detail_logs: JSON.stringify(finalLogs) }).catch(() => { });
                setTimeout(() => completeJobLog(jobId), 10000);
            }
        })();

        return reply.code(202).send({ success: true, jobId, message: 'Ingestion started in background' });
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/ingest/asset-metadata
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/ingest/asset-metadata', async (req, reply) => {
        const query = req.query as { day?: string; cms_id?: string; jobId?: string };
        const day = query.day; // Format: YYYY-MM-DD
        if (!day || !/^\d{4}-\d{2}-\d{2}$/.test(day)) {
            return reply.code(400).send({ error: 'day query param is required (format: YYYY-MM-DD)' });
        }

        let cms_id: string;
        let dbName: string;
        let client: any;

        if (req.isAdmin || req.authRole === 'org_admin') {
            const reqCmsId = query.cms_id || (req.body as any)?.cms_id;
            if (!reqCmsId) return reply.code(400).send({ error: 'cms_id is required for Admin Master Key multi-CMS uploads' });

            const memTenant = getTenantById(reqCmsId);
            if (memTenant) {
                cms_id = memTenant.cmsId;
                dbName = memTenant.dbName;
                client = memTenant.ingestClient;
            } else {
                try {
                    const defaultClient = getDefaultClient();
                    const res = await defaultClient.query({
                        query: `SELECT 1 FROM cms_registry WHERE cms_id = {cms_id: String} AND is_active = 1`,
                        query_params: { cms_id: reqCmsId },
                        format: 'JSONEachRow'
                    });
                    const rows = await res.json();
                    if ((rows as any[]).length === 0) {
                        return reply.code(404).send({ error: `CMS ${reqCmsId} not found or inactive` });
                    }
                    cms_id = reqCmsId;
                    dbName = `db_${reqCmsId.replace(/-/g, '_')}`;
                    client = getClickHouseClient({ database: dbName });
                } catch (err: any) {
                    return reply.code(500).send({ error: `DB check failed: ${err.message}` });
                }
            }
        } else {
            if (!req.tenant) return reply.code(401).send({ error: 'Unauthorized' });
            cms_id = req.tenant.cmsId;
            dbName = req.tenant.dbName;
            client = req.tenant.ingestClient;
        }

        const savedFiles = await req.saveRequestFiles({ tmpdir: UPLOAD_DIR });
        if (savedFiles.length === 0) {
            return reply.code(400).send({ error: 'No files uploaded' });
        }

        const file = savedFiles[0];
        const dest = `${file.filepath}_${file.filename}`;
        copyFileSync(file.filepath, dest);
        const tempFiles = [dest, file.filepath];

        try {
            console.log(`  [Upload Asset Metadata] Processing metadata for day: ${day}, CMS: ${cms_id}...`);
            
            // Step 1: Idempotency deletion
            await client.command({
                query: `ALTER TABLE youtube_asset_metadata DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                query_params: { cms_id, day }
            }).catch((e: any) => console.warn(`[Asset Metadata Ingest] Delete warning: ${e.message}`));

            // Step 2: Stream CSV to a temp table
            const tempTable = `temp_metadata_${uuidv4().replace(/-/g, '_')}`;
            await client.command({
                query: `
                    CREATE TABLE ${tempTable} (
                        "Asset ID"    String DEFAULT '',
                        "asset_id"    String DEFAULT '',
                        "Asset Title" String DEFAULT '',
                        "asset_title" String DEFAULT '',
                        "Title"       String DEFAULT '',
                        "Artist"      String DEFAULT '',
                        "artist"      String DEFAULT '',
                        "Album"       String DEFAULT '',
                        "album"       String DEFAULT '',
                        "Label"       String DEFAULT '',
                        "label"       String DEFAULT '',
                        "record_label" String DEFAULT '',
                        "ISRC"        String DEFAULT '',
                        "isrc"        String DEFAULT '',
                        "UPC"         String DEFAULT '',
                        "upc"         String DEFAULT '',
                        "GRid"        String DEFAULT '',
                        "grid"        String DEFAULT '',
                        "Custom ID"   String DEFAULT '',
                        "custom_id"   String DEFAULT '',
                        "Genre"       String DEFAULT '',
                        "genre"       String DEFAULT '',
                        "Asset Labels" String DEFAULT '',
                        "asset_labels" String DEFAULT ''
                    ) ENGINE = StripeLog()
                `
            });

            const stream = await extractFileStream(dest);
            const delimiter = await detectDelimiterFromFile(dest);
            await client.insert({
                table: tempTable,
                values: stream,
                format: delimiter === '\t' ? 'TabSeparatedWithNames' : 'CSVWithNames',
                clickhouse_settings: {
                    input_format_skip_unknown_fields: 1
                }
            });

            // Step 3: Insert into final table youtube_asset_metadata
            await client.command({
                query: `
                    INSERT INTO youtube_asset_metadata (
                        cms_id, day, asset_id, asset_title, artist, album, label, isrc, upc, grid, custom_id, genre, asset_labels
                    )
                    SELECT
                        '${cms_id}',
                        toDate('${day}'),
                        coalesce(nullIf("Asset ID", ''), nullIf(asset_id, '')),
                        coalesce(nullIf("Asset Title", ''), nullIf(asset_title, ''), nullIf("Title", ''), ''),
                        coalesce(nullIf("Artist", ''), nullIf(artist, ''), ''),
                        coalesce(nullIf("Album", ''), nullIf(album, ''), ''),
                        coalesce(nullIf("Label", ''), nullIf(label, ''), nullIf(record_label, ''), ''),
                        coalesce(nullIf("ISRC", ''), nullIf(isrc, ''), ''),
                        coalesce(nullIf("UPC", ''), nullIf(upc, ''), ''),
                        coalesce(nullIf("GRid", ''), nullIf(grid, ''), ''),
                        coalesce(nullIf("Custom ID", ''), nullIf(custom_id, ''), ''),
                        coalesce(nullIf("Genre", ''), nullIf(genre, ''), ''),
                        coalesce(nullIf("Asset Labels", ''), nullIf(asset_labels, ''), '')
                    FROM ${tempTable}
                    WHERE coalesce(nullIf("Asset ID", ''), nullIf(asset_id, '')) != ''
                `
            });

            // Cleanup temp table
            await client.command({ query: `DROP TABLE IF EXISTS ${tempTable}` }).catch(() => {});

            cleanupFiles(tempFiles);
            return reply.send({ success: true, message: `Asset metadata for ${day} ingested successfully` });
        } catch (err: any) {
            cleanupFiles(tempFiles);
            return reply.code(500).send({ error: err.message });
        }
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/ingest/video-metadata
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/ingest/video-metadata', async (req, reply) => {
        const query = req.query as { day?: string; cms_id?: string; jobId?: string };
        const day = query.day; // Format: YYYY-MM-DD
        if (!day || !/^\d{4}-\d{2}-\d{2}$/.test(day)) {
            return reply.code(400).send({ error: 'day query param is required (format: YYYY-MM-DD)' });
        }

        let cms_id: string;
        let dbName: string;
        let client: any;

        if (req.isAdmin || req.authRole === 'org_admin') {
            const reqCmsId = query.cms_id || (req.body as any)?.cms_id;
            if (!reqCmsId) return reply.code(400).send({ error: 'cms_id is required for Admin Master Key multi-CMS uploads' });

            const memTenant = getTenantById(reqCmsId);
            if (memTenant) {
                cms_id = memTenant.cmsId;
                dbName = memTenant.dbName;
                client = memTenant.ingestClient;
            } else {
                try {
                    const defaultClient = getDefaultClient();
                    const res = await defaultClient.query({
                        query: `SELECT 1 FROM cms_registry WHERE cms_id = {cms_id: String} AND is_active = 1`,
                        query_params: { cms_id: reqCmsId },
                        format: 'JSONEachRow'
                    });
                    const rows = await res.json();
                    if ((rows as any[]).length === 0) {
                        return reply.code(404).send({ error: `CMS ${reqCmsId} not found or inactive` });
                    }
                    cms_id = reqCmsId;
                    dbName = `db_${reqCmsId.replace(/-/g, '_')}`;
                    client = getClickHouseClient({ database: dbName });
                } catch (err: any) {
                    return reply.code(500).send({ error: `DB check failed: ${err.message}` });
                }
            }
        } else {
            if (!req.tenant) return reply.code(401).send({ error: 'Unauthorized' });
            cms_id = req.tenant.cmsId;
            dbName = req.tenant.dbName;
            client = req.tenant.ingestClient;
        }

        const savedFiles = await req.saveRequestFiles({ tmpdir: UPLOAD_DIR });
        if (savedFiles.length === 0) {
            return reply.code(400).send({ error: 'No files uploaded' });
        }

        const file = savedFiles[0];
        const dest = `${file.filepath}_${file.filename}`;
        copyFileSync(file.filepath, dest);
        const tempFiles = [dest, file.filepath];

        try {
            console.log(`  [Upload Video Metadata] Processing video metadata for day: ${day}, CMS: ${cms_id}...`);
            
            // Step 1: Idempotency deletion
            await client.command({
                query: `ALTER TABLE youtube_video_metadata DELETE WHERE cms_id = {cms_id: String} AND day = {day: Date} SETTINGS mutations_sync = 1`,
                query_params: { cms_id, day }
            }).catch((e: any) => console.warn(`[Video Metadata Ingest] Delete warning: ${e.message}`));

            // Step 2: Stream CSV to a temp table
            const tempTable = `temp_video_metadata_${uuidv4().replace(/-/g, '_')}`;
            await client.command({
                query: `
                    CREATE TABLE ${tempTable} (
                        "Video ID"             String DEFAULT '',
                        "video_id"             String DEFAULT '',
                        "Video Title"          String DEFAULT '',
                        "video_title"          String DEFAULT '',
                        "Channel ID"           String DEFAULT '',
                        "channel_id"           String DEFAULT '',
                        "Channel Display Name" String DEFAULT '',
                        "channel_display_name" String DEFAULT '',
                        "Video Length"         String DEFAULT '',
                        "video_length"         String DEFAULT '',
                        "Category"             String DEFAULT '',
                        "category"             String DEFAULT '',
                        "Asset ID"             String DEFAULT '',
                        "asset_id"             String DEFAULT '',
                        "Custom ID"            String DEFAULT '',
                        "custom_id"            String DEFAULT '',
                        "ISRC"                 String DEFAULT '',
                        "isrc"                 String DEFAULT '',
                        "Content Type"         String DEFAULT '',
                        "content_type"         String DEFAULT ''
                    ) ENGINE = StripeLog()
                `
            });

            const stream = await extractFileStream(dest);
            const delimiter = await detectDelimiterFromFile(dest);
            await client.insert({
                table: tempTable,
                values: stream,
                format: delimiter === '\t' ? 'TabSeparatedWithNames' : 'CSVWithNames',
                clickhouse_settings: {
                    input_format_skip_unknown_fields: 1
                }
            });

            // Step 3: Insert into final table youtube_video_metadata
            await client.command({
                query: `
                    INSERT INTO youtube_video_metadata (
                        cms_id, day, video_id, video_title, channel_id, channel_display_name, video_length_sec, category, asset_id, custom_id, isrc, content_type
                    )
                    SELECT
                        '${cms_id}',
                        toDate('${day}'),
                        coalesce(nullIf("Video ID", ''), nullIf(video_id, '')),
                        coalesce(nullIf("Video Title", ''), nullIf(video_title, ''), ''),
                        coalesce(nullIf("Channel ID", ''), nullIf(channel_id, ''), ''),
                        coalesce(nullIf("Channel Display Name", ''), nullIf(channel_display_name, ''), ''),
                        toInt32OrZero(coalesce(nullIf("Video Length", ''), nullIf(video_length, ''), '0')),
                        coalesce(nullIf("Category", ''), nullIf(category, ''), ''),
                        coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), ''),
                        coalesce(nullIf("Custom ID", ''), nullIf(custom_id, ''), ''),
                        coalesce(nullIf("ISRC", ''), nullIf(isrc, ''), ''),
                        coalesce(nullIf("Content Type", ''), nullIf(content_type, ''), '')
                    FROM ${tempTable}
                    WHERE coalesce(nullIf("Video ID", ''), nullIf(video_id, '')) != ''
                `
            });

            // Cleanup temp table
            await client.command({ query: `DROP TABLE IF EXISTS ${tempTable}` }).catch(() => {});

            cleanupFiles(tempFiles);
            return reply.send({ success: true, message: `Video metadata for ${day} ingested successfully` });
        } catch (err: any) {
            cleanupFiles(tempFiles);
            return reply.code(500).send({ error: err.message });
        }
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/ingest/subscription
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/ingest/subscription', async (req, reply) => {
        const query = req.query as { month?: string; cms_id?: string; batchSize?: string; jobId?: string; us_tax_rate?: string };
        const month = query.month;
        const parsedBatchSize = query.batchSize ? parseInt(query.batchSize, 10) : undefined;
        if (!month || !/^\d{6}$/.test(month)) {
            return reply.code(400).send({ error: 'month query param is required (format: YYYYMM)' });
        }

        let cms_id: string;
        let dbName: string;
        let client: any;

        if (req.isAdmin || req.authRole === 'org_admin') {
            const reqCmsId = query.cms_id || (req.body as any)?.cms_id;
            if (!reqCmsId) return reply.code(400).send({ error: 'cms_id is required for Admin Master Key multi-CMS uploads' });

            const memTenant = getTenantById(reqCmsId);
            if (memTenant) {
                cms_id = memTenant.cmsId;
                dbName = memTenant.dbName;
                client = memTenant.ingestClient;
            } else {
                try {
                    const defaultClient = getDefaultClient();
                    const res = await defaultClient.query({
                        query: `SELECT 1 FROM cms_registry WHERE cms_id = {cms_id: String} AND is_active = 1`,
                        query_params: { cms_id: reqCmsId },
                        format: 'JSONEachRow'
                    });
                    const rows = await res.json();
                    if ((rows as any[]).length === 0) {
                        try {
                            const newDbName = `db_${reqCmsId.replace(/-/g, '_')}`;
                            const apiKeyHash = createHash('sha256').update(uuidv4()).digest('hex');

                            await defaultClient.command({ query: `CREATE DATABASE IF NOT EXISTS ${newDbName}` });
                            const cmsClient = getClickHouseClient({ database: newDbName });

                            for (const ddl of CMS_DDL) {
                                await cmsClient.command({ query: ddl });
                            }

                            await defaultClient.insert({
                                table: 'cms_registry',
                                values: [{
                                    cms_id: reqCmsId,
                                    cms_name: reqCmsId,
                                    db_name: newDbName,
                                    api_key_hash: apiKeyHash,
                                    is_active: 1,
                                    org_id: (req as any).orgId || 'default_org'
                                }],
                                format: 'JSONEachRow',
                            });
                            console.log(`[Auto-Provision] Created ClickHouse DB and tables for Sub CMS: ${reqCmsId}`);
                        } catch (createErr: any) {
                            return reply.code(500).send({ error: `Auto-provisioning failed: ${createErr.message}` });
                        }
                    }

                    cms_id = reqCmsId;
                    dbName = `db_${reqCmsId.replace(/-/g, '_')}`;
                    client = getClickHouseClient({ database: dbName });
                } catch (err: any) {
                    return reply.code(500).send({ error: `DB check failed: ${err.message}` });
                }
            }
        } else {
            if (!req.tenant) return reply.code(401).send({ error: 'Unauthorized' });
            cms_id = req.tenant.cmsId;
            dbName = req.tenant.dbName;
            client = req.tenant.ingestClient;
        }

        const jobId = query.jobId || uuidv4();

        const lockKey = `subscription:${cms_id}:${month}`;
        if (activeLocks.has(lockKey)) {
            return reply.code(409).send({ error: `Sync for CMS ${cms_id} and Month ${month} is already running (Job ID: ${activeLocks.get(lockKey)}).` });
        }
        activeLocks.set(lockKey, jobId);

        const targetJobType = (req.query as any).job_type || 'subscription';
        let tempFiles: string[] = [];
        let files: Record<string, string> = {};
        let savedFilesList: any[] = [];

        try {
            console.log(`\n[Sub Upload] Saving request files...`);
            const savedFiles = await req.saveRequestFiles({ tmpdir: UPLOAD_DIR });
            savedFilesList = savedFiles;
            console.log(`[Sub Upload] Saved ${savedFiles.length} file(s) from request`);

            for (const f of savedFiles) {
                const dest = `${f.filepath}_${f.filename}`;
                console.log(`[Sub Upload] Copying ${f.filename} (${f.filepath})...`);
                copyFileSync(f.filepath, dest);
                files[f.fieldname] = dest;
                tempFiles.push(dest);
                tempFiles.push(f.filepath);
                const size = statSync(dest).size;
                console.log(`  [Upload] ${f.fieldname}: ${f.filename} (${(size / 1024 / 1024).toFixed(1)} MB)`);
            }

            // Check archive fallback for missing files
            const defaultCh = getDefaultClient();
            if (!files.subscription) {
                const archiveRes = await defaultCh.query({
                    query: `SELECT file_path FROM default.ingested_files_archive WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND file_type = 'subscription'`,
                    query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                    format: 'JSONEachRow'
                });
                const rows = await archiveRes.json() as { file_path: string }[];
                if (rows.length > 0 && existsSync(rows[0].file_path)) {
                    files.subscription = rows[0].file_path;
                    console.log(`  [Fallback] Field subscription falling back to archived file: ${rows[0].file_path}`);
                }
            }

            if (!files.adj_red_label) {
                const archiveRes = await defaultCh.query({
                    query: `SELECT file_path FROM default.ingested_files_archive WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND file_type = 'subscription_adjustment'`,
                    query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                    format: 'JSONEachRow'
                });
                const rows = await archiveRes.json() as { file_path: string }[];
                if (rows.length > 0 && existsSync(rows[0].file_path)) {
                    files.adj_red_label = rows[0].file_path;
                    console.log(`  [Fallback] Field adj_red_label falling back to archived file: ${rows[0].file_path}`);
                }
            }

            if (!files.shorts_subs) {
                const archiveRes = await defaultCh.query({
                    query: `SELECT file_path FROM default.ingested_files_archive WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND file_type = 'shorts_subs'`,
                    query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                    format: 'JSONEachRow'
                });
                const rows = await archiveRes.json() as { file_path: string }[];
                if (rows.length > 0 && existsSync(rows[0].file_path)) {
                    files.shorts_subs = rows[0].file_path;
                    console.log(`  [Fallback] Field shorts_subs falling back to archived file: ${rows[0].file_path}`);
                }
            }

            if (!files.affiliate_tax) {
                const archiveRes = await defaultCh.query({
                    query: `SELECT file_path FROM default.ingested_files_archive WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND file_type = 'affiliate_tax'`,
                    query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                    format: 'JSONEachRow'
                });
                const rows = await archiveRes.json() as { file_path: string }[];
                if (rows.length > 0 && existsSync(rows[0].file_path)) {
                    files.affiliate_tax = rows[0].file_path;
                    console.log(`  [Fallback] Field affiliate_tax falling back to archived file: ${rows[0].file_path}`);
                }
            }

            if (!files.subscription) {
                cleanupFiles(tempFiles);
                activeLocks.delete(lockKey);
                return reply.code(400).send({ error: 'Required file: subscription' });
            }

            // Create/Update job globally BEFORE responding 202
            await updateJob(jobId, {
                job_type: targetJobType, cms_id, status: 'processing', month: parseInt(month),
            });
        } catch (err: any) {
            cleanupFiles(tempFiles);
            activeLocks.delete(lockKey);
            return reply.code(500).send({ error: `Failed to initialize ingestion job: ${err.message}` });
        }

        // Safely parse clearLog query parameter (handles string "false" or boolean false)
        const qClearLog = (req.query as any).clearLog;
        const clearLog = qClearLog !== 'false' && qClearLog !== false && qClearLog !== 0;
        initJobLog(jobId, clearLog);

        reply.code(202).send({ job_id: jobId, status: 'processing', month });

        // Register job globally so interval polling doesn't misidentify it as a crash zombie
        runningJobs.add(jobId);

        // Background processing
        setImmediate(async () => {
            const log = (msg: string) => emitLog(jobId, msg);
            let finalStatus = 'processing';
            try {
                // Archive files before processing
                const fileFields = ['subscription', 'adj_red_label', 'shorts_subs', 'affiliate_tax'];
                for (const field of fileFields) {
                    if (files[field]) {
                        const orig = savedFilesList.find(sf => sf.fieldname === field);
                        if (orig) {
                            await archiveUploadedFile({
                                cmsId: cms_id,
                                month,
                                fileType: field === 'subscription' 
                                    ? 'subscription' 
                                    : field === 'adj_red_label' 
                                        ? 'subscription_adjustment' 
                                        : field === 'shorts_subs'
                                            ? 'shorts_subs'
                                            : 'affiliate_tax',
                                tempPath: files[field],
                                originalFilename: orig.filename,
                                log
                            });
                        }
                    }
                }

                const startTime = Date.now();
                const fileSize = (statSync(files.subscription).size / 1024 / 1024).toFixed(1);
                log(`🚀 Starting Subscription ingestion for month ${month}`);
                log(`📁 File: ${files.subscription.split(/[/\\]/).pop()} (${fileSize} MB)`);

                log(`[Step 1/2] Dropping SUB partition ${month} for CMS ${cms_id}...`);
                await client.command({
                    query: `ALTER TABLE subscription_revenue DROP PARTITION ({cmsId: String}, {partitionMonth: UInt32})`,
                    query_params: { cmsId: cms_id, partitionMonth: parseInt(month, 10) }
                }).catch((e: any) => {
                    log(`[Step 1/2] Partition drop warning: ${e.message}`);
                });
                log(`[Step 1/2] ✓ Partition dropped`);

                let lastUpdate = 0;
                let totalAllRows = 0;
                const breakdown: string[] = [];

                // Load channel mappings from uploaded file (if provided)
                const channelMap = new Map<string, string>();
                if (files.channelMappings && existsSync(files.channelMappings)) {
                    try {
                        const raw = readFileSync(files.channelMappings, 'utf-8');
                        const parsed = JSON.parse(raw);
                        for (const [k, v] of Object.entries(parsed)) channelMap.set(k, v as string);
                        log(`[Step 2/2] Loaded ${channelMap.size} Channel Mappings from uploaded file`);
                    } catch (e: any) { log(`[Step 2/2] Error loading mappings file: ${e.message}`); }
                } else {
                    log(`[Step 2/2] No mappings file uploaded. Mappings will remain empty.`);
                }

                log(`[Step 2/2] Processing subscription data...`);
                // Ingest files directly into ClickHouse Staging Tables and run SQL Join
                const usTaxRate = parseFloat(query.us_tax_rate || '10') || 10.0;
                const ingestResult = await runSubscriptionIngestionDirect({
                    jobId,
                    month,
                    cmsId: cms_id,
                    usTaxRate,
                    files: {
                        subscription: files.subscription,
                        adj_red_label: files.adj_red_label,
                        shorts_subs: files.shorts_subs,
                        affiliate_tax: files.affiliate_tax
                    },
                    channelMap,
                    client,
                    isAborted: () => activeJobTokens.has(jobId),
                    log
                });

                totalAllRows = ingestResult.totalAllRows;
                breakdown.push(`Subscription Revenue: ${ingestResult.subRows.toLocaleString()}`);
                if (files.adj_red_label) {
                    breakdown.push(`Sub Adjustment: ${ingestResult.adjSubRows.toLocaleString()}`);
                }
                if (files.shorts_subs) {
                    breakdown.push(`Shorts Subs: ${ingestResult.shortsSubRows.toLocaleString()}`);
                }

                // Query total sub revenues from clickhouse subscription table
                log(`[Step 2/2] Querying calculated subscription revenue totals from ClickHouse...`);
                const subRevRes = await client.query({
                    query: `SELECT sum(partner_rev_total) as total_rev FROM subscription_revenue WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND (report_type = 'subscription' OR (report_type = '' AND (adjustment_type = '' OR adjustment_type = 'None') AND claim_type != ''))`,
                    query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                    format: 'JSONEachRow'
                });
                const subRevRows = await subRevRes.json() as any[];
                const subRevenue = subRevRows[0]?.total_rev ? parseFloat(subRevRows[0].total_rev) : 0.0;

                let adjSubRevenue = 0.0;
                if (files.adj_red_label) {
                    const adjSubRevRes = await client.query({
                        query: `SELECT sum(partner_rev_total) as total_rev FROM subscription_revenue WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND (report_type = 'sub_adjustment' OR (report_type = '' AND adjustment_type != '' AND adjustment_type != 'None'))`,
                        query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                        format: 'JSONEachRow'
                    });
                    const adjSubRevRows = await adjSubRevRes.json() as any[];
                    adjSubRevenue = adjSubRevRows[0]?.total_rev ? parseFloat(adjSubRevRows[0].total_rev) : 0.0;
                }

                let shortsSubRevenue = 0.0;
                if (files.shorts_subs) {
                    const shortsSubRevRes = await client.query({
                        query: `SELECT sum(partner_rev_total) as total_rev FROM subscription_revenue WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND (report_type = 'shorts_subs' OR (report_type = '' AND (adjustment_type = '' OR adjustment_type = 'None') AND claim_type = ''))`,
                        query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                        format: 'JSONEachRow'
                    });
                    const shortsSubRevRows = await shortsSubRevRes.json() as any[];
                    shortsSubRevenue = shortsSubRevRows[0]?.total_rev ? parseFloat(shortsSubRevRows[0].total_rev) : 0.0;
                }

                const elapsed = Math.round((Date.now() - startTime) / 1000);
                const durStr = elapsed >= 60 ? `${Math.floor(elapsed / 60)}m ${elapsed % 60}s` : `${elapsed}s`;
                breakdown.push(`Duration: ${durStr}`);
                const detailStr = breakdown.join(' | ');
                finalStatus = 'completed';

                let finalMsg = `[Sub] ✅ ${totalAllRows.toLocaleString()} rows in ${durStr}`;
                
                // Query total us tax and net revenue for Subscription
                const taxAndNetRes = await client.query({
                    query: `SELECT sum(us_tax) as tax_total, sum(net_revenue) as net_total FROM subscription_revenue WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32}`,
                    query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                    format: 'JSONEachRow'
                });
                const taxAndNetRows = await taxAndNetRes.json() as any[];
                const subUsTax = taxAndNetRows[0]?.tax_total ? parseFloat(taxAndNetRows[0].tax_total) : 0.0;
                const subNetRevenue = taxAndNetRows[0]?.net_total ? parseFloat(taxAndNetRows[0].net_total) : 0.0;

                let finalUsTax = subUsTax;
                let finalNetRevenue = subNetRevenue;
                let finalAdsRows = 0;

                try {
                    const defaultCh = getDefaultClient();
                    const existingRes = await defaultCh.query({
                        query: `SELECT started_at, error_message, ads_rows, us_tax, net_revenue FROM ingestion_jobs WHERE job_id = {jobId: String} ORDER BY updated_at DESC, indexOf(['pending', 'checking', 'processing', 'failed', 'completed'], status) DESC, length(detail_logs) DESC LIMIT 1`,
                        query_params: { jobId },
                        format: 'JSONEachRow'
                    });
                    const rows = await existingRes.json() as any[];
                    if (rows.length > 0) {
                        const existing = rows[0];
                        finalAdsRows = parseInt(existing.ads_rows || '0');
                        
                        const existingUsTax = parseFloat(existing.us_tax || '0');
                        const existingNetRevenue = parseFloat(existing.net_revenue || '0');
                        finalUsTax += existingUsTax;
                        finalNetRevenue += existingNetRevenue;

                        if (existing.error_message && existing.error_message.includes('[Ads]')) {
                            const totalTime = Math.round((Date.now() - new Date(existing.started_at.replace(' ', 'T') + 'Z').getTime()) / 1000);
                            const totalTimeStr = totalTime >= 60 ? `${Math.floor(totalTime / 60)}m ${totalTime % 60}s` : `${totalTime}s`;
                            finalMsg = `${existing.error_message} | [Sub] ✅ ${totalAllRows.toLocaleString()} rows in ${durStr} | Total Time: ${totalTimeStr}`;
                        }
                    }
                } catch (e) {
                    console.error("Failed to merge job logs", e);
                }

                await updateJob(jobId, {
                    job_type: targetJobType, cms_id, status: 'completed',
                    month: parseInt(month),
                    total_rows: finalAdsRows + totalAllRows, processed_rows: finalAdsRows + totalAllRows,
                    sub_rows: ingestResult.subRows,
                    adj_sub_rows: ingestResult.adjSubRows,
                    shorts_sub_rows: ingestResult.shortsSubRows || 0,
                    sub_revenue: subRevenue,
                    adj_sub_revenue: adjSubRevenue,
                    shorts_sub_revenue: shortsSubRevenue,
                    us_tax: finalUsTax,
                    net_revenue: finalNetRevenue,
                    error_message: finalMsg,
                    completed_at: new Date().toISOString().replace('T', ' ').slice(0, 19),
                });

                log(`✅ Completed! ${totalAllRows.toLocaleString()} total rows in ${durStr}`);
            } catch (err: any) {
                finalStatus = 'failed';
                emitLog(jobId, `❌ Subscription Failed: ${err.message}`);
                console.error(err.stack || err);
                await client.command({
                    query: `ALTER TABLE subscription_revenue DROP PARTITION ({cmsId: String}, {partitionMonth: UInt32})`,
                    query_params: { cmsId: cms_id, partitionMonth: parseInt(month, 10) }
                }).catch(() => { });
                const targetJobType = (req.query as any).job_type || 'subscription';
                await updateJob(jobId, {
                    job_type: targetJobType, cms_id, status: 'failed',
                    month: parseInt(month), error_message: err.message,
                }).catch(() => { });
            } finally {
                activeLocks.delete(lockKey);
                runningJobs.delete(jobId);
                cleanupFiles(tempFiles);
                const finalLogs = getJobLogs(jobId);

                // Overwrite detail_logs entirely with the accumulated events from Memory and preserve final status
                await updateJob(jobId, { status: finalStatus, detail_logs: JSON.stringify(finalLogs) }).catch(() => { });

                // Wait 10s before closing terminal so Frontend finishes catching up
                setTimeout(() => completeJobLog(jobId), 10000);
            }
        });
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/ingest/paid-features — Upload YouTube Paid Features CSV report
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/ingest/paid-features', async (req, reply) => {
        const query = req.query as { jobId?: string; month?: string; cms_id?: string; us_tax_rate?: string; clearLog?: string };
        const month = query.month;
        if (!month) return reply.code(400).send({ error: 'month parameter is required (YYYYMM)' });

        let cms_id: string;
        let dbName: string;
        let client: any;

        if (req.isAdmin || req.authRole === 'org_admin') {
            const reqCmsId = query.cms_id || (req.body as any)?.cms_id;
            if (!reqCmsId) return reply.code(400).send({ error: 'cms_id is required for Admin Master Key multi-CMS uploads' });

            const memTenant = getTenantById(reqCmsId);
            if (memTenant) {
                cms_id = memTenant.cmsId;
                dbName = memTenant.dbName;
                client = memTenant.ingestClient;
            } else {
                try {
                    const defaultClient = getDefaultClient();
                    const res = await defaultClient.query({
                        query: `SELECT 1 FROM cms_registry WHERE cms_id = {cms_id: String} AND is_active = 1`,
                        query_params: { cms_id: reqCmsId },
                        format: 'JSONEachRow'
                    });
                    const rows = await res.json();
                    if ((rows as any[]).length === 0) {
                        try {
                            const newDbName = `db_${reqCmsId.replace(/-/g, '_')}`;
                            const apiKeyHash = createHash('sha256').update(uuidv4()).digest('hex');

                            await defaultClient.command({ query: `CREATE DATABASE IF NOT EXISTS ${newDbName}` });
                            const cmsClient = getClickHouseClient({ database: newDbName });

                            for (const ddl of CMS_DDL) {
                                await cmsClient.command({ query: ddl });
                            }

                            await defaultClient.insert({
                                table: 'cms_registry',
                                values: [{
                                    cms_id: reqCmsId,
                                    cms_name: reqCmsId,
                                    db_name: newDbName,
                                    api_key_hash: apiKeyHash,
                                    is_active: 1,
                                    org_id: (req as any).orgId || 'default_org'
                                }],
                                format: 'JSONEachRow',
                            });
                            console.log(`[Auto-Provision] Created ClickHouse DB and tables for Paid Features CMS: ${reqCmsId}`);
                        } catch (createErr: any) {
                            return reply.code(500).send({ error: `Auto-provisioning failed: ${createErr.message}` });
                        }
                    }

                    cms_id = reqCmsId;
                    dbName = `db_${reqCmsId.replace(/-/g, '_')}`;
                    client = getClickHouseClient({ database: dbName });
                } catch (err: any) {
                    return reply.code(500).send({ error: `DB check failed: ${err.message}` });
                }
            }
        } else {
            if (!req.tenant) return reply.code(401).send({ error: 'Unauthorized' });
            cms_id = req.tenant.cmsId;
            dbName = req.tenant.dbName;
            client = req.tenant.ingestClient;
        }

        const jobId = query.jobId || uuidv4();
        const lockKey = `paid-features:${cms_id}:${month}`;
        if (activeLocks.has(lockKey)) {
            return reply.code(409).send({ error: `Paid Features upload for CMS ${cms_id} and Month ${month} is already running.` });
        }
        activeLocks.set(lockKey, jobId);

        let tempFiles: string[] = [];
        let files: Record<string, string> = {};
        let savedFilesList: any[] = [];

        try {
            const savedFiles = await req.saveRequestFiles({ tmpdir: UPLOAD_DIR });
            savedFilesList = savedFiles;
            for (const f of savedFiles) {
                const dest = `${f.filepath}_${f.filename}`;
                copyFileSync(f.filepath, dest);
                files[f.fieldname] = dest;
                tempFiles.push(dest);
                tempFiles.push(f.filepath);
                console.log(`  [Upload Paid Features] ${f.fieldname}: ${f.filename} (${(statSync(dest).size / 1024 / 1024).toFixed(1)} MB)`);
            }

            // Check archive fallback for missing files
            const defaultCh = getDefaultClient();
            if (!files.paid_features) {
                const archiveRes = await defaultCh.query({
                    query: `SELECT file_path FROM default.ingested_files_archive WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND file_type = 'paid_features'`,
                    query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                    format: 'JSONEachRow'
                });
                const rows = await archiveRes.json() as { file_path: string }[];
                if (rows.length > 0 && existsSync(rows[0].file_path)) {
                    files.paid_features = rows[0].file_path;
                    console.log(`  [Fallback] Field paid_features falling back to archived file: ${rows[0].file_path}`);
                }
            }

            if (!files.paid_features) {
                cleanupFiles(tempFiles);
                activeLocks.delete(lockKey);
                return reply.code(400).send({ error: 'Required file: paid_features' });
            }

            // Create/Update job globally
            await updateJob(jobId, {
                job_type: 'ads_revenue', cms_id, status: 'processing', month: parseInt(month),
            });
        } catch (err: any) {
            cleanupFiles(tempFiles);
            activeLocks.delete(lockKey);
            return reply.code(500).send({ error: `Failed to initialize ingestion job: ${err.message}` });
        }

        reply.code(202).send({ job_id: jobId, status: 'processing', month });

        const qClearLog = (req.query as any).clearLog;
        const clearLog = qClearLog !== 'false' && qClearLog !== false && qClearLog !== 0;
        initJobLog(jobId, clearLog);

        runningJobs.add(jobId);

        setImmediate(async () => {
            const log = (msg: string) => emitLog(jobId, msg);
            let finalStatus = 'processing';
            try {
                // Archive files before processing
                if (files.paid_features) {
                    const orig = savedFilesList.find(sf => sf.fieldname === 'paid_features');
                    if (orig) {
                        await archiveUploadedFile({
                            cmsId: cms_id,
                            month,
                            fileType: 'paid_features',
                            tempPath: files.paid_features,
                            originalFilename: orig.filename,
                            log
                        });
                    }
                }

                const startTime = Date.now();
                log(`🚀 Starting Paid Features ETL for month ${month}`);

                const usTaxRate = parseFloat(query.us_tax_rate || '10');
                const result = await runPaidFeaturesIngestionDirect({
                    jobId,
                    month,
                    cmsId: cms_id,
                    filePath: files.paid_features,
                    usTaxRate,
                    client,
                    isAborted: () => activeJobTokens.has(jobId),
                    log
                });

                const elapsed = Math.round((Date.now() - startTime) / 1000);
                const durStr = elapsed >= 60 ? `${Math.floor(elapsed / 60)}m ${elapsed % 60}s` : `${elapsed}s`;
                finalStatus = 'completed';

                // Merge combined US Tax and Net Revenue from other tasks in this job
                let finalUsTax = result.usTax;
                let finalNetRevenue = result.netRevenue;
                let finalAdsRows = 0;
                let finalSubRows = 0;
                let finalShortsAdsRows = 0;
                let finalShortsSubRows = 0;
                let finalAdsRevenue = 0.0;
                let finalSubRevenue = 0.0;
                let finalAdjAdsRevenue = 0.0;
                let finalAdjSubRevenue = 0.0;
                let finalShortsAdsRevenue = 0.0;
                let finalShortsSubRevenue = 0.0;

                let finalMsg = `[Paid Features] ✅ ${result.paidRows.toLocaleString()} rows in ${durStr}`;

                try {
                    const defaultCh = getDefaultClient();
                    const existingRes = await defaultCh.query({
                        query: `SELECT started_at, error_message, ads_rows, sub_rows, shorts_ads_rows, shorts_sub_rows, ads_revenue, sub_revenue, adj_ads_revenue, adj_sub_revenue, shorts_ads_revenue, shorts_sub_revenue, us_tax, net_revenue FROM ingestion_jobs WHERE job_id = {jobId: String} ORDER BY updated_at DESC, indexOf(['pending', 'checking', 'processing', 'failed', 'completed'], status) DESC, length(detail_logs) DESC LIMIT 1`,
                        query_params: { jobId },
                        format: 'JSONEachRow'
                    });
                    const rows = await existingRes.json() as any[];
                    if (rows.length > 0) {
                        const existing = rows[0];
                        finalAdsRows = parseInt(existing.ads_rows || '0');
                        finalSubRows = parseInt(existing.sub_rows || '0');
                        finalShortsAdsRows = parseInt(existing.shorts_ads_rows || '0');
                        finalShortsSubRows = parseInt(existing.shorts_sub_rows || '0');
                        finalAdsRevenue = parseFloat(existing.ads_revenue || '0');
                        finalSubRevenue = parseFloat(existing.sub_revenue || '0');
                        finalAdjAdsRevenue = parseFloat(existing.adj_ads_revenue || '0');
                        finalAdjSubRevenue = parseFloat(existing.adj_sub_revenue || '0');
                        finalShortsAdsRevenue = parseFloat(existing.shorts_ads_revenue || '0');
                        finalShortsSubRevenue = parseFloat(existing.shorts_sub_revenue || '0');

                        const existingUsTax = parseFloat(existing.us_tax || '0');
                        const existingNetRevenue = parseFloat(existing.net_revenue || '0');
                        finalUsTax += existingUsTax;
                        finalNetRevenue += existingNetRevenue;

                        if (existing.error_message) {
                            finalMsg = `${existing.error_message} | [Paid Features] ✅ ${result.paidRows.toLocaleString()} rows in ${durStr}`;
                        }
                    }
                } catch (e) {
                    console.error("Failed to merge job logs", e);
                }

                await updateJob(jobId, {
                    job_type: 'ads_revenue', cms_id, status: 'completed',
                    month: parseInt(month),
                    total_rows: finalAdsRows + finalSubRows + finalShortsAdsRows + finalShortsSubRows + result.paidRows,
                    processed_rows: finalAdsRows + finalSubRows + finalShortsAdsRows + finalShortsSubRows + result.paidRows,
                    paid_features_rows: result.paidRows,
                    paid_revenue: result.paidRevenue,
                    us_tax: finalUsTax,
                    net_revenue: finalNetRevenue,
                    error_message: finalMsg,
                    completed_at: new Date().toISOString().replace('T', ' ').slice(0, 19),
                });

                log(`✅ Completed! ${result.paidRows.toLocaleString()} paid features rows in ${durStr}`);
            } catch (err: any) {
                finalStatus = 'failed';
                emitLog(jobId, `❌ Paid Features Ingestion Failed: ${err.message}`);
                console.error(err.stack || err);
                await client.command({
                    query: `ALTER TABLE paid_features_raw DROP PARTITION ({cmsId: String}, {partitionMonth: UInt32})`,
                    query_params: { cmsId: cms_id, partitionMonth: parseInt(month, 10) }
                }).catch(() => { });
                await updateJob(jobId, {
                    job_type: 'ads_revenue', cms_id, status: 'failed',
                    month: parseInt(month), error_message: err.message,
                }).catch(() => { });
            } finally {
                activeLocks.delete(lockKey);
                runningJobs.delete(jobId);
                cleanupFiles(tempFiles);
                const finalLogs = getJobLogs(jobId);
                await updateJob(jobId, { status: finalStatus, detail_logs: JSON.stringify(finalLogs) }).catch(() => { });
                setTimeout(() => completeJobLog(jobId), 10000);
            }
        });
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/ingest/audio-tier
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/ingest/audio-tier', async (req, reply) => {
        const query = req.query as { month?: string; cms_id?: string; jobId?: string; us_tax_rate?: string };
        const month = query.month;
        if (!month || !/^\d{6}$/.test(month)) {
            return reply.code(400).send({ error: 'month query param is required (format: YYYYMM)' });
        }

        let cms_id: string;
        let dbName: string;
        let client: any;

        if (req.isAdmin || req.authRole === 'org_admin') {
            const reqCmsId = query.cms_id || (req.body as any)?.cms_id;
            if (!reqCmsId) return reply.code(400).send({ error: 'cms_id is required' });

            const memTenant = getTenantById(reqCmsId);
            if (memTenant) {
                cms_id = memTenant.cmsId;
                dbName = memTenant.dbName;
                client = memTenant.ingestClient;
            } else {
                try {
                    const defaultClient = getDefaultClient();
                    const res = await defaultClient.query({
                        query: `SELECT 1 FROM cms_registry WHERE cms_id = {cms_id: String} AND is_active = 1`,
                        query_params: { cms_id: reqCmsId },
                        format: 'JSONEachRow'
                    });
                    const rows = await res.json();
                    if ((rows as any[]).length === 0) {
                        try {
                            const newDbName = `db_${reqCmsId.replace(/-/g, '_')}`;
                            const apiKeyHash = createHash('sha256').update(uuidv4()).digest('hex');

                            await defaultClient.command({ query: `CREATE DATABASE IF NOT EXISTS ${newDbName}` });
                            const cmsClient = getClickHouseClient({ database: newDbName });

                            for (const ddl of CMS_DDL) {
                                await cmsClient.command({ query: ddl });
                            }

                            await defaultClient.insert({
                                table: 'cms_registry',
                                values: [{
                                    cms_id: reqCmsId,
                                    cms_name: reqCmsId,
                                    db_name: newDbName,
                                    api_key_hash: apiKeyHash,
                                    is_active: 1,
                                    org_id: (req as any).orgId || 'default_org'
                                }],
                                format: 'JSONEachRow',
                            });
                            console.log(`[Auto-Provision] Created ClickHouse DB and tables for Audio Tier CMS: ${reqCmsId}`);
                        } catch (createErr: any) {
                            return reply.code(500).send({ error: `Auto-provisioning failed: ${createErr.message}` });
                        }
                    }

                    cms_id = reqCmsId;
                    dbName = `db_${reqCmsId.replace(/-/g, '_')}`;
                    client = getClickHouseClient({ database: dbName });
                } catch (err: any) {
                    return reply.code(500).send({ error: `DB check failed: ${err.message}` });
                }
            }
        } else {
            if (!req.tenant) return reply.code(401).send({ error: 'Unauthorized' });
            cms_id = req.tenant.cmsId;
            dbName = req.tenant.dbName;
            client = req.tenant.ingestClient;
        }

        const jobId = query.jobId || uuidv4();
        const lockKey = `audio-tier:${cms_id}:${month}`;
        if (activeLocks.has(lockKey)) {
            return reply.code(409).send({ error: `Audio Tier upload for CMS ${cms_id} and Month ${month} is already running.` });
        }
        activeLocks.set(lockKey, jobId);

        let tempFiles: string[] = [];
        let files: Record<string, string> = {};
        let savedFilesList: any[] = [];

        try {
            const savedFiles = await req.saveRequestFiles({ tmpdir: UPLOAD_DIR });
            savedFilesList = savedFiles;
            for (const f of savedFiles) {
                const dest = `${f.filepath}_${f.filename}`;
                copyFileSync(f.filepath, dest);
                files[f.fieldname] = dest;
                tempFiles.push(dest);
                tempFiles.push(f.filepath);
                console.log(`  [Upload Audio Tier] ${f.fieldname}: ${f.filename} (${(statSync(dest).size / 1024 / 1024).toFixed(1)} MB)`);
            }

            // Check archive fallback for missing files
            const defaultCh = getDefaultClient();
            if (!files.audio_tier) {
                const archiveRes = await defaultCh.query({
                    query: `SELECT file_path FROM default.ingested_files_archive WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND file_type = 'audio_tier'`,
                    query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                    format: 'JSONEachRow'
                });
                const rows = await archiveRes.json() as { file_path: string }[];
                if (rows.length > 0 && existsSync(rows[0].file_path)) {
                    files.audio_tier = rows[0].file_path;
                    console.log(`  [Fallback] Field audio_tier falling back to archived file: ${rows[0].file_path}`);
                }
            }

            if (!files.audio_tier) {
                cleanupFiles(tempFiles);
                activeLocks.delete(lockKey);
                return reply.code(400).send({ error: 'Required file: audio_tier' });
            }

            // Create/Update job globally
            await updateJob(jobId, {
                job_type: 'subscription', cms_id, status: 'processing', month: parseInt(month),
            });
        } catch (err: any) {
            cleanupFiles(tempFiles);
            activeLocks.delete(lockKey);
            return reply.code(500).send({ error: `Failed to initialize ingestion job: ${err.message}` });
        }

        reply.code(202).send({ job_id: jobId, status: 'processing', month });

        const qClearLog = (req.query as any).clearLog;
        const clearLog = qClearLog !== 'false' && qClearLog !== false && qClearLog !== 0;
        initJobLog(jobId, clearLog);

        runningJobs.add(jobId);

        setImmediate(async () => {
            const log = (msg: string) => emitLog(jobId, msg);
            let finalStatus = 'processing';
            try {
                // Archive files before processing
                if (files.audio_tier) {
                    const orig = savedFilesList.find(sf => sf.fieldname === 'audio_tier');
                    if (orig) {
                        await archiveUploadedFile({
                            cmsId: cms_id,
                            month,
                            fileType: 'audio_tier',
                            tempPath: files.audio_tier,
                            originalFilename: orig.filename,
                            log
                        });
                    }
                }

                const startTime = Date.now();
                log(`🚀 Starting Audio Tier ETL for month ${month}`);

                log(`[Audio Tier] Dropping partition ${month} for CMS ${cms_id}...`);
                await client.command({
                    query: `ALTER TABLE audio_tier_revenue DROP PARTITION ({cmsId: String}, {partitionMonth: UInt32})`,
                    query_params: { cmsId: cms_id, partitionMonth: parseInt(month, 10) }
                }).catch((e: any) => {
                    log(`[Audio Tier] Partition drop warning: ${e.message}`);
                });

                const usTaxRate = parseFloat(query.us_tax_rate || '10');
                const result = await runAudioTierIngestionDirect({
                    jobId,
                    month,
                    cmsId: cms_id,
                    filePath: files.audio_tier,
                    usTaxRate,
                    client,
                    isAborted: () => activeJobTokens.has(jobId),
                    log
                });

                const elapsed = Math.round((Date.now() - startTime) / 1000);
                const durStr = elapsed >= 60 ? `${Math.floor(elapsed / 60)}m ${elapsed % 60}s` : `${elapsed}s`;
                finalStatus = 'completed';

                // Query total calculated US Tax and Net Revenue for Audio Tier
                const taxAndNetRes = await client.query({
                    query: `SELECT sum(us_tax) as tax_total, sum(net_revenue) as net_total FROM audio_tier_revenue WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32}`,
                    query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                    format: 'JSONEachRow'
                });
                const taxAndNetRows = await taxAndNetRes.json() as any[];
                const audioUsTax = taxAndNetRows[0]?.tax_total ? parseFloat(taxAndNetRows[0].tax_total) : 0.0;
                const audioNetRevenue = taxAndNetRows[0]?.net_total ? parseFloat(taxAndNetRows[0].net_total) : 0.0;

                // Merge combined US Tax and Net Revenue from other tasks in this job
                let finalUsTax = audioUsTax;
                let finalNetRevenue = audioNetRevenue;
                let finalAdsRows = 0;
                let finalSubRows = 0;
                let finalAdjSubRows = 0;
                let finalShortsAdsRows = 0;
                let finalShortsSubRows = 0;
                let finalPaidRows = 0;
                let finalAdsRevenue = 0.0;
                let finalSubRevenue = 0.0;
                let finalAdjAdsRevenue = 0.0;
                let finalAdjSubRevenue = 0.0;
                let finalShortsAdsRevenue = 0.0;
                let finalShortsSubRevenue = 0.0;
                let finalPaidRevenue = 0.0;

                let finalMsg = `[Audio Tier] ✅ ${result.audioTierRows.toLocaleString()} rows in ${durStr}`;

                try {
                    const defaultCh = getDefaultClient();
                    const existingRes = await defaultCh.query({
                        query: `SELECT started_at, error_message, ads_rows, sub_rows, adj_sub_rows, shorts_ads_rows, shorts_sub_rows, paid_features_rows, ads_revenue, sub_revenue, adj_sub_revenue, adj_ads_revenue, shorts_ads_revenue, shorts_sub_revenue, paid_revenue, us_tax, net_revenue FROM ingestion_jobs WHERE job_id = {jobId: String} ORDER BY updated_at DESC, indexOf(['pending', 'checking', 'processing', 'failed', 'completed'], status) DESC, length(detail_logs) DESC LIMIT 1`,
                        query_params: { jobId },
                        format: 'JSONEachRow'
                    });
                    const rows = await existingRes.json() as any[];
                    if (rows.length > 0) {
                        const existing = rows[0];
                        finalAdsRows = parseInt(existing.ads_rows || '0');
                        finalSubRows = parseInt(existing.sub_rows || '0');
                        finalAdjSubRows = parseInt(existing.adj_sub_rows || '0');
                        finalShortsAdsRows = parseInt(existing.shorts_ads_rows || '0');
                        finalShortsSubRows = parseInt(existing.shorts_sub_rows || '0');
                        finalPaidRows = parseInt(existing.paid_features_rows || '0');
                        finalAdsRevenue = parseFloat(existing.ads_revenue || '0');
                        finalSubRevenue = parseFloat(existing.sub_revenue || '0');
                        finalAdjAdsRevenue = parseFloat(existing.adj_ads_revenue || '0');
                        finalAdjSubRevenue = parseFloat(existing.adj_sub_revenue || '0');
                        finalShortsAdsRevenue = parseFloat(existing.shorts_ads_revenue || '0');
                        finalShortsSubRevenue = parseFloat(existing.shorts_sub_revenue || '0');
                        finalPaidRevenue = parseFloat(existing.paid_revenue || '0');

                        const existingUsTax = parseFloat(existing.us_tax || '0');
                        const existingNetRevenue = parseFloat(existing.net_revenue || '0');
                        finalUsTax += existingUsTax;
                        finalNetRevenue += existingNetRevenue;

                        if (existing.error_message) {
                            finalMsg = `${existing.error_message} | [Audio Tier] ✅ ${result.audioTierRows.toLocaleString()} rows in ${durStr}`;
                        }
                    }
                } catch (e) {
                    console.error("Failed to merge job logs", e);
                }

                await updateJob(jobId, {
                    job_type: 'subscription', cms_id, status: 'completed',
                    month: parseInt(month),
                    total_rows: finalAdsRows + finalSubRows + finalAdjSubRows + finalShortsAdsRows + finalShortsSubRows + finalPaidRows + result.audioTierRows,
                    processed_rows: finalAdsRows + finalSubRows + finalAdjSubRows + finalShortsAdsRows + finalShortsSubRows + finalPaidRows + result.audioTierRows,
                    audio_tier_rows: result.audioTierRows,
                    audio_tier_revenue: result.audioTierRevenue,
                    us_tax: finalUsTax,
                    net_revenue: finalNetRevenue,
                    error_message: finalMsg,
                    completed_at: new Date().toISOString().replace('T', ' ').slice(0, 19),
                });

                log(`✅ Completed! ${result.audioTierRows.toLocaleString()} audio tier rows in ${durStr}`);
            } catch (err: any) {
                finalStatus = 'failed';
                emitLog(jobId, `❌ Audio Tier Ingestion Failed: ${err.message}`);
                console.error(err.stack || err);
                await client.command({
                    query: `ALTER TABLE audio_tier_revenue DROP PARTITION ({cmsId: String}, {partitionMonth: UInt32})`,
                    query_params: { cmsId: cms_id, partitionMonth: parseInt(month, 10) }
                }).catch(() => { });
                await updateJob(jobId, {
                    job_type: 'subscription', cms_id, status: 'failed',
                    month: parseInt(month), error_message: err.message,
                }).catch(() => { });
            } finally {
                activeLocks.delete(lockKey);
                runningJobs.delete(jobId);
                cleanupFiles(tempFiles);
                const finalLogs = getJobLogs(jobId);
                await updateJob(jobId, { status: finalStatus, detail_logs: JSON.stringify(finalLogs) }).catch(() => { });
                setTimeout(() => completeJobLog(jobId), 10000);
            }
        });
    });

    /**
     * POST /api/v1/ingest/publisher-usage
     * Ingest YouTube Publisher Mechanical Royalty usage reports (Subscription_Usage, AdSupport_Usage, HardwareAudioTier)
     */
    app.post('/api/v1/ingest/publisher-usage', async (req, reply) => {
        const query = req.query as { month?: string; cms_id?: string; batchSize?: string; jobId?: string; clearLog?: string };
        const month = query.month;
        if (!month || !/^\d{6}$/.test(month)) {
            return reply.code(400).send({ error: 'Parameter month required (YYYYMM format)' });
        }

        const isSuperAdmin = req.isAdmin || req.authRole === 'org_admin' || (req as any).userRole === 'super_admin' || (req as any).isApiKeyAuth;
        let cms_id: string;
        let dbName: string;
        let client: any;

        const reqCmsId = query.cms_id;
        if (reqCmsId) {
            if (!isSuperAdmin) {
                return reply.code(403).send({ error: 'Specifying cms_id parameter requires Admin privilege.' });
            }

            const memTenant = getTenantById(reqCmsId);
            if (memTenant) {
                cms_id = memTenant.cmsId;
                dbName = memTenant.dbName;
                client = memTenant.ingestClient;
            } else {
                try {
                    const defaultClient = getDefaultClient();
                    const registryRes = await defaultClient.query({
                        query: `SELECT cms_id, db_name FROM cms_registry FINAL WHERE (cms_id = {cmsId: String} OR cms_id = {cleanId: String}) AND is_active = 1 LIMIT 1`,
                        query_params: { cmsId: reqCmsId, cleanId: sanitizeDbName(reqCmsId) },
                        format: 'JSONEachRow'
                    });
                    const registryRows = await registryRes.json() as { cms_id: string; db_name: string }[];
                    if (registryRows.length > 0) {
                        cms_id = registryRows[0].cms_id;
                        dbName = registryRows[0].db_name;
                        client = getClickHouseClient({ database: dbName });
                    } else {
                        const newDbName = sanitizeDbName(reqCmsId);
                        const apiKeyHash = createHash('sha256').update(uuidv4()).digest('hex');
                        await defaultClient.command({ query: `CREATE DATABASE IF NOT EXISTS ${newDbName}` });
                        const cmsClient = getClickHouseClient({ database: newDbName });
                        for (const ddl of CMS_DDL) {
                            await cmsClient.command({ query: ddl });
                        }
                        await defaultClient.insert({
                            table: 'cms_registry',
                            values: [{
                                cms_id: reqCmsId,
                                cms_name: reqCmsId,
                                db_name: newDbName,
                                api_key_hash: apiKeyHash,
                                is_active: 1,
                                org_id: (req as any).orgId || 'default_org'
                            }],
                            format: 'JSONEachRow',
                        });
                        cms_id = reqCmsId;
                        dbName = newDbName;
                        client = cmsClient;
                    }
                } catch (err: any) {
                    return reply.code(500).send({ error: `DB check failed: ${err.message}` });
                }
            }
        } else {
            if (!req.tenant) return reply.code(401).send({ error: 'Unauthorized' });
            cms_id = req.tenant.cmsId;
            dbName = req.tenant.dbName;
            client = req.tenant.ingestClient;
        }

        const jobId = query.jobId || uuidv4();
        const lockKey = `publisher-usage:${cms_id}:${month}`;
        if (activeLocks.has(lockKey)) {
            return reply.code(409).send({ error: `Publisher Usage upload for CMS ${cms_id} and Month ${month} is already running.` });
        }
        activeLocks.set(lockKey, jobId);

        let tempFiles: string[] = [];
        let files: Record<string, string> = {};

        try {
            const savedFiles = await req.saveRequestFiles({ tmpdir: UPLOAD_DIR });
            for (const f of savedFiles) {
                const dest = `${f.filepath}_${f.filename}`;
                copyFileSync(f.filepath, dest);
                files[f.fieldname] = dest;
                tempFiles.push(dest);
                tempFiles.push(f.filepath);
            }

            await updateJob(jobId, {
                job_type: 'subscription', cms_id, status: 'processing', month: parseInt(month),
            });
        } catch (err: any) {
            cleanupFiles(tempFiles);
            activeLocks.delete(lockKey);
            return reply.code(500).send({ error: `Failed to initialize ingestion job: ${err.message}` });
        }

        reply.code(202).send({ job_id: jobId, status: 'processing', month });

        const qClearLog = (req.query as any).clearLog;
        const clearLog = qClearLog !== 'false' && qClearLog !== false && qClearLog !== 0;
        initJobLog(jobId, clearLog);
        runningJobs.add(jobId);

        setImmediate(async () => {
            const log = (msg: string) => emitLog(jobId, msg);
            let finalStatus = 'processing';
            try {
                const startTime = Date.now();
                log(`🚀 Starting Publisher Usage Mechanical Royalty ETL for month ${month}`);

                await client.command({
                    query: `ALTER TABLE raw_youtube_publisher_usage_reports DROP PARTITION ({cmsId: String}, {partitionMonth: UInt32})`,
                    query_params: { cmsId: cms_id, partitionMonth: parseInt(month, 10) }
                }).catch(() => {});

                const result = await runPublisherUsageIngestionDirect({
                    jobId,
                    month,
                    cmsId: cms_id,
                    files: {
                        subscription_usage: files.subscription_usage_path || files.subscription_usage,
                        adsupport_usage: files.adsupport_usage_path || files.adsupport_usage,
                        hardware_audio_tier: files.hardware_audio_tier_path || files.hardware_audio_tier
                    },
                    client,
                    isAborted: () => activeJobTokens.has(jobId),
                    log
                });

                const elapsed = Math.round((Date.now() - startTime) / 1000);
                const durStr = elapsed >= 60 ? `${Math.floor(elapsed / 60)}m ${elapsed % 60}s` : `${elapsed}s`;
                finalStatus = 'completed';

                await updateJob(jobId, {
                    job_type: 'subscription', cms_id, status: 'completed',
                    month: parseInt(month),
                    total_rows: result.totalRows,
                    processed_rows: result.totalRows,
                    completed_at: new Date().toISOString().replace('T', ' ').slice(0, 19),
                });

                log(`✅ Completed! Publisher Mechanical Usage reports ingested in ${durStr}`);
            } catch (err: any) {
                finalStatus = 'failed';
                emitLog(jobId, `❌ Publisher Usage Ingestion Failed: ${err.message}`);
                await updateJob(jobId, {
                    job_type: 'subscription', cms_id, status: 'failed',
                    month: parseInt(month), error_message: err.message,
                }).catch(() => {});
            } finally {
                activeLocks.delete(lockKey);
                runningJobs.delete(jobId);
                cleanupFiles(tempFiles);
                const finalLogs = getJobLogs(jobId);
                await updateJob(jobId, { status: finalStatus, detail_logs: JSON.stringify(finalLogs) }).catch(() => {});
                setTimeout(() => completeJobLog(jobId), 10000);
            }
        });
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/ingest/jobs — Initialize/Create a new job log entry
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/ingest/jobs', async (req, reply) => {
        const query = req.query as { jobId?: string };
        const body = req.body as { job_id: string; job_type: string; cms_id: string; status: string; month: number };
        const jobId = query.jobId || body.job_id;
        if (!jobId) return reply.code(400).send({ error: 'jobId is required' });

        const cms_id = body.cms_id;
        const status = body.status || 'processing';
        const month = body.month;

        // Clear cancel token if this job is being re-initiated/updated to an active state
        if (status === 'processing' || status === 'pending' || status === 'checking') {
            activeJobTokens.delete(jobId);
        }

        // 1. Initialize Log in Memory (SSE)
        if (status === 'processing' || status === 'checking') {
            const isFirstInit = !runningJobs.has(jobId);
            initJobLog(jobId, isFirstInit);
        }

        // 2. Insert/Update Job in ClickHouse
        const isFinished = status === 'completed' || status === 'failed';
        const updates: Record<string, any> = {
            job_type: body.job_type,
            cms_id,
            status,
            month,
            uploaded_by: (body as any).uploaded_by,
            error_message: (body as any).error_message,
            is_fallback: (body as any).is_fallback !== undefined ? (body as any).is_fallback : undefined,
            fallback_date: (body as any).fallback_date !== undefined ? (body as any).fallback_date : undefined,
            detail_logs: (body as any).detail_logs || (isFinished && getJobLogs(jobId).length > 0 ? JSON.stringify(getJobLogs(jobId)) : undefined),
        };

        // Explicitly copy/reset fields sent in body (like started_at, completed_at, total_rows, etc. for rerun)
        const fieldsToCopy = [
            'started_at', 'completed_at', 'total_rows', 'processed_rows',
            'ads_rows', 'adj_ads_rows', 'sub_rows', 'adj_sub_rows',
            'claims_rows', 'reach_rows', 'demo_rows', 'traffic_rows', 'device_rows',
            'ads_revenue', 'sub_revenue', 'is_fallback', 'fallback_date'
        ];
        for (const field of fieldsToCopy) {
            if ((body as any)[field] !== undefined) {
                updates[field] = (body as any)[field];
            }
        }

        await updateJob(jobId, updates).catch(() => {});

        // 3. Register as running job
        if (status === 'completed' || status === 'failed') {
            runningJobs.delete(jobId);
            completeJobLog(jobId);
        } else {
            runningJobs.add(jobId);
        }

        return reply.code(201).send({ success: true, job_id: jobId });
    });

    // ═════════════════════════════════════════════════════════
    // GET /api/v1/ingest/jobs/:id — Check job status
    // ═════════════════════════════════════════════════════════
    app.get('/api/v1/ingest/jobs/:id', async (req, reply) => {
        const { id } = req.params as { id: string };
        const result = await getDefaultClient().query({
            query: `SELECT * FROM ingestion_jobs WHERE job_id = {id: String} ORDER BY updated_at DESC, indexOf(['pending', 'checking', 'processing', 'failed', 'completed'], status) DESC, length(detail_logs) DESC LIMIT 1`,
            query_params: { id },
            format: 'JSONEachRow',
        });
        const rows = await result.json<Record<string, unknown>[]>();
        if (rows.length === 0) {
            return reply.code(404).send({ error: 'Job not found' });
        }

        const job = rows[0] as any;
        const updatedAtTime = job.updated_at ? new Date(job.updated_at + (job.updated_at.includes('Z') ? '' : 'Z')).getTime() : 0;
        const timeSinceUpdateMs = Date.now() - updatedAtTime;
        const isRecent = timeSinceUpdateMs < 30 * 1000; // 30 seconds buffer for ClickHouse eventual consistency

        if (job.status === 'processing' || job.status === 'pending') {
            if (job.job_type === 'estimated_ads_parent') {
                if (timeSinceUpdateMs > 15 * 60 * 1000) {
                    job.status = 'failed';
                    job.error_message = '🚨 Job timeout (Orchestration process timed out or crashed).';
                    updateJob(id, { status: 'failed', error_message: job.error_message, completed_at: new Date().toISOString() }).catch(() => { });
                }
            } else if (!runningJobs.has(id) && !isRecent) {
                // Found a Zombie job! (App B was restarted while processing)
                job.status = 'failed';
                job.error_message = '🚨 Job aborted unexpectedly (Server restarted/crashed during processing).';
                updateJob(id, { status: 'failed', error_message: job.error_message, completed_at: new Date().toISOString() }).catch(() => { });
            }
        }

        return reply.send({ data: job });
    });

    // ═════════════════════════════════════════════════════════
    // GET /api/v1/ingest/jobs/:id/preview — Get sample rows of a completed job
    // ═════════════════════════════════════════════════════════
    app.get('/api/v1/ingest/jobs/:id/preview', async (req, reply) => {
        const { id } = req.params as { id: string };
        const { table, search, startDate, endDate, limit } = req.query as { table?: string; search?: string; startDate?: string; endDate?: string; limit?: string };
        const rowLimit = limit ? Math.min(Math.max(parseInt(limit, 10) || 50, 5), 1000) : 50;

        // 1. Fetch job to verify status and retrieve metadata
        const defaultCh = getDefaultClient();
        const jobResult = await defaultCh.query({
            query: `SELECT job_id, job_type, cms_id, status, month FROM ingestion_jobs WHERE job_id = {id: String} ORDER BY updated_at DESC, indexOf(['pending', 'checking', 'processing', 'failed', 'completed'], status) DESC, length(detail_logs) DESC LIMIT 1`,
            query_params: { id },
            format: 'JSONEachRow',
        });
        const jobs = await jobResult.json() as any[];
        if (jobs.length === 0) {
            return reply.code(404).send({ error: 'Job not found' });
        }
        
        const job = jobs[0];
        if (job.status !== 'completed' && job.status !== 'failed' && job.status !== 'processing') {
            return reply.code(400).send({ error: `Data preview is only available for completed, failed, or processing jobs. Current status: ${job.status}` });
        }

        const { job_type, cms_id, month } = job;
        const memTenant = getTenantById(cms_id);
        const dbName = memTenant ? memTenant.dbName : `db_${cms_id.replace(/-/g, '_')}`;

        let targetTable = table || '';
        
        // Resolve default table if not provided
        if (!targetTable) {
            if (job_type === 'estimated_ads' || job_type === 'estimated_ads_parent') {
                targetTable = 'estimated_revenue_daily';
            } else if (job_type === 'ads_revenue') {
                targetTable = 'ads_revenue_enriched';
            } else if (job_type === 'subscription') {
                targetTable = 'subscription_revenue';
            }
        }

        // Validate table request against job type
        if (job_type === 'estimated_ads' || job_type === 'estimated_ads_parent') {
            const allowed = [
                'estimated_revenue_daily', 
                'video_reach_performance_daily', 
                'video_demographics_daily', 
                'video_traffic_sources_daily', 
                'video_devices_daily', 
                'mv_asset_performance_daily',
                'channel_subscribers_daily',
                'video_interactions_daily'
            ];
            if (!allowed.includes(targetTable)) {
                return reply.code(400).send({ error: `Table ${targetTable} is not valid for estimated_ads job.` });
            }
        } else if (job_type === 'ads_revenue' || job_type === 'subscription') {
            const allowed = ['ads_revenue_enriched', 'subscription_revenue', 'paid_features_raw', 'audio_tier_revenue'];
            if (!allowed.includes(targetTable)) {
                return reply.code(400).send({ error: `Table ${targetTable} is not valid for monthly revenue job.` });
            }
        } else {
            return reply.code(400).send({ error: `Preview is not supported for job type: ${job_type}` });
        }

        // Build table-specific queries
        let columns = '';
        let sortColumn = '';
        let dateFilter = '';
        const params: Record<string, any> = { cms_id };

        const isMonthlyTable = targetTable === 'ads_revenue_enriched' || targetTable === 'subscription_revenue' || targetTable === 'paid_features_raw' || targetTable === 'audio_tier_revenue';

        if (startDate && endDate) {
            dateFilter = 'day >= {startDate: String} AND day <= {endDate: String}';
            params.startDate = startDate;
            params.endDate = endDate;
        } else {
            if (isMonthlyTable) {
                dateFilter = 'upload_month = {month: UInt32}';
            } else {
                dateFilter = 'toYYYYMM(day) = {month: UInt32}';
            }
            params.month = Number(month);
        }

        if (targetTable === 'estimated_revenue_daily') {
            columns = 'day, country, asset_id, video_id, channel_id, channel_display_name, isrc, upc, grid, asset_title, video_title, artist, album, label, owned_views, (partner_rev_total - partner_rev_red) as partner_rev_ads, partner_rev_red, partner_rev_total, monetized_playbacks, ad_impressions, partner_rev_transaction, content_type, claim_type, policy, claim_origin, asset_type, multiple_claims, category, asset_labels, username, uploader, video_duration_sec';
            sortColumn = 'partner_rev_total';
        } else if (targetTable === 'video_reach_performance_daily') {
            columns = 'day, video_id, channel_id, impressions, impressions_ctr';
            sortColumn = 'impressions';
        } else if (targetTable === 'video_demographics_daily') {
            columns = 'day, video_id, channel_id, age_group, gender, views_percentage';
            sortColumn = 'views_percentage';
        } else if (targetTable === 'video_traffic_sources_daily') {
            columns = 'day, video_id, channel_id, traffic_source_type, views, watch_time_sec';
            sortColumn = 'views';
        } else if (targetTable === 'video_devices_daily') {
            columns = 'day, video_id, channel_id, device_type, operating_system, views, watch_time_sec';
            sortColumn = 'views';
        } else if (targetTable === 'channel_subscribers_daily') {
            columns = 'day, channel_id, country, subscribed_status, subscribers_gained, subscribers_lost';
            sortColumn = 'subscribers_gained';
        } else if (targetTable === 'video_interactions_daily') {
            columns = 'day, video_id, channel_id, likes, dislikes, comments, shares';
            sortColumn = 'likes';
        } else if (targetTable === 'mv_asset_performance_daily') {
            columns = 'day, asset_id, isrc, artist, asset_title, total_views, total_revenue_usd';
            sortColumn = 'total_views';
        } else if (targetTable === 'ads_revenue_enriched') {
            columns = 'day, country, asset_id, video_id, channel_id, channel_display_name, isrc, upc, grid, asset_title, video_title, artist, album, label, owned_views, partner_rev_total as partner_rev_ads, toDecimal64(0, 10) as partner_rev_red, partner_rev_total, content_type, claim_type, policy, claim_origin, asset_type, multiple_claims, category, asset_labels, username, uploader, video_duration_sec, us_tax, net_revenue';
            sortColumn = 'partner_rev_total';
        } else if (targetTable === 'subscription_revenue') {
            columns = 'day, country, asset_id, video_id, channel_id, channel_display_name, video_title, isrc, upc, grid, asset_title, artist, album, label, owned_views, toDecimal64(0, 10) as partner_rev_ads, partner_rev_total as partner_rev_red, partner_rev_total, content_type, claim_type, asset_type, asset_labels, offer, us_tax, net_revenue';
            sortColumn = 'partner_rev_total';
        } else if (targetTable === 'paid_features_raw') {
            columns = 'day, purchase_type, refund_chargeback, country, coalesce(nullIf(channel_display_name, \'\'), nullIf(channel_name, \'\'), \'\') as channel_display_name, coalesce(nullIf(channel_display_name, \'\'), nullIf(channel_name, \'\'), \'\') as channel_name, channel_id, video_id, retail_price_usd, total_tax_usd, partner_earnings_fraction, earnings_usd, us_tax, net_revenue';
            sortColumn = 'earnings_usd';
        } else if (targetTable === 'audio_tier_revenue') {
            columns = 'day, country, video_id, asset_id, asset_title, asset_labels, custom_id, isrc, upc, grid, artist, album, label, owned_views, yt_rev_total, partner_rev_pro_rata, partner_rev_per_play_min, partner_rev_total, us_tax, net_revenue';
            sortColumn = 'partner_rev_total';
        }

        let searchFilter = '';
        if (search) {
            const searchPattern = `%${search}%`;
            params.search_pattern = searchPattern;

            if (targetTable === 'estimated_revenue_daily' || targetTable === 'ads_revenue_enriched') {
                searchFilter = `AND (video_id ILIKE {search_pattern: String} OR asset_id ILIKE {search_pattern: String} OR channel_id ILIKE {search_pattern: String} OR channel_display_name ILIKE {search_pattern: String} OR video_title ILIKE {search_pattern: String} OR artist ILIKE {search_pattern: String} OR label ILIKE {search_pattern: String} OR isrc ILIKE {search_pattern: String} OR upc ILIKE {search_pattern: String})`;
            } else if (targetTable === 'subscription_revenue') {
                searchFilter = `AND (video_id ILIKE {search_pattern: String} OR asset_id ILIKE {search_pattern: String} OR channel_id ILIKE {search_pattern: String} OR channel_display_name ILIKE {search_pattern: String} OR asset_title ILIKE {search_pattern: String} OR artist ILIKE {search_pattern: String} OR label ILIKE {search_pattern: String} OR isrc ILIKE {search_pattern: String} OR upc ILIKE {search_pattern: String})`;
            } else if (targetTable === 'paid_features_raw') {
                searchFilter = `AND (video_id ILIKE {search_pattern: String} OR channel_id ILIKE {search_pattern: String} OR channel_display_name ILIKE {search_pattern: String} OR purchase_type ILIKE {search_pattern: String})`;
            } else if (targetTable === 'audio_tier_revenue') {
                searchFilter = `AND (video_id ILIKE {search_pattern: String} OR asset_id ILIKE {search_pattern: String} OR asset_title ILIKE {search_pattern: String} OR artist ILIKE {search_pattern: String} OR label ILIKE {search_pattern: String} OR isrc ILIKE {search_pattern: String} OR upc ILIKE {search_pattern: String})`;
            } else if (targetTable === 'mv_asset_performance_daily') {
                searchFilter = `AND (asset_id ILIKE {search_pattern: String} OR artist ILIKE {search_pattern: String} OR asset_title ILIKE {search_pattern: String} OR isrc ILIKE {search_pattern: String})`;
            } else if (targetTable === 'channel_subscribers_daily') {
                searchFilter = `AND (channel_id ILIKE {search_pattern: String} OR country ILIKE {search_pattern: String})`;
            } else {
                searchFilter = `AND (video_id ILIKE {search_pattern: String} OR channel_id ILIKE {search_pattern: String})`;
            }
        }

        let query = '';
        if (targetTable === 'mv_asset_performance_daily') {
            query = `SELECT day, asset_id, isrc, artist, asset_title, sum(total_views) as total_views, sum(total_revenue_usd) as total_revenue_usd 
                     FROM ${dbName}.${targetTable} 
                     WHERE cms_id = {cms_id: String} AND ${dateFilter} ${searchFilter}
                     GROUP BY day, asset_id, isrc, artist, asset_title
                     ORDER BY ${sortColumn} DESC
                     LIMIT ${rowLimit}`;
        } else {
            query = `SELECT ${columns} 
                     FROM ${dbName}.${targetTable} 
                     WHERE cms_id = {cms_id: String} AND ${dateFilter} ${searchFilter}
                     ORDER BY ${sortColumn} DESC
                     LIMIT ${rowLimit}`;
        }

        let summaryQuery = '';
        if (targetTable === 'estimated_revenue_daily') {
            summaryQuery = `SELECT sum(owned_views) as total_views, sum(partner_rev_total - partner_rev_red) as total_revenue_ads, sum(partner_rev_red) as total_revenue_red, sum(partner_rev_total) as total_revenue FROM ${dbName}.${targetTable} WHERE cms_id = {cms_id: String} AND ${dateFilter} ${searchFilter}`;
        } else if (targetTable === 'ads_revenue_enriched') {
            summaryQuery = `SELECT sum(owned_views) as total_views, sumIf(partner_rev_total, report_type = 'claim_raw') as total_revenue_ads, sumIf(partner_rev_total, report_type = 'ads_adjustment') as total_revenue_red, sum(partner_rev_total) as total_revenue FROM ${dbName}.${targetTable} WHERE cms_id = {cms_id: String} AND ${dateFilter} ${searchFilter}`;
        } else if (targetTable === 'subscription_revenue') {
            summaryQuery = `SELECT sum(owned_views) as total_views, sumIf(partner_rev_total, adjustment_type = '' OR adjustment_type = 'None') as total_revenue_ads, sumIf(partner_rev_total, adjustment_type != '' AND adjustment_type != 'None') as total_revenue_red, sum(partner_rev_total) as total_revenue FROM ${dbName}.${targetTable} WHERE cms_id = {cms_id: String} AND ${dateFilter} ${searchFilter}`;
        } else if (targetTable === 'paid_features_raw') {
            summaryQuery = `SELECT count() as total_views, sum(us_tax) as total_revenue_ads, sum(net_revenue) as total_revenue_red, sum(earnings_usd) as total_revenue FROM ${dbName}.${targetTable} WHERE cms_id = {cms_id: String} AND ${dateFilter} ${searchFilter}`;
        } else if (targetTable === 'audio_tier_revenue') {
            summaryQuery = `SELECT count() as total_views, sum(us_tax) as total_revenue_ads, sum(net_revenue) as total_revenue_red, sum(partner_rev_total) as total_revenue FROM ${dbName}.${targetTable} WHERE cms_id = {cms_id: String} AND ${dateFilter} ${searchFilter}`;
        } else if (targetTable === 'mv_asset_performance_daily') {
            summaryQuery = `SELECT sum(total_views) as total_views, sum(total_revenue_usd) as total_revenue FROM ${dbName}.${targetTable} WHERE cms_id = {cms_id: String} AND ${dateFilter} ${searchFilter}`;
        } else if (targetTable === 'video_reach_performance_daily') {
            summaryQuery = `SELECT sum(impressions) as total_views FROM ${dbName}.${targetTable} WHERE cms_id = {cms_id: String} AND ${dateFilter} ${searchFilter}`;
        } else if (targetTable === 'video_demographics_daily') {
            summaryQuery = '';
        } else if (targetTable === 'channel_subscribers_daily') {
            summaryQuery = `SELECT sum(subscribers_gained) as total_views, sum(subscribers_lost) as total_revenue FROM ${dbName}.${targetTable} WHERE cms_id = {cms_id: String} AND ${dateFilter} ${searchFilter}`;
        } else if (targetTable === 'video_interactions_daily') {
            summaryQuery = `SELECT sum(likes) as total_views, sum(comments) as total_revenue FROM ${dbName}.${targetTable} WHERE cms_id = {cms_id: String} AND ${dateFilter} ${searchFilter}`;
        } else {
            summaryQuery = `SELECT sum(views) as total_views FROM ${dbName}.${targetTable} WHERE cms_id = {cms_id: String} AND ${dateFilter} ${searchFilter}`;
        }

        try {
            const dataResult = await defaultCh.query({
                query,
                query_params: params,
                format: 'JSONEachRow',
            });
            const rows = await dataResult.json();

            let summary = { 
                total_views: 0, 
                total_revenue: 0,
                total_revenue_ads: 0,
                total_revenue_red: 0
            };
            try {
                if (summaryQuery) {
                    const summaryResult = await defaultCh.query({
                        query: summaryQuery,
                        query_params: params,
                        format: 'JSONEachRow',
                    });
                    const summaryRows = await summaryResult.json() as any[];
                    if (summaryRows.length > 0) {
                        const row = summaryRows[0];
                        summary.total_views = parseInt((row.total_views || 0).toString(), 10);
                        summary.total_revenue = parseFloat((row.total_revenue || 0).toString());
                        summary.total_revenue_ads = row.total_revenue_ads !== undefined 
                            ? parseFloat((row.total_revenue_ads || 0).toString()) 
                            : 0;
                        summary.total_revenue_red = row.total_revenue_red !== undefined 
                            ? parseFloat((row.total_revenue_red || 0).toString()) 
                            : 0;
                    }
                }
            } catch (sumErr) {
                console.error('Summary calculation error:', sumErr);
            }

            return reply.send({ success: true, rows, summary });
        } catch (err: any) {
            console.error(`[Preview Query Error]`, err);
            return reply.code(500).send({ error: `Failed to fetch ClickHouse data preview: ${err.message}` });
        }
    });

    // ═════════════════════════════════════════════════════════
    // GET /api/v1/ingest/jobs — Get global job history
    // ═════════════════════════════════════════════════════════
    app.get('/api/v1/ingest/jobs', async (req, reply) => {
        const { cms_id, limit = 50, job_type, month, status } = req.query as { cms_id?: string, limit?: number, job_type?: string, month?: string, status?: string };

        let query = `SELECT * FROM ingestion_jobs`;
        const conditions: string[] = [];
        const query_params: Record<string, unknown> = { limit: Number(limit) };
        if (cms_id) {
            if (cms_id.includes(',')) {
                const ids = cms_id.split(',').map(id => id.trim()).filter(Boolean);
                conditions.push(`cms_id IN {cms_ids: Array(String)}`);
                query_params.cms_ids = ids;
            } else {
                conditions.push(`cms_id = {cms_id: String}`);
                query_params.cms_id = cms_id;
            }
        }
        if (job_type) {
            if (job_type.includes(',')) {
                const types = job_type.split(',').map(t => t.trim());
                conditions.push(`job_type IN {job_types: Array(String)}`);
                query_params.job_types = types;
            } else {
                conditions.push(`job_type = {job_type: String}`);
                query_params.job_type = job_type;
            }
        }
        if (month) {
            conditions.push(`month = {month: UInt32}`);
            query_params.month = Number(month);
        }
        if (status) {
            conditions.push(`status = {status: String}`);
            query_params.status = status;
        }
        if (conditions.length > 0) {
            query += ` WHERE ` + conditions.join(' AND ');
        }

        query += ` ORDER BY updated_at DESC, indexOf(['pending', 'checking', 'processing', 'failed', 'completed'], status) DESC, length(detail_logs) DESC LIMIT 1 BY job_id LIMIT {limit: UInt32}`;

        const result = await getDefaultClient().query({
            query,
            query_params,
            format: 'JSONEachRow',
        });

        const rows = await result.json() as any[];
        const now = Date.now();
        for (const job of rows) {
            const status = job.status;
            if (status === 'processing' || status === 'pending') {
                const updatedAtTime = job.updated_at ? new Date(job.updated_at + (job.updated_at.includes('Z') ? '' : 'Z')).getTime() : 0;
                const timeSinceUpdateMs = now - updatedAtTime;
                
                if (job.job_type === 'estimated_ads_parent') {
                    if (timeSinceUpdateMs > 15 * 60 * 1000) {
                        job.status = 'failed';
                        job.error_message = '🚨 Job timeout (Orchestration process timed out or crashed).';
                        await updateJob(job.job_id, { status: 'failed', error_message: job.error_message, completed_at: new Date().toISOString() }).catch(() => { });
                    }
                } else {
                    if (!runningJobs.has(job.job_id) && timeSinceUpdateMs > 2 * 60 * 1000) {
                        job.status = 'failed';
                        job.error_message = '🚨 Job aborted unexpectedly (Server restarted/crashed during processing).';
                        await updateJob(job.job_id, { status: 'failed', error_message: job.error_message, completed_at: new Date().toISOString() }).catch(() => { });
                    }
                }
            }
        }
        return reply.send({ data: rows });
    });

    // ═════════════════════════════════════════════════════════
    // GET /api/v1/ingest/jobs/:id/logs/raw — Get raw JSON logs
    // ═════════════════════════════════════════════════════════
    app.get('/api/v1/ingest/jobs/:id/logs/raw', async (req, reply) => {
        const { id } = req.params as { id: string };

        // 1. Try in-memory logs first (if job is currently running)
        const memLogs = getJobLogs(id);
        if (memLogs && memLogs.length > 0) {
            return reply.send({ data: memLogs });
        }

        // 2. Otherwise query ClickHouse database for finished jobs
        try {
            const result = await getDefaultClient().query({
                query: `SELECT detail_logs FROM ingestion_jobs WHERE job_id = {id: String} ORDER BY updated_at DESC, indexOf(['pending', 'checking', 'processing', 'failed', 'completed'], status) DESC, length(detail_logs) DESC LIMIT 1`,
                query_params: { id },
                format: 'JSONEachRow',
            });
            const rows = await result.json<Record<string, unknown>[]>();
            if (rows.length > 0) {
                const job = rows[0] as any;
                const parsed = JSON.parse(job.detail_logs || '[]');
                return reply.send({ data: parsed });
            }
        } catch (e) {}

        return reply.send({ data: [] });
    });

    // ═════════════════════════════════════════════════════════
    // GET /api/v1/ingest/jobs/:id/logs — SSE live log stream
    // ═════════════════════════════════════════════════════════
    app.get('/api/v1/ingest/jobs/:id/logs', async (req, reply) => {
        const { id } = req.params as { id: string };

        reply.raw.writeHead(200, {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'X-Accel-Buffering': 'no',
            'Access-Control-Allow-Origin': '*',
        });

        // 🟢 Send heartbeat every 15s to keep HTTP connection forcefully alive during silent giant batches.
        const heartbeat = setInterval(() => {
            reply.raw.write(`: heartbeat\n\n`);
        }, 15000);

        const cleanup = subscribeJobLog(
            id,
            (entry) => {
                const data = JSON.stringify({ ts: entry.timestamp, msg: entry.message });
                reply.raw.write(`data: ${data}\n\n`);
            },
            () => {
                clearInterval(heartbeat);
                reply.raw.write(`data: {"done":true}\n\n`);
                reply.raw.end();
            },
        );

        // Client disconnected
        req.raw.on('close', () => {
            clearInterval(heartbeat);
            cleanup();
        });
    });

    // ═════════════════════════════════════════════════════════
    // DELETE /api/v1/ingest/jobs/:jobId
    // ═════════════════════════════════════════════════════════
    app.delete('/api/v1/ingest/jobs/:jobId', async (req, reply) => {
        const { jobId } = req.params as { jobId: string };
        const query = req.query as { action?: string };

        if (query.action === 'delete') {
            const ch = getDefaultClient();

            // 1. Delete actual ingested data from tables
            // Since we partition by upload_month and we don't store job_id in the enriched tables,
            // we first need to look up the cms_id, month, job_type, and error_message for this job.
            const jobRes = await ch.query({
                query: `SELECT cms_id, month, job_type, error_message FROM ingestion_jobs WHERE job_id = {jobId: String} ORDER BY updated_at DESC, indexOf(['pending', 'checking', 'processing', 'failed', 'completed'], status) DESC, length(detail_logs) DESC LIMIT 1`,
                query_params: { jobId },
                format: 'JSONEachRow'
            });
            const jobRows = await jobRes.json() as any[];
            if (jobRows.length > 0) {
                const cmsId = jobRows[0].cms_id;
                const uploadMonth = jobRows[0].month;
                const jobType = jobRows[0].job_type;
                const errMsg = jobRows[0].error_message || "";

                // Memastikan Client yang dipakai adalah milik CMS Database tersebut, BUKAN default!
                const dbName = `db_${cmsId.replace(/-/g, '_')}`;
                const tenantClient = getClickHouseClient({ database: dbName });

                if (jobType === "estimated_ads") {
                    // Parse date range
                    let start: string | null = null;
                    let end: string | null = null;
                    const matchRange = errMsg.match(/Sync range:\s*(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})/i);
                    if (matchRange) {
                        start = matchRange[1];
                        end = matchRange[2];
                    } else {
                        const match1 = errMsg.match(/dari\s+(\d{4}-\d{2}-\d{2})\s+ke\s+(\d{4}-\d{2}-\d{2})/i);
                        if (match1) {
                            start = match1[1];
                            end = match1[2];
                        } else {
                            const match2 = errMsg.match(/(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})/i);
                            if (match2) {
                                start = match2[1];
                                end = match2[2];
                            }
                        }
                    }

                    if (start && end) {
                        const dailyTables = [
                            "estimated_revenue_daily",
                            "video_reach_performance_daily",
                            "video_demographics_daily",
                            "video_traffic_sources_daily",
                            "video_devices_daily",
                            "mv_asset_performance_daily",
                            "channel_subscribers_daily",
                            "video_interactions_daily",
                            "youtube_raw_claims",
                            "youtube_raw_estimated_revenue",
                            "youtube_raw_asset_estimated_revenue",
                            "youtube_raw_channel_estimated_revenue"
                        ];
                        for (const tbl of dailyTables) {
                            await tenantClient.command({
                                query: `ALTER TABLE ${tbl} DELETE WHERE day >= {start: String} AND day <= {end: String} AND cms_id = {cmsId: String} SETTINGS mutations_sync = 1`,
                                query_params: { start, end, cmsId }
                            }).catch(e => {
                                console.error(`[Delete Job] Failed to delete from ${tbl}:`, e);
                            });
                        }
                    }
                } else {
                    await tenantClient.command({
                        query: `ALTER TABLE ads_revenue_enriched DROP PARTITION ({cmsId: String}, {partitionMonth: UInt32})`,
                        query_params: { cmsId, partitionMonth: parseInt(uploadMonth, 10) }
                    }).catch(() => { });

                    await tenantClient.command({
                        query: `ALTER TABLE subscription_revenue DROP PARTITION ({cmsId: String}, {partitionMonth: UInt32})`,
                        query_params: { cmsId, partitionMonth: parseInt(uploadMonth, 10) }
                    }).catch(() => { });

                    await tenantClient.command({
                        query: `ALTER TABLE paid_features_raw DROP PARTITION ({cmsId: String}, {partitionMonth: UInt32})`,
                        query_params: { cmsId, partitionMonth: parseInt(uploadMonth, 10) }
                    }).catch(() => { });

                    await tenantClient.command({
                        query: `ALTER TABLE audio_tier_revenue DROP PARTITION ({cmsId: String}, {partitionMonth: UInt32})`,
                        query_params: { cmsId, partitionMonth: parseInt(uploadMonth, 10) }
                    }).catch(() => { });

                    // Clean up archived raw files from disk & ingested_files_archive table
                    try {
                        const archiveRes = await ch.query({
                            query: `SELECT file_path FROM default.ingested_files_archive WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32}`,
                            query_params: { cmsId: cmsId, uploadMonth: parseInt(uploadMonth, 10) },
                            format: 'JSONEachRow'
                        });
                        const archiveRows = await archiveRes.json() as { file_path: string }[];
                        for (const row of archiveRows) {
                            try {
                                if (existsSync(row.file_path)) {
                                    unlinkSync(row.file_path);
                                    console.log(`[Delete Job Archive] Deleted file: ${row.file_path}`);
                                }
                            } catch (err: any) {
                                console.error(`[Delete Job Archive Error] Failed to delete file ${row.file_path}:`, err);
                            }
                        }

                        await ch.command({
                            query: `ALTER TABLE default.ingested_files_archive DELETE WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} SETTINGS mutations_sync = 1`,
                            query_params: { cmsId: cmsId, uploadMonth: parseInt(uploadMonth, 10) }
                        });
                    } catch (archiveErr: any) {
                        console.error(`[Delete Job Archive Error] Failed to clean up archives:`, archiveErr);
                    }
                }
            }

            // 2. Delete job metadata from ingestion_jobs for this job and month
            if (jobRows.length > 0 && jobRows[0].cms_id && jobRows[0].month && jobRows[0].job_type !== 'estimated_ads') {
                await ch.command({
                    query: `ALTER TABLE ingestion_jobs DELETE WHERE cms_id = {cmsId: String} AND month = {partitionMonth: UInt32} SETTINGS mutations_sync = 1`,
                    query_params: { cmsId: jobRows[0].cms_id, partitionMonth: parseInt(jobRows[0].month, 10) }
                }).catch(() => { });
            }

            await ch.command({
                query: `ALTER TABLE ingestion_jobs DELETE WHERE job_id = {jobId: String} SETTINGS mutations_sync = 1`,
                query_params: { jobId }
            });

            return reply.code(200).send({ success: true, message: `Job history and data deleted for ${jobId}` });
        }

        // Add to cancel token set if not already done
        activeJobTokens.add(jobId);

        // Immediately update the job status in ClickHouse DB to 'failed' so active parent/child loops halt and UI reflects the change.
        await updateJob(jobId, { status: 'failed', error_message: '🚨 Ingestion aborted/cancelled by user.', completed_at: new Date().toISOString() }).catch(() => { });

        reply.code(200).send({ success: true, message: `Abort signal sent for ${jobId}` });
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/ingest/reprocess-archive — Reprocess from raw reports archive
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/ingest/reprocess-archive', async (req, reply) => {
        const body = req.body as { cms_id: string; month: string; us_tax_rate?: number; job_id?: string };
        const { cms_id, month, us_tax_rate = 10 } = body;
        const jobId = body.job_id || uuidv4();
        const usTaxRate = parseFloat(String(us_tax_rate));

        if (!cms_id || !month) {
            return reply.code(400).send({ error: 'cms_id and month are required' });
        }

        const lockKey = `reprocess:${cms_id}:${month}`;
        if (activeLocks.has(lockKey)) {
            return reply.code(409).send({ error: `Reprocess untuk CMS ${cms_id} dan Bulan ${month} sedang berjalan.` });
        }
        activeLocks.set(lockKey, jobId);

        try {
            const defaultCh = getDefaultClient();
            const archiveRes = await defaultCh.query({
                query: `SELECT file_type, file_path FROM default.ingested_files_archive WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32}`,
                query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                format: 'JSONEachRow'
            });
            const archiveRows = await archiveRes.json() as { file_type: string; file_path: string }[];

            if (archiveRows.length === 0) {
                activeLocks.delete(lockKey);
                return reply.code(404).send({ error: `Arsip file laporan untuk CMS ${cms_id} di bulan ${month} tidak ditemukan. Silakan upload file terlebih dahulu.` });
            }

            const files: Record<string, string> = {};
            for (const row of archiveRows) {
                files[row.file_type] = row.file_path;
            }

            await updateJob(jobId, {
                job_type: 'reprocess_archive', cms_id, status: 'processing', month: parseInt(month),
            });

            reply.code(202).send({ job_id: jobId, status: 'processing', month });

            initJobLog(jobId, true);
            runningJobs.add(jobId);

            setImmediate(async () => {
                const log = (msg: string) => emitLog(jobId, msg);
                const isAborted = () => !activeJobTokens.has(jobId);
                activeJobTokens.add(jobId);

                let finalStatus = 'processing';
                let finalMsg = '';

                try {
                    const cleanCmsId = cms_id.replace(/-/g, '_');
                    const client = getClickHouseClient({ database: `db_${cleanCmsId}` });
                    const startTime = Date.now();
                    log(`🔄 Memulai proses ulang dari arsip file untuk bulan ${month}...`);

                    // 1. Ads Revenue ETL
                    let adsRows = 0;
                    let adsRevenue = 0;
                    let adjAdsRevenue = 0;
                    if (files.claim_raw && files.videoclaim && files.asset_summary) {
                        log(`[Reprocess 1/4] Memproses Ads Revenue...`);
                        await client.command({
                            query: `ALTER TABLE ads_revenue_enriched DROP PARTITION ({cmsId: String}, {partitionMonth: UInt32})`,
                            query_params: { cmsId: cms_id, partitionMonth: parseInt(month, 10) }
                        }).catch(() => {});

                        const adsRes = await runAdsIngestionDirect({
                            jobId, month, cmsId: cms_id, usTaxRate,
                            files: {
                                claim_raw: files.claim_raw,
                                videoclaim: files.videoclaim,
                                asset_summary: files.asset_summary,
                                adj_claim_raw: files.ads_adjustment,
                                shorts_ads: files.shorts_ads
                            },
                            channelMap: new Map<string, string>(),
                            client, isAborted, log
                        });
                        adsRows = adsRes.totalAllRows;

                        const totalRes = await client.query({
                            query: `SELECT sum(partner_rev_total) as total_rev FROM ads_revenue_enriched WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND report_type = 'claim_raw'`,
                            query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                            format: 'JSONEachRow'
                        });
                        const totalRows = await totalRes.json() as any[];
                        adsRevenue = totalRows[0]?.total_rev ? parseFloat(totalRows[0].total_rev) : 0.0;

                        if (files.ads_adjustment) {
                            const adjRes = await client.query({
                                query: `SELECT sum(partner_rev_total) as total_rev FROM ads_revenue_enriched WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND report_type = 'ads_adjustment'`,
                                query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                                format: 'JSONEachRow'
                            });
                            const adjRows = await adjRes.json() as any[];
                            adjAdsRevenue = adjRows[0]?.total_rev ? parseFloat(adjRows[0].total_rev) : 0.0;
                        }
                    }

                    // 2. Subscription ETL
                    let subRows = 0;
                    let adjSubRows = 0;
                    let subRevenue = 0;
                    let adjSubRevenue = 0;
                    if (files.subscription) {
                        log(`[Reprocess 2/4] Memproses Subscription Revenue...`);
                        await client.command({
                            query: `ALTER TABLE subscription_revenue DROP PARTITION ({cmsId: String}, {partitionMonth: UInt32})`,
                            query_params: { cmsId: cms_id, partitionMonth: parseInt(month, 10) }
                        }).catch(() => {});

                        const subRes = await runSubscriptionIngestionDirect({
                            jobId, month, cmsId: cms_id, usTaxRate,
                            files: {
                                subscription: files.subscription,
                                adj_red_label: files.subscription_adjustment,
                                shorts_subs: files.shorts_subs,
                                affiliate_tax: files.affiliate_tax
                            },
                            channelMap: new Map<string, string>(),
                            client, isAborted, log
                        });
                        subRows = subRes.subRows;
                        adjSubRows = subRes.adjSubRows;

                        const sRes = await client.query({
                            query: `SELECT sum(partner_rev_total) as total_rev FROM subscription_revenue WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND (adjustment_type = '' OR adjustment_type = 'None')`,
                            query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                            format: 'JSONEachRow'
                        });
                        const sRows = await sRes.json() as any[];
                        subRevenue = sRows[0]?.total_rev ? parseFloat(sRows[0].total_rev) : 0.0;

                        if (files.subscription_adjustment) {
                            const saRes = await client.query({
                                query: `SELECT sum(partner_rev_total) as total_rev FROM subscription_revenue WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32} AND (adjustment_type != '' AND adjustment_type != 'None')`,
                                query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                                format: 'JSONEachRow'
                            });
                            const saRows = await saRes.json() as any[];
                            adjSubRevenue = saRows[0]?.total_rev ? parseFloat(saRows[0].total_rev) : 0.0;
                        }
                    }

                    // 3. Paid Features ETL
                    let paidRows = 0;
                    let paidRevenue = 0;
                    if (files.paid_features) {
                        log(`[Reprocess 3/4] Memproses Paid Features...`);
                        await client.command({
                            query: `ALTER TABLE paid_features_raw DROP PARTITION ({cmsId: String}, {partitionMonth: UInt32})`,
                            query_params: { cmsId: cms_id, partitionMonth: parseInt(month, 10) }
                        }).catch(() => {});

                        const paidRes = await runPaidFeaturesIngestionDirect({
                            jobId, month, cmsId: cms_id, usTaxRate,
                            filePath: files.paid_features,
                            client, isAborted, log
                        });
                        paidRows = paidRes.paidRows;

                        const pRes = await client.query({
                            query: `SELECT sum(earnings_usd) as total_rev FROM paid_features_raw WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32}`,
                            query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                            format: 'JSONEachRow'
                        });
                        const pRows = await pRes.json() as any[];
                        paidRevenue = pRows[0]?.total_rev ? parseFloat(pRows[0].total_rev) : 0.0;
                    }

                    // 4. Audio Tier ETL
                    let audioTierRows = 0;
                    let audioTierRevenue = 0;
                    if (files.audio_tier) {
                        log(`[Reprocess 4/4] Memproses Audio Tier...`);
                        await client.command({
                            query: `ALTER TABLE audio_tier_revenue DROP PARTITION ({cmsId: String}, {partitionMonth: UInt32})`,
                            query_params: { cmsId: cms_id, partitionMonth: parseInt(month, 10) }
                        }).catch(() => {});

                        const audioRes = await runAudioTierIngestionDirect({
                            jobId, month, cmsId: cms_id, usTaxRate,
                            filePath: files.audio_tier,
                            client, isAborted, log
                        });
                        audioTierRows = audioRes.audioTierRows;

                        const aRes = await client.query({
                            query: `SELECT sum(partner_rev_total) as total_rev FROM audio_tier_revenue WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32}`,
                            query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                            format: 'JSONEachRow'
                        });
                        const aRows = await aRes.json() as any[];
                        audioTierRevenue = aRows[0]?.total_rev ? parseFloat(aRows[0].total_rev) : 0.0;
                    }

                    // Calculate accumulated US Tax & Net Revenue
                    let totalUsTax = 0;
                    let totalNetRevenue = 0;

                    const taxRes = await client.query({
                        query: `
                            SELECT sum(tax) as tax_total, sum(net) as net_total FROM (
                                SELECT sum(us_tax) as tax, sum(net_revenue) as net FROM ads_revenue_enriched WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32}
                                UNION ALL
                                SELECT sum(us_tax) as tax, sum(net_revenue) as net FROM subscription_revenue WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32}
                                UNION ALL
                                SELECT sum(us_tax) as tax, sum(net_revenue) as net FROM paid_features_raw WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32}
                                UNION ALL
                                SELECT sum(us_tax) as tax, sum(net_revenue) as net FROM audio_tier_revenue WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32}
                            )
                        `,
                        query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                        format: 'JSONEachRow'
                    });
                    const taxRows = await taxRes.json() as any[];
                    totalUsTax = taxRows[0]?.tax_total ? parseFloat(taxRows[0].tax_total) : 0.0;
                    totalNetRevenue = taxRows[0]?.net_total ? parseFloat(taxRows[0].net_total) : 0.0;

                    log(`[Step Reconcile] Rekonsiliasi label lagu untuk bulan ${month}...`);
                    await reconcileNullLabels({ dbName: `db_${cleanCmsId}`, month: parseInt(month, 10), cmsId: cms_id });

                    finalStatus = 'completed';
                    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
                    finalMsg = `✅ Reprocess selesai! Total Waktu: ${elapsed}s`;
                    log(finalMsg);

                    await updateJob(jobId, {
                        status: 'completed',
                        error_message: finalMsg,
                        processed_rows: adsRows + subRows + adjSubRows + paidRows + audioTierRows,
                        ads_rows: adsRows,
                        sub_rows: subRows,
                        adj_sub_rows: adjSubRows,
                        paid_features_rows: paidRows,
                        audio_tier_rows: audioTierRows,
                        ads_revenue: adsRevenue,
                        sub_revenue: subRevenue,
                        adj_ads_revenue: adjAdsRevenue,
                        adj_sub_revenue: adjSubRevenue,
                        paid_revenue: paidRevenue,
                        audio_tier_revenue: audioTierRevenue,
                        us_tax: totalUsTax,
                        net_revenue: totalNetRevenue
                    });

                    // Sync reprocessed stats back to original upload jobs (to update UI display)
                    try {
                        const defaultCh = getDefaultClient();
                        await defaultCh.command({
                            query: `
                                ALTER TABLE default.ingestion_jobs UPDATE
                                    processed_rows = {processedRows: UInt64},
                                    ads_rows = {adsRows: UInt64},
                                    sub_rows = {subRows: UInt64},
                                    adj_sub_rows = {adjSubRows: UInt64},
                                    paid_features_rows = {paidRows: UInt64},
                                    audio_tier_rows = {audioTierRows: UInt64},
                                    ads_revenue = {adsRevenue: Float64},
                                    sub_revenue = {subRevenue: Float64},
                                    adj_ads_revenue = {adjAdsRevenue: Float64},
                                    adj_sub_revenue = {adjSubRevenue: Float64},
                                    paid_revenue = {paidRevenue: Float64},
                                    audio_tier_revenue = {audioTierRevenue: Float64},
                                    us_tax = {usTax: Float64},
                                    net_revenue = {netRevenue: Float64}
                                WHERE cms_id = {cmsId: String} AND month = {uploadMonth: UInt32} AND job_type IN ('ads_revenue', 'subscription')
                                SETTINGS mutations_sync = 1
                            `,
                            query_params: {
                                processedRows: adsRows + subRows + adjSubRows + paidRows + audioTierRows,
                                adsRows,
                                subRows,
                                adjSubRows,
                                paidRows,
                                audioTierRows,
                                adsRevenue,
                                subRevenue,
                                adjAdsRevenue,
                                adjSubRevenue,
                                paidRevenue,
                                audioTierRevenue,
                                usTax: totalUsTax,
                                netRevenue: totalNetRevenue,
                                cmsId: cms_id,
                                uploadMonth: parseInt(month, 10)
                            }
                        });
                        log(`[Reprocess Sync] Summary successfully synced back to original job rows in ingestion_jobs.`);
                    } catch (syncErr: any) {
                        log(`⚠️ Warning: Gagal sinkronisasi summary ke job asli di ingestion_jobs: ${syncErr.message}`);
                    }
                } catch (e: any) {
                    finalStatus = 'failed';
                    finalMsg = `🚨 Reprocess gagal: ${e.message}`;
                    log(finalMsg);
                    await updateJob(jobId, { status: 'failed', error_message: finalMsg });
                } finally {
                    activeJobTokens.delete(jobId);
                    runningJobs.delete(jobId);
                    activeLocks.delete(lockKey);
                    completeJobLog(jobId);
                }
            });
        } catch (err: any) {
            activeLocks.delete(lockKey);
            return reply.code(500).send({ error: `Gagal menginisialisasi reprocess: ${err.message}` });
        }
    });

    // ═════════════════════════════════════════════════════════
    // GET /api/v1/ingest/archive-files — Check archived files
    // ═════════════════════════════════════════════════════════
    app.get('/api/v1/ingest/archive-files', async (req, reply) => {
        const query = req.query as { cms_id: string; month: string };
        const { cms_id, month } = query;

        if (!cms_id || !month) {
            return reply.code(400).send({ error: 'cms_id and month are required' });
        }

        try {
            const defaultCh = getDefaultClient();
            const res = await defaultCh.query({
                query: `SELECT file_type, file_name, file_size, uploaded_at FROM default.ingested_files_archive WHERE cms_id = {cmsId: String} AND upload_month = {uploadMonth: UInt32}`,
                query_params: { cmsId: cms_id, uploadMonth: parseInt(month, 10) },
                format: 'JSONEachRow'
            });
            const rows = await res.json();
            return reply.send({ success: true, data: rows });
        } catch (err: any) {
            return reply.code(500).send({ error: `Gagal query arsip file: ${err.message}` });
        }
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/ingest/jobs/:id/log — Emit a log entry for a job
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/ingest/jobs/:id/log', async (req, reply) => {
        const { id } = req.params as { id: string };
        const { message } = req.body as { message: string };
        if (!message) return reply.code(400).send({ error: 'message is required' });
        
        emitLog(id, message);
        return reply.send({ success: true });
    });

    // ═════════════════════════════════════════════════════════
    // GET /api/v1/ingest/active-videos
    // ═════════════════════════════════════════════════════════
    app.get('/api/v1/ingest/active-videos', async (req, reply) => {
        const { cms_id, day, min_views, official_channels } = req.query as { cms_id?: string; day?: string; min_views?: string; official_channels?: string };
        if (!cms_id || !day) {
            return reply.code(400).send({ error: 'cms_id and day query params are required' });
        }
        const minViews = parseInt(min_views || '10', 10);
        const dbName = `db_${cms_id}`;
        const officialChannels = official_channels ? official_channels.split(',').filter(Boolean) : [];

        try {
            const ch = getDefaultClient();
            let query = '';
            let videoIds: string[] = [];

            // Check if video_devices_daily table exists
            const checkDevices = await ch.query({
                query: `SELECT count() FROM system.tables WHERE database = {dbName: String} AND name = 'video_devices_daily'`,
                query_params: { dbName },
                format: 'JSONEachRow'
            });
            const checkDevicesRows = await checkDevices.json() as any[];
            const devicesExists = checkDevicesRows[0] && parseInt(checkDevicesRows[0]['count()'] || '0', 10) > 0;

            if (devicesExists) {
                query = `
                    SELECT video_id
                    FROM (
                        SELECT video_id, sum(views) as total_views
                        FROM \`${dbName}\`.video_devices_daily
                        WHERE cms_id = {cms_id: String} AND day = {day: String}
                        GROUP BY video_id
                    )
                    WHERE total_views >= {minViews: Int64}
                    ORDER BY total_views DESC
                `;
            } else {
                const checkCountries = await ch.query({
                    query: `SELECT count() FROM system.tables WHERE database = {dbName: String} AND name = 'video_countries_daily'`,
                    query_params: { dbName },
                    format: 'JSONEachRow'
                });
                const checkCountriesRows = await checkCountries.json() as any[];
                const countriesExists = checkCountriesRows[0] && parseInt(checkCountriesRows[0]['count()'] || '0', 10) > 0;
                if (countriesExists) {
                    query = `
                        SELECT video_id
                        FROM (
                            SELECT video_id, sum(views) as total_views
                            FROM \`${dbName}\`.video_countries_daily
                            WHERE cms_id = {cms_id: String} AND day = {day: String}
                            GROUP BY video_id
                        )
                        WHERE total_views >= {minViews: Int64}
                        ORDER BY total_views DESC
                    `;
                }
            }

            if (query) {
                const result = await ch.query({
                    query,
                    query_params: { cms_id, day, minViews },
                    format: 'JSONEachRow'
                });
                const rows = await result.json() as any[];
                videoIds = rows.map(r => r.video_id);
            }

            // Fallback: If no videoIds and we have official channels, pull from youtube_raw_claims
            if (videoIds.length === 0 && officialChannels.length > 0) {
                const checkClaims = await ch.query({
                    query: `SELECT count() FROM system.tables WHERE database = {dbName: String} AND name = 'youtube_raw_claims'`,
                    query_params: { dbName },
                    format: 'JSONEachRow'
                });
                const checkClaimsRows = await checkClaims.json() as any[];
                const claimsExists = checkClaimsRows[0] && parseInt(checkClaimsRows[0]['count()'] || '0', 10) > 0;

                if (claimsExists) {
                    const fallbackResult = await ch.query({
                        query: `
                            SELECT DISTINCT video_id
                            FROM \`${dbName}\`.youtube_raw_claims
                            WHERE channel_id IN ({channels: Array(String)})
                        `,
                        query_params: { channels: officialChannels },
                        format: 'JSONEachRow'
                    });
                    const fallbackRows = await fallbackResult.json() as any[];
                    videoIds = fallbackRows.map(r => r.video_id);
                }
            }

            return reply.send({ success: true, videoIds });
        } catch (err: any) {
            console.error(`[GET active-videos error]`, err);
            return reply.code(500).send({ error: `Failed to fetch active videos: ${err.message}` });
        }
    });

    // ═════════════════════════════════════════════════════════
    // GET /api/v1/ingest/youtube-history
    // ═════════════════════════════════════════════════════════
    app.get('/api/v1/ingest/youtube-history', async (req, reply) => {
        const { cms_id } = req.query as { cms_id?: string };
        if (!cms_id) {
            return reply.code(400).send({ error: 'cms_id query param is required' });
        }

        try {
            const ch = getDefaultClient();
            const result = await ch.query({
                query: `SELECT cms_id, report_type, toString(day) as day, report_id, formatDateTime(create_time, '%Y-%m-%dT%H:%i:%sZ') as create_time, formatDateTime(ingested_at, '%Y-%m-%dT%H:%i:%sZ') as ingested_at, is_fallback FROM youtube_ingest_history FINAL WHERE cms_id = {cms_id: String}`,
                query_params: { cms_id },
                format: 'JSONEachRow'
            });
            const rows = await result.json() as any[];
            return reply.send({ success: true, data: rows });
        } catch (err: any) {
            console.error(`[GET youtube-history error]`, err);
            return reply.code(500).send({ error: `Failed to fetch youtube history: ${err.message}` });
        }
    });
 
    // ═════════════════════════════════════════════════════════
    // GET /api/v1/ingest/existing-raw-rows
    // ═════════════════════════════════════════════════════════
    app.get('/api/v1/ingest/existing-raw-rows', async (req, reply) => {
        const { cms_id, day } = req.query as { cms_id?: string; day?: string };
        if (!cms_id || !day) {
            return reply.code(400).send({ error: 'cms_id and day query params are required' });
        }
 
        try {
            const dbName = `db_${cms_id.replace(/-/g, '_')}`;
            const ch = getClickHouseClient({ database: dbName });
            const result = await ch.query({
                query: `
                    SELECT
                        video_id,
                        any(creator_content_type) AS creator_content_type,
                        sum(views) AS views,
                        sum(estimated_partner_revenue) AS partner_rev_total,
                        sum(estimated_partner_ad_auction_revenue) AS partner_rev_auction,
                        sum(estimated_partner_ad_reserved_revenue) AS partner_rev_reserved,
                        sum(estimated_partner_red_revenue) AS partner_rev_red,
                        sum(estimated_partner_transaction_revenue) AS partner_rev_transaction
                    FROM youtube_raw_estimated_revenue
                    WHERE cms_id = {cms_id: String} AND day = toDate({day: String})
                    GROUP BY video_id
                `,
                query_params: { cms_id, day },
                format: 'JSONEachRow'
            });
            const rows = await result.json() as any[];
            return reply.send({ success: true, data: rows });
        } catch (err: any) {
            console.error(`[GET existing-raw-rows error]`, err);
            return reply.code(500).send({ error: `Failed to fetch existing raw rows: ${err.message}` });
        }
    });

    // ═════════════════════════════════════════════════════════
    // GET /api/v1/ingest/cms-video-ids
    // ═════════════════════════════════════════════════════════
    app.get('/api/v1/ingest/cms-video-ids', async (req, reply) => {
        const { cms_id, priority } = req.query as { cms_id?: string; priority?: string };
        if (!cms_id) {
            return reply.code(400).send({ error: 'cms_id query param is required' });
        }

        try {
            const dbName = `db_${cms_id.replace(/-/g, '_')}`;
            const ch = getClickHouseClient({ database: dbName });
            
            // Check which tables exist in ClickHouse
            const tablesToCheck = ['youtube_raw_claims', 'estimated_revenue_daily', 'supplementary_video_ids'];
            const checkTables = await ch.query({
                query: `SELECT name FROM system.tables WHERE database = {dbName: String} AND name IN (${tablesToCheck.map(t => `'${t}'`).join(',')})`,
                query_params: { dbName },
                format: 'JSONEachRow'
            });
            const checkTablesRows = await checkTables.json() as any[];
            const existingTables = new Set(checkTablesRows.map(r => r.name));

            const selectQueries: string[] = [];

            if (priority === 'high') {
                if (existingTables.has('estimated_revenue_daily')) {
                    selectQueries.push(`SELECT video_id FROM estimated_revenue_daily WHERE video_id != ''`);
                }
                if (existingTables.has('supplementary_video_ids')) {
                    selectQueries.push(`SELECT video_id FROM supplementary_video_ids WHERE video_id != ''`);
                }
            } else if (priority === 'low') {
                if (existingTables.has('youtube_raw_claims')) {
                    const excludeList = [];
                    if (existingTables.has('estimated_revenue_daily')) {
                        excludeList.push(`SELECT video_id FROM estimated_revenue_daily`);
                    }
                    if (existingTables.has('supplementary_video_ids')) {
                        excludeList.push(`SELECT video_id FROM supplementary_video_ids`);
                    }
                    const excludeQuery = excludeList.length > 0 
                        ? ` AND video_id NOT IN (${excludeList.join(' UNION DISTINCT ')})` 
                        : '';
                    selectQueries.push(`SELECT video_id FROM youtube_raw_claims WHERE video_id != ''${excludeQuery}`);
                }
            } else {
                if (existingTables.has('youtube_raw_claims')) {
                    selectQueries.push(`SELECT video_id FROM youtube_raw_claims WHERE video_id != ''`);
                }
                if (existingTables.has('estimated_revenue_daily')) {
                    selectQueries.push(`SELECT video_id FROM estimated_revenue_daily WHERE video_id != ''`);
                }
                if (existingTables.has('supplementary_video_ids')) {
                    selectQueries.push(`SELECT video_id FROM supplementary_video_ids WHERE video_id != ''`);
                }
            }

            let videoIds: string[] = [];
            if (selectQueries.length > 0) {
                const unionQuery = `SELECT DISTINCT video_id FROM (\n${selectQueries.join('\nUNION DISTINCT\n')}\n)`;
                const result = await ch.query({
                    query: unionQuery,
                    format: 'JSONEachRow'
                });
                const rows = await result.json() as any[];
                videoIds = rows.map(r => r.video_id);
            }

            return reply.send({ success: true, data: videoIds });
        } catch (err: any) {
            console.error(`[GET cms-video-ids error]`, err);
            return reply.code(500).send({ error: `Failed to fetch CMS video IDs: ${err.message}` });
        }
    });

    // ═════════════════════════════════════════════════════════
    // GET /api/v1/ingest/cms-channel-ids
    // ═════════════════════════════════════════════════════════
    app.get('/api/v1/ingest/cms-channel-ids', async (req, reply) => {
        const { cms_id } = req.query as { cms_id?: string };
        if (!cms_id) {
            return reply.code(400).send({ error: 'cms_id query param is required' });
        }

        try {
            const dbName = `db_${cms_id.replace(/-/g, '_')}`;
            const ch = getClickHouseClient({ database: dbName });
            
            // Check which tables exist in ClickHouse
            const tablesToCheck = ['youtube_raw_claims', 'estimated_revenue_daily'];
            const checkTables = await ch.query({
                query: `SELECT name FROM system.tables WHERE database = {dbName: String} AND name IN (${tablesToCheck.map(t => `'${t}'`).join(',')})`,
                query_params: { dbName },
                format: 'JSONEachRow'
            });
            const checkTablesRows = await checkTables.json() as any[];
            const existingTables = new Set(checkTablesRows.map(r => r.name));

            const selectQueries: string[] = [];
            if (existingTables.has('youtube_raw_claims')) {
                selectQueries.push(`SELECT channel_id FROM youtube_raw_claims WHERE channel_id != ''`);
            }
            if (existingTables.has('estimated_revenue_daily')) {
                selectQueries.push(`SELECT channel_id FROM estimated_revenue_daily WHERE channel_id != ''`);
            }

            let channelIds: string[] = [];
            if (selectQueries.length > 0) {
                const unionQuery = `SELECT DISTINCT channel_id FROM (\n${selectQueries.join('\nUNION DISTINCT\n')}\n)`;
                const result = await ch.query({
                    query: unionQuery,
                    format: 'JSONEachRow'
                });
                const rows = await result.json() as any[];
                channelIds = rows.map(r => r.channel_id);
            }

            return reply.send({ success: true, data: channelIds });
        } catch (err: any) {
            console.error(`[GET cms-channel-ids error]`, err);
            return reply.code(500).send({ error: `Failed to fetch CMS channel IDs: ${err.message}` });
        }
    });

    // ═════════════════════════════════════════════════════════
    // GET /api/v1/ingest/supplementary-video-ids
    // ═════════════════════════════════════════════════════════
    app.get('/api/v1/ingest/supplementary-video-ids', async (req, reply) => {
        const { cms_id } = req.query as { cms_id?: string };
        if (!cms_id) {
            return reply.code(400).send({ error: 'cms_id query param is required' });
        }

        try {
            const dbName = `db_${cms_id.replace(/-/g, '_')}`;
            const ch = getClickHouseClient({ database: dbName });

            const result = await ch.query({
                query: `SELECT video_id, video_title, added_at, added_by FROM supplementary_video_ids ORDER BY added_at DESC`,
                format: 'JSONEachRow'
            });
            const rows = await result.json() as any[];
            return reply.send({ success: true, data: rows });
        } catch (err: any) {
            console.error(`[GET supplementary-video-ids error]`, err);
            return reply.code(500).send({ error: `Failed to fetch supplementary video IDs: ${err.message}` });
        }
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/ingest/supplementary-video-ids (Bulk Register)
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/ingest/supplementary-video-ids', async (req, reply) => {
        const { cms_id, videos } = req.body as {
            cms_id: string;
            videos: Array<{ video_id: string; video_title?: string; added_by?: string }>;
        };

        if (!cms_id || !videos || !Array.isArray(videos)) {
            return reply.code(400).send({ error: 'cms_id and videos (array) are required' });
        }

        try {
            const dbName = `db_${cms_id.replace(/-/g, '_')}`;
            const ch = getClickHouseClient({ database: dbName });

            const values = videos.map(v => ({
                video_id: v.video_id,
                video_title: v.video_title || null,
                added_at: new Date().toISOString().replace('T', ' ').slice(0, 19),
                added_by: v.added_by || null
            }));

            await ch.insert({
                table: 'supplementary_video_ids',
                values,
                format: 'JSONEachRow'
            });

            return reply.send({ success: true, message: `Successfully registered ${videos.length} video IDs` });
        } catch (err: any) {
            console.error(`[POST supplementary-video-ids error]`, err);
            return reply.code(500).send({ error: `Failed to register supplementary video IDs: ${err.message}` });
        }
    });

    // ═════════════════════════════════════════════════════════
    // DELETE /api/v1/ingest/supplementary-video-ids (Bulk Delete)
    // ═════════════════════════════════════════════════════════
    app.delete('/api/v1/ingest/supplementary-video-ids', async (req, reply) => {
        const { cms_id, video_ids } = req.body as {
            cms_id: string;
            video_ids: string[];
        };

        if (!cms_id || !video_ids || !Array.isArray(video_ids) || video_ids.length === 0) {
            return reply.code(400).send({ error: 'cms_id and video_ids (array) are required' });
        }

        try {
            const dbName = `db_${cms_id.replace(/-/g, '_')}`;
            const ch = getClickHouseClient({ database: dbName });

            // Run ClickHouse delete mutation
            const query = `ALTER TABLE \`${dbName}\`.supplementary_video_ids DELETE WHERE video_id IN (${video_ids.map(id => `'${id}'`).join(',')})`;
            await ch.command({ query });

            return reply.send({ success: true, message: `Delete mutation submitted for ${video_ids.length} video IDs` });
        } catch (err: any) {
            console.error(`[DELETE supplementary-video-ids error]`, err);
            return reply.code(500).send({ error: `Failed to delete supplementary video IDs: ${err.message}` });
        }
    });


    // ═════════════════════════════════════════════════════════
    // POST /api/v1/ingest/youtube-history
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/ingest/youtube-history', async (req, reply) => {
        const { cms_id, report_type, day, report_id, create_time, is_fallback } = req.body as {
            cms_id: string;
            report_type: string;
            day: string;
            report_id: string;
            create_time: string;
            is_fallback?: number;
        };

        if (!cms_id || !report_type || !day || !report_id || !create_time) {
            return reply.code(400).send({ error: 'cms_id, report_type, day, report_id, and create_time are required' });
        }

        try {
            const ch = getDefaultClient();
            const formattedCreateTime = new Date(create_time).toISOString().replace('T', ' ').slice(0, 19);

            await ch.insert({
                table: 'youtube_ingest_history',
                values: [{
                    cms_id,
                    report_type,
                    day,
                    report_id,
                    create_time: formattedCreateTime,
                    ingested_at: new Date().toISOString().replace('T', ' ').slice(0, 19),
                    is_fallback: is_fallback ?? 0
                }],
                format: 'JSONEachRow'
            });

            return reply.send({ success: true, message: 'Ingest history recorded' });
        } catch (err: any) {
            console.error(`[POST youtube-history error]`, err);
            return reply.code(500).send({ error: `Failed to save youtube history: ${err.message}` });
        }
    });

    // ═════════════════════════════════════════════════════════
    // POST /api/v1/ingest/repair-mappings
    // ═════════════════════════════════════════════════════════
    app.post('/api/v1/ingest/repair-mappings', async (req, reply) => {
        const { cms_id, start_date, end_date } = req.body as {
            cms_id: string;
            start_date: string;
            end_date: string;
        };

        if (!cms_id || !start_date || !end_date) {
            return reply.code(400).send({ error: 'cms_id, start_date, and end_date are required' });
        }

        const memTenant = getTenantById(cms_id);
        let client: any;
        let dbName: string;
        if (memTenant) {
            client = memTenant.ingestClient;
            dbName = memTenant.dbName;
        } else {
            dbName = `db_${cms_id.replace(/-/g, '_')}`;
            client = getClickHouseClient({ database: dbName });
        }

        try {
            console.log(`[Repair Mappings] Starting metadata repair for CMS: ${cms_id} from ${start_date} to ${end_date}...`);
            
            const query = `
                ALTER TABLE estimated_revenue_daily
                UPDATE 
                  asset_id = coalesce(
                    (
                      SELECT argMax(asset_id, day)
                      FROM youtube_video_metadata
                      WHERE cms_id = estimated_revenue_daily.cms_id
                        AND video_id = estimated_revenue_daily.video_id
                        AND asset_id != ''
                    ),
                    'UNCLAIMED_VIDEO'
                  ),
                  claim_origin = if(asset_id != 'UNCLAIMED_VIDEO', 'METADATA_MATCH', 'Unclaimed')
                WHERE cms_id = {cms_id: String}
                  AND day >= toDate({start_date: String})
                  AND day <= toDate({end_date: String})
                  AND asset_id = ''
                  AND video_id != ''
                SETTINGS mutations_sync = 1
            `;

            await client.command({
                query,
                query_params: {
                    cms_id,
                    start_date,
                    end_date
                }
            });

            console.log(`[Repair Mappings] ✓ Metadata repair completed successfully for CMS: ${cms_id} (${start_date} to ${end_date}).`);
            return reply.send({ success: true, message: 'Metadata mappings repaired successfully' });
        } catch (err: any) {
            console.error(`[Repair Mappings Error]`, err);
            return reply.code(500).send({ error: `Failed to repair mappings: ${err.message}` });
        }
    });
}
