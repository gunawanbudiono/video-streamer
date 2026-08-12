import { createReadStream } from 'fs';
import { Readable } from 'stream';
import type { ClickHouseClient } from '@clickhouse/client';
import { createAutoDetectCsvStream, CsvRowHelper, extractFileStream, detectDelimiterFromFile } from './etl-ads.js';
import { randomUUID } from 'crypto';

// ── Types ─────────────────────────────────────────────────

export interface EstimatedAdRow {
    cms_id: string;
    day: string;
    country: string;
    video_id: string;
    channel_id: string;
    asset_id: string;
    asset_type: string;
    content_type: string;
    claim_type: string;
    policy: string;
    claim_origin: string;
    isrc: string;
    upc: string;
    grid: string;
    video_title: string;
    username: string;
    uploader: string;
    video_duration_sec: number;
    channel_display_name: string;
    multiple_claims: string;
    category: string;
    asset_labels: string;
    artist: string;
    asset_title: string;
    album: string;
    label: string;
    owned_views: number;
    yt_rev_auction: number;
    yt_rev_reserved: number;
    yt_rev_partner_sold_yt_served: number;
    yt_rev_partner_sold_p_served: number;
    yt_rev_red: number;
    yt_rev_total: number;
    partner_rev_auction: number;
    partner_rev_reserved: number;
    partner_rev_partner_sold_yt_served: number;
    partner_rev_partner_sold_p_served: number;
    partner_rev_red: number;
    partner_rev_total: number;
}

export interface VideoReachRow {
    cms_id: string;
    day: string;
    video_id: string;
    channel_id: string;
    impressions: number;
    impressions_ctr: number;
    views: number;
    watch_time_sec: number;
}

// ── Helpers ───────────────────────────────────────────────

function getCol(row: Record<string, string>, ...keys: string[]): string {
    for (const key of keys) {
        if (row[key] !== undefined) return row[key];
        const lowerKey = key.toLowerCase();
        const found = Object.keys(row).find(k => k.toLowerCase() === lowerKey);
        if (found && row[found] !== undefined) return row[found];
    }
    return '';
}

function num(val: string | undefined): number {
    if (!val || val === '') return 0;
    const n = parseFloat(val);
    return isNaN(n) ? 0 : n;
}

function parseDay(val: string): string {
    if (!val) return '';
    // YYYYMMDD → YYYY-MM-DD
    if (val.length === 8 && /^\d{8}$/.test(val)) {
        return `${val.slice(0, 4)}-${val.slice(4, 6)}-${val.slice(6, 8)}`;
    }
    return val;
}

// ── Ingest Workers ────────────────────────────────────────

async function parseClaimsFile(
    filePath: string, 
    log?: (msg: string) => void
): Promise<{ map: Map<string, any[]>; count: number }> {
    console.log(`[ETL] Starting parsing of daily claims file: ${filePath}`);
    const sizeMb = require('fs').statSync(filePath).size / 1024 / 1024;
    if (log) {
        log(`[ETL] Loading daily claims file (${sizeMb.toFixed(2)} MB)...`);
    }
    const map = new Map<string, any[]>();
    const parser = await createAutoDetectCsvStream(filePath);
    const helper = new CsvRowHelper();

    let count = 0;
    for await (const row of parser as any) {
        count++;
        if (count % 100000 === 0 && log) {
            log(`[ETL] Parsing claims: parsed ${count.toLocaleString()} rows...`);
        }

        const videoId = helper.get(row, 'Video ID', 'Video', 'video_id');
        if (!videoId) continue;

        const assetId = helper.get(row, 'Asset ID', 'Asset', 'asset_id');
        let isrc = helper.get(row, 'ISRC', 'isrc');
        let upc = helper.get(row, 'UPC', 'upc');
        let artist = helper.get(row, 'Artist', 'artist');
        let assetTitle = helper.get(row, 'Asset Title', 'Title', 'asset_title');
        let label = helper.get(row, 'Label', 'label', 'record_label');

        const views = parseInt(helper.get(row, 'Owned Views', 'Views', 'views'), 10) || 0;

        const claim = {
            asset_id: assetId,
            channel_id: helper.get(row, 'Channel ID', 'Channel', 'channel_id'),
            asset_type: helper.get(row, 'Asset Type', 'asset_type'),
            content_type: helper.get(row, 'Content Type', 'content_type'),
            claim_type: helper.get(row, 'Claim Type', 'claim_type'),
            policy: helper.get(row, 'Policy', 'policy'),
            claim_origin: helper.get(row, 'Claim Origin', 'claim_origin'),
            custom_id: helper.get(row, 'Custom ID', 'custom_id'),
            isrc,
            grid: helper.get(row, 'GRid', 'Grid', 'grid'),
            upc,
            video_title: helper.get(row, 'Video Title', 'video_title'),
            username: helper.get(row, 'Username', 'username'),
            uploader: helper.get(row, 'Uploader', 'uploader'),
            video_duration_sec: parseInt(helper.get(row, 'Video Duration', 'Video Duration (sec)', 'video_duration_sec'), 10) || 0,
            channel_display_name: helper.get(row, 'Channel Display Name', 'channel_display_name'),
            multiple_claims: helper.get(row, 'Multiple Claims', 'multiple_claims'),
            category: helper.get(row, 'Category', 'category'),
            asset_labels: helper.get(row, 'Asset Labels', 'asset_labels'),
            artist,
            asset_title: assetTitle,
            album: helper.get(row, 'Album', 'album'),
            label,
            views,
        };

        let list = map.get(videoId);
        if (!list) {
            list = [];
            map.set(videoId, list);
        }
        if (!list.some(c => c.asset_id === assetId)) {
            list.push(claim);
        }
    }
    if (log) {
        log(`[ETL] ✓ Finished parsing claims: ${count.toLocaleString()} rows mapped.`);
    }
    return { map, count };
}

// ── Ingest Workers ────────────────────────────────────────

export async function processEstimatedAds(opts: {
    filePath: string;
    claimsFilePath: string;
    assetRevenueFilePath?: string;
    channelRevenueFilePath?: string;
    channelTransactions?: Array<{ 
        channel_id: string; 
        channel_display_name?: string; 
        transaction_revenue?: number;
        total_revenue?: number;
        ad_revenue?: number;
        red_revenue?: number;
        total_remainder?: number;
        ad_remainder?: number;
        red_remainder?: number;
        tx_remainder?: number;
    }>;
    cmsId: string;
    day?: string;
    client: ClickHouseClient;
    batchSize?: number;
    log?: (msg: string) => void;
    onProgress?: (progress: { totalRows: number; processedRows: number; adsRows: number; subRows: number; batchesSent: number; claimsRows: number; adsRevenue: number; subRevenue: number }) => void;
    isAborted?: () => boolean;
    jobId?: string;
    totalCmsRevenue?: number;
    lowPriorityCount?: number;
}): Promise<{ totalRows: number; adsRows: number; subRows: number; claimsRows: number; adsRevenue: number; subRevenue: number; auditWarning?: boolean; auditMessage?: string }> {
    const { filePath, claimsFilePath, assetRevenueFilePath, channelRevenueFilePath, channelTransactions, cmsId, day, client, log, onProgress, isAborted, jobId = randomUUID(), totalCmsRevenue, lowPriorityCount } = opts;

    const cleanJobId = jobId.replace(/-/g, '_');
    const tempClaimsTable = `temp_claims_${cleanJobId}`;
    const tempEstimatedTable = `temp_estimated_${cleanJobId}`;
    const tempAssetTable = `temp_asset_${cleanJobId}`;
    const tempChannelTable = `temp_channel_${cleanJobId}`;

    if (log) {
        log(`[ETL] Creating temp tables: ${tempClaimsTable}, ${tempEstimatedTable}...`);
    }

    try {
        // 1. Create temporary tables
        if (assetRevenueFilePath) {
            await client.command({
                query: `
                    CREATE TABLE IF NOT EXISTS ${tempAssetTable} (
                        "Date"                                String DEFAULT '',
                        "Day"                                 String DEFAULT '',
                        "date"                                String DEFAULT '',
                        "day"                                 String DEFAULT '',
                        "Asset ID"                            String DEFAULT '',
                        "asset_id"                            String DEFAULT '',
                        "Asset"                               String DEFAULT '',
                        "Channel ID"                          String DEFAULT '',
                        "channel_id"                          String DEFAULT '',
                        "Channel"                             String DEFAULT '',
                        "Estimated partner revenue"           String DEFAULT '',
                        "Estimated partner revenue (USD)"     String DEFAULT '',
                        "estimated_partner_revenue"           String DEFAULT '',
                        "Estimated partner ad revenue"        String DEFAULT '',
                        "Estimated partner ad revenue (USD)"  String DEFAULT '',
                        "estimated_partner_ad_revenue"        String DEFAULT '',
                        "Estimated partner Premium revenue"   String DEFAULT '',
                        "Estimated partner Premium revenue (USD)" String DEFAULT '',
                        "Estimated partner Red revenue"       String DEFAULT '',
                        "Estimated partner Red revenue (USD)" String DEFAULT '',
                        "estimated_partner_red_revenue"       String DEFAULT '',
                        "Estimated partner transaction revenue" String DEFAULT '',
                        "Estimated partner transaction revenue (USD)" String DEFAULT '',
                        "estimated_partner_transaction_revenue" String DEFAULT ''
                    ) ENGINE = MergeTree()
                    ORDER BY tuple()
                `
            });
        }

        if (channelRevenueFilePath) {
            await client.command({
                query: `
                    CREATE TABLE IF NOT EXISTS ${tempChannelTable} (
                        "Date"                                String DEFAULT '',
                        "Day"                                 String DEFAULT '',
                        "date"                                String DEFAULT '',
                        "day"                                 String DEFAULT '',
                        "Channel ID"                          String DEFAULT '',
                        "channel_id"                          String DEFAULT '',
                        "Channel"                             String DEFAULT '',
                        "Estimated partner revenue"           String DEFAULT '',
                        "Estimated partner revenue (USD)"     String DEFAULT '',
                        "estimated_partner_revenue"           String DEFAULT '',
                        "Estimated partner ad revenue"        String DEFAULT '',
                        "Estimated partner ad revenue (USD)"  String DEFAULT '',
                        "estimated_partner_ad_revenue"        String DEFAULT '',
                        "Estimated partner Premium revenue"   String DEFAULT '',
                        "Estimated partner Premium revenue (USD)" String DEFAULT '',
                        "Estimated partner Red revenue"       String DEFAULT '',
                        "Estimated partner Red revenue (USD)" String DEFAULT '',
                        "estimated_partner_red_revenue"       String DEFAULT '',
                        "Estimated partner transaction revenue" String DEFAULT '',
                        "Estimated partner transaction revenue (USD)" String DEFAULT '',
                        "estimated_partner_transaction_revenue" String DEFAULT ''
                    ) ENGINE = MergeTree()
                    ORDER BY tuple()
                `
            });
        }
        await client.command({
            query: `
                CREATE TABLE IF NOT EXISTS ${tempClaimsTable} (
                    "Video ID"             String DEFAULT '',
                    "video_id"             String DEFAULT '',
                    "Video"                String DEFAULT '',
                    "Asset ID"             String DEFAULT '',
                    "asset_id"             String DEFAULT '',
                    "Asset"                String DEFAULT '',
                    "Channel ID"           String DEFAULT '',
                    "channel_id"           String DEFAULT '',
                    "Channel"              String DEFAULT '',
                    "Asset Type"           String DEFAULT '',
                    "asset_type"           String DEFAULT '',
                    "Content Type"         String DEFAULT '',
                    "content_type"         String DEFAULT '',
                    "Claim Type"           String DEFAULT '',
                    "claim_type"           String DEFAULT '',
                    "Policy"               String DEFAULT '',
                    "policy"               String DEFAULT '',
                    "Claim Origin"         String DEFAULT '',
                    "claim_origin"         String DEFAULT '',
                    "Custom ID"            String DEFAULT '',
                    "custom_id"            String DEFAULT '',
                    "ISRC"                 String DEFAULT '',
                    "isrc"                 String DEFAULT '',
                    "GRid"                 String DEFAULT '',
                    "grid"                 String DEFAULT '',
                    "Grid"                 String DEFAULT '',
                    "UPC"                  String DEFAULT '',
                    "upc"                  String DEFAULT '',
                    "Video Title"          String DEFAULT '',
                    "video_title"          String DEFAULT '',
                    "Username"             String DEFAULT '',
                    "username"             String DEFAULT '',
                    "Uploader"             String DEFAULT '',
                    "uploader"             String DEFAULT '',
                    "Video Duration"       String DEFAULT '',
                    "video_duration_sec"   String DEFAULT '',
                    "Video Duration (sec)" String DEFAULT '',
                    "Channel Display Name" String DEFAULT '',
                    "channel_display_name" String DEFAULT '',
                    "Multiple Claims"      String DEFAULT '',
                    "multiple_claims"      String DEFAULT '',
                    "Category"             String DEFAULT '',
                    "category"             String DEFAULT '',
                    "Asset Labels"         String DEFAULT '',
                    "asset_labels"         String DEFAULT '',
                    "Artist"               String DEFAULT '',
                    "artist"               String DEFAULT '',
                    "Asset Title"          String DEFAULT '',
                    "asset_title"          String DEFAULT '',
                    "Title"                String DEFAULT '',
                    "Album"                String DEFAULT '',
                    "album"                String DEFAULT '',
                    "Label"                String DEFAULT '',
                    "label"                String DEFAULT '',
                    "record_label"         String DEFAULT '',
                    "Views"                String DEFAULT '',
                    "views"                String DEFAULT '',
                    "Owned Views"          String DEFAULT '',
                    "claim_id"             String DEFAULT '',
                    "claim_status"         String DEFAULT '',
                    "Claim Status"         String DEFAULT '',
                    "claim_status_detail"  String DEFAULT '',
                    "engaged_views"        String DEFAULT '',
                    "matching_duration"    String DEFAULT '',
                    "video_matching_length" String DEFAULT '',
                    "longest_match"        String DEFAULT '',
                    "reference_video_id"   String DEFAULT '',
                    "reference_id"         String DEFAULT '',
                    "claim_policy_id"      String DEFAULT '',
                    "asset_policy_id"      String DEFAULT '',
                    "claim_policy_monetize" String DEFAULT '',
                    "claim_policy_track"   String DEFAULT '',
                    "claim_policy_block"   String DEFAULT '',
                    "asset_policy_monetize" String DEFAULT '',
                    "asset_policy_track"   String DEFAULT '',
                    "asset_policy_block"   String DEFAULT '',
                    "claim_created_date"   String DEFAULT '',
                    "video_upload_date"    String DEFAULT '',
                    "Video Upload Date"    String DEFAULT '',
                    "tms"                  String DEFAULT '',
                    "director"             String DEFAULT '',
                    "season"               String DEFAULT '',
                    "episode_number"       String DEFAULT '',
                    "episode_title"        String DEFAULT '',
                    "release_date"         String DEFAULT '',
                    "hfa_song_code"        String DEFAULT '',
                    "iswc"                 String DEFAULT '',
                    "writers"              String DEFAULT '',
                    "is_shorts_eligible"   String DEFAULT ''
                ) ENGINE = StripeLog()
            `
        });

        await client.command({
            query: `
                CREATE TABLE IF NOT EXISTS ${tempEstimatedTable} (
                    "Video ID"                             String DEFAULT '',
                    "video_id"                             String DEFAULT '',
                    "Video"                                String DEFAULT '',
                    "Date"                                 String DEFAULT '',
                    "Day"                                  String DEFAULT '',
                    "day"                                  String DEFAULT '',
                    "date"                                 String DEFAULT '',
                    "Country"                              String DEFAULT '',
                    "country_code"                         String DEFAULT '',
                    "country"                              String DEFAULT '',
                    "Channel ID"                           String DEFAULT '',
                    "channel_id"                           String DEFAULT '',
                    "Channel"                              String DEFAULT '',
                    "estimated_partner_revenue"            String DEFAULT '',
                    "Partner Revenue"                      String DEFAULT '',
                    "partner_rev_total"                    String DEFAULT '',
                    "estimated_partner_ad_revenue"         String DEFAULT '',
                    "estimatedPartnerAdRevenue"            String DEFAULT '',
                    "Estimated Partner Ad Revenue"         String DEFAULT '',
                    "estimated_partner_ad_auction_revenue" String DEFAULT '',
                    "Partner Revenue : Auction"            String DEFAULT '',
                    "partner_rev_auction"                  String DEFAULT '',
                    "estimated_partner_ad_reserved_revenue" String DEFAULT '',
                    "Partner Revenue : Reserved"           String DEFAULT '',
                    "partner_rev_reserved"                 String DEFAULT '',
                    "estimated_partner_red_revenue"        String DEFAULT '',
                    "Partner Revenue : Red"                String DEFAULT '',
                    "partner_rev_red"                      String DEFAULT '',
                    "estimated_youtube_ad_revenue"         String DEFAULT '',
                    "YouTube Revenue Split"                String DEFAULT '',
                    "yt_rev_total"                         String DEFAULT '',
                    "Asset ID"                             String DEFAULT '',
                    "asset_id"                             String DEFAULT '',
                    "Asset"                                String DEFAULT '',
                    "estimated_monetized_playbacks"        String DEFAULT '',
                    "Estimated Monetized Playbacks"         String DEFAULT '',
                    "ad_impressions"                        String DEFAULT '',
                    "adImpressions"                         String DEFAULT '',
                    "Ad Impressions"                        String DEFAULT '',
                    "Estimated partner transaction revenue" String DEFAULT '',
                    "Estimated partner transaction revenue (USD)" String DEFAULT '',
                    "estimated_partner_transaction_revenue" String DEFAULT '',
                    "estimatedPartnerTransactionRevenue"    String DEFAULT '',
                    "Partner Revenue : Transaction"         String DEFAULT '',
                    "Transaction Revenue"                   String DEFAULT '',
                    "partner_rev_transaction"               String DEFAULT '',
                    "estimated_playback_based_cpm"         String DEFAULT '',
                    "estimated_cpm"                        String DEFAULT '',
                    "Average CPM"                          String DEFAULT '',
                    "Playback-based CPM"                   String DEFAULT '',
                    "views"                                String DEFAULT '',
                    "Views"                                String DEFAULT '',
                    "Owned Views"                          String DEFAULT '',
                    "Video Title"                          String DEFAULT '',
                    "video_title"                          String DEFAULT '',
                    "uploader_type"                        String DEFAULT '',
                    "Uploader Type"                        String DEFAULT '',
                    "uploader"                             String DEFAULT '',
                    "claimed_status"                       String DEFAULT '',
                    "claimedStatus"                        String DEFAULT '',
                    "Claimed Status"                       String DEFAULT '',
                    "likes"                                String DEFAULT '',
                    "comments"                             String DEFAULT '',
                    "shares"                               String DEFAULT '',
                    "dislikes"                             String DEFAULT '',
                    "estimatedMinutesWatched"              String DEFAULT '',
                    "estimated_minutes_watched"            String DEFAULT '',
                    "averageViewDuration"                  String DEFAULT '',
                    "average_view_duration"                String DEFAULT '',
                    "averageViewPercentage"                String DEFAULT '',
                    "average_view_percentage"              String DEFAULT '',
                    "subscribersGained"                    String DEFAULT '',
                    "subscribers_gained"                   String DEFAULT '',
                    "subscribersLost"                      String DEFAULT '',
                    "subscribers_lost"                     String DEFAULT '',
                    "creator_content_type"                 String DEFAULT '',
                    "creatorContentType"                   String DEFAULT ''
                ) ENGINE = StripeLog()
            `
        });

        if (isAborted?.()) throw new Error("Job aborted by user");

        // 2. Stream claims file into ClickHouse
        if (log) {
            log(`[ETL] Streaming daily claims file into ClickHouse...`);
        }
        const claimsStream = await extractFileStream(claimsFilePath);
        const claimsDelimiter = await detectDelimiterFromFile(claimsFilePath);

        const claimsAbortInterval = setInterval(() => {
            if (isAborted?.()) {
                claimsStream.destroy();
                clearInterval(claimsAbortInterval);
            }
        }, 1000);

        try {
            await client.insert({
                table: tempClaimsTable,
                values: claimsStream,
                format: claimsDelimiter === '\t' ? 'TabSeparatedWithNames' : 'CSVWithNames',
                clickhouse_settings: {
                    input_format_skip_unknown_fields: 1
                }
            });
        } finally {
            clearInterval(claimsAbortInterval);
        }

        if (isAborted?.()) throw new Error("Job aborted by user");

        // 3. Stream estimated revenue file into ClickHouse
        if (log) {
            log(`[ETL] Streaming daily estimated revenue file into ClickHouse...`);
        }
        const estStream = await extractFileStream(filePath);
        const estDelimiter = await detectDelimiterFromFile(filePath);

        const estAbortInterval = setInterval(() => {
            if (isAborted?.()) {
                estStream.destroy();
                clearInterval(estAbortInterval);
            }
        }, 1000);

        try {
            await client.insert({
                table: tempEstimatedTable,
                values: estStream,
                format: estDelimiter === '\t' ? 'TabSeparatedWithNames' : 'CSVWithNames',
                clickhouse_settings: {
                    input_format_skip_unknown_fields: 1
                }
            });
        } finally {
            clearInterval(estAbortInterval);
        }

        if (assetRevenueFilePath) {
            if (log) log(`[ETL] Ingesting raw asset estimated revenue file...`);
            const assetStream = await extractFileStream(assetRevenueFilePath);
            const assetDelimiter = await detectDelimiterFromFile(assetRevenueFilePath);
            const assetAbortInterval = setInterval(() => {
                if (isAborted?.()) {
                    assetStream.destroy();
                    clearInterval(assetAbortInterval);
                }
            }, 1000);
            try {
                await client.insert({
                    table: tempAssetTable,
                    values: assetStream,
                    format: assetDelimiter === '\t' ? 'TabSeparatedWithNames' : 'CSVWithNames',
                    clickhouse_settings: { input_format_skip_unknown_fields: 1 }
                });
            } finally {
                clearInterval(assetAbortInterval);
            }
        }

        if (channelRevenueFilePath) {
            if (log) log(`[ETL] Ingesting raw channel estimated revenue file...`);
            const channelStream = await extractFileStream(channelRevenueFilePath);
            const channelDelimiter = await detectDelimiterFromFile(channelRevenueFilePath);
            const channelAbortInterval = setInterval(() => {
                if (isAborted?.()) {
                    channelStream.destroy();
                    clearInterval(channelAbortInterval);
                }
            }, 1000);
            try {
                await client.insert({
                    table: tempChannelTable,
                    values: channelStream,
                    format: channelDelimiter === '\t' ? 'TabSeparatedWithNames' : 'CSVWithNames',
                    clickhouse_settings: { input_format_skip_unknown_fields: 1 }
                });
            } finally {
                clearInterval(channelAbortInterval);
            }
        }

        if (isAborted?.()) throw new Error("Job aborted by user");

        // Resolve date if not passed
        let targetDay = day;
        if (!targetDay) {
            try {
                const dayQuery = `
                    SELECT coalesce(nullIf("Date", ''), nullIf("Day", ''), nullIf("day", ''), nullIf("date", '')) as raw_date 
                    FROM ${tempEstimatedTable} 
                    WHERE raw_date != '' 
                    LIMIT 1
                `;
                const dayResult = await client.query({ query: dayQuery, format: 'JSONEachRow' });
                const dayRows = await dayResult.json() as any[];
                const rawDate = dayRows[0]?.raw_date || '';
                if (rawDate) {
                    targetDay = rawDate.length === 8 
                        ? `${rawDate.substring(0, 4)}-${rawDate.substring(4, 6)}-${rawDate.substring(6, 8)}`
                        : rawDate;
                }
            } catch (e: any) {
                if (log) {
                    log(`[ETL] Warning: Failed to parse day from estimated table: ${e.message}`);
                }
            }
        }

        if (!targetDay) {
            throw new Error("Gagal mendeteksi tanggal transaksi (targetDay) dari data Laporan.");
        }

        if (targetDay) {
            if (log) {
                log(`[ETL] Cleaning existing raw staging records for CMS ${cmsId} and Date ${targetDay}...`);
            }
            try {
                await client.command({
                    query: `ALTER TABLE youtube_raw_claims DELETE WHERE cms_id = {cmsId: String} AND day = {targetDay: Date} SETTINGS mutations_sync = 1`,
                    query_params: { cmsId, targetDay }
                });
            } catch (e: any) {
                throw new Error(`Gagal menghapus data claims raw lama: ${e.message}`);
            }

            try {
                await client.command({
                    query: `ALTER TABLE youtube_raw_asset_estimated_revenue DELETE WHERE cms_id = {cmsId: String} AND day = {targetDay: Date} SETTINGS mutations_sync = 1`,
                    query_params: { cmsId, targetDay }
                });
            } catch (e: any) {
                throw new Error(`Gagal menghapus data asset estimated revenue raw lama: ${e.message}`);
            }

            try {
                await client.command({
                    query: `ALTER TABLE youtube_raw_estimated_revenue DELETE WHERE cms_id = {cmsId: String} AND day = {targetDay: Date} SETTINGS mutations_sync = 1`,
                    query_params: { cmsId, targetDay }
                });
            } catch (e: any) {
                throw new Error(`Gagal menghapus data estimated revenue raw lama: ${e.message}`);
            }

            if (log) {
                log(`[ETL] Copying claims to permanent raw staging table...`);
            }

            const insertRawClaimsQuery = `
                INSERT INTO youtube_raw_claims (
                    cms_id, day, video_id, asset_id, channel_id, asset_type, content_type, claim_type, policy,
                    claim_origin, custom_id, isrc, grid, upc, video_title, username, uploader, video_duration_sec,
                    channel_display_name, multiple_claims, category, asset_labels, artist, asset_title, album, label,
                    views,
                    claim_id, claim_status, claim_status_detail, record_label, engaged_views, matching_duration,
                    video_matching_length, longest_match, reference_video_id, reference_id, claim_policy_id,
                    asset_policy_id, claim_policy_monetize, claim_policy_track, claim_policy_block,
                    asset_policy_monetize, asset_policy_track, asset_policy_block, claim_created_date,
                    video_upload_date, tms, director, season, episode_number, episode_title, release_date,
                    hfa_song_code, iswc, writers, is_shorts_eligible
                )
                SELECT
                    {cmsId: String},
                    {targetDay: Date},
                    coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf(Video, '')),
                    coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), nullIf(Asset, '')),
                    coalesce(nullIf("Channel ID", ''), nullIf(channel_id, ''), nullIf(Channel, '')),
                    coalesce(nullIf("Asset Type", ''), nullIf(asset_type, '')),
                    coalesce(nullIf("Content Type", ''), nullIf(content_type, '')),
                    coalesce(nullIf("Claim Type", ''), nullIf(claim_type, '')),
                    coalesce(nullIf("Policy", ''), nullIf(policy, '')),
                    coalesce(nullIf("Claim Origin", ''), nullIf(claim_origin, '')),
                    coalesce(nullIf("Custom ID", ''), nullIf(custom_id, '')),
                    coalesce(nullIf("ISRC", ''), nullIf(isrc, '')),
                    coalesce(nullIf("GRid", ''), nullIf(grid, ''), nullIf(Grid, '')),
                    coalesce(nullIf("UPC", ''), nullIf(upc, '')),
                    coalesce(nullIf("Video Title", ''), nullIf(video_title, '')),
                    coalesce(nullIf("Username", ''), nullIf(username, '')),
                    coalesce(nullIf("Uploader", ''), nullIf(uploader, '')),
                    toInt32OrZero(coalesce(nullIf("Video Duration", ''), nullIf(video_duration_sec, ''), nullIf("Video Duration (sec)", ''))),
                    coalesce(nullIf("Channel Display Name", ''), nullIf(channel_display_name, '')),
                    coalesce(nullIf("Multiple Claims", ''), nullIf(multiple_claims, '')),
                    coalesce(nullIf("Category", ''), nullIf(category, '')),
                    coalesce(nullIf("Asset Labels", ''), nullIf(asset_labels, '')),
                    coalesce(nullIf("Artist", ''), nullIf(artist, '')),
                    coalesce(nullIf("Asset Title", ''), nullIf(asset_title, ''), nullIf(Title, '')),
                    coalesce(nullIf("Album", ''), nullIf(album, '')),
                    coalesce(nullIf("Label", ''), nullIf(label, ''), nullIf(record_label, '')),
                    toInt64OrZero(coalesce(nullIf("Views", ''), nullIf(views, ''), nullIf("Owned Views", ''))),
                    coalesce(nullIf(claim_id, ''), ''),
                    coalesce(nullIf(claim_status, ''), ''),
                    coalesce(nullIf(claim_status_detail, ''), ''),
                    coalesce(nullIf(record_label, ''), ''),
                    toInt64OrZero(coalesce(nullIf(engaged_views, ''), '0')),
                    coalesce(nullIf(matching_duration, ''), ''),
                    coalesce(nullIf(video_matching_length, ''), ''),
                    coalesce(nullIf(longest_match, ''), ''),
                    coalesce(nullIf(reference_video_id, ''), ''),
                    coalesce(nullIf(reference_id, ''), ''),
                    coalesce(nullIf(claim_policy_id, ''), ''),
                    coalesce(nullIf(asset_policy_id, ''), ''),
                    coalesce(nullIf(claim_policy_monetize, ''), ''),
                    coalesce(nullIf(claim_policy_track, ''), ''),
                    coalesce(nullIf(claim_policy_block, ''), ''),
                    coalesce(nullIf(asset_policy_monetize, ''), ''),
                    coalesce(nullIf(asset_policy_track, ''), ''),
                    coalesce(nullIf(asset_policy_block, ''), ''),
                    coalesce(nullIf(claim_created_date, ''), ''),
                    coalesce(nullIf(video_upload_date, ''), ''),
                    coalesce(nullIf(tms, ''), ''),
                    coalesce(nullIf(director, ''), ''),
                    coalesce(nullIf(season, ''), ''),
                    coalesce(nullIf(episode_number, ''), ''),
                    coalesce(nullIf(episode_title, ''), ''),
                    coalesce(nullIf(release_date, ''), ''),
                    coalesce(nullIf(hfa_song_code, ''), ''),
                    coalesce(nullIf(iswc, ''), ''),
                    coalesce(nullIf(writers, ''), ''),
                    coalesce(nullIf(is_shorts_eligible, ''), '')
                FROM ${tempClaimsTable}
            `;
            await client.command({ 
                query: insertRawClaimsQuery,
                query_params: { cmsId, targetDay }
            });

            if (log) {
                log(`[ETL] Copying estimated revenue to permanent raw staging table...`);
            }

            const insertRawEstQuery = `
                INSERT INTO youtube_raw_estimated_revenue (
                    cms_id, day, video_id, date, country, channel_id, claimed_status, uploader_type,
                    estimated_partner_revenue, estimated_partner_ad_revenue, estimated_partner_ad_auction_revenue,
                    estimated_partner_ad_reserved_revenue, estimated_youtube_ad_revenue, estimated_monetized_playbacks,
                    ad_impressions, estimated_partner_red_revenue, estimated_partner_transaction_revenue,
                    country_code, estimated_playback_based_cpm, estimated_cpm,
                    views, video_title, uploader
                )
                SELECT
                    {cmsId: String},
                    {targetDay: Date},
                    coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf(Video, '')),
                    coalesce(nullIf("Date", ''), nullIf("Day", ''), nullIf("day", ''), nullIf("date", '')),
                    coalesce(nullIf("Country", ''), nullIf(country_code, ''), nullIf(country, '')),
                    coalesce(nullIf("Channel ID", ''), nullIf(channel_id, ''), nullIf(Channel, '')),
                    coalesce(nullIf(claimed_status, ''), nullIf(claimedStatus, ''), nullIf("Claimed Status", ''), ''),
                    coalesce(nullIf(uploader_type, ''), ''),
                    CAST(coalesce(nullIf(estimated_partner_revenue, ''), nullIf("Partner Revenue", ''), nullIf(partner_rev_total, ''), '0') AS Decimal64(10)),
                    CAST(coalesce(nullIf(estimated_partner_ad_revenue, ''), nullIf(estimatedPartnerAdRevenue, ''), nullIf("Estimated Partner Ad Revenue", ''), '0') AS Decimal64(10)),
                    CAST(coalesce(nullIf(estimated_partner_ad_auction_revenue, ''), nullIf("Partner Revenue : Auction", ''), nullIf(partner_rev_auction, ''), '0') AS Decimal64(10)),
                    CAST(coalesce(nullIf(estimated_partner_ad_reserved_revenue, ''), nullIf("Partner Revenue : Reserved", ''), nullIf(partner_rev_reserved, ''), '0') AS Decimal64(10)),
                    CAST(coalesce(nullIf(estimated_youtube_ad_revenue, ''), nullIf("YouTube Revenue Split", ''), nullIf(yt_rev_total, ''), '0') AS Decimal64(10)),
                    toInt64OrZero(coalesce(nullIf(estimated_monetized_playbacks, ''), nullIf("Estimated Monetized Playbacks", ''), '0')),
                    toInt64OrZero(coalesce(nullIf(ad_impressions, ''), nullIf("Ad Impressions", ''), nullIf(adImpressions, ''), '0')),
                    CAST(coalesce(nullIf(estimated_partner_red_revenue, ''), nullIf("Partner Revenue : Red", ''), nullIf(partner_rev_red, ''), '0') AS Decimal64(10)),
                    CAST(coalesce(nullIf("Estimated partner transaction revenue (USD)", ''), nullIf("Estimated partner transaction revenue", ''), nullIf(estimated_partner_transaction_revenue, ''), nullIf("Partner Revenue : Transaction", ''), nullIf(estimatedPartnerTransactionRevenue, ''), nullIf(partner_rev_transaction, ''), '0') AS Decimal64(10)),
                    coalesce(nullIf(country_code, ''), ''),
                    CAST(coalesce(nullIf(estimated_playback_based_cpm, ''), '0') AS Decimal64(10)),
                    CAST(coalesce(nullIf(estimated_cpm, ''), '0') AS Decimal64(10)),
                    toInt64OrZero(replaceAll(coalesce(nullIf(views, ''), nullIf("Views", ''), '0'), ',', '')),
                    coalesce(nullIf(video_title, ''), nullIf("Video Title", '')),
                    coalesce(nullIf(uploader, ''), '')
                FROM ${tempEstimatedTable}
            `;
            await client.command({ 
                query: insertRawEstQuery,
                query_params: { cmsId, targetDay }
            });

            if (assetRevenueFilePath) {
                if (log) {
                    log(`[ETL] Copying asset estimated revenue to permanent raw staging table...`);
                }
                const insertRawAssetQuery = `
                    INSERT INTO youtube_raw_asset_estimated_revenue (
                        cms_id, day, asset_id, channel_id,
                        estimated_partner_revenue, estimated_partner_ad_revenue,
                        estimated_partner_red_revenue, estimated_partner_transaction_revenue
                    )
                    SELECT
                        {cmsId: String},
                        {targetDay: Date},
                        coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), nullIf(Asset, ''), ''),
                        coalesce(nullIf("Channel ID", ''), nullIf(channel_id, ''), nullIf(Channel, ''), ''),
                        CAST(coalesce(nullIf("Estimated partner revenue (USD)", ''), nullIf("Estimated partner revenue", ''), nullIf(estimated_partner_revenue, ''), '0') AS Decimal64(10)),
                        CAST(coalesce(nullIf("Estimated partner ad revenue (USD)", ''), nullIf("Estimated partner ad revenue", ''), nullIf(estimated_partner_ad_revenue, ''), '0') AS Decimal64(10)),
                        CAST(coalesce(nullIf("Estimated partner Premium revenue (USD)", ''), nullIf("Estimated partner Premium revenue", ''), nullIf("Estimated partner Red revenue (USD)", ''), nullIf("Estimated partner Red revenue", ''), nullIf(estimated_partner_red_revenue, ''), '0') AS Decimal64(10)),
                        CAST(coalesce(nullIf("Estimated partner transaction revenue (USD)", ''), nullIf("Estimated partner transaction revenue", ''), nullIf(estimated_partner_transaction_revenue, ''), '0') AS Decimal64(10))
                    FROM ${tempAssetTable}
                `;
                await client.command({
                    query: insertRawAssetQuery,
                    query_params: { cmsId, targetDay }
                });
            }

            if (channelRevenueFilePath) {
                if (log) {
                    log(`[ETL] Copying channel estimated revenue to permanent raw staging table...`);
                }
                const insertRawChannelQuery = `
                    INSERT INTO youtube_raw_channel_estimated_revenue (
                        cms_id, day, channel_id,
                        estimated_partner_revenue, estimated_partner_ad_revenue,
                        estimated_partner_red_revenue, estimated_partner_transaction_revenue
                    )
                    SELECT
                        {cmsId: String},
                        {targetDay: Date},
                        coalesce(nullIf("Channel ID", ''), nullIf(channel_id, ''), nullIf(Channel, ''), ''),
                        CAST(coalesce(nullIf("Estimated partner revenue (USD)", ''), nullIf("Estimated partner revenue", ''), nullIf(estimated_partner_revenue, ''), '0') AS Decimal64(10)),
                        CAST(coalesce(nullIf("Estimated partner ad revenue (USD)", ''), nullIf("Estimated partner ad revenue", ''), nullIf(estimated_partner_ad_revenue, ''), '0') AS Decimal64(10)),
                        CAST(coalesce(nullIf("Estimated partner Premium revenue (USD)", ''), nullIf("Estimated partner Premium revenue", ''), nullIf("Estimated partner Red revenue (USD)", ''), nullIf("Estimated partner Red revenue", ''), nullIf(estimated_partner_red_revenue, ''), '0') AS Decimal64(10)),
                        CAST(coalesce(nullIf("Estimated partner transaction revenue (USD)", ''), nullIf("Estimated partner transaction revenue", ''), nullIf(estimated_partner_transaction_revenue, ''), '0') AS Decimal64(10))
                    FROM ${tempChannelTable}
                `;
                await client.command({
                    query: insertRawChannelQuery,
                    query_params: { cmsId, targetDay }
                });
            }
        }

        // 4. Calculate ingestion metrics from temp tables (only claimed/valid rows)
        if (log) {
            log(`[ETL] Calculating metrics from temporary tables...`);
        }

        const metricsQuery = `
            WITH
            deduplicated_claims AS (
                SELECT
                    coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf(Video, '')) AS video_id,
                    coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), nullIf(Asset, '')) AS asset_id
                FROM ${tempClaimsTable}
                WHERE coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), nullIf(Asset, '')) != ''
                LIMIT 1 BY (video_id, asset_id)
            ),
            count_claims AS (
                SELECT video_id, count() as cnt FROM deduplicated_claims GROUP BY video_id
            ),
            clean_estimated AS (
                SELECT
                    coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf(Video, '')) AS video_id,
                    coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), nullIf(Asset, ''), '') AS asset_id,
                    CAST(coalesce(nullIf(estimated_partner_revenue, ''), nullIf("Partner Revenue", ''), nullIf(partner_rev_total, ''), '0') AS Decimal64(10)) as partner_rev_total,
                    CAST(coalesce(nullIf(estimated_partner_ad_auction_revenue, ''), nullIf("Partner Revenue : Auction", ''), nullIf(partner_rev_auction, ''), '0') AS Decimal64(10)) as partner_rev_auction,
                    CAST(coalesce(nullIf(estimated_partner_ad_reserved_revenue, ''), nullIf("Partner Revenue : Reserved", ''), nullIf(partner_rev_reserved, ''), '0') AS Decimal64(10)) as partner_rev_reserved,
                    CAST(coalesce(nullIf(estimated_partner_red_revenue, ''), nullIf("Partner Revenue : Red", ''), nullIf(partner_rev_red, ''), '0') AS Decimal64(10)) as partner_rev_red
                FROM ${tempEstimatedTable}
            ),
            combined_rows AS (
                -- Rows with non-empty Asset ID
                SELECT partner_rev_total, partner_rev_auction, partner_rev_reserved, partner_rev_red
                FROM clean_estimated
                WHERE asset_id != ''
                
                UNION ALL
                
                -- Rows with empty Asset ID, left joined with claims
                SELECT 
                    CAST(E.partner_rev_total / if(coalesce(count_claims.cnt, 0) = 0, toInt64(1), toInt64(count_claims.cnt)) AS Decimal64(10)) as partner_rev_total,
                    CAST(E.partner_rev_auction / if(coalesce(count_claims.cnt, 0) = 0, toInt64(1), toInt64(count_claims.cnt)) AS Decimal64(10)) as partner_rev_auction,
                    CAST(E.partner_rev_reserved / if(coalesce(count_claims.cnt, 0) = 0, toInt64(1), toInt64(count_claims.cnt)) AS Decimal64(10)) as partner_rev_reserved,
                    CAST(E.partner_rev_red / if(coalesce(count_claims.cnt, 0) = 0, toInt64(1), toInt64(count_claims.cnt)) AS Decimal64(10)) as partner_rev_red
                FROM clean_estimated E
                LEFT JOIN deduplicated_claims C ON E.video_id = C.video_id
                LEFT JOIN count_claims ON E.video_id = count_claims.video_id
                WHERE E.asset_id = ''
            )
            SELECT
                count() as total_rows,
                sum(if(partner_rev_auction > 0 OR partner_rev_reserved > 0, 1, 0)) as ads_rows,
                sum(if(partner_rev_red > 0, 1, 0)) as sub_rows,
                sum(partner_rev_total - partner_rev_red) as ads_revenue,
                sum(partner_rev_red) as sub_revenue
            FROM combined_rows
        `;

        const metricsResult = await client.query({
            query: metricsQuery,
            format: 'JSONEachRow'
        });
        const metricsRows = await metricsResult.json() as any[];
        const metrics = metricsRows[0] || { total_rows: 0, ads_rows: 0, sub_rows: 0, ads_revenue: 0, sub_revenue: 0 };

        const totalRows = parseInt(metrics.total_rows, 10) || 0;
        const adsRows = parseInt(metrics.ads_rows, 10) || 0;
        const subRows = parseInt(metrics.sub_rows, 10) || 0;
        const adsRevenue = parseFloat(metrics.ads_revenue) || 0;
        const subRevenue = parseFloat(metrics.sub_revenue) || 0;

        const claimsCountQuery = `SELECT count() as cnt FROM ${tempClaimsTable}`;
        const claimsCountResult = await client.query({ query: claimsCountQuery, format: 'JSONEachRow' });
        const claimsCountRows = await claimsCountResult.json() as any[];
        const claimsRows = parseInt(claimsCountRows[0]?.cnt, 10) || 0;

        const rawEstCountResult = await client.query({ query: `SELECT count() as cnt FROM ${tempEstimatedTable}`, format: 'JSONEachRow' });
        const rawEstCountRows = await rawEstCountResult.json() as any[];
        const rawEstRows = parseInt(rawEstCountRows[0]?.cnt, 10) || 0;

        if (log) {
            log(`[ETL] Claims rows: ${claimsRows.toLocaleString()} | Raw Revenue rows: ${rawEstRows.toLocaleString()} -> Ingested Claimed Revenue rows: ${totalRows.toLocaleString()}`);
        }
        // 5. Execute JOINs & Insertion
        const joinQueryA = `
            INSERT INTO estimated_revenue_daily (
                cms_id, day, country, video_id, channel_id, owner_channel_id, asset_id, asset_type, content_type, claimed_status, claim_type, claim_status,
                policy, claim_origin, isrc, upc, grid, custom_id, video_title, username, uploader, uploader_type, creator_content_type, video_duration_sec, video_upload_date,
                channel_display_name, multiple_claims, category, asset_labels, artist, asset_title, album, label, genre,
                owned_views, estimated_cpm, estimated_playback_based_cpm, yt_rev_auction, yt_rev_reserved, yt_rev_partner_sold_yt_served, yt_rev_partner_sold_p_served,
                yt_rev_red, yt_rev_total, partner_rev_auction, partner_rev_reserved, partner_rev_partner_sold_yt_served,
                partner_rev_partner_sold_p_served, partner_rev_red, partner_rev_total,
                monetized_playbacks, ad_impressions, partner_rev_transaction,
                likes, comments, shares, dislikes, watch_time_minutes, average_view_duration_seconds, average_view_duration_percentage, subscribers_gained, subscribers_lost
            )
            WITH
            asset_owners AS (
                SELECT 
                    asset_id, 
                    argMax(channel_id, ingested_at) as owner_channel_id
                FROM youtube_raw_claims
                WHERE cms_id = {cmsId: String}
                  AND content_type = 'PARTNER_UPLOADED'
                  AND asset_id != ''
                  AND channel_id != ''
                GROUP BY asset_id
            ),
            actual_views AS (
                SELECT 
                    video_id, 
                    sum(views) as total_actual_views 
                FROM video_devices_daily 
                WHERE cms_id = {cmsId: String} AND day = {targetDay: Date}
                GROUP BY video_id
            ),
            video_interactions AS (
                SELECT 
                    video_id,
                    sum(likes) as likes,
                    sum(dislikes) as dislikes,
                    sum(comments) as comments,
                    sum(shares) as shares
                FROM video_interactions_daily
                WHERE cms_id = {cmsId: String} AND day = {targetDay: Date}
                GROUP BY video_id
            ),
            video_devices AS (
                SELECT 
                    video_id,
                    sum(views) as device_views,
                    sum(watch_time_sec) as watch_time_sec
                FROM video_devices_daily
                WHERE cms_id = {cmsId: String} AND day = {targetDay: Date}
                GROUP BY video_id
            ),
            raw_stats AS (
                SELECT 
                    coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf(Video, '')) AS video_id, 
                    sum(toInt64OrZero(coalesce(nullIf(estimated_monetized_playbacks, ''), nullIf("Estimated Monetized Playbacks", ''), '0'))) as total_mp,
                    sum(toInt64OrZero(coalesce(nullIf(views, ''), nullIf("Views", ''), nullIf("Owned Views", ''), '0'))) as total_views,
                    count() as row_cnt
                FROM ${tempEstimatedTable}
                GROUP BY video_id
            ),
            metadata_claims AS (
                SELECT
                    custom_id AS video_id,
                    asset_id,
                    '' AS channel_id,
                    '' AS asset_type,
                    'PARTNER_UPLOADED' AS content_type,
                    'AUDIOVISUAL' AS claim_type,
                    '' AS policy,
                    'METADATA_MATCH' AS claim_origin,
                    custom_id,
                    '' AS isrc,
                    '' AS grid,
                    '' AS upc,
                    asset_title AS video_title,
                    '' AS username,
                    '' AS uploader,
                    0 AS video_duration_sec,
                    '' AS channel_display_name,
                    '' AS multiple_claims,
                    '' AS category,
                    '' AS asset_labels,
                    artist,
                    asset_title,
                    album,
                    label,
                    toInt64(0) AS views,
                    '' AS claim_status,
                    '' AS video_upload_date
                FROM youtube_asset_metadata
                WHERE cms_id = {cmsId: String}
                  AND length(custom_id) = 11
                  AND (custom_id, asset_id, day) IN (
                      SELECT custom_id, asset_id, max(day)
                      FROM youtube_asset_metadata
                      WHERE cms_id = {cmsId: String}
                        AND length(custom_id) = 11
                        AND custom_id IN (
                            SELECT DISTINCT coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf(Video, ''))
                            FROM ${tempEstimatedTable}
                        )
                      GROUP BY custom_id, asset_id
                  )
            ),
            deduplicated_claims AS (
                SELECT
                    video_id, asset_id, channel_id, asset_type, content_type, claim_type,
                    policy, claim_origin, custom_id, ISRC as isrc, Grid as grid, UPC as upc,
                    video_title, username, uploader, video_duration_sec, channel_display_name,
                    multiple_claims, category, asset_labels, artist, asset_title, album, label, views,
                    claim_status, video_upload_date
                FROM (
                    SELECT
                        coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf(Video, '')) AS video_id,
                        coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), nullIf(Asset, '')) AS asset_id,
                        coalesce(nullIf("Channel ID", ''), nullIf(channel_id, ''), nullIf(Channel, '')) AS channel_id,
                        coalesce(nullIf("Asset Type", ''), nullIf(asset_type, '')) AS asset_type,
                        coalesce(nullIf("Content Type", ''), nullIf(content_type, '')) AS content_type,
                        coalesce(nullIf("Claim Type", ''), nullIf(claim_type, '')) AS claim_type,
                        coalesce(nullIf("Policy", ''), nullIf(policy, '')) AS policy,
                        coalesce(nullIf("Claim Origin", ''), nullIf(claim_origin, '')) AS claim_origin,
                        coalesce(nullIf("Custom ID", ''), nullIf(custom_id, '')) AS custom_id,
                        coalesce(nullIf("ISRC", ''), nullIf(isrc, '')) AS ISRC,
                        coalesce(nullIf("GRid", ''), nullIf(grid, ''), nullIf(Grid, '')) AS Grid,
                        coalesce(nullIf("UPC", ''), nullIf(upc, '')) AS UPC,
                        coalesce(nullIf("Video Title", ''), nullIf(video_title, '')) AS video_title,
                        coalesce(nullIf("Username", ''), nullIf(username, '')) AS username,
                        coalesce(nullIf("Uploader", ''), nullIf(uploader, '')) AS uploader,
                        toInt32OrZero(coalesce(nullIf("Video Duration", ''), nullIf(video_duration_sec, ''), nullIf("Video Duration (sec)", ''))) AS video_duration_sec,
                        coalesce(nullIf("Channel Display Name", ''), nullIf(channel_display_name, '')) AS channel_display_name,
                        coalesce(nullIf("Multiple Claims", ''), nullIf(multiple_claims, '')) AS multiple_claims,
                        coalesce(nullIf("Category", ''), nullIf(category, '')) AS category,
                        coalesce(nullIf("Asset Labels", ''), nullIf(asset_labels, '')) AS asset_labels,
                        coalesce(nullIf("Artist", ''), nullIf(artist, '')) AS artist,
                        coalesce(nullIf("Asset Title", ''), nullIf(asset_title, ''), nullIf(Title, '')) AS asset_title,
                        coalesce(nullIf("Album", ''), nullIf(album, '')) AS album,
                        coalesce(nullIf("Label", ''), nullIf(label, ''), nullIf(record_label, '')) AS label,
                        toInt64OrZero(coalesce(nullIf("Views", ''), nullIf(views, ''), nullIf("Owned Views", ''))) AS views,
                        coalesce(nullIf("Claim Status", ''), nullIf(claim_status, ''), '') AS claim_status,
                        coalesce(nullIf("Video Upload Date", ''), nullIf(video_upload_date, ''), '') AS video_upload_date
                    FROM ${tempClaimsTable}
                    WHERE coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), nullIf(Asset, '')) != ''
                      AND coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf(Video, '')) IN (
                          SELECT DISTINCT coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf(Video, ''))
                          FROM ${tempEstimatedTable}
                      )

                    UNION ALL

                    SELECT
                        video_id, asset_id, channel_id, asset_type, content_type, claim_type,
                        policy, claim_origin, custom_id, isrc AS ISRC, grid AS Grid, upc AS UPC,
                        video_title, username, uploader, video_duration_sec, channel_display_name,
                        multiple_claims, category, asset_labels, artist, asset_title, album, label, views,
                        claim_status, video_upload_date
                    FROM metadata_claims
                )
                LIMIT 1 BY (video_id, asset_id)
            ),
            count_claims AS (
                SELECT video_id, count() as cnt FROM deduplicated_claims GROUP BY video_id
            ),
            clean_estimated AS (
                SELECT
                    coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf(Video, '')) AS video_id,
                    toDateOrZero(if(length(coalesce(nullIf("Date", ''), nullIf("Day", ''), nullIf("day", ''), nullIf("date", ''))) = 8,
                        concat(substring(coalesce(nullIf("Date", ''), nullIf("Day", ''), nullIf("day", ''), nullIf("date", '')), 1, 4), '-',
                               substring(coalesce(nullIf("Date", ''), nullIf("Day", ''), nullIf("day", ''), nullIf("date", '')), 5, 2), '-',
                               substring(coalesce(nullIf("Date", ''), nullIf("Day", ''), nullIf("day", ''), nullIf("date", '')), 7, 2)),
                        coalesce(nullIf("Date", ''), nullIf("Day", ''), nullIf("day", ''), nullIf("date", ''))
                    )) AS day,
                    coalesce(nullIf("Country", ''), nullIf(country_code, ''), nullIf(country, '')) AS country,
                    coalesce(nullIf("Channel ID", ''), nullIf(channel_id, ''), nullIf(Channel, '')) AS channel_id,
                    CAST(coalesce(nullIf(estimated_partner_revenue, ''), nullIf("Partner Revenue", ''), nullIf(partner_rev_total, ''), '0') AS Decimal64(10)) as partner_rev_total,
                    CAST(coalesce(nullIf(estimated_partner_ad_auction_revenue, ''), nullIf("Partner Revenue : Auction", ''), nullIf(partner_rev_auction, ''), '0') AS Decimal64(10)) as partner_rev_auction,
                    CAST(coalesce(nullIf(estimated_partner_ad_reserved_revenue, ''), nullIf("Partner Revenue : Reserved", ''), nullIf(partner_rev_reserved, ''), '0') AS Decimal64(10)) as partner_rev_reserved,
                    CAST(coalesce(nullIf(estimated_partner_red_revenue, ''), nullIf("Partner Revenue : Red", ''), nullIf(partner_rev_red, ''), '0') AS Decimal64(10)) as partner_rev_red,
                    CAST(coalesce(nullIf(estimated_youtube_ad_revenue, ''), nullIf("YouTube Revenue Split", ''), nullIf(yt_rev_total, ''), '0') AS Decimal64(10)) as yt_rev_total,
                    CAST(0 AS Decimal64(10)) as yt_rev_auction,
                    CAST(0 AS Decimal64(10)) as yt_rev_reserved,
                    CAST(0 AS Decimal64(10)) as yt_rev_red,
                    coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), nullIf(Asset, ''), '') AS asset_id,
                    toInt64OrZero(coalesce(nullIf(replaceAll(coalesce(nullIf(views, ''), nullIf("Views", ''), '0'), ',', ''), '0'), nullIf(estimated_monetized_playbacks, ''), '0')) AS views,
                    toInt64OrZero(coalesce(nullIf(estimated_monetized_playbacks, ''), nullIf("Estimated Monetized Playbacks", ''), '0')) AS monetized_playbacks,
                    toInt64OrZero(coalesce(nullIf(ad_impressions, ''), nullIf("Ad Impressions", ''), nullIf(adImpressions, ''), '0')) AS ad_impressions,
                    CAST(coalesce(nullIf("Estimated partner transaction revenue (USD)", ''), nullIf("Estimated partner transaction revenue", ''), nullIf(estimated_partner_transaction_revenue, ''), nullIf("Partner Revenue : Transaction", ''), nullIf(estimatedPartnerTransactionRevenue, ''), nullIf(partner_rev_transaction, ''), '0') AS Decimal64(10)) AS partner_rev_transaction,
                    coalesce(nullIf(video_title, ''), nullIf("Video Title", '')) AS video_title,
                    coalesce(nullIf(uploader_type, ''), nullIf(uploader, '')) AS uploader,
                    coalesce(nullIf(claimed_status, ''), nullIf("Claimed Status", ''), '') AS claimed_status,
                    coalesce(nullIf(uploader_type, ''), nullIf("Uploader Type", ''), '') AS uploader_type,
                    CAST(coalesce(nullIf(estimated_cpm, ''), nullIf("Average CPM", ''), '0') AS Decimal64(10)) AS estimated_cpm,
                    CAST(coalesce(nullIf(estimated_playback_based_cpm, ''), nullIf("Playback-based CPM", ''), '0') AS Decimal64(10)) AS estimated_playback_based_cpm,
                    toInt64OrZero(coalesce(nullIf(likes, ''), '0')) AS likes,
                    toInt64OrZero(coalesce(nullIf(comments, ''), '0')) AS comments,
                    toInt64OrZero(coalesce(nullIf(shares, ''), '0')) AS shares,
                    toInt64OrZero(coalesce(nullIf(dislikes, ''), '0')) AS dislikes,
                    toFloat64OrZero(coalesce(nullIf(estimatedMinutesWatched, ''), nullIf(estimated_minutes_watched, ''), '0.0')) AS watch_time_minutes,
                    toInt32OrZero(coalesce(nullIf(averageViewDuration, ''), nullIf(average_view_duration, ''), '0')) AS average_view_duration_seconds,
                    toFloat64OrZero(coalesce(nullIf(averageViewPercentage, ''), nullIf(average_view_percentage, ''), '0.0')) AS average_view_duration_percentage,
                    toInt64OrZero(coalesce(nullIf(subscribersGained, ''), nullIf(subscribers_gained, ''), '0')) AS subscribers_gained,
                    toInt64OrZero(coalesce(nullIf(subscribersLost, ''), nullIf(subscribers_lost, ''), '0')) AS subscribers_lost,
                    coalesce(nullIf(creator_content_type, ''), nullIf(creatorContentType, ''), '') AS creator_content_type
                FROM ${tempEstimatedTable}
            ),
            latest_metadata AS (
                SELECT 
                    asset_id,
                    argMax(asset_title, day) AS asset_title,
                    argMax(artist, day) AS artist,
                    argMax(album, day) AS album,
                    argMax(label, day) AS label,
                    argMax(isrc, day) AS isrc,
                    argMax(upc, day) AS upc,
                    argMax(custom_id, day) AS custom_id,
                    argMax(genre, day) AS genre,
                    argMax(asset_labels, day) AS asset_labels
                FROM youtube_asset_metadata
                WHERE cms_id = {cmsId: String}
                  AND asset_id IN (
                      SELECT DISTINCT coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), nullIf(Asset, ''), '')
                      FROM ${tempEstimatedTable}
                      WHERE coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), nullIf(Asset, ''), '') != ''
                  )
                GROUP BY asset_id
            )
            SELECT
                {cmsId: String}, E.day, E.country, E.video_id, coalesce(nullIf(C.channel_id, ''), E.channel_id) AS channel_id,
                if(coalesce(nullIf(C.content_type, ''), 'UGC') LIKE '%UGC%',
                    coalesce(nullIf(AO.owner_channel_id, ''), coalesce(nullIf(C.channel_id, ''), E.channel_id)),
                    coalesce(nullIf(C.channel_id, ''), E.channel_id)
                ) AS owner_channel_id,
                E.asset_id,
                coalesce(C.asset_type, ''), coalesce(C.content_type, ''), E.claimed_status, coalesce(C.claim_type, ''), coalesce(C.claim_status, ''),
                coalesce(C.policy, ''), coalesce(C.claim_origin, ''),
                coalesce(nullIf(M.isrc, ''), nullIf(C.isrc, ''), ''),
                coalesce(nullIf(M.upc, ''), nullIf(C.upc, ''), ''),
                coalesce(C.grid, ''),
                coalesce(nullIf(M.custom_id, ''), nullIf(C.custom_id, ''), ''),
                coalesce(nullIf(C.video_title, ''), E.video_title), coalesce(C.username, ''), coalesce(nullIf(C.uploader, ''), E.uploader),
                E.uploader_type,
                E.creator_content_type,
                coalesce(C.video_duration_sec, 0), coalesce(C.video_upload_date, ''), coalesce(C.channel_display_name, ''),
                if(coalesce(count_claims.cnt, 0) > 1, 'Yes', 'No'),
                coalesce(C.category, ''),
                coalesce(nullIf(M.asset_labels, ''), nullIf(C.asset_labels, ''), nullIf(L.asset_label, ''), ''),
                coalesce(nullIf(M.artist, ''), nullIf(C.artist, ''), ''),
                coalesce(nullIf(M.asset_title, ''), nullIf(C.asset_title, ''), nullIf(C.video_title, ''), E.video_title, ''),
                coalesce(nullIf(M.album, ''), nullIf(C.album, ''), ''),
                coalesce(nullIf(M.label, ''), nullIf(C.label, ''), ''),
                coalesce(M.genre, '') AS genre,
                if(coalesce(AV.total_actual_views, 0) > 0,
                    if(coalesce(RS.total_mp, 0) > 0,
                        toInt64(round(toInt64(E.monetized_playbacks) * (toInt64(AV.total_actual_views) / toInt64(RS.total_mp)))),
                        toInt64(round(toInt64(AV.total_actual_views) / toInt64(RS.row_cnt)))
                    ),
                    toInt64(E.views)
                ) AS views,
                E.estimated_cpm, E.estimated_playback_based_cpm,
                E.yt_rev_auction, E.yt_rev_reserved, 0, 0, E.yt_rev_red, E.yt_rev_total,
                E.partner_rev_auction,
                E.partner_rev_reserved, 0, 0, E.partner_rev_red, E.partner_rev_total,
                E.monetized_playbacks, E.ad_impressions,
                E.partner_rev_transaction,
                toInt64(round(coalesce(nullIf(E.likes, 0), toInt64(round(INT.likes * (toInt64(E.views) / coalesce(nullIf(RS.total_views, 0), 1)))), toInt64(0)) / coalesce(count_claims.cnt, 1))) AS likes,
                toInt64(round(coalesce(nullIf(E.comments, 0), toInt64(round(INT.comments * (toInt64(E.views) / coalesce(nullIf(RS.total_views, 0), 1)))), toInt64(0)) / coalesce(count_claims.cnt, 1))) AS comments,
                toInt64(round(coalesce(nullIf(E.shares, 0), toInt64(round(INT.shares * (toInt64(E.views) / coalesce(nullIf(RS.total_views, 0), 1)))), toInt64(0)) / coalesce(count_claims.cnt, 1))) AS shares,
                toInt64(round(coalesce(nullIf(E.dislikes, 0), toInt64(round(INT.dislikes * (toInt64(E.views) / coalesce(nullIf(RS.total_views, 0), 1)))), toInt64(0)) / coalesce(count_claims.cnt, 1))) AS dislikes,
                (coalesce(nullIf(E.watch_time_minutes, 0.0), (DEV.watch_time_sec / 60.0) * (toInt64(E.views) / coalesce(nullIf(RS.total_views, 0), 1)), 0.0) / coalesce(count_claims.cnt, 1)) AS watch_time_minutes,
                toInt32(round(coalesce(nullIf(E.average_view_duration_seconds, 0), DEV.watch_time_sec / coalesce(nullIf(DEV.device_views, 0), 1), 0) / coalesce(count_claims.cnt, 1))) AS average_view_duration_seconds,
                (if(coalesce(nullIf(E.average_view_duration_percentage, 0.0), 0.0) > 0.0,
                    E.average_view_duration_percentage,
                    if(coalesce(C.video_duration_sec, 0) > 0,
                       (DEV.watch_time_sec / coalesce(nullIf(DEV.device_views, 0), 1) / C.video_duration_sec) * 100.0,
                       0.0
                    )
                 ) / coalesce(count_claims.cnt, 1)) AS average_view_duration_percentage,
                toInt64(round(E.subscribers_gained / coalesce(count_claims.cnt, 1))) AS subscribers_gained,
                toInt64(round(E.subscribers_lost / coalesce(count_claims.cnt, 1))) AS subscribers_lost
            FROM clean_estimated E
            LEFT JOIN deduplicated_claims C ON E.video_id = C.video_id AND E.asset_id = C.asset_id
            LEFT JOIN count_claims ON E.video_id = count_claims.video_id
            LEFT JOIN video_interactions INT ON E.video_id = INT.video_id
            LEFT JOIN video_devices DEV ON E.video_id = DEV.video_id
            LEFT JOIN latest_metadata M ON E.asset_id = M.asset_id
            LEFT JOIN channel_label_map L ON coalesce(nullIf(C.channel_id, ''), E.channel_id) = L.channel_id
            LEFT JOIN actual_views AV ON E.video_id = AV.video_id
            LEFT JOIN raw_stats RS ON E.video_id = RS.video_id
            LEFT JOIN asset_owners AO ON E.asset_id = AO.asset_id
            WHERE E.asset_id != ''
            SETTINGS 
              join_use_nulls = 1,
              join_algorithm = 'auto', 
              max_memory_usage = 10000000000, 
              max_bytes_before_external_group_by = 4000000000
        `;

        const joinQueryB = `
            INSERT INTO estimated_revenue_daily (
                cms_id, day, country, video_id, channel_id, owner_channel_id, asset_id, asset_type, content_type, claimed_status, claim_type, claim_status,
                policy, claim_origin, isrc, upc, grid, custom_id, video_title, username, uploader, uploader_type, creator_content_type, video_duration_sec, video_upload_date,
                channel_display_name, multiple_claims, category, asset_labels, artist, asset_title, album, label, genre,
                owned_views, estimated_cpm, estimated_playback_based_cpm, yt_rev_auction, yt_rev_reserved, yt_rev_partner_sold_yt_served, yt_rev_partner_sold_p_served,
                yt_rev_red, yt_rev_total, partner_rev_auction, partner_rev_reserved, partner_rev_partner_sold_yt_served,
                partner_rev_partner_sold_p_served, partner_rev_red, partner_rev_total,
                monetized_playbacks, ad_impressions, partner_rev_transaction,
                likes, comments, shares, dislikes, watch_time_minutes, average_view_duration_seconds, average_view_duration_percentage, subscribers_gained, subscribers_lost
            )
            WITH
            asset_owners AS (
                SELECT 
                    asset_id, 
                    argMax(channel_id, ingested_at) as owner_channel_id
                FROM youtube_raw_claims
                WHERE cms_id = {cmsId: String}
                  AND content_type = 'PARTNER_UPLOADED'
                  AND asset_id != ''
                  AND channel_id != ''
                GROUP BY asset_id
            ),
            actual_views AS (
                SELECT 
                    video_id, 
                    sum(views) as total_actual_views 
                FROM video_devices_daily 
                WHERE cms_id = {cmsId: String} AND day = {targetDay: Date}
                GROUP BY video_id
            ),
            video_interactions AS (
                SELECT 
                    video_id,
                    sum(likes) as likes,
                    sum(dislikes) as dislikes,
                    sum(comments) as comments,
                    sum(shares) as shares
                FROM video_interactions_daily
                WHERE cms_id = {cmsId: String} AND day = {targetDay: Date}
                GROUP BY video_id
            ),
            video_devices AS (
                SELECT 
                    video_id,
                    sum(views) as device_views,
                    sum(watch_time_sec) as watch_time_sec
                FROM video_devices_daily
                WHERE cms_id = {cmsId: String} AND day = {targetDay: Date}
                GROUP BY video_id
            ),
            raw_stats AS (
                SELECT 
                    coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf(Video, '')) AS video_id, 
                    sum(toInt64OrZero(coalesce(nullIf(estimated_monetized_playbacks, ''), nullIf("Estimated Monetized Playbacks", ''), '0'))) as total_mp,
                    sum(toInt64OrZero(coalesce(nullIf(views, ''), nullIf("Views", ''), nullIf("Owned Views", ''), '0'))) as total_views,
                    count() as row_cnt
                FROM ${tempEstimatedTable}
                GROUP BY video_id
            ),
            metadata_claims AS (
                SELECT
                    custom_id AS video_id,
                    asset_id,
                    '' AS channel_id,
                    '' AS asset_type,
                    'PARTNER_UPLOADED' AS content_type,
                    'AUDIOVISUAL' AS claim_type,
                    '' AS policy,
                    'METADATA_MATCH' AS claim_origin,
                    custom_id,
                    '' AS isrc,
                    '' AS grid,
                    '' AS upc,
                    asset_title AS video_title,
                    '' AS username,
                    '' AS uploader,
                    0 AS video_duration_sec,
                    '' AS channel_display_name,
                    '' AS multiple_claims,
                    '' AS category,
                    '' AS asset_labels,
                    artist,
                    asset_title,
                    album,
                    label,
                    toInt64(0) AS views,
                    '' AS claim_status,
                    '' AS video_upload_date
                FROM youtube_asset_metadata
                WHERE cms_id = {cmsId: String}
                  AND length(custom_id) = 11
                  AND (custom_id, asset_id, day) IN (
                      SELECT custom_id, asset_id, max(day)
                      FROM youtube_asset_metadata
                      WHERE cms_id = {cmsId: String}
                        AND length(custom_id) = 11
                        AND custom_id IN (
                            SELECT DISTINCT coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf(Video, ''))
                            FROM ${tempEstimatedTable}
                            WHERE coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), nullIf(Asset, ''), '') = ''
                        )
                      GROUP BY custom_id, asset_id
                  )
            ),
            deduplicated_claims AS (
                SELECT
                    video_id, asset_id, channel_id, asset_type, content_type, claim_type,
                    policy, claim_origin, custom_id, ISRC as isrc, Grid as grid, UPC as upc,
                    video_title, username, uploader, video_duration_sec, channel_display_name,
                    multiple_claims, category, asset_labels, artist, asset_title, album, label, views,
                    claim_status, video_upload_date
                FROM (
                    SELECT
                        coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf(Video, '')) AS video_id,
                        coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), nullIf(Asset, '')) AS asset_id,
                        coalesce(nullIf("Channel ID", ''), nullIf(channel_id, ''), nullIf(Channel, '')) AS channel_id,
                        coalesce(nullIf("Asset Type", ''), nullIf(asset_type, '')) AS asset_type,
                        coalesce(nullIf("Content Type", ''), nullIf(content_type, '')) AS content_type,
                        coalesce(nullIf("Claim Type", ''), nullIf(claim_type, '')) AS claim_type,
                        coalesce(nullIf("Policy", ''), nullIf(policy, '')) AS policy,
                        coalesce(nullIf("Claim Origin", ''), nullIf(claim_origin, '')) AS claim_origin,
                        coalesce(nullIf("Custom ID", ''), nullIf(custom_id, '')) AS custom_id,
                        coalesce(nullIf("ISRC", ''), nullIf(isrc, '')) AS ISRC,
                        coalesce(nullIf("GRid", ''), nullIf(grid, ''), nullIf(Grid, '')) AS Grid,
                        coalesce(nullIf("UPC", ''), nullIf(upc, '')) AS UPC,
                        coalesce(nullIf("Video Title", ''), nullIf(video_title, '')) AS video_title,
                        coalesce(nullIf("Username", ''), nullIf(username, '')) AS username,
                        coalesce(nullIf("Uploader", ''), nullIf(uploader, '')) AS uploader,
                        toInt32OrZero(coalesce(nullIf("Video Duration", ''), nullIf(video_duration_sec, ''), nullIf("Video Duration (sec)", ''))) AS video_duration_sec,
                        coalesce(nullIf("Channel Display Name", ''), nullIf(channel_display_name, '')) AS channel_display_name,
                        coalesce(nullIf("Multiple Claims", ''), nullIf(multiple_claims, '')) AS multiple_claims,
                        coalesce(nullIf("Category", ''), nullIf(category, '')) AS category,
                        coalesce(nullIf("Asset Labels", ''), nullIf(asset_labels, '')) AS asset_labels,
                        coalesce(nullIf("Artist", ''), nullIf(artist, '')) AS artist,
                        coalesce(nullIf("Asset Title", ''), nullIf(asset_title, ''), nullIf(Title, '')) AS asset_title,
                        coalesce(nullIf("Album", ''), nullIf(album, '')) AS album,
                        coalesce(nullIf("Label", ''), nullIf(label, ''), nullIf(record_label, '')) AS label,
                        toInt64OrZero(coalesce(nullIf("Views", ''), nullIf(views, ''), nullIf("Owned Views", ''))) AS views,
                        coalesce(nullIf("Claim Status", ''), nullIf(claim_status, ''), '') AS claim_status,
                        coalesce(nullIf("Video Upload Date", ''), nullIf(video_upload_date, ''), '') AS video_upload_date
                    FROM ${tempClaimsTable}
                    WHERE coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), nullIf(Asset, '')) != ''
                      AND coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf(Video, '')) IN (
                          SELECT DISTINCT coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf(Video, ''))
                          FROM ${tempEstimatedTable}
                          WHERE coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), nullIf(Asset, ''), '') = ''
                      )

                    UNION ALL

                    SELECT
                        video_id, asset_id, channel_id, asset_type, content_type, claim_type,
                        policy, claim_origin, custom_id, isrc AS ISRC, grid AS Grid, upc AS UPC,
                        video_title, username, uploader, video_duration_sec, channel_display_name,
                        multiple_claims, category, asset_labels, artist, asset_title, album, label, views,
                        claim_status, video_upload_date
                    FROM metadata_claims
                )
                LIMIT 1 BY (video_id, asset_id)
            ),
            asset_daily_revenue AS (
                SELECT 
                    asset_id,
                    CAST(sum(estimated_partner_revenue) AS Float64) AS asset_revenue
                FROM youtube_raw_asset_estimated_revenue
                WHERE cms_id = {cmsId: String}
                  AND day = {targetDay: Date}
                GROUP BY asset_id
            ),
            claims_with_revenue AS (
                SELECT 
                    C.*,
                    coalesce(A.asset_revenue, 0.0) AS asset_revenue
                FROM deduplicated_claims C
                LEFT JOIN asset_daily_revenue A ON C.asset_id = A.asset_id
            ),
            claims_with_weights AS (
                SELECT 
                    *,
                    count() OVER (PARTITION BY video_id) AS claim_count,
                    sum(asset_revenue) OVER (PARTITION BY video_id) AS total_video_asset_revenue,
                    if(total_video_asset_revenue > 0.0,
                       asset_revenue / total_video_asset_revenue,
                       1.0 / claim_count
                    ) AS asset_weight
                FROM claims_with_revenue
            ),
            count_claims AS (
                SELECT video_id, count() as cnt FROM deduplicated_claims GROUP BY video_id
            ),
            clean_estimated AS (
                SELECT
                    coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf(Video, '')) AS video_id,
                    toDateOrZero(if(length(coalesce(nullIf("Date", ''), nullIf("Day", ''), nullIf("day", ''), nullIf("date", ''))) = 8,
                        concat(substring(coalesce(nullIf("Date", ''), nullIf("Day", ''), nullIf("day", ''), nullIf("date", '')), 1, 4), '-',
                               substring(coalesce(nullIf("Date", ''), nullIf("Day", ''), nullIf("day", ''), nullIf("date", '')), 5, 2), '-',
                               substring(coalesce(nullIf("Date", ''), nullIf(day, ''), nullIf(date, '')), 7, 2)),
                        coalesce(nullIf("Date", ''), nullIf(day, ''), nullIf(date, ''))
                    )) AS day,
                    coalesce(nullIf("Country", ''), nullIf(country_code, ''), nullIf(country, '')) AS country,
                    coalesce(nullIf("Channel ID", ''), nullIf(channel_id, ''), nullIf(Channel, '')) AS channel_id,
                    CAST(coalesce(nullIf(estimated_partner_revenue, ''), nullIf("Partner Revenue", ''), nullIf(partner_rev_total, ''), '0') AS Decimal64(10)) as partner_rev_total,
                    CAST(coalesce(nullIf(estimated_partner_ad_auction_revenue, ''), nullIf("Partner Revenue : Auction", ''), nullIf(partner_rev_auction, ''), '0') AS Decimal64(10)) as partner_rev_auction,
                    CAST(coalesce(nullIf(estimated_partner_ad_reserved_revenue, ''), nullIf("Partner Revenue : Reserved", ''), nullIf(partner_rev_reserved, ''), '0') AS Decimal64(10)) as partner_rev_reserved,
                    CAST(coalesce(nullIf(estimated_partner_red_revenue, ''), nullIf("Partner Revenue : Red", ''), nullIf(partner_rev_red, ''), '0') AS Decimal64(10)) as partner_rev_red,
                    CAST(coalesce(nullIf(estimated_youtube_ad_revenue, ''), nullIf("YouTube Revenue Split", ''), nullIf(yt_rev_total, ''), '0') AS Decimal64(10)) as yt_rev_total,
                    CAST(0 AS Decimal64(10)) as yt_rev_auction,
                    CAST(0 AS Decimal64(10)) as yt_rev_reserved,
                    CAST(0 AS Decimal64(10)) as yt_rev_red,
                    coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), nullIf(Asset, ''), '') AS asset_id,
                    toInt64OrZero(coalesce(nullIf(replaceAll(coalesce(nullIf(views, ''), nullIf("Views", ''), '0'), ',', ''), '0'), nullIf(estimated_monetized_playbacks, ''), '0')) AS views,
                    toInt64OrZero(coalesce(nullIf(estimated_monetized_playbacks, ''), nullIf("Estimated Monetized Playbacks", ''), '0')) AS monetized_playbacks,
                    toInt64OrZero(coalesce(nullIf(ad_impressions, ''), nullIf("Ad Impressions", ''), nullIf(adImpressions, ''), '0')) AS ad_impressions,
                    CAST(coalesce(nullIf("Estimated partner transaction revenue (USD)", ''), nullIf("Estimated partner transaction revenue", ''), nullIf(estimated_partner_transaction_revenue, ''), nullIf("Partner Revenue : Transaction", ''), nullIf(estimatedPartnerTransactionRevenue, ''), nullIf(partner_rev_transaction, ''), '0') AS Decimal64(10)) AS partner_rev_transaction,
                    coalesce(nullIf(video_title, ''), nullIf("Video Title", '')) AS video_title,
                    coalesce(nullIf(uploader_type, ''), nullIf(uploader, '')) AS uploader,
                    coalesce(nullIf(claimed_status, ''), nullIf("Claimed Status", ''), '') AS claimed_status,
                    coalesce(nullIf(uploader_type, ''), nullIf("Uploader Type", ''), '') AS uploader_type,
                    CAST(coalesce(nullIf(estimated_cpm, ''), nullIf("Average CPM", ''), '0') AS Decimal64(10)) AS estimated_cpm,
                    CAST(coalesce(nullIf(estimated_playback_based_cpm, ''), nullIf("Playback-based CPM", ''), '0') AS Decimal64(10)) AS estimated_playback_based_cpm,
                    toInt64OrZero(coalesce(nullIf(likes, ''), '0')) AS likes,
                    toInt64OrZero(coalesce(nullIf(comments, ''), '0')) AS comments,
                    toInt64OrZero(coalesce(nullIf(shares, ''), '0')) AS shares,
                    toInt64OrZero(coalesce(nullIf(dislikes, ''), '0')) AS dislikes,
                    toFloat64OrZero(coalesce(nullIf(estimatedMinutesWatched, ''), nullIf(estimated_minutes_watched, ''), '0.0')) AS watch_time_minutes,
                    toInt32OrZero(coalesce(nullIf(averageViewDuration, ''), nullIf(average_view_duration, ''), '0')) AS average_view_duration_seconds,
                    toFloat64OrZero(coalesce(nullIf(averageViewPercentage, ''), nullIf(average_view_percentage, ''), '0.0')) AS average_view_duration_percentage,
                    toInt64OrZero(coalesce(nullIf(subscribersGained, ''), nullIf(subscribers_gained, ''), '0')) AS subscribers_gained,
                    toInt64OrZero(coalesce(nullIf(subscribersLost, ''), nullIf(subscribers_lost, ''), '0')) AS subscribers_lost,
                    coalesce(nullIf(creator_content_type, ''), nullIf(creatorContentType, ''), '') AS creator_content_type
                FROM ${tempEstimatedTable}
            ),
            latest_metadata AS (
                SELECT 
                    asset_id,
                    argMax(asset_title, day) AS asset_title,
                    argMax(artist, day) AS artist,
                    argMax(album, day) AS album,
                    argMax(label, day) AS label,
                    argMax(isrc, day) AS isrc,
                    argMax(upc, day) AS upc,
                    argMax(custom_id, day) AS custom_id,
                    argMax(genre, day) AS genre,
                    argMax(asset_labels, day) AS asset_labels
                FROM youtube_asset_metadata
                WHERE cms_id = {cmsId: String}
                  AND asset_id IN (
                      SELECT DISTINCT coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), nullIf(Asset, ''), '')
                      FROM ${tempClaimsTable}
                      WHERE coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), nullIf(Asset, ''), '') != ''
                  )
                GROUP BY asset_id
            ),
            latest_video_metadata AS (
                SELECT
                    video_id,
                    argMax(video_title, day) AS video_title,
                    argMax(channel_id, day) AS channel_id,
                    argMax(channel_display_name, day) AS channel_display_name,
                    argMax(video_length_sec, day) AS video_length_sec,
                    argMax(category, day) AS category,
                    argMax(asset_id, day) AS asset_id,
                    argMax(custom_id, day) AS custom_id,
                    argMax(isrc, day) AS isrc,
                    argMax(content_type, day) AS content_type
                FROM youtube_video_metadata
                WHERE cms_id = {cmsId: String}
                  AND video_id IN (
                      SELECT DISTINCT coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf(Video, ''))
                      FROM ${tempEstimatedTable}
                      WHERE coalesce(nullIf("Asset ID", ''), nullIf(asset_id, ''), nullIf(Asset, ''), '') = ''
                  )
                GROUP BY video_id
            )
            SELECT
                {cmsId: String}, E.day, E.country, E.video_id, coalesce(nullIf(C.channel_id, ''), nullIf(V.channel_id, ''), E.channel_id) AS channel_id, 
                if(coalesce(nullIf(C.content_type, ''), nullIf(V.content_type, ''), 'UGC') LIKE '%UGC%',
                    coalesce(nullIf(AO.owner_channel_id, ''), coalesce(nullIf(C.channel_id, ''), nullIf(V.channel_id, ''), E.channel_id)),
                    coalesce(nullIf(C.channel_id, ''), nullIf(V.channel_id, ''), E.channel_id)
                ) AS owner_channel_id,
                coalesce(nullIf(C.asset_id, ''), nullIf(V.asset_id, ''), 'UNCLAIMED_VIDEO'),
                coalesce(nullIf(C.asset_type, ''), if(coalesce(nullIf(C.asset_id, ''), nullIf(V.asset_id, ''), '') != '', 'Sound Recording', 'Unclaimed')), 
                coalesce(nullIf(C.content_type, ''), nullIf(V.content_type, ''), 'Unclaimed'), E.claimed_status,
                coalesce(nullIf(C.claim_type, ''), if(coalesce(nullIf(C.asset_id, ''), nullIf(V.asset_id, ''), '') != '', 'Audio', 'Unclaimed')),
                coalesce(nullIf(C.claim_status, ''), ''),
                coalesce(nullIf(C.policy, ''), if(coalesce(nullIf(C.asset_id, ''), nullIf(V.asset_id, ''), '') != '', 'Monetize', 'Unclaimed')), 
                coalesce(nullIf(C.claim_origin, ''), if(coalesce(nullIf(C.asset_id, ''), nullIf(V.asset_id, ''), '') != '', 'Video Match', 'Unclaimed')), 
                coalesce(nullIf(M.isrc, ''), nullIf(C.isrc, ''), nullIf(V.isrc, ''), ''), 
                coalesce(nullIf(M.upc, ''), nullIf(C.upc, ''), ''), 
                coalesce(nullIf(C.grid, ''), ''),
                coalesce(nullIf(M.custom_id, ''), nullIf(C.custom_id, ''), nullIf(V.custom_id, ''), ''), 
                coalesce(nullIf(C.video_title, ''), V.video_title, E.video_title), 
                coalesce(nullIf(C.username, ''), V.channel_display_name, ''), 
                coalesce(nullIf(C.uploader, ''), V.channel_id, E.uploader),
                E.uploader_type,
                E.creator_content_type,
                coalesce(C.video_duration_sec, V.video_length_sec, 0), 
                coalesce(C.video_upload_date, ''),
                coalesce(nullIf(C.channel_display_name, ''), V.channel_display_name, ''),
                if(coalesce(count_claims.cnt, 0) > 1, 'Yes', 'No'),
                coalesce(nullIf(C.category, ''), V.category, 'Unclaimed'), 
                coalesce(nullIf(M.asset_labels, ''), nullIf(C.asset_labels, ''), nullIf(L.asset_label, ''), ''), 
                coalesce(nullIf(M.artist, ''), nullIf(C.artist, ''), ''), 
                coalesce(nullIf(M.asset_title, ''), nullIf(C.asset_title, ''), nullIf(V.video_title, ''), E.video_title, ''),
                coalesce(nullIf(M.album, ''), nullIf(C.album, ''), ''), 
                coalesce(nullIf(M.label, ''), nullIf(C.label, ''), ''),
                coalesce(M.genre, '') AS genre,
                if(coalesce(AV.total_actual_views, 0) > 0,
                    if(coalesce(RS.total_mp, 0) > 0,
                        toInt64(round((toInt64(round(toInt64(E.monetized_playbacks) * (toInt64(AV.total_actual_views) / toInt64(RS.total_mp))))) * coalesce(C.asset_weight, 1.0))),
                        toInt64(round((toInt64(round(toInt64(AV.total_actual_views) / toInt64(RS.row_cnt)))) * coalesce(C.asset_weight, 1.0)))
                    ),
                    toInt64(round(toInt64(E.views) * coalesce(C.asset_weight, 1.0)))
                ) AS views,
                E.estimated_cpm, E.estimated_playback_based_cpm,
                CAST(E.yt_rev_auction * coalesce(C.asset_weight, 1.0) AS Decimal64(10)),
                CAST(E.yt_rev_reserved * coalesce(C.asset_weight, 1.0) AS Decimal64(10)),
                0, 0,
                CAST(E.yt_rev_red * coalesce(C.asset_weight, 1.0) AS Decimal64(10)),
                CAST(E.yt_rev_total * coalesce(C.asset_weight, 1.0) AS Decimal64(10)),
                CAST(E.partner_rev_auction * coalesce(C.asset_weight, 1.0) AS Decimal64(10)) AS partner_rev_auction,
                CAST(E.partner_rev_reserved * coalesce(C.asset_weight, 1.0) AS Decimal64(10)),
                0, 0,
                CAST(E.partner_rev_red * coalesce(C.asset_weight, 1.0) AS Decimal64(10)),
                CAST(E.partner_rev_total * coalesce(C.asset_weight, 1.0) AS Decimal64(10)),
                toInt64(round(E.monetized_playbacks * coalesce(C.asset_weight, 1.0))),
                toInt64(round(E.ad_impressions * coalesce(C.asset_weight, 1.0))),
                CAST(E.partner_rev_transaction * coalesce(C.asset_weight, 1.0) AS Decimal64(10)) AS partner_rev_transaction,
                toInt64(round(coalesce(nullIf(E.likes, 0), toInt64(round(INT.likes * (toInt64(E.views) / coalesce(nullIf(RS.total_views, 0), 1)))), toInt64(0)) * coalesce(C.asset_weight, 1.0))) AS likes,
                toInt64(round(coalesce(nullIf(E.comments, 0), toInt64(round(INT.comments * (toInt64(E.views) / coalesce(nullIf(RS.total_views, 0), 1)))), toInt64(0)) * coalesce(C.asset_weight, 1.0))) AS comments,
                toInt64(round(coalesce(nullIf(E.shares, 0), toInt64(round(INT.shares * (toInt64(E.views) / coalesce(nullIf(RS.total_views, 0), 1)))), toInt64(0)) * coalesce(C.asset_weight, 1.0))) AS shares,
                toInt64(round(coalesce(nullIf(E.dislikes, 0), toInt64(round(INT.dislikes * (toInt64(E.views) / coalesce(nullIf(RS.total_views, 0), 1)))), toInt64(0)) * coalesce(C.asset_weight, 1.0))) AS dislikes,
                (coalesce(nullIf(E.watch_time_minutes, 0.0), (DEV.watch_time_sec / 60.0) * (toInt64(E.views) / coalesce(nullIf(RS.total_views, 0), 1)), 0.0) * coalesce(C.asset_weight, 1.0)) AS watch_time_minutes,
                toInt32(round(coalesce(nullIf(E.average_view_duration_seconds, 0), DEV.watch_time_sec / coalesce(nullIf(DEV.device_views, 0), 1), 0) * coalesce(C.asset_weight, 1.0))) AS average_view_duration_seconds,
                (if(coalesce(nullIf(E.average_view_duration_percentage, 0.0), 0.0) > 0.0,
                    E.average_view_duration_percentage,
                    if(coalesce(C.video_duration_sec, V.video_length_sec, 0) > 0,
                       (DEV.watch_time_sec / coalesce(nullIf(DEV.device_views, 0), 1) / coalesce(C.video_duration_sec, V.video_length_sec)) * 100.0,
                       0.0
                    )
                 ) * coalesce(C.asset_weight, 1.0)) AS average_view_duration_percentage,
                toInt64(round(E.subscribers_gained * coalesce(C.asset_weight, 1.0))) AS subscribers_gained,
                toInt64(round(E.subscribers_lost * coalesce(C.asset_weight, 1.0))) AS subscribers_lost
            FROM clean_estimated E
            LEFT JOIN claims_with_weights C ON E.video_id = C.video_id
            LEFT JOIN count_claims ON E.video_id = count_claims.video_id
            LEFT JOIN latest_video_metadata V ON E.video_id = V.video_id
            LEFT JOIN latest_metadata M ON coalesce(nullIf(C.asset_id, ''), nullIf(V.asset_id, '')) = M.asset_id
            LEFT JOIN channel_label_map L ON coalesce(nullIf(C.channel_id, ''), nullIf(V.channel_id, ''), E.channel_id) = L.channel_id
            LEFT JOIN actual_views AV ON E.video_id = AV.video_id
            LEFT JOIN raw_stats RS ON E.video_id = RS.video_id
            LEFT JOIN video_interactions INT ON E.video_id = INT.video_id
            LEFT JOIN video_devices DEV ON E.video_id = DEV.video_id
            LEFT JOIN asset_owners AO ON coalesce(nullIf(C.asset_id, ''), nullIf(V.asset_id, '')) = AO.asset_id
            WHERE E.asset_id = ''
            SETTINGS 
              join_use_nulls = 1,
              join_algorithm = 'auto', 
              max_memory_usage = 10000000000, 
              max_bytes_before_external_group_by = 4000000000
        `;

        if (log) log(`[ETL] Menghapus data lama untuk tanggal ${targetDay} di estimated_revenue_daily...`);
        try {
            await client.command({
                query: `ALTER TABLE estimated_revenue_daily DELETE WHERE cms_id = {cmsId: String} AND day = {targetDay: Date} SETTINGS mutations_sync = 1`,
                query_params: { cmsId, targetDay }
            });
        } catch (e: any) {
            throw new Error(`Gagal menghapus data lama di estimated_revenue_daily: ${e.message}`);
        }

        await client.command({ 
            query: joinQueryA,
            query_params: { cmsId, targetDay }
        });
        if (isAborted?.()) throw new Error("Job aborted by user");

        await client.command({ 
            query: joinQueryB,
            query_params: { cmsId, targetDay }
        });

        // Ingest Channel Rollup revenue mapping using asset metadata custom_id (Video ID)
        if (log) {
            log(`[ETL] Mapping channel rollup revenues to official Video IDs using custom_id...`);
        }
        const insertRollupMappingQuery = `
            INSERT INTO estimated_revenue_daily (
                cms_id, day, country, video_id, channel_id, asset_id, asset_type, content_type, claimed_status, claim_type, claim_status,
                policy, claim_origin, isrc, upc, grid, custom_id, video_title, username, uploader, uploader_type, video_duration_sec, video_upload_date,
                channel_display_name, multiple_claims, category, asset_labels, artist, asset_title, album, label, genre,
                owned_views, estimated_cpm, estimated_playback_based_cpm, yt_rev_auction, yt_rev_reserved, yt_rev_partner_sold_yt_served, yt_rev_partner_sold_p_served,
                yt_rev_red, yt_rev_total, partner_rev_auction, partner_rev_reserved, partner_rev_partner_sold_yt_served,
                partner_rev_partner_sold_p_served, partner_rev_red, partner_rev_total,
                monetized_playbacks, ad_impressions, partner_rev_transaction,
                likes, comments, shares, dislikes, watch_time_minutes, average_view_duration_seconds, average_view_duration_percentage, subscribers_gained, subscribers_lost
            )
            WITH
            asset_totals AS (
                SELECT
                    asset_id,
                    any(channel_id) AS channel_id,
                    sum(estimated_partner_revenue) AS asset_total,
                    sum(estimated_partner_ad_revenue) AS asset_ad,
                    sum(estimated_partner_red_revenue) AS asset_red,
                    sum(estimated_partner_transaction_revenue) AS asset_transaction
                FROM youtube_raw_asset_estimated_revenue
                WHERE cms_id = {cmsId: String} AND day = {targetDay: Date}
                GROUP BY asset_id
            ),
            joined_totals AS (
                SELECT
                    asset_id,
                    sum(partner_rev_total) AS joined_total,
                    sum(partner_rev_auction) AS joined_ad,
                    sum(partner_rev_red) AS joined_red,
                    sum(partner_rev_transaction) AS joined_transaction
                FROM estimated_revenue_daily
                WHERE cms_id = {cmsId: String} AND day = {targetDay: Date} AND asset_id != ''
                GROUP BY asset_id
            ),
            asset_meta AS (
                SELECT
                    asset_id,
                    argMax(custom_id, day) AS custom_id,
                    argMax(asset_title, day) AS asset_title,
                    argMax(artist, day) AS artist,
                    argMax(album, day) AS album,
                    argMax(label, day) AS label,
                    argMax(asset_labels, day) AS asset_labels,
                    argMax(isrc, day) AS isrc,
                    argMax(upc, day) AS upc,
                    argMax(grid, day) AS grid,
                    argMax(genre, day) AS genre
                FROM youtube_asset_metadata
                WHERE cms_id = {cmsId: String}
                  AND asset_id IN (
                      SELECT asset_id 
                      FROM youtube_raw_asset_estimated_revenue 
                      WHERE cms_id = {cmsId: String} AND day = {targetDay: Date}
                  )
                GROUP BY asset_id
            ),
            claims_with_weights AS (
                SELECT
                    cms_id,
                    day,
                    asset_id,
                    video_id,
                    channel_id,
                    channel_display_name,
                    video_title,
                    content_type,
                    claim_type,
                    claim_status,
                    policy,
                    isrc,
                    upc,
                    grid,
                    custom_id,
                    username,
                    uploader,
                    video_duration_sec,
                    video_upload_date,
                    multiple_claims,
                    views,
                    sum(if(content_type = 'UGC', toInt64(views), toInt64(0))) OVER (PARTITION BY asset_id) AS total_ugc_views,
                    sum(if(content_type = 'UGC', 1, 0)) OVER (PARTITION BY asset_id) AS ugc_count,
                    sum(toInt64(views)) OVER (PARTITION BY asset_id) AS total_all_views,
                    count() OVER (PARTITION BY asset_id) AS all_count
                FROM youtube_raw_claims
                WHERE cms_id = {cmsId: String} AND day = {targetDay: Date} AND video_id != ''
            ),
            claim_weights AS (
                SELECT
                    *,
                    if(ugc_count > 0,
                       if(content_type = 'UGC',
                          if(total_ugc_views > 0, toFloat64(views) / total_ugc_views, 1.0 / ugc_count),
                          0.0
                       ),
                       if(total_all_views > 0, toFloat64(views) / total_all_views, 1.0 / all_count)
                    ) AS claim_weight
                FROM claims_with_weights
            ),
            assets_with_claims AS (
                SELECT DISTINCT asset_id FROM claim_weights
            ),
            channel_names AS (
                SELECT
                    channel_id,
                    any(channel_display_name) AS channel_display_name
                FROM (
                    SELECT
                        channel_id,
                        channel_display_name
                    FROM youtube_video_metadata
                    WHERE cms_id = {cmsId: String} 
                      AND channel_display_name != ''
                      AND channel_id IN (
                          SELECT channel_id 
                          FROM youtube_raw_asset_estimated_revenue 
                          WHERE cms_id = {cmsId: String} AND day = {targetDay: Date}
                      )
                    
                    UNION ALL
                    
                    SELECT
                        channel_id,
                        channel_display_name
                    FROM youtube_raw_claims
                    WHERE cms_id = {cmsId: String} AND day = {targetDay: Date} AND channel_display_name != ''
                )
                GROUP BY channel_id
            ),
            claims_asset_meta AS (
                SELECT
                    asset_id,
                    any(artist) AS claim_artist,
                    any(album) AS claim_album,
                    any(coalesce(nullIf(label, ''), nullIf(record_label, ''), '')) AS claim_label,
                    any(asset_labels) AS claim_asset_labels
                FROM youtube_raw_claims
                WHERE cms_id = {cmsId: String} AND day = {targetDay: Date} AND asset_id != '' AND (artist != '' OR album != '' OR label != '' OR record_label != '' OR asset_labels != '')
                GROUP BY asset_id
            )
            -- 1. Claims-based Rollup Mapping (for assets with claims)
            SELECT
                {cmsId: String} AS cms_id,
                {targetDay: Date} AS day,
                '' AS country,
                CW.video_id AS video_id,
                coalesce(nullIf(CW.channel_id, ''), nullIf(A.channel_id, ''), '') AS channel_id,
                A.asset_id AS asset_id,
                if(CW.content_type = 'UGC', 'UGC Asset Fallback', 'Video Rollup') AS asset_type,
                CW.content_type AS content_type,
                'claimed' AS claimed_status,
                CW.claim_type AS claim_type,
                CW.claim_status AS claim_status,
                CW.policy AS policy,
                'Rollup Mapping' AS claim_origin,
                coalesce(nullIf(CW.isrc, ''), nullIf(M.isrc, ''), '') AS isrc,
                coalesce(nullIf(CW.upc, ''), nullIf(M.upc, ''), '') AS upc,
                coalesce(nullIf(CW.grid, ''), nullIf(M.grid, ''), '') AS grid,
                M.custom_id AS custom_id,
                coalesce(nullIf(CW.video_title, ''), nullIf(M.asset_title, ''), '') AS video_title,
                coalesce(CW.username, '') AS username,
                coalesce(CW.uploader, '') AS uploader,
                '' AS uploader_type,
                coalesce(CW.video_duration_sec, 0) AS video_duration_sec,
                coalesce(CW.video_upload_date, '') AS video_upload_date,
                coalesce(nullIf(CW.channel_display_name, ''), nullIf(CN.channel_display_name, ''), '') AS channel_display_name,
                coalesce(nullIf(CW.multiple_claims, ''), 'No') AS multiple_claims,
                'Rollup' AS category,
                coalesce(nullIf(M.asset_labels, ''), nullIf(CAM.claim_asset_labels, ''), '') AS asset_labels,
                coalesce(nullIf(M.artist, ''), nullIf(CAM.claim_artist, ''), '') AS artist,
                coalesce(nullIf(M.asset_title, ''), nullIf(CW.video_title, ''), '') AS asset_title,
                coalesce(nullIf(M.album, ''), nullIf(CAM.claim_album, ''), '') AS album,
                coalesce(nullIf(M.label, ''), nullIf(CAM.claim_label, ''), nullIf(L.asset_label, ''), '') AS label,
                coalesce(M.genre, '') AS genre,
                0 AS owned_views,
                CAST(0 AS Decimal64(10)) AS estimated_cpm,
                CAST(0 AS Decimal64(10)) AS estimated_playback_based_cpm,
                0 AS yt_rev_auction,
                0 AS yt_rev_reserved,
                0 AS yt_rev_partner_sold_yt_served,
                0 AS yt_rev_partner_sold_p_served,
                0 AS yt_rev_red,
                0 AS yt_rev_total,
                CAST(if(A.asset_ad > coalesce(J.joined_ad, 0.0), toFloat64(A.asset_ad - coalesce(J.joined_ad, 0.0)) * CW.claim_weight, toFloat64(0)) AS Decimal64(10)) AS partner_rev_auction,
                0 AS partner_rev_reserved,
                0 AS partner_rev_partner_sold_yt_served,
                0 AS partner_rev_partner_sold_p_served,
                CAST(if(A.asset_red > coalesce(J.joined_red, 0.0), toFloat64(A.asset_red - coalesce(J.joined_red, 0.0)) * CW.claim_weight, toFloat64(0)) AS Decimal64(10)) AS partner_rev_red,
                CAST(if(A.asset_total > coalesce(J.joined_total, 0.0), toFloat64(A.asset_total - coalesce(J.joined_total, 0.0)) * CW.claim_weight, toFloat64(0)) AS Decimal64(10)) AS partner_rev_total,
                0 AS monetized_playbacks,
                0 AS ad_impressions,
                CAST(if(A.asset_transaction > coalesce(J.joined_transaction, 0.0), toFloat64(A.asset_transaction - coalesce(J.joined_transaction, 0.0)) * CW.claim_weight, toFloat64(0)) AS Decimal64(10)) AS partner_rev_transaction,
                0, 0, 0, 0, 0.0, 0, 0.0, 0, 0
            FROM asset_totals A
            INNER JOIN claim_weights CW ON A.asset_id = CW.asset_id
            LEFT JOIN joined_totals J ON A.asset_id = J.asset_id
            LEFT JOIN asset_meta M ON A.asset_id = M.asset_id
            LEFT JOIN channel_names CN ON coalesce(nullIf(CW.channel_id, ''), nullIf(A.channel_id, ''), '') = CN.channel_id
            LEFT JOIN channel_label_map L ON coalesce(nullIf(CW.channel_id, ''), nullIf(A.channel_id, ''), '') = L.channel_id
            LEFT JOIN claims_asset_meta CAM ON A.asset_id = CAM.asset_id
            WHERE (A.asset_total - coalesce(J.joined_total, 0.0)) > 0.01 AND CW.claim_weight > 0.0001

            UNION ALL

            -- 2. Fallback Reference Mapping (for assets with NO claims)
            SELECT
                {cmsId: String} AS cms_id,
                {targetDay: Date} AS day,
                '' AS country,
                coalesce(if(length(M.custom_id) = 11, M.custom_id, ''), '') AS video_id,
                A.channel_id AS channel_id,
                A.asset_id AS asset_id,
                if(length(if(length(M.custom_id) = 11, M.custom_id, '')) = 11, 'Video Rollup', 'UGC Asset Fallback') AS asset_type,
                if(length(if(length(M.custom_id) = 11, M.custom_id, '')) = 11, 'PARTNER_UPLOADED', 'UGC') AS content_type,
                'claimed' AS claimed_status,
                'AUDIOVISUAL' AS claim_type,
                '' AS claim_status,
                'Monetize' AS policy,
                'Rollup Mapping' AS claim_origin,
                M.isrc AS isrc,
                M.upc AS upc,
                M.grid AS grid,
                M.custom_id AS custom_id,
                coalesce(nullIf(M.asset_title, ''), if(A.asset_transaction > 0, 'Channel Transaction Earnings', '')) AS video_title,
                '' AS username,
                '' AS uploader,
                '' AS uploader_type,
                0 AS video_duration_sec,
                '' AS video_upload_date,
                coalesce(CN.channel_display_name, '') AS channel_display_name,
                'No' AS multiple_claims,
                'Rollup' AS category,
                coalesce(M.asset_labels, '') AS asset_labels,
                coalesce(M.artist, '') AS artist,
                coalesce(nullIf(M.asset_title, ''), if(A.asset_transaction > 0, 'Channel Transaction Earnings', '')) AS asset_title,
                coalesce(M.album, '') AS album,
                coalesce(nullIf(M.label, ''), nullIf(L.asset_label, ''), '') AS label,
                coalesce(M.genre, '') AS genre,
                0 AS owned_views,
                CAST(0 AS Decimal64(10)) AS estimated_cpm,
                CAST(0 AS Decimal64(10)) AS estimated_playback_based_cpm,
                0 AS yt_rev_auction,
                0 AS yt_rev_reserved,
                0 AS yt_rev_partner_sold_yt_served,
                0 AS yt_rev_partner_sold_p_served,
                0 AS yt_rev_red,
                0 AS yt_rev_total,
                CAST(if(A.asset_ad > coalesce(J.joined_ad, 0.0), A.asset_ad - coalesce(J.joined_ad, 0.0), toDecimal64(0, 10)) AS Decimal64(10)) AS partner_rev_auction,
                0 AS partner_rev_reserved,
                0 AS partner_rev_partner_sold_yt_served,
                0 AS partner_rev_partner_sold_p_served,
                CAST(if(A.asset_red > coalesce(J.joined_red, 0.0), A.asset_red - coalesce(J.joined_red, 0.0), toDecimal64(0, 10)) AS Decimal64(10)) AS partner_rev_red,
                CAST(if(A.asset_total > coalesce(J.joined_total, 0.0), A.asset_total - coalesce(J.joined_total, 0.0), toDecimal64(0, 10)) AS Decimal64(10)) AS partner_rev_total,
                0 AS monetized_playbacks,
                0 AS ad_impressions,
                CAST(if(A.asset_transaction > coalesce(J.joined_transaction, 0.0), A.asset_transaction - coalesce(J.joined_transaction, 0.0), toDecimal64(0, 10)) AS Decimal64(10)) AS partner_rev_transaction,
                0, 0, 0, 0, 0.0, 0, 0.0, 0, 0
            FROM asset_totals A
            LEFT JOIN joined_totals J ON A.asset_id = J.asset_id
            LEFT JOIN asset_meta M ON A.asset_id = M.asset_id
            LEFT JOIN channel_names CN ON A.channel_id = CN.channel_id
            LEFT JOIN channel_label_map L ON A.channel_id = L.channel_id
            WHERE (A.asset_total - coalesce(J.joined_total, 0.0)) > 0.01
              AND A.asset_id NOT IN (SELECT asset_id FROM assets_with_claims)
        `;
        // Rollup Mapping disabled as requested to align DB with Analytics API totals.
        /*
        await client.command({
            query: insertRollupMappingQuery,
            query_params: { cmsId, targetDay }
        });
        */
        if (log) {
            log(`[ETL] Rollup Mapping step bypassed (disabled).`);
        }
        if (isAborted?.()) throw new Error("Job aborted by user");

        // Ingest channel-level non-video transaction revenue to estimated_revenue_daily
        if (channelRevenueFilePath) {
            if (log) {
                log(`[ETL] Inserting channel non-video transaction revenue to estimated_revenue_daily...`);
            }
            const insertNonVideoQuery = `
                INSERT INTO estimated_revenue_daily (
                    cms_id, day, country, video_id, channel_id, asset_id, asset_type, content_type, claimed_status, claim_type, claim_status,
                    policy, claim_origin, isrc, upc, grid, custom_id, video_title, username, uploader, uploader_type, video_duration_sec, video_upload_date,
                    channel_display_name, multiple_claims, category, asset_labels, artist, asset_title, album, label, genre,
                    owned_views, estimated_cpm, estimated_playback_based_cpm, yt_rev_auction, yt_rev_reserved, yt_rev_partner_sold_yt_served, yt_rev_partner_sold_p_served,
                    yt_rev_red, yt_rev_total, partner_rev_auction, partner_rev_reserved, partner_rev_partner_sold_yt_served,
                    partner_rev_partner_sold_p_served, partner_rev_red, partner_rev_total,
                    monetized_playbacks, ad_impressions, partner_rev_transaction,
                    likes, comments, shares, dislikes, watch_time_minutes, average_view_duration_seconds, average_view_duration_percentage, subscribers_gained, subscribers_lost
                )
                WITH
                channel_totals AS (
                    SELECT
                        channel_id,
                        sum(estimated_partner_revenue) AS chan_total,
                        sum(estimated_partner_ad_revenue) AS chan_ad,
                        sum(estimated_partner_red_revenue) AS chan_red,
                        sum(estimated_partner_transaction_revenue) AS chan_trans
                    FROM youtube_raw_channel_estimated_revenue
                    WHERE cms_id = {cmsId: String} AND day = {targetDay: Date}
                    GROUP BY channel_id
                ),
                video_totals AS (
                    SELECT
                        coalesce(nullIf(M.mapped_channel_id, ''), R.channel_id) AS channel_id,
                        sum(R.estimated_partner_revenue) AS vid_total,
                        sum(R.estimated_partner_ad_revenue) AS vid_ad,
                        sum(R.estimated_partner_red_revenue) AS vid_red,
                        sum(R.estimated_partner_transaction_revenue) AS vid_trans
                    FROM youtube_raw_estimated_revenue R
                    LEFT JOIN (
                        SELECT video_id, any(channel_id) AS mapped_channel_id
                        FROM (
                            SELECT video_id, channel_id FROM youtube_video_metadata WHERE channel_id != ''
                            UNION ALL
                            SELECT video_id, channel_id FROM youtube_raw_claims WHERE channel_id != ''
                        )
                        GROUP BY video_id
                    ) M ON R.video_id = M.video_id
                    WHERE R.cms_id = {cmsId: String} AND R.day = {targetDay: Date}
                    GROUP BY channel_id
                ),
                channel_display_names AS (
                    SELECT
                        channel_id,
                        any(channel_display_name) AS display_name
                    FROM youtube_raw_claims
                    WHERE cms_id = {cmsId: String} AND day = {targetDay: Date} AND channel_id != ''
                    GROUP BY channel_id
                )
                SELECT
                    {cmsId: String} AS cms_id,
                    {targetDay: Date} AS day,
                    'ZZ' AS country,
                    '' AS video_id,
                    C.channel_id AS channel_id,
                    '' AS asset_id,
                    'Channel Non-Video' AS asset_type,
                    'Non-Video' AS content_type,
                    'unclaimed' AS claimed_status,
                    'Non-Video' AS claim_type,
                    '' AS claim_status,
                    'Monetize' AS policy,
                    'Channel Adjustment' AS claim_origin,
                    '' AS isrc,
                    '' AS upc,
                    '' AS grid,
                    '' AS custom_id,
                    'Channel Non-Video Earnings' AS video_title,
                    coalesce(N.display_name, C.channel_id) AS username,
                    C.channel_id AS uploader,
                    '' AS uploader_type,
                    0 AS video_duration_sec,
                    '' AS video_upload_date,
                    coalesce(N.display_name, C.channel_id) AS channel_display_name,
                    'No' AS multiple_claims,
                    'Non-Video' AS category,
                    '' AS asset_labels,
                    '' AS artist,
                    'Channel Non-Video Earnings' AS asset_title,
                    '' AS album,
                    '' AS label,
                    '' AS genre,
                    0 AS owned_views,
                    CAST(0 AS Decimal64(10)) AS estimated_cpm,
                    CAST(0 AS Decimal64(10)) AS estimated_playback_based_cpm,
                    0 AS yt_rev_auction,
                    0 AS yt_rev_reserved,
                    0 AS yt_rev_partner_sold_yt_served,
                    0 AS yt_rev_partner_sold_p_served,
                    0 AS yt_rev_red,
                    0 AS yt_rev_total,
                    CAST((chan_ad - coalesce(V.vid_ad, 0.0)) AS Decimal64(10)) AS partner_rev_auction,
                    0 AS partner_rev_reserved,
                    0 AS partner_rev_partner_sold_yt_served,
                    0 AS partner_rev_partner_sold_p_served,
                    CAST((chan_red - coalesce(V.vid_red, 0.0)) AS Decimal64(10)) AS partner_rev_red,
                    CAST((chan_total - coalesce(V.vid_total, 0.0)) AS Decimal64(10)) AS partner_rev_total,
                    0 AS monetized_playbacks,
                    0 AS ad_impressions,
                    CAST((chan_trans - coalesce(V.vid_trans, 0.0)) AS Decimal64(10)) AS partner_rev_transaction,
                    0, 0, 0, 0, 0.0, 0, 0.0, 0, 0
                FROM channel_totals C
                LEFT JOIN video_totals V ON C.channel_id = V.channel_id
                LEFT JOIN channel_display_names N ON C.channel_id = N.channel_id
                WHERE abs(chan_total - coalesce(V.vid_total, 0.0)) > 0.01
            `;
            await client.command({
                query: insertNonVideoQuery,
                query_params: { cmsId, targetDay }
            });
        }

        // 5.5 Deduct video-level transactions from channelTransactions and insert them
        if (day && channelTransactions && channelTransactions.length > 0) {
            if (log) log(`[ETL] Processing ${channelTransactions.length} channel-level transaction adjustments...`);
            try {
                // Get the sum of video-level transaction revenue for each channel in this CMS for this day
                const videoTxQuery = `
                    SELECT 
                        channel_id,
                        SUM(partner_rev_total) as video_tx_sum
                    FROM estimated_revenue_daily
                    WHERE cms_id = {cmsId: String}
                      AND day = toDate({day: String})
                      AND video_id != ''
                      AND claim_origin != 'Rollup Mapping'
                      AND claim_origin != 'Channel Adjustment'
                    GROUP BY channel_id
                `;
                const videoTxRes = await client.query({
                    query: videoTxQuery,
                    query_params: { cmsId, day },
                    format: 'JSONEachRow'
                });
                const videoTxRows = await videoTxRes.json() as Array<{ channel_id: string; video_tx_sum: string | number }>;
                
                const videoTxMap = new Map<string, number>();
                for (const r of videoTxRows) {
                    videoTxMap.set(r.channel_id, parseFloat(r.video_tx_sum as string) || 0);
                }
                
                for (const tx of channelTransactions) {
                    const hasNewFormat = tx.total_revenue !== undefined;
                    const chanTotal = hasNewFormat ? Number(tx.total_revenue) : Number(tx.transaction_revenue || 0);
                    const videoSum = videoTxMap.get(tx.channel_id) || 0;
                    const remainingTx = chanTotal - videoSum;
                    if (remainingTx > 0.01) {
                        tx.total_remainder = remainingTx;
                        tx.transaction_revenue = remainingTx;
                        if (log) log(`[ETL] Channel ${tx.channel_id}: Total=$${tx.transaction_revenue + videoSum}, VideoSum=$${videoSum}, Remainder=$${remainingTx}`);
                    } else {
                        tx.total_remainder = 0;
                        tx.transaction_revenue = 0;
                    }
                }

                // Insert the channel transactions with video_id = ''
                for (const tx of channelTransactions) {
                    const txRev = tx.transaction_revenue !== undefined ? tx.transaction_revenue : (tx.total_remainder || 0);
                    if (txRev > 0) {
                        const chName = tx.channel_display_name || tx.channel_id;
                        const insertTxQuery = `
                            INSERT INTO estimated_revenue_daily (
                                cms_id, day, country, video_id, channel_id, asset_id, asset_type, content_type, claimed_status, claim_type, claim_status,
                                policy, claim_origin, isrc, upc, grid, custom_id, video_title, username, uploader, uploader_type, video_duration_sec, video_upload_date,
                                channel_display_name, multiple_claims, category, asset_labels, artist, asset_title, album, label, genre,
                                owned_views, estimated_cpm, estimated_playback_based_cpm, yt_rev_auction, yt_rev_reserved, yt_rev_partner_sold_yt_served, yt_rev_partner_sold_p_served,
                                yt_rev_red, yt_rev_total, partner_rev_auction, partner_rev_reserved, partner_rev_partner_sold_yt_served,
                                partner_rev_partner_sold_p_served, partner_rev_red, partner_rev_total,
                                monetized_playbacks, ad_impressions, partner_rev_transaction,
                                likes, comments, shares, dislikes, watch_time_minutes, average_view_duration_seconds, average_view_duration_percentage, subscribers_gained, subscribers_lost
                            ) VALUES (
                                {cmsId: String}, toDate({day: String}), 'ZZ', '', {channelId: String}, '', 'Channel Non-Video', 'Non-Video', 'unclaimed', 'Non-Video', '',
                                'Monetize', 'Channel Adjustment', '', '', '', '', 'Channel Non-Video Earnings', {channelName: String}, {channelId: String}, '', 0, '',
                                {channelName: String}, 'No', 'Non-Video', '', '', 'Channel Non-Video Earnings', '', '', '',
                                0, CAST(0 AS Decimal64(10)), CAST(0 AS Decimal64(10)), 0, 0, 0, 0,
                                0, CAST({txRev: Float64} AS Decimal64(10)), 0, 0, 0,
                                0, 0, CAST({txRev: Float64} AS Decimal64(10)),
                                0, 0, CAST({txRev: Float64} AS Decimal64(10)),
                                0, 0, 0, 0, 0.0, 0, 0.0, 0, 0
                            )
                        `;
                        await client.command({
                            query: insertTxQuery,
                            query_params: {
                                cmsId,
                                day,
                                channelId: tx.channel_id,
                                channelName: chName,
                                txRev: Number(txRev)
                            }
                        });
                    }
                }
            } catch (err: any) {
                if (log) log(`[ETL] ⚠️ Gagal memproses transaksi channel: ${err.message}`);
            }
        }

        let auditWarning = false;
        let auditMessage = "";

        // Self-Audit for GCS Bulk Ingestion (now including channel-level adjustments)
        if (log) log(`[Audit] Memulai verifikasi pasca-penyerapan (Post-Ingestion Self-Audit)...`);
        try {
            const auditRes = await client.query({
                query: `SELECT sum(owned_views) as total_views, sum(partner_rev_total) as total_rev FROM estimated_revenue_daily WHERE cms_id = {cmsId: String} AND day = {targetDay: Date}`,
                query_params: { cmsId, targetDay },
                format: 'JSONEachRow'
            });
            const auditRows = await auditRes.json() as any[];
            const ingestedViews = parseInt(auditRows[0]?.total_views || '0', 10);
            const ingestedRev = parseFloat(auditRows[0]?.total_rev || '0');

            // Check if GCS asset report was ingested
            const assetCountRes = await client.query({
                query: `SELECT count() as cnt, sum(estimated_partner_revenue) as total_rev FROM youtube_raw_asset_estimated_revenue WHERE cms_id = {cmsId: String} AND day = {targetDay: Date}`,
                query_params: { cmsId, targetDay },
                format: 'JSONEachRow'
            });
            const assetCountRows = await assetCountRes.json() as any[];
            const hasAssetReport = parseInt(assetCountRows[0]?.cnt || '0', 10) > 0;
            const rawAssetRevenue = parseFloat(assetCountRows[0]?.total_rev || '0');

            const baselineRevenue = totalCmsRevenue !== undefined && totalCmsRevenue > 0 ? totalCmsRevenue : (hasAssetReport ? rawAssetRevenue : adsRevenue);
            const revDiffVal = Math.abs(ingestedRev - baselineRevenue);
            const revDiffPercent = baselineRevenue > 0 ? (revDiffVal / baselineRevenue * 100) : 0;

            log?.(`[Audit] Total Raw Revenue Baseline: $${baselineRevenue.toFixed(2)}`);
            log?.(`[Audit] Total Ingested Revenue in ClickHouse: $${ingestedRev.toFixed(2)}`);
            if (revDiffVal > 0.01) {
                auditWarning = true;
                const pendingSuffix = lowPriorityCount !== undefined && lowPriorityCount > 0 ? ` [Pending: ${lowPriorityCount} videos]` : "";
                auditMessage = `Terdeteksi selisih pendapatan: ${revDiffPercent.toFixed(2)}% (Selisih: $${revDiffVal.toFixed(2)}, Raw: $${baselineRevenue.toFixed(2)}, Ingested: $${ingestedRev.toFixed(2)})${pendingSuffix}`;
                log?.(`[Audit] ⚠️ Warning: ${auditMessage}`);
            } else {
                auditMessage = `Verifikasi sukses! Selisih pendapatan sangat minimal (Revenue Diff: ${revDiffPercent.toFixed(2)}%, Selisih: $${revDiffVal.toFixed(2)})`;
                log?.(`[Audit] 🟢 Pass: ${auditMessage}`);
            }
        } catch (auditErr: any) {
            log?.(`[Audit] ⚠️ Warning: Gagal menjalankan self-audit: ${auditErr.message}`);
        }

        // Fetch final actual database sums for the logged output
        let finalTotalRows = totalRows;
        let finalAdsRevenue = adsRevenue;
        let finalSubRevenue = subRevenue;
        try {
            const finalStatsRes = await client.query({
                query: `
                    SELECT 
                        count() as total_rows,
                        sum(partner_rev_total - partner_rev_red) as ads_rev,
                        sum(partner_rev_red) as sub_rev
                    FROM estimated_revenue_daily 
                    WHERE cms_id = {cmsId: String} AND day = {targetDay: Date}
                `,
                query_params: { cmsId, targetDay },
                format: 'JSONEachRow'
            });
            const finalStatsRows = await finalStatsRes.json() as any[];
            if (finalStatsRows.length > 0) {
                finalTotalRows = parseInt(finalStatsRows[0].total_rows || '0', 10) || totalRows;
                finalAdsRevenue = parseFloat(finalStatsRows[0].ads_rev || '0') || adsRevenue;
                finalSubRevenue = parseFloat(finalStatsRows[0].sub_rev || '0') || subRevenue;
            }
        } catch (err: any) {
            if (log) log(`[ETL] Warning: Failed to query final database stats: ${err.message}`);
        }

        // Trigger one final progress update using final actual database sums
        onProgress?.({
            totalRows: finalTotalRows,
            processedRows: finalTotalRows,
            adsRows,
            subRows,
            batchesSent: 1,
            claimsRows,
            adsRevenue: finalAdsRevenue,
            subRevenue: finalSubRevenue
        });

        return { 
            totalRows: finalTotalRows, 
            adsRows, 
            subRows, 
            claimsRows, 
            adsRevenue: finalAdsRevenue, 
            subRevenue: finalSubRevenue, 
            auditWarning, 
            auditMessage 
        };
    } finally {
        // 6. Clean up temporary tables
        if (log) {
            log(`[ETL] Cleaning up temporary tables...`);
        }
        await client.command({ query: `DROP TABLE IF EXISTS ${tempClaimsTable}` }).catch(() => {});
        await client.command({ query: `DROP TABLE IF EXISTS ${tempEstimatedTable}` }).catch(() => {});
        if (assetRevenueFilePath) {
            await client.command({ query: `DROP TABLE IF EXISTS ${tempAssetTable}` }).catch(() => {});
        }
        if (channelRevenueFilePath) {
            await client.command({ query: `DROP TABLE IF EXISTS ${tempChannelTable}` }).catch(() => {});
        }
    }
}

export async function processVideoReach(opts: {
    filePath: string;
    cmsId: string;
    client: ClickHouseClient;
    batchSize?: number;
    onProgress?: (progress: { totalRows: number; processedRows: number; batchesSent: number }) => void;
    isAborted?: () => boolean;
    jobId?: string;
}): Promise<{ totalRows: number }> {
    const { filePath, cmsId, client, onProgress, isAborted, jobId = randomUUID() } = opts;

    if (isAborted?.()) throw new Error("Job aborted by user");

    const cleanJobId = jobId.replace(/-/g, '_');
    const tempTable = `temp_reach_${cleanJobId}`;

    try {
        await client.command({
            query: `
                CREATE TABLE IF NOT EXISTS ${tempTable} (
                    "date"                                 String DEFAULT '',
                    "day"                                  String DEFAULT '',
                    "Day"                                  String DEFAULT '',
                    "Date"                                 String DEFAULT '',
                    "Video ID"                             String DEFAULT '',
                    "video_id"                             String DEFAULT '',
                    "Video"                                String DEFAULT '',
                    "Channel ID"                           String DEFAULT '',
                    "channel_id"                           String DEFAULT '',
                    "Channel"                              String DEFAULT '',
                    "Impressions"                          String DEFAULT '',
                    "video_thumbnail_impressions"          String DEFAULT '',
                    "Impressions CTR"                      String DEFAULT '',
                    "CTR"                                  String DEFAULT '',
                    "video_thumbnail_impressions_ctr"      String DEFAULT '',
                    "Views"                                String DEFAULT '',
                    "owned_views"                          String DEFAULT '',
                    "views"                                String DEFAULT '',
                    "Watch Time"                           String DEFAULT '',
                    "Watch Time (sec)"                     String DEFAULT '',
                    "watch_time_sec"                       String DEFAULT ''
                ) ENGINE = StripeLog()
            `
        });

        const stream = await extractFileStream(filePath);
        const delimiter = await detectDelimiterFromFile(filePath);

        const abortInterval = setInterval(() => {
            if (isAborted?.()) {
                stream.destroy();
                clearInterval(abortInterval);
            }
        }, 1000);

        try {
            await client.insert({
                table: tempTable,
                values: stream,
                format: delimiter === '\t' ? 'TabSeparatedWithNames' : 'CSVWithNames',
                clickhouse_settings: {
                    input_format_skip_unknown_fields: 1
                }
            });
        } finally {
            clearInterval(abortInterval);
        }

        if (isAborted?.()) throw new Error("Job aborted by user");

        const countRes = await client.query({ query: `SELECT count() as cnt FROM ${tempTable}`, format: 'JSONEachRow' });
        const countRows = await countRes.json() as any[];
        const totalRows = parseInt(countRows[0]?.cnt, 10) || 0;

        const insertQuery = `
            INSERT INTO video_reach_performance_daily (
                cms_id, day, video_id, channel_id, impressions, impressions_ctr
            )
            SELECT
                {cmsId: String},
                toDateOrZero(if(length(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), '')) = 8,
                    concat(substring(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), ''), 1, 4), '-',
                           substring(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), ''), 5, 2), '-',
                           substring(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), ''), 7, 2)),
                    coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), '')
                )) AS day,
                coalesce(nullIf(T."Video ID", ''), nullIf(T.video_id, ''), nullIf(T.Video, ''), '') AS video_id,
                coalesce(nullIf(T."Channel ID", ''), nullIf(T.channel_id, ''), nullIf(T.Channel, ''), '') AS channel_id,
                sum(toInt64OrZero(replaceAll(coalesce(nullIf(T.Impressions, ''), nullIf(T.video_thumbnail_impressions, ''), '0'), ',', ''))) AS impressions,
                CAST(
                    any(toFloat64(coalesce(nullIf(T."Impressions CTR", ''), nullIf(T.CTR, ''), nullIf(T.video_thumbnail_impressions_ctr, ''), '0')))
                    AS Decimal64(4)
                ) AS impressions_ctr
            FROM ${tempTable} T
            GROUP BY day, video_id, channel_id
        `;

        await client.command({ 
            query: insertQuery,
            query_params: { cmsId }
        });

        onProgress?.({ totalRows, processedRows: totalRows, batchesSent: 1 });
        return { totalRows };
    } finally {
        await client.command({ query: `DROP TABLE IF EXISTS ${tempTable}` }).catch(() => {});
    }
}

// ── New Daily Analytics Row Interfaces ────────────────────

export interface VideoDemographicsRow {
    cms_id: string;
    day: string;
    video_id: string;
    channel_id: string;
    age_group: string;
    gender: string;
    views: number;
    watch_time_sec: number;
    views_percentage: number;
}

export interface VideoTrafficSourcesRow {
    cms_id: string;
    day: string;
    video_id: string;
    channel_id: string;
    traffic_source_type: string;
    views: number;
    watch_time_sec: number;
}

export interface VideoDevicesRow {
    cms_id: string;
    day: string;
    video_id: string;
    channel_id: string;
    device_type: string;
    operating_system: string;
    views: number;
    watch_time_sec: number;
}

// ── New Ingest Workers ────────────────────────────────────

export async function processVideoDemographics(opts: {
    filePath: string;
    cmsId: string;
    client: ClickHouseClient;
    batchSize?: number;
    isAborted?: () => boolean;
    jobId?: string;
}): Promise<{ totalRows: number }> {
    const { filePath, cmsId, client, isAborted, jobId = randomUUID() } = opts;

    if (isAborted?.()) throw new Error("Job aborted by user");

    const cleanJobId = jobId.replace(/-/g, '_');
    const tempTable = `temp_demo_${cleanJobId}`;

    try {
        await client.command({
            query: `
                CREATE TABLE IF NOT EXISTS ${tempTable} (
                    "date"                                 String DEFAULT '',
                    "day"                                  String DEFAULT '',
                    "Day"                                  String DEFAULT '',
                    "Date"                                 String DEFAULT '',
                    "Video ID"                             String DEFAULT '',
                    "video_id"                             String DEFAULT '',
                    "Video"                                String DEFAULT '',
                    "Channel ID"                           String DEFAULT '',
                    "channel_id"                           String DEFAULT '',
                    "Channel"                              String DEFAULT '',
                    "Age Group"                            String DEFAULT '',
                    "age_group"                            String DEFAULT '',
                    "ageGroup"                             String DEFAULT '',
                    "Gender"                               String DEFAULT '',
                    "gender"                               String DEFAULT '',
                    "Views"                                String DEFAULT '',
                    "views"                                String DEFAULT '',
                    "owned_views"                          String DEFAULT '',
                    "Watch Time"                           String DEFAULT '',
                    "Watch Time (sec)"                     String DEFAULT '',
                    "watch_time_sec"                       String DEFAULT '',
                    "Viewer Percentage"                    String DEFAULT '',
                    "viewer_percentage"                    String DEFAULT '',
                    "viewerPercentage"                     String DEFAULT '',
                    "views_percentage"                     String DEFAULT '',
                    "Views Percentage"                     String DEFAULT '',
                    "viewsPercentage"                      String DEFAULT ''
                ) ENGINE = StripeLog()
            `
        });

        const stream = await extractFileStream(filePath);
        const delimiter = await detectDelimiterFromFile(filePath);

        const abortInterval = setInterval(() => {
            if (isAborted?.()) {
                stream.destroy();
                clearInterval(abortInterval);
            }
        }, 1000);

        try {
            await client.insert({
                table: tempTable,
                values: stream,
                format: delimiter === '\t' ? 'TabSeparatedWithNames' : 'CSVWithNames',
                clickhouse_settings: {
                    input_format_skip_unknown_fields: 1
                }
            });
        } finally {
            clearInterval(abortInterval);
        }

        if (isAborted?.()) throw new Error("Job aborted by user");

        const countRes = await client.query({ query: `SELECT count() as cnt FROM ${tempTable}`, format: 'JSONEachRow' });
        const countRows = await countRes.json() as any[];
        const totalRows = parseInt(countRows[0]?.cnt, 10) || 0;

        const insertQuery = `
            INSERT INTO video_demographics_daily (
                cms_id, day, video_id, channel_id, age_group, gender, views_percentage
            )
            SELECT
                {cmsId: String},
                toDateOrZero(if(length(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), '')) = 8,
                    concat(substring(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), ''), 1, 4), '-',
                           substring(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), ''), 5, 2), '-',
                           substring(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), ''), 7, 2)),
                    coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), '')
                )) AS day,
                coalesce(nullIf(T."Video ID", ''), nullIf(T.video_id, ''), nullIf(T.Video, ''), '') AS video_id,
                coalesce(nullIf(T."Channel ID", ''), nullIf(T.channel_id, ''), nullIf(T.Channel, ''), '') AS channel_id,
                coalesce(nullIf(T."Age Group", ''), nullIf(T.age_group, ''), nullIf(T.ageGroup, ''), '') AS age_group,
                coalesce(nullIf(T.Gender, ''), nullIf(T.gender, ''), '') AS gender,
                CAST(
                    any(toFloat64(coalesce(nullIf(T."Viewer Percentage", ''), nullIf(T.viewer_percentage, ''), nullIf(T.viewerPercentage, ''), nullIf(T.views_percentage, ''), nullIf(T."Views Percentage", ''), nullIf(T.viewsPercentage, ''), '0')))
                    AS Decimal64(4)
                ) AS views_percentage
            FROM ${tempTable} T
            GROUP BY day, video_id, channel_id, age_group, gender
        `;

        await client.command({ 
            query: insertQuery,
            query_params: { cmsId }
        });

        return { totalRows };
    } finally {
        await client.command({ query: `DROP TABLE IF EXISTS ${tempTable}` }).catch(() => {});
    }
}

export async function processVideoTrafficSources(opts: {
    filePath: string;
    cmsId: string;
    client: ClickHouseClient;
    batchSize?: number;
    isAborted?: () => boolean;
    jobId?: string;
}): Promise<{ totalRows: number }> {
    const { filePath, cmsId, client, isAborted, jobId = randomUUID() } = opts;

    if (isAborted?.()) throw new Error("Job aborted by user");

    const cleanJobId = jobId.replace(/-/g, '_');
    const tempTable = `temp_traffic_${cleanJobId}`;

    try {
        await client.command({
            query: `
                CREATE TABLE IF NOT EXISTS ${tempTable} (
                    "date"                                 String DEFAULT '',
                    "day"                                  String DEFAULT '',
                    "Day"                                  String DEFAULT '',
                    "Date"                                 String DEFAULT '',
                    "Video ID"                             String DEFAULT '',
                    "video_id"                             String DEFAULT '',
                    "Video"                                String DEFAULT '',
                    "Channel ID"                           String DEFAULT '',
                    "channel_id"                           String DEFAULT '',
                    "Channel"                              String DEFAULT '',
                    "Traffic Source Type"                  String DEFAULT '',
                    "traffic_source_type"                  String DEFAULT '',
                    "trafficSourceType"                    String DEFAULT '',
                    "Views"                                String DEFAULT '',
                    "views"                                String DEFAULT '',
                    "owned_views"                          String DEFAULT '',
                    "Watch Time"                           String DEFAULT '',
                    "Watch Time (sec)"                     String DEFAULT '',
                    "watch_time_sec"                       String DEFAULT '',
                    "Watch Time (minutes)"                 String DEFAULT '',
                    "watch_time_minutes"                   String DEFAULT '',
                    "watchTimeMinutes"                     String DEFAULT ''
                ) ENGINE = StripeLog()
            `
        });

        const stream = await extractFileStream(filePath);
        const delimiter = await detectDelimiterFromFile(filePath);

        const abortInterval = setInterval(() => {
            if (isAborted?.()) {
                stream.destroy();
                clearInterval(abortInterval);
            }
        }, 1000);

        try {
            await client.insert({
                table: tempTable,
                values: stream,
                format: delimiter === '\t' ? 'TabSeparatedWithNames' : 'CSVWithNames',
                clickhouse_settings: {
                    input_format_skip_unknown_fields: 1
                }
            });
        } finally {
            clearInterval(abortInterval);
        }

        if (isAborted?.()) throw new Error("Job aborted by user");

        const countRes = await client.query({ query: `SELECT count() as cnt FROM ${tempTable}`, format: 'JSONEachRow' });
        const countRows = await countRes.json() as any[];
        const totalRows = parseInt(countRows[0]?.cnt, 10) || 0;

        const insertQuery = `
            INSERT INTO video_traffic_sources_daily (
                cms_id, day, video_id, channel_id, traffic_source_type, views, watch_time_sec
            )
            SELECT
                {cmsId: String},
                toDateOrZero(if(length(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), '')) = 8,
                    concat(substring(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), ''), 1, 4), '-',
                           substring(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), ''), 5, 2), '-',
                           substring(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), ''), 7, 2)),
                    coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), '')
                )) AS day,
                coalesce(nullIf(T."Video ID", ''), nullIf(T.video_id, ''), nullIf(T.Video, ''), '') AS video_id,
                coalesce(nullIf(T."Channel ID", ''), nullIf(T.channel_id, ''), nullIf(T.Channel, ''), '') AS channel_id,
                coalesce(nullIf(T."Traffic Source Type", ''), nullIf(T.traffic_source_type, ''), nullIf(T.trafficSourceType, ''), '') AS traffic_source_type,
                sum(toInt64OrZero(replaceAll(coalesce(nullIf(T.Views, ''), nullIf(T.views, ''), nullIf(T.owned_views, ''), '0'), ',', ''))) AS views,
                sum(if(coalesce(nullIf(T."Watch Time (sec)", ''), nullIf(T.watch_time_sec, ''), '') != '',
                       toInt64(round(toFloat64OrZero(replaceAll(coalesce(nullIf(T."Watch Time (sec)", ''), nullIf(T.watch_time_sec, ''), '0'), ',', '')))),
                       if(coalesce(nullIf(T."Watch Time (minutes)", ''), nullIf(T.watch_time_minutes, ''), nullIf(T.watchTimeMinutes, ''), '') != '',
                          toInt64(round(toFloat64OrZero(replaceAll(coalesce(nullIf(T."Watch Time (minutes)", ''), nullIf(T.watch_time_minutes, ''), nullIf(T.watchTimeMinutes, ''), '0'), ',', '')) * 60)),
                          toInt64(round(toFloat64OrZero(replaceAll(coalesce(nullIf(T."Watch Time", ''), '0'), ',', ''))))
                       )
                )) AS watch_time_sec
            FROM ${tempTable} T
            GROUP BY day, video_id, channel_id, traffic_source_type
        `;

        await client.command({ 
            query: insertQuery,
            query_params: { cmsId }
        });

        return { totalRows };
    } finally {
        await client.command({ query: `DROP TABLE IF EXISTS ${tempTable}` }).catch(() => {});
    }
}

export async function processVideoDevices(opts: {
    filePath: string;
    cmsId: string;
    client: ClickHouseClient;
    batchSize?: number;
    isAborted?: () => boolean;
    jobId?: string;
}): Promise<{ totalRows: number }> {
    const { filePath, cmsId, client, isAborted, jobId = randomUUID() } = opts;

    if (isAborted?.()) throw new Error("Job aborted by user");

    const cleanJobId = jobId.replace(/-/g, '_');
    const tempTable = `temp_device_${cleanJobId}`;

    try {
        await client.command({
            query: `
                CREATE TABLE IF NOT EXISTS ${tempTable} (
                    "date"                                 String DEFAULT '',
                    "day"                                  String DEFAULT '',
                    "Day"                                  String DEFAULT '',
                    "Date"                                 String DEFAULT '',
                    "Video ID"                             String DEFAULT '',
                    "video_id"                             String DEFAULT '',
                    "Video"                                String DEFAULT '',
                    "Channel ID"                           String DEFAULT '',
                    "channel_id"                           String DEFAULT '',
                    "Channel"                              String DEFAULT '',
                    "Device Type"                          String DEFAULT '',
                    "device_type"                          String DEFAULT '',
                    "deviceType"                           String DEFAULT '',
                    "Operating System"                     String DEFAULT '',
                    "operating_system"                     String DEFAULT '',
                    "operatingSystem"                      String DEFAULT '',
                    "os"                                   String DEFAULT '',
                    "Views"                                String DEFAULT '',
                    "views"                                String DEFAULT '',
                    "owned_views"                          String DEFAULT '',
                    "Watch Time"                           String DEFAULT '',
                    "Watch Time (sec)"                     String DEFAULT '',
                    "watch_time_sec"                       String DEFAULT '',
                    "Watch Time (minutes)"                 String DEFAULT '',
                    "watch_time_minutes"                   String DEFAULT '',
                    "watchTimeMinutes"                     String DEFAULT ''
                ) ENGINE = StripeLog()
            `
        });

        const stream = await extractFileStream(filePath);
        const delimiter = await detectDelimiterFromFile(filePath);

        const abortInterval = setInterval(() => {
            if (isAborted?.()) {
                stream.destroy();
                clearInterval(abortInterval);
            }
        }, 1000);

        try {
            await client.insert({
                table: tempTable,
                values: stream,
                format: delimiter === '\t' ? 'TabSeparatedWithNames' : 'CSVWithNames',
                clickhouse_settings: {
                    input_format_skip_unknown_fields: 1
                }
            });
        } finally {
            clearInterval(abortInterval);
        }

        if (isAborted?.()) throw new Error("Job aborted by user");

        const countRes = await client.query({ query: `SELECT count() as cnt FROM ${tempTable}`, format: 'JSONEachRow' });
        const countRows = await countRes.json() as any[];
        const totalRows = parseInt(countRows[0]?.cnt, 10) || 0;

        const insertQuery = `
            INSERT INTO video_devices_daily (
                cms_id, day, video_id, channel_id, device_type, operating_system, views, watch_time_sec
            )
            SELECT
                {cmsId: String},
                toDateOrZero(if(length(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), '')) = 8,
                    concat(substring(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), ''), 1, 4), '-',
                           substring(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), ''), 5, 2), '-',
                           substring(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), ''), 7, 2)),
                    coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), '')
                )) AS day,
                coalesce(nullIf(T."Video ID", ''), nullIf(T.video_id, ''), nullIf(T.Video, ''), '') AS video_id,
                coalesce(nullIf(T."Channel ID", ''), nullIf(T.channel_id, ''), nullIf(T.Channel, ''), '') AS channel_id,
                coalesce(nullIf(T."Device Type", ''), nullIf(T.device_type, ''), nullIf(T.deviceType, ''), '') AS device_type,
                coalesce(nullIf(T."Operating System", ''), nullIf(T.operating_system, ''), nullIf(T.operatingSystem, ''), nullIf(T.os, ''), '') AS operating_system,
                sum(toInt64OrZero(replaceAll(coalesce(nullIf(T.Views, ''), nullIf(T.views, ''), nullIf(T.owned_views, ''), '0'), ',', ''))) AS views,
                sum(if(coalesce(nullIf(T."Watch Time (sec)", ''), nullIf(T.watch_time_sec, ''), '') != '',
                       toInt64(round(toFloat64OrZero(replaceAll(coalesce(nullIf(T."Watch Time (sec)", ''), nullIf(T.watch_time_sec, ''), '0'), ',', '')))),
                       if(coalesce(nullIf(T."Watch Time (minutes)", ''), nullIf(T.watch_time_minutes, ''), nullIf(T.watchTimeMinutes, ''), '') != '',
                          toInt64(round(toFloat64OrZero(replaceAll(coalesce(nullIf(T."Watch Time (minutes)", ''), nullIf(T.watch_time_minutes, ''), nullIf(T.watchTimeMinutes, ''), '0'), ',', '')) * 60)),
                          toInt64(round(toFloat64OrZero(replaceAll(coalesce(nullIf(T."Watch Time", ''), '0'), ',', ''))))
                       )
                )) AS watch_time_sec
            FROM ${tempTable} T
            GROUP BY day, video_id, channel_id, device_type, operating_system
        `;

        await client.command({ 
            query: insertQuery,
            query_params: { cmsId }
        });

        return { totalRows };
    } finally {
        await client.command({ query: `DROP TABLE IF EXISTS ${tempTable}` }).catch(() => {});
    }
}

export async function processDailySubscribers(opts: {
    filePath: string;
    cmsId: string;
    client: ClickHouseClient;
    batchSize?: number;
    isAborted?: () => boolean;
    jobId?: string;
}): Promise<{ totalRows: number }> {
    const { filePath, cmsId, client, isAborted, jobId = randomUUID() } = opts;

    if (isAborted?.()) throw new Error("Job aborted by user");

    const cleanJobId = jobId.replace(/-/g, '_');
    const tempTable = `temp_subs_${cleanJobId}`;

    try {
        await client.command({
            query: `
                CREATE TABLE IF NOT EXISTS ${tempTable} (
                    "date"                                 String DEFAULT '',
                    "day"                                  String DEFAULT '',
                    "Day"                                  String DEFAULT '',
                    "Date"                                 String DEFAULT '',
                    "Channel ID"                           String DEFAULT '',
                    "channel_id"                           String DEFAULT '',
                    "Channel"                              String DEFAULT '',
                    "Country"                              String DEFAULT '',
                    "country_code"                         String DEFAULT '',
                    "country"                              String DEFAULT '',
                    "Subscribed Status"                    String DEFAULT '',
                    "subscribed_status"                    String DEFAULT '',
                    "subscribedStatus"                     String DEFAULT '',
                    "Subscribers Gained"                   String DEFAULT '',
                    "subscribers_gained"                   String DEFAULT '',
                    "subscribersGained"                    String DEFAULT '',
                    "Subscribers Lost"                     String DEFAULT '',
                    "subscribers_lost"                     String DEFAULT '',
                    "subscribersLost"                      String DEFAULT ''
                ) ENGINE = StripeLog()
            `
        });

        const stream = await extractFileStream(filePath);
        const delimiter = await detectDelimiterFromFile(filePath);

        const abortInterval = setInterval(() => {
            if (isAborted?.()) {
                stream.destroy();
                clearInterval(abortInterval);
            }
        }, 1000);

        try {
            await client.insert({
                table: tempTable,
                values: stream,
                format: delimiter === '\t' ? 'TabSeparatedWithNames' : 'CSVWithNames',
                clickhouse_settings: {
                    input_format_skip_unknown_fields: 1
                }
            });
        } finally {
            clearInterval(abortInterval);
        }

        if (isAborted?.()) throw new Error("Job aborted by user");

        const countRes = await client.query({ query: `SELECT count() as cnt FROM ${tempTable}`, format: 'JSONEachRow' });
        const countRows = await countRes.json() as any[];
        const totalRows = parseInt(countRows[0]?.cnt, 10) || 0;

        const insertQuery = `
            INSERT INTO channel_subscribers_daily (
                cms_id, day, channel_id, country, subscribed_status, subscribers_gained, subscribers_lost
            )
            SELECT
                {cmsId: String},
                toDateOrZero(if(length(coalesce(nullIf(date, ''), nullIf(day, ''), nullIf(Day, ''), nullIf(Date, ''))) = 8,
                    concat(substring(coalesce(nullIf(date, ''), nullIf(day, ''), nullIf(Day, ''), nullIf(Date, '')), 1, 4), '-',
                           substring(coalesce(nullIf(date, ''), nullIf(day, ''), nullIf(Day, ''), nullIf(Date, '')), 5, 2), '-',
                           substring(coalesce(nullIf(date, ''), nullIf(day, ''), nullIf(Date, '')), 7, 2)),
                    coalesce(nullIf(date, ''), nullIf(day, ''), nullIf(Day, ''), nullIf(Date, ''))
                )) AS parsed_day,
                coalesce(nullIf("Channel ID", ''), nullIf(channel_id, ''), nullIf(Channel, '')) AS parsed_channel_id,
                coalesce(nullIf(Country, ''), nullIf(country_code, ''), nullIf(country, '')) AS parsed_country,
                coalesce(nullIf("Subscribed Status", ''), nullIf(subscribed_status, ''), nullIf(subscribedStatus, '')) AS parsed_subscribed_status,
                sum(toInt64OrZero(replaceAll(coalesce(nullIf("Subscribers Gained", ''), nullIf(subscribers_gained, ''), nullIf(subscribersGained, ''), '0'), ',', ''))) AS subscribers_gained,
                sum(toInt64OrZero(replaceAll(coalesce(nullIf("Subscribers Lost", ''), nullIf(subscribers_lost, ''), nullIf(subscribersLost, ''), '0'), ',', ''))) AS subscribers_lost
            FROM ${tempTable}
            GROUP BY parsed_day, parsed_channel_id, parsed_country, parsed_subscribed_status
        `;

        await client.command({ 
            query: insertQuery,
            query_params: { cmsId }
        });

        return { totalRows };
    } finally {
        await client.command({ query: `DROP TABLE IF EXISTS ${tempTable}` }).catch(() => {});
    }
}

export async function processDailyInteractions(opts: {
    filePath: string;
    cmsId: string;
    client: ClickHouseClient;
    batchSize?: number;
    isAborted?: () => boolean;
    jobId?: string;
}): Promise<{ totalRows: number }> {
    const { filePath, cmsId, client, isAborted, jobId = randomUUID() } = opts;

    if (isAborted?.()) throw new Error("Job aborted by user");

    const cleanJobId = jobId.replace(/-/g, '_');
    const tempTable = `temp_inter_${cleanJobId}`;

    try {
        await client.command({
            query: `
                CREATE TABLE IF NOT EXISTS ${tempTable} (
                    "date"                                 String DEFAULT '',
                    "day"                                  String DEFAULT '',
                    "Day"                                  String DEFAULT '',
                    "Date"                                 String DEFAULT '',
                    "Video ID"                             String DEFAULT '',
                    "video_id"                             String DEFAULT '',
                    "Video"                                String DEFAULT '',
                    "Channel ID"                           String DEFAULT '',
                    "channel_id"                           String DEFAULT '',
                    "Channel"                              String DEFAULT '',
                    "Likes"                                String DEFAULT '',
                    "likes"                                String DEFAULT '',
                    "Dislikes"                             String DEFAULT '',
                    "dislikes"                             String DEFAULT '',
                    "Comments"                             String DEFAULT '',
                    "comments"                             String DEFAULT '',
                    "Shares"                               String DEFAULT '',
                    "shares"                               String DEFAULT ''
                ) ENGINE = StripeLog()
            `
        });

        const stream = await extractFileStream(filePath);
        const delimiter = await detectDelimiterFromFile(filePath);

        const abortInterval = setInterval(() => {
            if (isAborted?.()) {
                stream.destroy();
                clearInterval(abortInterval);
            }
        }, 1000);

        try {
            await client.insert({
                table: tempTable,
                values: stream,
                format: delimiter === '\t' ? 'TabSeparatedWithNames' : 'CSVWithNames',
                clickhouse_settings: {
                    input_format_skip_unknown_fields: 1
                }
            });
        } finally {
            clearInterval(abortInterval);
        }

        if (isAborted?.()) throw new Error("Job aborted by user");

        const countRes = await client.query({ query: `SELECT count() as cnt FROM ${tempTable}`, format: 'JSONEachRow' });
        const countRows = await countRes.json() as any[];
        const totalRows = parseInt(countRows[0]?.cnt, 10) || 0;

        const insertQuery = `
            INSERT INTO video_interactions_daily (
                cms_id, day, video_id, channel_id, likes, dislikes, comments, shares
            )
            SELECT
                {cmsId: String},
                toDateOrZero(if(length(coalesce(nullIf(date, ''), nullIf(day, ''), nullIf(Day, ''), nullIf(Date, ''))) = 8,
                    concat(substring(coalesce(nullIf(date, ''), nullIf(day, ''), nullIf(Day, ''), nullIf(Date, '')), 1, 4), '-',
                           substring(coalesce(nullIf(date, ''), nullIf(day, ''), nullIf(Day, ''), nullIf(Date, '')), 5, 2), '-',
                           substring(coalesce(nullIf(date, ''), nullIf(day, ''), nullIf(Date, '')), 7, 2)),
                    coalesce(nullIf(date, ''), nullIf(day, ''), nullIf(Day, ''), nullIf(Date, ''))
                )) AS day,
                coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf(Video, '')) AS video_id,
                coalesce(nullIf("Channel ID", ''), nullIf(channel_id, ''), nullIf(Channel, '')) AS channel_id,
                toInt64OrZero(coalesce(nullIf(Likes, ''), nullIf(likes, ''))) AS likes,
                toInt64OrZero(coalesce(nullIf(Dislikes, ''), nullIf(dislikes, ''))) AS dislikes,
                toInt64OrZero(coalesce(nullIf(Comments, ''), nullIf(comments, ''))) AS comments,
                toInt64OrZero(coalesce(nullIf(Shares, ''), nullIf(shares, ''))) AS shares
            FROM ${tempTable}
        `;

        await client.command({ 
            query: insertQuery,
            query_params: { cmsId }
        });

        return { totalRows };
    } finally {
        await client.command({ query: `DROP TABLE IF EXISTS ${tempTable}` }).catch(() => {});
    }
}

export async function processSpeedLayerData(opts: {
    client: any;
    cmsId: string;
    day: string;
    columns: string[];
    rows: any[][];
    channelTransactions?: Array<{ 
        channel_id: string; 
        channel_display_name?: string; 
        transaction_revenue?: number;
        total_revenue?: number;
        ad_revenue?: number;
        red_revenue?: number;
        total_remainder?: number;
        ad_remainder?: number;
        red_remainder?: number;
        tx_remainder?: number;
    }>;
    lowPriorityCount?: number;
    totalCmsRevenue?: number;
    log?: (msg: string) => void;
}): Promise<{ totalRows: number; auditWarning: boolean; auditMessage: string; pendingSuffix: string; subRevenue: number; adsRevenue: number }> {
    const { client, cmsId, day, columns, rows, channelTransactions, lowPriorityCount, totalCmsRevenue, log } = opts;

    const tempTable = `temp_speed_layer_${cmsId.replace(/-/g, '_')}_${day.replace(/-/g, '_')}`;

    try {
        if (log) log(`[Speed Layer] Membuat tabel temporary ClickHouse: ${tempTable}...`);

        await client.command({
            query: `
                CREATE TABLE IF NOT EXISTS ${tempTable} (
                    video_id String,
                    creator_content_type String,
                    views Int64,
                    estimated_revenue Decimal64(10),
                    estimated_ad_revenue Decimal64(10),
                    estimated_red_partner_revenue Decimal64(10),
                    estimated_transaction_revenue Decimal64(10),
                    monetized_playbacks Int64,
                    ad_impressions Int64,
                    likes Int64,
                    comments Int64,
                    shares Int64,
                    dislikes Int64,
                    watch_time_minutes Float64,
                    average_view_duration_seconds UInt32,
                    average_view_duration_percentage Float64,
                    subscribers_gained Int64,
                    subscribers_lost Int64
                ) ENGINE = Memory
            `
        });

        const videoIdIdx = columns.indexOf('video');
        const creatorContentTypeIdx = columns.indexOf('creatorContentType');
        const viewsIdx = columns.indexOf('views');
        const estRevIdx = columns.indexOf('estimatedRevenue');
        const estAdRevIdx = columns.indexOf('estimatedAdRevenue');
        const estRedRevIdx = columns.indexOf('estimatedRedPartnerRevenue');
        const estTransRevIdx = columns.indexOf('estimatedTransactionRevenue');
        const playbacksIdx = columns.indexOf('monetizedPlaybacks');
        const impressionsIdx = columns.indexOf('adImpressions');
        const likesIdx = columns.indexOf('likes');
        const commentsIdx = columns.indexOf('comments');
        const sharesIdx = columns.indexOf('shares');
        const dislikesIdx = columns.indexOf('dislikes');
        const watchTimeIdx = columns.indexOf('estimatedMinutesWatched');
        const avgDurationIdx = columns.indexOf('averageViewDuration');
        const avgPercentageIdx = columns.indexOf('averageViewPercentage');
        const subsGainedIdx = columns.indexOf('subscribersGained');
        const subsLostIdx = columns.indexOf('subscribersLost');

        const videoToChannel = new Map<string, string>();
        try {
            const claimsRes = await client.query({
                query: `
                    WITH deduplicated_claims AS (
                        SELECT 
                            video_id,
                            channel_id
                        FROM youtube_raw_claims
                        WHERE cms_id = {cmsId: String} AND claim_status = 'ACTIVE' AND day <= greatest(toDate({day: String}), (SELECT min(day) FROM youtube_raw_claims WHERE cms_id = {cmsId: String} AND claim_status = 'ACTIVE'))
                        LIMIT 1 BY video_id
                    )
                    SELECT 
                        video_id, 
                        coalesce(channel_id, '') as channel_id 
                    FROM deduplicated_claims
                `,
                query_params: { cmsId, day },
                format: 'JSONEachRow'
            });
            const claimsRows = await claimsRes.json() as Array<{ video_id: string; channel_id: string }>;
            for (const r of claimsRows) {
                videoToChannel.set(r.video_id, r.channel_id);
            }
        } catch (e: any) {
            if (log) log(`[Speed Layer] ⚠️ Gagal mengambil peta video -> channel: ${e.message}`);
        }

        const channelTxQuota = new Map<string, number>();
        if (channelTransactions) {
            for (const tx of channelTransactions) {
                const txRevVal = tx.transaction_revenue !== undefined 
                    ? tx.transaction_revenue 
                    : (tx.total_revenue !== undefined ? Math.max(0, tx.total_revenue - ((tx.ad_revenue || 0) + (tx.red_revenue || 0))) : 0);
                if (tx.channel_id === 'UCLhhqmJ_GODYKio5oHF0kJw' || txRevVal > 1000) {
                    continue;
                }
                channelTxQuota.set(tx.channel_id, txRevVal);
            }
        }

        const mappedRows = rows.map(r => {
            const videoId = r[videoIdIdx] || '';
            const chId = videoToChannel.get(videoId) || '';
            const rawTx = toDecimal(r[estTransRevIdx]);
            let txRev = 0;

            if (chId) {
                const quota = channelTxQuota.get(chId) || 0;
                if (quota > 0) {
                    txRev = Math.min(rawTx, quota);
                    channelTxQuota.set(chId, Math.max(0, quota - txRev));
                }
            }

            return {
                video_id: videoId,
                creator_content_type: r[creatorContentTypeIdx] || '',
                views: toInt(r[viewsIdx]),
                estimated_revenue: toDecimal(r[estRevIdx]),
                estimated_ad_revenue: toDecimal(r[estAdRevIdx]),
                estimated_red_partner_revenue: toDecimal(r[estRedRevIdx]),
                estimated_transaction_revenue: txRev,
                monetized_playbacks: toInt(r[playbacksIdx]),
                ad_impressions: toInt(r[impressionsIdx]),
                likes: toInt(r[likesIdx]),
                comments: toInt(r[commentsIdx]),
                shares: toInt(r[sharesIdx]),
                dislikes: toInt(r[dislikesIdx]),
                watch_time_minutes: toDecimal(r[watchTimeIdx]),
                average_view_duration_seconds: toInt(r[avgDurationIdx]),
                average_view_duration_percentage: toDecimal(r[avgPercentageIdx]),
                subscribers_gained: toInt(r[subsGainedIdx]),
                subscribers_lost: toInt(r[subsLostIdx])
            };
        });

        if (log) log(`[Speed Layer] Menulis ${mappedRows.length} baris data ke tabel temporary...`);

        await client.insert({
            table: tempTable,
            values: mappedRows,
            format: 'JSONEachRow'
        });

        if (log) log(`[Speed Layer] Menghapus data lama untuk tanggal ${day} di estimated_revenue_daily...`);

        await client.command({
            query: `ALTER TABLE estimated_revenue_daily DELETE WHERE cms_id = {cmsId: String} AND day = toDate({day: String}) SETTINGS mutations_sync = 1`,
            query_params: { cmsId, day }
        });

        let isDone = false;
        let attempts = 0;
        while (!isDone && attempts < 30) {
            const res = await client.query({
                query: `SELECT is_done FROM system.mutations WHERE table = 'estimated_revenue_daily' AND command LIKE '%DELETE WHERE cms_id = \\'${cmsId}\\' AND day = toDate(\\'${day}\\')%' ORDER BY create_time DESC LIMIT 1`,
                format: 'JSONEachRow'
            });
            const muts = await res.json() as any[];
            if (muts.length === 0 || muts[0].is_done === 1 || muts[0].is_done === '1') {
                isDone = true;
            } else {
                attempts++;
                await new Promise(r => setTimeout(r, 1000));
            }
        }

        if (log) log(`[Speed Layer] Menjalankan JOIN & ETL untuk pembagian pendapatan multi-claim...`);

        const etlQuery = `
            INSERT INTO estimated_revenue_daily (
                cms_id, day, country, video_id, channel_id, owner_channel_id, asset_id, asset_type, content_type, creator_content_type, claimed_status, claim_type, claim_status,
                policy, claim_origin, isrc, upc, grid, custom_id, video_title, username, uploader, uploader_type, video_duration_sec, video_upload_date,
                channel_display_name, multiple_claims, category, asset_labels, artist, asset_title, album, label, genre,
                owned_views, estimated_cpm, estimated_playback_based_cpm, yt_rev_auction, yt_rev_reserved, yt_rev_partner_sold_yt_served, yt_rev_partner_sold_p_served,
                yt_rev_red, yt_rev_total, partner_rev_auction, partner_rev_reserved, partner_rev_partner_sold_yt_served,
                partner_rev_partner_sold_p_served, partner_rev_red, partner_rev_total,
                monetized_playbacks, ad_impressions, partner_rev_transaction,
                likes, comments, shares, dislikes, watch_time_minutes, average_view_duration_seconds, average_view_duration_percentage, subscribers_gained, subscribers_lost
            )
            WITH 
            asset_owners AS (
                SELECT 
                    asset_id, 
                    argMax(channel_id, ingested_at) as owner_channel_id
                FROM youtube_raw_claims
                WHERE cms_id = {cmsId: String}
                  AND claim_status = 'ACTIVE'
                  AND day <= greatest(toDate({day: String}), (SELECT min(day) FROM youtube_raw_claims WHERE cms_id = {cmsId: String} AND claim_status = 'ACTIVE'))
                  AND content_type = 'PARTNER_UPLOADED'
                  AND asset_id != ''
                  AND channel_id != ''
                GROUP BY asset_id
            ),
            deduplicated_claims AS (
                SELECT 
                    video_id,
                    asset_id,
                    channel_id,
                    asset_type,
                    content_type,
                    claim_type,
                    policy,
                    claim_origin,
                    custom_id,
                    isrc,
                    grid,
                    upc,
                    video_title,
                    username,
                    uploader,
                    video_duration_sec,
                    channel_display_name,
                    multiple_claims,
                    category,
                    asset_labels,
                    artist,
                    asset_title,
                    album,
                    label
                FROM youtube_raw_claims
                WHERE cms_id = {cmsId: String} AND claim_status = 'ACTIVE' AND day <= greatest(toDate({day: String}), (SELECT min(day) FROM youtube_raw_claims WHERE cms_id = {cmsId: String} AND claim_status = 'ACTIVE'))
                ORDER BY channel_id DESC, ingested_at DESC
                LIMIT 1 BY (video_id, asset_id)
            ),
            claim_counts AS (
                SELECT video_id, count() as cnt 
                FROM deduplicated_claims 
                GROUP BY video_id
            ),
            latest_metadata AS (
                SELECT 
                    asset_id,
                    argMax(asset_title, day) AS asset_title,
                    argMax(artist, day) AS artist,
                    argMax(album, day) AS album,
                    argMax(label, day) AS label,
                    argMax(isrc, day) AS isrc,
                    argMax(upc, day) AS upc,
                    argMax(custom_id, day) AS custom_id,
                    argMax(genre, day) AS genre,
                    argMax(asset_labels, day) AS asset_labels
                FROM youtube_asset_metadata
                WHERE cms_id = {cmsId: String}
                GROUP BY asset_id
            ),
            latest_video_metadata AS (
                SELECT 
                    video_id,
                    argMax(asset_id, day) AS latest_asset_id,
                    argMax(channel_id, day) AS latest_channel_id,
                    argMax(video_title, day) AS latest_video_title,
                    argMax(video_length_sec, day) AS latest_video_length_sec,
                    argMax(content_type, day) AS latest_content_type,
                    argMax(custom_id, day) AS latest_custom_id,
                    argMax(isrc, day) AS latest_isrc,
                    argMax(category, day) AS latest_category
                FROM youtube_video_metadata
                WHERE cms_id = {cmsId: String} AND asset_id != ''
                GROUP BY video_id
            )
            SELECT
                {cmsId: String}, 
                toDate({day: String}) as day,
                '' as country,
                R.video_id,
                coalesce(nullIf(C.channel_id, ''), nullIf(V.latest_channel_id, ''), '') as channel_id,
                if(coalesce(nullIf(C.content_type, ''), nullIf(V.latest_content_type, '')) LIKE '%UGC%',
                    coalesce(nullIf(AO.owner_channel_id, ''), coalesce(nullIf(C.channel_id, ''), nullIf(V.latest_channel_id, ''))),
                    coalesce(nullIf(C.channel_id, ''), nullIf(V.latest_channel_id, ''))
                ) as owner_channel_id,
                coalesce(nullIf(C.asset_id, ''), nullIf(V.latest_asset_id, ''), concat('Unknown Asset - ', R.video_id)) as asset_id,
                coalesce(C.asset_type, if(coalesce(nullIf(C.asset_id, ''), nullIf(V.latest_asset_id, ''), '') != '', 'Sound Recording', '')) as asset_type,
                coalesce(nullIf(C.content_type, ''), nullIf(V.latest_content_type, ''), '') as content_type,
                coalesce(R.creator_content_type, '') as creator_content_type,
                'claimed' as claimed_status,
                coalesce(C.claim_type, if(coalesce(nullIf(C.asset_id, ''), nullIf(V.latest_asset_id, ''), '') != '', 'Audio', '')) as claim_type,
                '' as claim_status,
                coalesce(C.policy, if(coalesce(nullIf(C.asset_id, ''), nullIf(V.latest_asset_id, ''), '') != '', 'Monetize', '')) as policy,
                coalesce(nullIf(C.claim_origin, ''), if(coalesce(nullIf(C.asset_id, ''), nullIf(V.latest_asset_id, ''), '') != '', 'Video Match', '')) as claim_origin,
                coalesce(nullIf(M.isrc, ''), nullIf(C.isrc, ''), nullIf(V.latest_isrc, ''), '') as isrc,
                coalesce(nullIf(M.upc, ''), nullIf(C.upc, ''), '') as upc,
                coalesce(C.grid, '') as grid,
                coalesce(nullIf(M.custom_id, ''), nullIf(C.custom_id, ''), nullIf(V.latest_custom_id, ''), '') as custom_id,
                coalesce(nullIf(C.video_title, ''), V.latest_video_title, '') as video_title,
                coalesce(C.username, '') as username,
                coalesce(nullIf(C.uploader, ''), V.latest_channel_id, '') as uploader,
                '' as uploader_type,
                coalesce(C.video_duration_sec, V.latest_video_length_sec, 0) as video_duration_sec,
                '' as video_upload_date,
                coalesce(nullIf(C.channel_display_name, ''), V.latest_video_title, '') as channel_display_name,
                if(coalesce(CC.cnt, 0) > 1, 'Yes', 'No') as multiple_claims,
                coalesce(nullIf(C.category, ''), V.latest_category, '') as category,
                coalesce(nullIf(M.asset_labels, ''), nullIf(C.asset_labels, ''), '') as asset_labels,
                coalesce(nullIf(M.artist, ''), nullIf(C.artist, ''), '') as artist,
                coalesce(nullIf(M.asset_title, ''), nullIf(C.asset_title, ''), nullIf(V.latest_video_title, ''), '') as asset_title,
                coalesce(nullIf(M.album, ''), nullIf(C.album, ''), '') as album,
                coalesce(nullIf(M.label, ''), nullIf(C.label, ''), '') as label,
                coalesce(M.genre, V.latest_category, '') as genre,
                toInt64(round(R.views / coalesce(nullIf(CC.cnt, 0), 1))) as views,
                CAST(0 AS Decimal64(10)) as estimated_cpm,
                CAST(0 AS Decimal64(10)) as estimated_playback_based_cpm,
                CAST(R.estimated_ad_revenue / coalesce(nullIf(CC.cnt, 0), 1) AS Decimal64(10)) as yt_rev_auction,
                0 as yt_rev_reserved,
                0 as yt_rev_partner_sold_yt_served,
                0 as yt_rev_partner_sold_p_served,
                CAST(R.estimated_red_partner_revenue / coalesce(nullIf(CC.cnt, 0), 1) AS Decimal64(10)) as yt_rev_red,
                CAST(R.estimated_revenue / coalesce(nullIf(CC.cnt, 0), 1) AS Decimal64(10)) as yt_rev_total,
                CAST(R.estimated_ad_revenue / coalesce(nullIf(CC.cnt, 0), 1) AS Decimal64(10)) as partner_rev_auction,
                0 as partner_rev_reserved,
                0 as partner_rev_partner_sold_yt_served,
                0 as partner_rev_partner_sold_p_served,
                CAST(R.estimated_red_partner_revenue / coalesce(nullIf(CC.cnt, 0), 1) AS Decimal64(10)) as partner_rev_red,
                CAST(R.estimated_revenue / coalesce(nullIf(CC.cnt, 0), 1) AS Decimal64(10)) as partner_rev_total,
                toInt64(round(R.monetized_playbacks / coalesce(nullIf(CC.cnt, 0), 1))) as monetized_playbacks,
                toInt64(round(R.ad_impressions / coalesce(nullIf(CC.cnt, 0), 1))) as ad_impressions,
                CAST(R.estimated_transaction_revenue / coalesce(nullIf(CC.cnt, 0), 1) AS Decimal64(10)) as partner_rev_transaction,
                toInt64(round(R.likes / coalesce(nullIf(CC.cnt, 0), 1))) as likes,
                toInt64(round(R.comments / coalesce(nullIf(CC.cnt, 0), 1))) as comments,
                toInt64(round(R.shares / coalesce(nullIf(CC.cnt, 0), 1))) as shares,
                toInt64(round(R.dislikes / coalesce(nullIf(CC.cnt, 0), 1))) as dislikes,
                R.watch_time_minutes / coalesce(nullIf(CC.cnt, 0), 1) as watch_time_minutes,
                toInt32(round(R.average_view_duration_seconds / coalesce(nullIf(CC.cnt, 0), 1))) as average_view_duration_seconds,
                R.average_view_duration_percentage / coalesce(nullIf(CC.cnt, 0), 1) as average_view_duration_percentage,
                toInt64(round(R.subscribers_gained / coalesce(nullIf(CC.cnt, 0), 1))) as subscribers_gained,
                toInt64(round(R.subscribers_lost / coalesce(nullIf(CC.cnt, 0), 1))) as subscribers_lost
            FROM ${tempTable} R
            LEFT JOIN deduplicated_claims C ON R.video_id = C.video_id
            LEFT JOIN claim_counts CC ON R.video_id = CC.video_id
            LEFT JOIN latest_video_metadata V ON R.video_id = V.video_id
            LEFT JOIN latest_metadata M ON coalesce(nullIf(C.asset_id, ''), nullIf(V.latest_asset_id, '')) = M.asset_id
            LEFT JOIN asset_owners AO ON coalesce(nullIf(C.asset_id, ''), nullIf(V.latest_asset_id, '')) = AO.asset_id
        `;

        await client.command({
            query: etlQuery,
            query_params: { cmsId, day }
        });

        // Deduct video-level transactions from channelTransactions to prevent double counting
        if (channelTransactions && channelTransactions.length > 0) {
            try {
                const videoTxQuery = `
                    WITH deduplicated_claims AS (
                        SELECT 
                            video_id,
                            asset_id,
                            channel_id
                        FROM youtube_raw_claims
                        WHERE cms_id = {cmsId: String} AND claim_status = 'ACTIVE' AND day <= greatest(toDate({day: String}), (SELECT min(day) FROM youtube_raw_claims WHERE cms_id = {cmsId: String} AND claim_status = 'ACTIVE'))
                        ORDER BY channel_id DESC, ingested_at DESC
                        LIMIT 1 BY (video_id, asset_id)
                    ),
                    claim_counts AS (
                        SELECT video_id, count() as cnt 
                        FROM deduplicated_claims 
                        GROUP BY video_id
                    )
                    SELECT 
                        coalesce(nullIf(C.channel_id, ''), nullIf(M.channel_id, ''), '') as resolved_channel_id,
                        SUM(R.estimated_revenue / coalesce(nullIf(CC.cnt, 0), 1)) as video_total_sum,
                        SUM(R.estimated_ad_revenue / coalesce(nullIf(CC.cnt, 0), 1)) as video_ad_sum,
                        SUM(R.estimated_red_partner_revenue / coalesce(nullIf(CC.cnt, 0), 1)) as video_red_sum
                    FROM ${tempTable} R
                    LEFT JOIN deduplicated_claims C ON R.video_id = C.video_id
                    LEFT JOIN claim_counts CC ON R.video_id = CC.video_id
                    LEFT JOIN (
                        SELECT video_id, argMax(channel_id, day) as channel_id
                        FROM youtube_video_metadata
                        WHERE cms_id = {cmsId: String}
                        GROUP BY video_id
                    ) M ON R.video_id = M.video_id
                    GROUP BY resolved_channel_id
                `;
                const videoTxRes = await client.query({
                    query: videoTxQuery,
                    query_params: { cmsId, day },
                    format: 'JSONEachRow'
                });
                const videoTxRows = await videoTxRes.json() as Array<{ 
                    resolved_channel_id: string; 
                    video_total_sum: string | number;
                    video_ad_sum: string | number;
                    video_red_sum: string | number;
                }>;
                
                const videoTotalMap = new Map<string, number>();
                const videoAdMap = new Map<string, number>();
                const videoRedMap = new Map<string, number>();
                for (const r of videoTxRows) {
                    videoTotalMap.set(r.resolved_channel_id, parseFloat(r.video_total_sum as string) || 0);
                    videoAdMap.set(r.resolved_channel_id, parseFloat(r.video_ad_sum as string) || 0);
                    videoRedMap.set(r.resolved_channel_id, parseFloat(r.video_red_sum as string) || 0);
                }
                
                for (const tx of channelTransactions) {
                    const hasNewFormat = tx.total_revenue !== undefined;
                    const chanTotal = hasNewFormat ? Number(tx.total_revenue) : Number(tx.transaction_revenue || 0);
                    const chanAd = hasNewFormat ? Number(tx.ad_revenue || 0) : 0;
                    const chanRed = hasNewFormat ? Number(tx.red_revenue || 0) : 0;
                    
                    const videoTotal = videoTotalMap.get(tx.channel_id) || 0;
                    const videoAd = videoAdMap.get(tx.channel_id) || 0;
                    const videoRed = videoRedMap.get(tx.channel_id) || 0;
                    
                    const totalRemainder = Math.max(0, chanTotal - videoTotal);
                    const adRemainder = Math.max(0, chanAd - videoAd);
                    const redRemainder = Math.max(0, chanRed - videoRed);
                    const txRemainder = Math.max(0, totalRemainder - (adRemainder + redRemainder));
                    
                    if (totalRemainder > 0.01) {
                        tx.total_remainder = totalRemainder;
                        tx.ad_remainder = adRemainder;
                        tx.red_remainder = redRemainder;
                        tx.tx_remainder = txRemainder;
                        if (log) log(`[Speed Layer] Channel ${tx.channel_id}: TotalRem=$${totalRemainder.toFixed(4)} (Ad=$${adRemainder.toFixed(4)}, Red=$${redRemainder.toFixed(4)}, Tx=$${txRemainder.toFixed(4)})`);
                    } else {
                        tx.total_remainder = 0;
                        tx.ad_remainder = 0;
                        tx.red_remainder = 0;
                        tx.tx_remainder = 0;
                    }
                }
            } catch (err: any) {
                if (log) log(`[Speed Layer] ⚠️ Gagal menghitung sisa transaksi channel: ${err.message}`);
            }
        }

        // Insert channel transactions if present
        if (channelTransactions && channelTransactions.length > 0) {
            if (log) log(`[Speed Layer] Menyimpan data Transaction Revenue tingkat channel (setelah dikurangi video)...`);
            
            for (const tx of channelTransactions) {
                const totalRem = tx.total_remainder !== undefined ? tx.total_remainder : Number(tx.transaction_revenue || 0);
                if (totalRem > 0) {
                    const adRem = tx.ad_remainder !== undefined ? tx.ad_remainder : 0;
                    const redRem = tx.red_remainder !== undefined ? tx.red_remainder : 0;
                    const txRem = tx.tx_remainder !== undefined ? tx.tx_remainder : totalRem;
                    const chName = tx.channel_display_name || tx.channel_id;
                    const insertTxQuery = `
                        INSERT INTO estimated_revenue_daily (
                            cms_id, day, country, video_id, channel_id, asset_id, asset_type, content_type, claimed_status, claim_type, claim_status,
                            policy, claim_origin, isrc, upc, grid, custom_id, video_title, username, uploader, uploader_type, video_duration_sec, video_upload_date,
                            channel_display_name, multiple_claims, category, asset_labels, artist, asset_title, album, label, genre,
                            owned_views, estimated_cpm, estimated_playback_based_cpm, yt_rev_auction, yt_rev_reserved, yt_rev_partner_sold_yt_served, yt_rev_partner_sold_p_served,
                            yt_rev_red, yt_rev_total, partner_rev_auction, partner_rev_reserved, partner_rev_partner_sold_yt_served,
                            partner_rev_partner_sold_p_served, partner_rev_red, partner_rev_total,
                            monetized_playbacks, ad_impressions, partner_rev_transaction,
                            likes, comments, shares, dislikes, watch_time_minutes, average_view_duration_seconds, average_view_duration_percentage, subscribers_gained, subscribers_lost
                        ) VALUES (
                            {cmsId: String}, toDate({day: String}), 'ZZ', '', {channelId: String}, '', 'Channel Non-Video', 'Non-Video', 'unclaimed', 'Non-Video', '',
                            'Monetize', 'Channel Adjustment', '', '', '', '', 'Channel Non-Video Earnings', {channelName: String}, {channelId: String}, '', 0, '',
                            {channelName: String}, 'No', 'Non-Video', '', '', 'Channel Non-Video Earnings', '', '', '',
                            0, CAST(0 AS Decimal64(10)), CAST(0 AS Decimal64(10)), CAST({adRem: Float64} AS Decimal64(10)), 0, 0, 0,
                            CAST({redRem: Float64} AS Decimal64(10)), CAST({totalRem: Float64} AS Decimal64(10)), CAST({adRem: Float64} AS Decimal64(10)), 0, 0,
                            0, CAST({redRem: Float64} AS Decimal64(10)), CAST({totalRem: Float64} AS Decimal64(10)),
                            0, 0, CAST({txRem: Float64} AS Decimal64(10)),
                            0, 0, 0, 0, 0.0, 0, 0.0, 0, 0
                        )
                    `;
                    await client.command({
                        query: insertTxQuery,
                        query_params: {
                            cmsId,
                            day,
                            channelId: tx.channel_id,
                            channelName: chName,
                            totalRem,
                            adRem,
                            redRem,
                            txRem
                        }
                    });
                }
            }
        }

        let auditWarning = false;
        let auditMessage = "";
        let subRevenue = 0;
        let adsRevenue = 0;

        // Self-Audit for Speed Layer
        if (log) log(`[Audit] Memulai verifikasi pasca-penyerapan (Post-Ingestion Self-Audit)...`);
        try {
            const auditRes = await client.query({
                query: `SELECT sum(owned_views) as total_views, sum(yt_rev_total) as total_rev, sum(partner_rev_red) as total_red FROM estimated_revenue_daily WHERE cms_id = {cmsId: String} AND day = toDate({day: String})`,
                query_params: { cmsId, day },
                format: 'JSONEachRow'
            });
            const auditRows = await auditRes.json() as any[];
            const ingestedViews = parseInt(auditRows[0]?.total_views || '0', 10);
            const ingestedRev = parseFloat(auditRows[0]?.total_rev || '0');
            const ingestedRed = parseFloat(auditRows[0]?.total_red || '0');

            subRevenue = ingestedRed;
            adsRevenue = Math.max(0, ingestedRev - ingestedRed);

            const rawViews = mappedRows.reduce((sum, r) => sum + r.views, 0);
            const rawTxRev = (channelTransactions || []).reduce((sum, tx) => sum + Number(tx.total_remainder !== undefined ? tx.total_remainder : (tx.transaction_revenue || 0)), 0);
            const rawRev = mappedRows.reduce((sum, r) => sum + r.estimated_revenue, 0) + rawTxRev;

            const baselineRevenue = totalCmsRevenue !== undefined && totalCmsRevenue > 0 ? totalCmsRevenue : rawRev;
            const revDiffVal = Math.abs(ingestedRev - baselineRevenue);
            const revDiffPercent = baselineRevenue > 0 ? (revDiffVal / baselineRevenue * 100) : 0;
            const viewsDiffPercent = rawViews > 0 ? Math.abs(ingestedViews - rawViews) / rawViews * 100 : 0;

            log?.(`[Audit] Total Raw Revenue Baseline: $${baselineRevenue.toFixed(2)}`);
            log?.(`[Audit] Total Ingested Revenue in ClickHouse: $${ingestedRev.toFixed(2)}`);
            if (revDiffVal > 0.01) {
                auditWarning = true;
                auditMessage = `Terdeteksi selisih pendapatan: ${revDiffPercent.toFixed(2)}% (Selisih: $${revDiffVal.toFixed(2)}, Raw: $${baselineRevenue.toFixed(2)}, Ingested: $${ingestedRev.toFixed(2)})`;
                log?.(`[Audit] ⚠️ Warning: ${auditMessage}`);
            } else {
                auditMessage = `Verifikasi sukses! Selisih pendapatan sangat minimal (Revenue Diff: ${revDiffPercent.toFixed(2)}%, Selisih: $${revDiffVal.toFixed(2)})`;
                log?.(`[Audit] 🟢 Pass: ${auditMessage}`);
            }
        } catch (auditErr: any) {
            log?.(`[Audit] ⚠️ Warning: Gagal menjalankan self-audit: ${auditErr.message}`);
        }

        if (log) log(`[Speed Layer] ✓ Data Speed Layer sukses diproses dan disimpan ke ClickHouse.`);
        
        const pendingSuffix = lowPriorityCount !== undefined && lowPriorityCount > 0 ? ` [Pending: ${lowPriorityCount} videos]` : "";

        return { 
            totalRows: mappedRows.length, 
            auditWarning, 
            auditMessage,
            pendingSuffix,
            subRevenue,
            adsRevenue
        };
    } finally {
        await client.command({ query: `DROP TABLE IF EXISTS ${tempTable}` }).catch(() => {});
    }
}

function toInt(val: any): number {
    if (val === null || val === undefined) return 0;
    const n = parseInt(String(val).replace(/,/g, ''), 10);
    return isNaN(n) ? 0 : n;
}

function toDecimal(val: any): number {
    if (val === null || val === undefined) return 0;
    const n = parseFloat(String(val).replace(/,/g, ''));
    return isNaN(n) ? 0 : n;
}

export async function processVideoCountry(opts: {
    filePath: string;
    cmsId: string;
    client: ClickHouseClient;
    batchSize?: number;
    isAborted?: () => boolean;
    jobId?: string;
}): Promise<{ totalRows: number }> {
    const { filePath, cmsId, client, isAborted, jobId = randomUUID() } = opts;

    if (isAborted?.()) throw new Error("Job aborted by user");

    const cleanJobId = jobId.replace(/-/g, '_');
    const tempTable = `temp_country_${cleanJobId}`;

    try {
        await client.command({
            query: `
                CREATE TABLE IF NOT EXISTS ${tempTable} (
                    "date"                                 String DEFAULT '',
                    "day"                                  String DEFAULT '',
                    "Day"                                  String DEFAULT '',
                    "Date"                                 String DEFAULT '',
                    "Video ID"                             String DEFAULT '',
                    "video_id"                             String DEFAULT '',
                    "Video"                                String DEFAULT '',
                    "Country Code"                         String DEFAULT '',
                    "country_code"                         String DEFAULT '',
                    "Country"                              String DEFAULT '',
                    "country"                              String DEFAULT '',
                    "Views"                                String DEFAULT '',
                    "views"                                String DEFAULT '',
                    "Watch Time"                           String DEFAULT '',
                    "Watch Time (sec)"                     String DEFAULT '',
                    "watch_time_sec"                       String DEFAULT '',
                    "Watch Time (minutes)"                 String DEFAULT '',
                    "watch_time_minutes"                   String DEFAULT '',
                    "watchTimeMinutes"                     String DEFAULT ''
                ) ENGINE = StripeLog()
            `
        });

        const stream = await extractFileStream(filePath);
        const delimiter = await detectDelimiterFromFile(filePath);

        const abortInterval = setInterval(() => {
            if (isAborted?.()) {
                stream.destroy();
                clearInterval(abortInterval);
            }
        }, 1000);

        try {
            await client.insert({
                table: tempTable,
                values: stream,
                format: delimiter === '\t' ? 'TabSeparatedWithNames' : 'CSVWithNames',
                clickhouse_settings: {
                    input_format_skip_unknown_fields: 1
                }
            });
        } finally {
            clearInterval(abortInterval);
        }

        if (isAborted?.()) throw new Error("Job aborted by user");

        const countRes = await client.query({ query: `SELECT count() as cnt FROM ${tempTable}`, format: 'JSONEachRow' });
        const countRows = await countRes.json() as any[];
        const totalRows = parseInt(countRows[0]?.cnt, 10) || 0;

        const insertQuery = `
            INSERT INTO video_countries_daily (
                cms_id, day, country, video_id, views, watch_time_sec
            )
            SELECT
                {cmsId: String},
                toDateOrZero(if(length(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), '')) = 8,
                    concat(substring(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), ''), 1, 4), '-',
                           substring(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), ''), 5, 2), '-',
                           substring(coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), ''), 7, 2)),
                    coalesce(nullIf(T.date, ''), nullIf(T.day, ''), nullIf(T.Day, ''), nullIf(T.Date, ''), '')
                )) AS day,
                coalesce(nullIf(T."Country Code", ''), nullIf(T.country_code, ''), nullIf(T.Country, ''), nullIf(T.country, ''), '') AS country,
                coalesce(nullIf(T."Video ID", ''), nullIf(T.video_id, ''), nullIf(T.Video, ''), '') AS video_id,
                sum(toInt64OrZero(replaceAll(coalesce(nullIf(T.Views, ''), nullIf(T.views, ''), '0'), ',', ''))) AS views,
                sum(if(coalesce(nullIf(T."Watch Time (sec)", ''), nullIf(T.watch_time_sec, ''), '') != '',
                       toInt64(round(toFloat64OrZero(replaceAll(coalesce(nullIf(T."Watch Time (sec)", ''), nullIf(T.watch_time_sec, ''), '0'), ',', '')))),
                       if(coalesce(nullIf(T."Watch Time (minutes)", ''), nullIf(T.watch_time_minutes, ''), nullIf(T.watchTimeMinutes, ''), '') != '',
                          toInt64(round(toFloat64OrZero(replaceAll(coalesce(nullIf(T."Watch Time (minutes)", ''), nullIf(T.watch_time_minutes, ''), nullIf(T.watchTimeMinutes, ''), '0'), ',', '')) * 60)),
                          toInt64(round(toFloat64OrZero(replaceAll(coalesce(nullIf(T."Watch Time", ''), '0'), ',', ''))))
                       )
                )) AS watch_time_sec
            FROM ${tempTable} T
            GROUP BY day, country, video_id
        `;

        await client.command({ 
            query: insertQuery,
            query_params: { cmsId }
        });

        return { totalRows };
    } finally {
        await client.command({ query: `DROP TABLE IF EXISTS ${tempTable}` }).catch(() => {});
    }
}

