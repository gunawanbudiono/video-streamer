import { createReadStream, readFileSync } from 'fs';
import { createGunzip } from 'zlib';
import { parse } from 'csv-parse';
import { pipeline } from 'stream/promises';
import { Readable, PassThrough, Transform } from 'stream';
import type { ClickHouseClient } from '@clickhouse/client';
import yauzl from 'yauzl';

// ── Types ─────────────────────────────────────────────────

export interface VideoMeta {
    video_title: string;
    video_duration_sec: number;
    username: string;
    uploader: string;
    channel_display_name: string;
    multiple_claims: string;
    category: string;
    asset_labels: string;
}

export interface AssetMeta {
    artist: string;
    asset_title: string;
    album: string;
    label: string;
    asset_labels: string;
    isrc: string;
    upc: string;
}

export interface IngestionProgress {
    totalRows: number;
    processedRows: number;
    batchesSent: number;
}

// ── File Format Detection ─────────────────────────────────

type FileFormat = 'gzip' | 'zip' | 'plain';

/** Detect file format by reading magic bytes */
function detectFormat(filePath: string): FileFormat {
    const buf = Buffer.alloc(4);
    const fd = require('fs').openSync(filePath, 'r');
    try {
        require('fs').readSync(fd, buf, 0, 4, 0);
    } finally {
        require('fs').closeSync(fd);
    }

    // GZIP magic: 1F 8B
    if (buf[0] === 0x1F && buf[1] === 0x8B) return 'gzip';
    // ZIP magic: PK (50 4B)
    if (buf[0] === 0x50 && buf[1] === 0x4B) return 'zip';
    return 'plain';
}

/** Extract a readable text stream from any supported format */
export async function extractFileStream(filePath: string): Promise<Readable> {
    const format = detectFormat(filePath);
    const fileName = filePath.split(/[/\\]/).pop() || '';
    console.log(`  [Format] ${fileName}: ${format.toUpperCase()}`);

    if (format === 'gzip') {
        const gunzip = createGunzip();
        const fileStream = createReadStream(filePath);
        fileStream.pipe(gunzip);

        // Anti File Descriptor Leak: Destroy underlying file stream when gunzip stream closes or errors
        gunzip.on('close', () => {
            fileStream.destroy();
        });
        gunzip.on('error', () => {
            fileStream.destroy();
        });

        return gunzip;
    }

    if (format === 'zip') {
        // Extract first CSV/TSV/GZ file from ZIP
        return new Promise<Readable>((resolve, reject) => {
            yauzl.open(filePath, { lazyEntries: true }, (err: any, zipfile: any) => {
                if (err) return reject(err);
                zipfile.readEntry();
                zipfile.on('entry', (entry: any) => {
                    // Skip directories and __MACOSX / hidden system files
                    const baseName = entry.fileName.split(/[/\\]/).pop() || '';
                    if (entry.fileName.endsWith('/') || baseName.startsWith('.') || entry.fileName.includes('__MACOSX')) {
                        zipfile.readEntry(); // Skip non-payload entries
                        return;
                    }

                    console.log(`  [ZIP] Extracting: ${entry.fileName}`);
                    zipfile.openReadStream(entry, (err2: any, stream: Readable) => {
                        if (err2) {
                            zipfile.close();
                            return reject(err2);
                        }

                        let payloadStream: Readable = stream;
                        if (/\.gz$/i.test(entry.fileName)) {
                            console.log(`  [ZIP -> GZIP] Decompressing inner .gz: ${entry.fileName}`);
                            const gunzip = createGunzip();
                            stream.pipe(gunzip);
                            payloadStream = gunzip;
                        }

                        payloadStream.on('end', () => zipfile.close());
                        payloadStream.on('close', () => zipfile.close());
                        payloadStream.on('error', () => zipfile.close());
                        resolve(payloadStream);
                    });
                });
                zipfile.on('end', () => {
                    zipfile.close();
                    reject(new Error('No CSV/TSV file found in ZIP archive'));
                });
                zipfile.on('error', (zipErr: any) => {
                    zipfile.close();
                    reject(zipErr);
                });
            });
        });
    }

    // Plain CSV — just read directly
    return createReadStream(filePath, { encoding: 'utf-8' });
}

// ── CSV Helpers ───────────────────────────────────────────

/** Auto-detect delimiter from first line of file */
function detectDelimiter(firstLine: string): string {
    const tabCount = (firstLine.match(/\t/g) || []).length;
    const commaCount = (firstLine.match(/,/g) || []).length;
    return tabCount > commaCount ? '\t' : ',';
}

export interface FileFormatSpec {
    delimiter: string;
    skipFirstLines: number;
}

/** Detect delimiter and skipFirstLines by checking first few lines */
export async function detectFileFormatSpec(filePath: string): Promise<FileFormatSpec> {
    const stream = await extractFileStream(filePath);
    return new Promise((resolve, reject) => {
        let data = '';
        stream.on('data', (chunk: Buffer | string) => {
            data += chunk.toString();
            const lines = data.split(/\r?\n/);
            if (lines.length >= 3) {
                stream.destroy();
                resolve(analyzeLines(lines));
            }
        });
        stream.on('error', reject);
        stream.on('end', () => {
            const lines = data.split(/\r?\n/);
            resolve(analyzeLines(lines));
        });
    });
}

const KNOWN_HEADERS = new Set([
    'date', 'day', 'country', 'channel id', 'video id', 'asset id', 
    'purchase type', 'adjustment type', 'isrc', 'upc', 'grid', 
    'views', 'partner revenue', 'earnings (usd)', 'retail price (usd)',
    'report date', 'report data'
]);

function isHeaderLine(line: string, delimiter: string): boolean {
    const fields = line.split(delimiter).map(f => f.replace(/^["']|["']$/g, '').trim().toLowerCase());
    return fields.some(f => KNOWN_HEADERS.has(f));
}

function analyzeLines(lines: string[]): FileFormatSpec {
    const line0 = lines[0] || '';
    const tabCount0 = (line0.match(/\t/g) || []).length;
    const commaCount0 = (line0.match(/,/g) || []).length;
    const delimiter0 = tabCount0 > commaCount0 ? '\t' : ',';

    if (isHeaderLine(line0, delimiter0)) {
        return {
            delimiter: delimiter0,
            skipFirstLines: 0
        };
    }

    // Otherwise, first line is a title line, check second line
    const line1 = lines[1] || '';
    const tabCount1 = (line1.match(/\t/g) || []).length;
    const commaCount1 = (line1.match(/,/g) || []).length;
    const delimiter1 = tabCount1 > commaCount1 ? '\t' : ',';
    
    if (isHeaderLine(line1, delimiter1)) {
        return {
            delimiter: delimiter1,
            skipFirstLines: 1
        };
    }

    // Fallback if neither has standard headers
    const maxDelim0 = Math.max(tabCount0, commaCount0);
    if (maxDelim0 >= 4) {
        return {
            delimiter: delimiter0,
            skipFirstLines: 0
        };
    }
    return {
        delimiter: delimiter1,
        skipFirstLines: 1
    };
}

/** Detect delimiter by reading first chunk from any format (keeps backward compatibility) */
export async function detectDelimiterFromFile(filePath: string): Promise<string> {
    const spec = await detectFileFormatSpec(filePath);
    return spec.delimiter;
}

/** Create a CSV parser stream with auto-detected delimiter (supports gz, zip, plain) */
export async function createAutoDetectCsvStream(filePath: string, fromLine: number = 1) {
    const { delimiter, skipFirstLines } = await detectFileFormatSpec(filePath);
    const actualFromLine = fromLine + skipFirstLines;
    const fileName = filePath.split(/[/\\]/).pop() || '';
    console.log(`  [CSV] Delimiter: ${delimiter === '\t' ? 'TAB' : 'COMMA'} | from_line: ${actualFromLine} | for ${fileName}`);

    const stream = await extractFileStream(filePath);

    const parser = parse({
        columns: true,
        delimiter,
        from_line: actualFromLine,
        skip_empty_lines: true,
        relax_column_count: true,
        trim: true,
        bom: true,
    });

    stream.pipe(parser);
    return parser;
}

// ── Staging Parsers ───────────────────────────────────────

export class CsvRowHelper {
    private keyMap = new Map<string, string>();

    init(row: Record<string, string>) {
        this.keyMap.clear();
        for (const key of Object.keys(row)) {
            this.keyMap.set(key.toLowerCase(), key);
        }
    }

    get(row: Record<string, string>, ...keys: string[]): string {
        if (this.keyMap.size === 0 && row) {
            this.init(row);
        }
        for (const key of keys) {
            if (row[key] !== undefined) return row[key];
            const originalKey = this.keyMap.get(key.toLowerCase());
            if (originalKey !== undefined && row[originalKey] !== undefined) {
                return row[originalKey];
            }
        }
        return '';
    }
}

/**
 * Parse Claim Summary (videoclaim) into a lookup Map by video_id.
 * Only extracts video metadata — ignores revenue/views columns.
 */
export async function parseClaimSummary(
    filePath: string
): Promise<Map<string, VideoMeta>> {
    const map = new Map<string, VideoMeta>();
    const parser = await createAutoDetectCsvStream(filePath);
    const helper = new CsvRowHelper();

    return new Promise((resolve, reject) => {
        parser.on('data', (row: Record<string, string>) => {
            const videoId = helper.get(row, 'Video ID', 'Video');
            if (!videoId || map.has(videoId)) return; // First occurrence wins

            map.set(videoId, {
                video_title: helper.get(row, 'Video Title'),
                video_duration_sec: parseInt(helper.get(row, 'Video Duration (sec)', 'Video Duration'), 10) || 0,
                username: helper.get(row, 'Username'),
                uploader: helper.get(row, 'Uploader'),
                channel_display_name: helper.get(row, 'Channel Display Name', 'Channel'),
                multiple_claims: helper.get(row, 'Multiple Claims?', 'Multiple Claims'),
                category: helper.get(row, 'Category'),
                asset_labels: '', // Pulled from Asset Summary now
            });
        });
        parser.on('end', () => {
            console.log(`  [Staging] Claim Summary: ${map.size.toLocaleString()} unique videos`);
            resolve(map);
        });
        parser.on('error', reject);
    });
}

/**
 * Parse Asset Summary into a lookup Map by ISRC.
 * Only extracts music metadata — ignores revenue/views columns.
 */
export async function parseAssetSummary(
    filePath: string
): Promise<Map<string, AssetMeta>> {
    const map = new Map<string, AssetMeta>();
    const parser = await createAutoDetectCsvStream(filePath);
    const helper = new CsvRowHelper();

    return new Promise((resolve, reject) => {
        parser.on('data', (row: Record<string, string>) => {
            const assetId = helper.get(row, 'Asset ID', 'Asset');
            if (!assetId || map.has(assetId)) return; // First occurrence wins

            map.set(assetId, {
                artist: helper.get(row, 'Artist'),
                asset_title: helper.get(row, 'Asset Title'),
                album: helper.get(row, 'Album'),
                label: helper.get(row, 'Label'),
                asset_labels: helper.get(row, 'Asset Labels', 'Asset labels', 'Asset_Labels'),
                isrc: helper.get(row, 'ISRC'),
                upc: helper.get(row, 'UPC'),
            });
        });
        parser.on('end', () => {
            console.log(`  [Staging] Asset Summary: ${map.size.toLocaleString()} unique Asset IDs`);
            resolve(map);
        });
        parser.on('error', reject);
    });
}

// ── Main ETL ──────────────────────────────────────────────

/** Parse a numeric value from CSV, defaulting to 0 */
function num(val: string | undefined): number {
    if (!val || val === '') return 0;
    const n = parseFloat(val);
    return isNaN(n) ? 0 : n;
}

/** Parse Day column (YYYYMMDD) to YYYY-MM-DD string */
function parseDay(val: string): string {
    if (val.length === 8) {
        // YYYYMMDD → YYYY-MM-DD
        return `${val.slice(0, 4)}-${val.slice(4, 6)}-${val.slice(6, 8)}`;
    }
    return val; // Already formatted or unknown
}
// ── Direct-to-ClickHouse Ingestion ──────────────────────────

export function createSkipLinesStream(linesToSkip: number): Transform {
    let skipped = 0;
    let buffer = '';
    return new Transform({
        transform(chunk, encoding, callback) {
            if (skipped >= linesToSkip) {
                this.push(chunk);
                return callback();
            }
            buffer += chunk.toString('utf-8');
            const lines = buffer.split(/\r?\n/);
            while (skipped < linesToSkip && lines.length > 1) {
                lines.shift();
                skipped++;
            }
            if (skipped >= linesToSkip) {
                const remainder = lines.join('\n');
                if (remainder.length > 0) {
                    this.push(remainder);
                }
                buffer = '';
            } else {
                buffer = lines.join('\n');
            }
            callback();
        },
        flush(callback) {
            if (skipped >= linesToSkip && buffer.length > 0) {
            } else if (buffer.length > 0) {
                this.push(buffer);
            }
            callback();
        }
    });
}

export async function loadStagingTable(
    client: ClickHouseClient,
    tableName: string,
    filePath: string,
    isAborted?: () => boolean
) {
    let stream: Readable = await extractFileStream(filePath);
    const { delimiter, skipFirstLines } = await detectFileFormatSpec(filePath);

    if (skipFirstLines > 0) {
        const skipper = createSkipLinesStream(skipFirstLines);
        stream = stream.pipe(skipper) as any;
    }

    const abortInterval = setInterval(() => {
        if (isAborted?.()) {
            stream.destroy();
            clearInterval(abortInterval);
        }
    }, 1000);

    try {
        await client.insert({
            table: tableName,
            values: stream,
            format: delimiter === '\t' ? 'TabSeparatedWithNames' : 'CSVWithNames',
            clickhouse_settings: {
                input_format_skip_unknown_fields: 1,
                input_format_csv_allow_variable_number_of_columns: 1,
                input_format_tsv_allow_variable_number_of_columns: 1,
                format_csv_delimiter: delimiter,
                input_format_tsv_crlf_end_of_line: 1
            }
        });
    } finally {
        clearInterval(abortInterval);
    }
}

function getInsertSelectQuery(
    tempClaimRawTable: string,
    tempVideoClaimTable: string,
    tempAssetSummaryTable: string,
    tempChannelMapTable: string,
    cmsId: string,
    month: string,
    reportType: string,
    usTaxRate: number
): string {
    const cleanCmsId = cmsId.replace(/-/g, '_');
    const rawChannelId = `coalesce(nullIf(c."Channel ID", ''), nullIf(c.channel_id, ''), nullIf(c."Channel", ''), '')`;
    const normChannelId = `if(${rawChannelId} = '' OR startsWith(${rawChannelId}, 'UC'), ${rawChannelId}, concat('UC', ${rawChannelId}))`;
    const rawAssetChannelId = `coalesce(nullIf(c."Asset Channel ID", ''), nullIf(c.asset_channel_id, ''), '')`;
    const normAssetChannelId = `if(${rawAssetChannelId} = '' OR startsWith(${rawAssetChannelId}, 'UC'), ${rawAssetChannelId}, concat('UC', ${rawAssetChannelId}))`;

    return `
        INSERT INTO ads_revenue_enriched (
            cms_id, upload_month, report_type, label_source, day, country, video_id, channel_id, asset_id, asset_channel_id,
            asset_type, content_type, policy, claim_type, claim_origin, custom_id, isrc, grid, upc,
            video_title, video_duration_sec, username, uploader, channel_display_name, multiple_claims, category,
            asset_labels, artist, asset_title, album, label, adjustment_type, owned_views,
            yt_rev_auction, yt_rev_reserved, yt_rev_partner_sold_yt_served, yt_rev_partner_sold_p_served, yt_rev_total,
            partner_rev_auction, partner_rev_reserved, partner_rev_partner_sold_yt_served, partner_rev_partner_sold_p_served, partner_rev_total,
            us_tax, net_revenue
        )
        SELECT
            {cmsId: String} AS cms_id,
            {uploadMonth: UInt32} AS upload_month,
            {reportType: String} AS report_type,
            'report' AS label_source,
            toDateOrZero(
                if(length(coalesce(nullIf(c."Day", ''), nullIf(c.day, ''), '')) = 8, 
                    concat(
                        substring(coalesce(nullIf(c."Day", ''), nullIf(c.day, ''), ''), 1, 4), '-', 
                        substring(coalesce(nullIf(c."Day", ''), nullIf(c.day, ''), ''), 5, 2), '-', 
                        substring(coalesce(nullIf(c."Day", ''), nullIf(c.day, ''), ''), 7, 2)
                    ), 
                    coalesce(nullIf(c."Day", ''), nullIf(c.day, ''), '')
                )
            ) AS day,
            coalesce(nullIf(c."Country", ''), nullIf(c.country, ''), '') AS country,
            coalesce(nullIf(c."Video ID", ''), nullIf(c.video_id, ''), nullIf(c."Video", ''), '') AS video_id,
            ${normChannelId} AS channel_id,
            coalesce(nullIf(c."Asset ID", ''), nullIf(c.asset_id, ''), nullIf(c."Asset", ''), '') AS asset_id,
            ${normAssetChannelId} AS asset_channel_id,
            coalesce(nullIf(c."Asset Type", ''), nullIf(c.asset_type, ''), '') AS asset_type,
            coalesce(nullIf(c."Content Type", ''), nullIf(c.content_type, ''), '') AS content_type,
            coalesce(nullIf(c."Policy", ''), nullIf(c.policy, ''), '') AS policy,
            coalesce(nullIf(c."Claim Type", ''), nullIf(c.claim_type, ''), '') AS claim_type,
            coalesce(nullIf(c."Claim Origin", ''), nullIf(c.claim_origin, ''), '') AS claim_origin,
            coalesce(nullIf(trim(c."Custom ID"), ''), nullIf(trim(c.custom_id), ''), nullIf(trim(am.custom_id), ''), nullIf(trim(g_am.custom_id), ''), '') AS custom_id,
            coalesce(nullIf(trim(am.isrc), ''), nullIf(trim(c."ISRC"), ''), nullIf(trim(c.isrc), ''), nullIf(trim(g_am.isrc), ''), '') AS isrc,
            coalesce(nullIf(trim(am.grid), ''), nullIf(trim(c."GRid"), ''), nullIf(trim(c.grid), ''), nullIf(trim(c."Grid"), ''), nullIf(trim(g_am.grid), ''), '') AS grid,
            coalesce(nullIf(trim(am.upc), ''), nullIf(trim(c."UPC"), ''), nullIf(trim(c.upc), ''), nullIf(trim(g_am.upc), ''), '') AS upc,
            coalesce(nullIf(trim(vm.video_title), ''), nullIf(trim(g_vm.video_title), ''), '') AS video_title,
            toUInt32OrZero(coalesce(nullIf(vm.video_duration_sec, ''), toString(g_vm.video_length_sec), '0')) AS video_duration_sec,
            coalesce(nullIf(vm.username, ''), '') AS username,
            coalesce(nullIf(vm.uploader, ''), '') AS uploader,
            coalesce(nullIf(trim(vm.channel_display_name), ''), nullIf(trim(vc_channel_map.vc_channel_name), ''), nullIf(trim(g_vm.channel_display_name), ''), '') AS channel_display_name,
            coalesce(nullIf(vm.multiple_claims, ''), '') AS multiple_claims,
            coalesce(nullIf(vm.category, ''), nullIf(g_vm.category, ''), '') AS category,
            coalesce(
                nullIf(trim(am.asset_labels), ''), 
                nullIf(trim(cm.mapped_name), ''), 
                nullIf(trim(g_am.asset_labels), ''),
                ''
            ) AS asset_labels,
            coalesce(nullIf(trim(am.artist), ''), nullIf(trim(g_am.artist), ''), '') AS artist,
            coalesce(nullIf(trim(am.asset_title), ''), nullIf(trim(g_am.asset_title), ''), '') AS asset_title,
            coalesce(nullIf(trim(am.album), ''), nullIf(trim(g_am.album), ''), '') AS album,
            coalesce(nullIf(trim(am.label), ''), nullIf(trim(g_am.label), ''), '') AS label,
            coalesce(nullIf(c."Adjustment Type", ''), nullIf(c.adjustment_type, ''), '') AS adjustment_type,
            toUInt64OrZero(coalesce(nullIf(c."Owned Views", ''), nullIf(c.owned_views, ''), nullIf(c.views, ''), '0')) AS owned_views,
            CAST(coalesce(nullIf(c."YouTube Revenue Split : Auction", ''), nullIf(c.youtube_revenue_split_auction, ''), '0') AS Decimal64(10)) AS yt_rev_auction,
            CAST(coalesce(nullIf(c."YouTube Revenue Split : Reserved", ''), nullIf(c.youtube_revenue_split_reserved, ''), '0') AS Decimal64(10)) AS yt_rev_reserved,
            CAST(coalesce(nullIf(c."YouTube Revenue Split : Partner Sold YouTube Served", ''), nullIf(c.youtube_revenue_split_partner_sold_youtube_served, ''), '0') AS Decimal64(10)) AS yt_rev_partner_sold_yt_served,
            CAST(coalesce(nullIf(c."YouTube Revenue Split : Partner Sold Partner Served", ''), nullIf(c.youtube_revenue_split_partner_sold_partner_served, ''), '0') AS Decimal64(10)) AS yt_rev_partner_sold_p_served,
            CAST(coalesce(nullIf(c."YouTube Revenue Split", ''), nullIf(c.youtube_revenue_split, ''), '0') AS Decimal64(10)) AS yt_rev_total,
            CAST(coalesce(nullIf(c."Partner Revenue : Auction", ''), nullIf(c.partner_revenue_auction, ''), '0') AS Decimal64(10)) AS partner_rev_auction,
            CAST(coalesce(nullIf(c."Partner Revenue : Reserved", ''), nullIf(c.partner_revenue_reserved, ''), '0') AS Decimal64(10)) AS partner_rev_reserved,
            CAST(coalesce(nullIf(c."Partner Revenue : Partner Sold YouTube Served", ''), nullIf(c.partner_revenue_partner_sold_youtube_served, ''), '0') AS Decimal64(10)) AS partner_rev_partner_sold_youtube_served,
            CAST(coalesce(nullIf(c."Partner Revenue : Partner Sold Partner Served", ''), nullIf(c.partner_revenue_partner_sold_partner_served, ''), '0') AS Decimal64(10)) AS partner_rev_partner_sold_partner_served,
            CAST(coalesce(nullIf(c."Partner Revenue", ''), nullIf(c.partner_revenue, ''), '0') AS Decimal64(10)) AS partner_rev_total,
            
             -- US Tax calculation
             CAST(if(coalesce(nullIf(c."Country", ''), nullIf(c.country, ''), '') = 'US', partner_rev_total * toDecimal64(if(tax_map.channel_id != '', tax_map.tax_rate, {usTaxRate: Float64}) / 100.0, 4), 0) AS Decimal64(10)) AS us_tax,
            CAST(partner_rev_total - us_tax AS Decimal64(10)) AS net_revenue
        FROM ${tempClaimRawTable} c
        LEFT JOIN (
            SELECT
                coalesce(nullIf("Video ID", ''), nullIf(video_id, ''), nullIf("Video", ''), '') AS video_id,
                any(coalesce(nullIf(video_title, ''), nullIf("Video Title", ''), '')) AS video_title,
                any(coalesce(nullIf(video_duration_sec, ''), nullIf("Video Duration (sec)", ''), nullIf("Video Duration", ''), '')) AS video_duration_sec,
                any(coalesce(nullIf(username, ''), nullIf("Username", ''), '')) AS username,
                any(coalesce(nullIf(uploader, ''), nullIf("Uploader", ''), '')) AS uploader,
                any(coalesce(nullIf(channel_display_name, ''), nullIf(channel, ''), nullIf("Channel Display Name", ''), nullIf("Channel", ''), '')) AS channel_display_name,
                any(coalesce(nullIf(multiple_claims, ''), nullIf("Multiple Claims?", ''), nullIf("Multiple Claims", ''), '')) AS multiple_claims,
                any(coalesce(nullIf(category, ''), nullIf("Category", ''), '')) AS category
            FROM ${tempVideoClaimTable}
            GROUP BY video_id
        ) vm ON coalesce(nullIf(c."Video ID", ''), nullIf(c.video_id, ''), nullIf(c."Video", '')) = vm.video_id
        LEFT JOIN (
            SELECT
                if(
                    coalesce(nullIf(trim("Channel ID"), ''), nullIf(trim(channel_id), ''), nullIf(trim("Channel"), ''), nullIf(trim(channel), ''), '') = '' 
                    OR startsWith(coalesce(nullIf(trim("Channel ID"), ''), nullIf(trim(channel_id), ''), nullIf(trim("Channel"), ''), nullIf(trim(channel), ''), ''), 'UC'),
                    coalesce(nullIf(trim("Channel ID"), ''), nullIf(trim(channel_id), ''), nullIf(trim("Channel"), ''), nullIf(trim(channel), ''), ''),
                    concat('UC', coalesce(nullIf(trim("Channel ID"), ''), nullIf(trim(channel_id), ''), nullIf(trim("Channel"), ''), nullIf(trim(channel), ''), ''))
                ) AS vc_channel_id,
                max(nullIf(trim(coalesce(
                    nullIf(channel_display_name, ''), nullIf("Channel Display Name", ''), 
                    nullIf(uploader, ''), nullIf("Uploader", ''), ''
                )), '')) AS vc_channel_name
            FROM ${tempVideoClaimTable}
            GROUP BY vc_channel_id
            HAVING vc_channel_id != ''
        ) vc_channel_map ON ${normChannelId} = vc_channel_map.vc_channel_id
        LEFT JOIN (
            SELECT
                video_id,
                argMax(video_title, day) AS video_title,
                argMax(channel_display_name, day) AS channel_display_name,
                argMax(category, day) AS category,
                argMax(video_length_sec, day) AS video_length_sec
            FROM db_${cleanCmsId}.youtube_video_metadata
            GROUP BY video_id
        ) g_vm ON coalesce(nullIf(c."Video ID", ''), nullIf(c.video_id, ''), nullIf(c."Video", '')) = g_vm.video_id
        LEFT JOIN (
            SELECT
                coalesce(nullIf(trim("Asset ID"), ''), nullIf(trim(asset_id), ''), nullIf(trim("Asset"), ''), '') AS asset_id,
                max(nullIf(trim(coalesce(nullIf(isrc, ''), nullIf("ISRC", ''), '')), '')) AS isrc,
                max(nullIf(trim(coalesce(nullIf(upc, ''), nullIf("UPC", ''), '')), '')) AS upc,
                max(nullIf(trim(coalesce(nullIf(custom_id, ''), nullIf("Custom ID", ''), '')), '')) AS custom_id,
                max(nullIf(trim(coalesce(nullIf(grid, ''), nullIf("GRid", ''), nullIf("Grid", ''), '')), '')) AS grid,
                max(nullIf(trim(coalesce(nullIf(asset_labels, ''), nullIf("Asset Labels", ''), nullIf("Asset labels", ''), nullIf("Asset_Labels", ''), '')), '')) AS asset_labels,
                max(nullIf(trim(coalesce(nullIf(artist, ''), nullIf("Artist", ''), '')), '')) AS artist,
                max(nullIf(trim(coalesce(nullIf(asset_title, ''), nullIf("Asset Title", ''), nullIf("Title", ''), '')), '')) AS asset_title,
                max(nullIf(trim(coalesce(nullIf(album, ''), nullIf("Album", ''), '')), '')) AS album,
                max(nullIf(trim(coalesce(nullIf(label, ''), nullIf("Label", ''), nullIf("Record Label", ''), nullIf(record_label, ''), '')), '')) AS label
            FROM ${tempAssetSummaryTable}
            GROUP BY asset_id
            HAVING asset_id != ''
        ) am ON coalesce(nullIf(c."Asset ID", ''), nullIf(c.asset_id, ''), nullIf(c."Asset", '')) = am.asset_id
        LEFT JOIN (
            SELECT
                asset_id,
                argMax(asset_title, day) AS asset_title,
                argMax(artist, day) AS artist,
                argMax(album, day) AS album,
                argMax(label, day) AS label,
                argMax(isrc, day) AS isrc,
                argMax(upc, day) AS upc,
                argMax(grid, day) AS grid,
                argMax(custom_id, day) AS custom_id,
                argMax(asset_labels, day) AS asset_labels
            FROM db_${cleanCmsId}.youtube_asset_metadata
            GROUP BY asset_id
        ) g_am ON coalesce(nullIf(c."Asset ID", ''), nullIf(c.asset_id, ''), nullIf(c."Asset", '')) = g_am.asset_id
        LEFT JOIN ${tempChannelMapTable} cm ON ${normChannelId} = cm.channel_id
        LEFT JOIN (
            SELECT channel_id, argMax(tax_rate, upload_month) as tax_rate
            FROM db_${cleanCmsId}.youtube_affiliate_tax_rates
            WHERE upload_month = {uploadMonth: UInt32} AND revenue_source LIKE '%Ads%'
            GROUP BY channel_id
        ) tax_map ON ${normChannelId} = tax_map.channel_id
    `;
}

export async function ingestAffiliateTaxTable(opts: {
    client: ClickHouseClient;
    cmsId: string;
    month: string;
    jobId: string;
    affiliateTaxPath: string;
    isAborted: () => boolean;
    log: (msg: string) => void;
}) {
    const { client, cmsId, month, jobId, affiliateTaxPath, isAborted, log } = opts;
    const cleanCmsId = cmsId.replace(/-/g, '_');
    const cleanJobId = jobId.replace(/-/g, '_');
    const tempAffTaxTable = `temp_affiliate_tax_${cleanCmsId}_${cleanJobId}`;

    log(`[Affiliate Tax] Ensuring youtube_affiliate_tax_rates table exists...`);
    await client.command({
        query: `CREATE TABLE IF NOT EXISTS db_${cleanCmsId}.youtube_affiliate_tax_rates (
            channel_id       String,
            revenue_source   String,
            tax_rate         Float64       DEFAULT 0.0,
            tax_amount       Float64       DEFAULT 0.0,
            upload_month     UInt32        DEFAULT 0
        ) ENGINE = ReplacingMergeTree()
        PRIMARY KEY (channel_id, revenue_source, upload_month)
        ORDER BY (channel_id, revenue_source, upload_month)`
    });

    log(`[Affiliate Tax] Creating affiliate tax staging table...`);
    await client.command({
        query: `CREATE TABLE IF NOT EXISTS ${tempAffTaxTable} (
            "AdSense Earnings Month" String DEFAULT '',
            "Channel ID" String DEFAULT '',
            "Revenue Source" String DEFAULT '',
            "Local Currency" String DEFAULT '',
            "Total Revenue" String DEFAULT '',
            "US Sourced Revenue" String DEFAULT '',
            "Tax Withholding Rate" String DEFAULT '',
            "Tax Withheld Amount" String DEFAULT ''
        ) ENGINE = StripeLog()`
    });

    log(`[Affiliate Tax] Loading affiliate tax data into ClickHouse staging...`);
    await loadStagingTable(client, tempAffTaxTable, affiliateTaxPath, isAborted);

    log(`[Affiliate Tax] Ingesting tax withholding rates into permanent table...`);
    await client.command({
        query: `
            INSERT INTO db_${cleanCmsId}.youtube_affiliate_tax_rates (
                channel_id, revenue_source, tax_rate, tax_amount, upload_month
            )
            SELECT
                coalesce(nullIf("Channel ID", ''), ''),
                coalesce(nullIf("Revenue Source", ''), ''),
                toFloat64OrZero(nullIf("Tax Withholding Rate", '')),
                toFloat64OrZero(nullIf("Tax Withheld Amount", '')),
                {uploadMonth: UInt32}
            FROM ${tempAffTaxTable}
            WHERE "Channel ID" != ''
        `,
        query_params: { uploadMonth: parseInt(month, 10) }
    });

    log(`[Affiliate Tax] Cleaning up affiliate tax staging table...`);
    await client.command({ query: `DROP TABLE IF EXISTS ${tempAffTaxTable}` }).catch(() => {});
}

export async function runAdsIngestionDirect(opts: {
    jobId: string;
    month: string;
    cmsId: string;
    usTaxRate: number;
    files: {
        claim_raw: string;
        videoclaim: string;
        asset_summary: string;
        adj_claim_raw?: string;
        shorts_ads?: string;
    };
    channelMap: Map<string, string>;
    client: ClickHouseClient;
    isAborted: () => boolean;
    log: (msg: string) => void;
}): Promise<{ adsRows: number; adjAdsRows: number; shortsAdsRows: number; totalAllRows: number }> {
    const { jobId, month, cmsId, files, channelMap, client, isAborted, log, usTaxRate } = opts;

    const cleanCmsId = cmsId.replace(/-/g, '_');
    const cleanJobId = jobId.replace(/-/g, '_');
    const tempClaimRawTable = `temp_monthly_claim_raw_${cleanCmsId}_${cleanJobId}`;
    const tempVideoClaimTable = `temp_monthly_videoclaim_${cleanCmsId}_${cleanJobId}`;
    const tempAssetSummaryTable = `temp_monthly_asset_summary_${cleanCmsId}_${cleanJobId}`;
    const tempChannelMapTable = `temp_monthly_channel_map_${cleanCmsId}_${cleanJobId}`;
    const tempShortsAdsTable = `temp_monthly_shorts_ads_${cleanCmsId}_${cleanJobId}`;

    try {
        if (isAborted()) throw new Error("Job aborted by user");

        // Ensure youtube_affiliate_tax_rates table exists
        await client.command({
            query: `CREATE TABLE IF NOT EXISTS db_${cleanCmsId}.youtube_affiliate_tax_rates (
                channel_id       String,
                revenue_source   String,
                tax_rate         Float64       DEFAULT 0.0,
                tax_amount       Float64       DEFAULT 0.0,
                upload_month     UInt32        DEFAULT 0
            ) ENGINE = ReplacingMergeTree()
            PRIMARY KEY (channel_id, revenue_source, upload_month)
            ORDER BY (channel_id, revenue_source, upload_month)`
        });

        // 1. Create Staging Tables
        log(`[Step 2/5] Creating ClickHouse staging tables...`);
        await client.command({
            query: `CREATE TABLE IF NOT EXISTS ${tempClaimRawTable} (
                "Day" String DEFAULT '',
                "day" String DEFAULT '',
                "Country" String DEFAULT '',
                "country" String DEFAULT '',
                "Video ID" String DEFAULT '',
                "video_id" String DEFAULT '',
                "Video" String DEFAULT '',
                "Channel ID" String DEFAULT '',
                "channel_id" String DEFAULT '',
                "Channel" String DEFAULT '',
                "Asset ID" String DEFAULT '',
                "asset_id" String DEFAULT '',
                "Asset" String DEFAULT '',
                "Asset Channel ID" String DEFAULT '',
                "asset_channel_id" String DEFAULT '',
                "Asset Type" String DEFAULT '',
                "asset_type" String DEFAULT '',
                "Content Type" String DEFAULT '',
                "content_type" String DEFAULT '',
                "Policy" String DEFAULT '',
                "policy" String DEFAULT '',
                "Claim Type" String DEFAULT '',
                "claim_type" String DEFAULT '',
                "Claim Origin" String DEFAULT '',
                "claim_origin" String DEFAULT '',
                "Custom ID" String DEFAULT '',
                "custom_id" String DEFAULT '',
                "ISRC" String DEFAULT '',
                "isrc" String DEFAULT '',
                "GRid" String DEFAULT '',
                "grid" String DEFAULT '',
                "Grid" String DEFAULT '',
                "UPC" String DEFAULT '',
                "upc" String DEFAULT '',
                "Owned Views" String DEFAULT '',
                "owned_views" String DEFAULT '',
                "views" String DEFAULT '',
                "YouTube Revenue Split : Auction" String DEFAULT '',
                "YouTube Revenue Split : Reserved" String DEFAULT '',
                "YouTube Revenue Split : Partner Sold YouTube Served" String DEFAULT '',
                "YouTube Revenue Split : Partner Sold Partner Served" String DEFAULT '',
                "YouTube Revenue Split" String DEFAULT '',
                "Partner Revenue : Auction" String DEFAULT '',
                "Partner Revenue : Reserved" String DEFAULT '',
                "Partner Revenue : Partner Sold YouTube Served" String DEFAULT '',
                "Partner Revenue : Partner Sold Partner Served" String DEFAULT '',
                "Partner Revenue" String DEFAULT '',
                "partner_revenue_auction" String DEFAULT '',
                "partner_revenue_reserved" String DEFAULT '',
                "partner_revenue_partner_sold_youtube_served" String DEFAULT '',
                "partner_revenue_partner_sold_partner_served" String DEFAULT '',
                "partner_revenue" String DEFAULT '',
                "youtube_revenue_split_auction" String DEFAULT '',
                "youtube_revenue_split_reserved" String DEFAULT '',
                "youtube_revenue_split_partner_sold_youtube_served" String DEFAULT '',
                "youtube_revenue_split_partner_sold_partner_served" String DEFAULT '',
                "youtube_revenue_split" String DEFAULT '',
                "Adjustment Type" String DEFAULT '',
                "adjustment_type" String DEFAULT ''
            ) ENGINE = StripeLog()`
        });

        await client.command({
            query: `CREATE TABLE IF NOT EXISTS ${tempVideoClaimTable} (
                "Video ID" String DEFAULT '',
                "video_id" String DEFAULT '',
                "Video" String DEFAULT '',
                "Video Title" String DEFAULT '',
                "video_title" String DEFAULT '',
                "Video Duration" String DEFAULT '',
                "video_duration_sec" String DEFAULT '',
                "Video Duration (sec)" String DEFAULT '',
                "Username" String DEFAULT '',
                "username" String DEFAULT '',
                "Uploader" String DEFAULT '',
                "uploader" String DEFAULT '',
                "Channel Display Name" String DEFAULT '',
                "channel_display_name" String DEFAULT '',
                "Channel ID" String DEFAULT '',
                "channel_id" String DEFAULT '',
                "Channel" String DEFAULT '',
                "channel" String DEFAULT '',
                "Multiple Claims?" String DEFAULT '',
                "Multiple Claims" String DEFAULT '',
                "multiple_claims" String DEFAULT '',
                "Category" String DEFAULT '',
                "category" String DEFAULT ''
            ) ENGINE = StripeLog()`
        });

        await client.command({
            query: `CREATE TABLE IF NOT EXISTS ${tempAssetSummaryTable} (
                "Asset ID" String DEFAULT '',
                "asset_id" String DEFAULT '',
                "Asset" String DEFAULT '',
                "Artist" String DEFAULT '',
                "artist" String DEFAULT '',
                "Asset Title" String DEFAULT '',
                "asset_title" String DEFAULT '',
                "Title" String DEFAULT '',
                "Album" String DEFAULT '',
                "album" String DEFAULT '',
                "Label" String DEFAULT '',
                "label" String DEFAULT '',
                "Record Label" String DEFAULT '',
                "record_label" String DEFAULT '',
                "Asset Labels" String DEFAULT '',
                "Asset labels" String DEFAULT '',
                "asset_labels" String DEFAULT '',
                "Asset_Labels" String DEFAULT '',
                "Custom ID" String DEFAULT '',
                "custom_id" String DEFAULT '',
                "GRid" String DEFAULT '',
                "grid" String DEFAULT '',
                "Grid" String DEFAULT '',
                "ISRC" String DEFAULT '',
                "isrc" String DEFAULT '',
                "UPC" String DEFAULT '',
                "upc" String DEFAULT ''
            ) ENGINE = StripeLog()`
        });

        await client.command({
            query: `CREATE TABLE IF NOT EXISTS ${tempChannelMapTable} (
                channel_id String,
                mapped_name String
            ) ENGINE = StripeLog()`
        });

        if (isAborted()) throw new Error("Job aborted by user");

        // 2. Load Staging Tables
        log(`[Step 2/5] Loading Video Claim metadata into ClickHouse...`);
        await loadStagingTable(client, tempVideoClaimTable, files.videoclaim, isAborted);

        // Persist Video Claim reference metadata into persistent youtube_raw_claims table
        await client.command({
            query: `
                INSERT INTO youtube_raw_claims (
                    cms_id, day, video_id, channel_id, video_title, channel_display_name,
                    uploader, username, category, video_duration_sec, multiple_claims, ingested_at
                )
                SELECT
                    {cmsId: String} AS cms_id,
                    toDateOrZero(concat(substring(toString({uploadMonth: UInt32}), 1, 4), '-', substring(toString({uploadMonth: UInt32}), 5, 2), '-01')) AS day,
                    "Video ID" AS video_id,
                    if(
                        coalesce(nullIf(trim("Channel ID"), ''), nullIf(trim(channel_id), ''), nullIf(trim("Channel"), ''), nullIf(trim(channel), ''), '') = '' 
                        OR startsWith(coalesce(nullIf(trim("Channel ID"), ''), nullIf(trim(channel_id), ''), nullIf(trim("Channel"), ''), nullIf(trim(channel), ''), ''), 'UC'),
                        coalesce(nullIf(trim("Channel ID"), ''), nullIf(trim(channel_id), ''), nullIf(trim("Channel"), ''), nullIf(trim(channel), ''), ''),
                        concat('UC', coalesce(nullIf(trim("Channel ID"), ''), nullIf(trim(channel_id), ''), nullIf(trim("Channel"), ''), nullIf(trim(channel), ''), ''))
                    ) AS channel_id,
                    "Video Title" AS video_title,
                    "Channel Display Name" AS channel_display_name,
                    "Uploader" AS uploader,
                    "Username" AS username,
                    "Category" AS category,
                    toUInt32OrZero("Video Duration (sec)") AS video_duration_sec,
                    "Multiple Claims" AS multiple_claims,
                    now() AS ingested_at
                FROM ${tempVideoClaimTable}
                WHERE "Video ID" != ''
            `,
            query_params: { cmsId, uploadMonth: parseInt(month, 10) }
        }).catch((err: any) => {
            log(`[VideoClaim Ingestion Warning] ${err.message}`);
        });

        log(`[Step 2/5] Loading Asset Summary metadata into ClickHouse...`);
        await loadStagingTable(client, tempAssetSummaryTable, files.asset_summary, isAborted);

        log(`[Step 2/5] Loading Channel Mappings into ClickHouse...`);
        const channelMapRows = Array.from(channelMap.entries()).map(([channelId, mappedName]) => ({
            channel_id: channelId,
            mapped_name: mappedName
        }));
        if (channelMapRows.length > 0) {
            await client.insert({
                table: tempChannelMapTable,
                values: channelMapRows,
                format: 'JSONEachRow'
            });
        }

        if (isAborted()) throw new Error("Job aborted by user");

        // 3. Load Claim Raw File
        log(`[Step 3/5] Loading Claim Raw into ClickHouse staging...`);
        await loadStagingTable(client, tempClaimRawTable, files.claim_raw, isAborted);

        // 4. Query total rows in staging to report progress
        const countRes = await client.query({
            query: `SELECT count() as cnt FROM ${tempClaimRawTable}`,
            format: 'JSONEachRow'
        });
        const countRows = await countRes.json() as any[];
        const adsRows = countRows[0]?.cnt ? parseInt(countRows[0].cnt, 10) : 0;
        log(`[Step 3/5] Ingesting ${adsRows.toLocaleString()} rows using ClickHouse SQL Join...`);

        // 5. Execute INSERT SELECT for Ads
        const insertQuery = getInsertSelectQuery(tempClaimRawTable, tempVideoClaimTable, tempAssetSummaryTable, tempChannelMapTable, cmsId, month, 'claim_raw', usTaxRate);
        await client.command({
            query: insertQuery,
            query_params: { cmsId, uploadMonth: parseInt(month, 10), reportType: 'claim_raw', usTaxRate }
        });

        let adjAdsRows = 0;
        if (files.adj_claim_raw) {
            if (isAborted()) throw new Error("Job aborted by user");
            log(`[Step 4/5] Loading Ads Adjustments into ClickHouse staging...`);
            // Clear temp_claim_raw first
            await client.command({ query: `TRUNCATE TABLE ${tempClaimRawTable}` });
            await loadStagingTable(client, tempClaimRawTable, files.adj_claim_raw, isAborted);

            const countAdjRes = await client.query({
                query: `SELECT count() as cnt FROM ${tempClaimRawTable}`,
                format: 'JSONEachRow'
            });
            const countAdjRows = await countAdjRes.json() as any[];
            adjAdsRows = countAdjRows[0]?.cnt ? parseInt(countAdjRows[0].cnt, 10) : 0;
            log(`[Step 4/5] Ingesting ${adjAdsRows.toLocaleString()} adjustment rows using ClickHouse SQL Join...`);

            const insertAdjQuery = getInsertSelectQuery(tempClaimRawTable, tempVideoClaimTable, tempAssetSummaryTable, tempChannelMapTable, cmsId, month, 'ads_adjustment', usTaxRate);
            await client.command({
                query: insertAdjQuery,
                query_params: { cmsId, uploadMonth: parseInt(month, 10), reportType: 'ads_adjustment', usTaxRate }
            });
        }

        let shortsAdsRows = 0;
        if (files.shorts_ads) {
            if (isAborted()) throw new Error("Job aborted by user");
            log(`[Shorts Ads] Creating ClickHouse staging table for Shorts Ads...`);
            await client.command({
                query: `CREATE TABLE IF NOT EXISTS ${tempShortsAdsTable} (
                    "Adjustment Type" String DEFAULT '',
                    "Video ID" String DEFAULT '',
                    "Video Title" String DEFAULT '',
                    "Video Duration (sec)" String DEFAULT '',
                    "Category" String DEFAULT '',
                    "Channel ID" String DEFAULT '',
                    "Uploader" String DEFAULT '',
                    "Content Type" String DEFAULT '',
                    "Policy" String DEFAULT '',
                    "Engaged Views" String DEFAULT '',
                    "Net Partner Revenue (Post revshare)" String DEFAULT ''
                ) ENGINE = StripeLog()`
            });

            log(`[Shorts Ads] Loading Shorts Ads into ClickHouse staging...`);
            await loadStagingTable(client, tempShortsAdsTable, files.shorts_ads, isAborted);

            const countShortsRes = await client.query({
                query: `SELECT count() as cnt FROM ${tempShortsAdsTable}`,
                format: 'JSONEachRow'
            });
            const countShortsRows = await countShortsRes.json() as any[];
            shortsAdsRows = countShortsRows[0]?.cnt ? parseInt(countShortsRows[0].cnt, 10) : 0;
            log(`[Shorts Ads] Ingesting ${shortsAdsRows.toLocaleString()} shorts ads rows into ClickHouse...`);

            await client.command({
                query: `
                    INSERT INTO ads_revenue_enriched (
                        cms_id, upload_month, report_type, day, country,
                        video_id, channel_id, asset_id, asset_type, content_type,
                        policy, claim_type, custom_id, video_title, video_duration_sec,
                        uploader, channel_display_name, category, owned_views,
                        yt_rev_total, partner_rev_total, us_tax, net_revenue
                    )
                    SELECT
                        {cmsId: String} AS cms_id,
                        {uploadMonth: UInt32} AS upload_month,
                        'shorts_ads' AS report_type,
                        toDateOrZero(concat(substring(toString({uploadMonth: UInt32}), 1, 4), '-', substring(toString({uploadMonth: UInt32}), 5, 2), '-01')) AS day,
                        'ZZ' AS country,
                        s."Video ID" AS video_id,
                        s."Channel ID" AS channel_id,
                        '' AS asset_id,
                        'Video' AS asset_type,
                        s."Content Type" AS content_type,
                        s."Policy" AS policy,
                        'Claim' AS claim_type,
                        '' AS custom_id,
                        s."Video Title" AS video_title,
                        toUInt32OrZero(s."Video Duration (sec)") AS video_duration_sec,
                        s."Uploader" AS uploader,
                        s."Uploader" AS channel_display_name,
                        s."Category" AS category,
                        toUInt64OrZero(s."Engaged Views") AS owned_views,
                        CAST(s."Net Partner Revenue (Post revshare)" AS Decimal64(10)) AS yt_rev_total,
                        CAST(s."Net Partner Revenue (Post revshare)" AS Decimal64(10)) AS partner_rev_total,
                        CAST(if('ZZ' = 'US', partner_rev_total * toDecimal64({usTaxRate: Float64} / 100.0, 4), 0) AS Decimal64(10)) AS us_tax,
                        CAST(partner_rev_total - us_tax AS Decimal64(10)) AS net_revenue
                    FROM ${tempShortsAdsTable} s
                `,
                query_params: { cmsId, uploadMonth: parseInt(month, 10), usTaxRate }
            });
        }

        const totalAllRows = adsRows + adjAdsRows + shortsAdsRows;
        return { adsRows, adjAdsRows, shortsAdsRows, totalAllRows };

    } finally {
        // Cleanup staging tables
        log(`[Step 5/5] Cleaning up staging tables...`);
        await client.command({ query: `DROP TABLE IF EXISTS ${tempClaimRawTable}` }).catch(() => {});
        await client.command({ query: `DROP TABLE IF EXISTS ${tempVideoClaimTable}` }).catch(() => {});
        await client.command({ query: `DROP TABLE IF EXISTS ${tempAssetSummaryTable}` }).catch(() => {});
        await client.command({ query: `DROP TABLE IF EXISTS ${tempChannelMapTable}` }).catch(() => {});
        await client.command({ query: `DROP TABLE IF EXISTS ${tempShortsAdsTable}` }).catch(() => {});
    }
}
