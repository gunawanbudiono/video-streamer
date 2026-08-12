import { ClickHouseClient } from '@clickhouse/client';
import { extractFileStream, loadStagingTable } from './etl-ads.js';

function getPaidInsertSelectQuery(
    tempPaidTable: string,
    cmsId: string,
    month: string,
    usTaxRate: number
): string {
    const cleanCmsId = cmsId.replace(/-/g, '_');
    const rawChannelId = `coalesce(nullIf(p."Channel ID", ''), nullIf(p.channel_id, ''), '')`;
    const normChannelId = `if(${rawChannelId} = '' OR startsWith(${rawChannelId}, 'UC'), ${rawChannelId}, concat('UC', ${rawChannelId}))`;

    return `
        INSERT INTO paid_features_raw (
            cms_id, upload_month, day, purchase_type, refund_chargeback, country, channel_display_name, channel_name, channel_id, video_id,
            retail_price_usd, total_tax_usd, partner_earnings_fraction, earnings_usd,
            us_tax, net_revenue
        )
        SELECT
            {cmsId: String} AS cms_id,
            {uploadMonth: UInt32} AS upload_month,
            toDateOrZero(
                if(like(coalesce(nullIf(p."Date", ''), nullIf(p.date, ''), ''), '%/%/%'),
                    concat(
                        substring(coalesce(nullIf(p."Date", ''), nullIf(p.date, ''), ''), 7, 4), '-',
                        substring(coalesce(nullIf(p."Date", ''), nullIf(p.date, ''), ''), 4, 2), '-',
                        substring(coalesce(nullIf(p."Date", ''), nullIf(p.date, ''), ''), 1, 2)
                    ),
                    coalesce(nullIf(p."Date", ''), nullIf(p.date, ''), '')
                )
            ) AS day,
            coalesce(nullIf(p."Purchase Type", ''), nullIf(p.purchase_type, ''), '') AS purchase_type,
            if(
                lower(coalesce(nullIf(p."Refund/Chargeback", ''), nullIf(p.refund_chargeback, ''), 'false')) = 'true' 
                OR coalesce(nullIf(p."Refund/Chargeback", ''), nullIf(p.refund_chargeback, ''), '0') = '1',
                1, 0
            ) AS refund_chargeback,
            coalesce(nullIf(p."Country", ''), nullIf(p.country, ''), '') AS country,
            coalesce(nullIf(p."Channel Name", ''), nullIf(p.channel_name, ''), '') AS channel_display_name,
            coalesce(nullIf(p."Channel Name", ''), nullIf(p.channel_name, ''), '') AS channel_name,
            ${normChannelId} AS channel_id,
            coalesce(nullIf(p."Video ID", ''), nullIf(p.video_id, ''), '') AS video_id,
            toFloat64OrZero(coalesce(nullIf(p."Retail Price (USD)", ''), nullIf(p.retail_price_usd, ''), '0')) AS retail_price_usd,
            toFloat64OrZero(coalesce(nullIf(p."Total Tax (USD)", ''), nullIf(p.total_tax_usd, ''), '0')) AS total_tax_usd,
            toFloat64OrZero(coalesce(nullIf(p."Partner Earnings Fraction", ''), nullIf(p.partner_earnings_fraction, ''), '0')) AS partner_earnings_fraction,
            toFloat64OrZero(coalesce(nullIf(p."Earnings (USD)", ''), nullIf(p.earnings_usd, ''), '0')) AS earnings_usd,
            
            -- US Tax Calculation (Only for US country transactions, checking channel tax rates first)
            CAST(if(coalesce(nullIf(p."Country", ''), nullIf(p.country, ''), '') = 'US', earnings_usd * (if(tax_map.channel_id != '', tax_map.tax_rate, {usTaxRate: Float64}) / 100.0), 0) AS Decimal64(10)) AS us_tax,
            CAST(earnings_usd - us_tax AS Decimal64(10)) AS net_revenue
        FROM ${tempPaidTable} p
        LEFT JOIN (
            SELECT channel_id, argMax(tax_rate, upload_month) as tax_rate
            FROM db_${cleanCmsId}.youtube_affiliate_tax_rates
            WHERE upload_month = {uploadMonth: UInt32} AND (revenue_source LIKE '%Paid Features%' OR revenue_source LIKE '%Transaction%')
            GROUP BY channel_id
        ) tax_map ON ${normChannelId} = tax_map.channel_id
    `;
}

export async function runPaidFeaturesIngestionDirect(opts: {
    jobId: string;
    month: string;
    cmsId: string;
    filePath: string;
    usTaxRate: number;
    client: ClickHouseClient;
    isAborted: () => boolean;
    log: (msg: string) => void;
}): Promise<{ paidRows: number; paidRevenue: number; usTax: number; netRevenue: number }> {
    const { jobId, month, cmsId, filePath, usTaxRate, client, isAborted, log } = opts;

    const cleanCmsId = cmsId.replace(/-/g, '_');
    const cleanJobId = jobId.replace(/-/g, '_');
    const tempPaidTable = `temp_monthly_paid_${cleanCmsId}_${cleanJobId}`;

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

        // 1. Create Staging Table
        log(`[Paid Features] Creating ClickHouse staging tables...`);
        await client.command({
            query: `CREATE TABLE IF NOT EXISTS ${tempPaidTable} (
                "Date" String DEFAULT '',
                "date" String DEFAULT '',
                "Purchase Type" String DEFAULT '',
                "purchase_type" String DEFAULT '',
                "Refund/Chargeback" String DEFAULT '',
                "refund_chargeback" String DEFAULT '',
                "Country" String DEFAULT '',
                "country" String DEFAULT '',
                "Channel Name" String DEFAULT '',
                "channel_name" String DEFAULT '',
                "Channel ID" String DEFAULT '',
                "channel_id" String DEFAULT '',
                "Video ID" String DEFAULT '',
                "video_id" String DEFAULT '',
                "Status Change" String DEFAULT '',
                "status_change" String DEFAULT '',
                "Retail Price (USD)" String DEFAULT '',
                "retail_price_usd" String DEFAULT '',
                "Total Tax (USD)" String DEFAULT '',
                "total_tax_usd" String DEFAULT '',
                "Partner Earnings Fraction" String DEFAULT '',
                "partner_earnings_fraction" String DEFAULT '',
                "Earnings (USD)" String DEFAULT '',
                "earnings_usd" String DEFAULT ''
            ) ENGINE = StripeLog()`
        });

        if (isAborted()) throw new Error("Job aborted by user");

        // 2. Load CSV File into Staging
        log(`[Paid Features] Loading CSV file stream to staging...`);
        await loadStagingTable(client, tempPaidTable, filePath, isAborted);

        if (isAborted()) throw new Error("Job aborted by user");

        // 3. Drop partition on destination table for idempotency
        const uMonth = parseInt(month.replace(/-/g, ''));
        log(`[Paid Features] Cleaning existing partition ${uMonth}...`);
        await client.command({
            query: `ALTER TABLE db_${cleanCmsId}.paid_features_raw DROP PARTITION ({cmsId: String}, {partitionMonth: UInt32})`,
            query_params: { cmsId, partitionMonth: uMonth }
        });

        // 4. INSERT INTO final table
        log(`[Paid Features] Writing raw data to paid_features_raw...`);
        const insertQuery = getPaidInsertSelectQuery(tempPaidTable, cmsId, month, usTaxRate);
        await client.command({
            query: insertQuery,
            query_params: {
                cmsId,
                uploadMonth: uMonth,
                usTaxRate
            }
        });

        // 5. Aggregate metrics
        log(`[Paid Features] Aggregating results...`);
        const metricsRes = await client.query({
            query: `SELECT 
                count() as rows_cnt, 
                sum(earnings_usd) as total_rev,
                sum(us_tax) as tax_total,
                sum(net_revenue) as net_total
            FROM db_${cleanCmsId}.paid_features_raw 
            WHERE upload_month = {uploadMonth: UInt32}`,
            query_params: { uploadMonth: uMonth },
            format: 'JSONEachRow'
        });
        const metrics = await metricsRes.json<{ rows_cnt: string; total_rev: string; tax_total: string; net_total: string }>();
        const paidRows = parseInt(metrics[0]?.rows_cnt || '0');
        const paidRevenue = parseFloat(metrics[0]?.total_rev || '0');
        const usTax = parseFloat(metrics[0]?.tax_total || '0');
        const netRevenue = parseFloat(metrics[0]?.net_total || '0');

        log(`[Paid Features] Finished: processed ${paidRows} rows, Gross Revenue: $${paidRevenue.toFixed(2)} USD, US Tax: $${usTax.toFixed(2)} USD`);

        return { paidRows, paidRevenue, usTax, netRevenue };
    } finally {
        // Clean staging table
        await client.command({
            query: `DROP TABLE IF EXISTS ${tempPaidTable}`
        }).catch(err => console.error(`[Paid Features] Cleanup failed: ${err.message}`));
    }
}
