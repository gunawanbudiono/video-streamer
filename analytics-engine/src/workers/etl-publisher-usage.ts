import { ClickHouseClient } from '@clickhouse/client';
import { loadStagingTable } from './etl-ads.js';

function getPublisherUsageInsertQuery(
    tempTable: string,
    cmsId: string,
    month: string,
    defaultOffer: string = ''
): string {
    return `
        INSERT INTO raw_youtube_publisher_usage_reports (
            cms_id, payment_month, offer, country, claiming_asset_id, title, artist, isrc,
            claiming_asset_type, composition_asset_id, custom_id, composition_title, iswc,
            writers, content_category, copyright_type, ownership_percentage, views,
            youtube_revenue_split, partner_revenue
        )
        SELECT
            {cmsId: String} AS cms_id,
            {uploadMonth: UInt32} AS payment_month,
            coalesce(nullIf(s."Offer", ''), nullIf(s.offer, ''), {defaultOffer: String}) AS offer,
            coalesce(nullIf(s."Country", ''), nullIf(s.country, ''), '') AS country,
            coalesce(nullIf(s."Claiming Asset ID", ''), nullIf(s.claiming_asset_id, ''), nullIf(s."Art Track Asset ID", ''), '') AS claiming_asset_id,
            coalesce(nullIf(s."Title", ''), nullIf(s.title, ''), nullIf(s."Asset Title", ''), '') AS title,
            coalesce(nullIf(s."Artist", ''), nullIf(s.artist, ''), '') AS artist,
            coalesce(nullIf(s."ISRC", ''), nullIf(s.isrc, ''), '') AS isrc,
            coalesce(nullIf(s."Claiming Asset Type", ''), nullIf(s.claiming_asset_type, ''), 'Art Track') AS claiming_asset_type,
            coalesce(nullIf(s."Composition Asset ID", ''), nullIf(s.composition_asset_id, ''), '') AS composition_asset_id,
            coalesce(nullIf(s."Custom ID", ''), nullIf(s.custom_id, ''), '') AS custom_id,
            coalesce(nullIf(s."Composition Title", ''), nullIf(s.composition_title, ''), nullIf(s."Composition Asset Title", ''), '') AS composition_title,
            coalesce(nullIf(s."ISWC", ''), nullIf(s.iswc, ''), '') AS iswc,
            coalesce(nullIf(s."Writers", ''), nullIf(s.writers, ''), '') AS writers,
            coalesce(nullIf(s."Content Category", ''), nullIf(s.content_category, ''), 'GEMusic') AS content_category,
            coalesce(nullIf(s."Copyright Type", ''), nullIf(s.copyright_type, ''), nullIf(s."Composition Right Type", ''), 'Mechanical') AS copyright_type,
            toFloat64OrZero(coalesce(nullIf(s."Ownership Percentage", ''), nullIf(s.ownership_percentage, ''), '100')) AS ownership_percentage,
            toUInt64OrZero(coalesce(nullIf(s."Views", ''), nullIf(s.views, ''), '0')) AS views,
            CAST(coalesce(nullIf(s."YouTube Revenue Split", ''), nullIf(s.youtube_revenue_split, ''), '0') AS Decimal64(10)) AS youtube_revenue_split,
            CAST(coalesce(nullIf(s."Partner Revenue", ''), nullIf(s.partner_revenue, ''), '0') AS Decimal64(10)) AS partner_revenue
        FROM ${tempTable} s
    `;
}

async function createPublisherUsageStagingTable(client: ClickHouseClient, tableName: string) {
    await client.command({
        query: `CREATE TABLE IF NOT EXISTS ${tableName} (
            "Offer" String DEFAULT '',
            "offer" String DEFAULT '',
            "Country" String DEFAULT '',
            "country" String DEFAULT '',
            "Claiming Asset ID" String DEFAULT '',
            "claiming_asset_id" String DEFAULT '',
            "Art Track Asset ID" String DEFAULT '',
            "art_track_asset_id" String DEFAULT '',
            "Title" String DEFAULT '',
            "title" String DEFAULT '',
            "Asset Title" String DEFAULT '',
            "asset_title" String DEFAULT '',
            "Artist" String DEFAULT '',
            "artist" String DEFAULT '',
            "ISRC" String DEFAULT '',
            "isrc" String DEFAULT '',
            "Claiming Asset Type" String DEFAULT '',
            "claiming_asset_type" String DEFAULT '',
            "Composition Asset ID" String DEFAULT '',
            "composition_asset_id" String DEFAULT '',
            "Custom ID" String DEFAULT '',
            "custom_id" String DEFAULT '',
            "Composition Title" String DEFAULT '',
            "composition_title" String DEFAULT '',
            "Composition Asset Title" String DEFAULT '',
            "composition_asset_title" String DEFAULT '',
            "ISWC" String DEFAULT '',
            "iswc" String DEFAULT '',
            "Writers" String DEFAULT '',
            "writers" String DEFAULT '',
            "Content Category" String DEFAULT '',
            "content_category" String DEFAULT '',
            "Copyright Type" String DEFAULT '',
            "copyright_type" String DEFAULT '',
            "Composition Right Type" String DEFAULT '',
            "composition_right_type" String DEFAULT '',
            "Ownership Percentage" String DEFAULT '',
            "ownership_percentage" String DEFAULT '',
            "Views" String DEFAULT '',
            "views" String DEFAULT '',
            "YouTube Revenue Split" String DEFAULT '',
            "youtube_revenue_split" String DEFAULT '',
            "Partner Revenue" String DEFAULT '',
            "partner_revenue" String DEFAULT ''
        ) ENGINE = StripeLog()`
    });
}

async function ensurePublisherUsageTablesExist(client: ClickHouseClient) {
    await client.command({
        query: `CREATE TABLE IF NOT EXISTS raw_youtube_publisher_usage_reports (
            cms_id                String,
            payment_month         UInt32,
            offer                 LowCardinality(String) DEFAULT '',
            country               LowCardinality(String) DEFAULT '',
            claiming_asset_id     String DEFAULT '',
            title                 String DEFAULT '',
            artist                String DEFAULT '',
            isrc                  String DEFAULT '',
            claiming_asset_type   LowCardinality(String) DEFAULT '',
            composition_asset_id  String DEFAULT '',
            custom_id             String DEFAULT '',
            composition_title     String DEFAULT '',
            iswc                  String DEFAULT '',
            writers               String DEFAULT '',
            content_category      LowCardinality(String) DEFAULT '',
            copyright_type        LowCardinality(String) DEFAULT '',
            ownership_percentage  Float64 DEFAULT 100,
            views                 UInt64 DEFAULT 0,
            youtube_revenue_split Decimal64(10) DEFAULT 0,
            partner_revenue       Decimal64(10) DEFAULT 0,
            ingested_at           DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY (cms_id, payment_month)
        ORDER BY (cms_id, payment_month, offer, country, custom_id, isrc)
        SETTINGS index_granularity = 8192`
    });

    await client.command({
        query: `CREATE TABLE IF NOT EXISTS youtube_publisher_work_metadata (
            cms_id               String DEFAULT '',
            custom_id            String DEFAULT '',
            iswc                 String DEFAULT '',
            composition_asset_id String DEFAULT '',
            composition_title    String DEFAULT '',
            writers              String DEFAULT '',
            copyright_type       String DEFAULT '',
            updated_at           DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (cms_id, custom_id)`
    });
}

export async function runPublisherUsageIngestionDirect(opts: {
    jobId: string;
    month: string;
    cmsId: string;
    files: {
        subscription_usage?: string;
        adsupport_usage?: string;
        hardware_audio_tier?: string;
    };
    client: ClickHouseClient;
    isAborted?: () => boolean;
    log: (msg: string) => void;
}) {
    const { jobId, month, cmsId, files, client, isAborted, log } = opts;
    const cleanCmsId = cmsId.replace(/-/g, '_');
    const cleanJobId = jobId.replace(/-/g, '_');

    let totalRows = 0;

    await ensurePublisherUsageTablesExist(client);

    // Process Subscription Usage
    if (files.subscription_usage) {
        log(`[Publisher Usage] Loading Subscription Usage into staging...`);
        const tempTable = `temp_sub_usage_${cleanCmsId}_${cleanJobId}`;
        await createPublisherUsageStagingTable(client, tempTable);
        await loadStagingTable(client, tempTable, files.subscription_usage, isAborted);

        const insertQuery = getPublisherUsageInsertQuery(tempTable, cmsId, month, 'YouTube Premium');
        await client.command({
            query: insertQuery,
            query_params: { cmsId, uploadMonth: parseInt(month, 10), defaultOffer: 'YouTube Premium' }
        });
        
        // Populate work metadata lookup table
        await client.command({
            query: `
                INSERT INTO youtube_publisher_work_metadata (cms_id, custom_id, iswc, composition_asset_id, composition_title, writers, copyright_type, updated_at)
                SELECT DISTINCT
                    {cmsId: String} AS cms_id,
                    coalesce(nullIf(s."Custom ID", ''), nullIf(s.custom_id, ''), '') AS custom_id,
                    coalesce(nullIf(s."ISWC", ''), nullIf(s.iswc, ''), '') AS iswc,
                    coalesce(nullIf(s."Composition Asset ID", ''), nullIf(s.composition_asset_id, ''), '') AS composition_asset_id,
                    coalesce(nullIf(s."Composition Title", ''), nullIf(s.composition_title, ''), nullIf(s."Composition Asset Title", ''), '') AS composition_title,
                    coalesce(nullIf(s."Writers", ''), nullIf(s.writers, ''), '') AS writers,
                    coalesce(nullIf(s."Copyright Type", ''), nullIf(s.copyright_type, ''), nullIf(s."Composition Right Type", ''), 'Mechanical') AS copyright_type,
                    now() AS updated_at
                FROM ${tempTable} s
                WHERE coalesce(nullIf(s."Custom ID", ''), nullIf(s.custom_id, ''), '') != ''
            `,
            query_params: { cmsId }
        }).catch(() => {});

        await client.command({ query: `DROP TABLE IF EXISTS ${tempTable}` });
        log(`[Publisher Usage] ✓ Ingested Subscription Usage`);
    }

    // Process AdSupport Usage
    if (files.adsupport_usage) {
        log(`[Publisher Usage] Loading AdSupport Usage into staging...`);
        const tempTable = `temp_adsupport_usage_${cleanCmsId}_${cleanJobId}`;
        await createPublisherUsageStagingTable(client, tempTable);
        await loadStagingTable(client, tempTable, files.adsupport_usage, isAborted);

        const insertQuery = getPublisherUsageInsertQuery(tempTable, cmsId, month, 'Ad-Supported');
        await client.command({
            query: insertQuery,
            query_params: { cmsId, uploadMonth: parseInt(month, 10), defaultOffer: 'Ad-Supported' }
        });

        // Populate work metadata lookup table
        await client.command({
            query: `
                INSERT INTO youtube_publisher_work_metadata (cms_id, custom_id, iswc, composition_asset_id, composition_title, writers, copyright_type, updated_at)
                SELECT DISTINCT
                    {cmsId: String} AS cms_id,
                    coalesce(nullIf(s."Custom ID", ''), nullIf(s.custom_id, ''), '') AS custom_id,
                    coalesce(nullIf(s."ISWC", ''), nullIf(s.iswc, ''), '') AS iswc,
                    coalesce(nullIf(s."Composition Asset ID", ''), nullIf(s.composition_asset_id, ''), '') AS composition_asset_id,
                    coalesce(nullIf(s."Composition Title", ''), nullIf(s.composition_title, ''), nullIf(s."Composition Asset Title", ''), '') AS composition_title,
                    coalesce(nullIf(s."Writers", ''), nullIf(s.writers, ''), '') AS writers,
                    coalesce(nullIf(s."Copyright Type", ''), nullIf(s.copyright_type, ''), nullIf(s."Composition Right Type", ''), 'Mechanical') AS copyright_type,
                    now() AS updated_at
                FROM ${tempTable} s
                WHERE coalesce(nullIf(s."Custom ID", ''), nullIf(s.custom_id, ''), '') != ''
            `,
            query_params: { cmsId }
        }).catch(() => {});

        await client.command({ query: `DROP TABLE IF EXISTS ${tempTable}` });
        log(`[Publisher Usage] ✓ Ingested AdSupport Usage`);
    }

    // Process Hardware Audio Tier
    if (files.hardware_audio_tier) {
        log(`[Publisher Usage] Loading Hardware Audio Tier into staging...`);
        const tempTable = `temp_hw_audio_usage_${cleanCmsId}_${cleanJobId}`;
        await createPublisherUsageStagingTable(client, tempTable);
        await loadStagingTable(client, tempTable, files.hardware_audio_tier, isAborted);

        const insertQuery = getPublisherUsageInsertQuery(tempTable, cmsId, month, 'Hardware Tier');
        await client.command({
            query: insertQuery,
            query_params: { cmsId, uploadMonth: parseInt(month, 10), defaultOffer: 'Hardware Tier' }
        });

        // Populate work metadata lookup table
        await client.command({
            query: `
                INSERT INTO youtube_publisher_work_metadata (cms_id, custom_id, iswc, composition_asset_id, composition_title, writers, copyright_type, updated_at)
                SELECT DISTINCT
                    {cmsId: String} AS cms_id,
                    coalesce(nullIf(s."Custom ID", ''), nullIf(s.custom_id, ''), '') AS custom_id,
                    coalesce(nullIf(s."ISWC", ''), nullIf(s.iswc, ''), '') AS iswc,
                    coalesce(nullIf(s."Composition Asset ID", ''), nullIf(s.composition_asset_id, ''), '') AS composition_asset_id,
                    coalesce(nullIf(s."Composition Title", ''), nullIf(s.composition_title, ''), nullIf(s."Composition Asset Title", ''), '') AS composition_title,
                    coalesce(nullIf(s."Writers", ''), nullIf(s.writers, ''), '') AS writers,
                    coalesce(nullIf(s."Copyright Type", ''), nullIf(s.copyright_type, ''), nullIf(s."Composition Right Type", ''), 'Mechanical') AS copyright_type,
                    now() AS updated_at
                FROM ${tempTable} s
                WHERE coalesce(nullIf(s."Custom ID", ''), nullIf(s.custom_id, ''), '') != ''
            `,
            query_params: { cmsId }
        }).catch(() => {});

        await client.command({ query: `DROP TABLE IF EXISTS ${tempTable}` });
        log(`[Publisher Usage] ✓ Ingested Hardware Audio Tier Usage`);
    }

    return { totalRows };
}
