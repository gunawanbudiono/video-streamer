/**
 * DDL definitions for per-CMS databases.
 * These are applied when a new CMS is registered.
 */

export const CMS_DDL = [
  // ── ads_revenue_enriched ──────────────────────────────────
  `CREATE TABLE IF NOT EXISTS ads_revenue_enriched (
    cms_id               String        DEFAULT '',
    upload_month         UInt32        DEFAULT 0,
    report_type          LowCardinality(String) DEFAULT 'claim_raw',
    label_source         LowCardinality(String) DEFAULT 'report',
    adjustment_type      LowCardinality(String) DEFAULT '',
    day                  Date,
    country              LowCardinality(String),
    video_id             String,
    channel_id           String,
    asset_id             String,
    asset_channel_id     String        DEFAULT '',
    asset_type           LowCardinality(String),
    content_type         LowCardinality(String),
    policy               LowCardinality(String),
    claim_type           LowCardinality(String),
    claim_origin         LowCardinality(String),
    custom_id            String        DEFAULT '',
    isrc                 String        DEFAULT '',
    grid                 String        DEFAULT '',
    upc                  String        DEFAULT '',
    video_title          String        DEFAULT '',
    video_duration_sec   UInt32        DEFAULT 0,
    username             String        DEFAULT '',
    uploader             String        DEFAULT '',
    channel_display_name LowCardinality(String) DEFAULT '',
    multiple_claims      LowCardinality(String) DEFAULT '',
    category             LowCardinality(String) DEFAULT '',
    asset_labels         String        DEFAULT '',
    artist               String        DEFAULT '',
    asset_title          String        DEFAULT '',
    album                String        DEFAULT '',
    label                LowCardinality(String) DEFAULT '',
    owned_views                        UInt64       DEFAULT 0,
    yt_rev_auction                     Decimal64(10) DEFAULT 0,
    yt_rev_reserved                    Decimal64(10) DEFAULT 0,
    yt_rev_partner_sold_yt_served      Decimal64(10) DEFAULT 0,
    yt_rev_partner_sold_p_served       Decimal64(10) DEFAULT 0,
    yt_rev_total                       Decimal64(10) DEFAULT 0,
    partner_rev_auction                Decimal64(10) DEFAULT 0,
    partner_rev_reserved               Decimal64(10) DEFAULT 0,
    partner_rev_partner_sold_yt_served Decimal64(10) DEFAULT 0,
    yt_rev_partner_sold_p_served  Decimal64(10) DEFAULT 0,
    partner_rev_total                  Decimal64(10) DEFAULT 0,
    us_tax                             Decimal64(10) DEFAULT 0,
    net_revenue                        Decimal64(10) DEFAULT 0,
    ingested_at          DateTime      DEFAULT now()
  )
  ENGINE = MergeTree()
  PARTITION BY (cms_id, upload_month)
  ORDER BY (cms_id, upload_month, day, report_type, asset_id, video_id, channel_id, country, claim_type)
  SETTINGS index_granularity = 8192`,

  // ── subscription_revenue ──────────────────────────────────
  `CREATE TABLE IF NOT EXISTS subscription_revenue (
    cms_id               String        DEFAULT '',
    upload_month         UInt32        DEFAULT 0,
    report_type          LowCardinality(String) DEFAULT '',
    adjustment_type      LowCardinality(String) DEFAULT '',
    day                  Date,
    country              LowCardinality(String),
    video_id             String,
    channel_id           String        DEFAULT '',
    asset_id             String,
    asset_channel_id     String        DEFAULT '',
    asset_title          String        DEFAULT '',
    asset_labels         String        DEFAULT '',
    asset_type           LowCardinality(String),
    custom_id            String        DEFAULT '',
    isrc                 String        DEFAULT '',
    upc                  String        DEFAULT '',
    grid                 String        DEFAULT '',
    artist               String        DEFAULT '',
    album                String        DEFAULT '',
    label                LowCardinality(String) DEFAULT '',
    claim_type           LowCardinality(String),
    content_type         LowCardinality(String),
    offer                LowCardinality(String) DEFAULT '',
    video_title          String        DEFAULT '',
    video_duration_sec   UInt32        DEFAULT 0,
    uploader             String        DEFAULT '',
    channel_display_name LowCardinality(String) DEFAULT '',
    policy               LowCardinality(String) DEFAULT '',
    claim_origin         LowCardinality(String) DEFAULT '',
    owned_views                  UInt64    DEFAULT 0,
    monetized_views_audio        UInt64    DEFAULT 0,
    monetized_views_audiovisual  UInt64    DEFAULT 0,
    monetized_views_total        UInt64    DEFAULT 0,
    yt_rev_total                 Decimal64(10) DEFAULT 0,
    partner_rev_pro_rata         Decimal64(10) DEFAULT 0,
    partner_rev_per_sub_min      Decimal64(10) DEFAULT 0,
    partner_rev_total            Decimal64(10) DEFAULT 0,
    us_tax                       Decimal64(10) DEFAULT 0,
    net_revenue                  Decimal64(10) DEFAULT 0,
    ingested_at          DateTime      DEFAULT now()
  )
  ENGINE = MergeTree()
  PARTITION BY (cms_id, upload_month)
  ORDER BY (cms_id, upload_month, day, asset_id, video_id, country)
  SETTINGS index_granularity = 8192`,

  // ── audio_tier_revenue ────────────────────────────────────
  `CREATE TABLE IF NOT EXISTS audio_tier_revenue (
    cms_id                     String        DEFAULT '',
    upload_month               UInt32        DEFAULT 0,
    adjustment_type            LowCardinality(String) DEFAULT '',
    day                        Date,
    country                    LowCardinality(String),
    video_id                   String,
    asset_id                   String,
    channel_id                 String        DEFAULT '',
    asset_title                String        DEFAULT '',
    asset_labels               String        DEFAULT '',
    custom_id                  String        DEFAULT '',
    isrc                       String        DEFAULT '',
    upc                        String        DEFAULT '',
    grid                       String        DEFAULT '',
    artist                     String        DEFAULT '',
    album                      String        DEFAULT '',
    label                      LowCardinality(String) DEFAULT '',
    video_title                String        DEFAULT '',
    channel_display_name       LowCardinality(String) DEFAULT '',
    content_type               LowCardinality(String) DEFAULT '',
    policy                     LowCardinality(String) DEFAULT '',
    claim_type                 LowCardinality(String) DEFAULT '',
    claim_origin               LowCardinality(String) DEFAULT '',
    asset_channel_id           String        DEFAULT '',
    asset_type                 LowCardinality(String) DEFAULT '',
    owned_views                UInt64        DEFAULT 0,
    yt_rev_total               Decimal64(10) DEFAULT 0,
    partner_rev_pro_rata       Decimal64(10) DEFAULT 0,
    partner_rev_per_play_min   Decimal64(10) DEFAULT 0,
    partner_rev_total          Decimal64(10) DEFAULT 0,
    us_tax                     Decimal64(10) DEFAULT 0,
    net_revenue                Decimal64(10) DEFAULT 0,
    ingested_at                DateTime      DEFAULT now()
  )
  ENGINE = MergeTree()
  PARTITION BY (cms_id, upload_month)
  ORDER BY (cms_id, upload_month, day, asset_id, video_id, country)
  SETTINGS index_granularity = 8192`,

  // ── paid_features_raw ─────────────────────────────────────
  `CREATE TABLE IF NOT EXISTS paid_features_raw (
    cms_id                     String,
    upload_month               UInt32,
    day                        Date,
    purchase_type              String,
    refund_chargeback          UInt8,
    country                    String,
    channel_display_name       LowCardinality(String) DEFAULT '',
    channel_name               String        DEFAULT '',
    channel_id                 String,
    video_id                   String,
    retail_price_usd           Float64,
    total_tax_usd              Float64,
    partner_earnings_fraction  Float64,
    earnings_usd               Float64,
    content_type               LowCardinality(String) DEFAULT '',
    policy                     LowCardinality(String) DEFAULT '',
    claim_type                 LowCardinality(String) DEFAULT '',
    claim_origin               LowCardinality(String) DEFAULT '',
    us_tax                     Decimal64(10) DEFAULT 0,
    net_revenue                Decimal64(10) DEFAULT 0,
    ingested_at                DateTime DEFAULT now()
  ) ENGINE = MergeTree()
  PARTITION BY (cms_id, upload_month)
  ORDER BY (cms_id, upload_month, channel_id, purchase_type)
  SETTINGS index_granularity = 8192`,

  // ── channel_label_map ─────────────────────────────────────
  `CREATE TABLE IF NOT EXISTS channel_label_map (
    channel_id    String,
    asset_label   String,
    cms_name      LowCardinality(String),
    notes         String        DEFAULT '',
    updated_at    DateTime      DEFAULT now()
  )
  ENGINE = ReplacingMergeTree(updated_at)
  ORDER BY (channel_id)`,

  // ── v_unified_revenue ─────────────────────────────────────
  `CREATE VIEW IF NOT EXISTS v_unified_revenue AS
  SELECT cms_id, upload_month, day, video_id, asset_id, country,
         'ads' AS revenue_source, report_type AS sub_source,
         owned_views, yt_rev_total, partner_rev_total
  FROM ads_revenue_enriched
  UNION ALL
  SELECT cms_id, upload_month, day, video_id, asset_id, country,
         'subscription', 'subscription',
         owned_views, yt_rev_total, partner_rev_total
  FROM subscription_revenue
  UNION ALL
  SELECT cms_id, upload_month, day, video_id, asset_id, country,
         'audio_tier', 'audio_tier',
         owned_views, yt_rev_total, partner_rev_total
  FROM audio_tier_revenue`,

  // ── mv_daily_summary ──────────────────────────────────────
  `CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_summary
  ENGINE = SummingMergeTree()
  ORDER BY (upload_month, day, report_type)
  AS SELECT
      upload_month,
      day,
      report_type,
      count()              AS total_rows,
      sum(owned_views)     AS total_views,
      sum(yt_rev_total)    AS total_yt_revenue,
      sum(partner_rev_total) AS total_partner_revenue
  FROM ads_revenue_enriched
  GROUP BY upload_month, day, report_type`,

  // ── estimated_revenue_daily ────────────────────────────────
  `CREATE TABLE IF NOT EXISTS estimated_revenue_daily (
    cms_id               String        DEFAULT '',
    day                  Date,
    country              LowCardinality(String),
    video_id             String,
    channel_id           String,
    owner_channel_id     String        DEFAULT channel_id,
    asset_id             String,
    asset_type           LowCardinality(String),
    content_type         LowCardinality(String),
    creator_content_type LowCardinality(String) DEFAULT '',
    claim_type           LowCardinality(String),
    policy               LowCardinality(String) DEFAULT '',
    claim_origin         LowCardinality(String) DEFAULT '',
    isrc                 String        DEFAULT '',
    upc                  String        DEFAULT '',
    grid                 String        DEFAULT '',
    video_title          String        DEFAULT '',
    username             String        DEFAULT '',
    uploader             String        DEFAULT '',
    video_duration_sec   UInt32        DEFAULT 0,
    channel_display_name LowCardinality(String) DEFAULT '',
    multiple_claims      LowCardinality(String) DEFAULT '',
    category             LowCardinality(String) DEFAULT '',
    asset_labels         String        DEFAULT '',
    artist               String        DEFAULT '',
    asset_title          String        DEFAULT '',
    album                String        DEFAULT '',
    label                LowCardinality(String) DEFAULT '',
    owned_views          UInt64        DEFAULT 0,
    yt_rev_auction                     Decimal64(10) DEFAULT 0,
    yt_rev_reserved                    Decimal64(10) DEFAULT 0,
    yt_rev_partner_sold_yt_served      Decimal64(10) DEFAULT 0,
    yt_rev_partner_sold_p_served       Decimal64(10) DEFAULT 0,
    yt_rev_red                         Decimal64(10) DEFAULT 0,
    yt_rev_total                       Decimal64(10) DEFAULT 0,
    partner_rev_auction                Decimal64(10) DEFAULT 0,
    partner_rev_reserved               Decimal64(10) DEFAULT 0,
    partner_rev_partner_sold_yt_served Decimal64(10) DEFAULT 0,
    partner_rev_partner_sold_p_served  Decimal64(10) DEFAULT 0,
    partner_rev_red                    Decimal64(10) DEFAULT 0,
    partner_rev_total                  Decimal64(10) DEFAULT 0,
    monetized_playbacks                UInt64        DEFAULT 0,
    ad_impressions                     UInt64        DEFAULT 0,
    partner_rev_transaction            Decimal64(10) DEFAULT 0,
    claimed_status                     LowCardinality(String) DEFAULT '',
    claim_status                       LowCardinality(String) DEFAULT '',
    uploader_type                      LowCardinality(String) DEFAULT '',
    video_upload_date                  String        DEFAULT '',
    genre                              LowCardinality(String) DEFAULT '',
    estimated_cpm                      Decimal64(10) DEFAULT 0,
    estimated_playback_based_cpm       Decimal64(10) DEFAULT 0,
    likes                              UInt64        DEFAULT 0,
    comments                           UInt64        DEFAULT 0,
    shares                             UInt64        DEFAULT 0,
    dislikes                           UInt64        DEFAULT 0,
    watch_time_minutes                 Float64       DEFAULT 0.0,
    average_view_duration_seconds      UInt32        DEFAULT 0,
    average_view_duration_percentage   Float64       DEFAULT 0.0,
    subscribers_gained                 UInt64        DEFAULT 0,
    subscribers_lost                   UInt64        DEFAULT 0,
    custom_id                          String        DEFAULT '',
    ingested_at          DateTime      DEFAULT now(),
    INDEX idx_video_id video_id TYPE bloom_filter() GRANULARITY 1,
    INDEX idx_asset_id asset_id TYPE bloom_filter() GRANULARITY 1
  )
  ENGINE = MergeTree()
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, day, asset_id, video_id, country)
  TTL day + INTERVAL 3 YEAR
  SETTINGS index_granularity = 8192`,

  // ── video_reach_performance_daily ──────────────────────────
  `CREATE TABLE IF NOT EXISTS video_reach_performance_daily (
    cms_id               String        DEFAULT '',
    day                  Date,
    video_id             String,
    channel_id           String,
    impressions          UInt64        DEFAULT 0,
    impressions_ctr      Decimal64(4)  DEFAULT 0,
    ingested_at          DateTime      DEFAULT now()
  )
  ENGINE = MergeTree()
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, day, video_id, channel_id)
  TTL day + INTERVAL 3 YEAR
  SETTINGS index_granularity = 8192`,

  // ── video_demographics_daily ────────────────────────────────
  `CREATE TABLE IF NOT EXISTS video_demographics_daily (
    cms_id               String        DEFAULT '',
    day                  Date,
    video_id             String,
    channel_id           String,
    age_group            String,
    gender               String,
    views_percentage     Decimal64(4)  DEFAULT 0,
    ingested_at          DateTime      DEFAULT now()
  )
  ENGINE = MergeTree()
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, day, video_id, age_group, gender)
  TTL day + INTERVAL 3 YEAR
  SETTINGS index_granularity = 8192`,

  // ── video_traffic_sources_daily ─────────────────────────────
  `CREATE TABLE IF NOT EXISTS video_traffic_sources_daily (
    cms_id               String        DEFAULT '',
    day                  Date,
    video_id             String,
    channel_id           String,
    traffic_source_type  String,
    views                UInt64        DEFAULT 0,
    watch_time_sec       UInt64        DEFAULT 0,
    ingested_at          DateTime      DEFAULT now()
  )
  ENGINE = MergeTree()
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, day, video_id, traffic_source_type)
  TTL day + INTERVAL 3 YEAR
  SETTINGS index_granularity = 8192`,

  // ── video_devices_daily ─────────────────────────────────────
  `CREATE TABLE IF NOT EXISTS video_devices_daily (
    cms_id               String        DEFAULT '',
    day                  Date,
    video_id             String,
    channel_id           String,
    device_type          String,
    operating_system     String,
    views                UInt64        DEFAULT 0,
    watch_time_sec       UInt64        DEFAULT 0,
    ingested_at          DateTime      DEFAULT now()
  )
  ENGINE = MergeTree()
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, day, video_id, device_type, operating_system)
  TTL day + INTERVAL 3 YEAR
  SETTINGS index_granularity = 8192`,

  // ── video_countries_daily ──────────────────────────────────
  `CREATE TABLE IF NOT EXISTS video_countries_daily (
    cms_id               String        DEFAULT '',
    day                  Date,
    country              LowCardinality(String),
    video_id             String,
    views                UInt64        DEFAULT 0,
    watch_time_sec       UInt64        DEFAULT 0,
    ingested_at          DateTime      DEFAULT now()
  )
  ENGINE = MergeTree()
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, day, country, video_id)
  TTL day + INTERVAL 3 YEAR
  SETTINGS index_granularity = 8192`,

  // ── channel_subscribers_daily ─────────────────────────────
  `CREATE TABLE IF NOT EXISTS channel_subscribers_daily (
    cms_id               String        DEFAULT '',
    day                  Date,
    channel_id           String,
    country              LowCardinality(String),
    subscribed_status    LowCardinality(String) DEFAULT '',
    subscribers_gained   UInt64        DEFAULT 0,
    subscribers_lost     UInt64        DEFAULT 0,
    ingested_at          DateTime      DEFAULT now()
  )
  ENGINE = MergeTree()
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, day, channel_id, country)
  TTL day + INTERVAL 3 YEAR
  SETTINGS index_granularity = 8192`,

  // ── video_interactions_daily ──────────────────────────────
  `CREATE TABLE IF NOT EXISTS video_interactions_daily (
    cms_id               String        DEFAULT '',
    day                  Date,
    video_id             String,
    channel_id           String,
    likes                Int64         DEFAULT 0,
    dislikes             Int64         DEFAULT 0,
    comments             Int64         DEFAULT 0,
    shares               Int64         DEFAULT 0,
    ingested_at          DateTime      DEFAULT now()
  )
  ENGINE = MergeTree()
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, day, video_id, channel_id)
  TTL day + INTERVAL 3 YEAR
  SETTINGS index_granularity = 8192`,

  // ── video_province_daily ──────────────────────────────────
  // DIBATALKAN: Google hanya mendukung dimensi provinsi untuk Amerika Serikat (US).



  // ── mv_asset_performance_daily ─────────────────────────────
  `CREATE TABLE IF NOT EXISTS mv_asset_performance_daily (
    day                  Date,
    cms_id               String,
    asset_id             String,
    isrc                 String,
    artist               String,
    asset_title          String,
    total_views          UInt64,
    total_revenue_usd    Decimal64(10)
  )
  ENGINE = SummingMergeTree()
  ORDER BY (day, cms_id, asset_id, isrc)
  SETTINGS index_granularity = 8192`,

  // ── v_mv_asset_performance_daily (Materialized View) ───────
  `CREATE MATERIALIZED VIEW IF NOT EXISTS v_mv_asset_performance_daily
  TO mv_asset_performance_daily AS
  SELECT
      day,
      cms_id,
      asset_id,
      isrc,
      artist,
      asset_title,
      sum(owned_views) AS total_views,
      sum(partner_rev_total) AS total_revenue_usd
  FROM estimated_revenue_daily
  WHERE asset_id != '' AND asset_id != 'UNCLAIMED_VIDEO'
  GROUP BY day, cms_id, asset_id, isrc, artist, asset_title`,

  // ── youtube_asset_metadata (Tracking YouTube Asset Metadata) ──
  `CREATE TABLE IF NOT EXISTS youtube_asset_metadata (
    cms_id       String DEFAULT '',
    day          Date,
    asset_id     String,
    asset_title  String DEFAULT '',
    artist       String DEFAULT '',
    album        String DEFAULT '',
    label        String DEFAULT '',
    isrc         String DEFAULT '',
    upc          String DEFAULT '',
    grid         String DEFAULT '',
    custom_id    String DEFAULT '',
    genre        String DEFAULT '',
    asset_labels String DEFAULT '',
    ingested_at  DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(ingested_at)
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, asset_id)`,

  // ── youtube_video_metadata (Tracking YouTube Video Metadata) ──
  `CREATE TABLE IF NOT EXISTS youtube_video_metadata (
    cms_id               String DEFAULT '',
    day                  Date,
    video_id             String,
    video_title          String DEFAULT '',
    channel_id           String DEFAULT '',
    channel_display_name String DEFAULT '',
    video_length_sec     UInt32 DEFAULT 0,
    category             String DEFAULT '',
    asset_id             String DEFAULT '',
    custom_id            String DEFAULT '',
    isrc                 String DEFAULT '',
    content_type         String DEFAULT '',
    ingested_at          DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(ingested_at)
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, video_id, asset_id)`,

  // ── youtube_raw_claims ──────────────────────────────────
  `CREATE TABLE IF NOT EXISTS youtube_raw_claims (
    cms_id               String DEFAULT '',
    day                  Date,
    video_id             String DEFAULT '',
    asset_id             String DEFAULT '',
    channel_id           String DEFAULT '',
    asset_type           LowCardinality(String) DEFAULT '',
    content_type         LowCardinality(String) DEFAULT '',
    claim_type           LowCardinality(String) DEFAULT '',
    policy               LowCardinality(String) DEFAULT '',
    claim_origin         LowCardinality(String) DEFAULT '',
    custom_id            String DEFAULT '',
    isrc                 String DEFAULT '',
    grid                 String DEFAULT '',
    upc                  String DEFAULT '',
    video_title          String DEFAULT '',
    username             String DEFAULT '',
    uploader             String DEFAULT '',
    video_duration_sec   UInt32 DEFAULT 0,
    channel_display_name String DEFAULT '',
    multiple_claims      String DEFAULT '',
    category             String DEFAULT '',
    asset_labels         String DEFAULT '',
    artist               String DEFAULT '',
    asset_title          String DEFAULT '',
    album                String DEFAULT '',
    label                String DEFAULT '',
    views                UInt64 DEFAULT 0,
    claim_id             String DEFAULT '',
    claim_status         String DEFAULT '',
    claim_status_detail  String DEFAULT '',
    record_label         String DEFAULT '',
    engaged_views        UInt64 DEFAULT 0,
    matching_duration    String DEFAULT '',
    video_matching_length String DEFAULT '',
    longest_match        String DEFAULT '',
    reference_video_id   String DEFAULT '',
    reference_id         String DEFAULT '',
    claim_policy_id      String DEFAULT '',
    asset_policy_id      String DEFAULT '',
    claim_policy_monetize String DEFAULT '',
    claim_policy_track   String DEFAULT '',
    claim_policy_block   String DEFAULT '',
    asset_policy_monetize String DEFAULT '',
    asset_policy_track   String DEFAULT '',
    asset_policy_block   String DEFAULT '',
    claim_created_date   String DEFAULT '',
    video_upload_date    String DEFAULT '',
    tms                  String DEFAULT '',
    director             String DEFAULT '',
    season               String DEFAULT '',
    episode_number       String DEFAULT '',
    episode_title        String DEFAULT '',
    release_date         String DEFAULT '',
    hfa_song_code        String DEFAULT '',
    iswc                 String DEFAULT '',
    writers              String DEFAULT '',
    is_shorts_eligible   String DEFAULT '',
    ingested_at          DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(ingested_at)
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, claim_id, claim_status)`,

  // ── youtube_raw_estimated_revenue ───────────────────────
  `CREATE TABLE IF NOT EXISTS youtube_raw_estimated_revenue (
    cms_id                               String DEFAULT '',
    day                                  Date,
    video_id                             String DEFAULT '',
    date                                 String DEFAULT '',
    country                              LowCardinality(String) DEFAULT '',
    channel_id                           String DEFAULT '',
    claimed_status                       LowCardinality(String) DEFAULT '',
    uploader_type                        LowCardinality(String) DEFAULT '',
    estimated_partner_revenue            Decimal64(10) DEFAULT 0,
    estimated_partner_ad_revenue         Decimal64(10) DEFAULT 0,
    estimated_partner_ad_auction_revenue  Decimal64(10) DEFAULT 0,
    estimated_partner_ad_reserved_revenue Decimal64(10) DEFAULT 0,
    estimated_youtube_ad_revenue         Decimal64(10) DEFAULT 0,
    estimated_monetized_playbacks        UInt64 DEFAULT 0,
    ad_impressions                       UInt64 DEFAULT 0,
    estimated_partner_red_revenue        Decimal64(10) DEFAULT 0,
    estimated_partner_transaction_revenue Decimal64(10) DEFAULT 0,
    country_code                         String DEFAULT '',
    estimated_playback_based_cpm         Decimal64(10) DEFAULT 0,
    estimated_cpm                        Decimal64(10) DEFAULT 0,
    views                                UInt64 DEFAULT 0,
    video_title                          String DEFAULT '',
    uploader                             String DEFAULT '',
    ingested_at                          DateTime DEFAULT now()
  ) ENGINE = MergeTree()
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, day, video_id)`,

  // ── youtube_raw_asset_estimated_revenue ───────────────────
  `CREATE TABLE IF NOT EXISTS youtube_raw_asset_estimated_revenue (
    cms_id                               String DEFAULT '',
    day                                  Date,
    asset_id                             String DEFAULT '',
    channel_id                           String DEFAULT '',
    estimated_partner_revenue            Decimal64(10) DEFAULT 0,
    estimated_partner_ad_revenue         Decimal64(10) DEFAULT 0,
    estimated_partner_red_revenue        Decimal64(10) DEFAULT 0,
    estimated_partner_transaction_revenue Decimal64(10) DEFAULT 0,
    ingested_at                          DateTime DEFAULT now()
  ) ENGINE = MergeTree()
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, day, asset_id)`,

  // ── youtube_raw_channel_estimated_revenue ─────────────────
  `CREATE TABLE IF NOT EXISTS youtube_raw_channel_estimated_revenue (
    cms_id                               String DEFAULT '',
    day                                  Date,
    channel_id                           String DEFAULT '',
    estimated_partner_revenue            Decimal64(10) DEFAULT 0,
    estimated_partner_ad_revenue         Decimal64(10) DEFAULT 0,
    estimated_partner_red_revenue        Decimal64(10) DEFAULT 0,
    estimated_partner_transaction_revenue Decimal64(10) DEFAULT 0,
    ingested_at                          DateTime DEFAULT now()
  ) ENGINE = MergeTree()
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, day, channel_id)`,

  // ── raw_youtube_publisher_usage_reports ────────────────────
  `CREATE TABLE IF NOT EXISTS raw_youtube_publisher_usage_reports (
    cms_id                    String        DEFAULT '',
    payment_month             UInt32        DEFAULT 0,
    report_type               LowCardinality(String) DEFAULT 'publisher_usage',
    offer                     LowCardinality(String) DEFAULT '',
    country                   String        DEFAULT '',
    claiming_asset_id         String        DEFAULT '',
    title                     String        DEFAULT '',
    artist                    String        DEFAULT '',
    isrc                      String        DEFAULT '',
    claiming_asset_type       LowCardinality(String) DEFAULT '',
    composition_asset_id      String        DEFAULT '',
    custom_id                 String        DEFAULT '',
    composition_title         String        DEFAULT '',
    iswc                      String        DEFAULT '',
    writers                   String        DEFAULT '',
    content_category          LowCardinality(String) DEFAULT '',
    copyright_type            LowCardinality(String) DEFAULT '',
    ownership_percentage      Float64       DEFAULT 0,
    views                     UInt64        DEFAULT 0,
    youtube_revenue_split     Decimal64(10) DEFAULT 0,
    partner_revenue           Decimal64(10) DEFAULT 0,
    ingested_at               DateTime      DEFAULT now()
  ) ENGINE = MergeTree()
  PARTITION BY (cms_id, payment_month)
  ORDER BY (cms_id, payment_month, country, iswc)`,

  // ── youtube_publisher_work_metadata ─────────────────────
  `CREATE TABLE IF NOT EXISTS youtube_publisher_work_metadata (
    cms_id               String DEFAULT '',
    custom_id            String DEFAULT '',
    iswc                 String DEFAULT '',
    composition_asset_id String DEFAULT '',
    composition_title    String DEFAULT '',
    writers              String DEFAULT '',
    copyright_type       LowCardinality(String) DEFAULT 'Mechanical',
    updated_at           DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(updated_at)
  ORDER BY (cms_id, custom_id)`
];

/** Migrations for per-CMS databases. Executed after DDL */
export const CMS_MIGRATIONS = [
  `ALTER TABLE ads_revenue_enriched ADD COLUMN IF NOT EXISTS us_tax Decimal64(10) DEFAULT 0`,
  `ALTER TABLE ads_revenue_enriched ADD COLUMN IF NOT EXISTS net_revenue Decimal64(10) DEFAULT 0`,
  `ALTER TABLE subscription_revenue ADD COLUMN IF NOT EXISTS us_tax Decimal64(10) DEFAULT 0`,
  `ALTER TABLE subscription_revenue ADD COLUMN IF NOT EXISTS net_revenue Decimal64(10) DEFAULT 0`,
  `ALTER TABLE subscription_revenue ADD COLUMN IF NOT EXISTS video_title String DEFAULT ''`,
  `ALTER TABLE subscription_revenue ADD COLUMN IF NOT EXISTS video_duration_sec UInt32 DEFAULT 0`,
  `ALTER TABLE subscription_revenue ADD COLUMN IF NOT EXISTS uploader String DEFAULT ''`,
  `ALTER TABLE subscription_revenue ADD COLUMN IF NOT EXISTS channel_display_name String DEFAULT ''`,
  `ALTER TABLE subscription_revenue ADD COLUMN IF NOT EXISTS report_type LowCardinality(String) DEFAULT ''`,

  `ALTER TABLE ads_revenue_enriched 
   MODIFY COLUMN yt_rev_auction Decimal64(10),
   MODIFY COLUMN yt_rev_reserved Decimal64(10),
   MODIFY COLUMN yt_rev_partner_sold_yt_served Decimal64(10),
   MODIFY COLUMN yt_rev_partner_sold_p_served Decimal64(10),
   MODIFY COLUMN yt_rev_total Decimal64(10),
   MODIFY COLUMN partner_rev_auction Decimal64(10),
   MODIFY COLUMN partner_rev_reserved Decimal64(10),
   MODIFY COLUMN partner_rev_partner_sold_yt_served Decimal64(10),
   MODIFY COLUMN partner_rev_partner_sold_p_served Decimal64(10),
   MODIFY COLUMN partner_rev_total Decimal64(10)`,

  `ALTER TABLE subscription_revenue 
   MODIFY COLUMN yt_rev_total Decimal64(10),
   MODIFY COLUMN partner_rev_pro_rata Decimal64(10),
   MODIFY COLUMN partner_rev_per_sub_min Decimal64(10),
   MODIFY COLUMN partner_rev_total Decimal64(10)`,

  `ALTER TABLE estimated_revenue_daily 
   ADD COLUMN IF NOT EXISTS yt_rev_auction Decimal64(10) DEFAULT 0 AFTER owned_views,
   ADD COLUMN IF NOT EXISTS yt_rev_reserved Decimal64(10) DEFAULT 0 AFTER yt_rev_auction,
   ADD COLUMN IF NOT EXISTS yt_rev_partner_sold_yt_served Decimal64(10) DEFAULT 0 AFTER yt_rev_reserved,
   ADD COLUMN IF NOT EXISTS yt_rev_partner_sold_p_served Decimal64(10) DEFAULT 0 AFTER yt_rev_partner_sold_yt_served,
   ADD COLUMN IF NOT EXISTS partner_rev_auction Decimal64(10) DEFAULT 0 AFTER yt_rev_partner_sold_p_served,
   ADD COLUMN IF NOT EXISTS partner_rev_reserved Decimal64(10) DEFAULT 0 AFTER partner_rev_auction,
   ADD COLUMN IF NOT EXISTS partner_rev_partner_sold_yt_served Decimal64(10) DEFAULT 0 AFTER partner_rev_reserved,
   ADD COLUMN IF NOT EXISTS partner_rev_partner_sold_p_served Decimal64(10) DEFAULT 0 AFTER partner_rev_partner_sold_yt_served,
   ADD COLUMN IF NOT EXISTS policy LowCardinality(String) DEFAULT '' AFTER claim_type,
   ADD COLUMN IF NOT EXISTS claim_origin LowCardinality(String) DEFAULT '' AFTER policy,
   ADD COLUMN IF NOT EXISTS username String DEFAULT '' AFTER video_title,
   ADD COLUMN IF NOT EXISTS uploader String DEFAULT '' AFTER username,
   ADD COLUMN IF NOT EXISTS video_duration_sec UInt32 DEFAULT 0 AFTER uploader,
   ADD COLUMN IF NOT EXISTS category LowCardinality(String) DEFAULT '' AFTER video_duration_sec,
   ADD COLUMN IF NOT EXISTS multiple_claims LowCardinality(String) DEFAULT '' AFTER category,
   ADD COLUMN IF NOT EXISTS asset_labels String DEFAULT '' AFTER multiple_claims,
   ADD COLUMN IF NOT EXISTS grid String DEFAULT '' AFTER upc,
   ADD COLUMN IF NOT EXISTS yt_rev_red Decimal64(10) DEFAULT 0 AFTER yt_rev_partner_sold_p_served,
   ADD COLUMN IF NOT EXISTS partner_rev_red Decimal64(10) DEFAULT 0 AFTER partner_rev_partner_sold_p_served`,

  `ALTER TABLE video_demographics_daily 
   ADD COLUMN IF NOT EXISTS views_percentage Decimal64(4) DEFAULT 0 AFTER watch_time_sec`,
  `ALTER TABLE estimated_revenue_daily ADD COLUMN IF NOT EXISTS custom_id String DEFAULT ''`,
  `ALTER TABLE estimated_revenue_daily 
   ADD COLUMN IF NOT EXISTS monetized_playbacks UInt64 DEFAULT 0 AFTER partner_rev_total,
   ADD COLUMN IF NOT EXISTS ad_impressions UInt64 DEFAULT 0 AFTER monetized_playbacks,
   ADD COLUMN IF NOT EXISTS partner_rev_transaction Decimal64(10) DEFAULT 0 AFTER ad_impressions`,
  `ALTER TABLE estimated_revenue_daily ADD COLUMN IF NOT EXISTS owner_channel_id String DEFAULT channel_id`,
  `ALTER TABLE estimated_revenue_daily ADD COLUMN IF NOT EXISTS creator_content_type LowCardinality(String) DEFAULT ''`,

  `CREATE TABLE IF NOT EXISTS youtube_asset_metadata (
    cms_id       String DEFAULT '',
    day          Date,
    asset_id     String,
    asset_title  String DEFAULT '',
    artist       String DEFAULT '',
    album        String DEFAULT '',
    label        String DEFAULT '',
    isrc         String DEFAULT '',
    upc          String DEFAULT '',
    grid         String DEFAULT '',
    custom_id    String DEFAULT '',
    genre        String DEFAULT '',
    asset_labels String DEFAULT '',
    ingested_at  DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(ingested_at)
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, asset_id)`,

  `CREATE TABLE IF NOT EXISTS youtube_video_metadata (
    cms_id               String DEFAULT '',
    day                  Date,
    video_id             String,
    video_title          String DEFAULT '',
    channel_id           String DEFAULT '',
    channel_display_name String DEFAULT '',
    video_length_sec     UInt32 DEFAULT 0,
    category             String DEFAULT '',
    asset_id             String DEFAULT '',
    custom_id            String DEFAULT '',
    isrc                 String DEFAULT '',
    content_type         String DEFAULT '',
    ingested_at          DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(ingested_at)
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, video_id, asset_id)`,

  `CREATE TABLE IF NOT EXISTS youtube_raw_claims (
    cms_id               String DEFAULT '',
    day                  Date,
    video_id             String DEFAULT '',
    asset_id             String DEFAULT '',
    channel_id           String DEFAULT '',
    asset_type           LowCardinality(String) DEFAULT '',
    content_type         LowCardinality(String) DEFAULT '',
    claim_type           LowCardinality(String) DEFAULT '',
    policy               LowCardinality(String) DEFAULT '',
    claim_origin         LowCardinality(String) DEFAULT '',
    custom_id            String DEFAULT '',
    isrc                 String DEFAULT '',
    grid                 String DEFAULT '',
    upc                  String DEFAULT '',
    video_title          String DEFAULT '',
    username             String DEFAULT '',
    uploader             String DEFAULT '',
    video_duration_sec   UInt32 DEFAULT 0,
    channel_display_name String DEFAULT '',
    multiple_claims      String DEFAULT '',
    category             String DEFAULT '',
    asset_labels         String DEFAULT '',
    artist               String DEFAULT '',
    asset_title          String DEFAULT '',
    album                String DEFAULT '',
    label                String DEFAULT '',
    views                UInt64 DEFAULT 0,
    claim_id             String DEFAULT '',
    claim_status         String DEFAULT '',
    claim_status_detail  String DEFAULT '',
    record_label         String DEFAULT '',
    engaged_views        UInt64 DEFAULT 0,
    matching_duration    String DEFAULT '',
    video_matching_length String DEFAULT '',
    longest_match        String DEFAULT '',
    reference_video_id   String DEFAULT '',
    reference_id         String DEFAULT '',
    claim_policy_id      String DEFAULT '',
    asset_policy_id      String DEFAULT '',
    claim_policy_monetize String DEFAULT '',
    claim_policy_track   String DEFAULT '',
    claim_policy_block   String DEFAULT '',
    asset_policy_monetize String DEFAULT '',
    asset_policy_track   String DEFAULT '',
    asset_policy_block   String DEFAULT '',
    claim_created_date   String DEFAULT '',
    video_upload_date    String DEFAULT '',
    tms                  String DEFAULT '',
    director             String DEFAULT '',
    season               String DEFAULT '',
    episode_number       String DEFAULT '',
    episode_title        String DEFAULT '',
    release_date         String DEFAULT '',
    hfa_song_code        String DEFAULT '',
    iswc                 String DEFAULT '',
    writers              String DEFAULT '',
    is_shorts_eligible   String DEFAULT '',
    ingested_at          DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(ingested_at)
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, claim_id, claim_status)`,

  `CREATE TABLE IF NOT EXISTS youtube_raw_estimated_revenue (
    cms_id                               String DEFAULT '',
    day                                  Date,
    video_id                             String DEFAULT '',
    date                                 String DEFAULT '',
    country                              LowCardinality(String) DEFAULT '',
    channel_id                           String DEFAULT '',
    claimed_status                       LowCardinality(String) DEFAULT '',
    uploader_type                        LowCardinality(String) DEFAULT '',
    estimated_partner_revenue            Decimal64(10) DEFAULT 0,
    estimated_partner_ad_revenue         Decimal64(10) DEFAULT 0,
    estimated_partner_ad_auction_revenue  Decimal64(10) DEFAULT 0,
    estimated_partner_ad_reserved_revenue Decimal64(10) DEFAULT 0,
    estimated_youtube_ad_revenue         Decimal64(10) DEFAULT 0,
    estimated_monetized_playbacks        UInt64 DEFAULT 0,
    ad_impressions                       UInt64 DEFAULT 0,
    estimated_partner_red_revenue        Decimal64(10) DEFAULT 0,
    estimated_partner_transaction_revenue Decimal64(10) DEFAULT 0,
    country_code                         String DEFAULT '',
    estimated_playback_based_cpm         Decimal64(10) DEFAULT 0,
    estimated_cpm                        Decimal64(10) DEFAULT 0,
    views                                UInt64 DEFAULT 0,
    video_title                          String DEFAULT '',
    uploader                             String DEFAULT '',
    ingested_at                          DateTime DEFAULT now()
  ) ENGINE = MergeTree()
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, day, video_id)`,

  `ALTER TABLE youtube_raw_claims
   ADD COLUMN IF NOT EXISTS claim_id String DEFAULT '',
   ADD COLUMN IF NOT EXISTS claim_status String DEFAULT '',
   ADD COLUMN IF NOT EXISTS claim_status_detail String DEFAULT '',
   ADD COLUMN IF NOT EXISTS record_label String DEFAULT '',
   ADD COLUMN IF NOT EXISTS engaged_views UInt64 DEFAULT 0,
   ADD COLUMN IF NOT EXISTS matching_duration String DEFAULT '',
   ADD COLUMN IF NOT EXISTS video_matching_length String DEFAULT '',
   ADD COLUMN IF NOT EXISTS longest_match String DEFAULT '',
   ADD COLUMN IF NOT EXISTS reference_video_id String DEFAULT '',
   ADD COLUMN IF NOT EXISTS reference_id String DEFAULT '',
   ADD COLUMN IF NOT EXISTS claim_policy_id String DEFAULT '',
   ADD COLUMN IF NOT EXISTS asset_policy_id String DEFAULT '',
   ADD COLUMN IF NOT EXISTS claim_policy_monetize String DEFAULT '',
   ADD COLUMN IF NOT EXISTS claim_policy_track String DEFAULT '',
   ADD COLUMN IF NOT EXISTS claim_policy_block String DEFAULT '',
   ADD COLUMN IF NOT EXISTS asset_policy_monetize String DEFAULT '',
   ADD COLUMN IF NOT EXISTS asset_policy_track String DEFAULT '',
   ADD COLUMN IF NOT EXISTS asset_policy_block String DEFAULT '',
   ADD COLUMN IF NOT EXISTS claim_created_date String DEFAULT '',
   ADD COLUMN IF NOT EXISTS video_upload_date String DEFAULT '',
   ADD COLUMN IF NOT EXISTS tms String DEFAULT '',
   ADD COLUMN IF NOT EXISTS director String DEFAULT '',
   ADD COLUMN IF NOT EXISTS season String DEFAULT '',
   ADD COLUMN IF NOT EXISTS episode_number String DEFAULT '',
   ADD COLUMN IF NOT EXISTS episode_title String DEFAULT '',
   ADD COLUMN IF NOT EXISTS release_date String DEFAULT '',
   ADD COLUMN IF NOT EXISTS hfa_song_code String DEFAULT '',
   ADD COLUMN IF NOT EXISTS iswc String DEFAULT '',
   ADD COLUMN IF NOT EXISTS writers String DEFAULT '',
   ADD COLUMN IF NOT EXISTS is_shorts_eligible String DEFAULT ''`,

  `ALTER TABLE youtube_raw_estimated_revenue
   ADD COLUMN IF NOT EXISTS country_code String DEFAULT '',
   ADD COLUMN IF NOT EXISTS estimated_playback_based_cpm Decimal64(10) DEFAULT 0,
   ADD COLUMN IF NOT EXISTS estimated_cpm Decimal64(10) DEFAULT 0`,

  `ALTER TABLE estimated_revenue_daily ADD INDEX IF NOT EXISTS idx_video_id video_id TYPE bloom_filter() GRANULARITY 1`,
  `ALTER TABLE estimated_revenue_daily ADD INDEX IF NOT EXISTS idx_asset_id asset_id TYPE bloom_filter() GRANULARITY 1`,
  `ALTER TABLE estimated_revenue_daily MATERIALIZE INDEX idx_video_id`,
  `ALTER TABLE estimated_revenue_daily MATERIALIZE INDEX idx_asset_id`,

  `CREATE TABLE IF NOT EXISTS video_countries_daily (
    cms_id               String        DEFAULT '',
    day                  Date,
    country              LowCardinality(String),
    video_id             String,
    views                UInt64        DEFAULT 0,
    watch_time_sec       UInt64        DEFAULT 0,
    ingested_at          DateTime      DEFAULT now()
  )
  ENGINE = MergeTree()
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, day, country, video_id)
  TTL day + INTERVAL 3 YEAR
  SETTINGS index_granularity = 8192`,

  `ALTER TABLE estimated_revenue_daily 
   ADD COLUMN IF NOT EXISTS claimed_status LowCardinality(String) DEFAULT '',
   ADD COLUMN IF NOT EXISTS claim_status LowCardinality(String) DEFAULT '',
   ADD COLUMN IF NOT EXISTS uploader_type LowCardinality(String) DEFAULT '',
   ADD COLUMN IF NOT EXISTS video_upload_date String DEFAULT '',
   ADD COLUMN IF NOT EXISTS genre LowCardinality(String) DEFAULT '',
   ADD COLUMN IF NOT EXISTS estimated_cpm Decimal(18, 10) DEFAULT 0,
   ADD COLUMN IF NOT EXISTS estimated_playback_based_cpm Decimal(18, 10) DEFAULT 0,
   ADD COLUMN IF NOT EXISTS likes UInt64 DEFAULT 0,
   ADD COLUMN IF NOT EXISTS comments UInt64 DEFAULT 0,
   ADD COLUMN IF NOT EXISTS shares UInt64 DEFAULT 0,
   ADD COLUMN IF NOT EXISTS dislikes UInt64 DEFAULT 0,
   ADD COLUMN IF NOT EXISTS watch_time_minutes Float64 DEFAULT 0.0,
   ADD COLUMN IF NOT EXISTS average_view_duration_seconds UInt32 DEFAULT 0,
   ADD COLUMN IF NOT EXISTS average_view_duration_percentage Float64 DEFAULT 0.0,
   ADD COLUMN IF NOT EXISTS subscribers_gained UInt64 DEFAULT 0,
   ADD COLUMN IF NOT EXISTS subscribers_lost UInt64 DEFAULT 0,
   ADD COLUMN IF NOT EXISTS custom_id String DEFAULT ''`,

  `CREATE TABLE IF NOT EXISTS audio_tier_revenue (
    cms_id                     String        DEFAULT '',
    upload_month               UInt32        DEFAULT 0,
    adjustment_type            LowCardinality(String) DEFAULT '',
    day                        Date,
    country                    LowCardinality(String),
    video_id                   String,
    asset_id                   String,
    channel_id                 String        DEFAULT '',
    asset_title                String        DEFAULT '',
    asset_labels               String        DEFAULT '',
    custom_id                  String        DEFAULT '',
    isrc                       String        DEFAULT '',
    upc                        String        DEFAULT '',
    grid                       String        DEFAULT '',
    artist                     String        DEFAULT '',
    album                      String        DEFAULT '',
    label                      LowCardinality(String) DEFAULT '',
    owned_views                UInt64        DEFAULT 0,
    yt_rev_total               Decimal64(10) DEFAULT 0,
    partner_rev_pro_rata       Decimal64(10) DEFAULT 0,
    partner_rev_per_play_min   Decimal64(10) DEFAULT 0,
    partner_rev_total          Decimal64(10) DEFAULT 0,
    us_tax                     Decimal64(10) DEFAULT 0,
    net_revenue                Decimal64(10) DEFAULT 0,
    ingested_at                DateTime      DEFAULT now()
  ) ENGINE = MergeTree()
  PARTITION BY (cms_id, upload_month)
  ORDER BY (cms_id, upload_month, day, asset_id, video_id, country)
  SETTINGS index_granularity = 8192`,

  `DROP VIEW IF EXISTS v_unified_revenue`,

  `CREATE VIEW IF NOT EXISTS v_unified_revenue AS
  SELECT cms_id, upload_month, day, video_id, asset_id, country,
         'ads' AS revenue_source, report_type AS sub_source,
         owned_views, yt_rev_total, partner_rev_total
  FROM ads_revenue_enriched
  UNION ALL
  SELECT cms_id, upload_month, day, video_id, asset_id, country,
         'subscription', 'subscription',
         owned_views, yt_rev_total, partner_rev_total
  FROM subscription_revenue
  UNION ALL
  SELECT cms_id, upload_month, day, video_id, asset_id, country,
         'audio_tier', 'audio_tier',
         owned_views, yt_rev_total, partner_rev_total
  FROM audio_tier_revenue`,

  // ── youtube_affiliate_tax_rates ───────────────────────────
  `CREATE TABLE IF NOT EXISTS youtube_affiliate_tax_rates (
    channel_id       String,
    revenue_source   String,
    tax_rate         Float64       DEFAULT 0.0,
    tax_amount       Float64       DEFAULT 0.0,
    upload_month     UInt32        DEFAULT 0
  ) ENGINE = ReplacingMergeTree()
  PRIMARY KEY (channel_id, revenue_source, upload_month)
  ORDER BY (channel_id, revenue_source, upload_month)`
];


export const GLOBAL_DDL = [
  // ── org_registry (Global registry for aggregator/org tenants) ────
  `CREATE TABLE IF NOT EXISTS org_registry (
    org_id         String,
    org_name       String DEFAULT '',
    org_key_hash   String DEFAULT '',
    is_active      UInt8  DEFAULT 1,
    created_at     DateTime DEFAULT now()
  )
  ENGINE = ReplacingMergeTree(created_at)
  ORDER BY (org_id)`,

  // ── cms_registry (Global registry for all CMS tenants) ────
  `CREATE TABLE IF NOT EXISTS cms_registry (
    cms_id         String,
    cms_name       String DEFAULT '',
    db_name        String DEFAULT '',
    api_key_hash   String DEFAULT '',
    org_id         String DEFAULT '',
    is_active      UInt8  DEFAULT 1,
    created_at     DateTime DEFAULT now()
  )
  ENGINE = ReplacingMergeTree(created_at)
  ORDER BY (cms_id)`,

  `CREATE TABLE IF NOT EXISTS ingestion_jobs (
    job_id         String,
    job_type       LowCardinality(String),
    cms_id         String DEFAULT '',
    status         LowCardinality(String) DEFAULT 'pending',
    month          UInt32,
    total_rows     UInt64  DEFAULT 0,
    processed_rows UInt64  DEFAULT 0,
    ads_rows       UInt64  DEFAULT 0,
    adj_ads_rows   UInt64  DEFAULT 0,
    sub_rows       UInt64  DEFAULT 0,
    adj_sub_rows   UInt64  DEFAULT 0,
    audio_tier_rows UInt64 DEFAULT 0,
    claims_rows    UInt64  DEFAULT 0,
    reach_rows     UInt64  DEFAULT 0,
    demo_rows      UInt64  DEFAULT 0,
    traffic_rows   UInt64  DEFAULT 0,
    device_rows    UInt64  DEFAULT 0,
    ads_revenue    Float64 DEFAULT 0.0,
    sub_revenue    Float64 DEFAULT 0.0,
    adj_ads_revenue Float64 DEFAULT 0.0,
    adj_sub_revenue Float64 DEFAULT 0.0,
    shorts_ads_rows UInt64  DEFAULT 0,
    shorts_sub_rows UInt64  DEFAULT 0,
    shorts_ads_revenue Float64 DEFAULT 0.0,
    shorts_sub_revenue Float64 DEFAULT 0.0,
    audio_tier_revenue Float64 DEFAULT 0.0,
    uploaded_by    String  DEFAULT '',
    error_message  String  DEFAULT '',
    started_at     DateTime DEFAULT now(),
    updated_at     DateTime DEFAULT now(),
    completed_at   Nullable(DateTime),
    detail_logs    String  DEFAULT '[]',
    is_fallback    UInt8   DEFAULT 0,
    fallback_date  String  DEFAULT ''
  )
  ENGINE = MergeTree()
  ORDER BY (job_id)`,

  // ── settlement_statement_lines (Global storage for frozen payouts) ──
  `CREATE TABLE IF NOT EXISTS settlement_statement_lines (
    settlement_id       String,
    period              String,
    recipient_id        String,
    channel_id          String DEFAULT '',
    track_title         String,
    track_artist        String,
    isrc                String DEFAULT '',
    upc                 String DEFAULT '',
    track_type          String DEFAULT 'Original',
    views               UInt64,
    gross_revenue_usd   Float64,
    share_pct           Float32,
    net_revenue_usd     Float64,
    net_revenue_idr     Float64,
    exchange_rate       Float32,
    creator_name        String DEFAULT '',
    created_at          DateTime DEFAULT now()
  )
  ENGINE = MergeTree()
  ORDER BY (settlement_id, recipient_id, period)`,

  // ── youtube_ingest_history (Tracking YouTube metadata for Smart Ingest) ──
  `CREATE TABLE IF NOT EXISTS youtube_ingest_history (
    cms_id       String,
    report_type  LowCardinality(String),
    day          Date,
    report_id    String,
    create_time  DateTime,
    ingested_at  DateTime DEFAULT now(),
    is_fallback  UInt8 DEFAULT 0
  )
  ENGINE = ReplacingMergeTree(ingested_at)
  ORDER BY (cms_id, report_type, day)`,

  // ── youtube_asset_metadata (Tracking YouTube Asset Metadata) ──
  `CREATE TABLE IF NOT EXISTS youtube_asset_metadata (
    cms_id       String DEFAULT '',
    day          Date,
    asset_id     String,
    asset_title  String DEFAULT '',
    artist       String DEFAULT '',
    album        String DEFAULT '',
    label        String DEFAULT '',
    isrc         String DEFAULT '',
    upc          String DEFAULT '',
    grid         String DEFAULT '',
    custom_id    String DEFAULT '',
    genre        String DEFAULT '',
    asset_labels String DEFAULT '',
    ingested_at  DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(ingested_at)
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, asset_id)`,

  // ── youtube_video_metadata (Tracking YouTube Video Metadata) ──
  `CREATE TABLE IF NOT EXISTS youtube_video_metadata (
    cms_id               String DEFAULT '',
    day                  Date,
    video_id             String,
    video_title          String DEFAULT '',
    channel_id           String DEFAULT '',
    channel_display_name String DEFAULT '',
    video_length_sec     UInt32 DEFAULT 0,
    category             String DEFAULT '',
    asset_id             String DEFAULT '',
    custom_id            String DEFAULT '',
    isrc                 String DEFAULT '',
    content_type         String DEFAULT '',
    ingested_at          DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(ingested_at)
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, video_id, asset_id)`,

  `CREATE TABLE IF NOT EXISTS youtube_raw_asset_estimated_revenue (
    cms_id                               String DEFAULT '',
    day                                  Date,
    asset_id                             String DEFAULT '',
    channel_id                           String DEFAULT '',
    estimated_partner_revenue            Decimal64(10) DEFAULT 0,
    estimated_partner_ad_revenue         Decimal64(10) DEFAULT 0,
    estimated_partner_red_revenue        Decimal64(10) DEFAULT 0,
    estimated_partner_transaction_revenue Decimal64(10) DEFAULT 0,
    ingested_at                          DateTime DEFAULT now()
  ) ENGINE = MergeTree()
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, day, asset_id)`,

  `CREATE TABLE IF NOT EXISTS youtube_raw_channel_estimated_revenue (
    cms_id                               String DEFAULT '',
    day                                  Date,
    channel_id                           String DEFAULT '',
    estimated_partner_revenue            Decimal64(10) DEFAULT 0,
    estimated_partner_ad_revenue         Decimal64(10) DEFAULT 0,
    estimated_partner_red_revenue        Decimal64(10) DEFAULT 0,
    estimated_partner_transaction_revenue Decimal64(10) DEFAULT 0,
    ingested_at                          DateTime DEFAULT now()
  ) ENGINE = MergeTree()
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, day, channel_id)`
];

/** Migrations for existing installations — safe to re-run (IF NOT EXISTS) */
export const GLOBAL_MIGRATIONS = [
  `ALTER TABLE cms_registry ADD COLUMN IF NOT EXISTS org_id String DEFAULT ''`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS claims_rows UInt64 DEFAULT 0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS reach_rows UInt64 DEFAULT 0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS demo_rows UInt64 DEFAULT 0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS traffic_rows UInt64 DEFAULT 0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS device_rows UInt64 DEFAULT 0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS ads_revenue Float64 DEFAULT 0.0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS sub_revenue Float64 DEFAULT 0.0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS adj_ads_revenue Float64 DEFAULT 0.0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS adj_sub_revenue Float64 DEFAULT 0.0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS shorts_ads_rows UInt64 DEFAULT 0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS shorts_sub_rows UInt64 DEFAULT 0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS shorts_ads_revenue Float64 DEFAULT 0.0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS shorts_sub_revenue Float64 DEFAULT 0.0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS paid_features_rows UInt64 DEFAULT 0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS paid_revenue Float64 DEFAULT 0.0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS audio_tier_rows UInt64 DEFAULT 0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS audio_tier_revenue Float64 DEFAULT 0.0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS us_tax Float64 DEFAULT 0.0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS net_revenue Float64 DEFAULT 0.0`,
  `CREATE TABLE IF NOT EXISTS youtube_ingest_history (
    cms_id       String,
    report_type  LowCardinality(String),
    day          Date,
    report_id    String,
    create_time  DateTime,
    ingested_at  DateTime DEFAULT now(),
    is_fallback  UInt8 DEFAULT 0
  )
  ENGINE = ReplacingMergeTree(ingested_at)
  ORDER BY (cms_id, report_type, day)`,
  `ALTER TABLE youtube_ingest_history ADD COLUMN IF NOT EXISTS is_fallback UInt8 DEFAULT 0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS is_fallback UInt8 DEFAULT 0`,
  `ALTER TABLE ingestion_jobs ADD COLUMN IF NOT EXISTS fallback_date String DEFAULT ''`,
  `CREATE TABLE IF NOT EXISTS youtube_asset_metadata (
    cms_id       String DEFAULT '',
    day          Date,
    asset_id     String,
    asset_title  String DEFAULT '',
    artist       String DEFAULT '',
    album        String DEFAULT '',
    label        String DEFAULT '',
    isrc         String DEFAULT '',
    upc          String DEFAULT '',
    grid         String DEFAULT '',
    custom_id    String DEFAULT '',
    genre        String DEFAULT '',
    asset_labels String DEFAULT '',
    ingested_at  DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(ingested_at)
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, asset_id)`,
  `CREATE TABLE IF NOT EXISTS youtube_video_metadata (
    cms_id               String DEFAULT '',
    day                  Date,
    video_id             String,
    video_title          String DEFAULT '',
    channel_id           String DEFAULT '',
    channel_display_name String DEFAULT '',
    video_length_sec     UInt32 DEFAULT 0,
    category             String DEFAULT '',
    asset_id             String DEFAULT '',
    custom_id            String DEFAULT '',
    isrc                 String DEFAULT '',
    content_type         String DEFAULT '',
    ingested_at          DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(ingested_at)
  PARTITION BY (cms_id, toYYYYMM(day))
  ORDER BY (cms_id, video_id, asset_id)`,

  `CREATE TABLE IF NOT EXISTS ingested_files_archive (
    cms_id         String,
    upload_month   UInt32,
    file_type      LowCardinality(String),
    file_name      String,
    file_path      String,
    file_size      UInt64,
    uploaded_at    DateTime DEFAULT now()
  ) ENGINE = ReplacingMergeTree(uploaded_at)
  ORDER BY (cms_id, upload_month, file_type)`,

  `CREATE TABLE IF NOT EXISTS raw_youtube_publisher_usage_reports (
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
  SETTINGS index_granularity = 8192`,

  `CREATE TABLE IF NOT EXISTS youtube_publisher_work_metadata (
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
];
