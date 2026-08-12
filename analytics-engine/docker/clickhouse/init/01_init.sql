-- ============================================================
-- Central Analytics API — ClickHouse DDL Init Script
-- Creates: cms_registry (in default DB)
-- Per-CMS databases are created dynamically via CMS Registry API
-- ============================================================

-- ── CMS Registry (global, in default database) ─────────────
CREATE TABLE IF NOT EXISTS default.cms_registry
(
    cms_id       String,
    cms_name     String,
    db_name      String,
    api_key_hash String,
    is_active    UInt8        DEFAULT 1,
    created_at   DateTime     DEFAULT now()
)
ENGINE = ReplacingMergeTree(created_at)
ORDER BY (cms_id);
