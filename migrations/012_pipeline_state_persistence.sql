-- Spec v2.0.15 pipeline coordinator durable state tables.

CREATE TABLE IF NOT EXISTS scan_accumulators (
    scan_id       TEXT PRIMARY KEY,
    campaign_id   TEXT,
    mode          TEXT NOT NULL,
    geography     TEXT NOT NULL,
    expected      INT NOT NULL DEFAULT 1,
    completed_by  JSONB NOT NULL DEFAULT '{}'::jsonb,
    reports       INT NOT NULL DEFAULT 0,
    discovered    INT NOT NULL DEFAULT 0,
    skipped       INT NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pending_dedup_candidates (
    dedup_event_id  TEXT PRIMARY KEY,
    scan_id         TEXT NOT NULL,
    campaign_id     TEXT,
    mode            TEXT NOT NULL,
    geography       TEXT NOT NULL,
    name            TEXT NOT NULL,
    signal_strength DOUBLE PRECISION NOT NULL DEFAULT 0,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS validation_pipelines (
    vertical_id           UUID PRIMARY KEY,
    status                TEXT NOT NULL DEFAULT 'active',
    g1_research           BOOLEAN NOT NULL DEFAULT FALSE,
    g2_spec               BOOLEAN NOT NULL DEFAULT FALSE,
    g3_cto                BOOLEAN NOT NULL DEFAULT FALSE,
    g4_brand              BOOLEAN NOT NULL DEFAULT FALSE,
    research_payload      JSONB NOT NULL DEFAULT '{}'::jsonb,
    spec_payload          JSONB NOT NULL DEFAULT '{}'::jsonb,
    cto_payload           JSONB NOT NULL DEFAULT '{}'::jsonb,
    brand_payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    scoring_payload       JSONB NOT NULL DEFAULT '{}'::jsonb,
    revision_count        INT NOT NULL DEFAULT 0,
    inner_revision_count  INT NOT NULL DEFAULT 0,
    spec_version          INT NOT NULL DEFAULT 0,
    packaging_requested   BOOLEAN NOT NULL DEFAULT FALSE,
    packaging_requested_at TIMESTAMPTZ,
    packaging_retries     INT NOT NULL DEFAULT 0,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pipeline_processed_events (
    event_id      TEXT PRIMARY KEY,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_scan_accumulators_campaign
    ON scan_accumulators(campaign_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_pending_dedup_scan
    ON pending_dedup_candidates(scan_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_validation_pipelines_status
    ON validation_pipelines(status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_processed_events_at
    ON pipeline_processed_events(processed_at DESC);
