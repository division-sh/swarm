-- v2.0.34 canonical runtime schema alignment.
-- This migration targets existing developer databases that were initialized
-- before the canonical DDL rewrite. Runtime tables are recreated to match
-- contracts/ddl-canonical.sql.

-- Rebuild runtime-internal tables (safe in local/dev; runtime state only).
DROP TABLE IF EXISTS pending_dedup_candidates;
DROP TABLE IF EXISTS scan_accumulators;
DROP TABLE IF EXISTS validation_pipelines;
DROP TABLE IF EXISTS pipeline_processed_events;
DROP TABLE IF EXISTS pipeline_receipts;
DROP TABLE IF EXISTS runtime_config;
DROP TABLE IF EXISTS template_prompt_drafts;

CREATE TABLE IF NOT EXISTS runtime_config (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    config_yaml     TEXT NOT NULL,
    config_hash     TEXT NOT NULL,
    applied_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    applied_by      TEXT NOT NULL DEFAULT 'system'
);

CREATE TABLE IF NOT EXISTS pipeline_receipts (
    event_id        UUID PRIMARY KEY REFERENCES events(id),
    result          TEXT NOT NULL DEFAULT 'processed',
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_receipts_time ON pipeline_receipts(processed_at DESC);

CREATE TABLE IF NOT EXISTS scan_accumulators (
    scan_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id          UUID NOT NULL REFERENCES scan_campaigns(id),
    mode                 TEXT NOT NULL,
    geography            TEXT NOT NULL,
    expected_agents      INT NOT NULL,
    agents_complete      INT NOT NULL DEFAULT 0,
    completed_by         JSONB NOT NULL DEFAULT '[]',
    reports              JSONB NOT NULL DEFAULT '[]',
    verticals_discovered INT NOT NULL DEFAULT 0,
    verticals_skipped    INT NOT NULL DEFAULT 0,
    pending_dedup        INT NOT NULL DEFAULT 0,
    timeout_at           TIMESTAMPTZ NOT NULL,
    started_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_accum_campaign ON scan_accumulators(campaign_id);
CREATE INDEX IF NOT EXISTS idx_accum_timeout ON scan_accumulators(timeout_at) WHERE completed_at IS NULL;

CREATE TABLE IF NOT EXISTS pending_dedup_candidates (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    scan_id         UUID NOT NULL REFERENCES scan_accumulators(scan_id),
    candidate       JSONB NOT NULL,
    existing_id     UUID NOT NULL REFERENCES verticals(id),
    dedup_event_id  UUID,
    signal_strength INT NOT NULL,
    geography       TEXT NOT NULL,
    discovery_mode  TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    CONSTRAINT valid_dedup_status CHECK (status IN ('pending', 'resolved_keep', 'resolved_merge', 'resolved_skip')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_dedup_pending ON pending_dedup_candidates(status) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_dedup_scan ON pending_dedup_candidates(scan_id);

CREATE TABLE IF NOT EXISTS validation_pipelines (
    vertical_id           UUID PRIMARY KEY REFERENCES verticals(id),
    status                TEXT NOT NULL DEFAULT 'active',
    CONSTRAINT valid_vp_status CHECK (status IN ('active', 'rejected', 'packaged', 'parked', 'approved')),
    g1_research           BOOLEAN NOT NULL DEFAULT FALSE,
    g2_spec_approved      BOOLEAN NOT NULL DEFAULT FALSE,
    g3_cto_approved       BOOLEAN NOT NULL DEFAULT FALSE,
    g4_brand_ready        BOOLEAN NOT NULL DEFAULT FALSE,
    research_payload      JSONB,
    spec_payload          JSONB,
    cto_payload           JSONB,
    brand_payload         JSONB,
    revision_count        INT NOT NULL DEFAULT 0,
    inner_revision_count  INT NOT NULL DEFAULT 0,
    spec_version          INT NOT NULL DEFAULT 0,
    packaging_requested_at TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_vp_status ON validation_pipelines(status) WHERE status = 'active';

CREATE TABLE IF NOT EXISTS pipeline_processed_events (
    event_id        UUID PRIMARY KEY REFERENCES events(id),
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS template_prompt_drafts (
    role            TEXT PRIMARY KEY,
    prompt          TEXT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- scan_campaigns.strategic_context TEXT -> JSONB (legacy DBs).
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'scan_campaigns'
          AND column_name = 'strategic_context'
          AND data_type <> 'jsonb'
    ) THEN
        ALTER TABLE scan_campaigns
            ALTER COLUMN strategic_context TYPE JSONB
            USING CASE
                WHEN strategic_context IS NULL OR strategic_context = '' THEN '{}'::jsonb
                WHEN strategic_context ~ '^\s*[\{\[]' THEN strategic_context::jsonb
                ELSE jsonb_build_object('raw', strategic_context)
            END;
    END IF;
END $$;

-- shards.status CHECK constraint.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'valid_shard_status'
          AND conrelid = 'shards'::regclass
    ) THEN
        ALTER TABLE shards
            ADD CONSTRAINT valid_shard_status
            CHECK (status IN ('pending', 'assigned', 'completed', 'failed', 'timed_out'));
    END IF;
END $$;

-- spend_ledger.agent_id FK -> agents(id).
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'spend_ledger'::regclass
          AND contype = 'f'
          AND conname = 'spend_ledger_agent_id_fkey'
    ) THEN
        ALTER TABLE spend_ledger
            ADD CONSTRAINT spend_ledger_agent_id_fkey
            FOREIGN KEY (agent_id) REFERENCES agents(id);
    END IF;
END $$;

-- prompt_overrides.agent_id ON DELETE CASCADE.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'prompt_overrides'::regclass
          AND conname = 'prompt_overrides_agent_id_fkey'
    ) THEN
        ALTER TABLE prompt_overrides DROP CONSTRAINT prompt_overrides_agent_id_fkey;
    END IF;
    ALTER TABLE prompt_overrides
        ADD CONSTRAINT prompt_overrides_agent_id_fkey
        FOREIGN KEY (agent_id) REFERENCES agents(id) ON DELETE CASCADE;
END $$;

-- cycle_counters.vertical_id ON DELETE CASCADE.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'cycle_counters'::regclass
          AND conname = 'cycle_counters_vertical_id_fkey'
    ) THEN
        ALTER TABLE cycle_counters DROP CONSTRAINT cycle_counters_vertical_id_fkey;
    END IF;
    ALTER TABLE cycle_counters
        ADD CONSTRAINT cycle_counters_vertical_id_fkey
        FOREIGN KEY (vertical_id) REFERENCES verticals(id) ON DELETE CASCADE;
END $$;
