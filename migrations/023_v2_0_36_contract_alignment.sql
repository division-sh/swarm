-- v2.0.36 reconciliation: align runtime-state tables/indexes with contracts/ddl-canonical.sql.
-- This migration upgrades existing local/dev databases created before canonical DDL convergence.

-- ---------------------------------------------------------------------------
-- runtime_config: remove legacy hash/applied_by, add config_path + created_at.
-- ---------------------------------------------------------------------------
ALTER TABLE IF EXISTS runtime_config
    ADD COLUMN IF NOT EXISTS config_path TEXT;

UPDATE runtime_config
SET config_path = COALESCE(NULLIF(config_path, ''), 'empireai.yaml')
WHERE COALESCE(config_path, '') = '';

ALTER TABLE IF EXISTS runtime_config
    ALTER COLUMN config_path SET NOT NULL;

ALTER TABLE IF EXISTS runtime_config
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE IF EXISTS runtime_config
    DROP COLUMN IF EXISTS config_hash,
    DROP COLUMN IF EXISTS applied_by;

-- ---------------------------------------------------------------------------
-- pipeline_receipts: result -> status+error, FK cascade, status index.
-- ---------------------------------------------------------------------------
ALTER TABLE IF EXISTS pipeline_receipts
    ADD COLUMN IF NOT EXISTS status TEXT;

UPDATE pipeline_receipts
SET status = COALESCE(NULLIF(status, ''), result, 'processed')
WHERE status IS NULL OR status = '';

ALTER TABLE IF EXISTS pipeline_receipts
    ALTER COLUMN status SET DEFAULT 'processed';

ALTER TABLE IF EXISTS pipeline_receipts
    ALTER COLUMN status SET NOT NULL;

ALTER TABLE IF EXISTS pipeline_receipts
    ADD COLUMN IF NOT EXISTS error TEXT;

ALTER TABLE IF EXISTS pipeline_receipts
    DROP COLUMN IF EXISTS result;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'pipeline_receipts'::regclass
          AND conname = 'pipeline_receipts_event_id_fkey'
    ) THEN
        ALTER TABLE pipeline_receipts DROP CONSTRAINT pipeline_receipts_event_id_fkey;
    END IF;
    ALTER TABLE pipeline_receipts
        ADD CONSTRAINT pipeline_receipts_event_id_fkey
        FOREIGN KEY (event_id) REFERENCES events(id) ON DELETE CASCADE;
END $$;

CREATE INDEX IF NOT EXISTS idx_pipeline_receipts_status
    ON pipeline_receipts(status)
    WHERE status <> 'processed';

-- ---------------------------------------------------------------------------
-- scan_accumulators: UUID/int/jsonb legacy columns -> text/int canonical columns.
-- ---------------------------------------------------------------------------
ALTER TABLE IF EXISTS scan_accumulators
    ALTER COLUMN scan_id TYPE TEXT USING scan_id::text;

ALTER TABLE IF EXISTS scan_accumulators
    ALTER COLUMN campaign_id TYPE TEXT USING campaign_id::text;

ALTER TABLE IF EXISTS scan_accumulators
    ADD COLUMN IF NOT EXISTS expected INT,
    ADD COLUMN IF NOT EXISTS complete INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS discovered INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS skipped INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'scan_accumulators' AND column_name = 'expected_agents'
    ) THEN
        UPDATE scan_accumulators SET expected = COALESCE(expected, expected_agents);
        ALTER TABLE scan_accumulators DROP COLUMN expected_agents;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'scan_accumulators' AND column_name = 'agents_complete'
    ) THEN
        UPDATE scan_accumulators SET complete = COALESCE(complete, agents_complete);
        ALTER TABLE scan_accumulators DROP COLUMN agents_complete;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'scan_accumulators' AND column_name = 'verticals_discovered'
    ) THEN
        UPDATE scan_accumulators SET discovered = COALESCE(discovered, verticals_discovered);
        ALTER TABLE scan_accumulators DROP COLUMN verticals_discovered;
    END IF;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'scan_accumulators' AND column_name = 'verticals_skipped'
    ) THEN
        UPDATE scan_accumulators SET skipped = COALESCE(skipped, verticals_skipped);
        ALTER TABLE scan_accumulators DROP COLUMN verticals_skipped;
    END IF;
END $$;

DO $$
DECLARE reports_type text;
BEGIN
    SELECT data_type
    INTO reports_type
    FROM information_schema.columns
    WHERE table_name = 'scan_accumulators' AND column_name = 'reports';

    IF reports_type = 'jsonb' THEN
        ALTER TABLE scan_accumulators ADD COLUMN IF NOT EXISTS reports_count INT NOT NULL DEFAULT 0;
        UPDATE scan_accumulators
        SET reports_count = CASE
            WHEN jsonb_typeof(reports) = 'array' THEN jsonb_array_length(reports)
            WHEN jsonb_typeof(reports) = 'number' THEN COALESCE((reports::text)::int, 0)
            ELSE 0
        END;
        ALTER TABLE scan_accumulators DROP COLUMN reports;
        ALTER TABLE scan_accumulators RENAME COLUMN reports_count TO reports;
    END IF;
END $$;

UPDATE scan_accumulators
SET completed_by = '{}'::jsonb
WHERE jsonb_typeof(completed_by) = 'array';

ALTER TABLE IF EXISTS scan_accumulators
    ALTER COLUMN expected SET NOT NULL,
    ALTER COLUMN expected DROP DEFAULT;

ALTER TABLE IF EXISTS scan_accumulators
    ALTER COLUMN reports SET NOT NULL,
    ALTER COLUMN reports SET DEFAULT 0;

ALTER TABLE IF EXISTS scan_accumulators
    ALTER COLUMN completed_by SET DEFAULT '{}'::jsonb;

UPDATE scan_accumulators
SET created_at = COALESCE(created_at, started_at, now()),
    updated_at = COALESCE(updated_at, completed_at, started_at, now());

-- ---------------------------------------------------------------------------
-- pending_dedup_candidates: rebuild PK/shape to canonical schema.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF to_regclass('public.pending_dedup_candidates') IS NOT NULL AND EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'pending_dedup_candidates' AND column_name = 'id'
    ) THEN
        ALTER TABLE pending_dedup_candidates RENAME TO pending_dedup_candidates_legacy_023;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS pending_dedup_candidates (
    dedup_event_id  TEXT PRIMARY KEY,
    scan_id         TEXT NOT NULL,
    campaign_id     TEXT NOT NULL,
    mode            TEXT NOT NULL,
    name            TEXT NOT NULL,
    geography       TEXT NOT NULL,
    discovery_mode  TEXT NOT NULL,
    signal_strength DOUBLE PRECISION NOT NULL,
    payload         JSONB NOT NULL,
    existing_id     TEXT,
    status          TEXT NOT NULL DEFAULT 'pending',
    CONSTRAINT valid_dedup_status CHECK (status IN ('pending', 'resolved_keep', 'resolved_merge', 'resolved_skip')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_dedup_pending ON pending_dedup_candidates(status)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_dedup_scan ON pending_dedup_candidates(scan_id);

INSERT INTO pending_dedup_candidates (
    dedup_event_id, scan_id, campaign_id, mode, name, geography,
    discovery_mode, signal_strength, payload, existing_id, status, created_at, resolved_at
)
SELECT
    COALESCE(NULLIF(l.dedup_event_id::text, ''), l.id::text),
    l.scan_id::text,
    COALESCE(sa.campaign_id, ''),
    COALESCE(NULLIF(l.discovery_mode, ''), 'saas_gap'),
    COALESCE(NULLIF(l.candidate->>'name', ''), NULLIF(l.candidate->>'vertical_name', ''), 'unknown'),
    COALESCE(NULLIF(l.geography, ''), 'unspecified'),
    COALESCE(NULLIF(l.discovery_mode, ''), 'saas_gap'),
    COALESCE(l.signal_strength::double precision, 0),
    COALESCE(l.candidate, '{}'::jsonb),
    NULLIF(l.existing_id::text, ''),
    COALESCE(NULLIF(l.status, ''), 'pending'),
    COALESCE(l.created_at, now()),
    l.resolved_at
FROM pending_dedup_candidates_legacy_023 l
LEFT JOIN scan_accumulators sa ON sa.scan_id = l.scan_id::text
ON CONFLICT (dedup_event_id) DO NOTHING;

DROP TABLE IF EXISTS pending_dedup_candidates_legacy_023;

-- ---------------------------------------------------------------------------
-- validation_pipelines: renamed gate fields + canonical payload/retry columns.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'validation_pipelines' AND column_name = 'g2_spec_approved'
    ) THEN
        ALTER TABLE validation_pipelines RENAME COLUMN g2_spec_approved TO g2_spec;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'validation_pipelines' AND column_name = 'g3_cto_approved'
    ) THEN
        ALTER TABLE validation_pipelines RENAME COLUMN g3_cto_approved TO g3_cto;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'validation_pipelines' AND column_name = 'g4_brand_ready'
    ) THEN
        ALTER TABLE validation_pipelines RENAME COLUMN g4_brand_ready TO g4_brand;
    END IF;
END $$;

ALTER TABLE IF EXISTS validation_pipelines
    ADD COLUMN IF NOT EXISTS scoring_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS packaging_requested BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS packaging_retries INT NOT NULL DEFAULT 0;

UPDATE validation_pipelines
SET research_payload = COALESCE(research_payload, '{}'::jsonb),
    spec_payload = COALESCE(spec_payload, '{}'::jsonb),
    cto_payload = COALESCE(cto_payload, '{}'::jsonb),
    brand_payload = COALESCE(brand_payload, '{}'::jsonb),
    scoring_payload = COALESCE(scoring_payload, '{}'::jsonb);

ALTER TABLE IF EXISTS validation_pipelines
    ALTER COLUMN research_payload SET DEFAULT '{}'::jsonb,
    ALTER COLUMN research_payload SET NOT NULL,
    ALTER COLUMN spec_payload SET DEFAULT '{}'::jsonb,
    ALTER COLUMN spec_payload SET NOT NULL,
    ALTER COLUMN cto_payload SET DEFAULT '{}'::jsonb,
    ALTER COLUMN cto_payload SET NOT NULL,
    ALTER COLUMN brand_payload SET DEFAULT '{}'::jsonb,
    ALTER COLUMN brand_payload SET NOT NULL,
    ALTER COLUMN scoring_payload SET DEFAULT '{}'::jsonb,
    ALTER COLUMN scoring_payload SET NOT NULL;

-- ---------------------------------------------------------------------------
-- template_prompt_drafts: canonical metadata columns.
-- ---------------------------------------------------------------------------
ALTER TABLE IF EXISTS template_prompt_drafts
    ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'api',
    ADD COLUMN IF NOT EXISTS notes TEXT,
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- ---------------------------------------------------------------------------
-- Index alignment: session/conversation unique indexes with COALESCE + mode filter.
-- ---------------------------------------------------------------------------
DROP INDEX IF EXISTS idx_sessions_active_scope;
DROP INDEX IF EXISTS idx_sessions_active;
CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_active
ON agent_sessions(agent_id, runtime_mode, COALESCE(scope_key, ''))
WHERE status = 'active';

DROP INDEX IF EXISTS idx_conversations_active_agent_mode_scope;
DROP INDEX IF EXISTS idx_conversations_scope;
CREATE UNIQUE INDEX IF NOT EXISTS idx_conversations_scope
ON conversations(agent_id, COALESCE(scope_key, ''), mode)
WHERE status = 'active';
