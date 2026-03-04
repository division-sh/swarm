-- v2.0.44 reconciliation: mailbox enum alignment + vertical derivation metadata.

-- ---------------------------------------------------------------------------
-- verticals: add derivation and opportunity metadata columns used by 2.0.44.
-- ---------------------------------------------------------------------------
ALTER TABLE IF EXISTS verticals
    ADD COLUMN IF NOT EXISTS opportunity_pattern TEXT,
    ADD COLUMN IF NOT EXISTS signal_sources JSONB DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS required_capabilities JSONB DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS parent_id UUID REFERENCES verticals(id),
    ADD COLUMN IF NOT EXISTS generation_depth INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS generator_agent_id TEXT,
    ADD COLUMN IF NOT EXISTS derivation_rationale JSONB;

UPDATE verticals
SET signal_sources = '[]'::jsonb
WHERE signal_sources IS NULL;

UPDATE verticals
SET required_capabilities = '{}'::jsonb
WHERE required_capabilities IS NULL;

ALTER TABLE IF EXISTS verticals
    ALTER COLUMN signal_sources SET DEFAULT '[]'::jsonb,
    ALTER COLUMN required_capabilities SET DEFAULT '{}'::jsonb,
    ALTER COLUMN generation_depth SET DEFAULT 0;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'verticals'::regclass
          AND conname = 'chk_generation_depth'
    ) THEN
        ALTER TABLE verticals
            ADD CONSTRAINT chk_generation_depth
            CHECK (generation_depth >= 0 AND generation_depth <= 2);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_verticals_parent
    ON verticals(parent_id)
    WHERE parent_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_verticals_depth
    ON verticals(generation_depth);

-- ---------------------------------------------------------------------------
-- mailbox: align check constraints with runtime enums (v2.0.42+).
-- ---------------------------------------------------------------------------
ALTER TABLE IF EXISTS mailbox DROP CONSTRAINT IF EXISTS mailbox_type_check;
ALTER TABLE IF EXISTS mailbox
    ADD CONSTRAINT mailbox_type_check
    CHECK (type IN (
        'review',
        'escalation',
        'spend_request',
        'budget_increase',
        'digest',
        'vertical_approval',
        'migration_approval',
        'domain_approval'
    ));

ALTER TABLE IF EXISTS mailbox DROP CONSTRAINT IF EXISTS mailbox_priority_check;
ALTER TABLE IF EXISTS mailbox
    ADD CONSTRAINT mailbox_priority_check
    CHECK (priority IN ('low', 'normal', 'high', 'critical'));
