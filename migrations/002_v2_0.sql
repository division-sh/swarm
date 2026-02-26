-- EmpireAI schema upgrades for spec v2.0
-- Applied via managed migrations (schema_version).

-- Verticals: discovery/scoring metadata + marginal parking
ALTER TABLE verticals
  ADD COLUMN IF NOT EXISTS discovery_mode TEXT;

ALTER TABLE verticals
  ADD COLUMN IF NOT EXISTS scoring_rubric TEXT;

ALTER TABLE verticals
  ADD COLUMN IF NOT EXISTS parked_at TIMESTAMPTZ;

-- Spend ledger: allow approximate vs exact spend tracking
ALTER TABLE spend_ledger
  ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'exact';

ALTER TABLE spend_ledger
  ADD COLUMN IF NOT EXISTS meta JSONB;

ALTER TABLE spend_ledger
  ADD COLUMN IF NOT EXISTS metadata JSONB;

ALTER TABLE spend_ledger
  ADD COLUMN IF NOT EXISTS agent_id TEXT REFERENCES agents(id);

ALTER TABLE spend_ledger
  ALTER COLUMN source SET DEFAULT 'exact';

-- Human task queue (§14)
CREATE TABLE IF NOT EXISTS human_tasks (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    requesting_agent    TEXT NOT NULL,
    vertical_id         UUID REFERENCES verticals(id),
    category            TEXT NOT NULL,
    description         TEXT NOT NULL,
    talking_points      JSONB,
    expected_value      TEXT,
    priority            TEXT NOT NULL DEFAULT 'medium',
    deadline            TIMESTAMPTZ,
    status              TEXT NOT NULL DEFAULT 'pending_review',
    CONSTRAINT valid_task_status CHECK (status IN (
      'pending_review', 'approved', 'rejected', 'deferred',
      'assigned', 'completed', 'expired'
    )),
    review_decision     JSONB,
    assigned_to         TEXT,
    result              TEXT,
    outcome             TEXT,
    follow_up_needed    BOOLEAN DEFAULT false,
    requeue_count       INT DEFAULT 0,
    created_at          TIMESTAMPTZ DEFAULT now(),
    reviewed_at         TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.table_constraints
    WHERE table_name = 'human_tasks'
      AND constraint_name = 'valid_task_status'
  ) THEN
    ALTER TABLE human_tasks
      ADD CONSTRAINT valid_task_status CHECK (status IN (
        'pending_review', 'approved', 'rejected', 'deferred',
        'assigned', 'completed', 'expired'
      ));
  END IF;
END $$;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM information_schema.table_constraints
    WHERE table_name = 'scan_campaigns'
      AND constraint_name = 'valid_campaign_status'
  ) THEN
    ALTER TABLE scan_campaigns
      ADD CONSTRAINT valid_campaign_status CHECK (status IN ('queued', 'active', 'completed', 'failed', 'paused'));
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_human_tasks_status ON human_tasks(status);
CREATE INDEX IF NOT EXISTS idx_human_tasks_vertical ON human_tasks(vertical_id);
CREATE INDEX IF NOT EXISTS idx_human_tasks_category ON human_tasks(category);
