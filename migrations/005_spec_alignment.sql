-- EmpireAI schema alignment patch for v2.0.1-2
-- Safe/idempotent backfill for environments that already applied earlier migrations.

ALTER TABLE spend_ledger
  ADD COLUMN IF NOT EXISTS agent_id TEXT REFERENCES agents(id);

ALTER TABLE spend_ledger
  ADD COLUMN IF NOT EXISTS metadata JSONB;

ALTER TABLE spend_ledger
  ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'exact';

ALTER TABLE spend_ledger
  ALTER COLUMN source SET DEFAULT 'exact';

ALTER TABLE human_tasks
  ADD COLUMN IF NOT EXISTS tool_call_id TEXT;

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
