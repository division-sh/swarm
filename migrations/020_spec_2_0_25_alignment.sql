-- EmpireAI v2.0.25 alignment patch.
-- 1) Remove deprecated human_tasks.tool_call_id column.
-- 2) Reconcile scoring_digest_buffer to the v2.0.25 DDL contract.

ALTER TABLE IF EXISTS human_tasks
  DROP COLUMN IF EXISTS tool_call_id;

-- scoring_digest_buffer is an ephemeral audit buffer; rebuilding is safe.
DROP TABLE IF EXISTS scoring_digest_buffer;

CREATE TABLE IF NOT EXISTS scoring_digest_buffer (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vertical_id     UUID NOT NULL REFERENCES verticals(id),
    vertical_name   TEXT NOT NULL,
    geography       TEXT NOT NULL,
    composite       NUMERIC(5,2) NOT NULL,
    viability       NUMERIC(5,2),
    result          TEXT NOT NULL DEFAULT 'rejected',
    reason          TEXT NOT NULL,
    scored_at       TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_scoring_digest_buffer_time
  ON scoring_digest_buffer(scored_at);
