-- Spec v2.0.16 sharded execution runtime state.

CREATE TABLE IF NOT EXISTS shards (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    root_task_id    UUID NOT NULL,
    scan_id         UUID,
    stage           TEXT NOT NULL,
    shard_index     INT NOT NULL,
    shard_count     INT NOT NULL,
    shard_key       TEXT NOT NULL,
    scope           JSONB NOT NULL,
    agent_id        TEXT REFERENCES agents(id),
    status          TEXT NOT NULL DEFAULT 'pending',
    deadline_at     TIMESTAMPTZ NOT NULL,
    budget_cents    INT NOT NULL,
    spend_cents     INT NOT NULL DEFAULT 0,
    retry_count     INT NOT NULL DEFAULT 0,
    error           TEXT,
    assigned_at     TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT shards_valid_stage CHECK (stage IN ('market_research', 'trend_research')),
    CONSTRAINT shards_valid_status CHECK (status IN ('pending', 'assigned', 'completed', 'failed', 'timed_out'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_shards_idempotent
    ON shards(root_task_id, shard_key);
CREATE INDEX IF NOT EXISTS idx_shards_root
    ON shards(root_task_id);
CREATE INDEX IF NOT EXISTS idx_shards_status
    ON shards(status)
    WHERE status IN ('pending', 'assigned');
CREATE INDEX IF NOT EXISTS idx_shards_deadline
    ON shards(deadline_at)
    WHERE status = 'assigned';
