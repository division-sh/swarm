-- Hot path indexes for runtime/dashboard queries.

CREATE INDEX IF NOT EXISTS idx_conversations_active_lookup
ON conversations(agent_id, mode, updated_at DESC)
WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_mailbox_status_created
ON mailbox(status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_spend_created_at
ON spend_ledger(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_vertical_metrics_vertical_period_desc
ON vertical_metrics(vertical_id, period_end DESC);
