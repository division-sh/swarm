-- v2.0.19: session_per_vertical support via scope_key on conversations and session registry.

ALTER TABLE conversations
    ADD COLUMN IF NOT EXISTS scope_key TEXT;

ALTER TABLE agent_sessions
    ADD COLUMN IF NOT EXISTS scope_key TEXT;

-- Replace legacy unique constraints keyed only by (agent_id, mode/runtime_mode)
-- with scope-aware constraints.
DROP INDEX IF EXISTS idx_conversations_active_agent_mode;
CREATE UNIQUE INDEX IF NOT EXISTS idx_conversations_active_agent_mode_scope
ON conversations(agent_id, mode, (COALESCE(scope_key, '')))
WHERE status = 'active';

DROP INDEX IF EXISTS idx_sessions_active;
CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_active_scope
ON agent_sessions(agent_id, runtime_mode, (COALESCE(scope_key, '')))
WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_conversations_scope
ON conversations(agent_id, scope_key, status)
WHERE scope_key IS NOT NULL;

