-- Ensure only one active conversation exists per (agent_id, mode).
-- First, resolve any legacy duplicates by keeping the most recently updated row active.
WITH ranked AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY agent_id, mode
            ORDER BY updated_at DESC, created_at DESC, id DESC
        ) AS rn
    FROM conversations
    WHERE status = 'active'
)
UPDATE conversations c
SET
    status = 'superseded',
    updated_at = now()
FROM ranked r
WHERE c.id = r.id
  AND r.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS idx_conversations_active_agent_mode
ON conversations(agent_id, mode)
WHERE status = 'active';
