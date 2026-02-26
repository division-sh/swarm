-- Normalize session provider naming to spec convention.
-- Safe/idempotent for already-initialized environments.

ALTER TABLE agent_sessions
  ALTER COLUMN provider SET DEFAULT 'anthropic';

UPDATE agent_sessions
SET provider = 'anthropic'
WHERE provider = 'anthropic_api';
