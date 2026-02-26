-- Prompt override hot-reload support (spec v2.0.3/v2.0.4)
-- Allows per-agent runtime prompt overrides without mutating org template data.

CREATE TABLE IF NOT EXISTS prompt_overrides (
    agent_id        TEXT PRIMARY KEY REFERENCES agents(id) ON DELETE CASCADE,
    prompt          TEXT NOT NULL,
    previous_prompt TEXT,
    source          TEXT NOT NULL DEFAULT 'dashboard', -- dashboard | cli | api
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

