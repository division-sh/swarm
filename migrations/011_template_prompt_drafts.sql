-- Template prompt draft workspace for /api/templates/* hot-reload flows.

CREATE TABLE IF NOT EXISTS template_prompt_drafts (
    role        TEXT PRIMARY KEY,
    prompt      TEXT NOT NULL,
    source      TEXT NOT NULL DEFAULT 'api',
    notes       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_template_prompt_drafts_updated_at
    ON template_prompt_drafts(updated_at DESC);

