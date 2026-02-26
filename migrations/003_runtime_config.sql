-- EmpireAI schema upgrades: runtime_config persistence for cold start (§11.0)

CREATE TABLE IF NOT EXISTS runtime_config (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    config_path TEXT,
    config_yaml TEXT NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_runtime_config_created ON runtime_config(created_at DESC);

