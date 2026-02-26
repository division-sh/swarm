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
