CREATE TABLE IF NOT EXISTS cycle_counters (
    vertical_id   UUID NOT NULL REFERENCES verticals(id) ON DELETE CASCADE,
    event_pattern TEXT NOT NULL,
    count         INT NOT NULL DEFAULT 0,
    window_start  TIMESTAMPTZ NOT NULL,
    last_emitter  TEXT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (vertical_id, event_pattern)
);

CREATE INDEX IF NOT EXISTS idx_cycle_counters_updated_at ON cycle_counters(updated_at DESC);
