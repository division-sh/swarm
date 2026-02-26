-- Spec v2.0.14 hardening: interceptor/pipeline recovery receipts.
-- Tracks which persisted events have been successfully re-routed through
-- runtime publish/replay so crash recovery can replay only missing events.

CREATE TABLE IF NOT EXISTS pipeline_receipts (
    event_id       UUID PRIMARY KEY REFERENCES events(id) ON DELETE CASCADE,
    status         TEXT NOT NULL DEFAULT 'processed',
    error          TEXT,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_receipts_processed_at
    ON pipeline_receipts(processed_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_receipts_status
    ON pipeline_receipts(status, processed_at DESC);

-- Backfill existing events as processed so first deploy of this migration
-- does not replay historical events.
INSERT INTO pipeline_receipts (event_id, status, processed_at)
SELECT e.id, 'processed', now()
FROM events e
ON CONFLICT (event_id) DO NOTHING;
