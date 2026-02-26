-- Spec v2.0.13 / v2.0.14 diagnostics foundations:
-- - pipeline_transitions: interceptor/runtime transition audit trail
-- - runtime_log: structured runtime operations log

CREATE TABLE IF NOT EXISTS pipeline_transitions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID NOT NULL REFERENCES events(id),
    event_type      TEXT NOT NULL,
    handler         TEXT NOT NULL,
    pipeline_type   TEXT NOT NULL,
    pipeline_id     UUID NOT NULL,
    action          TEXT NOT NULL,
    state_before    JSONB,
    state_after     JSONB,
    events_emitted  TEXT[],
    drop_reason     TEXT,
    error           TEXT,
    duration_us     INT,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pt_pipeline
    ON pipeline_transitions(pipeline_type, pipeline_id, created_at);
CREATE INDEX IF NOT EXISTS idx_pt_event
    ON pipeline_transitions(event_id);
CREATE INDEX IF NOT EXISTS idx_pt_drops
    ON pipeline_transitions(action)
    WHERE action = 'dropped';
CREATE INDEX IF NOT EXISTS idx_pt_errors
    ON pipeline_transitions(action)
    WHERE action = 'error';

CREATE TABLE IF NOT EXISTS runtime_log (
    id              BIGSERIAL PRIMARY KEY,
    ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
    level           TEXT NOT NULL,
    component       TEXT NOT NULL,
    action          TEXT NOT NULL,
    event_id        UUID,
    event_type      TEXT,
    agent_id        TEXT,
    vertical_id     UUID,
    campaign_id     UUID,
    scan_id         UUID,
    session_id      UUID,
    detail          JSONB,
    error           TEXT,
    duration_us     INT
);

CREATE INDEX IF NOT EXISTS idx_rlog_time
    ON runtime_log(ts DESC);
CREATE INDEX IF NOT EXISTS idx_rlog_component
    ON runtime_log(component, ts DESC);
CREATE INDEX IF NOT EXISTS idx_rlog_level
    ON runtime_log(level, ts DESC)
    WHERE level IN ('warn', 'error', 'fatal');
CREATE INDEX IF NOT EXISTS idx_rlog_event
    ON runtime_log(event_id)
    WHERE event_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_rlog_agent
    ON runtime_log(agent_id, ts DESC)
    WHERE agent_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_rlog_vertical
    ON runtime_log(vertical_id, ts DESC)
    WHERE vertical_id IS NOT NULL;
