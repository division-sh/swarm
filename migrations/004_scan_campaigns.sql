-- Scan campaign queue (spec v2.0 GAP 3)
-- Tracks queued, active, and completed scan campaigns. Empire Coordinator (or
-- runtime scheduler) creates campaigns; Discovery Coordinator executes them.

CREATE TABLE IF NOT EXISTS scan_campaigns (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    geography_id    UUID NOT NULL REFERENCES geographies(id),
    directive_id    UUID REFERENCES events(id) ON DELETE SET NULL,
    mode            TEXT NOT NULL,      -- local_services | saas_gap | saas_trend
    categories      TEXT[],             -- NULL = full taxonomy; or specific categories
    priority        TEXT NOT NULL DEFAULT 'normal',  -- high | normal | low
    status          TEXT NOT NULL DEFAULT 'queued',
    CONSTRAINT valid_campaign_status CHECK (status IN ('queued', 'active', 'completed', 'failed', 'paused')),
    discoveries     INT DEFAULT 0,      -- Count from scan.completed
    rescan_interval TEXT,               -- NULL = one-shot, or '30d', '90d' for periodic
    strategic_context JSONB,
    created_at      TIMESTAMPTZ DEFAULT now(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    deadline_at     TIMESTAMPTZ,
    next_rescan_at  TIMESTAMPTZ         -- Scheduled after completion
);

CREATE INDEX IF NOT EXISTS idx_scan_campaigns_status ON scan_campaigns(status);
CREATE INDEX IF NOT EXISTS idx_scan_campaigns_next_rescan
    ON scan_campaigns(next_rescan_at)
    WHERE next_rescan_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_scan_campaigns_directive
    ON scan_campaigns(directive_id)
    WHERE directive_id IS NOT NULL;
