-- Spec v2.0.15 campaign context alignment.
-- Adds directive linkage + strategic context persistence for campaign lifecycle.

ALTER TABLE scan_campaigns
  ADD COLUMN IF NOT EXISTS directive_id UUID REFERENCES events(id) ON DELETE SET NULL;

ALTER TABLE scan_campaigns
  ADD COLUMN IF NOT EXISTS strategic_context JSONB;

ALTER TABLE scan_campaigns
  ADD COLUMN IF NOT EXISTS deadline_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_scan_campaigns_directive
  ON scan_campaigns(directive_id)
  WHERE directive_id IS NOT NULL;

