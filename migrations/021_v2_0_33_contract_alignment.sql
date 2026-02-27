-- v2.0.33 contract alignment
-- - enforce per-geography slug uniqueness contract
-- - enforce active-only uniqueness for routing rules

-- Slugs are contractually required and uniqueness is scoped by geography.
UPDATE verticals
SET slug = COALESCE(NULLIF(slug, ''), regexp_replace(lower(name), '[^a-z0-9]+', '-', 'g'))
WHERE COALESCE(slug, '') = '';

ALTER TABLE verticals
    ALTER COLUMN slug SET NOT NULL;

ALTER TABLE verticals
    DROP CONSTRAINT IF EXISTS verticals_slug_key;

DROP INDEX IF EXISTS idx_verticals_slug;
CREATE UNIQUE INDEX IF NOT EXISTS idx_verticals_slug_geo
    ON verticals(slug, geography);

-- Allow historical/deactivated route rows while enforcing one active rule per key.
DROP INDEX IF EXISTS idx_routing_rules_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_routing_rules_active_key
    ON routing_rules(vertical_id, event_pattern, subscriber_id)
    WHERE status = 'active';
