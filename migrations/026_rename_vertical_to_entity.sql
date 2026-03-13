BEGIN;

UPDATE mailbox
SET type = 'entity_approval'
WHERE type = 'vertical_approval';

UPDATE conversations
SET mode = 'session_per_entity'
WHERE mode = 'session_per_vertical';

ALTER TABLE IF EXISTS verticals RENAME TO entities;
ALTER TABLE IF EXISTS vertical_metrics RENAME TO entity_metrics;

ALTER TABLE IF EXISTS events RENAME COLUMN vertical_id TO entity_id;
ALTER TABLE IF EXISTS agents RENAME COLUMN vertical_id TO entity_id;
ALTER TABLE IF EXISTS template_migrations RENAME COLUMN vertical_id TO entity_id;
ALTER TABLE IF EXISTS routing_rules RENAME COLUMN vertical_id TO entity_id;
ALTER TABLE IF EXISTS mailbox RENAME COLUMN vertical_id TO entity_id;
ALTER TABLE IF EXISTS inbound_events RENAME COLUMN vertical_id TO entity_id;
ALTER TABLE IF EXISTS webhook_events RENAME COLUMN vertical_id TO entity_id;
ALTER TABLE IF EXISTS deployments RENAME COLUMN vertical_id TO entity_id;
ALTER TABLE IF EXISTS entity_metrics RENAME COLUMN vertical_id TO entity_id;
ALTER TABLE IF EXISTS spend_ledger RENAME COLUMN vertical_id TO entity_id;
ALTER TABLE IF EXISTS human_tasks RENAME COLUMN vertical_id TO entity_id;
ALTER TABLE IF EXISTS runtime_log RENAME COLUMN vertical_id TO entity_id;
ALTER TABLE IF EXISTS cycle_counters RENAME COLUMN vertical_id TO entity_id;
ALTER TABLE IF EXISTS scoring_digest_buffer RENAME COLUMN vertical_id TO entity_id;
ALTER TABLE IF EXISTS scoring_digest_buffer RENAME COLUMN vertical_name TO entity_name;
ALTER TABLE IF EXISTS validation_pipelines RENAME COLUMN vertical_id TO entity_id;
ALTER TABLE IF EXISTS scan_directives RENAME COLUMN vertical_ids TO entity_ids;

ALTER INDEX IF EXISTS idx_events_vertical RENAME TO idx_events_entity;
ALTER INDEX IF EXISTS idx_agents_vertical RENAME TO idx_agents_entity;
ALTER INDEX IF EXISTS idx_spend_vertical RENAME TO idx_spend_entity;
ALTER INDEX IF EXISTS idx_human_tasks_vertical RENAME TO idx_human_tasks_entity;
ALTER INDEX IF EXISTS idx_rlog_vertical RENAME TO idx_rlog_entity;
ALTER INDEX IF EXISTS idx_vertical_metrics_vertical_period_desc RENAME TO idx_entity_metrics_entity_period_desc;
ALTER INDEX IF EXISTS idx_deployments_vertical_env_version RENAME TO idx_deployments_entity_env_version;
ALTER INDEX IF EXISTS idx_vertical_metrics_period_end RENAME TO idx_entity_metrics_period_end;
ALTER INDEX IF EXISTS idx_verticals_stage RENAME TO idx_entities_stage;
ALTER INDEX IF EXISTS idx_verticals_mode RENAME TO idx_entities_mode;
ALTER INDEX IF EXISTS idx_verticals_geography RENAME TO idx_entities_geography;
ALTER INDEX IF EXISTS idx_verticals_slug_geo RENAME TO idx_entities_slug_geo;
ALTER INDEX IF EXISTS idx_verticals_parent RENAME TO idx_entities_parent;
ALTER INDEX IF EXISTS idx_verticals_depth RENAME TO idx_entities_depth;

DROP INDEX IF EXISTS idx_routing_rules_key;
DROP INDEX IF EXISTS idx_routing_rules_active_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_routing_rules_unique
    ON routing_rules(entity_id, event_pattern, subscriber_id)
    WHERE status = 'active';

ALTER TABLE IF EXISTS mailbox DROP CONSTRAINT IF EXISTS mailbox_type_check;
ALTER TABLE IF EXISTS mailbox
    ADD CONSTRAINT mailbox_type_check
    CHECK (type IN (
        'review',
        'escalation',
        'spend_request',
        'budget_increase',
        'digest',
        'entity_approval',
        'migration_approval',
        'domain_approval'
    ));

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'cycle_counters'::regclass
          AND conname = 'cycle_counters_vertical_id_fkey'
    ) THEN
        ALTER TABLE cycle_counters
            RENAME CONSTRAINT cycle_counters_vertical_id_fkey TO cycle_counters_entity_id_fkey;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'entities'::regclass
          AND conname = 'verticals_parent_id_fkey'
    ) THEN
        ALTER TABLE entities
            RENAME CONSTRAINT verticals_parent_id_fkey TO entities_parent_id_fkey;
    END IF;
END $$;

COMMIT;
