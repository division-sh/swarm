-- EmpireAI bootstrap schema aligned to v1.7 core data model

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS verticals (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name              TEXT NOT NULL,
    slug              TEXT UNIQUE,
    geography         TEXT NOT NULL,
    stage             TEXT NOT NULL DEFAULT 'discovered',
    CONSTRAINT valid_stage CHECK (stage IN (
      'discovered', 'scoring', 'shortlisted', 'marginal_review', 'researching',
      'mvp_speccing', 'spec_review', 'cto_spec_review', 'branding', 'ready_for_review',
      'approved', 'killed',
      'full_speccing', 'building', 'pre_launch', 'launched',
      'operating', 'expanding', 'winding_down'
    )),
    mode              TEXT NOT NULL DEFAULT 'factory',
    template_version  TEXT,
    raw_signals       JSONB,
    scores            JSONB,
    business_brief    JSONB,
    mvp_spec          JSONB,
    spec_review       JSONB,
    cto_feasibility   JSONB,
    brand             JSONB,
    validation_kit    JSONB,
    full_spec         JSONB,
    deploy_config     JSONB,
    live_url          TEXT,
    launch_targets    JSONB,
    credentials       JSONB,
    human_notes       TEXT,
    killed_at_stage   TEXT,
    kill_reason       TEXT,
    approved_at       TIMESTAMPTZ,
    launched_at       TIMESTAMPTZ,
    created_at        TIMESTAMPTZ DEFAULT now(),
    updated_at        TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_verticals_stage ON verticals(stage);
CREATE INDEX IF NOT EXISTS idx_verticals_mode ON verticals(mode);
CREATE INDEX IF NOT EXISTS idx_verticals_geography ON verticals(geography);

CREATE TABLE IF NOT EXISTS events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type            TEXT NOT NULL,
    source_agent    TEXT NOT NULL,
    task_id         UUID,
    vertical_id     UUID REFERENCES verticals(id),
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);
CREATE INDEX IF NOT EXISTS idx_events_vertical ON events(vertical_id);
CREATE INDEX IF NOT EXISTS idx_events_task ON events(task_id);
CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at);

CREATE TABLE IF NOT EXISTS agents (
    id               TEXT PRIMARY KEY,
    type             TEXT NOT NULL,
    role             TEXT NOT NULL,
    mode             TEXT NOT NULL DEFAULT 'factory',
    vertical_id      UUID REFERENCES verticals(id),
    parent_agent_id  TEXT REFERENCES agents(id),
    status           TEXT NOT NULL DEFAULT 'idle',
    current_task_id  UUID,
    coordinator_id   TEXT,
    config           JSONB NOT NULL,
    template_version TEXT,
    budget_envelope  NUMERIC,
    hired_by         TEXT,
    started_at       TIMESTAMPTZ DEFAULT now(),
    last_active_at   TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_agents_vertical ON agents(vertical_id);
CREATE INDEX IF NOT EXISTS idx_agents_mode ON agents(mode);
CREATE INDEX IF NOT EXISTS idx_agents_parent ON agents(parent_agent_id);

CREATE TABLE IF NOT EXISTS event_deliveries (
    event_id        UUID NOT NULL REFERENCES events(id),
    agent_id        TEXT NOT NULL REFERENCES agents(id),
    created_at      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (event_id, agent_id)
);

CREATE INDEX IF NOT EXISTS idx_deliveries_agent ON event_deliveries(agent_id);

CREATE TABLE IF NOT EXISTS event_receipts (
    event_id        UUID NOT NULL REFERENCES events(id),
    agent_id        TEXT NOT NULL REFERENCES agents(id),
    processed_at    TIMESTAMPTZ DEFAULT now(),
    status          TEXT NOT NULL DEFAULT 'processed',
    retry_count     INT NOT NULL DEFAULT 0,
    error           TEXT,
    PRIMARY KEY (event_id, agent_id)
);

CREATE INDEX IF NOT EXISTS idx_receipts_agent ON event_receipts(agent_id);
CREATE INDEX IF NOT EXISTS idx_receipts_agent_time ON event_receipts(agent_id, processed_at DESC);

CREATE TABLE IF NOT EXISTS org_templates (
    version          TEXT PRIMARY KEY,
    agents           JSONB NOT NULL,
    bootstrap_routes JSONB NOT NULL,
    seeded_routes    JSONB NOT NULL,
    created_by       TEXT NOT NULL,
    description      TEXT,
    created_at       TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS template_migrations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vertical_id     UUID NOT NULL REFERENCES verticals(id),
    from_version    TEXT NOT NULL,
    to_version      TEXT NOT NULL REFERENCES org_templates(version),
    plan            JSONB NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    mailbox_id      UUID,
    executed_at     TIMESTAMPTZ,
    error           TEXT,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS conversations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id        TEXT REFERENCES agents(id),
    task_id         UUID,
    mode            TEXT DEFAULT 'task',
    messages        JSONB NOT NULL,
    summary         TEXT,
    turn_count      INT DEFAULT 0,
    status          TEXT DEFAULT 'active',
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS agent_sessions (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id           TEXT NOT NULL REFERENCES agents(id),
    runtime_mode       TEXT NOT NULL,
    provider           TEXT NOT NULL DEFAULT 'anthropic',
    session_id         TEXT NOT NULL,
    status             TEXT NOT NULL DEFAULT 'active',
    turn_count         INT NOT NULL DEFAULT 0,
    checkpoint_summary TEXT,
    lock_owner         TEXT,
    lock_expires_at    TIMESTAMPTZ,
    last_used_at       TIMESTAMPTZ DEFAULT now(),
    created_at         TIMESTAMPTZ DEFAULT now(),
    rotated_at         TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_active
ON agent_sessions(agent_id, runtime_mode)
WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_sessions_last_used ON agent_sessions(last_used_at);
CREATE INDEX IF NOT EXISTS idx_sessions_lock_expiry ON agent_sessions(lock_expires_at)
WHERE lock_owner IS NOT NULL;

CREATE TABLE IF NOT EXISTS agent_turns (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id         TEXT NOT NULL REFERENCES agents(id),
    session_row_id   UUID NOT NULL REFERENCES agent_sessions(id),
    turn_index       INT NOT NULL,
    task_id          UUID,
    request_payload  JSONB,
    response_payload JSONB,
    parse_ok         BOOLEAN NOT NULL DEFAULT true,
    latency_ms       INT,
    retry_count      INT NOT NULL DEFAULT 0,
    error            TEXT,
    created_at       TIMESTAMPTZ DEFAULT now(),
    UNIQUE (session_row_id, turn_index)
);

CREATE INDEX IF NOT EXISTS idx_turns_agent_time ON agent_turns(agent_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_turns_parse_failures ON agent_turns(agent_id)
WHERE parse_ok = false;

CREATE TABLE IF NOT EXISTS mailbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID REFERENCES events(id),
    vertical_id     UUID REFERENCES verticals(id),
    from_agent      TEXT,
    type            TEXT NOT NULL,
    priority        TEXT DEFAULT 'normal',
    status          TEXT DEFAULT 'pending',
    context         JSONB NOT NULL,
    summary         TEXT,
    decision        TEXT,
    decision_notes  TEXT,
    timeout_at      TIMESTAMPTZ,
    notified        BOOLEAN DEFAULT false,
    created_at      TIMESTAMPTZ DEFAULT now(),
    decided_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_mailbox_pending ON mailbox(status) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_mailbox_critical ON mailbox(priority) WHERE priority = 'critical' AND status = 'pending';

-- Human tasks are asynchronous "real world" actions executed by the founder/operator.
CREATE TABLE IF NOT EXISTS human_tasks (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    requesting_agent  TEXT NOT NULL,
    vertical_id       UUID REFERENCES verticals(id),
    category          TEXT NOT NULL,
    description       TEXT NOT NULL,
    talking_points    JSONB,
    expected_value    TEXT,
    priority          TEXT DEFAULT 'medium',
    deadline          TIMESTAMPTZ,
    status            TEXT NOT NULL DEFAULT 'pending_review',
    CONSTRAINT valid_task_status CHECK (status IN (
      'pending_review', 'approved', 'rejected', 'deferred',
      'assigned', 'completed', 'expired'
    )),
    reviewed_at       TIMESTAMPTZ,
    review_decision   JSONB,
    assigned_to       TEXT,
    result            TEXT,
    outcome           TEXT,
    follow_up_needed  BOOLEAN DEFAULT false,
    requeue_count     INT DEFAULT 0,
    completed_at      TIMESTAMPTZ,
    created_at        TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_human_tasks_status ON human_tasks(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_human_tasks_reviewed ON human_tasks(reviewed_at DESC) WHERE reviewed_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_human_tasks_vertical ON human_tasks(vertical_id) WHERE vertical_id IS NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE constraint_name = 'fk_migration_mailbox'
          AND table_name = 'template_migrations'
    ) THEN
        ALTER TABLE template_migrations
            ADD CONSTRAINT fk_migration_mailbox
            FOREIGN KEY (mailbox_id) REFERENCES mailbox(id);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS schedules (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id        TEXT REFERENCES agents(id),
    vertical_id     UUID REFERENCES verticals(id),
    event_type      TEXT NOT NULL,
    mode            TEXT NOT NULL DEFAULT 'cron',
    cron_expr       TEXT,
    at_time         TIMESTAMPTZ,
    next_fire_at    TIMESTAMPTZ,
    payload         JSONB,
    active          BOOLEAN DEFAULT true,
    last_fired_at   TIMESTAMPTZ,
    cancelled_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_schedules_active ON schedules(active, next_fire_at) WHERE active = true;

CREATE TABLE IF NOT EXISTS geographies (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    country         TEXT NOT NULL,
    region          TEXT,
    scan_config     JSONB,
    last_scanned_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- Scan campaigns automate repeated scan runs across geographies (spec v2.0).
CREATE TABLE IF NOT EXISTS scan_campaigns (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    geography_id    UUID NOT NULL REFERENCES geographies(id),
    directive_id    UUID REFERENCES events(id) ON DELETE SET NULL,
    mode            TEXT NOT NULL,
    categories      TEXT[],
    priority        TEXT DEFAULT 'normal',
    status          TEXT NOT NULL DEFAULT 'queued', -- queued|active|completed|paused
    CONSTRAINT valid_campaign_status CHECK (status IN ('queued', 'active', 'completed', 'failed', 'paused')),
    discoveries     INT DEFAULT 0,
    rescan_interval TEXT,
    strategic_context JSONB,
    created_at      TIMESTAMPTZ DEFAULT now(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    deadline_at     TIMESTAMPTZ,
    next_rescan_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_scan_campaigns_status ON scan_campaigns(status);
CREATE INDEX IF NOT EXISTS idx_scan_campaigns_next_rescan ON scan_campaigns(next_rescan_at) WHERE next_rescan_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_scan_campaigns_directive ON scan_campaigns(directive_id) WHERE directive_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS inbound_events (
    provider_event_id TEXT NOT NULL,
    vertical_id       UUID NOT NULL REFERENCES verticals(id),
    provider          TEXT NOT NULL,
    received_at       TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (provider_event_id, vertical_id)
);

CREATE INDEX IF NOT EXISTS idx_inbound_events_age ON inbound_events(received_at);

CREATE TABLE IF NOT EXISTS deployments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vertical_id     UUID REFERENCES verticals(id),
    environment     TEXT NOT NULL DEFAULT 'production',
    version         INT NOT NULL DEFAULT 1,
    status          TEXT NOT NULL DEFAULT 'pending',
    url             TEXT,
    domain          TEXT,
    port            INT,
    binary_path     TEXT,
    migration_sql   TEXT,
    nginx_config    TEXT,
    db_schema       TEXT,
    deployed_by     TEXT REFERENCES agents(id),
    skip_staging    BOOLEAN DEFAULT false,
    health_status   TEXT DEFAULT 'unknown',
    deployed_at     TIMESTAMPTZ,
    last_health_at  TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE(vertical_id, environment, version)
);

CREATE TABLE IF NOT EXISTS technical_patterns (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pattern_type    TEXT NOT NULL,
    description     TEXT NOT NULL,
    vertical_ids    UUID[] NOT NULL,
    confidence      TEXT DEFAULT 'observed',
    cto_notes       TEXT,
    action_taken    TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS vertical_metrics (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vertical_id        UUID REFERENCES verticals(id),
    period_start       DATE NOT NULL,
    period_end         DATE NOT NULL,
    users_total        INT DEFAULT 0,
    users_new          INT DEFAULT 0,
    users_churned      INT DEFAULT 0,
    mrr_cents          INT DEFAULT 0,
    support_tickets    INT DEFAULT 0,
    bugs_reported      INT DEFAULT 0,
    bugs_fixed         INT DEFAULT 0,
    features_shipped   INT DEFAULT 0,
    outreach_sent      INT DEFAULT 0,
    outreach_responses INT DEFAULT 0,
    csat_avg           DECIMAL(3,2),
    api_cost_cents     INT DEFAULT 0,
    infra_cost_cents   INT DEFAULT 0,
    created_at         TIMESTAMPTZ DEFAULT now(),
    UNIQUE(vertical_id, period_start)
);

CREATE TABLE IF NOT EXISTS spend_ledger (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vertical_id   UUID REFERENCES verticals(id),
    agent_id      TEXT REFERENCES agents(id),
    category      TEXT NOT NULL,
    amount_cents  INT NOT NULL,
    currency      TEXT DEFAULT 'USD',
    description   TEXT,
    approved_by   TEXT,
    source        TEXT NOT NULL DEFAULT 'exact',
    metadata      JSONB,
    meta          JSONB,
    created_at    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_spend_vertical ON spend_ledger(vertical_id);

CREATE TABLE IF NOT EXISTS routing_rules (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vertical_id        UUID NOT NULL REFERENCES verticals(id),
    event_pattern      TEXT NOT NULL,
    subscriber_id      TEXT NOT NULL REFERENCES agents(id),
    installed_by       TEXT NOT NULL REFERENCES agents(id),
    reason             TEXT,
    status             TEXT NOT NULL DEFAULT 'active',
    source             TEXT NOT NULL DEFAULT 'bootstrap',
    bootstrap_version  INT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    deactivated_at     TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_routing_rules_key
ON routing_rules(vertical_id, event_pattern, subscriber_id);

CREATE TABLE IF NOT EXISTS bootstrap_versions (
    version      INT PRIMARY KEY,
    routes       JSONB NOT NULL,
    proposed_by  TEXT NOT NULL,
    approved_by  TEXT NOT NULL,
    evidence     TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS schema_version (
    version     INT PRIMARY KEY,
    name        TEXT NOT NULL,
    applied_at  TIMESTAMPTZ DEFAULT now()
);

-- Persist the last-known runtime YAML configuration for operator visibility.
CREATE TABLE IF NOT EXISTS runtime_config (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    config_path TEXT,
    config_yaml TEXT NOT NULL,
    created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_runtime_config_created ON runtime_config(created_at DESC);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'verticals'
          AND column_name = 'slug'
    ) THEN
        ALTER TABLE verticals ADD COLUMN slug TEXT;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'verticals'
          AND column_name = 'credentials'
    ) THEN
        ALTER TABLE verticals ADD COLUMN credentials JSONB;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'verticals'
          AND column_name = 'parked_at'
    ) THEN
        ALTER TABLE verticals ADD COLUMN parked_at TIMESTAMPTZ;
    END IF;
END $$;

WITH base AS (
    SELECT
        id,
        COALESCE(NULLIF(regexp_replace(lower(name), '[^a-z0-9]+', '-', 'g'), ''), 'vertical') AS bslug
    FROM verticals
    WHERE COALESCE(slug, '') = ''
),
ranked AS (
    SELECT
        id,
        bslug,
        ROW_NUMBER() OVER (PARTITION BY bslug ORDER BY id) AS rn
    FROM base
)
UPDATE verticals v
SET slug = CASE
    WHEN r.rn = 1 THEN r.bslug
    ELSE r.bslug || '-' || r.rn::text
END
FROM ranked r
WHERE v.id = r.id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_verticals_slug ON verticals(slug);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'deployments'
          AND column_name = 'environment'
    ) THEN
        ALTER TABLE deployments ADD COLUMN environment TEXT NOT NULL DEFAULT 'production';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'deployments'
          AND column_name = 'version'
    ) THEN
        ALTER TABLE deployments ADD COLUMN version INT NOT NULL DEFAULT 1;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'deployments'
          AND column_name = 'migration_sql'
    ) THEN
        ALTER TABLE deployments ADD COLUMN migration_sql TEXT;
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'deployments'
          AND column_name = 'deployed_by'
    ) THEN
        ALTER TABLE deployments ADD COLUMN deployed_by TEXT REFERENCES agents(id);
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'deployments'
          AND column_name = 'skip_staging'
    ) THEN
        ALTER TABLE deployments ADD COLUMN skip_staging BOOLEAN DEFAULT false;
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS idx_deployments_vertical_env_version
ON deployments(vertical_id, environment, version);
