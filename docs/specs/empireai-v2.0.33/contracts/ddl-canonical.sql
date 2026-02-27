-- EmpireAI Canonical DDL — authoritative schema definition
-- Generated from spec v2.0.28, v2.0.33
--
-- This file is AUTHORITATIVE. `empire init` executes this directly.
-- If the spec prose disagrees with this file, this file wins.
--
-- Execution order: tables ordered for FK dependency resolution.
-- routing_rules and bootstrap_versions execute after verticals + agents.
-- Deferred FKs added via ALTER TABLE after all tables created.
--
-- FIX (from spec v2.0.28 audit): deployments.deployed_by changed from
-- UUID to TEXT to match agents.id type. Original spec had type mismatch.

-- ===================================================================
-- BOOTSTRAP
-- ===================================================================

CREATE TABLE IF NOT EXISTS schema_version (
    version     INT PRIMARY KEY,
    name        TEXT NOT NULL,
    applied_at  TIMESTAMPTZ DEFAULT now()
);

-- ===================================================================
-- CORE TABLES (§8.1)
-- ===================================================================

-- Verticals: the central business object
CREATE TABLE verticals (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name              TEXT NOT NULL,
    slug              TEXT NOT NULL,           -- URL-safe identifier, unique per geography
    geography         TEXT NOT NULL,
    stage             TEXT NOT NULL DEFAULT 'discovered',
    -- Factory stages: discovered → scoring → shortlisted → researching →
    --   mvp_speccing → spec_review → cto_spec_review → branding → ready_for_review
    -- Marginal path: scoring → marginal_review → researching (or rejected)
    -- Decision stages: approved → killed
    -- Operating stages: full_speccing → building → pre_launch → launched →
    --   operating → expanding → winding_down
    -- More-data loop: ready_for_review → researching (back to research)
    CONSTRAINT valid_stage CHECK (stage IN (
      'discovered', 'scoring', 'shortlisted', 'marginal_review', 'researching',
      'mvp_speccing', 'spec_review', 'cto_spec_review', 'branding', 'ready_for_review',
      'approved', 'killed',
      'full_speccing', 'building', 'pre_launch', 'launched',
      'operating', 'expanding', 'winding_down'
    )),
    -- NOTE: This CHECK prevents invalid stage VALUES, not invalid TRANSITIONS.
    -- Valid transition graph is enforced at the runtime level via StageTransition():
    --   runtime checks (current_stage, new_stage) against allowed transitions map.
    --   Invalid transitions return error; agent cannot skip stages.
    -- DB enforcement of transitions would require a trigger or stored procedure,
    -- which adds complexity for marginal benefit since all stage writes go through
    -- the runtime anyway. If an agent somehow bypasses the runtime (direct SQL),
    -- the CHECK constraint catches invalid values but not invalid jumps.
    --
    -- Valid transition graph (enforced in Go via StageTransition()):
    -- Factory: discovered→scoring→{shortlisted,marginal_review}
    --          shortlisted→researching, marginal_review→{researching,killed}
    --          researching→mvp_speccing→spec_review→cto_spec_review→branding→ready_for_review
    --          ready_for_review→{approved,killed,researching(more-data loop)}
    -- Operating: approved→full_speccing→building→pre_launch→launched→operating→{expanding,winding_down}
    --   full_speccing→building requires spec.validation_passed from Spec Auditor
    -- Terminal: killed (reachable from any stage except launched/operating/expanding)
    -- Backward: ready_for_review→researching (more-data), expanding→operating (contraction)
    mode              TEXT NOT NULL DEFAULT 'factory',  -- factory | operating
    discovery_mode    TEXT,                              -- How this vertical was discovered: local_services | saas_gap | saas_trend | manual (human directive)
    scoring_rubric    TEXT,                              -- Which scoring rubric was used: local_services | saas (derived from discovery_mode)
    template_version  TEXT,                              -- Org template version used at spinup (NULL for factory-stage)
    raw_signals       JSONB,
    scores            JSONB,
    business_brief    JSONB,
    mvp_spec          JSONB,          -- Lightweight spec from factory
    spec_review       JSONB,
    cto_feasibility   JSONB,          -- CTO feasibility assessment from factory
    brand             JSONB,          -- Chosen brand: name, domain, handles, colors
    validation_kit    JSONB,
    -- Operating mode fields (populated after approval)
    full_spec         JSONB,          -- Full spec from OpCo PM agent (operating mode)
    deploy_config     JSONB,          -- Populated by OpCo CTO agent during build
    live_url          TEXT,            -- Populated by OpCo CTO agent after deploy
    launch_targets    JSONB,           -- 2-3 concrete goals from mandate for first 30 days
    credentials       JSONB,           -- Per-vertical secrets: WhatsApp, MercadoPago, etc. (encrypted at rest via pgcrypto, see §13.1)
    human_notes       TEXT,
    killed_at_stage   TEXT,
    kill_reason       TEXT,
    approved_at       TIMESTAMPTZ,
    launched_at       TIMESTAMPTZ,
    parked_at         TIMESTAMPTZ,    -- Set when marginal is parked (pipeline full). NULL when promoted or killed.
    created_at        TIMESTAMPTZ DEFAULT now(),
    updated_at        TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_verticals_stage ON verticals(stage);
CREATE INDEX idx_verticals_mode ON verticals(mode);
CREATE INDEX idx_verticals_geography ON verticals(geography);
CREATE UNIQUE INDEX idx_verticals_slug_geo ON verticals(slug, geography);

-- Events: full audit trail + recovery source
CREATE TABLE events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type            TEXT NOT NULL,
    source_agent    TEXT NOT NULL,
    task_id         UUID,
    vertical_id     UUID REFERENCES verticals(id),
    payload         JSONB NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_events_type ON events(type);
CREATE INDEX idx_events_vertical ON events(vertical_id);
CREATE INDEX idx_events_task ON events(task_id);
CREATE INDEX idx_events_created ON events(created_at);

-- Agent state (must precede event_deliveries/receipts which FK to agents)
CREATE TABLE agents (
    id              TEXT PRIMARY KEY,
    type            TEXT NOT NULL,
    role            TEXT NOT NULL,          -- e.g., empire_coordinator, factory_cto, holding_devops, operations_analyst, opco_ceo, chief_of_staff, head_of_product, head_of_growth, cto, pm, tech_writer, backend, frontend, devops, marketing, support, custom
    mode            TEXT NOT NULL DEFAULT 'factory',  -- factory | operating
    vertical_id     UUID REFERENCES verticals(id),    -- NULL for factory agents
    parent_agent_id TEXT REFERENCES agents(id),       -- Manager chain: worker→VP, VP→CEO. NULL for CEOs and factory agents
    status          TEXT NOT NULL DEFAULT 'idle',
    current_task_id UUID,
    coordinator_id  TEXT,
    config          JSONB NOT NULL,
    template_version TEXT,                  -- Org template version this agent was spawned from (NULL for factory agents)
    budget_envelope NUMERIC,               -- Monthly API budget allocated by manager (NULL for factory agents)
    hired_by        TEXT,                   -- Manager agent ID that hired this agent (NULL for factory + seeded agents)
    started_at      TIMESTAMPTZ DEFAULT now(),
    last_active_at  TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_agents_vertical ON agents(vertical_id);
CREATE INDEX idx_agents_mode ON agents(mode);
CREATE INDEX idx_agents_parent ON agents(parent_agent_id);

-- Event deliveries — persisted at publish-time for OpCo routing recovery.
-- When EventBus publishes an OpCo event, it resolves routing_rules to concrete
-- agent IDs and writes one row per intended recipient. This enables crash recovery
-- without re-evaluating routing rules (which may have changed post-publish).
CREATE TABLE event_deliveries (
    event_id        UUID NOT NULL REFERENCES events(id),
    agent_id        TEXT NOT NULL REFERENCES agents(id),
    created_at      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (event_id, agent_id)
);

CREATE INDEX idx_deliveries_agent ON event_deliveries(agent_id);

-- Event receipts — tracks which agents have processed which events
-- Replaces mutating a processed_by[] array on the event row.
-- Benefits: faster writes (INSERT vs UPDATE), easy "unprocessed for agent X"
-- queries, no unbounded array growth, audit trail with status + error.
CREATE TABLE event_receipts (
    event_id        UUID NOT NULL REFERENCES events(id),
    agent_id        TEXT NOT NULL REFERENCES agents(id),
    processed_at    TIMESTAMPTZ DEFAULT now(),
    status          TEXT NOT NULL DEFAULT 'processed',  -- 'processed' | 'skipped' | 'error' | 'dead_letter'
    retry_count     INT NOT NULL DEFAULT 0,
    error           TEXT,                                -- Error message if status = 'error' or 'dead_letter'
    PRIMARY KEY (event_id, agent_id)
);

CREATE INDEX idx_receipts_agent ON event_receipts(agent_id);
CREATE INDEX idx_receipts_agent_time ON event_receipts(agent_id, processed_at DESC);

-- Event routing is stored in routing_rules (see §5.5).
-- The EventBus loads routing_rules into an in-memory RoutingTable per vertical.
-- routing_rules is the source of truth; the in-memory table is a derived read model.

-- Org templates — versioned agent roster, prompts, and routing templates.
-- Factory CTO manages these. SpawnOpCo reads the current version.
-- Running verticals track which version they were spawned from (verticals.template_version).
CREATE TABLE org_templates (
    version         TEXT PRIMARY KEY,        -- Semantic: "1.0", "1.1", "2.0"
    agents          JSONB NOT NULL,          -- Array of AgentTemplate (role, parent_role, type, prompt, tools, subscriptions, constraints)
    bootstrap_routes JSONB NOT NULL,         -- Array of RouteTemplate (event_pattern, subscriber_role, reason)
    seeded_routes   JSONB NOT NULL,          -- Array of RouteTemplate
    created_by      TEXT NOT NULL,           -- Factory CTO agent ID or "initial"
    description     TEXT,                    -- What changed and why
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- Template migration tracking — one row per vertical per migration attempt
CREATE TABLE template_migrations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vertical_id     UUID NOT NULL REFERENCES verticals(id),
    from_version    TEXT NOT NULL,
    to_version      TEXT NOT NULL REFERENCES org_templates(version),
    plan            JSONB NOT NULL,          -- Migration plan: agents_to_add, agents_to_remove, agents_to_reconfigure, routes_to_add, routes_to_remove
    status          TEXT NOT NULL DEFAULT 'pending',  -- 'pending' | 'approved' | 'executing' | 'completed' | 'failed' | 'rejected'
    mailbox_id      UUID,                    -- FK added after mailbox table creation (ALTER TABLE)
    executed_at     TIMESTAMPTZ,
    error           TEXT,
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- Conversations
CREATE TABLE conversations (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id        TEXT REFERENCES agents(id),
    task_id         UUID,
    scope_key       TEXT,                 -- NULL for task/session, vertical_id for session_per_vertical
    mode            TEXT DEFAULT 'task',  -- task | session | session_per_vertical
    messages        JSONB NOT NULL,
    summary         TEXT,                 -- Compressed context for session-scoped
    turn_count      INT DEFAULT 0,
    status          TEXT DEFAULT 'active',
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- Index for session_per_vertical lookups: find active conversation for agent+vertical pair
CREATE UNIQUE INDEX idx_conversations_scope ON conversations(agent_id, scope_key) WHERE scope_key IS NOT NULL AND status = 'active';

-- Agent sessions — tracks active LLM runtime sessions per agent.
-- Enforces single-writer semantics via lock_owner/lock_expires_at.
-- Supports session rotation with checkpoint summaries for context bridging.
CREATE TABLE agent_sessions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id        TEXT NOT NULL REFERENCES agents(id),
    scope_key       TEXT,                    -- NULL for global sessions, vertical_id for session_per_vertical
    runtime_mode    TEXT NOT NULL,            -- 'api' | 'cli_test'
    provider        TEXT NOT NULL DEFAULT 'anthropic',
    session_id      TEXT NOT NULL,            -- Provider session ID (API conversation ID or CLI --session-id UUID)
    status          TEXT NOT NULL DEFAULT 'active',  -- 'active' | 'rotated' | 'failed'
    turn_count      INT NOT NULL DEFAULT 0,
    checkpoint_summary TEXT,                  -- Summary from previous session (context bridge on rotation)
    lock_owner      TEXT,                     -- Goroutine/process ID holding exclusive write lease
    lock_expires_at TIMESTAMPTZ,             -- Lease TTL — reclaimed if expired (crash recovery)
    last_used_at    TIMESTAMPTZ DEFAULT now(),
    created_at      TIMESTAMPTZ DEFAULT now(),
    rotated_at      TIMESTAMPTZ              -- When this session was closed/rotated
);

-- One active session per agent per runtime mode
CREATE UNIQUE INDEX idx_sessions_active ON agent_sessions(agent_id, runtime_mode)
    WHERE status = 'active';
CREATE INDEX idx_sessions_last_used ON agent_sessions(last_used_at);
CREATE INDEX idx_sessions_lock_expiry ON agent_sessions(lock_expires_at)
    WHERE lock_owner IS NOT NULL;

-- Agent turns — per-turn telemetry for observability, replay, and debugging.
-- Dashboard-ready: latency tracking, parse success rate, retry visibility.
CREATE TABLE agent_turns (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id        TEXT NOT NULL REFERENCES agents(id),
    session_row_id  UUID NOT NULL REFERENCES agent_sessions(id),
    turn_index      INT NOT NULL,
    task_id         UUID,                    -- NULL for session-scoped heartbeats
    request_payload JSONB,                   -- What was sent to the LLM (redacted per §12)
    response_payload JSONB,                  -- What came back (redacted per §12)
    parse_ok        BOOLEAN NOT NULL DEFAULT true,  -- Did the response parse as valid structured output?
    latency_ms      INT,                     -- Round-trip time for this turn
    retry_count     INT NOT NULL DEFAULT 0,  -- Retries before success
    error           TEXT,                    -- Error message if parse_ok = false or runtime error
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_turns_agent_time ON agent_turns(agent_id, created_at DESC);
CREATE INDEX idx_turns_parse_failures ON agent_turns(agent_id)
    WHERE parse_ok = false;
CREATE UNIQUE INDEX idx_turns_session_turn ON agent_turns(session_row_id, turn_index);

-- Mailbox: human decision queue (always async — agents never block on decisions)
CREATE TABLE mailbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID REFERENCES events(id),
    vertical_id     UUID REFERENCES verticals(id),
    from_agent      TEXT,                           -- Agent that originated the request
    type            TEXT NOT NULL,                   -- review, escalation, spend_request, budget_increase, digest
    priority        TEXT DEFAULT 'normal',           -- normal | critical
    status          TEXT DEFAULT 'pending',          -- pending | approved | rejected | more_data | timed_out
    context         JSONB NOT NULL,
    summary         TEXT,                            -- Human-readable one-liner
    decision        TEXT,
    decision_notes  TEXT,
    timeout_at      TIMESTAMPTZ,             -- Review gates: auto-transition to timed_out after this
    notified        BOOLEAN DEFAULT false,           -- Critical items: has notification been sent?
    created_at      TIMESTAMPTZ DEFAULT now(),
    decided_at      TIMESTAMPTZ
);

CREATE INDEX idx_mailbox_pending ON mailbox(status) WHERE status = 'pending';
CREATE INDEX idx_mailbox_critical ON mailbox(priority) WHERE priority = 'critical' AND status = 'pending';

-- Deferred FK: template_migrations.mailbox_id → mailbox(id)
ALTER TABLE template_migrations ADD CONSTRAINT fk_migration_mailbox
    FOREIGN KEY (mailbox_id) REFERENCES mailbox(id);

-- Deferred FK: routing_rules (defined in §5.5) references verticals and agents.
-- routing_rules DDL must execute after verticals and agents in actual migration.

-- Schedules: timer-based agent wake-ups (recurring or one-shot)
CREATE TABLE schedules (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id        TEXT REFERENCES agents(id),
    vertical_id     UUID REFERENCES verticals(id),
    event_type      TEXT NOT NULL,           -- Event to emit on trigger
    mode            TEXT NOT NULL DEFAULT 'cron',  -- 'cron' | 'once'
    cron_expr       TEXT,                    -- Cron expression (required if mode='cron')
    at_time         TIMESTAMPTZ,             -- One-shot fire time (required if mode='once')
    next_fire_at    TIMESTAMPTZ,             -- Computed next fire time (for both modes)
    payload         JSONB,
    active          BOOLEAN DEFAULT true,
    last_fired_at   TIMESTAMPTZ,
    cancelled_at    TIMESTAMPTZ,             -- NULL if active, set on cancellation
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_schedules_active ON schedules(active, next_fire_at) WHERE active = true;

-- Geographies
CREATE TABLE geographies (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT NOT NULL,
    country         TEXT NOT NULL,
    region          TEXT,
    scan_config     JSONB,          -- Scan campaign config:
    -- {
    --   "modes": ["local_services", "saas_gap", "saas_trend"],
    --   "saas_categories": null,   -- null = full taxonomy, or ["financial_ops", "workforce_hr"] to filter
    --   "depth": "full",
    --   "local_sources": ["google_maps", "instagram", "reviews", "directories"]
    -- }
    last_scanned_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- Scan campaign queue: tracks queued, active, and completed scan campaigns.
-- Tracks queued, active, and completed scan campaigns. Empire Coordinator
-- creates campaigns from directives; Discovery Coordinator executes them.
CREATE TABLE scan_campaigns (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    geography_id    UUID NOT NULL REFERENCES geographies(id),
    directive_id    UUID,                -- links to originating board.directive or system.directive
    mode            TEXT NOT NULL,      -- local_services | saas_gap | saas_trend
    categories      TEXT[],             -- NULL = full taxonomy; or specific categories
    priority        TEXT NOT NULL DEFAULT 'normal',  -- high | normal | low
    strategic_context TEXT,             -- human-readable context from directive
    status          TEXT NOT NULL DEFAULT 'queued',
    -- Status flow: queued → active → completed | failed
    CONSTRAINT valid_campaign_status CHECK (status IN ('queued', 'active', 'completed', 'failed', 'paused')),
    discoveries     INT DEFAULT 0,      -- Count from scan.completed
    rescan_interval TEXT,               -- NULL = one-shot, or '30d', '90d' for periodic
    created_at      TIMESTAMPTZ DEFAULT now(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    deadline_at     TIMESTAMPTZ,        -- directive-imposed deadline
    next_rescan_at  TIMESTAMPTZ         -- Scheduled by Empire Coordinator after completion
);

CREATE INDEX idx_scan_campaigns_status ON scan_campaigns(status);

-- Inbound webhook deduplication
-- Tracks provider event IDs to prevent duplicate processing on webhook replay.
-- Cleanup cron purges entries older than 7 days (matches §4.7 Inbound Gateway retention).
CREATE TABLE inbound_events (
    provider_event_id TEXT NOT NULL,
    vertical_id       UUID NOT NULL REFERENCES verticals(id),
    provider          TEXT NOT NULL,         -- 'whatsapp' | 'stripe' | 'email' | 'domain_registrar'
    received_at       TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (provider_event_id, vertical_id)
);

CREATE INDEX idx_inbound_events_age ON inbound_events(received_at);

-- Deployments
CREATE TABLE deployments (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vertical_id     UUID REFERENCES verticals(id),
    environment     TEXT NOT NULL DEFAULT 'production',  -- 'staging' | 'production'
    version         INT NOT NULL DEFAULT 1,              -- Auto-increment per vertical+environment
    status          TEXT NOT NULL DEFAULT 'pending',     -- 'pending' | 'deploying' | 'deployed' | 'failed' | 'rolled_back'
    url             TEXT,
    domain          TEXT,            -- Real domain once purchased
    port            INT,
    binary_path     TEXT,
    migration_sql   TEXT,            -- Migration applied in this deploy (needed for rollback)
    nginx_config    TEXT,
    db_schema       TEXT,
    deployed_by     TEXT REFERENCES agents(id),  -- OpCo DevOps agent that initiated
    skip_staging    BOOLEAN DEFAULT false,        -- Hotfix flag (logged, visible in digest)
    health_status   TEXT DEFAULT 'unknown',
    deployed_at     TIMESTAMPTZ,
    last_health_at  TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE(vertical_id, environment, version)
);

-- Technical patterns (Factory CTO intelligence)
CREATE TABLE technical_patterns (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pattern_type    TEXT NOT NULL,  -- code_reuse, integration, architecture, failure
    description     TEXT NOT NULL,
    vertical_ids    UUID[] NOT NULL,
    confidence      TEXT DEFAULT 'observed',  -- observed, confirmed, extraction_ready
    cto_notes       TEXT,
    action_taken    TEXT,
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- Operating metrics (per-vertical, per-week)
CREATE TABLE vertical_metrics (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vertical_id     UUID REFERENCES verticals(id),
    period_start    DATE NOT NULL,
    period_end      DATE NOT NULL,
    users_total     INT DEFAULT 0,
    users_new       INT DEFAULT 0,
    users_churned   INT DEFAULT 0,
    mrr_cents       INT DEFAULT 0,          -- Monthly recurring revenue in cents
    support_tickets INT DEFAULT 0,
    bugs_reported   INT DEFAULT 0,
    bugs_fixed      INT DEFAULT 0,
    features_shipped INT DEFAULT 0,
    outreach_sent   INT DEFAULT 0,
    outreach_responses INT DEFAULT 0,
    csat_avg        DECIMAL(3,2),
    api_cost_cents  INT DEFAULT 0,
    infra_cost_cents INT DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT now(),
    UNIQUE(vertical_id, period_start)
);

-- Spend ledger (tracks all real-money spending)
CREATE TABLE spend_ledger (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vertical_id     UUID REFERENCES verticals(id),  -- NULL for factory-level spend
    agent_id        TEXT,            -- Which agent incurred this cost (NULL for infra/manual)
    category        TEXT NOT NULL,   -- llm_api, domain, whatsapp_api, infrastructure, tool_cost
    amount_cents    INT NOT NULL,
    currency        TEXT DEFAULT 'USD',
    description     TEXT,
    source          TEXT NOT NULL DEFAULT 'exact',  -- 'exact' (parsed from API response) or 'estimated' (per-turn model)
    approved_by     TEXT,           -- 'auto' or mailbox item ID
    metadata        JSONB,          -- model, input_tokens, output_tokens, turn_count (for calibration)
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_spend_vertical ON spend_ledger(vertical_id);

-- Human task queue (§14)
-- Tasks requiring physical-world action by humans. Agents request, Empire Coordinator approves.
CREATE TABLE human_tasks (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    requesting_agent    TEXT NOT NULL,         -- Agent ID that called human_task_request. Used to route human_task.completed/rejected/deferred events back.
    vertical_id         UUID REFERENCES verticals(id),  -- NULL for holding-level tasks
    category            TEXT NOT NULL,         -- sales_call, government_visit, verification, escalated_support, partnership, ground_truth
    description         TEXT NOT NULL,         -- What needs to be done
    talking_points      JSONB,                 -- For sales calls: key points, offer details, objection handling
    expected_value      TEXT,                  -- Agent's justification: "close $50/mo customer", "verify SIFEN requirement"
    priority            TEXT NOT NULL DEFAULT 'medium',  -- critical, high, medium, low
    deadline            TIMESTAMPTZ,           -- When this needs to be done by
    status              TEXT NOT NULL DEFAULT 'pending_review',
    -- Status flow: pending_review → {approved, rejected, deferred} → assigned → {completed, expired}
    CONSTRAINT valid_task_status CHECK (status IN (
      'pending_review', 'approved', 'rejected', 'deferred',
      'assigned', 'completed', 'expired'
    )),
    review_decision     JSONB,                 -- Empire Coordinator's evaluation: reason, priority_rank
    assigned_to         TEXT,                  -- Human identifier (founder, employee name)
    result              TEXT,                  -- Human's completion report
    outcome             TEXT,                  -- success, partial, failed
    follow_up_needed    BOOLEAN DEFAULT false,
    requeue_count       INT DEFAULT 0,         -- Incremented on expiry-requeue. At 2+: escalate to mailbox.
    created_at          TIMESTAMPTZ DEFAULT now(),
    reviewed_at         TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ
);

CREATE INDEX idx_human_tasks_status ON human_tasks(status);
CREATE INDEX idx_human_tasks_vertical ON human_tasks(vertical_id);
CREATE INDEX idx_human_tasks_category ON human_tasks(category);

-- Pipeline diagnostics (§4.2.2.6) — every interceptor handler writes a transition record.
-- Primary debugging tool for the 26-event pipeline coordinator.
CREATE TABLE pipeline_transitions (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID NOT NULL REFERENCES events(id),
    event_type      TEXT NOT NULL,
    handler         TEXT NOT NULL,           -- e.g. "handleSpecApproved", "handleCTORevision"
    pipeline_type   TEXT NOT NULL,           -- "campaign" | "validation" | "scan" | "marginal"
    pipeline_id     UUID NOT NULL,           -- campaign_id, vertical_id, or scan_id
    action          TEXT NOT NULL,           -- "consumed" | "passthrough" | "dropped" | "error"
    state_before    JSONB,                   -- Snapshot of relevant state before mutation
    state_after     JSONB,                   -- Snapshot after mutation (null if dropped/error)
    events_emitted  TEXT[],                  -- List of event types emitted by this handler
    drop_reason     TEXT,                    -- Why the event was dropped (guard failed, stale version, etc.)
    error           TEXT,                    -- Error message if handler failed
    duration_us     INT,                     -- Handler execution time in microseconds
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_pt_pipeline ON pipeline_transitions(pipeline_type, pipeline_id, created_at);
CREATE INDEX idx_pt_event ON pipeline_transitions(event_id);
CREATE INDEX idx_pt_drops ON pipeline_transitions(action) WHERE action = 'dropped';
CREATE INDEX idx_pt_errors ON pipeline_transitions(action) WHERE action = 'error';

-- Shard tracking (§4.2.2.7) — sharded execution framework for heavy workloads.
-- Market Research Agent's 52 taxonomy subcategories, Trend Research Agent's categories, etc.
CREATE TABLE shards (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    root_task_id    UUID NOT NULL,              -- Parent scan/task
    scan_id         UUID,                       -- FK, nullable for non-scan shards
    stage           TEXT NOT NULL,              -- "market_research" | "trend_research"
    shard_index     INT NOT NULL,
    shard_count     INT NOT NULL,
    shard_key       TEXT NOT NULL,              -- Deterministic key for idempotency
    scope           JSONB NOT NULL,            -- Work payload for this shard
    agent_id        TEXT REFERENCES agents(id), -- Agent instance processing this shard
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending | assigned | completed | failed | timed_out
    deadline_at     TIMESTAMPTZ NOT NULL,
    budget_cents    INT NOT NULL,
    spend_cents     INT NOT NULL DEFAULT 0,
    retry_count     INT NOT NULL DEFAULT 0,
    error           TEXT,
    assigned_at     TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX idx_shards_idempotent ON shards(root_task_id, shard_key);
CREATE INDEX idx_shards_root ON shards(root_task_id);
CREATE INDEX idx_shards_status ON shards(status) WHERE status IN ('pending', 'assigned');
CREATE INDEX idx_shards_deadline ON shards(deadline_at) WHERE status = 'assigned';

-- Prompt overrides — hot-reload prompt editing for iteration.
-- When present, runtime uses this prompt instead of the org_templates version.
-- Keyed by agent_id: works for both holding agents (singletons) and
-- OpCo agents (per-instance overrides). Template role edits go through
-- the normal empire template publish flow, not this table.
CREATE TABLE prompt_overrides (
    agent_id        TEXT PRIMARY KEY REFERENCES agents(id),
    prompt          TEXT NOT NULL,
    previous_prompt TEXT,                    -- Snapshot of what was replaced (for diff/revert)
    source          TEXT NOT NULL DEFAULT 'dashboard',  -- 'dashboard' | 'cli' | 'api'
    notes           TEXT,                    -- Why this override exists
    created_at      TIMESTAMPTZ DEFAULT now(),
    updated_at      TIMESTAMPTZ DEFAULT now()
);

-- OpCo cycle detection counters (§4.2.2.9).
-- In-memory primary, DB-synced for crash recovery.
-- One row per active event pattern per vertical.
CREATE TABLE cycle_counters (
    vertical_id     UUID NOT NULL REFERENCES verticals(id),
    event_pattern   TEXT NOT NULL,           -- e.g., "qa.validation_failed"
    count           INT NOT NULL DEFAULT 0,
    window_start    TIMESTAMPTZ NOT NULL,
    last_emitter    TEXT,                    -- agent_id of last emission
    updated_at      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (vertical_id, event_pattern)
);

-- Expired windows are cleaned up by a periodic job (hourly).
-- Active counters are few: typically 0-3 per vertical during normal operation.

-- Scoring digest buffer: rejected verticals summarized for EC digest (§4.2.2.8).
-- Runtime writes rows on rejection. EC digest compilation reads and summarizes.
-- Rows retained 30 days for audit, cleaned by periodic job.
CREATE TABLE scoring_digest_buffer (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vertical_id     UUID NOT NULL REFERENCES verticals(id),
    vertical_name   TEXT NOT NULL,
    geography       TEXT NOT NULL,
    composite       NUMERIC(5,2) NOT NULL,
    viability       NUMERIC(5,2),
    result          TEXT NOT NULL DEFAULT 'rejected',
    reason          TEXT NOT NULL,           -- 'viability_floor' | 'low_composite'
    scored_at       TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_scoring_digest_buffer_time ON scoring_digest_buffer(scored_at);

-- ===================================================================
-- ROUTING (§5.5) — must execute AFTER verticals + agents
-- ===================================================================

CREATE TABLE routing_rules (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vertical_id     UUID NOT NULL REFERENCES verticals(id),
    event_pattern   TEXT NOT NULL,           -- e.g., "feature_deployed", "bug_*", "*"
    subscriber_id   TEXT NOT NULL REFERENCES agents(id),
    installed_by    TEXT NOT NULL REFERENCES agents(id),  -- who added this route
    reason          TEXT,                     -- why this route exists
    status          TEXT NOT NULL DEFAULT 'active',  -- 'active' | 'proposed' (CoS proposals awaiting CEO approval) | 'deactivated'
    source          TEXT NOT NULL DEFAULT 'bootstrap',  -- 'bootstrap' | 'seeded' | 'discovered' | 'retrospective'
    bootstrap_version INT,                   -- which bootstrap version installed this (NULL for discovered routes)
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    deactivated_at  TIMESTAMPTZ
);

CREATE UNIQUE INDEX idx_routing_rules_unique ON routing_rules(vertical_id, event_pattern, subscriber_id)
    WHERE status = 'active';

-- bootstrap_versions table (maintained by Factory CTO based on Operations Analyst proposals)
CREATE TABLE bootstrap_versions (
    version         INT PRIMARY KEY,
    routes          JSONB NOT NULL,          -- array of {event_pattern, subscriber_role, reason}
    proposed_by     TEXT NOT NULL,            -- 'initial' or analyst agent ID
    approved_by     TEXT NOT NULL,            -- 'initial' or factory_cto agent ID
    evidence        TEXT,                     -- "discovered in 5/5 verticals within 2 weeks"
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ===================================================================
-- RUNTIME OBSERVABILITY (§10.5.1)
-- ===================================================================

CREATE TABLE runtime_log (
    id              BIGSERIAL PRIMARY KEY,
    ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
    level           TEXT NOT NULL,           -- debug | info | warn | error | fatal
    component       TEXT NOT NULL,           -- eventbus | interceptor | agent_manager | 
                                             -- guardrails | scheduler | gateway | session | 
                                             -- recovery | budget | mailbox
    action          TEXT NOT NULL,           -- Verb: published, intercepted, spawned, 
                                             -- rotated, violated, timeout, delivered, 
                                             -- dropped, retried, failed, started, stopped
    -- Context fields (nullable — set when relevant)
    event_id        UUID,                    -- FK events(id) when log relates to a business event
    event_type      TEXT,                    -- Denormalized for fast filtering without join
    agent_id        TEXT,                    -- Agent involved
    vertical_id     UUID,                    -- Vertical involved
    campaign_id     UUID,                    -- Campaign involved
    scan_id         UUID,                    -- Scan involved
    session_id      UUID,                    -- Agent session involved
    -- Payload
    detail          JSONB,                   -- Structured metadata (varies by action)
    error           TEXT,                    -- Error message (level=error/fatal only)
    duration_us     INT                      -- Operation duration when measurable
);

-- Primary query patterns
CREATE INDEX idx_rlog_time ON runtime_log(ts DESC);
CREATE INDEX idx_rlog_component ON runtime_log(component, ts DESC);
CREATE INDEX idx_rlog_level ON runtime_log(level, ts DESC) WHERE level IN ('warn', 'error', 'fatal');
CREATE INDEX idx_rlog_event ON runtime_log(event_id) WHERE event_id IS NOT NULL;
CREATE INDEX idx_rlog_agent ON runtime_log(agent_id, ts DESC) WHERE agent_id IS NOT NULL;
CREATE INDEX idx_rlog_vertical ON runtime_log(vertical_id, ts DESC) WHERE vertical_id IS NOT NULL;

-- ===================================================================
-- RUNTIME INTERNAL TABLES (added v2.0.33 — from migrations 003, 009, 011, 012)
-- ===================================================================

-- runtime_config: Global runtime configuration key-value store (migration 003)
CREATE TABLE runtime_config (
    key             TEXT PRIMARY KEY,
    value           JSONB NOT NULL,
    description     TEXT,
    updated_by      TEXT,                    -- agent or 'system'
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- pipeline_receipts: Tracks which events each interceptor handler has processed (migration 009)
-- Prevents duplicate processing on crash recovery / replay.
CREATE TABLE pipeline_receipts (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID NOT NULL REFERENCES events(id),
    handler         TEXT NOT NULL,           -- e.g., 'handleDiscoveryReport', 'handleScanCompleted'
    vertical_id     UUID REFERENCES verticals(id),
    result          TEXT NOT NULL,           -- 'processed' | 'skipped' | 'error'
    detail          JSONB,                   -- handler-specific output (scores, decisions, etc.)
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX idx_pipeline_receipts_unique ON pipeline_receipts(event_id, handler);
CREATE INDEX idx_pipeline_receipts_vertical ON pipeline_receipts(vertical_id, processed_at DESC);

-- scan_accumulators: Tracks per-scan progress across MRA/TRA/Scanner shards (migration 012)
CREATE TABLE scan_accumulators (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    campaign_id     UUID NOT NULL REFERENCES scan_campaigns(id),
    scan_type       TEXT NOT NULL,           -- 'market_research' | 'trend_research' | 'scanner.{type}'
    total_expected  INT NOT NULL,            -- total subcategories/sources to process
    total_completed INT NOT NULL DEFAULT 0,
    total_failed    INT NOT NULL DEFAULT 0,
    status          TEXT NOT NULL DEFAULT 'active',
    CONSTRAINT valid_accum_status CHECK (status IN ('active', 'completed', 'failed')),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);

CREATE INDEX idx_accum_campaign ON scan_accumulators(campaign_id, status);

-- pending_dedup_candidates: Buffers verticals awaiting dedup check (migration 012)
CREATE TABLE pending_dedup_candidates (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vertical_id     UUID NOT NULL REFERENCES verticals(id),
    candidate_name  TEXT NOT NULL,
    geography       TEXT NOT NULL,
    discovery_mode  TEXT NOT NULL,
    raw_signals     JSONB,
    status          TEXT NOT NULL DEFAULT 'pending',
    CONSTRAINT valid_dedup_status CHECK (status IN ('pending', 'resolved', 'merged')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at     TIMESTAMPTZ
);

CREATE INDEX idx_dedup_pending ON pending_dedup_candidates(status, geography)
    WHERE status = 'pending';

-- validation_pipelines: Tracks validation stage progress per vertical (migration 012)
CREATE TABLE validation_pipelines (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vertical_id     UUID NOT NULL REFERENCES verticals(id),
    stage           TEXT NOT NULL,           -- 'research' | 'mvp_spec' | 'spec_review' | 'cto_review' | 'branding' | 'packaging'
    status          TEXT NOT NULL DEFAULT 'active',
    CONSTRAINT valid_vp_status CHECK (status IN ('active', 'completed', 'failed', 'blocked')),
    attempts        INT NOT NULL DEFAULT 1,
    max_attempts    INT NOT NULL DEFAULT 3,
    context         JSONB,                   -- stage-specific metadata
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);

CREATE UNIQUE INDEX idx_vp_active ON validation_pipelines(vertical_id, stage)
    WHERE status = 'active';

-- pipeline_processed_events: Idempotency guard for event processing (migration 012)
-- Lightweight alternative to pipeline_receipts for simple "did we see this?" checks.
CREATE TABLE pipeline_processed_events (
    event_id        UUID NOT NULL REFERENCES events(id),
    handler         TEXT NOT NULL,
    processed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, handler)
);

-- template_prompt_drafts: Stores Factory CTO prompt drafts during template editing (migration 011)
CREATE TABLE template_prompt_drafts (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    template_version TEXT NOT NULL,
    agent_role      TEXT NOT NULL,           -- which agent's prompt is being edited
    draft_prompt    TEXT NOT NULL,
    author_id       TEXT NOT NULL REFERENCES agents(id),
    status          TEXT NOT NULL DEFAULT 'draft',
    CONSTRAINT valid_draft_status CHECK (status IN ('draft', 'proposed', 'approved', 'rejected')),
    feedback        TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_drafts_template ON template_prompt_drafts(template_version, status);
