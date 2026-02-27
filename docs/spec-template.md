# System Spec Template
# Lessons from EmpireAI v2.0.25 audit: what a spec needs to be
# both a design document AND an implementation contract.

# ============================================================
# STRUCTURE OVERVIEW
# ============================================================
#
# Part 1: PROSE SPEC (human-readable, architectural decisions)
# Part 2: CONTRACTS (machine-readable, test-verifiable)
# Part 3: CHANGELOG (executable action items, not just narratives)
#
# The prose tells you WHY. The contracts tell you WHAT (exactly).
# The changelog tells you WHAT CHANGED and WHAT TO DO ABOUT IT.
# ============================================================


# =====================
# PART 1: PROSE SPEC
# =====================
# Split into sections. Each section should fit in a single Claude
# context window (~60-80KB). If a section exceeds that, split it.


## 1. Overview
# - What the system does (2-3 paragraphs)
# - Operating model diagram (ASCII)
# - Stack (languages, databases, APIs, infrastructure)
# - Design principles (bulleted, max 15)
#   - Each principle should be testable: "event-driven, async by default"
#     is testable. "Clean code" is not.


## 2. Authority Matrix
# WHO decides WHAT. This is the constitutional document.
#
# Structure:
#   2.1 Fully Autonomous (no human) — per actor, what they can do
#   2.2 Human Approval Required — what blocks on human
#   2.3 Human Review Gates (non-blocking) — high-leverage review points
#   2.4 Human Input Channel — advisory, non-blocking
#   2.5 Human Gets Briefed — digest/reporting
#
# WHY THIS MATTERS: Every ambiguity in authority causes either
# blocked agents (waiting for approval nobody configured) or
# unauthorized actions. Be explicit.


## 3. Actor Hierarchy
# - Org chart (ASCII diagram)
# - Per actor:
#   - Role description (what they do, what they DON'T do)
#   - Reports to / manages
#   - Model tier (with rationale)
#   - Wake-up sources (what events trigger them)
# - Lifecycle: how actors are created, destroyed, recovered
#
# LESSON: Distinguish "handles judgment" from "handles procedure."
# Procedure belongs in runtime state machines, not LLM agents.
# If the correct action is deterministic given (state + event),
# it's a state machine. If it requires reasoning, it's an agent.


## 4. Runtime Architecture
# - Process model (single process? microservices? containers?)
# - Event bus design
#   - Routing classification (how events find their consumers)
#   - Persistence (write-through, recovery)
#   - Delivery guarantees (at-least-once, exactly-once, retry policy)
# - State machines / coordinators
#   - For each: states, transitions, guards
#   - Persistence strategy (in-memory + DB? DB-only?)
#   - Transaction boundaries (what's atomic?)
#   - Idempotency (how are replays handled?)
# - Agent session management
#   - Context modes (stateless, session, session-per-scope)
#   - Rotation triggers and checkpoint strategy
#   - Locking / single-writer semantics
# - Tool execution pipeline
#   - Authorization model
#   - Tenant isolation
#   - Credential injection
#
# LESSON: The pipeline coordinator in EmpireAI handles 33 event
# types. Document EVERY intercept case in a switch-statement-style
# table. If you can't enumerate the cases, you don't understand
# the system yet.


## 5. Communication Model
# - Primitives (events, messages, tasks, etc.)
# - Naming conventions (with examples)
# - Per-event catalog tables:
#
#   | Event | Emitter | Consumer | Payload fields |
#   |-------|---------|----------|----------------|
#
# - Routing model (static subscriptions vs dynamic routing table)
# - Bootstrap / seeded / discoverable route classification
#
# LESSON: The event catalog is the most important artifact in the
# spec. Every missing entry = a broken handoff at runtime.
# Every wrong consumer = an agent that never wakes up.


## 6. Data Model
# - DDL for every table, ordered by FK dependencies
# - Constraints (CHECK, UNIQUE, FK)
# - Indexes (with rationale for non-obvious ones)
# - Stage/status enums with valid transition graphs
#
# LESSON: Write the DDL ONCE in the spec. Migrations copy from here.
# Never let an implementer write DDL from memory/interpretation.
# The canonical DDL IS the spec for the data model.


## 7. Pipeline / Workflow Definitions
# Per pipeline (factory, operating, deploy, etc.):
# - Flow diagram (ASCII)
# - Stage transitions with trigger events
# - Gates / quality checks
# - Revision loops and limits
# - Terminal states
# - Error/recovery paths


## 8. Cost & Budget
# - Per-actor model tier table
# - Per-pipeline cost estimates
# - Budget enforcement rules
# - Throttling behavior


## 9. External Interfaces
# - API surface (every endpoint, method, params, response)
# - CLI commands (every command, flags, behavior)
# - Webhook/inbound handling
# - Authentication model
# - Notification channels


## 10. Configuration
# - Full config file with comments
# - Per-field: type, default, what it controls
# - Runtime-modifiable vs restart-required


## 11. Recovery & Resilience
# - Cold start sequence (step by step)
# - Crash recovery (what's replayed, what's lost)
# - Per-actor failure modes and recovery
# - Backup strategy per phase


## 12. Security & Data Handling
# - Data classification table
# - Tenant isolation enforcement
# - Credential storage and injection
# - Redaction rules for logs/conversations


## 13. Implementation Phases
# - Per phase: what's built, what's deferred
# - Exit criteria per phase (testable, not vibes)
# - Pre-implementation checklist (gate before coding starts)


## 14. Open Questions
# - Numbered, with status: open / resolved (with resolution)
# - Resolved questions stay visible — they're design decisions


## Appendix: Actor Prompts
# Full system prompt for every actor.
# Per prompt:
#   - id (exact, matches config)
#   - type/mode
#   - parent
#   - model_tier
#   - subscriptions (exact event list)
#   - tools (exact tool list)
#   - prompt text
#
# LESSON: The appendix IS the config source of truth.
# Config YAML files should be mechanically derivable from this.


# =====================
# PART 2: CONTRACTS
# =====================
# Machine-readable extractions from the prose spec.
# Tests verify implementation against these files.
# When spec changes, contracts change, tests fail, drift is caught.


# --- contracts/agent-registry.yaml ---
# Extracted from: §3 (hierarchy), §8 (model tiers), Appendix (prompts)
#
# agents:
#   empire-coordinator:
#     model_tier: sonnet
#     mode: holding
#     parent: null
#     tools:
#       - agent_message
#       - mailbox_send
#       - schedule
#       - human_task_decide
#     subscriptions:
#       - system.started
#       - system.directive
#       - vertical.scored
#       - vertical.approved
#       - vertical.killed
#       # ... exhaustive list
#     emits:
#       - scan.requested
#       - opco.spinup_requested
#       # ... exhaustive list
#
#   marketing-agent:
#     model_tier: sonnet
#     mode: opco_template
#     parent: vp-growth
#     tools:
#       - domain_purchase
#       - domain_availability_check
#       - dns_configure
#       - instagram_api
#       - instagram_handle_check
#       - whatsapp_business_api
#       - whatsapp_name_check
#       - human_task_request
#     # ...


# --- contracts/event-catalog.yaml ---
# Extracted from: §5.4, §5.5 event tables
#
# events:
#   scan.requested:
#     routing: factory
#     emitters: [empire-coordinator]
#     consumers: [discovery-coordinator]  # or "runtime" if intercepted
#     payload:
#       required: [geography, mode]
#       optional: [taxonomy_categories, sources, depth]
#
#   bug_reported:
#     routing: opco
#     emitters: [support-agent]
#     consumers: [cto-agent]   # via bootstrap route
#     payload:
#       required: [description, severity]


# --- contracts/ddl-canonical.sql ---
# Extracted from: §6 data model
# This IS the DDL. Migrations copy from this file.
# One table per block, ordered by FK dependencies.
# Comments explain constraints.
#
# CREATE TABLE verticals ( ... );
# CREATE TABLE events ( ... );
# CREATE TABLE agents ( ... );
# -- etc.


# --- contracts/routes.yaml ---
# Extracted from: §5.5 bootstrap + seeded routes
#
# bootstrap:
#   - event: product_spec_ready
#     from: pm-agent
#     to: cto-agent
#     reason: "Engineering can't start without spec"
#   # ... all 20
#
# seeded:
#   - event: bug_fix_deployed
#     from: cto-agent
#     to: support-agent
#     reason: "Support needs to tell customers"
#     removable: true
#   # ... all 7


# --- contracts/stage-transitions.yaml ---
# Extracted from: §6 data model, §7 pipeline flow
#
# verticals:
#   discovered: [scoring]
#   scoring: [shortlisted, marginal_review]
#   shortlisted: [researching]
#   marginal_review: [researching, killed]
#   # ... full graph


# --- contracts/model-tiers.yaml ---
# Extracted from: §8 cost & budget
#
# tiers:
#   empire-coordinator: sonnet
#   factory-cto: sonnet
#   holding-devops: haiku
#   scanner-agent: haiku
#   # ... every agent


# =====================
# PART 3: CHANGELOG
# =====================
# Every spec revision produces TWO sections:
#
# 1. NARRATIVE: what changed and why (for humans reading the spec)
# 2. ACTIONS: what must change in the codebase (for implementers)
#
# Actions are typed:
#   ADD      — new file, table, column, tool, route
#   EDIT     — modify existing (specify file + what changes)
#   DROP     — remove from codebase (specify what + grep pattern)
#   MIGRATE  — new SQL migration needed (specify DDL)
#   VERIFY   — grep codebase for old references, confirm zero


# Example:
#
# ## v2.0.26
#
# ### Narrative
# - Removed tool_call_id from human_tasks (no longer needed after
#   emit_* tool architecture replaced JSON envelope responses)
# - Fixed model tier for Holding DevOps (Haiku, not Sonnet)
# - Added cancel_timer tool
#
# ### Actions
# - MIGRATE: `ALTER TABLE human_tasks DROP COLUMN IF EXISTS tool_call_id;`
# - EDIT: `configs/agents/holding-devops.yaml` — change model_tier: haiku
# - EDIT: `configs/agents/templates/marketing-agent.yaml` — add tools:
#     [domain_purchase, domain_availability_check, dns_configure,
#      instagram_api, instagram_handle_check, whatsapp_business_api,
#      whatsapp_name_check]
# - ADD: cancel_timer tool in tool_executor.go
# - DROP: `devops.deploy_complete → qa-agent` from routes.yaml seeded section
# - VERIFY: `grep -r "tool_call_id" --include="*.go" --include="*.yaml"`
#   should return zero results after migration


# =====================
# VERIFICATION TESTS
# =====================
# These tests run against contracts/ and catch drift automatically.
#
# 1. CONFIG COMPLETENESS
#    For each agent in contracts/agent-registry.yaml:
#      - Config YAML file exists
#      - model_tier matches
#      - tools list is a superset of contract (extras OK, missing = FAIL)
#      - subscriptions list is a superset of contract
#      - id matches
#
# 2. EVENT WIRING (6 checks from EmpireAI's §15.0)
#    - NO_SCHEMA: event in catalog but no schema in registry
#    - DEAD_SUB: agent subscribes to event nobody emits
#    - MISSING_SUB: catalog consumer not subscribed in config
#    - EMIT_NOT_IN_YAML: producer registry says agent emits but no tool
#    - ORPHAN_EMISSION: emitted but nobody subscribes or intercepts
#    - SCHEMA_NO_CATALOG: schema exists but not in catalog
#
# 3. DDL DRIFT
#    For each table in contracts/ddl-canonical.sql:
#      - Table exists in latest migration
#      - All columns present with correct types
#      - All constraints present
#      - All indexes present
#
# 4. ROUTE COMPLETENESS
#    For each route in contracts/routes.yaml:
#      - Route exists in routes config
#      - Count matches (no extras in bootstrap, extras OK in seeded)
#
# 5. STAGE TRANSITIONS
#    For each transition in contracts/stage-transitions.yaml:
#      - Runtime transition map includes it
#      - No transitions exist that aren't in contract
