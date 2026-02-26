# EmpireAI Architecture v0.4-2 Review Feedback

Reviewed spec: `docs/specs/empireai-architecture-v0.4-2.md`  
Review date: 2026-02-11

## Executive Summary

The architecture direction is strong and coherent at the strategy level. The hierarchy, async operating model, and phased rollout are all practical. The current draft is not implementation-ready yet because the spec has several contract-level inconsistencies that will cause rework if coding starts immediately.

Readiness assessment:
- Product and org design: strong
- Runtime and data contracts: needs normalization
- Prompt and tool wiring: needs correction
- Implementation readiness: **Amber/Red** (fix blockers first)

## What Is Strong

- Clear authority boundaries across Empire Coordinator, Factory CTO, Holding DevOps, and OpCo leadership.
- Good separation of factory validation vs operating execution.
- Strong async principles (non-blocking mailbox, milestone-driven reporting, dynamic heartbeats).
- Practical three-tier routing concept (bootstrap, seeded, discovered) with explicit learning loop.
- Good cost-awareness and operator ergonomics (action-oriented digest, founder mode scaling).

## Blockers (Must Fix Before Implementation)

### 1) Routing schema key type mismatch
- Severity: Critical
- Issue: `routing_rules.subscriber_id` and `routing_rules.installed_by` are `UUID` FKs to `agents(id)`, but `agents.id` is `TEXT`.
- Impact: Migration fails or FK integrity is impossible without ad-hoc casting.
- Recommendation: Choose one canonical agent ID type (`TEXT` or `UUID`) and use it consistently across all tables.
- References: `docs/specs/empireai-architecture-v0.4-2.md:1330`, `docs/specs/empireai-architecture-v0.4-2.md:1331`, `docs/specs/empireai-architecture-v0.4-2.md:2261`

### 2) Two competing routing persistence models
- Severity: Critical
- Issue: Spec defines both `routing_rules` (normalized rows) and `routing_tables` (single JSON document per vertical) as active models.
- Impact: Runtime behavior, authorization checks, and analytics become ambiguous.
- Recommendation: Pick one canonical storage model. If both are needed, define one as derived/read-model and specify source-of-truth + sync direction.
- References: `docs/specs/empireai-architecture-v0.4-2.md:1326`, `docs/specs/empireai-architecture-v0.4-2.md:2282`

### 3) Scheduler contract contradiction (one-shot vs cron-only schema)
- Severity: Critical
- Issue: Runtime requires one-shot `At` timers and dynamic self-scheduling, but `schedules` table only supports required `cron_expr`.
- Impact: Dynamic heartbeat/report fallback cannot be represented correctly in storage.
- Recommendation: Add explicit one-shot fields (`at_time`, `mode`, nullable cron) and lifecycle fields (`next_fire_at`, `cancelled_at`).
- References: `docs/specs/empireai-architecture-v0.4-2.md:836`, `docs/specs/empireai-architecture-v0.4-2.md:848`, `docs/specs/empireai-architecture-v0.4-2.md:962`, `docs/specs/empireai-architecture-v0.4-2.md:2335`

### 4) Deployment executor is duplicated
- Severity: Critical
- Issue: OpCo DevOps and Holding DevOps are both described as running migrations and deploying binaries for the same deploy flow.
- Impact: Race conditions, unclear rollback ownership, and operational deadlocks.
- Recommendation: Define single executor-of-record. Example: OpCo DevOps prepares artifact/migration plan; Holding DevOps executes privileged infra actions.
- References: `docs/specs/empireai-architecture-v0.4-2.md:3961`, `docs/specs/empireai-architecture-v0.4-2.md:3963`, `docs/specs/empireai-architecture-v0.4-2.md:4159`, `docs/specs/empireai-architecture-v0.4-2.md:4163`

### 5) Infrastructure ownership contradiction
- Severity: Critical
- Issue: Factory CTO is explicitly not infra owner, but Inbound Gateway infra is assigned to Factory CTO.
- Impact: Conflicting responsibility and escalation path for production webhooks.
- Recommendation: Move Inbound Gateway ownership to Holding DevOps (Factory CTO remains standards/advisory).
- References: `docs/specs/empireai-architecture-v0.4-2.md:90`, `docs/specs/empireai-architecture-v0.4-2.md:93`, `docs/specs/empireai-architecture-v0.4-2.md:995`

### 6) Prompt/tool mismatch: Chief of Staff cannot perform instructed routing changes
- Severity: Critical
- Issue: CoS prompt says to use `configure_routing`, but CoS tool list does not include it.
- Impact: Core routing-evolution workflow cannot execute.
- Recommendation: Add `configure_routing` to CoS tools or change prompt to route all changes through CEO.
- References: `docs/specs/empireai-architecture-v0.4-2.md:3357`, `docs/specs/empireai-architecture-v0.4-2.md:3393`

### 7) Prompt/tool mismatch: Head of Product cannot submit founder spec review
- Severity: Critical
- Issue: HoP is instructed to send `product_spec_review` to mailbox but does not have `mailbox_send`.
- Impact: Founder review gate cannot run as designed.
- Recommendation: Add `mailbox_send` tool to HoP or re-route gate submission through CEO.
- References: `docs/specs/empireai-architecture-v0.4-2.md:3448`, `docs/specs/empireai-architecture-v0.4-2.md:3498`

### 8) Undefined subscribed event in Head of Growth template
- Severity: High
- Issue: Head of Growth subscribes to `opco.{vertical_id}.spend_needed`, but this event is not defined in event catalog.
- Impact: Subscription never fires or forces ad-hoc event additions later.
- Recommendation: Either define `spend_needed` formally (emitter, payload, routing) or remove from default subscriptions.
- References: `docs/specs/empireai-architecture-v0.4-2.md:1138`, `docs/specs/empireai-architecture-v0.4-2.md:1157`, `docs/specs/empireai-architecture-v0.4-2.md:3565`

## Major Issues (Should Fix Before or During Phase 1)

### 9) Event naming and namespace inconsistency
- Severity: High
- Issue: Mixed event styles are used interchangeably (`bug_reported`, `feature_deployed`, `deploy_complete`, `opco.{v}.support_digest`, `devops.deploy_complete`).
- Impact: Hard to build typed event enums, routing filters, and analytics without brittle aliasing.
- Recommendation: Define one canonical naming convention and an explicit compatibility map for legacy aliases.
- References: `docs/specs/empireai-architecture-v0.4-2.md:1005`, `docs/specs/empireai-architecture-v0.4-2.md:1128`, `docs/specs/empireai-architecture-v0.4-2.md:1393`, `docs/specs/empireai-architecture-v0.4-2.md:1401`

### 10) Routing policy contradiction on launch coordination
- Severity: High
- Issue: Launch coordination is defined as seeded day-1 route, but operating sequence says launch coordination is not prescribed and should emerge.
- Impact: Different teams will implement incompatible defaults.
- Recommendation: Decide one: seeded default with optional removal, or pure discovery. Keep one policy in all sections.
- References: `docs/specs/empireai-architecture-v0.4-2.md:1223`, `docs/specs/empireai-architecture-v0.4-2.md:1224`, `docs/specs/empireai-architecture-v0.4-2.md:1895`

### 11) Promotion path contradiction (2-tier vs 3-tier evolution)
- Severity: High
- Issue: Some sections promote discovered routes directly to bootstrap, while others require discovered -> seeded -> bootstrap.
- Impact: Analyst automation and CTO approval workflow become inconsistent.
- Recommendation: Standardize on discovered -> seeded -> bootstrap and remove direct discovered -> bootstrap language.
- References: `docs/specs/empireai-architecture-v0.4-2.md:102`, `docs/specs/empireai-architecture-v0.4-2.md:2135`, `docs/specs/empireai-architecture-v0.4-2.md:2183`

### 12) Mailbox status enum mismatch
- Severity: Medium
- Issue: Table comments define `more_data`; example payload defines `timed_out`.
- Impact: Ambiguous state machine and API behavior.
- Recommendation: Publish canonical mailbox FSM and use one enum set everywhere.
- References: `docs/specs/empireai-architecture-v0.4-2.md:2316`, `docs/specs/empireai-architecture-v0.4-2.md:2552`

### 13) Spend approval policy conflict
- Severity: Medium
- Issue: Authority matrix says all real-money spend requires human approval, but budget config allows auto-approval below threshold.
- Impact: Governance ambiguity and potential policy breaches.
- Recommendation: Clarify policy: either strict human approval for all spend or explicit delegated threshold policy by configuration.
- References: `docs/specs/empireai-architecture-v0.4-2.md:145`, `docs/specs/empireai-architecture-v0.4-2.md:2519`, `docs/specs/empireai-architecture-v0.4-2.md:2828`

### 14) Event struct and DB schema mismatch
- Severity: Medium
- Issue: `Event.TaskID` is `string` in runtime struct while DB `task_id` is `UUID`; runtime uses `Source` while DB uses `source_agent`.
- Impact: Adapter complexity and serialization bugs.
- Recommendation: Align runtime struct to DB contract or define explicit transformation layer contract.
- References: `docs/specs/empireai-architecture-v0.4-2.md:1030`, `docs/specs/empireai-architecture-v0.4-2.md:1031`, `docs/specs/empireai-architecture-v0.4-2.md:2231`, `docs/specs/empireai-architecture-v0.4-2.md:2232`

### 15) Persistence semantics conflict in EventBus description
- Severity: Medium
- Issue: Process model says Postgres writes are asynchronous background durability, but EventBus says write-through before fanout.
- Impact: Different durability/latency behavior depending on interpretation.
- Recommendation: Decide one publish contract: strict write-then-fanout or async outbox-style with explicit guarantees.
- References: `docs/specs/empireai-architecture-v0.4-2.md:683`, `docs/specs/empireai-architecture-v0.4-2.md:696`, `docs/specs/empireai-architecture-v0.4-2.md:725`

### 16) Model assignment inconsistency for Tech Writer
- Severity: Medium
- Issue: Worker model summary labels Tech Writer as Haiku, while model selection table assigns Sonnet.
- Impact: Cost/performance expectations become unreliable.
- Recommendation: Keep one default and document override conditions.
- References: `docs/specs/empireai-architecture-v0.4-2.md:608`, `docs/specs/empireai-architecture-v0.4-2.md:2457`

### 17) CEO reporting path inconsistency
- Severity: Medium
- Issue: Event catalog defines `opco.ceo_report` to Empire Coordinator, while CEO prompt instructs sending report via `mailbox_send`.
- Impact: Split reporting channel and double-ingest risk.
- Recommendation: Pick primary transport for CEO->Empire reports (event bus recommended) and treat mailbox as human-facing only.
- References: `docs/specs/empireai-architecture-v0.4-2.md:1153`, `docs/specs/empireai-architecture-v0.4-2.md:3314`

## Minor Issues (Doc Quality / Cleanup)

### 18) Incomplete Head of Product prompt section
- Severity: Medium
- Issue: Prompt ends with `ESCALATE TO CEO WHEN:` and no criteria.
- Impact: Template appears truncated and implementation may copy incomplete logic.
- Recommendation: Complete this section or mark as intentionally omitted.
- References: `docs/specs/empireai-architecture-v0.4-2.md:3548`

### 19) Missing section header level around data model
- Severity: Low
- Issue: `### 8.1 Core Tables` appears without a visible `## 8` section header.
- Impact: Navigation/readability issue and TOC generation problems.
- Recommendation: Add `## 8. Data Model` (or equivalent) above `8.1`.
- References: `docs/specs/empireai-architecture-v0.4-2.md:2187`

### 20) Inbound gateway scope still unresolved in main body
- Severity: Low
- Issue: Main design says dedicated shared inbound process, but open questions suggest webhook receiver may need to live per-vertical service.
- Impact: Potential architectural fork discovered late.
- Recommendation: Resolve before Phase 1 to avoid rework in scaffold/runtime split.
- References: `docs/specs/empireai-architecture-v0.4-2.md:966`, `docs/specs/empireai-architecture-v0.4-2.md:979`, `docs/specs/empireai-architecture-v0.4-2.md:3107`

## Recommended Hardening Plan (Before Coding)

1. Publish canonical contracts doc.
- Include: event naming scheme, mailbox FSM enums, schedule schema semantics, routing source-of-truth.

2. Resolve ownership matrix with one RACI table.
- Include Inbound Gateway, deploy execution, migration execution, rollback ownership.

3. Run prompt/tool parity audit.
- For each prompt instruction, verify required tool exists in template tool list.

4. Freeze v0.4.3 as implementation baseline.
- Apply only normalization changes; avoid introducing net-new features.

5. Add “contract tests” in Phase 1.
- Migration tests for schemas/enums.
- Event name validation tests.
- Authorization tests for routing modifications.

## Suggested Go/No-Go Gate

Go to implementation when all are true:
- One routing storage model is canonical.
- Scheduler schema supports one-shot dynamic timers.
- Prompt/tool parity gaps are closed.
- Event naming and mailbox enums are canonicalized.
- Ownership conflicts (Inbound, deploy path) are resolved.

Until then: hold implementation to avoid expensive refactors during Phase 1-2.
