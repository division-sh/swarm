# EmpireAI Changelog

## v4.0.0 (2026-03-26)

### Breaking: messaging permissions renamed
- `message_all` removed — replaced by `message_flow` (same flow instance scope)
- `message_domain` removed — replaced by `message_flow` (same flow instance scope)
- `message_peers` unchanged (same manager_fallback parent, same flow instance)
- Cross-flow communication is events only. agent_message cannot cross flow boundaries.
- All bundles updated: coordinator, lead, specialist now use `message_flow`
- All agent explicit permissions updated across root, discovery, scoring, validation, operating

### Breaking: management and routing scoped to flow instance
- agent_hire/fire/reconfigure: target must be in same flow instance and below caller in manager_fallback chain
- configure_routing: self + manageable agents only
- manager_fallback is an escalation path (produces events), not a messaging or management grant

Requires platform >=1.3.0 (permission_scoping support).

## v3.1.0 (2026-03-26)

### Workspace classes defined
Added `workspace_classes` to policy.yaml — declares the three Empire workspace classes per platform spec §workspace_model:
- `holding` (per-agent) — root agents (empire-coordinator, operations-analyst, holding-devops)
- `factory` (per-agent) — discovery, scoring, validation agents
- `opco` (per-flow-instance) — operating company agents share /workspace within their OpCo instance

Platform dependency bumped to >=1.3.0 (requires workspace_model support).

## v3.0.5 (2026-03-25)

### Permission fixes (root agents)
- Added `human_task_decide` to coordinator bundle — empire-coordinator uses the human_task_decide tool but the permission was missing from the bundle
- Added `system_admin` workflow extension permission — gates infrastructure tools (nginx_reload, systemd_control, certbot_execute). Defined in `workflow_extension_permissions` per platform spec §permissions_model.workflow_extensions
- Added `system_admin` to holding-devops explicit permissions (broke off shared anchor since no other agent needs it)

## v3.0.4 (2026-03-25)

### Permission bundle fixes
- Added `schedule` to coordinator, lead, specialist, and worker bundles — all operating agents use the schedule tool for self-reminders; no bundle previously included it
- Added `human_task_request` to specialist bundle — opco-marketing and opco-support (promoted to specialist in prior fix) use human_task_request in tools_tier2 but specialist lacked the permission
- Net result: all 12 tool/permission mismatches resolved (10 schedule, 2 human_task_request)

## v3.0.3 (2026-03-13)

### Handler count: 62 (was 51)
New handlers: spec.draft_ready, spec_review.passed, spec_review.issues_found (validation review chain fix), plus build-orchestrator handlers.

### Event count: 195 (was 181)
New events: spec.draft_ready, spec_review.requested, spec_review.passed, spec_review.issues_found, budget.alert_sent, mailbox.review_requested, vertical.marginal_review_due, research.additional_requested, validation.package_ready, and others.

### System nodes: 7 (was 6)
build-orchestrator split from lifecycle-orchestrator for build pipeline gate enforcement.

### Validation review chain fixed
spec.draft_ready → spec_review.requested → [reviewer] → spec_review.passed → cto.spec_review_requested. Loop broken: spec.approved no longer routes to spec_review.requested. spec.validation_requested removed (stale).

### All legacy actions removed
36 legacy action values (set_gate, kill_vertical, finalize_validation, emit_spinup, accumulate_signal, etc.) replaced with declarative handler fields. Only platform actions remain (create_flow_instance, record_evidence).

### State machine fixes
- Validation: cto_spec_review → cto_review (name alignment)
- Operating: expanding → growth (name alignment)
- Both: killed added to states (was in terminal_states but not states)

### Condition → payload alignment
All guard/rule conditions verified against event payload schemas:
- scoring rules: payload.discovery_mode → payload.mode
- derivation guard: payload.derived_name → payload.opportunity_name
- mailbox rules: decision == approve → payload.decision == approve

### system.directive structured payload
directive_text (string) → directive: {type, parameters} (object). Matches portfolio-node rule conditions.

### on_complete ordering
Scoring on_complete converted from dict (unordered) to list (ordered, first-match-wins).

### Scoring dedup_by
score.dimension_complete accumulator: dedup_by: payload.dimension (content-identified, not sender-identified).

### Produces lists synced
All 7 system node produces lists verified against actual handler emits.

### Event metadata
All events categorized: _source: external, _consumer: mailbox_system, _status: planned. 0 unresolved warnings.

### Anti-bias routing
analysis-agent (primary) subscribes to scoring.requested. analysis-agent-secondary subscribes to scoring.derived_requested. Same prompt, different pool.

### spec.validation_passed gate fix
Sets g2_spec_review (not g3_cto). Advances to cto_review.

### Phantom events cleaned
scoring.contest_resolved, scoring.contested removed (YAGNI). Empty strings in pins cleaned. deploy.completed orphan removed. g_build_complete phantom gate removed.

## v3.0.2 (2026-03-10)

### Formally retired files
empire-spec.md, spec-writer-guide.md, SPEC-COMPLIANCE.md — replaced by contracts-as-spec, platform system_node_specification, and verify.py respectively.

## v3.0.1 (2026-03-09)

### All handlers declarative
8 product hooks eliminated. 100% YAML.

## v3.0.0 (2026-03-09)

### First independent release
Split from monolithic spec v2.6.0. 4 flows (discovery, scoring, validation, operating) + root. Platform dependency >=1.0.0.
