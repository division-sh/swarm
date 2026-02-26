# EmpireAI v2.0.1-2 Implementation Gap Review

Date: 2026-02-20
Scope: Implementation vs `docs/specs/empireai-v2_0_1-2.md` (with focus on Appendix B factory roles and end-to-end factory flow)

## Status Update (Post-Implementation Pass)

The following blockers from this review have now been implemented:

- P0.1 missing factory agents: added B.7-B.11 YAML configs and roster registration.
- P0.3 factory event mismatch: added `vertical.discovered`, `vertical.scored`, `vertical.shortlisted`, `vertical.marginal`, `vertical.rejected`, `validation.started`, `research.completed`, `spec.requested`, `spec.draft_ready`, `spec_review.requested`, `spec_review.passed`, `spec.approved`, `brand.requested`, `brand.candidates_ready`, and `vertical.ready_for_review`.
- P0.4 `agent_message` authority: now enforces sender->recipient authority and management-chain authorization.
- P1.1 discovery dedup: scan flow now deduplicates by `(name, geography)` against existing `verticals`.
- P1.2 mode propagation: scoring now applies mode-specific rubric selection and emits dimension events (`scoring.requested`, `score.dimension_complete`).
- P1 additional: mode research signals now emit `category.assessed` (saas_gap) and `trend.identified` (saas_trend).

Outstanding from this document:
- `produced_event_unrouted` remains a **warning** class by design (to allow direct-message-only flows). Unknown producers and unconsumed route patterns are now blockers.

## Executive Verdict

Current state is **not fully spec-compliant**. Core runtime infrastructure is strong (Postgres persistence, template publish flow, mailbox/human-task plumbing, budget tracking, dashboard graph), but the **factory role model and event contract remain materially incomplete**.

Launch/implementation readiness for strict v2.0.1-2 compliance: **No-Go** until blocker gaps below are closed.

## Note on Sub-Agents

Attempted to spawn parallel sub-agents for this review, but the current session is at agent-thread cap. This report is from direct code/spec inspection.

## Compliance Matrix for Requested Sections

| Spec Section | Status | Evidence | Gap Summary |
|---|---|---|---|
| B.0.1 Factory CTO | Implemented | `configs/agents/factory-cto.yaml:1` | Prompt includes architecture standards, feasibility approve/revise/veto, template ownership, analyst review, cross-vertical patterns. |
| B.4 Discovery Coordinator | Partial | `configs/agents/discovery-coordinator.yaml:1`, `internal/factory/scan_runner.go:16` | Prompt covers 3 modes and dedup intent, but runtime path is deterministic pipeline runner and does not implement true mode-specific delegation + evidence event chain. |
| B.5 Scoring Coordinator | Partial | `configs/agents/scoring-coordinator.yaml:1`, `internal/factory/pipeline.go:171` | Prompt references rubric orchestration, but runtime uses hash heuristic and emits `factory.scoring_completed` not full spec event set (`vertical.shortlisted/marginal/rejected/scored`). |
| B.6 Validation Coordinator | Partial | `configs/agents/validation-coordinator.yaml:1`, `internal/factory/pipeline.go:230` | Prompt has lifecycle language, but runtime performs monolithic function-based validation flow; missing full event-driven orchestration and more-data loop semantics. |
| B.7 Business Research Agent | Missing | no YAML under `configs/agents/` | Not seeded, no prompt, no runtime actor implementation. |
| B.8 Lightweight Spec Agent | Missing | no YAML under `configs/agents/` | Not seeded, no prompt, no runtime actor implementation. |
| B.9 Spec Reviewer | Missing | no YAML under `configs/agents/` | Not seeded, no stateless single-pass reviewer role in runtime flow. |
| B.10 Market Research Agent | Missing | no YAML under `configs/agents/` | Not seeded, no taxonomy walker runtime role. |
| B.11 Trend Research Agent | Missing | no YAML under `configs/agents/` | Not seeded, no trend categorization runtime role. |

## Blockers (P0)

1. Missing factory agents required by spec
- Severity: Blocker
- Evidence:
  - Global roster only has 8 agents: `configs/agents/roster.yaml:1`
  - Seed path uses roster as source of truth: `internal/dashboard/server.go:2592`, `internal/templateops/global_agents.go:89`
- Impact:
  - Required B.7-B.11 roles never spawn.
  - Factory cannot execute required delegated workflow.

2. Factory flow is pipeline-hardcoded, not role-orchestrated
- Severity: Blocker
- Evidence:
  - Deterministic scan runner bypassing agent behavior: `internal/factory/scan_runner.go:16`
  - Monolithic scoring/validation functions: `internal/factory/pipeline.go:171`, `internal/factory/pipeline.go:230`
- Impact:
  - Spec-mandated cross-agent behavior is not actually happening.
  - Prompt compliance alone does not translate to runtime compliance.

3. Event contract mismatch in factory path
- Severity: Blocker
- Evidence:
  - Emits `factory.scoring_completed` instead of spec event sequence: `internal/factory/pipeline.go:221`
  - Missing emissions in current implementation path: `vertical.discovered`, `vertical.shortlisted`, `vertical.marginal`, `vertical.rejected`, `validation.started`, `spec.requested`, `spec_review.requested`, `category.assessed`, `trend.identified` (no production emit path found)
- Impact:
  - Downstream routing/coordination semantics diverge from spec.
  - Dashboard and audit trail cannot represent required lifecycle.

4. `agent_message` authority model not enforced per hierarchy
- Severity: Blocker
- Evidence:
  - Message tool checks existence + same vertical only: `internal/runtime/tool_executor.go:298`
  - No manager-chain/authority validation in `agent_message` execution path.
- Impact:
  - Violates spec Message Authority constraints.
  - Allows out-of-policy direct messaging.

## High Gaps (P1)

1. Discovery dedup against vertical table is not implemented
- Evidence:
  - Scan flow inserts generated vertical rows directly without dedup query/merge: `internal/factory/pipeline.go:71`
- Impact:
  - Duplicate verticals and noisy pipeline state.

2. Mode propagation exists in payload but does not drive scoring rubric logic
- Evidence:
  - `mode` carried in `scan.completed`: `internal/factory/scan_runner.go:78`
  - Scoring model remains fixed heuristic (`v1.7-weighted`): `internal/factory/pipeline.go:201`
- Impact:
  - Does not satisfy dual-rubric selection requirement.

3. Business Brief / Lightweight Spec structures are under-scoped
- Evidence:
  - Business brief has minimal fields: `internal/factory/pipeline.go:243`
  - MVP spec lacks explicit exclusion list and formal user story artifacting: `internal/factory/pipeline.go:253`
- Impact:
  - Misses required content/discipline from B.7/B.8.

4. Spec Auditor does not enforce full contract completeness required by spec
- Evidence:
  - Template validator checks structure and producer hints, but not full producer/consumer completeness by event catalog contract: `internal/specaudit/auditor.go:85`
- Impact:
  - Invalid templates can pass without full communication-contract integrity.

## Medium Gaps (P2)

1. Test coverage does not include missing factory-role behavior
- Evidence:
  - No tests referencing required missing roles/events in test suite (`rg` over `internal/*test.go` returns no hits for B.7-B.11 role/event names)
- Impact:
  - Regressions likely during role-flow implementation.

2. Factory fallback remains tied to older model naming/behavior
- Evidence:
  - Scoring model tag `v1.7-weighted`: `internal/factory/pipeline.go:201`
- Impact:
  - Spec drift and operator confusion.

## What Is Already Strong / Aligned

1. YAML -> Postgres template publish path exists and is validated
- `internal/dashboard/server.go:2667`
- `internal/specaudit/auditor.go:24`

2. Communication graph section (5.7) now implemented in dashboard/backend
- `internal/dashboard/graph_communication.go:11`
- `internal/commgraph/registry.go:1`

3. Human task async completion is implemented in conversation context
- `internal/runtime/agent_llm.go:105`

4. Budget tracking + threshold events + spend ledger are implemented
- `internal/runtime/budget.go:135`
- `migrations/001_initial.sql:360`

## Recommended Implementation Slices

### Slice A (P0): Factory Role Surface Completion
- Add YAML configs + prompts for:
  - `business-research-agent`
  - `lightweight-spec-agent`
  - `spec-reviewer`
  - `market-research-agent`
  - `trend-research-agent`
- Add them to `configs/agents/roster.yaml` and ensure seed/init path spawns them.

### Slice B (P0): Event Contract-Correct Factory Flow
- Replace/augment `factory.scoring_completed` path with spec events:
  - `vertical.discovered`
  - `vertical.scored`
  - `vertical.shortlisted` / `vertical.marginal` / `vertical.rejected`
  - `validation.started`, `spec.requested`, `spec_review.requested`
- Keep deterministic fallback only behind explicit test/dev mode flag.

### Slice C (P0): Enforce Message Authority
- Add authority checks to `agent_message` tool path:
  - manager chain constraints
  - allowed sender->recipient role matrix (holding + opco)
- Reject violations with explicit error codes.

### Slice D (P1): Mode-Driven Delegation + Dedup
- Implement mode-specific discovery delegation and scoring rubric selection.
- Dedup discoveries against existing `verticals` using stable similarity key before insert.

### Slice E (P1): Spec Auditor Contract Hardening
- Add strict checks for producer-consumer coverage and required mailbox round-trip contract integrity.
- Keep medium warnings for extensibility, but fail on contract-critical holes.

### Slice F (P1/P2): Tests
- Add integration tests for:
  - B.4-B.11 role chain
  - full event chain from `scan.requested` to mailbox item
  - agent_message authority enforcement
  - mode rubric selection and marginal path handling

## Bottom Line

The platform is technically solid, but strict v2.0.1-2 compliance is currently blocked by the **factory role/runtime contract gap**. Closing P0 slices A-C first will unlock true spec-aligned behavior; P1 slices then harden correctness and auditability.
