# EmpireAI v1.7 Implementation Compliance Checklist

Date: 2026-02-12  
Spec baseline: `docs/specs/empireai-architecture-v1.7.md`

Status scale:
- `PASS`: implemented and wired
- `PARTIAL`: implemented in part, but with bounded simplifications
- `FAIL`: missing

## Pre-Implementation Checklist (Spec Gate)

### Agent completeness
- PASS: Runtime OpCo roster and default spinup are implemented (`internal/runtime/manager.go`)
- PARTIAL: Prompt parity against full Appendix A/B text is not auto-validated at runtime

### Event contract completeness
- PASS: Runtime contradiction path emits `spec.contradiction_detected` for unresolved opco routing (`internal/runtime/eventbus.go`)
- PARTIAL: No standalone static event-catalog linter over markdown spec text

### Routing consistency
- PASS: Routing persistence, mutation auth, and replay are implemented (`internal/runtime/tool_executor.go`, `internal/store/manager.go`, `internal/runtime/manager.go`)

### Deploy flow consistency
- PASS: Core runtime primitives and event path are implemented; hotfix/skip-staging remains represented as event-level policy

### Data model
- PASS: Postgres schema + migrations implemented (`migrations/001_initial.sql`)
- PASS: Runtime paths now use mailbox status canonical enums and timeout semantics (`internal/store/mailbox.go`)

### Cross-references / open questions
- PASS: No unresolved runtime blocker for implementation path

## Phase Status

### Phase 1: Runtime Foundation
- PASS: EventBus + Postgres write-through
- PASS: AgentManager lifecycle + recovery
- PASS: LLM runtime abstraction (API + CLI test)
- PASS: Session registry with lock/rotation
- PASS: Scheduler (cron + one-shot) + persistence restore
- PASS: Inbound gateway + idempotency tracking
- PASS: Mailbox + digest CLI surface
- PASS: Critical notification channel (webhook + Telegram + SMTP email, deduped via `mailbox.notified`)

### Phase 2: Discovery Pipeline
- PASS: `scan` command and scanner components implemented (Google Maps, Instagram, Reviews synthetic adapters) (`internal/factory/scanners.go`, `internal/factory/pipeline.go`, `cmd/empire/main.go`)

### Phase 3: Scoring Pipeline
- PASS: Weighted viability/attractiveness scoring and viability floor gating implemented in factory pipeline (`internal/factory/pipeline.go`)

### Phase 4: Factory Validation
- PASS: Research/spec/branding flow to `ready_for_review` with mailbox handoff implemented
- PASS: Spec auditor lifecycle events implemented (`spec.validation_requested/passed/failed`)
- PASS: `spec-audit` command implemented for explicit audit runs (`cmd/empire/main.go`, `internal/specaudit/auditor.go`)

### Phase 5: Operating Mode
- PASS: OpCo 13-agent roster + hierarchy + manager tools
- PASS: Mailbox semantics and decision side-effects (`vertical.needs_more_data`, spend decision fanout, founder response)
- PASS: Direct communication implemented:
  - `empire directive` async event path
  - `empire chat` live LLM chat with session continuity

### Phase 6: Intelligence & Learning
- PASS: Metrics recording path (`ops record-metrics`)
- PASS: Portfolio/ops tick loop (`ops tick`) with:
  - kill criteria detection mailbox escalation
  - budget pressure alerts
  - routing pattern proposal generation
- PASS: Template governance implemented:
  - template publish
  - migration planning + mailbox approval
  - approved migration apply (`internal/templateops/service.go`)

### Phase 7: Hardening
- PASS: Recovery smoke path and runtime stability checks
- PASS: Added soak-style tests:
  - long-run conversation continuity (60 turns)
  - lock contention behavior (`internal/runtime/soak_test.go`)
- PASS: CLI contention hardening for "session already in use" rotation/retry path (`internal/runtime/llm_cli.go`)

## Go/No-Go
- `Phase 1 production runtime`: **GO**
- `v1.7 implementation compliance baseline`: **GO**

## Residual Notes (Non-blocking)
- Some components use deterministic/synthetic adapters instead of live third-party APIs by default (intentional for local/dev and cost control).
- Spec markdown contract linting is not yet automated as a standalone static checker.
