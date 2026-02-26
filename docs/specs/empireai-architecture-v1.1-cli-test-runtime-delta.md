# EmpireAI v1.1 Delta: CLI Test Runtime + Session Registry

## Purpose
This document summarizes the exact delta added to `docs/specs/empireai-architecture-v1.1.md` to support a low-cost Claude CLI test mode with durable context and implementation-ready session management.

## Scope of Change
- Added dual LLM runtime model (API + CLI test runtime).
- Added runtime/session handling contract for non-interactive Claude CLI use.
- Added persistence schema for session lifecycle and per-turn observability.
- Updated resilience/config/phase sections to align with runtime abstraction.
- Explicitly clarified: tmux is optional and not required for this test mode.

## Delta by Section

## 1) Stack Update
Location: `docs/specs/empireai-architecture-v1.1.md:45`

- Replaced single intelligence line:
  - From: Claude API only
  - To: dual runtime:
    - Claude API (production)
    - Claude CLI non-interactive test mode (`claude -p`)

## 2) Runtime Contract (New/Expanded)
Location: `docs/specs/empireai-architecture-v1.1.md:823`

### Renamed section
- From: `4.4 Claude Conversation Manager`
- To: `4.4 Claude Conversation + Runtime Manager`

### Added runtime abstraction
- New `LLMRuntime` interface with:
  - `StartSession(...)`
  - `ContinueSession(...)`
- Runtime adapters:
  - `AnthropicAPIRuntime` (prod)
  - `ClaudeCLIRuntime` (test)

### Added Claude CLI continuity contract
- First turn:
  - `claude -p --session-id <uuid> ...`
- Subsequent turns:
  - `claude -p -r <uuid> ...`
- Must keep session persistence enabled (no `--no-session-persistence`).
- Must enforce single writer per session.
- Prefer structured output (`--output-format json`).

### Added tmux clarification
- tmux is not required for test mode.
- tmux remains optional for manual operator debugging only.

### Added Session Registry contract
- New `SessionRegistry` interface:
  - `Acquire(agentID)`
  - `Release(lease)`
  - `Rotate(agentID, summary)`
- Added rotation triggers:
  - Context budget threshold
  - Repeated parse/contract failures
  - Explicit reset by manager/operator
  - Optional phase boundary reset

## 3) Data Model Additions
Location: `docs/specs/empireai-architecture-v1.1.md:2445`

### New table: `agent_sessions`
Purpose:
- Track active runtime session per agent.
- Track lock/lease ownership to enforce single-writer semantics.
- Support session rotation with checkpoint summary.

Key fields:
- `agent_id`, `runtime_mode`, `provider`, `session_id`, `status`
- `turn_count`, `checkpoint_summary`
- `lock_owner`, `lock_expires_at`
- `last_used_at`, timestamps

Indexes:
- Unique active-session index:
  - one active session per `(agent_id, runtime_mode)`
- last-used and lock-expiry indexes for scheduling/recovery.

### New table: `agent_turns`
Purpose:
- Per-turn telemetry for dashboard/replay/debug.

Key fields:
- `agent_id`, `session_row_id`, `turn_index`, `task_id`
- request/response payloads
- `parse_ok`, `latency_ms`, `retry_count`, `error`

Indexes:
- agent time-series index
- parse-status index
- uniqueness on `(session_row_id, turn_index)`

## 4) Recovery/Failure Updates
Locations:
- `docs/specs/empireai-architecture-v1.1.md:2941`
- `docs/specs/empireai-architecture-v1.1.md:2954`

### Crash recovery
- Startup now reloads:
  - conversations
  - active runtime sessions (`agent_sessions`)

### Failure handling
- Section renamed:
  - From: `Claude API Failure`
  - To: `LLM Runtime Failure (API + CLI)`
- Added CLI-specific path:
  - retry
  - one repair turn on parse/schema failure
  - rotate session with checkpoint if still invalid
  - escalate with runtime metadata if persistent

## 5) Configuration Consolidation
Location: `docs/specs/empireai-architecture-v1.1.md:3000`

### Config namespace change
- From: `claude:`
- To: `llm:`

### New config structure
- `llm.runtime_mode: api | cli_test`
- `llm.session`:
  - lock TTL
  - rotate-after-turns
  - rotate-on-parse-failures
- `llm.claude_api`: model + retry settings
- `llm.claude_cli`:
  - command, timeout, output format, retries
  - `no_session_persistence: false` (required for continuity)
  - `use_tmux: false` (optional debug-only)

## 6) Implementation Plan Updates
Locations:
- `docs/specs/empireai-architecture-v1.1.md:3108`
- `docs/specs/empireai-architecture-v1.1.md:3172`

### Phase 1 additions
- LLM runtime abstraction
- Session registry implementation with single-writer locking

### Phase 7 additions
- CLI runtime soak testing:
  - long-run resume continuity
  - lock contention behavior
  - rotation/recovery validation

## Explicit Design Decisions Captured
- CLI test mode is viable without tmux.
- Session continuity should use provider session resume semantics.
- Concurrency safety is a runtime responsibility (lease/lock).
- Observability is first-class (turn-level persistence + dashboard readiness).

## Non-Goals in This Delta
- No change to org model (CEO/CoS/VP/workers).
- No change to routing philosophy (bootstrap/seeded/discovery).
- No change to authority matrix or governance gates.
