# Fail-Fast & Error Handling Audit

**Date:** 2026-04-02
**Scope:** All production Go code under `internal/` and `cmd/`
**Goal:** Catalog every instance where errors are silently swallowed, fallback values mask bugs, or observability is degraded. Each finding includes the fix expectation.

---

## Severity Definitions

| Severity | Meaning |
|----------|---------|
| **P0 — CRITICAL** | Silent data loss, state corruption, or security risk in hot paths. Fix immediately. |
| **P1 — HIGH** | Errors swallowed in important paths; bugs will be hard to diagnose. Fix before next release. |
| **P2 — MEDIUM** | Degraded observability or edge-case data loss. Fix as time permits. |
| **P3 — LOW** | Cosmetic or cold-path issues. Acceptable to defer. |
| **OK** | Acceptable Go idiom (e.g., `_ = tx.Rollback()` in defer). No action needed. |

---

## Table of Contents

1. [P0 — Critical: Silent Event & State Loss](#p0--critical)
2. [P1 — High: Swallowed Errors in Important Paths](#p1--high)
3. [P2 — Medium: Degraded Observability & Edge Cases](#p2--medium)
4. [P3 — Low: Cold-Path or Cosmetic Issues](#p3--low)
5. [Structural Anti-Patterns](#structural-anti-patterns)
6. [Acceptable Patterns (No Action)](#acceptable-patterns)

---

## P0 — Critical

### C01. Pipeline event emission silently dropped
**File:** `internal/runtime/pipeline/coordinator.go:429`
```go
_ = pc.bus.Publish(ctx, emitted)
```
**Also at:** `coordinator.go:458` (`_ = pc.bus.PublishDirect(ctx, emitted, recipients)`)

**Impact:** These are the main event emission paths for workflow pipeline state transitions. If `Publish` or `PublishDirect` fails, the event is permanently lost. Downstream agents, workflow steps, and entity state updates that depend on these events never fire. No log, no dead letter, no retry.

**Fix:** Return the error from `publish()` and `publishDirect()`. Callers must handle it — either retry, dead-letter the source event, or propagate up.

---

### C02. Workflow state transitions silently lost
**File:** `internal/runtime/pipeline/workflow_state_persistence.go:47`
```go
_ = pc.workflowStore.Mutate(ctx, entityID, func(instance *WorkflowInstance) { ... })
```
**Also at:** `:89` (gate mutations), `:118` (evidence recording)

**Impact:** If the DB write for a workflow state transition fails, the in-memory state and the persisted state silently diverge. The workflow appears to have transitioned but hasn't. On restart, the entity reverts to its old state. Gates and evidence are also silently lost.

**Fix:** Return the error. The caller (`applyWorkflowStageMutation`, `applyWorkflowGateMutation`, `recordWorkflowEvidence`) must propagate it so the pipeline coordinator can dead-letter or retry the source event.

---

### C03. Inbound webhook events lost despite 202 response
**File:** `internal/runtime/inbound.go:157-158`
```go
if err := g.bus.Publish(pubCtx, ...); err != nil {
    log.Printf("inbound publish failed provider=%s entity=%s err=%v", provider, entityID, err)
}
// ... writes 202 Accepted to the HTTP client
```

**Impact:** External callers (Stripe, GitHub, etc.) receive HTTP 202, believe the event was accepted, and will NOT retry. The event is permanently lost. This is a data loss boundary with external systems.

**Fix:** If `bus.Publish` fails, respond with HTTP 500 or 503 so the external provider retries. At minimum, persist the raw webhook payload to a dead-letter table before responding 202.

---

### C04. Human task result silently dropped
**File:** `internal/runtime/agents/agent_llm.go:181`
```go
_ = a.injectHumanTaskToolResult(ctx, evt)
```

**Impact:** When a human completes a task (approval, input, etc.), the result must be injected into the agent's conversation. If this fails silently, the agent never sees the human's response and continues without it. The human thinks their action was recorded; the agent ignores it.

**Fix:** Check the error. If injection fails, do NOT proceed with `conversation.Step`. Either retry or dead-letter the event.

---

### C05. `shouldFallbackLegacyEventsSchema` — SQL error string matching for query routing
**File:** `internal/store/events.go` (lines 33, 68), `internal/store/event_receipt_store.go` (lines 26, 51, and ~10 more call sites)
```go
if err := s.appendEventSpec(ctx, tx, evt); err == nil {
    return nil
} else if !shouldFallbackLegacyEventsSchema(err) {
    return err
}
// ... silently falls back to legacy query
```

The function `shouldFallbackLegacyEventsSchema` matches against error message substrings like `"event_id"`, `"run_id"`, etc.

**Impact:** Any SQL error whose message happens to contain a column name (e.g., a constraint violation mentioning `event_id`) is misidentified as a schema mismatch. The code silently executes a completely different query against a different table structure. No logging occurs on fallback. Schema migration failures are invisible.

**Fix:** Use explicit schema version detection (e.g., query `information_schema.columns` at startup) rather than error-message parsing at runtime. At minimum, log every fallback invocation with the original error.

---

### C06. Empty eventID/agentID returns silent success in receipt store
**File:** `internal/store/event_receipt_store.go:20-21`
```go
if eventID == "" || agentID == "" {
    return nil  // silent success
}
```
**Also at:** `:43-44` (UpsertEventReceipt)

**Impact:** An empty eventID or agentID is always a programming error upstream. By returning nil (success), the receipt is never written. Events appear undelivered and may be retried indefinitely, or error receipts are never recorded, preventing dead-letter escalation.

**Fix:** Return an error: `return fmt.Errorf("receipt: eventID and agentID required")`. An empty ID should fail fast so the caller is forced to fix the bug.

---

### C07. Empty receipt status defaults to "processed"
**File:** `internal/store/event_receipt_store.go:46-47`
```go
if status == "" {
    status = runtimemanager.ReceiptStatusProcessed
}
```

**Impact:** An empty status is a bug in the caller. Defaulting to "processed" means a failed event could be marked as successfully processed, preventing retry and dead-letter escalation.

**Fix:** Return an error for empty status.

---

### C08. `WithSystemPrompt` silently wipes agent config on malformed JSON
**File:** `internal/runtime/manager/helpers.go:196-198`
```go
obj := map[string]any{}
if len(raw) > 0 {
    _ = json.Unmarshal(raw, &obj)
}
obj["system_prompt"] = prompt
```

**Impact:** If `raw` contains malformed JSON, `obj` stays `{}`. All existing config keys (model, permissions, subscriptions, constraints) are silently wiped. The agent runs with only `{"system_prompt": "..."}`. No `json.Valid` guard. This is in the hot path during agent setup.

**Fix:** Check the unmarshal error. If `raw` is non-empty and fails to parse, return an error — do not silently replace the entire config.

---

## P1 — High

### H01. Dead letter DB inserts silently fail
**Files:**
- `internal/runtime/pipeline/coordinator.go:254` — `_ = runtimedeadletters.Insert(...)` (chain depth exceeded)
- `internal/runtime/pipeline/coordinator.go:298` — `_ = runtimedeadletters.Insert(...)` (handler error)
- `internal/runtime/pipeline/node_system_runner.go:226` — dead letter persist logged but swallowed
- `internal/runtime/manager/receipts.go:362` — dead letter recording logged but swallowed

**Impact:** The dead letter audit trail has silent gaps. Operators querying the `dead_letters` table get an incomplete picture of failures.

**Fix:** Propagate the error from `coordinator.go`. For `node_system_runner.go` and `receipts.go`, at minimum ensure the error is surfaced via a structured runtime log (not just `log.Printf`).

---

### H02. Idempotency ledger write fails silently — causes duplicate processing
**File:** `internal/runtime/pipeline/node_system_runner.go:335`
```go
if _, err := n.db.ExecContext(ctx, `INSERT INTO system_node_ledger ...`); err != nil {
    slog.Error("system node mark processed failed", ...)
}
```

**Impact:** If the ledger INSERT fails, the event is not marked as processed. On the next poll cycle, the same event is reprocessed, causing duplicate execution of potentially non-idempotent workflow nodes.

**Fix:** Return the error from `markProcessed`. The caller should decide whether to retry or dead-letter.

---

### H03. Session heartbeat failure can cause split-brain
**File:** `internal/runtime/sessions/heartbeat.go:46`
```go
if err != nil {
    log.Printf("session lease heartbeat failed: agent=%s runtime=%s err=%v", ...)
    continue
}
```

**Impact:** If multiple consecutive heartbeats fail, the lease expires and another runtime instance steals the session. The current runtime continues processing, unaware. Two runtimes process events for the same agent simultaneously — split-brain.

**Fix:** Track consecutive heartbeat failures. After N failures, the runtime should stop processing events for that agent and attempt to re-acquire the lease.

---

### H04. Scheduled event publish fails but schedule marked as fired
**File:** `internal/runtime/runtime.go:229-237`
```go
if err := rt.Bus.Publish(callbackCtx, ...); err != nil {
    log.Printf("schedule publish failed ...")
}
if err := exactStore.MarkScheduleFiredExact(callbackCtx, sc); err != nil {
    log.Printf("mark schedule fired failed ...")
}
```

**Impact:** Two failure modes: (1) Publish fails + mark-fired succeeds = event lost, schedule won't retry. (2) Publish succeeds + mark-fired fails = duplicate events on next tick.

**Fix:** These two operations should be in a transaction, or at minimum: do NOT mark as fired if publish failed.

---

### H05. Startup schedule restoration failures leave workflows stalled
**Files:**
- `internal/runtime/runtime.go:392` — `restoreSchedule` error logged, loop continues
- `internal/runtime/runtime.go:396` — `ensureLifecycleWorkflowSchedules` error logged, continues
- `internal/runtime/runtime.go:399` — `ensureRecurringWorkflowSchedules` error logged, continues

**Impact:** Timer-based workflow transitions (timeouts, delays, polling) silently fail to register. Workflows get permanently stuck at timer nodes with no indication.

**Fix:** Collect all schedule restoration errors and return them from `Start()`. The operator must know if the runtime started with missing schedules.

---

### H06. Budget guardrail evaluation errors silently swallowed
**File:** `internal/runtime/budget.go:180, 198, 282`
```go
_ = t.evaluateAndEmit(ctx, rec.EffectiveEntityID())
```

**Impact:** If guardrail evaluation fails (e.g., DB down), budget limits are not enforced. Spend continues uncapped. The emergency human-in-the-loop budget approval event never fires.

**Fix:** At minimum, log the error. Ideally, if budget evaluation fails persistently, pause the agent (similar to auth breaker).

---

### H07. Agent contract prompt load fails silently — agent runs with wrong prompt
**File:** `internal/runtime/manager/agent_manager.go:266`
```go
RuntimeWarn("agent-manager", "contract prompt load failed ...")
return cfg  // original config without the contract prompt
```

**Impact:** The agent starts without its contract-defined prompt. It uses a fallback/default prompt, producing incorrect behavior that looks like a logic bug rather than a config issue.

**Fix:** Return an error. Do not register an agent whose contract prompt failed to load.

---

### H08. Agent reconfigure leaves stale session
**File:** `internal/runtime/manager/agent_manager.go:344`
```go
if rotated, err := sessions.Rotate(...); err != nil {
    log.Printf("agent reconfigure session rotation failed ...")
}
```

**Impact:** The agent is reconfigured with new tools/prompt but continues using the old session. The old session's conversation history references tools/prompts that no longer exist, causing LLM confusion or tool-call failures.

**Fix:** If session rotation fails, either retry or fail the reconfiguration entirely.

---

### H09. Auth breaker shutdown failure — runtime continues in unauthorized state
**File:** `internal/runtime/manager/receipts.go:289`
```go
_ = am.Shutdown()
```

**Impact:** If shutdown fails after the auth breaker trips, the runtime continues processing events without valid credentials.

**Fix:** Check the error. If shutdown fails, escalate (e.g., os.Exit or a critical alert).

---

### H10. Builder breakpoint pause silently fails
**File:** `internal/builder/runs.go:417, 454, 547`
```go
_ = h.pauseRuntime()
```

**Impact:** When a breakpoint is hit or a human task requires input, the runtime should pause. If `pauseRuntime()` fails, the runtime keeps running past the breakpoint.

**Fix:** Check the error. If pause fails, at least log a structured error.

---

### H11. Conversation history silently lost
**Files:**
- `internal/runtime/llm/api_runtime.go:305` — `UpsertConversation` error logged, swallowed
- `internal/runtime/llm/cli_runtime_helpers.go:50` — same

**Impact:** On restart, the agent has no memory of prior turns. Dashboard shows stale or missing conversation data.

**Fix:** For the API runtime, this should be an error. For CLI runtime, at least emit a structured diagnostic event.

---

### H12. `mustJSON` doesn't "must" — returns nil on error
**File:** `internal/store/llm_store.go:334`
```go
func mustJSON(v any) []byte {
    b, err := json.Marshal(v)
    if err != nil {
        return nil  // "must" but doesn't panic
    }
    return b
}
```

**Impact:** Callers assume the result is valid JSON. A nil return gets written to the database, corrupting stored LLM conversation state. The misleading name causes false confidence.

**Fix:** Either panic (true `must` semantics) or rename to `tryJSON` and have callers check the result.

---

### H13. Workflow instance metadata silently empty on bad JSON
**File:** `internal/runtime/pipeline/workflow_instance_store.go:532`
```go
_ = json.Unmarshal(fieldsRaw, &fields)
```

**Impact:** In the pipeline hot path. If `fieldsRaw` contains corrupt JSON, all instance metadata fields (slug, custom fields) silently vanish. Downstream workflow routing/gating decisions that depend on metadata behave incorrectly.

**Fix:** Check the error. If metadata can't be parsed, return an error to the pipeline coordinator.

---

### H14. Retry count query failure breaks dead-letter escalation
**File:** `internal/store/event_receipt_store.go:235`
```go
_ = s.DB.QueryRowContext(ctx, `SELECT COALESCE((side_effects->>'retry_count')::int, 0) ...`).Scan(&retryCount)
```

**Impact:** If this query fails, `retryCount` stays 0. The dead-letter escalation threshold (2 retries) is never reached. A consistently-failing event retries indefinitely instead of being dead-lettered.

**Fix:** Check the error. If the retry count can't be read, use a safe default that still allows escalation, or propagate the error.

---

### H15. Agent panic/failure lifecycle events silently dropped
**File:** `internal/runtime/manager/runtime.go:729, 763`
```go
if err := am.bus.Publish(..., events.Event{Type: "platform.agent_panic", ...}); err != nil {
    RuntimeWarn(...)
}
```

**Impact:** Monitoring systems, manager agents, and dashboards never learn about agent panics or terminal failures. Agents die silently.

**Fix:** If the lifecycle event can't be published, persist it to a fallback table or at minimum emit a structured slog error (not just `RuntimeWarn` which is `log.Printf`).

---

### H16. Agent "failed" status not persisted after poison-pill panics
**File:** `internal/runtime/manager/runtime.go:737`
```go
_ = am.store.UpsertAgent(ctx, PersistedAgent{...Status: "failed"})
```

**Impact:** After 5 consecutive panics, the agent loop exits but the DB still shows the agent in its previous status. On restart, the runtime tries to run the agent again, hitting the same panic loop.

**Fix:** Check the error. If the upsert fails, log a critical error. Consider writing to a separate "poison agents" table as a fallback.

---

### H17. Contradiction events are log-only no-ops
**File:** `internal/runtime/bus/eventbus_routing.go:223-226` (called from `eventbus_publish.go:182,383` and `outbox.go:138`)
```go
func (eb *EventBus) emitContradiction(ctx context.Context, evt events.Event, reason string) error {
    log.Printf("contradiction detected: event=%s type=%s reason=%s", evt.ID, evt.Type, reason)
    return nil
}
```

**Impact:** Despite the function name, contradictions are never emitted as events. Callers discard the return with `_ =` thinking it might fail, but it always returns nil. Contradictions are only visible in stdout logs, which may not be monitored.

**Fix:** Either implement actual event emission, or rename to `logContradiction` and remove the error return to avoid confusion.

---

## P2 — Medium

### M01. LLM usage/budget tracking silently fails
**Files:**
- `internal/runtime/llm/cli_runtime.go:442` — CLI usage recording
- `internal/runtime/llm/api_runtime.go:271` — API usage recording

**Impact:** Budget tracking becomes inaccurate. Agents could exceed spend limits. Cost attribution per entity is silently wrong.

**Fix:** Log a structured error. Consider a background retry queue for failed budget writes.

---

### M02. Runtime event logging (`diagnostics.go`) is entirely fire-and-forget
**File:** `internal/runtime/diagnostics.go:91`
```go
_ = logRuntimeEventSpec(withoutSQLTxContext(ctx), l.db, level, component, action, e, detail)
```

**Impact:** The entire structured runtime logging pipeline is fire-and-forget. If the DB write fails, the runtime log is permanently lost. Operators have no way to know their observability is broken.

**Fix:** At minimum, fallback to `slog.Error` on failure so the log still appears in stdout. Consider a buffered write with retry.

---

### M03. Turn telemetry silently lost
**Files:**
- `internal/runtime/llm/api_runtime.go:315` — `AppendAgentTurn` error logged, swallowed
- `internal/runtime/llm/cli_runtime_helpers.go:24` — same

**Impact:** Agent turn audit trail has gaps. Debugging agent behavior from the dashboard is unreliable.

**Fix:** Log a structured error (not just `log.Printf`).

---

### M04. Session provider ID update fails silently — stale sessions
**File:** `internal/runtime/llm/cli_runtime.go:401`
```go
if err := adoptRegistrySessionID(ctx, ...); err != nil {
    log.Printf("failed to adopt claude session id ...")
}
```

**Impact:** The session registry keeps a stale provider session ID. Subsequent turns may fail or start fresh context unknowingly.

**Fix:** Log a structured error. Consider retrying the adoption.

---

### M05. `anyString` returns empty string on marshal failure — credential key loss
**Files:**
- `internal/runtime/mcp/client.go:480`
- `internal/runtime/credentials/requirements.go:250`

**Impact:** If a complex value can't be marshaled, the credential lookup key becomes `""`, potentially matching the wrong credential or failing silently.

**Fix:** Return `("<marshal_error>")` or similar sentinel so the empty string is not confused with a legitimate empty key.

---

### M06. Workflow transition history silently dropped on bad JSON
**File:** `internal/runtime/pipeline/workflow_instance_store.go:576-581`

Double error swallow — both `json.Marshal` and `json.Unmarshal` failures return `nil`.

**Impact:** Entire transition history lost for an entity. Could cause re-execution of already-completed transitions.

**Fix:** Return an error alongside the slice.

---

### M07. Config extension decode failures hide sharding/budget config
**File:** `internal/config/config.go:119, 132`
```go
_ = c.DecodeExtensions(&ext)
```

**Impact:** If config extensions contain malformed YAML/JSON, sharding and budget configuration silently defaults. Budget enforcement could be accidentally disabled.

**Fix:** Check the error. Fail startup if config cannot be parsed.

---

### M08. `deliverToAgents` drops events on slow consumers
**File:** `internal/runtime/bus/eventbus_routing.go:88-100`

If the channel send blocks past `deliverySendTimeout`, the event is dropped with only a warn log. No dead letter, no error return, no receipt.

**Fix:** On timeout, write a dead letter record and mark the delivery as failed.

---

### M09. Schema version detection via `isProcessed` is completely silent
**File:** `internal/runtime/pipeline/node_system_runner.go:280-299`

Two DB queries are tried; all errors are swallowed with no logging. Returns `false` on any failure.

**Fix:** Log the errors. The safe-side default (re-process) is fine, but invisible DB failures should be visible in logs.

---

### M10. Receipt write failure breaks delivery tracking
**File:** `internal/runtime/manager/receipts.go:307-324`

After retry with fresh context, final failure is `log.Printf` only.

**Fix:** Emit a structured diagnostic event on final receipt write failure.

---

### M11. `markPipelineReceipt` swallows upsert errors
**File:** `internal/runtime/bus/eventbus_routing.go:240-244`

**Impact:** Pipeline receipt tracking has silent gaps.

**Fix:** Log via structured runtime logger, not just `logRuntime`.

---

### M12. Entity type defaults to "default" instead of failing
**Files:**
- `internal/runtime/tools/executor_entity.go:116-117`
- `internal/runtime/pipeline/workflow_instance_store.go:622`

**Impact:** Missing entity types bypass type-specific validation, routing, or schema constraints.

**Fix:** Return an error if entity type is required by the workflow definition.

---

### M13. Credential provider defaults to "default"
**File:** `internal/runtime/credentials/requirements.go:221-222`

**Impact:** Malformed credential specs silently fall back to "default" provider, potentially returning wrong credentials.

**Fix:** Log a warning at minimum. Consider failing if the spec has other fields that suggest a provider was intended.

---

### M14. SSL mode defaults to "disable"
**File:** `internal/store/postgres.go:20-22`

**Impact:** If config fails to specify SSL mode, DB connection is unencrypted in production.

**Fix:** Default to `"require"` or fail if not explicitly set.

---

### M15. Backward-compatible config keys accepted without deprecation warning
**File:** `internal/runtime/agents/agent_llm.go:94-106`

Top-level `conversation_mode` and `max_turns_per_task` are silently accepted alongside the `constraints.*` versions.

**Fix:** Log a deprecation warning when the old location is used.

---

### M16. Inbound entity slug falls back to raw key
**File:** `internal/store/inbound.go:54-56`

**Impact:** An empty slug from DB means the entity record is incomplete. Using the raw key masks data integrity issues.

**Fix:** Log a warning. Consider returning an error.

---

### M17. Outbox sweeper failures are silently absorbed
**File:** `internal/runtime/bus/sweeper.go:58`

**Impact:** If sweeping persistently fails, events accumulate in the outbox indefinitely.

**Fix:** Track consecutive failures. After N failures, emit a critical alert or health check degradation.

---

### M18. `ListActiveAgentIDs` failure silently returns unfiltered recipients
**File:** `internal/runtime/bus/eventbus_routing.go:21-24`

**Impact:** On DB failure, delivery records may be created for agents that no longer exist.

**Fix:** Return the error instead of silently falling back.

---

## P3 — Low

### L01. Trace report unmarshal errors (7 instances)
**File:** `internal/store/trace.go:145, 234, 341, 343, 345, 346, 347`

All in cold observability path. Trace reports show empty fields for corrupted data. Low risk but could mislead debugging.

**Fix:** Log a warning on unmarshal failure.

---

### L02. Diagnostics default "unknown" for empty action/handler
**File:** `internal/runtime/diagnostics.go:85-86, 124-125`

Multiple distinct bugs map to the same "unknown" label.

**Fix:** Log a warning when defaulting. Consider `"unset"` instead of `"unknown"`.

---

### L03. Budget model defaults to "unknown"
**File:** `internal/runtime/budget.go:295`

**Fix:** Log a warning. The LLM runtime should always populate the model field.

---

### L04. Schema DDL index name collision
**File:** `internal/store/schema_ddl.go:285-287`

Multiple failed name derivations all get `"idx_generated"`, causing DDL conflicts.

**Fix:** Append a counter or hash to the fallback name.

---

### L05. `safeExecuteTool` recover loses stack trace
**File:** `internal/runtime/llm/conversation.go:256-264`

Panic is converted to error but `debug.Stack()` is not captured (unlike `safeProcessEvent`).

**Fix:** Capture and include the stack trace.

---

### L06. Monitor event summarization returns empty on errors
**File:** `internal/runtime/llm/monitor_sink.go:205-206, 236-237`

**Fix:** Log a warning instead of returning empty.

---

### L07. Schedule `resolvedPayload` returns original on parse failure
**File:** `internal/store/schedule_store.go:269-276`

`__schedule_task_id` never injected for malformed payloads.

**Fix:** Log a warning. Consider returning an error.

---

### L08. `withConfigString` returns original config on marshal failure
**File:** `internal/store/agent_store.go:456-459`

**Fix:** Return an error instead of silently ignoring the mutation.

---

### L09. Dashboard `intQuery` returns fallback for malformed params
**File:** `internal/dashboard/server/server.go:801-813`

Standard REST API behavior. Very low risk.

**Fix:** Optional: return 400 Bad Request for non-integer values.

---

## Structural Anti-Patterns

### S01. `RuntimeWarn` / `runtimeWarn` is just `log.Printf`
**File:** `internal/runtime/warnings.go:12-22`

All `RuntimeWarn` calls throughout the codebase are thin wrappers around `log.Printf`. They do NOT write to the structured runtime log table or emit events. If stdout is not monitored, these warnings are invisible.

**Recommendation:** `RuntimeWarn` should write to the runtime log table (same as `RuntimeLogger.Log`) as a fallback. At minimum, use `slog.Warn` with structured fields instead of `log.Printf`.

---

### S02. No metrics export
There is no Prometheus client, OTEL metrics, or any exported metric counters. Error rates, event throughput, dead letter rates, and budget spend are only visible via DB queries or the dashboard API.

**Recommendation:** Add at minimum: error rate per component, event publish/delivery rates, dead letter rate, budget spend rate. Even simple counters exposed via `/metrics` would enable alerting.

---

### S03. `emitContradiction` is misleadingly named
The function only calls `log.Printf` and returns nil. It does not emit an event. Three callers discard its return value with `_ =`.

**Recommendation:** Either implement it or rename it to `logContradiction`.

---

### S04. Transient agent errors silently return nil
**File:** `internal/runtime/manager/receipts.go:57-59`

When `isTransientAgentError` matches, `processEvent` returns `nil` — no receipt written, no log. The event depends entirely on the retry loop for re-delivery, but there's no guarantee it will be retried before expiry.

**Recommendation:** At minimum log the transient error. Consider writing a "pending" receipt so the retry loop explicitly knows to re-deliver.

---

## Acceptable Patterns

These patterns are standard Go idioms and require **no action**:

| Pattern | Count | Reason |
|---------|-------|--------|
| `_ = tx.Rollback()` in defer | 15 | Standard Go pattern; rollback after commit returns `sql.ErrTxDone` |
| `_ = conn.Close()` / `_ = file.Close()` | 15 | Resource cleanup; error is non-actionable |
| `_ = json.NewEncoder(w).Encode(v)` (HTTP responses) | 12 | Client may have disconnected; non-actionable |
| `_, _ = fmt.Fprint(w, ...)` (SSE keepalives) | 6 | Best-effort SSE writes |
| `_, _ = h.Write(...)` (hash.Hash) | 9 | `hash.Hash.Write` never returns an error per Go spec |
| `_, _ = w.Write([]byte("ok"))` (health checks) | 2 | Non-actionable |
| `_ = variable` (unused var suppression) | ~15 | Compiler idiom, not error swallowing |

---

## Summary Counts

| Severity | Count | Key Theme |
|----------|-------|-----------|
| **P0 — Critical** | 8 | Silent event loss, state corruption, data loss at system boundaries |
| **P1 — High** | 17 | Swallowed errors in pipeline, sessions, budget, config, lifecycle |
| **P2 — Medium** | 18 | Degraded observability, silent defaults, edge-case data loss |
| **P3 — Low** | 9 | Cold-path issues, cosmetic naming, minor fallbacks |
| **Structural** | 4 | Cross-cutting anti-patterns |
| **Acceptable** | ~74 | Standard Go idioms |

**Total actionable findings: 56**
