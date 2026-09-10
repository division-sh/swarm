# Live Restart Observation After The Approved Capability Repair

Agent: agent-g. #2321. This is a classification request, not a new runtime repair
or a Post-Implementation Proof Audit claiming closure.

Approved capability repair and bounded proof are pushed in **2c958d04f**.

## Proven Boundary

The approved inventory repair passes the deterministic channel/presence matrix,
startup/fork comparisons, actual fresh/resumed tool-result process failure
settlement, full LLM package and focused race checks. Actual public live serving
now completes the first Claude -> MCP -> approved Telegram connector turn and
settles the original delivery on SQLite and PostgreSQL. It no longer sends the
reply and then fails capability validation.

The unchanged retained mock H J1-J5 SQLite/PostgreSQL matrix also passes (55.288s).
It cannot prove provider-native session restoration.

## Remaining Live Failure

Command: authorized TestCommandLiveServeAndRestartParity through swarm-test,
both stores, fresh test databases, standard swarm-workspace image with Claude
2.1.87. Log: /tmp/agent-g-2321-live-inventory-repair-3.log.

The test fails after 246.257s (SQLite 122.85s, PostgreSQL 117.65s). Both stores:

1. First signed ingress, approval, real reply and original delivery converge.
2. Graceful shutdown completes; public serve restarts against the retained store.
3. Standing service retains run identity/started_at and reaches public readiness.
4. Second signed ingress is accepted, but no reply approval appears.

Read-only PostgreSQL public observation before cleanup:

- First conversation turn: parse_ok=true, one dispatch, two tools, two tool
  results, one output, no failure; conversation turn_count=1.
- Second turn: parse_ok=false, one dispatch, zero tools/results/output,
  failure claude_cli_process_failed, component claude-cli-adapter,
  operation run_streaming.
- Second delivery terminally dead-letters after one retry bookkeeping transition.
  External effect authority rejects re-entry with
  external_effect_replay_fingerprint_conflict; expected and requested fingerprints
  are equal, but operation/attempt state is outcome_uncertain. No second provider
  dispatch is recorded for that turn. This is not a claim of duplicate sends.
- First and second normalized event identities were distinct; the failure is not
  replaying the first ingress. The harness did not reset/replay any sent delivery.

SQLite's post-restart detailed public capture ran after cleanup and was unavailable.
Failure capture is now deferred until before cleanup; the existing fresh-store PG
diagnostic rerun additionally reads only uncertain attempt stage/operation/stdout/
stderr through the existing diagnostic DB connection and secret redactor.

## Classification To Resolve

Observed failure is provider process resumption after container/process restart,
not exact tool inventory parsing: the provider exits before response validation.
Do not automatically attribute it to #2432's durable diagnostic projection.

Source inspection suggests a provider-state lifetime mismatch, not yet proven as
the process error's root cause: cli_runtime.go resumes the confirmed provider
head; Dockerfile.workspace runs as agent with a container-local home; workspace/
manager.go persists /workspace, while ReleaseSourceProjection removes the
process-owned containers. No CLAUDE_CONFIG_DIR or Claude home persistence is
configured by these production paths. A stored provider ID is not proof its
provider-local transcript survived. Underlying attempt stderr is still needed
before treating that hypothesis as a confirmed defect or deciding its repair.

Request lead classification/absorb-or-split direction once the pending diagnostic
result is available. No provider-state storage, container retention, blind resume
fallback, fresh-session retry, compatibility path or new framework is authorized
or implemented. Existing inventory repair can be reviewed independently as the
approved bounded change, but #2321 remains not review-ready: live restart, #2432
and final integrated full-suite/merge proof remain outstanding.

## Diagnostic Result

The fresh PostgreSQL diagnostic rerun completed and failed in 120.763s (journey
115.40s), after waiting for the shared swarm-test slot. Log:
/tmp/agent-g-2321-live-restart-diagnostic.log. No diagnostic job remains pending.

It again completed the first live turn/delivery, restarted successfully, and failed
the distinct second turn. Deferred capture obtained the uncertain attempt's
persisted evidence before cleanup:

```json
{"stage":"provider_call","operation":"wait_streaming","stderr":""}
```

Its stdout starts with a provider result carrying type=result,
subtype=error_during_execution, duration_ms=0, duration_api_ms=0, is_error=true,
num_turns=0 and total_cost_usd=0. The existing summarizeCLIErrorOutput truncation
cuts the persisted JSON before its explanatory error field. Therefore this proves
failure before response/capability validation and no reported provider API work,
but does not prove the missing-transcript hypothesis. No further paid sends are
needed merely to re-demonstrate the stalled second turn. Diagnostic output
retention was not broadened in production under the inventory approval.

The lead classification request remains open. No new runtime class is silently
absorbed; no provider state migration, container-preservation or retry workaround
has been added. The integrated full suite has not been rerun on an approved #2432
repair because that repair remains frozen. No PR is opened or marked review-ready
while those explicit acceptance obligations remain outstanding.
