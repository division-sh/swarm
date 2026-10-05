# Lifecycle Proof-Reader Consumption: Not Runtime-Class Closure

Measured on clean `cb8b83ef9722bccb83a097d821bda70b4f274184`, based on
`origin/master@0fa24140aaa05e397509f451ba780fef3a260867`. This is the census of
the three touched lifecycle proof helpers and the two template-specific stage
and accumulator reads, not a replacement for the production owner/consumer
audit. The selected `flow_instances` header remains the runtime authority;
these helpers are bounded test readbacks, not new semantic owners.

## Retired Readers And Invariants

- `requireLifecycleFlowEntity`: exact run/path and stage come from the header;
  a field row is neither construction evidence nor required for presence.
- `readLifecycleTransitionHistory`: exact run/entity configuration comes from
  the header; historical transition evidence is not gated by companion presence.
- `lifecycleStoredSnapshot`: headers are inventoried with exact optional field
  companions. Stage/revision/accumulator belong to the header. `sql.NullString`
  explicitly preserves absent versus present-empty fields; no COALESCE repair.
  Existing event/delivery/receipt snapshot members remain unchanged.
- Template accumulator and field-bearing template lookup read header progress.
  The exact companion join in the latter still serves its authored-field proof,
  not field-row construction authority. The template's seven-companion count
  is not an inventory of all constructed, including fieldless, instances.

Source census commands:

```sh
rg -l 'requireLifecycleFlowEntity\(|readLifecycleTransitionHistory\(|lifecycleStoredSnapshot\(' internal/serveapp --glob '*test.go'
rg -n '^func Test' <the 13 files listed below>
rg -n 'e\.(current_state|accumulator|revision)' internal/serveapp/lifecycle_transition*test.go internal/serveapp/lifecycle_release_process_test.go internal/serveapp/lifecycle_emitter_competing_exit_test.go
```

The last search has zero matches. The 13 direct-consumer files contain 18 test
roots: 17 relevant lifecycle roots and one separate immutable-artifact check.
Every relevant root has a literal terminal receipt below: 15 earlier PASS receipts
and two newly passing template roots. These are not all current-head qualification.
No parent-path, nested recovery or refusal is inferred from a shared helper.

## Exact Receipt Owners

- **A**: `06383234726ab42a421687e165e000c1f769a652`, managed race/count1,
  four roots/26 named subtests PASS133.714s; no failure/skip.
  `issue-2496-header-lifecycle-063832347.json` includes the literal command,
  exact cells and log hash. Its three touched Go files match `cb8b83ef9`
  byte-for-byte; this equality does not relabel the source of execution.
- **B**: `cb8b83ef9722bccb83a097d821bda70b4f274184`, managed race/count1,
  nine roots/22 named subtests PASS305.546s; no failure/skip.
  `issue-2496-header-recovery-cb8b83ef9.json` retains exact supported recovery,
  frozen-control refusal, forced process kill, graceful restart, static-fork,
  duplicate and source-preservation receipts on both stores.
- **C**: the same `cb8b83ef9`, managed race/count1, two roots/22 named subtests
  (16 leaf scenarios) PASS117.941s; no failure/skip.
  `issue-2496-header-process-contention-cb8b83ef9.json` retains the four literal
  public `verify`/release `serve` journeys and twelve in-process ordinary/timer
  contender scenarios. Paid provider keys are unset; no real-provider claim.
- **U**: `869ff6e9a`, uninstrumented four-cell template journey/refusal matrix
  RED411.133s on both stores. `issue-2496-nested-consumer-qualification.json`
  retains the exact cells and separate diagnostic provenance. Parent-relative
  route admission is escalated within this PR at issue comment5964992264;
  the later recovery/refusal assertions are not reached and receive no credit.
- **V**: authorized local parent-identity repair, pre-commit worktree based on
  `c8139aad6`; the precise source boundary is in
  `issue-2496-four-dispositions-served-v2.json`. Both original template roots
  PASS under race/count1, 38.92s and 34.16s, all four store cells, actually
  reaching restart and fork-refusal assertions. The combined command remains
  RED at the provider replay's stale oracle; this is not exact-head qualification.
- **W**: `50107a2ff` production/test/spec inputs: both original nested journeys
  PASS race/count1,35.66s/34.29s, all four backend cells reaching restart/refusal.
  The whole receiver composition is five roots/28 cells PASS278.694s without
  failures/skips. `issue-2496-default-repair-50107a2ff.json` records the complete
  source/command/hash; this does not qualify A join consumers or the full default.

All consumer paths below are **moved to the canonical header in this work**.
That consumption classification is distinct from the execution result.
Paths are relative to `internal/serveapp/`.

| Direct Consumer File | Exact Resident Lifecycle Root | Proof |
| --- | --- | --- |
| `lifecycle_transition_gate_test.go` | `TestServedCompiledGateAdvanceOnlyOnBothStores` | A: PASS |
| `lifecycle_transition_nested_test.go` | `TestServedCompiledTransitionNestedCarrierCollisionOnBothStores` | A: PASS; exact eight ready headers before work, redundant seed refusal and sibling isolation |
| `lifecycle_transition_served_test.go` | `TestServedCompiledTransitionSelectedCarrierEvidenceOnBothStores`; `TestServedCompiledLoopEscapeSuppressesOrdinaryRepeatOnBothStores` | A: both PASS |
| `lifecycle_transition_fork_test.go` | `TestServedCompiledFrozenGateForkControlRefusalOnBothStores`; `TestServedCompiledRootLoopForkProjectsRevisionOnBothStores` | B: both PASS; original deferred-work refusals retained |
| `lifecycle_transition_gate_process_test.go` | `TestServedCompiledGateOutcomeRestartOnBothStores` | B: PASS; compiled internal process killed after commit, public RPC readback, not public launcher |
| `lifecycle_transition_gate_restart_test.go` | `TestServedCompiledGateOutcomeGracefulDrainRestartOnBothStores` | B: PASS |
| `lifecycle_transition_node_recovery_test.go` | `TestServedCompiledLoopNodeRecoveryReexecutionOnBothStores` | B: PASS; only the existing admitted frontier/recovery contract |
| `lifecycle_transition_replay_test.go` | `TestServedCompiledLoopTransitionReplayOnBothStores` | B: PASS |
| `lifecycle_transition_restart_test.go` | `TestServedCompiledGateFrozenTransitionEvidenceOnBothStores`; `TestServedCompiledTransitionRestartOnBothStores` | B: both PASS |
| `lifecycle_transition_static_fork_test.go` | `TestServedCompiledTransitionStaticForkEvidenceOnBothStores` | B: PASS; exact four scopes, duplicate response and source/child preservation |
| `lifecycle_release_process_test.go` | `TestReleaseCompiledLifecycleJourneysBothStores` | C: PASS; public verify/serve binary, nested gate and connected loop on both stores |
| `lifecycle_emitter_competing_exit_test.go` | `TestServedLifecycleEmitterCompetingExitPublication` | C: PASS; gate-first/exit-first/contended, ordinary/timer, both stores |
| `lifecycle_transition_template_test.go` | `TestServedCompiledTransitionNestedTemplatesFirstJourneyOnBothStores`; `TestServedCompiledTransitionTemplateSiblingForkCapabilityRefusalOnBothStores` | U: earlier RED retained; V/W: both PASS with original downstream assertions reached; W is current50107a2ff input |

The eighteenth resident root,
`TestRetainedRootForkOriginalArtifactPreserved`, is **different semantic concept,
with proof**: `lifecycle_transition_fork_test.go:20` only hashes the retained
historical source artifact and compares its literal SHA256. It calls none of
these readers, starts no runtime and cannot prove lifecycle/header consumption.
Its focused PASS0.010s receipt is
`issue-2496-retained-artifact-cb8b83ef9.json`, not counted among the 17 roots.

## Closure And Push Disposition

These passing named executions validate the test-reader migration and its live
sibling paths. Ruling5965454522 authorizes all four bounded owner corrections;
their focused execution evidence does not replace final composed qualification,
pending composed-head matrices or A's shared request fixtures. The complete
provider root now passes154 named cells race/count1 on471a0cd06,268.607s,
including both-store agent replay. The cleanup/selected-parent supplement passes
35 roots/120 named cells across three packages, including all six causal cells.
Their exact sources/commands are retained in issue-2496-provider-alias-471a0cd06.json
and issue-2496-cleanup-parent-471a0cd06.json; no prior default is relabeled.
A's separate #2394 reporter qualification remains distinct. The production audit
continues to claim major partial canonicalization, not class closure.

The passing default receipt is independently frozen on `446789777`: all14 units
PASS/90 inventoried skips. The newer a154729c7 default is RED:66 root failures,
230 named failures and13 unexecuted units. The historical repaired-source receipt
issue-2496-default-repair-50107a2ff.json accounts for all66:50 E-owned roots now
have passing focused reruns;16 A-owned fixture roots and the production join
coordinate consumer were pending at that freeze. The final two timer/template roots pass
race/count3 (18.623s), not a full default. No descendant credit is taken from
the earlier passing default. Final composed
supplements/default, exact-head hosted CI and independent review remain needed.
The input-only A handoff is now integrated as5a74adbae; all21 handoff roots
PASS race/count1 on d8d863c3d,97 named receipts. Canonical join lookup and
complete persisted/prospective header consumption pass seven component roots
race/count3 on3824b29a5. The13-file/17-root test-reader census above is unchanged;
these production consumers are separately enumerated in the full proof audit.

The actual d8d863c3d default passes all66 earlier failing roots but is RED at
the new real timed-descendant preparation control on both stores. All13 later
planned units PASS in separate unchanged invocations, not green-default credit.
Fresh45 describe cells PASS. Commands, skips, source attribution and raw hashes
are in issue-2496-default-d8d863c3d-red.json and
issue-2496-handoff-qualification.json. The isolated declared-agent source probe
also refuses the canonical descendant; it is component proof only.
Join/gate/agent source carriers remain a same-PR shared-owner obligation at
5967326775/5967680220; no downstream assertion or class closure is inferred.
All changes/evidence are local; every push is forbidden until G's #2544 merges.
