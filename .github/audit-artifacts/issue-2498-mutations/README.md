# #2498 Behavioral Mutation Qualification

Agent-g, 2026-09-30. Disposable candidate: `cfb6b3b5f`; base:
`origin/master@abdb07bfe`. These patches are archived evidence, not applied
production code, a mutation runner, or another corpus executor. Every arm was
applied alone in `/tmp/agent-g-2498-mutation-qualification`, compiled and run on
SQLite and host PostgreSQL, then removed with an exact inverse edit.

The local `.gitattributes` treats only these nine archived diff payloads as data
for whitespace checks: unified-diff context prefixes must precede the original
Go tabs. It does not exempt production code or other audit artifacts. Patch
bytes and their recorded mutation evidence remain unchanged.

## Historical Diff And Current-Owner Equivalence

The historical production diffs were inspected with `git show <commit> --
<path>`. Literal historical inverses no longer apply after routing/identity
refactors; the gate authorizes these bounded current-owner equivalents.

| Group | Historical production correction inspected | Admitted equivalent, not a generic router break |
|---|---|---|
| U1 | #2167 / PR #2216, `276f9d04d23e039d56627ff2dea4b0bf2537e93e`, `internal/runtime/bus/delivery_planner.go`: concrete/scoped node intent production and canonical target resolution; `ee346951f` later moved no-target ownership out of metadata | `U1-local-node.patch` removes only the real keyed `candidate.analyzed` local-node intents from `ordinaryPublicationSource.localNodeIntents`. Root, scout, agent subscription and other publication branches remain intact. |
| U2 | #2169 / PR #2221, `17fb74cf5872034dbb09c390e0a89e6feb951fe9`, `internal/runtime/bus/routing_derivation.go`: replace authored empty `IDTemplate` consumption with `AgentNamePlan.Materialize().AgentID` | `U2-subscriber-id.patch` leaves source/name compilation intact but drops that materialized subscriber ID at actual template instance route insertion. |
| U3 | #2226 (historical corpus label #2227) / PR #2237, `50de0fa259fb5c6416e4bd2579bfc6c36d9b5fb2`, `internal/runtime/pipeline/delivery_target_ownership.go` and `internal/runtime/bus/target_owner_projection.go`: receiver evidence rather than source/structural authority | `U3-static-receiver.patch` changes the canonical `ClassifyDeliveryTargetOwnership` decision only for the real root-to-static `scout.requested` route, stamping the producer root entity into the materializing scout target. It does not mutate just the EventBus wrapper. |
| U4 | #2246 / PR #2252, `09da85ed5b92a12b3063a56502436832e19600c5`, `internal/runtime/bus/delivery_planner.go`, `route_plan.go` and `target_owner_projection.go`: independent local/connect composition and recipient-specific targets | `U4a-local-suppression.patch` suppresses only the local branch of actual mixed `scout/scout.completed`; `U4b-target-collapse.patch` separately copies the connected intent's target into the local intents of that same event. No arbitrary broadcast mutation. |
| U5 | #2254 / PR #2235, `5285698142a67491bd66dd1036f00ccfd876a40b`, persisted descriptor write/hydration: carry FlowDataAccess and BudgetEnvelope instead of losing them | Four independent patches omit flow-data on write/read and the nonzero budget on write/read. Flow-data earns actual activation/restart credit; default-zero agent e2e never earns budget credit. |

## Exact Execution And Red Evidence

All commands use `-count=1`; verbose output records both backend subtests with
no skipped backend. These are failing qualification receipts, not passing
runtime repairs. The existing owned retained lifecycle child is **H**, not a
provisioned live-provider **L** or public fresh mock-test **T** journey.

Existing command families:

```sh
go test ./internal/releasee2e -run '^TestGoldenAgentWorkloadRestartAndForcedKillOnBothBackends$' -count=1 -v -timeout=5m
go test ./internal/releasee2e -run '^TestGoldenAgentWorkloadSequentialRunsBothStores$' -count=1 -v -timeout=7m
go test ./internal/store/internal/runtimepersistence -run '^TestPersistedAgentReadinessFieldsBothStores$' -count=1 -v -timeout=2m
```

| Arm | Existing executed proof | Both-store discriminating failure | Total package result |
|---|---|---|---|
| U1 | Restart/forced-kill | Real candidate turns and keyed emit calls are reached. Exact `candidate.analyzed` emits report `event_publish_failed`; scout completion publishes normally. Required keyed finalizer rows/state never converge. The canonical guard rejects the missing lawful branch, not source admission. | expected FAIL 221.332s |
| U2 | Sequential runs | Real root/scout work reaches the fan-out handler, then the public diagnostic names `fan_out_prepare_publication_failed` with cause `materialize route subscriber: delivery recipient kind and id are required`, flow `.`, `scout-collector`/`scout.completed` fan-out ordinal 0. Candidate agents never activate. | expected FAIL, both existing 150s completion deadlines |
| U3 | Sequential runs | Accepted root `search.requested` and persisted `scout.requested` reach exact encoded `scout-intake`. Its materializing target has `flow_id=scout`, `flow_instance=scout`, **entity_id=the root run ID**; delivery is `dead_letter/handler_terminal_failure`. No scout entity is created. | expected FAIL 331.035s |
| U4a | Sequential runs | Accepted `scout/scout.completed` has exactly one delivered `scout-collector` root-target row and no local scout-completion row. All ten candidate entities exist, while the root run remains running and the scout frontier cannot converge. | expected FAIL 346.036s |
| U4b | Sequential runs | Both exact real `emit_scout_completed` operations fail `platform.target_unreachable / route_plan_preflight_failed`; their ten-item payloads are valid. The wrong mixed target is rejected before persistence, so no scout completion event is falsely counted as accepted. | expected FAIL 329.335s |
| U5a flow-data write | Restart/forced-kill | Meaningful pre-kill work/activation checkpoint succeeds; retained startup then refuses the readiness revision whose capability was omitted. | expected FAIL 67.268s |
| U5b flow-data read | Restart/forced-kill | Same real pre-kill checkpoint; retained hydration loses capability and startup refuses the readiness revision. | expected FAIL 27.379s |
| U5c budget write | Readiness fields | Exact nonzero budget expected 1.25 hydrates as 0 on each store; flow-data/revision assertions remain separate. | expected FAIL 3.652s |
| U5d budget read | Readiness fields | Exact persisted nonzero budget hydrates as 0 on each store. | expected FAIL 1.635s |

The red assertion is unsuccessful exact run/route/state convergence, with its
captured public execution chain above. Deadline expiration alone is never the
mutation attribution. The expected backend-owned 150s wait remains unchanged.
No speculative tool retry or replay of already-settled work is used.

U4a public event receipts:

- SQLite run `ec03a527-b274-4e7d-9e99-dceb544cb447`, event
  `ef708fcd-fb1c-50b5-a007-b2492be63591`, sole delivered node
  `Lg.c2NvdXQtY29sbGVjdG9y`, target existing root entity/run.
- PostgreSQL run `44e365b9-4506-4da6-bf5c-758a46f12299`, event
  `414a7a3a-4d26-50cd-8b7e-96a9f13c051d`, same sole encoded root
  collector and exact run-bound root target; no local branch.

U4b exact failed emit receipts:

- SQLite run `7e7b7c48-2e51-4db0-aada-2f2d1b76997d`, attempted event
  `21a26abf-cbc4-534c-85f9-970797ad4ed0`.
- PostgreSQL run `8558a03e-8b2e-4319-bbde-b415c9a5ec2a`, attempted event
  `c0f1b53f-007c-5b37-ac77-3cd1e2d703d1`.
- Each public `emit_tool_outcome` names `emit_scout_completed`,
  `failure_stage=route_plan_preflight`, `ok=false` and the exact code above.

The first broad U1 experiment prevented the crash checkpoint and earns **no
credit**. The first U2 attempt used the restart proof whose checkpoint is before
the relevant failed fan-out; it also earns **no restart/mutation credit**.
The bounded U1 and existing steady U2 proof above supersede only those invalid
proof selections. No fixture, grammar exemption or new executor was introduced.

## Clean Restoration

After the last arm, `git diff --exit-code` in the disposable worktree passed
with no production/test mutation remaining. Clean candidate controls then ran:

```sh
go test ./internal/releasee2e -run '^(TestGoldenAgentWorkloadSequentialRunsBothStores|TestGoldenAgentWorkloadRestartAndForcedKillOnBothBackends)$' -count=1 -v -timeout=5m
go test ./internal/store/internal/runtimepersistence -run '^TestPersistedAgentReadinessFieldsBothStores$' -count=3 -timeout=2m
```

PASS 101.706s (both original paired journeys on both stores); PASS 3.631s
(readiness fields, both stores x3). These are clean restoration controls on the
qualified corpus candidate, not final-head full-suite or merge credit.

## Raw Receipt Integrity

Raw logs remain on the proof host under `/tmp/agent-g-2498-<name>.log`.
The archived patches and bounded facts above make the claims inspectable even
without transporting multi-megabyte failure payloads into the repository.

| Name | SHA256 |
|---|---|
| u1-local-node-bounded-e2e | 5b223006aeeacc9f8bf64b4140dcff4d66d91c26bd95c6bada864012f32451ee |
| u2-subscriber-id-steady-e2e | 40178da384339c31e5f0d8dd7ce6f04cd2763c4c51f3c69e60770321792b414c |
| u3-static-receiver-e2e | a0f20e26e6c6cc250af60771774ac3688cfff307643188d8a19ae739a2dd19c4 |
| u4-local-suppression-e2e | bceb23ce58f625096188e16c5014061bccb29069d0559e3912067a9c02cc7dd7 |
| u4-target-collapse-e2e | 2cdfe43782732666f8efc9072fa357737b0a2d8494e95577179674c9053dc751 |
| u5-flow-data-write-e2e | 58fe07b9d5398bd1e441971908c08c7d6e724b9fe9e4f3b11ad514f47136f8a9 |
| u5-flow-data-read-e2e | e923b0a8b8f1ed2ec27ff544fea504c548f1cc01e00c32458f98e572cbf27fd4 |
| u5-budget-write | cd7a8c7aecc20e41ffd60ea36056173aaa7d724ef2fa0c0ff2be294e76203705 |
| u5-budget-read | 35db9a798b4ee2959f4dc84ff1bcb58499b18596456748793538bd8cf2037958 |
| restored-paired-controls | 47d1a5ce7b42ab1ee58513a52a7d6e3e8b3683d06e27ab1c43037cd33a4a7656 |
| restored-readiness-controls | c68b7ca1bce3e1ac14e78559e21bfaa1ba7b55e19472ef64655069260882bbaf |

#2407/#2250 remain open. #642's selected-fork frontier is not credited by these
ordinary routing controls. Whole-suite and complete S/P/M/RB acceptance are
separate obligations; no merge or entire-parent closure is claimed here.
