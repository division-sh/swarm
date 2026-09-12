# #2444 Merged-Owner Integration

Agent-g. Qualification **pending**; not a closure or review-ready claim.

Rebased cleanly onto `origin/master` at `2344162f9` after the first qualification.
That run failed: strict environment admission rejected the test-only SWARM
export; direct pipeline calls lost dispatch after allocating an emission plan;
the selected negative fixture dropped acknowledged event evidence; cancellation
barriers expected SQL interruption instead of admitted drain; and SQL-mock
expectations lacked E's activity-order fence. The fixes preserve product
validation and existing owners. Focused pipeline activity and dual-store
generation/reset/receiver fencing controls now pass, including the latter with
race detection (46.577s). CLI read-window/validation and configured-monitor-deadline
spec controls pass with TEST_POSTGRES_BIN supplied (0.423s). Final rerun remains
pending; the full managed run is first in the shared capacity queue.

## Baseline and Boundary

- Actual merged master: `0d7513f76bea234a7c153f488c7167847d1d8bd5` (#2445).
- Preserved pre-integration checkpoint: `48f73ad30`.
- Binding sequencing: #2444 comment `5626314957`. E's preparation, retained
  selected lifetime, exact source inputs and fingerprints remain authoritative.
- Worktree: `/tmp/agent-g-2444-merged-owner-port`. The original #2008 worktree
  and checkpoint worktree remain untouched.
- Stock pq only; no vendoring, schema, compatibility reader, new owner or retry.

## Conflict and Consumer Disposition

The thirteen textual conflicts were resolved semantically, not with whole-file
ours/theirs selection. The consumer census includes cleanly merged call sites.

| Seam | Disposition and execution path |
| --- | --- |
| EventBus publication | Retain E's exact `liveDeliveryDispatch` recipient outcome; preserve G's acknowledged publication/deferred handoff even alongside an independent delivery error. |
| Continuation coordinator | Retain E's before-scan and exact held-entry comparison; drain an admitted read, then check retirement before dispatch or another page. |
| Delivery persistence | Both outcome runners include E's receiver-dependent settlement before revision finalization and COMMIT; no independent dependent-settlement writer. |
| External recovery | Existing handoff/outcome owner, with E's explicit source-scope argument and authority restrictions unchanged. |
| Fork materialization and activation | Existing transaction runner, plus E's original loop/fan-out carriage, exact source input projection, preparation binding and source-freeze admission. No predecessor-state reconstruction. |
| Selected container construction | E's container constructor owns failed claimed-execution settlement, including claim-plus-error. Caller cleanup does not call `Fail` again. Unsettled returned authority prevents discard. |
| Selected execute and activation consumers | Preserve acknowledged results and cleanup errors separately; retain the existing prepared context when activation committed even if a later close/handoff fails. |
| Selected stop | New merged-master writer uses the backend outcome runner and existing author-activity/revision finalization. Runtime returns the exact terminal result plus failed post-commit recovery rather than an empty result. |
| Selected recovery | New merged-master writer uses the same outcome owner; list uses the read runner. Runtime preserves committed earlier/current results and still fences recovery admission on any failure. |
| Selected startup grant | Existing process-capability owner withdraws local permission before blocked possession SQL completes; selected proof and ownership-read returns recheck retirement. Dependencies remain joined. |
| Preparation/readiness/declarations, receiver materialization, generation fence and effect recovery | Existing transaction-local helpers stay within their admitting materialization, claim, delivery or recovery transaction. No separate mutation/COMMIT owner was added by E. |
| Original-source lookup | Explicit different concept: `LoadRunForkSourceRunID` is a standalone read-only snapshot. Like planner and selected-plan reads it remains caller-cancellable, with no reusable session authority. |
| Persistence inventory | Regenerated and reviewed fourteen additional call sites, classified as private-domain adapters. No grandfathered writer bypass. |

The authoritative spec amendment belongs to
`engine.runtime_core_persistence_store_contracts.backend_neutral_runtime_mutation_write_boundary.rules`.
E's selected preparation/input/retained-lifetime contracts are not weakened.

## Proof Ledger

These receipts cover this integration; earlier component timings do not.

| Proof | Current result and limits |
| --- | --- |
| Production `go build ./...` | Passed after resolving the production conflicts; final qualification still required. |
| Startup-ownership and continuation packages, race | Passed. |
| API conversation-fork and selected adapter outcome matrix, race | Passed, 22.696s. |
| Selected writer/activation/source-freeze runtime-persistence matrix, race | Passed, 25.114s. |
| Native PostgreSQL claim exit matrix, race | Passed, 4.410s, after supplying E's real durable preparation/process facts to the relational fixture. |
| Selected proof/ownership-read local-fence matrix | Passed with race detection, three repetitions. Four barrier cases prove late success cannot revive authority and independent errors survive; these are owner fixtures, not transport proof. |
| Runtime selected execution and direct activation outcome matrix | Actual runtime cases passed; the old direct fixture omitted original source identity and was repaired. Direct controls pass with the control-lifetime matrix; the whole combined selector remains required in final qualification. |
| Selected stop/recovery consumer outcome matrix | Passed with the existing control-lifetime matrix and direct activation controls, race, 38.944s. Real dual-store lifecycle paths with a post-operation error wrapper prove runtime result preservation and refusal; not a native COMMIT fault injection. |
| Persistence inventory | Passed after regeneration and explicit classification. |
| API-spec and persistence-inventory guards | Passed, 0.424s and 5.285s. |
| Supported serve, remote loss/silence, SIGKILL and final managed whole suite | Pending on the integrated tree. Managed suite submitted with private PostgreSQL configuration; awaiting shared test capacity. |

## Tracking and Closure

No new issue or semantic gate: this implements the existing E-first disposition.
#2444 remains the repair tracker. #2250/#2412/#2159 retain broader architecture
debt; generic API response reconstruction remains #1930. Existing watchlist
mapping remains applicable. Intended closure is the whole approved #2444 class,
not a claim that merging E alone removed G's outcome/cancellation defects.
