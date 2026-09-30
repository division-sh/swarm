# Implementation Stop: Paused Interrupted-Feed Restart

Agent-g, #2498, 2026-09-30. Base `origin/master@4fccc57ec73ce54827167dddc5bbafac41ae66d2`. Bounded production readback commit: `44a5f88fb`. The corpus gate and bounded readback gate remain recorded approvals, not closure. The readback amendment was posted before production edits; semantic watchlist repair is docs master `771c598`, with active checkpoint-stop refinement `1b957a9`.

Historical stop evidence is preserved below. Gate5902935619 rejects the recovery-disabled proposal and requires the complete pause-aware executable-work eligibility class. The additive `issue-2498-pause-eligibility-preimplementation.md` supplies the repaired census/probes/proposed spec delta and requests a fresh independent gate. No option below is authorization to change runtime or substitute the interrupted-work proof.

## Completed Bounded Repair / Evidence

The candidate uses `IntentRequest.OriginBundleHash` in readback and store source/grant checks, deleting the store-local duplicate. Comparable `IntentKey` now owns strict disjoint wire encoding/decoding; deployment wire is exactly `run_id`/`deployment_feed_id`. In-memory page ordering includes all five SQL/cursor components. Authoritative OpenRPC schema/order and generated `openrpc.json` change together. No fallback, compatibility, new endpoint/filter, queue or framework.

| Approved row | Execution evidence on candidate |
|---|---|
| RB1 origin bundle | Full fanoutobligation package count=3; open/closed readback and malformed-origin bundle refusal controls pass |
| RB2 mixed/multiple feed ordering | `TestFanOutReadMixedOriginPaginationBothStores` count=3 and race count=3: limits1/2, exact five-key sequence, open/zero-row-closed/paused facts, every continuation, wrong run/filter and noncanonical cursor refusal; existing selected-store `TestFanOutRead*` controls pass |
| RB3 wire/OpenRPC | `TestFanOutIntentKeyExactOriginWire`, both-origin `TestFanOutReadAPISchema` positives/negatives count=3, API/readback controls, generated OpenRPC check; no empty handler fields on deployment keys |
| RB4 supported consumers | Actual compiled CLI run creation, acknowledged public pause, `run.fan_out.list` with real runtime enrichment (`available`, `eligible=false`, `reason=run_paused`) and compiled `run fan-out list --json` validate on both stores before the subsequent restart failure |

These are bounded repair receipts, not complete corpus or full-suite qualification. The process test's overall result remains failed. M09/M10/M11/M12 and U1-U5 are not credited from this prefix.

## Exact New Checkpoint

An immediate pause originally captured cursor=0, zero events and zero pending deliveries; that cannot earn interrupted-work credit. The corrected probe publicly polls for a positive partial cursor before pause, without sleeps as success, private hooks, larger input, replay, or changed pump configuration. After acknowledged pause it reads the exact durable feed and full public event/delivery rows, then forces death. First dual-store execution captured 32/100 issued, 68 owed, 32 row events and 32 nonterminal deliveries on each store. Thus the checkpoint is reachable and genuinely unsettled, not a settled restart.

Retained restart with the existing golden `recovery_on_startup: true` fails before readiness on both stores. The count=3 control reproduced the exact 32/100, 68 owed, 32 events, 32 nonterminal-delivery checkpoint and startup refusal in **all six backend executions**. SQLite checkpoint time was 1.3239/1.3253/1.3256 seconds; PostgreSQL was 1.8487/1.7596/1.7621 seconds. Exact diagnostics identify `purpose=recovery`, `event_type=item.registered`, `reason=run_dispatch_blocked`; the final error is `manager event loop start: pipeline recovery blocked before explicit exhaustion`. Public `run.continue` is therefore unreachable in this proof's proposed order. This is not the old readback error, a provider dependency, absent topology claim, deadline workaround or proven loss/duplicate.

Full command output is retained at `/home/youmew/.cache/agent-g-2498-paused-restart.log`, SHA-256 `5d8c2efe92dba23068835ab33db5b8ccf650d4577be81b2c798f50240639dcbc`. Reproducer:

```sh
go test ./internal/releasee2e -run '^TestGoldenNumericDataScatterParkRestartBothStores$' -count=3 -v -timeout=5m
```

The normal host PostgreSQL test DSN is required; no live provider or Telegram operation occurs. The exact checkpoint facts and both-store outcomes are captured before Fatal.

## Contract Classification / Stop Boundary

Relevant existing owners, not modified by this candidate:

- `EventBus.publishClaimedPipeline` / `dispatchQueueReason` consumes the canonical run dispatch gate and returns `ErrRunDispatchBlocked` for the paused run.
- `EventBus.sweepPipelineObligations` classifies this as locally blocked, retaining pending obligations rather than falsely settling them.
- `RecoveryManager.RecoverToExhaustion` refuses `Blocked`, even alongside `Exhausted`, and the runtime keeps readiness closed on that refusal.
- `platform-spec.yaml#engine.dynamic_instance_lifecycle.creation.route_materialization` requires mandatory process reconstruction before optional pipeline recovery; `cli_specification.command_catalog.serve` startup ordering requires pipeline exhaustion before continuation and public readiness. Both running and paused generations remain active/recoverable for topology; that is not permission to dispatch a paused run.

Existing `TestRecoveryManagerStartupRequiresExplicitExhaustion`, `TestRecoveryManagerRejectsBlockedEvenWhenScanExhausted` and dual-store `TestStartupRecoveryClassifiesBlockedBranchesOnBothStores/.../run_dispatch_blocked` all pass count=3. They protect the refusal boundary; weakening `Blocked` into ready or auto-unpausing would violate it. These controls do not independently decide whether a persistently paused run ought to be non-eligible for a startup scan, versus an otherwise eligible event blocked during dispatch.

The approved M12 proof requires pause -> kill -> recovery-enabled readiness -> continue, but the real existing owner refuses before continue is reachable. This is a **proof/contract contradiction requiring lead classification**, not permission to call the runtime incorrect or silently alter startup policy. Potential dispositions to review: approve an existing supported recovery-disabled restart + explicit public continue recovery leg (only if its topology admission and proof credit are valid), or separately approve the bounded paused-work versus eligible-startup-work distinction through existing owners. A running-process crash checkpoint could be another proof amendment, but must genuinely retain unsettled-work evidence rather than race to a settled restart. No option is implemented here.

## Tracking / Closure

Keep #2498 and #2407 open. Record this active M12 stop in the existing invariant-suite watchlist and in #2498, without inventing a new issue or forcing it into the bounded readback class. The readback repair is preserved separately; no pipeline, run-control, recovery, selected-store schema, retry, startup reorder, or compatibility repair is made. No further corpus implementation or qualification begins before the independent disposition. The original #2008 WIP is untouched. No full suite, PR, mutation qualification or interrupted-recovery success is claimed.
