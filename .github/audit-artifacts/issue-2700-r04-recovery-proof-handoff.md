# R04 Recovery-Proof Handoff To E

Engineering/proof handoff, not a second implementation or a completion claim.
A source is signed c800fa2980701942450cc671b851a8b3997fc731. The existing
[joint gate](https://github.com/division-sh/swarm/issues/2700#issuecomment-6221925281)
and [scope disposition](https://github.com/division-sh/swarm/issues/2700#issuecomment-6225685356)
bind. No CI-bearing push until the orchestrator releases the slot.

## Exact Existing Interface

```go
startupownership.ReconcileDisplacedHTTPActivities(
    ctx context.Context, capability startupownership.ProcessCapability,
) (pipeline.ActivityRecoverySummary, error)

// The original native journal's existing transaction-bound entry:
ReconcileDisplacedHTTPActivitiesTx(
    context.Context, *mutationprotocol.Attempt, storestartup.DisplacedHTTPActivityCut,
) (pipeline.ActivityRecoverySummary, error)

(*pipeline.PipelineCoordinator).PublishRecoveredHTTPActivity(
    context.Context, pipeline.RecoveredHTTPActivity,
) error
```

ActivityRecoverySummary.Results contains RecoveredHTTPActivity, embedding the
exact ActivityAttemptRecord plus Request events.Event and BundleHash. These
are original native recovery results, not a launch grant or commit verdict.
The retained session owns its transaction, cut, commit acknowledgement and
dispatch/readiness fence. A's adapter never opens a competing transaction or
publishes. E exposes results only after an acknowledged commit, then invokes
the existing publisher before opening dispatch. The publisher rereads the
original journal and compares identity, result content and terminal times.

The source-scope repair is A85bc859141, already composed by E as a16990adca:
prove native run-source equals the displaced grant source before deriving the
per-row BundleScopeForSource from the ORIGINAL caller context. Cancellation,
same-session ownership and current/selected/source fences remain unchanged.
The retained-journal bridge and both native constructor bindings are already
[pinned](https://github.com/division-sh/swarm/issues/642#issuecomment-6226308728).
No new port, pool fallback, replacement session or reference-only permission.

## Proof Reaches Still Needed From E

Prefer named subcases in the existing native original-session/sibling and
startup/resume roots. Adds 0 roots in this A handoff; E records any genuinely
new invariant/reach and its root cost before adding a root. No mirrored suite.

| Reach | Required connected observation |
| --- | --- |
| Recovery commit reply lost AFTER real native writes | The actual recovery transaction commits but its acknowledgement is lost. No candidate summary escapes, no result publishes and dispatch/readiness remain closed. Preserve durable row/story/obligation evidence, not just a component callback error. A legitimate subsequent recovery reuses the one terminal outcome without another HTTP call or duplicate settlement. |
| Recovery rollback after a real first-row change | Retain the existing sibling partial-write refusal if it already supplies this reach: one row mutates before the next refuses, then all recovery rows, story sequence and obligations roll back atomically. The failed step exposes no summary and does not release dispatch. Do not replace this with a before-write refusal. |
| Real resume/startup follow-through | Drive E's original retained-process fence/handoff on the actual supported resume path, across acknowledged recovery and result publication into dispatch/readiness. One physical predecessor HTTP call, one durable terminal outcome and one exact result; no synthetic recovery owner or direct invocation substituted for the startup path. |
| Still-live predecessor and scope conservation | The original predecessor's late writer refuses after takeover, while the proper successor completes through its own retained role. Keep current grant, selected child and foreign source/run exclusions; exercise two sibling bundles and the original caller's cancellation. |
| Known terminal and already-published history | Preserve exact succeeded/failed/uncertain content, reply context, original request/parent/source, loop generation and all terminal timestamps. Unpublished terminal history publishes only after ACK; already-published history does not publish again. Copied or NULL execution_binding never permits a new HTTP call or recovery. |

The [E receipt](https://github.com/division-sh/swarm/issues/2700#issuecomment-6225880280)
already credits native original-session/two-source and publication controls.
Carry unchanged reaches rather than rerun them for this handoff alone. Its
recovery-transaction ACK-loss and full-resume rows remain OPEN until E pins
their exact connected receipts. A's ordinary writer ACK-loss matrix, reopened
readback, API approval journey and compiled smoke do not discharge those rows.

For each remaining reach, return the signed composed head, exact existing root
and subcase, command, store, result and log path/hash; retain every original RED.
Use original bounds and race x3 only for changed concurrency. A consumes the
receipts in the ONE widened-deletion RED-to-GREEN checkpoint; E retains custody
of startup/session/handoff work. This document asks for no new production port.
