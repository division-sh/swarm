# #2700 R04: Exact Existing-Owner Boundary

C1'' BLOCKING EVENT under reviewer-a6221203713 condition1, not a new product
question or a proposed coordinator. Production remains dd39a85e4a. The retired
key/handler cutover has not started. No push before B; #2592 priority remains.

## Executed Component Evidence

TestHTTPActivityReopenedStartedAttemptNeedsRecoveryOwnerBothStores uses the
existing native journal, actual compiled tool source and constructed flow,
real HTTP server, existing dispatcher and native close/reopen owners.
GOMAXPROCS2, -p1, count1, timeout180s through swarm-test; package1.115s.
Receipt: ~/.local/state/agent-a-2700/effect-retirement-reopened-started-owner-red.log.

Both stores reach physical POST exactly once with a started native attempt.
While that provider call is held, the duplicate returns without changing the
exact receipt, durable publication accounting or physical call count. This
control passes. Then cancel and JOIN the original caller and provider before
closing/reopening the native store. Canceled uncertain settlement leaves the
original started record; the cancellation cause is retained, including SQLite's
before-attempt diagnostic. Reopened dispatcher returns nil, leaves the record
byte-for-field identical to the live record, and publishes no result. Both
cells are RED on missing caller uncertainty, calls remain1.

This is an actual postlaunch canceled-settlement/reopen component probe, NOT
a SIGKILL or legitimate pipeline-recovery delivery proof. It does not close
R04. Its diagnostic oracle observes the returned error; final acceptance must
also allow the canonical typed uncertainty delivered through the actual result
publication owner. No final return-channel design is selected by this probe.
No allowance, retry-to-green, raw SQL, new hook or weakened control was used.

## Exact Missing Fact / Trace

ActivityAttemptRecord stores request/run/source/input/mode/loop identity,
class/attempt/status/result/timestamps/reply, but no execution-owner or launch
association to the predecessor process/pipeline claim. Claim and terminal CAS
in backend/activityjournal validate active run plus exact request identity;
MarkUncertain can transition a started row but does not prove a predecessor
dead. publishExistingActivityAttempt deliberately returns nil for started.

The bus DOES have its existing opaque pipelineobligation.Claim and issuer:
processClaimedPipelineWork -> RecoverPersistedPipeline receives ClaimedWork.
RecoverPersistedPipeline currently passes only event/scope/recipients into
publishClaimedPipeline, then interceptor ctx/event into the activity dispatcher.
The claim is not lent to that handler, and the original attempt has no stored
association to it. StartupRecoveryDiagnosticsEnabled is a diagnostic marker,
not death or execution authority; a fresh pipeline claim alone is not proof
that a previously launched provider request has ceased.

The generic effects authority is not a free replacement: its current closed
kinds/targets describe agent turns, selected execution and specific registration/
channel authorities, not an ordinary handler activity launch. OwnerPipelineActivity
marks a different owner; it does not issue a completion controller or a durable
attempt/process binding. Do not manufacture an agent-turn target for a handler.

## Bounded Engineering Question

Which EXISTING authoritative retirement/recovery fact may the activity owner
consume to distinguish the ended predecessor from the live duplicate, and
how is it linked to this exact native attempt? If the current pipeline/execution
owner has that proof, pin its bounded handoff and same-transaction fence before
I wire MarkActivityAttemptUncertain/result publication to it. If no such fact
exists, the missing fact is the exact attempt-to-predecessor execution/launch
ownership and its authoritative retired disposition; please disposition that
bounded extension before implementation. I have not designed a new mechanism.

Only the dependent R04 recovery enabling cut is paused. Shared key retirement,
timeout/O04, conservative fresh handler settlement and accepted presentation
remain within the approved scope, but no complete retirement GREEN or hosted
qualification will be claimed without R04. The separately approved #1745
engineering can continue while this existing-owner handoff is resolved.
