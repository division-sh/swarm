# Same-Process Readiness Attempt Re-Stamp

Issue #2496 / PR #2525. Production/spec source45489b53b, focused proof391e3678a,
test-input repair67742d455, pinned basee63f4bdb1.
Binding user-ratified LEAD ruling:5971568624. This supersedes the held design
status in the historical terminal-drain checkpoint; its terminal proofs remain
source-attributed history. No push, final review or failure-class closure claim.

## Contract And Consumption

Working failure class remains flow_construction_and_attachment_authority_drift.
Its parent #2411 remains open, broader decomposition #2250 remains separate.
No new failure class, continuation framework, compatibility reader, grant ledger,
identity derivation, routing permission or replay support is introduced.

| Owner / consumer | Current disposition |
| --- | --- |
| `processbinding.Binding.SameProcessExecution` | Exact authority/owner/boot/source/runtime coordinate equality, excluding generation stamp. B's current-grant writer authorization remains separate. |
| Manager `PreparedDurableTopologySourceSetRebind` | Builds complete per-instance actor groups and includes agentless active attempts. Serializes against readiness passes and joined retirement; rejects unresolved admission/retirement. Current writer selection precedes commits, but all process actor/attempt projections publish only after all durable commits acknowledge. Partial cross-instance progress remains fenced and retryable. |
| `generationGrant.RebindFlowReadinessSourceSet` | Named ordinary admitted-grant port; current possession, complete-source-set and local fencing remain mandatory. Caller cannot supply a writer binding. Selected-fork grants refuse this ordinary source-set operation. |
| Retained-session composition | One existing mutation-protocol transaction per instance: exact readiness stamp, exhaustive actor census, all existing lifecycle mutations, diagnostics/history. PostgreSQL retained session and native SQLite transaction use the same semantic composition. Any actor failure rolls the entire instance back. |
| Pipeline `RebindFlowActivationAttemptTx` | Same process/unchanged instance+plan, exact accepted attempt and current admitted write grant. Adjacent retained predecessor or exact repeated successor only. Any forward phase; duplicate stamp makes no row/timestamp mutation. No resources or construction replay. |
| Agent `VerifyFlowReadinessRebindCensusTx` | Strictly decoded canonical topology; exact run/instance/attempt/hash and full persisted topology. No omitted/extra/duplicate actor or token/phase replacement. Registered/stopped preparation actors stay non-executable. |
| `authorizeFlowReadinessMutation` | Current grant still authorizes durable writes. Readiness admission checks exact current accepted attempt and same-process evidence, not grant-stamp equality. Prepared re-stamp exists only inside the named batch; ordinary preparation/admission and foreign takeover guards remain distinct. |
| Individual `CommitAgentLifecycleTransition` | Production flow-readiness source-set rebind refuses; no per-actor bypass of the named instance transaction. Static rebind remains a different topology concept. |
| Phase verification and creation commit | Exact attempt-ID/disposition plus current writer, plan/source/run/lifecycle remain mandatory. Grant stamp is evidence, not an additional attachment fence. Creation completion removes redundant ordinal/ID predicates and retains atomic event+marker commit. |
| Retirement and abandonment | Exact attempt cleanup remains possible using retained original binding evidence after re-stamp. No successor mutation, foreign-process adoption or forward work under a retired grant. Existing settlement dispositions unchanged. |
| Begin and lost-ack resolution | Exact request ID plus expected admitted attempt ordinal; original request survives a stamp change. Unrelated request and stale callback cannot adopt the successor. |
| Manager loop self-release | Exact unchanged lifecycle token AND persisted topology/attempt under the current writer permit settlement after partial refresh. No cross-process/config/attempt replacement, retired-grant write or new execution admission. |
| Aggregate/Runtime/serve shutdown | Prior bounded terminal-drain owner is retained. Whole-set fence precedes drain wake; individual pending-gate shutdown refuses. Settlement errors and owned joins precede dependency release. |
| Startup/takeover, plan replacement, aborted/retired/superseded attempts | New planned attempt after the applicable exact settlement. Never use the same-process survivor re-stamp. Construction, creating delivery, A's stage evidence and B's generation authority are unchanged. |

The additional creation-completion grant predicate was found by the complete
fence census and removed under the ratified "every fence" boundary. It was not
left as a surviving same-concept interpreter. The port is a semantic operation,
not a generic transaction or query adapter.

## Proof Obligations

The committed391e3678a receipts are in issue-2496-restamp-391-qualified.json:
57 port/Manager/serve root executions under race3, all60 required cells across
five native runtime roots under race3,45 compiled-describe cells, public standing/
progressive-presence and sequential-run controls, both1362-item fan-out journeys,
and all42 PostgreSQL attachment protocol root executions under race3. All pass
without skips. Earlier working-tree17-root results remain historical only.
Both-store native matrix uses real constructor, Runtime/Manager,
two keyed parents, eight descendant instances and16 actual actors for failure
journeys; it is not provider/public-launcher proof.

| Manifestation | Coverage disposition | Exact permanent proof on391e3678a |
| --- | --- | --- |
| Same-process survivor rejected by generation stamp | reproduced and fixed | `TestRuntimeConstructedActorCensusSourceSetRebindBothStores`;12 backend/field cells race3,212.926s, unchanged construction/business fields and exact generation rebind. |
| Partial attachment phase lost on refresh | reproduced and fixed | `TestFlowActivationSourceSetRebindBothStores`; both stores/all five phases race3, same attempt/phase/plan, duplicate raw-row/timestamp equality, wrong-plan/ordinal refusal. |
| Prepared stopped actor refused in accepted planned phase | reproduced and fixed | `TestAgentLifecyclePreparedReadinessRebindBothStores`; both stores race3, no executable actor or inherited progress. |
| Missing/individual actor transaction bypass | reproduced and fixed | Both lifecycle rebind roots reject the standalone production API and empty census under race3. Strict persisted topology is checked in the owned transaction. |
| Per-actor native failure commits a partial instance | reproduced and fixed | `TestRuntimeConstructedActorAtomicRebindRetryBothStores`;12 cells race3,254.059s; both actual actors failed in turn, exact stamp/actor rollback, retry preserves tokens/attempts. |
| Partial cross-instance refresh loses cleanup writer | reproduced and fixed | `TestRuntimeConstructedActorPartialRefreshShutdownBothStores`;12 cells race3,250.040s; failure after earlier commits, exact joined shutdown, higher restart ordinal without construction replay. |
| Original admission request loses cleanup/resolution after stamp | reproduced and fixed | Phase matrix race3 resolves original request after predecessor retirement, unrelated request refuses, original exact cleanup succeeds while retired-grant forward work refuses. |
| Physical admission/rebind acknowledgment loss and late callback | reproduced and fixed | `TestFlowAttachmentNativeLostAckAfterRebindBothStores`; both stores race3, real lost COMMIT acknowledgment. SQLite exact retry/resolution refuses old callbacks after successor admission; ambiguous retained PostgreSQL owner force-fences, while durable request readback/joined settlement remain. |
| Creation occurrence repeated or lost after ready re-stamp | reproduced and fixed | `TestDynamicFlowCreationSourceSetRebindBothStores`; pending/emitted variants on both stores race3, marker unchanged and exactly one occurrence. No claim that a second public publication preempts an asynchronously owned foreground publication claim. |
| Terminal refresh wake incorrectly restores execution | reproduced and fixed | Manager/serve/aggregate controls race3 and `TestRuntimeConstructedActorRetainedRefreshShutdownBothStores`,12 cells race3,130.543s; restart12 cells race3,128.084s. |

All backend leaves are mandatory in the proof plan and partition test. Native
failure receipts are retained; no sleeps, timeout inflation, swallowed failures,
test exclusions, production retry-policy edits or capacity bypass.

## Qualification And Architecture

Focused committed proofs above pass; the complete managed default was RED
on391e3678a in two selected-fork fixture roots. Native parent evidence exposed
stale route-only input and random child entities; the collision fixture also
omitted its business-variable declarations and named an undeclared sibling.
Test-only67742d455 uses canonical keyless identities, typed parent configuration
and the existing structured fixture overlay. All original ownership, mode,
collision and numeric-kind assertions remain; three roots pass race3 in40.747s
on the identical pre-commit Go files. No production validation is weakened.

The combined SQLite attachment race3 exceeded the unchanged600-second package
limit during repetition three; two complete repetitions pass, but this is RED,
not coverage credit. The complete14-root family passes on67742d455 in bounded
2+12 commands: six cleanup/phase root executions in413.516s and36 remaining
root executions in265.484s. Every original cell, race3 and case deadline is
unchanged; managed capacity is retained. Six served receiver/selected/restart/
failure roots pass on both stores at67742d455 in112.834s. Canonical artifact
setup is private in the selected tests; this is public execution/readback, not
publication-to-fork or paid-provider qualification. The current default's broad
complete14-unit managed default passes:6,487 root and35,290 cell PASS, zero
FAIL, five optional SKIP, no required-execution omission. Its test-event span
is38m01.124s, including inter-unit capacity waits. The fresh45 describe cells
also pass unchanged inside this run. Exact receipts and required-cell checks:
issue-2496-restamp-677-qualified.json. Exact Go
diff from391e3678a consists only of the two named selected-fork test files.
No source rebasing during qualification.
Conflict-only rebase, affected qualification and exact-head CI remain required.
All REDs and the earlier composed timeout are preserved in the history ledger.
No final closure claim.
Current complexity versus pinned base passes >=30 ratchets: cognitive573->572,
cyclomatic264->260. Disclose >=50 cognitive193->194 and cyclomatic53->55; this
is not uniform improvement. Readiness core remains under500 lines.

The attempt-versus-write-generation conflation is promoted and repaired here,
not split to a future issue. Longer decomposition remains tracked in #2250 and
#2411; estimated residual ownership decomposition is multiple bounded workstreams,
not a prerequisite or authority exception. The new invariant is atomic instance
re-stamping without minting installation progress or relaxing B's writer fence.
