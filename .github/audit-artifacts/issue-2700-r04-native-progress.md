# #2700 R04 Native Reference / Identity Staging

Intermediate implementation under6221888821/6221925281, not connected R04
RED-to-GREEN, FINAL HEAD or a qualification request. No push before B #2698.

## Implemented Boundary

- One nullable JSON execution_binding column in the existing activity table,
  consumed through generated native DDL on both stores. No new table/migration.
- ActivityAttemptRecord carries the exact version/grant/state reference. Native
  Start/Claim persist it without duplicate rebinding; the strict codec rejects
  extra/alias/missing fields, duplicate keys, JSON null, partial identities and
  invalid/overflowing versions. SQL NULL remains legacy/non-executing evidence.
- Execution identity is distinct from historical result reuse. Native terminal
  writers load and match the original row BEFORE status/result/story writes;
  PostgreSQL holds that activity row through commit, after the existing global
  mutation/story order. Source, parent, reply, attempt, input, mode, loop and
  exact reference cannot be replaced by a callback. Zero attempts are refused
  before normalization/SQL. Historical reuse does not turn an old reference
  into new execution permission.
- The terminal fork-copy writer explicitly inserts SQL NULL and checks it on
  duplicate readback. It copies result/time/lineage evidence, not a child grant.
  B's separate activityjournal/fork_history_fixture.go is still absent here;
  that writer is reserved for actual B integration, not silently omitted.
- The existing PostgreSQL held-domain-lock control observes the earlier
  SELECT-for-validation lock rather than the later UPDATE. It still proves
  global-before-domain order, native receipt/story rollback and sequence CAS;
  its business bounds and accounting are unchanged.

## Retained Receipts

All logs below are in ~/.local/state/agent-a-2700/, normal host load,
GOMAXPROCS2/-p1, unchanged package/business bounds.

| Receipt | Actual scope |
| --- | --- |
| r04-execution-identity-original-red.log | RED0.008s: existing comparison accepted changed source, parent, reply, attempt and missing/different grant references. |
| r04-native-terminal-identity-original-red.log | RED0.011s, both SQLmock dialects: terminal writers issue UPDATE before reading/matching the original row. Not a native-store authority proof. |
| r04-binding-native-identity-race3-candidate1.log | PASS pipeline1.046s / activityjournal1.194s / schemastore10.400s. Identity/codec/query-order components, schema guard, actual SQLite/PostgreSQL generated DDL NULL/reference round trips x3. No grant permission or takeover credit. |
| r04-native-identity-story-controls-race3-candidate2.log | Overall RED runtimepersistence88.542s: pre-write lookup changed the original missing-row diagnostic. The original story-disposition assertion caught it on both stores/modes. Existing lost-ACK, early global fence/no-op/rollback, terminal timestamp and story-failure atomicity roots independently PASS x3. Pipeline1.046s / activityjournal1.180s component checks PASS. |
| r04-native-identity-disposition-race3-candidate3.log | PASS activityjournal1.078s / runtimepersistence102.887s. Duplicate-reference24 component executions and exact-attempt36 checks; actual both-store changed-callback84 cells with unchanged original receipt/story/head. Original complete story-disposition and no-draft validation matrices PASS x3, at their original bounds. |
| r04-existing-schema-origin-race3-candidate1.log | PASS schemastore19.145s, original TestForkPointSchemaClosedArmsBothStores x3, including the NULL/exact-reference cells. Reuses that existing schema/FK test's source run; no new semantic run SQL, production test hook or guard exception. All original fork-point CHECK/FK negatives preserved. |
| r04-native-boundary-guards-candidate1.log | Compile-only RED: I moved the new standalone schema root into the existing schema-origin test during precompilation, leaving generated testmain with the old name. No tests ran; not a runtime or infrastructure classification. Raw incomplete receipt retained. |
| r04-native-boundary-guards-candidate2.log | Stable-source PASS runtimepersistence4.034s / schemastore0.236s: all5 selected event, activity-readback, lifecycle, semantic fixture and schema-interpreter boundaries. No guard edits or exemptions. |
| r04-original-grant-fence-component-race3-candidate1.log | Component RED0.037s: the positive first-fence probe used a plain SQLmock transaction without the mandatory native transaction-ordering frame. Capture/isolation/cancellation controls PASS; no currentness or native takeover credit. |
| r04-original-grant-fence-component-race3-candidate2.log | PASS activityjournal1.041s, all3 roots x3. The corrected first-fence probe uses the actual PostgreSQL/SQLite backend transaction frames over SQLmock, preserving the exact fence error/rollback oracle. Immutable normal/selected capture, no original reference replacement, pre-SQL refusal and original caller cancellation controls pass. Still not the connected four-writer/recovery proof. |

The candidate2 diagnostic was repaired in production, restoring the original
"was not found for completion/uncertain transition" message. No assertion,
timeout, accounting exception or baseline was relaxed.

## Still Required Before Enabling / Landing

Reference/identity staging is not the live execution fence. The original
installed normal/selected role must be captured, the BOTH-key historical
snapshot admitted by its canonical owner, and process/current grant/selected
fences held from BEFORE domain locks through commit in every original writer.
Fresh live NULL is not an allowed fallback; the existing unbound writer
behavior is an open implementation dependency, not accepted behavior or a
compatibility path.

E owns the opaque fixed-session cut and shared startup assembly. Exact port is
in issue-2700-r04-activity-port.md /6222302270; canonical-reader/shared-assembly
handoff is6222512961. E pinned the canonical leaf in6222592959:
ReadActivityExecutionGrantTx(context.Context, *sql.Tx, string, uint64)
(runtimestartupownership.GrantEvidence, error), in its reserved
store/internal/startupownership/activity_execution_grant.go. It reads exact
historical keys and projections, never current permission. Consume E's signed
implementation when supplied; do not edit its reserved file.

E supplied signed c6592d6724cb48ac303002f5b9cadfc883fc3f8e in6222739539.
Only its canonical production leaf is composed byte-for-byte here; E retains
authorship. The donor's history-proof source calls
openCompletionOutcomeFixture(t, backend, connectorWrapper), while this pre-B
tree has the two-argument constructor. The full donor proof must compose with
actual B, not by widening that shared fixture early or changing its assertions.
E's independent both-store race x3 receipt38.926s is donor evidence, not A's
connected fence/recovery qualification. No other E provider-journal or startup
ordering commit was taken.

A's next local layer is execution_grant.go: an immutable captured admitted
role, exact canonical history/reference comparison, and a native proof that
takes the original generation mutation fence BEFORE domain locks. The named
activity entry on agentpersistence delegates to its existing canonical admitted
generation/source-set/run/selected proof, then the current full-snapshot check
also verifies the original state version. No activity-side grant decoder,
source-head query or new permission interpreter was added. The layer is not yet
installed in the four native mutators/runtime dispatch and does NOT enable
recovery; its component tests are not connected native takeover qualification.

The original-role assembly handoff is6222905546. The proposed native session
port supplies ActivityJournalGrantBinder.BindActivityGrant(GrantEvidence)
(pipeline.ActivityAttemptJournal, error) from the same retained session.
Capture must be through the actual installed sealed generation; an in-flight
dispatcher retains that exact bound journal through completion. Shared startup/
source-refresh/selected-runtime installation hunks are reserved before editing,
not silently modified or replaced by mutable-global role lookup.

No recovery adapter or sweep has been enabled. Live
duplicate versus takeover in both commit orders, late still-alive predecessor,
normal/selected/foreign isolation, missing historical grant, native rollback/
lost ACK, copied-terminal reuse and one-call/one-uncertain-result remain open.
The original reopened-started RED is unchanged and is NOT passed-credit here.

Affected guards/debt/unused/complexity and actual-head preflight remain owed
before the eventual permitted push. No hosted or local whole-tier credit.
