# Whole-family PostgreSQL supplied-pool admission plan

Local branch: agent-b/2542-native-postgres-admission-tail, stacked on approved
#2604 accb585faa3a2d179a4e5714743c80731353209b. No push, new PR or edits to the
approved landing ref. This is preparation, not a completed migration or proof.
Binding plan: #2542 comment 6104238422, under the existing broad #2151 gate.

## Scope And Measurement

The committed census at this source records 10698 findings / 7754 raw debt sites
and 67 unresolved excluded-source occurrences. No fresh census is claimed.
The largest named ordinary escape family is supplied-pool PostgreSQL admission:
202 calls to AdmitPostgresRuntimeStore, 198 enclosing declaration identities,
92 source files. The 219 StartPostgres entries also include infrastructure;
neither count represents that many independent bugs.

Source-pinned AST survey of all 92 baseline-owned files reconciles exactly 202
actual calls, including nested literals and import aliases, without scanning
foreign sources or selecting a different build universe. This is a shape survey,
not type proof or permission to rewrite a site:

| Shape | Calls | Required treatment |
| --- | ---: | --- |
| Pool identifier has only two lexical references | 23 | Candidate only: distinguish local sandbox declaration from parameter/captured authority, prove exact constructor and cleanup ordering, preserve whole workload |
| Coupled setup, readback, faults or cleanup | 177 | Repair exact shared domain owners first, then propagate mechanically; do not substitute a generic getter |
| Nonlocal/expression input | 2 | Inspect original location/selected identity and exact precondition; no inferred authority |

Largest file clusters: runforkexecution/execution_test.go 33; API publication13;
served run-fork8; notify-all-children8. The completed family144 run-setup
inventory remains 107 -> 0 and is not counted again.

## Existing Owners And First Dependency Chain

StartPostgresRuntimeStore / StartPostgresRuntimeStoreWithReopen already own
native location construction, production bootstrap, payload admission and the
single close ledger registered before consumers. Reopen retains independent
instances over the same original location; it does not recover a pool or create
a coordinator around the running object's database.

The first fork cluster contains repeated reconstruction in
admitSelectedExecutionSourceArtifact, seedSelectedExecutionStateOnlySourceRun
and seedSelectedExecutionSourceRunWithPrimaryRouteModeAndSource. Its callers
already hold selected instances. Source-artifact Ensure/Get, native RequireRun,
CommitSemanticEventWithRoutes, runfork revision/snapshot ownership and the
existing closed selected-fork storage projections are the intended consumers.
The mode-aware source helper still has raw entity mutation/state setup; that
setup must move with the construction chain, not be hidden by a wrapper.

Ordinary constructor-only callers can advance while those shared dependencies
are repaired. Constructor substitution must happen at the original opening
point; consumer cleanup cannot overtake the canonical store close ledger.
Explicit test-service locations, real independent peers, custom capacity and
live fault cuts require their own named existing-owner paths. Testing helpers
that merely accept a caller's pool are not permission to open a new database.

## Delivery And Proof

Retire the entire 202-call capability, then delete its definition at the last
live caller. Use the existing finite codemod/current-successor machinery;
preserve immutable historical Before/After/Mechanical records. No new generic
SQL/query/mutation/callback port, Database/DatabaseForTest substitution,
classifier relaxation, compatibility behavior or incidental assertion change.
SQLite supplied-pool/global-getter debt remains explicitly in the parent tail.

One whole-family checkpoint, not six small query cohorts. Per development run:
affected roots on vemew, applicable native both-store construction/reopen/join,
foreign constructor/original-coordinator controls and no-partial-write tests.
Carry unaffected source-bound receipts. Shared global census/downward ratchet,
full guards, complexity/unused and source/recipe reconciliation at the family
checkpoint; no mid-family duplicate tier/server2/full execution.

Stop only for genuinely new production ownership/semantic decisions. Routine
native fixture substitution and replacing invalid incidental setup are already
authorized. After #2604 lands, rebase this local branch onto its actual landing.
No removal credit, zero-debt closure or integrated qualification is claimed now.
