# Selected discard cleanup fixtures

Track: FULL. Binding scope: reviewer-b #2698/6210011010 and placement/confinement
amendment #2698/6210588092. This is part of the single consolidated repair, not
a separate landing or proof of #2542/#642 closure.

**compiled fixture support added; production lifecycle behavior unchanged**.
The existing delivery lifecycle and general read-projection declarations are
unchanged. Fixed physical history operations live in the dedicated delivery
fixture file, with actor and fork predicates in their existing owner packages.
No new provider operation, grant, profile, live claim or caller SQL is exposed.

The same selected-coordinator transaction checks the admitted store, paused
materialized fork, exact source/frontier binding, original full actor identity,
absent target history and zero executions across the entire fork before writes.
Both events are admitted and checked absent before either is inserted. The
original claimed versus settled delivery/attempt/rule-selection footprint is
installed through fixed operations; only observation IDs are returned.

The unchanged discard runs only after a positive two-delivery/two-attempt/
one-settlement/one-selection witness and another transactional zero-execution
check. Original deletion assertions remain. Source-event conservation, complete
storage refusal snapshots and second-event-write rollback are explicit.

The retained siblings instead use the existing compiled provider fixture and
genuinely issued selected execution/generation authority. The generic fixture no
longer adds an unrelated raw actor when a compiled source is supplied. The real
selected actor preserves the original one-agent retained oracle. Delivery claims
use that selected authority; one stays claimed and the other settles. Quiescence
and close retain execution evidence before the unchanged discard/rollback cuts.

## Five-Root Disposition

| Root | Disposition and current proof |
| --- | --- |
| TestSelectedForkDiscardDeletesClaimedAndSettledDeliveryHistoryPostgres | Authorized unretained physical history only. Both stores, exact foreign/binding/frontier/occupied/canceled refusals, second-write rollback, positive pre-state and all original deletion assertions pass under race in pr2698-retained-and-physical-history-native-race-compiled.log. That aggregate remains RED on the earlier retained-constructor attempt. |
| TestSelectedForkDiscardSelectedStoreParity | Genuine selected authority. Both stores, retained-execution physical-fixture refusal without mutation, cancellation no-mutation, positive claimed/settled history, exact retained run/agent/execution/binding counts PASS under race in pr2698-retained-history-existing-native-provider-race.log. |
| TestSelectedForkDiscardRollbackSelectedStoreParity | Genuine selected authority. Both stores, retained-execution fixture refusal and original real-discard fault/whole-oracle rollback PASS under race in the same receipt. |
| TestSelectedContractActivationAllowsFreshForkConversationRows | Native selected construction through the original materialization process, compiled source and durable binding. Existing source session/audit/turn clocks and final activation oracle remain. Actual selected claim and completion owners persist the turn. Six missing/foreign admission, execution, fork, generation, source and target counterexamples refuse. Complete original PostgreSQL root PASS under race in pr2698-fresh-native-selected-refusal-controls-race.log. |
| TestSelectedContractActivationAllowsCausalForkLocalRuntimePlatformControlEvent | Same native selected actor/grant construction and selected claim/settlement, original causal platform event and final activation oracle. Complete original PostgreSQL root PASS under race in the same receipt (aggregate 9.208s). No physical-history exception applies. |

The two fresh-fork cases retain their original PostgreSQL surface. Their native
fixture consumes the process already held by the materialization owner; an
initial incorrect second-acquisition attempt is retained as a 240s RED receipt
(pr2698-native-fresh-selected-and-confinement-race.log), not classified as infra.
After construction, selected execution is quiesced/closed without deleting its
receipt. The temporary running/paused history shim is removed entirely. The
shared turn fixture consumes an explicit selected authority only when managed
admission/source/complete target match; ordinary callers retain their existing
path. No provider invocation or supported end-to-end execution is claimed for
this store-component fixture proof.

## Confinement And Accounting

The exact declaration/caller guard scans every owned Go source, including test,
excluded and package-initializer contexts. Its finite direct-call graph admits
only the named fixture chain and three cleanup proof roots. Unlisted production
or fixture callers, wrong declaration paths/receivers, initializers, function
values and method-value capture are rejected. Hostile controls exercise every
declared edge. The delivery SQL guard recognizes only those exact declarations;
there is no owner-file or test-file exemption. Event admission has one exact
named operation/call count, not a general fixture allowance.

The 42 resolved writer/reader/transaction findings belong to nine exact named
fixture declarations and retain fixture-2151 disposition. This is finite fixture
accounting authorized by 6210011010, not a1 retained-debt growth or deletion
credit. The 4,704-key retained set and collector rules are not expanded.
Registry and effective-method guard PASS, pr2698-fixed-history-confinement-registry.log
(11.734s): 19,806 exact findings, 4,704 retained, unchanged digest
14be09b12bd09bd915e54d6f8ce37feaaac00a3f4c1b24788774689927863f9e.
Final registry/ratchet qualification remains part of the consolidated freeze.
The read-only final family registry/effective-method check also PASSes (10.771s),
pr2698-selected-history-final-registry-readonly.log, with the same 19,806 exact
findings and unchanged 4,704-key digest. This is not the final landing preflight.

The physical writer census adds one exact declaration/two-table fixture row,
and mechanically remaps ten existing identities in five rows to their already
approved canonical owner files. Tables, symbols, predicates, transaction/cut,
finalizer and proof fields are unchanged by those five remaps. They repair the
old donor-placement census mismatch, not a new SQL allowance. CLI raw boundary
inventory PASS (0.798s), pr2698-fixed-history-cli-boundary-guard.log.
The complete production writer census PASSes (0.415s),
pr2698-selected-history-final-writer-census.log, after those exact path remaps.

Confinement, hostile controls, executable-delivery SQL ownership and admitted
event persistence boundary: PASS, pr2698-fixed-history-caller-confinement-controls.log
(6.825s). Earlier misplaced-file guard RED is retained. No guard assertion,
schema constraint, timeout, CASCADE behavior or reset retention policy changed.
The later combined race receipt retains its old writer-inventory RED while all
physical/retained/confinement roots pass; the dedicated writer receipt above
closes that exact mismatch. Final affected-package vet PASSes with no output.
