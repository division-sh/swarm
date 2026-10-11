# Native Workflow / Scheduler Timer Isolation Cohort

Reviewed base43f3bed66, one self-contained #2542 family on the long-lived branch.
No PR, aggregate qualification or global closure claim.

The sole remaining raw root in workflow_instance_store_mutate_test.go is migrated
to existing selected activation, generic-schedule admission, opaque workflow
readback and the engine-mutation owner. The original prepared workflow, queued
state, exact routing source, system owner, empty payload, live execution mode,
task key and absolute now+2h due remain unchanged. Acknowledged created admission
must succeed before mutation. The selected probe requires three write commits,
exactly one engine mutation, and zero active transactions after cleanup.

backend/genericschedule owns the fixed physical count for entity_id,
flow_instance and workflow-runtime owner. It consumes the exact original selected
read coordinator through a private adapter. No run, status, eligibility or history
predicate was introduced. Public storetest returns only an int64/error at exact
canonical coordinates; raw SQL, generic query/table selectors, payloads, callbacks,
handles and reconstructed coordinators are absent.

The whole mutation test file and both new component fixture files are now covered
by the existing mandatory NativeMutationSeed child of the fresh census. Its hostile
controls reject both the new root and an unlisted sibling's raw parameters, plus
the existing SQL callback. All fifteen required children/tier memberships remain;
no new inventory or selective exemption was introduced.

| Manifestation | Disposition | Exact executed proof |
| --- | --- | --- |
| Scheduler-owned rows incorrectly entering workflow mutation/readback | reproduced and fixed | TestWorkflowInstanceStoreMutate_IgnoresSchedulerOwnedTimerRows, sqlite/postgres under race; actual workflow load and count==1 retained |
| Narrowed timer history/scope or reconstructed reader | execution-proven through the same corrected path | TestWorkflowSchedulerObservationKeepsPhysicalHistoryAndExactScopeBothStores under race: active, cancelled and other-run matches count3; wrong entity/path/owner excluded; independent original SQL equals scalar; one original read commit |
| Invalid/cancelled/closed/raw ownership supplying evidence | execution-proven through the same corrected path | Both previous root's coordinate/cancellation/close cases and TestWorkflowSchedulerObservationRefusesNonNativeOwners under race |
| Raw authority reintroduced into completed mutation family | execution-proven through the same corrected path | Fresh shared census NativeMutationSeed child; TestNativeMutationSeedGuardRejectsRawParametersAndCallbacks, including scheduler and unlisted-sibling controls |

Runtime root PASS9.081s under race; private roots PASS10.855s under race, no
selected skip/failure. Complete finite codemod suite PASS22.806s:94 snapshots,
idempotence, hostile controls and actual combined candidate-overlay type preflight.
Independent AST workload equality retains every original callback/assertion and
pins the original SQL/bind order for both dialects; weakening controls reject
changed due, callback state, found/cardinality and acknowledgment conditions.
All78 structural guards PASS, no waiver. Current-source codemod application inert.

Debt14763->14757 findings and10957->10952 raw-operation sites: six/five removed,
ZERO added identities or multiplicity growth, all67 excluded uncertainties and
collector494fd3b6 unchanged. Five exact new private facts (three generic-schedule,
two original runtime adapter) are individually classified, not new permissions.
The initial oracle compile error and unclassified private facts were refused and
repaired before green evidence; no retries, skips or timeout/budget changes.
Fresh read-only ratchet,12550-fact registry, shared15-child census and hostile
controls PASS45.667s. Partition/envelope and timing contract PASS; receipts remain
under increment4-scheduler-*. No new predicate weakens the census.

The authoritative selected-runtime raw-SQL policy adds the exact scheduler
isolation clause. This is fixture repair, no production semantic decision,
architecture framework, compatibility behavior or additional handoff port.
Existing parent/watchlist mapping is sufficient. The complete mutation-file cohort
is canonicalized; shared legacy schedule seeding remains explicitly owed under
#2542 for its other live callers. Global zero debt, strict guards, generic getter
deletion, SQLite fork deadline and final integrated full remain open obligations.
