# Cohort 133: native runtime-log persistence and recovery consumers

Binding decisions: #2542 comments 6070596271 and 6071068973. This is the
authorized shared fixture/spec repair, not a new runtime logging policy.
platform-spec.yaml diagnostic_direct require_present/runless admission governs;
the stale runtime_diagnostics_log_rows attribution is corrected to distinguish
run creation/source availability, existing-run logging and startup integrity.

## Owners And Exhaustive Family Consumption

RuntimeLogger still owns canonical payload construction/admission and recorder
acknowledgment. The exact selected EventPostgresOwner/EventSQLiteOwner owns
PersistRuntimeLog, canonical lineage, named atomic commit, counters and complete
event equality. Run creation and source persistence use the existing lifecycle
and artifact owners; snapshot observation invokes the selected lifecycle owner's
existing LoadSnapshotTx inside its original read coordinator, never a replacement.

All seven persisted logger callers and all seven RuntimeDeps.RuntimeLogStore
recovery callers now receive native typed persistence. The three direct captures
and ten capture-helper consumers from cohort 131 remain intentionally pure units.
Their injected owner errors are NOT native transaction durability evidence.
The mixed raw runtimeLogPersistenceStub and its lineage, transaction story,
implicit scenario-run creation and manual counter synchronization are removed
after the last consumer. Seed preparation admits the same immutable authored
bytes; actual source persistence and positive run setup happen through owners.

The latest-log and exact startup-decision predicates are fixed private reads;
complete records decode through eventrecord, not a manually reconstructed event.
The original decision component/action, latest created_at ordering, source/run
predicates and canonical payload/detail assertions remain. There is no public
SQL, table/predicate selector, raw handle or transaction callback.

The recovery fan-out fixture receives explicit typed backend capacity, not
db.Stats or a raw DB. Its existing zero-work serving role remains synthetic and
refuses unowned fan-out work. Recovery source options explicitly carry the same
admitted module source. The readiness-failure consumer now supplies that existing
serving role so the original workflow-version failure is actually reachable.

## Manifestation Proof

- Seven original persisted roots execute on SQLite and PostgreSQL under race,
  retaining payload, canonical run ownership, source/origin/status preservation,
  spoofed detail exclusion, missing-subject and typed/same-run lineage assertions.
- The obsolete deleted-artifact logging expectation becomes a named setup witness
  using actual CreateRun: ErrSourceArtifactUnavailable and unchanged physical
  run/event/delivery/entity/receipt snapshot. Logging is not given creation rights.
- Native artifact-loss controls prove missing-run ErrRunNotFound and zero storage
  and recorder acknowledgment; existing running/terminal and runless logging still
  succeeds after the exact source row is deleted. New creation still fails closed.
- Exact supplied event ID, subsecond UTC clock, bytes, run, diagnostic admission
  and live/mock mode round-trip on both stores. Exact replay is unchanged;
  conflicting payload with the same ID returns ErrEventIdentityConflict with no
  mutation. Terminal end/status/source/origin and counters are preserved.
- Native cancellation refuses mutation and recorder acknowledgment. A fixed
  original-coordinator AFTER INSERT fault proves actual post-append transaction
  rollback, complete unchanged observed storage/counters, no recorder entry,
  cleanup restoration and subsequent real success. It does not delegate writes.
- Cross-run subject lookup returns no parent. An explicit cross-run diagnostic
  parent is rejected by the actual causal-source gate with zero storage/recorder
  mutation. Original same-run lineage roots remain positive controls.
- Seven recovery roots retain actual Start/Shutdown, denial, allowed/skipped,
  schedule/workflow summaries, degraded recovery and boot-fatal assertions, with
  native diagnostic persistence/readback on both stores.

These are native diagnostic witnesses, not fully native startup/restart journeys:
the retained synthetic event/manager/delivery collaborators still provide the
original controlled recovery inventory and failures. Their no-op boot publication
does not persist its generated run, so its run-bearing publication diagnostic
correctly logs ErrRunNotFound; the runless startup-decision record persists. That
unrelated synthetic publication is not repaired by reintroducing logging-driven
creation and is not credited as native boot, publication or manager replay proof.

## Guard, Codemod And Residual Disposition

Existing finite recipes are extended for the seven migrated logger callers and
the seven earlier recovery consumers, not duplicated. Five additional
read/precondition/capacity recipes retain the exact output (562 total).
Oracles compare protected whole logging/assertion blocks and
entire recovery bodies except the named owner/source/serving preconditions;
hostile scope/decision mutations must fail. A retirement control closes the
mixed stub and fake transaction helpers. The native authority guard closes all
five fixture/caller files, including future siblings, with raw-parameter and
callback negative controls.

The broader selected-authority parent remains open. Other runtime constructors,
synthetic recovery collaborators and unrelated raw persistence tests are not
claimed migrated. Existing #2151/#2542 tracking/watchlist is sufficient; no new
issue/framework/compatibility, timing change, retry, skip or vendored dependency.
Fresh census, guard, complete codemod/type overlays, unused and complexity receipts
are required before this family is submitted for incremental stacking review.
