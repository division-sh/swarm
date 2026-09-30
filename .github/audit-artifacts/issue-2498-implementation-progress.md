# #2498 Implementation Progress

Agent-g, 2026-09-30. Binding standing gate:
https://github.com/division-sh/swarm/issues/2498#issuecomment-5906100464.
This is an intermediate checkpoint, not a Post-Implementation Proof Audit,
PR readiness, complete S/P/M/RB/U qualification, or parent closure.

## Existing-Owner Implementation

- Selected-store standing operations lock the parent before the exact run,
  prove the canonical disposition before no-op/write, and compare the captured
  predecessor. Corrupt relations propagate errors rather than committing no-ops.
- The existing reconciliation result carries acknowledged applied/exact-no-op
  evidence; errors after commit do not erase identity or authorize rollback.
- RuntimeContextManager validates exact loaded source/target/child versus proved
  non-executable absence. Active resume no-op leaves the original child and
  scheduler untouched. Child-only missing-occurrence rejection remains intact.
- Serve retains pipeline exclusion and actual gateway/child/scheduler state
  through durable mutation and resource disposition. Rollback checks current
  authority and preserves prior suppression; acknowledged failure retires the
  captured predecessor without reopening it. Failed scheduler parking returns
  its retained transition to the composing owner rather than restoring blindly.
- The authoritative spec and generated OpenRPC describe the finite matrix.
  The standing classifier moves mechanically to runlifecycle, without an alias
  or second classifier. Its production guard requires exactly one definition.

## Executed Candidate Receipts

The selected-store and race commands use host PostgreSQL; all persistent
families below include SQLite and PostgreSQL. Public process tests use compiled
CLI/authenticated RPC and the existing retained internal mock lifecycle child
(H), not a live-provider claim.

| Proof | Executed result | Credit and limits |
|---|---|---|
| `TestStandingFreshNoopRejectsCorruptRelationBothStores` | PASS; race x3 | Both suspend and resume no-op relation errors fail closed; S10 supplement |
| `TestStandingOperatorAcknowledgesFreshNoopBothStores` | PASS; race x3 | Exact N, applied/no-op result and journal cardinality |
| `TestStandingOperatorRefusesStaleAuthorityBothStores` | PASS; race x3 | Every command refuses a changed generation without journal mutation; not every S17 race |
| `TestStandingOperatorActiveNoopPreservesExactChildAndSchedule`, `TestStandingOperatorNoChildRestoresPriorSuppression`, `TestStandingOperatorRejectsInvalidChildComposition` | PASS; race x3 | Exact original child/scheduler, typed absent states, missing/stale/foreign/fenced/suppressed refusal |
| `TestStandingOperatorCancellationJoinsHeldChildBeforeRestore` | PASS; race x3 | Cancelled restore cannot reopen before the actual held child joins |
| `TestStandingServiceMutationsUseSelectedRuntimePipelineOnBothStores` | PASS 4.749s; expanded cancellation/failure matrix race x3 PASS 35.510s | Selected-context isolation, active/no-child rollback, held-work cancellation, postcommit cleanup and publication failure, fail-closed retry |
| `TestStandingPauseAuthorityPublicBothStores` + `TestStandingResetNonExecutablePublicBothStores` | PASS together 112.741s through swarm-test | Fresh CLI/RPC no-ops, actual SQL rollback injection, suspended resets/restart/resume, exact historical replay, terminal current/revised/retained-override and validated-invalid current/revised reset; S02-S08/S14 components |
| Canonical standing owner/deletion guards | PASS 0.223s | One production classifier, retired interpreters absent, inbound consumes canonical reader |
| API specification and OpenRPC generation check | PASS | Contract coherence only, not runtime closure |
| `TestStandingResetNonExecutablePublicBothStores/{sqlite,postgres}/{orphan,terminal-orphan,invalid-orphan,broken-relation}` | PASS 74.401s | All three CLI/RPC commands refuse orphan and unknown services; exact cold declaration restoration then reset succeeds; broken relation refuses startup before readiness |
| `TestServedParityHarnessStandingServiceLifecycle` | PASS 14.909s, both stores | Real held connector route and scheduler drain, joined suspension/reset and lawful fresh-child resume; S01/S15 components |
| `TestGoldenNumericDataScatterParkRestartBothStores` | PASS 90.084s through swarm-test | Exact 32/100 unsettled public checkpoint, paused restart and continue, 100 exact numeric row/event/materializing-recipient/entity chains, 100 typed initial timer activations preserved after restart/retry, permanent creation receipt unchanged after process/cache loss; M01-M12 components |
| `TestGoldenNumericDataScatterParkRefusalBothStores` | PASS 21.574s | Late malformed numeric, required null and duplicate key each produce exact public row-100 defect and empty run binding; public versions/head history/run inventory unchanged and run/feed absent; M13 |
| `TestPersistedAgentReadinessFieldsBothStores` | PASS 1.727s | Exact nonzero budget 1.25 and flow-data capability, canonical revision unchanged across independent hydration reads; M19 supplementary store credit only |
| `TestGoldenAgentWorkloadRestartAndForcedKillOnBothBackends` | PASS 27.776s through swarm-test | Real field-bearing candidate activation/turn, graceful restart and forced recovery on both stores; M18 H credit, not public/live-provider or nonzero-budget credit |
| Catalog, test-planning and API-spec packages | PASS 1.318s / 17.469s / 1.917s | Disjoint numeric structural/runtime claims, existing sole executor and both-store CI children |

The earlier four-product public control passed 67.898s; the later five-product
receipt above supersedes it. The reconciled default qualification attempt failed
and earns no full-suite credit. Its captured evidence isolated incomplete SQLite
test schemas, a missing numeric routing-proof registration, and the candidate's
ordinary-pause guard incorrectly applied to selected-fork claims. The selected
fork branch now delegates to its pre-existing selected-state/live-lease fence;
ordinary claims/continuations alone consume normal dispatch-parked admission.
Canonical fixture schemas and proof registration were corrected without a
production fallback. Manager and fork packages then passed (20.812s / 154.310s).
Pipeline's sole remaining failure was an obsolete test-local standing table;
its canonical-fixture correction passed the exact timer test three times (1.425s),
but the complete pipeline package still needs the final-head rerun.

Rebase onto `origin/master@abdb07bfe` preserves the earlier approved commits and
newer continuation/lifetime contracts. The measured committed complexity ratchet
at `e442870f7` remains cognitive 594/594 and cyclomatic 282/282, with no hotspot
growth; the later test additions still require their own committed measurement.

## Remaining Mandatory Qualification

### 2026-09-30 Final-Head Work In Progress

The later default attempt at `cfb6b3b5f` passed the complete pipeline package
(190.499s) and run-fork execution package (189.347s), then stopped in
`local-runtime-bus-full`: the exact handoff test fixture omitted canonical
`run_control_state`. Canonical admission DDL was added to that fixture, and the
three affected cases pass three repetitions (0.375s). No permissive production
schema detection or whole-suite credit follows.

The expanded selected-context composition proof now passes race x3 (36.263s),
including actual pipeline exclusion and gateway admission at the writer,
active/no-child rollback, terminal race refusal without predecessor revival,
postcommit cleanup/publication error with committed identity, and sibling-store
isolation. Source, generation, suspension, generic-pause and terminal expectation
races for all commands pass on both stores (race x3, 115.651s).

`TestServedParityHarnessStandingServiceLifecycle` now includes concurrent public
runtime.nuke behind a held standing operation, fresh child after reconstruction,
and shutdown held until exact successor child work joins: PASS 9.274s on both
stores. Its first new S16 run exposed retained predecessor suppression after a
fresh reset. The existing staging owner now clears old-epoch suppression and
publication preserves only freshly reconstructed durable suppression; direct
controls pass race x3 (1.429s). The authoritative spec records this rule. The
next failed shutdown observation used lookup after legitimate context withdrawal;
the corrected oracle observes the exact retained child, not discoverability.

Actual SQL rollback in all five public terminal/typed-invalid reset products
passes both stores (101.925s), with unchanged exact predecessor public facts
and the later lawful successor. Cancelled scheduler parking retains its exact
transition and remains fenced until the callback joins (race x3, 1.073s).

All nine bounded U1-U5 arms now have dual-store red execution and clean
restoration receipts, recorded with exact patches, historical diffs, semantic
attribution, raw hashes and rejected proof selections in
`issue-2498-mutations/README.md`. Clean paired sequential/restart controls pass
101.706s; nonzero readiness fields pass both stores x3 (3.631s). Mutation work
never touched candidate production code.

M20's preserved six-variant safety run FAILED its unchanged SQLite reverse-100
deadline: one gather delivery was still in_progress after 20s; all 201 expected
events existed. This concurrent run earns no safety closure. Isolated matched
candidate/master reverse-100 controls pass (20.541s/24.517s total; reverse
settlement 10.693s/14.433s). That comparison does not reconstruct the initial
failure or establish a performance fix. The failure receipt is retained; no
deadline, workload or runtime performance owner was changed. The complete
isolated six-variant corpus subsequently passes both stores (54.633s), without
changing those limits. That pass qualifies this execution, not a fix or causal
classification of the earlier deadline failure. Final default qualification
remains required.

The served lifecycle proof now also makes all three authenticated public commands
refuse fenced, durable-suspended/live-child and durable-active/missing-child
compositions, without changing the exact service/run/generation/state. Its explicit
test setup uses the existing selected-store writers and actual child owner;
runtime.nuke reconstructs the next fresh child before the held shutdown control.
PASS 10.755s, both stores. The initial setup lacked the canonical author-activity
scope and was corrected as a test precondition, not a production fallback.

Master was re-fetched and remains `abdb07bfe`. API-spec passes (4.151s). None of
these additional receipts is a final Post-Implementation Proof Audit or PR
readiness claim.

- Recheck current origin/master and measure the complete committed candidate
  complexity baseline without increasing either hotspot count.
- Complete and explicitly map remaining S10-S17 barrier, cleanup,
  source/control-race and composed reset/shutdown proofs. The public orphan and
  corruption rows and held-route/scheduler receipt above do not substitute for
  every invalid composition or race.
- Execute the original P01-P16/RB1-RB4 preserved controls and full numeric/
  paired-agent M01-M23 corpus and its unchanged burst/safety controls.
  The separate nine-arm U1-U5 admitted behavioral mutation receipts are complete;
  positive timer, permanent-receipt and field-bearing restart rows alone do not
  pay that credit.
- Run final default `go run ./cmd/swarm-test` on the reconciled committed head,
  preserve failure evidence, and publish the complete manifestation proof audit
  before requesting PR review. No `--full` or direct whole-tree go test used.

No new issue, framework, schema, migration, compatibility path, vendor dependency
or changes to the separate #2008 WIP. #2407/#2250 remain open. No new semantic
gate is requested for these ordinary in-scope proof obligations.

### Final Pause-Consumer Proof Completion

The default candidate qualification at `f2b7a9113` passed all 14 planned units
(log `agent-g-2498-final-default-qualification`, SHA256
`e1a9f11382df60873de5655f50d33904a7a303e68997e5bc4d474d20764ff4ff`).
That receipt does not qualify the newer master or the later changes below.

- P06 now executes real composed two-context startup on one selected store,
  preserving each source's paused unhanded facts while later running work
  settles. Fresh delivery authority is asserted independently; legitimate
  authority rebinding is not mistaken for executing the paused delivery.
- P07's real authenticated public continue starts with a deliberately prepared
  already-handed-only backlog and no open pipeline work. No manual wake occurs
  after continue. Removing only ReleaseRunQueue's continuation signal in a
  disposable candidate fails both stores (124.844s, delivered=0); restoring it
  passes both (3.994s). This controlled selected-store precondition is not a
  claim that publication while paused naturally hands off the event.
- P14 missing and contradictory control authority is invalid, never harmless
  parking. Actual selected-store scans/claims leave the exact delivery unchanged.
- P15's real timer and accepted mailbox verdict are retained while paused.
  Their first supported-public execution exposed recovery skipping the pipeline
  coordinator when there are no node routes: both events received a success
  receipt without their transition. The correction reuses the existing
  event-wide interceptor path only when no exact node delivery exists. Node
  execution remains continuation-owned; no event-name exception, timer/decision
  owner rewrite or new adapter is introduced. Both stores now transition only
  after public continue, with exactly one occurrence event (PASS 8.181s).
- Closed receiver projection and P14 controls pass race x3 (30.026s); composed
  P06, public P07 and P15 pass race x3 together (137.277s). API specification and
  proof-plan guards pass (4.066s / 23.129s). The default canary plan now selects
  these real proofs and requires their exact backend children.

The P15 before/after logs are respectively SHA256
`196c9d4485535f1114e9cd04b88355b7d4e35b3a78846e42e05003396003d770`
and `12097d7997246a0670c2f0940147433441a8cd2dc815a200bb577fa126639d90`.
The P07 signal-removal/restoration logs are respectively SHA256
`c07875b35a275b5ede051cc2cff65be0f823960b08731669b6c2fe87306db094`
and `7bbdf376b77ab13ef9a0471af8ace9f8a6d5ea254b589d2f81f522f00d44684f`.
These qualify already-required P rows through the existing owners, not a new
failure-class expansion or U1-U5 mutation claim. Rebase and final-head default
qualification remain required; master is now `8fac0f2e7`, not `abdb07bfe`.
