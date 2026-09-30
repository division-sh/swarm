# Pre-Implementation Coverage Audit: #2506

Agent-g. Child of #2447, Part of #2407. Analysis baseline:
`swarm@efdd163513d6dd4e9e81441a422da0315eddc612`; docs baseline:
`b2b59a1c81113212074131ff0a89e26c8054a4ee`.
**Independent gate approved; implementation authorized, not closure.**
The audit below records submission-time evidence. The separate #2008 WIP and
all other agent worktrees remain untouched.

## 1. Binding Context, Class, And Closure

Category: **high-risk maintenance**, behavior-preserving read-boundary factoring
and removal of test-only production paths. A complexity score is not a runtime defect.

The complete #2447 issue/thread, #2407 R1.4 constraints, the lead's whole-family
direction, IMPLEMENTER_GUIDELINES.md and SEMANTIC_DRIFT.md govern this work.
No exact spec section mandates factoring or defines a complexity ceiling. Exact
existing semantic contracts nevertheless bind every preserved read:

| Exact platform-spec.yaml section at the analysis baseline | Binding rule |
| --- | --- |
| `durable_data_resources.public_readback`, line 13917 | Lightweight declaration aggregate; exact selectors; bounded metadata keysets; one atomic summary/payload read; no full version/provenance inventory before selection. |
| `.declaration_projection.file_import_shape_detail`, lines 13778/13780 | Exact bundle/declaration/schema binding and compiled field eligibility; detail is separate from lightweight inventory. |
| `.version_identity`, line 13783 | Immutable manifest, canonical bytes, 1-based keyless ordinal / typed business key, multiplicity, canonical empty input. |
| `.operation_receipts`, line 13858 | Selected-store aggregate integrity, typed-column agreement and durable historical evidence, not self-authorizing JSON. |
| `.run_pins.retention`, line 13865 | Fixed query fingerprint and strict run-ID keyset; concurrent insertion cannot shift offset. |
| `.run_creation_request_binding`, line 13867 | Bounded validated complete original request metadata, including rejected receipts; no payload/input/path/credential disclosure or cache authority. |
| `.prune`, line 13908, and `.provenance`, line 13914 | Retained metadata after payload prune; version-local sequence is sole provenance traversal order; no compatibility reader. |
| API method `data.show`, line 34589 | Eleven views, bounded typed readback, explicit pages for non-singletons, query-bound cursors; existing auth boundary. |
| API schemas DataVersionSelector (32897), DataPageRequest (33045), DataVersionSummary (33259), DataRowSelector (33283), DataOperationRef (33326), DataShowResult (33550), DataRunCreationRequestBinding (33590) | Closed wire arms, bounds, required fields and exact result shapes. |

Re-read the actual sections, not only these summaries. No semantic delta is proposed:
platform-spec.yaml and generated OpenRPC remain unchanged. Discovery of an uncaptured
code/spec disagreement requires a ruling, not an opportunistic semantic repair.

Class chain:

- Observed symptom: resource and operation readers score cyclo/cognit 69/112 and
  48/76; mixed steps obscure error/store-call ordering. Two obsolete in-memory
  pagers have only tests as callers and give those tests non-user-path credit.
- Chosen working class: `data_show_read_family_mixed_responsibilities_and_dead_pager_proof_paths`.
- Immediate parent: keep-zone behavior-preserving hotspot reduction (#2447).
- Parent above it: R1 executable regression/merge-proof maintenance (#2407).
- Broader plausible architecture concern: representation/validation and projection
  ownership drift in complex read models. That is not evidence of a live runtime
  bug or permission to change durable-data types/owners in this maintenance PR.
- Framing: the new child is broad enough for the complete API family, not a resource
  helper-only slice. The original local function was the entry point, not the boundary.
- Intended closure: **failure class eliminated for this bounded maintenance class**:
  every router arm and receipt kind becomes a readable local path, all six dead
  production functions disappear, and their unique proof moves to the live handler.
  This does not eliminate all model debt, complexity or either parent class.
- Commitment: close the chosen class entirely in one PR, `Closes #2506`,
  `Part of #2447 / Part of #2407` only after all manifestation proofs succeed.

## 2. Replacement Eligibility Table And Proposed Design

The historical `salvage_map.csv` / `gocyclo_prhead.txt` are still absent from the
tracked source/docs trees. Missing keep classification is **not inferred away**.
This table is proposed replacement evidence for explicit lead ratification.
The earlier baseline-only #2447 gate does not authorize factoring.

Fresh `go run ./cmd/swarm-complexity -head origin/master` passes exact baseline
validation: 19004 callable rows per metric; cyclo >=30 281, cognit >=30 594.
Pinned unmodified tools remain gocyclo v0.6.0 / gocognit v1.2.1.

| Family member | Cyclo/cognit | Existing direct execution coverage | Proposed disposition |
| --- | ---: | --- | --- |
| executeDataShow | 20/25 | TestDurableDataHTTPPublicSurfaceAcrossSelectedStores; import-shape, declaration and permanent-operation HTTP probes | Keep one live dispatcher; extract both declaration and import-shape paths explicitly, as required by the independent gate. |
| executeDataShowResource | 69/112 | Expanded canonical-payload read, HTTP selected-store views, real pin/provenance concurrent pagination | Explicit bounded inventory, selected metadata, atomic payload, row and export paths; each factored function <25 cyclo. |
| executeDataShowOperation | 48/76 | Permanent-operation HTTP probes, prune complete-pin readback, both-store run-creation and corruption tests | Explicit source/prune/run-creation branches with unchanged receipt validation and page/error ordering; each factored function <25 cyclo. |
| pageDataProvenance | 10/13 | Only TestDataProvenanceCursorSurvivesConcurrentLineageInsertion | Delete; replace that test's helper calls with real data.show handler calls. |
| pageDataPins | 10/12 | Only TestDataPinCursorRemainsBoundedForLongDeclaration | Delete; replace that test's helper calls with real data.show handler calls. |
| dataProvenanceCursor, dataProvenanceCursorKey, compareDataPinKey, dataPinCursor | 1/0, 1/0, 2/1, 1/0 | Only reached through the dead pagers | Delete. |
| dataPage, pageDataItems, pageDataItemsFrom, pageDataDynamic, dataExportChunk, live sequence/run-ID/offset codecs, validators/error mapping | All <25 cyclo | Existing API/cursor/export/receipt controls | Retain canonical implementation and wire behavior; no paging framework or parallel codec. |

Eligible only if the gate ratifies this entire read family as keep-zone, outside
R4/R5/R6/R7 rebuild targets. No exported type, port, facade, transaction or ownership
change is proposed. Keep `DurableDataStore` unchanged, including its writer methods.
Do not split it as a concealed god-store/type-model refactor.

Smallest design: local unexported functions for explicit per-view/per-receipt read
steps; the existing entry dispatchers may stay as actual live dispatchers, not
compatibility wrappers. Do not invent a planner, variant registry, generalized
reader, query DSL, request framework, new semantic owner or a third-party dependency.
Retain low-complexity shared validators/pagers/codecs in place rather than move
the entire 1145-line file and consume the cap without improving comprehension.

Preflight feasibility estimate: approximately 1100-1400 gross changed production
lines, net -20 to -80 after dead deletion; **estimates, not proof or cap waiver**.
Base router/resource/operation functions occupy roughly 440 lines; the dead cluster
adds roughly 90. Local extraction is plausibly within <=1500 additions+deletions.
Actual PR numstat and independent head measurement must prove compliance. Stop if
the whole family cannot fit; do not silently land only the first helper.

Required narrow exception to #2447's unchanged-test rule: change the bodies of
exactly TestDataPinCursorRemainsBoundedForLongDeclaration and
TestDataProvenanceCursorSurvivesConcurrentLineageInsertion to invoke the public
handler, preserving or strengthening every unique assertion. Other existing
live/supported-surface assertions remain unchanged. Add characterization in new
tests before production changes. Deleting dead code is not permission to discard
long-declaration cursor bounds, concurrent traversal, wrong-query refusal or the
private-sequence non-disclosure assertion. Do not retain a test-only production
shim or duplicate the deleted algorithm inside the tests.

## 3. Full User Paths And Gate Classification

Direct RPC: ready selected-store serve context -> configured bearer authentication
-> JSON-RPC envelope/method/schema admission -> OperatorDataHandlers.data.show ->
view-specific exact request checks -> selected-store read/aggregate validation ->
API integrity checks -> existing typed page/DTO -> JSON-RPC serialization/budgets.

CLI: invocation/config/token/API readiness -> selected bundle -> declaration
inventory and exact selector -> data.show version/row/export -> client validation
and existing human/JSON/YAML/quiet/JSONL rendering. Standalone/fused file imports
also use declaration inventory and import_shape. Same explicit-run retry first
uses operation/run_creation/request_binding before reading mutable heads/files.

| Gate in execution order | Classification and proof |
| --- | --- |
| Process start, selected-store current authority, bundle presence/readiness | Different lifecycle concept, existing serve and selected-store controls; do not change startup/leases. Public journey must still start normally. |
| Invocation root, config/token lookup, bearer auth, envelope/schema/message limit | Different transport/CLI concept, existing handler/CLI tests; new read-family HTTP proof includes unauthorized and malformed-envelope controls before store calls. |
| View, exact fields, declaration/UUID/selector/page admission | Same chosen family; characterization locks error and call precedence. |
| Store authority, existence, historical receipt and snapshot validation | Existing canonical owner already consumed; same-class invariant to preserve, not a relocated transaction. Both-store corruption and concurrent-payload tests. |
| DTO integrity, row selection, keyset/offset presentation and query binding | Same chosen class; exact result/cursor/error assertions on every arm. |
| HTTP serialization/result budget, CLI export/summary validation | Same read-family consumer behavior, unchanged owners; real HTTP plus served CLI controls, not only private function calls. |
| Later run creation/publication/fork/agent execution | Different mutation/runtime concept; unchanged source/run/fork tests and golden controls; no ownership change or expanded execution credit. |

Preserve the current nonuniform ordering; a universal "parse everything first"
helper would change observable behavior:

- view is parsed before arm fields. Versions/head_history page/cursor checks
  precede bounded list reads. No selector is accepted for those two views.
- version/provenance/pins resolve and validate selected summary before detail
  page/cursor checks; singleton version never requires a page.
- row/rows/export atomically resolve summary+payload before row selector/page
  validation; summary/version and canonical integrity precede payload-pruned refusal.
  Preserve which missing/corrupt/pruned/malformed condition wins.
- operation parses detail, exact outer fields, operation_ref and kind before the
  request_binding/kind incompatibility check, then exact kind fields and UUID.
  Existing receipt load/validation precedes paged evidence parsing and detail
  compatibility selection. Do not eagerly reject unknown details before reads.
- run-creation request_binding consumes the validated record's binding and its
  ValidateForRecord path before the ordinary summary/evidence validation branch;
  it stays payload-free and never becomes a cache/reconstruction fallback.
- Single unknown-field/error fixtures preserve deterministic facts. Do not invent
  a sorted-error guarantee for requests with several unknown map keys when the
  old implementation does not define one.

## 4. Canonical Owners And Systematic Consumption Audit

The real semantic owner is **selected-store durable-data Owner**, not the first API
helper encountered. It already owns selection, aggregate validation, transaction
snapshots and keyset order on both stores. API factoring changes no semantic owner.

| Owner | Every relevant known consuming seam / disposition |
| --- | --- |
| platform-spec / generated OpenRPC / apispec registry | data.show request/result schemas, generic HTTP admission and result checks, CLI transport and API tests already consume the contract. No new schema or contract interpretation. |
| `internal/durabledata` typed records and row codec | API summary/pin/provenance/receipt Validate methods, canonical CompileStoredJSONL, selected-store receipt validation, CLI request-binding and export validation, tool resource reads already consume the owner. No new exported types or local codec. |
| `internal/store/internal/durabledata.Owner` and receipt_validation/run_creation/import_shape_detail | Both runtimepersistence backend adapters forward unchanged; API all eleven views consume the named methods below. Read/transaction/typed-column validation stays there, never reconstructed from current catalog or raw SQL in API/CLI. |
| `internal/apiv1/operator_data.go` live read dispatch/validation/projection | serveapp main.go mounts OperatorDataHandlers with apiStoreCaps.Data; internal capability tests and conformance data/fan-out/file journeys install that same map. All are already consumers, not separate dispatch interpreters. API private resource/operation test callers remain live internal test entrances, retained unless explicitly superseded. |
| existing API cursor/page helpers and canonicaljson | Every data.show page consumes offset, sequence or run-ID owner as specified below. Two dead in-memory pagers bypass the selected-store path only in tests; moved to real handler proof in this work, not treated as production compatibility. |
| CLI `data.go` | runDataShowCommand consumes declaration/version/row/export; loadDataDeclarations, resolveDataVersion and buildRunDataEnvelope consume inventory/summary. Already canonical API consumers, unchanged. |
| CLI `data_file_import.go` | loadFileImportShape consumes import_shape; loadRunCreationRequestBinding consumes permanent operation detail and treats only typed receipt absence as absence. Standalone import, fused run.start and replay consumers already canonical; unchanged. |
| releasee2e numeric_feed_assertions/refusal | Public data.show used for run binding, inventory and refusal/no-mutation evidence. Already real API consumers; retain compiled public journey, no new fixture or runner. |
| apiv1 HTTP/read/receipt tests, store selected-data tests, runtime conformance journeys, CLI transport tests | Existing consumer paths retained. Only two dead-helper tests move; add missing live-path characterization without weakening existing supported-surface tests. |
| `runtime/tools.execReadResourceData`, `flowdata`, `dataaccess.Materializer` | **Different authority/projection concept, execution-proven sibling**: actor/run-pinned access, target fingerprint v2, inline tool budget. No global data.show selector/receipt API, no shared pagination implementation. Existing tool target/auth/budget tests retained. |
| data.check/import/prune and run/fork writers | Different mutation concepts; consume shared exact-field/scalar/error and typed data owners. These helpers remain unchanged; mutation/CAS/receipt owners remain out of factoring scope. |
| store LoadPins/LoadHeadHistory/unbounded historical aggregate helpers | Different aggregate validation/fork/prune/writer purpose, not public per-page selection. Do not delete them merely because the public API uses bounded methods. |
| complexity command and workflow | Existing sole exact-snapshot owner; local final measure/baseline update and every CI posture consume it. No analyzer/threshold/scope changes to bless factoring. |

Complete selected-store call table for the read boundary:

| View/detail | Exact selected-store call order | Continuation / preserved validation |
| --- | --- | --- |
| declarations | ListDataDeclarationSummaries(bundleHash) | Lightweight aggregate; existing offset page on bounded declaration population; no version payload/provenance calls. |
| import_shape | GetDeclarationImportShape(bundleHash, ref) | No page; exact compiled shape/digest agreement. |
| versions | ListDataVersionSummaries(ref, afterSequence, page.Limit+1) | Version-alias sequence keyset; validates aliases/ref/strict sequence. |
| head_history | ListDataHeadHistory(ref, afterRevision, page.Limit+1) | Revision keyset; same history DTO/source-operation reference. |
| version | ResolveDataVersionSummary(ref, selector) | Typed singleton; no payload call. |
| provenance | ResolveDataVersionSummary; ListDataVersionProvenance(versionID, afterSequence, page.Limit+1) | Version-local sequence keyset; typed producer facts; sequence not serialized. |
| pins | ResolveDataVersionSummary; ListDataPins(versionID, afterRunID, page.Limit+1) | Strict run-ID keyset; exact declaration/digest/version; no offset or current-head fallback. |
| rows / row / export_chunk | ResolveDataVersionPayload(ref, selector) once | One canonical summary+payload snapshot, no separate summary/payload loads; immutable payload offset/ordinal/key semantics and export digest. |
| operation/source | LoadDataSourceOperation(id) | Aggregate validated before bounded immutable receipt evidence page; summary via SummarizeSource; same raw-page result type. |
| operation/prune | LoadDataPruneOperation(id); only pins detail subsequently LoadDataPruneOperationPins(id) | Complete durable refusal evidence even beyond embedded summary page; same ValidateWithPins and offset fingerprint. |
| operation/run_creation | LoadDataRunCreationOperation(runID) once | Complete validated historical record/binding; singleton summary/binding, immutable evidence offset pages. |

Both backends' forwarding methods are in runtimepersistence/durable_data.go.
SQL keysets and row validation reside in durabledata/owner.go (871-1170).
Atomic payload read uses runReadTransaction at owner.go:971. Receipt validation
is shared; LoadRunCreationOperation at run_creation.go:431 validates stored
aggregate and binds retained request before API presentation. Pin strict ordering
and provenance-anchor validation already occur in the canonical selected store;
deleting the dead pagers must not relocate or erase those guarantees.

Repo-wide searches covered function names, data.show method strings and CLI
constant references across runtime, store, operator/CLI, diagnostics/dashboard,
conformance, harness and generated contract. No other live public data.show
interpreter was found; no known consumer remains unclassified.

Old paths made invalid: pageDataProvenance, pageDataPins, dataProvenanceCursor,
dataProvenanceCursorKey, compareDataPinKey, dataPinCursor, and their only test calls.
Retain dataSequenceCursor/Key/Value, dataPinCursorForRun/Key/RunID, payload cursor,
pageDataItems/From/Dynamic and dataExportChunk. Remove sort import only once its
sole dead Search consumers disappear. Exact zero caller/declaration deletion
census is mandatory after factoring; baseline JSON historical names must be
regenerated, not excluded from the search.

## 5. Complete Manifestation / Exact Proof Plan

**New test names below are planned commitments, not implemented or passed tests.**
`V` = new TestDataShowReadFamilyAcrossSelectedStores, executed through authenticated
/v1/rpc on real SQLite/PostgreSQL. `O` = new
TestDataShowOperationReadFamilyAcrossSelectedStores via the same HTTP owner.
`T` = new TestDataShowValidationPrecedenceAndStoreTrace, live handler plus a
call-recording adapter preserving order/args and returning hostile read projections.
All views/details compare typed/wire results, empty arrays, continuation, exact
error facts and non-mutation, not just successful JSON decode.

| ID | Known manifestation | Exact planned proof |
| --- | --- | --- |
| M01 | Declaration inventory | V/backend/declarations + unchanged TestDataShowDeclarationsConsumesBoundedStoreSummary; T forbids payload/provenance/shape expansion. |
| M02 | Import shape | V/backend/import_shape + unchanged TestDataShowImportShapeRequiresExactImmutableBinding; empty fields array, digest/bundle/ref contradictions. |
| M03 | Versions inventory | V/backend/versions: required page/no selector, empty and multi-page monotonic alias keyset; same list limit+1 and argument trace. |
| M04 | Version singleton | V/backend/version: exact head/version/alias, missing declaration/version, pruned metadata remains readable; no payload access. |
| M05 | Row page | V/backend/rows: canonical keyed/keyless/empty rows, immutable page continuation, exactly one payload snapshot. |
| M06 | Row singleton | V/backend/row: typed boolean/number/string key or 1-based ordinal, zero/missing/wrong-arm selectors and lookup misses; unchanged expanded-payload control. |
| M07 | Export chunk | V/backend/export_chunk; unchanged export ordinal/empty tests and CLI stream controls; row/byte budgets, exact canonical bytes/digest and cursor. |
| M08 | Provenance page | V/backend/provenance; unchanged both-store TestDataShowProvenanceCursorSurvivesConcurrentProducerAcrossSelectedStores; strict sequence and private-sequence non-disclosure. |
| M09 | Head history page | V/backend/head_history: exact revision traversal, source operation link, equal timestamp order, wrong/missing anchor/cursor refusal. |
| M10 | Pin page | V/backend/pins; unchanged TestDataShowPinCursorSurvivesConcurrentRunCreationAcrossSelectedStores and TestDataShowPinCursorIsStableAcrossConcurrentInsertions. |
| M11 | Operation routing and incompatible kinds/details | O/backend/router + T: canonical UUIDs, exact kind fields, request_binding only for run_creation, error precedence and no unauthorized store call. |
| M12 | Source summary | O/backend/source/summary for check/import accepted/rejected/conflict historical receipts, exact SummarizeSource output. |
| M13 | Source defects | O/backend/source/defects: complete bounded evidence, empty/over-budget pages, exact raw page result. |
| M14 | Source added delta | O/backend/source/delta_added: keyed and positional/empty evidence, unchanged fingerprint and byte accounting. |
| M15 | Source removed delta | O/backend/source/delta_removed: corresponding full immutable evidence traversal. |
| M16 | Source changed delta | O/backend/source/delta_changed: corresponding full immutable evidence traversal. |
| M17 | Prune summary | O/backend/prune/summary: accepted/already-pruned/head-conflict/current/pinned outcomes; historical receipt survives payload loss. |
| M18 | Prune defects | O/backend/prune/defects: absent evidence refusal and exact defects page. |
| M19 | Prune pins | O/backend/prune/pins + unchanged TestDataShowPagesCompletePrunePinEvidenceBeyondBoundedSummary; full durable evidence and ValidateWithPins, no truncation to embedded summary. |
| M20 | Run-creation summary | O/backend/run_creation/summary + unchanged both-store lifecycle controls; accepted/rejected and exact permanent identities. |
| M21 | Run-creation request binding | O/backend/run_creation/request_binding + unchanged TestRunCreationRequestBindingAcrossSelectedStores; complete original ordered imports/pins, payload-free response, nil/corrupt binding refusal. |
| M22 | Run child evaluations | O/backend/run_creation/child_evaluations: immutable complete multi-page evidence, accepted/failed-child cases. |
| M23 | Run child defects | O/backend/run_creation/child_defects: full rejected evidence with exact source invocation refs. |
| M24 | Run binding | O/backend/run_creation/run_binding: every pin/import/rejection arm, no invented target or partial summary substitution. |
| M25 | Malformed request and error/store-call precedence | T: all view/detail branches with paired-invalid fields and missing/corrupt/pruned data; preserve failure codes/reasons/retryability and exact call counts/arguments. Characterize before production changes. |
| M26 | Corrupt summaries, rows, provenance, pins, shape and receipt aggregates | New TestDataShowCorruptReadFamilyAcrossSelectedStores, T hostile projections, unchanged source/prune/run-creation typed/aggregate corruption controls; canonical store validates order/anchors, API retains own cross-field checks. |
| M27 | Prune/rematerialization concurrent snapshot | Unchanged TestResolveDataVersionPayloadIsAtomicAcrossPruneAndRematerialize and both-store V row/export; no torn independent reads. |
| M28 | Every live cursor's exact bytes/query binding | New TestDataShowLiveCursorWireAndQueryBinding using emitted handler cursor: offset/alias/revision/provenance/run ID, wrong query/format/version, malformed/noncanonical/checksum/oversize boundaries; compare captured pre-refactor bytes, no new version. |
| M29 | Dead pin pager's unique long-declaration proof | Replace only TestDataPinCursorRemainsBoundedForLongDeclaration helper calls with handler calls: two pages, same run ordering, <=4096 cursor, admission of emitted cursor, end state. |
| M30 | Dead provenance pager's unique traversal proof | Replace only TestDataProvenanceCursorSurvivesConcurrentLineageInsertion helper calls with handler calls: new later producers, no repeat/skip, bounded cursor, wrong-query rejection, no serialized sequence. |
| M31 | Public CLI summary/row/export/shape consumers | New TestDataShowCLIReadFamilyAcrossSelectedStores against real mounted API; declaration selection, head/version/alias, key/position, all existing output modes, JSONL multi-page/empty/digests. Existing stream/operand/selector tests unchanged. |
| M32 | Fused/file import and explicit-run retry binding consumer | Unchanged TestDataTextFile2456KeylessOperatorRouteRestartReplayBothStores and TestDataTextFile2456RejectedMultiPinServedReplayBothStores; complete binding before mutable selection, no payload disclosure or typed absence conflation. |
| M33 | Auth/transport read-only surface and domain errors | V/O unauthorized and malformed-envelope controls; unchanged TestOperatorDataHandlersMapTypedErrors and HTTP schema probes; new state-conservation assertion for all successful/failed reads. |
| M34 | Run-pinned tool sibling stays distinct | Unchanged TestExecutorReadResourceDataAcceptsExpandedCanonicalPayload, TestExecutorReadResourceDataFitsInlineEnvelopeAndExactArrayBudget, TestExecutorReadResourceDataCursorIsBoundedAndTargetBound, TestExecutorReadResourceDataRejectsIntrinsicallyOversizedRowsBeforeProjection, TestExecutorReadFlowDataFailsClosedForUndeclaredAndEscapingFiles, TestExecutorReadFlowDataRejectsRoleModeImpersonation and TestExecutorReadFlowDataRequiresWorkflowSource; no shared data.show cursor or actor-free API substitution. |
| M35 | Complete dead-code retirement and real-path proof coverage | Repo-wide exact six-name deletion/caller census + compiled unchanged supported tests; no moved test-only production functions. |
| M36 | Measured factoring and net/gross budget | Exact pinned base/head swarm-complexity evidence and same-PR baseline; each factored function <25 cyclo, both >=30 counts nonincreasing, no analyzer suppression; actual gross <=1500/net<=0 production lines. |
| M37 | Production-shaped behavior remains unchanged | Unchanged TestGoldenAgentWorkloadSQLiteSmoke, TestGoldenAgentWorkloadRestartAndForcedKillOnBothBackends, TestGoldenAgentWorkloadBurstConcurrencyOnBothBackendsIteration1 and TestGoldenAgentWorkloadBurstConcurrencyOnBothBackendsIteration2; final default swarm-test and exact-head CI, no --full unless requested. |

Generic proof is the V/O/T complete view x detail x malformed/hostile-state
characterization matrix, not a single green helper test. Existing HTTP/store tests
are retained regression controls; a test passing against the old dead pager is
explicitly not proof of the production selected-store route. Public API/CLI,
supplemental adapter/store, sibling tool and golden credits remain distinct.

## 6. Parent Probe, Watchlist Promotion, Tracking, And Active Lanes

Parent probe looked beyond the resource helper: operation receipts are the same
API boundary and are **absorbed now**; all eleven views and dead-test paths are
included. read_flow_data is different actor/run-pinned authority, proven by its
actual target/run/identity/budget tests, and stays unchanged. Other keep-family
hotspots remain #2447; no generic validation/model rewrite is justified here.

Current open PR file sweep: #2505 (C, grammar), #2504 (D, agents), #2482 (F,
channel delivery), #2372 (string helpers), #2325 (timing). No production change
to operator_data.go or the proposed new API read files. #2505/#2504 touch tool
tests and the complexity artifact; consume their integration and regenerate
the final baseline rather than duplicate their changes. Shared package rebuilds
and complexity-baseline conflicts remain possible; absence of production-file
overlap is not a promise of zero integration work.

Checked committed and dirty candidate-file differences in A's join worktree,
B's current lane, C's grammar lane, E's #2496 audit, D's #2485 and F's #2241.
No operator_data.go/operator_data_test.go/operator_data_run_lifecycle_test.go
overlap in those inspected lanes. Excluded describe despite initial attractiveness:
A changes its production/tests, E has dirty production edits and C dirty tests.
Excluded CLI-provider source for #2372 overlap; excluded boot/readiness/lifecycle,
model/admission, channel and fork owners for active R-lane work.
Recheck the active overlap immediately before coding and at rebase.

Tracker decision: create focused child #2506 before coding; update #2447's current
factoring queue and notify #2407 without closing/reopening old streams. No new
runtime/architecture defect issue and no POTENTIAL_ISSUES entry. #2499 is deferred
per lead priority, not absorbed. #2250/#2349 broader ownership/type debt stays open;
#2297 write proposals, #2496/#2495 readiness/lifetimes and F's channel work remain
separate, not dependencies for this read-only factoring.

Watchlist repair is already published to docs master at `swarm-docs@a2658dd`
after rebasing the audit-only change onto current docs master `c51401a`.
Only maintenance-and-cleanup.yaml changed: map #2506 and refine the existing
boundary_owned_decomposition / invariant_suite_coverage nodes. All four
watchlists parse; IDs are unique and every supplied node mapping resolves;
#2506 maps to exactly the two existing nodes. Eight pre-existing unmapped
historical semantic-correctness records are unchanged, not claimed repaired.
git diff --check passes. The initial analysis docs baseline remains pinned above.

Watchlist-backed promotion check: maintenance `boundary_owned_decomposition`
already tracks mixed responsibilities, fake extraction and cleanup lifetime loss;
`invariant_suite_coverage` tracks issue-local/non-executable proof. Their evidence
supports promoting from one helper to the whole data.show family and moving
dead-helper proof onto real reads, **not** expanding into read_flow_data or runtime
startup/model rewrites. Refine those two existing nodes, map #2506, retain #2447
and #2407 open. No new taxonomy. The class is cohesive and can be closed in one
PR; fixing only resources would leave the operation interpreter/dead proof live.

Remaining parent tail estimate: historical #2447 estimate 11-13 families is still
low-confidence; after this coherent child, roughly 10-12 families may remain,
grouped as API/CLI presentation, schema/contract validation, provider/tool reads
and currently occupied boot/channel families. This is not a validated queue or
approval: missing keep evidence and R-lane exclusions must be refreshed per family.
#2407 also retains #2499 monthly reporting and lead-owned R1.3 policy; no credit
for those rows. No broader-parent closure claim follows from this child.

Architecture feedback: raw map/string request arms plus several valid result
models can hide ordering, and tests can perpetuate a retired producer. The
long-run better direction here is explicit local read stages consuming existing
typed records and selected-store owners, not another sum-type/request framework.
Tracking decision: implement this concrete decomposition/deletion under #2506;
retain wider representable-invalid/type-model concerns **watchlist only** unless
a real new contradiction is found. Do not turn this into #2349 redesign by stealth.

## 7. Executed Baseline Evidence And Explicit Gate Request

At efdd16351, **unchanged baseline** controls executed, not implementation proof:

- Fresh exact-snapshot complexity command PASS; 69/112, 48/76, 20/25 reproduced.
- Focused API set of 13 top-level tests PASS 3.352s, using host PostgreSQL DSN:
  expanded payload, shape, lightweight declarations, complete prune pins, live
  pin insertion, the two dead-helper controls, export row/empty cases, and
  both-store request binding, pin/provenance concurrent traversal and HTTP journey.
- Focused five tool resource/target/budget/source tests PASS 0.315s.
- Focused five CLI export/empty/selector/invocation-root tests PASS 0.013s.
- Caller/SQL owner/open-PR/local-WIP census inspected. No production or test code
  changed. No full suite, new manifestation matrix, mutation sensitivity, live
  provider call or runtime closure is claimed by this pre-audit.

Exact baseline selectors (regular targeted go test, not the whole-suite runner):

```sh
go test ./internal/apiv1 -run '^(TestDataShowReadsExpandedStoredCanonicalPayload|TestDataShowImportShapeRequiresExactImmutableBinding|TestDataShowDeclarationsConsumesBoundedStoreSummary|TestDataShowPagesCompletePrunePinEvidenceBeyondBoundedSummary|TestDataShowPinCursorIsStableAcrossConcurrentInsertions|TestDataPinCursorRemainsBoundedForLongDeclaration|TestDataProvenanceCursorSurvivesConcurrentLineageInsertion|TestDataExportChunkHonorsRowLimitAndOneBasedOrdinals|TestDataExportChunkUsesZeroOrdinalForCanonicalEmptyVersion|TestRunCreationRequestBindingAcrossSelectedStores|TestDataShowPinCursorSurvivesConcurrentRunCreationAcrossSelectedStores|TestDataShowProvenanceCursorSurvivesConcurrentProducerAcrossSelectedStores|TestDurableDataHTTPPublicSurfaceAcrossSelectedStores)$' -count=1 -timeout=5m
go test ./internal/runtime/tools -run '^(TestExecutorReadResourceDataAcceptsExpandedCanonicalPayload|TestExecutorReadResourceDataFitsInlineEnvelopeAndExactArrayBudget|TestExecutorReadResourceDataCursorIsBoundedAndTargetBound|TestExecutorReadResourceDataRejectsIntrinsicallyOversizedRowsBeforeProjection|TestExecutorReadFlowDataRequiresWorkflowSource)$' -count=1 -timeout=5m
go test ./internal/cliapp -run '^(TestStreamDataJSONLAcceptsOneBasedMultiPageExport|TestStreamDataJSONLAcceptsCanonicalEmptyExport|TestStreamDataJSONLRejectsContradictoryEmptyExportShapes|TestDataVersionSelectorUsesCanonicalAliasOwner|TestDataFileOperandsUseInvocationRoot)$' -count=1 -timeout=5m
```

The API command had SWARM_TEST_POSTGRES_DSN set to the reachable host test
database; both backend leaves executed, not skipped. The additional actor
authorization tests in M34 remain required final sibling proof, not credited
to the five-test baseline selector above.

Required independent outcome on #2506: ratify (1) the exact replacement keep-zone
eligibility table, (2) the entire one-family router/resource/receipt/dead-pager scope,
and (3) the two-test exception with all unique assertions retained on real paths.
The gate must also confirm cap feasibility, exact error/call ordering, unchanged
canonical owners and the complete both-store supported-surface matrix.

**Gate outcome at original submission: pending, not approved or self-ratified.** A lead recommendation
and the old complexity-baseline gate are not the recorded factoring gate.

Stop conditions: missing owner/caller; newly live legacy pager or active-agent
conflict; uncaptured off-spec behavior; need to change error/cursor/store-call or
read-only semantics; impossible cap/under25/net target for the complete family;
required ownership/schema/recovery/exported-model framework or vendoring; proof
replacement losing an original invariant. Repair issue/gate before proceeding.
No Broad Refactor Escalation is presently required: the proposed wider child is a
bounded local family, not a runtime ownership refactor. Stop here for review.

## 8. Recorded Independent Gate And Implementation Boundary

The [independent gate](https://github.com/division-sh/swarm/issues/2506#issuecomment-5921436396)
records **approved**, ratifying the replacement keep-zone table, entire family
and the exact two dead-helper test replacements. This is coding approval only.
It tightens the dispatcher row above: declarations and import_shape must become
explicit local read paths, not remain embedded under "only as needed".

All six binding conditions are retained: complete eleven-view/three-receipt
scope; exact nonuniform error/read/integrity/cursor order with unchanged owners;
six-name dead-cluster deletion and only the two approved existing test edits;
pre-refactor precedence/cursor characterization and all 37 final surface proofs;
<=1500 gross production lines, net<=0, every factored function cyclo<25 and exact
final integrated complexity baseline; focused, unchanged dual-store golden,
default swarm-test, exact-head CI and final PR proof audit. No spec/OpenRPC
semantic change, universal parser, model/port split, framework or vendoring.

At implementation intake origin/master still equals efdd16351 and the open PR
set remains #2505/#2504/#2482/#2372/#2325. No operator_data production-file overlap
was introduced. Recheck integration before final qualification and baseline update.
Both parents remain open. The original pending statements above describe audit
submission history; this recorded gate supersedes them without claiming proof.
