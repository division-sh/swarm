# Pre-Implementation Coverage Audit: #2511

## Recorded Independent Gate

[Reviewer-g's ruling5943495819](https://github.com/division-sh/swarm/issues/2511#issuecomment-5943495819)
is **approved as first slice**. It authorizes complete five-validator maintenance,
not broader receipt-validity/type-model closure. N01/N02 must be added and run
on unchanged production before extraction; all48 proof rows, both-store public
readback/retained restart/fresh compiled CLI reconstruction, representative
mutations, exact-head ratchet, default swarm-test and CI remain required.
No schema, ownership, constructor, compatibility or vendor change is approved.
The pending statements below are the historical submitted audit, superseded
only by this recorded gate. Implementation starts with characterization.

## Pre-Extraction Characterization Receipt

N01/N02 were added with every production file unchanged from `6646ea588`.
`TestDurableDataAggregateValidationCharacterization` passes 150 leaf cases;
`TestDurableDataAggregateValidationErrorPrecedence` passes 169 leaf cases.
The cases cover all five targets, outcome decisions, canonical declaration
and local-value gates, exact errors/wrapping, nil versus empty evidence and
input immutability. Competing-error cases retain gate order; unordered child
count validation is deliberately not assigned a deterministic multi-error
order. Existing defensive branches dominated by earlier admission remain
intact, rather than being credited as directly reachable.

Commands run on 2026-10-02 before extraction:
`go test ./internal/durabledata -run '^TestDurableDataAggregateValidation(Characterization|ErrorPrecedence)$' -count=1 -timeout=2m`
and `go test ./internal/durabledata -race -count=3 -timeout=3m` (4.770s).
Both passed. This is baseline characterization, not post-extraction closure.

Agent-g, 2026-10-02. **Reviewer-g independent gate requested; implementation
has not started.** Source baseline: merged
`origin/master@6646ea5888f7c38d4d5e411717037a9d170ef062`. Source branch:
`agent-g/2511-preaudit`. Docs intake baseline:
`caaa73ceb5823868e5d0dce4a3a91187263e6b4e`.

## Binding Context And Class Model

Read the complete #2511 body/thread (no prior comments), the current #2447
child allocation and lead waivers, #2407's original acceptance/program rules
and current accounting, and #2349's architecture watchpoint. Re-read
`swarm-docs/docs/IMPLEMENTER_GUIDELINES.md` and `SEMANTIC_DRIFT.md`.

Category: high-risk, behavior-preserving maintenance. The observed symptom is
monolithic cross-field aggregate validation in a live permanent-receipt family,
not a newly demonstrated runtime defect. The named methods were entry points,
not the audit boundary. The sweep includes all 28 callables in
`internal/durabledata/aggregate_validation.go`, candidate/rejection/command
types, immutable evaluation, typed-column joins, selected-store readers and
writers, replay, API projections, CLI reconstruction, cleanup and fork siblings.

Working class: `monolithic_durable_data_operation_aggregate_validation` across
source results/records, failed fused-child evaluation, run-creation evidence,
and prune decisions. This PR aims to eliminate **the entire chosen maintenance
class**, including the two coupled high-complexity siblings found by the census:

| Callable in aggregate_validation.go | Line | Cyclo | Cognitive | Decision |
| --- | ---: | ---: | ---: | --- |
| `(PruneOperationResult).Validate` | 655 | 64 | 50 | Factor the whole decision family |
| `validateSourceOperationResult` | 78 | 38 | 34 | Factor all outcomes and ordered gates |
| `(RunCreationOperationRecord).Validate` | 441 | 32 | 35 | Factor full parent/child/binding aggregate |
| `(FusedChildEvaluation).Validate` | 274 | 28 | 22 | Absorb now; same failed-parent aggregate family |
| `validateCreatedRunBinding` | 534 | 25 | 26 | Absorb now; called by the parent validator, not a follow-up |

Unmodified tools: `github.com/fzipp/gocyclo/cmd/gocyclo@v0.6.0` and
`github.com/uudashr/gocognit/cmd/gocognit@v1.2.1`. These are current file-local
measurements, not a newly measured repository-wide ratchet receipt.

Immediate parent: oversized durable-data semantic admission/validation seams
under #2447. Parent above that: regression-prone keep-zone complexity under
#2407 R1.4. Adjacent broader architecture class: representable-invalid values
and review-enforced owner/consumer contracts under #2349; #2250 owns broader
orchestration debt. These are not silently made implementation dependencies.

Issue framing is **broad enough after the census amendment**, but deliberately
only a child of #2447/#2407. The five functions are one coherent family, not
five independent patches. The issue's initial three-function list needs the
two coupled additions and current baseline recorded before coding. No new child,
umbrella, superseded stream or broad-refactor framework is required.

The intake did not cite an exact spec section for extraction. Binding
maintenance context is #2511/#2447/#2407; exact domain sections do exist and
remain authoritative. At this source baseline, re-read:

| Exact platform-spec.yaml path | Binding constraint |
| --- | --- |
| `durable_data_resources.owner` | internal/durabledata semantic records and the selected-store durabledata owner remain canonical |
| `durable_data_resources.source_operations` | Permanent receipt lookup precedes mutable state; immutable admitted declaration, historical head/base and exact request own deterministic evaluation; accepted import and noncommitting check are distinct |
| `durable_data_resources.operation_receipts.integrity` and `.source_commit` | Typed columns, request, result, historical evaluation and exact commit/provenance/history facts agree on insert, replay and read; nonmutating outcomes cannot acquire commit facts |
| `durable_data_resources.run_pins` | Explicit selection only; exact run binding, immutable replay, retained terminal pins and failed-parent evidence; fork inheritance is separately owned |
| `durable_data_resources.run_creation_request_binding` | Permanent full request owns equality; bounded public metadata is payload-free and preserves the complete original selection, including early-rejected two-pin requests |
| `durable_data_resources.deployment_origin` | Event-only, feed-only and event-plus-feed initiation; no synthetic trigger or pin-without-feed accommodation |
| `durable_data_resources.prune` | Replay/conflict precedence, current/pinned refusal, complete pin evidence and payload-only removal preserve receipts/history/provenance |
| `durable_data_resources.version_identity`, `.public_readback` and `.legacy_policy` | Canonical identities, byte bounds and retained metadata; no payload reconstruction, legacy store, migration or compatibility |
| `api_specification.method_catalog.data.show`, `.data.prune` and `.data.import` | Exact public validation/errors and bounded operation details remain unchanged |
| `api_specification.components.schemas.DataOperationRef`, `.DataShowResult` and `.DataRunCreationRequestBinding` | Source/prune/run-creation closed union and payload-free request-binding wire contract |

The domain sections are at approximately lines13948-14130 and the API method
catalog at34773. No contradiction requiring a spec correction was found.
**No spec/runtime/data semantics change is proposed.** If that conclusion
changes, stop and repair the gate rather than treating a prose-only update as
authority to redesign the model.

## Smallest Implementation And Preserved Behavior

Retain all exported types, methods, callers and store contracts. Extract private,
ordered validation steps within internal/durabledata: local identity/field
validation, evidence validation, cross-field relationships, and closed-outcome
decisions. Reuse existing candidate, head, delta, page, lifecycle and request
owners; do not invent a second validator for an already-owned invariant.

All five entry/coupled callables and every new helper must be under cyclo25.
The other23 file callables remain in the consumption/proof census; they already
score at most20 and need not be rewritten merely to make more files. No giant
closure hidden inside a short wrapper, copied switch, reflection framework,
exported constructor, generic validation engine or new selected-store port.

Before production extraction, add characterization against the unchanged
baseline. Preserve exact error text, wrapping and deterministic gate order,
accepted result fields, request hashes, timestamps, nil/empty distinctions,
canonical evidence pages, sorted evidence, duplicate checks, count checks,
candidate-versus-committed states and no-op behavior. Return the first existing
error, not an accumulated error list. Preserve early exits and caller placement
of checks. The current child-count map iteration does not define a stable first
offending child when several counts are wrong; retain that behavior, characterize
isolated violations and deterministic surrounding precedence, and do not add
sorting to create an unapproved public error precedence.

No old interpreter becomes a compatibility path. The old monolithic bodies are
replaced, not left as alternate implementations. Lower scores alone do not prove
closure. Existing typed-column/historical/relational integrity checks remain
authoritative additional obligations; they are not redundant pure Validate
calls to delete. Summary-only internal validation markers are not durable child
evidence and are not authorized for removal by this audit.

## Ordered User-Visible Execution Paths

The final receipt read is reachable only after prior admission, ownership and
transaction gates. Each gate below is classified; none is forced successful to
earn whole-path proof.

| Ordered path/gate | Classification | Authority and execution proof |
| --- | --- | --- |
| All paths: admitted immutable bundle/declaration/schema, selected-store possession, HTTP authentication, read:data/write:data and bounded closed request decoding | Different semantic concept, with proof | Existing sourceartifact, API and store owners; E09/E12/E19 and Q01/Q04, not a boot/auth rewrite |
| Source: canonical SourceCommand/hash -> permanent receipt lookup before present head/catalog -> historical evaluation/base admission | Different semantic concept, with proof | SourceCommand and selected-store ExecuteSourceOperation/decode receipt; E01/E03/E04/E06 |
| Source: evaluate check/import -> candidate/head/delta/defects/result validation -> full evidence/command validation | Same chosen class | Source result/record/summary graph; A01-A12, N01/N02 |
| Source: accepted import commits immutable version/provenance/conditional head history; check/rejected/conflict commits no source facts; insert receipt -> typed-column and historical joins | Different semantic concept, with proof | Existing selected-store source transaction and receipt_validation owner; E04-E07 |
| Fused new run: full command canonicalization/initiation -> parent permanent replay lookup -> reserve child IDs and evaluate every import/explicit pin | Different semantic concept, with proof | RunCreationCommand and PrepareRunCreationTx; E10/E11/E14; no existing-run data amendment |
| Fused new run: child validation -> complete parent child/defect memberships -> rejected no-binding or created pin/import groups -> command agreement | Same chosen class | FusedChildEvaluation/RunCreationRecord/created binding; R01-R14, N01/N02 |
| Fused new run: one transaction commits or rejects exact parent, child/source evidence, pins and event/feed/run completion -> typed relational validation on replay/read | Different semantic concept, with proof | durabledata run_creation and eventpersistence event/deployment owners; E10/E11/E13/E14/E21 |
| Prune: canonical request/hash -> permanent replay/conflict before mutable declaration/head/payload/pins -> current aggregate integrity | Different semantic concept, with proof | PruneDataResource/decode receipt; E02/E08/E17 |
| Prune: validate identity/pages -> exact rejected/conflict/current/pinned/already-pruned/pruned outcome -> complete sorted pins and command | Same chosen class | Prune result and ValidateWithPins/ValidateForCommand graph; P01-P12, N01/N02 |
| Prune: retain or clear payload only -> insert permanent receipt and complete refusal evidence -> typed-column read validation | Different semantic concept, with proof | Existing selected-store transaction and receipt_validation; E02/E08/E17 |
| Public read: exact operation_ref/detail/page decode -> selected permanent source/prune/run-creation lookup -> aggregate validation -> summary/evidence page or request_binding validation | Same chosen class for aggregate consumption; bounded cursor/presentation is a different semantic concept, with proof | executeDataShowOperation and its three branches; E09/E12/E15/E16, N03; no new pager or resource reader |
| CLI reconstruction: fresh run start --run-id/--data/--pin -> data.show operation request_binding -> metadata/record relationship validation -> exact request reconstruction or one recorded conflict reread | Different semantic concept, with proof, consuming this class | data_file_import.go/run_command.go and retained permanent owner; E18/N03; no current-head retry substitution |

Public operation readback means `/v1/rpc` **data.show view=operation**, not an
invented `swarm data show --operation` flag. The existing CLI resource views are
sibling consumers, and run-start request reconstruction is the relevant CLI
operation consumer.

## Canonical Owners And Exhaustive Systematic Consumption

These are actual semantic owners, not the first local file encountered. Status
vocabulary is: **already consumes the canonical owner**; **moved to the canonical
owner in this work**; **different semantic concept, with proof**; **still bypasses
and explicitly split/escalated**. No moved or bypass row was discovered: this is
factoring under existing ownership, not a migration disguised as maintenance.

| Owner | Every currently known consumer/seam | Status and proof |
| --- | --- | --- |
| SourceOperationResult and validateSourceOperationResult | operations.go public Validate wrapper; SourceOperationRecord.Validate and SourceOperationSummary.Validate in aggregate_validation.go | Already consumes; A01-A12/N01/N02/E01/E03 |
| SourceOperationRecord command/evidence validation | decodeSourceReceipt/insertSourceReceipt/ExecuteSourceOperation/LoadSourceOperation in store/internal/durabledata/owner.go | Already consumes; insert/replay/read all delegate; E03-E07 |
| SourceOperationRecord command/evidence validation | API executeDataSource and executeDataShowSourceOperation, public source summaries and delta/defect pages | Already consumes; E09/E12/E15/N03; retain result-versus-complete-record distinction |
| FusedChildEvaluation validation | SourceEvaluationContext.FusedProjection; rejected parent RunCreationRecord.Validate; failed-child selected-store decode/recompute in run_creation.go | Already consumes; R04-R08/E10/E11/E13 |
| RunCreationOperationRecord/summary/binding validators | ValidateRunCreationReceiptForCommand wrapper; PrepareRunCreationTx/reject/Complete/insert/decode/load/replay; ValidateForCommand | Already consumes; R01-R14/E10-E14/E21 |
| RunCreationOperationRecord/summary/binding validators | EventBus PublishResult.Validate and API run-start completion; eventpersistence bindRunCreationCompletion and event commit | Already consumes; typed aggregate publication/completion, E10/E14/E21; no second child validator |
| RunCreationOperationRecord/summary/binding validators | Eventless deployment completion/replay and runtimepersistence deployment adapter | Already consumes; same receipt-for-command wrapper; E10/E14/E21, feed-only N01 |
| RunCreationOperationRecord/summary/binding validators | API executeDataShowRunCreationOperation summary/child_evaluations/child_defects/run_binding | Already consumes; E09/E12/E15 and N03 |
| Request-binding metadata/relationship owner in request_binding.go | BindRunCreationRequest in selected-store LoadRunCreationOperation; API request_binding branch; CLI loadRunCreationRequestBinding/reconstruction in data_file_import.go/run_command.go | Already consumes; E14/E18/N03; full stored request remains equality authority, not metadata DTO |
| PruneOperationResult, complete-pin and request validators | PruneDataResource/insert/decode/LoadPruneOperation/LoadPruneOperationPins in owner.go and typed receipt validation | Already consumes; P01-P12/E02/E08/E17; preserve integrity before ordinary refusal |
| PruneOperationResult, complete-pin and request validators | API executeDataPrune and executeDataShowPruneOperation summary/defects/pins; CLI data prune presentation | Already consumes; E09/E12/E15/E17/N03; API request-result check does not replace complete store evidence |
| Identity/local validation owners | HeadResult, ValidationDefect, FusedChildDefect, Pin, RunCreationDataItem, DataBinding, PruneDefect and canonical UUID/completed_at/run state in aggregate_validation.go | Already consumes; direct graph plus A/R/P identity and hostile matrix N01/N02; no alternate spelling/default/normalization |
| Evidence owner | FirstEvidencePage, validateDeltaEvidence, validateRejectedChildren, validateCreatedRunRequest and lower-score aggregate wrappers | Already consumes; all28 callables classified; A08-A12/R07-R14/P07-P12/N01/N02 |
| Candidate/rejection/request owners in operations.go | Source result/summary and fused child consume CandidateVersion and closed candidate-state validation; run summary consumes RunCreationRejection; command wrappers consume RequestHash/Initiation/Canonical envelope | Already consumes; E01/E10/E14/N01, preserve event-only/feed-only/event-plus-feed and explicit/fused selection |
| Immutable SourceEvaluationContext/codec owner | Standalone evaluation, canonical stored replay, failed-parent FusedProjection, API/store readback through ValidateRecord | Already consumes; E04/E06/E07/E11/E13; pure aggregate consistency does not substitute for recomputation |
| Selected-store durabledata receipt integrity owner in receipt_validation.go | Source/prune/run-creation insert, replay, permanent read; typed columns, immutable declaration/historical base and committed provenance/history/child joins | Already consumes pure aggregate owners, but different additional relational obligation, with proof; E03-E08/E10/E11/E13/E17 |
| Common durabledata selected-store owner | SQLite and PostgreSQL runtimepersistence durable_data.go entrypoints; eventpersistence source/run completion calls; runtime/bus/store.go typed ports | Already consumes; both adapters delegate to the same owner, E02-E17/E21; no backend semantic fork |
| Public operation router/union/cursor owner | API handler registration, HTTP/WebSocket dispatcher, source/prune/run-creation typed projections; CLI reads via the normal authenticated client | Already consumes; E09/E12/E15/E16/E18/N03; no extra receipt validator in the dispatcher |
| Declaration/version/provenance/pin/head/resource-read owners | Other data.show views in operator_data.go, CLI data.go, MCP read_flow_data/dataaccess, resource output/sink candidate, fork pin inheritance/override | Different semantic concept, with proof; E19/E20/E22 and existing #2510 resource proof; these are not alternate operation receipt validators |
| Fresh schema/table grammar | platformschema resource operation/evidence table declarations | Different semantic concept, with proof; E21, API spec/inventory checks Q06; DDL is not semantic receipt admission and no schema edit is proposed |
| Destructive reset retention owner | runtime/destructivereset/cleanup_catalog.go explicitly preserves source/prune/parent/child/evidence tables, separately removes run pins by run | Different semantic concept, with proof; E22/Q06; no cleanup reader reconstructs an operation result |

Reproducible census on the whole repository, not only the target file:

```sh
rg -n 'SourceOperationResult|SourceOperationRecord|SourceOperationSummary|RunCreationOperationRecord|RunCreationOperationSummary|PruneOperationResult|FusedChildEvaluation|RunCreationRequestBinding' --glob '*.go' --glob '!**/*test.go' --glob '!**/*generated*' --files-with-matches
rg -n 'resource_source_invocations|resource_prune_invocations|resource_run_creation_operations|resource_run_creation_child_|resource_prune_pin_evidence' --glob '*.go' --glob '!**/*test.go' --glob '!**/*generated*' --files-with-matches
rg -n 'LoadDataRunCreationOperation|ExecuteDataSourceOperation|LoadDataSourceOperation|PruneDataResource|LoadDataPruneOperation|ValidateRunCreationReceiptForCommand' internal --glob '*.go'
```

Typed-reference census: **19 production files**, including CLI data_file_import
and run_command (missed by a narrower record-name-only search). Table census:
**five production files**, the three durabledata owner files plus platformschema
and destructive-reset policy. Indirect event.publish entrance is additionally
classified in the execution table. No generated Go operation forwarding path
was found by the targeted generated-file sweep. Every SQL receipt writer/reader
is in the common selected-store owner; no runtime raw reader or production
bypass was found. Repeat the symbol/table/caller sweep after extraction and
integration; a missed live same-concept interpreter requires gate repair.

## Named Proof Inventory

Existing tests remain unchanged. Names below are **required future executions**,
not claims that every subcase already exists or that the current audit closes
anything. Add N01/N02 before extraction to cover missing direct branches and
precedence. If an E test lacks an intended cell, add a new direct test rather
than crediting it by name alone. All backend-required rows execute both stores.

| Proof | Exact existing test(s) or explicitly planned addition |
| --- | --- |
| E01 | internal/durabledata: TestPermanentReceiptAggregateValidatorsRejectHostileContradictions; TestCandidateVersionClosedStates; TestSourceCandidateStateAgreesWithOperationOutcome |
| E02 | internal/store: TestDurableDataSelectedStoreLifecycleReplayAndPrune; TestDurableDataConcurrentCASHasOneWinner; TestDurableDataHeadConflictEvidenceIgnoresHistoricalRetentionState |
| E03 | internal/store: TestDurableDataSourceReceiptRejectsCandidateStateCorruption; TestDurableDataSourceReceiptRejectsDefectsPageCorruption; TestDurableDataSourceReceiptRejectsAggregateCorruptionMatrix; TestDurableDataSourceReceiptRejectsTypedColumnCorruption |
| E04 | internal/durabledata: TestSourceEvaluationRejectsCoordinatedSemanticMutations; internal/store: TestDurableDataSourceReceiptRejectsCoordinatedSemanticCorruption |
| E05 | internal/store: TestDurableDataSourceReceiptRequiresExactCommitAggregate; TestDurableDataNonMutatingSourceReceiptsRejectCommitFacts |
| E06 | internal/store: TestDurableDataSourceReceiptAnchorsAdmittedContextAndHistoricalBase; TestDurableDataSourceReceiptOrdersEvaluationByRevisionNotWallClock |
| E07 | internal/store: TestDurableDataSourceReceiptsRetainHistoricalBaseAfterPrune; TestResolveDataVersionPayloadIsAtomicAcrossPruneAndRematerialize |
| E08 | internal/store: TestDurableDataPruneReceiptRejectsDestructiveDecisionCorruption; TestDurableDataPruneReceiptRejectsTypedColumnCorruption; TestDurableDataPruneValidatesCurrentAggregateBeforeHeadConflict |
| E09 | internal/apiv1: TestDataShowOperationReadFamilyAcrossSelectedStores (all source outcomes/details, all six prune outcomes including actual complete pins, created/rejected fused parent details) |
| E10 | internal/apiv1: TestRunCreationReceiptRejectsAggregateCorruptionAcrossSelectedStores; TestRunCreationReceiptRejectsTypedColumnCorruptionAcrossSelectedStores; internal/durabledata: TestFeedOnlyRunCreationReceiptValidation; TestRunCreationInitiationHasThreeClosedForms |
| E11 | internal/apiv1: TestFailedFusedRunRejectsTypedRevisionAndCoordinatedContextCorruptionAcrossSelectedStores |
| E12 | internal/apiv1: TestOperatorDataHandlersRejectContradictoryStoreResults; TestOpenRPCPermanentOperationHTTPRuntimeProbes; TestDurableDataHTTPPublicSurfaceAcrossSelectedStores |
| E13 | internal/apiv1: TestSuccessfulFusedRunRequiresExactSourceCommitAggregateAcrossSelectedStores; TestFusedRunBindingRejectsCorruptCanonicalSourceEvaluationAcrossSelectedStores |
| E14 | internal/durabledata: TestRunCreationRejectionClosedStates; TestRunCreationEnvelopeRejectsDuplicateFusedChildInvocationIDsBeforeHashing; TestRunCreationRequestBindingMetadataAndRelationships; TestRejectedTwoPinRequestBindingRetainsBothPins; internal/apiv1: TestRunCreationRequestBindingAcrossSelectedStores |
| E15 | internal/apiv1: TestDataShowValidationPrecedenceAndStoreTrace; TestDataShowCorruptReadFamilyAcrossSelectedStores |
| E16 | internal/apiv1: TestDataShowLiveCursorWireAndQueryBinding; TestDataShowPinCursorSurvivesConcurrentRunCreationAcrossSelectedStores; TestDataShowProvenanceCursorSurvivesConcurrentProducerAcrossSelectedStores |
| E17 | internal/store: TestDurableDataPruneRetainsCompletePinRefusalEvidence; internal/apiv1: TestDataShowPagesCompletePrunePinEvidenceBeyondBoundedSummary (mock-store local proof, not by itself dual-store proof) |
| E18 | internal/releasee2e: TestGoldenNumericDataScatterParkRefusalBothStores, compiled run start and actual permanent child-defect/empty-run-binding readback; N03 adds request-binding reconstruction and exact retry after retained restart |
| E19 | internal/apiv1: TestDataShowReadFamilyAcrossSelectedStores; internal/cliapp: TestDataShowCLIReadFamilyAcrossSelectedStores (real HTTP, in-process Cobra, not a compiled binary) |
| E20 | internal/apiv1: TestDurableDataRunLifecycleAcrossSelectedStores; internal/store: TestDurableDataProvenanceOwnsTypedLineageAndProjection |
| E21 | internal/store/internal/durabledata: TestDecodeFeedOnlyRunCreationReceipt; TestRunCreationDeploymentFeedPortPreservesEmptyAndMultiplePins; existing API spec/selected-store authority tests |
| E22 | internal/runtime/destructivereset: TestDefaultPlatformCleanupCatalogClassifiesEveryPlatformTable; TestDefaultPlatformCleanupCatalogMatchesPlatformSpecPolicy; inherited resource/fork integration under E20 |
| N01 (NEW, planned) | TestDurableDataAggregateValidationCharacterization: direct table over all five callable targets and every local/cross-field/outcome predicate, valid + isolated hostile variants, exact errors and no input mutation; code added and run before extraction |
| N02 (NEW, planned) | TestDurableDataAggregateValidationErrorPrecedence: pair invalid earlier/later gates per target; exact existing first error/wrapping, page nil/empty and UTC/microsecond controls; map-count multi-error ordering not invented |
| N03 (NEW, planned) | TestDurableDataOperationAggregatePublicRestartBothStores: compiled CLI -> real authenticated public operation RPC on a retained selected store -> stop/restart -> exact receipt/evidence/hash replay and fresh-process run-start reconstruction, source/prune/created/rejected details and pruned payload metadata |

N03 uses existing process/store/source admission and fixture owners, not a new
runner. Distinguish credits explicitly: real compiled public CLI and public RPC,
production serve readiness where actually used, and the existing compiled
internal MockOnly lifecycle host where retained mocked execution is needed.
An internal lifecycle host is not claimed as live-provider/public-serve proof.
No Claude/Telegram credentials, Docker provisioning, external messages or new
live-provider authorization is needed for this receipt-maintenance class.
E18's current compiled refusal and E19's in-process CLI are not credited as
N03's new retained restart/reconstructed retry proof.

## Manifestation Coverage Matrix: 48 Required Rows

All rows below are planned proof obligations. Passing baseline selections do not
mean the new characterization, extraction or supported-process rows have run.
Each row names executable proof; no row relies on 'shared owner' or 'same seam'.

| Row | Known manifestation / invariant preserved | Exact planned proof |
| --- | --- | --- |
| A01 | Source canonical invocation/bundle/declaration/schema/head identity and UTC microsecond completion | N01 source/identity plus E01/E03 |
| A02 | Accepted check has a valid uncommitted candidate and cannot change head | N01 source/check; E09 source/check/accepted/summary |
| A03 | Accepted import has a committed candidate, matching after head and correct changed/revision facts | N01 source/import; E05/E09 source/import/accepted |
| A04 | Validation rejection requires defects, rejected delta reason and unchanged head | N01 source/validation_rejected; E01/E09 check+import rejected |
| A05 | Head conflict requires unequal expected/observed heads, no defects, conflict delta and unchanged head | N01 source/head_conflict; E02/E09 source/import/head_conflict |
| A06 | Invalid outcome/operation and candidate state/manifest/schema combinations fail closed | N01 source/closed_states; E01/E03 candidate corruption |
| A07 | Command/result invocation, operation, bundle, declaration and expected head agree | N01 source/command; E03 typed-column matrix |
| A08 | Full defects agree with the bounded first page, count, byte size and continuation, including empty [] | N01 source/defects; E03 defects-page corruption; E09 defects paging |
| A09 | Keyed delta evidence is strictly sorted, complete, unique across classes and matches each count | N01 source/keyed_delta; E04/E09 added/removed/changed pages |
| A10 | Positional delta has no key evidence; uncomputed delta has no evidence; valid keyless multiplicity/order survive | N01 source/positional_delta; E01/E04 and TestDurableDataSelectedStoreKeylessPreservesMultiplicityOrderAndSchemaMode |
| A11 | Historical immutable evaluation defeats coordinated JSON mutations and typed revisions, including later head/prune | E04/E06/E07 on both stores; E09 permanent readback |
| A12 | Accepted import exact commit aggregate vs check/reject/conflict no-commit facts, replay before mutable catalog/head | E02/E05/E06 on both stores; N03 permanent source replay after restart |
| R01 | Run summary/binding canonical identity, event/feed initiation, outcome/status/counts/rejection/completion | N01 run/summary_binding; E10/E14/E21 |
| R02 | Created event-only unbound run has no imports/pins/child evidence | N01 run/unbound_created; E10/E20 |
| R03 | Created feed-only and event-plus-feed retain no-synthetic-event rules and exact complete pins | N01 run/initiation; E10/E21 and N03 feed receipt |
| R04 | Fused ready child has exact request/head/candidate/schema/delta with zero defects | N01 child/ready; E11/E13 |
| R05 | Fused rejected/conflicting child has closed candidate/delta/defect combinations; unsupported outcomes fail | N01 child/rejected_conflict; E11 and E09 rejected child pages |
| R06 | Duplicate child invocation or declaration and unsorted child declarations fail before later evidence | N01 run/child_membership; N02 run/child_order; E10/E14 |
| R07 | Defects have valid fields, known child IDs and exact per-child counts; map order is not invented | N01 run/child_defects; N02 isolated count failures; E10/E11 |
| R08 | Rejected parent has none binding, no committed items, complete child evaluations and exact rejection class precedence | N01 run/rejected_parent; E10/E11/E14 |
| R09 | Created parent forbids failed-child evidence and must agree with bounded binding page/counts/run identity | N01 run/created_evidence; E10/E13 |
| R10 | Created pin groups use exact parent status/run, allowed selection, sorted unique declarations and complete counts | N01 run/pin_groups; N02 run/group_precedence; E10/E13 |
| R11 | Fused pin has exactly adjacent accepted import with matching version/schema/bundle/declaration and unique source ID | N01 run/import_groups; E13 on both stores |
| R12 | Parent canonical request agrees with every selected import/pin, event and failed rejection target | N01 run/request; E10/E14 typed read/command cases |
| R13 | Failed children prohibit commit/source facts; created parent joins exact committed source aggregates | E11/E13 on both stores; E09 both parent outcomes |
| R14 | Payload-free request binding retains complete original early-rejected two-pin selection, exact hash and relationships | E14 both stores; E09 request_binding leak checks; N03 fresh-process reconstruction/replay |
| P01 | Prune canonical ID/declaration/version/heads/time/counts and wrapped local errors | N01 prune/identity; N02 prune/precedence; E08 typed-column matrix |
| P02 | Rejected prune contains one complete canonical defect, no pins/current/payload facts | N01 prune/rejected; E09 prune/rejected/summary+defects |
| P03 | Head-conflict prune has unequal heads and no payload/pin/current/defect side facts | N01 prune/head_conflict; E08/E09 prune/head_conflict |
| P04 | Current-version refusal has exact selected target/current version, no forbidden side facts | N01 prune/refused_current; E08/E09 prune/refused_current |
| P05 | Pinned refusal has positive count, matching summary/target and no payload/defect/current side facts | N01 prune/refused_pinned; E17/E09 actual pin refusal |
| P06 | Already-pruned and pruned have exact before/after payload states; unsupported outcome fails | N01 prune/payload_outcomes; E02/E08/E09 |
| P07 | Pin-summary local fields and target mismatch fail, including invalid lifecycle/selection | N01 prune/pin_summary; E01/E17 |
| P08 | Complete pin evidence is canonical, unique by run, full-count matched and first-page identical | N01 prune/complete_pins; E17 both stores and E09 pin paging |
| P09 | Non-pinned outcomes forbid all complete pin evidence, including nil/empty distinctions | N01 prune/no_pin_evidence; N02 prune/page_representation; E01/E08 |
| P10 | Prune defects have valid fields and a complete canonical page/continuation, not a partial summary | N01 prune/defect_page; E01/E08/E09 |
| P11 | Exact command/result, actor/flow/event typed columns and immutable pin evidence fail closed on corruption | E08/E17 both stores; E12 contradictory-return controls |
| P12 | Permanent replay precedes head/pins/payload state, survives rematerialization without re-pruning; metadata remains readable | E02/E07/E08; N03 public prune replay after restart/rematerialization |
| Q01 | Every operation kind/outcome/detail traverses real selected-store validation and authenticated HTTP | E09/E12 both stores plus N03; no fake DTO-only closure |
| Q02 | Query-bound cursors, byte-bounded complete evidence and concurrent keyset reads preserve exact public outputs | E16/E17/E09; N03 pinned/evidence pages where served |
| Q03 | Retained restart and compiled run-start permanent-request reconstruction preserve exact result/hash/facts | N03 both stores; E18 separate compiled refusal control |
| Q04 | Existing other resource views and CLI text/JSON/quiet output remain unchanged | E19 and existing compiled proof units; no invented CLI operation flag |
| Q05 | Later head/payload changes and historical terminal/fork pins do not invalidate permanent receipts | E07/E20 and N03, both stores |
| Q06 | Fresh schema, persistence authority, OpenRPC shape and cleanup retention remain unchanged | E21/E22; go test ./internal/apispec; existing persistence-authority checks |
| Q07 | Every deterministic validation phase's first error, wrapping and accepted fields is characterized before extraction | N01/N02 unchanged-baseline receipt followed by final-head receipt; map multi-error ordering excluded explicitly |
| Q08 | Disposable mutations dropping outcome checks, child/import matching, full-pin count and typed-context enforcement are detected | N01/E05/E11/E13/E17: one separately recorded representative mutation per source/run/prune family, reverted; no shipped bypass or mutation framework |
| Q09 | Five targets/all new helpers under25; exact repository ratchet and explicit diff accounting, no suppressed/shifted giant helper | Pinned per-callable tools and cmd/swarm-complexity exact committed base/head; original tests unchanged; program3000 cap |
| Q10 | Final integrated qualification and CI bind the actual rebased candidate | go run ./cmd/swarm-test, default profile; exact-head required CI; no --full unless separately required, no relaxed deadlines/omitted backend/capacity bypass |

Generic failing proof: the current pinned measurements discriminate the
maintenance acceptance condition (all five targets are not under25), while the
existing hostile aggregate/coordinated corruption suites discriminate semantic
weakening. N01/N02 make that proof direct and preserve exact baseline behavior;
Q08 verifies discrimination without shipping mutations. Do not claim a runtime
bug is fixed just because the baseline already rejects hostile inputs.

## Parent Probe, Tracker Repair And Watchlist Promotion Decision

Consulted existing nodes `maintenance-and-cleanup.boundary_owned_decomposition`,
`maintenance-and-cleanup.invariant_suite_coverage` and
`semantic-correctness.durable_resource_version_ingestion_and_run_snapshot_ownership`.
The broader nodes track owner-complete maintenance, executable proof and durable
source/run/prune authority, not a single Validate method. They suggested the
live fused-child/created-binding siblings, immutable evaluation, typed SQL joins,
CLI permanent reconstruction, fork pins and retained cleanup. All are now
classified above; the two coupled oversized pure validators are **absorbed now**.

Parent sibling probe: #2508 and #2510 are independently merge-closed, not live
follow-ups to absorb. #2509's schema and capability-subject families are distinct
owners and parked on F's #2482 merge/delta gate. Broader provider orchestration
under #2447 and startup/model enforcement under #2250/#2349 cannot honestly be
closed by this receipt extraction. Their watchlist evidence requires continued
tracking, not a universal validation or lifecycle framework.

Tracker-state decision: **current issue must be updated before coding** with
baseline6646, all five selected callables, this owner/48-row matrix, scope and
gate-pending state. The issue and parent thread must state the amendment, while
the current parent allocation remains correct. **Watchlist-only refinement is
sufficient** for this new census detail; update existing nodes, no new issue,
child, taxonomy or POTENTIAL_ISSUES.md entry. No older issue is superseded.

Post-pre-audit parent action: retain #2447/#2407 open and request first-slice
approval for this complete child family. Before this child closes, the named
tail is two containers/three families (#2509 two, #2511 one). After a successful
#2511 closure, expected named tail is one container/two families (#2509), plus
the explicitly open broader peer-provider/post-cohort census. Grouping confidence
medium; implementation effort confidence low until characterization and base
integration. This audit does not turn the named tail into a promise that only
one PR remains for all of #2447 or #2407.

No selected target production-file overlap was found in current open PRs2518(A),
2517(B),2482(F),2372 or2325, or E's inspected2496 worktree. E's construction and
readiness work is a distinct ownership lane. Shared complexity baseline, proof
registrations, API tests and spec metadata can still conflict on integration;
regenerate and requalify against the actual final base, not copy an old baseline.
#2511 does not wait for F's phone test or #2482.

## Architecture Feedback, Feasibility And Stop Conditions

Architecture smell: receipt DTOs remain representable-invalid and require
cross-field validation at construction/decode/replay/API boundaries. Smaller
functions do not remove that type-model debt. The long-run better direction is
domain-specific admitted receipt construction plus explicit persistence
evidence boundaries, evaluated under #2349 rather than a generic validator or
constructor framework. **Tracking decision: attach this concrete family census
to the existing #2349 umbrella; watchlist refinement only for the present
maintenance.** No new remediation design is chosen or implemented here.

Rough effort/ROI: this bounded extraction plus direct/public characterization
is approximately2-4 engineering days, medium confidence; low-to-medium
performance ROI (no speed claim), high review/regression-detection ROI. A later
owner-specific admitted-type migration would likely take1-2 weeks including
all decode/store/API consumers, low confidence and potentially high model-safety
ROI; it requires its own design gate and may not belong in one PR. #2349 remains
an architecture watchpoint, not an assigned fix or hidden blocker.

Closure feasibility: yes, one bounded PR can factor all five targets without
schema/SQL/transaction/lifecycle changes. Fixing only the original three would
leave the same-family28/25-callable tail; those two are included now. No other
live pure receipt interpreter is known. Chosen-class commitment is complete
maintenance closure with every A/R/P/Q row executed and independently reviewed,
not a partial extraction or parent/runtime/type-model closure.

The lead waives one-family packaging, gross1500 and net-neutral/negative limits
under #2447; keep explicit additions/deletions/net accounting and #2407's
program3000 production-line cap. Preserve all existing tests; only add
characterization/supported-proof tests. No vendoring without separate explicit
user approval; none is needed or proposed. No fallback, normalization repair,
old selected-store support, export/purge/migration, heuristic or compatibility.

Blocking ambiguity at submission: none requiring user/product input.
**Independent gate is still pending.** Stop and repair the gate if a spec
contradiction, unclassified production interpreter, required constructor/model
or ownership change, unrelated runtime defect, altered error/wire/store behavior,
unreachable proof cell or nondiscriminating mutation is discovered. Ordinary
in-scope test implementation does not require another design discussion.

## Baseline Verification Actually Run

Unchanged source6646, no production/test edits. All commands below passed:

```sh
go test ./internal/durabledata -count=1 -timeout=2m
go test ./internal/apiv1 -run '^(TestDataShowOperationReadFamilyAcrossSelectedStores|TestRunCreationReceiptRejectsTypedColumnCorruptionAcrossSelectedStores|TestRunCreationRequestBindingAcrossSelectedStores)$' -count=1 -timeout=3m
go test ./internal/apiv1 -run '^(TestRunCreationReceiptRejectsAggregateCorruptionAcrossSelectedStores|TestFailedFusedRunRejectsTypedRevisionAndCoordinatedContextCorruptionAcrossSelectedStores|TestSuccessfulFusedRunRequiresExactSourceCommitAggregateAcrossSelectedStores|TestFusedRunBindingRejectsCorruptCanonicalSourceEvaluationAcrossSelectedStores|TestOperatorDataHandlersRejectContradictoryStoreResults|TestDataShowPagesCompletePrunePinEvidenceBeyondBoundedSummary)$' -count=1 -timeout=3m
go test ./internal/store -run '^TestDurableData(SourceReceiptRejectsAggregateCorruptionMatrix|SourceReceiptRejectsCoordinatedSemanticCorruption|SourceReceiptRejectsTypedColumnCorruption|PruneReceiptRejectsTypedColumnCorruption|PruneRetainsCompletePinRefusalEvidence|NonMutatingSourceReceiptsRejectCommitFacts)$' -count=1 -timeout=3m
go run github.com/fzipp/gocyclo/cmd/gocyclo@v0.6.0 internal/durabledata/aggregate_validation.go
go run github.com/uudashr/gocognit/cmd/gocognit@v1.2.1 internal/durabledata/aggregate_validation.go
```

Package-reported test times:0.142s,5.207s,3.826s,23.876s respectively (not wall
clock or a performance comparison). Selected-store tests require and execute
SQLite/PostgreSQL; the mock-store-only proof is separately identified above.
No new N01/N02/N03, disposable mutation, extraction, exact full-repository
ratchet, full swarm-test, live-provider proof or candidate CI has run. Baseline
green is evidence of existing controls, not permission to code or closure.

Requested reviewer-g decision: verify whole-family eligibility, five targets,
owner/consumer exhaustion, all48 proof rows, preservation-first implementation
and parent/watchlist disposition; record independent **approved as first slice**
or the necessary correction on #2511. Implementation remains frozen until that
explicit issue-thread outcome exists.
