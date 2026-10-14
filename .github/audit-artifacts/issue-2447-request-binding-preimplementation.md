# #2447 request-binding family: characterization and extraction boundary

Coding ruling: #2447 comment 6168626666. Integration base:
7c689f2eecac9f9c7a4e53700a5df336cbd35865. This is one complete
three-function family in internal/durabledata/request_binding.go, not the
#2511 operation-result validators or #2349 model redesign. Parent #2447,
#2407 and #2349 remain open. No grammar, wire, hash, canonical owner,
store/API/CLI implementation, plan, registry or baseline change is authorized.

## Complete function and consumer table

| Function | Current cyclomatic/cognitive | Ordered responsibility | Direct live caller | Selected lifecycle owner and proof |
| --- | --- | --- | --- | --- |
| BindRunCreationRequest | 15/22 | Command hash/canonicalization; record-for-command validation; created or ordered rejected import metadata; copied full pins; binding-for-record validation. | Store internal/durabledata.LoadDataRunCreationOperation, inside its selected read transaction after validateStoredRunCreationReceipt returns the retained command and record. | broad-08 pure R09/R10 and existing TestRejectedTwoPinRequestBindingRetainsBothPins; api-llm-bus-full TestRunCreationRequestBindingAcrossSelectedStores and corrupt-receipt roots. |
| RunCreationRequestBinding.Validate | 31/37 | Run/bundle/event/hash; combined bound/initiation; ordered import fields/sort/source uniqueness; ordered pin fields/sort/overlap. | ValidateForRecord and CLI loadRunCreationRequestBinding. | broad-08 R01-R04/R10; catalog-required-verify CLI package; hitl-releasee2e-rest compiled retry/restart. |
| RunCreationRequestBinding.ValidateForRecord | 38/56 | Standalone validation; record validation; summary; created/rejected facts; ordered created import/pin or rejected target relationship. | Builder and API executeDataShowRunCreationOperation(request_binding). | broad-08 R05-R08/R10; api-llm-bus-full public selected-store created/rejected/corrupt readback; hitl-releasee2e-rest permanent public restart. |

These three functions are the complete current file. Only the two validators
will be extracted into same-package helpers. The builder need not move for
symmetry. The DTO remains metadata, not the final replay equality authority.
Complete retained canonical request/hash equality stays in the selected store.

| Transitive consumer | Facts consumed and refusal obligation | Execution proof / lifecycle selection |
| --- | --- | --- |
| Store LoadDataRunCreationOperation -> validateStoredRunCreationReceipt -> BindRunCreationRequest | Read-time validated retained command and receipt, no guessed rejected pins. | TestRunCreationRequestBindingAcrossSelectedStores; TestRunCreationReceiptRejectsAggregateCorruptionAcrossSelectedStores; TestRunCreationReceiptRejectsTypedColumnCorruptionAcrossSelectedStores, api-llm-bus-full. Store feed-only sibling TestDecodeFeedOnlyRunCreationReceipt, broad-02. |
| API executeDataShowRunCreationOperation | Nil binding refuses; validates binding against record before presenting payload-free metadata. | Same three API roots; TestDataShowSelectedStoreAllViewsAndOperationDetailsAcrossSelectedStores where present is a sibling, not inferred qualification. Exact selected roots will be copied from the final bound plan. |
| CLI loadRunCreationRequestBinding | Only OPERATION_MISSING means absence; every other RPC error propagates; standalone validation and exact requested run ID. | catalog-required-verify; compiled same-run retry controls are required before push (not yet credited). |
| CLI runCommandStart, initial binding load | Original bundle and event/data initiation; reconstructs the retained request rather than today's head. | TestDurableDataOperationAggregatePublicRestartBothStores, hitl-releasee2e-rest, created and rejected CLI retries after head movement and restart. |
| CLI runCommandStart, conflict recovery load | Exact recovered bundle/initiation; schema binding; original head/pins; retained command is the last equality owner. | Compiled retry mismatch/head and non-absence controls required before push, not JSON-only assertions. |
| CLI buildRunDataEnvelope -> validateRunDataBindingSchemas and rebindRunDataEnvelope | Original source identity/ref/schema/CAS, ordered complete import/pin set, exact explicit pin unless selector was head. | Same compiled retry/restart family; pure file-lowering tests are siblings, not substitute replay proof. |
| RunCreationOperationRecord.RequestBinding | Transient read projection excluded from record JSON, no second builder/validator. | R10 wire/detachment and selected-store/public readback. |

The live symbol census covers internal/**/*.go excluding tests, including every
RunCreationRequestBinding, BindRunCreationRequest, RequestBinding,
loadRunCreationRequestBinding and rebindRunDataEnvelope reference. No second
production builder or public binding interpreter was found. No concrete
full-only binding manifestation has been established; provisional hosted tier
is lifecycle, no CI-Units. Broad packing names above come from the actual
generated lifecycle plan on this base, not inferred alphabetical placement.

## R01-R10 characterization and manifestation table

All roots below are in the separate request_binding_characterization_test.go,
written and executed before production extraction. Literal diagnostics and
independent edits retain the current first-error order. Existing tests stay
unchanged.

| Row | Exact root | Positive and independently failing controls |
| --- | --- | --- |
| R01 | TestRequestBindingR01Identity | Canonical metadata; absent/zero/uppercase run/event, weak/noncanonical bundle, wrong/short/uppercase/nonhex request digest. |
| R02 | TestRequestBindingR02InitiationAndBound | Event-only/data-only/combined; no initiation; exact 256/257 import and pin limits; combined limit preceding malformed entries. |
| R03 | TestRequestBindingR03Imports | Sorted distinct imports; source/ref/schema/head validation, absent-version head, descending/duplicate refs, distinct refs sharing source ID. |
| R04 | TestRequestBindingR04Pins | Disjoint explicit pin; invalid ref/version, descending/duplicate pins and import/pin overlap. |
| R05 | TestRequestBindingR05CreatedSummary | Valid combined creation; changed run/bundle/import count/event/pin count and incomplete aggregate evidence. |
| R06 | TestRequestBindingR06CreatedRelationships | Exact imports/pins; changed source/schema/head, absent requested pin, wrong explicit version, reversed fused/explicit selection. |
| R07 | TestRequestBindingR07RejectedChildren | Ready child retained under explicit rejection; each source/ref/schema/head/bundle mismatch; ordered two-child control and swapped sources; missing/unsorted child evidence refused by the earlier record owner. |
| R08 | TestRequestBindingR08RejectedPins | Full canonical two-pin rejected request despite empty committed evidence; missing target and wrong target version. |
| R09 | TestRequestBindingR09BuilderAuthority | Hash and canonical selections, command error before bad record, record error before projection, changed requested CAS, changed actor changes hash, event-only nonnil empty arrays. |
| R10 | TestRequestBindingR10WireDetachmentAndPrecedence | Detached pins/import metadata; exact six-field eventful top-level wire, payload/actor absent; standalone before record, record before summary, summary before relationships, created summary before imports, identity before hash, imports before pins. |

Record validation already excludes invalid outcome/evidence unions and rejected
child-count contradictions. Later defensive guards are not falsely credited as
independently reachable with an otherwise valid record. R07 retains a literal
earlier-owner refusal and the valid two-child relationship control.

## Evidence checkpoint and remaining qualification

Before extraction, all R01-R10 pass race x3 (zero failed/skipped records).
The first development run exposed only two new test-fixture mistakes: an
uncommitted candidate must have no alias and the eventful DTO has six top-level
fields. Those were corrected without any production or old assertion edit.
No correctness counterexample against the existing owner was established.
This checkpoint is characterization, not closure or hosted qualification.

Before push: complete durabledata package, focused race and discriminating
omissions; the named dual-store API created/rejected/corrupt roots; real compiled
CLI retry/reconstruction including mismatched intent/head/non-absence refusal;
compiled public restart; affected census/guards, debt, complexity, native Linux
unused and clean preflight. The existing restart root is actual compiled replay
after head advancement, but does not alone prove the required CLI negative
controls. Those remain explicit open proof, not claimed credit. No local tier
or server2 booking is planned.

#2617 priority is retained. Its published donor paths on the current inspected
source do not overlap this production family or the named API/CLI callers.
The actual latest integration head and merge-tree will be checked again before
qualification/review; branch age is not evidence of a semantic conflict.
