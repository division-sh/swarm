# Post-Implementation Proof Audit: #2447 Authored Event-Catalog Admission

Agent G, 2026-10-12. Approved first-slice gate:
https://github.com/division-sh/swarm/issues/2447#issuecomment-6132342528
Pre-audit: issue-2447-event-admission-preimplementation.md, original audit-only
af7feee8f on merged ab1f10dac. Implementation/code-positive proof is local;
hosted exact-head lifecycle and independent PR approval remain required.

## Class, Ownership And Measured Outcome

Category: high-risk behavior-preserving admission maintenance. The issue is NOT
an endpoint/runtime bug repair. Chosen class closed by the candidate: monolithic
authored product events.yaml admission, all21 original functions, including
document/entry/field shape, exact declaration keys, bare-event grammar,
retired-on-presence vocabulary, required-by-default optionality, business keys,
refinements/citations and exact authored/derived provenance/error ordering.

Parents #2447 keep-zone maintenance, #2407 R1.4 proof and #2349 model debt remain
OPEN. Semantic parent is event-contract admission/lowering/consumption, not
#2250 process architecture. Achieved level: whole chosen maintenance family
factored with execution proof; broader semantic/model/parent closure NOT claimed.
No known remaining same-class authored parser or in-class maintenance tail.
The observed high-score helpers were entry points, not the scope boundary.

Characterization cedc2b0c9 precedes production4c166333b. Existing maintained
tests are unchanged. Independent ab1/e26 complexity comparison passes:
cyclo>=30 255->254, cognitive>=30 552->550; maxima186/302 unchanged.
Production only event_catalog_admission.go:88 additions/54 deletions,142 gross,
net+34. User waiver, confirmed by reviewer6132342528, controls the historical
one-family/1500gross/net<=0 targets; no compression/second waiver/baseline edit.

Original entry29/45->10/11, field36/48->13/17. Five private ordered phases:
admitEventCatalogFields12/17, populateEventCatalogPayload8/13,
validateEventCatalogBusinessKey5/4, admitEventPayloadMapping13/14,
populateEventPayloadRefinements13/18. All26 resulting callables cyclo<=13.
The two high-score bodies are BOTH factored. Required errors still precede
payload/key checks exactly as before; refinement ordering remains description,
pattern,length,range,equal_to,citation. Error wrapping/zero results, original
mapping order, alias introductions, sorted required inputs and exact bytes stay.

The remaining19 original bodies are unchanged: disk convenience, source-artifact
entrance, document admission, payloadless diagnostic/shape, both provenance
functions, shared optional-marker/refusal, pattern/citation, retired nested-block
classification, duplicate/literal/optional-string/list readers, missing-value
carrier, authored provenance and derived field paths. All21 are accounted,
not merely the two hotspots. Seven shared helper source spans compare byte-equal
to ab1; no helper semantic migration or new generic validator.

Canonical owners after the change:
- event_catalog_admission.go document/entry/field chain remains the sole product
  event YAML admission owner. The new functions are private phases of it.
- yamlsource/sourceartifact retain bounded source, exact bytes/hash and locations.
- optionalDeclarationAdmission retains absent/present/liveness rules;
  eventidentity retains declaration identity; Wave1/catalog/refinement owners
  retain resolved type meaning and strict bound/equality semantics.
- populateEffectiveProvenance/EffectiveProvenanceLedger consume admitted facts;
  CompiledEventSchema remains the retained downstream acceptance authority.
- Existing public_surface_backend_matrix.yaml parity catalog binds Q03/Q04
  mandatory sqlite/postgres children through the existing planner. The runtime
  purpose references both added proof IDs; no new tier/unit/selector/framework.

## Systematic Consumption And Supported Flow

Production path is unchanged: directory source or retained artifact -> source
index/schema/nodes gates -> loadFlowContractViewFromSource -> exactly one
loadOptionalEventCatalogFromSource call -> optional-file gate -> canonical
document/entry/field phases -> resolved type/business-key/compiled binding ->
structural/boot/runtime payload admission and selected persistence/readback.
The unchanged test-only disk convenience reaches the same document owner,
but is NOT credited as another production disk parser. Public exact-tree restart
and DB-retained reconstruction without the original path are separate proofs.

| Consumer family | Disposition and execution proof |
|---|---|
| workflow_contract_tree.go actual artifact entrance | Already canonical, unchanged; C/artifact_and_disk, native Q04 and actual public Q02/Q03 |
| Owner document -> entry -> field/provenance | Same canonical owner; all F cases/six mutations and existing event/optional/bare tests |
| Test-only loadOptionalEventCatalog | Already canonical delegation, not legacy/public reader; C/artifact_and_disk distinguishes blank label, explicit missing file and empty file |
| platform_event_value.go two optional-marker calls | Already shared owner; C/platform_marker/platform_retired_required; separate nested platform grammar |
| workflow_contract_yaml_wave1.go named/entity marker/literal/refinement calls | Already shared helpers; C/type_marker/entity_marker/type_refusal/shared_refinements; separate initial/immutable/materialization grammar |
| workflow_contract_policy_rules_value.go / policy_rules_provenance.go | Already duplicate/provenance helpers, unchanged byte spans; C/shared_vocabulary and maintained optional-declaration role matrix |
| workflow_contract_tools_value.go | Already duplicate/provenance helpers; maintained declaration/current-vocabulary tests through C/shared_vocabulary; tool grammar remains separate |
| workflow_contract_yaml.go string-list reader | Already optional literal/list helpers, unchanged; C/shared_vocabulary and existing optional declaration controls |
| workflow_contract_schema_value.go | Already duplicate/provenance helpers, unchanged; C/shared_vocabulary plus both actual compiled reader/public source gates |
| workflow_contract_nodes_root_value.go / nodes_provenance.go | Already helpers, unchanged; C/provenance_ledger and structural/census guards |
| workflow_contract_agents_value.go | Already helpers, unchanged; C/shared_vocabulary and retained optional declaration role tests |
| pack_platform_provenance.go / platform_spec_value.go | Already helpers, unchanged; C/pack_provenance and platform marker controls |
| optional_declaration_admission.go all roles | Already shared duplicate/source owners; complete existing role matrix and explicit C presence tests |
| schema_registry.go / compiled_event_schema.go | Different later resolved type/binding law; C/type_lowering, connected rename/census, Q04 exact canonical compiled bytes/digests/required fields |
| effective_provenance.go event ledger consumption | Already consumes admitted facts, no source reparse; C/provenance_ledger, Q04 full ledger equality |
| Imported/provider/generated event origins | Different source grammar, not a second product parser; generated publication/binding tests and normalized exact schema/catalog/runtime controls |
| Runtime eventschema/emit/persistence | Different concrete-payload law; actual served Q03 typed refusal/settlement/no partial run or event, public Q02 and selected-root readback |

No production caller/shared helper body is edited. No currently known bypass
remains inside the chosen authored grammar. W3 raw decoders and retired metadata,
nested payload and required-list interpretation stay invalid; conformance guards
detect restoration. No export/purge/migration/compatibility path or dead reader
introduced. The test-only disk convenience remains maintained canonical test
support, not speculative backward behavior or a hidden runtime surface.

## Manifestation Coverage

F = TestEventCatalogFactoringCharacterization, C = TestEventCatalogFactoringConsumers.
All F/C exact snapshots/control cases pass in final-owner-consumers.jsonl.
P = final-supported.jsonl; E = final-equality-sibling.jsonl; G = guard receipts.
Snapshots were recorded on unchanged ab1 before extraction, then compared without
update. Positive/refused populations are separately asserted; new selected-run
mutation controls do not depend on a false incomplete-population failure.

| ID | Status | Exact execution proof |
|---|---|---|
| F01 | execution-proven through the same corrected path | C/artifact_and_disk: blank labels vs explicit missing disk/artifact and present empty file |
| F02 | execution-proven through the same corrected path | F/document_missing,null,scalar,sequence,empty exact outputs/errors |
| F03 | execution-proven through the same corrected path | F/key_empty,key_normalized,key_path and maintained canonical-key matrix |
| F04 | execution-proven through the same corrected path | F/key_pattern/key_partial_glob exact admitted identities/refusal |
| F05 | execution-proven through the same corrected path | F/duplicate_declaration/duplicate_merge_declaration exact locations |
| F06 | execution-proven through the same corrected path | F/bare; P compiled describe `(no fields)` and public bare event |
| F07 | execution-proven through the same corrected path | F/bare_mapping,description,null,tilde,quoted,tag,alias plus maintained TestPayloadlessEventRejectsNullAliasAtUse |
| F08 | execution-proven through the same corrected path | F/reserved_fields all19 names/four values; maintained TestEventMetadataRetirementRejectsPresenceBeforeValueInterpretation alias/merge controls |
| F09 | execution-proven through the same corrected path | F/retired_required/retired_payload, existing mixed flat/nested and compiled reader negatives |
| F10 | execution-proven through the same corrected path | F/business_names and maintained ordinary business-name test |
| F11 | execution-proven through the same corrected path | F/duplicate_fields/duplicate_nested; X03 controlled check removal |
| F12 | execution-proven through the same corrected path | F/scalar_types; maintained all-Wave1-shape matrix |
| F13 | execution-proven through the same corrected path | F/sequence_types,multiple,empty,optional,wrong_kind |
| F14 | execution-proven through the same corrected path | F/mapping_types,missing_type,missing_of,foreign_of |
| F15 | execution-proven through the same corrected path | F/optionality exact required/type/provenance; X01 |
| F16 | execution-proven through the same corrected path | F/invalid,double,spaced,nested_optionality; X04 |
| F17 | execution-proven through the same corrected path | F/field_unknown,null,empty/type_null; maintained wrong/missing type forms |
| F18 | execution-proven through the same corrected path | F/text_refinements/pattern_bad; exact fixed error order |
| F19 | execution-proven through the same corrected path | F/length,null,float,overflow plus C/shared_refinements all four families/both bounds |
| F20 | execution-proven through the same corrected path | F/range,null,nonfinite plus C/shared_refinements finite fractional/zero controls |
| F21 | execution-proven through the same corrected path | F/equality/equality_empty; E InvalidSchemaRefinements undeclared sibling + JSONDomainRefinements wrong peer kinds |
| F22 | execution-proven through the same corrected path | F/citation,unknown,wrong_kind exact normalization/refusal |
| F23 | execution-proven through the same corrected path | F/business_key,missing,optional,space; C/business_key_lowering dynamic semantics; X05 |
| F24 | execution-proven through the same corrected path | F/introductions and existing alias/merge introduction cases; X06 |
| F25 | execution-proven through the same corrected path | F/provenance exact RuleID/InputPaths/source coordinates and C/provenance_ledger |
| F26 | execution-proven through the same corrected path | F/first_error_retirement,field_order,refinement_order,business_key |
| C01 | execution-proven through the same corrected path | C/artifact_and_disk, P public actual bundle loader and native artifact re-admission |
| C02 | execution-proven through the same corrected path | C/artifact_and_disk + TestPayloadlessEventCanonicalGrammarDiskAndReconstructedSource; explicit internal/test-only credit |
| C03 | execution-proven through the same corrected path | C/platform_marker/platform_retired_required |
| C04 | execution-proven through the same corrected path | C/type_marker/entity_marker/type_refusal |
| C05 | execution-proven through the same corrected path | C/shared_vocabulary/shared_refinements, optional role matrix, seven unchanged helper source hashes and G complete source guards |
| C06 | execution-proven through the same corrected path | C/type_lowering/business_key_lowering; E peer/type refinement controls |
| C07 | execution-proven through the same corrected path | C/provenance_ledger/pack_provenance; native Q04 complete ledger comparison |
| C08 | execution-proven through the same corrected path | C/connected_rename + TestCompiledConnectEventConsumerProjectionCensus/EffectiveEventResolution* |
| C09 | execution-proven through the same corrected path | GeneratedPublicationSchema*/Compiled*Bindings*, ExecutableEventRolesRemainVisibleWithoutMetadata, NormalizedEventSchemaRemainsExactThroughAdmissionCatalogAndRuntime and CompiledPackPlanOwnsNormalizedOutputAuthorization |
| C10 | execution-proven through the same corrected path | TestCanonicalFormsRegistryPinsWave3EventAdmission + census restoration guards; old decoders absent |
| C11 | execution-proven through the same corrected path | OptionalDeclarationAdmission* and C/shared_refinements strict source/expansion/refusal matrix |
| C12 | execution-proven through the same corrected path | C/artifact_and_disk mutation/re-admission and byte equality; Q04 exact stored/rebuilt event map/ledger/compiled schema |
| Q01 | execution-proven through the same corrected path | P TestEventCatalogFactoringCompiledReaders actual verify/describe positives and both canonical/required-list refusals |
| Q02 | execution-proven through the same corrected path | P TestPayloadlessEventPublicPersistenceJourneyBothStores sqlite/postgres: publication, terminal readback, exact source bytes/blob hash and relocated same-tree serve restart |
| Q03 | execution-proven through the same corrected path | P TestEventCatalogFactoringRefinedPublicationBothStores sqlite/postgres: real served valid payload, four typed PAYLOAD_VALIDATION_FAILED cases, unchanged run inventory and exact stored event list |
| Q04 | execution-proven through the same corrected path | P TestEventCatalogFactoringRetainedArtifactBothStores sqlite/postgres: close/reopen exact DB, original source path absent, GetSourceArtifact/Decode real persisted bytes, re-admit and compare compiled bytes/digest/required fields plus event map/provenance |
| Q05 | execution-proven through the same corrected path | P TestSelectedRootEventBoundarySQLitePostgres both stores; owned internal mock lifecycle + actual compiled/HTTP/WS readers, NOT live agent startup |
| X01 | execution-proven through the same corrected path | required mutant compiles, F/optionality fails3/3, restored control PASS |
| X02 | execution-proven through the same corrected path | retirement mutant compiles, F/reserved_fields/swarm/null fails3/3, restored |
| X03 | execution-proven through the same corrected path | duplicate mutant compiles, F/duplicate_fields fails3/3; ambiguous automated restore caught and manually corrected to exact committed source before next mutant |
| X04 | execution-proven through the same corrected path | marker mutant compiles, F/invalid_optionality fails exact error3/3; later type rejection is not substituted for its original diagnostic; helper restored |
| X05 | execution-proven through the same corrected path | business-key mutant compiles, F/business_key_optional fails3/3; restored |
| X06 | execution-proven through the same corrected path | provenance mutant compiles, F/introductions fails exact type-source coordinates/presence3/3; restored |

No row relies only on shared-owner prose or loose identity membership.
Q04 is internal native reconstruction, not public process restart; Q02 supplies
the separate real public restart. No paid provider/Docker/soak/local full credit.

## Receipts, Qualification And Remaining Risk

Evidence directory: /home/youmew/.local/state/agent-g-2447-event-admission-20261012.
Raw outputs and failed controls are retained, not relabelled. Code/test qualified
head e26f2b1c9493e4f1ce9701b333ad38887945b313; cb4788dea adds only the runtime
purpose's two proof refs; e3c6e16e3 changes only audit wording. Metadata-sensitive
matrix/planner controls were rerun on corrected cb; complete structural unit
rerun on e3c. No fabricated new-head execution or whole-tier local receipt.
The final audit-only commit will preserve all relevant implementation/test/data
bytes. Hosted lifecycle must qualify the actual pushed final head.

| Receipt | Passing test/root count | SHA256 |
|---|---:|---|
| characterization-final-before-extraction.jsonl | 4158/6 repeated | c43b05cab29716df987a1955639d406a501ee5efb767fb26033abf53f478f6c0 |
| final-owner-consumers.jsonl, race3 | 5703/147 repeated | 3bab9fd33ded04eb970004730c86adc3e28bad025937e0fde36a536f7ea07a79 |
| final-supported.jsonl | 14/6 | 716d30d2b46db01e3bb3322a9fdd4182496fa71dc9abe9912c4b31e0109a2fff |
| final-equality-sibling.jsonl, race3 | 6/6 | 875cb787c27f72699246ef25b1db5e00aefd9aa124144790a7f549d2b35156c6 |
| final-matrix-correction.jsonl | 210/24 | 41136ea3e3930473008d8d8f5c680dd73e7fc5927073f70b5d845a3b43771b73 |
| core-structural-corrected.jsonl | 442/81 | f130bb9b23b6aed8e27f6780269b3647831bc758f151ae95f0789a0278c74fba |
| final-guard-complement.jsonl | 9/9 | 41f014bf6698b4624d3c2eb911ce09e0854bd61e8374cf0905580f4c472b9fb5 |

Scoped eight-package census/registry/static/ownership/authority aggregate
final-census-guards.jsonl is FAILED due solely to the then-unreferenced parity
entries;1190 instances/266 roots pass across remaining controls, no skip.
SHA256 fe9ba35fca6adad8d4204655a440bcb9389f711dba6a84ca6f8f53661eb36bc0.
API matrix corrected through its canonical runtime purpose, not an exemption;
fresh matrix/planner210/24 PASS. Store authority registry and exact debt ratchet
PASS: actual10688 == inherited10688, no classification/baseline increase.
All34 file-defined contracts guard/static roots ran:29 in census plus5 named
complement; four additional managed-effect/compute primitive controls pass.
Complete81-root core structural unit PASS, not a whole core-tier claim.

Retained earlier reds: new fixture initially mistook explicitly missing disk
path for absent optional label, corrected before production; parity refs lacked
consumption, repaired; a private provenance-type spelling collided with the
retired transport prose scan, replaced by the precise canonical ledger owner.
No maintained test/guard/assertion weakened. Ruby alias loading and planner flag/
initial source-coordinate setup mistakes are NOT qualification; the verified
plan is generated afresh from actual Git HEAD, never the discarded coordinate.
All six mutants fail their named leaf3/3 without build failures and restore;
no mutant is shipped. Their individual JSON receipt hashes are retained next
to the controls. Independent complexity final-complexity/{base,head,delta}.json
compares exact Git snapshots, without a baseline/policy change.

Verified final-lifecycle-plan.json source e3c6e16e3bdebcfae91a6f34df60ffb32527cc84,
linux/amd64,cgo1,gowork empty,go1.26.8: every five new roots has exactly one
owner. F/C -> contracts-first, retained Q04 -> broad-05, compiled readers/Q03
and existing Q02 -> hitl-releasee2e-rest. Q02-Q04 all require sqlite/postgres
children. No new unit/tier/selector/retier/timeout or CI-Units. CI-Tier lifecycle
under approved gate; local focused only, no duplicate whole local tier/server2.

Watchlist decision: refine existing boundary_owned_decomposition and
invariant_suite_coverage; gate/waiver correction already published docs d15b23ac.
Candidate implementation/proof checkpoint will retain all parents open. No new
issue/node/POTENTIAL_ISSUES or #2250 architecture expansion. Existing #2353
unexplained R5 variance remains separate, not fixed/waived by these receipts.

Parent sibling probe checks platform/named/entity grammars, shared refinement/
provenance, connected/imported/generated schema and concrete runtime admission.
These are separate authorities or already canonical shared helpers, not another
live authored parser. Remaining parent tail: schema projection, tool-entry,
catalog/type admission and provider continuations/Responses SSE; grouping
confidence medium, effort/PR-count low. This extraction introduces no new tail.
Cycle1 integration refresh: #2624 has MERGED at0bfc09a2f and is consumed by
the single signed rebase. The only text conflict was the runtime-purpose proof
ID list: preserve all seven process-generation IDs plus both event-catalog IDs.
All #2624 catalog entries, startup capability evidence/table changes and retired
authority-maintenance removal match master; no proof is replaced or resurrected.
Authored admission, characterization, native retained and public proof file bytes
match reviewed8aa9592e0 exactly. The production source entrance still calls
loadOptionalEventCatalogFromSource once. Process-generation publication/join is
a distinct surrounding authority; public Q02/Q03 and native Q04 require fresh
integrated execution rather than receipt carry. #2614/#2616/#2617 remain open;
#2616 at7933779be and #2617 at0f039defa share only parity metadata. Refreshed
#2637 at7624a5622 has no production/new-test overlap. Prior source coordinates
and receipt table above remain historical; integrated receipts are added below
only after actual execution. No whole local tier or hosted pass is inferred.

Model feedback: mutable admission intermediates are distinct from retained
compiled acceptance authority; opaque construction remains open #2349. Tracking
decision: existing issue/watchlist only, no redesign here. Refactoring's ROI is
explicit ordered refusal/provenance phases and executable counterexamples at
142 gross/net34 production cost, not runtime throughput. Model-redesign effort
is uncertain without its own census; one or more cross-family PRs, no invented
estimate or closure promise. Exact-head independent review and hosted lifecycle
remain the final merge gates; no merge approval is claimed in this audit.
