# Pre-Implementation Coverage Audit: Complete Authored Event-Catalog Admission

Agent G, 2026-10-12. Independent coding gate approved as first slice:
https://github.com/division-sh/swarm/issues/2447#issuecomment-6132342528
The approved conditions supersede the original pending/target wording below.
The user previously waived one-family/1500-gross/net<=0 limits; they are targets,
NOT stop conditions. Exact behavior/shared helpers/tests/owner proof,
characterization-before-extraction, all changed/new callables<25 and the
independent complexity ratchet remain binding. No vendoring is authorized.
Source: merged master `ab1f10dac2db203eae9d77382a8962f540081c8c`, Go1.26.8.
Read IMPLEMENTER_GUIDELINES.md and SEMANTIC_DRIFT.md; re-read the issue's
current/historical scope and binding first-slice conditions. No production,
test, spec, plan, registry, baseline or workflow change in this checkpoint.

## Class, Binding Context And Parent Decision

- Category: high-risk, behavior-preserving semantic-admission maintenance.
- Observed symptom: authored event entry and field admission interleave shape,
  reserved-vocabulary refusal, type/optionality, refinements, business-key checks
  and diagnostic provenance. Current cyclo/cognitive entry29/45 and field36/48
  are the maintenance entry points, NOT the audit boundary. No new runtime bug
  is asserted; existing unchanged branch controls pass.
- Exact concepts: flat product `events.yaml` declaration grammar; exact
  declaration-key admission; canonical bare/payloadless form; required-by-default
  field-level optionality; field/refinement/citation admission; business-key
  syntactic eligibility; authored/derived provenance and first-error ordering.
- Chosen working class: monolithic authored event-catalog source admission,
  comprising ALL21 existing functions in event_catalog_admission.go, 613 file
  lines. Account for each, not just the two current hotspots. Private ordered
  phases only; no second validator, exported carrier, generic framework or
  move of semantic ownership.
- Immediate maintenance parent: oversized keep-zone admission families, #2447.
  Broader proof parent: #2407 R1.4. Broader model debt: open #2349. The semantic
  parent is complete event-contract admission/lowering/consumption, NOT #2250's
  process/transaction architecture. No universal event-schema closure claimed.
- Framing: this full family is broad enough as a proposed first slice, subject
  to independent approval. Absorbing all event lowering, runtime wire admission,
  cross-flow binding, named-type/entity grammar and shared optional-file readers
  would mix separate authorities and overlap active work. Parent action: retain
  first-slice scope; #2447/#2407/#2349 stay OPEN. No new issue or supersession.

Binding authoritative clauses exist (line numbers at this source):

1. platform-spec.yaml `contract_formats.event_schema`, L1091: flat event payload,
   retired metadata on presence, canonical type-aware lowering, required-by-default
   and exactly one complete-type trailing `?`, derived presence before consumption.
2. `contract_formats.event_schema.payloadless_declaration`, L1152, and
   `contract_formats.canonical_form_registry.payloadless_event`, L1084: ONLY bare
   `event.name:`; precise authored-variant refusal, `(no fields)` describe,
   exact byte/hash persistence and public same-tree restart on both stores.
3. `type_system.schema_refinements`, L2876: closed pattern/length/range/equal_to
   vocabulary, strict source kinds and bounds, same-object typed equality.
4. `flow_model.cross_flow_events.declaration_identity_admission`, L14126,
   and `flow_model.compiled_event_schema_projection`, L14136: exact authored
   key identity before insertion; retained compiled schemas for downstream
   interpretation, diagnostic provenance separate from identity.
5. #2447 issue/thread: one complete separately gated keep-zone family; target
   <=1500 gross changed-production lines, net<=0, changed/new callables<25,
   unchanged maintained tests, pre-extraction characterization and independently
   measured ratchet. Reviewer6132342528 confirms the user's earlier size/net
   waiver controls; the first three are targets, not additional merge gates.

No spec delta is proposed for pure extraction. A discovered contradiction,
changed error/accepted value, bypass or required semantic repair stops coding
for issue/gate repair; it is not characterized as intended behavior.

## Complete Owner Inventory

Pinned upstream gocyclo0.6.0/gocognit1.2.1 on the exact owner file:

| Existing callable | Cyclo / Cognitive | Role / disposition |
|---|---:|---|
| loadOptionalEventCatalog | 4/3 | Test-only disk convenience; delegates to same document owner, NOT production-loader credit |
| loadOptionalEventCatalogFromSource | 4/3 | Actual production artifact entrance; keep |
| admitEventCatalogDocument | 9/9 | Root shape, duplicate declarations and canonical keys |
| admitEventCatalogEntry | 29/45 | Factor complete ordered entry admission |
| payloadlessEventDeclarationError | 1/0 | Exact diagnostic; preserve |
| admitEventDeclarationShape | 12/10 | Bare/null/empty/alias/tag spelling authority |
| populateEventPayloadFieldAdmissionProvenance | 10/13 | Authored field/refinement provenance |
| populateEventNestedAdmissionProvenance | 7/7 | Nested bounds/citation introductions |
| admitEventPayloadField | 36/48 | Factor all scalar/list/mapping forms and ordered refinements |
| admitEventFieldTypeMarker | 5/5 | Shared field-level optionality authority; preserve |
| rejectEventTypeOptionalMarker | 2/1 | Shared complete-type marker refusal; preserve |
| admitEventPattern | 3/2 | Compile pattern at admission |
| admitEventCitation | 6/6 | Closed criteria/class metadata |
| eventPayloadValueIsRetiredNestedBlock | 11/13 | Presence-based old nested-block refusal |
| uniqueYAMLMappingFields | 4/4 | Shared duplicate-effective-field owner; preserve |
| eventFieldValue | 2/1 | Explicit missing-value carrier |
| requiredLiteralString | 4/3 | Shared literal source reader; preserve |
| optionalScalarString | 4/3 | Shared optional source reader; preserve |
| optionalStrictStringSequence | 4/3 | Shared strict metadata list; preserve |
| authoredSourceProvenance | 1/0 | Shared introduction-location projection; preserve |
| eventFieldTypePaths | 2/1 | Deterministic derived provenance inputs |

These are real semantic/source owners: production flow loading invokes this
document/entry/field chain; the conformance registry expressly names the W3
owner and bans raw YAML decoding. Complexity alone is not ownership evidence.
The other relevant unchanged owners are yamlsource expansion/location,
sourceartifact exact bytes/hash, optionalDeclarationAdmission presence/liveness,
eventidentity.IsCanonicalDeclaration, validateWave1TypeRef/catalog resolution,
strict decodeSchemaLengthRefinementValue/decodeSchemaRangeRefinementValue,
effectiveProvenanceBuilder and retained CompiledEventSchema projection.

## Full Execution Paths And Gates

Directory CLI verify/describe or public serve -> sourceartifact.AdmitDirectory
-> bounded parsed exact artifact -> indexFlowSources ->
LoadWorkflowContractBundleFromArtifact -> sorted per-flow schema admission ->
loadFlowContractViewFromSource -> nodes -> loadOptionalEventCatalogFromSource
-> optional declaration presence/shape/liveness -> admitEventCatalogDocument
-> canonical declaration key -> admitEventCatalogEntry -> bare/retired/duplicate
checks -> business-key text -> field type/optionality/refinements/provenance ->
sorted required/provenance inputs -> syntactic key checks -> flow-tree compilation
-> resolved type/business-key compatibility -> effective compiled schema binding
-> boot/structural readers or executable payload admission -> persistence/readback.

Retained reconstruction: exact persisted source_artifact -> admitted artifact
-> SAME LoadWorkflowContractBundleFromArtifact and event-source chain -> retained
compiled schema. Public exact-tree restart and internal retained reconstruction
are distinct proof surfaces, not interchangeable claims.

| Gate in execution order | Classification / proof |
|---|---|
| Public flags/config/store/source selection | Different concept: selects bytes/location before contract loading; compiled CLI proof includes it, not edited |
| Sourceartifact/YAML whole-document expansion and alias admission | Different concept: generic bounded source ownership before optional-file reader; alias/source parity controls required |
| Source indexing and preceding schema/nodes load | Different concept: flow topology and node/schema grammar; valid fixtures must pass before event entry is reachable |
| Optional events file presence/root/liveness | Same chosen family composition: explicit empty file differs from absent label; preserve existing delegated owner/errors |
| Document keys, entry shape, retirement, field grammar, provenance and key syntax | Same chosen class; each has distinct F rows below |
| Resolved named types and business-key scalar/dynamic meaning | Different concept: validateWave1TypeRef and later bundle/type-aware compatibility; preserve integration C rows, no direct-map shortcut |
| Effective compiled producer/receiver schema and role liveness | Different concept: compiled event ownership; connected/pattern/provider controls, not raw authored runtime lookup |
| API actor/run authority, store event receipts, dispatch and settlement | Different concept: runtime/publication ownership; actual public both-store proof retains those gates without claiming them extracted |
| Persisted bytes/hash and exact-tree restart | Different concept: source identity/lifetime; public proof must check bytes/hash and terminal readback |

Nothing can fix a final projection error by bypassing an earlier source or
compiled-owner refusal. Negative fixture cases must demonstrate the intended
gate, not die at an unrelated malformed schema/node or public flag.

## Exhaustive Systematic Consumption And Sibling Probe

Repo-wide production census at ab1f10dac; caller search includes all Go files,
then separately classifies _test.go. No live alternate product event YAML
decoder or direct production caller of the disk convenience was found.

| Seam / named execution | Status / owner consumption |
|---|---|
| workflow_contract_tree.go:loadFlowContractViewFromSource -> loadOptionalEventCatalogFromSource (one production call) | Already consumes canonical owner; directory and artifact loaders converge before this call |
| Document -> entry -> field/provenance in owner file | Already consumes canonical owner; whole ordered family extracted in this work if gated |
| loadOptionalEventCatalog disk path in optional/payloadless/current-vocabulary tests | Already consumes document owner, TEST-ONLY convenience; not a production reader or legacy fallback |
| platform_event_value.go:admitPlatformEventFieldValue, two marker calls | Already consumes shared admitEventFieldTypeMarker; platform catalog's nested payload grammar is different, evidenced by admitPlatformPayloadValue recursion |
| workflow_contract_yaml_wave1.go:decodeWave1FieldValue, two marker calls and sequence refusal | Already consumes shared optionality/refusal/literal owners; named-type/entity fields have initial/immutable/materialization grammar, not product event-entry grammar |
| workflow_contract_policy_rules_value.go | Already consumes uniqueYAMLMappingFields for policy/rules/inputs/classes/params; different declaration grammar, preserve helpers unchanged |
| workflow_contract_tools_value.go | Already consumes uniqueYAMLMappingFields and authoredSourceProvenance for declarations/response/text mappings; different tool execution grammar |
| workflow_contract_yaml.go | Already consumes optionalScalarString/optionalStrictStringSequence for string lists; different projection concept |
| workflow_contract_schema_value.go | Already consumes unique mapping/provenance helpers; different schema topology grammar |
| workflow_contract_nodes_root_value.go / nodes_provenance.go | Already consumes duplicate/provenance owners; different node syntax and lifecycle |
| workflow_contract_agents_value.go | Already consumes unique/provenance helpers for agents/entity_writes; different agent grammar |
| workflow_contract_policy_rules_provenance.go | Already consumes authoredSourceProvenance; policy value truth is different |
| pack_platform_provenance.go / platform_spec_value.go | Already consume provenance/unique helpers; platform law and pack provenance are different source contracts |
| optional_declaration_admission.go | Already consumes duplicate owner across all roles; same events presence gate, other roles separately owned |
| schema_registry.go:eventSchemaFromCatalogEntry / compiled_event_schema.go | Different semantic concept: lower resolved catalog fields and bind immutable acceptance schema; consume already-admitted entries, cannot reread YAML |
| effective_provenance.go:populateEffectiveProvenance | Already consumes admitted event provenance via canonicalCurrentEventDeclarationRecords; no source reparse |
| providertriggers normalized/imported entries, generated activity schemas, connected receiver projections | Different origin/lowering law, not authored event YAML; composed compiled-schema sibling controls required, F2614 integration remains explicit |
| runtime/eventschema payload acceptance, engine/bus/tool emit consumers | Different semantic concept: concrete value/compiled-schema admission, not authored syntax; compiled public publication proves integration |

For shared utility consumers the above is the complete production FILE set,
not a claim that those other function families are being refactored. Shared
helpers stay unchanged; any need to alter them widens affected proof and stops
for an amendment. The file-local eventFieldValue/eventFieldTypePaths helpers
have no external production callers. No unclassified bypass or "same seam"
execution-equivalence assertion is used.

Removal decision: the old W3 EventCatalogEntry/EventFieldSpec raw YAML decoders
remain retired; conformance negative controls must detect restoration. Retired
metadata/nested payload/required-list syntax remains refused. No migration,
compatibility or old-store path. The disk convenience is test support that
delegates to the canonical grammar, not a duplicate interpreter. Its eventual
removal is worth consideration but would require replacement of maintained
disk/absent-file assertions; this proposal preserves existing tests and does
not ask for an exception merely to gain lines. No dead semantic producer is
being preserved for hypothetical users.

## Planned Manifestation And Proof Matrix

ALL49 rows below are PLANNED extraction proof, NOT completed proof. New direct
characterization roots `TestEventCatalogFactoringCharacterization` (F) and
`TestEventCatalogFactoringConsumers` (C) must be committed/passing before any
production edit. They assert exact outputs/refusals/location/order, not merely
substrings. Existing maintained tests remain unchanged. Q roots use actual
compiled commands/native owners; no new raw SQL or internal release imports.

| ID | Manifestation | Exact planned execution/oracle |
|---|---|---|
| F01 | Missing/absent/present optional declaration | F/optional_presence plus existing OptionalDeclarationAdmission matrix, preserve nil/empty/error distinction |
| F02 | Document missing/null/scalar/sequence/empty mapping | F/document_shapes through document and actual artifact entrance |
| F03 | Canonical concrete keys | F/concrete_keys, exact key/location and no insertion on refusal |
| F04 | Supported whole-token/segment patterns vs partial glob | F/pattern_keys, preserve exact admitted map keys |
| F05 | Duplicate and merge-expanded colliding declarations | F/duplicate_declarations, both source locations and first error |
| F06 | Canonical bare payloadless event | F/bare, exact empty properties/required/declaration provenance |
| F07 | Empty mapping/quoted text/explicit null/alias/tag spellings | F/noncanonical_bare, exact authored-variant message |
| F08 | Reserved metadata null/empty/alias/merge presence | F/reserved_fields, preserve retirement before interpretation |
| F09 | Retired required list and nested payload block | F/retired_payload, mixed-shape first error |
| F10 | Ordinary payload/source/status/note names | F/business_names, no metadata reinterpretation |
| F11 | Duplicate effective entry fields and nested mapping | F/duplicate_fields, ordered duplicate/refinement refusal |
| F12 | Scalar primitive/named/list/map field types | F/scalar_types, exact admitted type and source carrier |
| F13 | One-element sequence list vs empty/multiple/wrong kind | F/sequence_types, no element-optional coercion |
| F14 | Mapping type/list-of/missing or foreign of | F/mapping_types, exact field diagnostics |
| F15 | Field-level optional marker across all forms | F/optionality, sorted required and derived/authored provenance |
| F16 | Interior/repeated/whitespace optional marker | F/invalid_optionality, no acceptance broadening |
| F17 | Unknown field/null/empty type and refinement keys | F/field_presence, exact undefined-field vocabulary |
| F18 | Description and pattern | F/text_refinements, trim/regex admission/error order unchanged |
| F19 | Length kind/bounds/presence | F/length, shared strict integer reader incl zero/overflow/fraction/null |
| F20 | Range kind/bounds/presence | F/range, shared finite numeric reader incl negative fractions/zero/null |
| F21 | equal_to source spelling plus later peer/type refusal | F/equality and C/type_lowering, no source-level typed-peer shortcut |
| F22 | Citation criteria/classes/unknowns | F/citation, normalization/order and exact diagnostic |
| F23 | Business-key text/whitespace/undeclared/optional/type | F/business_key, exact syntactic error precedence |
| F24 | Alias/merge introduction locations | F/introductions, file/line/column/presence for declaration and nested values |
| F25 | Required/type/refinement/citation derived provenance | F/provenance, exact RuleID/InputPaths and sorted required paths |
| F26 | Simultaneously invalid entry/field gate ordering | F/first_error, source-ordered field failures and fixed refinement order |
| C01 | Actual artifact entrance and public loader composition | C/artifact, same admitted map + source/provenance through bundle loading |
| C02 | Test-only disk convenience vs artifact parity | Existing PayloadlessEventCanonicalGrammarDiskAndReconstructedSource + C/disk_parity; no production-disk credit |
| C03 | Platform shared optionality | Existing PlatformEventCatalogUsesRequiredByDefaultTypedOmission/RejectsRetiredRequiredList + C/platform_marker |
| C04 | Named-type/entity shared optionality | Existing TypeCatalogDocumentDecode_PreservesNamedFieldOptionalityAndMergedMappings/RejectsInvalidOptionalMarkers and EntityContractsDocumentDecode_PreservesOptionalTopLevelField + C/type_entity_marker |
| C05 | All other shared helper consumers | Existing optional/strict-vocabulary/tool-schema/platform/node/policy tests plus complete unchanged-helper source comparison; C/shared_helpers asserts relevant literal/duplicate/provenance results |
| C06 | Resolved type/refinement and dynamic business-key boundary | Existing EventCatalogBusinessKeyRejectsDynamicSemanticsAtBundleAdmission/EventSchemaRegistryFromBundle_PreservesWave1TypeMeaning + C/type_lowering |
| C07 | Effective provenance isolation/composition | Existing NodeAdmissionProvenanceComposesWithEvents/PackPlatformProvenanceComposesWithoutOverwrite + C/provenance_ledger |
| C08 | Compiled producer/receiver ownership and renamed inputs | Existing CompiledConnectEventConsumerProjectionCensus/EffectiveEventResolutionPreservesConnectedRenameOwnership; no raw authored runtime fallback |
| C09 | Imported/provider/generated source origins | Existing GeneratedPublicationSchema*/ExecutableEventRolesRemainVisibleWithoutMetadata and provider normalized schema controls; inspect F2614 integration before final qualification |
| C10 | Retired custom decoders cannot return | Existing CanonicalFormsRegistryPinsWave3EventAdmission and its restoration negative guards |
| C11 | Whole-document alias/expansion/refinement reader boundaries | Existing OptionalDeclarationAdmission*/SharedRefinementBoundPresenceMatrix; C/bounded_source no off-owner coercion |
| C12 | Mutating admitted/readback maps cannot change source bytes or other projections | C/nonmutation through fresh artifact re-admission and exact canonical schema/provenance comparison |
| Q01 | Compiled verify/describe valid/refused flat events | New public compiled fixture matrix using real authored files, assert exit and exact event/provenance/no-fields readback |
| Q02 | Bare event actual publication/settlement and exact-tree restart, both stores | Existing TestPayloadlessEventPublicPersistenceJourneyBothStores, both leaves; exact source bytes/blob hash and terminal event payload/readback |
| Q03 | Refined typed payload actual served accept/refuse, both stores | New compiled served fixture; one valid and one rejected publication, exact typed RPC event counts/payloads and no partial rejected publication |
| Q04 | Retained artifact reconstruction away from original path | C/retained_artifact through exact admitted artifact and native selected source readback both stores; distinguish from Q02 public same-tree restart |
| Q05 | Broad compiled golden/selected-root consumer regressions | Existing golden lifecycle/selected-root tests selected by reviewer tier; no additional paid provider/Docker/soak credit |
| X01 | Requiredness check removal | One compiling mutant; F/optionality rejects loss of required membership/provenance |
| X02 | Metadata retirement check removal | Separate mutant; F/reserved_fields must fail, even null/merge value |
| X03 | Duplicate effective mapping check removal | Separate mutant; F/duplicate_fields fails exact output/refusal |
| X04 | Optional marker interior refusal removal | Separate mutant; F/invalid_optionality fails |
| X05 | Business-key required-field check removal | Separate mutant; F/business_key fails |
| X06 | Introduction/provenance input substitution | Separate mutant; F/introductions or F/provenance fails exact coordinates/RuleID/InputPaths |

Each mutation must compile, fail its named assertion (not setup), and restore
before final tests. An unreachable Q checkpoint or nondiscriminating mutation
requires a new ruling, not loose shared-owner proof. No arbitrary new database
observation ports, fixture SQL, skip, deadline increase or assertion reduction.

## Baseline Evidence And Qualification Request

On untouched ab1f10dac, this exact focused command completed exit0:

```sh
go run ./cmd/swarm-test -- ./internal/runtime/contracts ./internal/runtime/conformance ./internal/runtime/bootverify -run '^(TestEventCatalogAdmission.*|TestEventCatalogBusinessKey.*|TestEventCatalogPayloadField.*|TestEventMetadataRetirement.*|TestPayloadlessEvent.*|TestOptionalDeclarationAdmission.*|TestCanonicalFormsRegistryPinsWave3EventAdmission|TestTypedFieldDecoderFamilyUsesYAMLSourceProjectionOnly|TestExecutableEventRolesRemainVisibleWithoutMetadata)$' -race -count=3 -timeout=5m -json
```

Receipt `/home/youmew/.local/state/agent-g-2447-event-admission-20261012/baseline.jsonl`:
SHA256 `a11cabf1d54ea3bda06f11c8568012b99193e6b7eb6f9075ab3105d0bf8bca8d`;
1281 passing test instances, 78 repeated root executions/26 distinct roots,
zero failure/skip; contracts5.543s, conformance1.636s, bootverify2.568s.
This is unchanged component/owner/registry evidence, NOT new characterization,
public compiled proof, backend restart, post-extraction qualification or closure.
No whole local tier/server2/full run has been started for this audit.

Pre-push source census/guard baseline also completes exit0 on unchanged ab1:
same wrapper, packages contracts/conformance/bootverify/testplanning,
`-run '^Test.*(Census|Inventory|Registry|Guard|Ownership|Static)' -count=1 -timeout=5m -json`.
Receipt `census.jsonl` SHA256
`7cc234c0673bbd16ce93dc33f713ea93ac009e13b899c7ed479034091aea2062`;
502 passing instances/161 roots, zero failure/skip. Contracts138.499s,
conformance29.629s, bootverify9.564s, testplanning9.045s. This is the scoped
baseline inventory, not the complete future core structural unit or extraction
proof. No timing allowance/timeout/census population is changed.

Parent-class sibling probe also completes exit0 on ab1, same wrapper/contracts,
`-run '^(TestPlatformEventCatalog.*|TestTypeCatalogDocumentDecode.*|TestEntityContractsDocumentDecode.*|TestNodeAdmissionProvenanceComposesWithEvents|TestPackPlatformProvenanceComposesWithoutOverwrite|TestSharedRefinementBoundPresenceMatrix|TestEventSchemaRegistryFromBundle_PreservesWave1TypeMeaning|TestEffectiveEventResolutionPreservesConnectedRenameOwnership)$' -race -count=3 -timeout=5m -json`.
Receipt `siblings.jsonl` SHA256
`2769f9585cf9bde21802873a6dc59f3511e20b9dc690cb305ec23948e9844e39`;
726 passing instances/45 repeated roots/15 distinct roots, zero failure/skip,
12.729s. This probes platform/type/entity optionality, shared refinements,
compiled catalog meaning, connected rename and provenance composition without
folding those separately owned families into this maintenance PR.

Requested gate ceiling: hosted lifecycle, no extra CI-Units, focused local
characterization/consumer/compiled/public both-store supplements, exact
independent complexity plus full core structural owner-guard and touched-package
census/inventory/registry/static/authority ratchet checks BEFORE every push.
The existing payloadless public journey is in lifecycle hitl-releasee2e-rest
with mandatory sqlite/postgres children; do not confuse its legacy local
full-unit alias with hosted placement. Reviewer owns the actual tier decision.

## Overlap, Watchlist And Closure Feasibility

Open PR file census on2026-10-12: #2624 at9ba0631f2, #2617 at0f039defa,
#2616 at7933779be, #2614 at5cd44ae0e, #2607, #2372 and #2325. NONE edits
event_catalog_admission.go, event_catalog_admission_test.go, the planned new
event-factoring files, platform_event_value.go or schema_registry.go. No target
production overlap. F2614 changes providertriggers/packs/channel consumers;
C2616/2617 changes construction/composition and D2624 process lifetime. These
are integration proof risks, NOT authority to edit those files. Shared spec/plan
may move on master; no spec/plan change is intended here. Recheck all actual
merged deltas before qualification and amend if a chosen-class interpreter or
behavior changes. Do not polish runtime/fork/rebuild-zone hotspots here.

Watchlist-backed promotion: boundary_owned_decomposition already tracks broad
oversized-owner maintenance, plus invariant_suite_coverage for mechanical proof.
The merged relation family has zero intended maintenance tail; schema
projection31 cognitive, ToolSchemaEntry validation30/32, catalog reference26/35,
and Wave1 field decoder64/89 remain separate eligible-or-overlap-dependent
families. Peer-provider continuations and unresolved Responses SSE remain
separate. This evidence does NOT support whole-parent closure or a generic
event/type validator. Proposed immediate next slice is this21-function family.
The parent tail is at least four named schema/admission candidates plus provider
orchestration; grouping confidence medium, effort/remaining-PR count low until
their own current owner/consumer gates. No fabricated finite parent exit count.

Tracker decision: update #2447 current state and #2407 R1.4 ledger with merged
PR2636 and this new gate request; refine existing boundary/invariant nodes.
No new issue, POTENTIAL_ISSUES entry, parent supersession or historical-issue
reopening. Historical pending receipts remain historical, not current gates.
The merge closeout and existing watchlist refinements are already landed at
docs master `82150fca349af0cc6053a7db363606385c8841ff`, artifact
`docs/audits/2026-10-12-pr2636-merge-closure.md`. Prior accepted relation files
match reviewed/merged bytes, hosted lifecycle38294511413 attempt2 is GREEN;
attempt1 toolchain-download failure remains infrastructure evidence.

Intended closure: complete extraction of the chosen authored event-admission
maintenance class in ONE PR, approved-first-slice only for the broader parent.
Closure commitment: ALL chosen-class manifestations, not only one helper.
Feasible? Yes, tentative: one613-line file and bounded local phases. Report
actual gross/net lines honestly; reviewer6132342528 confirms the user waived
those targets, so necessary positive net needs no second waiver. No compression.
Every changed/new callable<25 and exact independent ratchet remain required.
Fixing only entry would leave field's same-class monolith live, so both and
the full family are required. Shared utility bodies stay unchanged unless gated.

Architecture feedback: mutable EventCatalogEntry/EventFieldSpec and provenance
maps remain admission intermediates; retained CompiledEventSchema is the
downstream authority. Opaque construction can reduce representable-invalid
states, but that is open #2349 model debt, not authorized by this extraction.
Keep feedback in existing issue/watchlist; no model redesign/new framework.
This PR's ROI is clearer phase/error ownership and discriminating executable
proof, not a claimed runtime speedup. Long-run model effort is not reliably
estimable without its own cross-family census.

Independent approved-first-slice outcome6132342528 is recorded. Remaining
stop conditions: spec contradiction,
new same-concept interpreter, changed accepted shape/error/provenance/identity,
unreachable supported proof, nondiscriminating mutant, unapproved shared-helper
change or inability to meet the exact size/complexity/closure boundary.

Reviewer-g: please record an independent approved-first-slice or insufficient
gate on #2447 for this complete21-function/49-row boundary, including eligibility,
the unchanged shared-helper boundary and proposed hosted/local proof ceiling.
No new characterization, extraction or semantic change begins before that ruling.
