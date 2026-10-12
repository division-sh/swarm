# Pre-Implementation Coverage Audit: #2509 Post-Merge Delta

## Status And Binding Boundary

Agent-g,2026-10-12. Audited source `e3883a6dd59b5411d1a86d95abbf000101fd1c0b`;
all production/test/spec bytes remain unchanged in this audit branch. Read the
full issue/thread, prior audit6b8bb58d1 and rulings5933171289/6115726207/6115727390;
re-read IMPLEMENTER_GUIDELINES.md and SEMANTIC_DRIFT.md. This document replaces
the old42-row audit as current planning, preserving every original S/N/Q row.

#2482 actually merged `c3f38293e16cebc09b552447cb6235f600692d12` onOct2;
#2514 merged `37ccd8cfc73d4ebb9707991948d8b53470264333`. Their merged code is
in this baseline, not copied from an anticipated candidate. The parked
dependency is obsolete. **Design direction accepted; CODING GATE CLOSED**
pending the independent short re-gate on this complete amendment.

Reuse #2509: one PR, two complete separately owned maintenance families.
Existing tests remain unchanged; characterize before private extraction.
No new owner/exported type/framework, schema dialect, credential policy,
activation/lifetime redesign, compatibility, migration or vendoring.
The explicit four-batch one-family/1500-gross/net-neutral waiver5923421059
applies to this child; the3000 gross-production-line program cap, cyclo<25
target for every factored/new helper and independent exact-head ratchet remain.
Estimated900-2000 gross authored production lines, medium-low confidence;
measure actual helper/diff inventory and stop before exceeding3000.

## Class Model And Closure Commitment

Category: **high-risk behavior-preserving maintenance**. Symptom: intertwined
branch/recursion/error/status validation bodies impede review; no runtime
defect is inferred solely from a metric. Named helpers are entry points, not
the audit boundary. Both classes stop at their distinct semantic owners.

| Working failure class closed entirely by the PR | Immediate parent | Parent above it / boundary |
| --- | --- | --- |
| S: monolithic definition and semantic-value admission within immutable ToolInputSchema | Complete tool-schema admission, projection and acceptance | #2447/#2407 keep-zone maintenance; lexical presence, directional relations and projected/provider acceptance are separately named concepts, not excluded same-class copies. |
| N: monolithic normalization of all supported installed/effective capability-subject kinds | Complete admitted capability/status readback | #2447/#2407; credential observation, compilation, activation and provider delivery retain distinct existing owners. Wider phase/type-model debt is #2250. |

Issue framing is broad enough for both complete finite classes, not a
definition-only or trigger-only slice. Chosen-class commitment: eliminate S
and N entirely, with every known manifestation proven. Intended closure:
failure-class elimination within these maintenance boundaries only, not
platform schema/lifecycle/parent closure. Current closure: audit only.

Feasibility: yes, in one PR using private phase/kind helpers inside existing
owners. Fixing only the local definition or effective-trigger arm would leave
same-class interpreters live and is prohibited. Public constructors/methods
and NormalizeSubjects remain the single authorities. The old monolithic
bodies are replaced, not kept alongside new interpretations.

Fresh pinned snapshot: definition76/107, value62/121, common subject50/74,
trigger69/103 cyclo/cognitive. Source population21858 rows per metric;
cyclo>=30 259/ >=50 59, cognitive>=30 557/ >=50 196. Independent9e058bfc2->e388
measurement PASS, equal reviewed policy; counts are not allowances. No score
snapshot/policy refresh, suppression or analyzer change is proposed.

## Governing Spec And Exact Prose Delta

Authoritative platform-spec.yaml sections re-read, not review drafts:

- tool_model.authored_admission: closed recursive typed/lexical schema admission.
- tool_model.hitl_channel_pack_interface.owners and contract: immutable
  ToolInputSchema/ToolSchemaEntry, exact projection, lexical presence, shared
  acceptance and directional relation owners; no runtime reconstruction.
- tool_model.provider_capability_surface: identity, kind/source/applicability,
  requirements, status rollup, guarantee registry and projections.
- tool_model.provider_trigger_adapters.compiled_admission_authority and
  pack_envelope_source_authority: exact compiled target/catalog/pack facts.
- engine.boot_verification.structural_validation/output and CLI verify output:
  structural subjects are not operational capability/readiness claims;
  default deployment verify still preserves its distinct admission checks.
- standing ingress readback: compiled exact identity and frozen/evaluated
  diagnostic facts, not hot activation from fresh credential presence.

Binding context also includes #2447's lead allocation/replacement eligibility,
#2407's cap/ratchet, merged #2482/#2514 and reviewer6115726207. Existing S/N
direct branch tests establish eligibility; missing historical salvage files
do not authorize unrelated R4-R7 successors.

| Item | Current contradiction | Exact eventual authoritative repair |
| --- | --- | --- |
| G1 | subject.source_values says channel_outbound_binding; code requires channel_binding while kind remains channel_outbound_binding. | Rename only stale source prose to channel_binding; preserve source/kind/wire bytes, no alias. |
| G2a | provider_capability_surface.description lists verify as an operational subject reader. | Structural verify/describe validate admitted source/pack facts; live operational subjects remain on preflight/doctor/serve. |
| G2b | status.AVAILABLE requires operator-facing verify to evaluate effective readiness. | Distinguish structural/deployment admission from live subject evaluation; do not restore retired capability_subjects. |
| G2c | compiled_admission_authority.projections gives verify installed/effective operational subjects. | Preserve structural inventory/declaration projection and separate live diagnostic readers. |
| G2d | standing_ingress.readback repeats that operational verify claim. | Same separation; leave newer valid default deployment checks, topology/readback and credential owners untouched. |
| G3 | requirements.trigger_rule says signing requirements are never evaluated, while status.rollup admits exact live BOUND/UNBOUND tuples. | Apply no-observation rule to immutable installed/base evidence; live evaluation uses existing exact-target credential owners. |
| G4 | same trigger_rule still renders ingress ID from alias; code and newer status rule use declaring FlowPath. | State existing packs.IngressSubjectID source/flow/provider identity; do not rename IDs or preserve alias-based compatibility. |

Prior G1-G3 dispositions remain binding; G4 is explicit in the current re-gate
request. All changes are proposed prose alignment with current code, not
implemented here. Any semantic rather than prose-only correction stops coding
for a repaired gate. Authoritative changes belong with extraction/proof in the
eventual PR, never prematurely on master.

## Ordered Supported Flows And Gates

S structural: scoped authored/generated/MCP schema -> lexical/JSON-token
admission -> immutable constructor/definition validation -> ToolSchemaEntry /
pack/registry admission -> exact projection/source compilation -> structural
verify/describe or boot -> provider tool registration. Invalid definitions must
fail before indexing, hashing or registration.

S execution: admitted source -> exact actor/catalog/permission and origin ->
registered input validation -> authorized effect/tool -> projected provider
result -> admitted output validation -> existing journal/publication/settlement.
Input acceptance, output acceptance and provider schema closure are not silently
merged into one error-order policy.

N: admitted installed/effective pack/source -> kind-specific body producer ->
compiled target/outbound binding -> optional exact credential observation on a
live surface -> NormalizeSubjects -> isolated ordered typed readback -> common
text/JSON renderer. Frozen standing admission remains a prerequisite; a fresh
credential cannot activate a dormant/recovery-blocked target.

| Gate | Classification and owner | Named execution proof |
| --- | --- | --- |
| Source/pack/contract presence and lexical alias/null/numeric tokens | Different semantic concept, with proof: yamlsource and scoped schema ingress. | Existing W5 schema/presence/MCP HTTP+stdio matrices, channel programmatic admission; Q01/Q03 compiled source path. |
| Typed schema definition/value and recursive constraints | Same S. | S01-S26 characterization before/after extraction. |
| Directional containment and projected argument acceptance | Different concept, with proof: schema relation and eventschema. | ToolSchemaRelationHandlesAnyAndEqualityWithoutDrift, propertyNames relation controls, exact structured-violation tests and Q03. |
| Generated/provider delivered JSON Schema closure | Different representation/closure concept, with proof, not absent. | ProviderToolSchema propertyNames/oneOf/nested/array tests, EntityOperationSchemaClosure, MapsGeneratedToolSchemaClosureToBootCheck and runtime extra-field refusal. |
| Actor/run/catalog permission and effect dispatch/settlement | Different concept, with proof: existing executor/managed/effect/pipeline owners. | Q03 real host MCP; direct HTTP output refusal; both-store channel journal/replay; Q04 unchanged compiled retained proof. |
| Subject identity/requirement/status/order | Same N. | N01-N27; all four kinds and every current producer below. |
| Credential snapshot/frozen enablement/learned recovery | Different concept, with proof; supplies facts to N, not status derivation. | StandingLearnedAuthorityCredentialMatrix, RecoveryReadbackDoesNotObserveDeclarationFallback, DormantIngressReadbackCannotHotActivate; Q02/N26. |
| Channel activation/physical registration and process ownership | Explicitly separately tracked: merged #2482 plus live #2612/#2617/#2624/#642 owners. | Existing channel compilation/journal/supported controls qualify prerequisites; no changes/closure credit for their runtime classes. |

Each preceding gate must succeed before the later tool/status branch is
reachable. Q proofs must show that path, not call a private helper and claim
public runtime credit.

## S Owner And Systematic Consumption Audit

Real owners: immutable ToolInputSchema New/Must constructors,
ValidateDefinition/Projection/Validate and semanticvalue conversion/equality;
ToolSchemaEntry ingress; lexical owner is separate. Repo-wide non-test Go
search finds28 literal ToolInputSchema-reference files. The table covers all
28 plus indirect accepted-entry/output/projection consumers and siblings.
Every row has one disposition; no same-class bypass is asserted away.

| Production consumers/files | Disposition | Planned retained execution |
| --- | --- | --- |
| contracts/tool_input_schema_value.go and tool_input_schema.go | Already consumes canonical S; private extraction here only. | S01-S26, constructors/definition/value/projection characterization. |
| contracts/tool_schema_admission.go, tool_schema_entry_value.go, tool_module.go, schema_registry.go | Already consumes canonical S through lexical/typed entry admission; lexical/module semantics unchanged. | W5ToolSchemaSharedBounds, AnyVersusOmitted, recursive presence, ToolSchemaEntryWithSchemasRevalidatesCompiledResultSource. |
| contracts/workflow_contract_types.go, workflow_contract_activity.go, workflow_contract_tools_value.go, platform_interface_value.go, platform_event_value.go | Already consumes accepted schemas/entries and projection; no post-admission reparsing. | W5 platform-interface/schema equality/provenance, malformed recursive tool tests and Q01. |
| contracts/tool_http_execution.go | Already consumes S for static mapped inputs, output/result admission; execution/response mapping retains its owner. | ToolCompiledResultAdmissionIsIndependentOfOptionOrder, statically-impossible mappings; direct/durable output refusal and Q03. |
| packs/channel.go, channel_generation.go, channel_source.go, channel_registration.go, channel_opaque_reference.go | Already consumes S for interface/context/tool/destination/registration definitions and execution values. | ChannelSchemaAdmissionRejectsRecursiveMalformedSchemasAtEveryTypedBoundary, CompilerPreservesExactEnumAndPinsItInGeneration, registration and opaque-reference relations; Q03 journal/replay. |
| providertriggers/manifest_source.go, normalized_events.go; indirect providertriggers.go | Already consumes S; typed recursive event descriptors retain exact schema. | TriggerBodySharedSchemaMatrix, TelegramInstalledAndEffectiveSubjectsCarryExactSelectedDescriptors, connector/schema composition. |
| providerconnectors/catalog_generator.go; indirect mock_response_compiler.go/mock_response_plan.go and registry | Already consumes S for generated typed contracts, accepted output responses and registry readback. | NewPackRegistryAdmitsSchemasAndReadbacksAreMutationIsolated, MockResponsePlanRejectsOutputOutsideCanonicalToolSchema, deterministic catalog controls. |
| runtime/tools/registry.go, platform_builtin_catalog.go; runtime/mcp/client.go | Already consumes S through entry admission/projection for static/native/remote catalogs. | Real HTTP/stdio W5MCPSchemaIntegerTokenBounds, catalog atomicity, missing/malformed discovery; Q03 compiled registered invocation. |
| runtime/activity_validation.go, pipeline/activity_engine.go, pipeline/workflow_executable_readers.go | Already consumes admitted schema/projection, not free authored schema maps. | generated-activity topology, executable-reader sink validation, direct/durable output rejection and Q03. |
| runtime/registration/provider_http.go, tools/executor_http.go/executor.go | Already consumes S/execution tool contracts; input/output validation before success retained. | Executor_HTTPToolRejectsProviderOutputOutsideAdmittedSchema and ActivityHTTPRejectsProviderOutputOutsideAdmittedSchemaWithoutCompiledResult; Q03. |
| contracts/tool_input_schema_relation.go and packs/channel_relation.go | Different concept, with proof: directional schema containment/cardinality, not instance acceptance. | PropertyNamesAdmissionProjectionAndValidation relation arms, ToolSchemaRelationHandlesAnyAndEqualityWithoutDrift, recursive directional/cardinality channel gaps. |
| runtime/eventschema/{registry,validate,inhabit}.go; tools/{validator,emit}.go; llm/mock_runtime.go; scenarioderivation/plan.go | Different projected acceptance/inhabitation concept, with proof: consumes admitted projections, preserves structured violation and per-context ordering. | exact enum/pattern/additional-schema acceptance, role-scoped RuntimeRejectsExtras, Q03 valid/invalid call and mapped output. |
| llm/provider_tool_schema.go; compatible/Responses request builders, mock_runtime/fork_chat_transport, tools/emit_runtime.go | Different delivered-representation concept, with proof: JSON-map provider subset/syntax, not another typed immutable constructor. | four ValidateProviderToolSchema roots, mock/runtime catalog controls and Q03 delivered generated schema. No removal or shared validator rewrite. |
| tools/generated_schema_closure.go; role_scoped_entity_tools.go; bootverify/runtime_tool_surface_checks.go | Different generated closed-schema law, with proof: additionalProperties and required/branch preservation at boot and runtime. | GeneratedToolSchemaClosureRejectsUndeclaredRequiredAndInvalidEnums, MapsGeneratedToolSchemaClosureToBootCheck, LoadRejectsGeneratedEmitToolSchemaWithUnresolvedFlowLocalType, RuntimeRejectsExtras, Q03. |

S lifecycle: definition entry installs active-map membership then defers its
deletion on all recursive/enum/error exits; a shared DAG is not a cycle.
Every child retains depth/path and equalTo scope. Value validation consumes
immutable admitted schema and semantic values; Any/omission/enum/oneOf and
schema-versus-event error order remain distinct. No DB/IO/resource lease is
introduced. Copying this defer into a shorter-lived helper is prohibited.

## N Owner And Systematic Consumption Audit

Canonical N: packs.NormalizeSubjects plus admitted Subject/Requirement/
TriggerAdmission, PackSource/PackIdentity and guarantee/remediation registries.
Search has26 production hits: one declaration and25 calls across10 files.
No second status normalizer found. All four subject kinds and renderers retain
canonical consumption, including the formerly omitted credentials seam.

| Consumer/producer | Disposition | Exact planned proof |
| --- | --- | --- |
| packs/capability_surface.go: entry, clone, private normalizers, both renderers and registries | Already consumes N; private phase/kind extraction here. | N01-N27, registry/error/order/clone and exact text/JSON characterization. |
| packs/channel.go: SatisfactionPlan.CapabilitySubject and OutboundBindingPlan.CapabilitySubject | Already consumes N; installed structural channel versus effective outbound credential tuple remain distinct. | ConfiguredChannelPackDrivesAvailableAndOutboundReadinessSurfaces, channel structural/runtime compilation, N04/N05/Q02/Q03. |
| providertriggers/providertriggers.go: LoadedPack.CapabilitySubject; admission.go: InstalledCapabilitySubjects / InboundAdmissionPlan.EffectiveCapabilitySubject | Already consumes N; immutable accepted descriptor/catalog and exact target are prerequisites. | TelegramInstalledAndEffectiveSubjectsCarryExactSelectedDescriptors, N10-N15/N19-N25, Q02. |
| providerconnectors/providerconnectors.go: CapabilitySubjects, capabilitySubjectForTool, availableCapabilitySubject | Already consumes N; installed import remediation cannot create callable effective inventory. | OwnsConnectorReadinessRollup, exact installed/effective inventory/multiplicity, N02/N03/N16/Q02. |
| runtime/standing_targets.go: base subject, exact provider/standing evaluation and aggregate readback | Already consumes N at base and evaluated stages; exact credential projection supplies facts. | N12/N15/N19-N27; dormant/recovery controls and Q02 exact target tuple. |
| runtime/standing_ingress_credentials.go: ineligibleStandingCapabilitySubjects | **Already consumes N; explicit repaired old-table omission.** Frozen enabled/block/recovery facts, optional signing observation flag and declared identity feed NormalizeSubjects; no new status owner. | StandingRecoveryReadbackDoesNotObserveDeclarationFallback, DormantIngressReadbackCannotHotActivate, StandingLearnedAuthorityCredentialMatrix and ExplicitStandingAdmissionCannotRefreshSiblingAuthority; N22-N27/Q02. Assert NOT_READY and recovery remedy even with newly present credentials, no target installation/fallback credential observation. |
| runtime/context_manager.go: installed generation validation, refresh, declared/scoped base, evaluated aggregate and evaluateIneligibleStandingSubject | Already consumes N/cloning; aggregate IDs retain source/flow and frozen runtime admission. | CapabilitySubjectAggregateBothStores characterization, A9DormantAliasesRemainDistinct, exact recovery/dormancy/context composition and Q02. Do not edit E's lifetime changes. |
| runtime/workflow_validation.go | Already consumes N for the relevant execution purpose, not structural credential readiness. | StructuralReadersShareAdmittedSource, SeparateStructuralValidityFromEffectReadiness, Q01/Q03. |
| cliapp/local_preflight.go; indirect provider_trigger_packs.go/provider_connector_tools.go/channel_packs.go/doctor.go | Already consumes N via canonical append/rollup/render and installed inventory; source-free versus deployment-bound evidence distinct. | source-free doctor, trigger/connector multiplicity, configured channel, N17/N18 and actual compiled Q01/Q02. |
| serveapp/main.go / serve_lifecycle_presentation.go | Already consumes base/evaluated canonical readback and concise renderer; no local status recomputation. | Actual compiled serve readiness/refusal in Q02 plus exact trigger text/JSON; lifecycle/publication order remains other owners' contract. |
| CLI authoring verify/describe and admitted-source/routing readers | Different structural/deployment-admission concept, with proof; not operational subject evaluation. | all six retained CLI baseline roots plus compiled Q01, G2 retired-field absence. Default deployment admission stays intact. |
| credentials/managedcredentials snapshot owners and accepted pack/catalog compilers | Different observation/structural fact concept, with proof; no owner move into N. | standing credential absence/invalid/recovery/sibling controls, accepted registry immutability and real selected-store channel journal/replay. |

N lifecycle: CloneSubjects precedes any normalization; caller inputs remain
unchanged on success and failure. BindingEnabled, Satisfied, scopes, token
headers, pack/admission and event/evidence nested data retain isolation.
Normalization validates existing frozen tuples then derives status; it never
queries a store, installs a target, promotes enabled authority or refreshes
sibling credentials. A supplied contradictory status remains an error before
later capability/guarantee phases. Stable ordering/dedup and unknown-code
readback remain exact. No new process/credential lifetime mechanism.

## Manifestation Matrix:59 Planned Rows, Not Earned Proof

Six planned characterization roots (not implemented here):
TestFactoringSchemaDefinition (SD), TestFactoringSchemaValue (SV),
TestFactoringSchemaProjectionIsolation (SP), TestFactoringCapabilitySubject (CS),
TestFactoringTriggerSubject (TS), TestFactoringCapabilitySubjectAggregateBothStores (CA).
Commit their pre-extraction expected outcomes on the actual integrated source,
then rerun the SAME cases after extraction. Named slash labels below are table
subcases; existing direct tests remain unchanged. Retain context-specific
precedence; do not sort unordered multi-error maps or normalize authority bytes.

| Row | Manifestation | Exact characterization / retained and post-extraction proof |
| --- | --- | --- |
| S01 | Missing/zero/kind/options and inappropriate constraint precedence | SD/shape-order; ValidateToolInputSchemaRejectsMalformedRecursiveSchemas. |
| S02 | Cycles versus shared DAG, depth64 and active-map cleanup | SD/graph; RejectsCyclesAndExcessiveDepthBeforeProjectionOrClone, SP/reentry-after-error. |
| S03 | Finite/safe/negative-zero numeric bounds and ordering | SD/numeric; W5ToolSchemaSharedBounds; exact same error/path/value after extraction. |
| S04 | Length/item integer presence and bound relations | SD/bounds; W5 recursive presence and HTTP/stdio integer-token matrices. |
| S05 | UTF-8/regex/format and string-only applicability | SD/string; DirectValidationEnforcesFormatAndEqualTo; exact projected pattern. |
| S06 | Array items and recursive child definitions | SD/array; malformed nested paths and SP/projection. |
| S07 | Property/required canonical keys, duplicate/undeclared required and additional alternatives | SD/object-order; same map traversal semantics, projection isolation. |
| S08 | equalTo property-only scope and declared sibling target | SD/equality-order; W5SchemaEqualityPresenceMatrix and direct equality tests. |
| S09 | Empty enum, recursive member validity, duplicates and authored order | SD/enum; RejectsExplicitEmptyEnum, ProjectPreservesAuthoredEnumOrder. |
| S10 | Omitted versus Any, YAML/constructor/map entrances | SD/entrance; W5ToolAnyVersusOmittedObjectAcrossConsumers, null/alias/provenance tests. |
| S11 | Exact projection/hash and immutable registry readback | SP/readback; AdmittedToolExecutionContractIsOpaqueAndMutationProof, NewPackRegistryAdmitsSchemasAndReadbacksAreMutationIsolated. |
| S12 | Value enum/type/semantic equality and invalid carriers | SV/enum-kind; AdmittedToolSemanticJSONCarriersRejectCycles. |
| S13 | String type/uuid/time/pattern/rune-length precedence | SV/string-order; direct-format and separate eventschema structured-violation controls. |
| S14 | All scalar/Any kinds and exact numeric bounds | SV/scalar; no coercion, unsafe number widening or extra fallback. |
| S15 | Array length before child items in typed value context | SV/array-order; separate projected items-before-length ordering retained, acceptance parity on single-invalid inputs. |
| S16 | Required-before-members, known/open/closed/schema-valued extra keys | SV/object-order; exact nested path/refusal and projected closed input controls. |
| S17 | Optional equality source, missing target and semantic sibling equality | SV/equality; typed/projected direct equality tests, no stringification. |
| S18 | Admitted schema through compile/register/input/direct and durable output | Q01/Q03; actual registered call, direct HTTP output refusal and both-store channel journal/replay. |
| S19 | oneOf definition shape, explicit empty/wrong branch and type applicability | SD/oneof-definition; OneOfAdmissionProjectionAndWrongBranch and OneOfRejectsMalformedAndMultipleMatches. |
| S20 | oneOf exactly-one value acceptance and error before enum/type | SV/oneof-count-order; zero/single/multiple matches, exclusive object branch, exact errors. |
| S21 | oneOf exact projection/order/hash and nested readback isolation | SP/oneof-projection; YAML/map roundtrip and branch slice mutation cannot change admitted value. |
| S22 | oneOf nested enum/graph/depth and repeated membership checks | SD/oneof-graph-enum plus SV/oneof-recursion; preserve active-map defer and per-branch enum revalidation. |
| S23 | propertyNames definition, applicability, malformed/null/cycle/depth | SD/property-names-definition; PropertyNamesRejectsMalformedAndCycles; preserve accepted schema vocabulary, no new string-only rule. |
| S24 | Semantic object-key validation before member constraints | SV/property-names-value-order; PropertyNamesAdmissionProjectionAndValidation, allowed/disallowed/empty keys and extra-key paths. |
| S25 | propertyNames exact projection/hash/isolation and relation parity | SP/property-names-projection; current direct propertyNames YAML/map/mutation/assignability arms unchanged. |
| S26 | propertyNames recursion and coexistence with oneOf/additional schema/equality | SD/property-names-graph and SV/property-names-composition; generated entity branch/key parity and Q03 registered call. |
| N01 | Required fields/trimming/kind and first error | CS/identity-order; no newly accepted noncanonical authority fields. |
| N02 | Requirement kind/status/scope/satisfaction/remediation full finite matrix | CS/requirement; partial/contradictory tuples and exact first errors across all kinds. |
| N03 | Installed/import AVAILABLE versus effective connector readiness | CS/connector; OwnsConnectorReadinessRollup, exact inventory/effective multiplicity. |
| N04 | Installed channel plan AVAILABLE without runtime requirement | CS/channel-plan; configured channel and wrong source/applicability cases. |
| N05 | Outbound binding READY/NOT_READY and channel_binding source | CS/channel-binding; Q02 exact wire and G1 source/kind assertion. |
| N06 | Supplied status contradiction before fact/guarantee normalization | CS/status-order; multi-invalid cases and unchanged registry failures. |
| N07 | Capabilities/unknown codes and exact guarantee registry owner | CS/facts; RenderSubjectUsesRegistriesAndPreservesUnknownCapabilityCode, registry failures. |
| N08 | Subject/fact/requirement ordering and duplicate refusal | CS/sort-dedup; deterministic canonical bytes, repeated normalization and reversed inputs. |
| N09 | Nested clone and input unchanged even on error | CS/readback-isolation; CloneSubjectsDoesNotExposeNestedCapabilityState and failure-path mutation control. |
| N10 | Trigger event/field descriptors, ordering and duplicate event rejection | TS/descriptors; exact Telegram installed/effective descriptors. |
| N11 | Installed trigger target-free/unevaluated constraints | TS/installed; RejectsGlobalTriggerReadiness and prohibited admission/observations. |
| N12 | Effective target coordinates and missing admission | TS/coordinate; OwnsEffectiveTriggerAdmissionShape; every required coordinate/error path. |
| N13 | Auth/policy source and verified pack versus raw no-pack | TS/policy; typed PackIdentity/PackSource and exact projected hashes. |
| N14 | Authenticated one secret versus unsigned zero requirements | TS/auth-count-scope; no invented global credential or status. |
| N15 | Unevaluated base AVAILABLE versus evaluated BOUND/UNBOUND tuple | TS/evaluation; exact trigger text/JSON facts, G3 and current evaluator path. |
| N16 | Installed/effective multiplicity across imports and contexts | CA/multiplicity on both stores; exact inventory and runtime context/target aggregation. |
| N17 | Redacted common text/JSON/concise output and registry phrases | CS/output; EffectiveTriggerTextAndJSONProjectTheSameTypedFacts and RenderEffectiveTriggerReadinessIsConciseAndRedacted; Q02. |
| N18 | Structural readers versus installed doctor and live serve | Q01/Q02; retired operational-field absence and live evaluated readback, not fake output parity between different purposes. |
| N19 | Source/declaring FlowPath/provider identity, not alias | TS/flow-identity; A9CapabilitySubjectDeclarationIdentity; G4 exact IngressSubjectID. |
| N20 | Dormant equal aliases remain distinct across flows/bundles | CA/dormant-alias-multiplicity; A9DormantAliasesRemainDistinct and context aggregation, no identity collapse. |
| N21 | Exact alias grammar/lookup without trim or rewrite | TS/strict-alias; A9AliasSystematicConsumption, A9IngressLookupUsesExactAlias; original diagnostic order. |
| N22 | Disabled authenticated binding never READY despite BOUND tuple | TS/disabled-authenticated; DormantIngressReadbackCannotHotActivate, Q02 frozen NOT_READY before/after credential insertion. |
| N23 | Disabled unsigned/base binding NOT_READY and legal enabled/absent controls | TS/disabled-unsigned; unsigned credential-owner absence control, no invented key/observation; exact supplied-status contradiction. |
| N24 | credentials_absent finite reason, blocked+enabled contradiction and invalid reason | TS/block-reason-order; empty/missing/enabled/disabled and invalid combinations, no new fallback reason. |
| N25 | recovery_required operation+command exact iff tuple and error order | TS/recovery-tuple; missing/foreign/partial extra fields and exact bytes/redaction, learned authority controls. |
| N26 | Frozen recovery readback refuses declaration fallback and sibling refresh | CA/recovery-frozen; RecoveryReadbackDoesNotObserveDeclarationFallback, ExplicitStandingAdmissionCannotRefreshSiblingAuthority; Q02 actual live readback. |
| N27 | BindingEnabled and recovery nested clone/error isolation | CS/disabled-admission-clone; mutate returned pointer/evidence/pack, caller unchanged on success/failure, Q02 readback unchanged. |
| Q01 | Actual compiled structural source readers | Planned TestAdmissionFactoringCompiledStructuralSurfaces: compiled verify portable/default deployment purpose and describe text/JSON/quiet, valid nested/branch schemas and invalid admission exits; pre/post captured output normalization limited to declared volatile paths. |
| Q02 | Actual compiled operational presentation and both stores | Planned TestAdmissionFactoringCompiledCapabilitySurfacesBothStores: source-free doctor installed inventory, source-bound node/standing serve readiness/dormancy/recovery refusal, canonical credential owners and exact status/IDs/redaction; no mock under public serve, paid calls or selected-row seeding as public credit. |
| Q03 | Actual registered tool and downstream direct/durable validation | Planned TestAdmissionFactoringRegisteredToolExecutionBothStores: authored public host MockOnly test -> native worker -> real HTTP MCP registered tool -> observable result/settlement; retained local HTTP direct refusal and both-store channel projected result/journal/replay. Full setup/auth/actor/origin must succeed before input/value checks. |
| Q04 | Unchanged wider supported consumer proof | Existing compiled host MCP, golden smoke and retained restart/forced kill on both stores; burst siblings are mapped, not presumed extra local/full requirements. Reviewer selects current CI/supplements. |
| Q05 | Exact size/complexity/construction/ownership integrity | Source-bound complete inventory/registry/guards, pinned base/head metric comparison, every new helper<25, existing-test diff unchanged, no production overlap or exported escape; no stale score baseline update. |
| Q06 | Frozen final-source qualification and proof audit | Reviewer-ruled CI/Local tier and exact named supplements; complete59-row distinct receipts, source/version/attempt identity and PR proof audit, not copied baseline credit. |

Generic future mutation checks: remove oneOf exclusivity, property-name
acceptance, disabled-binding status check or recovery iff validation in separate
disposable candidates; each named characterization/registered path must fail
and restore. Not executed or committed in this phase. No weakened old oracle.

## Current CI Selection Map (Not A Tier Ruling)

Canonical planning probes on e388, go1.25.5 linux/amd64/CGO1, no GOWORK:
full94 units digest ae78bea6b50513fabfbaa7048a1105674610da7297fc273ad4599bfa37f8f746;
proposed lifecycle78 digest038b9498347974a061e1a8c0ad0325dd30759d93dda9dc8d957d8042985dcd7f.
Lifecycle input is an explicitly synthetic PR-body planning probe, not an
actual PR/approved selection or execution receipt. Manual lower-tier request
correctly refuses; no CI policy change. New roots are not yet in the source:
regenerate/rebind after characterization before asserting their exact owner.

| Required proof family | Current full-plan unit(s) / tier ownership |
| --- | --- |
| contracts typed/lexical schemas | contracts-first and contracts-rest; both in lifecycle. Planned Factoring roots should enter existing contracts partition, census must prove one owner. |
| packs / triggers / connectors / MCP | broad-09 / broad-03 / broad-08 / broad-03; current lifecycle maps identically. |
| direct tools / generated closure / boot checks | broad-05 / broad-05 / broad-04; lifecycle. |
| runtime standing credentials/context and blocked readback | runtime-full; lifecycle contains this owner. |
| provider JSON-map / mock/fork provider consumers | api-llm-bus-full; lifecycle includes it. |
| durable channel output/journal/replay | pipeline-rest; lifecycle includes it; exact existing both-store root identified. |
| CLI structural/installed/capability proof and new compiled CLI roots | catalog-required-verify; lifecycle includes it, rebinding must account new roots. |
| actual compiled host MCP / new composed release proof if placed there | hitl-releasee2e-rest; existing host proof is selected; maintain public-boundary fixture rules. |
| golden smoke and retained restart/kill | hitl-releasee2e-golden; reviewer decides minimal hosted/named local obligation. |
| existing burst siblings | hitl-releasee2e-burst-1/-2 full-only, disclosed as parent consumers; no automatic duplicate Local full. |

This is an ownership map for reviewer selection, not permission to retier or
move a root. Exact CI-Tier/CI-Units and minimal Local matrix await re-gate;
do not inherit old2026-10-01 qualification defaults or claim any tier passed.

## Active-Lane Overlap And Stop Conditions

Refreshed paginated changed-file inventory: #2612@77ee17fc8,
#2617@4c220bff3 plus clean local36b454b60, #2624@06f5d3dff,
#642 local3f7d315eb plus its uncommitted path inventory. None edits the three
targets/direct schema-subject tests. E edits shutdown lifetime in N consumer
context_manager; C/A own instance/receiver/fork/activity authority; D edits
deployment process/verify possession. Do not touch those owners. Shared
platform-spec/proof-plan metadata and composed source consumers are integration
boundaries, not claims of zero semantic interaction. Repeat this census
immediately before coding and after any dependency integration.

No new plan/YAML unit changes expected for ordinary characterization. Source
fixtures must follow current CEL/quoted-text and authored-schema admission,
canonical construction/store observation owners, public compiled-process
boundaries and no raw SQL. Stop rather than add ports or fix other lanes.
If any new live S/N interpreter appears, any characterization contradicts
binding spec, any public proof checkpoint is unreachable or any semantic
change is needed, repair class/tracker/gate before implementation. No narrow
error-only patch, heuristic fallback, speculative compatibility or full-run
retry may replace that decision.

## Parent, Watchlist, Tracking And Architecture

Parent probe: completed #2506/#2508/#2510/#2511 remain closed within their
approved families; current provider continuations are36/50,41/58,41/58,29/35,
sharing lifecycle parent but separate orchestration. Responses SSE still
advertised while live sendRequest decodes JSON and parser has only two test
callers; that unresolved product/spec class stays separate. R4-R7/root/fork
and serve composition186/288 are active rebuild owners, not same-S/N copies
that a small private extraction can honestly absorb.

Post-audit parent action: **keep the reviewed two-class first slice**; promote
the complete S/N phase and consumer coverage now, leave #2447/#2407/#2250
explicitly OPEN. Named finite tail is this one child/two maintenance classes;
peer provider family and broader rebuild groups remain unbounded/low-confidence
in PR count. No guarantee this one PR closes either parent. Estimate2-4
engineer-days after coding gate, medium confidence, high review/regression ROI;
no runtime performance promise or invalid-state model redesign.

Watchlist-backed promotion check: existing boundary_owned_decomposition and
invariant_suite_coverage track oversized seams, moved complexity, missing
proof and lost lifecycle/error order; pack_interface_structural_satisfaction_
and_channel_adapter_ownership names immutable schema, registry, directional,
trigger/channel consumers. All those relevant siblings are classified above.
They support complete S/N closure, not a generic validator or absorbing other
agents' activation/transport models. Refine those EXISTING nodes, no new node.

Tracker-state decision: **update #2509 and #2447 before coding** with merged
dependencies, scores,59-row audit, explicit standing-credentials row, spec
corrections and closed coding gate. Old parked/body/tail wording is superseded
as current status, historical comments/receipts preserved. No new issue,
superseded active stream or POTENTIAL_ISSUES entry. Architecture feedback is
tracked in #2509/existing nodes: clear private phases inside correct opaque
owners; broader type/phase redesign remains #2250 and dedicated R4-R7 streams.

## Baseline Receipts Actually Executed

No characterization, extraction, public new journey, candidate mutation,
Docker/paid proof or whole-tier qualification in this phase. Fresh sources
remain e388 for these baseline runs, ordinary focused go test only:

- Prior refreshed25 owner roots (contracts/packs/triggers/connectors), six
  provider/generated sibling roots/79 terminals and six CLI structural/
  configured-channel/multiplicity/source-free roots PASS, no failure/skip.
- Standing/recovery/alias/connector composition: eight roots PASS, runtime2.006s;
  no selected-store parity claim from its learned-store stub/file fixtures.
- Direct/durable/MCP baseline: seven roots PASS; tools .011s, MCP .118s,
  pipeline2.255s. ChannelProjectedActivityResultJournalsAndReplays ran BOTH
  SQLite .450s and PostgreSQL1.790s, no skip; native PG16 binaries explicit.
- Generated-schema boot/runtime sibling: four roots PASS, bootverify .008s,
  tools2.947s; actual load/boot-check mapping and runtime rejection, not AST only.
- Combined new baseline:19 roots/321 pass terminals, zero failures/skips.
- Canonical full/lifecycle planning probes PASS, no unit execution or approvals;
  independent exact source-complexity collection PASS, policy unchanged.

Raw JSONL, production reference inventory, paginated overlap lists and bound
plans retained in `~/.local/state/agent-g-2509-postmerge-preaudit-20261012/`;
earlier metric/37-root data in the #2447 preanalysis directory. An initially
mistyped sibling regex matched no llm tests; only corrected selection is
credited. No absent or skipped backend is counted as execution.

## Independent Gate Request

Reviewer-g: re-gate coding on the repaired full S/N59-row boundary, precise
standing-credentials/provider/generated/relation distinctions, G1-G4 prose
alignment, current overlap/size estimate and actual CI owner map. Please record
exact CI-Tier/CI-Units and minimal Local/supplement selection. **The required
independent coding outcome is pending, not self-approved. Stop here before
characterization or implementation.**
