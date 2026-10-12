# Pre-Implementation Coverage Audit: Catalog Structural-Type Integrated Delta

Agent G. Source: master 241bbbdde095c424f8a99ea05cb0f18a80a9f912,
effective go1.26.8, Linux. Independent read-only ruling:
https://github.com/division-sh/swarm/issues/2447#issuecomment-6134475032 .
The earlier f2d intake/receipts remain historical, not relabelled integrated
proof. No production, source-test, spec, plan, baseline or workflow edits.
IMPLEMENTER_GUIDELINES and SEMANTIC_DRIFT re-read. **Coding gate NOT satisfied.**

## Class, Context And Stop

Category: high-risk maintenance, with independently reproduced correctness
stops. Observed maintenance symptom: complete 14-function/403-line structural
type-reference family contains named-resolution 26/35 cyclo/cognitive and JSON
lowering 20/31. The failing copy probe and these hotspots were entry points,
not audit boundaries. Chosen proposed maintenance class: factor the complete
catalog structural-type resolution/shape/lookup family without changing admitted
meaning, output, error order or provenance. This is not a generic validator.

Immediate semantic parent: recursive structural shape, field-edge presence and
detached type evidence across event/entity/collection/expression consumers.
Broader parent: semantic contracts not mechanically preserved at owner/consumer
boundaries (#2349), with #2447 maintenance and #2407 qualification streams OPEN.
#2250 is adjacent lifecycle/orchestration debt, NOT this semantic parent.
The proposed file is a coherent maintenance first slice, but alone is **too
narrow for the newly observed copy and scalar-alias correctness classes**.

Binding source of truth at this exact source:
- `contract_formats.event_schema.recursive_field_presence` (L1180).
- `design_principles.no_structural_primitive_elides_type_discipline` (L3828).
- `handler_specification.expression_context.namespaces.payload.read_model`
  recursive_maps/shared_readers/structural_identity (L5255-5257).
- `type_system.vocabulary`, `type_system.schema_refinements` and
  `type_system.restrictions.retired` (L2830-2966). Scalar aliases decode to
  built-in scalars; recursive Wave1 types are explicitly unsupported.
- `flow_model.flow_instance_authoring.template_instance_model` (L13307),
  singleton_cardinality_model and singleton_coordinator_model (L13527-13595).
  Identity is one scalar/enum field; cardinality and actual typed coordinator
  demand are not granted by structural map/list presence alone.
- `flow_model.compiled_event_schema_projection` (L14156): admitted immutable
  event acceptance and structural evidence, not downstream reconstruction.
- The issue/thread and independent breadth ruling above constrain this
  maintenance boundary and freeze correctness implementation pending disposition.

There is no permission to preserve off-spec forms as characterization, add
compatibility, invent read-time type evidence, migrate old stores, vendor, widen
runtime ownership or weaken a maintained assertion. A required semantic repair
must update the authoritative spec in its own approved PR boundary.

## Owners And Complete Census

Real semantic owners, not the first helper encountered:
1. `CatalogTypeReference.Resolve/ResolveReference`, `ResolveJSONSchemaStructuralType`
   and the ResolvedCatalogType/ResolvedCatalogField carrier own recursive shape.
2. `StructuralCatalogTypesEqual/Assignable` own shape/presence relation, NOT
   enum/refinement value-set inclusion. Integer-to-number scalar widening is the
   only non-equality relation here. Names/paths are diagnostics, not identity.
3. `ResolvedCatalogType.Clone` owns recursive carrier copies;
   `SchemaRefinements.Clone` already correctly detaches all four mutable bounds.
   Their composition is missing. Declaration cloning and compiled input capture
   are separate production edges of this same detachment obligation.
4. `PrimaryEntityContract.StructuralType` composes declared root/field evidence;
   `newCompiledEventSchema` composes admitted JSON shape plus declaration TypeRef
   and refinements; `withRequiredField` composes exact connected-field evidence.
   They are canonical composition, but their shallow-copy edges are live defects.
5. Event-field lookup owns declaring flow/executable catalog precedence, not
   shape parsing: ResolveEventFieldType and ResolveExecutableNodeEventFieldType.
6. Singleton/template admission owns cardinality, contained-state demand and
   scalar identity. Its duplicated type classification is **not** an ordinary
   caller; scalar-alias disagreement is separately execution-proven below.

Whole-repository typed `go/packages` production census: 298 packages, 1,304
sites; 1,012 outside the target file across **49 files**. Exact file/line/caller,
actual type/method/field, constructor and initializer/assignment evidence is
`issue-2447-catalog-type/typed-census.jsonl`, SHA256
73efc804f4a9a8992ee5b617489d1f321b9ce6f0b228826ed3adf06695818831.
It includes 38 outside-owner Clone uses, 22 Field uses and 5 FieldPath uses.
This supersedes the initial lexical 36-file sketch; 11 lexical misses were
found by typed scanning, followed by two more carrier-only files in the final
whole-repo constructor sweep. No second structural carrier/resolver definition.
Current target/producer files are byte-identical across f2d -> 241bb.

Separate typed refinements/cloner sweep: **202 sites, 24 files, 68 callers**,
`refinement-census.jsonl`, SHA256
542f82dbb9e1f3428d28baa0e5cf1781586c25b3f5ec77e17dbbf2c459f85fc1.
It includes root/flow/resolved catalogs, primary entity, connection evidence,
join/fan-out clones, provider event producer/cloner, decoder admission,
refinement validators, entity normalizer and tool-schema projection.
Lexical searches cover nonselected source/build variants and raw helper names;
typed census is Linux production, not a claim to have executed every GOOS.
Every existing production structural-type file found is classified below.

Status codes used **only** in this audit:
A = already consumes canonical shape owner (not claimed detached until D fixes).
C = canonical composition of admitted facts, consumes shape owner.
E = still bypasses same-concept owner and explicitly escalated before coding.
Different concepts are named with their distinct law and execution proof.
No row is labelled merely "same seam". No consumer has been moved in this work.

| Production seam, under internal/runtime unless stated | Status, semantic role | Named execution/proof family |
|---|---|---|
| accprojection/resolver.go | A: target/item structure; project mapping separately governs fields | C01 ValidateProject_PreservesOptionalNamedFieldPresence, ValidateEventTypedViewDoesNotUseAnotherFlowDeclaration |
| accumulator/spec.go | A: catalog field/key relation | C01 A2AccumulatorKeyRequiresCatalogTextField |
| bootverify/wave1_entity_contracts.go | A: canonical leaf/path resolution; platform metadata is a different namespace | C02 EntityNestedPresenceAcrossBootConsumers |
| bootverify/workflow_cross_surface_named_type_checks.go | A: shape candidates; lint scoring differs from admission | C02 Run_ReportsCrossSurfaceExactDuplicateShapesAsLintEvidence |
| bootverify/workflow_expression_checks.go | A: payload/entity/item and reader roots | C02 EntityNestedPresenceAcrossBootConsumers |
| bootverify/workflow_fan_out_checks.go | A: collection item validation | C03 A2FanOutMapProjectionAndTypedOrdinalAlias plus consumer-check controls |
| bootverify/workflow_flow_boundary_checks.go | A: declared payload fields | C04 CompileConnectPlansValidatesAuthoritativeInstanceSourceTypeMatrix |
| bootverify/workflow_input_alignment_checks.go | A: handler/policy roots and field presence | C02 StructuralPresenceRootShapeAndDecisionMatrix |
| bootverify/workflow_join_checks.go | A: arrival/result structure; join cardinality separate | C05 WorkflowSemanticsJoinPlanPreservesDeclaringResultCatalog |
| bootverify/workflow_loop_checks.go | A: required text revision field; loop lifecycle separate | C05 TestLoopValidationRejectsMissingOrNonIntegerAttemptPolicy |
| bootverify/workflow_node_state_schema_checks.go | A: node-state declared catalog | C06 TestRun_AllowsDeclaredNodeStateScalarAndEnumRefs / TestRun_ReportsUndeclaredNodeStateNamedType |
| bootverify/workflow_payload_completeness_checks.go | A: compiled trigger-field presence | C02 planned TestCatalogStructuralConsumerCharacterization/C02/payload_completeness, supplied/missing required/optional nested fields |
| bootverify/workflow_stage_gate_checks.go | A: entity/context/emit shapes; stage readiness separate | C06 TestStageGateContextPreservesNestedEntityPresence / TestStageGateEmitDoesNotUseAnotherFlowDeclaration |
| contracts/collection_projection.go | A: collection source, copied item; D08 | C03 A2CollectionProjectionAdmissionAndOrdering/CatalogAndDigestWitness |
| contracts/compiled_connection_input.go | C: producer/receiver field evidence; D09 | C04 instance-source and root-reply controls |
| contracts/compiled_event_schema.go | C + E: exact JSON shape/declaration augmentation/required-field addition, shallow D01/D02 | C07 CompiledEventSchemasOwnOptionalityAndCanonicalSchema/PreserveFilesystemFlowOwner |
| contracts/durable_data_catalog.go | A: exact compiled file-field import shape, not storage identity | C08 DurableDataImportShapeUsesCompiledStructuralFields |
| contracts/schema_refinement_validation.go | A: resolved refinement targets; richer constraints differ from shape | C09 ValidateWorkflowContractBundleLoadConstraintsRejectsInvalidSchemaRefinements |
| contracts/schema_registry.go | A: shape -> acceptance projection adds enum/refinement meaning | C09 EventSchemaRegistryFromBundle_PreservesWave1TypeMeaning |
| contracts/standalone_projection_type.go | A: type resolution then concrete value validation | C03 planned TestCatalogStructuralConsumerCharacterization/C03/standalone, type/value positive and malformed/unknown negatives |
| contracts/workflow_contract_emit_lowering.go | A: exact admitted source/target fields, not event routing | C10 HandleEmitTool_FailsClosedOnNamedTypeViolation |
| contracts/workflow_contract_wave1_accessors.go | C + E: primary composition, shallow declaration clones, duplicated singleton/template interpretation | D03-D07, S01-S08 |
| contracts/workflow_fan_out_semantics.go | A/C: source/item/query projection; D09 | C03 FanOutCollectionItemTypePreservesNamedFieldPresence |
| contracts/workflow_instance_resolution_source.go | A: exact producer/receiver catalog relation; D09 | C04 scalar alias/UUID/integer/mismatch controls |
| contracts/workflow_join_plan.go | A/C: declared result catalog plus join-owned result wrapper | C05 WorkflowSemanticsJoinPlanPreservesDeclaringResultCatalog |
| contracts/workflow_join_plan_clone.go | A: result type + declaration catalog copies; D09 | C05 clone and retained join projection controls |
| core/pinrouting/connect_route_plan.go | A: required fields/instance/reply shape; route selection differs | C04 CompileConnectPlansValidatesAuthoritativeInstanceSourceTypeMatrix/RootReplyCorrelationRequiresBothSchemas |
| engine/entity_assignment.go | A: declared paths and branch facts; definite assignment differs from shape | C11 FlowConstructorNestedPresence/EntityAssignmentStructuralTransfer |
| engine/executor.go | A: payload/item/join/expression roots and typed views | C12 executor collection/join/accumulator controls |
| engine/fan_out_evaluator.go | A: copied capsule item shape | C03 A2FanOutMapProjectionAndTypedOrdinalAlias |
| engine/flow_constructor.go | A: compiled constructor supplied-field projection | C11 FlowConstructorConsumesEffectiveSchema/NestedPresence |
| engine/types.go | A: typed execution frame carrier, no independent parser | C12 executor frame/typed fan-out controls |
| entityruntime/assignment.go | A: FieldPath, write/observation facts | C11 EntityAssignmentStructuralTransfer/ObservationNeverSynthesizesRequiredMembers |
| entityruntime/contained_operations.go | A: record paths vs map keys and partial-object normalization | C13 EntityMutationPrerequisitesDistinguishMapKeysFromRecordPaths |
| entityruntime/contracts.go | A: canonical type/field lookup; concrete values/equality/refinements differ | C13 NormalizeFieldValue_EnforcesSchemaRefinements |
| pipeline/workflow_executable_readers.go | C: admitted activity result JSON -> executable reader root | C14 TestExecutableReaderCensusCoversEveryReaderFamily / planned TestCatalogStructuralConsumerCharacterization/C14/activity_result |
| pipeline/workflow_gate_lifecycle.go | A: compiled context shape; lifecycle/time ownership differs | C06 planned TestCatalogStructuralConsumerCharacterization/C06/gate_context, execute admitted required/optional nested context |
| pipeline/workflow_lifecycle_plan.go | A: copied join result wrapper; persistence/settlement differs | C05/C12 TestExecutor_JoinCompletionConsumesCatalogResultType plus Q04 retained reconstruction |
| semanticview/compiled_activity_source.go | C: generated typed result wrapper, same admitted JSON entrance | C14 GeneratedPublicationStructuralSchemaPreservesExactOwner |
| semanticview/entity_structural_type.go | A: primary/entity semantic view | C13 planned TestCatalogStructuralConsumerCharacterization/C13/entity_view, primary/type/refinement immutable readback |
| semanticview/event_schema.go | A: compiled structural view/field readback; D01 | C07 ResolveEventSchemaBindsConcreteTemplateInstanceToAuthoredDeclaration |
| semanticview/source.go | A: interface exposes typed semantic shape, no parser | C15 Q04 TestEventCatalogFactoringRetainedArtifactBothStores through semantic source plus generated source C14 |
| semanticview/source_artifact.go | A: admitted bundle -> structural view | C15 retained artifact Q04 |
| tools/executor_entity_query.go | A: query CEL env; query leaf policy is distinct | C16 TestValidateEntityFilterExpression_AllowsDeclaredNestedLeaf / TestEntityTools_QueryEntitiesFilterRejectsUndeclaredFieldBeforeEvalWithNearestMatch |
| tools/platform_builtin_catalog.go | A: canonical shape -> richer schema/enum/refinements | C16 EntityOperationCatalogSchemaAliasAndMapKeyParity |
| tools/role_scoped_entity_tools.go | A: typed actor-readable/writable subpaths, authorization separate | C16 TestRoleScopedEntityTools_GeneratedSchemasAreClosedAndRuntimeRejectsExtras / TestToolDefinitionsForActor_DeriveRoleScopedEntitySchemasFromActorContract |
| workflowexpr/data_expression.go | A: root carrier obligations, no catalog parser | C17 TestStructuralPresenceRootShapeAndDecisionMatrix / planned TestCatalogStructuralConsumerCharacterization/C17/result |
| workflowexpr/structural_expression.go | A/C: canonical shape -> CEL handles and checked CEL result -> shape; reverse mapping is language interop, not catalog parsing | C17 ImportedDictionaryRecursivePresence/UnknownRecordTraversalFailsClosed |
| workflowexpr/structural_predicate.go | A: entity expression roots | C17 StructuralPresenceRootShapeAndDecisionMatrix |

Additional refinement/cloner files beyond the 49-file shape census:
- `contracts/schema_refinements.go`: canonical bound Clone; positive control
  detaches all four bounds. Real semantic owner, currently bypassed by D01-D09.
- `contracts/tool_input_schema.go`, `event_catalog_clone.go`,
  `event_schema_ownership.go`, `platform_event_catalog.go`,
  `workflow_contract_accessors.go`, `workflow_contract_activity.go`: event
  declaration copies already deep-copy bounds but use local pointer copiers,
  not SchemaRefinements.Clone. **E: include in the repair-first scope ruling**
  as correct-but-noncanonical copy paths, preserve exact schema/citation facts.
- `internal/runtime/provider_trigger_source.go`: same correct-but-local four-bound copy;
  E/D10. Do not count correctness as systematic owner consumption.
- `providertriggers/normalized_events.go`: fresh bounds from immutable schema
  value accessors, not a copy of a retained SchemaRefinements; different
  producer concept. NormalizedEventSchemaRemainsExactThroughAdmissionCatalogAndRuntime.
- `contracts/workflow_contract_yaml_wave1.go`, `event_catalog_admission.go`,
  `workflow_contract_types.go`: fresh admitted declaration/field carrier and
  definitions, not retained-view copy. These remain unchanged entrances; typed
  declaration clone/capture is still D03-D07, not excused by fresh parsing.
- Remaining refinement validation and entity/tool projection sites consume
  values but cannot grant structural shape or change admitted bounds; C09/C13/C16.

## Producer And Mutation Accounting

The typed evidence enumerates every constructor/initializer/assignment, not
only resolver calls. Outside the owner, admitted shape is composed by primary
entity, newCompiledEventSchema/withRequiredField, query item projection, join
wrapper projection, activity result lowering and CEL language interop. Other
constructors are zero/error returns or call the resolver; none supplies a second
named-catalog parser. Clone consumers are readbacks/snapshots, not business writes.
No production post-admission bound mutation was found by this sweep; the
counterexample shows a reachable mutable API and broken immutable readback,
not an asserted payload validation bypass or persisted-store corruption.

PrimaryEntityContract.StructuralType resolves each declaration Type through the
canonical catalog then composes root Name, TypeRef, optionality and refinements.
JSON compiled-event construction first canonicalizes acceptance, lowers shape,
then attaches exact declared TypeRef/refinements. withRequiredField copies the
old shape, appends the connected field required by default, sorts fields and
updates acceptance bytes/digest. Its new field has no invented declaration
refinement; retained old fields still share D01 pointers. Requiredness/provenance
composition is tested separately from JSON admission, not a second raw parser.

Field/FieldPath return values borrow nested pointers from their **caller-owned
resolved value**. They do not themselves promise an opaque immutable admission.
The immutable compiled/projection getter must first return a detached clone;
its failure is D01/D08. Nested FieldPath on a Clone proves that Clone does not
detach bounds (D04), not that every local lookup must mint another semantic owner.
Planned controls mutate local views and immutable readbacks separately, preserving
all field/type/presence/source facts and original acceptance bytes.

## Manifestations And Exact Planned Proof

All rows are **planned**, except explicitly named baseline/control evidence;
no row has extraction or repair credit. The original 14-row function table in
the intake remains complete, with these proof IDs bound to it:

| IDs | Concepts/manifestations | Exact planned discriminating proof |
|---|---|---|
| F01 | Clone | TestCatalogStructuralTypeFamilyCharacterization/clone: nested map/list/record, all four bounds, pattern/equality strings; D RED->GREEN first |
| F02 | StructuralCatalogTypesEqual | same root/field order/name-independent shape; kind/optionality/nested-carrier negatives; formats/refinements not payload-acceptance credit |
| F03 | StructuralCatalogTypeAssignable | equality and scalar integer->number only; reverse/nested collection widening refused |
| F04 | StructuralCatalogTypeSyntax | exact deterministic strings for every kind, nested absent carrier and sorted record fields |
| F05-F06 | Field and FieldPath | found/absent/blank/nested/empty segment/map boundary; local borrowed view vs immutable getter detachment |
| F07-F08 | JSON entrance and recursion | actual compiled admitted ToolInputSchema/event objects, required sets, arrays/maps/dynamic/primitive forms, exact nested errors; raw malformed map only defensive credit |
| F09-F12 | reference Empty/Resolve/ResolveReference/recursive resolver | builtin scalar/format, enum/alias, declared fields, nested list/map, unknown/empty negatives; recursive/retired forms must be ruled before characterization |
| F13-F14 | event flow/executable lookup | exact flow-local shadowing, executable declaration then platform fallback, nil/invalid/missing field negatives; preserved catalog detachment |
| C01-C17 | all 49-file consumption rows above | named existing roots plus new narrow actual-consumer controls wherever an existing root does not execute that listed branch; no file-list-only credit |
| D01 | compiled StructuralType/StructuralField retained readback | imported and authored valid schema, each mutable bound; mutate returned field, fresh read must agree with retained acceptance/digest/source |
| D02 | compiled input capture + required-field successor | mutate original declaration after compile; old and connected views/acceptance remain frozen independently |
| D03 | root/flow/resolved type catalog getters | returned named field vs fresh getter, each bound, scalar/enum unaffected |
| D04 | recursive clone + nested FieldPath | Probe/[Probe]/map[text]Probe/Outer.child.value, each bound; mutate copied view, original unchanged |
| D05 | root/flow/resolved entity getters | mutate returned declaration bounds; fresh entity and source artifact unchanged |
| D06 | root/flow/test-setup primary entity getters | real admitted primary and wrapper projections preserve bounds/optionality/source; synthetic no-entity setup remains a distinct recipe |
| D07 | PrimaryEntityContract.StructuralType | real loaded contract -> structural view -> fresh primary view; all four bounds |
| D08 | CollectionProjection ItemType/Clone/Validate | list/map named items and source type, bound isolation, order/cardinality/digest unchanged |
| D09 | connection SourceEvidence, FanOutPlanSemantics/FanOutCompiledPlan evidence, WorkflowJoinPlan.Clone | each producer's real admitted accessor independently mutated/read back, not only direct Clone |
| D10 | event/provider local deep-copy paths | all four bound pointer controls + retained imported consumer; migrate only under repair gate, exact byte/provenance/schema parity |
| S01 | text scalar alias in singleton list values | real loaded [Label] vs [text], same resolved shape, current alias falsely refused |
| S02 | text scalar alias in map keys | real loaded map[Label]Item vs map[text]Item; rule/owner disagreement escalated |
| S03 | text scalar alias in template identity | real loaded worker/id: Label vs text, ID/Time/enum controls; current Label falsely refused |
| S04 | nested map/list + enum/UUID/timestamp | both interpreters agree on actual loaded supported cases; retain field path/kind and role-specific demands |
| S05 | unknown reference and empty alias | same canonical refusal, raw/catalog/load preconditions separately named |
| S06 | alias chain/cycle | loader rejects non-built-in alias bases; defensive resolver cycle controls cannot earn authored acceptance credit |
| S07 | named recursive declaration | loader/resolver/coordinator currently accept [Cycle] with child:Cycle, while spec says unsupported; checker probe finds only writer coverage, NOT a recursion rejection. Explicit off-spec escalation, not an approved stopping rule |
| S08 | list spellings and map-key subset | loaded list<Item>/Item[]/[]Item agree; spec explicitly names list<...>/[...], suffix/prefix and coordinator integer-key status need governing disposition. No compatibility claim by observation |
| Q01 | public structural readers | compiled verify + describe on authored/nested/refined/optional fixture; exact output/error/provenance; no operational readiness credit |
| Q02 | public compiled runtime | NamedRecordEmitSurvivesServedRestartBothStores, exact named values then served restart and completed-conversation fork/restart |
| Q03 | public retained source readback | PayloadlessEventPublicPersistenceJourneyBothStores, compiled verify/describe/serve, persist exact bytes/hash, relocate same tree and restart |
| Q04 | independent artifact reconstruction | EventCatalogFactoringRetainedArtifactBothStores + SchemaRelationFactoringRetainedReconstructionBothStores; close/reopen store, move original directory, read durable artifact, reconstruct exact compiled schema/types/provenance without original source |
| Q05 | typed selection/execution | SelectedRootEventBoundarySQLitePostgres; separate internal mock lifecycle and public HTTP/WS/CLI reads, not live-provider startup |
| X01-X06 | compiling mutation qualification | separately remove clone detachment, optionality, integer-widening direction, nested recursion, flow provenance and field-path boundary; each exact named case must fail, restore after each; no mutation code in this read-only audit |

The repair-first gate must decide S01-S03 and S07-S08 before F09-F12 freeze
their verdicts. These are not waived because another family was selected.

## Supported Execution Path And Gates

Relevant order: exact authored source -> source-artifact/YAML admission ->
typed event/entity declarations -> compiled acceptance/structural projection ->
connection required-field composition -> semantic Source -> structural verify /
compiled constructor/executable admission -> run-local typed execution -> event
payload admission -> selected event/source persistence -> public readback ->
restart/fork retained artifact reconstruction -> same compiled structural owner.

Every relevant gate is classified:
- Source bytes/strict YAML/retired grammar: different admission concept, proven
  through unchanged event/field grammar and schema-refinement refusal roots.
- Type/reference/field presence resolution: same chosen family, F01-F14/C rows.
- Compiled input/copy/readback: same newly exposed detachment class, D01-D10;
  escalated before coding, not hidden behind the projection caller.
- Connection/schema provenance: lookup in F13/F14; lifecycle instance policy
  is different, C04 scalar-source/reply controls. S01-S03 duplicate scalar
  interpretation remains explicitly escalated, not labelled different wholesale.
- Definite assignment/CEL branch feasibility: different concept consuming shape;
  C02/C11/C17 prove supplied value vs declared presence is not interchangeable.
- Runtime/startup authorization, selected-store admission/transaction settlement:
  different concepts; Q02-Q05 must reach them before any retained readback credit.
- Rich concrete payload/refinement/enum/oneOf acceptance: different concept,
  canonical ToolInputSchema/event/entity validators, C09/C13/C16 and Q02
  malformed named payload refusal; shape equality never substitutes for it.
- Public presentation: different concept, actual compiled Q01/Q03 commands.
- Restart/fork reconstruction: consumer of same type owner; durable source,
  exact backend and original-path absence required in Q04, not a transport cache.

## Baseline Evidence And Tier Reachability

On clean **241bb**, fresh 17-root contracts/workflowexpr/bootverify/pinrouting
race x3: **588 passing instances, 51 root executions, zero fail/skip**.
Receipt SHA256 d4b398b55b7fab71b19bc137fc0a67179ca9e08399fa0b618e1ceefaee11f3bf.
Fresh Q02/Q03/Q05 existing roots: **9 passing instances /3 roots**, both stores,
zero fail/skip; receipt 52acfcd560eabf17789d7ccb1e7faf86b779d31829b2fd8117735089e4ad0181.
No paid provider/Docker/business messages. Separate 19-root expanded
consumer controls race x3 are recorded independently; final numbers added only
after completion. No whole Local tier or hosted run is claimed for this audit.

The external admitted detachment matrix completes **44 cells: 40 changed
retained-view counterexamples and 4 SchemaRefinements.Clone positive controls**.
Ten failing paths x length_min/length_max/range_min/range_max, including original
input capture and nested FieldPath; program exit1 is the expected defect result.
Original f2d imported counterexample failed independently 3/3; the integrated
matrix is a new source receipt, not three repetitions. Sibling matrix separately
retains load errors, actual loaded alias comparisons, named cycles and raw
carrier-only template controls. The first polluted alias-chain fixture was
corrected; its parse refusals remain setup history, not accepted evidence.

Exact current lifecycle plan: actual clean head241bb, Go1.26.8;
digest in lifecycle-plan.json, bytes SHA256
2cf02beed9b58fb2bea91bccaea08412702cd20f5d4c3c56196705f8d33b3344.
Named existing relevant proofs are selected exactly once:
- contracts-first/rest: F/C declared/compiled/collection/instance controls.
- broad-02: semanticview generated/imported/primary source controls.
- broad-03: pinrouting and accumulator field/key checks.
- broad-05: entityruntime/tools; Q04 native both-store reopen/reconstruction.
- broad-06: workflowexpr/bootverify and optional field/root checks.
- broad-12: engine typed collection/constructor/contained execution.
- serveapp-runtime: Q02 served named-record restart/fork/restart.
- hitl-releasee2e-rest: Q03 and Q05 public compiled readers/persistence.
Both required backend leaves are mandatory, not root-only package success.
Full-only catalog-replay-1/2-3 and catalog-runtime do not get claimed executed:
their relevant reconstruction law is covered by actual lifecycle Q04, not a
second catalog runner. Provisional lifecycle/no extra CI-Units is sufficient
for this maintenance family if these named proofs and approved new controls
remain selected; correctness widening may require reviewer adjustment.

Fresh expanded consumer controls completed at241bb: 19 distinct roots race x3,
**507 passing instances /57 root executions**, zero fail/skip. This includes
Q04 native artifact reconstruction on both stores; it is not a public restart.
Receipt SHA256 592d692cc538f29e5c3e350de0d91ccca6bb7bfc948ccd49fc2e0f1d1645f653.
Detachment matrix SHA256
26bf7df3a0149ab621c3caca72ff31e934ad58112172ce809401294fa1107e75;
sibling matrix SHA256
e1c00674c0e99b4791f66cd879bc61c19c6e1c0725218d184da3c7e7cc9ebe9b.
All raw receipts/probe sources/plan and collector evidence are retained under
/home/youmew/.local/state/agent-g-2447-catalog-type-preaudit/241bb-integration
and its sibling probe directory. The two complete typed censuses and both
counterexample matrices are committed adjacent to this audit for review.

Overlap checked with paginated current PR file lists: 2616/2617 overlap only
future shared spec/proof-plan/parity metadata, not the14-function owner or
primary/compiled/refinement cloners.2642 is store-constructor-only;2641 changes
CI/spec.2614 changes provider/channel consumers and normalized-event tests,
not the owner/copy files.2372 changes normalized_events and multiple reader
wrappers/accessors, not target/primary/compiled/refinement cloners. Recheck if
the correctness gate promotes those consumers; no other lane is modified.
#2638 is merged241bb: all8 reviewed files equal qualifieda29 bytes, hosted
38315817685 attempt1 SUCCESS,87 checks succeeded/6 expected skips. That is
the complete event-admission maintenance child, not new type-slice credit.

Independent exact source complexity: 253 cyclo>=30 /549 cognit>=30,
maxima186/302. Complete target remains403 lines/14 functions,26/35 and20/31;
no target delta or global improvement attributed to this read-only branch.
Extraction sizing: the original one-family/1500 gross rules and controlling
user waiver bind; every new/changed callable must be <25 cyclo, independent
ratchet non-growth, maintained tests unchanged except authorized added proof.
Semantic repair cannot be disguised as this behavior-preserving size claim.

## Parent Action, Tracking, Feasibility And Architecture

Parent sibling probe/action: **do not promote universal schema closure**.
Promote the concrete detachment and scalar-alias mismatch now for an independent
repair-first/absorb-versus-separate-child ruling. The 14-function maintenance
first-slice cannot leave these live same-concept defects blessed as baseline.
S07/S08 are explicit off-spec/contract-gap stops. No production edits until the
ruling repairs those obligations and names the correct chosen repair class.

Tracker-state decision: update current #2447 body to merged #2638 and this
integrated audit/stop, update #2407's parent note, retain #2349's model-debt note.
No old issue is superseded; no watchlist-only substitute for these concrete
manifestations. A new correctness child **may be required before coding** if
reviewer chooses repair-first separately; no unapproved child/design invented.
Required tracker repair is complete only once those records/watchlists link
this audit; the independent coding-gate outcome still must be posted afterward.

Watchlist-backed promotion check: maintenance-and-cleanup nodes
`boundary_owned_decomposition` and `invariant_suite_coverage` track full-family
factoring plus consumer-qualified regression coverage, not just hotspot counts.
Semantic-correctness node
`pack_interface_structural_satisfaction_and_channel_adapter_ownership` already
tracks immutable admitted/readback evidence, clone sequencing, normalized
trigger projection and distinct rich schema relations. Its live copy/consumer
evidence supports promoting D01-D10, not leaving one Clone patch as closure.
All three nodes must retain the sibling/type-model stops and merged2638 child
closure; parents stay open. Do not add a redundant watchlist/framework.

Intended closure level after approval: entire chosen maintenance family factored
and execution-qualified (`touched seam canonicalized`), not parent failure-class
elimination. **Chosen-class commitment: eliminate the chosen working failure
class entirely in its PR**, not improve only two high-score helpers. If a
correctness class is absorbed, all its admitted producers/getters/copies need
explicit RED->GREEN rows, not only direct Clone. This audit makes no closure.

Feasibility: pure14-function factoring is one-PR-sized, but **cannot currently
be safely closed as behavior-preserving**. A local Clone fix would still leave
root/flow declaration getters and enqueue-time compiled input capture live;
alias classification would still disagree in singleton/template consumers.
Existing copy owner can likely close the detachment cluster in one bounded
repair PR; no new lifecycle or schema framework is required. Independent gate
must decide repair ordering and exact semantics before any implementation.

Remaining parent tail estimate: concrete groups are detached bounds across
declaration/compiled/collection views, singleton/template type classification,
unsupported recursion/spelling admission, then other already tracked tool-entry,
Wave1 and provider/model families. Roughly 2-4 bounded structural children are
plausible, **low confidence until the correctness gate**; no finite estimate
for closing all #2349/#2407/#2447 obligations. Watchlist refinement only is not
sufficient for the assignable copy/alias obligations.

Architecture feedback: opaque compiled schemas coexist with exported mutable
structural/declaration carriers and manual pointer-clone sequencing. Long-run
better direction is owner-enforced detached immutable readbacks and canonical
type classification, not another generic validator/registry. **Tracking:
update #2349 plus existing watchlist now**, not residual-risk-only; no broad
type redesign is requested in this PR. A small copy-consumption repair is
likely hours-to-one-day plus proofs, high ROI; opaque carrier/type-model work
is a multi-slice estimate with insufficient design evidence, not authorized.

Blocking conditions: independent repair disposition for D and S rows; explicit
unsupported-recursion/spelling/map-key ruling or tracked split; complete new
case/consumer characterization before extraction; final overlap check on the
actual coding base; independent coding outcome explicitly on issue2447.
Current result: **READ-ONLY AUDIT / REPAIR-FIRST RE-GATE REQUEST, NOT CODING**.
