# Read-Only Pre-Audit Intake: Complete Catalog Structural-Type Family

Agent G,2026-10-12. Proposal:
https://github.com/division-sh/swarm/issues/2447#issuecomment-6134301267
Source f2d0538c741a87a1b0cd40fe025fd1fc6a1437fc,Go1.26.8.
This original intake is historical; the integrated audit on merged master
241bbbdde is in issue-2447-catalog-type-amendment.md. Coding remains frozen.
No production/new-test/spec/workflow/plan/baseline edit; #2638 is now merged.
Re-read IMPLEMENTER_GUIDELINES/SEMANTIC_DRIFT and current2447/2407 ledgers.

## Class And Binding Context

Proposed category: high-risk behavior-preserving admission maintenance.
Entry points: named catalog resolution26/35 cyclo/cognitive and admitted JSON
structural lowering20/31. Chosen tentative maintenance class: the WHOLE14-function,
403-line catalog_type_reference.go family. Both origins produce the same
ResolvedCatalogType/ResolvedCatalogField structural carrier. Neither hotspot
alone is an honest boundary. Private ordered phases only; callers and richer
validation owners remain unchanged unless independently re-gated.

Immediate semantic parent: recursive structural shape/field-edge admission,
projection and consumption across event/entity/tool sources. Maintenance parent
2447 and proof parent2407 remain OPEN;2349 retains type-model debt.2250 is not
this semantic parent. Preliminary parent action: keep a coherent first slice,
not universal schema correctness/parent closure. Tool entry, Wave1 decoder,
provider continuation and Responses SSE remain separate owners/obligations.

Exact governing spec clauses at this source:
- contract_formats.event_schema.recursive_field_presence,L1180: one recursive
  field-edge carrier for named/event structures and all consuming compilers.
- design_principles.no_structural_primitive_elides_type_discipline,L3828:
  preserve named sources/explicit mapping and hard invalidity on unknown fields.
- handler_specification.expression_context.namespaces.payload.read_model:
  recursive_maps,shared_readers,structural_identity,L5255-L5257. Structural shape
  and presence, not diagnostic naming, own record reuse; unknown/open values
  cannot obtain typed fields through a dynamic fallback.
- flow_model.compiled_event_schema_projection: immutable admitted event
  acceptance/structural evidence; downstream callers consume retained facts.

The source/admitted-schema precondition must be audited before characterization.
Unvalidated raw maps/catalogs are not automatically supported product inputs.
Full JSON-schema refinements/oneOf/value subset proof remain the separately
owned ToolInputSchema/compiled acceptance laws, not this shape comparator.
Structural equality does not grant payload acceptance or authorization.

## Full Function Population

Scores are pinned collector facts on0bfc; this entire file is byte-identical
on currentf2d. Final extraction must measure current committed base/head anew.
Every row requires characterization or named execution proof before closure;
the baseline below is not complete direct coverage of every row.

| Original function | Cyclo/cognitive | Required characterization |
|---|---:|---|
| ResolvedCatalogType.Clone | 5/4 | Recursive list/map/record detachment, including mutable refinement bounds; stop finding below |
| StructuralCatalogTypesEqual | 17/16 | Shape/field-edge equality, order/name independence, absent nested carriers and type distinctions |
| StructuralCatalogTypeAssignable | 3/2 | Exact equality plus integer-to-number only; no number-to-integer or cross-kind credit |
| StructuralCatalogTypeSyntax | 13/15 | Stable diagnostic spelling and sorted record fields without semantic naming credit |
| ResolvedCatalogType.Field | 3/3 | Exact admitted field lookup, omission and returned carrier handling |
| ResolvedCatalogType.FieldPath | 5/6 | Nested records, empty segments, absent field and no map-key-as-record lookup |
| ResolveJSONSchemaStructuralType | 1/0 | Real admitted-schema entrance versus unvalidated descriptor precondition |
| resolveJSONSchemaStructuralType | 20/31 | Scalars/list/map/object, recursive field presence, sorted fields and nested error paths |
| CatalogTypeReference.Empty | 1/0 | Blank reference only; no inferred semantic authority |
| CatalogTypeReference.Resolve | 1/0 | Original declared catalog and same reference entrance |
| CatalogTypeReference.ResolveReference | 1/0 | Fresh resolving state and exact reference/diagnostic handling |
| resolveCatalogTypeReference | 26/35 | Alias/enum/named/list/map resolution, unknown/empty alias refusal and cycle preconditions |
| ResolveEventFieldType | 5/4 | Declaring flow/event/field ownership and detached type catalog |
| ResolveExecutableNodeEventFieldType | 8/6 | Exact executable-node evidence and separately owned platform lookup boundary |

## Initial Systematic Consumer Census

Exported carrier/entrance/relation census finds36 production files, listed in
the retained structural-consumers.txt. No second definition of this structural
carrier or resolver was found. Typed-method aliases and each producer's admitted
input precondition still require completion; no exhaustive gate claim yet.

| Known consumer family and files | Current owner relationship / planned proof |
|---|---|
| compiled_event_schema.go; semanticview/event_schema.go | Consume JSON structural lowering and Clone; real compiled fields/acceptance/refinement readbacks, including confirmed stop below |
| semanticview/source.go,source_artifact.go,compiled_activity_source.go,entity_structural_type.go | Project retained event/entity types through existing semantic Source; preserve exact scope and imported/generated origins |
| schema_registry.go,schema_refinement_validation.go,workflow_contract_wave1_accessors.go | Consume declared catalogs; existing recursive named-field/refinement/type-meaning controls and exact catalog detachment |
| collection_projection.go,standalone_projection_type.go | Resolve and clone exact collection item/key shape; existing list/map admission/order/digest witnesses |
| workflow_join_plan.go,workflow_fan_out_semantics.go | Resolve named collection/result fields through the same carrier; planned actual typed collection/join execution and child/cardinality controls |
| workflow_instance_resolution_source.go | Consume exact declared source/receiver catalogs; existing alias/UUID/mismatch and integer-constraint controls |
| workflow_contract_emit_lowering.go,durable_data_catalog.go | Consume retained structural fields; explicit emit mapping/file-field checks remain separate richer owners |
| workflowexpr/structural_expression.go,structural_predicate.go,data_expression.go | Register/compare/clone structural roots and sink types; imported dictionary, unknown traversal, optional root matrix, lexical-scope and record-name controls |
| bootverify/workflow_input_alignment_checks.go,workflow_stage_gate_checks.go,workflow_expression_checks.go | Walk/check retained payload/entity/item fields, not raw schema re-admission; reader/constructor/optional-presence controls |
| bootverify/wave1_entity_contracts.go,workflow_node_state_schema_checks.go,workflow_cross_surface_named_type_checks.go,workflow_join_checks.go | Resolve declared entity/node/join type references; full named-type, typed counterpart and joined result controls |
| engine/types.go,executor.go,flow_constructor.go,entity_assignment.go | Carry and compare resolved shapes; flow constructor also validates field supply through its distinct refinement owner, so shape equality is not complete acceptance |
| core/pinrouting/connect_route_plan.go | Request/reply structural compatibility; ordinary routing/instance/key ownership stays outside this extraction |
| accprojection/resolver.go | Exact collection/accumulator type alignment; projection mappings remain a distinct authority |
| entityruntime/assignment.go,contracts.go | Resolve typed nested entity fields; normalization/persistence remain separate, unchanged owners |
| tools/platform_builtin_catalog.go | Resolve catalog-backed builtin contracts; execution/tool identity stays outside the resolver |
| pipeline/workflow_executable_readers.go | Lower admitted activity input property schemas; fixed executable-reader census and compile-before-run remain binding |

No consumer is proposed for migration; all must remain on the canonical owner
and have named execution proof. Rich provider schema validation/ToolInputSchema
assignability, authored YAML decoding, CEL presence/control-flow checking,
concrete payload validation and selected-store lifecycle are different concepts,
not duplicate shape resolvers. Their exact sibling proofs remain to be mapped.

## Baseline And Correctness Stop

Existing untouched contracts/workflowexpr/bootverify/pinrouting controls:
`go run ./cmd/swarm-test -- <four packages> -run <17 exact roots> -race -count=3
-timeout=5m -json`. Complete receipt baseline.jsonl:588 passing instances,
51 root executions, zero failures/skips; SHA256
14f43423a3d86e2188f8c60d0697bf3fe0c5b1ad83635c08f18d498feee28737.
Names/arguments are retained in baseline.stderr; these are component baseline
receipts, NOT extraction, mutation, public both-store or whole-tier proof.

**D01: confirmed readback detachment failure on untouchedf2d.** A disposable
external audit program imports the unchanged contracts package and calls its
real CompileImportedEventSchema with a valid required text field/length min1.
StructuralType -> Field returns a bound pointer. Changing that returned minimum
to99 makes a fresh StructuralField read report99, while AcceptanceSchema still
reports minLength1. Expected: later retained read remains1. The existing
SchemaRefinements.Clone control detaches correctly. This is not a malformed
schema acceptance claim and does not demonstrate changed runtime validation.

Three independent process invocations compile and fail the exact retained-view
assertion3/3; all identical outputs SHA256
729db072780512a24bee195eccfef0546705008ffe78b330ba2086f4bfeda8de.
Probe source SHA256
fb266e04290d20d8255f58e715560b27c35e2028cdee6011b0a390e35d7fe887.
The program lives outside the source worktree, with a local module replacement
pointing at frozenf2d; no production/test change, DB, Docker, paid provider or
network business invocation. Initial GOTOOLCHAIN=local setup refused Go1.25.5;
that is separate setup evidence, not a defect repetition. All three actual
probes explicitly use the source's Go1.26.8 and GOWORK=off.

Owner cause: ResolvedCatalogType.Clone copies each whole field and recurses into
its Type, but leaves Length.Min/Max and Range.Min/Max refinement pointers shared.
CompiledEventSchema.StructuralType/StructuralField expose that path. Direct
named resolution and compiled structural-field construction also copy the
refinement carrier; their producer/input and readback obligations need a full
same-concept sweep before defining the semantic repair class. Existing
SchemaRefinements.Clone is the candidate bound-detachment owner, not a new
framework. No production repair is authorized by this intake.

**Stop/decision:** do not characterize changed retained readback as correct.
Request reviewer-g's repair-first/absorb-versus-split disposition before the
maintenance coding gate. No broader runtime failure or parent closure claimed.

## Outstanding Before The Coding Gate

- Complete typed Clone/Field/FieldPath and all refinement-producing/copying
  consumer census; capture every mutable bound, nesting and readback producer.
- Separate admitted-origin evidence from defensive invalid catalogs/maps;
  classify named recursion/dynamic forms against their exact earlier gates.
- Publish the full manifestation matrix with exact proof for each supported
  surface, consumer and compiling mutation; no loose shared-path attribution.
- Name the actual compiled verify/describe and both-store execution/restart/
  reconstruction fixtures, complete execution path and prior admission gates.
- Generate exact lifecycle selection and mandatory backend-child obligations;
  proposed lifecycle/no extra units is not a verified ceiling yet.
- Record reviewer-g's correctness disposition and repair issue/tracker state
  before coding. Existing boundary_owned_decomposition/invariant_suite_coverage
  nodes map this maintenance slice; deep copy-model feedback stays under2349
  unless the reviewer creates/names a concrete correctness child.

Proposed Local focused plus race/owner/census/structural/debt/complexity and
compiled/both-store supplements, no duplicate whole Local tier. No full run or
new hosted run is requested at intake. Final reviewer decides the ceiling.
Size targets follow the controlling user waiver; changed/new callables<25,
unchanged maintained tests, characterization-before-extraction and exact ratchet
remain required. Any semantic fix requires its own explicitly approved class,
binding spec context and RED-to-GREEN proof, not this maintenance assumption.

Provisional closure commitment: one PR closes the entire chosen maintenance
family, not just the two hotspots; no in-class tail accepted. Feasibility is
withheld until D01 and its consumer census are ruled. Broader tail includes
tool-entry/Wave1/provider families and Responses SSE; no fabricated finite
parent PR count. No generic validator, compatibility, vendor or baseline waiver.
