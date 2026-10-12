# Pre-Implementation Coverage Audit: #2447 Schema Relation Family

Read-only candidate integrated onto actual master770b99f93, effective go1.26.8,
2026-10-12. Baseline executions below used568dd9ab0; all relation/entry/registry/
channel-wrapper/constructor owner bytes are unchanged across568..770.
No production, maintained-test, schema, proof-plan or baseline change. Disposable
counterexamples are removed; their exact source/evidence is retained below.
#2629 merged770b99f93 after green lifecycle38226277770. Its reviewed/merged trees
are identical508f28fe; this carries that source proof, not a fresh master run.
Its branch and qualification were untouched by this audit.

**Coding CLOSED.** Request an independent first-slice gate AND explicit
disposition of the newly proven semantic gap tracked as #2630. No authorization
to preserve its off-spec acceptance as a valid characterization. Proposed
maintenance extraction cannot silently absorb a semantic repair either.

## Class, Governing Context And Scope

Category: high-risk behavior-preserving maintenance with a separately escalated
semantic counterexample. Observed maintenance symptom is the finite relation's
mixed ordered decisions/recursion: validateToolSchemaSubset cyclo/cognitive27/29,
validateToolSchemaObjectSubset25/50. This is not a reported production failure
fixed merely by extracting a helper. The separately proven semantic symptom is
an accepted relation with a concrete source-valid, target-invalid value.

Chosen maintenance class: the complete monolithic finite ToolInputSchema
assignability-admission family, not just its first high-score line. Immediate
maintenance parent is complex tool/schema admission; broad maintenance tracker
#2447 and proof tracker #2407 remain open. Broad semantic concept is sound
recursive admitted-schema implication; #2630 tracks the concrete counterexample.
#2349 retains representation/model debt; #2250 explicitly excludes this schema
family and is not its semantic parent. Proposed framing is narrower-but-acceptable
as a first slice ONLY if reviewer-g explicitly resolves the semantic split.

Binding authoritative clauses at this source:
- platform-spec.yaml engine.hitl_channel_pack_interface.contract,11375-11442:
  opaque admitted schemas/tool execution, pre-authority result-path proof and
  one canonical assignability owner across channel/result/response proof.
- engine.hitl_channel_pack_interface.finite_directional_compiler.rules,
 11943-11972: complete admitted source-domain proof, recursive requiredness,
  additional properties, array/items/bounds, enums/patterns/scalar bounds;
  reject unproved relations; intersection is NOT assignability.
- Its selected_constraints,11974 onward: channel authoring bounds use a distinct
  intersection operation, not a substitute direction proof.
- #2447 issue/thread, latest6117802058 gate: one complete family, original
  <=1500 gross production lines, net<=0, every changed/new callable cyclo<25,
  exact independent ratchet, characterization before extraction, existing tests
  unchanged. The historical #2509 batch waiver is NOT inherited.

IMPLEMENTER_GUIDELINES.md and SEMANTIC_DRIFT.md reread. Spec governs; no
compatibility, vendor dependency, generic validator/walker, grammar or public
owner change. No spec delta is planned for pure extraction. A semantic correction
requires its own approved boundary and authoritative spec decision first.

Production boundary: only contracts/tool_input_schema_relation.go,276 lines.
All12 functions are one family: ValidateAssignableTo; validateToolSchemaSubset;
validateToolSchemaStringSubset; validateFiniteToolSchemaSourceEnum;
toolSchemaEnumSet; admittedToolSchemaInt/Float;
validateToolSchemaIntBoundsSubset/FloatBoundsSubset;
validateToolSchemaObjectSubset; admittedToolSchemaAdditionalProperties;
toolSchemaStringSet. The enum helper scores9/14, bound helpers7/6, remaining
helpers<=6; no other relation body is excluded. Real owner is the exported
finite implication law, not the first helper/file encountered.

Proposed decomposition separates ordered early relation gates, kind-specific
proof and object declared/additional-member phases using small private helpers
inside this owner. Preserve nil laws, conservative oneOf behavior, integer-to-
number widening, complete exact errors/paths/first-error order, admitted accessors,
canonical enum bytes/order, nonmutation and all caller wrapping. No speculative
general implication algorithm. Expected below1500 gross; net-neutral feasibility
must be verified on the actual diff, not by compressed formatting. Stop for a
user-as-lead size decision if genuinely necessary positive net remains.

## Full Relevant Execution Paths And Gates

| Path in order | Earlier gates and classification | Reachable relation / later gates |
| --- | --- | --- |
| Channel source -> accepted interface/trigger/connector inventories -> CompileChannelInventory/CompileChannel -> operation/event/learned mapping -> immutable SatisfactionPlan -> runtime private activity | Source lexical/typed schema admission, dependency identity/role/effect and mapping cardinality: different concepts, ChannelSchemaAdmission and ProductionCompilerFailsClosedAcrossChannelContractPhases controls. | Input/output/each/event/learned use this same chosen relation; plan generation/readback/private activity dispatch and connector output acceptance are different concepts, ProductionCompilerAcceptsStructurallyDifferentTighterSatisfier, RuntimeActivityTargetPinsCompleteCompiledPlanGeneration and compiled Q proofs. |
| Tool options -> NewToolSchemaEntry or WithSchemas/WithCompiledResult -> source/output definition admission -> path guarantee/overlap -> compileToolResultProjection -> published immutable entry -> direct/durable HTTP result projection | Definition and guaranteed source/target paths/overlap: different concepts, ToolSchemaEntryRejectsCompiledResultPathsOutsideAdmittedSchemas; option order/revalidation controls. | Each mapped source/target invokes chosen relation before authority; final required-output coverage and result inhabitance are different concepts, same path test plus registered-tool execution Q proof. #2630 proves relation currently grants a bad entry here, NOT an actual paid-provider execution. |
| Declared flow/input -> effective compiled structural/event schema -> entity constructor presence/type/internal-initial checks -> ValidateCatalogFieldSupply -> catalog map-key proof and value implication -> constructor eligibility -> selected runtime | Presence/type/collision/assignment: different concepts, FlowConstructorEligibilityUsesAssignmentChecker, SuppliedInitialFacts, ProjectionAdmission. | Catalog-field and map-key calls use chosen relation; required assignment/runtime entity initialization are different concepts, CatalogSupplyAdmission and compiled constructor Q proof. Unconstrained json/jsonb target fast path is explicit opaque target semantics, not a relation bypass. |

Observed high-score helper is an entry point, not the audit boundary. Every
known consumer entrance is accounted below, including caller gates that may
prevent malformed/refined values from reaching the relation. No final-endpoint-
only or shared-owner-only proof credit; Q proofs must show actual ordered gates.

## Exhaustive Known Owner Consumption

Repo-wide ValidateAssignableTo/helper/schema-implication search finds FOUR
production direct sites in THREE files. TestSemanticOwnershipProducerConsumer-
LedgerIsClosed independently enforces these exact path/count facts. No separate
finite ToolInputSchema implication interpreter found. All are already consumers;
none needs migration to a new owner. Production callers stay untouched.

| Consumer/seam | Status / actual canonical owner consumption | Exact planned execution |
| --- | --- | --- |
| packs/channel_relation.go validateDirectionalRelation | Already consumes ValidateAssignableTo after pointer-presence admission; one direct site. | Pointer-negative characterization plus ProductionCompilerRejectsRecursiveDirectionalAndCardinalityGaps. |
| packs/channel.go input1883 / output1917 / each2044 / normalized event2093 | Already consumes through the above wrapper; four distinct mapped paths. | New channel_schema_relation_factoring_test.go tests each path's incompatible direction and unchanged valid plan; existing recursive gap and exact normalized identifier controls. |
| packs/channel_opaque_reference.go learned_destination98 | Already consumes same wrapper, not a second relation. | ChannelLearnedDestinationRelationAdmissionAndGeneration plus new exact direction/error/generation control. |
| contracts/tool_http_execution.go compileToolResultProjection980 | Already consumes ValidateAssignableTo for every guaranteed result field; one direct site. | Existing path, option-order and WithSchemas controls; new constructor/WithSchemas/WithCompiledResult error-precedence and projection-byte controls. |
| contracts/tool_schema_entry_value.go constructor291 / WithSchemas504 / WithCompiledResult629 | Already consumes through compileToolResultProjection; three public producer entrances. | All three separately executed by the new relation-consumer characterization, not assumed from one helper. |
| contracts/schema_registry.go ValidateCatalogFieldSupply367 | Already consumes after separate key-schema proof and immutable source/target admission; one direct site. | FlowConstructorProjectionAdmission plus new exact field/refinement/error controls. |
| contracts/schema_registry.go validateCatalogMapKeySupply389 | Already consumes for map keys; recursively traverses items/additional/declared members before value projection; one direct site. | FlowConstructorCatalogSupplyAdmission enum/unbounded map key/nested map; new nested key first-error controls. |
| engine/flow_constructor.go seedConstructorAssignment51 | Already consumes catalog-field owner; paired nominal structural gate is NOT refinement implication. | FlowConstructorCatalogSupplyAdmission, ProjectionAdmission and new compiled constructor proof. |
| contracts/tool_input_schema.go/value/admission/projection | Different concept: lexical/definition/value admission and immutable representation. | Existing FactoringSchemaDefinition/Value/ProjectionIsolation, PropertyNames and OneOf controls. No source edit. |
| contracts/catalog_type_reference.go StructuralCatalogTypeAssignable; workflowexpr/accprojection | Different concept: resolved nominal shape/presence and sole integer-to-number widening, not full constraint implication. | StructuralCatalogTypeAssignable body plus constructor CatalogSupplyAdmission/ProjectionAdmission and accumulator mismatch controls; constructor THEN invokes this chosen relation. |
| packs/channel.go intersectSelectedConstraint / ToolInputSchema.IntersectBounds | Different concept: compute supported selected authoring bounds; does not prove source-domain inclusion. | Normative operation inventory and differential tighter-satisfier tests; authoritative selected_constraints clause. |
| contracts/tool_http_execution.go validateCompiledToolTemplateShape | Different concept: partial static template/value inhabitance; fully static literal calls schema.Validate, unresolved dynamic template is not a proved source-domain relation. | ToolSchemaEntryRejectsStaticallyImpossibleResponseMappingsBeforeAuthority and direct/durable registered-tool execution. Do not claim it is a fifth ValidateAssignableTo call. |
| eventschema acceptance/inhabitance and generated_schema_closure | Different concepts: concrete payload/witness acceptance and generated closedness, not implication. | PropertyNames/OneOf, generated closure and typed schema controls. |
| llm/provider_tool_schema / request/MCP/target readers | Different concept: provider wire-syntax/subset admission, separately #2629. | Existing provider and immutable definition controls; no changed file or cross-PR qualification credit. |

Old non-authoritative producers/readers: NONE found inside chosen implication
family. No dead pager, decoder or compatibility owner is preserved/introduced.
Old monolithic bodies are extraction candidates only, not semantic invalidation.
#2630's constrained-any shortcut is a real removal/repair candidate under its
semantic gate; no new test may bless it as a valid compatibility path.

## Manifestation And Exact Proof Plan

All rows are PLANNED candidate proof, not earned closure. New characterization
must be committed and pass before extraction; existing tests remain unchanged.
TC means planned TestToolSchemaRelationFactoringCharacterization with the named
case; CC means planned TestToolSchemaRelationFactoringConsumers. These exact
new roots/files are disjoint from #2629. Defensive private impossible states
must be labeled as such, never as public admitted-source reachability.

| ID | Known manifestation | Planned exact proof |
| --- | --- | --- |
| P01 | Zero source/target | TC/zero_source, zero_target: exact subject/error before recursion. |
| P02 | Identical oneOf / truly unconstrained any target | TC/oneof_identical, oneof_free_any: exact nil, unchanged canonical bytes. |
| P03 | Nonidentical oneOf / constrained-any target | TC/oneof_unproved and oneof_constrained_any: exact conservative refusal, no generic union algorithm. |
| P04 | Target equality identity | TC/equality_same, equality_missing, equality_different, target_no_equality: precedence before enum/type. |
| P05 | Finite source enums | TC/finite_enum_source with typed numeric/text/null values admitted through actual owner; every value consumed in declaration order. |
| P06 | Target enum inclusion | TC/enum_subset and enum_missing_first: exact canonical-byte identity and first missing value. |
| P07 | Unbounded source into target enum | TC/target_enum_unbounded_source: same exact refusal. |
| P08 | Enum defensive errors | TC/source_enum_constraint_defense and target_constraint: bounded private impossible-carrier controls, explicit non-public credit. |
| P09 | Unconstrained any target | TC/free_any_target across admitted kinds; constraints checked before broad acceptance. |
| P10 | Type equality / integer-to-number widening | TC/kind_equal, integer_to_number, reverse_refusal, incompatible_types. |
| P11 | Boolean/null and unsupported defensive kind | TC/boolean, null, private_bad_kind: unreachable private corruption not a new admitted kind. |
| P12 | String min/max | TC/string_min and string_max: source absent/broader/equal/narrower matrix. |
| P13 | String patterns and formats | TC/pattern and format: exact identity or conservative refusal, no regex implication heuristic. |
| P14 | Numeric min/max | TC/numeric_min and numeric_max: both bounds, widening and first error. |
| P15 | Array length | TC/array_min and array_max: exact []/length paths and precedence. |
| P16 | Array item recursion | TC/array_target_items, source_items_absent, nested_items: recursive first error. |
| P17 | Both sides constrain property names | TC/property_names_relation: recursive propertyNames subject. |
| P18 | Closed finite source property names | TC/property_names_closed_good and closed_bad: target.Validate over exact source names. |
| P19 | Open unconstrained names | TC/property_names_open: rejection before required/member decisions. |
| P20 | Required target fields | TC/required_equal, stronger_source, missing_required_first: declaration order. |
| P21 | Both sides declare a property | TC/declared_nested: canonical source-name order and nested path. |
| P22 | Source member absent in target | TC/extra_forbidden and extra_schema: closed target or schema-valued target policy. |
| P23 | Unconstrained source additional -> genuinely free-any target property | TC/additional_free_any: unchanged valid law only. Constrained-any negative is B01/#2630, not blessed as valid. |
| P24 | Schema-valued source additional -> new target property | TC/additional_source_schema: exact recursive relation including integer-to-number. |
| P25 | Additional policy closure | TC/source_closed, target_closed, target_open: order after declared members. |
| P26 | Both additional schemas / absent source schema | TC/additional_nested and additional_unconstrained: exact .* subject/refusal. |
| P27 | No mutation / repeated deterministic invocation | TC/repeat_nonmutation: before/after CanonicalJSON/Equal/projection facts, same exact result twice. |
| P28 | Multiple-invalid first-error order | TC/first_error_order: oneOf -> equality -> enum -> kind -> kind-specific; names -> required -> declared members -> additional members/policy. |
| C01 | Channel input relation | CC/channel_input plus existing recursive directional gap leaf; exact error and valid plan bytes. |
| C02 | Channel output relation | CC/channel_output plus existing output scalar/object gap leaves. |
| C03 | Each-item relation | CC/channel_each: actual ordered projection and recursive source item identity, not only cardinality failure. |
| C04 | Normalized event field relation | TelegramChannelCompilerConsumesExactNormalizedIdentifierSchemas plus CC/channel_event range/pattern rejection. |
| C05 | Learned destination relation | ChannelLearnedDestinationRelationAdmissionAndGeneration plus CC/learned_destination exact generation/readback. |
| C06 | NewToolSchemaEntry compiled-result relation | Existing path controls plus CC/result_constructor: validity/refusal and full projected bytes. |
| C07 | WithSchemas revalidation | WithSchemasRevalidatesCompiledResultSource plus CC/result_with_schemas. |
| C08 | WithCompiledResult revalidation | CC/result_with_compiled_result, not inferred from constructor. |
| C09 | Catalog field/value implication | FlowConstructorProjectionAdmission plus CC/catalog_field_refinements. |
| C10 | Catalog map key nested implication | CatalogSupplyAdmission plus CC/map_key_nested items/additional/member traversal, exact first-error wrapping. |
| C11 | Full constructor eligibility path | Existing effective-schema/assignment/supplied-facts roots plus CC/constructor_relation: earlier gate success then relation refusal. |
| Q01 | Compiled structural CLI | New TestSchemaRelationFactoringCompiledReaders: real verify --portable/describe on admitted directional source, malformed directional source refuses exact public finding; no readiness/provider credit. |
| Q02 | Compiled host/public tool path | Existing TestWorkspaceMCPCompiledHostConformance and new compiled relation-consumer fixture; actual MockOnly host HTTP MCP/tool mapping/settlement, not paid-provider or Docker proof. |
| Q03 | Both-store selected-store execution | New TestSchemaRelationFactoringBothStores through sanctioned native store construction and admitted source: valid mapped projection/settlement; invalid directional source refused before provider/effect facts. No SQL construction/readback bypass. |
| Q04 | Fresh source reconstruction | Same immutable schemas/mapping projection/generation through fresh reconstruction on each store; existing compiled H/mock restart/kill is separately credited, not public serve or real Anthropic continuation. |
| Q05 | Generic failing proof | Four separately compiling check-removal mutants (requiredness, enum, bounds, additional-member recursion), each fails its named TC/CC assertion and is restored. Non-discriminating mutant stops for reviewer, never same-helper credit. |
| Q06 | Coverage/ratchet/pre-push guards | Exact base/head pinned complexity; every new callable<25; gross/net; all new roots exactly once in reviewer-selected tier; all touched-package census/static/ownership tests listed before push. No baseline increase. |
| B01 | Constrained-any enum unsoundness | split / escalated #2630: direct relation and real compiled-result constructor counterexamples each RED3/3; concrete source-valid/target-invalid witness. No public/backend/network proof claimed. |

Total46 planned rows: P28+C11+Q6+B1. Required supported proof is Q01-Q04 even
though this is maintenance, not a public parity feature. No loose identity or
shared-helper claims; compiled CLI, real HTTP MCP, selected-store and retained
internal harness proof retain distinct credit. Actual class cannot be called
semantically sound while B01 remains open; intended closure is maintenance-family
elimination only after reviewer explicitly approves that split, or a separately
amended semantic gate before any combined coding.

## Tracker, Promotion, Architecture And Stop Decisions

Tracker repair: #2630 created with exact counterexample/class/consumer/spec;
#2447 issue header/thread updated with this audited candidate and split decision
request; existing boundary_owned_decomposition and pack-interface watchlist nodes
refined before coding. No new node or POTENTIAL_ISSUES entry.

Watchlist promotion check: existing pack-interface node already tracks directional
relations across input/context/each/event/output, patterns/enums/additional policy,
catalog representation and immutable plan consumers. That evidence enlarged the
audit to all four direct sites and the catalog constructor, not just channels.
Sibling structural shape, intersection, concrete inhabitance and provider-wire
validation are distinct named laws with execution above. No duplication warrants
a generic framework or whole-parent refactor. B01 is live same-concept behavior,
so it is promoted to a concrete semantic tracker rather than watchlist-only.
Reviewer must decide repair-first versus explicit split; no silent deferral.

Parent action: retain complete relation-family first-slice proposal, no broader
#2447/#2407/#2349 closure. Current finite maintenance tail has relation and
ToolSchemaEntry.validate30/32 families plus peer provider continuations/catalog
groups; medium grouping confidence, low effort/count confidence, no honest finite
whole-parent closure estimate. Responses SSE remains separately unresolved.
#2509/#2626, #2628 and #2629 are merged, not pending dependency gates. #2629's
semantic receipts are not this candidate's relation/extraction proof.

Closure feasibility: twelve-function maintenance owner can plausibly close in
one PR, all direct consumers already consume it; no second local implication
interpreter remains to migrate. A local high-score fix alone would NOT close
the semantic parent: B01 remains real. Stop if another live same-concept
consumer, spec contradiction, size violation or changed gate model is found.

Architecture smell: an admitted schema Kind()==any is not a proof that its domain
is unconstrained. Long-run direction is explicit conservative relation evidence
from the existing immutable schema owner, never caller kind shortcuts or a new
general solver. Concrete correction belongs to #2630; representation/design
feedback to #2349 and existing watchlist only. Rough1-2 days for bounded semantic
repair/consumer proof,0.5-1 day extraction plus proof; high correctness ROI, low-
confidence estimates until semantic census/gate. No new architecture framework.

## Overlap And Qualification Proposal

Open heads checked: #2629bbf24b1e6, #2612dee2af006, #26174c220bff3,
#26167933779be, #26145cd44ae0e, #2624d5a9209fa. None touches target relation
file or proposed NEW test/audit files. C's active2413 worktree has bus/planner/
receiver-test/spec dirt, not this owner; E2612 and F2577 worktrees clean at census.
Consumers/spec/proof-plan are integration surfaces, not licensed edit overlap:
no edit to contracts/schema_registry.go, tool_http_execution.go, channel.go,
channel_relation.go, engine/flow_constructor.go, platform-spec.yaml or planner
is proposed by pure maintenance. Recheck before implementation. #642 catalog/
selected execution remains separate supported-context proof; not an absorbed
fork/publication owner refactor.

Proposed CI-Tier lifecycle/no CI-Units, Local focused; reviewer sets final tier.
Contracts/packs/boot/runtime/public host consumers are selected in lifecycle;
no identified full-only soak/native-unused root is affected. Do not duplicate a
local whole tier or use server2 for audit. Before EVERY push run census/ratchet/
static guards for each touched package, not after hosted failure: contracts
Guard/Census/Inventory/Registry/Ownership tests; packs compiler source/ownership
guards; runtime SemanticOwnershipProducerConsumerLedgerIsClosed; CLI census if
CLI proof touches that package; testplanning+testcatalog census if fixtures/roots
change; store PersistenceAuthority tests if a store fixture changes. Ask reviewer
about new census entries at pre-push checkpoint. Audit must list actual commands
and results; no generated registry-only or guard-weakening correction.

## Read-Only Baseline Evidence / Limits

Unchanged master baseline through swarm-test admission, PG16 override,
race3: contracts1.082s, packs7.190s, bootverify3.577s, runtime16.142s; no fail/skip.
Known relation/oneOf/propertyNames/result-path/order/revalidation/template/channel
direction/differential/generation/constructor-assignment and exact four-call-site
ownership-ledger roots. Receipt relation-preaudit-baseline-race3.jsonl SHA256
fcce4e8b51332d214e4866a8a255588da0ff785a35394f7049ed82bd8d7378ba.
Full existing catalog-supply/projection/supplied-facts/effective-schema controls
race3 pass5.808s, relation-preaudit-constructor-race3.jsonl SHA256
229e0e9c94ba4ae74a954315f8fde203caa8f5a7ff7e22f1b5b08adc90ee90ce.
These baseline passes do not erase B01 or qualify an extraction/public journey.

B01 probes each fail3/3 on unchanged production after witness controls pass;
receipt relation-constrained-any-consumer-counterexamples.jsonl SHA256
9ab83b1e0703eb6f05cd8938a9e17c228b07b137a7d76f8ea7657a1c90074aee.
Only temporary probe source was added then removed. Exact reproduced core:

```go
source := MustToolInputSchema(ToolSchemaObject)
target := MustToolInputSchema(ToolSchemaObject, ToolSchemaProperties(map[string]ToolInputSchema{
    "mode": MustToolInputSchema(ToolSchemaAny, ToolSchemaEnum("approved")),
}))
witness := map[string]any{"mode": "unapproved"}
// source.Validate(witness) == nil; target.Validate(witness) != nil.
// The failing assertion expects relation refusal, but this returns nil:
source.ValidateAssignableTo("audit constrained any", target)
```

The second exact probe wraps source/target as required payload/projected object
fields, admits an HTTP tool with WithToolSchemas(object, sourceOutput) and
WithToolCompiledResult({projected: result.payload}, targetOutput), and expects
constructor refusal. NewToolSchemaEntry instead returns a nonzero admitted entry.
No paid call, live credentials, Docker, backend execution or public exploit is
credited. No candidate mutation, new characterization, whole-tier or closure
proof yet. Independent coding gate and semantic split condition remain pending.
