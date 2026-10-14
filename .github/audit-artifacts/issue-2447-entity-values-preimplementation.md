# #2447 entity-value normalization: characterization checkpoint

Gate: https://github.com/division-sh/swarm/issues/2447#issuecomment-6161971880.
Base: `590cdce8ba34a602eb6c0cc31d2aa08d35896f2e`.
This checkpoint adds characterization only. No production extraction or existing
test/assertion edit. #2617 reservations take priority.

## Distinct law proofs

All names below have the prefix `TestEntityValueFactoring` and live in the separate
`internal/runtime/entityruntime/value_normalization_factoring_test.go`.
Negatives use independent literal expectations, not production-generated oracles.

| ID | Root suffix | Manifestation and distinguishing negative |
| --- | --- | --- |
| V1 | CreationAndSnapshots | Initialization occurs only at creation; zero/false/empty override; invalid initializer refuses before override; snapshots never restore omitted initializers |
| V2 | PresenceAndErrorPhases | Missing differs from nil, typed-nil map/slice/pointer and explicit zero/empty; undeclared paths refuse before values; undeclared snapshot refuses |
| V3 | ScalarRepresentations | Text/alias, integer versus fraction, integer/float/lexical numeric kinds, finite numeric domain, boolean, UTC timestamp, trimmed case-preserving UUID and enum; each wrong domain refuses |
| V4 | ContainersAndDetachment | Required/optional supplied records, typed slices/arrays, nested record/list/JSON detachment in both directions; missing/extra/null nested members refuse |
| V5 | MapKeysAndContainedConsumer | Enum-key normalization, duplicate-normalized-key refusal, actual MapTypeParts contained-operation consumer, integer value admission; malformed map parts and wrong value refuse |
| V6 | RefinementOrderAndBounds | Pattern before length, inclusive numeric/length endpoints, Unicode code-point length, invalid-pattern defensive branch; each lower/upper bound and pattern has its own failing input |
| V7 | EqualityPresence | Root both-missing versus one-sided/mismatch; optional structural both-missing versus one-sided; field validation alone does not prove final equality |
| V8 | ProjectionAndCloning | Sorted declarations, undeclared lookup, selected declaration projection, fallback versus typed metadata projection, preserved metadata and detached nested values; output mutation must not affect caller |

Retain every unchanged entityruntime root, including sparse equality chains/cycles,
creation/mutation separation, map-key preservation/collision atomicity, JSON number
reload/isolation and contained-operation capabilities. No incidental Go map error
order is promised. Private clone nil-list normalization is distinct from public
typed-nil refusal. Retired spellings are not characterized as public acceptance.

## Exhaustive 40-function role / direct-caller / proof table

`NV` = normalizeValueForType; `VF` = validateValueRefinements;
`CO` = contained_operations.go; `MP` = mutation.go. External entry sites follow.
The 40 functions are an audit ceiling, not a mandate to move all small support.

| Function | Role | Direct production callers | Proof / negative |
| --- | --- | --- | --- |
| InitialValues | Creation-only explicit defaults | Initialize; NewCreationMutationPlan | V1 invalid initializer and absent reload |
| Initialize | Defaults then provided state | FlowConstructor.InitialFields | V1 override and invalid-before-override |
| NormalizeState | Snapshot admission and final root equality | Initialize; NormalizeMetadataForFlow; MP.NewMutationPlan/Validate; external snapshot readers | V1/V2/V7 omission, undeclared, one-sided equality |
| normalizeAssignedFields | Assigned fields without synthesis | NormalizeState; NewCreationMutationPlan | V1/V2 missing/null/unknown |
| NormalizeMetadataForFlow | Typed declared fields, preserved metadata | engine.BuildBaseContext | V8 fallback/typed and alias negative |
| NormalizeFieldValue | Path then type then refinements | InitialValues; normalizeAssignedFields; MP.apply; external constructor/tools | V2 phase, V3 domains, V6 bounds |
| normalizeValueForType | Recursive admitted-value dispatch | NormalizeFieldValue; self; CO.NormalizeContainedOperationValue/Key/normalizePartialObjectValue | V2-V5 null/domain/record/collision |
| normalizeJSONFieldValue | Canonical JSON clone admission | NV | V4 detachment; unchanged JSON invalid/cycle roots |
| validateValueRefinements | Pattern/length/range sequencing | NormalizeFieldValue; NV named record | V6 each independent bound/pattern |
| refinementLength | Code-point or list length | VF | V6 Unicode and no boolean length; unchanged list refinements |
| validateEntityFieldEqualities | Sparse-root equality | NormalizeState | V7 one-sided and unequal |
| validateStructuralFieldEqualities | Supplied-record equality | NV | V7 optional one-sided; unchanged required-record equality |
| validateEqualityValue | Presence then exact equality | Both equality validators | V7 missing source/target/mismatch |
| resolvedStructuralField | Exact peer declaration lookup | validateStructuralFieldEqualities | V7 optional peer; unchanged required equality |
| FieldNames | Sorted nonempty declaration projection | InitialValues; normalizeAssignedFields; external schemas/query/boot readers | V8 order and V1/V2 omission |
| DeclaredValues | Detached selected entity projection | workflowentityquery.count; pipelineEngineEntityCollectionReader.QueryEntityCollection | V8 undeclared filter and nested alias |
| FieldDecl | Exact trimmed declaration lookup | ResolveFieldPath; MP.ValidateClearTarget/apply; CO.resolveMapScopedOperationTarget; external schemas/tools | V8 missing declaration; V5 real contained consumer |
| resolveStructuralType | Canonical type resolver delegation | ResolveFieldPath; NV; CO.resolveTypePath/normalizePartialObjectValue | V4 required/extra record; unchanged unresolved/path roots |
| fieldTypeError | Existing field-path error wrapping | NV; normalizeJSONFieldValue; VF; validateEqualityValue; CO.normalizePartialObjectValue | V2/V3/V6 exact single-field errors |
| joinFieldName | Existing nested diagnostic location | NV; validateEqualityValue; CO.normalizePartialObjectValue | V4 nested refusal; V7 exact equality target |
| typeName | Scalar alias projection | NV; all catalog-aware kind predicates | V3 text alias and scalar representations |
| isNamedType | Record dispatch membership | NV; CO.validateOperationTargetKind | V4 record versus wrong carrier; unchanged partial merge |
| isEnumType | Enum dispatch membership | NV | V3 enum nonmember/nonstring |
| isTextType | Plain-text dispatch | NV | V3 exact untrimmed text and alias |
| isIntegerType | Integer dispatch/refinement eligibility | NV; VF | V3 fractional refusal; V6 numeric bounds |
| isNumericType | Numeric dispatch/refinement eligibility | NV; VF | V3 strings/NaN and kind preservation |
| isBooleanType | Boolean dispatch | NV | V3 string refusal and false presence |
| isJSONObjectType | Object dispatch/partial merge eligibility | NV; CO.validateOperationTargetKind/normalizePartialObjectValue | V4 JSON detachment; unchanged JSON object wrong-carrier control |
| isJSONValueType | Dynamic JSON dispatch/partial merge eligibility | NV; CO.validateOperationTargetKind/normalizePartialObjectValue | V2 empty/present JSON; V4 alias; unchanged JSON invalid values |
| isJSONArrayType | JSON-array dispatch/list operation eligibility | NV; CO.validateOperationTargetKind/listElementType | V4 list carriers; unchanged JSON operation capabilities |
| isMapType | Finite map shape dispatch | NV | V5 actual map and malformed parts |
| MapTypeParts | Existing map key/value shape projection | isMapType; NV; CO.resolveMapScopedOperationTarget | V5 exact consumer and invalid parts |
| isTimestampType | Timestamp dispatch | NV | V3 UTC instant and invalid RFC3339 |
| isUUIDType | UUID dispatch | NV | V3 valid case-preserving/invalid UUID |
| isListType | Existing private list-shape dispatch | NV; CO.validateOperationTargetKind/listElementType | V4 typed list/array; unchanged public retired-list refusal |
| listItemType | Existing private element projection | NV; CO.listElementType | V4 integer/record lists; unchanged nested canonical lists |
| listValues | Typed slice/array projection | NV; refinementLength | V4 both carrier forms; V6 no boolean length |
| asString | Exact string carrier only | ResolveForEntityRow; NV UUID branch | V8 nonstring fallback; V3 UUID |
| cloneMap | Detached map projection | NormalizeMetadataForFlow; cloneValue; MP.Draft/Append | V8 nil shape and bidirectional alias; unchanged mutation isolation |
| cloneValue | Existing map/list recursive clone | DeclaredValues; NormalizeMetadataForFlow; cloneMap; self; NewCreationMutationPlan | V8 map/list detachment and nil-list shape |

## External consumer map and remaining qualification

Current direct value-admission entrances (17 sites / 13 files) are:
apiv1/operator_test_setup.go:341; cliapp/test_command.go:962;
engine/context_builder.go:32; workflowentityquery/reader.go:94;
tools/executor_entity.go:82; pipeline/engine_adapter.go:601;
pipeline/flow_constructor.go:241,246,274;
pipeline/workflow_instance_activation.go:75,76;
pipeline/workflow_validation_helpers.go:123; manager/flow_activation.go:345;
bus/root_construction.go:124,133; tools/entity_schema.go:75;
tools/executor_entity_write.go:193. FieldNames/FieldDecl presentation callers also
include tools/executor_entity_query.go, tools/platform_builtin_catalog.go,
tools/role_scoped_entity_tools.go and bootverify/workflow_flow_boundary_checks.go.
Their signatures, selected reads, authorization and phase wrappers stay unchanged.

Before production extraction, finish the exact named native/public root mapping
for constructor, ordinary handler, clear+cold-reload, sparse equality, generated
read/save/contained mutation and compiled public restart. This checkpoint does
not claim those executions or final tier selection. Provisional hosted lifecycle;
add catalog-replay-2-3 only if a real value/replay manifestation is established.
Local focused only. Independent omission trials for presence, cloning,
refinement and root/structural equality remain post-extraction work, not credit
from successful pre-extraction characterization.

Proposed production files: contracts.go, value_normalization.go,
value_refinements.go, value_normalization_helpers.go; separate characterization
test file above. Keep the small support functions in place unless they belong to
the extracted value law; no gratuitous relocation. No identity/path resolver,
mutation/contained-operation execution, catalog grammar, SQL/persistence,
spec/CI/plan/baseline/registry edits. A correctness counterexample stops for
repair-first classification, not an acceptance frozen into these tests.

#2617 owns pipeline/flow_constructor.go and manager/flow_activation.go, both live
consumers. Do not edit them. Recheck its latest head/overlap and actual landed
constructor semantics before qualification. #2447/#2407/#2349 remain open.
