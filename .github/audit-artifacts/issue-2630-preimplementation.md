# Pre-Implementation Coverage Audit: #2630

Read-only audit, 2026-10-12, actual master
770b99f93019c8cf26fea3fcb4b579e37fddff99, Go go1.26.8.
IMPLEMENTER_GUIDELINES.md and SEMANTIC_DRIFT.md reread.
No production, maintained-test, spec, proof-plan, registry or baseline change.
Disposable probe source is preserved in issue-2630-counterexamples.md and
removed from the Go package. Independent coding and sequencing gate PENDING.

## Plain-English Correctness Summary

Assignability means: every value allowed by the source must also be allowed by
the target. The source below permits any object property and value:

```json
{"type":"object"}
```

This target permits an optional mode property, but only with the value approved:

```json
{"type":"object","properties":{"mode":{"type":"any","enum":["approved"]}}}
```

The value {"mode":"unapproved"} passes source.Validate and fails target.Validate.
Expected: source.ValidateAssignableTo refuses the direction.
Actual: it returns nil. The target property's primitive kind is any, but its
enum still restricts allowed values. The object relation checks only that kind
in the implicit/boolean additional-property branch and skips the restriction.

The same disagreement exists for an any property constrained by oneOf(string,
integer), with {"mode":true}, and for property-scoped x-swarm-equalTo:peer, with
{"mode":"unapproved","peer":"approved"}. These are admitted schemas, not malformed
schema inputs. Equality was constructed through public AdmitToolInputSchemaMap,
not private-state mutation.

Omitted additionalProperties and explicit true both fail. Explicit
additionalProperties: {"type":"any"} already takes the full recursive proof and
correctly refuses all three. Closed sources that cannot provide the optional
property and identically constrained declared sources remain valid controls.

## Category, Class Chain And Closure Commitment

Category: semantic correctness / failure-class, with inconsistent interpretation
inside one existing finite implication owner. Observed enum example is
symptom-shaped. It is an entry point, not the audit boundary.

Chosen working failure class: implicit or boolean-true unrestricted additional
source values incorrectly accepted for target-declared properties missing from
the source declaration, when an admitted target any property restricts its value
domain. Cover all admitted restrictions applicable to any (enum, oneOf and
property equality), combinations, recursive placements, and every caller of
this decision. Not an enum-only or compiled-result-constructor-only fix.

Immediate parent: sound recursive implication between admitted ToolInputSchema
values. Broadest plausible parent: consistent schema admission, implication and
projection across semantic-source construction. #2349 tracks representation/model
debt; #2407 tracks proof coverage; #2447 tracks maintenance/decomposition.
#2250 explicitly excludes schema validators and is NOT this semantic parent.

Framing decision: repair the issue's enum-focused evidence to the complete
chosen class before coding. This class should fit one semantic PR and that PR
must eliminate it entirely. Broader parent soundness remains unproven; no claim
to complete #2349/#2407/#2447. Parent action: keep the general parent explicitly
open, absorb all confirmed additional-value siblings now, and do not manufacture
a universal implication solver or a staged enum-only follow-up.

The finite implication owner's other requiredness, string/numeric bounds,
arrays, unions, map-key and equality decisions were probed through existing
baseline controls and inspected. No second implication interpreter was found.
Those results do not establish complete parent soundness. Remaining parent tail:
no other concrete recursive-implication bug identified by this audit; any further
semantic slice count is unknown. Rough groups are relation-law completeness,
representation debt and supported-path proof, not an invented finite estimate.
Confidence high in this local class map, low in a total parent-tail estimate.

Intended closure: the chosen additional-value failure class eliminated, after
named execution proof. Feasibility: one canonical branch can delegate to the
existing full proof; the four production direct callers already consume it.
Fixing only a constructor or adding an enum check would leave the oneOf/equality
siblings or other callers live and would not meet this commitment.

## Binding Governing Context And Proposed Spec Delta

The full #2630 issue/body/thread and #2447 relation audit were reread.
Authoritative platform-spec.yaml at this base:

- engine.hitl_channel_pack_interface.contract, around line 11426: one canonical
  assignability owner for channel relations and compiled result projection;
  mapped source/target paths are proved before tool authority is published.
- engine.hitl_channel_pack_interface.finite_directional_compiler.rules, around
  line 11951: every admitted source value must be assignable to target, including
  requiredness, additional-properties policy, arrays, enums, patterns and bounds;
  reject unproved relations. Intersection is not assignability.
- Its selected_constraints clause: selected authoring-limit intersection is a
  different operation, not a substitute for implication.

There is no ambiguity permitting current acceptance. Proposed authoritative
clarification in the eventual semantic PR: omission/true additionalProperties
represents an unrestricted value domain, and a target property of kind any is
not unrestricted when enum, oneOf or property equality applies. Such a direction
must consume the same complete finite proof as schema-valued additional source
values. No general JSON-Schema implication promise, new grammar, normalization,
compatibility path, vendoring, identity migration or wire-shape change.

## Exact Rule Correction Proposed, Not Implemented

Real owner: ToolInputSchema.ValidateAssignableTo in
internal/runtime/contracts/tool_input_schema_relation.go.
Its private validateToolSchemaSubset proves full admitted domains; the object
phase validateToolSchemaObjectSubset contains the inconsistent branch near 228.

For a target-declared property absent from source declarations:
1. Preserve the existing object-phase position and ordered earlier gates.
2. If source additional values have an admitted schema, keep existing recursion.
3. If source additional values are unrestricted, represent that source value
   through the existing admitted unconstrained ToolSchemaAny and invoke
   validateToolSchemaSubset against the target property.
4. Preserve the existing earlier error text/order for non-any target kinds;
   use the canonical enum/oneOf/equality refusal for the new constrained-any cases.
5. Do not replace full proof with a copied list of recognized target constraints.

The canonical recursive owner already rejects unbounded-to-enum, unproved oneOf,
and missing equality. Reuse those laws; do not broaden its conservative union
proof, special-case consumers, add a generic validator, or infer relationships
from intersection. Identical oneOf remains provable; arbitrary different unions
remain unproved even if a broader general solver might prove them.

Old path to invalidate: Kind()==ToolSchemaAny as sufficient positive evidence
for these unrestricted additional values. No old producer/reader or separate
semantic owner to migrate. Callers, accessors, source/output schemas and projected
bytes stay under their existing owners. No consumer-local compatibility seam.

## Full Execution Paths, Earlier Gates And Later Effects

Every gate is classified below. S = same chosen implication class;
D = different semantic concept, with named control; T = explicitly tracked
separately. This audit is a compiler correctness audit, not a reported live
provider or backend parity failure.

| Ordered path | Earlier gates before implication | Implication and later stages |
| --- | --- | --- |
| Authored/typed channel -> accepted interface/connector/trigger inventory -> CompileChannel -> operation/event/learned mappings -> immutable plan -> registered private activity | D: schema-definition admission, exact dependency identity/role/effect and mapping cardinality/topology; ChannelSchemaAdmissionRejectsRecursiveMalformedSchemasAtEveryTypedBoundary and ProductionCompilerFailsClosedAcrossChannelContractPhases. D: opaque object contract is closed, every member required and its scalar leaves bounded; learned destination allows scalar leaves only. | S: input/output/each/event/learned directions use the wrapper below. D: required mapping coverage, generation/readback and runtime effect/result validation; ProductionCompilerRejectsRecursiveDirectionalAndCardinalityGaps, ProductionCompilerAcceptsStructurallyDifferentTighterSatisfier, ChannelLearnedDestinationRelationAdmissionAndGeneration. The simple open-object example is NOT shown reachable through every earlier gate. |
| NewToolSchemaEntry / WithSchemas / WithCompiledResult -> typed input/output admission -> compileToolResultProjection definition/source/target paths -> finite relation -> required output construction -> published immutable entry -> HTTP execution | D: option/handler/HTTP definition and guaranteed paths/overlap; ToolSchemaEntryRejectsCompiledResultPathsOutsideAdmittedSchemas, ToolCompiledResultAdmissionIsIndependentOfOptionOrder. S: each source/target field calls the owner. | All three entry entrances are baseline execution-proven to publish a nonzero entry for the incompatible direction. D: final coverage and provider-result value acceptance are later laws. No HTTP call was made; this is constructor admission, not public transport or provider-effect proof. |
| Flow source/input -> effective event/structural schema -> constructor presence/type/collision/internal-initial checks -> ValidateCatalogFieldSupply -> recursive map-key proof -> value implication -> assignment eligibility -> runtime creation | D: structural/presence/assignment; FlowConstructorEligibilityUsesAssignmentChecker, FlowConstructorProjectionAdmission, FlowConstructorSuppliedInitialFacts and effective-schema controls. S: catalog key and value implications. | D: catalog target projection emits string enums, closed records and string map keys, not the example's constrained-any target property. Bare json/jsonb target is intentionally unrestricted. Current counterexample exposure here is not demonstrated; retain the actual target projector and its gates. |
| Maintained characterization -> extraction -> complexity/consumer proof | T: #2447, pending reviewer sequencing; correctness must be repaired or explicitly split, never characterized as expected acceptance. | No production or maintenance extraction is authorized by this audit. |

A valid entry and actual actor/source/effect admission must precede any HTTP/tool
invocation. We will not change opaque/channel/catalog gates to force the example
through an otherwise unreachable public route.

## Exhaustive Systematic Owner-Consumption Audit

Repo-wide production search plus TestSemanticOwnershipProducerConsumerLedgerIsClosed
find FOUR direct ValidateAssignableTo call sites in THREE files. The ledger
counts one channel, one compiled-result, and two catalog sites. No separate live
recursive ToolInputSchema implication implementation found. All named same-concept
seams already consume the canonical owner; no bypass left for a consumer-local fix.

| Consumer | Status and exact consumption | Exposure and planned execution |
| --- | --- | --- |
| packs/channel_relation.go::validateDirectionalRelation:118 | Already consumes canonical owner, one direct call after source/target pointer-presence gate. | Preserve pointer refusal and full production compiler directional controls, not just wrapper invocation. |
| packs/channel.go::validateOperationBinding:1883 input / :1917 output | Already consumes through wrapper, two separate paths. | Earlier opaque/interface gates restrict admitted shapes. Execute both valid tighter-satisfier and incompatible direction leaves. No direct example route claim. |
| packs/channel.go::validateEachItem:2044 | Already consumes through wrapper. | Execute ordered each-item recursion after cardinality/coverage admission. Preserve source item and required output evidence. |
| packs/channel.go::validateEventBinding:2093 | Already consumes through wrapper. | TelegramChannelCompilerConsumesExactNormalizedIdentifierSchemas: exact normalized range/pattern direction and valid plan. |
| packs/channel_opaque_reference.go::compileLearnedDestination:98 | Already consumes through wrapper. | Scalar-only mapping gate; ChannelLearnedDestinationRelationAdmissionAndGeneration. Not a constrained-object route. |
| contracts/tool_http_execution.go::compileToolResultProjection:980 | Already consumes canonical owner, one direct call per proved field. | Counterexamples reach this real owner; enumerate error paths and unchanged valid projection bytes. |
| contracts/tool_schema_entry_value.go:291 constructor / :504 WithSchemas / :629 WithCompiledResult | Already consumes result compiler in all three producers. | All three fail 3/3 on baseline expected-refusal assertions. After repair each must refuse before publishing a nonzero entry, with valid controls retained. |
| contracts/schema_registry.go::ValidateCatalogFieldSupply:367 | Already consumes canonical owner, one direct call after target derivation, refinements, key proof and schema admission. | FlowConstructorProjectionAdmission and CatalogSupplyAdmission; enum target is string, record target closed. Unrefined json/jsonb is a different unrestricted-target law, not a missing implication owner. |
| contracts/schema_registry.go::validateCatalogMapKeySupply:389 | Already consumes canonical owner, one direct call, recurses items/additional/declared properties. | Existing catalog map-key/nested controls; map key target is string. No alternate kind-any decision. |
| engine/flow_constructor.go::seedConstructorAssignment:51 | Already consumes catalog-field owner after assignment eligibility gates. | Complete effective-schema/catalog/refinement/initial-fact controls, not a direct helper shortcut. |
| tool_input_schema lexical/typed definition and value validators | Different semantic concept, with proof: schema validity/value inhabitance, not inclusion between all source/target values. | Existing PropertyNames/OneOf/schema-definition controls; new witnesses explicitly compare each value before the relation assertion. |
| StructuralCatalogTypeAssignable / workflowexpr / accprojection | Different semantic concept, with proof: nominal shape/presence and integer-number compatibility precede the catalog refinement proof. | FlowConstructorProjectionAdmission and CatalogSupplyAdmission; do not merge these owners. |
| selected-constraint intersection / IntersectBounds | Different semantic concept, with proof: selected_constraints governing clause and tighter-satisfier controls. | Cannot justify direction by intersecting the source and target. |
| Static HTTP template shape / static response literal validation | Different semantic concept, with proof: schema.Validate on known values, not full source-domain implication. | ToolSchemaEntryRejectsStaticallyImpossibleResponseMappingsBeforeAuthority. Not a fifth direct relation call. |
| Event-schema acceptance/inhabitance, generated closedness, provider-wire validation | Different semantic concepts, with proof: concrete payload/domain shape and serialized provider subset; immutable owner/closure/provider tests. | #2629 closed provider enforcement; no edit or transfer of its proof to this class. |

No seam is described as fixed merely because it shares an owner. Existing source
gates, actual constructor executions and planned corrected-path tests are distinct.

## Manifestation Table And Exact Planned Proof

R is planned TestSchemaRelation2630AdditionalValues with the named case. C is
planned TestSchemaRelation2630CompiledResultEntrances. P is planned
TestSchemaRelation2630ConsumerRegression. These roots do not yet exist as
maintained tests. Baseline evidence is the exact disposable probe linked above;
all candidate closure proof remains planned, not earned.

| ID | Manifestation/control | Current evidence | Exact planned proof after gate |
| --- | --- | --- | --- |
| R01 | Omitted additional, target any+enum | RED 3/3, source-valid/target-invalid witness | R/enum/implicit refuses mode=unapproved. |
| R02 | True additional, target any+enum | RED 3/3 | R/enum/explicit_true, same witness and canonical refusal. |
| R03 | Omitted additional, any+oneOf | RED 3/3 | R/oneof/implicit refuses boolean witness, no union solver. |
| R04 | True additional, any+oneOf | RED 3/3 | R/oneof/explicit_true, same prechecked witness. |
| R05 | Omitted additional, any+equality | RED 3/3 | R/equality/implicit with public whole-object admission and unequal peer values. |
| R06 | True additional, any+equality | RED 3/3 | R/equality/explicit_true, retain actual object-wide equality check. |
| R07 | Schema-valued any additional vs enum | PASS 3/3 refusal control | R/enum/schema_any remains refused via existing full relation. |
| R08 | Schema-valued any additional vs oneOf | PASS 3/3 refusal control | R/oneof/schema_any preserves conservative refusal. |
| R09 | Schema-valued any additional vs equality | PASS 3/3 refusal control | R/equality/schema_any preserves equality-before-enum ordering. |
| R10 | Matching declared enum/union/equality | PASS 3/3 identical controls | R/declared with same admitted constraints stays valid; exact canonical projections unchanged. |
| R11 | Closed source omits optional constrained property | PASS 3/3 each constraint | R/closed_absent: witness absence cannot introduce a bad value; remains valid. |
| R12 | Truly free any target, both unrestricted policies | Source inspected / prior baseline free-any controls PASS | R/free_any across null/text/bool/number/object/array; no extra refusal. |
| R13 | Any with only non-semantic description | Planned, not executed | R/description_only: valid relation and unmodified description/projection bytes. |
| R14 | Nested declared object occurrence | Planned | R/nested_object x enum/oneof/equality x both policies, nested subject and witness validity. |
| R15 | Array item object occurrence | Planned | R/array_items x three constraints x both policies, [] subject and source/target value checks. |
| R16 | Schema-valued outer additional object occurrence | Planned | R/nested_additional x three constraints x both policies, .* path and witness. |
| R17 | Combined applicable any constraints | Planned | R/combined: publicly admit enum/oneOf/equality combinations where valid; restrictive target never accepted from free source. Definition-rejected combinations retain refusal. |
| R18 | Multiple target names and first new refusal | Planned | R/sorted_targets: declaration order independent, sorted canonical property path/error stable. |
| R19 | Earlier propertyNames / required / declared-member refusals | Existing baseline controls PASS | R/earlier_gates: same first error even with a later new constrained-any failure. |
| R20 | Existing non-any target refusal and closed-target policy | Existing baseline controls PASS | R/non_any_and_closed: exact old additional-property messages/order retained. |
| R21 | Narrow constrained source / widening law | Existing baseline controls PASS | R/constrained_source; integer_to_number accepted, reverse refused; no unrelated change. |
| R22 | Any constraints inapplicable to primitive kind | PASS 3/3 definition refusal controls | R/inapplicable: pattern/length/format/numeric/items/required refuse at actual definition admission. |
| R23 | Conservative union behavior | Existing baseline controls PASS | R/oneof_conservative: identical accepted, other implication unproved, no expansion. |
| R24 | Determinism and immutable source/target | Planned | R/nonmutation: repeated decisions, before/after CanonicalJSON/Equal/schema projection facts. |
| C01 | NewToolSchemaEntry result constructor | RED 3/3, nonzero entry with nil error | C/constructor x constraint/policy refuses before authority; exact source witness, good entry control. |
| C02 | WithSchemas result revalidation | RED 3/3, nonzero entry with nil error | C/with_schemas x constraint/policy refuses without changing original good entry. |
| C03 | WithCompiledResult result addition | RED 3/3, nonzero entry with nil error | C/with_compiled_result x constraint/policy refuses without changing unprojected entry. |
| C04 | Result path/coverage/option-order precedence | Existing baseline controls PASS | C/path_and_option_order plus existing path/source-guarantee/overlap/coverage roots; no later-stage error substituted. |
| P01 | Channel input/output/each mapped direction | Baseline production compiler controls PASS; simple example exposure not proved | P/channel_mappings plus complete existing recursive/cardinality gap and tighter-satisfier roots, keeping opaque gates. |
| P02 | Normalized channel event direction | Baseline exact normalized-schema controls PASS | Existing TelegramChannelCompilerConsumesExactNormalizedIdentifierSchemas and P/event. |
| P03 | Learned-destination scalar direction | Baseline PASS | Existing ChannelLearnedDestinationRelationAdmissionAndGeneration plus P/learned; scalar gate unchanged. |
| P04 | Catalog value/refinements and complete constructor | Baseline race x3 PASS; constrained-any target not emitted by projector | Complete FlowConstructorCatalogSupplyAdmission, ProjectionAdmission, SuppliedInitialFacts, ConsumesEffectiveSchema and P/catalog. |
| P05 | Nested catalog map-key directions | Baseline race x3 PASS | P/map_keys through items/additional/member traversal, actual catalog projector, exact first error. |
| Q01 | Supported structural reader consumer proof | Planned, no new public receipt | Compiled verify --portable and describe on valid admitted channel source; existing malformed directional fixture refuses through production compiler. Not claimed as enum-bug exposure when earlier gates exclude it. |
| Q02 | Supported runtime projection / readback parity | Planned | Existing compiled host MCP valid channel/tool projection plus selected SQLite/PostgreSQL valid projection/readback via sanctioned fixture owners; source compiled before effect. No paid-provider or Docker claim. |
| Q03 | Generic failing proof discriminates exact correction | Baseline counterexample discriminates current branch | After repair, temporarily restore only the old kind-only decision; R01-R06 and C01-C03 must fail while good-entry/closed-source controls pass. Restore source; no mutation result replaces public proof. |
| Q04 | Owner census / static guards / complexity / tier coverage | Existing unchanged-source guard receipt PASS | Exact four-site ledger; all touched-package census/registry/static/ratchet guards BEFORE each push; new roots selected once in reviewer-chosen tier, exact complexity ratchet and no baseline increase. |

All 37 rows have named evidence or explicit planned execution, never "same seam"
as closure proof. R14-R18 and supported Q01/Q02 have not been executed by this
read-only audit. If a supposed public path is unreachable, name the earlier gate
and keep its regression proof distinct rather than counting direct owner tests
as public execution.

## Receipts, Existing Controls And Limits

Fresh source770 probes:
```text
go test ./internal/runtime/contracts -run '^TestIssue2630Audit' -race -count=3 -timeout=3m -json
```
Receipt issue2630-audit-constraints-producers-race3.jsonl,
SHA256 9f16cce4f7977015b40e252638af3f978faab12ab5bce0503233b193aa75d08e.
Six failing relation leaves and three failing producer leaves each RED 3/3.
Source.Validate/target.Validate disagree as expected before each failed relation
assertion; explicit-schema, matching-source and closed-source controls PASS 3/3.
No skip/race reports. Original production/maintained tests unchanged.

Prior568 baseline owner/caller bytes match770. Existing relation/channel/result/
constructor/ownership controls passed race x3:
relation-preaudit-baseline-race3.jsonl,
SHA256 fcce4e8b51332d214e4866a8a255588da0ff785a35394f7049ed82bd8d7378ba.
Complete catalog-supply/refinement/map-key/constructor controls passed race x3:
relation-preaudit-constructor-race3.jsonl,
SHA256 229e0e9c94ba4ae74a954315f8fde203caa8f5a7ff7e22f1b5b08adc90ee90ce.
These passes do not repair this confirmed baseline disagreement.

Actual pre-push guard receipt for the earlier audit-only148a40d36 (same source770):
```text
TEST_POSTGRES_BIN=/usr/lib/postgresql/16/bin go run ./cmd/swarm-test -- \
  ./internal/runtime/contracts ./internal/packs ./internal/runtime \
  ./internal/cliapp ./internal/testplanning ./internal/testcatalog \
  -run '^Test.*(Census|Inventory|Registry|Guard|Ownership)' -count=1 -timeout=10m -json
```
relation-integrated-prepush-guards.jsonl SHA256
218599296614123b195009fec3cf02db5cb7ed202b2744de38e019221d3442b5.
All six packages PASS, 146 root / 492 test passes, zero failure/skip.
This is unchanged-source baseline proof, NOT execution on a future fix/audit SHA.

This publication changes Markdown only after deleting the temporary probe;
no Go package, maintained root, fixture, registry, source corpus, plan, spec or
baseline is touched. Pre-push audit checks: diff whitespace, exact artifact source
preservation, clean package diff and watchlist YAML/unique issue/node mapping.
No duplicate whole tier, server2 run or new persistence-debt construction.
After coding approval, run each changed package's census/ratchet/static tests
before every push, list the actual commands/results in the PR audit, and raise
any census change at the pre-push reviewer checkpoint.

Qualification proposed for reviewer: focused relation/consumer race controls,
compiled supported structural/runtime consumers with both-store readback as
above; hosted lifecycle/no extra units if its census selects every required root.
Reviewer owns tier and any different supported-surface requirement. Not a
SQLite/PostgreSQL parity bug; direct pure compiler proof must not be misrepresented
as a live provider/backend test. No full/local qualification is claimed now.

## Watchlist Promotion, Tracker Repair And Architecture

Mapped semantic node:
pack_interface_structural_satisfaction_and_channel_adapter_ownership.
Its broader class is immutable directional schema/plan/registry semantics;
it already names enum/refinements, recursive mappings, constructor catalog
supply and model debt. Mapped maintenance node: boundary_owned_decomposition.

Promotion check: the watchlist makes an enum-only fix dishonest. This audit
absorbs the confirmed union/equality siblings and all three result producers into
#2630. It does not justify a universal schema solver or merging different value,
catalog-shape, generated-closure and provider-wire concepts into one PR. Parent
action remains explicitly open and probed, not silently deferred.

Tracker-state decision: current #2630 body must be updated before coding to this
broader confirmed class; #2447 B01 and watchlist notes must name all constraints/
producer entrances and actual early consumer gates. Existing issue/node suffices;
no new child, umbrella, node or POTENTIAL_ISSUES entry. #2447/#2407/#2349 remain
open; no already-closed provider issue reopened and no #2250 schema-parent claim.
Publish these repairs with this audit, then request independent gate.
Watchlist refinement is published at swarm-docs
c9bee8c2eaf201cd8cf7146e9fcc3c990b769cb2. YAML, unique node/issue IDs and
all existing mapped references validate; #2630 maps exactly to the node above.
Eight older unclassified semantic-watchlist rows are unchanged, not claimed
repaired or newly mapped by this audit. Both watchlist diffs pass whitespace checks.

Architecture smell: primitive Kind is not the accepted value domain. The
existing immutable ToolInputSchema and full recursive relation already model
the difference; reuse them instead of inventing another "unrestricted" predicate.
Concrete repair is tracked in2630. Broader model feedback remains #2349 and
existing watchlist only; no separate redesign obligation established.
Rough effort after gate: half to one day for bounded correction/ordered tests,
about one additional day for complete caller and supported-path proof, moderate
confidence. High correctness ROI, no runtime/storage framework or extra owner.

## Sequencing With #2447 And Stop Conditions

Recommended: semantic repair #2630 first, then re-characterize the correct
relation for #2447 maintenance extraction and request its independent coding gate.
If reviewer chooses split packaging/ordering, it must explicitly define where
semantic correction lands and how correct characterization is established.
No extraction test may record invalid acceptance as intended behavior. This
audit does not authorize a combined PR, a repair under the maintenance gate,
or maintenance coding before the sequencing ruling.

Open #2612/#2617/#2624/#2616/#2614 PR heads were checked for the prior maintenance
audit; no target relation-file overlap then. The open-PR list was rechecked for
this audit. Shared platform-spec is integration overlap only; recheck exact
changed files before eventual coding and coordinate any overlap rather than
editing another lane's owner. #2629 is already merged770 and not a dependency.

Blocking conditions: independent #2630 class/coding/sequence ruling is missing;
#2447 sequencing remains pending reviewer-g's priority queue. Stop if another
same-concept consumer, a wider semantic obligation, changed error precedence,
unreachable claimed public fixture, non-discriminating correction test or
contradictory spec is discovered. Repair the issue/audit and re-gate rather than
silently widening or replacing proof with shared-owner claims.

**Current gate result: pending. Production implementation remains frozen.**
