# Issue #2630: Cycle-1 Detached Equality Amendment

Binding ruling: https://github.com/division-sh/swarm/pull/2631#pullrequestreview-5490306823.
This supersedes the complete-closure claim of the 44-row audit at c8f58f528;
its receipts remain evidence for the first two defects, not this third defect.
#2447 remains frozen. The original gates and 44 obligations remain binding.

## Class And Tracker Decision

Category: correctness / failure-class. Observed symptom: a publicly admitted
compiled-result entry later fails projection because its required equality peer
is absent. The failing constructor is an entry point, not the audit boundary.
Chosen class now includes unrestricted additional values, contextual union
identity false refusal, and loss of sibling-equality context at a field-path
implication boundary. All three are absorbed in #2630/#2631, through the existing
finite recursive relation owner. Immediate parent: sound recursive admitted
schema implication; broader model debt #2349 and proof #2407 remain open.
#2250 is not this semantic parent. No new issue, solver, compatibility or dialect.

Repair issue body, this audit and both existing watchlist nodes before coding.
The old complete-closure statement is retracted. Parent action: close these
bounded defects now, keep broader implication/model work open. Estimated parent
tail remains unknown (general unions and representable-context model), not a
claim that one more child closes it. Both existing nodes already map the broader
class; detached equality is a live manifestation requiring promotion now.

## Canonical Owner And Full Execution Path

`ToolInputSchema.ValidateAssignableTo` / `validateToolSchemaSubset` owns proof.
Definition admission -> HTTP/result definitions -> compiled path parsing ->
source guaranteedness -> target resolution -> canonical relation -> required
output coverage -> immutable entry -> projection -> whole output validation.
The relation is the same class; other gates are distinct definition/path/effect
concepts and retain their named regression controls. Projection failure is the
source-valid/target-invalid witness, not a substitute for admission refusal.

Preserve whole admitted-object recursion and declared-property contextual
identity. Refuse a detached target equality even when its source has the same
marker: matching labels do not prove the output sibling. Preserve earlier
oneOf refusal and mismatching-equality error order. A surgical equality phase
inside the same relation owner is permitted; public Equal/Project stay unchanged.
Authoritative spec must explicitly forbid marker-only detached proof.

## Exhaustive Systematic Consumption And Sibling Sweep

| Production entrance | Disposition / named execution |
| --- | --- |
| compileToolResultProjection, NewToolSchemaEntry | Already canonical; actual public counterexample -> refusal/zero entry, both any and string. |
| WithCompiledResult | Already canonical; valid unprojected entry -> attempted detached projection -> refusal/zero. |
| WithSchemas | Already canonical; defensive revalidation of the exact pre-repair admitted carrier -> refusal/zero, original immutable carrier unchanged. After repair fresh constructors cannot produce that bad carrier. A fixed test-only carrier seeds the previous admitted shape; this is not claimed as a new publicly constructible valid entry or compatibility machinery. Existing valid whole-object WithSchemas controls still execute. |
| channel relation: input/output/each/event | Already canonical; extracted equality-bearing bounded scalars can reach the relation. Compiler regression controls and an actual output-mapping refusal exercise this, not an any-only exclusion. |
| channel learned destination | Already canonical; scalar-only does not exclude equality. Execute the real compileLearnedDestination owner with admitted bounded scalar equality leaves; this is an internal compiler proof, not a claim of public learned setup bypassing earlier operation/event gates. Earlier public opaque/object/cardinality gates remain executed. |
| catalog field supply / constructor seed | Already canonical; equal_to refinement exists. Direct equality-bearing field target fails earlier standalone definition admission; nested record fields retain admitted object context. Execute both cases plus existing constructor controls. |
| catalog map-key supply | Already canonical; extracted equality-bearing key schema fails standalone admission before implication. Execute that earlier refusal and ordinary map-key controls. |

No second implication owner/bypass found. Root/items/propertyNames/additional
comparisons do not invent sibling scope. Old marker-only detached acceptance is
invalid everywhere; declared-object proof and no-equality scalar projections
remain authoritative. No caller exception or target-peer reconstruction.

## Added Manifestations / Planned Exact Proof

| ID | Manifestation | Proof |
| --- | --- | --- |
| D01 | Detached any equality accepts matching marker without peer proof | TestSchemaRelation2630DetachedEqualityRelation/any; admitted whole schemas, source-valid/target-invalid witnesses, detached refusal. |
| D02 | Same error on non-any primitive | Same root/string; exact refusal and earlier mismatching-marker order. |
| D03 | NewToolSchemaEntry grants invalid projection authority | TestSchemaRelation2630DetachedEqualityEntrances/*/constructor; nonzero before / zero after. |
| D04 | WithCompiledResult grants invalid projection authority | Same root/*/with_compiled_result; real valid unprojected entry, refusal/nonmutation. |
| D05 | WithSchemas revalidation preserves previously admitted bad projection | Same root/*/with_schemas; explicit defensive pre-repair carrier, refusal/zero/nonmutation, no fresh constructor claim. |
| D06 | Admitted projection fails because peer is absent | Same fixed carrier's actual Project with valid source: required projected is present but equality peer missing. Keep this negative after admission repair. |
| D07 | Valid whole-object equality and no-equality scalar projection | Both new roots plus original matching/compiled-result controls; exact output and immutable state. |
| D08 | Bounded channel output and learned scalar extraction | TestSchemaRelation2630ChannelDetachedEquality exercises actual public CompileChannel output with integer equality; TestSchemaRelation2630LearnedDetachedEquality executes the internal learned compiler with bounded string equality. Both baseline acceptance and fixed refusal; public earlier gates remain separately classified. |
| D09 | Catalog field/key earlier admission and nested contextual proof | TestSchemaRelation2630CatalogEqualityGates; actual catalog supply entrances and nested record relation, plus existing constructor controls. |

Intended closure: all 53 known rows (44 retained + D01-D09), not arbitrary JSON
Schema implication. Feasible in one PR via the existing owner. No live same-
concept interpreter remains after correction; all four direct sites consume it.
Architecture feedback: marker-bearing properties without their containing scope
remain representable; long-run explicit context would improve the model, tracked
only on existing #2349/watchlist. No model redesign now; low-confidence effort
several days to a week, ROI useful but not closure-bearing for this repair.

## Qualification / Split Conditions

First run the new expected-refusal tests RED on c8f58f528, then GREEN race x3
after correction. Repeat original owner/consumer regressions, supported valid
both-store channel execution/readback, compiled structural/host MCP controls,
touched-package census/registry/static/file-defined guards, exact complexity and
one-owner selection. Hosted lifecycle, no extra units; Local focused only.
No duplicate local full or tier. Preserve all old failed receipts.
Stop if sibling proof requires a new projection framework, public Equal change
or broader admission grammar. No such requirement is currently identified.
