# Issue #2630: Approved Two-Defect Audit Amendment

Binding amendment: https://github.com/division-sh/swarm/issues/2630#issuecomment-6122944537.
Original gate6122667610 remains binding. Repair-first; #2447 stays frozen.
Coding base5182245ed. Signed baseline checkpoint50d38504c predates production edits.

## Class And Tracker Repair

Two distinct sibling defects in the existing recursive relation owner:
A. Unrestricted omitted/true additional source values incorrectly accepted for
   missing source-declared target any properties with enum/oneOf/equality,
   combinations and recursive placements.
B. Identical admitted oneOf/equality properties incorrectly refused because
   union identity projects an extracted property outside its containing object.

The chosen PR closes both bounded defects, not arbitrary schema implication.
The original37 R/C/P/Q rows are retained; the seven I rows below extend identity
proof explicitly. Original direct-production caller ledger remains four sites,
already canonical. Broader recursive soundness, representation/model and proof
coverage stay open under2349/2407;2447 is maintenance,2250 not this parent.

Tracker-state decision: update2630 body and existing semantic/maintenance nodes
before coding; no new issue. Promote B into this PR, not deferred model-only debt.
Gate amendment resolves the earlier checkpoint's two-way scope question.
No top-level equality shortcut alone, pointer-identity proof, caller exception,
new dialect, generic union solver, representation redesign or vendoring.

## Proposed Canonical Identity Correction

Keep public ToolInputSchema.Equal/Project unchanged. Their module-schema/default
consumers are not edit targets. Only the finite relation's private comparison
receives whether both values are declared properties of admitted objects.
The object declared-member phase supplies that context; root, items,
propertyNames and schema-valued additional comparisons do not invent it.

Before contextual positive identity, consume existing schema definition admission
with object-property scope, and the existing immutable canonical projection/JSON
owner. Independently admitted equal properties must compare structurally,
not by pointer address. Nonidentical objects still prove every declared member,
including peer constraints; no whole-object identity shortcut replaces recursion.
Standalone equality-bearing roots still fail definition admission/projection.
No unadmitted root's authority-free projection is counted as positive proof.

For defect A, preserve old non-any messages/precedence and represent unrestricted
additional values as existing admitted free Any, passing them to full recursion.

Authoritative spec clarification in this PR at
engine.hitl_channel_pack_interface.contract / finite_directional_compiler.rules:
admitted contextual identity/reflexivity; any kind versus constrained domain;
same finite conservative law for nonidentical/unproved unions.

## Additional Manifestation And Consumer Proof

| ID | Manifestation | Exact execution planned |
| --- | --- | --- |
| I01 | oneOf+equality and enum+oneOf+equality identical whole schemas | TestSchemaRelation2630MatchingUnionEqualityControl, original publicly admitted schema and separately admitted structural copy; RED3 baseline12fe14c7 to GREEN3. |
| I02 | Different enclosing objects with identical contextual property | Same root/nonidentical_parent: stricter source other field proves against weaker target, canonical hashes differ, nested property must still prove. |
| I03 | Incompatible peer contexts despite identical mode property | Same root/incompatible_peer: actual source-valid/target-invalid value and exact peer refusal after contextual mode identity. |
| I04 | Changed enum/equality target/unproved unions | Same root/changed_enum, changed_equality, changed_union: refusal, no generic solver or equality widening. |
| I05 | Equality-bearing standalone root remains invalid | Same root/standalone: public definition admission and Project refuse; root-level union relation cannot acquire contextual identity. |
| I06 | Nonmutation and unchanged public Equal | Same root/projection_and_copy: whole and separately admitted projection/hash equality before/after; public extracted-property Equal remains invalid-context false. Existing module/default/schema controls remain selected. |
| I07 | Identity correction is independently discriminated | Restore only the old identity path and run I01/I02: tests fail; restore. Separately restore only old kind-only path and run original relation/producer refusals: tests fail; restore. |

Consumer classification: all four implication sites consume the same owner after
repair. Constructor/WithSchemas/WithCompiledResult combinations each execute;
declared contextual identity fixes their previously failing valid-entry control.
Channel input/output/each/event/learned and catalog value/map-key paths retain
their earlier gates and separately named proofs from the original audit.
Public Equal does not change, so its other consumers do not gain new acceptance.

## Baseline And Qualification

Original A: receipt2630-coding-base-exact-red3.jsonl SHA256
a396633b0d014e94bf61ce17680e8df62f8cb5f0ace01dd62275332b4e455378;
24 relation and18 producer leaves each RED3 after witness checks.
B: receipt2630-matching-combination-red.jsonl SHA256
12fe14c7bea91defab3c6db40697be5bb40e53431e76472485a4ff41aa1509a2;
two original combination leaves RED3. No passing qualification inferred.

Hosted CI-Tier lifecycle, no CI-Units; Local focused plus supported valid both-store
readback. No duplicate local full/tier. Touched-package census/registry/static
guards and exact complexity admission before push; all new roots selected once.
Final PR audit has44 rows with distinct A/B proof, all old paths and parent states.
Stop/re-gate if identity needs a public Equal redesign or broader schema admission.
