# Semantic Drift Prevention

Semantic drift is a design defect class in this codebase.

It happens when one concept exists in the spec, but multiple subsystems encode or interpret that concept differently. The usual result is not an immediate crash. The usual result is delayed contradiction:

- verify disagrees with boot
- runtime disagrees with store truth
- diagnostics disagree with canonical state
- local names and canonical identities are resolved differently
- one reader reconstructs semantics that another writer never persisted

This document defines the default prevention strategy.

## What To Treat As Drift-Prone

The following concepts should be treated as high-risk semantic seams:

- identity and canonical naming
- ownership and lineage
- status vs metadata semantics
- retry state and terminality
- validation result classes and warning fatality
- flow-local vs canonical event identity
- live owner vs audit/history owner
- persisted truth vs reconstructed reader truth

If a change touches one of these, assume drift risk until proven otherwise.

## Core Rules

### 1. Define one canonical semantic owner

Every important concept should have one explicit owner in code.

If an issue spans multiple coupled semantic concepts, each concept must have its own explicit canonical owner.

Examples:

- event identity should not be re-derived independently in boot verification, runtime emit tooling, and contract accessors
- session lifecycle meaning should not be split across status enums, ad hoc JSON fields, and audit-only readers
- retry semantics should not be interpreted separately by store selectors and runtime callers

Rule:

- if multiple subsystems need the same semantic concept, they should consume one owner
- do not let nearby helpers encode slightly different versions of the same rule

Review question:

- where is the one canonical owner of this concept?
- if this issue touches more than one concept, what are the owners for each?

### 2. Prefer typed semantic models over loose strings and maps

Stringly-typed semantics drift quickly because each caller can reinterpret the same value differently.

Default bias:

- use typed structs and explicit fields for important semantics
- do not pass around bare names plus partial context when the concept has multiple dimensions

Examples:

- event references should carry enough typed information to distinguish local name, canonical identity, and owning flow
- session termination should use explicit metadata fields instead of overloading status with undocumented meanings

Review question:

- is this concept represented as an explicit model, or are multiple callers reconstructing meaning from strings?

### 3. Ban reader-side semantic reconstruction where write-time truth can be persisted

If canonical truth can be materialized once, persist it. Do not make every reader recompute it.

Why:

- reader-side reconstruction is a drift factory
- later readers tend to add slightly different fallback logic

Examples:

- defaults should be materialized at create time, not inferred by downstream readers
- canonical identifiers should be persisted or derived in one canonical write-time owner, not reassembled by projections

Rule:

- if the writer can persist canonical truth, readers should consume that truth directly

Review question:

- is this reader interpreting state, or consuming canonical persisted state?

### 4. Map sibling seams before accepting a “first-slice” fix

Many bugs look local but are really one duplicated semantic concept spread across several seams.

Before approving a semantic fix, explicitly map sibling seams that use the same concept.

For each sibling seam, state one of:

- moved to the canonical owner now
- already aligned and execution-proven
- explicitly split or escalated

Rule:

- “fixed in the touched seam” is not enough if sibling seams still interpret the same concept independently

Review question:

- where else is this concept interpreted?

### 4a. Audit systematic consumption of the canonical owner

Naming the right owner is necessary but not sufficient.

A concept still drifts if the codebase knows the canonical owner but nearby seams continue to bypass it.

Required audit:

- identify where the canonical owner is defined
- identify every currently known sibling seam that should consume that owner
- classify each seam as exactly one of:
  - already consumes the canonical owner
  - moved to the canonical owner in this PR
  - still bypasses the canonical owner and is explicitly split or escalated

Rule:

- do not claim failure-class elimination unless the canonical owner is used systematically across the relevant sibling seams
- if the owner is not yet used systematically, the PR must either widen the fix or narrow the claim explicitly

Review question:

- who is still bypassing the canonical owner?

### 5. Prefer broader refactors when duplication is the real failure class

When repeated drift comes from duplicated semantics, a broader refactor is often cheaper than repeated local patches.

Default bias:

- if the sibling-seam map shows the same concept encoded in several places, prefer consolidation
- do not artificially keep the patch tiny if that just preserves the duplicated semantic model

This does not mean every issue must become a repo-wide rewrite.

It means:

- if the problem is structurally duplicated, fix the structure
- if a first slice is necessary, it must still account for the sibling seams explicitly

Review question:

- is this issue really a local bug, or is the bug evidence that the model is duplicated?

### 5a. Close the semantic gap in the PR by default

If a semantic gap can be closed cleanly in the current PR, close it in the current PR.

Default bias:

- remove the bypass seam
- delete the old interpreter
- move the touched sibling seams now
- do not leave “temporary” dual semantic ownership in place just because staging feels safer

Rule:

- migration exists to move state, schema, or deployment ordering safely to the canonical model
- migration does not justify preserving two competing semantic owners
- if the implementer proposes leaving the old semantic path in place, they must prove why complete closure is not feasible in the same PR

Insufficient reasons by themselves:

- smaller patch
- easier rollout
- less risky by default
- follow-up is easy

Review question:

- why is the semantic gap not being closed in this PR?

### 6. Treat cross-surface parity as a first-class invariant

When multiple surfaces answer the same semantic question, agreement is part of correctness.

Examples:

- verify vs boot
- boot vs runtime
- writer vs selector
- store truth vs diagnostics

Rule:

- if two surfaces claim to validate or expose the same concept, they must share the same semantic owner or be proven equivalent

Review question:

- what are the adjacent surfaces that should agree here, and how is that agreement proved?

### 7. Escalate spec gaps early

Under-defined semantics create drift because different implementers fill the gaps differently.

Rule:

- if the spec does not clearly define the concept, stop implementation
- draft the spec change first
- approve the draft
- then promote it into the authoritative spec in the same PR as the implementation

Review question:

- is this implementation applying an authoritative rule, or inventing one?

### 8. Track drift-prone concepts explicitly

Do not rely on memory alone.

Maintain a small working set of concepts that have already shown drift pressure, such as:

- event identity
- session lifecycle semantics
- workflow instance identity
- retry ownership
- validation parity

Rule:

- if a new issue touches a known drift-prone concept, increase the review bar immediately

Review question:

- is this one of our already-known drift-prone concepts?

### 9. Test invariants, not only symptoms

Symptom tests are necessary but not sufficient.

Add tests that prove:

- canonical owner behavior
- cross-surface agreement
- migration invariants
- counterexamples that would reveal semantic drift

Examples:

- verify and boot agree on the same contract set
- persisted canonical state is visible identically through the main reader
- local and canonical event identities resolve to one coherent model

Review question:

- what invariant test would fail if a sibling interpretation drifted later?

### 10. Use review to force semantic accounting, not just patch inspection

Review must ask:

- what concept is being modeled?
- where is its canonical owner?
- where else is it interpreted?
- does this patch reduce drift or merely move it?

Rule:

- do not approve a semantic change because the local code looks cleaner
- approve only when the semantic owner and sibling-seam accounting are explicit

Review question:

- does this patch eliminate a drift source, or just produce a nicer local variant of it?

## When A Large Refactor Is Warranted

A broader refactor is justified when most of the following are true:

- the same concept is interpreted in three or more seams
- multiple issues have appeared from the same duplicated meaning
- the current local fix would add yet another helper or local normalization layer
- the implementer cannot explain the full concept without naming several separate code paths
- cross-surface agreement currently depends on “remember to keep these in sync”

If these conditions hold, default to a canonicalization refactor instead of another narrow patch.

## Practical Review Prompt

For semantic/runtime work, reviewers should always ask:

- what is the exact concept?
- where is the canonical owner?
- what sibling seams consume or reinterpret it?
- did this PR move them, prove them aligned, or split them explicitly?
- what invariant test now guards against this drift returning?

If those answers are weak, the review is incomplete.
