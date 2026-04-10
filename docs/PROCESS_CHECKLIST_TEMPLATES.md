# Process Checklist Templates

Use these when a short copy-paste checklist is better than re-deriving the process from prose.

These templates are operational helpers, not replacements for:

- [IMPLEMENTER_GUIDELINES.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_GUIDELINES.md)
- [IMPLEMENTER_REVIEW_CHECKLIST.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_REVIEW_CHECKLIST.md)
- [PROMPT_TEMPLATES.md](/Users/youmew/dev/swarm/docs/PROMPT_TEMPLATES.md)

Default rule:

- use the smallest template that removes ambiguity
- do not fill a template mechanically if half the fields are irrelevant
- but do not skip a template when the workflow depends on an explicit recorded decision
- default to complete closure over staged follow-up
- treat first-slice approval as an exception that must be justified explicitly

## 1. Pre-Implementation Coverage Audit

Use on the issue before coding for semantic/runtime/spec-governed work.

```text
Pre-Implementation Coverage Audit

Category:
- <local | failure-class | parity | semantic-drift | high-risk maintenance>

Observed symptom:
- <what visibly failed first>

Concept:
- <exact semantic concept(s) being changed>

Failure class:
- broader failure class: <...>
- chosen working failure class for this PR: <...>
- immediate parent failure class above it, if any: <...>
- parent failure class above it, if any: <...>
- was the observed failing line / helper / reproducer treated as an entry point rather than the audit boundary?: <yes/no>
- issue framing assessment:
  - broad enough
  - narrower-but-acceptable as first slice
  - too narrow

Relevant execution path, if this issue concerns a multi-step user-visible flow:
- step 1: <...>
- step 2: <...>
- step 3: <...>

Gate classification along that path, if applicable:
- <gate/surface> -> same chosen class
- <gate/surface> -> different semantic concept, with proof
- <gate/surface> -> explicitly split / tracked separately

Canonical owner(s):
- <owner 1>
- <owner 2 if any>
- chosen owner is a real semantic owner rather than only the first local helper/file encountered: <yes/no and why>

Consumers of each owner:
- <owner> -> <consumers/surfaces>

Systematic consumption audit:
- <seam> -> already consumes canonical owner
- <seam> -> moved to canonical owner in this work
- <seam> -> different semantic concept, with proof
- <seam> -> still bypasses canonical owner and is explicitly split / escalated

Old non-authoritative paths:
- <old helper/reader/writer/interpreter now invalid or removal-candidate>

Currently known manifestations in scope:
- <manifestation 1>
- <manifestation 2>

Manifestation coverage table:
- manifestation: <...>
  - canonical owner: <...>
  - planned coverage status: <direct reproducer and fix | execution proof through same corrected path | split / escalate as separate class>
  - exact proof planned: <...>
  - required supported-surface / end-to-end proof: <... or none>
  - same class or separate class: <...>

Parent-class sibling probe:
- sibling seam: <...>
  - status: <broken now | apparently clean, with proof | different class, with proof | still unproven>
- watchlist-backed promotion check:
  - mapped node: <...>
  - broader class already tracked there: <...>
  - additional sibling manifestations / consumer families suggested by that node: <... or none>
  - does that watchlist evidence mean the parent should be absorbed now: <yes/no and why>
- estimated remaining child slices to close the parent class: <number or range>
- rough grouping of remaining slices: <...>
- confidence level: <high | medium | low>
- action decision for parent class:
  - absorb parent now
  - keep first-slice scope
  - open or update dedicated follow-up stream
  - leave parent explicitly open as still unproven
- remaining parent obligation tracked at: <issue/watchlist/follow-up>

Tracker-state decision:
- <current issue remains correct as written | current issue must be updated before coding | new child issue required before coding | parent issue / umbrella issue must be updated before coding | older issue / stream superseded and must be marked accordingly before coding | watchlist-only refinement is sufficient>
- if tracker repair is required, what was updated before coding?: <issue/body/title/parent/follow-up/watchlist>

Watchlist decision:
- <maps to existing node | existing node refined | new manifestation added | new node needed>
- node: <...>

Intended closure level:
- <local symptom fixed | touched seam canonicalized | failure class eliminated>

Chosen-class closure commitment:
- this PR aims to eliminate the chosen working failure class entirely

Closure-feasibility check:
- can this chosen class actually be closed in one PR?: <yes/no and why>
- if the local endpoint were fixed, would another same-concept interpreter still remain live?: <yes/no and where>
- is the remediation changing real ownership/codepaths rather than only reconciling narrative around them?: <yes/no and how>

Required closure proof:
- focused proof: <...>
- supported-surface / end-to-end proof: <...>
- if this is a multi-step user-visible flow, what upstream gates on the relevant execution path must that proof preserve?: <...>

Architecture feedback:
- deeper architecture issue or type-model smell noticed: <... or none>
- long-run better direction: <... or none>
- tracking decision: <create/update issue | watchlist only | residual risk only | none>
- tracked at: <issue/watchlist/none>

Unsafe assumptions you are NOT making:
- <...>

Blocking ambiguities or split conditions:
- <... or none>
```

## 2. Independent Pre-Audit Gate Decision

Use on the issue thread before coding when the category requires independent gate review.

```text
Independent Pre-Audit Gate Decision

Issue:
- #<NNN>

Category:
- <failure-class | parity | semantic-drift | high-risk maintenance>

Decision:
- <pre-audit approved | pre-audit approved as first slice | pre-audit insufficient; widen class | pre-audit insufficient; escalate broad refactor>

Adversarial checks performed:
- <what parent / follow-up / closure / tracker claims were independently re-checked>
- <what claim was actively challenged or falsified>
- <what the mapped watchlist node implied about broader parent closure, and whether that changed the gate decision>

Why complete closure is not being required, if first-slice approval is used:
- <why the broader class is not the honest default for this PR>

Why first-slice approval is honest, if used:
- <chosen class is a real class boundary>
- <parent tracker is valid>
- <PR can still eliminate the chosen class entirely>

Why:
- <1-3 short bullets on why this gate outcome is honest>

Conditions before coding:
- <none>
- or <specific required correction if not approved>

Watchlist:
- <node / refinement decision>

Parent-class action:
- <absorb now | keep first-slice scope | open/update follow-up stream | leave parent open as unproven>
```

## 3. Broad Refactor Escalation

Use after pre-audit, before coding, when a narrow slice would be dishonest.

```text
Broad Refactor Escalation

Concept:
- <exact concept>

Chosen working failure class:
- <...>

Parent failure class:
- <...>

Canonical owner:
- <...>

Remaining sibling seams bypassing the owner:
- <seam 1>
- <seam 2>

Why a narrow fix would be dishonest:
- <...>

Proposed broader refactor scope:
- <what would move now>

Out of scope if broad refactor is approved:
- <...>

Main risks:
- migration risk: <...>
- behavior risk: <...>
- test-surface risk: <...>
- coordination risk: <...>

Lead decision requested:
- approved as broad refactor
- denied; keep first-slice scope
- split: do first slice now, open canonicalization follow-up
```

## 4. Post-Implementation Proof Audit

Use on the PR before review/merge.

```text
Post-Implementation Proof Audit

Head SHA:
- <sha>

Concept changed:
- <exact concept(s)>

Failure class:
- broader failure class: <...>
- chosen working failure class closed by this PR: <...>
- parent failure class above it, if any: <...>
- issue was symptom-shaped: <yes/no>

Canonical owner accounting:
- <owner> -> <consumers now using it>

Repo-wide consumer accounting:
- <seam> -> already consumes canonical owner
- <seam> -> moved to canonical owner in this PR
- <seam> -> different semantic concept, with proof
- <seam> -> still bypasses canonical owner and is explicitly split / escalated

Old non-authoritative paths:
- <invalid / non-authoritative / still surviving>

Sibling contexts checked:
- <...>

Generic failing proof used or created:
- <...>

Manifestation coverage table:
- manifestation: <...>
  - final status: <reproduced and fixed | execution-proven through same corrected path | split / escalated as separate class>
  - exact proof used: <...>
  - required supported-surface / end-to-end proof: <... or none>

Achieved closure level:
- <local symptom fixed | touched seam canonicalized | failure class eliminated>

If claiming failure class eliminated:
- <one explicit statement defending why the full currently known class is closed>

Parent-class residual state:
- <parent still open | parent apparently clean | no broader parent>
- tracked at: <issue/watchlist/follow-up>
- updated estimated remaining child slices to close the parent class: <number or range>
- rough grouping of remaining slices: <...>
- confidence level: <high | medium | low>
- did implementation/review change that estimate?: <yes/no and why>

Watchlist decision:
- <no watchlist change needed | refine existing node | add manifestation to existing node | create new node>

Architecture feedback:
- deeper architecture issue or type-model smell noticed: <... or none>
- long-run better direction: <... or none>
- tracking decision: <create/update issue | watchlist only | residual risk only | none>
- tracked at: <issue/watchlist/none>

Optional general feedback:
- <freeform engineering feedback, implementation feedback, or long-run notes; non-closure-bearing>
```

## 5. Follow-Up Decision

Use during pre-audit or review whenever something smells off and the next action is not obvious.

```text
Follow-Up Decision

Finding:
- <short statement of what smells off>

Classification:
- <concrete same-class remaining seam | broader parent class than PR chose | different class worth tracking | architecture smell / cleanup opportunity | still too weak to track>

Named seam:
- <... or none>

Named class:
- <... or none>

Remaining obligation:
- <... or none>

Action:
- absorb now
- update existing parent issue/stream
- create new child issue
- watchlist only
- residual risk only

Why this action is the honest one:
- <1-3 short bullets>

Tracked at:
- <issue/watchlist/PR residual risk>
```

## 6. Parent-Class State Update

Use on the parent issue or watchlist record after a child-slice PR merges.

```text
Parent-Class State Update

Parent failure class:
- <...>

Child failure class closed:
- <...>

Closed by:
- PR: #<NNN>

What is now proven:
- <...>

What remains open or unproven in the parent:
- <...>

Next known same-class stream or sibling seam:
- <... or none known yet>

Tracked at:
- <parent issue/watchlist/new child issue>
```
