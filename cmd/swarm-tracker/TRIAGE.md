# Triage rubric

The board's single source of truth is GitHub. An issue is **triaged** when it carries
five facts, each on its own axis, none substituting for another:

| Fact | Where it lives | Axis | Answers |
|---|---|---|---|
| score | `## tracker` yaml block in the body | **value** | how much shipping this is worth |
| priority | `priority:P0..P3` label | **urgency** | how soon it must start |
| complexity | `complexity:low/medium/high/extreme` label | **effort** | how big the work is |
| milestone | GitHub milestone | **program** | which arc it advances |
| agent | `agent:X` label (or `agent:unassigned`) | **owner** | whose queue it sits in |

The axes are independent by design: a trivial P0 (small fix, ship today) and a
high-complexity P3 (big work, no rush) are both coherent. Never encode urgency or
effort into the score.

## The score table

Score is **value only** — what delivering the issue is worth to the program.

| Score | Class | Test |
|---|---|---|
| 100 | wedge / regression-critical | a shipped capability is broken, or the GTM wedge depends on it; not shipping this costs us users we already claim to serve |
| 80 | program-critical / architecture | a program (milestone arc) cannot close without it, or it settles an architectural fact many issues consume |
| 60 | arc contributor | advances a live arc; the arc survives without it but is poorer |
| 50 | **must-line** | the check threshold: everything ≥50 must have an owner and must not go stale |
| 40 | worthwhile | real value, no arc pressure |
| 25 | nice-to-have | opportunistic; do when adjacent |
| 10 | recorded | kept for the record; may be closed if it rots |

Floors and rules:

- **DX/grammar floor**: developer-experience and grammar issues score at least 50.
  Developers are the first users; authoring friction is never below the must-line.
- **Score 0 / missing block = UNTRIAGED**, not "worthless". The tool flags it; the
  fix is a triage pass, never a silent zero.
- Blockers do not change an issue's own score — the graph computes unlocked score
  (own + transitively blocked) itself. Score the issue, not its position.
- Tracking-only umbrellas and watchpoint ledgers get a real score for what closing
  the whole class is worth; their children carry the executable scores.

## Priority

| Label | Meaning |
|---|---|
| P0 | drop everything; the build/operator is broken now |
| P1 | current-wave work; queues jump for it |
| P2 | normal queue order |
| P3 | when reached |

A blocker of a P1 inherits at least P1 (the graph already ranks it; the label keeps
humans honest).

## The tracker block

Machine-readable, one per issue body, exactly this shape:

```markdown
## tracker
```yaml
blockers: [123, 456]
score: 80
```
```

`blockers` lists open issues that must close first (issue numbers only).
`swarm-tracker assign` merges — unset flags preserve existing values. Body rewrites
must preserve the block; deleting it makes the issue UNTRIAGED.

## What the check sections mean against this rubric

| Section | Broken invariant |
|---|---|
| UNBLOCKED | blockers all closed — queue it |
| UNASSIGNED MUSTS | value/urgency above the must-line with no owner |
| TRIAGE DEBT | 2+ label/milestone facts missing |
| UNTRIAGED | no score recorded — the value judgment was never made |
| STALE MUSTS | must-line work going quiet |
| PHANTOM ASSIGNMENTS | an owner label with no activity behind it |
| DISCIPLINE | a PR branch working an issue its author isn't assigned to |
| BLOCKER CYCLES | the graph lies; fix the edges |

Process rules that ride the rubric: assignment is the `agent:` label alone (no
assignment comments); scores and rulings land on the tracker, never only in
conversation; a design-gated issue (`triage:needs-design-gate`) is triaged when
scored and owned — the gate governs *implementation*, not triage.
