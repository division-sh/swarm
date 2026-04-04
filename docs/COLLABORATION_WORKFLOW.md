# Collaboration Workflow

This document defines how multiple implementers should work in parallel on the Swarm runtime codebase.

It is designed to minimize:

- semantic drift
- merge conflicts
- duplicated fixes
- architecture regression caused by parallel local patches

It should be used together with:

- [IMPLEMENTER_GUIDELINES.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_GUIDELINES.md)
- [IMPLEMENTER_REVIEW_CHECKLIST.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_REVIEW_CHECKLIST.md)
- [RUNTIME_IMPROVEMENTS_AND_WATCHLIST.md](/Users/youmew/dev/swarm/docs/RUNTIME_IMPROVEMENTS_AND_WATCHLIST.md)

## Source Of Truth

Use these layers of truth:

1. Local architecture memory:
   - `docs/RUNTIME_IMPROVEMENTS_AND_WATCHLIST.md`

2. Execution queue:
   - GitHub issues

3. Implementation constraints:
   - `docs/IMPLEMENTER_GUIDELINES.md`
   - `docs/IMPLEMENTER_REVIEW_CHECKLIST.md`

The watchlist is the backlog and memory.
GitHub issues are the active execution units.

## Default Working Model

One implementer should own:

- one issue
- one branch
- one worktree

Default rule:

- do not let one branch or one PR accumulate multiple unrelated architecture items

## Branch And Worktree Naming

Use one issue number as the primary key.

Branch naming:

- `issue/<number>-<short-slug>`

Examples:

- `issue/1-schema-capability-boundary`
- `issue/2-typed-expression-model`

Worktree path naming:

- `worktrees/agent-a`
- `worktrees/agent-b`
- `worktrees/agent-c`
- `worktrees/agent-d`

Default rule:

- worktree paths are stable per implementer
- branch names rotate per issue
- the repo root remains the coordination/integration workspace and should stay on `main`

Suggested commands:

```sh
git fetch origin
git worktree add worktrees/agent-a -b issue/1-schema-capability-boundary origin/main
```

## Issue Standards

Each GitHub issue should define:

- problem statement
- why it matters
- source watchlist/backlog item
- canonical owner
- in-scope modules
- out-of-scope modules
- acceptance criteria
- required tests
- forbidden shortcuts
- suggested ownership boundary

Use:

- `Runtime Bug` template for concrete incidents/failures
- `Runtime Improvement` template for architecture/refactor initiatives

## Review Standards

Before merge, every implementer should apply:

- `docs/IMPLEMENTER_REVIEW_CHECKLIST.md`

And every review should ask:

- did this reduce semantic owners?
- did this remove duplication?
- did this avoid heuristic fallback?
- did this avoid product leakage?
- did this keep core logic elegant rather than branch-heavy?

## Coordination Rules

1. Parallelize by architecture seam, not by convenience.
2. Avoid overlapping write surfaces in the same tranche.
3. If a change expands beyond issue scope, update the issue before continuing.
4. If tests become unexpectedly hard to fix, escalate to architecture review instead of patching forward.
5. If two issues start converging on the same abstraction, pause one and re-scope rather than racing divergent implementations.

## Initial Parallel Tranche

Recommended maximum:

- 4 implementers

Recommended first tranche:

1. Explicit store schema-capability boundary
   - GitHub issue:
     - `#1`
   - suggested implementer workspace:
     - `worktrees/agent-a`
   - suggested branch:
     - `issue/1-schema-capability-boundary`
   - source backlog item:
     - High Priority #13
   - primary area:
     - `internal/store`
   - likely secondary area:
     - `internal/dashboard/server`
   - avoid editing:
     - `internal/runtime/engine`
     - `internal/runtime/tools`

2. Typed expression model and explicit scoped variable semantics
   - GitHub issue:
     - `#2`
   - suggested implementer workspace:
     - `worktrees/agent-b`
   - suggested branch:
     - `issue/2-typed-expression-model`
   - source backlog items:
     - High Priority #15
     - High Priority #19
   - primary area:
     - `internal/runtime/engine`
   - likely secondary area:
     - `internal/runtime/pipeline`
     - `internal/runtime/contracts`
   - avoid editing:
     - `internal/store`
     - `internal/runtime/sessions`

3. Session-state versus audit-state split
   - GitHub issue:
     - `#3`
   - suggested implementer workspace:
     - `worktrees/agent-c`
   - suggested branch:
     - `issue/3-session-audit-split`
   - source backlog item:
     - High Priority #17
   - primary area:
     - `internal/runtime/sessions`
     - `internal/store/llm_store.go`
   - likely secondary area:
     - `internal/dashboard/server`
   - avoid editing:
     - `internal/runtime/engine`
     - `internal/runtime/tools`

4. Per-turn structured flight recorder and denial diagnostics
   - GitHub issue:
     - `#4`
   - suggested implementer workspace:
     - `worktrees/agent-d`
   - suggested branch:
     - `issue/4-flight-recorder`
   - source backlog items:
     - High Priority #5
     - High Priority #6
   - primary area:
     - `internal/runtime`
     - `internal/runtime/diaglog`
   - likely secondary area:
     - `internal/runtime/tools`
     - `internal/runtime/mcp`
     - `internal/runtime/pipeline`
   - avoid editing:
     - store schema-compatibility internals unless explicitly coordinated

## When To Split Versus Merge Issues

Split issues when:

- the canonical owner is different
- the write surface is different
- the tests are different
- implementers can work independently without semantic overlap

Merge issues when:

- they are two halves of the same abstraction
- they would otherwise create two partial semantic owners
- one cannot be completed correctly without the other

## Definition Of Effective Parallelism

Parallel work is effective only if it produces:

- one canonical owner per concept
- fewer fallback paths
- fewer divergent implementations
- clean merges

If parallel work produces:

- overlapping abstractions
- duplicated partial fixes
- repeated rebases on the same files
- or branch-heavy compromise code

then the issue split was wrong and should be reworked.

## Root Workspace

Use the repo root as the coordination workspace.

Default expectations:

- keep root on `main`
- keep root relatively clean
- use root for:
  - issue triage
  - docs and backlog review
  - worktree creation
  - integration checks

Do not use the root workspace as a fifth feature branch by default.
