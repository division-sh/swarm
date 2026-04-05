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

## Spec Change Workflow

When a workstream requires a platform semantic change:

1. Keep `docs/specs/swarm-platform/platform/contracts/platform-spec.yaml` authoritative for `main`.
2. Do not put future semantics onto `main` ahead of code.
3. Use a review-spec draft file while the semantic change is still in design or implementation.
4. Land the authoritative YAML update in the same issue branch/PR as the matching implementation.

Default review-spec convention:

- store draft files in a dedicated local-only review directory
- name them with a workstream-specific suffix, for example:
  - `docs/specs/swarm-platform/platform/contracts/review/platform-spec.session-audit-split.yaml`
  - `docs/specs/swarm-platform/platform/contracts/review/platform-spec.schema-capabilities.yaml`

Default rule:

- review-spec draft: proposed future semantics
- authoritative YAML: semantics that are already true on `main`
- review-spec drafts stay gitignored until the matching implementation branch is ready to copy the finalized semantics into the authoritative YAML

If code on `main` and a proposed spec update would disagree:

- the proposed spec change stays in a review-spec draft
- the implementation branch becomes the convergence point for:
  - code
  - tests
  - authoritative YAML update

## Spec Gap Resolution Workflow

Use this sequence whenever an implementer believes they found a spec ambiguity or gap:

1. The implementer surfaces the ambiguity or suspected spec gap.
2. The lead reviewer evaluates it first.
3. If it is not a real gap, the implementer proceeds with the clarified interpretation.
4. If it is a real gap, escalate to the spec writer.
5. The spec writer works in a review-spec draft, not in the authoritative YAML on `main`.
6. The lead reviewer reviews the draft spec.
7. If the draft is approved, copy the reviewed draft into the relevant implementer worktree.
8. The implementer uses that reviewed draft as the target semantic contract.
9. When code, tests, and reviewed draft semantics align, the implementer updates the authoritative
   `platform-spec.yaml` in their branch by applying the approved semantic delta.
10. The PR lands with:
    - code
    - tests
    - authoritative spec update
    together.

Rules:

- implementers do not invent missing platform semantics locally
- spec writers do not put future semantics onto `main` ahead of implementation
- reviewed drafts are the staging area for semantic changes
- the implementation branch is the convergence point where reviewed draft semantics become authoritative

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

## Risk Classification

Every PR review must start with a risk classification:

- `risk: low`
- `risk: medium`
- `risk: high`

High-risk PRs include:

- canonical-owner migrations
- fail-closed semantic shifts
- typed-surface rollouts that stop trusting an older form
- store/runtime contract boundary changes
- broad dependency wiring refactors

Default rule:

- high-risk PRs require a second reviewer before merge
- the reviewer must state why the PR is high risk
- no high-risk PR merges on one review alone

## Migration Completeness Gate

For any PR that changes a canonical owner or removes trust in an older form:

1. Identify the new canonical owner explicitly.
2. Identify the old producers/readers/writers that are now invalid.
3. Check all relevant production paths, not only the main path.

Examples of paths that must be checked when relevant:

- builder
- scheduler
- replay / restore
- bootstrap
- reconfigure
- repair / retry
- store-side readers
- runtime-side readers

Default rule:

- "main path looks good" is not enough
- before approval, the reviewer must be able to say either:
  - `migration complete`
  - or `migration incomplete`

If migration is incomplete:

- the PR is not approved

## Semantic PR Workflow

For non-trivial semantic changes, default to:

1. reviewed branch-local spec delta first
2. implementation second
3. PR review against the reviewed spec delta

Use this by default for:

- contract changes
- identity / scope / routing changes
- persistence / read-model contract changes
- fail-closed semantic changes

This is not only for blocked work.
It is the default operating model for non-trivial semantic seams.

## Pull Request Standards

Default rule:

- do not use draft PRs

Why:

- draft PRs add coordination friction
- they create unnecessary extra state in a workflow that is already issue-scoped and review-driven
- the issue, worktree, branch, and direct reviewer feedback already provide enough staging structure

PR opening rule:

- open a normal PR only when the branch is ready for actual review
- if the work is not ready for review yet, keep working locally on the issue branch instead of opening a draft PR

PR description rule:

- every PR description must include:
  - `Human Summary`
  - `What Changed`
  - `Why This Is Needed`
  - `Scope Boundaries`
  - `Tests Run`
  - `Residual Risk`
  - `Follow-Up`

Default expectation:

- `Human Summary` must be plain language and easy to read quickly
- `Residual Risk` must name what still feels sharp, incomplete, or assumption-heavy
- `Scope Boundaries` must say what is intentionally not covered in the PR
- `Tests Run` must list exact commands, not vague claims like "tests passed"
- `Follow-Up` must not be used to push out narrow same-seam work that should be absorbed before merge

Review rule:

- reviewers must read the implementer summary and residual risk before deciding whether the code review is complete
- no merge recommendation should be given without checking that PR description context first
- if the PR describes a narrow follow-up item in the same seam, reviewers should flag it and ask for it to be encompassed in the PR instead of creating another tiny issue
- leave a follow-up separate only when it is meaningfully broader, riskier, or cross-boundary

PR title rule:

- every PR title must include:
  - the implementer identifier
  - the issue number
  - the workstream title

Canonical title format:

- `[agent-x][issue #123] Short workstream title`

Examples:

- `[agent-a][issue #12] Separate semantic flow scope from concrete flow-instance path`
- `[agent-c][issue #10] Introduce a typed persisted runtime descriptor for agents`

Default rule:

- PR titles should be operationally readable from the GitHub list view alone
- no generic titles such as:
  - `fix stuff`
  - `runtime cleanup`
  - `[codex] update`
- the title should make ownership and workstream identity obvious without opening the PR

Commit message rule:

- every commit subject should use:
  - `<type>: <short summary>`

Allowed commit types:

- `runtime`
- `store`
- `security`
- `conformance`
- `maintenance`
- `docs`
- `spec`
- `tests`

Default expectation:

- keep the subject short and readable in `git log`
- prefer verb + object phrasing
- keep issue numbers in branch names and PR titles, not in every commit subject unless they add real value
- do not use vague commit subjects such as:
  - `fix`
  - `updates`
  - `wip`

Examples:

- `runtime: preserve replay correlation envelope`
- `store: fail closed on non-UUID entity_id`
- `security: require auth on builder websocket`
- `maintenance: split boot verification modules`
- `docs: require PR summaries and narrow follow-up absorption`

## External Side-Effect Protocol

For external actions such as:

- PR merge
- issue close
- branch delete
- push
- deployment

Use this sequence:

1. attempt the action
2. verify the result against the authoritative external system
3. only then report completion

Default rule:

- textual narration is not authoritative
- completion claims must follow post-action verification
- if verification is missing or fails, report `attempted, unverified` or `failed`, not `done`

Preferred runtime/event model:

- action requested
- action attempted
- action verified
- action failed

Absence of a verified terminal event/state means:

- the work is not complete
- retry or escalate instead of advancing workflow

## Post-Merge Protocol

After a merge, follow this sequence in order:

1. verify the PR is actually merged on GitHub
2. close the linked issue only after merge verification
3. create any follow-on issue only after merge verification
4. update the local coordination/watchlist docs
5. send reassignment / cleanup instructions

Default rule:

- do not advance downstream workflow from a merge claim alone
- advance only from verified merge state

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
