# Autonomous Work Protocol

This document defines the autonomous work loop between the lead and issue-scoped agents.

It is designed to let agents keep working through GitHub issues and PR comments with minimal
interactive coordination, while still preserving clear escalation boundaries.

Use it together with:

- [COLLABORATION_WORKFLOW.md](/Users/youmew/dev/swarm/docs/COLLABORATION_WORKFLOW.md)
- [IMPLEMENTER_GUIDELINES.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_GUIDELINES.md)
- [IMPLEMENTER_REVIEW_CHECKLIST.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_REVIEW_CHECKLIST.md)

## Source Of Truth

GitHub is the authoritative control plane.

Use these layers of truth:

1. GitHub issues:
   - assignment
   - scope
   - priority
   - completion state

2. GitHub PR comments:
   - fix requests
   - approval state
   - block / unblock decisions

3. Local status files in agent worktrees:
   - operational mirror only

Default rule:

- if GitHub and a local status file disagree, GitHub wins

## Core Action Types

There are only two control actions:

- `fix-requested`
- `next-task`

Default routing:

- `fix-requested` goes on the PR conversation
- `next-task` goes on the GitHub issue that owns the next assignment

## Agent Labels

Each agent has a dedicated GitHub label:

- `agent:A`
- `agent:B`
- `agent:C`
- `agent:D`

Default rules:

- at most one open issue should carry a given agent label at a time
- agent labels are assignment routing, not scope definition
- the issue should still receive a structured assignment comment

## Structured Assignment Comment

Use this on the assigned issue:

```text
[agent-action]
Type: next-task
Agent: A
Issue: #139
Status: assigned
Branch-Base: origin/master
Next-Step: implement gateway auth fail-closed and remove caller-owned privilege inputs from authorization
Scope-Rule: keep scope on gateway/tool auth seam; absorb narrow same-seam fixes
Tests: focused tests for deny-on-missing-auth and caller-privilege rejection
Respond-With: issue comment when work starts, PR link when PR opens
```

## Structured PR Review Comment

Use this after each substantive review pass:

```text
[agent-action]
Type: fix-requested
Agent: A
PR: #203
Status: changes-needed
Blocking: yes
Next-Step: mark session terminal on persistence-error paths before emitting run.failed
Scope-Rule: absorb-narrow-same-seam-fix
Tests: go test ./internal/builder ./internal/store -count=1
Respond-In: same PR comment thread
```

Allowed review statuses:

- `approved`
- `changes-needed`
- `blocked`
- `superseded`
- `awaiting-second-review`

## Review State Semantics

Agents must not infer what a review state means.

Default meanings:

- `changes-needed`
  - actionable
  - stop waiting and do the requested work
- `approved`
  - not actionable
  - no new code change is requested
  - keep polling until merge or a newer structured comment changes the state
- `awaiting-second-review`
  - not actionable by default
  - wait unless a newer structured comment requests code changes or explicitly hands the agent back to the queue
- `blocked`
  - stop that stream
  - escalate rather than guessing
- `superseded`
  - stop following the older instruction
  - the newer structured comment wins

If the PR is merged:

- do not wait for another comment
- return to idle state immediately
- switch to issue polling on the next one-shot cycle

## Agent Polling Loop

### While the agent has an open PR

The agent should:

1. run the poller in waiting mode
2. read the latest structured PR review comment
3. act on the latest valid review state

Rules:

- if status is `changes-needed`, address it
- if status is `approved`, wait for merge
- if status is `awaiting-second-review`, wait unless a newer structured comment changes the instruction
- if status is `blocked`, stop that stream and escalate
- if the PR is merged, return to unassigned state and switch to issue polling

Default command:

```sh
python scripts/agent_poll.py --agent A --watch
```

### Rebase And Refresh Rule

Other PRs may merge while an agent is still working.

Default rule:

- agents are responsible for rebasing their active branch onto `origin/master` whenever:
  - the lead explicitly requests it
  - CI or mergeability indicates drift from the current base
  - a newly merged PR changed the same seam enough that stale base state is likely to matter

Recommended command:

```sh
git fetch origin --prune
git rebase origin/master
```

If conflicts appear:

- resolve them in the issue branch
- rerun the relevant tests
- comment on the PR if the rebase changed behavior in a meaningful way

Do not rebase just because time passed.
Rebase when branch drift is likely to affect correctness, mergeability, or review clarity.

### While the agent has no open PR

The agent should:

1. run the poller in waiting mode
2. filter for its own label, for example `agent:A`
3. if one assigned issue exists:
   - read the latest structured assignment comment
   - start work
4. if none exists:
   - remain idle
   - poll again later

If an issue has the agent label but no structured assignment comment:

- treat it as not yet assigned
- wait
- do not infer the task from the label alone

Default rules:

- the agent should not self-pick unassigned issues
- the agent should not scan all issues looking for work
- the agent should only act on:
  - its current PR review comments
  - or the issue carrying its agent label

Default command:

```sh
python scripts/agent_poll.py --agent A --watch
```

## Polling Modes

The poller has two useful modes.

### Waiting Mode

```sh
python scripts/agent_poll.py --agent A --watch
```

Rules:

- use `--watch` when the agent is idle or waiting for review
- `--watch` owns the sleep / retry loop internally
- when actionable work appears, `--watch` returns control to the agent
- if the poller reports an invalid control state, stop and escalate instead of guessing

### One-Shot Mode

```sh
python scripts/agent_poll.py --agent A
```

Use one-shot mode for:

- manual verification
- debugging the current control state
- checking the latest status before starting the waiting loop

Default rule:

- normal autonomous waiting should use `--watch`
- one-shot mode is for explicit checks, not the steady-state waiting loop

## Agent Required Responses

When starting assigned work, comment on the issue:

```text
Acknowledged. Starting work on #139.
```

When opening a PR, comment on the issue:

```text
PR opened: #203
```

When responding to a fix request, comment on the PR:

```text
Addressed on latest head.
Tests run:
- ...
```

When the PR is merged:

- the agent returns to idle polling automatically

## Local Agent Status File

Each agent keeps one status file in its own worktree:

- `worktrees/agent-a/AGENT_STATUS.md`
- `worktrees/agent-b/AGENT_STATUS.md`
- `worktrees/agent-c/AGENT_STATUS.md`
- `worktrees/agent-d/AGENT_STATUS.md`

Recommended format:

```md
Agent: A
State: idle
Issue: none
PR: none
Branch: none
Control-Target: idle
Last-Seen-Comment-ID: none
Last-Updated: 2026-04-05T14:20:00Z
Next-Action: polling for assigned issue
```

Allowed `State` values:

- `idle`
- `assigned`
- `in_pr`
- `changes_requested`
- `blocked`

The status file must be updated:

- when assignment starts
- when the PR opens
- when review requests changes
- when blocked
- when the PR merges and the agent returns to idle

The poller updates `AGENT_STATUS.md` automatically after each check.

## Poller Script

The first polling implementation lives at:

- `scripts/agent_poll.py`

Supported modes:

- default one-shot mode
  - check once
  - update `AGENT_STATUS.md`
  - print the current state
  - exit `0` when no actionable change is found
  - exit `10` when actionable work is found
- `--watch`
  - poll every 10 minutes by default
  - return when a new structured assignment or PR review comment appears

Examples:

```sh
python scripts/agent_poll.py --agent A
python scripts/agent_poll.py --agent A --watch
```

The poller uses:

- the current branch to discover an open PR
- the agent label to discover issue assignments when no PR is open
- the latest structured top-level `[agent-action]` comment as the current instruction

If the poller detects an invalid control state, it should stop and report it instead of guessing.

Examples:

- more than one active PR for the same agent branch state
- more than one open issue with the same agent label
- assignment label present but the expected structured control comment is malformed or ambiguous

Default rule:

- use `--watch` for normal idle / waiting operation
- use one-shot mode for explicit manual checks

## Lead Responsibilities

The lead loop is:

1. keep backlog triaged
2. assign one issue to each free agent
3. review agent PRs
4. merge approved work
5. close completed issues
6. reassign freed agents promptly

In practice, the lead should:

- add the agent label to the assigned issue
- leave the structured assignment comment
- monitor active PRs for:
  - review comments
  - CI state
  - merge readiness
- leave a structured PR review comment after each substantive review pass
- merge when:
  - code is approved
  - CI is green
  - no unresolved blocking concern remains
- ensure the issue is closed
- immediately move the freed agent to the next ready assignment

## Autonomous Scope Limit

The autonomous review / merge loop is intentionally limited.

Default rule:

- autonomous PR handling applies only to `low` and `medium` risk PRs
- `high` risk PRs remain under explicit lead control

This means:

- agents may still open high-risk PRs
- but those PRs do not auto-advance through the autonomous loop
- high-risk PRs require explicit lead review handling before merge

After first review, the lead should make one of two agent-availability decisions for a high-risk PR:

- keep the agent on the PR
- or hand the agent back to the assignment queue while the PR stays under manual lead control

## Lead Polling Loop

The lead should poll continuously, ideally every 5 minutes, for:

- open PRs with new comments
- active PR CI state changes
- merged PRs that free agents
- assigned issues with agent updates
- idle agents that need new work
- stalled agents with no progress for too long

Default rule:

- PR comments are the live correction channel
- issue comments are the live assignment channel

The lead-side poller lives at:

- `scripts/lead_poll.py`

Recommended commands:

```sh
python scripts/lead_poll.py
python scripts/lead_poll.py --watch
```

Default behavior:

- one-shot mode prints the current system snapshot and exits
- `--watch` sleeps internally and returns when the system snapshot changes
- in this foreground TUI workflow, the lead should default to one-shot mode
- use `--watch` only in a separate background or dedicated waiting session
- after taking a lead action in the foreground TUI, rerun one-shot mode instead of blocking the session with `--watch`

## Escalation Rules

Escalate to the user when:

- issue scope no longer fits the assigned PR
- the required fix crosses into a different issue boundary
- an agent proposes a compatibility shim
- spec and implementation conflict and the right resolution is not obvious
- two open issues overlap materially and need prioritization judgment
- CI failure suggests a broader systemic regression rather than a local patch
- reviewer comments conflict in substance
- the next assignment is ambiguous between multiple issues
- the protocol itself gives conflicting instructions

Do not escalate for:

- ordinary fix requests
- narrow same-seam follow-up absorption
- routine rebases
- straightforward CI fixes
- obvious duplicate closures
- label cleanup
- assignment of already well-shaped issues

## Second Review Rule

For high-risk PRs, the lead may require a second review before merge.

Use the PR status:

- `awaiting-second-review`

Recommended comment shape:

```text
[agent-action]
Type: review-update
Agent: A
PR: #203
Status: awaiting-second-review
Blocking: no
Next-Step: wait
Reason:
- high-risk seam; second review requested before merge
```

Default rules:

- second review is lead-controlled
- the agent should not recruit its own second reviewer
- while a PR is in `awaiting-second-review`, the agent waits for the next structured PR comment
- the next structured PR comment should resolve the state to:
  - `approved`
  - `changes-needed`
  - or `blocked`

If the lead wants to free the agent while the PR stays under manual handling, the structured comment should say so explicitly.

Recommended extension:

```text
[agent-action]
Type: review-update
Agent: A
PR: #203
Status: awaiting-second-review
Blocking: no
Agent-Mode: handoff-and-reassign
Next-Step: wait for manual review; return to issue polling
Reason:
- high-risk seam; manual second review pending
```

Default rule:

- if `Agent-Mode: handoff-and-reassign` is present, the agent returns to issue polling on the next cycle instead of waiting on the PR

## Pause-One-Agent Rule

If one agent hits an escalation condition:

- pause only that agent's stream
- do not stop the whole wave

Continue operating with the other agents:

- review their PRs
- merge ready work
- assign next tasks
- keep backlog shaping moving

## Overnight / No-Response Rule

If the user is not responding, the lead should continue ordinary work without waiting:

- review PRs
- request fixes
- merge approved green PRs
- close completed issues
- reassign agents from the ready queue
- continue backlog shaping

If a specific stream needs user judgment:

- pause that specific stream
- continue the rest of the system

Default rule:

- while the user is away, keep the machine running
- only stop the stream that actually needs user judgment

## Overnight Assignment Bias

When the user is away, prefer assigning work that is likely to produce `low` or `medium` risk PRs.

Default overnight bias:

- prefer narrow, well-bounded issues
- prefer single-seam fixes
- prefer already-well-shaped `ready + fit` work
- avoid starting work that is likely to become a `high` risk PR unless the user already made that priority explicit

Avoid queuing overnight by default:

- broad architectural slices
- spec-ambiguity work
- compatibility / migration judgment calls
- work likely to require Reviewer B or manual merge handling

If a PR becomes `high` risk after first review:

- keep merge/review under manual lead control
- usually return the agent to the assignment queue instead of leaving it idle

## Scope Discipline

Narrow same-seam follow-up fixes should be absorbed into the current PR.

Open a new issue only when the work is meaningfully:

- broader
- cross-boundary
- riskier to review inside the current PR

## Conflict Resolution

If instructions conflict:

- the newest structured GitHub comment wins

If an issue label exists but no structured assignment comment exists:

- the agent should wait
- do not assume scope from the label alone

If the agent session restarts:

- run the one-shot poller immediately
- rebuild state from GitHub
- do not trust old terminal memory over the current GitHub control state

## Failure Prevention Rules

The protocol relies on a few hard rules to avoid ambiguous control states.

### One Open Assigned Issue Per Agent

At most one open issue should carry a given agent label at a time.

Default rule:

- do not assign a second open issue to the same agent until the prior assigned issue is merged,
  superseded, or explicitly unassigned

### One Active PR Per Agent

Each agent should have at most one active PR at a time.

Default rule:

- do not assign a new issue to an agent that still has an active PR unless parallel work is
  explicitly intended

### Assignment Requires Label And Comment

An issue is not a valid assignment unless both of these exist:

- the agent label
- the structured assignment comment

Default rule:

- label alone is not enough
- comment alone is not enough

### Only Top-Level Structured Comments Drive Actions

For protocol purposes, only top-level GitHub conversation comments count as control actions.

Default rule:

- agents should not act on inline review comments as their machine-readable control channel
- inline review comments remain normal review discussion, not polling protocol actions
- the newest valid top-level structured comment wins

### GitHub Wins Over Local Files

Local `AGENT_STATUS.md` files are an operational mirror only.

Default rule:

- do not make control decisions from the local status file when GitHub says otherwise

### Rebase Requests Are Explicit Control Actions

If the lead needs an agent to refresh an open PR branch, the lead should say so explicitly in a
structured PR comment.

Example:

```text
[agent-action]
Type: fix-requested
Agent: A
PR: #203
Status: changes-needed
Blocking: no
Next-Step: rebase onto origin/master, rerun touched tests, and update the PR head
Scope-Rule: no scope change; base refresh only
Tests: go test ./internal/runtime/... -count=1
Respond-In: same PR comment thread
```

### Idle Polling Is Read-Only

Polling should not create noise.

Default rule:

- agents poll silently
- they only comment when state changes:
  - assignment acknowledged
  - PR opened
  - review addressed
  - blocked

### Issue Closure Follows Merge

Default rule:

- do not close an issue when a PR is merely opened
- use `Part of #...` for partial work
- use `Closes #...` only when the PR really completes the issue
