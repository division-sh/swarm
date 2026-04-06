# Autonomous Work Protocol

This document defines the control loop between the lead and issue-scoped agents.

The previous GitHub-only wake-up model was too brittle:

- running watcher processes did not pick up script fixes until restarted
- GitHub comment polling was acceptable as an audit trail but weak as a wake-up signal
- local `AGENT_STATUS.md` mirrors could remain stale even when GitHub state was correct

The current protocol uses a local file bus for wake-up and GitHub for audit/history.

Use this together with:

- [COLLABORATION_WORKFLOW.md](/Users/youmew/dev/swarm/docs/COLLABORATION_WORKFLOW.md)
- [IMPLEMENTER_GUIDELINES.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_GUIDELINES.md)
- [IMPLEMENTER_REVIEW_CHECKLIST.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_REVIEW_CHECKLIST.md)

## Control Model

There are two control directions:

1. lead to agent
   - assignment
   - review outcome
   - fix request
   - handoff
   - stop / escalate

2. agent to lead
   - acknowledged
   - started
   - PR opened
   - fixes pushed
   - blocked
   - merged / returned idle

GitHub remains the source of truth for:

- issues
- PRs
- review comments
- merge history

But wake-up behavior now comes from local control files, not GitHub polling.

## Per-Agent Files

Each agent worktree owns these local files:

- `CONTROL.yaml`
- `RESPONSE.yaml`
- `AGENT_STATUS.md`

Examples:

- `worktrees/agent-a/CONTROL.yaml`
- `worktrees/agent-a/RESPONSE.yaml`
- `worktrees/agent-a/AGENT_STATUS.md`

Rules:

- `CONTROL.yaml` is written by the lead
- `RESPONSE.yaml` is written by the agent
- `AGENT_STATUS.md` is an operational mirror for humans
- `CONTROL.yaml` and `RESPONSE.yaml` are the actual wake-up contract

Helper scripts now exist for this bus:

- agent side:
  - `scripts/agent_control_watch.py`
- lead side:
  - `scripts/lead_response_watch.py`

If GitHub and local files disagree:

- GitHub wins for issue / PR truth
- the local files should be corrected to match

## Lead To Agent File

`CONTROL.yaml` is the lead-side command file.

Required fields:

```yaml
version: 12
agent: A
state: assigned
issue: 184
pr: null
action_type: next-task
action_status: assigned
branch_base: origin/master
next_step: Stop runtime diagnostics from inferring run_id from payload details.
scope_rule: Keep scope on the diagnostics/run-id seam; absorb narrow same-seam fixes only.
tests: Focused runtime/store/operator tests proving diagnostics no longer infer run selection from payload details.
risk: medium
agent_mode: work
source_issue: 184
source_pr: null
source_url: https://github.com/yazzaoui/Swarm/issues/184#issuecomment-4189991620
updated_at: 2026-04-06T02:07:00Z
```

Meaning of important fields:

- `version`
  - monotonically increasing integer
  - every lead-side control change must increment it
- `state`
  - control state the agent should honor now
- `action_type`
  - `next-task`
  - `review-update`
  - `fix-requested`
  - `stop`
- `action_status`
  - `assigned`
  - `approved`
  - `changes-needed`
  - `blocked`
  - `awaiting-second-review`
  - `handoff-and-reassign`
- `agent_mode`
  - `work`
  - `wait`
  - `handoff`
  - `stop`

## Agent To Lead File

`RESPONSE.yaml` is the agent-side response file.

Required fields:

```yaml
version: 7
agent: A
state: working
issue: 184
pr: 204
response_type: pr-opened
response_status: waiting-review
summary: Started issue #184 and opened PR #204.
tests_run:
  - go test ./internal/runtime ./internal/store -count=1
needs_lead: false
source_control_version: 12
updated_at: 2026-04-06T02:20:00Z
```

Meaning of important fields:

- `version`
  - monotonically increasing integer
  - every agent-side response change must increment it
- `response_type`
  - `acknowledged`
  - `started`
  - `pr-opened`
  - `fixes-pushed`
  - `blocked`
  - `idle`
- `response_status`
  - current state from the agent’s side
- `needs_lead`
  - `true` means the lead loop should wake and inspect immediately
- `source_control_version`
  - the exact `CONTROL.yaml` version the agent is responding to

## AGENT_STATUS.md

`AGENT_STATUS.md` remains useful, but it is not the wake-up contract.

Recommended format:

```md
Agent: A
State: working
Issue: #184
PR: #204
Branch: issue/184-diagnostics-run-id
Control-Version: 12
Response-Version: 7
Last-Updated: 2026-04-06T02:20:00Z
Next-Action: waiting for first review on PR #204
```

Default rule:

- `AGENT_STATUS.md` is for humans
- `CONTROL.yaml` and `RESPONSE.yaml` drive the automation

## Wake-Up Rule

The wake-up contract is simple:

- the engineer watcher returns when `CONTROL.yaml` changes
- the lead watcher returns when `RESPONSE.yaml` changes

Recommended detection:

- compare `version`
- or compare file content hash
- do not parse GitHub state as the wake-up trigger

GitHub can still be polled for verification, but not as the primary interrupt signal.

Default commands:

```sh
python scripts/agent_control_watch.py --watch
python scripts/lead_response_watch.py --watch
```

## Lead Loop

The lead loop is:

1. choose or review work
2. write `CONTROL.yaml`
3. update GitHub to match
4. wait for `RESPONSE.yaml` to change
5. react
6. repeat

When assigning work:

1. add the agent label to the GitHub issue
2. leave the structured issue comment
3. update `CONTROL.yaml` with:
   - new `version`
   - `action_type: next-task`
   - `action_status: assigned`
   - `agent_mode: work`

When reviewing a PR:

1. leave the structured PR comment
2. update `CONTROL.yaml` with:
   - new `version`
   - `action_type: review-update` or `fix-requested`
   - matching review state
   - exact `next_step`

When merging a PR:

1. merge on GitHub
2. ensure the issue is closed
3. either:
   - assign the next issue in the same `CONTROL.yaml` update
   - or set the agent to idle / wait

## Agent Loop

The agent loop is:

1. wait for `CONTROL.yaml` change
2. read `CONTROL.yaml`
3. do the requested work
4. update GitHub as needed
5. write `RESPONSE.yaml`
6. wait again

Default behaviors:

- if `agent_mode: work`
  - start or continue the requested work
- if `agent_mode: wait`
  - do not start unrelated work
  - wait for a newer control version
- if `agent_mode: handoff`
  - stop active waiting on the old PR
  - return to idle after writing the response
- if `agent_mode: stop`
  - stop that stream and escalate

## GitHub Synchronization Rules

GitHub remains mandatory.

The local files do not replace:

- issue labels
- issue comments
- PR comments
- merge actions

Instead:

- `CONTROL.yaml` is the wake-up / execution instruction
- GitHub is the authoritative external record

Required GitHub writes:

- assignment:
  - add agent label
  - leave issue comment
- PR opened:
  - agent comments on the issue with PR link
- review pass:
  - lead leaves structured PR comment
- fixes pushed:
  - agent comments on the PR with tests run
- merge:
  - PR merged
  - issue closed

## Risk Handling

Autonomous flow applies by default to:

- `low` risk PRs
- `medium` risk PRs

`high` risk PRs stay under explicit lead control.

For high-risk PRs, the lead must choose:

- `agent_mode: wait`
  - keep the agent on the PR
- `agent_mode: handoff`
  - free the agent for the next issue while the PR stays under manual lead handling

## Overnight Rule

When you are away:

- keep the system moving on low/medium-risk work
- prefer narrow `ready + fit` issues
- avoid broad architecture/spec/migration work by default

If one stream needs escalation:

- pause only that stream
- keep the others moving

## Failure Prevention Rules

Hard rules:

1. one open assigned issue per agent
2. one active PR per agent
3. every lead instruction must increment `CONTROL.yaml.version`
4. every agent response must increment `RESPONSE.yaml.version`
5. GitHub and local files must refer to the same issue/PR identifiers
6. agents do not self-pick unassigned issues
7. local files are structured data, not free-form chat

If any of these are violated:

- stop
- do not guess
- correct the control state first

## Transition From The Old Poller

The older GitHub polling scripts remain useful for inspection and debugging:

- `scripts/agent_poll.py`
- `scripts/lead_poll.py`

But they should no longer be treated as the primary wake-up transport.

Current position:

- GitHub polling scripts are debug/status tools
- `CONTROL.yaml` and `RESPONSE.yaml` are the wake-up bus

Operational rule:

- any old long-running `agent_poll.py --watch` process should be stopped
- move agents to the local-file watcher model

## Recommended Next Step

Implement the local file bus before the next autonomous overnight run.

Minimum implementation:

1. create `CONTROL.yaml` and `RESPONSE.yaml` in each agent worktree
2. ignore them in git
3. add a small agent-side watcher that wakes on `CONTROL.yaml` version change
4. add a small lead-side watcher that wakes on `RESPONSE.yaml` version change
5. keep GitHub writes as mandatory audit/history steps
