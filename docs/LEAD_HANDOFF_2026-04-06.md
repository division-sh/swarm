# Lead Handoff: 2026-04-06

## Context

This handoff is for the next lead engineer taking over the queue on `Swarm`.

The previous lead cycle got some useful issue/PR shaping done, but process discipline degraded. In particular:

- some reviews were approved too early
- existing PR conversation and inline review comments were not always checked before recommending merge
- the abandoned autonomous watcher/control-bus experiment created noise and should not be used operationally

Use direct manual lead control from here:

- assign via explicit prompt
- review PR description, conversation comments, inline review comments, code, and tests
- leave a human-readable review comment plus the short checklist-style PR comment

## Local Repo State

Root workspace:

- branch: `master`
- local HEAD: `bf689ee1f053fc8136b775e191f4b349b0b45e7e`
- remote `origin/master`: `37c82bcd0ae9dd431e8fd98da8ae6244510126b9`

Local uncommitted changes:

- [IMPLEMENTER_GUIDELINES.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_GUIDELINES.md)
- [IMPLEMENTER_REVIEW_CHECKLIST.md](/Users/youmew/dev/swarm/docs/IMPLEMENTER_REVIEW_CHECKLIST.md)

Those local doc edits add a new rule:

- when changing any derived selector/projection/read-model surface, compare it against the adjacent canonical owner
- require a counterexample test that would catch selector drift

There was also local Python bytecode junk from the abandoned private helper flow.

## Operational Mode

Do not use the autonomous protocol docs/scripts as the live operating model.

Files such as:

- [AUTONOMOUS_WORK_PROTOCOL.md](/Users/youmew/dev/swarm/docs/AUTONOMOUS_WORK_PROTOCOL.md)

record a prior operating model, but the user explicitly abandoned that
approach. Current expectation is manual lead orchestration. The private helper
scripts from that model are no longer public repo surfaces.

## Current Open PRs

### PR #214

- URL: `https://github.com/yazzaoui/Swarm/pull/214`
- Title: `[agent-b][issue #143] Drain in-flight work before runtime shutdown`
- CI: green
- Current correct status: `changes needed`

Why:

- there is a substantive inline review comment on the PR: `discussion_r3040634615`
- it points out a real race between `Shutdown()` and concurrent `Run()`
- `Shutdown()` sets `am.running = false` before canceling the old run
- a concurrent `Run()` can enter during the drain window, replace `am.runCtx` / `am.cancelRun`, clear `shuttingDown`, and then the shutdown path can cancel the new run instead of the old one

Important:

- an earlier approval/checklist comment was posted too early and is stale/wrong
- do not merge `#214` in its current state

Immediate next step:

- leave or refresh a `changes needed` review on `#214`
- require the race to be fixed before merge

### PR #213

- URL: `https://github.com/yazzaoui/Swarm/pull/213`
- Title: `[agent-c][issue #167] Remove remaining raw-response summary inference from agent and status readers`
- CI: green
- Review status: not fully reviewed in the last lead pass

What is known:

- no final substantive review was completed in the last pass
- do not assume readiness from green CI alone

Immediate next step:

- review `#213` from scratch using the full checklist

### PR #204

- URL: `https://github.com/yazzaoui/Swarm/pull/204`
- Title: `[agent-d][issue #194] Make canonical event payload validation fail closed before persistence`
- CI: green
- Current correct status: `changes needed`

Why:

- the current validator path still widens acceptance instead of enforcing the intended fail-closed seam
- in [eventbus_logger.go](/Users/youmew/dev/swarm/worktrees/agent-d/internal/runtime/eventbus_logger.go), the replacement validation path borrows trigger-schema properties and workflow entity fields into the allowed validation schema
- that means caller-supplied undeclared fields can still pass if they happen to match those names
- this is broader than the intended exception for runtime-owned canonical context

Immediate next step:

- keep `#204` blocked until the validator rejects undeclared caller payload rather than silently permitting it through schema widening

## Recent Review Mistakes To Correct For

### PR #214 was wrongly approved

The prior lead pass missed an existing inline review comment. Treat the current approval state as invalid.

### PR #212 was wrongly approved

`#212` itself is no longer in the open-PR list, but the review mistake matters because it exposed a repeatable failure mode.

The miss:

- the operator backlog projection counted only `d.status IN ('pending', 'failed')`
- store-side pending selection still treated stranded `in_progress` with no receipt as pending work
- this could hide real backlog after a crash

Follow-up issue created:

- `#215` `[runtime improvement] Keep operator backlog projection aligned with canonical pending-delivery selection`

This issue exists because the review failed to compare the derived operator selector against the adjacent canonical store owner.

### Process lesson

When reviewing any read-model / selector / projection change, explicitly ask:

- what adjacent canonical owner already defines the same semantic boundary?
- did this PR align to that owner or silently fork it?
- what counterexample state would reveal selector drift?

## Agent / Assignment State

At the moment the active streams appear to be:

- A: last assigned `#136`
- B: on open PR `#214`
- C: on open PR `#213`
- D: on open PR `#204`

Do not assume any autonomous polling state. Prompt each agent directly.

## Queue / Triage Notes

Dependency pass already found and corrected:

- `#187` is not ready; it is blocked by `#186`
- `#200` was packed into `#195` and closed

Confirmed queue correction:

- `#187` should remain `status:blocked` until the canonical terminal `flow_instances` owner from `#186` exists

New follow-up issue created from review miss:

- `#215` ready for implementation

## Review Process The Next Lead Should Follow

For every substantive PR review:

1. Read the PR description and ensure scope matches the issue.
2. Read existing PR conversation comments.
3. Read inline review comments, not just top-level comments.
4. Review code and tests.
5. Compare any derived selector/projection against the adjacent canonical owner.
6. Run focused tests locally where possible.
7. Leave:
   - a human-readable review comment
   - a short checklist-style PR comment
8. Only then recommend approve / changes needed.

This matters because the recent lead pass failed specifically on steps 2, 3, and 5.

## Recommended Immediate Next Actions

1. Sync local root branch with `origin/master` before doing more local review work.
2. Re-review `#214` and leave an explicit `changes needed` note for the shutdown race.
3. Review `#213` fully from scratch.
4. Keep `#204` in `changes needed` until the validator truly fails closed.
5. Decide whether to commit/push the local guideline/checklist doc updates.
6. Clean or ignore local Python bytecode junk from the abandoned private helper flow.

## Bottom Line

The queue is still usable, but trust should be placed in explicit artifacts, not assumed discipline.

The next lead should operate as if:

- no background automation exists
- prior approval state may be stale if existing comments were not checked
- selector/read-model changes need adjacent-owner comparison every time
