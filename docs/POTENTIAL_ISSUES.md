# Potential Issues

This file is a staging area for concrete issue candidates that are not yet worth
opening as GitHub issues.

It is not a second backlog.

Use this file only when all of the following are true:
- the concern is specific and technically real
- it is not yet causing current drift, breakage, or repeated review findings
- it would be easy to lose track of it if left only in chat or memory

Promotion rule:
- if the same concern repeats
- or starts causing real semantic drift
- or blocks a clean implementation/review path
- open a GitHub issue and remove or mark the entry here

Entry format:
- `Candidate`: short concrete description
- `Why not an issue yet`: why it does not clear the issue bar today
- `Trigger to promote`: exact condition that should cause issue creation
- `Seen in`: PRs, reviews, or seams where it was observed
- `Captured by`: related issue or watchlist if one already exists
- `Status`: `watch`, `promoted`, or `dropped`

## Current Candidates

### 1. Delivery lifecycle log-detail assembly duplication

- `Candidate`: repeated assembly of `delivery_lifecycle_transition` detail fields across bus, manager, and runtime emit points
- `Why not an issue yet`: the touched seam is still small and current heads are aligned; the duplication is noticeable but not yet causing drift
- `Trigger to promote`: another same-seam PR duplicates the lifecycle detail shaping again, or any delivery state / reason / terminal-outcome field drifts between emit points
- `Seen in`: PR `#251`
- `Captured by`: none
- `Status`: `watch`

### 2. Flow-instance route rollback capability is optional next to route persistence

- `Candidate`: `FlowInstanceRouteRollbackPersistence` is modeled as optional even though partial-write route persistence makes rollback semantically coupled to `UpsertFlowInstanceRoute(...)`
- `Why not an issue yet`: current persisting implementations in the repo that matter for this seam already provide rollback behavior, and no current drift or breakage remains on PR `#258`
- `Trigger to promote`: another persisting route store appears without rollback support, or a future PR has to reason about whether rollback cleanup is available for a store that can partially write materialized routes
- `Seen in`: PR `#258`
- `Captured by`: none
- `Status`: `watch`
