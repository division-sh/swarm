# #2444 Decision and External-Effect Consumer Proof

This is a component receipt, not final full-suite qualification.

## Owners and Consumers

| Consumer | Disposition and exact path |
| --- | --- |
| PostgreSQL `AuthorizeExternalAttempt` | Moved manual transaction into the existing backend runner. Authority validation, provider-turn lease, launch reservation and row insertion all receive its SQL context. No provider callback is detached. |
| PostgreSQL `MarkExternalAttemptResponseObserved` | Moved named update into the existing runner; required authority and exact transition checks remain inside it. |
| SQLite authorization/response observation | Already use named mutation ownership; caller and busy-recovery policy remains intact. |
| Effect `Handle.MarkResponseObserved` | Existing response-evidence owner deliberately retains a received response across caller cancellation. This differs from cancelling an explicitly invoked store mutation before its COMMIT; neither grants a provider retry. |
| Both-store external recovery/settlement | Existing author-activity mutation outcome is propagated to candidate handoff. Acknowledged recovery summaries survive a later error; no-ack summaries are withheld. |
| Both-store human outcome/proposed route completion | Existing decision-card and private author-activity owners return acknowledged outcome independently of error; committed continuations survive failed handoff. |
| Both-store stage and loop supersession | Existing decision-card owners hand off on acknowledged COMMIT even with cleanup failure; uncertain COMMIT cannot submit candidate work. |

Removed the error-only decision handoff interpretation. Existing error-only
transaction entry points delegate to the acknowledged-outcome owner for consumers
without postcommit work; this is not a second commit authority.

## Execution Proof

- `TestExternalEffectClosedMutationCancellation`: PostgreSQL authorization,
  direct response observation and owned response observation, each under healthy,
  stopped admission, cancellation after write and injected lost COMMIT acknowledgement.
  Twelve cases, race PASS (22.355s). Real SQL/COMMIT through a fault-injecting driver
  wrapper, not a claim of wire failure. No automatic replay; observed response
  retention follows the existing controller, not a new cancellation exception.
- `TestDecisionCompletionPreservesCommittedHandoffOutcome`: SQLite and PostgreSQL
  human/proposed completion after exact persisted outcome evidence. Injected sink
  error after real COMMIT preserves exact card/state and error; one submission.
  PASS under race alongside existing human/proposed lifecycle tests (22.293s).
- `TestExternalEffectRecovery*`, `TestLifecycleAndExternalEffectAuthority*`,
  `TestProposedEffectDecisionAndSupersessionWinnerParity`,
  `TestProposedEffectSupersessionScopesParity`, and
  `TestDecisionCardStoreDeferDraftCancelAndSupersedeParity` are the adjacent
  existing-owner parity controls. The exact prefix selection passed under race
  (41.654s), including both-store recovery posture cases and stage supersession.

Underlying acknowledged/uncertain/cleanup behavior is independently exercised by
the backend transaction and completion-outcome matrices. The real process/transport
receipts are recorded separately; helper passes are not substituted for them.
