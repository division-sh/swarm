# Exact Delivery Reader Native Fixtures

Track: FULL. Local portion of the single consolidated repair under
2698/6205327685; not a PR push, final-head event or landing receipt.

Seven original diagnostic roots reached Claim with no admitted native actor.
Their positive callers now enumerate their complete executable identities,
resolve descriptors, prepare actors through the existing source-aware fixture
owner and explicitly admit the original generation before publication. Claim,
its validation, storage-only controls and pending-only siblings are unchanged.
The A2 proof prepares both original agent identities, not just the successful
one. The bounded projection proof prepares only its executable current-agent;
its pagination-only pending agent remains unadmitted.

The author-activity proof retains its exact run-started, event-emitted,
in-progress and delivered records. Native preparation additionally records
agent-registered and agent-running. It explicitly asserts all three setup
records, their unchanged full prefix and exactly three additional receipt
records; it does not filter the activity stream or suppress native history.
Rendering still checks the full stream for private payload leakage, duplicate
settlement still refuses, and complete occurrence equality and source timestamps
remain unchanged after replay. The initial six-row/four-row oracle mismatch and
the intermediate setup-count failure are retained as RED evidence.

Logs under /home/youmew/.cache/swarm-2542-family145/:

| Receipt | Exact result |
| --- | --- |
| pr2698-remaining-persistence-root-ledger.jsonl | Original seven roots RED on missing lifecycle/grant, within the larger unresolved selection. |
| pr2698-positive-claim-readers-race.log | Six complete roots PASS on SQLite/PostgreSQL under race; author-activity RED, not an aggregate PASS. |
| pr2698-completion-command-purpose-race.log | Repaired complete author-activity root PASS on both stores under race, 7.71s; other selected roots have their own dispositions. |
| pr2698-positive-claim-readers-vet.log | Package vet PASS before the author-activity observation adjustment; later package vet accompanies the combined batch. |

The six roots are TestA2ClaimObserversKeepExactNodeIdentityAndSettledAggregateBothStores,
TestActiveRunDeliveryQuiescenceReadbackParity,
TestAgentSettledAttemptStoragePreservesEventRoleSubscriberAndOriginalReadBothStores,
TestCanonicalClaimAndAppearanceObserversKeepExactAttemptCutsBothStores,
TestPendingAgentDeliveryRetryEligibilityPreservesSubsecondStoreParity and
TestDeliveryReadProjectionBoundsAndExactIdentityParity. The seventh is
TestAuthorActivityDuplicateTerminalReceiptIsNoOpParity.

No other failing ledger row inherits a PASS from this owner. Final finite
history composition, registry/ratchet/native guards, frozen-head HumanTask
both-store proof and canonical preflight remain required before the one PR push.
