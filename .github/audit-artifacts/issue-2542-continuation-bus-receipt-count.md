# #2542 continuation: shared bus pipeline receipt cardinality

The shared receipt-count helper and all ten call occurrences across six roots
consume existing CountPipelineEventReceiptStorage. Its private projection uses
the original native read transaction and existing pipeline cardinality owner:
exact event_id, subscriber_type=platform, subscriber_id=pipeline, all outcomes.
No run, status, success, eligibility or replay filter is added. Error still fails
the consuming witness rather than supplying a fabricated zero.

Run pause/continue, target isolation, pre-interceptor pause, post-commit emitted
event pause, ingress pause and post-commit receipt failure/recovery cuts remain
complete execution journeys. Zero pending receipts and one completed receipt
remain strict, as do actual delivery and interceptor assertions. Every call
forwards its identical selected owner instead of a raw pool. Other raw fixture
construction and observations in these roots remain explicit parent migration
debt, not hidden authority or approved compatibility.

Whole-function finite caller rewrites change only that argument. Two prior
construction snapshots have their After updated; there is no duplicate inventory.
Exact helper shape rejects changed owner/event, swallowed errors and manufactured
counts. Actual affected roots run under race; existing both-backend acknowledgment
count and refusal controls independently prove the canonical reader scopes.

No new SQL, classifier, framework, production behavior or spec semantic amendment
is introduced. Existing gate/watchlist and governing raw_sql_policy suffice.
Final census/guards and receipts are on #2542; no tier, hosted, server2,
fork-deadline or parent closure claim.
