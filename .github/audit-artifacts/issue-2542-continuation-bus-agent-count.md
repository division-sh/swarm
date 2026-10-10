# #2542 continuation: shared agent delivery cardinality

The bus delivery-count helper and four caller occurrences consume existing
CountAgentEventDeliveryStorage, whose native projection delegates to the closed
delivery owner. The original exact event and subscriber_type=agent predicate
counts every physical state; no lifecycle, eligibility, completion or run filter
was added. Read failures remain fatal, never a fabricated count.

Actual standalone platform event types retain their zero-delivery completion
assertions, and routed standalone, ingress pause and receipt-failure witnesses
retain strict nonzero counts, settlement/recovery and real delivered-event checks.
Two now raw-free caller roots use the existing native PostgreSQL factory and sole
fixture close owner. Remaining standalone raw lifecycle/claim readers are parent
debt, not permitted legacy behavior or claimed family closure.

Finite complete-root rewrites change only the exact observation argument and
the enumerated unused construction. Existing construction snapshots are updated,
not duplicated, and their complete workload oracles retain every temporal and
correctness cut. Foreign owner/event, swallowed read error and zero fabrication
are negative controls. Affected actual roots run under race; both-store canonical
cardinality/refusal controls prove the existing private reader independently.

No new SQL, semantic owner, classifier, framework or production/spec behavior.
The governing raw_sql_policy, approved migration gate and current watchlist
remain sufficient. Counts and focused receipts are on #2542; no aggregate tier,
server2, hosted, fork-deadline or parent completion claim.
