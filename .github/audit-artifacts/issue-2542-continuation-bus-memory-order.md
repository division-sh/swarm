# #2542 continuation: memory publication ordering witness

The in-memory publication interceptor retains its real recorded-event lookup,
exact event-type match and failure if interception precedes persistence. It no
longer inspects a fabricated SQL-context key: this component store has no SQL
transaction to begin with. Its final fake protocol import is removed from the
bus publication test file. No new context interpreter replaces it.

The existing receiver context owner and separately qualified actual native
settlement/pool witnesses own value isolation and durable resource lifetime.
This cohort claims only memory persistence ordering, not durable atomicity or
public backend parity. The finite method rewrite removes exactly the invalid
three-line check and renames only the now-unused context parameter. The actual
memory root and receiver context/routing controls run under race.

The approved fake-protocol migration, current watchlist and governing raw_sql_policy
apply. No SQL, runtime semantics, new abstraction, compatibility, assertion,
timeout or permission change. Other raw bus/replay fixtures and the remaining
protocol package consumers stay parent debt. Counts and proof receipts are on
#2542; no tier/server2/hosted/fork-deadline or parent closure claim.
