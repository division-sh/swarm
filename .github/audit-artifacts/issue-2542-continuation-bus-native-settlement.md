# #2542 continuation: original-writer post-commit observation

Three native post-commit interceptor witnesses no longer inspect the fabricated
SQL-transaction context key. Each consumes the existing original selected
transaction collector and refuses missing instrumentation, no observed write
commit or any still-active selected transaction before its original event read.
The collector is installed after fixture/bus setup (and subscription for the
routed witness), immediately before actual publication, rather than after the
failing step. Actual post-commit visibility, recipients, scope, signal, returned
error/failure receipt and deferred event assertions are unchanged.

The deferred root's last raw setup was removed in favor of the existing native
PostgreSQL factory. Its root event still emits the original deferred event and
requires it committed before interception. The existing receiver-context owner
separately owns context isolation; native transaction settlement is not inferred
from an unrelated context-key namespace. One shared test assertion consumes the
same typed collector for all three witnesses, not a new runtime coordinator or
transaction context protocol.

Finite whole-method/root snapshots replace only the invalid key probes and their
collaborator construction. Exact AST normalization restores the original
interceptor initializer and removes only the named original-owner probe setup;
complete source/publication/visibility/assertion cuts still compare. Missing
observer, fabricated zero-write witness and active-work suppression are negative
controls. Actual three native PostgreSQL roots passed under race. Counter and
context proofs remain separate and are not credited as public parity closure.

Existing approved fake-protocol retirement, original native reader/writer and
receiver owners, raw_sql_policy and watchlist apply. No new SQL, raw permission,
runtime semantic change, framework or spec teaching. The in-memory component's
remaining fake transaction-key check and other fixture/Pool families are explicit
parent debt, not compatibility approval. Counts and source-bound receipts are
on #2542; no aggregate tier/server2/hosted/fork-deadline or parent closure.
