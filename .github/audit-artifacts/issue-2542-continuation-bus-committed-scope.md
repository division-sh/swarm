# #2542 continuation: committed pipeline scope observation

The existing pipelinepersistence.LoadCommittedScope is the semantic owner of
stored scope selection and ParseCommittedScope validation. A named fixture read
in the existing event-readback adapter now calls that owner in one original
selected read transaction. The storetest bridge returns only CommittedScope or
an error, validates the exact event/native owner, and returns no partial value.
No SQL, selector, callback, raw getter or alternate scope parser is public.

loadCommittedPipelineScopePostgres consumes this owner instead of SQL/Parse and
no longer accepts sql.DB. Both callers pass their actual selected store: the
post-commit interceptor and the acknowledged-return/held-dispatch test. Their
complete finite inverses permit only these owner argument changes. Event commit,
recipient set, requested scope, context-transaction absence, completion signal,
early acknowledgment, held work, unchanged quiescence deadline, eventual delivery
and joined teardown remain intact. Existing fake transaction-context assertions
and other raw bus families are still named parent migration debt, not claimed
closed or treated as an authoritative production protocol by this cohort.

Missing scope now uses the existing canonical ErrMissingScope rather than local
sql.ErrNoRows; either remains a fatal refusal in these callers. Parse failures
consume the same canonical enum parser, not a new field policy or fallback.
PostgreSQL/SQLite query ownership remains in the existing pipeline owner.

Actual race proof: TestEventBusPublishTransactional_RunsInterceptorsAfterCommit
and TestEventBusPublishAcknowledgedReturnsBeforePostCommitDispatchCompletes run
their real held post-commit path. TestCommittedPipelineScopeObservationUsesOriginalOwnerBothStores
constructs direct/subscribed evidence through existing semantic event fixture
recipes and proves one original read commit/no write on both stores. Missing,
invalid, foreign, canceled and closed readers must refuse without a scope value.

The finite rewrite owner also now distinguishes exact receiver type when method
names repeat. This is necessary for the actual postCommitTxAbsentInterceptor
consumer, not a general matcher or source-wide replacement rule. Hostile controls
reject foreign receiver, pointer/value drift, a free function, duplicate matching
declarations and whole-body parameter drift, while preserving an unrelated
same-named method. Both actual application and inverse/type-overlay consumers
use uniqueRecipeFunction; no second interpretation is introduced.

The complete package initially caught an existing free-function/method ambiguity
negative control. Exact receiver selection now applies only to method recipes;
free-function recipes retain their original ambiguity refusal. The original
negative control is unchanged and passes alongside the new receiver controls.

The three complete recipes and hostile caller/helper controls preserve all
assertions. An initial private-test context-helper signature mistake failed
compilation before execution; an initial producer probe also lacked its run.
The latter was repaired through requireRunningRunForTest before the unchanged
semantic event publication recipe, never by raw seeding. Failed receipts earn
no proof credit. Governing boundary is
selected_contracts.selected_runtime_store_projection.raw_sql_policy. Existing
gate/watchlist suffice; no new framework, tracker, runtime policy/spec change or
compatibility. This closes the committed-scope observation cohort, not other bus
setup/fault/context seams or #2542/#2151. Exact counts/final receipts are on the
increment issue record; no core/full/server2/hosted/fork-deadline closure claimed.
