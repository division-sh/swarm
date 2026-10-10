# #2542 continuation: real empty receiver-context proof

The old component store installed a test-only SQL-connection value only inside
CommitPublication's local context, which its return type cannot export. That
disconnected injection proved neither transaction lifetime nor dispatch context
reset. Its in-memory database, connection, raw field and fake SQL-key probes are
removed. The existing receiver owner remains eventreceiver.NewContext, invoked
by acceptedWorkExecutionContext before post-commit dispatch.

The revised component witness places an ordinary opaque value on the actual
publisher context, proves it reached commit, and requires it to be absent at the
exact PostCommitDispatchStarted signal. The native PostgreSQL root carries the
same witness through real publication. Original root names remain for inventory
stability, but this is universal receiver value isolation, not fabricated SQL
authority or new context plumbing. The existing core context proof verifies
empty values plus lifetime/deadline semantics. Actual publication/replay pool
saturation roots separately prove selected connection lifetime and all original
receipt assertions; they are not replaced by an ambient-value assertion.

All five focused roots passed under race. An external overlay changes only the
existing receiver owner's empty base to context.WithoutCancel(lifetime); both
bus roots then fail at the leak observer, rather than compilation, timing or
missing input. This deterministic falsification prevents the earlier vacuity.
Production remains unchanged. The mutation and its red receipts are not committed.

Finite exact method/root snapshots and one updated native root preserve actual
commit, publisher-value positive, dispatch signal and error handling. The small
owner repair changes the component store's field to a boolean observation, never
a resource or authority. Other post-commit fake transaction-key tests remain
explicit parent debt and are not credited as production ownership proof.

The approved fake-protocol retirement class, existing receiver/lifecycle owners,
raw_sql_policy and watchlist suffice. No new framework, runtime interpretation,
store hook, compatibility path or spec amendment. Final counts/proof receipts
are on #2542; no aggregate tier/server2/hosted/fork-deadline or parent closure.
