# #2542 continuation: native run-control fixture family

The shared run-control seed consumes existing RunFixtureStore and RequireRun,
retaining scenario origin, exact caller run, identical canonical source hash,
running default and start-time materialization. Four callers use existing native
PostgreSQL construction and forward that original owner. The old helper raw
transaction path, raw setup/imports and duplicate manual cleanup are removed;
the file no longer consumes a raw database or legacy run fixture.

Both-store positive controls observe original native writer commits, canonical
active source and exact running lifecycle with no unfinished work. Actual four
pause/continue roots retain target-run isolation, pending delivery release,
pre-interceptor pause and post-commit deferred-event pause. All receipt, delivery,
no-early-execution and replay assertions remain unchanged under race.

The complete helper snapshot requires exact typed lifecycle/source construction.
Four prior whole-caller snapshots are updated, not duplicated. Only the explicit
constructor and seed arguments normalize in their old complete-journey oracles;
foreign owner, run, hash and origin negative controls still refuse. No source
reconstruction, SQL, callback, dialect interpreter, new framework or permission
is introduced. This is component fixture proof, not public admission parity.

Existing approved gate/watchlist and governing raw_sql_policy suffice; no runtime
semantic change calls for a new spec amendment. Census, guards and focused proof
receipts are on #2542. No aggregate tier, server2, hosted, fork-deadline or parent
closure claim; other bus/public-runtime construction and raw readers remain owed.
