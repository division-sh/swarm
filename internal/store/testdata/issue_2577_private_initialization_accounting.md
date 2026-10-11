# #2577 Private Initialization Owner Accounting

The existing connection-private sessionpersistence owner now verifies WAL,
FULL synchronization and foreign keys before SDK preparation. Its three pragma
queries and three scans are private backend operations; no selected-store pool
or SDK execution permission is exported.

The existing capture owner acquires one private schema-initialization transaction
and owns both table definitions, required-column reads, row closure, commit and
rollback. Ten new exact findings describe that transaction/second-column reader;
three superseded database-level DDL/query calls are removed. The inventory adds
13 findings net. These are private-backend classifications, not fixture waivers,
raw selected-store admission or changes to the collector/debt policy.

The permanent initialization tests live inside the same private backend. The
public provider fixtures continue to consume its named storage operations.
WAL/shared-memory permission and no-follow probes use the existing session-state
fixture, without database access. SDK/capture files remain connection-private.
