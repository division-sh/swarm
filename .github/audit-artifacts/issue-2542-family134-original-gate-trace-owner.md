# Cohort 134: original gate fixture trace owner

Both shared gate factories previously created a second store over the same
database exclusively for trace readback. SQLite used StartSQLiteRuntimeStorePair;
PostgreSQL called AdmitPostgresRuntimeStore twice. These were independent
selected coordinators, not supported close/reopen journeys.

The trace role now retains the exact first selected owner used by events,
lifecycle, cards and workflow persistence. SQLite opens one native store;
PostgreSQL no longer constructs the second reader. Both factories check exact
trace/selected pointer identity before returning. No new owner, delegate,
transaction protocol, reader cache, query or classifier exemption is introduced.

All 69 known caller references consume these two factories without caller-body
changes. Real both-store race proofs cover the six-rule supported-surface matrix
(persisted selection, trace selection and exact replay/delivery count), real CAS
retry selection with trace readback, and unavailable-pin recovery. The historical
ReconstructedTrace test selector is not credited as a restart/reopen proof: its
assertions now inspect durable records through the original selected reader.

Two finite codemod recipes preserve all other constructor fields, role bindings,
PostgreSQL raw setup/cleanup placement and original source/context choices.
The oracle rejects reader/role substitution and divergence from actual output.
Fresh census drops three findings / one raw operation, with zero additions.

This closes duplicate trace-reader construction in this shared factory family,
not the whole gate fixture. Its raw database field, SQLite Database getter,
PostgreSQL raw opener and remaining raw fault/observation consumers explicitly
remain #2542/#2151 migration debt. Other independent-store fixture families remain
parent-owned. This is shared construction/ownership consolidation, not measured
flake reduction, public parity expansion or global completion.
