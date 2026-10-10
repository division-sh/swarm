# Q6 matched submitted-copy measurement

Acceptance: issue #2592, reviewer-d ruling 6091382892. This replaces missing
E1+ artifacts for Q6 only, not other ledger slices or historical E1 equivalence.

Pin delivered baseline 44c4047f0 plus the identical opt-in observer and workload
tests, and the complete candidate. Record Git source/tree and hashes for the
observer, both test sources, public registry corpus/input, command, Go/PostgreSQL
version and provider configuration before execution. The registry uses declared
system nodes, not a paid agent/provider substitute. The native sequence is a
synthetic mechanism workload. Use fresh stores and the same host/settings.

Run three paired baseline/candidate executions for each workload and each store:

```sh
TEST_POSTGRES_BIN=/usr/lib/postgresql/16/bin GOMAXPROCS=4 \
go test ./internal/runtime/pipeline \
  -run '^TestWorkflowCurrentTransitionNativeBytesAndHistoricalCutsBothStores$/sqlite$' \
  -count=1 -json -timeout=10m > native-sqlite.jsonl 2>&1

TEST_POSTGRES_BIN=/usr/lib/postgresql/16/bin GOMAXPROCS=4 \
go test ./internal/serveapp \
  -run '^TestVerifyRunJobflowRegistryIntegrationBothStores$/sqlite$' \
  -count=1 -json -timeout=5m > registry-sqlite.jsonl 2>&1

jq -s -f scripts/measure-q6/receipts.jq native-sqlite.jsonl
```

Repeat selectors with `/postgres$`. Keep the command exit, terminal tests and
every assertion. The unchanged native bound assertions intentionally fail on
the cumulative baseline: record those individual diagnostics. Any different
failure stops the pair; do not count it as expected baseline discrimination.
Candidate bounds and common constructor/handler/oracle/cold/historical checks
must pass. Registry keeps all clean/drift x text/JSON leaves per database.

Boundaries: native collection surrounds actual construction and each real
publication only, ending before physical inspection/cut planning/cold reads.
Registry collection starts immediately before public CLI run.start and detaches
after completed is observed, before business/history inspection, verify or
intentional corruption. Captured transactions are joined before snapshots; stop
does not erase in-flight attempts or grant future transactions the old receipt.

Counters describe logical already-serialized config and entity-metadata arguments
at actual write calls. Successful returned write calls, acknowledged commits,
uncommitted attempts, commit-indeterminate results, failed writes and attempted
rollback are separate. Unfinished bytes remain explicit. A no-op is not assumed
to perform zero writes. Revision SQL counts are attempted finalizer-interface
calls only; transaction attempts/outcomes include the selected transaction owner
within each window. Neither counter covers whole-workload SQL, WAL, physical
disk I/O or throughput. Retain actual stored byte lengths as separate checks.

Report every pair, including failures, zeros, count differences and unchanged
counts. Demonstrate bounded latest sizes and removed accumulated trajectory-copy
growth; no target savings ratio is assumed. These measurements do not clear the
#642 cut/evidence handoff or replace integrated lifecycle qualification.
