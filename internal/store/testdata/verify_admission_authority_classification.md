# Verify Admission Authority Registry Accounting

Issue #2286, approved complete predicate/consumer class. This is exact static
authority accounting, not execution authority or a failure-class proof by census.

The unchanged guard found 178 new signatures and 20 obsolete signatures after
read-only inspection was separated from startup preparation. Each full resolved
signature is individually recorded in `persistence_authority_findings.tsv`.
No scanner, disposition vocabulary, raw-authority exception or ceiling changes.

## Classified Additions

| Owner / exact files | New findings | Classification and boundary |
| --- | ---: | --- |
| runtime/destructivereset/operation.go | 1 | typed-process-local: named pending-operation read, no transaction callback |
| runtime/llm/runtime_resolver.go | 2 | typed-process-local: immutable provider contract resolution, not runtime construction |
| runtime/manager/flow_runtime_readiness.go | 1 | typed-process-local: exact source-applicability read |
| runtime/manager/recovery_inspection.go | 5 | typed-process-local: closed existing recovery inventory readers |
| runtime/manager/retained_actor_inspection.go | 2 | typed-process-local: persisted actor/source reads before hydration |
| runtime/manager/selected_contract_route_recovery.go | 1 | typed-process-local: named route inventory reader |
| runtime/mcp/client.go | 1 | typed-process-local: owned discovery cancellation, no tool execution |
| runtime/runbundle/model.go | 1 | typed-process-local: active source availability reader |
| runtime/runforkexecution/recovery_inspection.go | 2 | typed-process-local: named exact fork-entry and recovery reads |
| runtime/startup_recovery_diagnostics.go | 1 | typed-process-local: manager-state observation callback, no SQL |
| runtime/tools/actor_tool_admission.go | 3 | typed-process-local: existing permission/native decisions and warning projection |
| runtime/tools/executor.go | 2 | typed-process-local: the executor consumes those same admission decisions |
| runtime/workspace/capability_admission.go | 1 | typed-process-local: immutable capability target resolution, no workspace creation |
| runtime/workspace/manager.go | 1 | typed-process-local: bounded command runner for dependency inspection |
| store/construction/open.go | 2 | construction-owner: native connector and private backend construction only |
| store/internal/backend/agentpersistence/execution_authority.go | 5 | private-backend: ordinary-run source observation through the existing read transaction |
| store/internal/backend/postgres/backend.go | 11 | private-backend: inspection-bound reads, private constructor and checked native disposal |
| store/internal/backend/postgres/inspection.go | 13 | private-backend: read-only transaction context, private SQL carrier, active-role revocation and mutation refusal |
| store/internal/backend/postgres/inspection_io.go | 7 | private-backend: bounded native inspection connection ownership and cleanup |
| store/internal/backend/postgres/possession_observation.go | 15 | private-backend: exact-key independent native session, transient try/release and checked disposal; no durable grant |
| store/internal/backend/postgres/transaction.go | 3 | private-backend: read transactions reuse the exact private inspection transaction |
| store/internal/backend/runforkpersistence/selected_recovery.go | 16 | private-backend: existing record/settlement/prospective-plan reads moved into reusable private helpers |
| store/internal/backend/runforkpersistence/selected_recovery_inspection.go | 11 | private-backend: exact fork recovery inspection through those same helpers, without fencing or settlement |
| store/internal/backend/sqlite/backend.go | 7 | private-backend: inspection-bound existing query methods |
| store/internal/backend/sqlite/fixed_read_statement.go | 3 | private-backend: canonical fixed reads consume the same inspection transaction |
| store/internal/backend/sqlite/inspection.go | 8 | private-backend: private read transaction context, active-role revocation and mutation refusal |
| store/internal/backend/sqlite/transaction.go | 3 | private-backend: read transactions reuse the exact private inspection transaction |
| store/internal/runtimepersistence/admission_inspection.go | 2 | private-runtime-adapter: typed observation callbacks delegate to the private backend, not raw transaction callbacks |
| store/internal/schemastore/inspection.go | 6 | private-domain-adapter: canonical compatibility and missing-DDL decisions, reads only |
| store/internal/schemastore/postgres_bootstrap.go | 1 | private-domain-adapter: boot consumes the same state-plan inspection before applying missing DDL |
| store/internal/schemastore/sqlite_bootstrap.go | 1 | private-domain-adapter: identical SQLite inspection-before-DDL ordering |
| store/internal/startupownership/reset_operations.go | 4 | private-domain-adapter: pending reset reads under the selected read transaction |
| store/selected/admission.go | 24 | typed-process-local: closed typed readers and revocable observation callback; no SQL/TX, process grant or writer escapes |
| store/store.go | 12 | typed-public-facade: six typed inspection/source/possession methods on each backend, no raw handle |

The 178 additions comprise 102 private-backend, 12 private-domain-adapter,
2 private-runtime-adapter, 2 construction-owner, 48 typed-process-local and
12 typed-public-facade findings. Private read contexts expire at callback exit;
they cannot authorize mutation or acquire another backend's session. The
momentary possession result is an observation, never a retained capability.

## Removed Signatures

The 20 stale entries are deleted signatures, not ignored live paths:

- construction/open.go: two calls changed to connector-backed construction.
- selected/authority.go: one read-only constructor call replaced the old open.
- backend/runforkpersistence/selected_recovery.go: ten reads moved to private
  record, settled-state and prospective-plan helpers shared with inspection.
- schemastore/postgres_bootstrap.go and sqlite_bootstrap.go: four reads moved to
  the canonical state-plan inspection helper.
- manager/selected_contract_route_recovery.go: the named reader replaces the old
  local lister interface.
- runbundle/model.go and startuprecovery/recovery.go: two availability methods
  now consume the shared exact active-source reader.

## Independent Proof Boundary

`TestPersistenceAuthorityFindingRegistry` still compares every exact signature.
`TestPersistenceEffectiveMethodSetsDoNotExposeRawAuthority` rejects public or
semantic-owner raw authority; the unknown-local-operation and hostile raw
carrier tests remain unchanged. Passing this accounting is not sufficient for
admission closure. Real selected read-scope revocation, both-store reset/source/
fork/schema inspection, transient possession lifecycle and public output tests
remain the execution witnesses in the SHA-bound PR proof audit.
