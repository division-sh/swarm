# G-24: SQL Migration Flattening — Final Cleanup

**Date:** 2026-03-14
**Risk level:** TRIVIAL — delete one empty directory
**Scope:** ~1 command

---

## Current state

G-24 is already 95% complete from earlier phases:

- `ApplyMigrationFile()`, `ApplyManagedMigrations()`, `MigrationSpec` — **deleted** (zero Go callers)
- `contracts/ddl-canonical.sql` — **deleted**
- `migrations/*.sql` files (26) — **deleted**
- Migration test references (`migrationPath`, `001_initial`) — **deleted** (zero test callers)
- `template_routing_store.go` — **deleted**
- Production boot — **contract-driven** via `GeneratePlatformTableDDLs` + `GenerateEntityTableDDLs` + `GenerateNodeStateTableDDLs` → `EnsureSchemaTables` (cmd/mas/main.go:317-350)
- Test bootstrap — **contract-driven** via `internal/testutil/postgres.go` `bootstrapPlatformTableStatements()` reading from platform-spec.yaml
- Spec — **complete**: `platform_tables` section at platform-spec.yaml:1501 defines all 12 platform tables with full DDL, timers entity_id/flow_instance nullable for global schedules

## What remains

**Step 1:** Delete the empty `migrations/` directory:

```bash
rmdir migrations/
```

That's it. The directory is empty — zero files inside.

## Verification

```bash
# Directory is gone
ls migrations/ 2>/dev/null && echo "FAIL: migrations/ still exists" || echo "PASS"

# Zero migration references in Go code
grep -rn 'migrations/' internal/ cmd/ --include='*.go'
# Should return zero results

# Zero migration API references
grep -rn 'ApplyMigrationFile\|ApplyManagedMigrations\|MigrationSpec' internal/ --include='*.go'
# Should return zero results

# Zero ddl-canonical references
grep -rn 'ddl-canonical' internal/ cmd/ --include='*.go'
# Should return zero results

# Tests pass
go test $(go list ./... | grep -v promptcontracts) -count=1 -timeout 180s
```
