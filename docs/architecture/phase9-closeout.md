# Phase 9 Closeout

## Summary
Phase 9 is effectively complete.

The production-code genericity audit now leaves only two intentional non-test exceptions:

1. [`internal/runtime/pipeline/legacy_ids.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/legacy_ids.go)
- Contains the quarantined retired coordinator token for explicit legacy compatibility only.

2. [`internal/protocolheaders/headers.go`](/Users/youmew/dev/empireai/internal/protocolheaders/headers.go)
- Keeps the existing wire/header/query token names for protocol compatibility.

Everything else under `cmd/` and `internal/` production Go code is now clean by the current Phase 9 policy:
- no stray `pipeline-coordinator` references in active runtime/factory code
- no Empire-bearing literals outside:
  - approved product packages
  - the explicitly allowed wire-compat package

## What Changed

### Retired coordinator cleanup
- `internal/runtime/pipeline` no longer mentions `pipeline-coordinator` outside the explicit compatibility surface in [`legacy_ids.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/legacy_ids.go).
- `internal/factory/pipeline.go` no longer emits or routes as `pipeline-coordinator`; it now uses current node identities / contract-derived recipients.

### Scratch-code cleanup
- Removed the stray repo-root scratch file:
  - [`.phase2_repro.go`](/Users/youmew/dev/empireai/.phase2_repro.go)

### Guard/documentation alignment
- [`internal/runtime/architecture_guards_test.go`](/Users/youmew/dev/empireai/internal/runtime/architecture_guards_test.go) now explicitly documents the allowed wire-compat exception for [`internal/protocolheaders/headers.go`](/Users/youmew/dev/empireai/internal/protocolheaders/headers.go).
- [`internal/runtime/pipeline/pipeline_architecture_test.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/pipeline_architecture_test.go) now allows the retired coordinator token only in [`legacy_ids.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/legacy_ids.go).

## Final Audit

### Empire-bearing production files outside approved product packages
```bash
rg -l "Empire|empire-|empire_|empirecoordinator" cmd internal --glob '!**/*_test.go' --glob '!internal/runtime/productpolicy/empire/**' --glob '!internal/commgraph/empire/**' --glob '!internal/dashboard/**'
```

Result:
- [`internal/protocolheaders/headers.go`](/Users/youmew/dev/empireai/internal/protocolheaders/headers.go)

### Retired coordinator references in runtime/factory/commgraph/cmd production code
```bash
rg -l "pipeline-coordinator|legacyPipelineCoordinatorID" internal/runtime internal/factory internal/commgraph cmd --glob '!**/*_test.go'
```

Result:
- [`internal/runtime/pipeline/legacy_ids.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/legacy_ids.go)

## Verification
```bash
go test ./... -count=1
```

## Exit Assessment
Phase 9 completion: about `98/100`.

Why not `100`:
- the wire-compat header/query names are intentionally retained
- the retired coordinator token still exists in one explicit compatibility file

Why this is enough to move on:
- active production runtime and factory code are no longer coordinator-shaped
- non-test Empire-bearing code is effectively reduced to:
  - approved product packages
  - one explicitly documented wire-compat package
- the next work is boundary enforcement and regression prevention, which is Phase 10
