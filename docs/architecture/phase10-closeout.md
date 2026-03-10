# Phase 10 Closeout

## Status
- Phase 10 completion: 100/100
- Active production generic boundary: locked and green

## What Was Achieved
- Removed retired coordinator production references from non-test `internal/runtime/pipeline`, `internal/runtime`, and `internal/factory` code.
- Moved Empire scan taxonomy, directive parsing, corpus batching, and discovery candidate expansion out of generic `pipeline` and into approved product packages:
  - `internal/runtime/pipeline/empire`
  - `internal/runtime/productpolicy/empire`
- Moved prompt-schema guard product tokens out of generic runtime contracts and into product policy.
- Added hard guards so generic production files cannot reintroduce:
  - retired coordinator literals
  - raw wire-compat Empire header/query tokens outside `internal/protocolheaders/headers.go`
  - Empire/taxonomy tokens in generic `pipeline`
  - Empire/taxonomy tokens in generic `factory`

## Remaining Allowed Exception
- `internal/protocolheaders/headers.go`
  - allowed as wire-compat surface only

## Current Residual Generic Surface
Generic `pipeline` still contains neutral contract payload plumbing for fields such as `corpus_path`, but no Empire-specific taxonomy or business logic.

## Verification
```bash
go test ./... -count=1
rg -n "Empire|empire-|empire_|saas_gap|saas_trend|local_services|automation_micro" \
  internal/runtime internal/factory internal/commgraph \
  --glob '!**/*_test.go' \
  --glob '!**/empire/**' \
  --glob '!internal/protocolheaders/headers.go'
rg -n "pipeline-coordinator|legacyPipelineCoordinatorID" \
  internal/runtime/pipeline internal/runtime internal/factory \
  --glob '!**/*_test.go'
```

## Definition Met
- No Empire/taxonomy references remain in generic non-test production code under:
  - `internal/runtime`
  - `internal/factory`
  - `internal/commgraph`
  excluding approved `*/empire/*` packages and the allowed wire-compat file `internal/protocolheaders/headers.go`
- No retired coordinator references remain in generic non-test production code.
- Remaining generic `pipeline` mentions such as `corpus_path` are payload-shape plumbing, not Empire logic.

## Next Phase
- Phase 11 remains ahead for full genericity in the stronger sense:
  - more declarative node execution
  - less handwritten node behavior
  - further reduction of product-shaped payload semantics where feasible
