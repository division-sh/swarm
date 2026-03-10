# Phase 9 Plan

## Goal
Finish the final genericity audit and close the remaining non-test production-code exceptions that still block the "no Empire in generic Go code" bar.

Phase 9 is not another large refactor. It is a closeout phase:
- eliminate the last retired `pipeline-coordinator` references from active production code
- isolate or justify the last wire-compat Empire strings
- leave only explicit product packages as acceptable non-test Empire-bearing code

## Current Audit

### Phase 8 carryover
Active runtime behavior is no longer coordinator-shaped.

The remaining production `pipeline-coordinator` / `legacyPipelineCoordinatorID` references are now limited to:
1. [`internal/runtime/pipeline/legacy_ids.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/legacy_ids.go)
2. [`internal/runtime/pipeline/workflow_nodes.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_nodes.go)
3. [`internal/runtime/pipeline/workflow_nodes_runtime.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_nodes_runtime.go)
4. [`internal/runtime/pipeline/workflow_contract_validation.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_contract_validation.go)
5. [`internal/factory/pipeline.go`](/Users/youmew/dev/empireai/internal/factory/pipeline.go)

### Remaining non-test Empire-bearing production files outside the agreed product packages
Repository-wide audit of production `cmd/` + `internal/` code leaves:
1. [`internal/protocolheaders/headers.go`](/Users/youmew/dev/empireai/internal/protocolheaders/headers.go)

The explicitly allowed product-specific packages remain:
1. [`internal/runtime/productpolicy/empire/policy.go`](/Users/youmew/dev/empireai/internal/runtime/productpolicy/empire/policy.go)
2. [`internal/commgraph/empire/policy.go`](/Users/youmew/dev/empireai/internal/commgraph/empire/policy.go)

### Scratch / non-production cleanup
There is also a stray repo-root scratch file:
1. [`.phase2_repro.go`](/Users/youmew/dev/empireai/.phase2_repro.go)

That file should not survive a final genericity audit as a production `.go` file.

## Step-By-Step Plan

### 1. Finish the retired coordinator cleanup in `internal/runtime/pipeline`
- Remove or reduce `legacyPipelineCoordinatorID` references in:
  - [`internal/runtime/pipeline/workflow_nodes.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_nodes.go)
  - [`internal/runtime/pipeline/workflow_nodes_runtime.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_nodes_runtime.go)
  - [`internal/runtime/pipeline/workflow_contract_validation.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_contract_validation.go)
- Keep only one clearly labeled compatibility surface if it is still needed:
  - [`internal/runtime/pipeline/legacy_ids.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/legacy_ids.go)
- Exit bar:
  - active production runtime files no longer mention `pipeline-coordinator`
  - any remaining legacy constant lives in one compatibility file only

### 2. Remove `pipeline-coordinator` from `internal/factory`
- Replace hardcoded `pipeline-coordinator` delivery/source behavior in:
  - [`internal/factory/pipeline.go`](/Users/youmew/dev/empireai/internal/factory/pipeline.go)
- Use one of:
  - contract-derived recipient lookup
  - neutral runtime source identity
  - injected factory/runtime policy helper
- Exit bar:
  - `internal/factory` contains no `pipeline-coordinator` production references

### 3. Decide the final treatment of wire-compat headers
- Audit [`internal/protocolheaders/headers.go`](/Users/youmew/dev/empireai/internal/protocolheaders/headers.go).
- Choose one of two explicit outcomes:
  1. keep wire names as-is but classify this package as an allowed protocol-compat boundary, or
  2. rename the wire tokens and migrate callers/tests
- Recommendation:
  - keep wire compatibility for now, but explicitly document `internal/protocolheaders` as an allowed non-product exception if the external protocol must remain stable
- Exit bar:
  - the exception is explicit and documented, or the strings are removed

### 4. Remove the stray scratch Go file from the genericity audit surface
- Delete, rename, or otherwise quarantine:
  - [`.phase2_repro.go`](/Users/youmew/dev/empireai/.phase2_repro.go)
- Exit bar:
  - repo-wide production Go audit no longer sees ad hoc scratch code

### 5. Update architecture guards to the true Phase 9 end state
- Tighten:
  - [`internal/runtime/architecture_guards_test.go`](/Users/youmew/dev/empireai/internal/runtime/architecture_guards_test.go)
  - [`internal/runtime/pipeline/pipeline_architecture_test.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/pipeline_architecture_test.go)
- New guard expectations:
  - `pipeline-coordinator` is not allowed in active production files
  - Empire literals are only allowed in:
    - approved product packages
    - optionally `internal/protocolheaders` if retained as a documented wire-compat boundary
- Exit bar:
  - Phase 9 boundary is enforced automatically

### 6. Run and record the final audit
- Run:
```bash
rg -n "Empire|empire-|empire_|empirecoordinator" cmd internal --glob '!**/*_test.go'
rg -n "pipeline-coordinator|legacyPipelineCoordinatorID" internal/runtime internal/factory internal/commgraph cmd --glob '!**/*_test.go'
```
- Classify each remaining hit as:
  - allowed product package
  - allowed wire-compat package
  - bug
- Record the result in a short Phase 9 closeout note.
- Exit bar:
  - no surprises remain

## Suggested Execution Order
1. `internal/runtime/pipeline` legacy coordinator closeout
2. `internal/factory/pipeline.go` cleanup
3. `internal/protocolheaders/headers.go` decision
4. remove `.phase2_repro.go`
5. tighten guards
6. final audit + closeout note

## Definition Of Done
Phase 9 is complete when:
- active production runtime/factory code no longer uses `pipeline-coordinator`
- all remaining non-test Empire-bearing Go files are either:
  - explicit product packages, or
  - one explicitly documented wire-compat package
- the repo-wide non-test Go audit is clean by policy
- architecture guards enforce that state
