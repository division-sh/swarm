# Phase 10 Plan

## Goal
Finish Phase 10 with no irreducible Empire logic leaking through generic runtime code.

By the end of Phase 10:
- generic/shared production code under `internal/runtime/**`, `internal/factory/**`, and `internal/commgraph/**` should contain only platform/runtime semantics
- any irreducible product-specific logic must live in explicit product packages, primarily:
  - [`internal/runtime/pipeline/empire`](/Users/youmew/dev/empireai/internal/runtime/pipeline/empire)
  - [`internal/runtime/productpolicy/empire`](/Users/youmew/dev/empireai/internal/runtime/productpolicy/empire)
  - [`internal/commgraph/empire`](/Users/youmew/dev/empireai/internal/commgraph/empire)
- the only tolerated non-product exception is the protocol wire-compat package:
  - [`internal/protocolheaders/headers.go`](/Users/youmew/dev/empireai/internal/protocolheaders/headers.go)

So Phase 10 is both:
- boundary locking
- and the final sweep that pushes any remaining irreducible product logic out of generic packages

## Current State

## File-Level Migration Matrix

### Delete Entirely
These are pure compatibility/migration scaffolding and should disappear rather than survive in generic packages.
- [`internal/runtime/pipeline/coordinator_scan_compat.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/coordinator_scan_compat.go)
- [`internal/runtime/pipeline/coordinator_validation_compat.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/coordinator_validation_compat.go)

### Move To Empire Packages
These currently carry irreducible Empire behavior and should stop living in generic/shared packages.
- [`internal/runtime/pipeline/helpers.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/helpers.go)
- [`internal/runtime/pipeline/scan_campaign_manager.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/scan_campaign_manager.go)
- [`internal/runtime/pipeline/coordinator_scan.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/coordinator_scan.go)
- [`internal/runtime/pipeline/scan_orchestrator_runtime.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/scan_orchestrator_runtime.go)
- [`internal/runtime/pipeline/coordinator_discovery.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/coordinator_discovery.go)
- [`internal/runtime/pipeline/discovery_aggregator_runtime.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/discovery_aggregator_runtime.go)
- [`internal/runtime/pipeline/coordinator_scoring.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/coordinator_scoring.go)
- [`internal/runtime/pipeline/directive_parser.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/directive_parser.go)
- [`internal/runtime/contracts/prompt_schema_guard.go`](/Users/youmew/dev/empireai/internal/runtime/contracts/prompt_schema_guard.go)
- [`internal/runtime/corpusobs/observability.go`](/Users/youmew/dev/empireai/internal/runtime/corpusobs/observability.go)
- [`internal/runtime/mcp_hooks.go`](/Users/youmew/dev/empireai/internal/runtime/mcp_hooks.go)
- [`internal/runtime/manager/receipts.go`](/Users/youmew/dev/empireai/internal/runtime/manager/receipts.go)
- [`internal/factory/contracts_policy.go`](/Users/youmew/dev/empireai/internal/factory/contracts_policy.go)
- [`internal/factory/pipeline.go`](/Users/youmew/dev/empireai/internal/factory/pipeline.go)
- [`internal/factory/scan_runner.go`](/Users/youmew/dev/empireai/internal/factory/scan_runner.go)
- [`internal/factory/scanners.go`](/Users/youmew/dev/empireai/internal/factory/scanners.go)

### Genericize In Place
These should remain, but must be reduced to platform-only semantics with product logic removed.
- [`internal/runtime/pipeline/workflow_instance_projection.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_instance_projection.go)
- [`internal/runtime/pipeline/coordinator_projection.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/coordinator_projection.go)
- [`internal/runtime/pipeline/coordinator.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/coordinator.go)
- [`internal/runtime/pipeline/workflow_nodes.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_nodes.go)
- [`internal/runtime/pipeline/workflow_nodes_runtime.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_nodes_runtime.go)
- [`internal/runtime/pipeline/workflow_contract_validation.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_contract_validation.go)

### Leave As Explicit Non-Product Exception
These may remain outside Empire packages because they are compatibility shells, not product logic.
- [`internal/protocolheaders/headers.go`](/Users/youmew/dev/empireai/internal/protocolheaders/headers.go)


The boundary is already much stronger than before:

### Enforced exceptions
1. [`internal/protocolheaders/headers.go`](/Users/youmew/dev/empireai/internal/protocolheaders/headers.go)
- explicit wire-compat header/query surface

### Existing guards
1. [`internal/runtime/architecture_guards_test.go`](/Users/youmew/dev/empireai/internal/runtime/architecture_guards_test.go)
- `TestNonTestGoFilesDoNotGainNewEmpireLiterals`
- `TestRetiredCoordinatorLiteralIsQuarantinedToLegacyIDFile`
- `TestWireCompatEmpireTokensAreQuarantinedToProtocolheaders`

2. [`internal/runtime/pipeline/pipeline_architecture_test.go`](/Users/youmew/dev/empireai/internal/runtime/pipeline/pipeline_architecture_test.go)
- isolates compatibility buckets
- isolates the reduced scoring exception
- keeps split executors from routing back through coordinator wrappers

### Remaining Phase 10 gap
The repo now has strong local guards, but Phase 10 is not done until:
- the remaining exceptions are documented as permanent policy
- compatibility-bucket live-read rules are locked outside the existing pipeline tests where needed
- any remaining irreducible Empire logic in generic runtime/factory/commgraph files is either:
  - moved into an Empire package, or
  - proven to be pure wire compatibility
- the closeout audit is written as the durable handoff for future work

## Step-By-Step Plan

### 1. Audit the remaining compatibility surfaces one more time
- Re-run and record:
```bash
rg -l "pipeline-coordinator|legacyPipelineCoordinatorID" internal/runtime internal/factory internal/commgraph cmd --glob '!**/*_test.go'
rg -l "X-Empire-|empire_agent_|empire_vertical_|empire_allowed_tools|empire_ctx_token|empire_trace_id" cmd internal --glob '!**/*_test.go'
```
- Confirm the output remains limited to:
  - [`internal/protocolheaders/headers.go`](/Users/youmew/dev/empireai/internal/protocolheaders/headers.go)
- Exit bar:
  - no additional production-code exceptions appear

### 2. Audit generic packages for remaining irreducible Empire logic
- Inspect the remaining generic/shared production files for logic that is still product-shaped rather than merely compatibility-shaped.
- Primary targets:
  - `internal/runtime/pipeline/**`
  - `internal/runtime/**`
  - `internal/factory/**`
  - `internal/commgraph/**`
- For each remaining product-shaped behavior, classify it as one of:
  1. platform logic that only looks product-shaped and can stay
  2. true irreducible Empire logic that must move into an Empire package
  3. protocol/wire compatibility that is allowed to stay outside Empire packages
- Exit bar:
  - all true irreducible Empire logic is either moved or explicitly isolated into approved product packages

### 3. Check for any remaining live-read dependence on deprecated compatibility buckets outside the guarded pipeline surface
- Audit for reads of:
  - `"pipeline-coordinator"`
  - `"scoring-state"`
  - `"scoring-restore"`
- Focus on non-test production files in:
  - `internal/runtime/**`
  - `internal/factory/**`
- If any new live-read path exists outside the approved migration files, fix it or add a targeted guard.
- Exit bar:
  - deprecated compatibility buckets remain isolated to the known migration/restore files only

### 4. Add one more permanent guard if the audit exposes an unguarded class of regression
- Candidates:
  - repo-wide guard for deprecated compatibility bucket tokens outside approved pipeline files
  - guard against reintroducing `pipeline-coordinator` into `internal/factory`
  - guard against product-specific logic living outside approved Empire packages
  - guard against raw wire-compat token strings outside `internal/protocolheaders`
- Prefer not to add redundant tests if current guards already cover the class.
- Exit bar:
  - every meaningful regression class has a fast failing test

### 5. Document the allowed exceptions as policy, not just as code comments
- Write a Phase 10 closeout note that states:
  - what the two remaining non-test exceptions are
  - why they are still allowed
  - what future phase removes or revisits them
- The note should explicitly separate:
  - product-specific packages that remain allowed
  - compatibility-only package exceptions
  - dashboard/UI carveouts
- Exit bar:
  - the boundary is understandable without reverse-engineering tests

### 6. Re-run the full acceptance gate
- Run:
```bash
go test ./internal/runtime -run 'Test(RuntimeGenericLayersStayFreeOfEmpireLiterals|NonTestGoFilesDoNotGainNewEmpireLiterals|RetiredCoordinatorLiteralIsQuarantinedToLegacyIDFile|WireCompatEmpireTokensAreQuarantinedToProtocolheaders)' -count=1
go test ./internal/runtime/pipeline -run 'Test(PipelineArchitecture|WorkflowNodes|WorkflowConformance|RuntimeInterfaces)' -count=1
go test ./... -count=1
```
- Exit bar:
  - the current boundary is fully enforced and green

### 7. Write the Phase 10 closeout / handoff
- Summarize:
  - what is now permanently locked
  - what is intentionally still allowed
  - which non-test packages still contain irreducible Empire logic and why
  - what is deferred to Phase 11
- Exit bar:
  - future work starts from a stable, enforced boundary rather than an informal understanding

## Suggested Execution Order
1. re-audit the remaining exceptions
2. move any remaining irreducible Empire logic out of generic packages
3. confirm compatibility-bucket isolation outside pipeline
4. add any missing guard only if needed
5. write the closeout note
6. run the final acceptance gate

## Definition Of Done
Phase 10 is complete when:
- raw wire-compat Empire tokens are permanently quarantined to [`headers.go`](/Users/youmew/dev/empireai/internal/protocolheaders/headers.go)
- all irreducible Empire logic in non-test production code lives only in approved Empire packages
- deprecated compatibility bucket usage is isolated and enforced
- the architecture/genericity boundary is documented and continuously tested
- the repo is ready to move into Phase 11 declarative node execution without boundary ambiguity
