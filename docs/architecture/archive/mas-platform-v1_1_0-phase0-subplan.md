# MAS Platform v1.1.0 Phase 0 Subplan

Date: 2026-03-10
Repo: `/Users/youmew/dev/empireai`
Parent plan: `docs/architecture/mas-platform-v1_1_0-implementation-plan.md`

## Goal

Phase 0 exists to remove lossy contract translation before executor changes begin.

The practical outcome of this phase is:

- the Go contract model can represent the MAS platform YAML surface needed by later phases
- the semantic bundle preserves that data without collapsing it back into legacy shapes
- later phases do not need Empire-specific fallback logic because required contract fields were dropped during load

## Current Readout

### The loader is ahead of the structs

The runtime already does meaningful package-aware loading:

- recursive package discovery
- `runtime_contracts` bridge selection
- flow schema loading
- semantic derivation of handler transitions, event owners, namespaces, write-pin ownership, and required agents

But the concrete Go structs in [workflow_contracts.go](/Users/youmew/dev/empireai/internal/runtime/contracts/workflow_contracts.go) are still too old for the current MAS surface.

### The loader may also be behind the authoritative package format

Phase 0 should not assume the current loader already matches the latest spec package layout just because it can walk package trees.

There have been subtle format and shape changes in the MAS spec package set, so the loader itself must be re-verified against the authoritative tree in:

- `docs/specs/mas-platform/platform/contracts/`
- `docs/specs/mas-platform/empire/contracts/`

This includes more than field presence. It includes:

- package manifest shape
- flow reference shape
- target-vs-runtime contract path assumptions
- nested package/path resolution
- flow schema metadata shape
- timer declaration shape
- node handler field shape
- project-level versus flow-level merge expectations

### Confirmed struct gaps

1. `ProjectFlowRef`

Current gap:
- no `mode`

MAS need:
- `mode: static | template`

Why it matters:
- later dynamic flow work needs template/static distinction from package metadata, not runtime inference

2. `FlowSchemaDocument`

Current gap:
- no `instance_variables`
- no `auto_emit_on_create`

MAS need:
- instance-creation metadata for template flows

Why it matters:
- Phase 3 should not need Empire-specific spawn assumptions to reconstruct operating flow bootstrap behavior

3. `WorkflowDataAccumulation`

Current gap:
- `writes []string` only

MAS need:
- mixed write forms already present in checked-in contracts:
  - plain field names
  - `source_field -> target_field`
  - rule-level writes with value-setting behavior

Why it matters:
- Phase 1 cannot faithfully execute current YAML if loader/executor only understand flat string writes

4. `SystemNodeEventHandler`

Current gap:
- missing or weakly modeled fields needed by current MAS contracts:
  - `clear_gates`
  - `compute`
  - `accumulate`
  - `query`
  - `fan_out`
  - `filter`
  - `reduce`
  - `count`
  - `clear`
  - `template`
  - `instance_id_from`
  - `config_from`
  - `payload_transform`

Why it matters:
- even before execution support exists, Phase 0 must stop silently dropping these fields

5. `WorkflowTimerContract`

Current gap:
- legacy stage/delay struct:
  - `stage`
  - `action`
  - `cancellation`
  - delay components

MAS need:
- timer lifecycle metadata:
  - `delay`
  - `start_on`
  - `cancel_on`
  - `recurring`

Why it matters:
- Phase 4 should build on typed lifecycle data rather than ad hoc YAML access

6. `PlatformSpecDocument`

Current gap:
- only a very small subset of `platform-spec.yaml` is typed

MAS need:
- enough typed access for later boot-verification and execution work, especially:
  - handler field vocabulary
  - action definitions
  - boot verification structure

Why it matters:
- Phase 5 should not parse the spec as unstructured maps where typed fields are already stable enough to model

### Confirmed downstream blast radius

These runtime paths will be affected as soon as Phase 0 changes land:

- [workflow_contracts.go](/Users/youmew/dev/empireai/internal/runtime/contracts/workflow_contracts.go)
- [workflow_contracts_test.go](/Users/youmew/dev/empireai/internal/runtime/contracts/workflow_contracts_test.go)
- [workflow_contract_validation.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_contract_validation.go)
- [workflow_transition_engine.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine.go)
- [runtime.go](/Users/youmew/dev/empireai/internal/runtime/runtime.go)
- [contract_compliance_test.go](/Users/youmew/dev/empireai/internal/runtime/contract_compliance_test.go)

The highest-risk compatibility point is `WorkflowDataAccumulation`, because current validation and execution paths assume `Writes []string`.

### Confirmed fixture debt

Some package-aware tests still copy fixtures from the old archived spec tree rather than the MAS platform tree:

- [workflow_contract_validation_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_contract_validation_test.go)
- [workflow_nodes_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_nodes_test.go)
- fallback logic in [workflow_contracts_test.go](/Users/youmew/dev/empireai/internal/runtime/contracts/workflow_contracts_test.go)

That is Phase 0 debt. We should not keep proving the future loader against the old archive once the MAS platform tree is the target baseline.

## Decision To Make Early

### Contract source path

There are two viable Phase 0 approaches:

1. Keep `contracts/` as the active runtime source and mirror the MAS contract surface into the bridge files.

Pros:
- smallest runtime-loader change
- preserves current startup assumptions

Cons:
- keeps us investing in the bridge surface longer
- risks duplicating MAS structure in two places

2. Teach the loader to target `docs/specs/mas-platform/...` for Phase 0 analysis/tests while preserving `runtime_contracts` as the executable bridge.

Pros:
- tighter alignment with the actual spec of record
- makes Phase 0 tests more trustworthy

Cons:
- larger fixture/test change up front
- may broaden the diff before executor work starts

Tentative recommendation:
- keep `runtime_contracts` as the executable bridge for now
- but move Phase 0 tests and fixtures to the MAS platform tree wherever possible

That keeps runtime behavior stable while forcing the contract model to match the actual spec of record.

### Loader alignment rule

Even if `runtime_contracts` remains the executable bridge during Phase 0, the loader work should treat the MAS package tree as authoritative for structure.

That means:

- do not treat current bridge-path assumptions as proof that package discovery is correct
- verify path resolution rules against the authoritative spec package tree first
- only preserve bridge behavior where it is an intentional compatibility layer rather than an accidental dependency

## Tentative Work Breakdown

### Slice 0.1: Add fields without changing behavior

Files:
- [workflow_contracts.go](/Users/youmew/dev/empireai/internal/runtime/contracts/workflow_contracts.go)
- [workflow_contracts_test.go](/Users/youmew/dev/empireai/internal/runtime/contracts/workflow_contracts_test.go)

Work:
- verify package discovery and path resolution against the authoritative MAS package tree
- extend structs to capture missing MAS fields
- keep runtime behavior unchanged
- prove parsing only

Done when:
- the loader can walk and resolve the authoritative package tree without relying on outdated archive assumptions
- MAS YAML fields survive load into the bundle

### Slice 0.2: Make data accumulation backward-compatible

Files:
- [workflow_contracts.go](/Users/youmew/dev/empireai/internal/runtime/contracts/workflow_contracts.go)
- [workflow_contract_validation.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_contract_validation.go)
- [workflow_transition_engine.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_transition_engine.go)

Work:
- introduce a typed write-entry model that can represent:
  - plain write
  - mapped write
  - future constant/value write
- preserve compatibility for existing call sites that only need flat field names

Done when:
- validation/execution can still work against plain writes
- mapped writes are preserved in semantics for Phase 1

### Slice 0.3: Type timer lifecycle metadata

Files:
- [workflow_contracts.go](/Users/youmew/dev/empireai/internal/runtime/contracts/workflow_contracts.go)
- [runtime.go](/Users/youmew/dev/empireai/internal/runtime/runtime.go)
- tests around recurring timer provisioning

Work:
- add `delay`, `start_on`, `cancel_on`
- keep existing recurring-timer startup behavior compiling
- do not implement new lifecycle behavior yet

Done when:
- old recurring timer tests still pass
- MAS lifecycle timer fields are loaded and preserved

### Slice 0.4: Refresh package-aware fixtures

Files:
- [workflow_contracts_test.go](/Users/youmew/dev/empireai/internal/runtime/contracts/workflow_contracts_test.go)
- [workflow_contract_validation_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_contract_validation_test.go)
- [workflow_nodes_test.go](/Users/youmew/dev/empireai/internal/runtime/pipeline/workflow_nodes_test.go)

Work:
- shift package-aware fixtures from archived v2.6.0 sources to `docs/specs/mas-platform`
- keep old fallback fixtures only where explicitly needed for regression coverage
- add at least one fixture/assertion path that exercises the authoritative package format directly rather than a simplified copied tree

Done when:
- Phase 0 tests prove current MAS surface, not stale archive-only behavior

### Slice 0.5: Add targeted loader assertions for future phases

Tests to add:
- template-mode flow metadata is loaded
- `auto_emit_on_create` is loaded
- `clear_gates` is loaded
- `query`/`fan_out`/`compute`/`accumulate` fields are preserved
- wildcard semantic lookup still works after struct expansion

Done when:
- later phases can rely on tests rather than re-discovering struct gaps during implementation

## Risks

1. `WorkflowDataAccumulation` refactor can spread farther than expected.

Why:
- validation, semantic parity, and runtime execution all assume `[]string`

Mitigation:
- add a normalized helper API first
- keep flat `WriteFields()`-style accessors for old call sites during migration

2. Phase 0 can accidentally become Phase 1.

Why:
- once fields exist, it is tempting to implement behavior immediately

Mitigation:
- keep Phase 0 behavior-preserving except for loader/semantic preservation
- defer execution changes to the Phase 1 tranche

3. Tests may be coupled to old archived fixture semantics.

Why:
- several package-aware tests still use old spec archives

Mitigation:
- move those fixtures deliberately in Slice 0.4, not incidentally during executor work

4. The loader may appear to work while still encoding stale package-format assumptions.

Why:
- current package walking support was built against earlier contract shapes

Mitigation:
- treat authoritative package-format verification as first-class Phase 0 work
- add tests that prove real-tree path resolution and merge behavior against `docs/specs/mas-platform`

## Tentative Acceptance Checks

Minimal targeted checks after each slice:

```bash
go test ./internal/runtime/contracts -run 'TestResolveWorkflowContractPaths_DiscoversPackageLayout|TestLoadWorkflowContractBundle_LoadsCurrentRootFields' -count=1
go test ./internal/runtime/pipeline -run 'TestValidateWorkflowContracts_CurrentBundle|TestValidateWorkflowContracts_CurrentRootBundleFixture' -count=1
go test ./internal/runtime -run 'TestContractCompliance' -count=1
```

Expanded Phase 0 gate before moving to Phase 1:

```bash
go test ./internal/runtime/contracts -count=1
go test ./internal/runtime/pipeline -run 'TestValidateWorkflowContracts_|TestWorkflowNodes_' -count=1
go test ./internal/runtime -run 'TestContractCompliance' -count=1
```

## Recommendation

Start with Slice 0.1 and Slice 0.2.

That gives the best leverage:

- most of the later phases depend on richer handler/data shapes
- it exposes the true compile/test blast radius early
- it keeps the work behavior-preserving while setting up Phase 1 cleanly
