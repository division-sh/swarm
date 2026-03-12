## Phase 2 Step 1.3 Layer 3 Protocol

Architectural center: the recursive typed flow tree.

This is the first implementation slice for the Phase 2 restart. No downstream
cleanup counts as real progress until the loader produces a populated flow tree.

### End state

- `WorkflowContractBundle.FlowTree.Root` is populated.
- `WorkflowContractBundle.FlowTree.ByPath` contains hierarchical flow paths.
- `WorkflowContractBundle.FlowTree.ByID` resolves flow IDs to tree-backed views.
- `FlowContractView.Parent` and `FlowContractView.Children` are populated in the
  tree-backed views.
- Policy resolution can walk the tree instead of reading only `MergedPolicy`.

### Execution order

1. Make package discovery include nested flow packages at `flows/*/package.yaml`.
2. Build the recursive tree immediately after flat flow loading.
3. Backfill `FlowContracts` from the tree-backed views so flat consumers do not
   diverge from the authoritative structure.
4. Add tests for:
   - nested flow package discovery
   - `Root`, `ByPath`, `ByID`
   - parent/child links
   - hierarchical path construction

### Guardrails

- Do not start by deleting `productpolicy`.
- Do not start by renaming vocabulary.
- Do not add another flat compatibility map instead of populating `FlowTree`.
- Do not treat passing builds as sufficient if `FlowTree.ByPath` stays empty.

### First validation gate

- `go test ./internal/runtime/contracts ./internal/runtime/pipeline -count=1`
