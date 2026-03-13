**Phase 3 Pipeline Boundary Move Plan**

This plan replaces the previous "surgical cleanup inside `internal/runtime/pipeline/`" approach.

The rule is now:
- `internal/runtime/pipeline/` keeps only platform orchestration and engine integration
- Empire orchestration moves wholesale to `internal/empire/pipeline/`
- vocabulary cleanup inside mixed pipeline files stops until after the move

**Goal**

Reduce generic pipeline ownership to the true platform subset by:
1. moving all Empire and mixed pipeline files to `internal/empire/pipeline/`
2. keeping only the generic platform files in `internal/runtime/pipeline/`
3. replacing the generic coordinator with a thin platform coordinator

**Classification**

1. `EMPIRE` — move wholesale to `internal/empire/pipeline/`
- `coordinator_discovery.go`
- `coordinator_scoring.go`
- `workflow_instance_projection.go`
- `payload_factory.go`
- `module.go`
- `lifecycle_compat.go`
- `scan_campaign_compat.go`
- `coordinator_validation.go`
- `coordinator_subsystems.go`
- `portfolio_node.go`

2. `MIXED` — move wholesale first, then split later only if needed
- `coordinator.go`
- `coordinator_scan.go`
- `workflow_transition_engine.go`
- `engine_adapter.go`
- `coordinator_projection.go`
- `sharding.go`
- `shard_dispatcher.go`
- `runtime_support.go`
- `workflow_timer_lifecycle.go`
- `workflow_nodes.go`
- `coordinator_state.go`
- `handler_engine_builtins.go`
- `workflow_expression_context_builder.go`
- `pipeline_mode_resolution.go`
- `directive_parser.go`
- `workflow_contract_validation.go`

3. `GENERIC` — keep in `internal/runtime/pipeline/`
- `workflow.go`
- `workflow_instance_store.go`
- `workflow_expression_evaluator.go`
- `workflow_nodes_runtime.go`
- `workflow_compat_helpers.go`
- `engine_bridge.go`
- `declarative_default_node.go`
- `declarative_workflow_node.go`
- `guard_action_registry.go`
- `handler_preview.go`
- `generic_test_module.go`
- `system_node_runner.go`
- `scheduler.go`
- `transitions.go`
- `recovery.go`
- `runtime_interfaces.go`
- `persistence.go`
- `state_machine.go`
- `scan_normalization.go`
- `runtime_ids.go`
- `pipeline_helpers.go`
- `background_workflow_node.go`
- `coordinator_projection_expected_agents.go`
- `coordinator_projection_snapshot.go`
- `coordinator_runtime_support.go`
- `coordinator_scan_compat.go`
- `coordinator_workflow_projection.go`
- `workflow_hook_runtime.go`
- `workflow_node_scan.go`

**Target Architecture**

1. `internal/runtime/pipeline/`
- thin platform coordinator
- engine execution bridge
- workflow instance persistence
- scheduler/background node support
- generic registries and declarative node support

2. `internal/empire/pipeline/`
- discovery orchestration
- scoring orchestration
- validation/portfolio compatibility
- payload shaping
- scan campaign compatibility
- timer/state helpers tied to Empire flow semantics
- any remaining product-specific contract validation or workflow-node overrides

**Thin Platform Coordinator**

The rewritten generic `coordinator.go` should do only:
1. receive event
2. resolve authoritative node from contracts
3. invoke `engine.Execute(...)`
4. persist `workflow_instances`
5. dispatch post-commit effects

Anything involving:
- scoring composites
- validation gates
- scan shard planning
- discovery candidate processing
- portfolio digest/budget behavior
belongs in `internal/empire/pipeline/`

**Execution Strategy**

1. Freeze
- stop further cleanup inside the `MIXED` files listed above
- no more vocabulary-only edits there before extraction

2. Move product files first
- create `internal/empire/pipeline/`
- move all `EMPIRE` files there with minimal edits
- keep imports compiling through temporary adapters or aliases where necessary

3. Move mixed files as product-owned wrappers
- move the 16 `MIXED` files into `internal/empire/pipeline/` next
- do not surgically clean them during the move
- only patch imports and package names needed to compile

4. Rebuild generic pipeline
- create a thin `internal/runtime/pipeline/coordinator.go`
- keep generic interfaces in `runtime_interfaces.go`
- keep engine bridge and declarative node path in generic runtime

5. Bridge through `WorkflowModule`
- Empire `WorkflowModule` becomes the product seam
- optional product providers hang off the module interface or nearby Empire-owned adapters
- generic runtime should not import Empire payloads, gates, or subsystem structs

6. Cleanup after the boundary move
- only after the move, do vocabulary cleanup inside the reduced generic pipeline
- then do selective cleanup inside `internal/empire/pipeline/` where useful

**Risk Controls**

1. Do not move the `GENERIC` files out of `internal/runtime/pipeline/`
2. Do not rewrite moved files during relocation beyond compile fixes
3. Keep the engine authoritative throughout
4. Keep `go test ./... -count=1` green after each move batch

**Move Order**

1. Save this plan
2. Move the 10 `EMPIRE` files
3. Compile and patch imports
4. Move the 16 `MIXED` files
5. Compile and patch imports
6. Write the thin generic coordinator
7. Reconnect runtime boot to the new boundary
8. Run full tests

**Definition of Done**

- `internal/runtime/pipeline/` contains only the platform subset
- `internal/empire/pipeline/` owns Empire orchestration
- generic runtime no longer owns Empire subsystem structs, payload factories, or scoring/discovery orchestration
- Phase 3 vocabulary cleanup can continue on a much smaller platform surface
