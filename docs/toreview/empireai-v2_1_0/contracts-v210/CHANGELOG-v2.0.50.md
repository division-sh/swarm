# EmpireAI v2.0.50 — Workflow Schema + Platform Abstraction (Phase 1)

**Version:** 2.0.50
**Previous:** 2.0.49
**Date:** 2026-03-06

## Summary

Phase 1 of platform generalization. Introduces declarative workflow definition, guard/action registry, and system-node workflow ownership. Separates platform primitives from EmpireAI-specific business logic. The vertical pipeline is now a formally defined state machine with inspectable transitions.

This is the foundation for extracting the orchestration layer as a standalone multi-agent workflow SDK.

## Architecture

Three layers, cleanly separated:

| Layer | Files | What it defines |
|-------|-------|----------------|
| Platform | workflow engine (Go) | Stage transitions, guard evaluation, action execution, timer management |
| Workflow | workflow-schema.yaml, guard-action-registry.yaml | Stages, transitions, guards, actions for the vertical pipeline |
| Policy | prompt-variables.yaml, system-nodes.yaml scoring config | Thresholds, enum values, business rules |

## New Contract Files

### workflow-schema.yaml
Declarative definition of the vertical pipeline state machine:
- **19 stages** across 4 phases (factory, decision, operating, terminal)
- **26 transitions** with trigger events, owning nodes, guards, and actions
- **5 timers** (marginal review, marginal kill, portfolio digest, scan timeout, campaign deadline)
- **Terminal stages** explicitly declared
- **Wildcard transitions** (any_factory_to_killed) for cross-cutting behavior

### guard-action-registry.yaml
Named code-backed behaviors referenced by transitions:
- **22 guards** (4 platform, 18 empire-specific)
- **17 actions** (3 platform, 14 empire-specific)
- Each entry: ID, category, description, inputs, check/effect
- Empire guards reference prompt-variables.yaml via `policy_ref`
- Platform guards are reusable across any workflow

### system-nodes.yaml expansion
- `execution_type: workflow_node` — distinguishes workflow nodes from transport nodes
- `owned_transitions` — which workflow transitions each node is responsible for
- Cross-referenceable with workflow-schema.yaml

## Also in this version

- **event-catalog.yaml enhanced** — 18 events gained explicit `required` field lists, `score.dimension_complete.score` gained `minimum: 0, maximum: 100` bounds (from implementer)
- **4 OpCo emit_events fixed** — mandate_updated→opco-ceo, launch_ready→opco-cto, channel_update→opco-marketing, market_feedback→opco-support
- **Audit fixes** — F1-F12 from Claude auditor, all resolved

## Compliance Implications

New gates needed:
- Every transition in workflow-schema.yaml has a handler in code
- Every guard/action ID in workflow-schema.yaml resolves in guard-action-registry.yaml
- Every transition owner (node) exists in agent-tools.yaml or system-nodes.yaml
- Terminal stages have no outgoing transitions (except explicit overrides)
- No orphan transitions (unreachable from any stage)

## Post-Update Verification

```
cd contracts/prompts && sha256sum -c ../prompt-manifest.sha256
python3 -c "import yaml; yaml.safe_load(open('contracts/workflow-schema.yaml')); yaml.safe_load(open('contracts/guard-action-registry.yaml'))"
```
