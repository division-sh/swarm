# EmpireAI v2.2.1 — Contract Coherence (Implementer Validation)

**Version:** 2.2.1
**Previous:** 2.2.0
**Date:** 2026-03-08

## Summary

Aligns contracts with validated runtime behavior after implementer confirmed the 5-node decomposition is working and green. No new features — purely contract coherence.

## Changes

### Event handlers: 30 → 47

**validation-orchestrator (9 → 15):** Added spec.validation_passed, vertical.ready_for_review, vertical.needs_more_data, brand.revision_needed, spec.revision_requested, spec.revision_needed.

**lifecycle-orchestrator (8 → 19):** Added build_complete, launch_ready, qa.validation_passed, review.deploy_feedback, opco.steady_state_reached, opco.growth_triggered, opco.growth_stabilized, opco.teardown_requested, budget.threshold_crossed, mailbox.item_decided, system.directive.

### Ownership reconciliation

8 operating-phase transitions moved from opco-cto/opco-ceo/runtime to lifecycle-orchestrator:
ready_to_approved, full_speccing_to_building, building_to_pre_launch, pre_launch_to_launched, launched_to_operating, operating_to_expanding, expanding_to_operating, operating_to_winding_down.

lifecycle-orchestrator now owns 9 transitions (was 1).

Event catalog owning_node updated to match for build_complete, launch_ready, opco.steady_state_reached, opco.growth_triggered, opco.growth_stabilized, opco.teardown_requested, vertical.approved.

spec.approved marked _dual_use: factory (validation-orchestrator g2) + operating (lifecycle-orchestrator).

### Scoring policy

score.dimension_complete, scoring.contest_resolved: consuming → dual_delivery. Matches runtime behavior.

## Touches

- system-nodes.yaml: 17 new event_handlers, lifecycle owned_transitions → 9
- event-catalog.yaml: 8 owning_node updates, 2 runtime_handling fixes, 1 _dual_use annotation
- workflow-schema.yaml: 8 transition node reassignments
- upgrade-actions.yaml: previous_version → 2.2.0
- tooling.lock: → 2.2.1
- verification-gates.yaml: version-field-consistency → 2.2.1
