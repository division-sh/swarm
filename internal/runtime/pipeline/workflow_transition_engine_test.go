package pipeline

import (
	"context"
	"database/sql"
	"testing"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestFactoryPipelineCoordinator_ResolveDerivedWorkflowTransitionByEvent_ShadowMatchesShortlisted(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("vertical.shortlisted"),
			VerticalID: uuid.NewString(),
			Payload:    mustJSON(map[string]any{"vertical_id": uuid.NewString()}),
		},
		State: WorkflowState{Stage: "shortlisted"},
	}

	flat, guardsEvaluated, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected flat shortlisted transition")
	}
	if len(guardsEvaluated) == 0 {
		t.Fatal("expected shortlisted transition to evaluate guards")
	}
	derived, ok := pc.resolveDerivedWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected derived shortlisted transition candidate")
	}
	comparison := pc.shadowCompareDerivedWorkflowTransition(triggerCtx, flat)
	if !comparison.Matched {
		t.Fatalf("expected shadow shortlisted transition match, flat=%+v derived=%+v", flat, derived)
	}
	if comparison.Reason != "emit_match" {
		t.Fatalf("expected shortlisted parity via shared emit, got %+v", comparison)
	}
	if derived.To != "researching" {
		t.Fatalf("expected derived shortlisted target researching, got %+v", derived)
	}
}

func TestFactoryPipelineCoordinator_ResolveWorkflowTransitionByEvent_PromotesDerivedShortlistedToFlatTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("vertical.shortlisted"),
			VerticalID: uuid.NewString(),
			Payload:    mustJSON(map[string]any{"vertical_id": uuid.NewString()}),
		},
		State: WorkflowState{Stage: "shortlisted"},
	}

	transition, guards, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected promoted shortlisted transition")
	}
	if transition.Name != "shortlisted_to_researching" {
		t.Fatalf("expected flat shortlisted transition name, got %+v", transition)
	}
	if len(guards) == 0 || guards[0] != "has_vertical_id" {
		t.Fatalf("expected flat shortlisted guard evaluation, got %+v", guards)
	}
	if len(transition.Actions) == 0 || transition.Actions[0].Name != "emit_validation_started" {
		t.Fatalf("expected flat shortlisted action payload, got %+v", transition.Actions)
	}
}

func TestFactoryPipelineCoordinator_ResolveWorkflowTransitionByEvent_PromotesResearchCompletedToFlatTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("research.completed"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "researching",
			Metadata: map[string]any{
				"g1_research": true,
			},
		},
	}

	transition, guards, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected promoted research.completed transition")
	}
	if transition.Name != "researching_to_mvp_speccing" {
		t.Fatalf("expected flat research.completed transition name, got %+v", transition)
	}
	if len(guards) == 0 || guards[0] != "gate_g1_research" {
		t.Fatalf("expected flat research.completed guard evaluation, got %+v", guards)
	}
}

func TestFactoryPipelineCoordinator_ResolveWorkflowTransitionByEvent_PromotesCTOApprovedToFlatTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("cto.spec_approved"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "cto_spec_review",
			Metadata: map[string]any{
				"g3_cto": true,
			},
		},
	}

	transition, guards, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected promoted cto.spec_approved transition")
	}
	if transition.Name != "cto_review_to_branding" {
		t.Fatalf("expected flat cto.spec_approved transition name, got %+v", transition)
	}
	if len(guards) == 0 || guards[0] != "gate_g3_cto" {
		t.Fatalf("expected flat cto.spec_approved guard evaluation, got %+v", guards)
	}
}

func TestFactoryPipelineCoordinator_ResolveWorkflowTransitionByEvent_PromotesSpecValidationFailedAliasToFlatTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("spec.validation_failed"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "cto_spec_review",
			Metadata: map[string]any{
				"revision_count": 0,
			},
		},
	}

	transition, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected promoted validation_failed transition")
	}
	if transition.Name != "validation_failed_to_speccing" {
		t.Fatalf("expected promoted flat validation_failed transition, got %+v", transition)
	}
}

func TestFactoryPipelineCoordinator_ResolveWorkflowTransitionByEvent_FallsBackForNonPromotedVerticalApproved(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("vertical.approved"),
			VerticalID:  uuid.NewString(),
			SourceAgent: "human",
			Payload:     mustJSON(map[string]any{"mailbox_decision_id": uuid.NewString()}),
		},
		State: WorkflowState{Stage: "ready_for_review"},
	}

	transition, guards, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected flat vertical.approved transition")
	}
	if transition.Name != "ready_to_approved" {
		t.Fatalf("expected fallback flat vertical.approved transition, got %+v", transition)
	}
	if len(guards) == 0 || guards[0] != "has_human_decision" {
		t.Fatalf("expected flat vertical.approved guard evaluation, got %+v", guards)
	}
}

func TestFactoryPipelineCoordinator_ResolveDerivedWorkflowTransitionByEvent_ShadowClassifiesValidationFailedAsAliasMatch(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("spec.validation_failed"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{Stage: "cto_spec_review"},
	}

	flat, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected promoted validation_failed transition")
	}
	comparison := pc.shadowCompareDerivedWorkflowTransition(triggerCtx, flat)
	if !comparison.Matched {
		t.Fatalf("expected validation_failed shadow comparison to match alias semantics, got %+v", comparison)
	}
	if comparison.Reason != "match" {
		t.Fatalf("expected validation_failed semantic match, got %+v", comparison)
	}
}

func TestFactoryPipelineCoordinator_ResolveDerivedWorkflowTransitionByEvent_ShadowMatchesResearchCompleted(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("research.completed"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "researching",
			Metadata: map[string]any{
				"g1_research": true,
			},
		},
	}

	flat, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected flat research.completed transition")
	}
	comparison := pc.shadowCompareDerivedWorkflowTransition(triggerCtx, flat)
	if !comparison.Matched {
		t.Fatalf("expected research.completed shadow comparison to be parity-safe, got %+v", comparison)
	}
	if comparison.Reason != "match" {
		t.Fatalf("expected research.completed semantic match, got %+v", comparison)
	}
}

func TestFactoryPipelineCoordinator_ResolveDerivedWorkflowTransitionByEvent_ShadowMatchesCTOApproved(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("cto.spec_approved"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "cto_spec_review",
			Metadata: map[string]any{
				"g3_cto": true,
			},
		},
	}

	flat, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected flat cto.spec_approved transition")
	}
	comparison := pc.shadowCompareDerivedWorkflowTransition(triggerCtx, flat)
	if !comparison.Matched {
		t.Fatalf("expected cto.spec_approved shadow comparison to be parity-safe, got %+v", comparison)
	}
	if comparison.Reason != "match" {
		t.Fatalf("expected cto.spec_approved semantic match, got %+v", comparison)
	}
}

func TestFactoryPipelineCoordinator_ResolveDerivedWorkflowTransitionByEvent_FailsOnAmbiguousOwners(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	trigger := "vertical.shortlisted"
	bundle := pc.ContractBundle()
	bundle.Semantics.EventOwners[trigger] = append([]string{}, bundle.Semantics.EventOwners[trigger]...)
	bundle.Semantics.EventOwners[trigger] = append(bundle.Semantics.EventOwners[trigger], "shadow-node")
	if bundle.Semantics.HandlerTransitionIndex == nil {
		bundle.Semantics.HandlerTransitionIndex = map[string]map[string]runtimecontracts.HandlerTransitionSemantic{}
	}
	bundle.Semantics.HandlerTransitionIndex["shadow-node"] = map[string]runtimecontracts.HandlerTransitionSemantic{
		trigger: {
			ID:        "shadow-node:" + trigger,
			NodeID:    "shadow-node",
			EventType: trigger,
			AdvancesTo:"researching",
		},
	}

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType(trigger),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{Stage: "shortlisted"},
	}

	if _, ok := pc.resolveDerivedWorkflowTransitionByEvent(triggerCtx); ok {
		t.Fatal("expected ambiguous derived transition ownership to fail closed")
	}
}

func TestFactoryPipelineCoordinator_ResolveDerivedWorkflowTransitionByEvent_ShadowClassifiesMismatch(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("vertical.approved"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{Stage: "ready_for_review"},
	}

	flat := WorkflowTransition{
		Name:    "ready_to_approved",
		From:    []PipelineStage{"ready_for_review"},
		To:      "approved",
		Trigger: "vertical.approved",
		Node:    "lifecycle-orchestrator",
		GuardIDs: []string{"has_human_decision"},
		Actions: []WorkflowAction{{Name: "emit_opco_spinup_requested"}},
	}
	comparison := pc.shadowCompareDerivedWorkflowTransition(triggerCtx, flat)
	if comparison.Matched {
		t.Fatalf("expected vertical.approved shadow comparison to surface mismatch, got %+v", comparison)
	}
	if comparison.Reason == "" || comparison.Reason == "match" {
		t.Fatalf("expected concrete mismatch reason, got %+v", comparison)
	}
	if comparison.Reason != "action_mismatch" {
		t.Fatalf("expected vertical.approved action mismatch, got %+v", comparison)
	}
}

func TestFactoryPipelineCoordinator_ResolveDerivedWorkflowTransitionByEvent_ShadowMatchesSteadyStateReached(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("opco.steady_state_reached"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{Stage: "launched"},
	}

	flat, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected flat opco.steady_state_reached transition")
	}
	comparison := pc.shadowCompareDerivedWorkflowTransition(triggerCtx, flat)
	if !comparison.Matched {
		t.Fatalf("expected opco.steady_state_reached parity, got %+v", comparison)
	}
	if comparison.Reason != "match" {
		t.Fatalf("expected opco.steady_state_reached match, got %+v", comparison)
	}
}

func TestFactoryPipelineCoordinator_ResolveDerivedWorkflowTransitionByEvent_ShadowMatchesGrowthTriggered(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("opco.growth_triggered"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{Stage: "operating"},
	}

	flat, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected flat opco.growth_triggered transition")
	}
	comparison := pc.shadowCompareDerivedWorkflowTransition(triggerCtx, flat)
	if !comparison.Matched {
		t.Fatalf("expected opco.growth_triggered parity, got %+v", comparison)
	}
	if comparison.Reason != "match" {
		t.Fatalf("expected opco.growth_triggered match, got %+v", comparison)
	}
}

func TestFactoryPipelineCoordinator_ResolveDerivedWorkflowTransitionByEvent_ShadowMatchesGrowthStabilized(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("opco.growth_stabilized"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{Stage: "expanding"},
	}

	flat, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected flat opco.growth_stabilized transition")
	}
	comparison := pc.shadowCompareDerivedWorkflowTransition(triggerCtx, flat)
	if !comparison.Matched {
		t.Fatalf("expected opco.growth_stabilized parity, got %+v", comparison)
	}
	if comparison.Reason != "match" {
		t.Fatalf("expected opco.growth_stabilized match, got %+v", comparison)
	}
}

func TestFactoryPipelineCoordinator_ResolveDerivedWorkflowTransitionByEvent_ShadowMatchesBuildComplete(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("build_complete"),
			VerticalID: uuid.NewString(),
			Payload:    mustJSON(map[string]any{"status": "passed"}),
		},
		State: WorkflowState{Stage: "building"},
	}

	flat, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected flat build_complete transition")
	}
	comparison := pc.shadowCompareDerivedWorkflowTransition(triggerCtx, flat)
	if !comparison.Matched {
		t.Fatalf("expected build_complete parity, got %+v", comparison)
	}
	if comparison.Reason != "match" {
		t.Fatalf("expected build_complete match, got %+v", comparison)
	}
}

func TestFactoryPipelineCoordinator_ResolveDerivedWorkflowTransitionByEvent_ShadowMatchesLaunchReady(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("launch_ready"),
			VerticalID: uuid.NewString(),
			Payload:    mustJSON(map[string]any{"decision": "approved"}),
		},
		State: WorkflowState{Stage: "pre_launch"},
	}

	flat, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected flat launch_ready transition")
	}
	comparison := pc.shadowCompareDerivedWorkflowTransition(triggerCtx, flat)
	if !comparison.Matched {
		t.Fatalf("expected launch_ready parity, got %+v", comparison)
	}
	if comparison.Reason != "match" {
		t.Fatalf("expected launch_ready match, got %+v", comparison)
	}
}

func TestFactoryPipelineCoordinator_ResolveDerivedWorkflowTransitionByEvent_ShadowMatchesTeardownRequested(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("opco.teardown_requested"),
			VerticalID:  uuid.NewString(),
			SourceAgent: "human",
			Payload:     mustJSON(map[string]any{"mailbox_decision_id": uuid.NewString()}),
		},
		State: WorkflowState{Stage: "operating"},
	}

	flat, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected flat opco.teardown_requested transition")
	}
	comparison := pc.shadowCompareDerivedWorkflowTransition(triggerCtx, flat)
	if !comparison.Matched {
		t.Fatalf("expected opco.teardown_requested parity, got %+v", comparison)
	}
	if comparison.Reason != "match" {
		t.Fatalf("expected opco.teardown_requested match, got %+v", comparison)
	}
}

func TestFactoryPipelineCoordinator_ResolveDerivedWorkflowTransitionByEvent_ShadowMatchesReadyForReview(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("vertical.ready_for_review"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "branding",
			Metadata: map[string]any{
				"g4_brand":    true,
				"g1_research": true,
				"g2_spec":     true,
				"g3_cto":      true,
			},
		},
	}

	flat, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected flat vertical.ready_for_review transition")
	}
	comparison := pc.shadowCompareDerivedWorkflowTransition(triggerCtx, flat)
	if !comparison.Matched {
		t.Fatalf("expected vertical.ready_for_review parity, got %+v", comparison)
	}
}

func TestFactoryPipelineCoordinator_ResolveDerivedWorkflowTransitionByEvent_ShadowMatchesResearchRejected(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("research.vertical_rejected"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{Stage: "researching"},
	}

	flat, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected flat research.vertical_rejected transition")
	}
	comparison := pc.shadowCompareDerivedWorkflowTransition(triggerCtx, flat)
	if !comparison.Matched {
		t.Fatalf("expected research.vertical_rejected parity, got %+v", comparison)
	}
}

func TestFactoryPipelineCoordinator_ResolveWorkflowTransitionByEvent_PromotesSteadyStateReachedToFlatTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("opco.steady_state_reached"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{Stage: "launched"},
	}

	transition, guards, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected promoted opco.steady_state_reached transition")
	}
	if transition.Name != "launched_to_operating" {
		t.Fatalf("expected flat launched_to_operating transition, got %+v", transition)
	}
	if len(guards) != 0 {
		t.Fatalf("expected no guard evaluation, got %+v", guards)
	}
}

func TestFactoryPipelineCoordinator_ResolveWorkflowTransitionByEvent_PromotesGrowthTriggeredToFlatTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("opco.growth_triggered"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{Stage: "operating"},
	}

	transition, guards, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected promoted opco.growth_triggered transition")
	}
	if transition.Name != "operating_to_expanding" {
		t.Fatalf("expected flat operating_to_expanding transition, got %+v", transition)
	}
	if len(guards) != 0 {
		t.Fatalf("expected no guard evaluation, got %+v", guards)
	}
}

func TestFactoryPipelineCoordinator_ResolveWorkflowTransitionByEvent_PromotesGrowthStabilizedToFlatTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("opco.growth_stabilized"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{Stage: "expanding"},
	}

	transition, guards, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected promoted opco.growth_stabilized transition")
	}
	if transition.Name != "expanding_to_operating" {
		t.Fatalf("expected flat expanding_to_operating transition, got %+v", transition)
	}
	if len(guards) != 0 {
		t.Fatalf("expected no guard evaluation, got %+v", guards)
	}
}

func TestFactoryPipelineCoordinator_ResolveWorkflowTransitionByEvent_PromotesBuildCompleteToFlatTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("build_complete"),
			VerticalID: uuid.NewString(),
			Payload:    mustJSON(map[string]any{"status": "passed"}),
		},
		State: WorkflowState{Stage: "building"},
	}

	transition, guards, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected promoted build_complete transition")
	}
	if transition.Name != "building_to_pre_launch" {
		t.Fatalf("expected flat building_to_pre_launch transition, got %+v", transition)
	}
	if len(guards) == 0 || guards[0] != "qa_passed" {
		t.Fatalf("expected qa_passed guard evaluation, got %+v", guards)
	}
}

func TestFactoryPipelineCoordinator_ResolveWorkflowTransitionByEvent_PromotesLaunchReadyToFlatTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("launch_ready"),
			VerticalID: uuid.NewString(),
			Payload:    mustJSON(map[string]any{"decision": "approved"}),
		},
		State: WorkflowState{Stage: "pre_launch"},
	}

	transition, guards, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected promoted launch_ready transition")
	}
	if transition.Name != "pre_launch_to_launched" {
		t.Fatalf("expected flat pre_launch_to_launched transition, got %+v", transition)
	}
	if len(guards) == 0 || guards[0] != "deploy_approved" {
		t.Fatalf("expected deploy_approved guard evaluation, got %+v", guards)
	}
}

func TestFactoryPipelineCoordinator_ResolveWorkflowTransitionByEvent_PromotesTeardownRequestedToFlatTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("opco.teardown_requested"),
			VerticalID:  uuid.NewString(),
			SourceAgent: "human",
			Payload:     mustJSON(map[string]any{"mailbox_decision_id": uuid.NewString()}),
		},
		State: WorkflowState{Stage: "operating"},
	}

	transition, guards, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected promoted opco.teardown_requested transition")
	}
	if transition.Name != "operating_to_winding_down" {
		t.Fatalf("expected flat operating_to_winding_down transition, got %+v", transition)
	}
	if len(guards) == 0 || guards[0] != "has_human_decision" {
		t.Fatalf("expected has_human_decision guard evaluation, got %+v", guards)
	}
}

func TestFactoryPipelineCoordinator_ResolveWorkflowTransitionByEvent_PromotesReadyForReviewToFlatTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("vertical.ready_for_review"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "branding",
			Metadata: map[string]any{
				"g4_brand":    true,
				"g1_research": true,
				"g2_spec":     true,
				"g3_cto":      true,
			},
		},
	}

	transition, guards, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected promoted vertical.ready_for_review transition")
	}
	if transition.Name != "branding_to_ready" {
		t.Fatalf("expected flat branding_to_ready transition, got %+v", transition)
	}
	if len(guards) != 2 || guards[0] != "gate_g4_brand" || guards[1] != "all_gates_met" {
		t.Fatalf("expected branding guards, got %+v", guards)
	}
}

func TestFactoryPipelineCoordinator_ResolveWorkflowTransitionByEvent_PromotesCTORevisionNeededToFlatTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("cto.spec_revision_needed"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "cto_spec_review",
			Metadata: map[string]any{
				"revision_count": 0,
			},
		},
	}

	transition, guards, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected promoted cto.spec_revision_needed transition")
	}
	if transition.Name != "cto_revision_to_speccing" {
		t.Fatalf("expected promoted cto_revision_to_speccing transition, got %+v", transition)
	}
	if len(guards) != 1 || guards[0] != "inner_revision_count_below_limit" {
		t.Fatalf("expected revision guard evaluation, got %+v", guards)
	}
}

func TestFactoryPipelineCoordinator_ResolveDerivedWorkflowTransitionByEvent_ShadowClassifiesCTORevisionNeededAsAliasMatch(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("cto.spec_revision_needed"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "cto_spec_review",
			Metadata: map[string]any{
				"revision_count": 0,
			},
		},
	}

	flat, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected promoted cto.spec_revision_needed transition")
	}
	comparison := pc.shadowCompareDerivedWorkflowTransition(triggerCtx, flat)
	if !comparison.Matched {
		t.Fatalf("expected cto.spec_revision_needed alias match, got %+v", comparison)
	}
	if comparison.Reason != "match" {
		t.Fatalf("expected cto.spec_revision_needed semantic match, got %+v", comparison)
	}
}

func TestFactoryPipelineCoordinator_ResolveWorkflowTransitionByEvent_PromotesResearchRejectedToFlatTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("research.vertical_rejected"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{Stage: "researching"},
	}

	transition, guards, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected promoted research.vertical_rejected transition")
	}
	if transition.Name != "researching_to_killed" {
		t.Fatalf("expected flat researching_to_killed transition, got %+v", transition)
	}
	if len(guards) != 0 {
		t.Fatalf("expected no guard evaluation, got %+v", guards)
	}
}

func TestFactoryPipelineCoordinator_ResolveDerivedWorkflowTransitionByEvent_ShadowClassifiesNeedsMoreDataNodeMismatch(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("vertical.needs_more_data"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{Stage: "ready_for_review"},
	}

	flat, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected flat vertical.needs_more_data transition")
	}
	comparison := pc.shadowCompareDerivedWorkflowTransition(triggerCtx, flat)
	if comparison.Matched {
		t.Fatalf("expected vertical.needs_more_data mismatch, got %+v", comparison)
	}
	if comparison.Reason != "node_mismatch" {
		t.Fatalf("expected vertical.needs_more_data node mismatch, got %+v", comparison)
	}
}

func TestFactoryPipelineCoordinator_ResolveDerivedHandlerExecutionPlanByEvent_ResearchCompleted(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("research.completed"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "researching",
			Metadata: map[string]any{
				"g1_research": true,
			},
		},
	}

	plan, ok := pc.resolveDerivedHandlerExecutionPlanByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected derived handler execution plan for research.completed")
	}
	if plan.NodeID != "validation-orchestrator" {
		t.Fatalf("expected validation-orchestrator plan owner, got %+v", plan)
	}
	if plan.AdvancesTo != "mvp_speccing" {
		t.Fatalf("expected research.completed to advance to mvp_speccing, got %+v", plan)
	}
	if plan.SetsGate != "g1_research" {
		t.Fatalf("expected research.completed to set g1_research, got %+v", plan)
	}
	if len(plan.ExecutionOrder) == 0 || plan.ExecutionOrder[0] != "compute" {
		t.Fatalf("expected compute-led execution order, got %+v", plan.ExecutionOrder)
	}
	foundAdvance := false
	foundSetsGate := false
	for _, step := range plan.ExecutionOrder {
		if step == "advances_to" {
			foundAdvance = true
		}
		if step == "sets_gate" {
			foundSetsGate = true
		}
	}
	if !foundAdvance || !foundSetsGate {
		t.Fatalf("expected advances_to and sets_gate in execution order, got %+v", plan.ExecutionOrder)
	}
}

func TestFactoryPipelineCoordinator_ResolveDerivedHandlerExecutionPlanByEvent_CTOSpecApproved(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("cto.spec_approved"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "cto_spec_review",
			Metadata: map[string]any{
				"g2_spec": true,
			},
		},
	}

	plan, ok := pc.resolveDerivedHandlerExecutionPlanByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected derived handler execution plan for cto.spec_approved")
	}
	if plan.SetsGate != "g3_cto" {
		t.Fatalf("expected cto.spec_approved to set g3_cto, got %+v", plan)
	}
	if plan.AdvancesTo != "branding" {
		t.Fatalf("expected cto.spec_approved to advance to branding, got %+v", plan)
	}
	if plan.Emits != "brand.requested" {
		t.Fatalf("expected cto.spec_approved to emit brand.requested, got %+v", plan)
	}
	foundSetsGate := false
	for _, step := range plan.ExecutionOrder {
		if step == "sets_gate" {
			foundSetsGate = true
			break
		}
	}
	if !foundSetsGate {
		t.Fatalf("expected sets_gate in execution order, got %+v", plan.ExecutionOrder)
	}
}

func TestHandlerExecutionPlanParity_ResearchCompleted(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("research.completed"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "researching",
			Metadata: map[string]any{
				"g1_research": true,
			},
		},
	}

	transition, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected research.completed workflow transition")
	}
	plan, ok := pc.resolveDerivedHandlerExecutionPlanByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected derived handler execution plan for research.completed")
	}

	comparison := shadowCompareHandlerExecutionPlan(transition, plan)
	if !comparison.Matched {
		t.Fatalf("expected research.completed execution plan parity, got %+v", comparison)
	}
}

func TestHandlerExecutionPlanParity_SpecValidationFailedAlias(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("spec.validation_failed"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "cto_spec_review",
			Metadata: map[string]any{
				"revision_count": 0,
			},
		},
	}

	transition, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected spec.validation_failed workflow transition")
	}
	plan, ok := pc.resolveDerivedHandlerExecutionPlanByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected derived handler execution plan for spec.validation_failed")
	}

	comparison := shadowCompareHandlerExecutionPlan(transition, plan)
	if !comparison.Matched {
		t.Fatalf("expected spec.validation_failed execution plan alias parity, got %+v", comparison)
	}
}

func TestHandlerExecutionPlanSafety_ResearchCompleted_IsNotExecutionSafeYet(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("research.completed"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "researching",
			Metadata: map[string]any{
				"g1_research": true,
			},
		},
	}

	transition, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected research.completed workflow transition")
	}
	plan, ok := pc.resolveDerivedHandlerExecutionPlanByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected derived handler execution plan for research.completed")
	}

	comparison := classifyHandlerExecutionPlanSafety(transition, plan)
	if comparison.Safe {
		t.Fatalf("expected research.completed to remain execution-unsafe, got %+v", comparison)
	}
	if comparison.Reason == "safe" {
		t.Fatalf("expected non-safe reason for research.completed, got %+v", comparison)
	}
}

func TestHandlerExecutionPlanSafety_CTOSpecApproved_IsNotExecutionSafeYet(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("cto.spec_approved"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "cto_spec_review",
			Metadata: map[string]any{
				"g3_cto": true,
			},
		},
	}

	transition, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected cto.spec_approved workflow transition")
	}
	plan, ok := pc.resolveDerivedHandlerExecutionPlanByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected derived handler execution plan for cto.spec_approved")
	}

	comparison := classifyHandlerExecutionPlanSafety(transition, plan)
	if comparison.Safe {
		t.Fatalf("expected cto.spec_approved to remain execution-unsafe, got %+v", comparison)
	}
}

func TestHandlerExecutionPlanSafety_BuildComplete_IsExecutionSafe(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("build_complete"),
			VerticalID: uuid.NewString(),
			Payload:    mustJSON(map[string]any{"status": "passed"}),
		},
		State: WorkflowState{Stage: "building"},
	}

	transition, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected build_complete workflow transition")
	}
	plan, ok := pc.resolveDerivedHandlerExecutionPlanByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected derived handler execution plan for build_complete")
	}

	comparison := classifyHandlerExecutionPlanSafety(transition, plan)
	if !comparison.Safe {
		t.Fatalf("expected build_complete execution safety, got %+v", comparison)
	}
	if comparison.Reason != "safe" {
		t.Fatalf("expected build_complete safety reason safe, got %+v", comparison)
	}
}

func TestHandlerExecutionPlanSafety_LaunchReady_IsExecutionSafe(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("launch_ready"),
			VerticalID: uuid.NewString(),
			Payload:    mustJSON(map[string]any{"decision": "approved"}),
		},
		State: WorkflowState{Stage: "pre_launch"},
	}

	transition, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected launch_ready workflow transition")
	}
	plan, ok := pc.resolveDerivedHandlerExecutionPlanByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected derived handler execution plan for launch_ready")
	}

	comparison := classifyHandlerExecutionPlanSafety(transition, plan)
	if !comparison.Safe {
		t.Fatalf("expected launch_ready execution safety, got %+v", comparison)
	}
	if comparison.Reason != "safe" {
		t.Fatalf("expected launch_ready safety reason safe, got %+v", comparison)
	}
}

func TestHandlerExecutionPlanSafety_ReadyForReview_IsNotExecutionSafeYet(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("vertical.ready_for_review"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "branding",
			Metadata: map[string]any{
				"g4_brand":    true,
				"g1_research": true,
				"g2_spec":     true,
				"g3_cto":      true,
			},
		},
	}

	transition, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected vertical.ready_for_review workflow transition")
	}
	plan, ok := pc.resolveDerivedHandlerExecutionPlanByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected derived handler execution plan for vertical.ready_for_review")
	}

	comparison := classifyHandlerExecutionPlanSafety(transition, plan)
	if comparison.Safe {
		t.Fatalf("expected vertical.ready_for_review to remain execution-unsafe, got %+v", comparison)
	}
	if comparison.Reason != "guard_mismatch" {
		t.Fatalf("expected vertical.ready_for_review guard mismatch, got %+v", comparison)
	}
}

func TestHandlerExecutionPlanSafety_ResearchRejected_IsExecutionSafe(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("research.vertical_rejected"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{Stage: "researching"},
	}

	transition, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected research.vertical_rejected workflow transition")
	}
	plan, ok := pc.resolveDerivedHandlerExecutionPlanByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected derived handler execution plan for research.vertical_rejected")
	}

	comparison := classifyHandlerExecutionPlanSafety(transition, plan)
	if !comparison.Safe {
		t.Fatalf("expected research.vertical_rejected execution safety, got %+v", comparison)
	}
	if comparison.Reason != "safe" {
		t.Fatalf("expected research.vertical_rejected safety reason safe, got %+v", comparison)
	}
}

func TestHandlerExecutionPlanSafety_CTOVetoed_IsExecutionSafe(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("cto.spec_vetoed"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{Stage: "cto_spec_review"},
	}

	transition, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected cto.spec_vetoed workflow transition")
	}
	plan, ok := pc.resolveDerivedHandlerExecutionPlanByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected derived handler execution plan for cto.spec_vetoed")
	}

	comparison := classifyHandlerExecutionPlanSafety(transition, plan)
	if !comparison.Safe {
		t.Fatalf("expected cto.spec_vetoed execution safety, got %+v", comparison)
	}
	if comparison.Reason != "safe" {
		t.Fatalf("expected cto.spec_vetoed safety reason safe, got %+v", comparison)
	}
}

func TestHandlerExecutionPlanSafety_SpecValidationFailed_IsExecutionSafe(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("spec.validation_failed"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "cto_spec_review",
			Metadata: map[string]any{
				"revision_count": 1,
			},
		},
	}

	transition, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected spec.validation_failed workflow transition")
	}
	plan, ok := pc.resolveDerivedHandlerExecutionPlanByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected derived handler execution plan for spec.validation_failed")
	}

	comparison := classifyHandlerExecutionPlanSafety(transition, plan)
	if !comparison.Safe {
		t.Fatalf("expected spec.validation_failed execution safety, got %+v", comparison)
	}
	if comparison.Reason != "safe" {
		t.Fatalf("expected spec.validation_failed safety reason safe, got %+v", comparison)
	}
}

func TestHandlerExecutionPlanSafety_CTORevisionNeeded_IsExecutionSafe(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("cto.spec_revision_needed"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "cto_spec_review",
			Metadata: map[string]any{
				"revision_count": 1,
			},
		},
	}

	transition, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected cto.spec_revision_needed workflow transition")
	}
	plan, ok := pc.resolveDerivedHandlerExecutionPlanByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected derived handler execution plan for cto.spec_revision_needed")
	}

	comparison := classifyHandlerExecutionPlanSafety(transition, plan)
	if !comparison.Safe {
		t.Fatalf("expected cto.spec_revision_needed execution safety, got %+v", comparison)
	}
	if comparison.Reason != "safe" {
		t.Fatalf("expected cto.spec_revision_needed safety reason safe, got %+v", comparison)
	}
}

func TestHandlerExecutionPlanSafety_OperatingAdvanceSet_IsExecutionSafe(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	cases := []struct {
		name      string
		eventType events.EventType
		stage     PipelineStage
	}{
		{name: "steady_state", eventType: events.EventType("opco.steady_state_reached"), stage: PipelineStage("launched")},
		{name: "growth_triggered", eventType: events.EventType("opco.growth_triggered"), stage: StageOperating},
		{name: "growth_stabilized", eventType: events.EventType("opco.growth_stabilized"), stage: PipelineStage("expanding")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			triggerCtx := workflowTriggerContext{
				Event: events.Event{
					ID:         uuid.NewString(),
					Type:       tc.eventType,
					VerticalID: uuid.NewString(),
				},
				State: WorkflowState{Stage: tc.stage},
			}

			transition, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
			if !ok {
				t.Fatalf("expected %s workflow transition", tc.eventType)
			}
			plan, ok := pc.resolveDerivedHandlerExecutionPlanByEvent(triggerCtx)
			if !ok {
				t.Fatalf("expected derived handler execution plan for %s", tc.eventType)
			}

			comparison := classifyHandlerExecutionPlanSafety(transition, plan)
			if !comparison.Safe {
				t.Fatalf("expected %s execution safety, got %+v", tc.eventType, comparison)
			}
			if comparison.Reason != "safe" {
				t.Fatalf("expected %s safety reason safe, got %+v", tc.eventType, comparison)
			}
		})
	}
}

func TestHandlerExecutionPlanSafety_TeardownRequested_IsExecutionSafe(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("opco.teardown_requested"),
			VerticalID:  uuid.NewString(),
			SourceAgent: "human",
			Payload:     mustJSON(map[string]any{"mailbox_decision_id": uuid.NewString()}),
		},
		State: WorkflowState{Stage: StageOperating},
	}

	transition, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected opco.teardown_requested workflow transition")
	}
	plan, ok := pc.resolveDerivedHandlerExecutionPlanByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected derived handler execution plan for opco.teardown_requested")
	}

	comparison := classifyHandlerExecutionPlanSafety(transition, plan)
	if !comparison.Safe {
		t.Fatalf("expected opco.teardown_requested execution safety, got %+v", comparison)
	}
	if comparison.Reason != "safe" {
		t.Fatalf("expected opco.teardown_requested safety reason safe, got %+v", comparison)
	}
}

func TestFactoryPipelineCoordinator_ApplyWorkflowEventTransition_UsesHandlerExecutionPlanForOperatingAdvanceSubset(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Launch Stage Co', 'launch-stage-co', 'us', 'launched', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	outcome, ok := pc.applyWorkflowEventTransition(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("opco.steady_state_reached"),
		VerticalID: verticalID,
	})
	if !ok {
		t.Fatal("expected opco.steady_state_reached workflow transition")
	}
	if outcome.Transition.Name != "launched_to_operating" {
		t.Fatalf("expected launched_to_operating transition, got %+v", outcome.Transition)
	}
	assertVerticalStage(t, ctx, db, verticalID, "operating")
	if len(outcome.ActionsExecuted) != 0 {
		t.Fatalf("expected no flat transition actions executed for operating advance alias, got %+v", outcome.ActionsExecuted)
	}
}

func TestFactoryPipelineCoordinator_ApplyWorkflowEventTransition_UsesHandlerExecutionPlanForTeardownRequested(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Teardown Stage Co', 'teardown-stage-co', 'us', 'operating', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	outcome, ok := pc.applyWorkflowEventTransition(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("opco.teardown_requested"),
		VerticalID:  verticalID,
		SourceAgent: "human",
		Payload:     mustJSON(map[string]any{"mailbox_decision_id": uuid.NewString()}),
	})
	if !ok {
		t.Fatal("expected opco.teardown_requested workflow transition")
	}
	if outcome.Transition.Name != "operating_to_winding_down" {
		t.Fatalf("expected operating_to_winding_down transition, got %+v", outcome.Transition)
	}
	assertVerticalStage(t, ctx, db, verticalID, "winding_down")
	if len(outcome.ActionsExecuted) == 0 || outcome.ActionsExecuted[len(outcome.ActionsExecuted)-1] != "begin_teardown" {
		t.Fatalf("expected begin_teardown action to execute post-stage, got %+v", outcome.ActionsExecuted)
	}
}

func TestFactoryPipelineCoordinator_ApplyWorkflowEventTransition_UsesHandlerExecutionPlanForBuildComplete(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Build Stage Co', 'build-stage-co', 'us', 'building', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	outcome, ok := pc.applyWorkflowEventTransition(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("build_complete"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"status": "passed"}),
	})
	if !ok {
		t.Fatal("expected build_complete workflow transition")
	}
	if outcome.Transition.Name != "building_to_pre_launch" {
		t.Fatalf("expected building_to_pre_launch transition, got %+v", outcome.Transition)
	}
	assertVerticalStage(t, ctx, db, verticalID, "pre_launch")
	if len(outcome.ActionsExecuted) != 0 {
		t.Fatalf("expected no flat transition actions executed for build_complete handler-order alias, got %+v", outcome.ActionsExecuted)
	}
}

func TestFactoryPipelineCoordinator_ApplyWorkflowEventTransition_UsesHandlerExecutionPlanForLaunchReady(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Launch Ready Co', 'launch-ready-co', 'us', 'pre_launch', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	outcome, ok := pc.applyWorkflowEventTransition(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("launch_ready"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"decision": "approved"}),
	})
	if !ok {
		t.Fatal("expected launch_ready workflow transition")
	}
	if outcome.Transition.Name != "pre_launch_to_launched" {
		t.Fatalf("expected pre_launch_to_launched transition, got %+v", outcome.Transition)
	}
	assertVerticalStage(t, ctx, db, verticalID, "launched")
	if len(outcome.ActionsExecuted) != 0 {
		t.Fatalf("expected no flat transition actions executed for launch_ready handler-order alias, got %+v", outcome.ActionsExecuted)
	}
}

func TestFactoryPipelineCoordinator_ApplyWorkflowEventTransition_UsesHandlerExecutionPlanForSpecValidationFailed(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Revision Loop Co', 'revision-loop-co', 'us', 'cto_spec_review', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	pc.validationGate.states[verticalID] = &validationPipelineState{
		VerticalID:    verticalID,
		Status:        "active",
		G1Research:    true,
		G2Spec:        true,
		G3CTO:         true,
		RevisionCount: 0,
		SpecVersion:   1,
	}

	outcome, ok := pc.applyWorkflowEventTransition(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.validation_failed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"status": "blocker"}),
	})
	if !ok {
		t.Fatal("expected spec.validation_failed workflow transition")
	}
	if outcome.Transition.Name != "validation_failed_to_speccing" {
		t.Fatalf("unexpected transition: %+v", outcome.Transition)
	}
	assertVerticalStage(t, ctx, db, verticalID, "mvp_speccing")
	if got := pc.validationGate.states[verticalID].RevisionCount; got != 1 {
		t.Fatalf("expected revision_count=1, got %d", got)
	}
	if len(outcome.ActionsExecuted) == 0 || outcome.ActionsExecuted[len(outcome.ActionsExecuted)-1] != "increment_revision_count" {
		t.Fatalf("expected increment_revision_count post-stage action, got %+v", outcome.ActionsExecuted)
	}
}

func TestFactoryPipelineCoordinator_ApplyWorkflowEventTransition_UsesHandlerExecutionPlanForCTORevisionNeeded(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'CTO Revision Co', 'cto-revision-co', 'us', 'cto_spec_review', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	pc.validationGate.states[verticalID] = &validationPipelineState{
		VerticalID:    verticalID,
		Status:        "active",
		G1Research:    true,
		G2Spec:        true,
		G3CTO:         true,
		RevisionCount: 0,
		SpecVersion:   1,
	}

	outcome, ok := pc.applyWorkflowEventTransition(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("cto.spec_revision_needed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"reason": "needs_more_detail"}),
	})
	if !ok {
		t.Fatal("expected cto.spec_revision_needed workflow transition")
	}
	if outcome.Transition.Name != "cto_revision_to_speccing" {
		t.Fatalf("unexpected transition: %+v", outcome.Transition)
	}
	assertVerticalStage(t, ctx, db, verticalID, "mvp_speccing")
	if got := pc.validationGate.states[verticalID].RevisionCount; got != 1 {
		t.Fatalf("expected revision_count=1, got %d", got)
	}
	if len(outcome.ActionsExecuted) == 0 || outcome.ActionsExecuted[len(outcome.ActionsExecuted)-1] != "increment_revision_count" {
		t.Fatalf("expected increment_revision_count post-stage action, got %+v", outcome.ActionsExecuted)
	}
}

func TestFactoryPipelineCoordinator_ApplyWorkflowEventTransition_UsesHandlerExecutionPlanForResearchRejected(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Rejected Co', 'rejected-co', 'us', 'researching', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	outcome, ok := pc.applyWorkflowEventTransition(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("research.vertical_rejected"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"reason": "market too small"}),
	})
	if !ok {
		t.Fatal("expected research.vertical_rejected workflow transition")
	}
	if outcome.Transition.Name != "researching_to_killed" {
		t.Fatalf("unexpected transition: %+v", outcome.Transition)
	}
	assertVerticalStage(t, ctx, db, verticalID, "killed")
	if len(outcome.ActionsExecuted) != 0 {
		t.Fatalf("expected no flat transition actions executed for research.vertical_rejected handler-order alias, got %+v", outcome.ActionsExecuted)
	}
}

func TestFactoryPipelineCoordinator_ApplyWorkflowEventTransition_UsesHandlerExecutionPlanForCTOVetoed(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Vetoed Co', 'vetoed-co', 'us', 'cto_spec_review', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	outcome, ok := pc.applyWorkflowEventTransition(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("cto.spec_vetoed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"reason": "cto veto"}),
	})
	if !ok {
		t.Fatal("expected cto.spec_vetoed workflow transition")
	}
	if outcome.Transition.Name != "cto_vetoed_to_killed" {
		t.Fatalf("unexpected transition: %+v", outcome.Transition)
	}
	assertVerticalStage(t, ctx, db, verticalID, "killed")
	if len(outcome.ActionsExecuted) != 0 {
		t.Fatalf("expected no flat transition actions executed for cto.spec_vetoed handler-order alias, got %+v", outcome.ActionsExecuted)
	}
}

func TestFactoryPipelineCoordinator_ResolveWorkflowTransitionByEvent_FallsBackForNeedsMoreData(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("vertical.needs_more_data"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{Stage: "ready_for_review"},
	}

	transition, guards, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected flat vertical.needs_more_data transition")
	}
	if transition.Name != "ready_to_researching" {
		t.Fatalf("expected fallback ready_to_researching transition, got %+v", transition)
	}
	if len(guards) != 0 {
		t.Fatalf("expected no guard evaluation, got %+v", guards)
	}
}

func TestFactoryPipelineCoordinator_ApplyWorkflowEventTransition_ValidationStarted(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()
	emitted := []events.Event{}
	ctx = context.WithValue(ctx, pipelineEmitCollectorKey{}, &emitted)

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Claims Automation', 'claims-automation', 'us', 'shortlisted', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	outcome, ok := pc.applyWorkflowEventTransition(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.shortlisted"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"vertical_id": verticalID, "composite_score": 82.5}),
	})
	if !ok {
		t.Fatal("expected shortlisted workflow transition")
	}
	if outcome.Transition.Name != "shortlisted_to_researching" {
		t.Fatalf("unexpected transition: %+v", outcome.Transition)
	}
	assertVerticalStage(t, ctx, db, verticalID, "researching")
	if len(emitted) != 1 || string(emitted[0].Type) != "validation.started" {
		t.Fatalf("expected validation.started emit, got %+v", emitted)
	}
}

func TestFactoryPipelineCoordinator_ApplyWorkflowEventTransition_IncrementsRevisionCount(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'EU VAT Reconciliation', 'eu-vat-reconciliation', 'eu', 'cto_spec_review', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}
	pc.validationGate.states[verticalID] = &validationPipelineState{
		VerticalID:     verticalID,
		Status:         "active",
		G1Research:     true,
		G2Spec:         true,
		G3CTO:          true,
		RevisionCount:  0,
		SpecVersion:    1,
		ScoringPayload: mustJSON(map[string]any{"vertical_id": verticalID}),
	}

	outcome, ok := pc.applyWorkflowEventTransition(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.validation_failed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"status": "blocker"}),
	})
	if !ok {
		t.Fatal("expected spec.validation_failed workflow transition")
	}
	if outcome.Transition.Name != "validation_failed_to_speccing" {
		t.Fatalf("unexpected transition: %+v", outcome.Transition)
	}
	assertVerticalStage(t, ctx, db, verticalID, "mvp_speccing")
	if got := pc.validationGate.states[verticalID].RevisionCount; got != 1 {
		t.Fatalf("expected revision_count=1, got %d", got)
	}
	instance, ok, err := pc.workflowStore.Load(ctx, verticalID)
	if err != nil || !ok {
		t.Fatalf("expected workflow instance after revision increment, ok=%v err=%v", ok, err)
	}
	if got := asInt(instance.Metadata["revision_count"]); got != 1 {
		t.Fatalf("expected workflow metadata revision_count=1, got %+v", instance.Metadata)
	}
	bucket, ok := asObject(instance.AccumulatorState["validation-orchestrator"])
	if !ok {
		t.Fatalf("expected validation-orchestrator bucket, got %+v", instance.AccumulatorState)
	}
	if got := asInt(bucket["revision_count"]); got != 1 {
		t.Fatalf("expected validation-orchestrator revision_count=1, got %+v", bucket)
	}
}

func TestFactoryPipelineCoordinator_ResolveWorkflowTransitionByEvent_PromotesSpecValidationFailedToFlatTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("spec.validation_failed"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "cto_spec_review",
			Metadata: map[string]any{
				"revision_count": 0,
			},
		},
	}

	transition, guards, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected promoted spec.validation_failed transition")
	}
	if transition.Name != "validation_failed_to_speccing" {
		t.Fatalf("expected promoted validation_failed_to_speccing transition, got %+v", transition)
	}
	if len(guards) != 1 || guards[0] != "inner_revision_count_below_limit" {
		t.Fatalf("expected revision guard evaluation, got %+v", guards)
	}
}

func TestFactoryPipelineCoordinator_ResolveDerivedWorkflowTransitionByEvent_ShadowClassifiesSpecValidationFailedAsAliasMatch(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	triggerCtx := workflowTriggerContext{
		Event: events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("spec.validation_failed"),
			VerticalID: uuid.NewString(),
		},
		State: WorkflowState{
			Stage: "cto_spec_review",
			Metadata: map[string]any{
				"revision_count": 0,
			},
		},
	}

	flat, _, ok := pc.resolveWorkflowTransitionByEvent(triggerCtx)
	if !ok {
		t.Fatal("expected promoted spec.validation_failed transition")
	}
	comparison := pc.shadowCompareDerivedWorkflowTransition(triggerCtx, flat)
	if !comparison.Matched {
		t.Fatalf("expected spec.validation_failed alias match, got %+v", comparison)
	}
	if comparison.Reason != "match" {
		t.Fatalf("expected spec.validation_failed semantic match, got %+v", comparison)
	}
}

func TestFactoryPipelineCoordinator_ApplyWorkflowEventTransition_IncrementsRevisionCountFromWorkflowState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'EU VAT Reconciliation', 'eu-vat-reconciliation', 'eu', 'cto_spec_review', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}
	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:      verticalID,
		WorkflowName:    pc.ContractBundle().Workflow.Workflow.Name,
		WorkflowVersion: pc.ContractBundle().Workflow.Workflow.Version,
		CurrentStage:    "cto_spec_review",
		Metadata: map[string]any{
			"status":         "active",
			"revision_count": 2,
			"g1_research":    true,
			"g2_spec":        true,
			"g3_cto":         true,
		},
		AccumulatorState: map[string]any{
			"validation-orchestrator": map[string]any{
				"vertical_id": verticalID,
				"gate_state": map[string]any{
					"g1_research": true,
					"g2_spec":     true,
					"g3_cto":      true,
					"g4_brand":    false,
				},
				"revision_count": 2,
			},
		},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	outcome, ok := pc.applyWorkflowEventTransition(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.validation_failed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"status": "blocker"}),
	})
	if !ok {
		t.Fatal("expected spec.validation_failed workflow transition")
	}
	if outcome.Transition.Name != "validation_failed_to_speccing" {
		t.Fatalf("unexpected transition: %+v", outcome.Transition)
	}
	assertVerticalStage(t, ctx, db, verticalID, "mvp_speccing")
	st := pc.validationGate.states[verticalID]
	if st == nil {
		t.Fatal("expected workflow-backed validation state to hydrate into memory")
	}
	if got := st.RevisionCount; got != 3 {
		t.Fatalf("expected hydrated revision_count=3, got %+v", st)
	}
	instance, ok, err := pc.workflowStore.Load(ctx, verticalID)
	if err != nil || !ok {
		t.Fatalf("expected workflow instance after revision increment, ok=%v err=%v", ok, err)
	}
	if got := asInt(instance.Metadata["revision_count"]); got != 3 {
		t.Fatalf("expected workflow metadata revision_count=3, got %+v", instance.Metadata)
	}
	bucket, ok := asObject(instance.AccumulatorState["validation-orchestrator"])
	if !ok {
		t.Fatalf("expected validation-orchestrator bucket, got %+v", instance.AccumulatorState)
	}
	if got := asInt(bucket["revision_count"]); got != 3 {
		t.Fatalf("expected validation-orchestrator revision_count=3, got %+v", bucket)
	}
}

func TestFactoryPipelineCoordinator_ApplyWorkflowEventTransition_RequiresHumanApproval(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Revenue Recovery', 'revenue-recovery', 'us', 'ready_for_review', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	if _, ok := pc.applyWorkflowEventTransition(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.approved"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"vertical_id": verticalID}),
	}); ok {
		t.Fatal("expected non-human vertical.approved to fail guard evaluation")
	}
	assertVerticalStage(t, ctx, db, verticalID, "ready_for_review")

	outcome, ok := pc.applyWorkflowEventTransition(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.approved"),
		SourceAgent: "human",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"vertical_id": verticalID, "mailbox_decision_id": uuid.NewString()}),
	})
	if !ok {
		t.Fatal("expected human vertical.approved to transition")
	}
	if outcome.Transition.Name != "ready_to_approved" {
		t.Fatalf("unexpected transition: %+v", outcome.Transition)
	}
	assertVerticalStage(t, ctx, db, verticalID, "approved")
}


func TestFactoryPipelineCoordinator_HandleScoringRequested_ProjectsScoringStage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Clinical Billing', 'clinical-billing', 'us', 'discovered', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	pc.handleScoringRequested(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.discovered"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"vertical_id": verticalID, "vertical_name": "Clinical Billing", "geography": "us", "mode": "saas_gap", "signal_strength": 81}),
	})

	assertVerticalStage(t, ctx, db, verticalID, "scoring")
}

func TestFactoryPipelineCoordinator_ApplyWorkflowDataAccumulation_PersistsDeclaredWrites(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Clinical Billing', 'clinical-billing', 'us', 'discovered', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}
	if err := pc.workflowStore.Upsert(ctx, WorkflowInstance{
		InstanceID:       verticalID,
		WorkflowName:     "empire_vertical_pipeline",
		WorkflowVersion:  "2.2.0",
		CurrentStage:     "discovered",
		AccumulatorState: map[string]any{},
		Metadata:         map[string]any{},
	}); err != nil {
		t.Fatalf("seed workflow instance: %v", err)
	}

	transition := WorkflowTransition{
		Name: "discovered_to_scoring",
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes:      []string{"name", "geography", "signal_strength"},
			SourceEvent: "vertical.discovered",
		},
	}
	evt := events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.discovered"),
		VerticalID: verticalID,
		Payload: mustJSON(map[string]any{
			"name":            "Clinical Billing",
			"geography":       "us",
			"signal_strength": 81,
			"ignored_field":   "x",
		}),
	}

	pc.applyWorkflowDataAccumulation(ctx, verticalID, transition, evt)

	instance, ok, err := pc.workflowStore.Load(ctx, verticalID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance")
	}
	entityProjection, ok := instance.AccumulatorState["entity_projection"].(map[string]any)
	if !ok {
		t.Fatalf("expected entity_projection map, got %#v", instance.AccumulatorState["entity_projection"])
	}
	if asString(entityProjection["name"]) != "Clinical Billing" {
		t.Fatalf("expected name to be accumulated, got %#v", entityProjection["name"])
	}
	if asString(entityProjection["geography"]) != "us" {
		t.Fatalf("expected geography to be accumulated, got %#v", entityProjection["geography"])
	}
	if asInt(entityProjection["signal_strength"]) != 81 {
		t.Fatalf("expected signal_strength to be accumulated, got %#v", entityProjection["signal_strength"])
	}
	if _, exists := entityProjection["ignored_field"]; exists {
		t.Fatalf("did not expect ignored_field in entity_projection: %#v", entityProjection)
	}
	if asString(instance.Metadata["last_data_accumulation_event"]) != "vertical.discovered" {
		t.Fatalf("expected last_data_accumulation_event to be recorded, got %#v", instance.Metadata["last_data_accumulation_event"])
	}
}

func TestFactoryPipelineCoordinator_FinalizeScoringAccumulator_UsesWorkflowTransition(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	store := &captureStore{}
	bus := NewEventBus(store)
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Claims Workflow', 'claims-workflow', 'us', 'scoring', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	acc := newUniversalAccumulator(verticalID, "Claims Workflow", "us", "saas_gap")
	setScores(acc, map[string]int{
		"build_complexity":        90,
		"automation_completeness": 90,
		"icp_crispness":           90,
		"distribution_leverage":   90,
		"time_to_value":           90,
		"operational_drag":        90,
		"pain_severity":           90,
		"competition_gap":         90,
		"monetization_clarity":    90,
		"retention_architecture":  90,
		"expansion_potential":     90,
	})
	pc.mu.Lock()
	pc.scoringState.accumulators[verticalID] = acc
	pc.mu.Unlock()

	pc.finalizeScoringAccumulator(ctx, verticalID, false)

	assertVerticalStage(t, ctx, db, verticalID, "shortlisted")
	if !hasPersistedEventType(store.events, "vertical.shortlisted") {
		t.Fatalf("expected vertical.shortlisted emit, got %+v", store.events)
	}
	instance, ok, err := pc.workflowStore.Load(ctx, verticalID)
	if err != nil {
		t.Fatalf("load workflow instance: %v", err)
	}
	if !ok {
		t.Fatal("expected workflow instance")
	}
	found := false
	for _, item := range instance.TransitionHistory {
		if item.To == "shortlisted" && (item.TransitionID == "scoring_to_shortlisted" || item.TransitionID == "vertical.scored") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected shortlisted transition record in history, got %+v", instance.TransitionHistory)
	}
}

func assertVerticalStage(t *testing.T, ctx context.Context, db *sql.DB, verticalID, want string) {
	t.Helper()
	var got string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(stage,'') FROM verticals WHERE id = $1::uuid`, verticalID).Scan(&got); err != nil {
		t.Fatalf("load vertical stage: %v", err)
	}
	if got != want {
		t.Fatalf("expected stage=%q got=%q", want, got)
	}
}
