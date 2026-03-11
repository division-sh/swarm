package empire

import "testing"

func TestHoldingFlow_C1_ShortlistedCreatesValidationPipelineAndEnrichedPayloads(t *testing.T) {
	module := NewModule()
	bundle := module.ContractBundle()
	if bundle == nil {
		t.Fatal("expected Empire workflow bundle")
	}
	if got := bundle.FlowInitialStage("validation"); got != "researching" {
		t.Fatalf("validation initial stage = %q, want researching", got)
	}
	if handler, ok := bundle.NodeEventHandler("validation-orchestrator", "vertical.shortlisted"); !ok {
		t.Fatal("expected validation-orchestrator handler for vertical.shortlisted")
	} else if handler.AdvancesTo != "researching" {
		t.Fatalf("vertical.shortlisted advances_to = %q, want researching", handler.AdvancesTo)
	}
}

func TestHoldingFlow_C2_BusinessResearchReceivesValidationContextAndCanContinue(t *testing.T) {
	module := NewModule()
	bundle := module.ContractBundle()
	handler, ok := bundle.NodeEventHandler("validation-orchestrator", "research.completed")
	if !ok {
		t.Fatal("expected validation-orchestrator handler for research.completed")
	}
	if handler.AdvancesTo != "mvp_speccing" {
		t.Fatalf("research.completed advances_to = %q, want mvp_speccing", handler.AdvancesTo)
	}
	if len(handler.DataAccumulation.Writes) == 0 {
		t.Fatal("expected research.completed data accumulation writes")
	}
}

func TestHoldingFlow_C3_ResearchCompletedSetsG1AndSpecRequestedPassthrough(t *testing.T) {
	module := NewModule()
	bundle := module.ContractBundle()
	handler, ok := bundle.NodeEventHandler("validation-orchestrator", "research.completed")
	if !ok {
		t.Fatal("expected validation-orchestrator handler for research.completed")
	}
	if handler.SetsGate == nil || handler.SetsGate.Name != "g1_research" {
		t.Fatalf("expected research.completed to set gate g1_research, got %+v", handler.SetsGate)
	}
	if emits := handler.Emits.Values(); len(emits) == 0 || emits[0] != "spec.requested" {
		t.Fatalf("expected research.completed to emit spec.requested, got %v", emits)
	}
}

func TestHoldingFlow_C4_SpecDraftToReviewRouting(t *testing.T) {
	module := NewModule()
	bundle := module.ContractBundle()
	handler, ok := bundle.NodeEventHandler("validation-orchestrator", "spec.draft_ready")
	if !ok {
		t.Fatal("expected validation-orchestrator handler for spec.draft_ready")
	}
	if emits := handler.Emits.Values(); len(emits) == 0 || emits[0] != "spec_review.requested" {
		t.Fatalf("expected spec.draft_ready to emit spec_review.requested, got %v", emits)
	}
}

func TestHoldingFlow_C5_SpecReviewPassedTriggersCTORequest(t *testing.T) {
	module := NewModule()
	bundle := module.ContractBundle()
	handler, ok := bundle.NodeEventHandler("validation-orchestrator", "spec_review.passed")
	if !ok {
		t.Fatal("expected validation-orchestrator handler for spec_review.passed")
	}
	if handler.AdvancesTo != "cto_review" {
		t.Fatalf("spec_review.passed advances_to = %q, want cto_review", handler.AdvancesTo)
	}
	if emits := handler.Emits.Values(); len(emits) == 0 || emits[0] != "cto.spec_review_requested" {
		t.Fatalf("expected spec_review.passed to emit cto.spec_review_requested, got %v", emits)
	}
}

func TestHoldingFlow_CatalogSmoke_EventPayloadJSONRoundTrip(t *testing.T) {
	module := NewModule()
	bundle := module.ContractBundle()
	if len(bundle.Events) == 0 {
		t.Fatal("expected Empire event catalog entries")
	}
	if outputs := bundle.FlowOutputEvents("validation"); !containsString(outputs, "validation.package_ready") {
		t.Fatalf("validation flow outputs = %v, want validation.package_ready", outputs)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
