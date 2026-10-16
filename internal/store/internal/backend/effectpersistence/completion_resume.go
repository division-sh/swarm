package effectpersistence

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	storemanagedcapability "github.com/division-sh/swarm/internal/store/internal/backend/managedcapability"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

func queueCompletionResume(ctx context.Context, mutation *mutationprotocol.Attempt, delivery providerDrainDeliveryOwner, recovered completionRecoveryAttempt, attempt runtimeeffects.Attempt, settlement runtimeeffects.CompletionSettlement, resolution providerDrainRecoveryResolution, takeover *startupProviderTakeover) (bool, error) {
	if takeover == nil || resolution.Kind != completionSettlementDrained || resolution.Expired ||
		resolution.Drain.Target != runtimeeffects.ProviderDrainTargetRunning || attempt.Authority.Kind != runtimeeffects.AuthorityNormalAgent ||
		attempt.Authority.ExecutionMode != executionmode.Live ||
		attempt.Origin.Kind != runtimeeffects.CompletionOriginDelivery || settlement.Settlement.State != runtimeeffects.StateOutcomeUncertain {
		return false, nil
	}
	eligible, err := completionResumeJournalEligible(recovered, attempt)
	if err != nil || !eligible {
		return false, err
	}
	if delivery == nil {
		return false, fmt.Errorf("provider resume delivery owner is not bound")
	}
	return delivery.QueueProviderResumeTx(ctx, mutation, attempt.Origin.Delivery, deliverylifecycle.Settlement{
		Disposition: deliverylifecycle.FailureRetry,
		ReasonCode:  "provider_resume_queued", Failure: settlement.Settlement.Failure,
		RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation(),
	})
}

func completionResumeJournalEligible(recovered completionRecoveryAttempt, attempt runtimeeffects.Attempt) (bool, error) {
	if recovered.ProviderRequestJournal == "" {
		return false, nil
	}
	journal, err := runtimeeffects.DecodeProviderRequestJournal([]byte(recovered.ProviderRequestJournal))
	if err != nil {
		return false, nil
	}
	surface, err := storemanagedcapability.Decode([]byte(recovered.CapabilitySurface))
	if err != nil {
		return false, err
	}
	eligible, err := completionResumeSurfaceAllowsHTTP(attempt.Adapter, attempt.Authority.CompletionRequest, surface)
	if err != nil || !eligible {
		return false, err
	}
	frame, err := agentframe.DecodeDurable(recovered.AgentFrame)
	if err != nil {
		return false, err
	}
	if journal.Adapter != attempt.Adapter || journal.Memory == nil || *journal.Memory != attempt.Authority.Target.Memory ||
		journal.SessionID != attempt.Authority.Target.SessionID || journal.BodyFingerprint != recovered.RequestFingerprint {
		return false, nil
	}
	identityContext := runtimeeffects.WithLogicalOperationIdentity(context.Background(), journal.LogicalOperationIdentity)
	if err := runtimeeffects.ValidateProviderRequestJournalAdmission(identityContext, attempt.Authority, runtimeeffects.AuthorizeRequest{
		OperationID: attempt.OperationID, Adapter: attempt.Adapter, Kind: attempt.Kind,
		Class: runtimeeffects.EffectWriteOrUnknown, Transport: attempt.Transport,
		RequestFingerprint: recovered.RequestFingerprint, AgentFrame: &frame, CapabilitySurface: &surface,
		ProviderRequestJournal: &journal, Lineage: journal.Lineage,
	}); err != nil {
		return false, nil
	}
	// The independent renderer validates the original body and physical-request
	// pins. Recovery never substitutes current configuration or conversation.
	if _, err := runtimellm.RebuildProviderRequestJournal(journal, frame, surface, recovered.RequestFingerprint); err != nil {
		return false, nil
	}
	return true, nil
}

// This classifies only the original frozen provider surface. It does not
// authorize recovery, an origin retry, or a network invocation.
func completionResumeSurfaceAllowsHTTP(adapter string, request runtimeeffects.CompletionRequest, surface managedcapabilities.Surface) (bool, error) {
	var backend string
	switch adapter {
	case "anthropic_api":
		backend = llmselection.BackendAnthropic
	case "openai_compatible":
		backend = llmselection.BackendOpenAICompatible
	case "openai_responses":
		backend = llmselection.BackendOpenAIResponses
	default:
		return false, nil
	}
	if err := request.Validate(); err != nil {
		return false, fmt.Errorf("provider resume request identity: %w", err)
	}
	if request.BackendProfile != backend {
		return false, fmt.Errorf("provider resume adapter contradicts its frozen request")
	}
	profile, err := llmselection.ResolveActiveBackend(backend)
	if err != nil {
		return false, err
	}
	if err := surface.Validate(); err != nil {
		return false, fmt.Errorf("provider resume frozen surface: %w", err)
	}
	if surface.Authority.Kind != managedcapabilities.AuthorityProviderTurn || surface.Provider != profile.Provider ||
		surface.Transport != profile.Transport || surface.RuntimeMode != profile.RuntimeMode || surface.HasMismatch() {
		return false, fmt.Errorf("provider resume surface contradicts its frozen request")
	}
	if surface.Authority.ExecutionKind != managedcapabilities.ExecutionNormalAgent {
		return false, nil
	}
	for _, tool := range surface.Tools {
		if tool.Capability.AuthorizationClass == "provider_native" || len(tool.Bindings) == 0 {
			return false, nil
		}
		for _, binding := range tool.Bindings {
			if binding.Kind != managedcapabilities.BindingAPIDefinition {
				return false, nil
			}
		}
	}
	return true, nil
}
