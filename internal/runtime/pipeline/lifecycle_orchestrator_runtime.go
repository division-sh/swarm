package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"empireai/internal/events"
)

func (n *LifecycleOrchestrator) handleVerticalApproved(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	n.coordinator.validationGate.handleVerticalApproved(ctx, evt)
}

func (n *LifecycleOrchestrator) handleVerticalKilled(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	n.coordinator.validationGate.handleVerticalKilled(ctx, evt)
}

func (n *LifecycleOrchestrator) handleVerticalResumed(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	n.coordinator.validationGate.handleVerticalResumed(ctx, evt)
}

func (n *LifecycleOrchestrator) handleOpCoCEOReady(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	n.coordinator.validationGate.handleOpCoCEOReady(ctx, evt)
}

func (n *LifecycleOrchestrator) handleRuntimeReset(ctx context.Context) {
	if n == nil || n.coordinator == nil {
		return
	}
	n.coordinator.resetWorkflowRuntimeState(ctx)
}

func (n *LifecycleOrchestrator) handleMarginalReviewTimer(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil {
		return
	}
	n.coordinator.forwardMarginalReviewTimer(ctx, evt)
}

func (n *LifecycleOrchestrator) handlePortfolioDigestTimer(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil {
		return
	}
	n.coordinator.forwardPortfolioDigestTimer(ctx, evt)
}

func (n *LifecycleOrchestrator) handleBudgetThresholdCrossed(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil {
		return
	}
	n.coordinator.handleLifecycleBudgetThreshold(ctx, evt)
}

func (n *LifecycleOrchestrator) handleMailboxItemDecided(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil {
		return
	}
	n.coordinator.handleLifecycleMailboxDecision(ctx, evt)
}

func (n *LifecycleOrchestrator) handleQAValidationPassed(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil {
		return
	}
	n.coordinator.persistLifecycleEvidence(ctx, evt, "qa_passed", true)
}

func (n *LifecycleOrchestrator) handleReviewDeployFeedback(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	n.coordinator.persistLifecycleEvidence(ctx, evt, "deploy_approved", truthyMetadataFlag(payload["deploy_approved"]) || truthyMetadataFlag(payload["approved"]))
}

func (n *LifecycleOrchestrator) handleSystemDirective(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil {
		return
	}
	n.coordinator.forwardSystemDirective(ctx, evt)
}

func (n *LifecycleOrchestrator) handleBuildComplete(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil {
		return
	}
	n.coordinator.applyLifecycleStageEvent(ctx, evt, "pre_launch")
}

func (n *LifecycleOrchestrator) handleLaunchReady(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil {
		return
	}
	n.coordinator.applyLifecycleStageEvent(ctx, evt, "launched")
}

func (n *LifecycleOrchestrator) handleOpcoSteadyStateReached(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil {
		return
	}
	n.coordinator.applyLifecycleStageEvent(ctx, evt, "operating")
}

func (n *LifecycleOrchestrator) handleOpcoGrowthTriggered(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil {
		return
	}
	n.coordinator.applyLifecycleStageEvent(ctx, evt, "expanding")
}

func (n *LifecycleOrchestrator) handleOpcoGrowthStabilized(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil {
		return
	}
	n.coordinator.applyLifecycleStageEvent(ctx, evt, "operating")
}

func (n *LifecycleOrchestrator) handleOpcoTeardownRequested(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil {
		return
	}
	n.coordinator.applyLifecycleTeardownEvent(ctx, evt)
}

func (pc *FactoryPipelineCoordinator) persistLifecycleEvidence(ctx context.Context, evt events.Event, key string, value any) {
	if pc == nil || strings.TrimSpace(key) == "" {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	verticalID := strings.TrimSpace(firstNonEmptyString(evt.VerticalID, asString(payload["vertical_id"])))
	if verticalID == "" {
		return
	}
	pc.PersistWorkflowMetadata(ctx, verticalID, func(metadata map[string]any) {
		metadata[key] = value
	})
}

func (pc *FactoryPipelineCoordinator) forwardSystemDirective(ctx context.Context, evt events.Event) {
	if pc == nil || pc.bus == nil {
		return
	}
	recipients := []string{"scan-campaign-manager"}
	pc.publishDirect(ctx, string(evt.Type), strings.TrimSpace(evt.VerticalID), parsePayloadMap(evt.Payload), recipients)
}

func (pc *FactoryPipelineCoordinator) applyLifecycleStageEvent(ctx context.Context, evt events.Event, fallbackStage string) {
	if pc == nil {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	verticalID := strings.TrimSpace(firstNonEmptyString(evt.VerticalID, asString(payload["vertical_id"])))
	if verticalID == "" {
		return
	}
	if _, ok := pc.applyWorkflowEventTransition(ctx, evt); ok {
		return
	}
	if strings.TrimSpace(fallbackStage) != "" {
		pc.updateVerticalStage(ctx, verticalID, fallbackStage, string(evt.Type))
	}
}

func (pc *FactoryPipelineCoordinator) applyLifecycleTeardownEvent(ctx context.Context, evt events.Event) {
	if pc == nil {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	verticalID := strings.TrimSpace(firstNonEmptyString(evt.VerticalID, asString(payload["vertical_id"])))
	if verticalID == "" {
		return
	}
	if _, ok := pc.applyWorkflowEventTransition(ctx, evt); ok {
		return
	}
	pc.PersistWorkflowMetadata(ctx, verticalID, func(metadata map[string]any) {
		metadata["teardown_requested"] = true
	})
	pc.updateVerticalStage(ctx, verticalID, "winding_down", string(evt.Type))
}

func (pc *FactoryPipelineCoordinator) resetWorkflowRuntimeState(ctx context.Context) {
	if pc == nil {
		return
	}
	pc.resetInMemoryState()
	pc.clearPersistentState(ctx)
}

func (pc *FactoryPipelineCoordinator) forwardMarginalReviewTimer(ctx context.Context, evt events.Event) {
	if pc == nil || pc.bus == nil {
		return
	}
	recipients := pc.bus.ResolveSubscribedRecipients("timer.marginal_review")
	if len(recipients) == 0 {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	if boolFromAny(payload["review_request_injected"]) {
		return
	}
	count := pc.countVerticalsInStage(ctx, "marginal_review")
	if strings.TrimSpace(asString(payload["parked_marginals_summary"])) == "" {
		payload["parked_marginals_summary"] = fmt.Sprintf("%d marginal vertical(s) parked for review", count)
	}
	payload["review_request_injected"] = true
	pc.publishDirect(ctx, "timer.marginal_review", strings.TrimSpace(evt.VerticalID), payload, recipients)
}

func (pc *FactoryPipelineCoordinator) forwardPortfolioDigestTimer(ctx context.Context, evt events.Event) {
	if pc == nil || pc.bus == nil {
		return
	}
	recipients := pc.bus.ResolveSubscribedRecipients("timer.portfolio_digest")
	if len(recipients) == 0 {
		return
	}
	raw := parsePayloadMap(evt.Payload)
	if boolFromAny(raw["scoring_rejections_injected"]) {
		return
	}
	pc.mu.Lock()
	since := pc.lastScoringDigestReadAt
	pc.mu.Unlock()
	entries, newest := pc.consumeScoringDigestEntries(ctx, 100, since)
	now := time.Now().UTC()
	if !newest.IsZero() {
		now = newest
	}
	pc.mu.Lock()
	pc.lastScoringDigestReadAt = now
	pc.mu.Unlock()

	snapshot, _ := raw["snapshot"].(map[string]any)
	metadata, _ := raw["metadata"].(map[string]any)
	payload := PortfolioDigestTimerPayload{
		Message:                   strings.TrimSpace(asString(raw["message"])),
		DigestText:                strings.TrimSpace(asString(raw["digest_text"])),
		TriggerReason:             strings.TrimSpace(asString(raw["trigger_reason"])),
		Snapshot:                  snapshot,
		Metadata:                  metadata,
		VerticalID:                strings.TrimSpace(asString(raw["vertical_id"])),
		TaskID:                    strings.TrimSpace(asString(raw["task_id"])),
		RecentRejections:          entries,
		RejectionCount:            len(entries),
		ScoringRejectionsInjected: true,
		ScoringRejectionsCount:    len(entries),
		ScoringRejectionSummaries: entries,
	}
	pc.publishDirect(ctx, "timer.portfolio_digest", strings.TrimSpace(evt.VerticalID), payloadMap(payload), recipients)
}

func (pc *FactoryPipelineCoordinator) handleLifecycleBudgetThreshold(context.Context, events.Event) {
	// 2.2.0 lifecycle compatibility hook. Budget reactions still live in the
	// broader runtime and campaign manager while the system-node contract lands.
}

func (pc *FactoryPipelineCoordinator) handleLifecycleMailboxDecision(context.Context, events.Event) {
	// 2.2.0 lifecycle compatibility hook. Mailbox resolution continues to drive
	// follow-on work through existing manager and campaign paths.
}

func (pc *FactoryPipelineCoordinator) applyMarginalKillTimer(ctx context.Context, evt events.Event) {
	if pc == nil {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	verticalID := strings.TrimSpace(firstNonEmptyString(evt.VerticalID, asString(payload["vertical_id"])))
	if verticalID == "" {
		return
	}
	if pc.currentWorkflowState(ctx, verticalID).Stage != NormalizePipelineStage("marginal_review") {
		return
	}
	if strings.TrimSpace(asString(payload["reason"])) == "" {
		payload["reason"] = "marginal_kill_timer"
	}
	payload["timer_id"] = firstNonEmptyString(asString(payload["timer_id"]), "marginal_kill_timer")
	pc.publish(ctx, "vertical.killed", verticalID, payload)
}
