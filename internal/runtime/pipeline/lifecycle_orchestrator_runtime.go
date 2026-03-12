package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"empireai/internal/events"
)

func (n *LifecycleOrchestrator) handleVerticalApproved(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	verticalID := workflowEventEntityID(evt)
	if verticalID == "" {
		return
	}
	n.coordinator.validationGate.mu.Lock()
	st := n.coordinator.validationGate.getStateLocked(verticalID)
	st.Status = "approved"
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	n.coordinator.validationGate.mu.Unlock()
	if _, ok := n.coordinator.applyWorkflowEventTransition(ctx, evt); !ok {
		n.coordinator.updateVerticalStage(ctx, verticalID, "approved", "")
	}
}

func (n *LifecycleOrchestrator) handleVerticalKilled(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	verticalID := workflowEventEntityID(evt)
	if verticalID == "" {
		return
	}
	n.coordinator.validationGate.mu.Lock()
	st := n.coordinator.validationGate.getStateLocked(verticalID)
	st.Status = "rejected"
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	n.coordinator.validationGate.mu.Unlock()
	if _, ok := n.coordinator.applyWorkflowEventTransition(ctx, evt); !ok {
		n.coordinator.updateVerticalStage(ctx, verticalID, "killed", string(evt.Type))
	}
}

func (n *LifecycleOrchestrator) handleVerticalResumed(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	verticalID := workflowEventEntityID(evt)
	if verticalID == "" {
		return
	}
	n.coordinator.validationGate.mu.Lock()
	st := n.coordinator.validationGate.getStateLocked(verticalID)
	st.Status = "active"
	st.RevisionCount = 0
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	missingG1 := !st.G1Research
	missingG2 := !st.G2Spec
	missingG3 := !st.G3CTO
	missingG4 := !st.G4Brand
	all := st.G1Research && st.G2Spec && st.G3CTO && st.G4Brand
	stage := n.coordinator.validationGate.stageForState(st)
	scoringRaw := cloneRaw(st.ScoringPayload)
	var bundle ValidationPackageReadyPayload
	hasBundle := false
	if all {
		now := time.Now().UTC()
		hasBundle = true
		bundle = n.coordinator.payloadFactory.BuildValidationPackageReadyPayload(ctx, verticalID, validationContextSnapshot{
			Research:    parsePayloadMap(st.ResearchPayload),
			Spec:        parsePayloadMap(st.SpecPayload),
			CTONotes:    parsePayloadMap(st.CTOPayload),
			Brand:       parsePayloadMap(st.BrandPayload),
			Scoring:     parsePayloadMap(st.ScoringPayload),
			SpecVersion: st.SpecVersion,
		})
		st.PackagingRequested = true
		st.PackagingRequestedAt = &now
	}
	n.coordinator.validationGate.mu.Unlock()
	if _, ok := n.coordinator.applyWorkflowEventTransition(ctx, evt); !ok {
		n.coordinator.updateVerticalStage(ctx, verticalID, stage, "")
	}

	resumePayload := parsePayloadMap(evt.Payload)
	snap := n.coordinator.payloadFactory.ValidationContext(verticalID)
	if missingG1 {
		scoringPayload := parsePayloadMap(scoringRaw)
		if len(scoringPayload) == 0 {
			scoringPayload = parsePayloadMap(evt.Payload)
		}
		n.coordinator.publish(ctx, "validation.started", verticalID, payloadMap(n.coordinator.payloadFactory.BuildValidationStartedPayload(ctx, verticalID, scoringPayload, resumePayload)))
	}
	if missingG2 {
		n.coordinator.publish(ctx, "spec.revision_requested", verticalID, payloadMap(n.coordinator.payloadFactory.BuildSpecRevisionRequestedPayload(ctx, verticalID, "resume", resumePayload)))
	}
	if missingG3 {
		n.coordinator.publish(ctx, "cto.spec_review_requested", verticalID, payloadMap(n.coordinator.payloadFactory.BuildCTOSpecReviewRequestedPayload(ctx, verticalID, resumePayload)))
	}
	if missingG4 {
		n.coordinator.publish(ctx, "brand.requested", verticalID, payloadMap(n.coordinator.payloadFactory.BuildBrandRequestedPayload(ctx, verticalID, snap.Scoring, snap.Research)))
	}
	if hasBundle {
		n.coordinator.publish(ctx, "validation.package_ready", verticalID, payloadMap(bundle))
	}
}

func (n *LifecycleOrchestrator) handleOpCoCEOReady(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	verticalID := workflowEventEntityID(evt)
	if verticalID == "" {
		payload := parsePayloadMap(evt.Payload)
		verticalID = strings.TrimSpace(asString(payload["vertical_id"]))
	}
	if verticalID == "" {
		return
	}
	n.coordinator.updateVerticalStage(ctx, verticalID, "operating", "")
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

func (n *LifecycleOrchestrator) handleBuildOrchestratorEvent(ctx context.Context, evt events.Event) bool {
	if n == nil || n.coordinator == nil {
		return false
	}
	return n.coordinator.executeNodeHandlerPlan(ctx, "build-orchestrator", evt)
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
	verticalID := workflowEventEntityIDWithPayload(evt, payload)
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
	pc.publishDirect(ctx, string(evt.Type), workflowEventEntityID(evt), parsePayloadMap(evt.Payload), recipients)
}

func (pc *FactoryPipelineCoordinator) handlePortfolioSystemDirective(ctx context.Context, evt events.Event) {
	if pc == nil {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	if _, ok := asObject(payload["directive"]); !ok {
		pc.forwardSystemDirective(ctx, evt)
		return
	}
	source := pc.SemanticSource()
	if source == nil {
		return
	}
	handler, ok := source.NodeEventHandler("portfolio-node", string(evt.Type))
	if !ok || len(handler.Rules) == 0 {
		return
	}
	triggerCtx := workflowTriggerContext{
		Event: evt,
		State: WorkflowState{},
	}
	match, ok := pc.matchTypedRules(triggerCtx, handler.Rules)
	if !ok {
		return
	}
	for _, emitEvent := range match.Emits {
		emitEvent = strings.TrimSpace(emitEvent)
		if emitEvent == "" {
			continue
		}
		verticalID, emitPayload := portfolioDirectiveEmitPayload(evt, emitEvent)
		pc.publish(ctx, emitEvent, verticalID, emitPayload)
	}
}

func (pc *FactoryPipelineCoordinator) applyLifecycleStageEvent(ctx context.Context, evt events.Event, fallbackStage string) {
	if pc == nil {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	verticalID := workflowEventEntityIDWithPayload(evt, payload)
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

func portfolioDirectiveEmitPayload(evt events.Event, emitEvent string) (string, map[string]any) {
	payload := parsePayloadMap(evt.Payload)
	directive, _ := asObject(payload["directive"])
	params, _ := asObject(directive["parameters"])
	out := cloneStringAnyMap(params)
	if out == nil {
		out = map[string]any{}
	}
	directiveType := strings.TrimSpace(asString(directive["type"]))
	directiveID := strings.TrimSpace(evt.ID)
	switch strings.TrimSpace(emitEvent) {
	case "scan.requested":
		if strings.TrimSpace(asString(out["campaign_id"])) == "" {
			out["campaign_id"] = firstNonEmptyString(directiveID, evt.ID)
		}
		if _, ok := out["campaign_context"]; !ok {
			mode := strings.TrimSpace(asString(out["mode"]))
			modes := []string{}
			if mode != "" {
				modes = append(modes, mode)
			}
			out["campaign_context"] = map[string]any{
				"directive_id":      directiveID,
				"modes":             modes,
				"strategic_context": portfolioDirectiveContextString(directive),
			}
		}
		return "", out
	case "budget.adjustment_requested":
		return strings.TrimSpace(asString(out["vertical_id"])), out
	case "policy.change_requested":
		if strings.TrimSpace(asString(out["requested_by"])) == "" {
			out["requested_by"] = strings.TrimSpace(evt.SourceAgent)
		}
		return "", out
	case "vertical.resumed":
		verticalID := strings.TrimSpace(firstNonEmptyString(asString(out["vertical_id"]), workflowEventEntityID(evt)))
		if verticalID != "" && strings.TrimSpace(asString(out["vertical_id"])) == "" {
			out["vertical_id"] = verticalID
		}
		if strings.TrimSpace(asString(out["reason"])) == "" {
			out["reason"] = "system.directive"
		}
		return verticalID, out
	case "directive.unhandled":
		return "", map[string]any{
			"directive_text": portfolioDirectiveContextString(directive),
			"directive_type": directiveType,
			"reason":         "no_matching_rule",
		}
	default:
		return strings.TrimSpace(asString(out["vertical_id"])), out
	}
}

func portfolioDirectiveContextString(directive map[string]any) string {
	if len(directive) == 0 {
		return ""
	}
	if data, err := json.Marshal(directive); err == nil {
		return string(data)
	}
	return fmt.Sprintf("%v", directive)
}

func (pc *FactoryPipelineCoordinator) applyLifecycleTeardownEvent(ctx context.Context, evt events.Event) {
	if pc == nil {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	verticalID := workflowEventEntityIDWithPayload(evt, payload)
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
	pc.publishDirect(ctx, "timer.marginal_review", workflowEventEntityID(evt), payload, recipients)
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
	pc.publishDirect(ctx, "timer.portfolio_digest", workflowEventEntityID(evt), payloadMap(payload), recipients)
}

func (pc *FactoryPipelineCoordinator) handleLifecycleBudgetThreshold(ctx context.Context, evt events.Event) {
	if pc == nil {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	entityID := firstNonEmptyString(
		workflowEventEntityID(evt),
		strings.TrimSpace(asString(payload["vertical_id"])),
		strings.TrimSpace(asString(payload["entity_id"])),
	)
	alertType := firstNonEmptyString(
		strings.TrimSpace(asString(payload["state"])),
		strings.TrimSpace(asString(payload["level"])),
		"warning",
	)
	pc.publish(ctx, "budget.alert_sent", workflowEventEntityID(evt), map[string]any{
		"entity_id":  entityID,
		"alert_type": alertType,
		"details":    payload,
	})
}

func (pc *FactoryPipelineCoordinator) handleLifecycleMailboxDecision(ctx context.Context, evt events.Event) {
	source := pc.SemanticSource()
	if pc == nil || source == nil {
		return
	}
	handler, ok := source.NodeEventHandler("lifecycle-orchestrator", string(evt.Type))
	if !ok || len(handler.Rules) == 0 {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	verticalID := workflowEventEntityIDWithPayload(evt, payload)
	triggerCtx := workflowTriggerContext{
		Event:           evt,
		State:           pc.currentWorkflowState(ctx, verticalID),
		ValidationState: pc.validationStateSnapshot(verticalID),
	}
	match, ok := pc.matchTypedRules(triggerCtx, handler.Rules)
	if !ok {
		return
	}
	if verticalID != "" && match.AdvancesTo != "" {
		pc.updateVerticalStage(ctx, verticalID, match.AdvancesTo, string(evt.Type))
	}
	for _, emitEvent := range match.Emits {
		emitEvent = strings.TrimSpace(emitEvent)
		if emitEvent == "" {
			continue
		}
		pc.publish(ctx, emitEvent, verticalID, cloneStringAnyMap(payload))
	}
}

func (pc *FactoryPipelineCoordinator) applyMarginalKillTimer(ctx context.Context, evt events.Event) {
	if pc == nil {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	verticalID := workflowEventEntityIDWithPayload(evt, payload)
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
