package pipeline

import (
	"context"
	"strings"
	"time"

	"empireai/internal/events"
)

func (n *ValidationOrchestrator) executeDeclarativeValidationHandler(ctx context.Context, evt events.Event) bool {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return false
	}
	if !n.coordinator.executeNodeHandlerPlan(ctx, n.NodeID(), evt) {
		return false
	}
	n.syncDeclarativeValidationState(evt)
	n.maybePublishDeclarativeValidationPackage(ctx, evt)
	return true
}

func (n *ValidationOrchestrator) syncDeclarativeValidationState(evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	n.coordinator.validationGate.mu.Lock()
	defer n.coordinator.validationGate.mu.Unlock()

	st := n.coordinator.validationGate.getStateLocked(verticalID)
	switch strings.TrimSpace(string(evt.Type)) {
	case "vertical.shortlisted":
		st.Status = "active"
		st.G1Research = false
		st.G2Spec = false
		st.G3CTO = false
		st.G4Brand = false
		st.ScoringPayload = cloneRaw(evt.Payload)
		st.PackagingRequested = false
		st.PackagingRequestedAt = nil
		st.PackagingRetries = 0
	case "research.completed":
		st.Status = "active"
		st.G1Research = true
		if len(st.ResearchPayload) > 0 && len(evt.Payload) > 0 {
			st.ResearchPayload = mergeRawPayload(st.ResearchPayload, evt.Payload)
		} else {
			st.ResearchPayload = cloneRaw(evt.Payload)
		}
	case "spec.approved":
		st.Status = "active"
		st.SpecPayload = cloneRaw(evt.Payload)
	case "cto.spec_approved":
		st.Status = "active"
		st.G3CTO = true
		st.CTOPayload = cloneRaw(evt.Payload)
	case "brand.candidates_ready":
		st.Status = "active"
		st.G4Brand = true
		st.BrandPayload = cloneRaw(evt.Payload)
		now := time.Now().UTC()
		st.PackagingRequested = true
		st.PackagingRequestedAt = &now
		st.PackagingRetries = 0
	case "research.vertical_rejected", "cto.spec_vetoed":
		st.Status = "rejected"
		st.PackagingRequested = false
		st.PackagingRequestedAt = nil
		st.PackagingRetries = 0
	case "vertical.ready_for_review":
		st.Status = "packaged"
		st.PackagingRequested = false
		st.PackagingRequestedAt = nil
		st.PackagingRetries = 0
	case "vertical.needs_more_data":
		st.Status = "active"
		st.G1Research = false
		st.ResearchPayload = nil
		st.PackagingRequested = false
		st.PackagingRequestedAt = nil
		st.PackagingRetries = 0
	case "spec.revision_requested":
		st.G2Spec = false
		st.G3CTO = false
		st.SpecPayload = nil
		st.CTOPayload = nil
		st.PackagingRequested = false
		st.PackagingRequestedAt = nil
		st.PackagingRetries = 0
		st.InnerRevisionCount = 0
	case "spec.validation_failed", "cto.spec_revision_needed":
		st.Status = "active"
		st.G2Spec = false
		st.G3CTO = false
		st.SpecPayload = nil
		st.CTOPayload = nil
		st.PackagingRequested = false
		st.PackagingRequestedAt = nil
		st.PackagingRetries = 0
		st.RevisionCount++
	case "brand.revision_needed":
		st.Status = "active"
		st.G4Brand = false
		st.BrandPayload = nil
		st.PackagingRequested = false
		st.PackagingRequestedAt = nil
		st.PackagingRetries = 0
	}
}

func (n *ValidationOrchestrator) maybePublishDeclarativeValidationPackage(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	switch strings.TrimSpace(string(evt.Type)) {
	case "research.completed", "cto.spec_approved":
	default:
		return
	}

	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}

	var bundle ValidationPackageReadyPayload
	hasBundle := false

	n.coordinator.validationGate.mu.Lock()
	st := n.coordinator.validationGate.getStateLocked(verticalID)
	if st.Status == "active" && !st.PackagingRequested && st.G1Research && st.G2Spec && st.G3CTO && st.G4Brand {
		now := time.Now().UTC()
		st.PackagingRequested = true
		st.PackagingRequestedAt = &now
		st.PackagingRetries = 0
		hasBundle = true
		bundle = n.coordinator.payloadFactory.BuildValidationPackageReadyPayload(ctx, verticalID, validationContextSnapshot{
			Research:    parsePayloadMap(st.ResearchPayload),
			Spec:        parsePayloadMap(st.SpecPayload),
			CTONotes:    parsePayloadMap(st.CTOPayload),
			Brand:       parsePayloadMap(st.BrandPayload),
			Scoring:     parsePayloadMap(st.ScoringPayload),
			SpecVersion: st.SpecVersion,
		})
	}
	n.coordinator.validationGate.mu.Unlock()

	if hasBundle {
		n.coordinator.publish(ctx, "validation.package_ready", verticalID, payloadMap(bundle))
	}
}

func (n *ValidationOrchestrator) handleValidationStarted(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	scoringPayload := parsePayloadMap(evt.Payload)
	n.coordinator.validationGate.mu.Lock()
	st := n.coordinator.validationGate.states[verticalID]
	if st == nil {
		st = &validationPipelineState{VerticalID: verticalID, Status: "active"}
		n.coordinator.validationGate.states[verticalID] = st
	}
	if st.Status == "" {
		st.Status = "active"
	}
	if st.Status == "parked" || st.Status == "rejected" {
		st.Status = "active"
	}
	if len(evt.Payload) > 0 {
		st.ScoringPayload = cloneRaw(evt.Payload)
	}
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	n.coordinator.validationGate.mu.Unlock()

	if _, ok := n.coordinator.applyWorkflowEventTransition(ctx, evt); !ok {
		n.coordinator.updateVerticalStage(ctx, verticalID, "researching", "")
		validationPayload := n.coordinator.payloadFactory.BuildValidationStartedPayload(ctx, verticalID, scoringPayload, nil)
		n.coordinator.publish(ctx, "validation.started", verticalID, payloadMap(validationPayload))
	}
}

func (n *ValidationOrchestrator) handleValidationGate(ctx context.Context, evt events.Event, gate string) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}

	n.coordinator.validationGate.mu.Lock()
	st := n.coordinator.validationGate.getStateLocked(verticalID)
	if st.Status == "rejected" || st.Status == "packaged" {
		n.coordinator.validationGate.mu.Unlock()
		return
	}
	switch gate {
	case "g1":
		st.G1Research = true
		if len(st.ResearchPayload) > 0 && len(evt.Payload) > 0 {
			st.ResearchPayload = mergeRawPayload(st.ResearchPayload, evt.Payload)
		} else {
			st.ResearchPayload = cloneRaw(evt.Payload)
		}
	case "g2":
		st.G2Spec = true
		st.SpecPayload = cloneRaw(evt.Payload)
		st.InnerRevisionCount = 0
		st.SpecVersion++
	case "g3":
		st.G3CTO = true
		st.CTOPayload = cloneRaw(evt.Payload)
	case "g4":
		st.G4Brand = true
		st.BrandPayload = cloneRaw(evt.Payload)
	}
	st.Status = "active"
	shouldPackage := st.G1Research && st.G2Spec && st.G3CTO && st.G4Brand && !st.PackagingRequested
	stage := n.coordinator.validationGate.stageForState(st)
	var bundle ValidationPackageReadyPayload
	hasBundle := false
	if shouldPackage {
		now := time.Now().UTC()
		st.PackagingRequestedAt = &now
		st.PackagingRetries = 0
		st.PackagingRequested = true
		hasBundle = true
		bundle = n.coordinator.payloadFactory.BuildValidationPackageReadyPayload(ctx, verticalID, validationContextSnapshot{
			Research:    parsePayloadMap(st.ResearchPayload),
			Spec:        parsePayloadMap(st.SpecPayload),
			CTONotes:    parsePayloadMap(st.CTOPayload),
			Brand:       parsePayloadMap(st.BrandPayload),
			Scoring:     parsePayloadMap(st.ScoringPayload),
			SpecVersion: st.SpecVersion,
		})
	}
	n.coordinator.validationGate.mu.Unlock()

	if _, ok := n.coordinator.applyWorkflowEventTransition(ctx, evt); !ok {
		n.coordinator.updateVerticalStage(ctx, verticalID, stage, "")
	}
	if hasBundle {
		n.coordinator.publish(ctx, "validation.package_ready", verticalID, payloadMap(bundle))
	}
}

func (n *ValidationOrchestrator) handleSpecReviewPassed(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	n.handleValidationGate(ctx, evt, "g2")
	n.handleSpecValidationPassed(ctx, evt)
}

func (n *ValidationOrchestrator) handleSpecReviewIssuesFound(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	n.handleSpecValidationFailed(ctx, evt)
}

func (n *ValidationOrchestrator) handleCTOApproved(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	if !n.coordinator.specVersionMatches(verticalID, payload) {
		return
	}
	n.handleValidationGate(ctx, evt, "g3")
}

func (n *ValidationOrchestrator) handleSpecValidationPassed(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	if !n.coordinator.specVersionMatches(verticalID, payload) {
		return
	}
	n.coordinator.publish(ctx, "cto.spec_review_requested", verticalID, payloadMap(n.coordinator.payloadFactory.BuildCTOSpecReviewRequestedPayload(ctx, verticalID, payload)))
}

func (n *ValidationOrchestrator) handleSpecValidationFailed(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	if !n.coordinator.specVersionMatches(verticalID, payload) {
		return
	}
	status := strings.ToLower(strings.TrimSpace(asString(payload["status"])))
	passed := strings.EqualFold(strings.TrimSpace(asString(payload["passed"])), "true")
	if status == "non-blocker" || passed {
		n.coordinator.publish(ctx, "cto.spec_review_requested", verticalID, payloadMap(n.coordinator.payloadFactory.BuildCTOSpecReviewRequestedPayload(ctx, verticalID, payload)))
		return
	}
	escalate := false
	applied := false
	n.coordinator.validationGate.mu.Lock()
	st := n.coordinator.validationGate.getStateLocked(verticalID)
	st.G2Spec = false
	st.G3CTO = false
	st.SpecPayload = nil
	st.CTOPayload = nil
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	n.coordinator.validationGate.mu.Unlock()
	if _, ok := n.coordinator.applyWorkflowEventTransition(ctx, evt); ok {
		applied = true
	} else {
		n.coordinator.validationGate.mu.Lock()
		st := n.coordinator.validationGate.getStateLocked(verticalID)
		st.RevisionCount++
		if st.RevisionCount > maxRevisionCycles {
			st.Status = "parked"
			escalate = true
		}
		n.coordinator.validationGate.mu.Unlock()
	}
	if escalate {
		n.coordinator.parkVerticalWithMailbox(ctx, verticalID, "Vertical stuck in revision loop after repeated spec-auditor blockers.", payload)
		return
	}
	if !applied {
		n.coordinator.updateVerticalStage(ctx, verticalID, "mvp_speccing", "")
	}
	n.coordinator.publish(ctx, "spec.revision_requested", verticalID, payloadMap(n.coordinator.payloadFactory.BuildSpecRevisionRequestedPayload(ctx, verticalID, "spec-auditor", payload)))
}

func (n *ValidationOrchestrator) handleCTORevisionNeeded(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	escalate := false
	applied := false
	n.coordinator.validationGate.mu.Lock()
	st := n.coordinator.validationGate.getStateLocked(verticalID)
	st.G2Spec = false
	st.G3CTO = false
	st.SpecPayload = nil
	st.CTOPayload = nil
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	n.coordinator.validationGate.mu.Unlock()
	if _, ok := n.coordinator.applyWorkflowEventTransition(ctx, evt); ok {
		applied = true
	} else {
		n.coordinator.validationGate.mu.Lock()
		st := n.coordinator.validationGate.getStateLocked(verticalID)
		st.RevisionCount++
		if st.RevisionCount > maxRevisionCycles {
			st.Status = "parked"
			escalate = true
		}
		n.coordinator.validationGate.mu.Unlock()
	}
	if escalate {
		n.coordinator.parkVerticalWithMailbox(ctx, verticalID, "Vertical stuck in revision loop after repeated CTO revisions.", parsePayloadMap(evt.Payload))
		return
	}
	if !applied {
		n.coordinator.updateVerticalStage(ctx, verticalID, "mvp_speccing", "")
	}
	n.coordinator.publish(ctx, "spec.revision_requested", verticalID, payloadMap(n.coordinator.payloadFactory.BuildSpecRevisionRequestedPayload(ctx, verticalID, "factory-cto", parsePayloadMap(evt.Payload))))
}

func (n *ValidationOrchestrator) handleValidationRejected(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
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
	if _, ok := n.coordinator.applyWorkflowEventTransition(ctx, evt); ok && validationDeclarativeHandlerSupported(n.coordinator, string(evt.Type)) {
		return
	}
	n.coordinator.updateVerticalStage(ctx, verticalID, "killed", string(evt.Type))
	n.coordinator.publish(ctx, "vertical.killed", verticalID, payloadMap(n.coordinator.payloadFactory.BuildVerticalKilledPayload(ctx, verticalID, string(evt.Type), parsePayloadMap(evt.Payload))))
}

func (n *ValidationOrchestrator) handleValidationPackaged(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	n.coordinator.validationGate.mu.Lock()
	st := n.coordinator.validationGate.getStateLocked(verticalID)
	st.Status = "packaged"
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	n.coordinator.validationGate.mu.Unlock()
	if _, ok := n.coordinator.applyWorkflowEventTransition(ctx, evt); !ok {
		n.coordinator.updateVerticalStage(ctx, verticalID, "ready_for_review", "")
	}
}

func (n *ValidationOrchestrator) handleValidationMoreData(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	n.coordinator.validationGate.mu.Lock()
	st := n.coordinator.validationGate.getStateLocked(verticalID)
	st.Status = "active"
	st.G1Research = false
	st.ResearchPayload = nil
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	snap := validationContextSnapshot{
		Research: parsePayloadMap(st.ResearchPayload),
		Spec:     parsePayloadMap(st.SpecPayload),
		Scoring:  parsePayloadMap(st.ScoringPayload),
	}
	n.coordinator.validationGate.mu.Unlock()
	if _, ok := n.coordinator.applyWorkflowEventTransition(ctx, evt); ok {
		return
	}
	n.coordinator.updateVerticalStage(ctx, verticalID, "researching", "")
	request := parsePayloadMap(evt.Payload)
	focusAreas := []string{}
	if question := strings.TrimSpace(asString(request["reason"])); question != "" {
		focusAreas = append(focusAreas, question)
	}
	if question := strings.TrimSpace(asString(request["request"])); question != "" {
		focusAreas = append(focusAreas, question)
	}
	n.coordinator.publish(ctx, "research.additional_requested", verticalID, map[string]any{
		"entity_id":    verticalID,
		"requested_by": firstNonEmptyString(strings.TrimSpace(evt.SourceAgent), "human"),
		"focus_areas":  focusAreas,
		"request":      request,
		"research":     snap.Research,
		"spec":         snap.Spec,
		"scoring":      snap.Scoring,
	})
}

func (n *ValidationOrchestrator) handleBrandRevision(ctx context.Context, evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	if isRuntimeWorkflowSource(evt.SourceAgent) {
		return
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	feedback := parsePayloadMap(evt.Payload)
	n.coordinator.validationGate.mu.Lock()
	st := n.coordinator.validationGate.getStateLocked(verticalID)
	brand := parsePayloadMap(st.BrandPayload)
	st.Status = "active"
	st.G4Brand = false
	st.BrandPayload = nil
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	n.coordinator.validationGate.mu.Unlock()
	n.coordinator.updateVerticalStage(ctx, verticalID, "branding", "")
	n.coordinator.publish(ctx, "brand.revision_needed", verticalID, payloadMap(n.coordinator.payloadFactory.BuildBrandRevisionNeededPayload(ctx, verticalID, feedback, brand)))
}

func (n *ValidationOrchestrator) handleSpecRevisionRequested(evt events.Event) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	n.coordinator.validationGate.mu.Lock()
	defer n.coordinator.validationGate.mu.Unlock()
	st := n.coordinator.validationGate.getStateLocked(verticalID)
	st.InnerRevisionCount = 0
}

func (n *ValidationOrchestrator) handleInnerSpecRevision(ctx context.Context, evt events.Event) bool {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return false
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return false
	}
	escalate := false
	n.coordinator.validationGate.mu.Lock()
	st := n.coordinator.validationGate.getStateLocked(verticalID)
	if st.Status != "active" {
		n.coordinator.validationGate.mu.Unlock()
		return false
	}
	st.InnerRevisionCount++
	if st.InnerRevisionCount > maxInnerRevisions {
		st.Status = "parked"
		escalate = true
	}
	n.coordinator.validationGate.mu.Unlock()
	if escalate {
		n.coordinator.parkVerticalWithMailbox(ctx, verticalID, "Spec creation stuck in revision loop after 5 cycles.", parsePayloadMap(evt.Payload))
	}
	return escalate
}

func (n *ValidationOrchestrator) checkPackagingTimeouts(ctx context.Context, now time.Time) {
	if n == nil || n.coordinator == nil || n.coordinator.validationGate == nil {
		return
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	type timedOut struct {
		verticalID string
		retry      bool
		snapshot   validationContextSnapshot
	}
	expired := make([]timedOut, 0, 4)
	n.coordinator.validationGate.mu.Lock()
	for _, st := range n.coordinator.validationGate.states {
		if st == nil || st.Status != "active" || st.PackagingRequestedAt == nil {
			continue
		}
		if now.Before(st.PackagingRequestedAt.Add(packagingTimeout)) {
			continue
		}
		if st.PackagingRetries == 0 {
			st.PackagingRetries = 1
			retryAt := now
			st.PackagingRequestedAt = &retryAt
			expired = append(expired, timedOut{
				verticalID: st.VerticalID,
				retry:      true,
				snapshot: validationContextSnapshot{
					Research:    parsePayloadMap(st.ResearchPayload),
					Spec:        parsePayloadMap(st.SpecPayload),
					CTONotes:    parsePayloadMap(st.CTOPayload),
					Brand:       parsePayloadMap(st.BrandPayload),
					Scoring:     parsePayloadMap(st.ScoringPayload),
					SpecVersion: st.SpecVersion,
				},
			})
			continue
		}
		st.Status = "parked"
		st.PackagingRequested = false
		st.PackagingRequestedAt = nil
		expired = append(expired, timedOut{verticalID: st.VerticalID, retry: false})
	}
	n.coordinator.validationGate.mu.Unlock()

	for _, item := range expired {
		if item.retry {
			n.coordinator.publish(ctx, "validation.package_ready", item.verticalID, payloadMap(n.coordinator.payloadFactory.BuildValidationPackageReadyPayload(ctx, item.verticalID, item.snapshot)))
			continue
		}
		n.coordinator.parkVerticalWithMailbox(ctx, item.verticalID, "Validation packaging timed out after retry. Human intervention required.", map[string]any{"vertical_id": item.verticalID})
	}
}
