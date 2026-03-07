package pipeline

import (
	"context"
	"strings"
	"time"

	"empireai/internal/events"
)

func (vg *ValidationGate) handleValidationStarted(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	scoringPayload := parsePayloadMap(evt.Payload)
	vg.mu.Lock()
	st := vg.states[verticalID]
	if st == nil {
		st = &validationPipelineState{VerticalID: verticalID, Status: "active"}
		vg.states[verticalID] = st
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
	vg.mu.Unlock()

	vg.runtime.updateVerticalStage(ctx, verticalID, "researching", "")
	validationPayload := vg.payloadFactory.BuildValidationStartedPayload(ctx, verticalID, scoringPayload, nil)
	vg.runtime.publish(ctx, "validation.started", verticalID, payloadMap(validationPayload))
	brandPayload := vg.payloadFactory.BuildBrandRequestedPayload(ctx, verticalID, scoringPayload, nil)
	vg.runtime.publish(ctx, "brand.requested", verticalID, payloadMap(brandPayload))
}

func (vg *ValidationGate) handleValidationGate(ctx context.Context, evt events.Event, gate string) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	payload := parsePayloadMap(evt.Payload)

	vg.mu.Lock()
	st := vg.getStateLocked(verticalID)
	if st.Status == "rejected" || st.Status == "packaged" {
		vg.mu.Unlock()
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
	stage := vg.stageForState(st)
	var bundle ValidationPackageReadyPayload
	hasBundle := false
	if shouldPackage {
		now := time.Now().UTC()
		st.PackagingRequestedAt = &now
		st.PackagingRetries = 0
		st.PackagingRequested = true
		hasBundle = true
		bundle = vg.payloadFactory.BuildValidationPackageReadyPayload(ctx, verticalID, validationContextSnapshot{
			Research:    parsePayloadMap(st.ResearchPayload),
			Spec:        parsePayloadMap(st.SpecPayload),
			CTONotes:    parsePayloadMap(st.CTOPayload),
			Brand:       parsePayloadMap(st.BrandPayload),
			Scoring:     parsePayloadMap(st.ScoringPayload),
			SpecVersion: st.SpecVersion,
		})
	}
	vg.mu.Unlock()

	vg.runtime.updateVerticalStage(ctx, verticalID, stage, "")
	if gate == "g2" {
		vg.runtime.publish(ctx, "spec.validation_requested", verticalID, payloadMap(vg.payloadFactory.BuildSpecValidationRequestedPayload(ctx, verticalID, payload)))
	}
	if hasBundle {
		vg.runtime.publish(ctx, "validation.package_ready", verticalID, payloadMap(bundle))
	}
}

func (vg *ValidationGate) handleSpecValidationPassed(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	if !vg.runtime.specVersionMatches(verticalID, payload) {
		return
	}
	vg.runtime.publish(ctx, "cto.spec_review_requested", verticalID, payloadMap(vg.payloadFactory.BuildCTOSpecReviewRequestedPayload(ctx, verticalID, payload)))
}

func (vg *ValidationGate) handleSpecValidationFailed(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	if !vg.runtime.specVersionMatches(verticalID, payload) {
		return
	}
	status := strings.ToLower(strings.TrimSpace(asString(payload["status"])))
	passed := strings.EqualFold(strings.TrimSpace(asString(payload["passed"])), "true")
	if status == "non-blocker" || passed {
		vg.runtime.publish(ctx, "cto.spec_review_requested", verticalID, payloadMap(vg.payloadFactory.BuildCTOSpecReviewRequestedPayload(ctx, verticalID, payload)))
		return
	}
	escalate := false
	vg.mu.Lock()
	st := vg.getStateLocked(verticalID)
	st.G2Spec = false
	st.G3CTO = false
	st.SpecPayload = nil
	st.CTOPayload = nil
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	st.RevisionCount++
	if st.RevisionCount > maxRevisionCycles {
		st.Status = "parked"
		escalate = true
	}
	vg.mu.Unlock()
	vg.runtime.updateVerticalStage(ctx, verticalID, "mvp_speccing", "")
	if escalate {
		vg.runtime.parkVerticalWithMailbox(ctx, verticalID, "Vertical stuck in revision loop after repeated spec-auditor blockers.", payload)
		return
	}
	vg.runtime.publish(ctx, "spec.revision_requested", verticalID, payloadMap(vg.payloadFactory.BuildSpecRevisionRequestedPayload(ctx, verticalID, "spec-auditor", payload)))
}

func (vg *ValidationGate) handleCTORevisionNeeded(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	escalate := false
	vg.mu.Lock()
	st := vg.getStateLocked(verticalID)
	st.G2Spec = false
	st.G3CTO = false
	st.SpecPayload = nil
	st.CTOPayload = nil
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	st.RevisionCount++
	if st.RevisionCount > maxRevisionCycles {
		st.Status = "parked"
		escalate = true
	}
	vg.mu.Unlock()
	vg.runtime.updateVerticalStage(ctx, verticalID, "mvp_speccing", "")
	if escalate {
		vg.runtime.parkVerticalWithMailbox(ctx, verticalID, "Vertical stuck in revision loop after repeated CTO revisions.", parsePayloadMap(evt.Payload))
		return
	}
	vg.runtime.publish(ctx, "spec.revision_requested", verticalID, payloadMap(vg.payloadFactory.BuildSpecRevisionRequestedPayload(ctx, verticalID, "factory-cto", parsePayloadMap(evt.Payload))))
}

func (vg *ValidationGate) handleValidationRejected(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	vg.mu.Lock()
	st := vg.getStateLocked(verticalID)
	st.Status = "rejected"
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	vg.mu.Unlock()
	vg.runtime.updateVerticalStage(ctx, verticalID, "killed", string(evt.Type))
	vg.runtime.publish(ctx, "vertical.killed", verticalID, payloadMap(vg.payloadFactory.BuildVerticalKilledPayload(ctx, verticalID, string(evt.Type), parsePayloadMap(evt.Payload))))
}

func (vg *ValidationGate) handleValidationPackaged(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	vg.mu.Lock()
	st := vg.getStateLocked(verticalID)
	st.Status = "packaged"
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	vg.mu.Unlock()
	vg.runtime.updateVerticalStage(ctx, verticalID, "ready_for_review", "")
}

func (vg *ValidationGate) handleValidationMoreData(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	vg.mu.Lock()
	st := vg.getStateLocked(verticalID)
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
	vg.mu.Unlock()
	vg.runtime.updateVerticalStage(ctx, verticalID, "researching", "")
	vg.runtime.publish(ctx, "validation.more_data_needed", verticalID, payloadMap(vg.payloadFactory.BuildValidationMoreDataPayload(ctx, verticalID, parsePayloadMap(evt.Payload), snap)))
}

func (vg *ValidationGate) handleBrandRevision(ctx context.Context, evt events.Event) {
	if strings.TrimSpace(evt.SourceAgent) == "pipeline-coordinator" {
		return
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	feedback := parsePayloadMap(evt.Payload)
	vg.mu.Lock()
	st := vg.getStateLocked(verticalID)
	brand := parsePayloadMap(st.BrandPayload)
	st.Status = "active"
	st.G4Brand = false
	st.BrandPayload = nil
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	vg.mu.Unlock()
	vg.runtime.updateVerticalStage(ctx, verticalID, "branding", "")
	vg.runtime.publish(ctx, "brand.revision_needed", verticalID, payloadMap(vg.payloadFactory.BuildBrandRevisionNeededPayload(ctx, verticalID, feedback, brand)))
}

func (vg *ValidationGate) handleVerticalResumed(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	vg.mu.Lock()
	st := vg.getStateLocked(verticalID)
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
	stage := vg.stageForState(st)
	scoringRaw := cloneRaw(st.ScoringPayload)
	var bundle ValidationPackageReadyPayload
	hasBundle := false
	if all {
		now := time.Now().UTC()
		hasBundle = true
		bundle = vg.payloadFactory.BuildValidationPackageReadyPayload(ctx, verticalID, validationContextSnapshot{
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
	vg.mu.Unlock()
	vg.runtime.updateVerticalStage(ctx, verticalID, stage, "")

	resumePayload := parsePayloadMap(evt.Payload)
	snap := vg.payloadFactory.ValidationContext(verticalID)
	if missingG1 {
		scoringPayload := parsePayloadMap(scoringRaw)
		if len(scoringPayload) == 0 {
			scoringPayload = parsePayloadMap(evt.Payload)
		}
		vg.runtime.publish(ctx, "validation.started", verticalID, payloadMap(vg.payloadFactory.BuildValidationStartedPayload(ctx, verticalID, scoringPayload, resumePayload)))
	}
	if missingG2 {
		vg.runtime.publish(ctx, "spec.revision_requested", verticalID, payloadMap(vg.payloadFactory.BuildSpecRevisionRequestedPayload(ctx, verticalID, "resume", resumePayload)))
	}
	if missingG3 {
		vg.runtime.publish(ctx, "cto.spec_review_requested", verticalID, payloadMap(vg.payloadFactory.BuildCTOSpecReviewRequestedPayload(ctx, verticalID, resumePayload)))
	}
	if missingG4 {
		vg.runtime.publish(ctx, "brand.requested", verticalID, payloadMap(vg.payloadFactory.BuildBrandRequestedPayload(ctx, verticalID, snap.Scoring, snap.Research)))
	}
	if hasBundle {
		vg.runtime.publish(ctx, "validation.package_ready", verticalID, payloadMap(bundle))
	}
}

func (vg *ValidationGate) handleCTOApproved(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	payload := parsePayloadMap(evt.Payload)
	if !vg.runtime.specVersionMatches(verticalID, payload) {
		return
	}
	vg.handleValidationGate(ctx, evt, "g3")
}

func (vg *ValidationGate) handleVerticalApproved(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	vg.mu.Lock()
	st := vg.getStateLocked(verticalID)
	st.Status = "approved"
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	vg.mu.Unlock()
	vg.runtime.updateVerticalStage(ctx, verticalID, "approved", "")
}

func (vg *ValidationGate) handleVerticalKilled(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	vg.mu.Lock()
	st := vg.getStateLocked(verticalID)
	st.Status = "rejected"
	st.PackagingRequested = false
	st.PackagingRequestedAt = nil
	st.PackagingRetries = 0
	vg.mu.Unlock()
	vg.runtime.updateVerticalStage(ctx, verticalID, "killed", string(evt.Type))
}

func (vg *ValidationGate) handleOpCoCEOReady(ctx context.Context, evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		payload := parsePayloadMap(evt.Payload)
		verticalID = strings.TrimSpace(asString(payload["vertical_id"]))
	}
	if verticalID == "" {
		return
	}
	vg.runtime.updateVerticalStage(ctx, verticalID, "operating", "")
}

func (vg *ValidationGate) handleInnerSpecRevision(ctx context.Context, evt events.Event) bool {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return false
	}
	escalate := false
	vg.mu.Lock()
	st := vg.getStateLocked(verticalID)
	if st.Status != "active" {
		vg.mu.Unlock()
		return false
	}
	st.InnerRevisionCount++
	if st.InnerRevisionCount > maxInnerRevisions {
		st.Status = "parked"
		escalate = true
	}
	vg.mu.Unlock()
	if escalate {
		vg.runtime.parkVerticalWithMailbox(ctx, verticalID, "Spec creation stuck in revision loop after 5 cycles.", parsePayloadMap(evt.Payload))
	}
	return escalate
}

func (vg *ValidationGate) handleSpecRevisionRequested(evt events.Event) {
	verticalID := strings.TrimSpace(evt.VerticalID)
	if verticalID == "" {
		return
	}
	vg.mu.Lock()
	defer vg.mu.Unlock()
	st := vg.getStateLocked(verticalID)
	st.InnerRevisionCount = 0
}

func (pc *FactoryPipelineCoordinator) specVersionMatches(verticalID string, payload map[string]any) bool {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	st := pc.validationGate.getStateLocked(verticalID)
	if st.SpecVersion <= 0 {
		return true
	}
	got := asInt(payload["spec_version"])
	if got == 0 {
		return true
	}
	return got == st.SpecVersion
}

func (pc *FactoryPipelineCoordinator) parkVerticalWithMailbox(ctx context.Context, verticalID, summary string, details map[string]any) {
	if pc == nil || pc.db == nil {
		return
	}
	if details == nil {
		details = map[string]any{}
	}
	contextPayload := map[string]any{
		"vertical_id": verticalID,
		"source":      "pipeline-coordinator",
		"details":     details,
	}
	_, _ = dbExecContext(ctx, pc.db, `
		INSERT INTO mailbox (event_id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
		VALUES (NULL, NULLIF($1,'')::uuid, 'pipeline-coordinator', 'vertical_approval', 'high', 'pending', $2::jsonb, $3, now())
	`, strings.TrimSpace(verticalID), string(mustJSON(contextPayload)), strings.TrimSpace(summary))
	pc.updateVerticalStage(ctx, verticalID, "ready_for_review", "")
}

func (pc *FactoryPipelineCoordinator) checkPackagingTimeouts(ctx context.Context, now time.Time) {
	if pc == nil {
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
	pc.mu.Lock()
	for _, st := range pc.validationGate.states {
		if st == nil || st.Status != "active" || st.PackagingRequestedAt == nil {
			continue
		}
		if now.Before(st.PackagingRequestedAt.Add(packagingTimeout)) {
			continue
		}
		if st.PackagingRetries == 0 {
			st.PackagingRetries = 1
			n := now
			st.PackagingRequestedAt = &n
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
	pc.mu.Unlock()

	for _, item := range expired {
		if item.retry {
			pc.publish(ctx, "validation.package_ready", item.verticalID, payloadMap(pc.payloadFactory.BuildValidationPackageReadyPayload(ctx, item.verticalID, item.snapshot)))
			continue
		}
		pc.parkVerticalWithMailbox(ctx, item.verticalID, "Validation packaging timed out after retry. Human intervention required.", map[string]any{"vertical_id": item.verticalID})
	}
}
