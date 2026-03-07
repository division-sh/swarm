package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	runtimetestkit "empireai/internal/runtime/testkit"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func seedValidationFlowUntilCTORequest(t *testing.T, ctx context.Context, bus *EventBus, verticalID string, ch <-chan events.Event) int {
	t.Helper()
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.shortlisted"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"composite_score": 85}),
		CreatedAt:   time.Now().UTC(),
	})
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("research.completed"),
		SourceAgent: "business-research-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"business_brief": map[string]any{"pain": "manual workflows"}}),
		CreatedAt:   time.Now().UTC(),
	})
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec.approved"),
		SourceAgent: "business-research-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"spec": map[string]any{"core_workflow": "capture > schedule > invoice"}}),
		CreatedAt:   time.Now().UTC(),
	})
	specValidation := waitForEventType(t, ch, "spec.validation_requested")
	specPayload := parsePayloadMap(specValidation.Payload)
	specVersion := asInt(specPayload["spec_version"])
	if specVersion == 0 {
		specVersion = 1
	}
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec.validation_passed"),
		SourceAgent: "spec-auditor",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"spec_version": specVersion, "status": "passed"}),
		CreatedAt:   time.Now().UTC(),
	})
	waitForEventType(t, ch, "cto.spec_review_requested")
	return specVersion
}

func TestHoldingFlow_C6_CTOApprovedSetsGate3(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)
	verticalID := uuid.NewString()
	insertTestVertical(t, db, verticalID, "Field Service Ops", "argentina")
	ch := bus.Subscribe("watch-c6", events.EventType("spec.validation_requested"), events.EventType("cto.spec_review_requested"))

	specVersion := seedValidationFlowUntilCTORequest(t, ctx, bus, verticalID, ch)
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("cto.spec_approved"),
		SourceAgent: "factory-cto",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"spec_version": specVersion, "feasibility_notes": "approved"}),
		CreatedAt:   time.Now().UTC(),
	})

	pc.mu.Lock()
	st := pc.validations[verticalID]
	pc.mu.Unlock()
	if st == nil || !st.G3CTO {
		t.Fatalf("expected G3 true after CTO approval, got %+v", st)
	}
}

func TestHoldingFlow_C7_BrandCandidatesSetGate4(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)
	verticalID := uuid.NewString()
	insertTestVertical(t, db, verticalID, "Inventory Ops", "paraguay")

	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.shortlisted"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"composite_score": 83}),
		CreatedAt:   time.Now().UTC(),
	})
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("brand.candidates_ready"),
		SourceAgent: "pre-brand-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"candidates": []string{"OpsFlow", "StockPilot"}}),
		CreatedAt:   time.Now().UTC(),
	})

	pc.mu.Lock()
	st := pc.validations[verticalID]
	pc.mu.Unlock()
	if st == nil || !st.G4Brand {
		t.Fatalf("expected G4 true after brand candidates, got %+v", st)
	}
}

func TestHoldingFlow_C8_AllGatesMetEmitValidationPackageReady(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)
	verticalID := uuid.NewString()
	insertTestVertical(t, db, verticalID, "Clinic Billing SaaS", "argentina")
	ch := bus.Subscribe("watch-c8",
		events.EventType("spec.validation_requested"),
		events.EventType("cto.spec_review_requested"),
		events.EventType("validation.package_ready"),
	)

	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.shortlisted"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"composite_score": 88}),
		CreatedAt:   time.Now().UTC(),
	})
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("brand.candidates_ready"),
		SourceAgent: "pre-brand-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"candidates": []string{"Cliniko", "Schedulink"}}),
		CreatedAt:   time.Now().UTC(),
	})
	specVersion := seedValidationFlowUntilCTORequest(t, ctx, bus, verticalID, ch)
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("cto.spec_approved"),
		SourceAgent: "factory-cto",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"spec_version": specVersion, "feasibility_notes": "good"}),
		CreatedAt:   time.Now().UTC(),
	})

	pkg := waitForEventType(t, ch, "validation.package_ready")
	payload := parsePayloadMap(pkg.Payload)
	for _, key := range []string{"research", "spec", "cto_notes", "brand"} {
		if _, ok := payload[key]; !ok {
			t.Fatalf("expected validation.package_ready payload to include %s: %v", key, payload)
		}
	}
}

func TestHoldingFlow_C9_ValidationCoordinatorPackagingCreatesMailbox(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)
	verticalID := uuid.NewString()
	insertTestVertical(t, db, verticalID, "Patient Followup Ops", "argentina")
	vcCh := bus.Subscribe("validation-coordinator", events.EventType("validation.package_ready"))
	readyCh := bus.Subscribe("watch-c9", events.EventType("vertical.ready_for_review"))

	go func() {
		evt := waitForEventType(t, vcCh, "validation.package_ready")
		payload := parsePayloadMap(evt.Payload)
		summary := "Vertical package ready"
		if geography := strings.TrimSpace(asString(payload["geography"])); geography != "" {
			summary = "Vertical package ready for " + geography
		}
		_, _ = db.ExecContext(ctx, `
			INSERT INTO mailbox (id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
			VALUES ($1::uuid, $2::uuid, 'validation-coordinator', 'vertical_approval', 'normal', 'pending', $3::jsonb, $4, now())
		`, uuid.NewString(), evt.VerticalID, string(evt.Payload), summary)
		_ = bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("vertical.ready_for_review"),
			SourceAgent: "validation-coordinator",
			VerticalID:  evt.VerticalID,
			Payload:     mustJSON(map[string]any{"mailbox_summary": summary}),
			CreatedAt:   time.Now().UTC(),
		})
	}()

	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("validation.package_ready"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id":  verticalID,
			"geography":    "argentina",
			"research":     map[string]any{"brief": "ok"},
			"spec":         map[string]any{"mvp": "ok"},
			"cto_notes":    map[string]any{"approved": true},
			"brand":        map[string]any{"candidates": []string{"A", "B"}},
			"spec_version": 1,
		}),
		CreatedAt: time.Now().UTC(),
	})

	waitForEventType(t, readyCh, "vertical.ready_for_review")
	var pending int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM mailbox
		WHERE vertical_id = $1::uuid AND type = 'vertical_approval' AND status = 'pending'
	`, verticalID).Scan(&pending); err != nil {
		t.Fatalf("query mailbox: %v", err)
	}
	if pending != 1 {
		t.Fatalf("expected one pending vertical_approval mailbox item, got %d", pending)
	}
}

func TestHoldingFlow_C10_HumanApprovalCanLeadToSpinupRequest(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	ecCh := bus.Subscribe("empire-coordinator", events.EventType("vertical.approved"))
	spinupCh := bus.Subscribe("watch-c10", events.EventType("opco.spinup_requested"))
	verticalID := uuid.NewString()

	go func() {
		evt := waitForEventType(t, ecCh, "vertical.approved")
		_ = bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("opco.spinup_requested"),
			SourceAgent: "empire-coordinator",
			VerticalID:  evt.VerticalID,
			Payload:     mustJSON(map[string]any{"vertical_id": evt.VerticalID, "trigger": "human_approved"}),
			CreatedAt:   time.Now().UTC(),
		})
	}()

	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.approved"),
		SourceAgent: "human",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"decision": "approve"}),
		CreatedAt:   time.Now().UTC(),
	})
	waitForEventType(t, spinupCh, "opco.spinup_requested")
}

func TestHoldingFlow_D1_CTORevisionNeededResetsSpecAndLoops(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)
	verticalID := uuid.NewString()
	insertTestVertical(t, db, verticalID, "Legal Ops SaaS", "argentina")
	ch := bus.Subscribe("watch-d1",
		events.EventType("spec.validation_requested"),
		events.EventType("cto.spec_review_requested"),
		events.EventType("spec.revision_requested"),
	)

	specVersion := seedValidationFlowUntilCTORequest(t, ctx, bus, verticalID, ch)
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("cto.spec_revision_needed"),
		SourceAgent: "factory-cto",
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"spec_version": specVersion,
			"issues":       []string{"missing tenancy model"},
		}),
		CreatedAt: time.Now().UTC(),
	})
	waitForEventType(t, ch, "spec.revision_requested")

	pc.mu.Lock()
	st := pc.validations[verticalID]
	pc.mu.Unlock()
	if st == nil || st.G2Spec || st.G3CTO {
		t.Fatalf("expected G2/G3 reset after CTO revision, got %+v", st)
	}
	if st.RevisionCount != 1 {
		t.Fatalf("expected RevisionCount=1, got %d", st.RevisionCount)
	}
}

func TestHoldingFlow_D2_MaxCTORevisionsParkPipelineAndEscalateMailbox(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)
	verticalID := uuid.NewString()
	insertTestVertical(t, db, verticalID, "Salon Ops", "paraguay")
	ch := bus.Subscribe("watch-d2", events.EventType("spec.revision_requested"))

	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.shortlisted"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"composite_score": 77}),
		CreatedAt:   time.Now().UTC(),
	})
	for i := 0; i < 4; i++ {
		_ = bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("cto.spec_revision_needed"),
			SourceAgent: "factory-cto",
			VerticalID:  verticalID,
			Payload:     mustJSON(map[string]any{"cycle": i + 1}),
			CreatedAt:   time.Now().UTC(),
		})
	}

	timer := time.NewTimer(700 * time.Millisecond)
	defer timer.Stop()
	revisionEvents := 0
loop:
	for {
		select {
		case <-ch:
			revisionEvents++
		case <-timer.C:
			break loop
		}
	}
	if revisionEvents != 3 {
		t.Fatalf("expected 3 revision requests before park, got %d", revisionEvents)
	}

	pc.mu.Lock()
	st := pc.validations[verticalID]
	pc.mu.Unlock()
	if st == nil || strings.TrimSpace(st.Status) != "parked" {
		t.Fatalf("expected parked status after max revisions, got %+v", st)
	}
	var pending int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM mailbox
		WHERE vertical_id = $1::uuid AND type = 'vertical_approval' AND status = 'pending'
	`, verticalID).Scan(&pending); err != nil {
		t.Fatalf("query mailbox: %v", err)
	}
	if pending == 0 {
		t.Fatal("expected mailbox escalation after max revision cycles")
	}
}

func TestHoldingFlow_D3_SpecReviewerIssuesIncrementInnerRevisionCount(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)
	verticalID := uuid.NewString()
	insertTestVertical(t, db, verticalID, "Dental Claims Ops", "argentina")

	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.shortlisted"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"composite_score": 79}),
		CreatedAt:   time.Now().UTC(),
	})
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec.revision_needed"),
		SourceAgent: "business-research-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"issues": []string{"scope too broad"}}),
		CreatedAt:   time.Now().UTC(),
	})

	pc.mu.Lock()
	st := pc.validations[verticalID]
	pc.mu.Unlock()
	if st == nil || st.InnerRevisionCount != 1 || strings.TrimSpace(st.Status) != "active" {
		t.Fatalf("expected active state with inner revision count=1, got %+v", st)
	}
}

func TestHoldingFlow_D4_ResearchRejectedKillsPipeline(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)
	verticalID := uuid.NewString()
	insertTestVertical(t, db, verticalID, "Auto Shop Ops", "paraguay")
	ch := bus.Subscribe("watch-d4", events.EventType("vertical.killed"))

	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.shortlisted"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"composite_score": 76}),
		CreatedAt:   time.Now().UTC(),
	})
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("research.vertical_rejected"),
		SourceAgent: "business-research-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"reason": "unit economics fail"}),
		CreatedAt:   time.Now().UTC(),
	})
	waitForEventType(t, ch, "vertical.killed")

	pc.mu.Lock()
	st := pc.validations[verticalID]
	pc.mu.Unlock()
	if st == nil || strings.TrimSpace(st.Status) != "rejected" {
		t.Fatalf("expected rejected status, got %+v", st)
	}
}

func TestHoldingFlow_E1_StaleSpecApprovedDroppedAfterRejection(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)
	verticalID := uuid.NewString()
	insertTestVertical(t, db, verticalID, "Retail Ops", "argentina")
	specValidationCh := bus.Subscribe("watch-e1", events.EventType("spec.validation_requested"))

	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.shortlisted"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"composite_score": 80}),
		CreatedAt:   time.Now().UTC(),
	})
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("research.vertical_rejected"),
		SourceAgent: "business-research-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"reason": "market too small"}),
		CreatedAt:   time.Now().UTC(),
	})
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("spec.approved"),
		SourceAgent: "business-research-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"spec": map[string]any{"v": 1}}),
		CreatedAt:   time.Now().UTC(),
	})
	assertNoEventType(t, specValidationCh, "spec.validation_requested", 300*time.Millisecond)

	reason := pc.interceptStateDropReason("spec.approved", events.Event{VerticalID: verticalID})
	if strings.TrimSpace(reason) != "status=rejected, expected=active" {
		t.Fatalf("expected stale-drop reason, got %q", reason)
	}
}

func TestHoldingFlow_E2_DuplicateShortlistedDropped(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)
	verticalID := uuid.NewString()
	insertTestVertical(t, db, verticalID, "Logistics Ops", "paraguay")
	ch := bus.Subscribe("watch-e2", events.EventType("validation.started"))

	for i := 0; i < 2; i++ {
		_ = bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("vertical.shortlisted"),
			SourceAgent: "pipeline-coordinator",
			VerticalID:  verticalID,
			Payload:     mustJSON(map[string]any{"composite_score": 81}),
			CreatedAt:   time.Now().UTC(),
		})
	}
	waitForEventType(t, ch, "validation.started")
	assertNoEventType(t, ch, "validation.started", 300*time.Millisecond)
	if got := pc.interceptStateDropReason("vertical.shortlisted", events.Event{VerticalID: verticalID}); got != "pipeline already exists" {
		t.Fatalf("expected duplicate shortlist drop reason, got %q", got)
	}
}

func TestHoldingFlow_E3_BrandRevisionResetsG4AndCanBeRegenerated(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)
	verticalID := uuid.NewString()
	insertTestVertical(t, db, verticalID, "Pharmacy Ops", "argentina")
	ch := bus.Subscribe("watch-e3",
		events.EventType("spec.validation_requested"),
		events.EventType("cto.spec_review_requested"),
		events.EventType("validation.package_ready"),
		events.EventType("brand.revision_needed"),
	)

	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.shortlisted"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"composite_score": 87}),
		CreatedAt:   time.Now().UTC(),
	})
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("research.completed"),
		SourceAgent: "business-research-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"brief": "ok"}),
		CreatedAt:   time.Now().UTC(),
	})
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("brand.candidates_ready"),
		SourceAgent: "pre-brand-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"candidates": []string{"One", "Two"}}),
		CreatedAt:   time.Now().UTC(),
	})
	specVersion := seedValidationFlowUntilCTORequest(t, ctx, bus, verticalID, ch)
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("cto.spec_approved"),
		SourceAgent: "factory-cto",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"spec_version": specVersion}),
		CreatedAt:   time.Now().UTC(),
	})
	waitForEventType(t, ch, "validation.package_ready")

	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("brand.revision_needed"),
		SourceAgent: "human",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"reason": "conflict with trademark"}),
		CreatedAt:   time.Now().UTC(),
	})
	waitForEventType(t, ch, "brand.revision_needed")
	pc.mu.Lock()
	st := pc.validations[verticalID]
	g4 := st != nil && st.G4Brand
	pc.mu.Unlock()
	if g4 {
		t.Fatal("expected G4 reset to false after brand revision")
	}

	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("brand.candidates_ready"),
		SourceAgent: "pre-brand-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"candidates": []string{"Three", "Four"}}),
		CreatedAt:   time.Now().UTC(),
	})
	pc.mu.Lock()
	g4 = pc.validations[verticalID].G4Brand
	pc.mu.Unlock()
	if !g4 {
		t.Fatal("expected G4 true after regenerated brand candidates")
	}
}

func TestHoldingFlow_E4_NeedsMoreDataResetsG1ThenAllowsReseal(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)
	verticalID := uuid.NewString()
	insertTestVertical(t, db, verticalID, "Waste Mgmt Ops", "paraguay")
	ch := bus.Subscribe("watch-e4", events.EventType("validation.more_data_needed"))

	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.shortlisted"),
		SourceAgent: "pipeline-coordinator",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"composite_score": 78}),
		CreatedAt:   time.Now().UTC(),
	})
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("research.completed"),
		SourceAgent: "business-research-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"brief": "ok"}),
		CreatedAt:   time.Now().UTC(),
	})
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.needs_more_data"),
		SourceAgent: "human",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"request": "more TAM evidence"}),
		CreatedAt:   time.Now().UTC(),
	})
	waitForEventType(t, ch, "validation.more_data_needed")
	pc.mu.Lock()
	g1 := pc.validations[verticalID].G1Research
	pc.mu.Unlock()
	if g1 {
		t.Fatal("expected G1 reset after vertical.needs_more_data")
	}
	_ = bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("research.completed"),
		SourceAgent: "business-research-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"brief": "updated"}),
		CreatedAt:   time.Now().UTC(),
	})
	pc.mu.Lock()
	g1 = pc.validations[verticalID].G1Research
	pc.mu.Unlock()
	if !g1 {
		t.Fatal("expected G1 re-set after follow-up research.completed")
	}
}

func TestHoldingFlow_E5_ScanTimeoutEmitsTimedOutCompletion(t *testing.T) {
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	ch := bus.Subscribe("watch-e5", events.EventType("scan.completed"))

	pc.handleScanRequested(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("scan.requested"),
		Payload: mustJSON(map[string]any{
			"scan_id":     "scan-timeout-e5",
			"campaign_id": "campaign-e5",
			"mode":        "saas_gap",
			"geography":   "argentina",
		}),
	})
	pc.mu.Lock()
	acc := pc.scans["scan-timeout-e5"]
	acc.Reports = 3
	acc.Discovered = 1
	acc.Skipped = 2
	acc.CreatedAt = time.Now().UTC().Add(-(scanTimeout + time.Minute))
	pc.mu.Unlock()

	pc.checkScanTimeouts(ctx, time.Now().UTC())
	evt := waitForEventType(t, ch, "scan.completed")
	payload := parsePayloadMap(evt.Payload)
	if !strings.EqualFold(strings.TrimSpace(asString(payload["timed_out"])), "true") {
		t.Fatalf("expected timed_out=true payload, got %v", payload)
	}
	if strings.TrimSpace(asString(payload["scan_id"])) != "scan-timeout-e5" {
		t.Fatalf("expected timeout event for scan-timeout-e5, got %v", payload["scan_id"])
	}
	if got := len(pc.SnapshotScans()); got != 0 {
		t.Fatalf("expected timed-out scan removed from active state, got %d", got)
	}
}

func TestHoldingFlow_GoldenPath_DirectiveToMailbox_WithStubAgents(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)
	store := &holdingFlowCampaignStore{db: db}
	manager := NewScanCampaignManager(bus, store, ScanCampaignHooks{}, db)

	errCh := make(chan error, 8)

	marketCh := bus.Subscribe("market-research-agent", events.EventType("market_research.scan_assigned"))
	go func() {
		evt := runtimetestkit.WaitForEventTypes(t, marketCh, []string{"market_research.scan_assigned"}, 5*time.Second)["market_research.scan_assigned"]
		payload := parsePayloadMap(evt.Payload)
		scanID := strings.TrimSpace(asString(payload["scan_id"]))
		campaignID := strings.TrimSpace(asString(payload["campaign_id"]))
		geography := strings.TrimSpace(asString(payload["geography"]))
		if scanID == "" {
			errCh <- context.DeadlineExceeded
			return
		}
		_ = bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("category.assessed"),
			SourceAgent: "market-research-agent",
			Payload: mustJSON(map[string]any{
				"scan_id":         scanID,
				"campaign_id":     campaignID,
				"vertical_name":   "Pet Grooming Operations",
				"signal_strength": 82,
				"geography":       geography,
				"mode":            "saas_gap",
			}),
			CreatedAt: time.Now().UTC(),
		})
		_ = bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("market_research.scan_complete"),
			SourceAgent: "market-research-agent",
			Payload: mustJSON(map[string]any{
				"scan_id":     scanID,
				"campaign_id": campaignID,
			}),
			CreatedAt: time.Now().UTC(),
		})
		errCh <- nil
	}()

	scoringCh := bus.Subscribe("pipeline-coordinator", events.EventType("vertical.discovered"))
	go func() {
		evt := runtimetestkit.WaitForEventTypes(t, scoringCh, []string{"vertical.discovered"}, 5*time.Second)["vertical.discovered"]
		payload := parsePayloadMap(evt.Payload)
		_ = bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("vertical.scored"),
			SourceAgent: "pipeline-coordinator",
			VerticalID:  evt.VerticalID,
			Payload: mustJSON(map[string]any{
				"vertical_id":       evt.VerticalID,
				"name":              payload["name"],
				"geography":         payload["geography"],
				"composite_score":   81.25,
				"viability_score":   72,
				"result":            "shortlisted",
				"scoring_breakdown": map[string]any{"market": 80, "distribution": 82},
			}),
			CreatedAt: time.Now().UTC(),
		})
		_ = bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("vertical.shortlisted"),
			SourceAgent: "pipeline-coordinator",
			VerticalID:  evt.VerticalID,
			Payload: mustJSON(map[string]any{
				"vertical_id":       evt.VerticalID,
				"name":              payload["name"],
				"geography":         payload["geography"],
				"composite_score":   81.25,
				"viability_score":   72,
				"scoring_breakdown": map[string]any{"market": 80, "distribution": 82},
			}),
			CreatedAt: time.Now().UTC(),
		})
		errCh <- nil
	}()

	braCh := bus.Subscribe("business-research-agent", events.EventType("validation.started"))
	go func() {
		evt := runtimetestkit.WaitForEventTypes(t, braCh, []string{"validation.started"}, 5*time.Second)["validation.started"]
		_ = bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("research.completed"),
			SourceAgent: "business-research-agent",
			VerticalID:  evt.VerticalID,
			Payload: mustJSON(map[string]any{
				"business_brief": map[string]any{
					"problem":      "manual scheduling",
					"users":        "SMB services",
					"monetization": "subscription",
				},
			}),
			CreatedAt: time.Now().UTC(),
		})
		_ = bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("spec.requested"),
			SourceAgent: "business-research-agent",
			VerticalID:  evt.VerticalID,
			Payload: mustJSON(map[string]any{
				"business_brief": map[string]any{"problem": "manual scheduling"},
			}),
			CreatedAt: time.Now().UTC(),
		})
		errCh <- nil
	}()

	lsaCh := bus.Subscribe("lightweight-spec-agent", events.EventType("spec.requested"))
	go func() {
		evt := runtimetestkit.WaitForEventTypes(t, lsaCh, []string{"spec.requested"}, 5*time.Second)["spec.requested"]
		_ = bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("spec.approved"),
			SourceAgent: "business-research-agent",
			VerticalID:  evt.VerticalID,
			Payload: mustJSON(map[string]any{
				"spec": map[string]any{
					"core_workflow": "capture lead > schedule > invoice",
					"features":      []string{"intake", "calendar", "billing"},
				},
			}),
			CreatedAt: time.Now().UTC(),
		})
		errCh <- nil
	}()

	auditorCh := bus.Subscribe("spec-auditor", events.EventType("spec.validation_requested"))
	go func() {
		evt := runtimetestkit.WaitForEventTypes(t, auditorCh, []string{"spec.validation_requested"}, 5*time.Second)["spec.validation_requested"]
		payload := parsePayloadMap(evt.Payload)
		_ = bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("spec.validation_passed"),
			SourceAgent: "spec-auditor",
			VerticalID:  evt.VerticalID,
			Payload: mustJSON(map[string]any{
				"spec_version": asInt(payload["spec_version"]),
				"status":       "passed",
			}),
			CreatedAt: time.Now().UTC(),
		})
		errCh <- nil
	}()

	ctoCh := bus.Subscribe("factory-cto", events.EventType("cto.spec_review_requested"))
	go func() {
		evt := runtimetestkit.WaitForEventTypes(t, ctoCh, []string{"cto.spec_review_requested"}, 5*time.Second)["cto.spec_review_requested"]
		payload := parsePayloadMap(evt.Payload)
		_ = bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("cto.spec_approved"),
			SourceAgent: "factory-cto",
			VerticalID:  evt.VerticalID,
			Payload: mustJSON(map[string]any{
				"spec_version": asInt(payload["spec_version"]),
				"notes":        "feasible",
			}),
			CreatedAt: time.Now().UTC(),
		})
		errCh <- nil
	}()

	brandCh := bus.Subscribe("pre-brand-agent", events.EventType("brand.requested"))
	go func() {
		evt := runtimetestkit.WaitForEventTypes(t, brandCh, []string{"brand.requested"}, 5*time.Second)["brand.requested"]
		_ = bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("brand.candidates_ready"),
			SourceAgent: "pre-brand-agent",
			VerticalID:  evt.VerticalID,
			Payload:     mustJSON(map[string]any{"candidates": []string{"SchedPilot", "OpsFlow"}}),
			CreatedAt:   time.Now().UTC(),
		})
		errCh <- nil
	}()

	vcCh := bus.Subscribe("validation-coordinator", events.EventType("validation.package_ready"))
	go func() {
		evt := runtimetestkit.WaitForEventTypes(t, vcCh, []string{"validation.package_ready"}, 5*time.Second)["validation.package_ready"]
		payload := parsePayloadMap(evt.Payload)
		summary := "Ready for review"
		if g := strings.TrimSpace(asString(payload["geography"])); g != "" {
			summary = "Ready for review: " + g
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO mailbox (id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
			VALUES ($1::uuid, $2::uuid, 'validation-coordinator', 'vertical_approval', 'normal', 'pending', $3::jsonb, $4, now())
		`, uuid.NewString(), evt.VerticalID, string(evt.Payload), summary); err != nil {
			errCh <- err
			return
		}
		_ = bus.Publish(ctx, events.Event{
			ID:          uuid.NewString(),
			Type:        events.EventType("vertical.ready_for_review"),
			SourceAgent: "validation-coordinator",
			VerticalID:  evt.VerticalID,
			Payload:     mustJSON(map[string]any{"summary": summary}),
			CreatedAt:   time.Now().UTC(),
		})
		errCh <- nil
	}()

	manager.onEvent(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload: mustJSON(map[string]any{
			"directive_text": "SaaS in Argentina",
			"sent_by":        "dashboard",
		}),
		CreatedAt: time.Now().UTC(),
	})

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var pending int
		err := db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM mailbox
			WHERE type = 'vertical_approval' AND status = 'pending'
		`).Scan(&pending)
		if err == nil && pending > 0 {
			for i := 0; i < 8; i++ {
				select {
				case e := <-errCh:
					if e != nil {
						t.Fatalf("stub pipeline error: %v", e)
					}
				default:
				}
			}
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Fatal("timed out waiting for golden-path mailbox output")
}
