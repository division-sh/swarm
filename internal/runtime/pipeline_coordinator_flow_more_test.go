package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func waitForEventTypes(t *testing.T, ch <-chan events.Event, expected []string) map[string]events.Event {
	t.Helper()
	need := make(map[string]struct{}, len(expected))
	for _, typ := range expected {
		need[typ] = struct{}{}
	}
	got := make(map[string]events.Event, len(expected))
	deadline := time.After(1500 * time.Millisecond)
	for len(got) < len(expected) {
		select {
		case evt := <-ch:
			typ := string(evt.Type)
			if _, ok := need[typ]; ok {
				if _, seen := got[typ]; !seen {
					got[typ] = evt
				}
			}
		case <-deadline:
			t.Fatalf("timed out waiting for events; got=%v expected=%v", keysFromEventMap(got), expected)
		}
	}
	return got
}

func keysFromEventMap(m map[string]events.Event) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestFactoryPipelineCoordinator_ValidationLifecycleHappyPath(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	ctx := context.Background()
	verticalID := uuid.NewString()

	ch := bus.Subscribe("watch-validation-lifecycle",
		events.EventType("validation.started"),
		events.EventType("brand.requested"),
		events.EventType("spec.validation_requested"),
		events.EventType("cto.spec_review_requested"),
		events.EventType("validation.package_ready"),
	)

	pc.handleValidationStarted(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.shortlisted"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"composite_score": 82}),
	})
	waitForEventTypes(t, ch, []string{"validation.started", "brand.requested"})

	pc.handleValidationGate(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("research.completed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"brief": "ok"}),
	}, "g1")

	pc.handleValidationGate(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.approved"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"spec": "draft"}),
	}, "g2")
	waitForEventTypes(t, ch, []string{"spec.validation_requested"})

	pc.handleSpecValidationPassed(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.validation_passed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"spec_version": 1, "status": "passed"}),
	})
	waitForEventTypes(t, ch, []string{"cto.spec_review_requested"})

	pc.handleCTOApproved(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("cto.spec_approved"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"spec_version": 1, "notes": "looks good"}),
	})
	pc.handleValidationGate(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("brand.candidates_ready"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"names": []string{"a", "b"}}),
	}, "g4")

	got := waitForEventTypes(t, ch, []string{"validation.package_ready"})
	pkg := got["validation.package_ready"]
	var payload map[string]any
	if err := json.Unmarshal(pkg.Payload, &payload); err != nil {
		t.Fatalf("decode package payload: %v", err)
	}
	if asInt(payload["spec_version"]) != 1 {
		t.Fatalf("expected packaged spec_version=1, got %#v", payload["spec_version"])
	}
}

func TestFactoryPipelineCoordinator_RevisionAndResumePaths(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	ctx := context.Background()
	verticalID := uuid.NewString()

	ch := bus.Subscribe("watch-validation-revision",
		events.EventType("spec.revision_requested"),
		events.EventType("validation.more_data_needed"),
		events.EventType("brand.revision_needed"),
		events.EventType("validation.started"),
		events.EventType("cto.spec_review_requested"),
		events.EventType("brand.requested"),
		events.EventType("vertical.killed"),
	)

	pc.handleValidationStarted(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.shortlisted"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"composite_score": 79}),
	})
	pc.handleValidationGate(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("research.completed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"brief": "ok"}),
	}, "g1")
	pc.handleValidationGate(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.approved"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"spec": "v1"}),
	}, "g2")

	pc.handleSpecValidationFailed(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.validation_failed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"spec_version": 1, "status": "blocker"}),
	})
	waitForEventTypes(t, ch, []string{"spec.revision_requested"})

	pc.handleValidationMoreData(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.needs_more_data"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"reason": "need more evidence"}),
	})
	waitForEventTypes(t, ch, []string{"validation.more_data_needed"})

	pc.handleBrandRevision(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("brand.revision_needed"),
		SourceAgent: "pre-brand-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"note": "rename"}),
	})
	waitForEventTypes(t, ch, []string{"brand.revision_needed"})

	pc.handleVerticalResumed(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.resumed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"resumed_by": "human"}),
	})
	waitForEventTypes(t, ch, []string{"validation.started", "spec.revision_requested", "cto.spec_review_requested", "brand.requested"})

	pc.handleValidationRejected(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("research.vertical_rejected"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"reason": "bad economics"}),
	})
	waitForEventTypes(t, ch, []string{"vertical.killed"})

	// CTO revision branch should also emit spec.revision_requested.
	pc.handleCTORevisionNeeded(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("cto.spec_revision_needed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"reason": "tighten architecture"}),
	})
	waitForEventTypes(t, ch, []string{"spec.revision_requested"})
}

func TestFactoryPipelineCoordinator_ScanDedupAndCompletion(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ctx := context.Background()

	existingVerticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, raw_signals)
		VALUES ($1::uuid, 'Dental Clinic Scheduling', 'dental-clinic-scheduling', 'paraguay', 'discovered', 'factory', '{}'::jsonb)
	`, existingVerticalID); err != nil {
		t.Fatalf("insert existing vertical: %v", err)
	}

	ch := bus.Subscribe("watch-scan-dedup",
		events.EventType("dedup.ambiguous"),
		events.EventType("vertical.discovered"),
		events.EventType("scan.completed"),
	)

	scanID := uuid.NewString()
	pc.handleScanRequested(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("scan.requested"),
		Payload: mustJSON(map[string]any{
			"scan_id":   scanID,
			"mode":      "saas_gap",
			"geography": "paraguay",
		}),
	})

	pc.handleDiscoveryReport(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("category.assessed"),
		SourceAgent: "market-research-agent",
		Payload: mustJSON(map[string]any{
			"scan_id":         scanID,
			"vertical_name":   "Dental Clinic Scheduling SaaS",
			"signal_strength": 88,
			"geography":       "paraguay",
			"mode":            "saas_gap",
		}),
	})

	dedupEvt := waitForEventTypes(t, ch, []string{"dedup.ambiguous"})["dedup.ambiguous"]
	var dedupPayload map[string]any
	if err := json.Unmarshal(dedupEvt.Payload, &dedupPayload); err != nil {
		t.Fatalf("decode dedup payload: %v", err)
	}
	dedupEventID := strings.TrimSpace(asString(dedupPayload["dedup_event_id"]))
	if dedupEventID == "" {
		t.Fatal("expected dedup_event_id in dedup.ambiguous payload")
	}

	pc.handleDedupResolved(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("dedup.resolved"),
		Payload: mustJSON(map[string]any{
			"dedup_event_id": dedupEventID,
			"action":         "keep_both",
		}),
	})
	waitForEventTypes(t, ch, []string{"vertical.discovered"})

	pc.handleScanCompletion(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("market_research.scan_complete"),
		Payload: mustJSON(map[string]any{
			"scan_id": scanID,
		}),
	})
	waitForEventTypes(t, ch, []string{"scan.completed"})

	if got := len(pc.SnapshotScans()); got != 0 {
		t.Fatalf("expected scan accumulator cleared after completion, got %d", got)
	}
}

func TestFactoryPipelineCoordinator_DiscoveryNameAndSlugAreCanonical(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ctx := context.Background()

	scanID := uuid.NewString()
	pc.handleScanRequested(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("scan.requested"),
		Payload: mustJSON(map[string]any{
			"scan_id":   scanID,
			"mode":      "saas_trend",
			"geography": "paraguay",
		}),
	})

	ch := bus.Subscribe("watch-canonical-discovery", events.EventType("vertical.discovered"))
	pc.handleDiscoveryReport(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("trend.identified"),
		SourceAgent: "trend-research-agent",
		Payload: mustJSON(map[string]any{
			"scan_id":                scanID,
			"mode":                   "saas_trend",
			"geography":              "paraguay",
			"trend_category":         "instant_payments",
			"trend_description":      "Paraguay's instant payment system is experiencing explosive growth, with 28M transactions in a month and a regulatory interoperability mandate.",
			"opportunity_hypothesis": "Build unified rails to orchestrate payments, merchant onboarding, reconciliation, and compliant reporting across acquirers and banks.",
			"evidence":               "sample",
			"signal_strength":        73,
		}),
	})

	evt := waitForEventTypes(t, ch, []string{"vertical.discovered"})["vertical.discovered"]
	payload := parsePayloadMap(evt.Payload)
	if got := strings.TrimSpace(asString(payload["name"])); got != "Instant Payments" {
		t.Fatalf("expected concise canonical vertical name, got %q", got)
	}
	verticalID := strings.TrimSpace(asString(payload["vertical_id"]))
	if verticalID == "" {
		t.Fatalf("expected vertical_id in payload, got %v", payload)
	}
	var dbName, dbSlug string
	if err := db.QueryRowContext(ctx, `
		SELECT name, slug
		FROM verticals
		WHERE id = $1::uuid
	`, verticalID).Scan(&dbName, &dbSlug); err != nil {
		t.Fatalf("load discovered vertical: %v", err)
	}
	if strings.TrimSpace(dbName) != "Instant Payments" {
		t.Fatalf("expected persisted canonical name, got %q", dbName)
	}
	if len(dbSlug) > maxVerticalSlugLen+1+8 {
		t.Fatalf("expected slug length <= %d, got %d (%q)", maxVerticalSlugLen+1+8, len(dbSlug), dbSlug)
	}
	if !strings.HasSuffix(dbSlug, "-"+verticalID[:8]) {
		t.Fatalf("expected slug suffix to include id prefix, slug=%q vertical=%q", dbSlug, verticalID)
	}
}

func TestFactoryPipelineCoordinator_ValidationStartedPayloadEnrichedFromVertical(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Pet Grooming Operations', 'pet-grooming-ops', 'argentina', 'shortlisted', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	ch := bus.Subscribe("watch-validation-enriched", events.EventType("validation.started"))
	pc.handleValidationStarted(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.shortlisted"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"composite_score": 81.25}),
	})
	got := waitForEventTypes(t, ch, []string{"validation.started"})
	evt := got["validation.started"]
	payload := parsePayloadMap(evt.Payload)
	if strings.TrimSpace(asString(payload["vertical_name"])) != "Pet Grooming Operations" {
		t.Fatalf("expected vertical_name from verticals table, got %+v", payload["vertical_name"])
	}
	if strings.TrimSpace(asString(payload["geography"])) != "argentina" {
		t.Fatalf("expected geography from verticals table, got %+v", payload["geography"])
	}
	scoring, _ := payload["scoring"].(map[string]any)
	if scoring == nil || asFloat(scoring["composite_score"]) != 81.25 {
		t.Fatalf("expected scoring payload preserved, got %+v", payload["scoring"])
	}
}

func TestFactoryPipelineCoordinator_BrandRequestedPayloadEnrichedFromVertical(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Payroll Ops', 'payroll-ops', 'paraguay', 'shortlisted', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	ch := bus.Subscribe("watch-brand-request-enriched",
		events.EventType("validation.started"),
		events.EventType("brand.requested"),
	)
	pc.handleValidationStarted(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.shortlisted"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"composite_score": 86.5}),
	})
	got := waitForEventTypes(t, ch, []string{"validation.started", "brand.requested"})
	brand := parsePayloadMap(got["brand.requested"].Payload)
	if strings.TrimSpace(asString(brand["vertical_name"])) != "Payroll Ops" {
		t.Fatalf("expected brand payload vertical_name from verticals table, got %+v", brand["vertical_name"])
	}
	if strings.TrimSpace(asString(brand["geography"])) != "paraguay" {
		t.Fatalf("expected brand payload geography from verticals table, got %+v", brand["geography"])
	}
	scoring, _ := brand["scoring"].(map[string]any)
	if scoring == nil || asFloat(scoring["composite_score"]) != 86.5 {
		t.Fatalf("expected brand payload scoring preserved, got %+v", brand["scoring"])
	}
}

func TestFactoryPipelineCoordinator_ValidationResumedPayloadEnrichedFromVertical(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Dental Clinic Scheduling', 'dental-clinic-scheduling', 'paraguay', 'researching', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	pc.mu.Lock()
	pc.validations[verticalID] = &validationPipelineState{
		VerticalID:     verticalID,
		Status:         "active",
		G1Research:     false,
		G2Spec:         true,
		G3CTO:          true,
		G4Brand:        true,
		ScoringPayload: mustJSON(map[string]any{"composite_score": 77.5}),
	}
	pc.mu.Unlock()

	ch := bus.Subscribe("watch-validation-resume-enriched", events.EventType("validation.started"))
	pc.handleVerticalResumed(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.resumed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"resumed_by": "human"}),
	})
	got := waitForEventTypes(t, ch, []string{"validation.started"})
	evt := got["validation.started"]
	payload := parsePayloadMap(evt.Payload)
	if strings.TrimSpace(asString(payload["vertical_name"])) != "Dental Clinic Scheduling" {
		t.Fatalf("expected vertical_name from verticals table on resumed path, got %+v", payload["vertical_name"])
	}
	if strings.TrimSpace(asString(payload["geography"])) != "paraguay" {
		t.Fatalf("expected geography from verticals table on resumed path, got %+v", payload["geography"])
	}
	scoring, _ := payload["scoring"].(map[string]any)
	if scoring == nil || asFloat(scoring["composite_score"]) != 77.5 {
		t.Fatalf("expected scoring payload from validation state, got %+v", payload["scoring"])
	}
}

func TestFactoryPipelineCoordinator_InnerRevisionAndPackagedState(t *testing.T) {
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, nil)
	ctx := context.Background()
	verticalID := uuid.NewString()

	pc.mu.Lock()
	pc.validations[verticalID] = &validationPipelineState{
		VerticalID: verticalID,
		Status:     "active",
	}
	pc.mu.Unlock()

	// First five revisions: no escalation.
	for i := 0; i < maxInnerRevisions; i++ {
		if escalated := pc.handleInnerSpecRevision(ctx, events.Event{
			ID:         uuid.NewString(),
			Type:       events.EventType("spec.revision_needed"),
			VerticalID: verticalID,
			Payload:    mustJSON(map[string]any{"cycle": i + 1}),
		}); escalated {
			t.Fatalf("did not expect escalation at cycle %d", i+1)
		}
	}
	// Sixth should escalate (park).
	if escalated := pc.handleInnerSpecRevision(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.revision_needed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"cycle": maxInnerRevisions + 1}),
	}); !escalated {
		t.Fatal("expected escalation after max inner revision cycles")
	}

	// Reset inner revision loop on spec.revision_requested.
	pc.handleSpecRevisionRequested(events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.revision_requested"),
		VerticalID: verticalID,
	})
	pc.mu.Lock()
	gotInner := pc.validations[verticalID].InnerRevisionCount
	pc.mu.Unlock()
	if gotInner != 0 {
		t.Fatalf("expected inner revision count reset to 0, got %d", gotInner)
	}

	// Mark packaged on vertical.ready_for_review.
	pc.handleValidationPackaged(events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.ready_for_review"),
		VerticalID: verticalID,
	})
	pc.mu.Lock()
	gotStatus := pc.validations[verticalID].Status
	pc.mu.Unlock()
	if gotStatus != "packaged" {
		t.Fatalf("expected packaged status, got %q", gotStatus)
	}
}
