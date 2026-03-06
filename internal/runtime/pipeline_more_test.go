package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"empireai/internal/events"
	runtimetestkit "empireai/internal/runtime/testkit"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

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
	runtimetestkit.WaitForEventTypes(t, ch, []string{"validation.started", "brand.requested"}, 0)

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
	runtimetestkit.WaitForEventTypes(t, ch, []string{"spec.validation_requested"}, 0)

	pc.handleSpecValidationPassed(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.validation_passed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"spec_version": 1, "status": "passed"}),
	})
	runtimetestkit.WaitForEventTypes(t, ch, []string{"cto.spec_review_requested"}, 0)

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

	got := runtimetestkit.WaitForEventTypes(t, ch, []string{"validation.package_ready"}, 0)
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
	runtimetestkit.WaitForEventTypes(t, ch, []string{"spec.revision_requested"}, 0)

	pc.handleValidationMoreData(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.needs_more_data"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"reason": "need more evidence"}),
	})
	runtimetestkit.WaitForEventTypes(t, ch, []string{"validation.more_data_needed"}, 0)

	pc.handleBrandRevision(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("brand.revision_needed"),
		SourceAgent: "pre-brand-agent",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"note": "rename"}),
	})
	runtimetestkit.WaitForEventTypes(t, ch, []string{"brand.revision_needed"}, 0)

	pc.handleVerticalResumed(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.resumed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"resumed_by": "human"}),
	})
	runtimetestkit.WaitForEventTypes(t, ch, []string{"validation.started", "spec.revision_requested", "cto.spec_review_requested", "brand.requested"}, 0)

	pc.handleValidationRejected(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("research.vertical_rejected"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"reason": "bad economics"}),
	})
	runtimetestkit.WaitForEventTypes(t, ch, []string{"vertical.killed"}, 0)

	pc.handleCTORevisionNeeded(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("cto.spec_revision_needed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"reason": "tighten architecture"}),
	})
	runtimetestkit.WaitForEventTypes(t, ch, []string{"spec.revision_requested"}, 0)
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

	dedupEvt := runtimetestkit.WaitForEventTypes(t, ch, []string{"dedup.ambiguous"}, 0)["dedup.ambiguous"]
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
	runtimetestkit.WaitForEventTypes(t, ch, []string{"vertical.discovered"}, 0)

	pc.handleScanCompletion(ctx, events.Event{
		ID:   uuid.NewString(),
		Type: events.EventType("market_research.scan_complete"),
		Payload: mustJSON(map[string]any{
			"scan_id": scanID,
		}),
	})
	runtimetestkit.WaitForEventTypes(t, ch, []string{"scan.completed"}, 0)

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

	evt := runtimetestkit.WaitForEventTypes(t, ch, []string{"vertical.discovered"}, 0)["vertical.discovered"]
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
	got := runtimetestkit.WaitForEventTypes(t, ch, []string{"validation.started"}, 0)
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
	got := runtimetestkit.WaitForEventTypes(t, ch, []string{"validation.started", "brand.requested"}, 0)
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
	got := runtimetestkit.WaitForEventTypes(t, ch, []string{"validation.started"}, 0)
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

	if escalated := pc.handleInnerSpecRevision(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.revision_needed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"cycle": maxInnerRevisions + 1}),
	}); !escalated {
		t.Fatal("expected escalation after max inner revision cycles")
	}

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

	pc.handleValidationPackaged(ctx, events.Event{
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

func TestFactoryPipelineCoordinator_ShardsHelpersAndAsFloat(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS shards (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			root_task_id UUID NOT NULL,
			scan_id UUID,
			stage TEXT NOT NULL,
			shard_index INT NOT NULL,
			shard_count INT NOT NULL,
			shard_key TEXT NOT NULL,
			scope JSONB NOT NULL,
			agent_id TEXT,
			status TEXT NOT NULL DEFAULT 'pending',
			deadline_at TIMESTAMPTZ NOT NULL,
			budget_cents INT NOT NULL,
			spend_cents INT NOT NULL DEFAULT 0,
			retry_count INT NOT NULL DEFAULT 0,
			error TEXT,
			assigned_at TIMESTAMPTZ,
			completed_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		t.Fatalf("create shards table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (id, type, role, mode, status, config, started_at, last_active_at)
		VALUES ('agent-a', 'ephemeral', 'market-research-agent', 'factory', 'active', '{}'::jsonb, now(), now())
		ON CONFLICT (id) DO NOTHING
	`); err != nil {
		t.Fatalf("seed shard agent: %v", err)
	}

	scanID := "scan-runtime-helper"
	scanUUID := stableUUID(scanID).String()
	shardID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO shards (
			id, root_task_id, scan_id, stage, shard_index, shard_count, shard_key,
			scope, agent_id, status, deadline_at, budget_cents, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3::uuid, 'market_research', 0, 1, 'finance',
			'{}'::jsonb, 'agent-a', 'assigned', now() + interval '15 minute', 100, now()
		)
	`, shardID, uuid.NewString(), scanUUID); err != nil {
		t.Fatalf("seed shard: %v", err)
	}

	total, completed, failed, ok := pc.shardTerminalProgress(ctx, scanID)
	if !ok || total != 1 || completed != 0 || failed != 0 {
		t.Fatalf("unexpected shard progress before completion: total=%d completed=%d failed=%d ok=%v", total, completed, failed, ok)
	}

	if got := pc.markShardCompletedByAgent(ctx, "agent-a"); got == "" {
		t.Fatal("expected markShardCompletedByAgent to return completed shard id")
	}
	if got := pc.markShardCompletedByAgent(ctx, "agent-a"); got != "" {
		t.Fatalf("expected no second completion id after terminal update, got %q", got)
	}

	total, completed, failed, ok = pc.shardTerminalProgress(ctx, scanID)
	if !ok || total != 1 || completed != 1 || failed != 0 {
		t.Fatalf("unexpected shard progress after completion: total=%d completed=%d failed=%d ok=%v", total, completed, failed, ok)
	}

	if got := asFloat("12.5"); got != 12.5 {
		t.Fatalf("asFloat string parse mismatch: %v", got)
	}
	if got := asFloat(7); got != 7 {
		t.Fatalf("asFloat int parse mismatch: %v", got)
	}
	if got := asFloat(nil); got != 0 {
		t.Fatalf("asFloat nil should be zero, got %v", got)
	}
}

func TestFactoryPipelineCoordinator_InterceptPolicyAndRunMaintenance(t *testing.T) {
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), nil)

	if consume, handled := pc.interceptPolicy("scan.requested", events.Event{ID: uuid.NewString()}); !consume || !handled {
		t.Fatalf("scan.requested should be intercepted and consumed; got consume=%v handled=%v", consume, handled)
	}
	if consume, handled := pc.interceptPolicy("vertical.shortlisted", events.Event{ID: uuid.NewString(), VerticalID: uuid.NewString()}); !consume || !handled {
		t.Fatalf("vertical.shortlisted should be intercepted and consumed; got consume=%v handled=%v", consume, handled)
	}
	if consume, handled := pc.interceptPolicy("spec.validation_passed", events.Event{ID: uuid.NewString()}); consume || handled {
		t.Fatalf("spec.validation_passed without vertical should not be handled; got consume=%v handled=%v", consume, handled)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pc.RunMaintenance(ctx)
}

func ensurePipelineStateTables(t *testing.T, ctx context.Context, pc *FactoryPipelineCoordinator) {
	t.Helper()
	if pc == nil || pc.db == nil {
		t.Fatal("pipeline coordinator db required")
	}
	if _, err := pc.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS scan_accumulators (
			scan_id UUID PRIMARY KEY,
			campaign_id UUID NOT NULL,
			mode TEXT NOT NULL,
			geography TEXT NOT NULL,
			expected_agents INT NOT NULL,
			agents_complete INT NOT NULL DEFAULT 0,
			completed_by JSONB NOT NULL DEFAULT '[]'::jsonb,
			reports JSONB NOT NULL DEFAULT '[]'::jsonb,
			verticals_discovered INT NOT NULL DEFAULT 0,
			verticals_skipped INT NOT NULL DEFAULT 0,
			pending_dedup INT NOT NULL DEFAULT 0,
			timeout_at TIMESTAMPTZ NOT NULL DEFAULT now() + interval '90 minutes',
			started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			completed_at TIMESTAMPTZ
		);
		CREATE TABLE IF NOT EXISTS pending_dedup_candidates (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			scan_id UUID NOT NULL REFERENCES scan_accumulators(scan_id),
			candidate JSONB NOT NULL DEFAULT '{}'::jsonb,
			existing_id UUID NOT NULL DEFAULT gen_random_uuid(),
			dedup_event_id UUID,
			signal_strength INT NOT NULL DEFAULT 0,
			geography TEXT NOT NULL,
			discovery_mode TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE TABLE IF NOT EXISTS validation_pipelines (
			vertical_id UUID PRIMARY KEY,
			status TEXT NOT NULL,
			g1_research BOOLEAN NOT NULL DEFAULT false,
			g2_spec_approved BOOLEAN NOT NULL DEFAULT false,
			g3_cto_approved BOOLEAN NOT NULL DEFAULT false,
			g4_brand_ready BOOLEAN NOT NULL DEFAULT false,
			research_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			spec_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			cto_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			brand_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			revision_count INT NOT NULL DEFAULT 0,
			inner_revision_count INT NOT NULL DEFAULT 0,
			spec_version INT NOT NULL DEFAULT 0,
			packaging_requested_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE TABLE IF NOT EXISTS pipeline_processed_events (
			event_id UUID PRIMARY KEY,
			processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`); err != nil {
		t.Fatalf("create pipeline persistence tables: %v", err)
	}
}

func TestFactoryPipelineCoordinator_PersistAndLoadState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ensurePipelineStateTables(t, ctx, pc)

	verticalID := uuid.NewString()
	existingVerticalID := uuid.NewString()
	scanID := uuid.NewString()
	dedupID := uuid.NewString()
	campaignID := uuid.NewString()
	geoID := uuid.NewString()
	now := time.Now().UTC()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES
			($1::uuid, 'Payroll Ops', 'payroll-ops', 'argentina', 'shortlisted', 'factory', now(), now()),
			($2::uuid, 'Existing Payroll', 'existing-payroll', 'argentina', 'operating', 'operating', now(), now())
	`, verticalID, existingVerticalID); err != nil {
		t.Fatalf("seed verticals: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, region, created_at)
		VALUES ($1::uuid, 'argentina', 'AR', 'latam', now())
	`, geoID); err != nil {
		t.Fatalf("seed geography: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO scan_campaigns (id, geography_id, mode, priority, status, created_at)
		VALUES ($1::uuid, $2::uuid, 'saas_gap', 'normal', 'active', now())
	`, campaignID, geoID); err != nil {
		t.Fatalf("seed campaign: %v", err)
	}

	pc.mu.Lock()
	pc.scans[scanID] = &scanAccumulator{
		ScanID:      scanID,
		CampaignID:  campaignID,
		Mode:        "saas_gap",
		Geography:   "argentina",
		Expected:    2,
		CompletedBy: map[string]struct{}{"market-research-agent": {}},
		ReportData: []map[string]any{
			{"signal_strength": 79},
			{"signal_strength": 74},
			{"signal_strength": 66},
		},
		Reports:    3,
		Discovered: 2,
		Skipped:    1,
		CreatedAt:  now.Add(-15 * time.Minute),
	}
	pc.pendingDedup[dedupID] = pendingCandidate{
		DedupEventID: dedupID,
		ExistingID:   existingVerticalID,
		ScanID:       scanID,
		CampaignID:   campaignID,
		Mode:         "saas_gap",
		Geography:    "argentina",
		Name:         "Payroll Ops",
		Signal:       79,
		Payload:      map[string]any{"scan_id": scanID, "campaign_id": campaignID, "name": "Payroll Ops"},
	}
	pc.validations[verticalID] = &validationPipelineState{
		VerticalID:           verticalID,
		Status:               "active",
		G1Research:           true,
		G2Spec:               true,
		G3CTO:                false,
		G4Brand:              false,
		ResearchPayload:      mustJSON(map[string]any{"brief": "ok"}),
		SpecPayload:          mustJSON(map[string]any{"spec": "v1"}),
		CTOPayload:           mustJSON(map[string]any{"notes": "pending"}),
		BrandPayload:         mustJSON(map[string]any{"candidates": []string{"x"}}),
		ScoringPayload:       mustJSON(map[string]any{"composite_score": 81.5}),
		RevisionCount:        1,
		InnerRevisionCount:   2,
		SpecVersion:          3,
		PackagingRequested:   true,
		PackagingRequestedAt: runtimetestkit.PtrTime(now.Add(-5 * time.Minute)),
		PackagingRetries:     1,
	}
	pc.processed["evt-processed"] = struct{}{}
	pc.mu.Unlock()

	if !pc.isStatePersistenceEnabled(ctx) {
		t.Fatal("expected state persistence tables to be detected as enabled")
	}
	pc.persistRuntimeState(ctx)
	var persistedScans, persistedPending, persistedValidations int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM scan_accumulators`).Scan(&persistedScans)
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_dedup_candidates`).Scan(&persistedPending)
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM validation_pipelines`).Scan(&persistedValidations)
	if persistedValidations == 0 {
		t.Fatalf("expected persisted validation state rows, got scans=%d pending=%d validations=%d", persistedScans, persistedPending, persistedValidations)
	}

	pcLoaded := NewFactoryPipelineCoordinator(bus, db)
	ensurePipelineStateTables(t, ctx, pcLoaded)
	pcLoaded.ensureStateLoaded(ctx)

	_ = len(pcLoaded.SnapshotScans())
	_ = pcLoaded.pendingDedupCountForScan(scanID)
	loaded := pcLoaded.validationContext(verticalID)
	if loaded.SpecVersion != 3 {
		t.Fatalf("unexpected loaded validation context: %+v", loaded)
	}
	if ok := pcLoaded.markEventProcessed(ctx, "evt-processed"); !ok {
		t.Fatal("expected markEventProcessed to accept new event after fresh load")
	}

	pcLoaded.clearPersistentState(ctx)
	var scansCount, pendingCount, validationsCount, processedCount int
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM scan_accumulators`).Scan(&scansCount)
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_dedup_candidates`).Scan(&pendingCount)
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM validation_pipelines`).Scan(&validationsCount)
	_ = db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pipeline_processed_events`).Scan(&processedCount)
	if scansCount != 0 || pendingCount != 0 || validationsCount != 0 || processedCount != 0 {
		t.Fatalf("expected persistent state cleared, got scans=%d pending=%d validations=%d processed=%d", scansCount, pendingCount, validationsCount, processedCount)
	}
}

func TestFactoryPipelineCoordinator_CheckPackagingTimeoutsRetryAndPark(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ensurePipelineStateTables(t, ctx, pc)

	verticalRetry := uuid.NewString()
	verticalPark := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES
			($1::uuid, 'Retry Vertical', 'retry-vertical', 'argentina', 'researching', 'factory', now(), now()),
			($2::uuid, 'Park Vertical', 'park-vertical', 'argentina', 'researching', 'factory', now(), now())
	`, verticalRetry, verticalPark); err != nil {
		t.Fatalf("seed verticals: %v", err)
	}

	old := time.Now().UTC().Add(-(packagingTimeout + 2*time.Minute))
	pc.mu.Lock()
	pc.validations[verticalRetry] = &validationPipelineState{
		VerticalID:           verticalRetry,
		Status:               "active",
		G1Research:           true,
		G2Spec:               true,
		G3CTO:                true,
		G4Brand:              true,
		ResearchPayload:      mustJSON(map[string]any{"brief": "ok"}),
		SpecPayload:          mustJSON(map[string]any{"spec": "v1"}),
		CTOPayload:           mustJSON(map[string]any{"notes": "ok"}),
		BrandPayload:         mustJSON(map[string]any{"names": []string{"r"}}),
		ScoringPayload:       mustJSON(map[string]any{"composite_score": 82}),
		SpecVersion:          1,
		PackagingRequested:   true,
		PackagingRequestedAt: runtimetestkit.PtrTime(old),
		PackagingRetries:     0,
	}
	pc.validations[verticalPark] = &validationPipelineState{
		VerticalID:           verticalPark,
		Status:               "active",
		G1Research:           true,
		G2Spec:               true,
		G3CTO:                true,
		G4Brand:              true,
		ResearchPayload:      mustJSON(map[string]any{"brief": "ok"}),
		SpecPayload:          mustJSON(map[string]any{"spec": "v1"}),
		CTOPayload:           mustJSON(map[string]any{"notes": "ok"}),
		BrandPayload:         mustJSON(map[string]any{"names": []string{"p"}}),
		ScoringPayload:       mustJSON(map[string]any{"composite_score": 76}),
		SpecVersion:          1,
		PackagingRequested:   true,
		PackagingRequestedAt: runtimetestkit.PtrTime(old),
		PackagingRetries:     1,
	}
	pc.mu.Unlock()

	ch := bus.Subscribe("watch-packaging-timeout", events.EventType("validation.package_ready"))
	pc.checkPackagingTimeouts(ctx, time.Now().UTC())

	select {
	case evt := <-ch:
		if strings.TrimSpace(evt.VerticalID) != verticalRetry {
			t.Fatalf("expected retry event for %s, got vertical=%s", verticalRetry, evt.VerticalID)
		}
		var payload map[string]any
		if err := json.Unmarshal(evt.Payload, &payload); err != nil {
			t.Fatalf("decode validation.package_ready payload: %v", err)
		}
		if strings.TrimSpace(asString(payload["vertical_id"])) == "" {
			t.Fatalf("expected vertical_id in retry payload: %+v", payload)
		}
	case <-time.After(600 * time.Millisecond):
		t.Fatal("expected validation.package_ready retry event")
	}

	pc.mu.Lock()
	retryState := pc.validations[verticalRetry]
	parkState := pc.validations[verticalPark]
	pc.mu.Unlock()
	if retryState == nil || retryState.PackagingRetries != 1 || retryState.Status != "active" {
		t.Fatalf("unexpected retry state after timeout handling: %+v", retryState)
	}
	if parkState == nil || parkState.Status != "parked" || parkState.PackagingRequested {
		t.Fatalf("expected parked validation after retry exhaustion: %+v", parkState)
	}

	var mailboxCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM mailbox
		WHERE vertical_id = $1::uuid AND from_agent = 'pipeline-coordinator' AND type = 'vertical_approval'
	`, verticalPark).Scan(&mailboxCount); err != nil {
		t.Fatalf("count mailbox park escalation: %v", err)
	}
	if mailboxCount < 1 {
		t.Fatal("expected mailbox escalation row after packaging timeout exhaustion")
	}
}

func TestAgentRuntimeHelpers_InferAndBudgetMapping(t *testing.T) {
	if got := inferDiscoveryMode("local services in argentina"); got != "local_services" {
		t.Fatalf("expected local_services, got %q", got)
	}
	if got := inferDiscoveryMode("follow saas_trend signals"); got != "saas_trend" {
		t.Fatalf("expected saas_trend, got %q", got)
	}
	if got := inferDiscoveryMode("generic directive"); got != "saas_gap" {
		t.Fatalf("expected default saas_gap, got %q", got)
	}

	if got := inferGeographyHint("SaaS in Paraguay"); got != "paraguay" {
		t.Fatalf("expected recognized geography paraguay, got %q", got)
	}
	if got := inferGeographyHint("Focus LATAM"); got != "Focus LATAM" {
		t.Fatalf("expected passthrough geography hint, got %q", got)
	}
	if got := inferGeographyHint(" "); got != "" {
		t.Fatalf("expected empty hint for blank input, got %q", got)
	}

	for state, evtType := range map[string]events.EventType{
		"warning":   events.EventType("budget.warning"),
		"throttle":  events.EventType("budget.throttle"),
		"emergency": events.EventType("budget.emergency"),
		"resumed":   events.EventType("budget.resumed"),
		"ok":        events.EventType("budget.resumed"),
	} {
		raw := mustJSON(map[string]any{"state": state})
		if got := budgetEventTypeFromThresholdPayload(raw); got != evtType {
			t.Fatalf("state %q expected %q, got %q", state, evtType, got)
		}
	}
	if got := budgetEventTypeFromThresholdPayload(mustJSON(map[string]any{"state": "unknown"})); got != "" {
		t.Fatalf("expected empty event type for unknown state, got %q", got)
	}

	if got := fieldStringFromJSON(mustJSON(map[string]any{"k": " v "}), "k"); got != "v" {
		t.Fatalf("expected trimmed field string, got %q", got)
	}
	if got := fieldStringFromJSON([]byte("{"), "k"); got != "" {
		t.Fatalf("expected empty for invalid json, got %q", got)
	}
}

func TestPipelineHelpers_NormalizationAndSimilarity(t *testing.T) {
	if got := normalizeName("  Dental-Clinic  Scheduling!! "); got != "dental clinic scheduling" {
		t.Fatalf("normalizeName mismatch: %q", got)
	}
	if slug := buildVerticalSlug("Dental Clinic Scheduling", "1234567890abcdef"); slug != "dental-clinic-scheduling-12345678" {
		t.Fatalf("unexpected slug %q", slug)
	}
	best, score := fuzzyBestMatch("Dental Clinic Scheduling SaaS", []verticalCandidate{
		{ID: "v1", Name: "Dental Clinic Scheduling"},
		{ID: "v2", Name: "Restaurant Ordering"},
	})
	if best.ID != "v1" || score <= 0.7 {
		t.Fatalf("expected v1 fuzzy match above threshold, got best=%+v score=%.2f", best, score)
	}
	if got := jaccard(tokenSet("a b"), tokenSet("b c")); got <= 0 || got >= 1 {
		t.Fatalf("expected partial overlap jaccard in (0,1), got %.2f", got)
	}
	merged := parsePayloadMap(mergeRawPayload(mustJSON(map[string]any{"a": 1, "b": 1}), mustJSON(map[string]any{"b": 2, "c": 3})))
	if asInt(merged["a"]) != 1 || asInt(merged["b"]) != 2 || asInt(merged["c"]) != 3 {
		t.Fatalf("unexpected merged payload: %+v", merged)
	}
}

func TestPipelineHelpers_DeriveDiscoveryCandidateName(t *testing.T) {
	name := deriveDiscoveryCandidateName(map[string]any{
		"trend_category":         "instant_payments",
		"trend_description":      "Paraguay's instant payment system is experiencing explosive growth with regulatory mandates and interoperability standards.",
		"opportunity_hypothesis": "Build a complete all-in-one orchestration layer for instant payment operators.",
	})
	if name != "Instant Payments" {
		t.Fatalf("expected concise taxonomy-derived name, got %q", name)
	}

	if got := deriveDiscoveryCandidateName(map[string]any{
		"opportunity_hypothesis": strings.Repeat("very long narrative hypothesis ", 8),
	}); got != "" {
		t.Fatalf("expected long narrative-only payload to be rejected, got %q", got)
	}
}

func TestPipelineHelpers_BuildVerticalSlugCapsLongBase(t *testing.T) {
	slug := buildVerticalSlug(strings.Repeat("instant-payment-growth-opportunity-", 6), "abcdef1234567890")
	if !strings.HasSuffix(slug, "-abcdef12") {
		t.Fatalf("expected stable id suffix, got %q", slug)
	}
	if len(slug) > maxVerticalSlugLen+1+8 {
		t.Fatalf("expected slug length cap <= %d, got %d (%q)", maxVerticalSlugLen+1+8, len(slug), slug)
	}
}

func TestDBTxContextWrappers_UseTransactionFromContext(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS tx_probe (id INT PRIMARY KEY)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	txCtx := withSQLTxContext(ctx, tx)
	if _, err := dbExecContext(txCtx, db, `INSERT INTO tx_probe (id) VALUES (1)`); err != nil {
		t.Fatalf("insert in tx context: %v", err)
	}

	var inTxCount int
	if err := dbQueryRowContext(txCtx, db, `SELECT count(*) FROM tx_probe`).Scan(&inTxCount); err != nil {
		t.Fatalf("count in tx: %v", err)
	}
	if inTxCount != 1 {
		t.Fatalf("expected in-tx count=1, got %d", inTxCount)
	}

	rows, err := dbQueryContext(txCtx, db, `SELECT id FROM tx_probe`)
	if err != nil {
		t.Fatalf("query rows in tx: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected one row in tx query")
	}

	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	var postRollbackCount int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM tx_probe`).Scan(&postRollbackCount); err != nil {
		t.Fatalf("count after rollback: %v", err)
	}
	if postRollbackCount != 0 {
		t.Fatalf("expected rollback to clear insert, got count=%d", postRollbackCount)
	}
}

func TestScanCampaignManager_EmitCampaignCompletedIfDone(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	bus := NewEventBus(InMemoryEventStore{})
	store := &scanStoreStub{}
	manager := NewScanCampaignManager(bus, store, db)

	geoID := uuid.NewString()
	campaignID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, scan_config, created_at)
		VALUES ($1::uuid, 'Argentina', 'Argentina', '{}'::jsonb, now())
	`, geoID); err != nil {
		t.Fatalf("seed geography: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO scan_campaigns (
			id, geography_id, mode, priority, status, discoveries, directive_id, strategic_context, created_at, started_at, completed_at
		) VALUES (
			$1::uuid, $2::uuid, 'saas_gap', 'high', 'completed', 2, NULL, '{"directive_text":"SaaS in Argentina"}'::jsonb,
			now() - interval '2 hour', now() - interval '90 minute', now() - interval '80 minute'
		)
	`, campaignID, geoID); err != nil {
		t.Fatalf("seed campaign: %v", err)
	}

	ch := bus.Subscribe("watch-campaign-completed", events.EventType("campaign.completed"))
	emitted := manager.emitCampaignCompletedIfDone(ctx, campaignID, 3, "evt-source-1")
	if !emitted {
		t.Fatal("expected campaign.completed emission")
	}

	select {
	case evt := <-ch:
		var payload map[string]any
		if err := json.Unmarshal(evt.Payload, &payload); err != nil {
			t.Fatalf("decode campaign payload: %v", err)
		}
		if strings.TrimSpace(asString(payload["campaign_id"])) != campaignID {
			t.Fatalf("unexpected campaign_id in payload: %+v", payload)
		}
		if strings.TrimSpace(asString(payload["priority"])) != "high" {
			t.Fatalf("expected priority=high in payload: %+v", payload)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("expected campaign.completed event")
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO scan_campaigns (id, geography_id, mode, priority, status, discoveries, created_at)
		VALUES ($1::uuid, $2::uuid, 'saas_trend', 'normal', 'queued', 0, now())
	`, uuid.NewString(), geoID); err != nil {
		t.Fatalf("seed follow-on campaign: %v", err)
	}
	if emitted := manager.emitCampaignCompletedIfDone(ctx, campaignID, 1, "evt-source-2"); emitted {
		t.Fatal("expected no campaign.completed while additional campaigns remain")
	}
}

func TestScanCampaignManager_BackpressurePauseResumeAndHelpers(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	store := &scanStoreStub{}
	manager := NewScanCampaignManager(NewEventBus(InMemoryEventStore{}), store, db)

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'ready_for_review', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	for i := 0; i < 6; i++ {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO mailbox (id, vertical_id, from_agent, type, priority, status, context, summary, created_at)
			VALUES ($1::uuid, $2::uuid, 'validation-coordinator', 'vertical_approval', 'normal', 'pending', '{}'::jsonb, 'review', now())
		`, uuid.NewString(), verticalID); err != nil {
			t.Fatalf("seed mailbox item %d: %v", i, err)
		}
	}

	if pending, err := manager.pendingMailboxCount(ctx); err != nil || pending != 6 {
		t.Fatalf("expected pending mailbox count=6, got pending=%d err=%v", pending, err)
	}
	if !manager.shouldPauseForBackpressure(ctx) {
		t.Fatal("expected manager to pause for mailbox backpressure")
	}
	if store.pauseCalls == 0 {
		t.Fatal("expected PauseQueuedScanCampaigns to be called")
	}

	if _, err := db.ExecContext(ctx, `UPDATE mailbox SET status = 'approved' WHERE status = 'pending'`); err != nil {
		t.Fatalf("clear mailbox pending status: %v", err)
	}
	if manager.shouldPauseForBackpressure(ctx) {
		t.Fatal("expected manager to resume after backpressure clears")
	}
	if store.resumeCalls == 0 {
		t.Fatal("expected ResumePausedScanCampaigns to be called")
	}

	manager.setBudgetPausedForTest(true)
	if !manager.shouldPauseForBackpressure(ctx) {
		t.Fatal("budget pause should short-circuit as paused")
	}
	manager.resetFlags()
	if manager.budgetPausedForTest() || manager.backpressurePausedForTest() {
		t.Fatalf("resetFlags should clear pause flags: budget=%v backpressure=%v", manager.budgetPausedForTest(), manager.backpressurePausedForTest())
	}

	if got := sanitizeGeographyPhrase("argentina with complex filters and extras"); got != "Argentina" {
		t.Fatalf("unexpected sanitizeGeographyPhrase result: %q", got)
	}
}
