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

func ensurePipelineStateTables(t *testing.T, ctx context.Context, pc *FactoryPipelineCoordinator) {
	t.Helper()
	if pc == nil || pc.db == nil {
		t.Fatal("pipeline coordinator db required")
	}
	if _, err := pc.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS scan_accumulators (
			scan_id TEXT PRIMARY KEY,
			campaign_id TEXT,
			mode TEXT NOT NULL,
			geography TEXT NOT NULL,
			expected INT NOT NULL,
			completed_by JSONB NOT NULL DEFAULT '{}'::jsonb,
			reports INT NOT NULL DEFAULT 0,
			discovered INT NOT NULL DEFAULT 0,
			skipped INT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE TABLE IF NOT EXISTS pending_dedup_candidates (
			dedup_event_id TEXT PRIMARY KEY,
			scan_id TEXT NOT NULL,
			campaign_id TEXT,
			mode TEXT NOT NULL,
			geography TEXT NOT NULL,
			name TEXT NOT NULL,
			signal_strength DOUBLE PRECISION NOT NULL DEFAULT 0,
			payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE TABLE IF NOT EXISTS validation_pipelines (
			vertical_id UUID PRIMARY KEY,
			status TEXT NOT NULL,
			g1_research BOOLEAN NOT NULL DEFAULT false,
			g2_spec BOOLEAN NOT NULL DEFAULT false,
			g3_cto BOOLEAN NOT NULL DEFAULT false,
			g4_brand BOOLEAN NOT NULL DEFAULT false,
			research_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			spec_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			cto_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			brand_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			scoring_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			revision_count INT NOT NULL DEFAULT 0,
			inner_revision_count INT NOT NULL DEFAULT 0,
			spec_version INT NOT NULL DEFAULT 0,
			packaging_requested BOOLEAN NOT NULL DEFAULT false,
			packaging_requested_at TIMESTAMPTZ,
			packaging_retries INT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE TABLE IF NOT EXISTS pipeline_processed_events (
			event_id TEXT PRIMARY KEY,
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
	scanID := uuid.NewString()
	dedupID := uuid.NewString()
	campaignID := uuid.NewString()
	now := time.Now().UTC()

	pc.mu.Lock()
	pc.scans[scanID] = &scanAccumulator{
		ScanID:      scanID,
		CampaignID:  campaignID,
		Mode:        "saas_gap",
		Geography:   "argentina",
		Expected:    2,
		CompletedBy: map[string]struct{}{"market-research-agent": {}},
		Reports:     3,
		Discovered:  2,
		Skipped:     1,
		CreatedAt:   now.Add(-15 * time.Minute),
	}
	pc.pendingDedup[dedupID] = pendingCandidate{
		DedupEventID: dedupID,
		ScanID:       scanID,
		CampaignID:   campaignID,
		Mode:         "saas_gap",
		Geography:    "argentina",
		Name:         "Payroll Ops",
		Signal:       79,
		Payload:      map[string]any{"scan_id": scanID, "name": "Payroll Ops"},
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
		PackagingRequestedAt: ptrTime(now.Add(-5 * time.Minute)),
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
	if persistedScans == 0 || persistedPending == 0 || persistedValidations == 0 {
		t.Fatalf("expected persisted state rows, got scans=%d pending=%d validations=%d", persistedScans, persistedPending, persistedValidations)
	}

	pcLoaded := NewFactoryPipelineCoordinator(bus, db)
	ensurePipelineStateTables(t, ctx, pcLoaded)
	pcLoaded.ensureStateLoaded(ctx)

	if got := len(pcLoaded.SnapshotScans()); got != 1 {
		t.Fatalf("expected one loaded scan accumulator, got %d", got)
	}
	if got := pcLoaded.pendingDedupCountForScan(scanID); got != 1 {
		t.Fatalf("expected one pending dedup candidate, got %d", got)
	}
	loaded := pcLoaded.validationContext(verticalID)
	if loaded.SpecVersion != 3 || asFloat(loaded.Scoring["composite_score"]) != 81.5 {
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
		PackagingRequestedAt: ptrTime(old),
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
		PackagingRequestedAt: ptrTime(old),
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

func ptrTime(t time.Time) *time.Time { return &t }
