package pipeline

import (
	"context"
	"testing"

	"empireai/internal/events"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestFactoryPipelineCoordinator_ProjectsValidationStagesToVertical(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	bus := NewEventBus(InMemoryEventStore{})
	pc := NewFactoryPipelineCoordinator(bus, db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'EU AI Act Compliance', 'eu-ai-act-compliance', 'eu', 'shortlisted', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	assertStage := func(want string) {
		t.Helper()
		var got string
		if err := db.QueryRowContext(ctx, `SELECT COALESCE(stage,'') FROM verticals WHERE id = $1::uuid`, verticalID).Scan(&got); err != nil {
			t.Fatalf("load vertical stage: %v", err)
		}
		if got != want {
			t.Fatalf("expected stage=%q got=%q", want, got)
		}
	}

	pc.handleValidationStarted(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.shortlisted"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"composite_score": 81.25}),
	})
	assertStage("researching")

	pc.handleValidationGate(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("research.completed"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"business_brief": map[string]any{"problem": "manual compliance workflows"}}),
	}, "g1")
	assertStage("mvp_speccing")

	pc.handleValidationGate(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("spec.approved"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"spec": map[string]any{"features": []string{"audit log"}}}),
	}, "g2")
	assertStage("cto_spec_review")

	pc.handleValidationGate(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("cto.spec_approved"),
		VerticalID: verticalID,
		Payload:    mustJSON(map[string]any{"cto_notes": map[string]any{"decision": "approved"}}),
	}, "g3")
	assertStage("branding")

	pc.handleValidationPackaged(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.ready_for_review"),
		VerticalID: verticalID,
	})
	assertStage("ready_for_review")

	pc.handleVerticalApproved(ctx, events.Event{
		ID:         uuid.NewString(),
		Type:       events.EventType("vertical.approved"),
		VerticalID: verticalID,
	})
	assertStage("approved")
}

func TestFactoryPipelineCoordinator_ParkMailboxProjectsReadyForReview(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pc := NewFactoryPipelineCoordinator(NewEventBus(InMemoryEventStore{}), db)
	ctx := context.Background()
	verticalID := uuid.NewString()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'Policy Compliance Ops', 'policy-compliance-ops', 'eu', 'branding', 'factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical: %v", err)
	}

	pc.parkVerticalWithMailbox(ctx, verticalID, "Needs human review.", map[string]any{"source": "test"})

	var stage string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(stage,'') FROM verticals WHERE id = $1::uuid`, verticalID).Scan(&stage); err != nil {
		t.Fatalf("load vertical stage: %v", err)
	}
	if stage != "ready_for_review" {
		t.Fatalf("expected ready_for_review after mailbox park, got %q", stage)
	}
}
