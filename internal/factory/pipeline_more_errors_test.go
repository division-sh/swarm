package factory

import (
	"context"
	"testing"
	"time"

	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestPipeline_RunScan_Validations(t *testing.T) {
	p := (*Pipeline)(nil)
	if _, err := p.RunScan(context.Background(), "x", "discovery", 1); err == nil {
		t.Fatalf("expected db required error")
	}
	_, db, _ := testutil.StartPostgres(t)
	p2 := NewPipeline(db, nil, nil)
	if _, err := p2.RunScan(context.Background(), "   ", "discovery", 1); err == nil {
		t.Fatalf("expected geography required error")
	}
}

func TestPipeline_ScoreVertical_RejectsUnknownStage(t *testing.T) {
	if err := validateStageTransition("weird_stage", "scoring"); err == nil {
		t.Fatalf("expected unknown current stage error")
	}
}

func TestPipeline_ValidateVertical_StopsOnTerminalStages(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	p := NewPipeline(db, nil, nil)
	ctx := context.Background()

	idKilled := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'k', 'us', 'killed', 'factory', now(), now())
	`, idKilled); err != nil {
		t.Fatalf("seed killed: %v", err)
	}
	ok, err := p.validateVertical(ctx, idKilled)
	if err != nil || ok {
		t.Fatalf("expected killed -> false, got ok=%v err=%v", ok, err)
	}

	idReady := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'r', 'us', 'ready_for_review', 'factory', now(), now())
	`, idReady); err != nil {
		t.Fatalf("seed ready: %v", err)
	}
	ok, err = p.validateVertical(ctx, idReady)
	if err != nil || !ok {
		t.Fatalf("expected ready_for_review -> true, got ok=%v err=%v", ok, err)
	}
}

func TestPipeline_UpdateStageField_DefaultJSON(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	p := NewPipeline(db, nil, nil)
	ctx := context.Background()
	id := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'V', 'v', 'us', 'shortlisted', 'factory', now(), now())
	`, id); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	// raw empty should become {} and still update.
	if err := p.updateStageField(ctx, id, "researching", "business_brief", nil); err != nil {
		t.Fatalf("updateStageField: %v", err)
	}
	var stage string
	if err := db.QueryRowContext(ctx, `SELECT stage FROM verticals WHERE id=$1::uuid`, id).Scan(&stage); err != nil {
		t.Fatalf("load stage: %v", err)
	}
	if stage != "researching" {
		t.Fatalf("expected stage researching, got %s", stage)
	}
	_ = time.Second
}
