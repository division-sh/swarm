package factory

import (
	"context"
	"testing"
	"time"

	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestPipeline_ValidateVertical_AdvancesStagesAndWritesFields(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	p := NewPipeline(db, pg, pg)

	verticalID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','shortlisted','factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}

	ok, err := p.validateVertical(context.Background(), verticalID)
	if err != nil {
		t.Fatalf("validateVertical err: %v", err)
	}
	if !ok {
		t.Fatalf("expected validation ok")
	}

	// Verify stage advanced.
	var stage string
	if err := db.QueryRowContext(context.Background(), `SELECT stage FROM verticals WHERE id=$1::uuid`, verticalID).Scan(&stage); err != nil {
		t.Fatalf("load stage: %v", err)
	}
	if stage != "ready_for_review" {
		t.Fatalf("expected ready_for_review, got %q", stage)
	}

	// Ensure mailbox row exists (validateVertical creates one when mailbox store is configured).
	var n int
	_ = db.QueryRowContext(context.Background(), `SELECT count(*) FROM mailbox WHERE vertical_id=$1::uuid`, verticalID).Scan(&n)
	if n < 1 {
		t.Fatalf("expected mailbox item created, got %d", n)
	}
	// Keep time import referenced for determinism across platforms.
	_ = time.Second
}

func TestPipeline_UpdateStageField_RejectsUnsupportedField(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	p := NewPipeline(db, nil, nil)
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'TestCo','testco','us','shortlisted','factory', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("seed vertical: %v", err)
	}
	if err := p.updateStageField(context.Background(), verticalID, "researching", "unknown_field", []byte(`{}`)); err == nil {
		t.Fatalf("expected unsupported field error")
	}
}
