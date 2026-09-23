package activityjournal

import (
	"context"
	"errors"
	"testing"

	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestActivityOwnerSchemaGuardPrecedesProtocolBothStores(t *testing.T) {
	guardErr := errors.New("schema not ready")
	postgres := &ActivityPostgresOwner{schemaGuard: func() error { return guardErr }}
	sqlite := &ActivitySQLiteOwner{schemaGuard: func() error { return guardErr }}
	record := runtimepipeline.ActivityAttemptRecord{}
	ctx := context.Background()
	for _, test := range []struct {
		name string
		run  func() error
	}{
		{"postgres/start", func() error { _, _, err := postgres.StartActivityAttempt(ctx, record); return err }},
		{"postgres/claim", func() error { _, _, err := postgres.ClaimActivityAttemptForLoopGeneration(ctx, record); return err }},
		{"postgres/complete", func() error { _, _, err := postgres.CompleteActivityAttempt(ctx, record); return err }},
		{"postgres/uncertain", func() error { _, _, err := postgres.MarkActivityAttemptUncertain(ctx, record); return err }},
		{"sqlite/start", func() error { _, _, err := sqlite.StartActivityAttempt(ctx, record); return err }},
		{"sqlite/claim", func() error { _, _, err := sqlite.ClaimActivityAttemptForLoopGeneration(ctx, record); return err }},
		{"sqlite/complete", func() error { _, _, err := sqlite.CompleteActivityAttempt(ctx, record); return err }},
		{"sqlite/uncertain", func() error { _, _, err := sqlite.MarkActivityAttemptUncertain(ctx, record); return err }},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); !errors.Is(err, guardErr) {
				t.Fatalf("schema guard error = %v, want %v", err, guardErr)
			}
		})
	}
}
