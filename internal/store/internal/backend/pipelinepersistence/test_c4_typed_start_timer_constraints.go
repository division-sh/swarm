package pipelinepersistence

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

// ProbeC4TypedStartTimerConstraintsTxForTest reaches the existing native insert,
// without its front-door semantic validator. The proof must require a native
// constraint error and roll back its attempt; decoder refusal is not CHECK proof.
func (s *PipelinePostgresOwner) ProbeC4TypedStartTimerConstraintsTxForTest(ctx context.Context, attempt *mutationprotocol.Attempt, activation pipeline.WorkflowTimerActivation) error {
	if s == nil {
		return fmt.Errorf("native PostgreSQL timer owner is required")
	}
	return probeC4TypedStartTimerConstraintsTxForTest(ctx, attempt, true, activation)
}

func (s *PipelineSQLiteOwner) ProbeC4TypedStartTimerConstraintsTxForTest(ctx context.Context, attempt *mutationprotocol.Attempt, activation pipeline.WorkflowTimerActivation) error {
	if s == nil {
		return fmt.Errorf("native SQLite timer owner is required")
	}
	return probeC4TypedStartTimerConstraintsTxForTest(ctx, attempt, false, activation)
}

func probeC4TypedStartTimerConstraintsTxForTest(ctx context.Context, attempt *mutationprotocol.Attempt, postgres bool, activation pipeline.WorkflowTimerActivation) error {
	if attempt == nil || !activation.Ref.Valid() || !activation.Route.Valid() {
		return fmt.Errorf("C4 timer constraint probe requires an exact native attempt and timer identity")
	}
	for _, id := range []string{activation.Ref.ActivationID, activation.RunID, activation.EntityID} {
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			return fmt.Errorf("C4 timer constraint probe requires canonical storage identity")
		}
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		_, err := insertWorkflowEngineTimerActivation(ctx, tx, postgres, attempt, activation)
		return err
	})
}
