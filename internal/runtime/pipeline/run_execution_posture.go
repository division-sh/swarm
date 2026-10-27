package pipeline

import (
	"context"
	"errors"

	"github.com/division-sh/swarm/internal/runtime/executionposture"
)

type RunExecutionPostureAdmission interface {
	AdmitRunExecutionPosture(context.Context, string, executionposture.Posture) error
}

func (s *workflowInstanceStore) AdmitRunExecutionPosture(ctx context.Context, runID string, posture executionposture.Posture) error {
	if s == nil || s.runPosture == nil {
		return errors.New("run execution posture owner is required")
	}
	return s.runPosture.AdmitRunExecutionPosture(ctx, runID, posture)
}
