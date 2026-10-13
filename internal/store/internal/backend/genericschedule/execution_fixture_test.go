package genericschedule

import (
	"context"
	"database/sql"
	"errors"

	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
)

// Claim-session unit tests use standalone clocks only, not selected-run grants.
type standaloneExecutionFixture struct{}

func (standaloneExecutionFixture) ObserveRunExecution(ctx context.Context, _ sourceadmission.ExecutionQuery, runID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return runID == "", nil
}

func (standaloneExecutionFixture) RequireRunExecutionTx(ctx context.Context, _ *sql.Tx, runID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if runID != "" {
		return errors.New("standalone claim fixture cannot authorize run execution")
	}
	return nil
}
