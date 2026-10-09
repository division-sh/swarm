package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
)

type selectedExecutionFailureOwner interface {
	RequireCurrentExternalEffectAuthorityTx(context.Context, *sql.Tx, effects.Authority) error
	RequireCompletionAuthorityNoLiveAttemptsTx(context.Context, *sql.Tx, effects.Authority) error
	MarkTerminalTx(context.Context, *mutationprotocol.Attempt, runlifecycle.TerminalRequest) (runlifecycle.Snapshot, runlifecycle.MutationDisposition, error)
}

func settleActivatedSelectedExecutionFailure(ctx context.Context, attempt *mutationprotocol.Attempt, authority effects.Authority, raw json.RawMessage, owner selectedExecutionFailureOwner, loadSnapshot runForkLifecycleSnapshotLoader, postgres bool) error {
	if authority.Kind != effects.AuthoritySelectedContractFork || !authority.Valid() {
		return fmt.Errorf("selected execution failure requires exact current authority")
	}
	failure, err := failures.UnmarshalEnvelope(raw)
	if err != nil {
		return err
	}
	return attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
		// Execution claim and run control acquire the run before its execution.
		snapshot, err := loadSnapshot(ctx, tx, authority.SelectedFork.ForkRunID)
		if err != nil {
			return err
		}
		if err := owner.RequireCurrentExternalEffectAuthorityTx(ctx, tx, authority); err != nil {
			return err
		}
		if err := owner.RequireCompletionAuthorityNoLiveAttemptsTx(ctx, tx, authority); err != nil {
			return err
		}
		if !snapshot.State.Terminal() {
			if _, _, err := owner.MarkTerminalTx(ctx, attempt, runlifecycle.TerminalRequest{
				RunID: snapshot.RunID, State: runlifecycle.StateFailed, Failure: &failure, EndedAt: time.Now().UTC(),
			}); err != nil {
				return err
			}
		}
		query := `UPDATE run_fork_selected_contract_runtime_executions
			SET state='closed',lease_expires_at=NULL,failure=$2,terminal_at=$3,updated_at=$3
			WHERE execution_id=$1 AND state='running' AND fence_generation=$4`
		if postgres {
			query = `UPDATE run_fork_selected_contract_runtime_executions
				SET state='closed',lease_expires_at=NULL,failure=$2::jsonb,terminal_at=$3,updated_at=$3
				WHERE execution_id=$1::uuid AND state='running' AND fence_generation=$4`
		}
		result, err := tx.ExecContext(ctx, query, authority.ID, string(raw), time.Now().UTC(), authority.FenceGeneration)
		return requireExactlyOneMutation(result, err, "settle activated selected execution failure")
	})
}

func (s *RunForkPostgresOwner) FailActivatedRunForkSelectedContractRuntimeExecution(ctx context.Context, authority effects.Authority, failure json.RawMessage) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("activated selected failure requires postgres owner")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	result := mutationprotocol.RunPostgres(ctx, s.backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.candidates,
		func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			return struct{}{}, settleActivatedSelectedExecutionFailure(ctx, attempt, authority, failure, s,
				func(ctx context.Context, tx *sql.Tx, runID string) (runlifecycle.Snapshot, error) {
					return s.RunLifecyclePostgresOwner.LoadSnapshotTx(ctx, tx, runID, true)
				}, true)
		})
	return result.Acknowledged(), result.Err()
}

func (s *RunForkSQLiteOwner) FailActivatedRunForkSelectedContractRuntimeExecution(ctx context.Context, authority effects.Authority, failure json.RawMessage) (bool, error) {
	if s == nil || s.backend == nil {
		return false, fmt.Errorf("activated selected failure requires sqlite owner")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return false, err
	}
	result := mutationprotocol.RunSQLite(ctx, s.backend, "settle activated selected execution failure", mutationprotocol.Story, mutationprotocol.Ordinary, nil, s.candidates,
		func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
			return struct{}{}, settleActivatedSelectedExecutionFailure(ctx, attempt, authority, failure, s, s.RunLifecycleSQLiteOwner.LoadSnapshotTx, false)
		})
	return result.Acknowledged(), result.Err()
}
