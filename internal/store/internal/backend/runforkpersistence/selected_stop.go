package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	storestartup "github.com/division-sh/swarm/internal/store/internal/startupownership"
)

func (s *RunForkPostgresOwner) StopSelectedFork(ctx context.Context, req runcontrol.SelectedStopRequest) (runcontrol.State, error) {
	if err := req.Validate(); err != nil {
		return runcontrol.State{}, err
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runcontrol.State{}, err
	}
	var state runcontrol.State
	committed, err := s.backend.RunTransactionOutcome(ctx, func(ctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(ctx, tx, privateauthoractivity.DialectPostgres)
		if err != nil {
			return err
		}
		if err := requireSelectedStopTx(ctx, tx, req, false); err != nil {
			return err
		}
		effects := runforkrevision.NewEffects()
		state, err = s.StopSelectedRunTx(ctx, tx, story, effects, selectedStopTransition(req.Transition))
		if err != nil {
			return err
		}
		if err := finalizeRunForkAuthorActivityTransaction(ctx, tx, story, effects); err != nil {
			return err
		}
		return nil
	})
	if !committed {
		return runcontrol.State{}, err
	}
	return state, err
}

func (s *RunForkSQLiteOwner) StopSelectedFork(ctx context.Context, req runcontrol.SelectedStopRequest) (runcontrol.State, error) {
	if err := req.Validate(); err != nil {
		return runcontrol.State{}, err
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runcontrol.State{}, err
	}
	var state runcontrol.State
	committed, err := s.backend.RunTransactionOutcome(ctx, "stop selected fork", func(ctx context.Context, tx *sql.Tx) error {
		story, err := privateauthoractivity.Begin(ctx, tx, privateauthoractivity.DialectSQLite)
		if err != nil {
			return err
		}
		if err := requireSelectedStopTx(ctx, tx, req, true); err != nil {
			return err
		}
		effects := runforkrevision.NewEffects()
		state, err = s.StopSelectedRunTx(ctx, tx, story, effects, selectedStopTransition(req.Transition))
		if err != nil {
			return err
		}
		if _, err := runforkrevision.FinalizeSQLite(ctx, tx, effects); err != nil {
			return err
		}
		return story.Finalize(ctx)
	})
	if !committed {
		return runcontrol.State{}, err
	}
	return state, err
}

func requireSelectedStopTx(ctx context.Context, tx *sql.Tx, req runcontrol.SelectedStopRequest, sqlite bool) error {
	current, err := storestartup.ProcessAuthorityCurrent(ctx, tx, req.Process, sqlite, true)
	if err != nil {
		return err
	}
	if !current {
		return fmt.Errorf("selected stop process possession is no longer current")
	}
	query := `SELECT run_id FROM runs WHERE run_id=$1`
	if !sqlite {
		query += ` FOR UPDATE`
	}
	var runID string
	if err := tx.QueryRowContext(ctx, query, req.Transition.RunID).Scan(&runID); err != nil {
		return fmt.Errorf("lock selected stop run: %w", err)
	}
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, runID)
	if err != nil {
		return fmt.Errorf("read selected stop binding: %w", err)
	}
	if binding.BindingID != req.Binding.BindingID || binding.ForkRunID != req.Binding.ForkRunID ||
		binding.SourceRunID != req.Binding.SourceRunID || binding.ForkEventID != req.Binding.ForkEventID ||
		binding.ContractSelection != req.Binding.ContractSelection || !binding.CreatedAt.Equal(req.Binding.CreatedAt) {
		return fmt.Errorf("selected stop binding changed")
	}
	// Claim and terminalization both lock the run. No execution can enter running
	// after this check and before the canonical run terminal mutation commits.
	var unsettled bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM run_fork_selected_contract_runtime_executions
		WHERE fork_run_id=$1 AND state NOT IN ('prepared','quiesced','failed','closed'))`, runID).Scan(&unsettled); err != nil {
		return err
	}
	if unsettled {
		return fmt.Errorf("selected stop requires accepted execution settlement")
	}
	failure := failures.FromError(context.Canceled, "runtime.run_fork.selected_contract_execution", "stop_before_claim")
	raw, err := json.Marshal(failure.Failure)
	if err != nil {
		return err
	}
	query = `UPDATE run_fork_selected_contract_runtime_executions
		SET state='closed', lease_expires_at=NULL, fence_generation=fence_generation+1, failure=$2, terminal_at=$3, updated_at=$3
		WHERE fork_run_id=$1 AND state='prepared'`
	if _, err := tx.ExecContext(ctx, query, runID, string(raw), time.Now().UTC()); err != nil {
		return fmt.Errorf("settle selected execution cancelled before claim: %w", err)
	}
	return nil
}

func selectedStopTransition(req runcontrol.TransitionRequest) runcontrol.TransitionRequest {
	if req.Now.IsZero() {
		req.Now = time.Now().UTC()
	}
	if req.Reason = strings.TrimSpace(req.Reason); req.Reason == "" {
		req.Reason = "operator_request"
	}
	if req.ControlledBy = strings.TrimSpace(req.ControlledBy); req.ControlledBy == "" {
		req.ControlledBy = "api.v1"
	}
	return req
}
