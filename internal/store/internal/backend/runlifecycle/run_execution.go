package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle/sourceadmission"
)

type runAuthorityOwner interface {
	ObserveCurrentExternalEffectAuthority(context.Context, sourceadmission.ExecutionQuery, runtimeeffects.Authority) (bool, error)
	RequireCurrentExternalEffectAuthorityTx(context.Context, *sql.Tx, runtimeeffects.Authority) error
}

func (s *RunLifecyclePostgresOwner) BindExecutionAuthority(owner runAuthorityOwner) error {
	if s == nil || owner == nil || s.runAuthority != nil {
		return errors.New("run lifecycle PostgreSQL execution authority must be bound exactly once")
	}
	s.runAuthority = owner
	return nil
}

func (s *RunLifecycleSQLiteOwner) BindExecutionAuthority(owner runAuthorityOwner) error {
	if s == nil || owner == nil || s.runAuthority != nil {
		return errors.New("run lifecycle SQLite execution authority must be bound exactly once")
	}
	s.runAuthority = owner
	return nil
}

// Observations select work; they never mint source admission or fence mutations.
func (s *RunLifecyclePostgresOwner) ObserveRunExecution(ctx context.Context, q sourceadmission.ExecutionQuery, runID string) (bool, error) {
	if s == nil {
		return false, errors.New("run execution observation requires lifecycle owner")
	}
	return observeRunExecution(ctx, q, true, runID, s.runAuthority)
}

func (s *RunLifecycleSQLiteOwner) ObserveRunExecution(ctx context.Context, q sourceadmission.ExecutionQuery, runID string) (bool, error) {
	if s == nil {
		return false, errors.New("run execution observation requires lifecycle owner")
	}
	return observeRunExecution(ctx, q, false, runID, s.runAuthority)
}

func observeRunExecution(ctx context.Context, q sourceadmission.ExecutionQuery, postgres bool, runID string, owner runAuthorityOwner) (bool, error) {
	if ctx == nil || q == nil {
		return false, errors.New("run execution observation requires context and query authority")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if runID == "" {
		return !selectedExecutionContext(ctx), nil
	}
	var err error
	var fact runtimecorrelation.SourceArtifactFact
	if postgres {
		fact, err = sourceadmission.ReadPostgresActiveSourceQuery(ctx, q, runID)
	} else {
		fact, err = sourceadmission.ReadSQLiteActiveSourceQuery(ctx, q, runID)
	}
	if errors.Is(err, runtimerunlifecycle.ErrRunNotActive) || errors.Is(err, runtimerunlifecycle.ErrRunNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	selectedRunID, err := selectedRunBinding(ctx, q, postgres, runID)
	if err != nil {
		return false, err
	}
	if selectedRunID == "" {
		return !selectedExecutionContext(ctx), nil
	}
	return observeSelectedRunExecution(ctx, q, postgres, runID, fact, owner)
}

func observeSelectedRunExecution(ctx context.Context, q sourceadmission.ExecutionQuery, postgres bool, runID string, fact runtimecorrelation.SourceArtifactFact, owner runAuthorityOwner) (bool, error) {
	state, err := executionRunState(ctx, q, postgres, runID)
	if err != nil || state != runtimerunlifecycle.StateRunning {
		return false, err
	}
	authority, err := selectedRunAuthority(ctx, runID, fact.BundleHash())
	if err != nil || owner == nil {
		return false, nil
	}
	return owner.ObserveCurrentExternalEffectAuthority(ctx, q, authority)
}

// Mutable admission locks the canonical run before the selected execution fence.
func (s *RunLifecyclePostgresOwner) RequireRunExecutionTx(ctx context.Context, tx *sql.Tx, runID string) error {
	if s == nil || ctx == nil || tx == nil {
		return errors.New("run execution mutation requires lifecycle owner, context and transaction")
	}
	if runID == "" {
		return requireRunlessExecution(ctx)
	}
	fact, sourceErr := s.RequireActiveSourceTx(ctx, tx, runID)
	return requireRunExecutionTx(ctx, tx, true, runID, fact, sourceErr, s.runAuthority)
}

func (s *RunLifecycleSQLiteOwner) RequireRunExecutionTx(ctx context.Context, tx *sql.Tx, runID string) error {
	if s == nil || ctx == nil || tx == nil {
		return errors.New("run execution mutation requires lifecycle owner, context and transaction")
	}
	if runID == "" {
		return requireRunlessExecution(ctx)
	}
	fact, sourceErr := s.RequireActiveSourceTx(ctx, tx, runID)
	return requireRunExecutionTx(ctx, tx, false, runID, fact, sourceErr, s.runAuthority)
}

func requireRunExecutionTx(ctx context.Context, tx *sql.Tx, postgres bool, runID string, fact runtimecorrelation.SourceArtifactFact, sourceErr error, owner runAuthorityOwner) error {
	if sourceErr != nil && !errors.Is(sourceErr, runtimerunlifecycle.ErrRunNotActive) && !errors.Is(sourceErr, runtimerunlifecycle.ErrRunNotFound) {
		return sourceErr
	}
	selectedRunID, err := selectedRunBinding(ctx, tx, postgres, runID)
	if err != nil {
		return err
	}
	if selectedRunID == "" {
		if err := requireRunlessExecution(ctx); err != nil {
			return err
		}
		return sourceErr
	}
	if sourceErr != nil {
		return errors.Join(runtimerunlifecycle.ErrRunExecutionAuthority, sourceErr)
	}
	state, err := executionRunState(ctx, tx, postgres, runID)
	if err != nil {
		return err
	}
	if state != runtimerunlifecycle.StateRunning {
		return fmt.Errorf("%w: selected run %s is %s", runtimerunlifecycle.ErrRunExecutionAuthority, runID, state)
	}
	return requireSelectedRunAuthorityTx(ctx, tx, runID, fact.BundleHash(), owner)
}

func executionRunState(ctx context.Context, q sourceadmission.RowQueryer, postgres bool, runID string) (runtimerunlifecycle.State, error) {
	query := `SELECT status FROM runs WHERE run_id = ?`
	if postgres {
		query = `SELECT status FROM runs WHERE run_id = $1::uuid`
	}
	var state string
	if err := q.QueryRowContext(ctx, query, runID).Scan(&state); err != nil {
		return "", fmt.Errorf("read current run execution state: %w", err)
	}
	return runtimerunlifecycle.ParseState(state)
}

func selectedExecutionContext(ctx context.Context) bool {
	authority, _ := runtimeeffects.AuthorityFromContext(ctx)
	return authority.Kind == runtimeeffects.AuthoritySelectedContractFork
}

func requireRunlessExecution(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if selectedExecutionContext(ctx) {
		return fmt.Errorf("%w: selected authority cannot execute ordinary or runless work", runtimerunlifecycle.ErrRunExecutionAuthority)
	}
	return nil
}

func selectedRunAuthority(ctx context.Context, runID, bundleHash string) (runtimeeffects.Authority, error) {
	authority, present := runtimeeffects.AuthorityFromContext(ctx)
	if !present || !authority.Valid() || authority.Kind != runtimeeffects.AuthoritySelectedContractFork ||
		authority.SelectedFork.ForkRunID != runID || runtimecorrelation.RunIDFromContext(ctx) != runID {
		return runtimeeffects.Authority{}, fmt.Errorf("%w: exact selected run and execution are required", runtimerunlifecycle.ErrRunExecutionAuthority)
	}
	source, admitted := runtimecorrelation.SourceArtifactFactFromContext(ctx)
	if !admitted || bundleHash != "" && source.BundleHash() != bundleHash {
		return runtimeeffects.Authority{}, fmt.Errorf("%w: exact selected source is required", runtimerunlifecycle.ErrRunExecutionAuthority)
	}
	return authority, nil
}

func requireSelectedRunAuthorityTx(ctx context.Context, tx *sql.Tx, runID, bundleHash string, owner runAuthorityOwner) error {
	authority, err := selectedRunAuthority(ctx, runID, bundleHash)
	if err != nil {
		return err
	}
	if owner == nil {
		return fmt.Errorf("%w: selected execution authority owner is required", runtimerunlifecycle.ErrRunExecutionAuthority)
	}
	return owner.RequireCurrentExternalEffectAuthorityTx(ctx, tx, authority)
}
