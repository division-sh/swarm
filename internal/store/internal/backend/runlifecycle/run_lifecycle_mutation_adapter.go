package runlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	privaterunforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
)

type postgresRunLifecycleMutation struct {
	store    *RunLifecyclePostgresOwner
	tx       *sql.Tx
	readOnly bool
	story    runtimeauthoractivity.Mutation
	effects  *privaterunforkrevision.Effects
	handoff  *CandidateHandoff
}

type sqliteRunLifecycleMutation struct {
	store    *RunLifecycleSQLiteOwner
	tx       *sql.Tx
	readOnly bool
	story    runtimeauthoractivity.Mutation
	effects  *privaterunforkrevision.Effects
	handoff  *CandidateHandoff
}

// TransactionMutation exposes the lifecycle operations that legitimately share
// an already-owned lifecycle transaction. Transaction ownership remains with
// the caller's named workflow operation.
type TransactionMutation interface {
	Create(context.Context, runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error)
	TransitionActive(context.Context, runtimerunlifecycle.ActiveTransitionRequest) (runtimerunlifecycle.MutationDisposition, error)
	MarkTerminal(context.Context, runtimerunlifecycle.TerminalRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error)
	SyncCounters(context.Context, string) error
}

func NewPostgresTransactionMutation(owner *RunLifecyclePostgresOwner, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *privaterunforkrevision.Effects) TransactionMutation {
	return postgresRunLifecycleMutation{store: owner, tx: tx, story: story, effects: effects}
}

func NewSQLiteTransactionMutation(owner *RunLifecycleSQLiteOwner, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *privaterunforkrevision.Effects) TransactionMutation {
	return sqliteRunLifecycleMutation{store: owner, tx: tx, story: story, effects: effects}
}

func (s *RunLifecyclePostgresOwner) RequireActiveTx(ctx context.Context, tx *sql.Tx, runID string) error {
	return (postgresRunLifecycleMutation{store: s, tx: tx}).RequireActive(ctx, runID)
}

func (s *RunLifecycleSQLiteOwner) RequireActiveTx(ctx context.Context, tx *sql.Tx, runID string) error {
	return (sqliteRunLifecycleMutation{store: s, tx: tx}).RequireActive(ctx, runID)
}

func (s *RunLifecyclePostgresOwner) RequirePresentTx(ctx context.Context, tx *sql.Tx, runID string) error {
	return (postgresRunLifecycleMutation{store: s, tx: tx}).RequirePresent(ctx, runID)
}

func (s *RunLifecycleSQLiteOwner) RequirePresentTx(ctx context.Context, tx *sql.Tx, runID string) error {
	return (sqliteRunLifecycleMutation{store: s, tx: tx}).RequirePresent(ctx, runID)
}

func (s *RunLifecyclePostgresOwner) SyncCountersTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, runID string) error {
	return (postgresRunLifecycleMutation{store: s, tx: tx, story: story}).SyncCounters(ctx, runID)
}

func (s *RunLifecycleSQLiteOwner) SyncCountersTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, runID string) error {
	return (sqliteRunLifecycleMutation{store: s, tx: tx, story: story}).SyncCounters(ctx, runID)
}

func (s *RunLifecyclePostgresOwner) CreateRunTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, request runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return (postgresRunLifecycleMutation{store: s, tx: tx, story: story}).Create(ctx, request)
}

func (s *RunLifecycleSQLiteOwner) CreateRunTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, request runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return (sqliteRunLifecycleMutation{store: s, tx: tx, story: story}).Create(ctx, request)
}

func (s *RunLifecyclePostgresOwner) TransitionActiveTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, handoff *CandidateHandoff, request runtimerunlifecycle.ActiveTransitionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return (postgresRunLifecycleMutation{store: s, tx: tx, story: story, handoff: handoff}).TransitionActive(ctx, request)
}

func (s *RunLifecycleSQLiteOwner) TransitionActiveTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, handoff *CandidateHandoff, request runtimerunlifecycle.ActiveTransitionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return (sqliteRunLifecycleMutation{store: s, tx: tx, story: story, handoff: handoff}).TransitionActive(ctx, request)
}

func (s *RunLifecyclePostgresOwner) MarkTerminalTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *privaterunforkrevision.Effects, request runtimerunlifecycle.TerminalRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return (postgresRunLifecycleMutation{store: s, tx: tx, story: story, effects: effects}).MarkTerminal(ctx, request)
}

func (s *RunLifecycleSQLiteOwner) MarkTerminalTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *privaterunforkrevision.Effects, request runtimerunlifecycle.TerminalRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return (sqliteRunLifecycleMutation{store: s, tx: tx, story: story, effects: effects}).MarkTerminal(ctx, request)
}

func (s *RunLifecyclePostgresOwner) ForkSourceTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *privaterunforkrevision.Effects, request runtimerunlifecycle.ForkSourceRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return (postgresRunLifecycleMutation{store: s, tx: tx, story: story, effects: effects}).ForkSource(ctx, request)
}

func (s *RunLifecycleSQLiteOwner) ForkSourceTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, effects *privaterunforkrevision.Effects, request runtimerunlifecycle.ForkSourceRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return (sqliteRunLifecycleMutation{store: s, tx: tx, story: story, effects: effects}).ForkSource(ctx, request)
}

func (s *RunLifecyclePostgresOwner) ReviseSourceTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, handoff *CandidateHandoff, request runtimerunlifecycle.SourceRevisionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return (postgresRunLifecycleMutation{store: s, tx: tx, story: story, handoff: handoff}).ReviseSource(ctx, request)
}

func (s *RunLifecycleSQLiteOwner) ReviseSourceTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, handoff *CandidateHandoff, request runtimerunlifecycle.SourceRevisionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return (sqliteRunLifecycleMutation{store: s, tx: tx, story: story, handoff: handoff}).ReviseSource(ctx, request)
}

func (s *RunLifecyclePostgresOwner) RequireActiveSourceTx(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	return (postgresRunLifecycleMutation{store: s, tx: tx}).RequireActiveSource(ctx, runID)
}

func (s *RunLifecycleSQLiteOwner) RequireActiveSourceTx(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	return (sqliteRunLifecycleMutation{store: s, tx: tx}).RequireActiveSource(ctx, runID)
}

func (s *RunLifecyclePostgresOwner) RequirePresentSourceTx(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	return (postgresRunLifecycleMutation{store: s, tx: tx}).RequirePresentSource(ctx, runID)
}

func (s *RunLifecycleSQLiteOwner) RequirePresentSourceTx(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	return (sqliteRunLifecycleMutation{store: s, tx: tx}).RequirePresentSource(ctx, runID)
}

func runPostgresLifecycleOperation[T any](
	ctx context.Context,
	store *RunLifecyclePostgresOwner,
	fn func(context.Context, postgresRunLifecycleMutation) (T, error),
) (T, error) {
	return WithCandidateHandoffResult(ctx, func(handoff *CandidateHandoff) (T, error) {
		var result T
		effects := privaterunforkrevision.NewEffects()
		err := store.runPrivateAuthorActivityMutation(ctx, effects, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			var err error
			result, err = fn(txctx, postgresRunLifecycleMutation{
				store: store, tx: tx, story: runtimeAuthorActivityMutation(story), effects: effects, handoff: handoff,
			})
			return err
		})
		return result, err
	})
}

func runSQLiteLifecycleOperation[T any](
	ctx context.Context,
	store *RunLifecycleSQLiteOwner,
	fn func(context.Context, sqliteRunLifecycleMutation) (T, error),
) (T, error) {
	return WithCandidateHandoffResult(ctx, func(handoff *CandidateHandoff) (T, error) {
		var result T
		effects := privaterunforkrevision.NewEffects()
		err := store.runPrivateAuthorActivityMutation(ctx, "sqlite run lifecycle operation", effects, func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			var err error
			result, err = fn(txctx, sqliteRunLifecycleMutation{
				store: store, tx: tx, story: runtimeAuthorActivityMutation(story), effects: effects, handoff: handoff,
			})
			return err
		})
		return result, err
	})
}

func runPostgresLifecycleRead[T any](
	ctx context.Context,
	store *RunLifecyclePostgresOwner,
	fn func(context.Context, postgresRunLifecycleMutation) (T, error),
) (T, error) {
	var result T
	err := store.runRead(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = fn(txctx, postgresRunLifecycleMutation{store: store, tx: tx, readOnly: true})
		return err
	})
	return result, err
}

func runSQLiteLifecycleRead[T any](
	ctx context.Context,
	store *RunLifecycleSQLiteOwner,
	fn func(context.Context, sqliteRunLifecycleMutation) (T, error),
) (T, error) {
	var result T
	err := store.runRead(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		result, err = fn(txctx, sqliteRunLifecycleMutation{store: store, tx: tx, readOnly: true})
		return err
	})
	return result, err
}

func (s *RunLifecyclePostgresOwner) TransitionActiveRun(ctx context.Context, request runtimerunlifecycle.ActiveTransitionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return runPostgresLifecycleOperation(ctx, s, func(ctx context.Context, mutation postgresRunLifecycleMutation) (runtimerunlifecycle.MutationDisposition, error) {
		return mutation.TransitionActive(ctx, request)
	})
}

func (s *RunLifecycleSQLiteOwner) TransitionActiveRun(ctx context.Context, request runtimerunlifecycle.ActiveTransitionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return runSQLiteLifecycleOperation(ctx, s, func(ctx context.Context, mutation sqliteRunLifecycleMutation) (runtimerunlifecycle.MutationDisposition, error) {
		return mutation.TransitionActive(ctx, request)
	})
}

func (s *RunLifecyclePostgresOwner) RequirePresentRun(ctx context.Context, runID string) error {
	_, err := runPostgresLifecycleRead(ctx, s, func(ctx context.Context, mutation postgresRunLifecycleMutation) (struct{}, error) {
		return struct{}{}, mutation.RequirePresent(ctx, runID)
	})
	return err
}

func (s *RunLifecycleSQLiteOwner) RequirePresentRun(ctx context.Context, runID string) error {
	_, err := runSQLiteLifecycleRead(ctx, s, func(ctx context.Context, mutation sqliteRunLifecycleMutation) (struct{}, error) {
		return struct{}{}, mutation.RequirePresent(ctx, runID)
	})
	return err
}

func (s *RunLifecyclePostgresOwner) RequireActiveRun(ctx context.Context, runID string) error {
	_, err := runPostgresLifecycleRead(ctx, s, func(ctx context.Context, mutation postgresRunLifecycleMutation) (struct{}, error) {
		return struct{}{}, mutation.RequireActive(ctx, runID)
	})
	return err
}

func (s *RunLifecycleSQLiteOwner) RequireActiveRun(ctx context.Context, runID string) error {
	_, err := runSQLiteLifecycleRead(ctx, s, func(ctx context.Context, mutation sqliteRunLifecycleMutation) (struct{}, error) {
		return struct{}{}, mutation.RequireActive(ctx, runID)
	})
	return err
}

// RequirePublicationRunActive performs the closed preflight used before route
// planning. The event commit repeats this check in its own transaction; this
// operation only guarantees that terminal-run refusal cannot be shadowed by a
// later route-planning error.
func (s *RunLifecyclePostgresOwner) RequirePublicationRunActive(ctx context.Context, runID string) error {
	return s.runRead(ctx, func(txctx context.Context, tx *sql.Tx) error {
		return (postgresRunLifecycleMutation{store: s, tx: tx, readOnly: true}).RequireActive(txctx, runID)
	})
}

func (s *RunLifecycleSQLiteOwner) RequirePublicationRunActive(ctx context.Context, runID string) error {
	return s.runRead(ctx, func(txctx context.Context, tx *sql.Tx) error {
		return (sqliteRunLifecycleMutation{store: s, tx: tx, readOnly: true}).RequireActive(txctx, runID)
	})
}

func (s *RunLifecyclePostgresOwner) RequirePresentRunSource(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	return runPostgresLifecycleRead(ctx, s, func(ctx context.Context, mutation postgresRunLifecycleMutation) (runtimecorrelation.SourceArtifactFact, error) {
		return mutation.RequirePresentSource(ctx, runID)
	})
}

func (s *RunLifecycleSQLiteOwner) RequirePresentRunSource(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	return runSQLiteLifecycleRead(ctx, s, func(ctx context.Context, mutation sqliteRunLifecycleMutation) (runtimecorrelation.SourceArtifactFact, error) {
		return mutation.RequirePresentSource(ctx, runID)
	})
}

func (s *RunLifecyclePostgresOwner) RequireActiveRunSource(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	return runPostgresLifecycleRead(ctx, s, func(ctx context.Context, mutation postgresRunLifecycleMutation) (runtimecorrelation.SourceArtifactFact, error) {
		return mutation.RequireActiveSource(ctx, runID)
	})
}

func (s *RunLifecycleSQLiteOwner) RequireActiveRunSource(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	return runSQLiteLifecycleRead(ctx, s, func(ctx context.Context, mutation sqliteRunLifecycleMutation) (runtimecorrelation.SourceArtifactFact, error) {
		return mutation.RequireActiveSource(ctx, runID)
	})
}

func (s *RunLifecyclePostgresOwner) CreateRun(ctx context.Context, request runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return runPostgresLifecycleOperation(ctx, s, func(ctx context.Context, mutation postgresRunLifecycleMutation) (runtimerunlifecycle.MutationDisposition, error) {
		return mutation.Create(ctx, request)
	})
}

func (s *RunLifecycleSQLiteOwner) CreateRun(ctx context.Context, request runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return runSQLiteLifecycleOperation(ctx, s, func(ctx context.Context, mutation sqliteRunLifecycleMutation) (runtimerunlifecycle.MutationDisposition, error) {
		return mutation.Create(ctx, request)
	})
}

func (s *RunLifecyclePostgresOwner) ForkRunSource(ctx context.Context, request runtimerunlifecycle.ForkSourceRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	type result struct {
		snapshot    runtimerunlifecycle.Snapshot
		disposition runtimerunlifecycle.MutationDisposition
	}
	value, err := runPostgresLifecycleOperation(ctx, s, func(ctx context.Context, mutation postgresRunLifecycleMutation) (result, error) {
		snapshot, disposition, err := mutation.ForkSource(ctx, request)
		return result{snapshot: snapshot, disposition: disposition}, err
	})
	return value.snapshot, value.disposition, err
}

func (s *RunLifecycleSQLiteOwner) ForkRunSource(ctx context.Context, request runtimerunlifecycle.ForkSourceRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	type result struct {
		snapshot    runtimerunlifecycle.Snapshot
		disposition runtimerunlifecycle.MutationDisposition
	}
	value, err := runSQLiteLifecycleOperation(ctx, s, func(ctx context.Context, mutation sqliteRunLifecycleMutation) (result, error) {
		snapshot, disposition, err := mutation.ForkSource(ctx, request)
		return result{snapshot: snapshot, disposition: disposition}, err
	})
	return value.snapshot, value.disposition, err
}

func (s *RunLifecyclePostgresOwner) ReviseRunSource(ctx context.Context, request runtimerunlifecycle.SourceRevisionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return runPostgresLifecycleOperation(ctx, s, func(ctx context.Context, mutation postgresRunLifecycleMutation) (runtimerunlifecycle.MutationDisposition, error) {
		return mutation.ReviseSource(ctx, request)
	})
}

func (s *RunLifecycleSQLiteOwner) ReviseRunSource(ctx context.Context, request runtimerunlifecycle.SourceRevisionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return runSQLiteLifecycleOperation(ctx, s, func(ctx context.Context, mutation sqliteRunLifecycleMutation) (runtimerunlifecycle.MutationDisposition, error) {
		return mutation.ReviseSource(ctx, request)
	})
}

func (s *RunLifecyclePostgresOwner) SyncRunCounters(ctx context.Context, runID string) error {
	_, err := runPostgresLifecycleOperation(ctx, s, func(ctx context.Context, mutation postgresRunLifecycleMutation) (struct{}, error) {
		return struct{}{}, mutation.SyncCounters(ctx, runID)
	})
	return err
}

func (s *RunLifecycleSQLiteOwner) SyncRunCounters(ctx context.Context, runID string) error {
	_, err := runSQLiteLifecycleOperation(ctx, s, func(ctx context.Context, mutation sqliteRunLifecycleMutation) (struct{}, error) {
		return struct{}{}, mutation.SyncCounters(ctx, runID)
	})
	return err
}

func (m postgresRunLifecycleMutation) RequirePresent(ctx context.Context, runID string) error {
	if m.tx == nil {
		return errors.New("PostgreSQL run lifecycle mutation requires transaction")
	}
	var state string
	query := `
		SELECT status
		FROM runs
		WHERE run_id = $1::uuid
	`
	if !m.readOnly {
		query += ` FOR UPDATE`
	}
	err := m.tx.QueryRowContext(ctx, query, strings.TrimSpace(runID)).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return fmt.Errorf("require PostgreSQL run: %w", err)
	}
	_, err = runtimerunlifecycle.ParseState(state)
	return err
}

func (m sqliteRunLifecycleMutation) RequirePresent(ctx context.Context, runID string) error {
	if m.tx == nil {
		return errors.New("SQLite run lifecycle mutation requires transaction")
	}
	var state string
	err := m.tx.QueryRowContext(ctx, `
		SELECT status
		FROM runs
		WHERE run_id = ?
	`, strings.TrimSpace(runID)).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return fmt.Errorf("require SQLite run: %w", err)
	}
	_, err = runtimerunlifecycle.ParseState(state)
	return err
}

func (m postgresRunLifecycleMutation) RequireActive(ctx context.Context, runID string) error {
	_, err := m.RequireActiveSource(ctx, runID)
	return err
}

func (m sqliteRunLifecycleMutation) RequireActive(ctx context.Context, runID string) error {
	_, err := m.RequireActiveSource(ctx, runID)
	return err
}

func (m postgresRunLifecycleMutation) RequirePresentSource(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	return m.loadSource(ctx, runID, false)
}

func (m sqliteRunLifecycleMutation) RequirePresentSource(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	return m.loadSource(ctx, runID, false)
}

func (m postgresRunLifecycleMutation) RequireActiveSource(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	return m.loadSource(ctx, runID, true)
}

func (m sqliteRunLifecycleMutation) RequireActiveSource(ctx context.Context, runID string) (runtimecorrelation.SourceArtifactFact, error) {
	return m.loadSource(ctx, runID, true)
}

func (m postgresRunLifecycleMutation) loadSource(
	ctx context.Context,
	runID string,
	requireActive bool,
) (runtimecorrelation.SourceArtifactFact, error) {
	if m.tx == nil {
		return runtimecorrelation.SourceArtifactFact{}, errors.New("PostgreSQL run lifecycle mutation requires transaction")
	}
	runID = strings.TrimSpace(runID)
	var state, bundleHash string
	query := `
		SELECT status, bundle_hash
		FROM runs
		WHERE run_id = $1::uuid
	`
	if !m.readOnly {
		query += ` FOR UPDATE`
	}
	err := m.tx.QueryRowContext(ctx, query, runID).Scan(&state, &bundleHash)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimecorrelation.SourceArtifactFact{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("load PostgreSQL run lifecycle source: %w", err)
	}
	parsed, err := runtimerunlifecycle.ParseState(state)
	if err != nil {
		return runtimecorrelation.SourceArtifactFact{}, err
	}
	if requireActive && !parsed.Active() {
		return runtimecorrelation.SourceArtifactFact{}, &runtimerunlifecycle.RunNotActiveError{RunID: runID, State: parsed}
	}
	fact, err := runtimecorrelation.DecodeSourceArtifactFact(bundleHash)
	if err != nil {
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("decode PostgreSQL run lifecycle source: %w", err)
	}
	if err := m.requireSourceArtifact(ctx, fact); err != nil {
		return runtimecorrelation.SourceArtifactFact{}, err
	}
	return fact, nil
}

func (m sqliteRunLifecycleMutation) loadSource(
	ctx context.Context,
	runID string,
	requireActive bool,
) (runtimecorrelation.SourceArtifactFact, error) {
	if m.tx == nil {
		return runtimecorrelation.SourceArtifactFact{}, errors.New("SQLite run lifecycle mutation requires transaction")
	}
	runID = strings.TrimSpace(runID)
	var state, bundleHash string
	err := m.tx.QueryRowContext(ctx, `
		SELECT status, bundle_hash
		FROM runs
		WHERE run_id = ?
	`, runID).Scan(&state, &bundleHash)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimecorrelation.SourceArtifactFact{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("load SQLite run lifecycle source: %w", err)
	}
	parsed, err := runtimerunlifecycle.ParseState(state)
	if err != nil {
		return runtimecorrelation.SourceArtifactFact{}, err
	}
	if requireActive && !parsed.Active() {
		return runtimecorrelation.SourceArtifactFact{}, &runtimerunlifecycle.RunNotActiveError{RunID: runID, State: parsed}
	}
	fact, err := runtimecorrelation.DecodeSourceArtifactFact(bundleHash)
	if err != nil {
		return runtimecorrelation.SourceArtifactFact{}, fmt.Errorf("decode SQLite run lifecycle source: %w", err)
	}
	if err := m.requireSourceArtifact(ctx, fact); err != nil {
		return runtimecorrelation.SourceArtifactFact{}, err
	}
	return fact, nil
}

func (m postgresRunLifecycleMutation) requireSourceArtifact(ctx context.Context, fact runtimecorrelation.SourceArtifactFact) error {
	var exists bool
	if err := m.tx.QueryRowContext(ctx, `
		SELECT EXISTS (SELECT 1 FROM source_artifacts WHERE bundle_hash = $1)
	`, fact.BundleHash()).Scan(&exists); err != nil {
		return fmt.Errorf("validate PostgreSQL run source artifact: %w", err)
	}
	if !exists {
		return &runtimerunlifecycle.SourceArtifactUnavailableError{
			BundleHash: fact.BundleHash(),
			Cause:      "missing_source_artifact",
		}
	}
	return nil
}

func (m sqliteRunLifecycleMutation) requireSourceArtifact(ctx context.Context, fact runtimecorrelation.SourceArtifactFact) error {
	var exists bool
	if err := m.tx.QueryRowContext(ctx, `
		SELECT EXISTS (SELECT 1 FROM source_artifacts WHERE bundle_hash = ?)
	`, fact.BundleHash()).Scan(&exists); err != nil {
		return fmt.Errorf("validate SQLite run source artifact: %w", err)
	}
	if !exists {
		return &runtimerunlifecycle.SourceArtifactUnavailableError{
			BundleHash: fact.BundleHash(),
			Cause:      "missing_source_artifact",
		}
	}
	return nil
}

func (m postgresRunLifecycleMutation) Create(
	ctx context.Context,
	request runtimerunlifecycle.CreateRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	if m.tx == nil {
		return "", errors.New("PostgreSQL run lifecycle creation requires transaction")
	}
	request.StartedAt = runtimerunlifecycle.CanonicalTimestamp(request.StartedAt)
	if err := request.Validate(); err != nil {
		return "", err
	}
	if err := m.requireSourceArtifactForWrite(ctx, request.Source); err != nil {
		return "", err
	}
	bundleHash := request.Source.BundleHash()
	origin := request.Origin
	var inserted bool
	err := m.tx.QueryRowContext(ctx, `
		INSERT INTO runs (
			run_id, status, bundle_hash, origin_kind,
			trigger_event_id, trigger_event_type,
			origin_service_id, origin_generation,
			forked_from_run_id, forked_from_event_id,
			started_at
		)
		VALUES (
			$1::uuid, 'running', $2, $3,
			NULLIF($4, '')::uuid, NULLIF($5, ''),
			NULLIF($6, '')::uuid, NULLIF($7, 0),
			NULLIF($8, '')::uuid, NULLIF($9, '')::uuid,
			$10
		)
		ON CONFLICT (run_id) DO NOTHING
		RETURNING TRUE
	`, request.RunID, bundleHash, origin.Kind(),
		origin.EventID(), origin.EventType(), origin.ServiceID(), origin.Generation(),
		origin.SourceRunID(), origin.SourceEventID(), request.StartedAt.UTC()).Scan(&inserted)
	if errors.Is(err, sql.ErrNoRows) {
		return m.classifyCreateExisting(ctx, request)
	}
	if err != nil {
		return "", fmt.Errorf("create PostgreSQL run lifecycle: %w", err)
	}
	if !inserted {
		return runtimerunlifecycle.MutationExactNoop, nil
	}
	if err := m.recordRunStarted(ctx, request); err != nil {
		return "", err
	}
	return runtimerunlifecycle.MutationApplied, nil
}

func (m sqliteRunLifecycleMutation) Create(
	ctx context.Context,
	request runtimerunlifecycle.CreateRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	if m.tx == nil {
		return "", errors.New("SQLite run lifecycle creation requires transaction")
	}
	request.StartedAt = runtimerunlifecycle.CanonicalTimestamp(request.StartedAt)
	if err := request.Validate(); err != nil {
		return "", err
	}
	if err := m.requireSourceArtifact(ctx, request.Source); err != nil {
		return "", err
	}
	bundleHash := request.Source.BundleHash()
	origin := request.Origin
	result, err := m.tx.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, status, bundle_hash, origin_kind,
			trigger_event_id, trigger_event_type,
			origin_service_id, origin_generation,
			forked_from_run_id, forked_from_event_id,
			started_at
		)
		VALUES (
			?, 'running', ?, ?,
			NULLIF(?, ''), NULLIF(?, ''),
			NULLIF(?, ''), NULLIF(?, 0),
			NULLIF(?, ''), NULLIF(?, ''),
			?
		)
		ON CONFLICT (run_id) DO NOTHING
	`, request.RunID, bundleHash, origin.Kind(),
		origin.EventID(), origin.EventType(), origin.ServiceID(), origin.Generation(),
		origin.SourceRunID(), origin.SourceEventID(), request.StartedAt.UTC())
	if err != nil {
		return "", fmt.Errorf("create SQLite run lifecycle: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return "", fmt.Errorf("inspect SQLite run lifecycle creation result: %w", err)
	}
	if rows == 0 {
		return m.classifyCreateExisting(ctx, request)
	}
	if err := m.recordRunStarted(ctx, request); err != nil {
		return "", err
	}
	return runtimerunlifecycle.MutationApplied, nil
}

func (s *RunLifecyclePostgresOwner) InsertRunForkRunTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	forkRunID, sourceRunID, forkEventID string,
	entityCount int,
	startedAt time.Time,
	identity runtimecorrelation.SourceArtifactFact,
) error {
	if tx == nil {
		return errors.New("fork run lifecycle creation requires transaction")
	}
	if err := identity.Validate(); err != nil {
		return fmt.Errorf("fork run lifecycle creation requires canonical executable bundle identity: %w", err)
	}
	if story == nil {
		return errors.New("fork run lifecycle creation requires private story ownership")
	}
	mutation := postgresRunLifecycleMutation{store: s, tx: tx}
	if err := mutation.requireSourceArtifactForWrite(ctx, identity); err != nil {
		return err
	}
	bundleHash := identity.BundleHash()
	origin, err := runtimerunlifecycle.ForkMaterializationRunOrigin(sourceRunID, forkEventID)
	if err != nil {
		return err
	}
	startedAt = runtimerunlifecycle.CanonicalTimestamp(startedAt)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, status, origin_kind, forked_from_run_id, forked_from_event_id,
			entity_count, event_count, started_at, bundle_hash
		)
		VALUES ($1::uuid, $2, $3, $4::uuid, $5::uuid, $6, 0, $7, $8)
	`, forkRunID, string(runtimerunlifecycle.StatePaused), origin.Kind(), origin.SourceRunID(), origin.SourceEventID(),
		entityCount, startedAt.UTC(), bundleHash); err != nil {
		return fmt.Errorf("insert fork run lifecycle: %w", err)
	}
	scope, err := runtimeauthoractivity.BundleScopeForSource(ctx, bundleHash)
	if err != nil {
		return err
	}
	return story.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindRunLifecycle, Transition: "fork_prepared",
		SourceOwner: "runs", SourceIdentity: forkRunID, DedupKey: "run-created:" + forkRunID,
		OccurredAt: startedAt.UTC(), RunID: forkRunID, Scope: scope,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "run", SubjectID: forkRunID, ParentRunID: sourceRunID, TriggerEventType: "run.fork",
		},
	})
}

func (s *RunLifecycleSQLiteOwner) InsertRunForkRunTx(
	ctx context.Context,
	tx *sql.Tx,
	story runtimeauthoractivity.Mutation,
	forkRunID, sourceRunID, forkEventID string,
	entityCount int,
	startedAt time.Time,
	identity runtimecorrelation.SourceArtifactFact,
) error {
	if tx == nil {
		return errors.New("fork run lifecycle creation requires transaction")
	}
	if err := identity.Validate(); err != nil {
		return fmt.Errorf("fork run lifecycle creation requires canonical executable bundle identity: %w", err)
	}
	if story == nil {
		return errors.New("fork run lifecycle creation requires private story ownership")
	}
	mutation := sqliteRunLifecycleMutation{store: s, tx: tx}
	if err := mutation.requireSourceArtifact(ctx, identity); err != nil {
		return err
	}
	bundleHash := identity.BundleHash()
	origin, err := runtimerunlifecycle.ForkMaterializationRunOrigin(sourceRunID, forkEventID)
	if err != nil {
		return err
	}
	startedAt = runtimerunlifecycle.CanonicalTimestamp(startedAt)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, status, origin_kind, forked_from_run_id, forked_from_event_id,
			entity_count, event_count, started_at, bundle_hash
		)
		VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)
	`, forkRunID, string(runtimerunlifecycle.StatePaused), origin.Kind(), origin.SourceRunID(), origin.SourceEventID(),
		entityCount, startedAt.UTC(), bundleHash); err != nil {
		return fmt.Errorf("insert fork run lifecycle: %w", err)
	}
	scope, err := runtimeauthoractivity.BundleScopeForSource(ctx, bundleHash)
	if err != nil {
		return err
	}
	return story.Record(ctx, runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindRunLifecycle, Transition: "fork_prepared",
		SourceOwner: "runs", SourceIdentity: forkRunID, DedupKey: "run-created:" + forkRunID,
		OccurredAt: startedAt.UTC(), RunID: forkRunID, Scope: scope,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "run", SubjectID: forkRunID, ParentRunID: sourceRunID, TriggerEventType: "run.fork",
		},
	})
}

func (m postgresRunLifecycleMutation) TransitionActive(
	ctx context.Context,
	request runtimerunlifecycle.ActiveTransitionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	if m.tx == nil {
		return "", errors.New("PostgreSQL active run lifecycle transition requires transaction")
	}
	if err := request.Validate(); err != nil {
		return "", err
	}
	current, err := loadPostgresRunLifecycleSnapshot(ctx, m.tx, request.RunID, true)
	if err != nil {
		return "", err
	}
	if current.State == request.State {
		return runtimerunlifecycle.MutationExactNoop, nil
	}
	if !current.State.Active() {
		return "", &runtimerunlifecycle.RunNotActiveError{RunID: request.RunID, State: current.State}
	}
	if current.State == runtimerunlifecycle.StateRunning && request.State != runtimerunlifecycle.StatePaused {
		return "", fmt.Errorf("invalid PostgreSQL run lifecycle transition %s -> %s", current.State, request.State)
	}
	if current.State == runtimerunlifecycle.StatePaused && request.State != runtimerunlifecycle.StateRunning {
		return "", fmt.Errorf("invalid PostgreSQL run lifecycle transition %s -> %s", current.State, request.State)
	}
	result, err := transitionPostgresActiveRunStateTx(
		ctx,
		m.tx,
		request.RunID,
		request.State,
		current.State,
	)
	if err != nil {
		return "", fmt.Errorf("transition PostgreSQL run lifecycle: %w", err)
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		return "", errors.Join(fmt.Errorf("transition PostgreSQL run lifecycle affected %d rows", rows), rowsErr)
	}
	if request.State == runtimerunlifecycle.StateRunning {
		if m.store == nil {
			return "", errors.New("PostgreSQL run lifecycle resume requires selected-store candidate owner")
		}
		if _, err := m.store.RequestCompletionCandidateTx(ctx, m.tx, request.RunID, nil, m.handoff); err != nil {
			return "", err
		}
	}
	return runtimerunlifecycle.MutationApplied, nil
}

func (m sqliteRunLifecycleMutation) TransitionActive(
	ctx context.Context,
	request runtimerunlifecycle.ActiveTransitionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	if m.tx == nil {
		return "", errors.New("SQLite active run lifecycle transition requires transaction")
	}
	if err := request.Validate(); err != nil {
		return "", err
	}
	current, err := loadSQLiteRunLifecycleSnapshot(ctx, m.tx, request.RunID)
	if err != nil {
		return "", err
	}
	if current.State == request.State {
		return runtimerunlifecycle.MutationExactNoop, nil
	}
	if !current.State.Active() {
		return "", &runtimerunlifecycle.RunNotActiveError{RunID: request.RunID, State: current.State}
	}
	if current.State == runtimerunlifecycle.StateRunning && request.State != runtimerunlifecycle.StatePaused {
		return "", fmt.Errorf("invalid SQLite run lifecycle transition %s -> %s", current.State, request.State)
	}
	if current.State == runtimerunlifecycle.StatePaused && request.State != runtimerunlifecycle.StateRunning {
		return "", fmt.Errorf("invalid SQLite run lifecycle transition %s -> %s", current.State, request.State)
	}
	result, err := transitionSQLiteActiveRunStateTx(
		ctx,
		m.tx,
		request.RunID,
		request.State,
		current.State,
	)
	if err != nil {
		return "", fmt.Errorf("transition SQLite run lifecycle: %w", err)
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		return "", errors.Join(fmt.Errorf("transition SQLite run lifecycle affected %d rows", rows), rowsErr)
	}
	if request.State == runtimerunlifecycle.StateRunning {
		if m.store == nil {
			return "", errors.New("SQLite run lifecycle resume requires selected-store candidate owner")
		}
		if _, err := m.store.RequestCompletionCandidateTx(ctx, m.tx, request.RunID, nil, m.handoff); err != nil {
			return "", err
		}
	}
	return runtimerunlifecycle.MutationApplied, nil
}

func (m postgresRunLifecycleMutation) MarkTerminal(
	ctx context.Context,
	request runtimerunlifecycle.TerminalRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if m.store == nil {
		return runtimerunlifecycle.Snapshot{}, "", errors.New("PostgreSQL terminal run lifecycle transition requires selected store")
	}
	return m.store.markRunTerminalTx(ctx, m.tx, m.story, m.effects, request)
}

func (m sqliteRunLifecycleMutation) MarkTerminal(
	ctx context.Context,
	request runtimerunlifecycle.TerminalRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if m.store == nil {
		return runtimerunlifecycle.Snapshot{}, "", errors.New("SQLite terminal run lifecycle transition requires selected store")
	}
	return m.store.markRunTerminalTx(ctx, m.tx, m.story, m.effects, request)
}

func (m postgresRunLifecycleMutation) ForkSource(
	ctx context.Context,
	request runtimerunlifecycle.ForkSourceRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if m.store == nil {
		return runtimerunlifecycle.Snapshot{}, "", errors.New("PostgreSQL fork source lifecycle transition requires selected store")
	}
	return m.store.markForkSourceTx(ctx, m.tx, m.story, m.effects, request)
}

func (m sqliteRunLifecycleMutation) ForkSource(
	ctx context.Context,
	request runtimerunlifecycle.ForkSourceRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if m.store == nil {
		return runtimerunlifecycle.Snapshot{}, "", errors.New("SQLite fork source lifecycle transition requires selected store")
	}
	return m.store.markForkSourceTx(ctx, m.tx, m.story, m.effects, request)
}

func (m postgresRunLifecycleMutation) classifyCreateExisting(
	ctx context.Context,
	request runtimerunlifecycle.CreateRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	current, err := loadPostgresRunLifecycleSnapshot(ctx, m.tx, request.RunID, true)
	if err != nil {
		return "", err
	}
	return classifyCreateExisting(request, current)
}

func (m sqliteRunLifecycleMutation) classifyCreateExisting(
	ctx context.Context,
	request runtimerunlifecycle.CreateRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	current, err := loadSQLiteRunLifecycleSnapshot(ctx, m.tx, request.RunID)
	if err != nil {
		return "", err
	}
	return classifyCreateExisting(request, current)
}

func classifyCreateExisting(
	request runtimerunlifecycle.CreateRequest,
	current runtimerunlifecycle.Snapshot,
) (runtimerunlifecycle.MutationDisposition, error) {
	wantHash := request.Source.BundleHash()
	if strings.TrimSpace(current.BundleHash) != wantHash {
		return "", fmt.Errorf(
			"run lifecycle source conflict for run_id=%s: stored=%s requested=%s",
			request.RunID, current.BundleHash, wantHash,
		)
	}
	if !current.State.Active() {
		return "", &runtimerunlifecycle.RunNotActiveError{RunID: request.RunID, State: current.State}
	}
	if !current.Origin.Equal(request.Origin) {
		return "", fmt.Errorf(
			"run lifecycle origin conflict for run_id=%s: stored=%s requested=%s",
			request.RunID, current.Origin.Kind(), request.Origin.Kind(),
		)
	}
	return runtimerunlifecycle.MutationExactNoop, nil
}

func (m postgresRunLifecycleMutation) requireSourceArtifactForWrite(
	ctx context.Context,
	fact runtimecorrelation.SourceArtifactFact,
) error {
	if _, err := m.tx.ExecContext(ctx, `LOCK TABLE runs IN ROW EXCLUSIVE MODE`); err != nil {
		return fmt.Errorf("serialize PostgreSQL run source admission: %w", err)
	}
	return m.requireSourceArtifact(ctx, fact)
}

func (m postgresRunLifecycleMutation) recordRunStarted(ctx context.Context, request runtimerunlifecycle.CreateRequest) error {
	return recordRunStartedWithStory(ctx, m.story, request)
}

func (m sqliteRunLifecycleMutation) recordRunStarted(ctx context.Context, request runtimerunlifecycle.CreateRequest) error {
	return recordRunStartedWithStory(ctx, m.story, request)
}

func recordRunStartedWithStory(ctx context.Context, story runtimeauthoractivity.Mutation, request runtimerunlifecycle.CreateRequest) error {
	scope, err := runtimeauthoractivity.BundleScopeForSource(ctx, request.Source.BundleHash())
	if err != nil {
		return fmt.Errorf("record run lifecycle creation: %w", err)
	}
	draft := runtimeauthoractivity.Draft{
		Kind: runtimeauthoractivity.KindRunLifecycle, Transition: "started",
		SourceOwner: "runs", SourceIdentity: request.RunID, DedupKey: "run-created:" + request.RunID,
		OccurredAt: request.StartedAt.UTC(), RunID: request.RunID, Scope: scope,
		Projection: runtimeauthoractivity.Projection{
			SubjectType: "run", SubjectID: request.RunID,
			TriggerEventType: request.Origin.ActivityTriggerType(),
		},
	}
	if story != nil {
		return story.Record(ctx, draft)
	}
	return errors.New("run lifecycle creation requires private story ownership")
}

func (m postgresRunLifecycleMutation) ReviseSource(
	ctx context.Context,
	request runtimerunlifecycle.SourceRevisionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	current, err := m.RequireActiveSource(ctx, request.RunID)
	if err != nil {
		return "", err
	}
	if current.Matches(request.Source) {
		return runtimerunlifecycle.MutationExactNoop, nil
	}
	if err := m.requireSourceArtifactForWrite(ctx, request.Source); err != nil {
		return "", err
	}
	bundleHash := request.Source.BundleHash()
	result, err := m.tx.ExecContext(ctx, `
		UPDATE runs
		SET bundle_hash = $2
		WHERE run_id = $1::uuid
		  AND status IN (`+runLifecycleActiveStateSQLValues+`)
	`, request.RunID, bundleHash)
	if err != nil {
		return "", fmt.Errorf("revise PostgreSQL run lifecycle source: %w", err)
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		return "", errors.Join(fmt.Errorf("revise PostgreSQL run lifecycle source affected %d rows", rows), rowsErr)
	}
	if _, err := m.store.RequestCompletionCandidateTx(ctx, m.tx, request.RunID, nil, m.handoff); err != nil {
		return "", err
	}
	return runtimerunlifecycle.MutationApplied, nil
}

func (m sqliteRunLifecycleMutation) ReviseSource(
	ctx context.Context,
	request runtimerunlifecycle.SourceRevisionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	current, err := m.RequireActiveSource(ctx, request.RunID)
	if err != nil {
		return "", err
	}
	if current.Matches(request.Source) {
		return runtimerunlifecycle.MutationExactNoop, nil
	}
	if err := m.requireSourceArtifact(ctx, request.Source); err != nil {
		return "", err
	}
	bundleHash := request.Source.BundleHash()
	result, err := m.tx.ExecContext(ctx, `
		UPDATE runs
		SET bundle_hash = ?
		WHERE run_id = ?
		  AND status IN (`+runLifecycleActiveStateSQLValues+`)
	`, bundleHash, request.RunID)
	if err != nil {
		return "", fmt.Errorf("revise SQLite run lifecycle source: %w", err)
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		return "", errors.Join(fmt.Errorf("revise SQLite run lifecycle source affected %d rows", rows), rowsErr)
	}
	if _, err := m.store.RequestCompletionCandidateTx(ctx, m.tx, request.RunID, nil, m.handoff); err != nil {
		return "", err
	}
	return runtimerunlifecycle.MutationApplied, nil
}

func (m postgresRunLifecycleMutation) SyncCounters(ctx context.Context, runID string) error {
	if m.tx == nil {
		return errors.New("PostgreSQL run lifecycle counter synchronization requires transaction")
	}
	if _, err := m.tx.ExecContext(ctx, `
		UPDATE runs
		SET event_count = (
				SELECT COUNT(*)::integer FROM events WHERE run_id = $1::uuid
			),
			entity_count = (
				SELECT COUNT(DISTINCT entity_id)::integer FROM entity_state WHERE run_id = $1::uuid
			)
		WHERE run_id = $1::uuid
	`, strings.TrimSpace(runID)); err != nil {
		return fmt.Errorf("synchronize PostgreSQL run lifecycle counters: %w", err)
	}
	return nil
}

func (m sqliteRunLifecycleMutation) SyncCounters(ctx context.Context, runID string) error {
	if m.tx == nil {
		return errors.New("SQLite run lifecycle counter synchronization requires transaction")
	}
	if _, err := m.tx.ExecContext(ctx, `
		UPDATE runs
		SET event_count = (
				SELECT COUNT(*) FROM events WHERE run_id = ?
			),
			entity_count = (
				SELECT COUNT(DISTINCT entity_id) FROM entity_state WHERE run_id = ?
			)
		WHERE run_id = ?
	`, strings.TrimSpace(runID), strings.TrimSpace(runID), strings.TrimSpace(runID)); err != nil {
		return fmt.Errorf("synchronize SQLite run lifecycle counters: %w", err)
	}
	return nil
}

func deleteMaterializedForkRunTx(ctx context.Context, tx *sql.Tx, runID string) error {
	if tx == nil || strings.TrimSpace(runID) == "" {
		return errors.New("materialized fork lifecycle deletion requires transaction and run_id")
	}
	result, err := tx.ExecContext(ctx, `
		DELETE FROM runs
		WHERE run_id = $1::uuid
		  AND status = $2
	`, strings.TrimSpace(runID), string(runtimerunlifecycle.StatePaused))
	if err != nil {
		return fmt.Errorf("delete materialized fork run lifecycle: %w", err)
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr != nil || rows != 1 {
		return errors.Join(fmt.Errorf("delete materialized fork run lifecycle affected %d rows", rows), rowsErr)
	}
	return nil
}

func (s *RunLifecyclePostgresOwner) DeleteMaterializedForkRunTx(ctx context.Context, tx *sql.Tx, runID string) error {
	return deleteMaterializedForkRunTx(ctx, tx, runID)
}

func (s *RunLifecycleSQLiteOwner) DeleteMaterializedForkRunTx(ctx context.Context, tx *sql.Tx, runID string) error {
	return deleteMaterializedForkRunTx(ctx, tx, runID)
}
