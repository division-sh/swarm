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
	storerunstate "github.com/division-sh/swarm/internal/store/internal/backend/runstate"
)

type postgresRunLifecycleMutation struct {
	store   *RunLifecyclePostgresOwner
	tx      *sql.Tx
	story   runtimeauthoractivity.Mutation
	handoff *CandidateHandoff
}

type sqliteRunLifecycleMutation struct {
	store   *RunLifecycleSQLiteOwner
	tx      *sql.Tx
	story   runtimeauthoractivity.Mutation
	handoff *CandidateHandoff
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

func NewPostgresTransactionMutation(owner *RunLifecyclePostgresOwner, tx *sql.Tx, story runtimeauthoractivity.Mutation) TransactionMutation {
	return postgresRunLifecycleMutation{store: owner, tx: tx, story: story}
}

func NewSQLiteTransactionMutation(owner *RunLifecycleSQLiteOwner, tx *sql.Tx, story runtimeauthoractivity.Mutation) TransactionMutation {
	return sqliteRunLifecycleMutation{store: owner, tx: tx, story: story}
}

type activeRunSourceOwnerFunc func(context.Context, string) (runtimecorrelation.BundleSourceFact, error)

func (fn activeRunSourceOwnerFunc) RequireActiveRunSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return fn(ctx, runID)
}

func postgresActiveRunSourceOwner(store *RunLifecyclePostgresOwner, tx *sql.Tx) activeRunSourceOwnerFunc {
	return func(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
		return (postgresRunLifecycleMutation{store: store, tx: tx}).RequireActiveSource(ctx, runID)
	}
}

func sqliteActiveRunSourceOwner(store *RunLifecycleSQLiteOwner, tx *sql.Tx) activeRunSourceOwnerFunc {
	return func(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
		return (sqliteRunLifecycleMutation{store: store, tx: tx}).RequireActiveSource(ctx, runID)
	}
}

func requirePostgresRunPresent(ctx context.Context, tx *sql.Tx, runID string) error {
	return (postgresRunLifecycleMutation{tx: tx}).RequirePresent(ctx, runID)
}

func requireSQLiteRunPresent(ctx context.Context, tx *sql.Tx, runID string) error {
	return (sqliteRunLifecycleMutation{tx: tx}).RequirePresent(ctx, runID)
}

func requirePostgresRunActive(ctx context.Context, tx *sql.Tx, runID string) error {
	return storerunstate.RequirePostgresActiveTx(ctx, tx, runID)
}

func requireSQLiteRunActive(ctx context.Context, tx *sql.Tx, runID string) error {
	return storerunstate.RequireSQLiteActiveTx(ctx, tx, runID)
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

func (s *RunLifecyclePostgresOwner) MarkTerminalTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, request runtimerunlifecycle.TerminalRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return (postgresRunLifecycleMutation{store: s, tx: tx, story: story}).MarkTerminal(ctx, request)
}

func (s *RunLifecycleSQLiteOwner) MarkTerminalTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, request runtimerunlifecycle.TerminalRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return (sqliteRunLifecycleMutation{store: s, tx: tx, story: story}).MarkTerminal(ctx, request)
}

func (s *RunLifecyclePostgresOwner) ForkSourceTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, request runtimerunlifecycle.ForkSourceRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return (postgresRunLifecycleMutation{store: s, tx: tx, story: story}).ForkSource(ctx, request)
}

func (s *RunLifecyclePostgresOwner) ReviseSourceTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, handoff *CandidateHandoff, request runtimerunlifecycle.SourceRevisionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return (postgresRunLifecycleMutation{store: s, tx: tx, story: story, handoff: handoff}).ReviseSource(ctx, request)
}

func (s *RunLifecycleSQLiteOwner) ReviseSourceTx(ctx context.Context, tx *sql.Tx, story runtimeauthoractivity.Mutation, handoff *CandidateHandoff, request runtimerunlifecycle.SourceRevisionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return (sqliteRunLifecycleMutation{store: s, tx: tx, story: story, handoff: handoff}).ReviseSource(ctx, request)
}

func (s *RunLifecyclePostgresOwner) RequireActiveSourceTx(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return (postgresRunLifecycleMutation{store: s, tx: tx}).RequireActiveSource(ctx, runID)
}

func (s *RunLifecycleSQLiteOwner) RequireActiveSourceTx(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return (sqliteRunLifecycleMutation{store: s, tx: tx}).RequireActiveSource(ctx, runID)
}

func (s *RunLifecyclePostgresOwner) RequirePresentSourceTx(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return (postgresRunLifecycleMutation{store: s, tx: tx}).RequirePresentSource(ctx, runID)
}

func (s *RunLifecycleSQLiteOwner) RequirePresentSourceTx(ctx context.Context, tx *sql.Tx, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return (sqliteRunLifecycleMutation{store: s, tx: tx}).RequirePresentSource(ctx, runID)
}

func runPostgresLifecycleOperation[T any](
	ctx context.Context,
	store *RunLifecyclePostgresOwner,
	fn func(context.Context, postgresRunLifecycleMutation) (T, error),
) (T, error) {
	return WithCandidateHandoffResult(ctx, func(handoff *CandidateHandoff) (T, error) {
		var result T
		err := store.runPrivateAuthorActivityMutation(ctx, privaterunforkrevision.NewEffects(), func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			var err error
			result, err = fn(txctx, postgresRunLifecycleMutation{
				store: store, tx: tx, story: runtimeAuthorActivityMutation(story), handoff: handoff,
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
		err := store.runPrivateAuthorActivityMutation(ctx, "sqlite run lifecycle operation", privaterunforkrevision.NewEffects(), func(txctx context.Context, tx *sql.Tx, story *privateauthoractivity.Mutation) error {
			var err error
			result, err = fn(txctx, sqliteRunLifecycleMutation{
				store: store, tx: tx, story: runtimeAuthorActivityMutation(story), handoff: handoff,
			})
			return err
		})
		return result, err
	})
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
	_, err := runPostgresLifecycleOperation(ctx, s, func(ctx context.Context, mutation postgresRunLifecycleMutation) (struct{}, error) {
		return struct{}{}, mutation.RequirePresent(ctx, runID)
	})
	return err
}

func (s *RunLifecycleSQLiteOwner) RequirePresentRun(ctx context.Context, runID string) error {
	_, err := runSQLiteLifecycleOperation(ctx, s, func(ctx context.Context, mutation sqliteRunLifecycleMutation) (struct{}, error) {
		return struct{}{}, mutation.RequirePresent(ctx, runID)
	})
	return err
}

func (s *RunLifecyclePostgresOwner) RequireActiveRun(ctx context.Context, runID string) error {
	_, err := runPostgresLifecycleOperation(ctx, s, func(ctx context.Context, mutation postgresRunLifecycleMutation) (struct{}, error) {
		return struct{}{}, mutation.RequireActive(ctx, runID)
	})
	return err
}

func (s *RunLifecycleSQLiteOwner) RequireActiveRun(ctx context.Context, runID string) error {
	_, err := runSQLiteLifecycleOperation(ctx, s, func(ctx context.Context, mutation sqliteRunLifecycleMutation) (struct{}, error) {
		return struct{}{}, mutation.RequireActive(ctx, runID)
	})
	return err
}

// RequirePublicationRunActive performs the closed preflight used before route
// planning. The event commit repeats this check in its own transaction; this
// operation only guarantees that terminal-run refusal cannot be shadowed by a
// later route-planning error.
func (s *RunLifecyclePostgresOwner) RequirePublicationRunActive(ctx context.Context, runID string) error {
	return s.runPrivateAuthorActivityMutation(ctx, privaterunforkrevision.NewEffects(), func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
		return (postgresRunLifecycleMutation{store: s, tx: tx}).RequireActive(txctx, runID)
	})
}

func (s *RunLifecycleSQLiteOwner) RequirePublicationRunActive(ctx context.Context, runID string) error {
	return s.runPrivateAuthorActivityMutation(ctx, "sqlite publication run preflight", privaterunforkrevision.NewEffects(), func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
		return (sqliteRunLifecycleMutation{store: s, tx: tx}).RequireActive(txctx, runID)
	})
}

func (s *RunLifecyclePostgresOwner) RequirePresentRunSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return runPostgresLifecycleOperation(ctx, s, func(ctx context.Context, mutation postgresRunLifecycleMutation) (runtimecorrelation.BundleSourceFact, error) {
		return mutation.RequirePresentSource(ctx, runID)
	})
}

func (s *RunLifecycleSQLiteOwner) RequirePresentRunSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return runSQLiteLifecycleOperation(ctx, s, func(ctx context.Context, mutation sqliteRunLifecycleMutation) (runtimecorrelation.BundleSourceFact, error) {
		return mutation.RequirePresentSource(ctx, runID)
	})
}

func (s *RunLifecyclePostgresOwner) RequireActiveRunSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return runPostgresLifecycleOperation(ctx, s, func(ctx context.Context, mutation postgresRunLifecycleMutation) (runtimecorrelation.BundleSourceFact, error) {
		return mutation.RequireActiveSource(ctx, runID)
	})
}

func (s *RunLifecycleSQLiteOwner) RequireActiveRunSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return runSQLiteLifecycleOperation(ctx, s, func(ctx context.Context, mutation sqliteRunLifecycleMutation) (runtimecorrelation.BundleSourceFact, error) {
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
	return runtimerunlifecycle.Snapshot{}, "", fmt.Errorf("%w: backend=sqlite run_id=%s", runtimerunlifecycle.ErrForkSourceUnsupported, strings.TrimSpace(request.RunID))
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

func requirePostgresRunActiveQuery(ctx context.Context, queryer rowQueryer, runID string) error {
	if queryer == nil {
		return errors.New("PostgreSQL run lifecycle query authority is required")
	}
	runID = strings.TrimSpace(runID)
	var state, bundleHash, bundleSource string
	err := queryer.QueryRowContext(ctx, `
		SELECT status, bundle_hash, bundle_source
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&state, &bundleHash, &bundleSource)
	if errors.Is(err, sql.ErrNoRows) {
		return &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return fmt.Errorf("read PostgreSQL active run lifecycle: %w", err)
	}
	parsed, err := runtimerunlifecycle.ParseState(state)
	if err != nil {
		return err
	}
	if !parsed.Active() {
		return &runtimerunlifecycle.RunNotActiveError{RunID: runID, State: parsed}
	}
	fact, err := runtimecorrelation.DecodeBundleSourceFact(bundleHash, bundleSource)
	if err != nil {
		return fmt.Errorf("decode PostgreSQL active run lifecycle source: %w", err)
	}
	if fact.IsPersisted() {
		var exists bool
		if err := queryer.QueryRowContext(ctx, `
			SELECT EXISTS (SELECT 1 FROM bundles WHERE bundle_hash = $1)
		`, fact.BundleHash()).Scan(&exists); err != nil {
			return fmt.Errorf("validate PostgreSQL active run lifecycle source: %w", err)
		}
		if !exists {
			return &runtimerunlifecycle.PersistedBundleUnavailableError{
				BundleHash: fact.BundleHash(), BundleSource: runtimerunlifecycle.BundleSourcePersisted,
				Cause: "persisted_missing_bundle_row",
			}
		}
	}
	return nil
}

func requireSQLiteRunActiveQuery(ctx context.Context, queryer rowQueryer, runID string) error {
	if queryer == nil {
		return errors.New("SQLite run lifecycle query authority is required")
	}
	runID = strings.TrimSpace(runID)
	var state, bundleHash, bundleSource string
	err := queryer.QueryRowContext(ctx, `
		SELECT status, bundle_hash, bundle_source
		FROM runs
		WHERE run_id = ?
	`, runID).Scan(&state, &bundleHash, &bundleSource)
	if errors.Is(err, sql.ErrNoRows) {
		return &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return fmt.Errorf("read SQLite active run lifecycle: %w", err)
	}
	parsed, err := runtimerunlifecycle.ParseState(state)
	if err != nil {
		return err
	}
	if !parsed.Active() {
		return &runtimerunlifecycle.RunNotActiveError{RunID: runID, State: parsed}
	}
	fact, err := runtimecorrelation.DecodeBundleSourceFact(bundleHash, bundleSource)
	if err != nil {
		return fmt.Errorf("decode SQLite active run lifecycle source: %w", err)
	}
	if fact.IsPersisted() {
		var exists bool
		if err := queryer.QueryRowContext(ctx, `
			SELECT EXISTS (SELECT 1 FROM bundles WHERE bundle_hash = ?)
		`, fact.BundleHash()).Scan(&exists); err != nil {
			return fmt.Errorf("validate SQLite active run lifecycle source: %w", err)
		}
		if !exists {
			return &runtimerunlifecycle.PersistedBundleUnavailableError{
				BundleHash: fact.BundleHash(), BundleSource: runtimerunlifecycle.BundleSourcePersisted,
				Cause: "persisted_missing_bundle_row",
			}
		}
	}
	return nil
}

func requirePostgresRunActiveSource(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
) (runtimecorrelation.BundleSourceFact, error) {
	return (postgresRunLifecycleMutation{tx: tx}).RequireActiveSource(ctx, runID)
}

func requireSQLiteRunActiveSource(
	ctx context.Context,
	tx *sql.Tx,
	runID string,
) (runtimecorrelation.BundleSourceFact, error) {
	return (sqliteRunLifecycleMutation{tx: tx}).RequireActiveSource(ctx, runID)
}

func (m postgresRunLifecycleMutation) RequirePresent(ctx context.Context, runID string) error {
	if m.tx == nil {
		return errors.New("PostgreSQL run lifecycle mutation requires transaction")
	}
	var state string
	err := m.tx.QueryRowContext(ctx, `
		SELECT status
		FROM runs
		WHERE run_id = $1::uuid
		FOR UPDATE
	`, strings.TrimSpace(runID)).Scan(&state)
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

func (m postgresRunLifecycleMutation) RequirePresentSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return m.loadSource(ctx, runID, false)
}

func (m sqliteRunLifecycleMutation) RequirePresentSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return m.loadSource(ctx, runID, false)
}

func (m postgresRunLifecycleMutation) RequireActiveSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return m.loadSource(ctx, runID, true)
}

func (m sqliteRunLifecycleMutation) RequireActiveSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return m.loadSource(ctx, runID, true)
}

func (m postgresRunLifecycleMutation) loadSource(
	ctx context.Context,
	runID string,
	requireActive bool,
) (runtimecorrelation.BundleSourceFact, error) {
	if m.tx == nil {
		return runtimecorrelation.BundleSourceFact{}, errors.New("PostgreSQL run lifecycle mutation requires transaction")
	}
	runID = strings.TrimSpace(runID)
	var state, bundleHash, bundleSource string
	err := m.tx.QueryRowContext(ctx, `
		SELECT status, bundle_hash, bundle_source
		FROM runs
		WHERE run_id = $1::uuid
		FOR UPDATE
	`, runID).Scan(&state, &bundleHash, &bundleSource)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimecorrelation.BundleSourceFact{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("load PostgreSQL run lifecycle source: %w", err)
	}
	parsed, err := runtimerunlifecycle.ParseState(state)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	if requireActive && !parsed.Active() {
		return runtimecorrelation.BundleSourceFact{}, &runtimerunlifecycle.RunNotActiveError{RunID: runID, State: parsed}
	}
	fact, err := runtimecorrelation.DecodeBundleSourceFact(bundleHash, bundleSource)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("decode PostgreSQL run lifecycle source: %w", err)
	}
	if err := m.requirePersistedSource(ctx, fact); err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	return fact, nil
}

func (m sqliteRunLifecycleMutation) loadSource(
	ctx context.Context,
	runID string,
	requireActive bool,
) (runtimecorrelation.BundleSourceFact, error) {
	if m.tx == nil {
		return runtimecorrelation.BundleSourceFact{}, errors.New("SQLite run lifecycle mutation requires transaction")
	}
	runID = strings.TrimSpace(runID)
	var state, bundleHash, bundleSource string
	err := m.tx.QueryRowContext(ctx, `
		SELECT status, bundle_hash, bundle_source
		FROM runs
		WHERE run_id = ?
	`, runID).Scan(&state, &bundleHash, &bundleSource)
	if errors.Is(err, sql.ErrNoRows) {
		return runtimecorrelation.BundleSourceFact{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
	}
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("load SQLite run lifecycle source: %w", err)
	}
	parsed, err := runtimerunlifecycle.ParseState(state)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	if requireActive && !parsed.Active() {
		return runtimecorrelation.BundleSourceFact{}, &runtimerunlifecycle.RunNotActiveError{RunID: runID, State: parsed}
	}
	fact, err := runtimecorrelation.DecodeBundleSourceFact(bundleHash, bundleSource)
	if err != nil {
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("decode SQLite run lifecycle source: %w", err)
	}
	if err := m.requirePersistedSource(ctx, fact); err != nil {
		return runtimecorrelation.BundleSourceFact{}, err
	}
	return fact, nil
}

func (m postgresRunLifecycleMutation) requirePersistedSource(ctx context.Context, fact runtimecorrelation.BundleSourceFact) error {
	if !fact.IsPersisted() {
		return nil
	}
	var exists bool
	if err := m.tx.QueryRowContext(ctx, `
		SELECT EXISTS (SELECT 1 FROM bundles WHERE bundle_hash = $1)
	`, fact.BundleHash()).Scan(&exists); err != nil {
		return fmt.Errorf("validate PostgreSQL persisted run source: %w", err)
	}
	if !exists {
		return &runtimerunlifecycle.PersistedBundleUnavailableError{
			BundleHash: fact.BundleHash(), BundleSource: "persisted",
			Cause: "persisted_missing_bundle_row",
		}
	}
	return nil
}

func (m sqliteRunLifecycleMutation) requirePersistedSource(ctx context.Context, fact runtimecorrelation.BundleSourceFact) error {
	if !fact.IsPersisted() {
		return nil
	}
	var exists bool
	if err := m.tx.QueryRowContext(ctx, `
		SELECT EXISTS (SELECT 1 FROM bundles WHERE bundle_hash = ?)
	`, fact.BundleHash()).Scan(&exists); err != nil {
		return fmt.Errorf("validate SQLite persisted run source: %w", err)
	}
	if !exists {
		return &runtimerunlifecycle.PersistedBundleUnavailableError{
			BundleHash: fact.BundleHash(), BundleSource: "persisted",
			Cause: "persisted_missing_bundle_row",
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
	if err := m.requirePersistedSourceForWrite(ctx, request.Source); err != nil {
		return "", err
	}
	bundleHash, bundleSource := request.Source.StorageValues()
	origin := request.Origin
	var inserted bool
	err := m.tx.QueryRowContext(ctx, `
		INSERT INTO runs (
			run_id, status, bundle_hash, bundle_source, origin_kind,
			trigger_event_id, trigger_event_type,
			origin_service_id, origin_generation,
			forked_from_run_id, forked_from_event_id,
			started_at
		)
		VALUES (
			$1::uuid, 'running', $2, $3, $4,
			NULLIF($5, '')::uuid, NULLIF($6, ''),
			NULLIF($7, '')::uuid, NULLIF($8, 0),
			NULLIF($9, '')::uuid, NULLIF($10, '')::uuid,
			$11
		)
		ON CONFLICT (run_id) DO NOTHING
		RETURNING TRUE
	`, request.RunID, bundleHash, bundleSource, origin.Kind(),
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
	if err := m.requirePersistedSource(ctx, request.Source); err != nil {
		return "", err
	}
	bundleHash, bundleSource := request.Source.StorageValues()
	origin := request.Origin
	result, err := m.tx.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, status, bundle_hash, bundle_source, origin_kind,
			trigger_event_id, trigger_event_type,
			origin_service_id, origin_generation,
			forked_from_run_id, forked_from_event_id,
			started_at
		)
		VALUES (
			?, 'running', ?, ?, ?,
			NULLIF(?, ''), NULLIF(?, ''),
			NULLIF(?, ''), NULLIF(?, 0),
			NULLIF(?, ''), NULLIF(?, ''),
			?
		)
		ON CONFLICT (run_id) DO NOTHING
	`, request.RunID, bundleHash, bundleSource, origin.Kind(),
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
	identity runtimecorrelation.BundleSourceFact,
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
	if err := mutation.requirePersistedSourceForWrite(ctx, identity); err != nil {
		return err
	}
	bundleHash, bundleSource := identity.StorageValues()
	origin, err := runtimerunlifecycle.ForkMaterializationRunOrigin(sourceRunID, forkEventID)
	if err != nil {
		return err
	}
	startedAt = runtimerunlifecycle.CanonicalTimestamp(startedAt)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO runs (
			run_id, status, origin_kind, forked_from_run_id, forked_from_event_id,
			entity_count, event_count, started_at, bundle_hash, bundle_source
		)
		VALUES ($1::uuid, $2, $3, $4::uuid, $5::uuid, $6, 0, $7, $8, $9)
	`, forkRunID, string(runtimerunlifecycle.StatePaused), origin.Kind(), origin.SourceRunID(), origin.SourceEventID(),
		entityCount, startedAt.UTC(), bundleHash, bundleSource); err != nil {
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
	return m.store.markRunTerminalTx(ctx, m.tx, m.story, request)
}

func (m sqliteRunLifecycleMutation) MarkTerminal(
	ctx context.Context,
	request runtimerunlifecycle.TerminalRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if m.store == nil {
		return runtimerunlifecycle.Snapshot{}, "", errors.New("SQLite terminal run lifecycle transition requires selected store")
	}
	return m.store.markRunTerminalTx(ctx, m.tx, m.story, request)
}

func (m postgresRunLifecycleMutation) ForkSource(
	ctx context.Context,
	request runtimerunlifecycle.ForkSourceRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if m.store == nil {
		return runtimerunlifecycle.Snapshot{}, "", errors.New("PostgreSQL fork source lifecycle transition requires selected store")
	}
	return m.store.markForkSourceTx(ctx, m.tx, m.story, request)
}

func (m sqliteRunLifecycleMutation) ForkSource(
	_ context.Context,
	request runtimerunlifecycle.ForkSourceRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return runtimerunlifecycle.Snapshot{}, "", fmt.Errorf(
		"%w: backend=sqlite run_id=%s",
		runtimerunlifecycle.ErrForkSourceUnsupported,
		strings.TrimSpace(request.RunID),
	)
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
	wantHash, wantSource := request.Source.StorageValues()
	if strings.TrimSpace(current.BundleHash) != wantHash || strings.TrimSpace(current.BundleSource) != wantSource {
		return "", fmt.Errorf(
			"run lifecycle source conflict for run_id=%s: stored=%s/%s requested=%s/%s",
			request.RunID, current.BundleHash, current.BundleSource, wantHash, wantSource,
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

func (m postgresRunLifecycleMutation) requirePersistedSourceForWrite(
	ctx context.Context,
	fact runtimecorrelation.BundleSourceFact,
) error {
	if !fact.IsPersisted() {
		return nil
	}
	if _, err := m.tx.ExecContext(ctx, `LOCK TABLE runs IN ROW EXCLUSIVE MODE`); err != nil {
		return fmt.Errorf("serialize persisted PostgreSQL run source admission: %w", err)
	}
	return m.requirePersistedSource(ctx, fact)
}

func recordRunStarted(ctx context.Context, request runtimerunlifecycle.CreateRequest) error {
	return recordRunStartedWithStory(ctx, nil, request)
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
	if err := m.requirePersistedSourceForWrite(ctx, request.Source); err != nil {
		return "", err
	}
	bundleHash, bundleSource := request.Source.StorageValues()
	result, err := m.tx.ExecContext(ctx, `
		UPDATE runs
		SET bundle_hash = $2, bundle_source = $3
		WHERE run_id = $1::uuid
		  AND status IN (`+runLifecycleActiveStateSQLValues+`)
	`, request.RunID, bundleHash, bundleSource)
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
	if err := m.requirePersistedSource(ctx, request.Source); err != nil {
		return "", err
	}
	bundleHash, bundleSource := request.Source.StorageValues()
	result, err := m.tx.ExecContext(ctx, `
		UPDATE runs
		SET bundle_hash = ?, bundle_source = ?
		WHERE run_id = ?
		  AND status IN (`+runLifecycleActiveStateSQLValues+`)
	`, bundleHash, bundleSource, request.RunID)
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

func normalizedRunLifecycleTime(value time.Time) time.Time {
	if value.IsZero() {
		value = time.Now().UTC()
	}
	return runtimerunlifecycle.CanonicalTimestamp(value)
}
