package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

const semanticRunFixtureFlow = "semantic-run-fixture"

type runLifecycleTerminalTestStore interface {
	runtimerunlifecycle.OperationOwner
	runtimerunlifecycle.CandidateStore
	RunRuntimeMutationContext(context.Context, func(context.Context) error) error
}

type runLifecycleFixtureMutation interface {
	CreateRun(context.Context, runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error)
	TransitionActiveRun(context.Context, runtimerunlifecycle.ActiveTransitionRequest) (runtimerunlifecycle.MutationDisposition, error)
	MarkTerminalRun(context.Context, runtimerunlifecycle.TerminalRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error)
}

type postgresRunLifecycleFixtureMutation struct{ postgresRunLifecycleMutation }

func (m postgresRunLifecycleFixtureMutation) CreateRun(ctx context.Context, request runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return m.Create(ctx, request)
}

func (m postgresRunLifecycleFixtureMutation) TransitionActiveRun(ctx context.Context, request runtimerunlifecycle.ActiveTransitionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return m.TransitionActive(ctx, request)
}

func (m postgresRunLifecycleFixtureMutation) MarkTerminalRun(ctx context.Context, request runtimerunlifecycle.TerminalRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return m.MarkTerminal(ctx, request)
}

type sqliteRunLifecycleFixtureMutation struct{ sqliteRunLifecycleMutation }

func (m sqliteRunLifecycleFixtureMutation) CreateRun(ctx context.Context, request runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return m.Create(ctx, request)
}

func (m sqliteRunLifecycleFixtureMutation) TransitionActiveRun(ctx context.Context, request runtimerunlifecycle.ActiveTransitionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return m.TransitionActive(ctx, request)
}

func (m sqliteRunLifecycleFixtureMutation) MarkTerminalRun(ctx context.Context, request runtimerunlifecycle.TerminalRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return m.MarkTerminal(ctx, request)
}

type semanticRunFixture struct {
	RunID        string
	State        runtimerunlifecycle.State
	Origin       runtimerunlifecycle.RunOrigin
	BundleHash   string
	BundleSource string
	StartedAt    time.Time
	EndedAt      time.Time
	Failure      *runtimefailures.Envelope
}

func semanticScenarioSetupRunOriginForTest() runtimerunlifecycle.RunOrigin {
	return runtimerunlifecycle.ScenarioSetupRunOrigin()
}

func semanticEventRunOriginForTest(
	t testing.TB,
	eventID string,
	eventType string,
) runtimerunlifecycle.RunOrigin {
	t.Helper()
	origin, err := runtimerunlifecycle.EventRunOrigin(eventID, eventType)
	if err != nil {
		t.Fatalf("construct semantic event run origin: %v", err)
	}
	return origin
}

func materializeRunFixtureForTest(
	ctx context.Context,
	selected any,
	fixture semanticRunFixture,
) error {
	owner, ok := selected.(runLifecycleTerminalTestStore)
	if !ok || owner == nil {
		return fmt.Errorf("test run lifecycle owner is required, got %T", selected)
	}
	fixture.RunID = strings.TrimSpace(fixture.RunID)
	if fixture.RunID == "" {
		return errors.New("semantic run fixture requires run_id")
	}
	if fixture.State == "" {
		fixture.State = runtimerunlifecycle.StateRunning
	}
	if fixture.State == runtimerunlifecycle.StateForked {
		return errors.New("semantic fork fixture requires the named fork operation")
	}
	ctx, source, err := semanticRunFixtureContext(ctx, fixture.BundleHash, fixture.BundleSource)
	if err != nil {
		return err
	}
	err = owner.RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
		if err := materializeRunFixtureInCurrentMutationForTest(txctx, owner, fixture, source); err != nil {
			return err
		}
		if fixture.State != runtimerunlifecycle.StateCompleted {
			return nil
		}
		if err := materializeCompletedRunEntityForTest(txctx, selected, fixture.RunID); err != nil {
			return err
		}
		_, err := owner.RequestCompletionCandidate(
			txctx,
			runtimerunlifecycle.ImmediateCandidate(fixture.RunID),
		)
		return err
	})
	if err != nil || fixture.State != runtimerunlifecycle.StateCompleted {
		return err
	}
	result, err := executeRunCompletionCandidateForRun(
		ctx,
		owner,
		source.BundleHash(),
		fixture.RunID,
		runtimerunlifecycle.NewTerminalCatalog(
			nil,
			map[string][]string{semanticRunFixtureFlow: {"completed"}},
		),
	)
	if err != nil {
		return err
	}
	if result.Outcome != runtimerunlifecycle.OutcomeTerminallyEligible &&
		result.Outcome != runtimerunlifecycle.OutcomeExactNoop {
		return fmt.Errorf(
			"semantic completed run fixture outcome = %s, want terminally eligible",
			result.Outcome,
		)
	}
	return nil
}

func materializeCompletedRunEntityForTest(
	ctx context.Context,
	selected any,
	runID string,
) error {
	tx, ok := runtimepipeline.PipelineSQLTxFromContext(ctx)
	if !ok || tx == nil {
		return errors.New("completed semantic run fixture requires current named transaction")
	}
	switch selected.(type) {
	case *PostgresStore:
		_, err := tx.ExecContext(ctx, `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, current_state
			)
			VALUES ($1::uuid, $1::uuid, $2, 'semantic-run-fixture', 'completed')
		`, runID, semanticRunFixtureFlow)
		return err
	case *SQLiteRuntimeStore:
		_, err := tx.ExecContext(ctx, `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, current_state
			)
			VALUES (?, ?, ?, 'semantic-run-fixture', 'completed')
		`, runID, runID, semanticRunFixtureFlow)
		return err
	default:
		return fmt.Errorf("completed semantic run fixture requires selected store, got %T", selected)
	}
}

func semanticRunFixtureContext(
	ctx context.Context,
	bundleHash string,
	bundleSource string,
) (context.Context, runtimecorrelation.BundleSourceFact, error) {
	bundleHash = strings.TrimSpace(bundleHash)
	bundleSource = strings.TrimSpace(bundleSource)
	if current, ok := runtimecorrelation.BundleSourceFactFromContext(ctx); ok {
		_, currentSource := current.StorageValues()
		if (bundleHash == "" || current.BundleHash() == bundleHash) &&
			(bundleSource == "" || currentSource == bundleSource) {
			bundleHash = current.BundleHash()
			bundleSource = currentSource
		}
	}
	if bundleHash == "" {
		if scope, ok := runtimeauthoractivity.ScopeFromContext(ctx); ok &&
			scope.Kind == runtimeauthoractivity.ScopeBundle {
			bundleHash = strings.TrimSpace(scope.BundleHash)
		}
	}
	if bundleHash == "" {
		bundleHash = "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	}
	var source runtimecorrelation.BundleSourceFact
	var err error
	switch bundleSource {
	case "", runtimerunlifecycle.BundleSourceEphemeral:
		source, err = runtimecorrelation.NewEphemeralBundleSourceFact(bundleHash)
	case runtimerunlifecycle.BundleSourcePersisted:
		source, err = runtimecorrelation.NewPersistedBundleSourceFact(bundleHash)
	default:
		return ctx, runtimecorrelation.BundleSourceFact{}, fmt.Errorf(
			"semantic run fixture forbids bundle_source %q",
			bundleSource,
		)
	}
	if err != nil {
		return ctx, runtimecorrelation.BundleSourceFact{}, err
	}
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, source)
	runtimeInstanceID := "00000000-0000-4000-8000-000000000001"
	if scope, ok := runtimeauthoractivity.ScopeFromContext(ctx); ok &&
		strings.TrimSpace(scope.RuntimeInstanceID) != "" {
		runtimeInstanceID = scope.RuntimeInstanceID
	}
	ctx = runtimeauthoractivity.WithScope(
		ctx,
		runtimeauthoractivity.BundleScope(runtimeInstanceID, bundleHash),
	)
	return ctx, source, nil
}

func materializeRunFixtureInCurrentMutationForTest(
	ctx context.Context,
	owner runLifecycleFixtureMutation,
	fixture semanticRunFixture,
	source runtimecorrelation.BundleSourceFact,
) error {
	if fixture.StartedAt.IsZero() {
		fixture.StartedAt = time.Now().UTC()
	}
	if fixture.EndedAt.IsZero() {
		fixture.EndedAt = fixture.StartedAt
	}
	if _, err := owner.CreateRun(ctx, runtimerunlifecycle.CreateRequest{
		RunID:     fixture.RunID,
		Origin:    fixture.Origin,
		Source:    source,
		StartedAt: fixture.StartedAt.UTC(),
	}); err != nil {
		return err
	}
	switch fixture.State {
	case runtimerunlifecycle.StateRunning:
		return nil
	case runtimerunlifecycle.StatePaused:
		_, err := owner.TransitionActiveRun(ctx, runtimerunlifecycle.ActiveTransitionRequest{
			RunID: fixture.RunID,
			State: runtimerunlifecycle.StatePaused,
		})
		return err
	case runtimerunlifecycle.StateCompleted:
		return nil
	case runtimerunlifecycle.StateFailed,
		runtimerunlifecycle.StateCancelled:
		_, _, err := owner.MarkTerminalRun(ctx, runtimerunlifecycle.TerminalRequest{
			RunID:   fixture.RunID,
			State:   fixture.State,
			Failure: fixture.Failure,
			EndedAt: fixture.EndedAt.UTC(),
		})
		return err
	default:
		return fmt.Errorf("semantic run fixture state %q is unsupported", fixture.State)
	}
}

func requireRunFixtureInCurrentMutationForTest(
	t testing.TB,
	ctx context.Context,
	owner runLifecycleFixtureMutation,
	fixture semanticRunFixture,
) {
	t.Helper()
	if fixture.State == "" {
		fixture.State = runtimerunlifecycle.StateRunning
	}
	if fixture.BundleHash == "" {
		fixture.BundleHash = "bundle-v1:sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
	}
	ctx, source, err := semanticRunFixtureContext(ctx, fixture.BundleHash, fixture.BundleSource)
	if err != nil {
		t.Fatalf("construct semantic run fixture context: %v", err)
	}
	if fixture.State == runtimerunlifecycle.StateCompleted {
		t.Fatal("completed semantic run fixtures require the selected-store owner")
	}
	if err := materializeRunFixtureInCurrentMutationForTest(ctx, owner, fixture, source); err != nil {
		t.Fatalf("materialize semantic run fixture %s in current mutation: %v", fixture.RunID, err)
	}
}

func requirePostgresRunFixtureTxForTest(
	t testing.TB,
	ctx context.Context,
	tx *sql.Tx,
	fixture semanticRunFixture,
) {
	t.Helper()
	requireRunFixtureInCurrentMutationForTest(
		t,
		ctx,
		postgresRunLifecycleFixtureMutation{postgresRunLifecycleMutation{tx: tx}},
		fixture,
	)
}

func requirePostgresRunFixtureInRawTxForTest(
	t testing.TB,
	ctx context.Context,
	tx *sql.Tx,
	fixture semanticRunFixture,
) {
	t.Helper()
	txctx, err := runtimeauthoractivity.Begin(
		ctx,
		tx,
		runtimeauthoractivity.DialectPostgres,
	)
	if err != nil {
		t.Fatalf("begin semantic run fixture author activity: %v", err)
	}
	requirePostgresRunFixtureTxForTest(t, txctx, tx, fixture)
	if err := runtimeauthoractivity.Finalize(txctx); err != nil {
		t.Fatalf("finalize semantic run fixture author activity: %v", err)
	}
}

func requireSQLiteRunFixtureTxForTest(
	t testing.TB,
	ctx context.Context,
	tx *sql.Tx,
	fixture semanticRunFixture,
) {
	t.Helper()
	requireRunFixtureInCurrentMutationForTest(
		t,
		ctx,
		sqliteRunLifecycleFixtureMutation{sqliteRunLifecycleMutation{tx: tx}},
		fixture,
	)
}

func requireSQLiteRunFixtureInRawTxForTest(
	t testing.TB,
	ctx context.Context,
	tx *sql.Tx,
	fixture semanticRunFixture,
) {
	t.Helper()
	txctx, err := runtimeauthoractivity.Begin(
		ctx,
		tx,
		runtimeauthoractivity.DialectSQLite,
	)
	if err != nil {
		t.Fatalf("begin SQLite semantic run fixture author activity: %v", err)
	}
	requireSQLiteRunFixtureTxForTest(t, txctx, tx, fixture)
	if err := runtimeauthoractivity.Finalize(txctx); err != nil {
		t.Fatalf("finalize SQLite semantic run fixture author activity: %v", err)
	}
}

func requireRunFixtureForTest(
	t testing.TB,
	ctx context.Context,
	selected any,
	fixture semanticRunFixture,
) {
	t.Helper()
	switch store := selected.(type) {
	case *PostgresStore:
		bootstrapTestPostgresStore(t, store)
	case *SQLiteRuntimeStore:
		if err := store.BootstrapSchema(context.Background(), canonicalSchemaBootstrapTestRequest(t)); err != nil {
			t.Fatalf("BootstrapSchema: %v", err)
		}
	}
	if err := materializeRunFixtureForTest(ctx, selected, fixture); err != nil {
		t.Fatalf("materialize semantic run fixture %s: %v", fixture.RunID, err)
	}
}

func requireRunningRunForTest(
	t testing.TB,
	ctx context.Context,
	selected any,
	runID string,
	startedAt time.Time,
) {
	t.Helper()
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(),
		RunID:     runID,
		State:     runtimerunlifecycle.StateRunning,
		StartedAt: startedAt,
	})
}

func requirePausedRunForTest(
	t testing.TB,
	ctx context.Context,
	selected any,
	runID string,
	startedAt time.Time,
) {
	t.Helper()
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(),
		RunID:     runID,
		State:     runtimerunlifecycle.StatePaused,
		StartedAt: startedAt,
	})
}

func requireRunningPostgresRunForTest(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	runID string,
	startedAt time.Time,
) {
	t.Helper()
	requireRunningRunForTest(t, ctx, admitTestPostgresStore(t, db), runID, startedAt)
}

func requireRunningSQLiteRunForTest(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	runID string,
	startedAt time.Time,
) {
	t.Helper()
	selected := NewSQLiteRuntimeStoreForTest(db)
	if err := selected.BootstrapSchema(context.Background(), canonicalSchemaBootstrapTestRequest(t)); err != nil {
		t.Fatalf("BootstrapSchema: %v", err)
	}
	requireRunningRunForTest(t, ctx, selected, runID, startedAt)
}

func ensureEphemeralRunForTest(
	ctx context.Context,
	selected any,
	runID string,
	startedAt time.Time,
) error {
	return materializeRunFixtureForTest(ctx, selected, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(),
		RunID:     runID,
		State:     runtimerunlifecycle.StateRunning,
		StartedAt: startedAt,
	})
}

func markRunTerminalStatusForTest(
	ctx context.Context,
	selected any,
	runID string,
	status string,
	failure *runtimefailures.Envelope,
	endedAt time.Time,
) (runtimebus.RunLifecycleSnapshot, error) {
	owner, ok := selected.(runLifecycleTerminalTestStore)
	if !ok || owner == nil {
		return runtimebus.RunLifecycleSnapshot{}, fmt.Errorf("test run lifecycle owner is required, got %T", selected)
	}
	state, err := runtimerunlifecycle.ParseTerminalState(status)
	if err != nil {
		return runtimebus.RunLifecycleSnapshot{}, err
	}
	if state == runtimerunlifecycle.StateForked {
		return runtimebus.RunLifecycleSnapshot{}, fmt.Errorf(
			"%w: run_id=%s",
			runtimerunlifecycle.ErrForkSourceUnsupported,
			strings.TrimSpace(runID),
		)
	}
	if endedAt.IsZero() {
		endedAt = time.Now().UTC()
	}
	var snapshot runtimerunlifecycle.Snapshot
	err = owner.RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
		var mutationErr error
		snapshot, _, mutationErr = owner.MarkTerminalRun(
			txctx,
			runtimerunlifecycle.TerminalRequest{
				RunID: strings.TrimSpace(runID), State: state, Failure: failure, EndedAt: endedAt,
			},
		)
		return mutationErr
	})
	if err != nil {
		return runtimebus.RunLifecycleSnapshot{}, err
	}
	return projectBusRunLifecycleSnapshot(snapshot), nil
}

func transitionRunForTest(
	ctx context.Context,
	selected any,
	request runtimerunlifecycle.ActiveTransitionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	owner, ok := selected.(runLifecycleTerminalTestStore)
	if !ok || owner == nil {
		return "", fmt.Errorf("test run lifecycle owner is required, got %T", selected)
	}
	var disposition runtimerunlifecycle.MutationDisposition
	err := owner.RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
		var mutationErr error
		disposition, mutationErr = owner.TransitionActiveRun(txctx, request)
		return mutationErr
	})
	return disposition, err
}

func forkRunForTest(
	ctx context.Context,
	selected any,
	request runtimerunlifecycle.ForkSourceRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	owner, ok := selected.(runLifecycleTerminalTestStore)
	if !ok || owner == nil {
		return runtimerunlifecycle.Snapshot{}, "", fmt.Errorf(
			"test run lifecycle owner is required, got %T",
			selected,
		)
	}
	var (
		snapshot    runtimerunlifecycle.Snapshot
		disposition runtimerunlifecycle.MutationDisposition
	)
	err := owner.RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
		var mutationErr error
		snapshot, disposition, mutationErr = owner.ForkRunSource(txctx, request)
		return mutationErr
	})
	return snapshot, disposition, err
}

func reviseRunSourceForTest(
	ctx context.Context,
	selected any,
	request runtimerunlifecycle.SourceRevisionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	owner, ok := selected.(runLifecycleTerminalTestStore)
	if !ok || owner == nil {
		return "", fmt.Errorf("test run lifecycle owner is required, got %T", selected)
	}
	var disposition runtimerunlifecycle.MutationDisposition
	err := owner.RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
		var mutationErr error
		disposition, mutationErr = owner.ReviseRunSource(txctx, request)
		return mutationErr
	})
	return disposition, err
}

func syncRunCountersForTest(
	ctx context.Context,
	selected any,
	runID string,
) error {
	owner, ok := selected.(runLifecycleTerminalTestStore)
	if !ok || owner == nil {
		return fmt.Errorf("test run lifecycle owner is required, got %T", selected)
	}
	return owner.RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
		return owner.SyncRunCounters(txctx, strings.TrimSpace(runID))
	})
}

func executeRunCompletionCandidateForEvent(
	ctx context.Context,
	selected any,
	eventID string,
	workflowTerminalStates []string,
	flowTerminalStates map[string][]string,
) error {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return errors.New("completion candidate test helper requires event_id")
	}
	catalog := runtimerunlifecycle.NewTerminalCatalog(workflowTerminalStates, flowTerminalStates)
	var (
		request runtimerunlifecycle.CandidateRequestResult
		store   runtimerunlifecycle.CandidateStore
	)
	switch owner := selected.(type) {
	case *PostgresStore:
		store = owner
		var runID string
		if err := owner.backend.db.QueryRowContext(ctx, `
			SELECT run_id::text FROM events WHERE event_id = $1::uuid
		`, eventID).Scan(&runID); err != nil {
			return fmt.Errorf("load PostgreSQL completion candidate run: %w", err)
		}
		if err := owner.runAuthorActivityMutation(ctx, "test request PostgreSQL completion candidate", func(txctx context.Context, tx *sql.Tx) error {
			var err error
			request, err = requestPostgresCompletionCandidateTx(txctx, tx, runID, nil, false)
			return err
		}); err != nil {
			return err
		}
	case *SQLiteRuntimeStore:
		store = owner
		var runID string
		if err := owner.backend.db.QueryRowContext(ctx, `
			SELECT run_id FROM events WHERE event_id = ?
		`, eventID).Scan(&runID); err != nil {
			return fmt.Errorf("load SQLite completion candidate run: %w", err)
		}
		if err := owner.runAuthorActivityMutation(ctx, "test request SQLite completion candidate", func(txctx context.Context, tx *sql.Tx) error {
			var err error
			request, err = requestSQLiteCompletionCandidateTx(txctx, tx, runID, nil, owner.now(), false)
			return err
		}); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported completion candidate test store %T", selected)
	}
	if request.Disposition == runtimerunlifecycle.CandidateAbsorbedTerminal {
		return nil
	}
	_, err := executeCompletionCandidateUntilSettledForTest(ctx, store, request.Candidate, catalog)
	return err
}

func executeRunCompletionCandidateForRun(
	ctx context.Context,
	store runtimerunlifecycle.CandidateStore,
	bundleHash string,
	runID string,
	catalog runtimerunlifecycle.TerminalCatalog,
) (runtimerunlifecycle.CompletionResult, error) {
	scope := runtimerunlifecycle.CandidateScope{BundleHash: strings.TrimSpace(bundleHash)}
	cursor := runtimerunlifecycle.CandidateCursor{}
	for {
		page, err := store.ListCompletionCandidates(ctx, scope, cursor, 128)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		for _, candidate := range page.Candidates {
			if candidate.RunID == strings.TrimSpace(runID) {
				return executeCompletionCandidateUntilSettledForTest(ctx, store, candidate, catalog)
			}
		}
		if page.Exhausted {
			return runtimerunlifecycle.CompletionResult{}, fmt.Errorf(
				"completion candidate for run %s is not durable",
				runID,
			)
		}
		cursor = page.Next
	}
}

func executeCompletionCandidateUntilSettledForTest(
	ctx context.Context,
	store runtimerunlifecycle.CandidateStore,
	candidate runtimerunlifecycle.Candidate,
	catalog runtimerunlifecycle.TerminalCatalog,
) (runtimerunlifecycle.CompletionResult, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		result, err := store.ExecuteCompletionCandidate(ctx, candidate, catalog)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		if result.Outcome != runtimerunlifecycle.OutcomeRetryCurrent {
			return result, nil
		}
		var beforeStart *runtimerunlifecycle.SelectedStoreBeforeRunStartError
		if !errors.As(result.Retryable, &beforeStart) {
			return result, result.Retryable
		}
		if time.Now().After(deadline) {
			return result, fmt.Errorf("completion candidate selected-store clock did not reach run start: %w", beforeStart)
		}
		timer := time.NewTimer(time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, context.Cause(ctx)
		case <-timer.C:
		}
	}
}
