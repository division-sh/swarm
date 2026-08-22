package pipeline

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
)

type testRunLifecycleMutation struct {
	tx      *sql.Tx
	dialect workflowStoreDialect
}

func (m testRunLifecycleMutation) RequirePresent(ctx context.Context, runID string) error {
	_, _, err := m.load(ctx, runID, false)
	return err
}

func (m testRunLifecycleMutation) RequireActive(ctx context.Context, runID string) error {
	_, _, err := m.load(ctx, runID, true)
	return err
}

func (m testRunLifecycleMutation) RequirePresentSource(
	ctx context.Context,
	runID string,
) (runtimecorrelation.BundleSourceFact, error) {
	_, source, err := m.load(ctx, runID, false)
	return source, err
}

func (m testRunLifecycleMutation) RequireActiveSource(
	ctx context.Context,
	runID string,
) (runtimecorrelation.BundleSourceFact, error) {
	_, source, err := m.load(ctx, runID, true)
	return source, err
}

func (m testRunLifecycleMutation) Create(
	ctx context.Context,
	request runtimerunlifecycle.CreateRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	bundleHash, bundleSource := request.Source.StorageValues()
	origin := request.Origin
	query := `
		INSERT INTO runs (
			run_id, status, bundle_hash, bundle_source, origin_kind,
			trigger_event_id, trigger_event_type, origin_service_id, origin_generation,
			forked_from_run_id, forked_from_event_id, started_at
		)
		VALUES (
			?, 'running', ?, ?, ?,
			NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, ''), NULLIF(?, 0),
			NULLIF(?, ''), NULLIF(?, ''), ?
		)
		ON CONFLICT (run_id) DO NOTHING
	`
	if m.dialect == workflowStoreDialectPostgres {
		query = `
			INSERT INTO runs (
				run_id, status, bundle_hash, bundle_source, origin_kind,
				trigger_event_id, trigger_event_type, origin_service_id, origin_generation,
				forked_from_run_id, forked_from_event_id, started_at
			)
			VALUES (
				$1::uuid, 'running', $2, $3, $4,
				NULLIF($5, '')::uuid, NULLIF($6, ''), NULLIF($7, '')::uuid, NULLIF($8, 0),
				NULLIF($9, '')::uuid, NULLIF($10, '')::uuid, $11
			)
			ON CONFLICT (run_id) DO NOTHING
		`
	}
	result, err := m.tx.ExecContext(ctx, query, request.RunID, bundleHash, bundleSource,
		origin.Kind(), origin.EventID(), origin.EventType(), origin.ServiceID(), origin.Generation(),
		origin.SourceRunID(), origin.SourceEventID(), request.StartedAt.UTC())
	if err != nil {
		return "", err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return "", err
	}
	if rows == 1 {
		return runtimerunlifecycle.MutationApplied, nil
	}
	state, source, err := m.load(ctx, request.RunID, true)
	if err != nil {
		return "", err
	}
	if !state.Active() || source != request.Source {
		return "", errors.New("pipeline test lifecycle run creation conflicts with existing run")
	}
	snapshot, err := m.loadSnapshot(ctx, request.RunID)
	if err != nil {
		return "", err
	}
	if !snapshot.Origin.Equal(request.Origin) {
		return "", errors.New("pipeline test lifecycle run creation conflicts with existing origin")
	}
	return runtimerunlifecycle.MutationExactNoop, nil
}

func (m testRunLifecycleMutation) TransitionActive(
	ctx context.Context,
	request runtimerunlifecycle.ActiveTransitionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	state, _, err := m.load(ctx, request.RunID, false)
	if err != nil {
		return "", err
	}
	if state == request.State {
		return runtimerunlifecycle.MutationExactNoop, nil
	}
	if !state.Active() {
		return "", &runtimerunlifecycle.RunNotActiveError{RunID: request.RunID, State: state}
	}
	query := `UPDATE runs SET status = ?, ended_at = NULL, failure = NULL WHERE run_id = ? AND status = ?`
	args := []any{string(request.State), request.RunID, string(state)}
	if m.dialect == workflowStoreDialectPostgres {
		query = `UPDATE runs SET status = $2, ended_at = NULL, failure = NULL WHERE run_id = $1::uuid AND status = $3`
		args = []any{request.RunID, string(request.State), string(state)}
	}
	result, err := m.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return "", err
	}
	return testRunLifecycleAffected(result)
}

func (m testRunLifecycleMutation) MarkTerminal(
	ctx context.Context,
	request runtimerunlifecycle.TerminalRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if err := request.Validate(); err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	current, err := m.loadSnapshot(ctx, request.RunID)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	if current.State.Terminal() {
		if current.State != request.State || !testRunLifecycleFailuresEqual(current.Failure, request.Failure) {
			return runtimerunlifecycle.Snapshot{}, "", fmt.Errorf(
				"pipeline test run %s already terminal with state %s",
				request.RunID, current.State,
			)
		}
		return current, runtimerunlifecycle.MutationExactNoop, nil
	}
	failureJSON, err := testRunLifecycleFailureJSON(request.Failure)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	query := `
		UPDATE runs
		SET status = ?, failure = ?, ended_at = ?, continued_as_run_id = NULL
		WHERE run_id = ? AND status IN ('running', 'paused')
	`
	args := []any{string(request.State), failureJSON, request.EndedAt.UTC(), request.RunID}
	if m.dialect == workflowStoreDialectPostgres {
		query = `
			UPDATE runs
			SET status = $2, failure = NULLIF($3, '')::jsonb, ended_at = $4, continued_as_run_id = NULL
			WHERE run_id = $1::uuid AND status IN ('running', 'paused')
		`
		args = []any{request.RunID, string(request.State), failureJSON, request.EndedAt.UTC()}
	}
	result, err := m.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	disposition, err := testRunLifecycleAffected(result)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	snapshot, err := m.loadSnapshot(ctx, request.RunID)
	return snapshot, disposition, err
}

func (m testRunLifecycleMutation) ForkSource(
	ctx context.Context,
	request runtimerunlifecycle.ForkSourceRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if err := request.Validate(); err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	current, err := m.loadSnapshot(ctx, request.RunID)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	if current.State.Terminal() {
		if current.State != runtimerunlifecycle.StateForked ||
			strings.TrimSpace(current.ContinuedAsRunID) != strings.TrimSpace(request.ContinuedAsRunID) {
			return runtimerunlifecycle.Snapshot{}, "", fmt.Errorf(
				"pipeline test run %s already terminal with state %s",
				request.RunID, current.State,
			)
		}
		return current, runtimerunlifecycle.MutationExactNoop, nil
	}
	query := `
		UPDATE runs
		SET status = 'forked', failure = NULL, ended_at = ?, continued_as_run_id = ?
		WHERE run_id = ? AND status IN ('running', 'paused')
	`
	args := []any{request.EndedAt.UTC(), request.ContinuedAsRunID, request.RunID}
	if m.dialect == workflowStoreDialectPostgres {
		query = `
			UPDATE runs
			SET status = 'forked', failure = NULL, ended_at = $3, continued_as_run_id = $2::uuid
			WHERE run_id = $1::uuid AND status IN ('running', 'paused')
		`
		args = []any{request.RunID, request.ContinuedAsRunID, request.EndedAt.UTC()}
	}
	result, err := m.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	disposition, err := testRunLifecycleAffected(result)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, "", err
	}
	snapshot, err := m.loadSnapshot(ctx, request.RunID)
	return snapshot, disposition, err
}

func (m testRunLifecycleMutation) ReviseSource(
	ctx context.Context,
	request runtimerunlifecycle.SourceRevisionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	_, current, err := m.load(ctx, request.RunID, true)
	if err != nil {
		return "", err
	}
	if current.Matches(request.Source) {
		return runtimerunlifecycle.MutationExactNoop, nil
	}
	bundleHash, bundleSource := request.Source.StorageValues()
	query := `
		UPDATE runs SET bundle_hash = ?, bundle_source = ?
		WHERE run_id = ? AND status IN ('running', 'paused')
	`
	args := []any{bundleHash, bundleSource, request.RunID}
	if m.dialect == workflowStoreDialectPostgres {
		query = `
			UPDATE runs SET bundle_hash = $2, bundle_source = $3
			WHERE run_id = $1::uuid AND status IN ('running', 'paused')
		`
		args = []any{request.RunID, bundleHash, bundleSource}
	}
	result, err := m.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return "", err
	}
	return testRunLifecycleAffected(result)
}

func (testRunLifecycleMutation) SyncCounters(context.Context, string) error {
	return nil
}

func (m testRunLifecycleMutation) RequirePresentRun(ctx context.Context, runID string) error {
	return m.RequirePresent(ctx, runID)
}

func (m testRunLifecycleMutation) RequireActiveRun(ctx context.Context, runID string) error {
	return m.RequireActive(ctx, runID)
}

func (m testRunLifecycleMutation) RequirePresentRunSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return m.RequirePresentSource(ctx, runID)
}

func (m testRunLifecycleMutation) RequireActiveRunSource(ctx context.Context, runID string) (runtimecorrelation.BundleSourceFact, error) {
	return m.RequireActiveSource(ctx, runID)
}

func (m testRunLifecycleMutation) CreateRun(ctx context.Context, request runtimerunlifecycle.CreateRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return m.Create(ctx, request)
}

func (testRunLifecycleMutation) RequestCompletionCandidate(context.Context, runtimerunlifecycle.CandidateRequest) (runtimerunlifecycle.CandidateRequestDisposition, error) {
	return "", errors.New("pipeline test lifecycle candidate operation is unavailable")
}

func (m testRunLifecycleMutation) TransitionActiveRun(ctx context.Context, request runtimerunlifecycle.ActiveTransitionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return m.TransitionActive(ctx, request)
}

func (m testRunLifecycleMutation) MarkTerminalRun(ctx context.Context, request runtimerunlifecycle.TerminalRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return m.MarkTerminal(ctx, request)
}

func (m testRunLifecycleMutation) ForkRunSource(ctx context.Context, request runtimerunlifecycle.ForkSourceRequest) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	return m.ForkSource(ctx, request)
}

func (m testRunLifecycleMutation) ReviseRunSource(ctx context.Context, request runtimerunlifecycle.SourceRevisionRequest) (runtimerunlifecycle.MutationDisposition, error) {
	return m.ReviseSource(ctx, request)
}

func (m testRunLifecycleMutation) SyncRunCounters(ctx context.Context, runID string) error {
	return m.SyncCounters(ctx, runID)
}

func (m testRunLifecycleMutation) loadSnapshot(
	ctx context.Context,
	runID string,
) (runtimerunlifecycle.Snapshot, error) {
	if m.tx == nil {
		return runtimerunlifecycle.Snapshot{}, errors.New("pipeline test lifecycle transaction is required")
	}
	query := `
		SELECT run_id, status, bundle_hash, bundle_source,
		       origin_kind, COALESCE(trigger_event_id, ''), COALESCE(trigger_event_type, ''),
		       COALESCE(origin_service_id, ''), COALESCE(origin_generation, 0),
		       COALESCE(forked_from_run_id, ''), COALESCE(forked_from_event_id, ''),
		       COALESCE(event_count, 0), COALESCE(entity_count, 0),
		       COALESCE(failure, ''), COALESCE(continued_as_run_id, ''),
		       started_at, ended_at
		FROM runs WHERE run_id = ?
	`
	if m.dialect == workflowStoreDialectPostgres {
		query = `
			SELECT run_id::text, status, bundle_hash, bundle_source,
			       origin_kind, COALESCE(trigger_event_id::text, ''), COALESCE(trigger_event_type, ''),
			       COALESCE(origin_service_id::text, ''), COALESCE(origin_generation, 0),
			       COALESCE(forked_from_run_id::text, ''), COALESCE(forked_from_event_id::text, ''),
			       COALESCE(event_count, 0), COALESCE(entity_count, 0),
			       COALESCE(failure::text, ''), COALESCE(continued_as_run_id::text, ''),
			       started_at, ended_at
			FROM runs WHERE run_id = $1::uuid FOR UPDATE
		`
	}
	var (
		snapshot    runtimerunlifecycle.Snapshot
		stateRaw    string
		failureRaw  string
		originKind  string
		eventID     string
		eventType   string
		serviceID   string
		generation  int64
		sourceRun   string
		sourceEvent string
		startedAt   sql.NullTime
		endedAt     sql.NullTime
	)
	if err := m.tx.QueryRowContext(ctx, query, strings.TrimSpace(runID)).Scan(
		&snapshot.RunID, &stateRaw, &snapshot.BundleHash, &snapshot.BundleSource,
		&originKind, &eventID, &eventType, &serviceID, &generation, &sourceRun, &sourceEvent,
		&snapshot.EventCount, &snapshot.EntityCount, &failureRaw, &snapshot.ContinuedAsRunID,
		&startedAt, &endedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return runtimerunlifecycle.Snapshot{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
		}
		return runtimerunlifecycle.Snapshot{}, err
	}
	var err error
	snapshot.State, err = runtimerunlifecycle.ParseState(stateRaw)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, err
	}
	snapshot.Origin, err = runtimerunlifecycle.DecodeRunOrigin(
		originKind, eventID, eventType, serviceID, generation, sourceRun, sourceEvent,
	)
	if err != nil {
		return runtimerunlifecycle.Snapshot{}, err
	}
	if strings.TrimSpace(failureRaw) != "" {
		failure, decodeErr := runtimefailures.UnmarshalEnvelope([]byte(failureRaw))
		if decodeErr != nil {
			return runtimerunlifecycle.Snapshot{}, decodeErr
		}
		snapshot.Failure = &failure
	}
	if startedAt.Valid {
		snapshot.StartedAt = startedAt.Time.UTC()
	}
	if endedAt.Valid {
		value := endedAt.Time.UTC()
		snapshot.EndedAt = &value
	}
	if err := snapshot.Validate(); err != nil {
		return runtimerunlifecycle.Snapshot{}, err
	}
	return snapshot, nil
}

func testRunLifecycleAffected(result sql.Result) (runtimerunlifecycle.MutationDisposition, error) {
	rows, err := result.RowsAffected()
	if err != nil {
		return "", err
	}
	if rows != 1 {
		return "", fmt.Errorf("pipeline test lifecycle mutation affected %d rows, want 1", rows)
	}
	return runtimerunlifecycle.MutationApplied, nil
}

func testRunLifecycleFailureJSON(failure *runtimefailures.Envelope) (string, error) {
	if failure == nil {
		return "", nil
	}
	raw, err := runtimefailures.MarshalEnvelope(*failure)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func testRunLifecycleFailuresEqual(left, right *runtimefailures.Envelope) bool {
	if left == nil || right == nil {
		return left == right
	}
	leftRaw, leftErr := runtimefailures.MarshalEnvelope(*left)
	rightRaw, rightErr := runtimefailures.MarshalEnvelope(*right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftRaw, rightRaw)
}

func (m testRunLifecycleMutation) load(
	ctx context.Context,
	runID string,
	requireActive bool,
) (runtimerunlifecycle.State, runtimecorrelation.BundleSourceFact, error) {
	if m.tx == nil {
		return "", runtimecorrelation.BundleSourceFact{}, errors.New("pipeline test lifecycle transaction is required")
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return "", runtimecorrelation.BundleSourceFact{}, errors.New("pipeline test lifecycle run_id is required")
	}
	query := `SELECT status, bundle_hash, bundle_source FROM runs WHERE run_id = ?`
	if m.dialect == workflowStoreDialectPostgres {
		query = `SELECT status, bundle_hash, bundle_source FROM runs WHERE run_id = $1::uuid FOR UPDATE`
	}
	var statusRaw, bundleHash, bundleSource string
	if err := m.tx.QueryRowContext(ctx, query, runID).Scan(&statusRaw, &bundleHash, &bundleSource); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", runtimecorrelation.BundleSourceFact{}, &runtimerunlifecycle.RunNotFoundError{RunID: runID}
		}
		return "", runtimecorrelation.BundleSourceFact{}, fmt.Errorf("load pipeline test run lifecycle: %w", err)
	}
	state, err := runtimerunlifecycle.ParseState(statusRaw)
	if err != nil {
		return "", runtimecorrelation.BundleSourceFact{}, err
	}
	if requireActive && !state.Active() {
		return "", runtimecorrelation.BundleSourceFact{}, &runtimerunlifecycle.RunNotActiveError{RunID: runID, State: state}
	}
	source, err := testRunLifecycleSource(bundleHash, bundleSource)
	if err != nil {
		return "", runtimecorrelation.BundleSourceFact{}, err
	}
	return state, source, nil
}

func testRunLifecycleSource(bundleHash, source string) (runtimecorrelation.BundleSourceFact, error) {
	switch strings.TrimSpace(source) {
	case runtimerunlifecycle.BundleSourceEphemeral:
		return runtimecorrelation.NewEphemeralBundleSourceFact(strings.TrimSpace(bundleHash))
	case runtimerunlifecycle.BundleSourcePersisted:
		return runtimecorrelation.NewPersistedBundleSourceFact(strings.TrimSpace(bundleHash))
	default:
		return runtimecorrelation.BundleSourceFact{}, fmt.Errorf("pipeline test run has unsupported bundle_source %q", source)
	}
}

func newPostgresWorkflowInstanceStoreForTest(db *sql.DB) *workflowInstanceStore {
	store := newTestWorkflowInstanceStore(db)
	runner := &recordingRuntimeMutationRunner{
		db:      db,
		dialect: workflowStoreDialectPostgres,
	}
	registerWorkflowPersistenceFixture(store, db, workflowStoreDialectPostgres, runner)
	store.runLifecycle = runner
	store.instanceReader = runner
	store.entityStateReader = runner
	store.targetReader = runner
	store.engineMutations = runner
	store.initialCommits = runner
	return store
}

func newPostgresPipelineCoordinatorForTest(
	bus Bus,
	db *sql.DB,
	opts PipelineCoordinatorOptions,
) *PipelineCoordinator {
	if db != nil && !opts.Persistence.Valid() {
		opts.Persistence = workflowPersistenceForTest(newPostgresWorkflowInstanceStoreForTest(db))
	}
	if opts.PipelineObligations == nil {
		opts.PipelineObligations = unavailablePipelineTestObligationOwner{}
	}
	return newDurablePipelineCoordinatorForTest(bus, db, opts)
}
