package storetest

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

const SemanticFixtureBundleHash = sourceartifactfixture.BundleHash
const semanticFixtureRuntimeInstanceID = "00000000-0000-4000-8000-000000000001"

type runLifecycleOperationRunner interface {
	runtimerunlifecycle.OperationOwner
	runtimerunlifecycle.CandidateStore
}

type RunFixture struct {
	RunID      string
	State      runtimerunlifecycle.State
	Origin     runtimerunlifecycle.RunOrigin
	Artifact   *sourceartifact.AdmittedSourceArtifact
	BundleHash string
	StartedAt  time.Time
	EndedAt    time.Time
	Failure    *runtimefailures.Envelope
}

func ScenarioSetupOrigin() runtimerunlifecycle.RunOrigin {
	return runtimerunlifecycle.ScenarioSetupRunOrigin()
}

func EventOrigin(t testing.TB, eventID, eventType string) runtimerunlifecycle.RunOrigin {
	t.Helper()
	origin, err := runtimerunlifecycle.EventRunOrigin(eventID, eventType)
	if err != nil {
		t.Fatalf("construct semantic event run origin: %v", err)
	}
	return origin
}

func RequireRun(
	t testing.TB,
	ctx context.Context,
	runner runLifecycleOperationRunner,
	fixture RunFixture,
) {
	t.Helper()
	if err := MaterializeRun(ctx, runner, fixture); err != nil {
		t.Fatalf("materialize semantic run fixture %s: %v", strings.TrimSpace(fixture.RunID), err)
	}
}

func RequireRunningRun(
	t testing.TB,
	ctx context.Context,
	runner runLifecycleOperationRunner,
	runID string,
	startedAt time.Time,
) {
	t.Helper()
	RequireRun(t, ctx, runner, RunFixture{
		RunID:     runID,
		State:     runtimerunlifecycle.StateRunning,
		Origin:    runtimerunlifecycle.ScenarioSetupRunOrigin(),
		StartedAt: startedAt,
	})
}

func RequirePausedRun(
	t testing.TB,
	ctx context.Context,
	runner runLifecycleOperationRunner,
	runID string,
	startedAt time.Time,
) {
	t.Helper()
	RequireRun(t, ctx, runner, RunFixture{
		RunID:     runID,
		State:     runtimerunlifecycle.StatePaused,
		Origin:    runtimerunlifecycle.ScenarioSetupRunOrigin(),
		StartedAt: startedAt,
	})
}

func RequirePostgresRun(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	fixture RunFixture,
) {
	t.Helper()
	RequireRun(t, ctx, AdmitPostgresRuntimeStore(t, db), fixture)
}

func RequireSQLiteRun(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	fixture RunFixture,
) {
	t.Helper()
	RequireRun(t, ctx, AdmitSQLiteRuntimeStore(t, db), fixture)
}

func RequireRunningPostgresRun(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	runID string,
	startedAt time.Time,
) {
	t.Helper()
	RequireRunningRun(t, ctx, AdmitPostgresRuntimeStore(t, db), runID, startedAt)
}

func RequireRunningSQLiteRun(
	t testing.TB,
	ctx context.Context,
	db *sql.DB,
	runID string,
	startedAt time.Time,
) {
	t.Helper()
	RequireRunningRun(t, ctx, AdmitSQLiteRuntimeStore(t, db), runID, startedAt)
}

func MaterializeRun(
	ctx context.Context,
	runner runLifecycleOperationRunner,
	fixture RunFixture,
) error {
	if runner == nil {
		return fmt.Errorf("semantic run fixture requires lifecycle owner")
	}
	fixture.RunID = strings.TrimSpace(fixture.RunID)
	if fixture.RunID == "" {
		return fmt.Errorf("semantic run fixture requires run_id")
	}
	if fixture.State == "" {
		fixture.State = runtimerunlifecycle.StateRunning
	}
	if fixture.State == runtimerunlifecycle.StateForked {
		return fmt.Errorf("semantic run fixture must use the named fork operation")
	}
	bundleHash := fixture.BundleHash
	if fixture.Artifact != nil {
		if bundleHash != "" && strings.TrimSpace(bundleHash) != fixture.Artifact.BundleHash() {
			return fmt.Errorf("semantic run fixture bundle_hash %s contradicts artifact %s", bundleHash, fixture.Artifact.BundleHash())
		}
		bundleHash = fixture.Artifact.BundleHash()
	}
	source, err := semanticFixtureSource(ctx, bundleHash)
	if err != nil {
		return err
	}
	if err := ensureSemanticFixtureSourceArtifact(ctx, runner, source.BundleHash(), fixture.Artifact); err != nil {
		return err
	}
	if fixture.StartedAt.IsZero() {
		fixture.StartedAt = time.Now().UTC()
	}
	if fixture.EndedAt.IsZero() {
		fixture.EndedAt = fixture.StartedAt
	}
	ctx = semanticFixtureContext(ctx, source)
	if _, err = runner.CreateRun(ctx, runtimerunlifecycle.CreateRequest{
		RunID:     fixture.RunID,
		Origin:    fixture.Origin,
		Source:    source,
		StartedAt: fixture.StartedAt.UTC(),
	}); err != nil {
		return err
	}
	switch fixture.State {
	case runtimerunlifecycle.StateRunning:
	case runtimerunlifecycle.StatePaused:
		_, err = runner.TransitionActiveRun(ctx, runtimerunlifecycle.ActiveTransitionRequest{
			RunID: fixture.RunID,
			State: runtimerunlifecycle.StatePaused,
		})
	case runtimerunlifecycle.StateCompleted:
		_, err = runner.RequestCompletionCandidate(
			ctx,
			runtimerunlifecycle.ImmediateCandidate(fixture.RunID),
		)
	case runtimerunlifecycle.StateFailed,
		runtimerunlifecycle.StateCancelled:
		_, _, err = runner.MarkTerminalRun(ctx, runtimerunlifecycle.TerminalRequest{
			RunID: fixture.RunID, State: fixture.State, Failure: fixture.Failure, EndedAt: fixture.EndedAt.UTC(),
		})
	default:
		return fmt.Errorf("semantic run fixture state %q is unsupported", fixture.State)
	}
	if err != nil || fixture.State != runtimerunlifecycle.StateCompleted {
		return err
	}
	result, err := ExecuteRunCompletionCandidate(
		ctx,
		runner,
		source.BundleHash(),
		fixture.RunID,
		runtimerunlifecycle.TerminalCatalog{},
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

type sourceArtifactFixtureOwner interface {
	sourceartifactfixture.Writer
	GetSourceArtifact(context.Context, string) (sourceartifact.Persisted, error)
}

func ensureSemanticFixtureSourceArtifact(ctx context.Context, runner runLifecycleOperationRunner, bundleHash string, artifact *sourceartifact.AdmittedSourceArtifact) error {
	owner, ok := runner.(sourceArtifactFixtureOwner)
	if !ok {
		return fmt.Errorf("semantic fixture source artifact owner is required; got %T", runner)
	}
	if artifact == nil && bundleHash == SemanticFixtureBundleHash {
		artifact = sourceartifactfixture.Artifact()
	}
	if artifact != nil {
		if artifact.BundleHash() != bundleHash {
			return fmt.Errorf("semantic fixture source artifact %s contradicts requested %s", artifact.BundleHash(), bundleHash)
		}
		return sourceartifactfixture.EnsureArtifact(ctx, owner, artifact)
	}
	persisted, err := owner.GetSourceArtifact(ctx, bundleHash)
	if err != nil {
		return fmt.Errorf("semantic run fixture requires exact admitted source artifact %s: %w", bundleHash, err)
	}
	if err := persisted.Validate(); err != nil {
		return fmt.Errorf("validate semantic run fixture source artifact %s: %w", bundleHash, err)
	}
	return nil
}

func EnsureEphemeralRun(
	ctx context.Context,
	runner runLifecycleOperationRunner,
	runID string,
	startedAt time.Time,
) error {
	if runner == nil || strings.TrimSpace(runID) == "" {
		return fmt.Errorf("semantic fixture run requires mutation owner and run_id")
	}
	source, err := semanticFixtureSource(ctx, "")
	if err != nil {
		return err
	}
	if err := ensureSemanticFixtureSourceArtifact(ctx, runner, source.BundleHash(), nil); err != nil {
		return err
	}
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	ctx = semanticFixtureContext(ctx, source)
	_, err = runner.CreateRun(ctx, runtimerunlifecycle.CreateRequest{
		RunID:     strings.TrimSpace(runID),
		Origin:    runtimerunlifecycle.ScenarioSetupRunOrigin(),
		Source:    source,
		StartedAt: startedAt.UTC(),
	})
	return err
}

func EnsureRunForAdmittedEvent(
	ctx context.Context,
	runner runLifecycleOperationRunner,
	admitted events.AdmittedEvent,
	startedAt time.Time,
) error {
	event := admitted.Event()
	runID := strings.TrimSpace(event.RunID())
	if admitted.RunDisposition() == events.AdmittedRunless {
		if runID != "" {
			return fmt.Errorf("runless event %s declares run_id %s", admitted.ID(), runID)
		}
		return nil
	}
	if runner == nil || runID == "" {
		return fmt.Errorf("semantic event fixture requires mutation owner and run_id")
	}
	switch admitted.RunDisposition() {
	case events.AdmittedRunRequireActive:
		return runner.RequireActiveRun(ctx, runID)
	case events.AdmittedRunRequirePresent:
		return runner.RequirePresentRun(ctx, runID)
	case events.AdmittedRunCreateAuthorized:
	default:
		return fmt.Errorf(
			"semantic event fixture run disposition %q is unsupported",
			admitted.RunDisposition(),
		)
	}
	source, err := semanticFixtureSource(ctx, "")
	if err != nil {
		return err
	}
	if err := ensureSemanticFixtureSourceArtifact(ctx, runner, source.BundleHash(), nil); err != nil {
		return err
	}
	if startedAt.IsZero() {
		startedAt = time.Now().UTC()
	}
	request := runtimerunlifecycle.CreateRequest{
		RunID:     runID,
		Origin:    runtimerunlifecycle.RunOrigin{},
		Source:    source,
		StartedAt: startedAt.UTC(),
	}
	request.Origin, err = runtimerunlifecycle.EventRunOrigin(
		admitted.ID(),
		strings.TrimSpace(string(event.Type())),
	)
	if err != nil {
		return err
	}
	ctx = semanticFixtureContext(ctx, source)
	_, err = runner.CreateRun(ctx, request)
	return err
}

func semanticFixtureSource(
	ctx context.Context,
	bundleHash string,
) (runtimecorrelation.SourceArtifactFact, error) {
	bundleHash = strings.TrimSpace(bundleHash)
	if current, ok := runtimecorrelation.SourceArtifactFactFromContext(ctx); ok {
		if bundleHash == "" || current.BundleHash() == bundleHash {
			return current, nil
		}
	}
	if bundleHash == "" {
		if scope, ok := runtimeauthoractivity.ScopeFromContext(ctx); ok &&
			scope.Kind == runtimeauthoractivity.ScopeBundle {
			bundleHash = strings.TrimSpace(scope.BundleHash)
		}
	}
	if bundleHash == "" {
		bundleHash = SemanticFixtureBundleHash
	}
	return runtimecorrelation.NewSourceArtifactFact(bundleHash)
}

func semanticFixtureContext(
	ctx context.Context,
	source runtimecorrelation.SourceArtifactFact,
) context.Context {
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, source)
	runtimeInstanceID := semanticFixtureRuntimeInstanceID
	if scope, ok := runtimeauthoractivity.ScopeFromContext(ctx); ok &&
		strings.TrimSpace(scope.RuntimeInstanceID) != "" {
		runtimeInstanceID = scope.RuntimeInstanceID
	}
	return runtimeauthoractivity.WithScope(
		ctx,
		runtimeauthoractivity.BundleScope(runtimeInstanceID, source.BundleHash()),
	)
}

func TerminalizeRun(
	ctx context.Context,
	runner runLifecycleOperationRunner,
	request runtimerunlifecycle.TerminalRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if runner == nil {
		return runtimerunlifecycle.Snapshot{}, "", fmt.Errorf("semantic fixture terminal run requires mutation owner")
	}
	return runner.MarkTerminalRun(ctx, request)
}

func TransitionRun(
	ctx context.Context,
	runner runLifecycleOperationRunner,
	request runtimerunlifecycle.ActiveTransitionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	if runner == nil {
		return "", fmt.Errorf("semantic fixture active transition requires mutation owner")
	}
	return runner.TransitionActiveRun(ctx, request)
}

func ForkRun(
	ctx context.Context,
	runner runLifecycleOperationRunner,
	request runtimerunlifecycle.ForkSourceRequest,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if runner == nil {
		return runtimerunlifecycle.Snapshot{}, "", fmt.Errorf("semantic fixture fork transition requires mutation owner")
	}
	return runner.ForkRunSource(ctx, request)
}

func ReviseRunSource(
	ctx context.Context,
	runner runLifecycleOperationRunner,
	request runtimerunlifecycle.SourceRevisionRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	if runner == nil {
		return "", fmt.Errorf("semantic fixture source revision requires mutation owner")
	}
	return runner.ReviseRunSource(ctx, request)
}

func SyncRunCounters(
	ctx context.Context,
	runner runLifecycleOperationRunner,
	runID string,
) error {
	if runner == nil {
		return fmt.Errorf("semantic fixture counter synchronization requires mutation owner")
	}
	return runner.SyncRunCounters(ctx, strings.TrimSpace(runID))
}

func ExecuteRunCompletionCandidate(
	ctx context.Context,
	store runtimerunlifecycle.CandidateStore,
	bundleHash string,
	runID string,
	catalog runtimerunlifecycle.TerminalCatalog,
) (runtimerunlifecycle.CompletionResult, error) {
	scope := runtimerunlifecycle.CandidateScope{BundleHash: strings.TrimSpace(bundleHash)}
	if err := scope.Validate(); err != nil {
		return runtimerunlifecycle.CompletionResult{}, err
	}
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return runtimerunlifecycle.CompletionResult{}, fmt.Errorf("completion candidate test execution requires run_id")
	}
	cursor := runtimerunlifecycle.CandidateCursor{}
	for {
		page, err := store.ListCompletionCandidates(ctx, scope, cursor, 128)
		if err != nil {
			return runtimerunlifecycle.CompletionResult{}, err
		}
		for _, candidate := range page.Candidates {
			if candidate.RunID == runID {
				return store.ExecuteCompletionCandidate(ctx, candidate, catalog)
			}
		}
		if page.Exhausted {
			return runtimerunlifecycle.CompletionResult{}, fmt.Errorf("completion candidate for run %s is not durable", runID)
		}
		cursor = page.Next
	}
}
