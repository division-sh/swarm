package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

const runLifecycleCandidateParityBundleHash = sourceartifactfixture.BundleHash

var runLifecycleCandidateParityReplacementArtifact = sourceartifactfixture.New("agents.yaml", []byte("agents:\n  replacement: {}\n"))
var runLifecycleCandidateParityReplacementHash = runLifecycleCandidateParityReplacementArtifact.BundleHash()

type runLifecycleCandidateParityStore interface {
	sourceartifactfixture.Writer
	runtimerunlifecycle.CandidateStore
	runtimerunlifecycle.OperationOwner
	LoadRunLifecycleSnapshot(context.Context, string) (runtimebus.RunLifecycleSnapshot, error)
}

type runLifecycleCandidateParityFixture struct {
	store    runLifecycleCandidateParityStore
	db       *sql.DB
	postgres bool
}

func TestRunLifecycleTimestampAdmissionParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := testAuthorActivitySourceArtifactContext()
			ctx = storeTestOriginalProcessContext(t, ctx, fixture.store)
			runID := uuid.NewString()
			startedAt := time.Date(2026, 7, 29, 12, 0, 0, 987654321, time.FixedZone("offset", -4*60*60))
			ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, startedAt)

			created, err := fixture.store.LoadRunLifecycleSnapshot(ctx, runID)
			if err != nil {
				t.Fatalf("load created lifecycle: %v", err)
			}
			wantStartedAt := runtimerunlifecycle.CanonicalTimestamp(startedAt)
			if !created.StartedAt.Equal(wantStartedAt) {
				t.Fatalf("started_at = %s, want %s", created.StartedAt, wantStartedAt)
			}

			endedAt := startedAt.Add(time.Minute + 789*time.Nanosecond)
			terminal, _, err := terminalizeRunLifecycleCandidateParity(
				fixture, ctx, runID, runtimerunlifecycle.StateCancelled, endedAt,
			)
			if err != nil {
				t.Fatalf("terminalize lifecycle: %v", err)
			}
			wantEndedAt := runtimerunlifecycle.CanonicalTimestamp(endedAt)
			if terminal.EndedAt == nil || !terminal.EndedAt.Equal(wantEndedAt) {
				t.Fatalf("ended_at = %v, want %s", terminal.EndedAt, wantEndedAt)
			}
		})
	}
}

func TestRunLifecycleCreateIsInsertOrExactNoopParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := testAuthorActivitySourceArtifactContext()
			ctx = storeTestOriginalProcessContext(t, ctx, fixture.store)
			sourceartifactfixture.Require(t, ctx, fixture.store)
			source, err := runtimecorrelation.NewSourceArtifactFact(runLifecycleCandidateParityBundleHash)
			if err != nil {
				t.Fatal(err)
			}
			at := time.Date(2026, 7, 29, 14, 0, 0, 0, time.UTC)

			t.Run("exact_duplicate", func(t *testing.T) {
				origin, err := runtimerunlifecycle.EventRunOrigin(uuid.NewString(), "test.started")
				if err != nil {
					t.Fatal(err)
				}
				request := runtimerunlifecycle.CreateRequest{
					RunID: uuid.NewString(), Origin: origin,
					Source: source, StartedAt: at,
				}
				if got, err := createRunLifecycleParity(fixture, ctx, request); err != nil || got != runtimerunlifecycle.MutationApplied {
					t.Fatalf("first create = %s/%v", got, err)
				}
				request.StartedAt = at.Add(time.Hour)
				if got, err := createRunLifecycleParity(fixture, ctx, request); err != nil || got != runtimerunlifecycle.MutationExactNoop {
					t.Fatalf("exact duplicate create = %s/%v", got, err)
				}
			})

			t.Run("trigger_conflict_is_not_backfilled", func(t *testing.T) {
				runID := uuid.NewString()
				request := runtimerunlifecycle.CreateRequest{
					RunID: runID, Origin: runtimerunlifecycle.ScenarioSetupRunOrigin(),
					Source: source, StartedAt: at,
				}
				if got, err := createRunLifecycleParity(fixture, ctx, request); err != nil || got != runtimerunlifecycle.MutationApplied {
					t.Fatalf("first create = %s/%v", got, err)
				}
				request.Origin, err = runtimerunlifecycle.EventRunOrigin(uuid.NewString(), "test.late_trigger")
				if err != nil {
					t.Fatal(err)
				}
				if got, err := createRunLifecycleParity(fixture, ctx, request); err == nil || got != "" {
					t.Fatalf("conflicting create = %s/%v", got, err)
				}
				kind, triggerID, triggerType := loadRunLifecycleOriginFacts(t, fixture, ctx, runID)
				if kind != string(runtimerunlifecycle.OriginScenarioSetup) || triggerID != "" || triggerType != "" {
					t.Fatalf("conflicting create rewrote origin = %s/%s/%s", kind, triggerID, triggerType)
				}
			})
		})
	}
}

func openRunLifecycleCandidateParityFixture(t *testing.T, backend string) runLifecycleCandidateParityFixture {
	t.Helper()
	if backend == "sqlite" {
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		store.nowFn = func() time.Time {
			return time.Date(2026, 7, 29, 12, 0, 0, 0, time.UTC)
		}
		return runLifecycleCandidateParityFixture{store: store, db: store.backend.ConstructionHandle()}
	}
	_, db, _ := testutil.StartPostgres(t)
	store := admitTestPostgresStore(t, db)
	return runLifecycleCandidateParityFixture{store: store, db: db, postgres: true}
}

func ensureRunLifecycleCandidateParityRun(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
	startedAt time.Time,
) {
	t.Helper()
	sourceartifactfixture.Require(t, ctx, fixture.store)
	source, err := runtimecorrelation.NewSourceArtifactFact(runLifecycleCandidateParityBundleHash)
	if err != nil {
		t.Fatal(err)
	}
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, source)
	if _, ok := runtimeauthoractivity.ScopeFromContext(ctx); !ok {
		ctx = runtimeauthoractivity.WithScope(
			ctx,
			runtimeauthoractivity.BundleScope(uuid.NewString(), runLifecycleCandidateParityBundleHash),
		)
	}
	if _, err := fixture.store.CreateRun(ctx, runtimerunlifecycle.CreateRequest{
		RunID: runID, Origin: runtimerunlifecycle.ScenarioSetupRunOrigin(),
		Source: source, StartedAt: startedAt,
	}); err != nil {
		t.Fatalf("ensure run %s: %v", runID, err)
	}
}

func createRunLifecycleParity(
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	request runtimerunlifecycle.CreateRequest,
) (runtimerunlifecycle.MutationDisposition, error) {
	return fixture.store.CreateRun(ctx, request)
}

func transitionRunLifecycleParity(
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
	state runtimerunlifecycle.State,
) (runtimerunlifecycle.MutationDisposition, error) {
	return fixture.store.TransitionActiveRun(ctx, runtimerunlifecycle.ActiveTransitionRequest{
		RunID: runID, State: state,
	})
}

func reviseRunLifecycleSourceParity(
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
	source runtimecorrelation.SourceArtifactFact,
) (runtimerunlifecycle.MutationDisposition, error) {
	if source.BundleHash() == runLifecycleCandidateParityReplacementHash {
		if err := sourceartifactfixture.EnsureArtifact(ctx, fixture.store, runLifecycleCandidateParityReplacementArtifact); err != nil {
			return "", err
		}
	}
	return fixture.store.ReviseRunSource(ctx, runtimerunlifecycle.SourceRevisionRequest{
		RunID: runID, Source: source,
	})
}

func requestRunLifecycleCandidateParity(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
) runtimerunlifecycle.CandidateRequestResult {
	t.Helper()
	disposition, err := fixture.store.RequestCompletionCandidate(
		ctx,
		runtimerunlifecycle.ImmediateCandidate(runID),
	)
	if err != nil {
		t.Fatalf("request completion candidate: %v", err)
	}
	if disposition == runtimerunlifecycle.CandidateAbsorbedTerminal {
		return runtimerunlifecycle.CandidateRequestResult{Disposition: disposition}
	}
	candidate := loadRunLifecycleCandidate(t, fixture, ctx, runID)
	return runtimerunlifecycle.CandidateRequestResult{Disposition: disposition, Candidate: candidate}
}

func forceRunLifecycleCandidateParity(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
	dueAt time.Time,
) runtimerunlifecycle.Candidate {
	t.Helper()
	var result runtimerunlifecycle.CandidateRequestResult
	var err error
	switch store := fixture.store.(type) {
	case *PostgresStore:
		err = store.runPrivateAuthorActivityMutation(ctx, func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
			result, err = requestPostgresCompletionCandidateTx(txctx, tx, runID, &dueAt, true)
			return err
		})
	case *SQLiteRuntimeStore:
		err = store.runPrivateAuthorActivityMutation(ctx, "test force sqlite completion candidate revision", func(txctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
			result, err = requestSQLiteCompletionCandidateTx(txctx, tx, runID, &dueAt, store.now(), true)
			return err
		})
	default:
		err = errors.New("unsupported run lifecycle candidate parity store")
	}
	if err != nil {
		t.Fatalf("force completion candidate revision: %v", err)
	}
	if result.Disposition != runtimerunlifecycle.CandidateRequested {
		t.Fatalf("forced completion candidate disposition = %s", result.Disposition)
	}
	return result.Candidate
}

func loadRunLifecycleCandidate(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
) runtimerunlifecycle.Candidate {
	t.Helper()
	candidate, found := findRunLifecycleCandidate(
		t, fixture, ctx, runLifecycleCandidateParityBundleHash, runID,
	)
	if found {
		return candidate
	}
	t.Fatalf("candidate %s is not durable", runID)
	return runtimerunlifecycle.Candidate{}
}

func findRunLifecycleCandidate(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	bundleHash string,
	runID string,
) (runtimerunlifecycle.Candidate, bool) {
	t.Helper()
	page, err := fixture.store.ListCompletionCandidates(
		ctx,
		runtimerunlifecycle.CandidateScope{BundleHash: bundleHash},
		runtimerunlifecycle.CandidateCursor{},
		128,
	)
	if err != nil {
		t.Fatalf("list completion candidates: %v", err)
	}
	for _, candidate := range page.Candidates {
		if candidate.RunID == runID {
			return candidate, true
		}
	}
	return runtimerunlifecycle.Candidate{}, false
}

func terminalizeRunLifecycleCandidateParity(
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
	state runtimerunlifecycle.State,
	endedAt time.Time,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	if state == runtimerunlifecycle.StateCompleted {
		return completeRunLifecycleCandidateParity(fixture, ctx, runID, endedAt)
	}
	failure := (*runtimefailures.Envelope)(nil)
	if state == runtimerunlifecycle.StateFailed {
		failure = testRetryableFailure()
	}
	return fixture.store.MarkTerminalRun(ctx, runtimerunlifecycle.TerminalRequest{
		RunID: runID, State: state, Failure: failure, EndedAt: endedAt,
	})
}

func completeRunLifecycleCandidateParity(
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
	endedAt time.Time,
) (runtimerunlifecycle.Snapshot, runtimerunlifecycle.MutationDisposition, error) {
	var snapshot runtimerunlifecycle.Snapshot
	var disposition runtimerunlifecycle.MutationDisposition
	err := runSelectedFixtureMutation(ctx, fixture.store, "test successful completion", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
		var inner error
		switch store := fixture.store.(type) {
		case *PostgresStore:
			snapshot, disposition, inner = store.runLifecyclePostgresOwner.CompleteRunTx(txctx, attempt, runID, endedAt, nil)
		case *SQLiteRuntimeStore:
			snapshot, disposition, inner = store.runLifecycleSQLiteOwner.CompleteRunTx(txctx, attempt, runID, endedAt, nil)
		default:
			inner = errors.New("unsupported run lifecycle candidate parity store")
		}
		return inner
	})
	return snapshot, disposition, err
}

func loadRunLifecycleCandidateFacts(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
) (string, bool, int64) {
	t.Helper()
	query := `SELECT status, completion_due_at, completion_revision FROM runs WHERE run_id = ?`
	if fixture.postgres {
		query = `SELECT status, completion_due_at, completion_revision FROM runs WHERE run_id = $1::uuid`
	}
	var state string
	var revision int64
	if fixture.postgres {
		var dueAt sql.NullTime
		if err := fixture.db.QueryRowContext(ctx, query, runID).Scan(&state, &dueAt, &revision); err != nil {
			t.Fatalf("load run lifecycle candidate facts: %v", err)
		}
		return state, dueAt.Valid, revision
	}
	var dueAt any
	if err := fixture.db.QueryRowContext(ctx, query, runID).Scan(&state, &dueAt, &revision); err != nil {
		t.Fatalf("load run lifecycle candidate facts: %v", err)
	}
	_, present, err := sqliteTimeValue(dueAt)
	if err != nil {
		t.Fatalf("decode run lifecycle candidate due_at: %v", err)
	}
	return state, present, revision
}

func loadRunLifecycleOriginFacts(
	t *testing.T,
	fixture runLifecycleCandidateParityFixture,
	ctx context.Context,
	runID string,
) (string, string, string) {
	t.Helper()
	query := `SELECT origin_kind, COALESCE(trigger_event_id, ''), COALESCE(trigger_event_type, '') FROM runs WHERE run_id = ?`
	if fixture.postgres {
		query = `SELECT origin_kind, COALESCE(trigger_event_id::text, ''), COALESCE(trigger_event_type, '') FROM runs WHERE run_id = $1::uuid`
	}
	var kind, triggerID, triggerType string
	if err := fixture.db.QueryRowContext(ctx, query, runID).Scan(&kind, &triggerID, &triggerType); err != nil {
		t.Fatalf("load run lifecycle origin facts: %v", err)
	}
	return kind, triggerID, triggerType
}
