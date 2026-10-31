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
	"github.com/division-sh/swarm/internal/sourceartifact"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

const runLifecycleCandidateParityBundleHash = sourceartifactfixture.BundleHash

// Physical readback stays independent of the lifecycle owner's projection.
const completionEvidenceNativeSnapshotSQL = `SELECT r.status, COALESCE(e.revision,0),
	COALESCE(CAST(CASE WHEN consumed.run_id IS NULL THEN e.due_at ELSE NULL END AS TEXT),''),r.bundle_hash
	FROM runs r LEFT JOIN run_completion_evidence e ON e.run_id=r.run_id AND e.record_kind='request'
	AND e.revision=(SELECT MAX(revision) FROM run_completion_evidence WHERE run_id=r.run_id AND record_kind='request')
	LEFT JOIN run_completion_evidence consumed ON consumed.run_id=e.run_id AND consumed.revision=e.revision AND consumed.record_kind='clear'
	WHERE r.run_id=$1`

var runLifecycleCandidateParityReplacementArtifact = func() *sourceartifact.AdmittedSourceArtifact {
	artifact, err := sourceartifact.AdmitMembers(map[string][]byte{
		"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
		"agents.yaml":   []byte("agents:\n  replacement: {}\n"),
	})
	if err != nil {
		panic(err)
	}
	return artifact
}()
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
			ctx = originalStoreFixtureContext(t, ctx, fixture.store)
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
			ctx = originalStoreFixtureContext(t, ctx, fixture.store)
			rooted, _ := sourceartifactfixture.Rooted(t, "candidate-source", sourceartifactfixture.Artifact())
			ctx = runtimecorrelation.WithRootAdmission(ctx, rooted.Admission())
			sourceartifactfixture.RequireArtifact(t, ctx, fixture.store, rooted.Artifact())
			source, err := runtimecorrelation.NewSourceArtifactFact(runLifecycleCandidateParityBundleHash)
			if err != nil {
				t.Fatal(err)
			}
			at := time.Date(2026, 7, 29, 14, 0, 0, 0, time.UTC)

			t.Run("live_selection", func(t *testing.T) {
				requireLiveRunControlSelection(t, fixture, ctx)
			})

			t.Run("exact_duplicate", func(t *testing.T) {
				origin, err := runtimerunlifecycle.EventRunOrigin(uuid.NewString(), "test.started")
				if err != nil {
					t.Fatal(err)
				}
				request := runtimerunlifecycle.CreateRequest{
					RunID: uuid.NewString(), Origin: origin,
					Source: source, RootAdmission: rooted.Admission(), StartedAt: at,
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
					Source: source, RootAdmission: rooted.Admission(), StartedAt: at,
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
) sourceartifact.RootAdmission {
	t.Helper()
	rooted, _ := sourceartifactfixture.Rooted(t, "fixture", sourceartifactfixture.Artifact())
	sourceartifactfixture.RequireArtifact(t, ctx, fixture.store, rooted.Artifact())
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
		Source: source, RootAdmission: rooted.Admission(), StartedAt: startedAt,
	}); err != nil {
		t.Fatalf("ensure run %s: %v", runID, err)
	}
	return rooted.Admission()
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
	rooted sourceartifact.RootedSource,
) (runtimerunlifecycle.MutationDisposition, error) {
	if err := sourceartifactfixture.EnsureArtifact(ctx, fixture.store, rooted.Artifact()); err != nil {
		return "", err
	}
	return fixture.store.ReviseRunSource(ctx, runtimerunlifecycle.SourceRevisionRequest{
		RunID: runID, Source: source, RootAdmission: rooted.Admission(),
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
