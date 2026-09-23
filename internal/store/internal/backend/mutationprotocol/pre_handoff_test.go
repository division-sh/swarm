package mutationprotocol

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	runtimelifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateactivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
	"github.com/google/uuid"
)

type testClaimRetirement func(context.Context) error

func (r testClaimRetirement) RetireCommittedClaim(ctx context.Context) error { return r(ctx) }

type preHandoffCandidateWriter struct{ candidate runtimelifecycle.Candidate }

func (w preHandoffCandidateWriter) WriteCompletionCandidateTx(ctx context.Context, tx *sql.Tx, runID string, _ *time.Time) (runtimelifecycle.CandidateRequestResult, error) {
	if runID != w.candidate.RunID {
		return runtimelifecycle.CandidateRequestResult{}, errors.New("candidate writer received a different run")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO mutation_protocol_probe (value) VALUES (?)`, runID); err != nil {
		return runtimelifecycle.CandidateRequestResult{}, err
	}
	return runtimelifecycle.CandidateRequestResult{Disposition: runtimelifecycle.CandidateRequested, Candidate: w.candidate}, nil
}

type preHandoffCandidateSink struct {
	reserved *preHandoffCandidateAdmission
}

func (s *preHandoffCandidateSink) ReserveCompletionCandidate(context.Context) (runtimelifecycle.CandidateAdmission, error) {
	if s.reserved != nil {
		return nil, errors.New("candidate was reserved twice")
	}
	s.reserved = &preHandoffCandidateAdmission{}
	return s.reserved, nil
}

type preHandoffCandidateAdmission struct {
	submitted   []runtimelifecycle.Candidate
	submitCalls int
	cancelled   int
	onSubmit    func() error
}

func (a *preHandoffCandidateAdmission) Submit(candidate runtimelifecycle.Candidate) error {
	a.submitCalls++
	if a.onSubmit != nil {
		if err := a.onSubmit(); err != nil {
			return err
		}
	}
	a.submitted = append(a.submitted, candidate)
	return nil
}

func (a *preHandoffCandidateAdmission) Cancel() error {
	a.cancelled++
	return nil
}

func TestClaimRetirementCleanupErrorStillHandsOffAcknowledgedCandidate(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		retirementCleanupFail bool
		nativeCleanupFail     bool
	}{
		{name: "retirement cleanup fails after release", retirementCleanupFail: true, nativeCleanupFail: true},
		{name: "retirement succeeds"},
		{name: "retirement succeeds after native cleanup error", nativeCleanupFail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := mutationTestDB(t)
			candidate := runtimelifecycle.Candidate{
				RunID: uuid.NewString(), BundleHash: "pre-handoff-test", Revision: 1,
				DueAt: time.Now().UTC().Truncate(time.Microsecond),
			}
			coordinator := runhandoff.NewCandidateCoordinator()
			sink := &preHandoffCandidateSink{}
			registration, err := coordinator.Register(ctx, runtimelifecycle.CandidateScope{BundleHash: candidate.BundleHash}, sink)
			if err != nil {
				t.Fatal(err)
			}
			defer registration.Release()
			retired := false
			retirementErr := errors.New("claim retirement cleanup failed after release")
			cleanupErr := errors.New("native cleanup failed after commit")
			native := mutationTestRunner(t, db, true)
			result := run(ctx, privateactivity.DialectSQLite, RevisionOnly, Ordinary, NewBaseline(), coordinator,
				func(ctx context.Context, write func(context.Context, *sql.Tx) error) (bool, error) {
					acknowledged, err := native(ctx, write)
					if acknowledged && tc.nativeCleanupFail {
						return true, errors.Join(err, cleanupErr)
					}
					return acknowledged, err
				}, func(ctx context.Context, attempt *Attempt) (string, error) {
					if _, err := attempt.RequestCompletion(ctx, preHandoffCandidateWriter{candidate}, candidate.RunID, nil); err != nil {
						return "", err
					}
					if err := attempt.RetireClaimBeforeCandidateHandoff(testClaimRetirement(func(ctx context.Context) error {
						var rows int
						if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mutation_protocol_probe WHERE value=?`, candidate.RunID).Scan(&rows); err != nil || rows != 1 {
							return errors.New("retirement ran before candidate commit")
						}
						retired = true
						if tc.retirementCleanupFail {
							return retirementErr
						}
						return nil
					})); err != nil {
						return "", err
					}
					sink.reserved.onSubmit = func() error {
						if !retired {
							return errors.New("candidate submitted before claim retirement")
						}
						return nil
					}
					return candidate.RunID, nil
				})
			if !result.Acknowledged() {
				t.Fatalf("committed candidate lost acknowledgement: %+v", result)
			}
			if value, ok := result.Value(); !ok || value != candidate.RunID {
				t.Fatalf("committed candidate value=%q acknowledged=%v", value, ok)
			}
			var durableRows int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mutation_protocol_probe WHERE value=?`, candidate.RunID).Scan(&durableRows); err != nil || durableRows != 1 {
				t.Fatalf("durable candidate rows=%d err=%v", durableRows, err)
			}
			if sink.reserved == nil {
				t.Fatal("candidate reservation was not made")
			}
			if tc.nativeCleanupFail && !errors.Is(result.Err(), cleanupErr) {
				t.Fatalf("native postcommit diagnostic lost: %v", result.Err())
			}
			if tc.retirementCleanupFail && !errors.Is(result.Err(), retirementErr) {
				t.Fatalf("retirement postcommit diagnostic lost: %v", result.Err())
			}
			if !tc.nativeCleanupFail && !tc.retirementCleanupFail && result.Err() != nil {
				t.Fatalf("clean retirement returned error: %v", result.Err())
			}
			if sink.reserved.submitCalls != 1 || len(sink.reserved.submitted) != 1 || !sink.reserved.submitted[0].SameIdentity(candidate) || sink.reserved.cancelled != 0 || !retired {
				t.Fatalf("released claim did not hand off candidate once: admission=%+v retired=%v err=%v", sink.reserved, retired, result.Err())
			}
		})
	}
}

func TestClaimRetirementRunsBeforeCandidateHandoffOnlyForAcknowledgedAttempt(t *testing.T) {
	db := mutationTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cleanupErr := errors.New("native cleanup failed after commit")
	hookErr := errors.New("claim retirement failed after commit")
	var called []int
	attempts := 0
	native := mutationTestRunner(t, db, false, true)
	result := run(ctx, privateactivity.DialectSQLite, RevisionOnly, Ordinary, NewBaseline(), nil,
		func(ctx context.Context, write func(context.Context, *sql.Tx) error) (bool, error) {
			acknowledged, err := native(ctx, write)
			if acknowledged {
				cancel()
				return true, errors.Join(err, cleanupErr)
			}
			return false, err
		}, func(ctx context.Context, attempt *Attempt) (int, error) {
			attempts++
			current := attempts
			if err := attempt.RetireClaimBeforeCandidateHandoff(testClaimRetirement(func(hookCtx context.Context) error {
				if hookCtx.Err() != nil {
					t.Fatalf("postcommit hook inherited caller cancellation: %v", hookCtx.Err())
				}
				var count int
				if err := db.QueryRowContext(hookCtx, `SELECT COUNT(*) FROM mutation_protocol_probe`).Scan(&count); err != nil || count != 1 {
					t.Fatalf("postcommit hook observed uncommitted state: count=%d err=%v", count, err)
				}
				called = append(called, current)
				return hookErr
			})); err != nil {
				return 0, err
			}
			err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `INSERT INTO mutation_protocol_probe (value) VALUES (?)`, current)
				return err
			})
			return current, err
		})
	if !result.Acknowledged() || !errors.Is(result.Err(), cleanupErr) || !errors.Is(result.Err(), hookErr) {
		t.Fatalf("acknowledged postcommit errors=%+v", result)
	}
	if value, ok := result.Value(); !ok || value != 2 || !reflect.DeepEqual(called, []int{2}) {
		t.Fatalf("postcommit attempt: value=%d acknowledged=%v hooks=%v", value, ok, called)
	}
}

func TestClaimRetirementSkipsMissingAcknowledgement(t *testing.T) {
	db := mutationTestDB(t)
	called := false
	result := run(context.Background(), privateactivity.DialectSQLite, RevisionOnly, Ordinary, NewBaseline(), nil,
		mutationTestRunner(t, db, false), func(_ context.Context, attempt *Attempt) (struct{}, error) {
			return struct{}{}, attempt.RetireClaimBeforeCandidateHandoff(testClaimRetirement(func(context.Context) error {
				called = true
				return nil
			}))
		})
	if result.Acknowledged() || called {
		t.Fatalf("unacknowledged attempt ran postcommit hook: result=%+v called=%v", result, called)
	}
}
