package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

// Invariant/reach: ordinary same-run admission overlaps an uncommitted request;
// immutable candidate identity survives dedup, clear, rollback and rediscovery.
func TestRunCompletionEvidenceRequestClearAndRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openRunLifecycleCandidateParityFixture(t, backend)
			ctx := originalStoreFixtureContext(t, testAuthorActivitySourceArtifactContext(), fixture.store)
			runID := uuid.NewString()
			root := ensureRunLifecycleCandidateParityRun(t, fixture, ctx, runID, time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC))
			ctx = runtimecorrelation.WithRootAdmission(ctx, root)
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			writer := selectedAdmissionLifecycleWriter(fixture.store)
			candidates := writer.(mutationprotocol.CandidateWriter)
			due := time.Date(2040, 1, 1, 0, 0, 0, 123456789, time.UTC)
			inserted := make(chan struct{})
			request := func(entered chan<- struct{}, release <-chan struct{}) (runtimerunlifecycle.CandidateRequestResult, error) {
				var result runtimerunlifecycle.CandidateRequestResult
				err := runSelectedFixtureMutation(ctx, fixture.store, "completion evidence overlap", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
					if err := attempt.WithSQL(txctx, func(txctx context.Context, tx *sql.Tx) error {
						_, err := writer.RequireActiveSourceTx(txctx, tx, runID)
						return err
					}); err != nil {
						return err
					}
					if entered != nil {
						close(entered)
					}
					var err error
					result, err = attempt.RequestCompletion(txctx, candidates, runID, &due)
					if err != nil || release == nil {
						return err
					}
					close(inserted)
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
				return result, err
			}
			if fixture.postgres {
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer unblock()
				first := make(chan error, 1)
				firstDone := make(chan struct{})
				var secondDone chan struct{}
				defer func() {
					unblock()
					cancel()
					<-firstDone
					if secondDone != nil {
						<-secondDone
					}
				}()
				go func() {
					defer close(firstDone)
					_, err := request(nil, release)
					first <- err
				}()
				select {
				case <-inserted:
				case err := <-first:
					t.Fatalf("first request failed before its physical cut: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				second := make(chan error, 1)
				secondDone = make(chan struct{})
				go func() {
					defer close(secondDone)
					_, err := request(entered, nil)
					second <- err
				}()
				select {
				case <-entered:
				case err := <-second:
					t.Fatalf("second request failed before shared admission: %v", err)
				case <-ctx.Done():
					t.Fatal("same-run ordinary admission was serialized by completion request", ctx.Err())
				}
				unblock()
				if err := errors.Join(<-first, <-second); err != nil {
					t.Fatal(err)
				}
			} else {
				for i := 0; i < 2; i++ {
					if _, err := request(nil, nil); err != nil {
						t.Fatal(err)
					}
				}
			}
			initial := loadRunLifecycleCandidate(t, fixture, ctx, runID)
			if initial.Revision != 1 || !initial.DueAt.Equal(runtimerunlifecycle.CanonicalTimestamp(due)) {
				t.Fatalf("concurrent request identity: %+v", initial)
			}
			before := snapshotRotationTableRows(t, ctx, fixture.db, `SELECT revision,record_kind,due_at FROM run_completion_evidence WHERE run_id=$1 ORDER BY revision,record_kind`, runID)
			if disposition, err := fixture.store.RequestCompletionCandidate(ctx, runtimerunlifecycle.CandidateAtTime(runID, due)); err != nil || disposition != runtimerunlifecycle.CandidateAlreadyCurrent {
				t.Fatalf("exact duplicate request: %s %v", disposition, err)
			}
			if after := snapshotRotationTableRows(t, ctx, fixture.db, `SELECT revision,record_kind,due_at FROM run_completion_evidence WHERE run_id=$1 ORDER BY revision,record_kind`, runID); !reflect.DeepEqual(before, after) {
				t.Fatal("duplicate rewrote immutable request evidence")
			}
			current := forceRunLifecycleCandidateParity(t, fixture, ctx, runID, due.Add(time.Hour))
			if current.Revision <= initial.Revision {
				t.Fatal("forced request reused a consumed revision")
			}
			if result, err := fixture.store.ExecuteCompletionCandidate(ctx, initial, runtimerunlifecycle.FinalCatalog{}); err != nil || result.Outcome != runtimerunlifecycle.OutcomeExactNoop {
				t.Fatalf("stale candidate consumed newer responsibility: %+v %v", result, err)
			}
			if rediscovered := loadRunLifecycleCandidate(t, fixture, ctx, runID); rediscovered != current {
				t.Fatalf("rediscovery changed current candidate: %+v want %+v", rediscovered, current)
			}
			rollback := errors.New("rollback completion request")
			err := runSelectedFixtureMutation(ctx, fixture.store, "rollback completion evidence", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
				future := due.Add(2 * time.Hour)
				if _, err := attempt.RequestCompletion(txctx, candidates, runID, &future); err != nil {
					return err
				}
				return rollback
			})
			if !errors.Is(err, rollback) || loadRunLifecycleCandidate(t, fixture, ctx, runID) != current {
				t.Fatalf("rolled-back request displaced durable identity: %v", err)
			}
			if disposition, err := fixture.store.RequestCompletionCandidate(ctx, runtimerunlifecycle.ImmediateCandidate(runID)); err != nil || disposition != runtimerunlifecycle.CandidateRequested {
				t.Fatalf("immediate request did not replace the future coordinate: %s %v", disposition, err)
			}
			immediate := loadRunLifecycleCandidate(t, fixture, ctx, runID)
			if immediate.Revision <= current.Revision || !immediate.DueAt.Before(current.DueAt) {
				t.Fatalf("immediate coordinate did not advance identity: %+v after %+v", immediate, current)
			}
			if disposition, err := fixture.store.RequestCompletionCandidate(ctx, runtimerunlifecycle.CandidateAtTime(runID, due)); err != nil || disposition != runtimerunlifecycle.CandidateAlreadyCurrent || loadRunLifecycleCandidate(t, fixture, ctx, runID) != immediate {
				t.Fatalf("future request displaced immediate responsibility: %s %v", disposition, err)
			}
			if _, err := fixture.store.TransitionActiveRun(ctx, runtimerunlifecycle.ActiveTransitionRequest{RunID: runID, State: runtimerunlifecycle.StatePaused}); err != nil {
				t.Fatal(err)
			}
			if _, present := findRunLifecycleCandidate(t, fixture, ctx, runLifecycleCandidateParityBundleHash, runID); present {
				t.Fatal("paused run retained executable completion evidence")
			}
			if disposition, err := fixture.store.RequestCompletionCandidate(ctx, runtimerunlifecycle.ImmediateCandidate(runID)); err != nil || disposition != runtimerunlifecycle.CandidateDeferredPaused {
				t.Fatalf("paused request disposition: %s %v", disposition, err)
			}
			if _, err := fixture.store.TransitionActiveRun(ctx, runtimerunlifecycle.ActiveTransitionRequest{RunID: runID, State: runtimerunlifecycle.StateRunning}); err != nil {
				t.Fatal(err)
			}
			resumed := requestRunLifecycleCandidateParity(t, fixture, ctx, runID).Candidate
			if resumed.Revision <= immediate.Revision {
				t.Fatal("resumed request reused a cleared revision")
			}
			if _, _, err := terminalizeRunLifecycleCandidateParity(fixture, ctx, runID, runtimerunlifecycle.StateCancelled, time.Now().UTC()); err != nil {
				t.Fatal(err)
			}
			if disposition, err := fixture.store.RequestCompletionCandidate(ctx, runtimerunlifecycle.ImmediateCandidate(runID)); err != nil || disposition != runtimerunlifecycle.CandidateAbsorbedTerminal {
				t.Fatalf("terminal request was not absorbed: %s %v", disposition, err)
			}
			if result, err := fixture.store.ExecuteCompletionCandidate(ctx, resumed, runtimerunlifecycle.FinalCatalog{}); err != nil || result.Outcome != runtimerunlifecycle.OutcomeExactNoop {
				t.Fatalf("terminal callback recreated completion responsibility: %+v %v", result, err)
			}
		})
	}
}
