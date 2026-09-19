package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

type fanOutRetrySink struct {
	mu          sync.Mutex
	blocker     *sql.Tx
	admissions  []*fanOutRetryAdmission
	oldCanceled atomic.Bool
}

type fanOutRetryAdmission struct {
	sink      *fanOutRetrySink
	cancels   int
	submitted []runlifecycle.Candidate
}

func (s *fanOutRetrySink) ReserveCompletionCandidate(context.Context) (runlifecycle.CandidateAdmission, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := &fanOutRetryAdmission{sink: s}
	s.admissions = append(s.admissions, a)
	return a, nil
}

func (a *fanOutRetryAdmission) Submit(candidate runlifecycle.Candidate) error {
	a.sink.mu.Lock()
	defer a.sink.mu.Unlock()
	a.submitted = append(a.submitted, candidate)
	return nil
}

func (a *fanOutRetryAdmission) Cancel() error {
	s := a.sink
	s.mu.Lock()
	a.cancels++
	blocker := s.blocker
	s.blocker = nil
	s.mu.Unlock()
	if blocker != nil {
		if err := blocker.Rollback(); err != nil {
			return err
		}
		s.oldCanceled.Store(true)
	}
	return nil
}

// Real granted owner, prepared/sealed group, driver COMMIT, and native SQLite
// reader contention. The existing SQL seam only injects the second-attempt
// refusal; it never manufactures BUSY or supplies successful SQL.
func TestSQLiteFanOutChunkNativeCommitBusyRetry(t *testing.T) {
	for _, abortRetry := range []bool{false, true} {
		name := "committed_retry"
		if abortRetry {
			name = "retry_callback_refused"
		}
		t.Run(name, func(t *testing.T) {
			f := newGroupProofFixture(t, "sqlite", 2)
			f.prepare(t)
			f.seal(t)
			before := f.snapshot(t)
			revision := countP16RunRevisions(t, f.db, f.seed.runID)
			sink := &fanOutRetrySink{}
			registration, err := f.raw.(runlifecycle.CandidateRegistrar).RegisterCompletionCandidateSink(f.ctx, runlifecycle.CandidateScope{BundleHash: f.seed.bundleHash}, sink)
			if err != nil {
				t.Fatal(err)
			}
			defer registration.Release()
			dsn, _, _ := strings.Cut(f.probe.dsn, "?")
			sink.blocker = llmSQLiteBusyBlocker(t, f.db, strings.TrimPrefix(dsn, "file:"), "commit")
			collector, restore, err := InstallTransactionProbeForTest(f.raw, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			injected := errors.New("second fan-out callback refused after attempt reset")
			var afterResetIntentReads atomic.Int32
			var commits atomic.Int32
			f.probe.set(func(phase, query string) error {
				if phase == "before_commit" {
					commits.Add(1)
				}
				// Admission's first intent read precedes the callback reset.
				// Once cancellation has happened this is lockClaimedFanOutIntent;
				// any later matching read after refusal would be stale reconciliation.
				if abortRetry && phase == "before_query" && sink.oldCanceled.Load() && strings.Contains(query, " FROM fan_out_intents WHERE run_id=$1 AND triggering_delivery_id=$2") {
					afterResetIntentReads.Add(1)
					return injected
				}
				return nil
			})
			committed, err := f.owner.CommitFanOutChunk(f.ctx, f.command)
			f.probe.set(nil)
			receipt := collector.Snapshot()
			restore()
			chunk := receipt.ByOperation[transactiontest.FanOutChunk]
			wantCommits, wantWrites, wantRollbacks := uint64(2), uint64(1), uint64(1)
			if abortRetry {
				wantCommits, wantWrites, wantRollbacks = 1, 0, 2
			}
			if chunk.Begun != 2 || chunk.CommitAttempts != wantCommits || chunk.CommitFailures != 1 || chunk.WriteCommits != wantWrites || chunk.RollbackAttempts != wantRollbacks || receipt.Active != 0 || uint64(commits.Load()) != wantCommits {
				t.Fatalf("native fan-out retry accounting: %+v driverCommits=%d", receipt, commits.Load())
			}
			sink.mu.Lock()
			admissions := append([]*fanOutRetryAdmission(nil), sink.admissions...)
			sink.mu.Unlock()
			wantAdmissions := 2
			if abortRetry {
				wantAdmissions = 1
			}
			if len(admissions) != wantAdmissions || admissions[0].cancels != 1 || len(admissions[0].submitted) != 0 {
				t.Fatalf("old attempt admission survived: %+v", admissions)
			}
			if abortRetry {
				if !errors.Is(err, injected) || afterResetIntentReads.Load() != 1 || len(committed.Publications) != 0 || committed.Intent.Request.Key.RunID != "" || committed.PostCommitFailure != nil {
					t.Fatalf("stale operationComplete/result triggered reconciliation: result=%+v err=%v reads=%d", committed, err, afterResetIntentReads.Load())
				}
				f.unchanged(t, before)
			} else {
				if err != nil || committed.PostCommitFailure != nil || committed.Intent.Status != fanoutobligation.StatusClosed || committed.Intent.Cursor != 2 || len(committed.Publications) != 2 {
					t.Fatalf("committed retry lost or duplicated publications: result=%+v err=%v", committed, err)
				}
				seen := map[string]bool{}
				for _, publication := range committed.Publications {
					id := publication.CommittedDurablePublicationEventID()
					if seen[id] {
						t.Fatalf("duplicate returned publication %s", id)
					}
					seen[id] = true
				}
				for _, event := range f.events {
					var events, outcomes int
					if err := f.db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_id=$1`, event.ID()).Scan(&events); err != nil {
						t.Fatal(err)
					}
					if err := f.db.QueryRow(`SELECT COUNT(*) FROM fan_out_outcomes WHERE run_id=$1 AND event_id=$2`, f.seed.runID, event.ID()).Scan(&outcomes); err != nil {
						t.Fatal(err)
					}
					if !seen[event.ID()] || events != 1 || outcomes != 1 {
						t.Fatalf("exact publication readback %s: returned=%v events=%d outcomes=%d", event.ID(), seen, events, outcomes)
					}
				}
				if countP16RunRevisions(t, f.db, f.seed.runID) != revision+1 {
					t.Fatal("retry published more than one revision")
				}
				if admissions[1].cancels != 0 || len(admissions[1].submitted) != 1 {
					t.Fatalf("current candidate lost: %+v", admissions[1])
				}
				page, err := f.raw.(runlifecycle.CandidateStore).ListCompletionCandidates(f.ctx, runlifecycle.CandidateScope{BundleHash: f.seed.bundleHash}, runlifecycle.CandidateCursor{}, 10)
				if err != nil || len(page.Candidates) != 1 || page.Candidates[0].Identity() != admissions[1].submitted[0].Identity() {
					t.Fatalf("candidate readback=%+v err=%v", page, err)
				}
				if err := f.group.ValidateCommitted(f.ctx, f.claims); err != nil {
					t.Fatal(err)
				}
				settled, err := f.group.Settle(f.ctx, f.members())
				if err != nil || len(settled.Results) != 2 {
					t.Fatalf("committed group settlement=%+v err=%v", settled, err)
				}
			}
			if err := f.group.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			for _, claim := range f.claims {
				if err := f.store().Release(f.ctx, claim); !errors.Is(err, pipelineobligation.ErrStaleClaim) {
					t.Fatalf("group claim survived cleanup: %v", err)
				}
			}
			t.Logf("native BUSY then %s: %+v", name, chunk)
		})
	}
}
