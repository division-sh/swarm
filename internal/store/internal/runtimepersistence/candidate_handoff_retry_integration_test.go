package runtimepersistence

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

type lifecycleRetrySink struct {
	blocker    *sql.Tx
	admissions []*lifecycleRetryAdmission
}

type lifecycleRetryAdmission struct {
	sink      *lifecycleRetrySink
	cancels   int
	submitted []runlifecycle.Candidate
}

func (s *lifecycleRetrySink) ReserveCompletionCandidate(context.Context) (runlifecycle.CandidateAdmission, error) {
	a := &lifecycleRetryAdmission{sink: s}
	s.admissions = append(s.admissions, a)
	return a, nil
}

func (a *lifecycleRetryAdmission) Submit(candidate runlifecycle.Candidate) error {
	a.submitted = append(a.submitted, candidate)
	return nil
}

func (a *lifecycleRetryAdmission) Cancel() error {
	a.cancels++
	if a.sink.blocker != nil {
		blocker := a.sink.blocker
		a.sink.blocker = nil
		return blocker.Rollback()
	}
	return nil
}

func TestSQLiteLifecycleCandidateCommitBusyResetsOuterHandoff(t *testing.T) {
	path := filepath.Join(t.TempDir(), "candidate.db")
	store := newBootstrappedSQLiteRuntimeStoreForPath(t, path)
	db := store.backend.ConstructionHandle()
	f := newExactFactFixture(t, exactFactStore{db: db, selected: store})
	var bundle string
	if err := db.QueryRow(`SELECT bundle_hash FROM runs WHERE run_id=$1`, f.runID).Scan(&bundle); err != nil {
		t.Fatal(err)
	}
	sink := &lifecycleRetrySink{}
	registration, err := store.RegisterCompletionCandidateSink(context.Background(), runlifecycle.CandidateScope{BundleHash: bundle}, sink)
	if err != nil {
		t.Fatal(err)
	}
	defer registration.Release()
	// A real reader forces the first COMMIT to return native SQLITE_BUSY.
	// Canceling its rolled-back admission releases that reader before retry.
	sink.blocker = llmSQLiteBusyBlocker(t, db, path, "commit")
	probe, restore, err := store.backend.InstallTransactionProbeForTest(transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 10*time.Second)
	defer cancel()
	disposition, err := store.RequestCompletionCandidate(ctx, runlifecycle.ImmediateCandidate(f.runID))
	if err != nil || disposition != runlifecycle.CandidateRequested {
		t.Fatalf("candidate disposition=%s err=%v", disposition, err)
	}
	counts := probe.Snapshot()
	if counts.Total.Begun != 2 || counts.Total.CommitAttempts != 2 || counts.Total.CommitFailures != 1 || counts.Total.RollbackAttempts != 1 || counts.Total.WriteCommits != 1 || counts.Active != 0 {
		t.Fatalf("real lifecycle retry accounting: %+v", counts)
	}
	if len(sink.admissions) != 2 {
		t.Fatalf("candidate admissions=%d want two attempts", len(sink.admissions))
	}
	old, current := sink.admissions[0], sink.admissions[1]
	if old.cancels != 1 || len(old.submitted) != 0 || current.cancels != 0 || len(current.submitted) != 1 {
		t.Fatalf("stale or lost notification: old=%+v current=%+v", old, current)
	}
	page, err := store.ListCompletionCandidates(ctx, runlifecycle.CandidateScope{BundleHash: bundle}, runlifecycle.CandidateCursor{}, 10)
	if err != nil || len(page.Candidates) != 1 {
		t.Fatalf("committed candidate readback=%+v err=%v", page, err)
	}
	if page.Candidates[0].Identity() != current.submitted[0].Identity() {
		t.Fatalf("submission differs from durable candidate: %+v / %+v", current.submitted, page)
	}
	t.Logf("native commit BUSY lifecycle proof: %+v", counts.Total)
}
