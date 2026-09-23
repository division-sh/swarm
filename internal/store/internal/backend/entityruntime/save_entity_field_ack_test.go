package entitystore

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	runtimelifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	_ "modernc.org/sqlite"
)

type entityFieldFaultCandidateWriter struct{ candidate runtimelifecycle.Candidate }

func (w entityFieldFaultCandidateWriter) WriteCompletionCandidateTx(context.Context, *sql.Tx, string, *time.Time) (runtimelifecycle.CandidateRequestResult, error) {
	return runtimelifecycle.CandidateRequestResult{Disposition: runtimelifecycle.CandidateRequested, Candidate: w.candidate}, nil
}

type entityFieldFaultSink struct {
	fault   error
	submits int
}

func (s *entityFieldFaultSink) ReserveCompletionCandidate(context.Context) (runtimelifecycle.CandidateAdmission, error) {
	return s, nil
}

func (s *entityFieldFaultSink) Submit(runtimelifecycle.Candidate) error {
	s.submits++
	return s.fault
}

func (*entityFieldFaultSink) Cancel() error { return nil }

func TestEntityFieldRevisionCarrierRetainsAcknowledgedPostCommitErrorOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			if backend == "postgres" {
				_, db, _ = testutil.StartEmptyPostgres(t)
			} else {
				var err error
				db, err = sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "entity-field-ack.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
			}
			if _, err := db.Exec(`CREATE TABLE entity_field_ack_probe (revision INTEGER NOT NULL)`); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			const bundleHash = "entity-field-ack-bundle"
			coordinator := runhandoff.NewCandidateCoordinator()
			postCommitErr := errors.New("injected post-commit cleanup failure")
			sink := &entityFieldFaultSink{fault: postCommitErr}
			registration, err := coordinator.Register(ctx, runtimelifecycle.CandidateScope{BundleHash: bundleHash}, sink)
			if err != nil {
				t.Fatal(err)
			}
			defer registration.Release()
			run := func(write func(context.Context, *mutationprotocol.Attempt) (int, error)) mutationprotocol.Result[int] {
				if backend == "postgres" {
					selected, err := postgresbackend.New(db)
					if err != nil {
						t.Fatal(err)
					}
					return mutationprotocol.RunPostgres(ctx, selected, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, coordinator, write)
				}
				selected, err := sqlitebackend.New(db)
				if err != nil {
					t.Fatal(err)
				}
				return mutationprotocol.RunSQLite(ctx, selected, "entity field revision acknowledgement", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, coordinator, write)
			}
			runID := uuid.NewString()
			candidate := runtimelifecycle.Candidate{RunID: runID, BundleHash: bundleHash, Revision: 1, DueAt: time.Now().UTC().Truncate(time.Microsecond)}
			result := run(func(ctx context.Context, attempt *mutationprotocol.Attempt) (int, error) {
				err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `INSERT INTO entity_field_ack_probe (revision) VALUES (7)`)
					return err
				})
				if err != nil {
					return 0, err
				}
				_, err = attempt.RequestCompletion(ctx, entityFieldFaultCandidateWriter{candidate: candidate}, runID, nil)
				return 7, err
			})
			committed, err := committedEntityFieldRevision(result)
			if !errors.Is(err, postCommitErr) || !committed.Acknowledged || committed.Revision != 7 || sink.submits != 1 {
				t.Fatalf("acknowledged revision = %+v, error = %v, submits = %d", committed, err, sink.submits)
			}
			var count int
			if err := db.QueryRow(`SELECT COUNT(*) FROM entity_field_ack_probe`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("committed revision rows = %d, error = %v; want one write", count, err)
			}
			preCommitErr := errors.New("injected pre-commit failure")
			result = run(func(ctx context.Context, attempt *mutationprotocol.Attempt) (int, error) {
				err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `INSERT INTO entity_field_ack_probe (revision) VALUES (8)`)
					return err
				})
				if err != nil {
					return 0, err
				}
				return 8, preCommitErr
			})
			committed, err = committedEntityFieldRevision(result)
			if !errors.Is(err, preCommitErr) || committed.Acknowledged || committed.Revision != 0 {
				t.Fatalf("unacknowledged revision = %+v, error = %v", committed, err)
			}
			if err := db.QueryRow(`SELECT COUNT(*) FROM entity_field_ack_probe`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("failed write rows = %d, error = %v; want no duplicate", count, err)
			}
		})
	}
}
