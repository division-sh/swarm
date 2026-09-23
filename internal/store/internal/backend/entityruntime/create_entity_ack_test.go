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

func TestEntityCreateResultRetainsIDAfterAcknowledgedPostCommitFailureOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			if backend == "postgres" {
				_, db, _ = testutil.StartEmptyPostgres(t)
			} else {
				var err error
				db, err = sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "entity-create-ack.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
			}
			if _, err := db.Exec(`CREATE TABLE entity_create_ack_probe (entity_id TEXT NOT NULL)`); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			const bundleHash = "entity-create-ack-bundle"
			coordinator := runhandoff.NewCandidateCoordinator()
			postCommitErr := errors.New("injected post-commit cleanup failure")
			sink := &entityFieldFaultSink{fault: postCommitErr}
			registration, err := coordinator.Register(ctx, runtimelifecycle.CandidateScope{BundleHash: bundleHash}, sink)
			if err != nil {
				t.Fatal(err)
			}
			defer registration.Release()
			run := func(write func(context.Context, *mutationprotocol.Attempt) (string, error)) mutationprotocol.Result[string] {
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
				return mutationprotocol.RunSQLite(ctx, selected, "entity create acknowledgement", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, coordinator, write)
			}
			runID, entityID := uuid.NewString(), uuid.NewString()
			candidate := runtimelifecycle.Candidate{RunID: runID, BundleHash: bundleHash, Revision: 1, DueAt: time.Now().UTC().Truncate(time.Microsecond)}
			result := run(func(ctx context.Context, attempt *mutationprotocol.Attempt) (string, error) {
				if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `INSERT INTO entity_create_ack_probe (entity_id) VALUES ($1)`, entityID)
					return err
				}); err != nil {
					return "", err
				}
				_, err := attempt.RequestCompletion(ctx, entityFieldFaultCandidateWriter{candidate: candidate}, runID, nil)
				return entityID, err
			})
			created, err := committedEntityCreate(result)
			if !errors.Is(err, postCommitErr) || !created.Acknowledged || created.EntityID != entityID || sink.submits != 1 {
				t.Fatalf("acknowledged create = %+v, error = %v, submits = %d", created, err, sink.submits)
			}
			var count int
			if err := db.QueryRow(`SELECT COUNT(*) FROM entity_create_ack_probe WHERE entity_id = $1`, entityID).Scan(&count); err != nil || count != 1 {
				t.Fatalf("committed create rows = %d, error = %v", count, err)
			}
			preCommitErr := errors.New("injected pre-commit failure")
			result = run(func(ctx context.Context, attempt *mutationprotocol.Attempt) (string, error) {
				if err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `INSERT INTO entity_create_ack_probe (entity_id) VALUES ($1)`, uuid.NewString())
					return err
				}); err != nil {
					return "", err
				}
				return uuid.NewString(), preCommitErr
			})
			created, err = committedEntityCreate(result)
			if !errors.Is(err, preCommitErr) || created.Acknowledged || created.EntityID != "" {
				t.Fatalf("unacknowledged create = %+v, error = %v", created, err)
			}
			if err := db.QueryRow(`SELECT COUNT(*) FROM entity_create_ack_probe`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("failed create rows = %d, error = %v", count, err)
			}
		})
	}
}
