package pipelinepersistence

import (
	"context"
	"database/sql"
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
)

func TestFanOutUnacknowledgedCommitCannotBorrowSuccessorOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, disposition := range []string{"none", "claimed", "advanced", "canceled"} {
			t.Run(backend+"/"+disposition, func(t *testing.T) {
				ctx := context.Background()
				db := fanOutReadbackTestDB(t, backend)
				if backend == "sqlite" {
					db.SetMaxOpenConns(1)
					if _, err := db.Exec(`PRAGMA foreign_keys=ON`); err != nil {
						t.Fatal(err)
					}
				}
				for _, statement := range []string{
					`CREATE TABLE fan_out_commit_parent (id INTEGER PRIMARY KEY)`,
					`CREATE TABLE fan_out_commit_child (parent_id INTEGER REFERENCES fan_out_commit_parent(id) DEFERRABLE INITIALLY DEFERRED)`,
				} {
					if _, err := db.Exec(statement); err != nil {
						t.Fatal(err)
					}
				}
				command := seedFanOutReadbackClaim(t, db)
				var successor fanoutobligation.Claim
				run := func(ctx context.Context, _ func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedFanOutChunk, error)) mutationprotocol.Result[runtimepipeline.CommittedFanOutChunk] {
					write := func(ctx context.Context, attempt *mutationprotocol.Attempt) (runtimepipeline.CommittedFanOutChunk, error) {
						err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
							_, err := tx.ExecContext(ctx, `INSERT INTO fan_out_commit_child(parent_id) VALUES (999)`)
							return err
						})
						return runtimepipeline.CommittedFanOutChunk{}, err
					}
					var outcome mutationprotocol.Result[runtimepipeline.CommittedFanOutChunk]
					if backend == "postgres" {
						selected, err := postgresbackend.New(db)
						if err != nil {
							t.Fatal(err)
						}
						outcome = mutationprotocol.RunPostgres(ctx, selected, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
					} else {
						selected, err := sqlitebackend.New(db)
						if err != nil {
							t.Fatal(err)
						}
						outcome = mutationprotocol.RunSQLite(ctx, selected, "fan-out rejected native commit", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
					}
					if outcome.Acknowledged() || outcome.Phase() != mutationprotocol.CommitAdmission || outcome.Err() == nil {
						t.Fatalf("native commit rejection = acknowledged:%t phase:%v err:%v", outcome.Acknowledged(), outcome.Phase(), outcome.Err())
					}
					if disposition != "none" {
						persistFanOutReadbackChunk(t, db, command)
						tx, err := db.BeginTx(ctx, nil)
						if err != nil {
							t.Fatal(err)
						}
						defer tx.Rollback()
						intent, err := scanFanOutIntent(tx.QueryRowContext(ctx, `SELECT `+fanOutIntentColumns+` FROM fan_out_intents`))
						if err != nil {
							t.Fatal(err)
						}
						if intent.ClaimOwner != "" || !intent.LeaseExpiresAt.IsZero() || intent.Cursor != 1 || intent.NextChunkSize != fanoutobligation.MaxChunkSize || intent.LastServedAt.IsZero() {
							t.Fatalf("successor observed non-atomic publication/release: %+v", intent)
						}
						if err := claimFanOutIntentRow(ctx, tx, runtimepipeline.FanOutClaimRequest{Owner: "successor", Lease: time.Minute}, time.Now().UTC(), &intent, &successor); err != nil {
							t.Fatal(err)
						}
						if err := tx.Commit(); err != nil {
							t.Fatal(err)
						}
						switch disposition {
						case "advanced":
							if _, err := db.Exec(`UPDATE fan_out_intents SET cursor=2,status='closed',claim_owner=NULL,lease_expires_at=NULL`); err != nil {
								t.Fatal(err)
							}
						case "canceled":
							if _, err := db.Exec(`UPDATE fan_out_intents SET status='canceled',blocked_reason='run_stopped',claim_owner=NULL,lease_expires_at=NULL`); err != nil {
								t.Fatal(err)
							}
						}
					}
					return outcome
				}
				committed, err := commitFanOutChunk(ctx, nil, backend == "postgres", run, time.Now, nil, command)
				if failure := runtimefailures.Normalize(err, "runtime.fan_out", "commit_chunk"); err == nil || failure.Class != runtimefailures.ClassOutcomeUncertain {
					t.Fatalf("unacknowledged attempt = %+v, %v; want uncertain outcome", committed, err)
				}
				if committed.Intent.Request.Key.RunID != "" || len(committed.Publications) != 0 || committed.PostCommitFailure != nil {
					t.Fatalf("unacknowledged attempt returned successor evidence: %+v", committed)
				}
				current, err := scanFanOutIntent(db.QueryRowContext(ctx, `SELECT `+fanOutIntentColumns+` FROM fan_out_intents`))
				if err != nil {
					t.Fatal(err)
				}
				if disposition == "none" {
					if current.ClaimOwner != command.Claim.Owner || current.Cursor != 0 || current.ClaimGeneration != command.Claim.Generation {
						t.Fatalf("failed commit changed claim: %+v", current)
					}
					return
				}
				if current.ClaimGeneration != successor.Generation {
					t.Fatalf("failed attempt changed successor generation: %+v", current)
				}
				switch disposition {
				case "claimed":
					if current.ClaimOwner != successor.Owner || current.Cursor != 1 {
						t.Fatalf("failed attempt released successor: %+v", current)
					}
				case "advanced":
					if current.Status != fanoutobligation.StatusClosed || current.Cursor != 2 {
						t.Fatalf("failed attempt lost successor progress: %+v", current)
					}
				case "canceled":
					if current.Status != fanoutobligation.StatusCanceled || current.Cursor != 1 || current.BlockedReason != "run_stopped" {
						t.Fatalf("failed attempt lost cancellation: %+v", current)
					}
				}
			})
		}
	}
}
