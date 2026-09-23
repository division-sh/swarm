package pipelinepersistence

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestFanOutReadbackPreservesSuccessorOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, disposition := range []string{"claimed", "advanced", "canceled"} {
			t.Run(backend+"/"+disposition, func(t *testing.T) {
				ctx := context.Background()
				db := fanOutReadbackTestDB(t, backend)
				command := seedFanOutReadbackClaim(t, db)
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
				var successor fanoutobligation.Claim
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
				committed, err := reconcileFanOutChunk(ctx, db, backend == "postgres", command)
				if !committed || err != nil {
					t.Fatalf("earlier commit not reconciled after %s: %t, %v", disposition, committed, err)
				}
				current, err := scanFanOutIntent(db.QueryRowContext(ctx, `SELECT `+fanOutIntentColumns+` FROM fan_out_intents`))
				if err != nil {
					t.Fatal(err)
				}
				if current.ClaimGeneration != successor.Generation {
					t.Fatalf("reconciliation changed successor generation: %+v", current)
				}
				switch disposition {
				case "claimed":
					if current.ClaimOwner != successor.Owner || current.Cursor != 1 {
						t.Fatalf("reconciliation released successor: %+v", current)
					}
				case "advanced":
					if current.Status != fanoutobligation.StatusClosed || current.Cursor != 2 {
						t.Fatalf("reconciliation lost successor progress: %+v", current)
					}
				case "canceled":
					if current.Status != fanoutobligation.StatusCanceled || current.Cursor != 1 || current.BlockedReason != "run_stopped" {
						t.Fatalf("reconciliation lost cancellation: %+v", current)
					}
				}
			})
		}
	}
}
