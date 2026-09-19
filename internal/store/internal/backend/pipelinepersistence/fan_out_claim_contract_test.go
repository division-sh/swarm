package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
)

func TestFanOutLostAcknowledgementPreservesSuccessorOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, disposition := range []string{"claimed", "advanced", "canceled"} {
			t.Run(backend+"/"+disposition, func(t *testing.T) {
				ctx := context.Background()
				db := fanOutReadbackTestDB(t, backend)
				command := seedFanOutReadbackClaim(t, db)
				postgres := backend == "postgres"
				plainRun := func(ctx context.Context, _ *revisionEffects, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) (bool, error) {
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						return false, err
					}
					defer tx.Rollback()
					if err := operation(ctx, tx, nil); err != nil {
						return false, err
					}
					err = tx.Commit()
					return err == nil, err
				}
				prepare := func(*runLifecycleCandidateHandoffReservation, runtimerunlifecycle.CandidateRequestResult) error {
					return nil
				}
				candidate := func(context.Context, *sql.Tx, string) (runtimerunlifecycle.CandidateRequestResult, error) {
					return runtimerunlifecycle.CandidateRequestResult{}, nil
				}
				ackLost := errors.New("lost acknowledgement after successor admission")
				var successor fanoutobligation.Claim
				run := func(ctx context.Context, effects *revisionEffects, operation func(context.Context, *sql.Tx, *privateauthoractivity.Mutation) error) (bool, error) {
					committed, err := plainRun(ctx, effects, operation)
					if err != nil || !committed {
						return committed, err
					}
					_, err = plainRun(ctx, effects, func(ctx context.Context, tx *sql.Tx, _ *privateauthoractivity.Mutation) error {
						intent, err := scanFanOutIntent(tx.QueryRowContext(ctx, `SELECT `+fanOutIntentColumns+` FROM fan_out_intents`))
						if err != nil {
							return err
						}
						if intent.ClaimOwner != "" || !intent.LeaseExpiresAt.IsZero() || intent.Cursor != 1 || intent.NextChunkSize != fanoutobligation.MaxChunkSize || intent.LastServedAt.IsZero() {
							t.Fatalf("successor observed non-atomic publication/release: %+v", intent)
						}
						return claimFanOutIntentRow(ctx, tx, runtimepipeline.FanOutClaimRequest{Owner: "successor", Lease: time.Minute}, time.Now().UTC(), &intent, &successor)
					})
					if err != nil {
						return false, err
					}
					switch disposition {
					case "advanced":
						next := command
						next.Claim = successor
						next.Outcomes = append([]runtimepipeline.FanOutChunkOutcome(nil), command.Outcomes...)
						next.Outcomes[0].Ordinal = 1
						_, err = commitFanOutChunk(ctx, nil, postgres, plainRun, time.Now, prepare, candidate, db, next)
					case "canceled":
						_, err = db.ExecContext(ctx, `UPDATE fan_out_intents SET status='canceled',blocked_reason='run_stopped',claim_owner=NULL,lease_expires_at=NULL`)
					}
					if err != nil {
						return false, err
					}
					return false, ackLost
				}
				result, err := commitFanOutChunk(ctx, nil, postgres, run, time.Now, prepare, candidate, db, command)
				if err != nil || !errors.Is(result.PostCommitFailure, ackLost) || result.Intent.Cursor != 1 {
					t.Fatalf("earlier commit not reconciled: result=%+v err=%v", result, err)
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
