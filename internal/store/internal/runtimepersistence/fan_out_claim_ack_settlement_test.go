package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/testutil"
)

type fanOutPostCommitFaultOwner struct {
	pipeline.FanOutObligationOwner
	fault error
}

func (o *fanOutPostCommitFaultOwner) ReleaseFanOutClaim(ctx context.Context, claim fanoutobligation.Claim) (pipeline.FanOutClaimSettlement, error) {
	settlement, err := o.FanOutObligationOwner.ReleaseFanOutClaim(ctx, claim)
	if settlement.Acknowledged {
		err = errors.Join(err, o.fault)
	}
	return settlement, err
}

func (o *fanOutPostCommitFaultOwner) ReleaseFanOutRetryable(ctx context.Context, release pipeline.FanOutRetryableRelease) (pipeline.FanOutClaimSettlement, error) {
	settlement, err := o.FanOutObligationOwner.ReleaseFanOutRetryable(ctx, release)
	if settlement.Acknowledged {
		err = errors.Join(err, o.fault)
	}
	return settlement, err
}

func (o *fanOutPostCommitFaultOwner) BlockFanOutClaim(ctx context.Context, block pipeline.FanOutBlockRequest) (pipeline.FanOutClaimSettlement, error) {
	settlement, err := o.FanOutObligationOwner.BlockFanOutClaim(ctx, block)
	if settlement.Acknowledged {
		err = errors.Join(err, o.fault)
	}
	return settlement, err
}

func TestFanOutClaimSettlementAcknowledgedPostCommitErrorBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, operation := range []string{"release", "retry_wait", "block"} {
			t.Run(backend+"/"+operation, func(t *testing.T) {
				ctx := testAuthorActivityContext()
				var (
					owner    selectedFanOutOwner
					db       *sql.DB
					postgres bool
				)
				if backend == "postgres" {
					_, opened, cleanup := testutil.StartPostgres(t)
					t.Cleanup(cleanup)
					db, owner, postgres = opened, newPostgresStoreWithBackend(mustPostgresBackend(opened)), true
					owner.(*PostgresStore).acceptCurrentSchemaForTest()
				} else {
					store := newBootstrappedSQLiteRuntimeStoreForTest(t)
					db, owner = store.backend.ConstructionHandle(), store
				}
				fixture := seedFanOutOwnerFixture(t, ctx, db, owner, postgres, 2, time.Now().UTC().Truncate(time.Microsecond))
				_, claim, found, err := owner.ClaimFanOutIntent(ctx, pipeline.FanOutClaimRequest{
					Owner: "fan-out-ack-proof", BundleHash: fixture.bundleHash,
					Now: time.Now().UTC(), Lease: time.Minute,
				})
				if err != nil || !found {
					t.Fatalf("claim fan-out intent: found=%v err=%v", found, err)
				}

				cleanupFault := errors.New("injected postcommit claim cleanup failure")
				handoffFault := errors.New("injected postcommit claim handoff failure")
				faulty := &fanOutPostCommitFaultOwner{FanOutObligationOwner: owner, fault: errors.Join(cleanupFault, handoffFault)}
				var settlement pipeline.FanOutClaimSettlement
				switch operation {
				case "release":
					settlement, err = faulty.ReleaseFanOutClaim(ctx, claim)
				case "retry_wait":
					settlement, err = faulty.ReleaseFanOutRetryable(ctx, pipeline.FanOutRetryableRelease{
						Claim: claim, Now: time.Now().UTC(), Failure: fanOutRetryFailureForTest(),
					})
				case "block":
					settlement, err = faulty.BlockFanOutClaim(ctx, pipeline.FanOutBlockRequest{
						Claim: claim, Now: time.Now().UTC(),
						Failure: runtimefailures.Normalize(errors.New("block claim"), "runtime.fan_out", "ack_proof"),
					})
				}
				if !settlement.Acknowledged || !errors.Is(err, cleanupFault) || !errors.Is(err, handoffFault) {
					t.Fatalf("%s settlement=%+v err=%v", operation, settlement, err)
				}
				stale, staleErr := faulty.ReleaseFanOutClaim(ctx, claim)
				if stale.Acknowledged || !errors.Is(staleErr, fanoutobligation.ErrStaleClaim) || errors.Is(staleErr, cleanupFault) {
					t.Fatalf("stale %s settlement=%+v err=%v", operation, stale, staleErr)
				}

				var status string
				var claimOwner, blocked sql.NullString
				var retryReady any
				if err := db.QueryRowContext(ctx, `SELECT status,claim_owner,retry_ready_at,blocked_reason FROM fan_out_intents WHERE run_id=$1 AND triggering_delivery_id=$2 AND semantic_path=$3`, fixture.runID, fixture.deliveryID, fixture.semanticPath).Scan(&status, &claimOwner, &retryReady, &blocked); err != nil {
					t.Fatal(err)
				}
				if claimOwner.Valid {
					t.Fatalf("%s retained claim owner %q", operation, claimOwner.String)
				}
				switch operation {
				case "release":
					if status != "open" || retryReady != nil || blocked.Valid {
						t.Fatalf("release durable state=%s retry=%v blocked=%v", status, retryReady, blocked)
					}
				case "retry_wait":
					if status != "open" || retryReady == nil || blocked.Valid {
						t.Fatalf("retry durable state=%s retry=%v blocked=%v", status, retryReady, blocked)
					}
				case "block":
					if status != "blocked" || retryReady != nil || !blocked.Valid {
						t.Fatalf("block durable state=%s retry=%v blocked=%v", status, retryReady, blocked)
					}
				}
			})
		}
	}
}
