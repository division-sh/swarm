package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

// The old independent check/claim scaffold and its six real-store REDs are
// frozen at 92eeb7ebd8. Only this invocation changes; every proof oracle stays.
func claimAgentRecoveryHandoffCandidate(ctx context.Context, process startupownership.ProcessCapability, f agentClaimProfileFixture, event events.Event, beforeEntry func()) (deliverylifecycle.ClaimResult, error) {
	remove, err := startupownership.InstallAgentRecoverySeedFaultProbeForTest(process, &startupownership.AgentRecoverySeedFaultProbeForTest{AfterCheckProof: beforeEntry})
	if err != nil {
		return deliverylifecycle.ClaimResult{}, err
	}
	defer remove()
	return startupownership.ClaimAgentRecoveryDelivery(ctx, process, f.authority, event, f.route)
}

func TestAgentRecoveryNativeEntryRetainsClaimDispositionsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, state := range []string{"pending", "busy", "terminal", "future_retry", "eligible_retry", "reclaim", "wrong_authority"} {
			t.Run(backend+"/"+state, func(t *testing.T) {
				f := newAgentClaimProfileFixture(t, backend)
				event, first, original := f.claim(t)
				want := deliverylifecycle.ClaimAcquired
				wantVersion := int64(2)
				claimAuthority := f.authority
				switch state {
				case "pending":
					event = publishAgentProfileRecoveryEvent(t, f)
					wantVersion = 1
				case "busy":
					want = deliverylifecycle.ClaimBusy
				case "terminal":
					settled, err := f.store.SettleSuccess(f.ctx, first.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection())
					if err != nil || settled.Status != deliverylifecycle.StatusDelivered {
						t.Fatalf("terminal setup: %+v err=%v", settled, err)
					}
					want = deliverylifecycle.ClaimTerminal
				case "future_retry", "eligible_retry":
					base := time.Hour
					if state == "eligible_retry" {
						base = time.Nanosecond
					}
					settled, err := f.store.SettleFailure(f.ctx, first.Claim, deliverylifecycle.Settlement{
						Disposition: deliverylifecycle.FailureRetry, Failure: testRetryableFailure(), RetryBase: base,
						RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation(),
					})
					if err != nil || settled.Status != deliverylifecycle.StatusFailed || settled.NextEligibleAt.IsZero() {
						t.Fatalf("retry setup: %+v err=%v", settled, err)
					}
					if state == "future_retry" {
						want = deliverylifecycle.ClaimDeferred
					}
				case "reclaim":
					if err := ExpireExactDeliveryClaimFaultForTest(f.ctx, f.store, first.Claim); err != nil {
						t.Fatal(err)
					}
				case "wrong_authority":
					var err error
					claimAuthority, err = deliverylifecycle.NewNormalExecutionAuthority(f.source, uuid.NewString(), f.authority.Generation())
					if err != nil {
						t.Fatal(err)
					}
					want = deliverylifecycle.ClaimWrongAuthority
				}
				process, _ := agentRecoveryFixtureOwners(t, f)
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
					t.Fatal(err)
				}
				before := profileFixtureSnapshot(t, f)
				collector, remove, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer remove()
				result, err := startupownership.ClaimAgentRecoveryDelivery(f.ctx, process, claimAuthority, event, f.route)
				if err != nil || !result.Acknowledged || result.Disposition != want {
					t.Fatalf("native disposition changed: want=%s got=%+v err=%v", want, result, err)
				}
				if got := collector.Snapshot().ByOperation[transactiontest.DeliveryClaim].Begun; got != 1 {
					t.Fatalf("checked invocation began %d claim transactions, want one", got)
				}
				if want == deliverylifecycle.ClaimAcquired {
					claim, acquired := result.Acquired()
					if !acquired || claim.Claim.Version() != wantVersion {
						t.Fatalf("native acquired identity changed: %+v", result)
					}
					captured, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, claim.Snapshot)
					if err != nil || state != "pending" && captured != original || !captured.MatchesFirstAdmission(event, f.route, f.authority) {
						t.Fatalf("immutable first claim profile changed: %+v err=%v", captured, err)
					}
				} else if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
					t.Fatal("non-acquiring native disposition changed complete storage")
				}
				if err := startupownership.CheckAgentRecoverySeed(f.ctx, process); err == nil {
					t.Fatal("native write retained or implicitly reseeded the private observation")
				}
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
					t.Fatalf("native return did not finish original write cleanup: %v", err)
				}
				if err := startupownership.CheckAgentRecoverySeed(f.ctx, process); err != nil {
					t.Fatalf("explicit complete reseed did not validate: %v", err)
				}
				assertAgentProfileProcessNoAttempts(t, f)
			})
		}
	}
}

func TestAgentRecoveryNativeEntryRejectsInterveningWriteBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, change := range []string{"claim", "settlement", "count_aba_reseed"} {
			t.Run(backend+"/"+change, func(t *testing.T) {
				f := newAgentClaimProfileFixture(t, backend)
				_, first, profile := f.claim(t)
				wanted := publishAgentProfileRecoveryEvent(t, f)
				competing := publishAgentProfileRecoveryEvent(t, f)
				process, _ := agentRecoveryFixtureOwners(t, f)
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
					t.Fatal(err)
				}
				id, err := deliverylifecycle.DeliveryID(wanted.ID(), f.route)
				if err != nil {
					t.Fatal(err)
				}
				collector, remove, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer remove()
				var written map[string]SelectedForkStorageTableSnapshot
				var claims uint64
				beforeEntry := func() {
					if change != "settlement" {
						result, err := f.store.ClaimDelivery(f.ctx, f.authority, competing, f.route)
						claim, acquired := result.Acquired()
						if err != nil || !result.Acknowledged || !acquired || claim.Claim.Version() != 1 {
							t.Fatalf("competing native first claim: %+v err=%v", result, err)
						}
					}
					if change != "claim" {
						settled, err := f.store.SettleSuccess(f.ctx, first.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection())
						if err != nil || settled.Status != deliverylifecycle.StatusDelivered {
							t.Fatalf("competing exact native settlement: %+v err=%v", settled, err)
						}
					}
					wantCount := 2
					if change == "settlement" {
						wantCount = 0
					} else if change == "count_aba_reseed" {
						wantCount = 1
						if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
							t.Fatalf("explicit same-count complete reconstruction: %v", err)
						}
					}
					counts := map[string]int{}
					if wantCount > 0 {
						counts[profile.ProfileID] = wantCount
					}
					assertNativeAgentRecoveryOccupancy(t, f, counts)
					written = profileFixtureSnapshot(t, f)
					claims = collector.Snapshot().ByOperation[transactiontest.DeliveryClaim].Begun
				}
				result, err := claimAgentRecoveryHandoffCandidate(f.ctx, process, f, wanted, beforeEntry)
				if err == nil || result.Acknowledged || result.Disposition == deliverylifecycle.ClaimAcquired {
					t.Errorf("stale native-entry binding acquired target: %+v err=%v", result, err)
				}
				if got := collector.Snapshot().ByOperation[transactiontest.DeliveryClaim].Begun; got != claims {
					t.Errorf("refused handoff entered target claim SQL: before=%d after=%d", claims, got)
				}
				actual, err := f.store.Snapshot(f.ctx, id)
				if err != nil || actual.Status != deliverylifecycle.StatusPending || actual.ClaimVersion != 0 {
					t.Errorf("native-entry refusal changed exact pending target: %+v err=%v", actual, err)
				}
				if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(written, after) {
					t.Error("native-entry refusal changed complete post-competing-write storage")
				}
				assertAgentProfileProcessNoAttempts(t, f)
			})
		}
	}
}
