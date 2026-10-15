package runtimepersistence

import (
	"context"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
)

// This initial test-first candidate deliberately composes the old independent
// check and claim. The frozen RED packet names this scaffold, not a defect in
// the approved diagnostic checker, which never promised a later claim right.
func claimAgentRecoveryHandoffCandidate(ctx context.Context, process startupownership.ProcessCapability, f agentClaimProfileFixture, event events.Event, beforeEntry func()) (deliverylifecycle.ClaimResult, error) {
	if err := startupownership.CheckAgentRecoverySeed(ctx, process); err != nil {
		return deliverylifecycle.ClaimResult{}, err
	}
	if beforeEntry != nil {
		beforeEntry()
	}
	return f.store.ClaimDelivery(ctx, f.authority, event, f.route)
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
