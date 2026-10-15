package runtimepersistence

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

func TestAgentRecoveryCapacityKeepsNonFirstClaimOutcomesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, state := range []string{"busy", "terminal", "future_retry", "eligible_retry", "reclaim"} {
			t.Run(backend+"/"+state, func(t *testing.T) {
				f := newAgentClaimProfileFixture(t, backend)
				event, first, profile := f.claim(t)
				want := deliverylifecycle.ClaimBusy
				refused := state == "eligible_retry" || state == "reclaim"
				switch state {
				case "terminal":
					if _, err := f.store.SettleSuccess(f.ctx, first.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
						t.Fatal(err)
					}
					want = deliverylifecycle.ClaimTerminal
				case "future_retry", "eligible_retry":
					base := time.Hour
					if state == "eligible_retry" {
						base = time.Nanosecond
					}
					if _, err := f.store.SettleFailure(f.ctx, first.Claim, deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureRetry, Failure: testRetryableFailure(), RetryBase: base, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation()}); err != nil {
						t.Fatal(err)
					}
					want = deliverylifecycle.ClaimDeferred
				case "reclaim":
					if err := ExpireExactDeliveryClaimFaultForTest(f.ctx, f.store, first.Claim); err != nil {
						t.Fatal(err)
					}
				}
				process, _ := agentRecoveryFixtureOwners(t, f)
				bindAgentRecoveryCapacityPolicy(t, process, profile.ProfileID, 1)
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
					t.Fatal(err)
				}
				before := profileFixtureSnapshot(t, f)
				result, err := startupownership.TryClaimAgentRecoveryDelivery(f.ctx, process, f.authority, event, f.route)
				claim, native := result.NativeClaim()
				if result.Waiting() || (err != nil) != refused || result.Acknowledged() == refused || native == refused || !refused && claim.Disposition != want {
					t.Fatalf("non-first claim turned into capacity admission/wait: %+v native=%+v err=%v", result, claim, err)
				}
				if !reflect.DeepEqual(before, profileFixtureSnapshot(t, f)) {
					t.Fatal("out-of-slice capacity call changed durable work")
				}
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
					t.Fatalf("out-of-slice cleanup did not join: %v", err)
				}
				// The ordinary owner, not capacity, still admits eligible retries
				// and reclaims with their original immutable first profile.
				if refused {
					ordinary, err := f.store.ClaimDelivery(f.ctx, f.authority, event, f.route)
					acquired, ok := ordinary.Acquired()
					if err != nil || !ordinary.Acknowledged || !ok || acquired.Claim.Version() != 2 {
						t.Fatalf("ordinary retry/reclaim admission changed: %+v err=%v", ordinary, err)
					}
					actual, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, acquired.Snapshot)
					if err != nil || actual != profile {
						t.Fatalf("ordinary retry/reclaim changed first profile: %+v err=%v", actual, err)
					}
				}
				assertAgentProfileProcessNoAttempts(t, f)
			})
		}
	}
}

func TestAgentRecoveryCapacityRefusesInterveningNativeOwnerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, change := range []string{"plain_claim", "settlement", "retirement"} {
			t.Run(backend+"/"+change, func(t *testing.T) {
				f := newAgentClaimProfileFixture(t, backend)
				_, first, profile := f.claim(t)
				wanted := publishAgentProfileRecoveryEvent(t, f)
				other := publishAgentProfileRecoveryEvent(t, f)
				process, _ := agentRecoveryFixtureOwners(t, f)
				bindAgentRecoveryCapacityPolicy(t, process, profile.ProfileID, 2)
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
					t.Fatal(err)
				}
				var changed map[string]SelectedForkStorageTableSnapshot
				remove, err := startupownership.InstallAgentRecoverySeedFaultProbeForTest(process, &startupownership.AgentRecoverySeedFaultProbeForTest{AfterCheckProof: func() {
					switch change {
					case "plain_claim":
						claim, err := f.store.ClaimDelivery(f.ctx, f.authority, other, f.route)
						if err != nil || !claim.Acknowledged || claim.Disposition != deliverylifecycle.ClaimAcquired {
							t.Fatalf("intervening ordinary claim: %+v err=%v", claim, err)
						}
					case "settlement":
						if _, err := f.store.SettleSuccess(f.ctx, first.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection()); err != nil {
							t.Fatal(err)
						}
					case "retirement":
						if err := process.Release(f.ctx); err != nil {
							t.Fatal(err)
						}
					}
					changed = profileFixtureSnapshot(t, f)
				}})
				if err != nil {
					t.Fatal(err)
				}
				defer remove()
				result, err := startupownership.TryClaimAgentRecoveryDelivery(f.ctx, process, f.authority, wanted, f.route)
				_, native := result.NativeClaim()
				if err == nil || result.Acknowledged() || result.Waiting() || native || changed == nil || !reflect.DeepEqual(changed, profileFixtureSnapshot(t, f)) {
					t.Fatalf("stale private seed became a claim/wait: %+v err=%v", result, err)
				}
				assertAgentProfileProcessNoAttempts(t, f)
			})
		}
	}
}
