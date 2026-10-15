package runtimepersistence

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

// Individually exact historical reads do not establish one occupancy cut.
// No dispatcher or production recovery-image implementation is exercised here.
func TestAgentClaimProfileRecoveryCompositionRequiresOneReadCutBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newAgentClaimProfileFixture(t, backend)
			first := claimAgentProfileRecoveryEvent(t, f)
			pending := publishAgentProfileRecoveryEvent(t, f)
			assertAgentProfileCompositionInventory(t, f, 1, 0, 1)

			before := profileFixtureSnapshot(t, f)
			old, err := f.store.Snapshot(f.ctx, first.Snapshot.DeliveryID)
			if err != nil || old.Status != deliverylifecycle.StatusInProgress || old.ClaimVersion != 1 {
				t.Fatalf("first exact occupancy observation: %+v err=%v", old, err)
			}
			original, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, old)
			if err != nil || !original.MatchesDelivery(old) {
				t.Fatalf("first exact immutable profile: %+v err=%v", original, err)
			}
			if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
				t.Fatal("first recovery observation changed application storage")
			}

			if _, err := f.store.SettleFailure(f.ctx, first.Claim, deliverylifecycle.Settlement{
				Disposition: deliverylifecycle.FailureRetry, Failure: testRetryableFailure(), RetryBase: time.Hour,
				RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation(),
			}); err != nil {
				t.Fatal(err)
			}
			assertAgentProfileCompositionInventory(t, f, 1, 1, 0)
			result, err := f.store.ClaimDelivery(f.ctx, f.authority, pending, f.route)
			second, acquired := result.Acquired()
			if err != nil || !result.Acknowledged || !acquired || second.Snapshot.ClaimVersion != 1 {
				t.Fatalf("second exact native first claim: %+v err=%v", result, err)
			}
			assertAgentProfileCompositionInventory(t, f, 0, 1, 1)
			assertAgentProfileMixedRecoveryCut(t, f, old, original, second.Snapshot)
		})
	}
}

func assertAgentProfileCompositionInventory(t *testing.T, f agentClaimProfileFixture, pending, failed, inProgress int) {
	t.Helper()
	want := deliverylifecycle.RecoveryInventory{Runs: []deliverylifecycle.RecoveryRunInventory{{
		RunID: f.route.AgentIdentity.RunID, Pending: pending, Failed: failed, InProgress: inProgress,
	}}}
	actual, err := f.store.InspectDeliveryRecovery(f.ctx, f.source)
	if err != nil || !reflect.DeepEqual(actual, want) {
		t.Fatalf("native ordered occupancy cut: %+v want=%+v err=%v", actual, want, err)
	}
}

func assertAgentProfileMixedRecoveryCut(t *testing.T, f agentClaimProfileFixture, old deliverylifecycle.Snapshot, original deliverylifecycle.AgentClaimProfile, second deliverylifecycle.Snapshot) {
	t.Helper()
	before := profileFixtureSnapshot(t, f)
	current, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, second)
	if err != nil || !current.MatchesDelivery(second) || second.Status != deliverylifecycle.StatusInProgress {
		t.Fatalf("second exact immutable capture: %+v snapshot=%+v err=%v", current, second, err)
	}
	settled, err := f.store.Snapshot(f.ctx, old.DeliveryID)
	if err != nil || settled.Status != deliverylifecycle.StatusFailed || settled.ClaimVersion != 1 || !settled.RetryScheduled {
		t.Fatalf("first claim did not release occupancy into future retry: %+v err=%v", settled, err)
	}
	assertAgentProfileRecoveryCapture(t, f, settled, original)
	if old.DeliveryID == second.DeliveryID || original.ProfileID != current.ProfileID {
		t.Fatal("counterexample requires two distinct claims in the same captured profile")
	}
	// This deliberately mixes two individually valid snapshots. The synchronous
	// native transitions above never had two in-progress claims at the same time.
	mixed := map[string]int{}
	for _, observation := range []struct {
		snapshot deliverylifecycle.Snapshot
		profile  deliverylifecycle.AgentClaimProfile
	}{{old, original}, {second, current}} {
		if observation.snapshot.Status == deliverylifecycle.StatusInProgress && observation.profile.MatchesDelivery(observation.snapshot) {
			mixed[observation.profile.ProfileID]++
		}
	}
	if mixed[original.ProfileID] != 2 {
		t.Fatalf("independent-read counterexample did not expose the false occupancy image: %v", mixed)
	}
	assertAgentProfileCompositionInventory(t, f, 0, 1, 1)
	assertAgentProfileProcessNoAttempts(t, f)
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
		t.Fatal("mixed-cut diagnostic observations changed application storage")
	}
	t.Log("exact native cuts retain at most one in-progress claim; independent valid snapshot/profile reads invent two; historical capture is not live occupancy permission")
}
