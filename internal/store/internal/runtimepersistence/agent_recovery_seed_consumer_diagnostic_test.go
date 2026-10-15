package runtimepersistence

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/startuprecovery"
)

// These are counterexamples to a proposed possession-only consumer, not a
// consumer implementation or proof of observable native private-seed bytes.
func TestAgentRecoveryPossessionProofDoesNotValidateRetainedSeedBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newAgentClaimProfileFixture(t, backend)
			_, first, profile := f.claim(t)
			pending := publishAgentProfileRecoveryEvent(t, f)
			process, grant := agentRecoveryFixtureOwners(t, f)
			original := readNativeAgentRecoveryCandidate(t, f)
			assertAgentRecoveryCandidateOwner(t, original, process, grant)
			if !reflect.DeepEqual(original.InProgressByProfile, map[string]int{profile.ProfileID: 1}) {
				t.Fatalf("original complete observation: %+v", original)
			}
			before := profileFixtureSnapshot(t, f)
			if err := process.ProveCurrent(f.ctx); err != nil {
				t.Fatal(err)
			}
			// Possession succeeds before ANY seed exists. Only the explicit
			// complete reconstruction below installs the original observation.
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
				t.Fatal(err)
			}
			if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
				t.Fatal("proof/initial seed changed complete durable storage")
			}
			claimed, err := f.store.ClaimDelivery(f.ctx, f.authority, pending, f.route)
			second, acquired := claimed.Acquired()
			if err != nil || !claimed.Acknowledged || !acquired || second.Claim.Version() != 1 {
				t.Fatalf("accepted later first claim: %+v err=%v", claimed, err)
			}
			assertNativeAgentRecoveryOccupancy(t, f, map[string]int{profile.ProfileID: 2})
			if err := process.ProveCurrent(f.ctx); err != nil {
				t.Fatalf("a stale seed is not native possession loss: %v", err)
			}
			assertAgentRecoveryReseedsAfterNativeMutation(t, f.ctx, f.store, process)
			settled, err := f.store.SettleSuccess(f.ctx, first.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection())
			if err != nil || settled.Status != deliverylifecycle.StatusDelivered || settled.ClaimVersion != first.Claim.Version() {
				t.Fatalf("exact first delivery settlement: %+v err=%v", settled, err)
			}
			current, err := startuprecovery.InspectAgentRecoveryOccupancyCandidate(f.ctx, f.store.(startuprecovery.AgentRecoveryReader))
			if err != nil || len(current.Image.Members) != 1 || current.Image.Process.Possession != nil {
				t.Fatalf("complete post-settlement observation: %+v err=%v", current, err)
			}
			assertAgentRecoveryCandidateOwner(t, current, process, grant)
			if !reflect.DeepEqual(current.InProgressByProfile, original.InProgressByProfile) ||
				!reflect.DeepEqual(current.Image.SourceHead, original.Image.SourceHead) ||
				*current.Image.Process.Authority != *original.Image.Process.Authority {
				t.Fatalf("same count/stamp/source counterexample changed its coordinates: %+v", current)
			}
			if reflect.DeepEqual(current.Image, original.Image) {
				t.Fatal("later claim plus exact settlement did not change the complete image")
			}
			if err := process.ProveCurrent(f.ctx); err != nil {
				t.Fatalf("same-count later epoch lost native possession: %v", err)
			}
			assertAgentRecoveryReseedsAfterNativeMutation(t, f.ctx, f.store, process)
			assertAgentProfileRecoveryCapture(t, f, settled, profile)
			assertAgentProfileProcessNoAttempts(t, f)
			t.Log("DIAGNOSTIC: possession succeeds with no seed and across 1->2->1 claims; unchanged stamp/source/count is not a freshness epoch; each native write requires explicit complete reseed; native seed bytes UNOBSERVED; no consumer, slots or launch")
		})
	}
}
