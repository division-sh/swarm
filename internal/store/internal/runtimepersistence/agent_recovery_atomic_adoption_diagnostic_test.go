package runtimepersistence

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startuprecovery"
)

// This counterexample specifies the gap a future owner-bound adoption must
// close. It deliberately does not implement adoption or install any slots.
func TestAgentRecoveryPostReadProofIsNotAtomicAdoptionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { proveAgentRecoveryPostReadProofGap(t, backend) })
	}
}

func proveAgentRecoveryPostReadProofGap(t *testing.T, backend string) {
	t.Helper()
	open := agentClaimProfileReopener(t, backend)
	f := admitAgentClaimProfileFixture(t, testAuthorActivityContext(), open())
	first := claimAgentProfileRecoveryEvent(t, f)
	profile, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, first.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	pending := publishAgentProfileRecoveryEvent(t, f)
	process, grant := agentRecoveryFixtureOwners(t, f)
	candidate := readNativeAgentRecoveryCandidate(t, f)
	if !reflect.DeepEqual(candidate.InProgressByProfile, map[string]int{profile.ProfileID: 1}) {
		t.Fatalf("original complete reconstruction: %+v", candidate)
	}
	// The read has ended. Even a successful exact-owner proof now cannot
	// protect a later consumer after this operation boundary is released.
	assertAgentRecoveryCandidateOwner(t, candidate, process, grant)
	previous := agentProfileProcessEvidence{Profile: profile, Process: *candidate.Image.Process.Authority,
		Attempts: profileFixtureSnapshot(t, f)["event_delivery_attempts"]}
	previous.State, _, err = f.store.LoadAgentLifecycleState(f.ctx, f.route.AgentIdentity)
	if err != nil || previous.State.Generation == 0 {
		t.Fatalf("exact pre-takeover lifecycle: %+v err=%v", previous.State, err)
	}
	if err := process.Release(f.ctx); err != nil {
		t.Fatal(err)
	}
	next := f
	next.store = open()
	next.authority = admitAgentClaimProfileAuthority(t, next.ctx, next.store, next.source)
	nextProcess, nextGrant := agentRecoveryFixtureOwners(t, next)
	rebindAgentProfileProcessLifecycle(t, next, nextGrant, previous)
	if err := next.store.ActivateDeliveryAuthority(next.ctx, next.authority); err != nil {
		t.Fatal(err)
	}
	result, err := next.store.ClaimDelivery(next.ctx, next.authority, pending, next.route)
	second, acquired := result.Acquired()
	if err != nil || !result.Acknowledged || !acquired || second.Claim.Version() != 1 {
		t.Fatalf("native successor's exact first claim: %+v err=%v", result, err)
	}
	current := readNativeAgentRecoveryCandidate(t, next)
	assertAgentRecoveryCandidateOwner(t, current, nextProcess, nextGrant)
	if current.Image.Process.Authority.AuthorityGeneration != previous.Process.AuthorityGeneration+1 ||
		current.Image.Process.Authority.AuthorityID == previous.Process.AuthorityID ||
		!reflect.DeepEqual(current.Image.SourceHead, candidate.Image.SourceHead) ||
		!reflect.DeepEqual(current.InProgressByProfile, map[string]int{profile.ProfileID: 2}) ||
		candidate.Image.Process.Validate() != nil ||
		!reflect.DeepEqual(candidate.InProgressByProfile, map[string]int{profile.ProfileID: 1}) {
		t.Fatalf("post-read native takeover did not distinguish old count1 from new count2 with the SAME source plan: old=%+v new=%+v", candidate, current)
	}
	before := profileFixtureSnapshot(t, next)
	if err := process.ProveCurrent(next.ctx); err == nil {
		t.Fatal("previously successful point-in-time proof survived native replacement")
	}
	assertAgentRecoveryRetiredGrant(t, next.ctx, grant, first.Snapshot.RunID)
	assertAgentRecoveryClaimRefused(t, next, pending, f.authority, deliverylifecycle.ClaimWrongAuthority)
	if after := profileFixtureSnapshot(t, next); !reflect.DeepEqual(before, after) {
		t.Fatal("old-owner refusals changed complete successor storage")
	}
	// Native possession alone also cannot turn an incomplete reconstruction
	// into empty/free capacity. Fault only the second exact first-capture cell.
	secondProfile, err := ReadFirstAgentClaimProfileForTest(next.ctx, next.store, second.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := ApplyAgentClaimProfileFaultForTest(next.ctx, next.store, second.Claim, secondProfile,
		AgentClaimProfileMissing, deliverylifecycle.Claim{}); err != nil {
		t.Fatal(err)
	}
	corrupted := profileFixtureSnapshot(t, next)
	assertOnlyAgentProfileCellChanged(t, before, corrupted, second.Snapshot.DeliveryID)
	if err := nextProcess.ProveCurrent(next.ctx); err != nil {
		t.Fatalf("required capture fault changed live possession: %v", err)
	}
	actual, err := next.store.Snapshot(next.ctx, first.Snapshot.DeliveryID)
	if err != nil {
		t.Fatal(err)
	}
	assertAgentProfileRecoveryCapture(t, next, actual, profile)
	refused, err := startuprecovery.InspectAgentRecoveryOccupancyCandidate(next.ctx, next.store.(startuprecovery.AgentRecoveryReader))
	if err == nil || !reflect.DeepEqual(refused, startuprecovery.AgentRecoveryOccupancyCandidate{}) {
		t.Fatalf("live successor exposed partial/empty replacement occupancy after one required capture failed: %+v err=%v", refused, err)
	}
	assertAgentProfileProcessNoAttempts(t, next)
	if after := profileFixtureSnapshot(t, next); !reflect.DeepEqual(corrupted, after) {
		t.Fatal("incomplete reconstruction refusal changed complete storage")
	}
	t.Log("diagnostic cut: read -> exact ProveCurrent succeeds -> native replacement/reopen -> same source plan but stale count1 versus count2; current possession plus a missing second capture still yields ZERO candidate, no provider attempt or adoption")
}
