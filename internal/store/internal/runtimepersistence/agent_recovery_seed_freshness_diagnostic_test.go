package runtimepersistence

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/startuprecovery"
)

// The original gap is preserved at fe4db94f3d. These fixed-contract cuts prove
// private observation revocation/reconstruction, not a capacity decision.
func TestAgentRecoverySeedRevokesAcrossClaimAndSettlementBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mutation := range []string{"claim", "settlement"} {
			for _, cut := range []string{"read", "publication", "installed"} {
				t.Run(backend+"/"+mutation+"/"+cut, func(t *testing.T) {
					proveAgentRecoverySeedFreshness(t, backend, mutation, cut)
				})
			}
		}
	}
}

func proveAgentRecoverySeedFreshness(t *testing.T, backend, mutation, cut string) {
	t.Helper()
	f := newAgentClaimProfileFixture(t, backend)
	_, first, profile := f.claim(t)
	pending := publishAgentProfileRecoveryEvent(t, f)
	wantBefore, wantAfter := 1, 2
	if mutation == "settlement" {
		result, err := f.store.ClaimDelivery(f.ctx, f.authority, pending, f.route)
		if _, acquired := result.Acquired(); err != nil || !result.Acknowledged || !acquired {
			t.Fatalf("exact second claim before seed: %+v err=%v", result, err)
		}
		wantBefore, wantAfter = 2, 1
	}
	process, grant := agentRecoveryFixtureOwners(t, f)
	before := profileFixtureSnapshot(t, f)
	old := readNativeAgentRecoveryCandidate(t, f)
	assertAgentRecoveryCandidateOwner(t, old, process, grant)
	if !reflect.DeepEqual(old.InProgressByProfile, map[string]int{profile.ProfileID: wantBefore}) {
		t.Fatalf("complete original occupancy: %+v", old)
	}
	var seeded chan error
	unblock := func() {}
	joined := false
	if cut == "installed" {
		if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
			t.Fatal(err)
		}
	} else {
		var entered <-chan struct{}
		entered, unblock = holdAgentRecoverySeedCut(t, process, cut)
		seeded = make(chan error, 1)
		go func() { seeded <- startupownership.SeedAgentRecovery(f.ctx, process) }()
		t.Cleanup(func() {
			unblock()
			if !joined {
				<-seeded
			}
		})
		awaitAgentRecoverySeedCut(t, entered)
	}
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
		t.Fatal("seed changed complete durable storage before the named mutation")
	}
	// The writer uses the original ordinary context, never the held read's
	// borrowed context. Its native ACK must precede release of either cut.
	ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	defer cancel()
	if mutation == "claim" {
		result, err := f.store.ClaimDelivery(ctx, f.authority, pending, f.route)
		claimed, acquired := result.Acquired()
		if err != nil || !result.Acknowledged || !acquired || claimed.Claim.Version() != 1 {
			t.Fatalf("accepted later claim while seed is %s: %+v err=%v", cut, result, err)
		}
		capture, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, claimed.Snapshot)
		if err != nil || capture.ProfileID != profile.ProfileID || !capture.MatchesDelivery(claimed.Snapshot) {
			t.Fatalf("accepted later claim lost exact immutable profile: %+v err=%v", capture, err)
		}
	} else {
		settled, err := f.store.SettleSuccess(ctx, first.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection())
		if err != nil || settled.Status != deliverylifecycle.StatusDelivered || settled.ClaimVersion != first.Claim.Version() {
			t.Fatalf("exact settlement while seed is %s: %+v err=%v", cut, settled, err)
		}
		actual, err := f.store.Snapshot(f.ctx, first.Snapshot.DeliveryID)
		if err != nil || !reflect.DeepEqual(actual, settled) {
			t.Fatalf("native settlement did not commit its exact result: %+v err=%v", actual, err)
		}
	}
	written := profileFixtureSnapshot(t, f)
	if reflect.DeepEqual(before, written) {
		t.Fatal("named writer did not change durable storage")
	}
	if cut != "installed" {
		select {
		case err := <-seeded:
			joined = true
			t.Fatalf("held seed returned before release: %v", err)
		default:
		}
		unblock()
		err := <-seeded
		joined = true
		if err == nil || !strings.Contains(err.Error(), "complete fresh reconstruction") {
			t.Fatalf("held seed did not refuse stale publication after accepted writer: %v", err)
		}
	}
	// No grant/source operation runs here: those deliberately retain their
	// established locking transaction. Only the named writer may revoke this seed.
	if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
		t.Fatalf("accepted writer did not permit explicit complete reconstruction: %v", err)
	}
	current, err := startuprecovery.InspectAgentRecoveryOccupancyCandidate(f.ctx, f.store.(startuprecovery.AgentRecoveryReader))
	if err != nil || current.Image.Process.Authority == nil || current.Image.Process.Possession != nil ||
		!reflect.DeepEqual(current.InProgressByProfile, map[string]int{profile.ProfileID: wantAfter}) ||
		!reflect.DeepEqual(current.Image.SourceHead, old.Image.SourceHead) ||
		*current.Image.Process.Authority != *old.Image.Process.Authority {
		t.Fatalf("same live process/source has wrong post-writer occupancy: %+v err=%v", current, err)
	}
	if err := process.ProveCurrent(f.ctx); err != nil {
		t.Fatalf("freshness refusal or read-only inspection lost native possession: %v", err)
	}
	if err := startupownership.SeedAgentRecovery(f.ctx, process); err == nil || !strings.Contains(err.Error(), "already installed") {
		t.Fatalf("read-only inspection/proof revoked the newly reconstructed seed: %v", err)
	}
	if !reflect.DeepEqual(old.InProgressByProfile, map[string]int{profile.ProfileID: wantBefore}) {
		t.Fatal("original complete observation was mutated rather than replaced")
	}
	assertAgentProfileProcessNoAttempts(t, f)
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(written, after) {
		t.Fatal("seed completion, duplicate refusal or fresh reconstruction changed complete writer storage")
	}
	t.Logf("fixed private freshness: %s ACK during seed %s revoked/refused the earlier observation, explicit reconstruction succeeds once; same native process/source, old count=%d current count=%d; native seed bytes UNOBSERVED; no provider attempt or slot/capacity decision", mutation, cut, wantBefore, wantAfter)
}
