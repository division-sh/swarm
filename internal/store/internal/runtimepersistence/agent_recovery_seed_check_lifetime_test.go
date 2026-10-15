package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func TestAgentRecoverySeedCheckAcrossNativeLifetimeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"capture", "proof"} {
			for _, change := range []string{"claim", "settlement", "reclaim_reseed", "cancel", "takeover", "native_loss"} {
				t.Run(backend+"/"+cut+"/"+change, func(t *testing.T) {
					proveAgentRecoverySeedCheckNativeCut(t, backend, cut, change)
				})
			}
		}
	}
}

func proveAgentRecoverySeedCheckNativeCut(t *testing.T, backend, cut, change string) {
	t.Helper()
	f := newAgentClaimProfileFixture(t, backend)
	event, first, profile := f.claim(t)
	pending := publishAgentProfileRecoveryEvent(t, f)
	if change == "reclaim_reseed" {
		if err := ExpireExactDeliveryClaimFaultForTest(f.ctx, f.store, first.Claim); err != nil {
			t.Fatal(err)
		}
	}
	process, grant := agentRecoveryFixtureOwners(t, f)
	stamp, err := process.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	original := readNativeAgentRecoveryCandidate(t, f)
	assertAgentRecoveryCandidateOwner(t, original, process, grant)
	before := profileFixtureSnapshot(t, f)
	if err := startupownership.CheckAgentRecoverySeed(f.ctx, process); err == nil {
		t.Fatal("original possessed process validated before complete seeding")
	}
	if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
		t.Fatal(err)
	}
	if err := startupownership.CheckAgentRecoverySeed(f.ctx, process); err != nil {
		t.Fatalf("real original complete image seed did not validate: %v", err)
	}
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	observe := func() { close(entered); <-resume }
	probe := &startupownership.AgentRecoverySeedFaultProbeForTest{}
	if cut == "capture" {
		probe.AfterCheckCapture = observe
	} else {
		probe.AfterCheckProof = observe
	}
	seedEntered, seedResume := make(chan struct{}), make(chan struct{})
	var seedOnce sync.Once
	unblockSeed := func() { seedOnce.Do(func() { close(seedResume) }) }
	if change == "native_loss" {
		probe.InsideRead = func() { close(seedEntered); <-seedResume }
	}
	remove, err := startupownership.InstallAgentRecoverySeedFaultProbeForTest(process, probe)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- startupownership.CheckAgentRecoverySeed(ctx, process) }()
	joined := false
	t.Cleanup(func() {
		unblockSeed()
		unblock()
		remove()
		if !joined {
			<-result
		}
	})
	awaitAgentRecoverySeedCut(t, entered)
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
		t.Fatal("initial check/capture/proof changed complete durable storage")
	}
	if err := interruptAgentRecoverySeedFixture(f.ctx, f.store, process, stamp); err == nil {
		t.Fatal("passive check hold authorized original-session interruption")
	}
	currentProcess := process
	switch change {
	case "claim", "native_loss":
		claimed, err := f.store.ClaimDelivery(f.ctx, f.authority, pending, f.route)
		second, acquired := claimed.Acquired()
		if err != nil || !claimed.Acknowledged || !acquired || second.Claim.Version() != 1 {
			t.Fatalf("actual first claim across check: %+v err=%v", claimed, err)
		}
		assertNativeAgentRecoveryOccupancy(t, f, map[string]int{profile.ProfileID: 2})
	case "settlement":
		settled, err := f.store.SettleSuccess(f.ctx, first.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection())
		if err != nil || settled.Status != deliverylifecycle.StatusDelivered || settled.ClaimVersion != first.Claim.Version() {
			t.Fatalf("actual exact settlement across check: %+v err=%v", settled, err)
		}
		assertNativeAgentRecoveryOccupancy(t, f, map[string]int{})
		assertAgentProfileRecoveryCapture(t, f, settled, profile)
	case "reclaim_reseed":
		claimed, err := f.store.ClaimDelivery(f.ctx, f.authority, event, f.route)
		reclaimed, acquired := claimed.Acquired()
		if err != nil || !claimed.Acknowledged || !acquired || reclaimed.Claim.Version() != first.Claim.Version()+1 {
			t.Fatalf("same-count reclaim across check: %+v err=%v", claimed, err)
		}
		assertAgentProfileRecoveryCapture(t, f, reclaimed.Snapshot, profile)
		assertNativeAgentRecoveryOccupancy(t, f, original.InProgressByProfile)
		if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
			t.Fatalf("explicit complete reseed could not proceed across unlocked check: %v", err)
		}
	case "cancel":
		cancel()
	case "takeover":
		if err := process.Release(f.ctx); err != nil {
			t.Fatalf("check retained serialization across native release: %v", err)
		}
		next, err := f.store.(startupownership.Store).AcquireProcessCapability(f.ctx, startupownership.AcquireRequest{
			OwnerID: "check-successor", BootID: uuid.NewString(), RuntimeInstanceID: stamp.RuntimeInstanceID,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := next.Release(context.WithoutCancel(f.ctx)); err != nil {
				t.Error(err)
			}
		})
		currentProcess = next
		current, err := next.Evidence()
		if err != nil || current.AuthorityGeneration != stamp.AuthorityGeneration+1 || current.AuthorityID == stamp.AuthorityID {
			t.Fatalf("takeover lost exact successor identity: %+v err=%v", current, err)
		}
		if err := startupownership.CheckAgentRecoverySeed(f.ctx, next); err == nil {
			t.Fatal("successor inherited predecessor private seed")
		}
		if err := startupownership.SeedAgentRecovery(f.ctx, next); err != nil {
			t.Fatalf("successor complete reconstruction: %v", err)
		}
	}
	written := profileFixtureSnapshot(t, f)
	if change == "native_loss" {
		// Only an actual held Seed read activates the existing native fault
		// port. The simultaneous checker hold grants no such right.
		seeded := make(chan error, 1)
		go func() { seeded <- startupownership.SeedAgentRecovery(f.ctx, process) }()
		seedJoined := false
		t.Cleanup(func() {
			unblockSeed()
			if !seedJoined {
				<-seeded
			}
		})
		awaitAgentRecoverySeedCut(t, seedEntered)
		if err := interruptAgentRecoverySeedFixture(f.ctx, f.store, process, stamp); err != nil {
			t.Fatalf("actual retained Seed cut could not interrupt original session: %v", err)
		}
		unblockSeed()
		seedErr := <-seeded
		seedJoined = true
		if seedErr == nil || backend == "sqlite" && !errors.Is(seedErr, sql.ErrConnDone) {
			t.Fatalf("native loss returned late seed success or lost original error: %v", seedErr)
		}
	}
	select {
	case err := <-result:
		joined = true
		t.Fatalf("check returned before its exact passive cut joined: %v", err)
	default:
	}
	unblock()
	err = <-result
	joined = true
	remove()
	if err == nil || change == "cancel" && !errors.Is(err, context.Canceled) {
		t.Fatalf("check accepted changed/canceled observation: change=%s err=%v", change, err)
	}
	if change == "native_loss" {
		if err := startupownership.CheckAgentRecoverySeed(f.ctx, process); err == nil {
			t.Fatal("native loss allowed old owner to check again")
		}
		if err := startupownership.SeedAgentRecovery(f.ctx, process); err == nil {
			t.Fatal("native loss allowed old owner to reconstruct")
		}
	} else {
		if err := currentProcess.ProveCurrent(f.ctx); err != nil {
			t.Fatalf("seed refusal/cancellation lost current native possession: %v", err)
		}
		if change == "claim" || change == "settlement" {
			if err := startupownership.CheckAgentRecoverySeed(f.ctx, currentProcess); err == nil {
				t.Fatal("joined writer implicitly reseeded observation")
			}
			if err := startupownership.SeedAgentRecovery(f.ctx, currentProcess); err != nil {
				t.Fatalf("explicit current complete reseed: %v", err)
			}
		}
		if err := startupownership.CheckAgentRecoverySeed(f.ctx, currentProcess); err != nil {
			t.Fatalf("current original/reseeded/successor observation did not validate: %v", err)
		}
	}
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(written, after) {
		t.Fatal("late refusal/current check/explicit reseed changed committed writer storage")
	}
	if change == "cancel" && !reflect.DeepEqual(before, written) {
		t.Fatal("check cancellation changed durable storage")
	}
	if change == "takeover" {
		for _, table := range []string{"runtime_process_generation", "runtime_generation_grants"} {
			if _, found := before[table]; !found {
				t.Fatalf("complete fingerprint is missing exact native authority table %s", table)
			}
			delete(before, table)
			delete(written, table)
		}
		if !reflect.DeepEqual(before, written) {
			t.Fatal("takeover changed application rows outside native generation/grant facts")
		}
	}
	assertAgentProfileProcessNoAttempts(t, f)
	t.Logf("error-only check refused %s across %s; exact native read/write/lifetime, complete storage conserved after named mutation, passive hold has no native fault rights; no counts/slots/launch", change, cut)
}
