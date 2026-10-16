package runtimepersistence

import (
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/startuprecovery"
)

func TestAgentRecoverySeedNativeReadBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newAgentClaimProfileFixture(t, backend)
			_, _, profile := f.claim(t)
			publishAgentProfileRecoveryEvent(t, f)
			process, grant := agentRecoveryFixtureOwners(t, f)
			candidate := readNativeAgentRecoveryCandidate(t, f)
			assertAgentRecoveryCandidateOwner(t, candidate, process, grant)
			if !reflect.DeepEqual(candidate.InProgressByProfile, map[string]int{profile.ProfileID: 1}) {
				t.Fatalf("complete native candidate lost its immutable profile: %+v", candidate)
			}
			before := profileFixtureSnapshot(t, f)
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
				t.Fatalf("seed original possessed complete image: %v", err)
			}
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err == nil || !strings.Contains(err.Error(), "already installed") {
				t.Fatalf("duplicate invocation did not refuse reseeding: %v", err)
			}
			if err := process.ProveCurrent(f.ctx); err != nil {
				t.Fatal(err)
			}
			if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
				t.Fatal("private seed or duplicate invocation changed complete durable storage")
			}
			assertAgentProfileProcessNoAttempts(t, f)
			if err := process.Release(f.ctx); err != nil {
				t.Fatal(err)
			}
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err == nil {
				t.Fatal("released owner re-adopted an observed image")
			}
		})
	}
}

func TestAgentRecoverySeedMalformedMemberDoesNotFenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fault := range []AgentClaimProfileFault{AgentClaimProfileMissing, AgentClaimProfileCorrupt} {
			t.Run(backend+"/"+string(fault), func(t *testing.T) { proveAgentRecoverySeedMalformedMember(t, backend, fault) })
		}
	}
}

func proveAgentRecoverySeedMalformedMember(t *testing.T, backend string, fault AgentClaimProfileFault) {
	t.Helper()
	f := newAgentClaimProfileFixture(t, backend)
	_, first, firstProfile := f.claim(t)
	_, second, secondProfile := f.claim(t)
	process, _ := agentRecoveryFixtureOwners(t, f)
	if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
		t.Fatalf("seed original complete image before the native corruption cut: %v", err)
	}
	if err := startupownership.CheckAgentRecoverySeed(f.ctx, process); err != nil {
		t.Fatalf("healthy original seed check: %v", err)
	}
	before := profileFixtureSnapshot(t, f)
	if err := ApplyAgentClaimProfileFaultForTest(f.ctx, f.store, second.Claim, secondProfile, fault, deliverylifecycle.Claim{}); err != nil {
		t.Fatal(err)
	}
	corrupted := profileFixtureSnapshot(t, f)
	assertOnlyAgentProfileCellChanged(t, before, corrupted, second.Snapshot.DeliveryID)
	candidate, refusal := startuprecovery.InspectAgentRecoveryOccupancyCandidate(f.ctx, f.store.(startuprecovery.AgentRecoveryReader))
	if refusal == nil || !reflect.DeepEqual(candidate, startuprecovery.AgentRecoveryOccupancyCandidate{}) {
		t.Fatalf("fault did not refuse the canonical image: %+v err=%v", candidate, refusal)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := startupownership.SeedAgentRecovery(f.ctx, process); err == nil || !strings.Contains(err.Error(), refusal.Error()) {
			t.Fatalf("malformed member lost its domain refusal or installed a partial seed (attempt %d): %v; want %v", attempt, err, refusal)
		}
		if err := process.ProveCurrent(f.ctx); err != nil {
			t.Fatalf("malformed image fenced native possession: %v", err)
		}
		if err := startupownership.CheckAgentRecoverySeed(f.ctx, process); err == nil {
			t.Fatal("missing/corrupt immutable profile validated a private seed")
		}
	}
	assertAgentProfileRecoveryCapture(t, f, first.Snapshot, firstProfile)
	if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(corrupted, after) {
		t.Fatal("malformed seed refusal changed complete durable storage")
	}
	assertAgentProfileProcessNoAttempts(t, f)
}

func TestAgentRecoverySeedSourceRefusalCanRecoverBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newAgentClaimProfileFixture(t, backend)
			f.claim(t)
			process, _ := agentRecoveryFixtureOwners(t, f)
			plan, found, err := process.CurrentSourceSet(f.ctx)
			if err != nil || !found {
				t.Fatalf("original native source set: found=%t err=%v", found, err)
			}
			before := profileFixtureSnapshot(t, f)
			receipt, err := ApplySourceSetHeadFaultForTest(f.ctx, f.store, plan, SourceSetHeadRevisionMismatch)
			if err != nil {
				t.Fatal(err)
			}
			restored := false
			t.Cleanup(func() {
				if !restored {
					if err := RestoreSourceSetHeadFaultForTest(f.ctx, f.store, receipt); err != nil {
						t.Error(err)
					}
				}
			})
			corrupt := profileFixtureSnapshot(t, f)
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err == nil || !strings.Contains(err.Error(), "source") {
				t.Fatalf("invalid source head lost its domain refusal: %v", err)
			}
			if err := process.ProveCurrent(f.ctx); err != nil {
				t.Fatalf("source refusal fenced native possession: %v", err)
			}
			if err := startupownership.CheckAgentRecoverySeed(f.ctx, process); err == nil {
				t.Fatal("corrupt source head validated a private seed")
			}
			if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(corrupt, after) {
				t.Fatal("source refusal changed complete durable storage")
			}
			if err := RestoreSourceSetHeadFaultForTest(f.ctx, f.store, receipt); err != nil {
				t.Fatal(err)
			}
			restored = true
			if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
				t.Fatal("exact source-head restoration changed other durable evidence")
			}
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
				t.Fatalf("source refusal installed a partial seed or lost its owner: %v", err)
			}
			if err := startupownership.CheckAgentRecoverySeed(f.ctx, process); err != nil {
				t.Fatalf("restored complete source seed did not validate: %v", err)
			}
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err == nil || !strings.Contains(err.Error(), "already installed") {
				t.Fatalf("restored complete image reseeded: %v", err)
			}
			assertAgentProfileProcessNoAttempts(t, f)
		})
	}
}

func TestAgentRecoverySeedValidEmptyNativeImageBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newAgentClaimProfileFixture(t, backend)
			publishAgentProfileRecoveryEvent(t, f)
			process, grant := agentRecoveryFixtureOwners(t, f)
			candidate, err := startuprecovery.InspectAgentRecoveryOccupancyCandidate(f.ctx, f.store.(startuprecovery.AgentRecoveryReader))
			if err != nil || len(candidate.Image.Members) != 1 || candidate.InProgressByProfile == nil || len(candidate.InProgressByProfile) != 0 {
				t.Fatalf("valid pending-only complete image: %+v err=%v", candidate, err)
			}
			assertAgentRecoveryCandidateOwner(t, candidate, process, grant)
			before := profileFixtureSnapshot(t, f)
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
				t.Fatalf("valid empty native census refused: %v", err)
			}
			if err := startupownership.CheckAgentRecoverySeed(f.ctx, process); err != nil {
				t.Fatalf("valid complete-empty native seed did not validate: %v", err)
			}
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err == nil || !strings.Contains(err.Error(), "already installed") {
				t.Fatalf("empty census was treated as failure/uninitialized: %v", err)
			}
			if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
				t.Fatal("empty native seed changed complete durable storage")
			}
			assertAgentProfileProcessNoAttempts(t, f)
		})
	}
}
