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
		t.Run(backend, func(t *testing.T) {
			f := newAgentClaimProfileFixture(t, backend)
			_, first, firstProfile := f.claim(t)
			_, second, secondProfile := f.claim(t)
			process, _ := agentRecoveryFixtureOwners(t, f)
			before := profileFixtureSnapshot(t, f)
			if err := ApplyAgentClaimProfileFaultForTest(f.ctx, f.store, second.Claim, secondProfile, AgentClaimProfileMissing, deliverylifecycle.Claim{}); err != nil {
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
			}
			assertAgentProfileRecoveryCapture(t, f, first.Snapshot, firstProfile)
			if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(corrupted, after) {
				t.Fatal("malformed seed refusal changed complete durable storage")
			}
			assertAgentProfileProcessNoAttempts(t, f)
		})
	}
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
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err == nil || !strings.Contains(err.Error(), "already installed") {
				t.Fatalf("restored complete image reseeded: %v", err)
			}
			assertAgentProfileProcessNoAttempts(t, f)
		})
	}
}
