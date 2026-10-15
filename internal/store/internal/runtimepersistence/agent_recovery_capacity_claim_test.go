package runtimepersistence

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// The original RED used the freshness-only entry. The assertions below are
// unchanged; this candidate now invokes the original owner's capacity role.
func claimAgentRecoveryCapacityCandidate(ctx context.Context, process startupownership.ProcessCapability, f agentClaimProfileFixture, event events.Event) (bool, deliverylifecycle.ClaimResult, error) {
	result, err := startupownership.TryClaimAgentRecoveryDelivery(ctx, process, f.authority, event, f.route)
	claim, _ := result.NativeClaim()
	return result.Waiting(), claim, err
}

func bindAgentRecoveryCapacityPolicy(t *testing.T, process startupownership.ProcessCapability, profile string, limit int) {
	t.Helper()
	cfg := &config.Config{LLM: config.LLMConfig{Backend: "anthropic",
		Session:        config.LLMSessionConfig{LockTTL: time.Second, RotateAfterTurns: 1, RotateOnParseFailures: 1},
		ProviderLimits: map[string]config.LLMProviderLimitPolicy{profile: {MaxConcurrency: limit}},
	}}
	if err := startupownership.BindAgentRecoveryProfilePolicy(t.Context(), process, cfg); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRecoveryCapacityNativeFullAndRoomBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, full := range []bool{true, false} {
			name := "room"
			if full {
				name = "full"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				f := newAgentClaimProfileFixture(t, backend)
				_, first, profile := f.claim(t)
				wanted := publishAgentProfileRecoveryEvent(t, f)
				process, _ := agentRecoveryFixtureOwners(t, f)
				limit := 2
				if full {
					limit = 1
				}
				bindAgentRecoveryCapacityPolicy(t, process, profile.ProfileID, limit)
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
					t.Fatal(err)
				}
				before := profileFixtureSnapshot(t, f)
				wait, result, err := claimAgentRecoveryCapacityCandidate(f.ctx, process, f, wanted)
				if err != nil || wait != full {
					t.Errorf("capacity result: full=%t wait=%t native=%+v err=%v", full, wait, result, err)
				}
				id, err := deliverylifecycle.DeliveryID(wanted.ID(), f.route)
				if err != nil {
					t.Fatal(err)
				}
				actual, err := f.store.Snapshot(f.ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				if full {
					if actual.Status != deliverylifecycle.StatusPending || actual.ClaimVersion != 0 {
						t.Errorf("full profile mutated the exact Pending/v0 target: %+v", actual)
					}
					if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
						t.Error("capacity wait changed complete durable storage")
					}
					assertNativeAgentRecoveryOccupancy(t, f, map[string]int{profile.ProfileID: 1})
				} else {
					claim, acquired := result.Acquired()
					if !result.Acknowledged || !acquired || claim.Claim.Version() != 1 || actual.Status != deliverylifecycle.StatusInProgress {
						t.Errorf("room did not acquire the original native first claim: %+v", result)
					}
					captured, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, actual)
					if err != nil || !captured.MatchesFirstAdmission(wanted, f.route, f.authority) || captured.ProfileID != profile.ProfileID {
						t.Errorf("room lost canonical immutable admission: %+v err=%v", captured, err)
					}
					assertNativeAgentRecoveryOccupancy(t, f, map[string]int{profile.ProfileID: 2})
				}
				retained, err := f.store.Snapshot(f.ctx, first.Claim.DeliveryID())
				if err != nil || retained.Status != deliverylifecycle.StatusInProgress || retained.ClaimVersion != first.Claim.Version() {
					t.Errorf("capacity decision disturbed the existing occupant: %+v err=%v", retained, err)
				}
				if err := startupownership.CheckAgentRecoverySeed(f.ctx, process); err == nil {
					t.Error("native capacity transaction implicitly restored the consumed seed")
				}
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
					t.Fatalf("native return did not join original cleanup: %v", err)
				}
				assertAgentProfileProcessNoAttempts(t, f)
			})
		}
	}
}
