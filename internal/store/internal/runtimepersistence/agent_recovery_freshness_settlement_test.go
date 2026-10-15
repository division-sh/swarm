package runtimepersistence

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/testutil/agentfixture"
)

func assertAgentRecoveryReseedsAfterNativeMutation(t *testing.T, ctx context.Context, store any, process startupownership.ProcessCapability) {
	t.Helper()
	written, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, store)
	if err != nil {
		t.Fatal(err)
	}
	if err := startupownership.SeedAgentRecovery(ctx, process); err != nil {
		t.Fatalf("native mutation did not revoke the earlier private observation: %v", err)
	}
	if err := startupownership.SeedAgentRecovery(ctx, process); err == nil || !strings.Contains(err.Error(), "already installed") {
		t.Fatalf("complete explicit reconstruction did not install once: %v", err)
	}
	after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, store)
	if err != nil || !reflect.DeepEqual(written, after) {
		t.Fatalf("reconstruction/refusal changed full native writer storage: %v", err)
	}
}

func TestAgentRecoverySeedReclaimRetryFailureAndBulkFreshnessBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mutation := range []string{"reclaim", "retry", "dead_letter", "bulk"} {
			t.Run(backend+"/"+mutation, func(t *testing.T) {
				f := newAgentClaimProfileFixture(t, backend)
				event, claim, profile := f.claim(t)
				if mutation == "reclaim" {
					if err := ExpireExactDeliveryClaimFaultForTest(f.ctx, f.store, claim.Claim); err != nil {
						t.Fatal(err)
					}
				}
				process, err := agentfixture.ProcessCapability(t, f.ctx, f.store)
				if err != nil {
					t.Fatal(err)
				}
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
					t.Fatal(err)
				}
				var snapshot deliverylifecycle.Snapshot
				switch mutation {
				case "reclaim":
					result, claimErr := f.store.ClaimDelivery(f.ctx, f.authority, event, f.route)
					current, acquired := result.Acquired()
					if claimErr != nil || !result.Acknowledged || !acquired || current.Claim.Version() != 2 {
						t.Fatalf("exact same-count reclaim: %+v err=%v", result, claimErr)
					}
					snapshot = current.Snapshot
				case "retry", "dead_letter":
					settlement := deliverylifecycle.Settlement{Disposition: deliverylifecycle.FailureRetry, Failure: testRetryableFailure(),
						RetryBase: time.Hour, RuleSelection: deliverylifecycle.NotApplicableHandlerRuleObservation()}
					if mutation == "dead_letter" {
						settlement.Disposition, settlement.ReasonCode = deliverylifecycle.FailureDeadLetter, "freshness_terminal"
					}
					snapshot, err = f.store.SettleFailure(f.ctx, claim.Claim, settlement)
					want := deliverylifecycle.StatusFailed
					if mutation == "dead_letter" {
						want = deliverylifecycle.StatusDeadLetter
					}
					if err != nil || snapshot.Status != want {
						t.Fatalf("exact %s settlement: %+v err=%v", mutation, snapshot, err)
					}
				case "bulk":
					terminal, terminalErr := f.store.TerminalizeRun(f.ctx, claim.Claim.RunID(), "freshness_bulk")
					if terminalErr != nil || len(terminal) != 1 || terminal[0].Current.Status != deliverylifecycle.StatusDeadLetter {
						t.Fatalf("original bulk terminalization: %+v err=%v", terminal, terminalErr)
					}
					snapshot = terminal[0].Current
				}
				capture, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, snapshot)
				if err != nil || capture != profile {
					t.Fatalf("native mutation changed immutable first profile: %+v err=%v", capture, err)
				}
				assertAgentRecoveryReseedsAfterNativeMutation(t, f.ctx, f.store, process)
				want := map[string]int{}
				if mutation == "reclaim" {
					want[profile.ProfileID] = 1
				}
				assertNativeAgentRecoveryOccupancy(t, f, want)
				assertAgentProfileProcessNoAttempts(t, f)
			})
		}
	}
}

func TestAgentRecoverySeedNativeForeignBackendDoesNotInvalidateBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newAgentClaimProfileFixture(t, backend)
			f.claim(t)
			foreign := newAgentClaimProfileFixture(t, backend)
			process, _ := agentRecoveryFixtureOwners(t, f)
			before := profileFixtureSnapshot(t, f)
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
				t.Fatal(err)
			}
			if err := startupownership.CheckAgentRecoverySeed(f.ctx, process); err != nil {
				t.Fatalf("original independent-backend control seed did not validate: %v", err)
			}
			foreign.claim(t)
			if err := startupownership.CheckAgentRecoverySeed(f.ctx, process); err != nil {
				t.Fatalf("foreign native backend invalidated this process seed: %v", err)
			}
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err == nil || !strings.Contains(err.Error(), "already installed") {
				t.Fatalf("foreign native backend invalidated this private observation: %v", err)
			}
			if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
				t.Fatal("foreign backend write or duplicate refusal changed complete original storage")
			}
			assertAgentProfileProcessNoAttempts(t, f)
			assertAgentProfileProcessNoAttempts(t, foreign)
		})
	}
}
