package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

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

func TestAgentRecoveryPossessionProofDoesNotJoinOriginalWriteBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, rollback := range []bool{false, true} {
			name := "commit"
			if rollback {
				name = "rollback"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				f := newAgentClaimProfileFixture(t, backend)
				f.claim(t)
				publishAgentProfileRecoveryEvent(t, f)
				process, _ := agentRecoveryFixtureOwners(t, f)
				original := readNativeAgentRecoveryCandidate(t, f)
				before := profileFixtureSnapshot(t, f)
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(f.ctx, 10*time.Second)
				defer cancel()
				entered, resume := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(resume) }) }
				primary := errors.New("original native consumer diagnostic rollback")
				writer := f.store.(interface {
					runEventTransaction(context.Context, func(context.Context, *sql.Tx) error) error
				})
				result := make(chan error, 1)
				go func() {
					result <- writer.runEventTransaction(ctx, func(context.Context, *sql.Tx) error {
						close(entered)
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-resume:
						}
						if rollback {
							return primary
						}
						return nil
					})
				}()
				joined := false
				t.Cleanup(func() {
					unblock()
					cancel()
					if !joined {
						<-result
					}
				})
				awaitAgentRecoverySeedCut(t, entered)
				if err := process.ProveCurrent(ctx); err != nil {
					t.Fatalf("held original pooled write unexpectedly lost possession: %v", err)
				}
				if err := startupownership.SeedAgentRecovery(ctx, process); err == nil || !strings.Contains(err.Error(), "complete fresh reconstruction") {
					t.Fatalf("possession proof admitted reconstruction before the original write joined: %v", err)
				}
				select {
				case err := <-result:
					joined = true
					t.Fatalf("original transaction returned before its held cut joined: %v", err)
				default:
				}
				unblock()
				err := <-result
				joined = true
				if rollback && !errors.Is(err, primary) || !rollback && err != nil {
					t.Fatalf("observer changed original native disposition: %v", err)
				}
				if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
					t.Fatal("empty original transaction changed complete application storage")
				}
				if current := readNativeAgentRecoveryCandidate(t, f); !reflect.DeepEqual(original, current) {
					t.Fatal("empty joined write changed the complete durable observation")
				}
				assertAgentRecoveryReseedsAfterNativeMutation(t, f.ctx, f.store, process)
				assertAgentProfileProcessNoAttempts(t, f)
				t.Log("DIAGNOSTIC: original pooled transaction held; exact native possession still passes but seed reconstruction refuses until original commit/rollback and cleanup join; unchanged durable image still requires explicit complete reseed; no consumer, slots or launch")
			})
		}
	}
}
