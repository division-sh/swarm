package runtimepersistence

import (
	"database/sql/driver"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

func TestAgentRecoveryCapacityNativeCommitOutcomeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, full := range []bool{true, false} {
			for _, outcome := range []string{"acknowledged", "rollback_before_lost_ack", "commit_before_lost_ack"} {
				name := "room"
				if full {
					name = "full"
				}
				t.Run(backend+"/"+name+"/"+outcome, func(t *testing.T) {
					selected, _, connector := newP16RaceStore(t, backend)
					f := admitAgentClaimProfileFixture(t, testAuthorActivityContext(), selected.(agentFixtureFlowStore))
					_, _, profile := f.claim(t)
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
					fault := errors.New("capacity native acknowledgement unavailable")
					var commits atomic.Int32
					remove, err := startupownership.InstallAgentRecoverySeedFaultProbeForTest(process, &startupownership.AgentRecoverySeedFaultProbeForTest{AfterCheckProof: func() {
						connector.arm(func(tx driver.Tx) error {
							commits.Add(1)
							if outcome == "rollback_before_lost_ack" {
								return errors.Join(fault, tx.Rollback())
							}
							err := tx.Commit()
							if outcome == "commit_before_lost_ack" {
								return errors.Join(fault, err)
							}
							return err
						})
					}})
					if err != nil {
						t.Fatal(err)
					}
					defer remove()
					result, err := startupownership.TryClaimAgentRecoveryDelivery(f.ctx, process, f.authority, wanted, f.route)
					claim, native := result.NativeClaim()
					ack := outcome == "acknowledged"
					if commits.Load() != 1 || result.Acknowledged() != ack || result.Waiting() != (ack && full) || native != (ack && !full) ||
						ack && err != nil || !ack && !errors.Is(err, fault) || native && claim.Disposition != deliverylifecycle.ClaimAcquired {
						t.Fatalf("native capacity outcome changed: result=%+v native=%+v err=%v commits=%d", result, claim, err, commits.Load())
					}
					id, err := deliverylifecycle.DeliveryID(wanted.ID(), f.route)
					if err != nil {
						t.Fatal(err)
					}
					actual, err := f.store.Snapshot(f.ctx, id)
					if err != nil {
						t.Fatal(err)
					}
					if full || outcome == "rollback_before_lost_ack" {
						if actual.Status != deliverylifecycle.StatusPending || actual.ClaimVersion != 0 || !reflect.DeepEqual(before, profileFixtureSnapshot(t, f)) {
							t.Fatal("full or rolled-back capacity attempt changed complete durable storage")
						}
					} else if actual.Status != deliverylifecycle.StatusInProgress || actual.ClaimVersion != 1 {
						t.Fatalf("physical claim commit was lost: %+v", actual)
					}
					if err := startupownership.CheckAgentRecoverySeed(f.ctx, process); err == nil {
						t.Fatal("native outcome restored its consumed seed")
					}
					if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
						t.Fatalf("native outcome returned before original cleanup joined: %v", err)
					}
					assertAgentProfileProcessNoAttempts(t, f)
				})
			}
		}
	}
}

func TestAgentRecoveryCapacityFinalSlotConsumesOneSeedBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newAgentClaimProfileFixture(t, backend)
			_, _, profile := f.claim(t)
			events := []events.Event{publishAgentProfileRecoveryEvent(t, f), publishAgentProfileRecoveryEvent(t, f)}
			process, _ := agentRecoveryFixtureOwners(t, f)
			bindAgentRecoveryCapacityPolicy(t, process, profile.ProfileID, 2)
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
				t.Fatal(err)
			}
			arrived, release := make(chan struct{}, 2), make(chan struct{})
			remove, err := startupownership.InstallAgentRecoverySeedFaultProbeForTest(process, &startupownership.AgentRecoverySeedFaultProbeForTest{AfterCheckProof: func() {
				arrived <- struct{}{}
				<-release
			}})
			if err != nil {
				t.Fatal(err)
			}
			defer remove()
			type attempt struct {
				index  int
				result startupownership.AgentRecoveryCapacityClaimResult
				err    error
			}
			done := make(chan attempt, 2)
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			joined := 0
			t.Cleanup(func() {
				unblock()
				for joined < 2 {
					<-done
					joined++
				}
			})
			for i, event := range events {
				go func() {
					result, err := startupownership.TryClaimAgentRecoveryDelivery(f.ctx, process, f.authority, event, f.route)
					done <- attempt{i, result, err}
				}()
			}
			awaitAgentRecoverySeedCut(t, arrived)
			awaitAgentRecoverySeedCut(t, arrived)
			unblock()
			acquired, stale, rejected := 0, 0, -1
			for joined < 2 {
				outcome := <-done
				joined++
				claim, native := outcome.result.NativeClaim()
				if outcome.err != nil && !outcome.result.Acknowledged() && !outcome.result.Waiting() && !native {
					stale++
					rejected = outcome.index
				} else if outcome.err == nil && native && claim.Acknowledged && claim.Disposition == deliverylifecycle.ClaimAcquired && !outcome.result.Waiting() {
					acquired++
				} else {
					t.Errorf("final-slot race returned an invented wait or claim: %+v err=%v", outcome.result, outcome.err)
				}
			}
			remove()
			if acquired != 1 || stale != 1 || rejected < 0 {
				t.Fatalf("same seed admitted %d claims, %d stale refusals", acquired, stale)
			}
			assertNativeAgentRecoveryOccupancy(t, f, map[string]int{profile.ProfileID: 2})
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
				t.Fatal(err)
			}
			before := profileFixtureSnapshot(t, f)
			result, err := startupownership.TryClaimAgentRecoveryDelivery(f.ctx, process, f.authority, events[rejected], f.route)
			if err != nil || !result.Waiting() || !result.Acknowledged() || !reflect.DeepEqual(before, profileFixtureSnapshot(t, f)) {
				t.Fatalf("explicit final-slot reseed did not produce a pure wait: %+v err=%v", result, err)
			}
			assertAgentProfileProcessNoAttempts(t, f)
		})
	}
}
