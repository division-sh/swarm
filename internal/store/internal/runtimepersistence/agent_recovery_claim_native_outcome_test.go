package runtimepersistence

import (
	"database/sql/driver"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

func TestAgentRecoveryNativeEntryPreservesPhysicalOutcomeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, outcome := range []string{"acknowledged", "rollback_before_lost_ack", "commit_before_lost_ack"} {
			t.Run(backend+"/"+outcome, func(t *testing.T) {
				selected, _, connector := newP16RaceStore(t, backend)
				f := admitAgentClaimProfileFixture(t, testAuthorActivityContext(), selected.(agentFixtureFlowStore))
				f.claim(t)
				wanted := publishAgentProfileRecoveryEvent(t, f)
				process, _ := agentRecoveryFixtureOwners(t, f)
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
					t.Fatal(err)
				}
				before := profileFixtureSnapshot(t, f)
				fault := errors.New("original physical claim COMMIT acknowledgement unavailable")
				var calls atomic.Int32
				result, err := claimAgentRecoveryHandoffCandidate(f.ctx, process, f, wanted, func() {
					connector.arm(func(tx driver.Tx) error {
						calls.Add(1)
						if outcome == "rollback_before_lost_ack" {
							return errors.Join(fault, tx.Rollback())
						}
						commitErr := tx.Commit()
						if outcome == "commit_before_lost_ack" {
							return errors.Join(fault, commitErr)
						}
						return commitErr
					})
				})
				if calls.Load() != 1 || result.Acknowledged != (outcome == "acknowledged") ||
					outcome == "acknowledged" && (err != nil || result.Disposition != deliverylifecycle.ClaimAcquired) ||
					outcome != "acknowledged" && (!errors.Is(err, fault) || result.Disposition == deliverylifecycle.ClaimAcquired) {
					t.Fatalf("native ACK/outcome changed: %+v err=%v physical_calls=%d", result, err, calls.Load())
				}
				id, err := deliverylifecycle.DeliveryID(wanted.ID(), f.route)
				if err != nil {
					t.Fatal(err)
				}
				actual, err := f.store.Snapshot(f.ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				if outcome == "rollback_before_lost_ack" {
					if actual.Status != deliverylifecycle.StatusPending || actual.ClaimVersion != 0 || !reflect.DeepEqual(before, profileFixtureSnapshot(t, f)) {
						t.Fatal("actual physical rollback changed the pending claim or complete storage")
					}
				} else {
					if actual.Status != deliverylifecycle.StatusInProgress || actual.ClaimVersion != 1 {
						t.Fatalf("real physical COMMIT missing exact acquired row: %+v", actual)
					}
					profile, err := ReadFirstAgentClaimProfileForTest(f.ctx, f.store, actual)
					if err != nil || !profile.MatchesFirstAdmission(wanted, f.route, f.authority) {
						t.Fatalf("physical COMMIT missing immutable first admission: %+v err=%v", profile, err)
					}
				}
				if err := startupownership.CheckAgentRecoverySeed(f.ctx, process); err == nil {
					t.Fatal("native outcome implicitly reinstated its revoked observation")
				}
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
					t.Fatalf("original native cleanup did not join before return: %v", err)
				}
				if err := startupownership.CheckAgentRecoverySeed(f.ctx, process); err != nil {
					t.Fatalf("complete reconstruction after native outcome: %v", err)
				}
				assertAgentProfileProcessNoAttempts(t, f)
			})
		}
	}
}

func TestAgentRecoveryNativeEntryRejectsHeldOriginalWriterBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected, _, connector := newP16RaceStore(t, backend)
			f := admitAgentClaimProfileFixture(t, testAuthorActivityContext(), selected.(agentFixtureFlowStore))
			f.claim(t)
			wanted := publishAgentProfileRecoveryEvent(t, f)
			competing := publishAgentProfileRecoveryEvent(t, f)
			process, _ := agentRecoveryFixtureOwners(t, f)
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
				t.Fatal(err)
			}
			before := profileFixtureSnapshot(t, f)
			entered, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(resume) }) }
			fault := errors.New("held original writer rolls back before lost ACK")
			finished := make(chan error, 1)
			started := false
			joined := false
			t.Cleanup(func() {
				unblock()
				if started && !joined {
					<-finished
				}
			})
			result, err := claimAgentRecoveryHandoffCandidate(f.ctx, process, f, wanted, func() {
				connector.arm(func(tx driver.Tx) error {
					close(entered)
					<-resume
					return errors.Join(fault, tx.Rollback())
				})
				started = true
				go func() {
					_, err := f.store.ClaimDelivery(f.ctx, f.authority, competing, f.route)
					finished <- err
				}()
				awaitAgentRecoverySeedCut(t, entered)
			})
			if err == nil || result.Acknowledged || result.Disposition == deliverylifecycle.ClaimAcquired {
				t.Fatalf("entry consumed a seed while its original writer was held: %+v err=%v", result, err)
			}
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err == nil {
				t.Fatal("held native writer allowed explicit reconstruction before original cleanup")
			}
			select {
			case err := <-finished:
				joined = true
				t.Fatalf("original native writer returned before held physical outcome: %v", err)
			default:
			}
			unblock()
			err = <-finished
			joined = true
			if !errors.Is(err, fault) {
				t.Fatalf("original physical rollback lost its failure: %v", err)
			}
			for _, event := range []string{wanted.ID(), competing.ID()} {
				id, err := deliverylifecycle.DeliveryID(event, f.route)
				if err != nil {
					t.Fatal(err)
				}
				actual, err := f.store.Snapshot(f.ctx, id)
				if err != nil || actual.Status != deliverylifecycle.StatusPending || actual.ClaimVersion != 0 {
					t.Fatalf("held-writer cut changed exact pending row: %+v err=%v", actual, err)
				}
			}
			if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
				t.Fatal("held original rollback and refused entry changed complete storage")
			}
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
				t.Fatalf("original cleanup did not release writer freshness: %v", err)
			}
			accepted, err := startupownership.ClaimAgentRecoveryDelivery(f.ctx, process, f.authority, wanted, f.route)
			if err != nil || !accepted.Acknowledged || accepted.Disposition != deliverylifecycle.ClaimAcquired {
				t.Fatalf("explicit complete reconstruction did not admit a fresh invocation: %+v err=%v", accepted, err)
			}
			assertAgentProfileProcessNoAttempts(t, f)
		})
	}
}
