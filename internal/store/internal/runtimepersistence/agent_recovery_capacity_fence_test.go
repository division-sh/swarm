package runtimepersistence

import (
	"database/sql/driver"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

func TestAgentRecoveryCapacityPostFenceWriterRequiresCompleteReseedBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, change := range []string{"plain_claim", "settlement"} {
			t.Run(backend+"/"+change, func(t *testing.T) {
				selected, _, connector := newP16RaceStore(t, backend)
				f := admitAgentClaimProfileFixture(t, testAuthorActivityContext(), selected.(agentFixtureFlowStore))
				_, first, profile := f.claim(t)
				wanted := publishAgentProfileRecoveryEvent(t, f)
				other := publishAgentProfileRecoveryEvent(t, f)
				process, _ := agentRecoveryFixtureOwners(t, f)
				bindAgentRecoveryCapacityPolicy(t, process, profile.ProfileID, 2)
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
					t.Fatal(err)
				}
				firstEntered, firstRelease := make(chan struct{}), make(chan struct{})
				secondEntered, secondRelease := make(chan struct{}), make(chan struct{})
				remove, err := startupownership.InstallAgentRecoverySeedFaultProbeForTest(process, &startupownership.AgentRecoverySeedFaultProbeForTest{AfterCheckProof: func() {
					connector.arm(func(tx driver.Tx) error {
						close(firstEntered)
						<-firstRelease
						return tx.Commit()
					})
				}})
				if err != nil {
					t.Fatal(err)
				}
				defer remove()
				type outcome struct {
					result startupownership.AgentRecoveryCapacityClaimResult
					err    error
				}
				firstDone := make(chan outcome, 1)
				secondDone := make(chan error, 1)
				var firstOnce, secondOnce sync.Once
				unblockFirst := func() { firstOnce.Do(func() { close(firstRelease) }) }
				unblockSecond := func() { secondOnce.Do(func() { close(secondRelease) }) }
				firstJoined, secondStarted, secondJoined := false, false, false
				t.Cleanup(func() {
					unblockFirst()
					unblockSecond()
					if !firstJoined {
						<-firstDone
					}
					if secondStarted && !secondJoined {
						<-secondDone
					}
				})
				go func() {
					result, err := startupownership.TryClaimAgentRecoveryDelivery(f.ctx, process, f.authority, wanted, f.route)
					firstDone <- outcome{result, err}
				}()
				awaitAgentRecoverySeedCut(t, firstEntered)
				connector.arm(func(tx driver.Tx) error {
					close(secondEntered)
					<-secondRelease
					return tx.Commit()
				})
				began := make(chan struct{})
				if backend == "postgres" {
					connector.mu.Lock()
					connector.begin = began
					connector.mu.Unlock()
				}
				secondStarted = true
				go func() {
					if change == "settlement" {
						_, err := f.store.SettleSuccess(f.ctx, first.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection())
						secondDone <- err
						return
					}
					claim, err := f.store.ClaimDelivery(f.ctx, f.authority, other, f.route)
					if err == nil && (!claim.Acknowledged || claim.Disposition != deliverylifecycle.ClaimAcquired) {
						err = deliverylifecycle.ErrConflict
					}
					secondDone <- err
				}()
				if backend == "postgres" {
					awaitAgentRecoverySeedCut(t, began)
				} else {
					awaitCapacitySQLiteWriterQueue(t, f.store.(*SQLiteRuntimeStore))
				}
				select {
				case <-secondEntered:
					t.Fatal("later writer crossed the held original author-activity fence")
				case err := <-secondDone:
					secondJoined = true
					t.Fatalf("later writer returned across the held fence: %v", err)
				default:
				}
				unblockFirst()
				accepted := <-firstDone
				firstJoined = true
				remove()
				claim, native := accepted.result.NativeClaim()
				if accepted.err != nil || !native || !claim.Acknowledged || claim.Disposition != deliverylifecycle.ClaimAcquired || accepted.result.Waiting() {
					t.Fatalf("later writer changed fenced decision: %+v err=%v", accepted.result, accepted.err)
				}
				awaitAgentRecoverySeedCut(t, secondEntered)
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err == nil {
					t.Fatal("post-fence original writer was omitted from seed reconstruction")
				}
				result, err := startupownership.TryClaimAgentRecoveryDelivery(f.ctx, process, f.authority, wanted, f.route)
				_, native = result.NativeClaim()
				if err == nil || result.Waiting() || result.Acknowledged() || native {
					t.Fatalf("post-fence writer allowed stale capacity consumption: %+v err=%v", result, err)
				}
				unblockSecond()
				err = <-secondDone
				secondJoined = true
				if err != nil {
					t.Fatalf("ordered original writer failed: %v", err)
				}
				count := 3 // Ordinary ClaimDelivery remains a separate, unchanged API.
				if change == "settlement" {
					count = 1
				}
				assertNativeAgentRecoveryOccupancy(t, f, map[string]int{profile.ProfileID: count})
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
					t.Fatalf("complete reseed after both original cleanups failed: %v", err)
				}
				if err := startupownership.CheckAgentRecoverySeed(f.ctx, process); err != nil {
					t.Fatal(err)
				}
				assertAgentProfileProcessNoAttempts(t, f)
			})
		}
	}
}

func awaitCapacitySQLiteWriterQueue(t *testing.T, store *SQLiteRuntimeStore) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for store.backend.QueuedWritersForTest() == 0 {
		select {
		case <-timer.C:
			t.Fatal("later SQLite writer did not reach the original coordinator queue")
		case <-poll.C:
		}
	}
}
