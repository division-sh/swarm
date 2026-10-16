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
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestAgentRecoveryNativeEntryRejectsChangedPossessionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, change := range []string{"cancel", "takeover", "native_loss"} {
			t.Run(backend+"/"+change, func(t *testing.T) {
				f := newAgentClaimProfileFixture(t, backend)
				_, first, _ := f.claim(t)
				wanted := publishAgentProfileRecoveryEvent(t, f)
				process, _ := agentRecoveryFixtureOwners(t, f)
				stamp, err := process.Evidence()
				if err != nil {
					t.Fatal(err)
				}
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(f.ctx)
				defer cancel()
				entered, resume := make(chan struct{}), make(chan struct{})
				seedEntered, seedResume := make(chan struct{}), make(chan struct{})
				var once, seedOnce sync.Once
				unblock := func() { once.Do(func() { close(resume) }) }
				unblockSeed := func() { seedOnce.Do(func() { close(seedResume) }) }
				probe := &startupownership.AgentRecoverySeedFaultProbeForTest{AfterCheckProof: func() { close(entered); <-resume }}
				if change == "native_loss" {
					probe.InsideRead = func() { close(seedEntered); <-seedResume }
				}
				remove, err := startupownership.InstallAgentRecoverySeedFaultProbeForTest(process, probe)
				if err != nil {
					t.Fatal(err)
				}
				collector, removeTransactions, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer removeTransactions()
				type outcome struct {
					result deliverylifecycle.ClaimResult
					err    error
				}
				finished := make(chan outcome, 1)
				go func() {
					result, err := startupownership.ClaimAgentRecoveryDelivery(ctx, process, f.authority, wanted, f.route)
					finished <- outcome{result, err}
				}()
				joined := false
				t.Cleanup(func() {
					unblockSeed()
					unblock()
					remove()
					if !joined {
						<-finished
					}
				})
				awaitAgentRecoverySeedCut(t, entered)
				current := process
				switch change {
				case "cancel":
					cancel()
				case "takeover":
					if err := process.Release(f.ctx); err != nil {
						t.Fatal(err)
					}
					next, err := f.store.(startupownership.Store).AcquireProcessCapability(f.ctx, startupownership.AcquireRequest{
						OwnerID: "claim-entry-successor", BootID: uuid.NewString(), RuntimeInstanceID: stamp.RuntimeInstanceID,
					})
					if err != nil {
						t.Fatal(err)
					}
					current = next
					t.Cleanup(func() {
						if err := next.Release(context.WithoutCancel(f.ctx)); err != nil {
							t.Error(err)
						}
					})
					evidence, err := next.Evidence()
					if err != nil || evidence.AuthorityGeneration != stamp.AuthorityGeneration+1 || evidence.AuthorityID == stamp.AuthorityID {
						t.Fatalf("takeover did not retain exact new generation: %+v err=%v", evidence, err)
					}
					if err := startupownership.CheckAgentRecoverySeed(f.ctx, next); err == nil {
						t.Fatal("successor inherited the old private observation")
					}
				case "native_loss":
					// The fault remains confined to a genuine held Seed read.
					// An exact ordinary settlement first revokes the installed
					// seed, as in the accepted native-lifetime checker controls.
					settled, err := f.store.SettleSuccess(f.ctx, first.Claim, nil, 0, deliverylifecycle.NotApplicableHandlerRuleSelection())
					if err != nil || settled.Status != deliverylifecycle.StatusDelivered {
						t.Fatalf("native loss reconstruction setup: %+v err=%v", settled, err)
					}
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
						t.Fatal(err)
					}
					unblockSeed()
					seedErr := <-seeded
					seedJoined = true
					if seedErr == nil || backend == "sqlite" && !errors.Is(seedErr, sql.ErrConnDone) {
						t.Fatalf("lost native session returned late seed success or lost its original error: %v", seedErr)
					}
				}
				written := profileFixtureSnapshot(t, f)
				unblock()
				actual := <-finished
				joined = true
				remove()
				if actual.err == nil || actual.result.Acknowledged || actual.result.Disposition == deliverylifecycle.ClaimAcquired ||
					change == "cancel" && !errors.Is(actual.err, context.Canceled) {
					t.Fatalf("changed native possession entered checked claim: %+v err=%v", actual.result, actual.err)
				}
				if got := collector.Snapshot().ByOperation[transactiontest.DeliveryClaim].Begun; got != 0 {
					t.Fatalf("refused possession entered %d target claim transactions", got)
				}
				if change == "native_loss" {
					if err := startupownership.SeedAgentRecovery(f.ctx, process); err == nil {
						t.Fatal("native-lost process reconstructed another observation")
					}
				} else {
					if err := current.ProveCurrent(f.ctx); err != nil {
						t.Fatalf("caller refusal lost current native possession: %v", err)
					}
					if change == "takeover" {
						if err := startupownership.SeedAgentRecovery(f.ctx, current); err != nil {
							t.Fatalf("current process complete reseed: %v", err)
						}
					}
					if err := startupownership.CheckAgentRecoverySeed(f.ctx, current); err != nil {
						t.Fatal(err)
					}
				}
				if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(written, after) {
					t.Fatal("entry refusal changed complete post-lifetime-cut storage")
				}
				assertAgentProfileProcessNoAttempts(t, f)
			})
		}
	}
}
