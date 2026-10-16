package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/startuprecovery"
	"github.com/google/uuid"
)

func holdAgentRecoverySeedCut(t *testing.T, process startupownership.ProcessCapability, cut string) (<-chan struct{}, func()) {
	t.Helper()
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	hold := func() { close(entered); <-resume }
	probe := &startupownership.AgentRecoverySeedFaultProbeForTest{}
	if cut == "read" {
		probe.InsideRead = hold
	} else if cut == "publication" {
		probe.BeforePublication = hold
	} else {
		t.Fatalf("unsupported seed cut %q", cut)
	}
	remove, err := startupownership.InstallAgentRecoverySeedFaultProbeForTest(process, probe)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(remove)
	t.Cleanup(unblock)
	return entered, func() { unblock(); remove() }
}

func awaitAgentRecoverySeedCut(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("agent recovery did not reach the exact held cut")
	}
}

func TestAgentRecoverySeedReleaseAndTakeoverJoinBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"read", "publication"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				f := newAgentClaimProfileFixture(t, backend)
				f.claim(t)
				process, _ := agentRecoveryFixtureOwners(t, f)
				previous, err := process.Evidence()
				if err != nil {
					t.Fatal(err)
				}
				before := profileFixtureSnapshot(t, f)
				entered, unblock := holdAgentRecoverySeedCut(t, process, cut)
				seeded := make(chan error, 1)
				go func() { seeded <- startupownership.SeedAgentRecovery(f.ctx, process) }()
				awaitAgentRecoverySeedCut(t, entered)
				takeoverCtx, cancelTakeover := context.WithTimeout(f.ctx, 10*time.Second)
				defer cancelTakeover()
				type takeoverResult struct {
					process startupownership.ProcessCapability
					err     error
				}
				takeoverStarted, takeover := make(chan struct{}), make(chan takeoverResult, 1)
				go func() {
					close(takeoverStarted)
					next, err := f.store.(startupownership.Store).AcquireProcessCapability(takeoverCtx, startupownership.AcquireRequest{
						OwnerID: "seed-successor", BootID: uuid.NewString(), RuntimeInstanceID: previous.RuntimeInstanceID,
					})
					takeover <- takeoverResult{next, err}
				}()
				joinedTakeover := false
				t.Cleanup(func() {
					unblock()
					cancelTakeover()
					if !joinedTakeover {
						result := <-takeover
						if result.process != nil {
							if err := result.process.Release(context.WithoutCancel(f.ctx)); err != nil {
								t.Error(err)
							}
						}
					}
				})
				<-takeoverStarted
				select {
				case result := <-takeover:
					takeover <- result
					t.Fatalf("native takeover returned while original seed/possession is held: %v", result.err)
				default:
				}
				releaseStarted, released := make(chan struct{}), make(chan error, 1)
				go func() { close(releaseStarted); released <- process.Release(f.ctx) }()
				<-releaseStarted
				select {
				case err := <-released:
					t.Fatalf("release overtook the exact held seed: %v", err)
				case <-process.Done():
					t.Fatal("release terminalized before its owned read/publication joined")
				default:
				}
				unblock()
				if err := <-seeded; err != nil {
					t.Fatalf("admitted seed failed before joined release: %v", err)
				}
				if err := <-released; err != nil {
					t.Fatal(err)
				}
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err == nil {
					t.Fatal("released process reused its private seed")
				}
				// The takeover requested DURING the cut can finish only after the
				// held operation and native release; no old point proof substitutes.
				result := <-takeover
				joinedTakeover = true
				if result.err != nil {
					t.Fatal(result.err)
				}
				next := result.process
				t.Cleanup(func() {
					if err := next.Release(context.WithoutCancel(f.ctx)); err != nil {
						t.Error(err)
					}
				})
				current, err := next.Evidence()
				if err != nil || current.AuthorityGeneration != previous.AuthorityGeneration+1 || current.AuthorityID == previous.AuthorityID {
					t.Fatalf("takeover lost its exact native generation: %+v err=%v", current, err)
				}
				if err := startupownership.SeedAgentRecovery(f.ctx, next); err != nil {
					t.Fatalf("native successor could not reconstruct its own complete seed: %v", err)
				}
				assertAgentProfileProcessNoAttempts(t, f)
				// Only the canonical takeover generation is allowed to change.
				after := profileFixtureSnapshot(t, f)
				for _, table := range []string{"runtime_process_generation", "runtime_generation_grants"} {
					if _, found := before[table]; !found {
						t.Fatalf("full fingerprint is missing canonical authority table %s", table)
					}
					delete(before, table)
					delete(after, table)
				}
				if !reflect.DeepEqual(before, after) {
					t.Fatal("read/release/takeover seed changed storage outside its native generation/grant facts")
				}
			})
		}
	}
}

func TestAgentRecoverySeedCancellationJoinsLateReturnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"read", "publication"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				f := newAgentClaimProfileFixture(t, backend)
				f.claim(t)
				process, _ := agentRecoveryFixtureOwners(t, f)
				before := profileFixtureSnapshot(t, f)
				entered, unblock := holdAgentRecoverySeedCut(t, process, cut)
				ctx, cancel := context.WithCancel(f.ctx)
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- startupownership.SeedAgentRecovery(ctx, process) }()
				awaitAgentRecoverySeedCut(t, entered)
				cancel()
				select {
				case err := <-result:
					t.Fatalf("cancellation returned before the held read/close joined: %v", err)
				default:
				}
				unblock()
				if err := <-result; !errors.Is(err, context.Canceled) {
					t.Fatalf("late success lost cancellation: %v", err)
				}
				if err := process.ProveCurrent(f.ctx); err != nil {
					t.Fatalf("caller cancellation became possession loss: %v", err)
				}
				// The observed probe is now removed. Prove the failed invocation
				// installed nothing: the ordinary exact owner can seed once.
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
					t.Fatalf("canceled invocation installed a seed or lost possession: %v", err)
				}
				if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
					t.Fatal("cancellation/recovery changed complete durable storage")
				}
				assertAgentProfileProcessNoAttempts(t, f)
			})
		}
	}
}

func TestAgentRecoverySeedRefusesBorrowedInspectionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newAgentClaimProfileFixture(t, backend)
			f.claim(t)
			process, _ := agentRecoveryFixtureOwners(t, f)
			foreign := newAgentClaimProfileFixture(t, backend)
			before := profileFixtureSnapshot(t, f)
			var closed context.Context
			for _, owner := range []agentClaimProfileFixture{f, foreign} {
				if err := owner.store.(startuprecovery.AgentRecoveryReader).InspectSnapshot(owner.ctx, func(snapshot context.Context) error {
					closed = snapshot
					if err := startupownership.SeedAgentRecovery(snapshot, process); err == nil {
						t.Fatal("seed borrowed a caller/foreign inspection cut")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err := startupownership.SeedAgentRecovery(closed, process); err == nil || !strings.Contains(err.Error(), "snapshot") {
				t.Fatalf("closed inspection was accepted: %v", err)
			}
			if err := process.ProveCurrent(f.ctx); err != nil {
				t.Fatalf("rejected context fenced possession: %v", err)
			}
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
				t.Fatal(err)
			}
			if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
				t.Fatal("borrowed context/refusals changed complete storage")
			}
			assertAgentProfileProcessNoAttempts(t, f)
		})
	}
}
