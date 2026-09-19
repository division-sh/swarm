package runtimepersistence

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
)

func awaitGroupProof(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("group operation barrier timed out")
	}
}
func receiveGroupProof(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("group operation did not join")
		return nil
	}
}

func TestFanOutPublicationGroupReleaseRaceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for position := 0; position < 3; position++ {
			for _, winner := range []string{"settlement", "release", "close"} {
				t.Run(fmt.Sprintf("%s/member%d/%s", backend, position, winner), func(t *testing.T) {
					f := newGroupProofFixture(t, backend, 3)
					f.prepare(t)
					f.seal(t)
					f.commit(t)
					before := f.snapshot(t)
					entered, unblock := make(chan struct{}), make(chan struct{})
					var once sync.Once
					block := func() { once.Do(func() { close(entered); <-unblock }) }
					var unblockOnce sync.Once
					releaseBarrier := func() { unblockOnce.Do(func() { close(unblock) }) }
					t.Cleanup(releaseBarrier)
					if winner == "settlement" {
						seen := 0
						f.probe.set(func(phase, q string) error {
							if phase == "before_exec" && strings.Contains(strings.ToLower(q), "insert into event_receipts") {
								if seen == position {
									block()
								}
								seen++
							}
							return nil
						})
					} else if f.postgres {
						state, err := f.raw.(*PostgresStore).pipelinePostgresOwner.PostgresPipelineClaimStateForTest(f.claims[position])
						if err != nil {
							t.Fatal(err)
						}
						state.LeaseForTest().SetBeforeBeginOperationForTest(block)
					} else {
						if err := f.raw.(*SQLiteRuntimeStore).pipelineSQLiteOwner.SetSQLiteClaimOperationHooksForTest(f.claims[position], nil, block); err != nil {
							t.Fatal(err)
						}
					}
					settled := make(chan error, 1)
					released := make(chan error, 1)
					closed := make(chan error, 1)
					settle := func() {
						out, err := f.group.Settle(f.ctx, f.members())
						if err == nil && len(out.Results) != 3 {
							err = errors.New("lost segment outcomes")
						}
						settled <- err
					}
					release := func() { released <- f.store().Release(f.ctx, f.claims[position]) }
					closeGroup := func() { closed <- f.group.Close(f.ctx) }
					// Close does not use the singleton operation hook on SQLite. A release
					// holds the group first while the Close contender queues behind it.
					if winner == "settlement" {
						go settle()
					} else if winner == "close" && f.postgres {
						go closeGroup()
					} else {
						go release()
					}
					awaitGroupProof(t, entered)
					if winner == "settlement" {
						go release()
						go closeGroup()
					} else if winner == "close" && f.postgres {
						go settle()
						go release()
					} else {
						go settle()
						go closeGroup()
					}
					releaseBarrier()
					settleErr, releaseErr, closeErr := receiveGroupProof(t, settled), receiveGroupProof(t, released), receiveGroupProof(t, closed)
					f.probe.set(nil)
					if closeErr != nil {
						t.Fatal(closeErr)
					}
					if winner == "settlement" {
						if settleErr != nil {
							t.Fatal(settleErr)
						}
					} else {
						if settleErr == nil {
							t.Fatal("released/closed member retained settlement authority")
						}
						f.unchanged(t, before)
					}
					if releaseErr != nil && !errors.Is(releaseErr, pipelineobligation.ErrStaleClaim) {
						t.Fatal(releaseErr)
					}
					for _, old := range f.claims {
						current, err := f.store().ClaimPublication(f.ctx, old.EventID())
						if err != nil {
							t.Fatal(err)
						}
						if err := f.store().Release(f.ctx, old); !errors.Is(err, pipelineobligation.ErrStaleClaim) {
							t.Fatalf("old release=%v", err)
						}
						if err := f.group.Close(f.ctx); err != nil {
							t.Fatal(err)
						}
						if err := f.store().Release(f.ctx, current); err != nil {
							t.Fatalf("cleanup stole successor: %v", err)
						}
					}
				})
			}
		}
	}
}

func TestFanOutPublicationGroupSessionPoisonConcurrentCleanupPostgres(t *testing.T) {
	for position := 0; position < 3; position++ {
		t.Run(fmt.Sprintf("member%d", position), func(t *testing.T) {
			f := newGroupProofFixture(t, "postgres", 3)
			selected := f.raw.(*PostgresStore)
			baseline := selected.backend.CapacityReservationsForTest()
			f.prepare(t)
			f.seal(t)
			f.commit(t)
			before := f.snapshot(t)
			state, err := selected.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(f.claims[position])
			if err != nil {
				t.Fatal(err)
			}
			session := state.LeaseForTest().Session()
			entered, unblock := make(chan struct{}), make(chan struct{})
			var once sync.Once
			var unblockOnce sync.Once
			releaseBarrier := func() { unblockOnce.Do(func() { close(unblock) }) }
			t.Cleanup(releaseBarrier)
			f.probe.set(func(phase, q string) error {
				if phase == "before_exec" && strings.Contains(strings.ToLower(q), "insert into event_receipts") {
					once.Do(func() { close(entered); <-unblock })
				}
				return nil
			})
			settled, released, closed := make(chan error, 1), make(chan error, 1), make(chan error, 1)
			go func() { _, err := f.group.Settle(f.ctx, f.members()); settled <- err }()
			awaitGroupProof(t, entered)
			if err := session.ForceDiscard(); err != nil {
				t.Fatal(err)
			}
			go func() { released <- f.store().Release(f.ctx, f.claims[position]) }()
			go func() { closed <- f.group.Close(f.ctx) }()
			releaseBarrier()
			if err := receiveGroupProof(t, settled); err == nil {
				t.Fatal("poisoned session published settlement")
			}
			_ = receiveGroupProof(t, released)
			if err := receiveGroupProof(t, closed); err != nil {
				t.Fatal(err)
			}
			f.probe.set(nil)
			f.unchanged(t, before)
			if got := selected.backend.CapacityReservationsForTest(); got != baseline {
				t.Fatalf("poison retained capacity %d want%d", got, baseline)
			}
			for _, claim := range f.claims {
				if selected.pipelinePostgresOwner.PostgresPipelineClaimsForTest().ContainsEventForTest(claim.EventID()) {
					t.Fatal("poison retained registry member")
				}
			}
		})
	}
}

func TestFanOutPublicationGroupFullCapacityLifetimeBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newGroupProofFixture(t, backend, 32)
			baseline := 0
			if f.postgres {
				baseline = f.raw.(*PostgresStore).backend.CapacityReservationsForTest()
			}
			f.prepare(t)
			if len(f.plans) != 32 || len(f.claims) != 32 {
				t.Fatal("full bounded preparation lost members")
			}
			if f.postgres {
				selected := f.raw.(*PostgresStore)
				var session *postgresbackend.SessionAuthority
				for _, claim := range f.claims {
					state, err := selected.pipelinePostgresOwner.PostgresPipelineClaimStateForTest(claim)
					if err != nil {
						t.Fatal(err)
					}
					if session == nil {
						session = state.LeaseForTest().Session()
					}
					if session != state.LeaseForTest().Session() {
						t.Fatal("adopted another session")
					}
				}
				if selected.backend.CapacityReservationsForTest() != baseline+32 {
					t.Fatal("per-lease capacity not retained")
				}
			}
			f.seal(t)
			f.commit(t)
			out, err := f.group.Settle(f.ctx, f.members()[:16])
			if err != nil || len(out.Results) != 16 {
				t.Fatalf("first segment=%+v %v", out, err)
			}
			for _, row := range out.Results {
				if !row.Outcome.DeliveryHandoffCommitted() {
					t.Fatal("claim retired before mandatory handoff")
				}
			}
			if f.postgres && f.raw.(*PostgresStore).backend.CapacityReservationsForTest() != baseline+16 {
				t.Fatal("segment prematurely retired sibling capacity")
			}
			out, err = f.group.Settle(f.ctx, f.members()[16:])
			if err != nil || len(out.Results) != 16 {
				t.Fatalf("second segment=%+v %v", out, err)
			}
			if err := f.group.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if f.postgres && f.raw.(*PostgresStore).backend.CapacityReservationsForTest() != baseline {
				t.Fatal("full group capacity leaked")
			}
		})
	}
}

func TestFanOutPublicationGroupPendingParentContenderPostgres(t *testing.T) {
	f := newGroupProofFixture(t, "postgres", 1)
	selected := f.raw.(*PostgresStore)
	registry := selected.pipelinePostgresOwner.PostgresPipelineClaimsForTest()
	entered, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var unblockOnce sync.Once
	releaseBarrier := func() { unblockOnce.Do(func() { close(unblock) }) }
	t.Cleanup(releaseBarrier)
	registry.SetHooksForTest(nil, nil, func(*postgresbackend.AdvisoryLockLease) { once.Do(func() { close(entered); <-unblock }) })
	t.Cleanup(func() { registry.SetHooksForTest(nil, nil, nil) })
	done := make(chan error, 1)
	go func() { _, err := f.group.Claim(f.ctx, 0, f.events[0]); done <- err }()
	awaitGroupProof(t, entered)
	ctx, cancel := context.WithTimeout(f.ctx, 300*time.Millisecond)
	defer cancel()
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = selected.pipelinePostgresOwner.TerminalizePipelineObligationTx(ctx, tx, nil, f.events[0].ID(), pipelineobligation.DeadLetter("run_stopped", nil), time.Now())
	_ = tx.Rollback()
	releaseBarrier()
	if claimErr := receiveGroupProof(t, done); claimErr != nil {
		t.Fatal(claimErr)
	}
	if !errors.Is(err, pipelineobligation.ErrBusy) {
		t.Fatalf("parent must refuse reserved exact event before waiting with registry: %v", err)
	}
}

func TestFanOutPublicationGroupIndependentRunOverlapBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			first := newGroupProofFixture(t, backend, 1)
			second := newGroupProofFixtureOn(t, backend, 1, first)
			first.prepare(t)
			first.seal(t)
			first.commit(t)
			entered, unblock := make(chan struct{}), make(chan struct{})
			var unblockOnce sync.Once
			releaseBarrier := func() { unblockOnce.Do(func() { close(unblock) }) }
			t.Cleanup(releaseBarrier)
			var mu sync.Mutex
			blocked := false
			first.probe.set(func(phase, q string) error {
				if phase == "before_exec" && strings.Contains(strings.ToLower(q), "insert into event_receipts") {
					mu.Lock()
					hold := !blocked
					blocked = true
					mu.Unlock()
					if hold {
						close(entered)
						<-unblock
					}
				}
				return nil
			})
			firstDone, secondDone := make(chan error, 1), make(chan error, 1)
			go func() { _, err := first.group.Settle(first.ctx, first.members()); firstDone <- err }()
			awaitGroupProof(t, entered)
			prepared := make(chan struct{})
			go func() { defer close(prepared); second.prepare(t) }()
			awaitGroupProof(t, prepared)
			if len(second.claims) != 1 {
				t.Fatal("independent preparation failed")
			}
			second.seal(t)
			go func() { _, err := second.owner.CommitFanOutChunk(second.ctx, second.command); secondDone <- err }()
			// Both dialects retain the existing generation/story mutation order.
			// Independent groups and preparation overlap, not these SQL writers.
			select {
			case err := <-secondDone:
				t.Fatalf("writer bypassed existing mutation order: %v", err)
			case <-time.After(50 * time.Millisecond):
			}
			releaseBarrier()
			if err := receiveGroupProof(t, firstDone); err != nil {
				t.Fatal(err)
			}
			if err := receiveGroupProof(t, secondDone); err != nil {
				t.Fatal(err)
			}
			if _, err := second.group.Settle(second.ctx, second.members()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFanOutPublicationGroupConnectionWaitOutsideRegistryPostgres(t *testing.T) {
	f := newGroupProofFixture(t, "postgres", 1)
	if err := f.group.Close(f.ctx); err != nil {
		t.Fatal(err)
	}
	selected := f.raw.(*PostgresStore)
	registry := selected.pipelinePostgresOwner.PostgresPipelineClaimsForTest()
	// Exhaust the selected pool at constructor entry. No process registry or
	// group registration can be held while waiting for its required connection.
	inUse := f.db.Stats().InUse
	f.db.SetMaxOpenConns(inUse + 1)
	held, err := f.db.Conn(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Close(); f.db.SetMaxOpenConns(16) })
	baseline := f.db.Stats().WaitCount
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		group, err := f.owner.BeginFanOutPublicationGroup(ctx, f.claim)
		if group != nil {
			_ = group.Close(context.Background())
		}
		done <- err
	}()
	deadline := time.Now().Add(3 * time.Second)
	for f.db.Stats().WaitCount == baseline {
		if time.Now().After(deadline) {
			t.Fatal("constructor did not wait for pool")
		}
		time.Sleep(time.Millisecond)
	}
	free := make(chan struct{})
	go func() { unlock := registry.LockForTest(); unlock(); close(free) }()
	awaitGroupProof(t, free)
	cancel()
	if err := receiveGroupProof(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("constructor cancellation=%v", err)
	}
	if registry.ClaimCountForTest() != 0 {
		t.Fatal("waiting constructor registered members")
	}
	_ = held.Close()
	f.db.SetMaxOpenConns(16)
	group, err := f.owner.BeginFanOutPublicationGroup(f.ctx, f.claim)
	if err != nil {
		t.Fatalf("canceled constructor left registered group: %v", err)
	}
	if err := group.Close(f.ctx); err != nil {
		t.Fatal(err)
	}
}

func TestFanOutPublicationGroupPrivateSessionWaitOutsideRegistryPostgres(t *testing.T) {
	f := newGroupProofFixture(t, "postgres", 1)
	if err := f.group.Close(f.ctx); err != nil {
		t.Fatal(err)
	}
	registry := f.raw.(*PostgresStore).pipelinePostgresOwner.PostgresPipelineClaimsForTest()
	entered, unblock := make(chan struct{}), make(chan struct{})
	var unblocked sync.Once
	releaseBarrier := func() { unblocked.Do(func() { close(unblock) }) }
	t.Cleanup(releaseBarrier)
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	var readCommitted atomic.Bool
	var captured atomic.Bool
	f.probe.set(func(phase, q string) error {
		if phase == "after_commit" {
			readCommitted.Store(true)
		}
		if phase == "reset_session" && readCommitted.Load() && captured.CompareAndSwap(false, true) {
			close(entered)
			<-unblock
			return ctx.Err()
		}
		return nil
	})
	done := make(chan error, 1)
	go func() {
		group, err := f.owner.BeginFanOutPublicationGroup(ctx, f.claim)
		if group != nil {
			_ = group.Close(context.Background())
		}
		done <- err
	}()
	awaitGroupProof(t, entered)
	available := make(chan struct{})
	go func() { unlock := registry.LockForTest(); unlock(); close(available) }()
	awaitGroupProof(t, available)
	cancel()
	releaseBarrier()
	if err := receiveGroupProof(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("private session acquisition cancellation=%v", err)
	}
	f.probe.set(nil)
	if registry.ClaimCountForTest() != 0 {
		t.Fatal("private acquisition registered members while waiting")
	}
	group, err := f.owner.BeginFanOutPublicationGroup(f.ctx, f.claim)
	if err != nil {
		t.Fatalf("private session failure leaked group registration: %v", err)
	}
	if err := group.Close(f.ctx); err != nil {
		t.Fatal(err)
	}
}

func TestFanOutPublicationGroupShutdownRaceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, winner := range []string{"shutdown", "settlement"} {
			for position := 0; position < 3; position++ {
				t.Run(fmt.Sprintf("%s/%s/member%d", backend, winner, position), func(t *testing.T) {
					f := newGroupProofFixture(t, backend, 3)
					f.prepare(t)
					f.seal(t)
					f.commit(t)
					before := f.snapshot(t)
					if winner == "shutdown" {
						if err := f.process.Release(f.ctx); err != nil {
							t.Fatal(err)
						}
						if _, err := f.group.Settle(f.ctx, f.members()); err == nil {
							t.Fatal("retired process authorized settlement")
						}
						f.unchanged(t, before)
					} else {
						entered, unblock := make(chan struct{}), make(chan struct{})
						seen := 0
						var unblocked sync.Once
						releaseBarrier := func() { unblocked.Do(func() { close(unblock) }) }
						t.Cleanup(releaseBarrier)
						f.probe.set(func(phase, q string) error {
							if phase == "before_exec" && strings.Contains(strings.ToLower(q), "insert into event_receipts") {
								if seen == position {
									close(entered)
									<-unblock
								}
								seen++
							}
							return nil
						})
						settled, shutdown := make(chan error, 1), make(chan error, 1)
						go func() { _, err := f.group.Settle(f.ctx, f.members()); settled <- err }()
						awaitGroupProof(t, entered)
						go func() { shutdown <- f.process.Release(f.ctx) }()
						releaseBarrier()
						if err := receiveGroupProof(t, settled); err != nil {
							t.Fatal(err)
						}
						if err := receiveGroupProof(t, shutdown); err != nil {
							t.Fatal(err)
						}
						f.probe.set(nil)
					}
					if err := f.group.ValidateCommittedMembership(f.claims); err != nil {
						t.Fatal(err)
					}
					if err := f.group.Close(context.Background()); err != nil {
						t.Fatal(err)
					}
					if err := f.group.Close(context.Background()); err != nil {
						t.Fatal(err)
					}
					for _, claim := range f.claims {
						if err := f.store().Release(f.ctx, claim); !errors.Is(err, pipelineobligation.ErrStaleClaim) {
							t.Fatalf("retired claim=%v", err)
						}
					}
				})
			}
		}
	}
}

func TestFanOutPublicationGroupCleanupFailureIdempotencyBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for position := 0; position < 3; position++ {
			t.Run(fmt.Sprintf("%s/member%d", backend, position), func(t *testing.T) {
				f := newGroupProofFixture(t, backend, 3)
				baseline := 0
				if f.postgres {
					baseline = f.raw.(*PostgresStore).backend.CapacityReservationsForTest()
				}
				f.prepare(t)
				f.seal(t)
				f.commit(t)
				before := f.snapshot(t)
				fault := errors.New("exact cleanup retirement failed")
				if f.postgres {
					state, err := f.raw.(*PostgresStore).pipelinePostgresOwner.PostgresPipelineClaimStateForTest(f.claims[position])
					if err != nil {
						t.Fatal(err)
					}
					state.LeaseForTest().SetUnlockForTest(func(context.Context, *postgresbackend.SessionAuthority, string) (bool, error) { return false, fault })
				} else {
					calls := 0
					f.raw.(*SQLiteRuntimeStore).pipelineSQLiteOwner.SetPipelineReleaseErrorForTest(func() error {
						calls++
						if calls == position+1 {
							return fault
						}
						return nil
					})
					t.Cleanup(func() { f.raw.(*SQLiteRuntimeStore).pipelineSQLiteOwner.SetPipelineReleaseErrorForTest(nil) })
				}
				if err := f.group.Close(f.ctx); !errors.Is(err, fault) {
					t.Fatalf("cleanup failure lost: %v", err)
				}
				if err := f.group.Close(f.ctx); err != nil {
					t.Fatalf("idempotent close repeated retirement: %v", err)
				}
				f.unchanged(t, before)
				if !f.postgres {
					f.raw.(*SQLiteRuntimeStore).pipelineSQLiteOwner.SetPipelineReleaseErrorForTest(nil)
				}
				if f.postgres && f.raw.(*PostgresStore).backend.CapacityReservationsForTest() != baseline {
					t.Fatal("failed exact cleanup leaked capacity")
				}
				for _, old := range f.claims {
					current, err := f.store().ClaimPublication(f.ctx, old.EventID())
					if err != nil {
						t.Fatal(err)
					}
					if err := f.store().Release(f.ctx, old); !errors.Is(err, pipelineobligation.ErrStaleClaim) {
						t.Fatal(err)
					}
					if err := f.store().Release(f.ctx, current); err != nil {
						t.Fatalf("cleanup stole successor: %v", err)
					}
				}
			})
		}
	}
}

func TestFanOutPublicationGroupPermitThroughMandatoryHandoffBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newGroupProofFixture(t, backend, 3)
			f.prepare(t)
			f.seal(t)
			f.commit(t)
			workers := 1
			r, err := startupownership.RegisterFanOutServing(f.ctx, f.grant, f.occurrence, &workers)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(r.Close)
			permit, found, err := r.BeginTurn(f.ctx)
			if err != nil || !found {
				t.Fatalf("permit=%v %v", found, err)
			}
			t.Cleanup(permit.Done)
			entered, unblock := make(chan struct{}), make(chan struct{})
			var commitHeld atomic.Bool
			var unblocked sync.Once
			releaseBarrier := func() { unblocked.Do(func() { close(unblock) }) }
			t.Cleanup(releaseBarrier)
			f.probe.set(func(phase, q string) error {
				if phase == "after_write_commit" && commitHeld.CompareAndSwap(false, true) {
					close(entered)
					<-unblock
				}
				return nil
			})
			done := make(chan error, 1)
			go func() {
				out, err := f.group.Settle(permit.Context(), f.members())
				if err == nil {
					for _, row := range out.Results {
						if !row.Outcome.DeliveryHandoffCommitted() {
							err = errors.New("missing mandatory handoff")
						}
					}
				}
				permit.Done()
				done <- err
			}()
			awaitGroupProof(t, entered)
			if next, found, err := r.BeginTurn(f.ctx); err != nil || found {
				if next != nil {
					next.Done()
				}
				t.Fatalf("capacity released before acknowledgement/handoff: %v %v", found, err)
			}
			releaseBarrier()
			if err := receiveGroupProof(t, done); err != nil {
				t.Fatal(err)
			}
			f.probe.set(nil)
			next, found, err := r.BeginTurn(f.ctx)
			if err != nil || !found {
				t.Fatalf("completed handoff stranded permit: %v %v", found, err)
			}
			next.Done()
		})
	}
}

func TestFanOutPublicationGroupResetDrainBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, winner := range []string{"reset-drain", "settlement"} {
			for position := 0; position < 3; position++ {
				t.Run(fmt.Sprintf("%s/%s/member%d", backend, winner, position), func(t *testing.T) {
					f := newGroupProofFixture(t, backend, 3)
					f.prepare(t)
					f.seal(t)
					f.commit(t)
					if winner == "settlement" {
						entered, unblock := make(chan struct{}), make(chan struct{})
						seen := 0
						var unblocked sync.Once
						releaseBarrier := func() { unblocked.Do(func() { close(unblock) }) }
						t.Cleanup(releaseBarrier)
						f.probe.set(func(phase, q string) error {
							if phase == "before_exec" && strings.Contains(strings.ToLower(q), "insert into event_receipts") {
								if seen == position {
									close(entered)
									<-unblock
								}
								seen++
							}
							return nil
						})
						settled, drained := make(chan error, 1), make(chan error, 1)
						go func() { _, err := f.group.Settle(f.ctx, f.members()); settled <- err }()
						awaitGroupProof(t, entered)
						go func() { drained <- f.group.Close(f.ctx) }()
						releaseBarrier()
						if err := receiveGroupProof(t, settled); err != nil {
							t.Fatal(err)
						}
						if err := receiveGroupProof(t, drained); err != nil {
							t.Fatal(err)
						}
						f.probe.set(nil)
					} else {
						if err := f.group.Close(f.ctx); err != nil {
							t.Fatal(err)
						}
						if _, err := f.group.Settle(f.ctx, f.members()); err == nil {
							t.Fatal("reset-drained group still settled")
						}
					}
					// Production reset drains accepted work before cleanup. Exercise that
					// ordering with the real retained reset operation, not a SQL deletion.
					request := admitRetainedResetCleanupProof(t, f.process, f.raw.(destructivereset.QuiescenceStore), f.seed.runID, false)
					if _, err := f.process.ApplyDestructiveResetCleanup(f.ctx, request, nil); err != nil {
						t.Fatal(err)
					}
					var count int
					if err := f.db.QueryRow(`SELECT COUNT(*) FROM runs WHERE run_id=$1`, f.seed.runID).Scan(&count); err != nil || count != 0 {
						t.Fatalf("reset did not delete exact run: %d %v", count, err)
					}
					if err := f.group.ValidateCommittedMembership(f.claims); err != nil {
						t.Fatal(err)
					}
					if err := f.group.Close(f.ctx); err != nil {
						t.Fatal(err)
					}
					for _, old := range f.claims {
						current, err := f.store().ClaimPublication(f.ctx, old.EventID())
						if err != nil {
							t.Fatal(err)
						}
						if err := f.store().Release(f.ctx, old); !errors.Is(err, pipelineobligation.ErrStaleClaim) {
							t.Fatal(err)
						}
						if err := f.store().Release(f.ctx, current); err != nil {
							t.Fatalf("post-reset cleanup stole new capability: %v", err)
						}
					}
				})
			}
		}
	}
}
