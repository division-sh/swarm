//go:build linux || darwin

package sessionprovider

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/google/uuid"
	waStore "go.mau.fi/whatsmeow/store"
)

func TestWhatsAppRuntimeLogoutPreparationUsesOriginalSDKBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			c := openRuntimeConnectionFixture(t, f)
			if _, err := c.PrepareSessionLogout(f.ctx, f.operation); err == nil {
				t.Fatal("unconnected retained state granted a logout target")
			}
			connectRuntimeConnectionFixture(t, f, c)
			original := c.state.currentOccurrence()
			target, err := c.PrepareSessionLogout(f.ctx, f.operation)
			if err != nil || !target.MatchesOperation(f.operation) || target.OccurrenceID != original.occurrenceID || target.Account != f.operation.SessionAccount {
				t.Fatalf("original SDK preparation lost exact evidence: %+v, %v", target, err)
			}
			for _, mutation := range []struct {
				name string
				edit func(*channelonboarding.Operation)
			}{
				{"operation", func(o *channelonboarding.Operation) { o.OperationID = uuid.NewString() }},
				{"revision", func(o *channelonboarding.Operation) { o.Revision++ }},
				{"principal", func(o *channelonboarding.Operation) { o.PrincipalID = uuid.NewString() }},
				{"interface", func(o *channelonboarding.Operation) { o.Interface.ChannelPackVersion = "9.0.0" }},
				{"connection", func(o *channelonboarding.Operation) { o.SessionConnectionID = uuid.NewString() }},
				{"account", func(o *channelonboarding.Operation) { o.SessionAccount.AdmissionID = uuid.NewString() }},
				{"source", func(o *channelonboarding.Operation) { o.Coordinate.BundleIdentity += "-other" }},
				{"runtime", func(o *channelonboarding.Operation) { o.Coordinate.RuntimeInstanceID = uuid.NewString() }},
				{"target", func(o *channelonboarding.Operation) { o.Coordinate.TargetGeneration++ }},
				{"selector", func(o *channelonboarding.Operation) { o.TargetSelector += "-other" }},
			} {
				t.Run(mutation.name, func(t *testing.T) {
					foreign := f.operation
					mutation.edit(&foreign)
					if _, err := c.PrepareSessionLogout(f.ctx, foreign); err == nil {
						t.Fatal("foreign caller selected the original SDK destruction target")
					}
				})
			}
			ctx, cancel := context.WithCancel(f.ctx)
			cancel()
			if _, err := c.PrepareSessionLogout(ctx, f.operation); err == nil {
				t.Fatal("canceled preparation disclosed a target")
			}
			if original.logoutReserved || original.client.Store.Deleted || !original.client.IsConnected() {
				t.Fatal("metadata preparation reserved, destroyed or replaced the connection")
			}
			rows, err := f.selected.ListChannelTeardowns(f.ctx)
			if err != nil || len(rows) != 0 {
				t.Fatal("metadata preparation manufactured durable destruction authority", err)
			}
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			if err := c.Close(closeCtx); err != nil {
				t.Fatal(err)
			}
			if _, err := c.PrepareSessionLogout(f.ctx, f.operation); err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("closed owner prepared a new logout target", err)
			}
		})
	}
}

type logoutDeletionFaultFixture struct {
	waStore.DeviceContainer
	cause  error
	called bool
}

type logoutSettlementObservationFixture struct {
	effects.Store
	results chan error
}

func (f *logoutSettlementObservationFixture) SettleExternalAttempt(ctx context.Context, s effects.Settlement) error {
	err := f.Store.SettleExternalAttempt(ctx, s)
	f.results <- err
	return err
}

func (f *logoutDeletionFaultFixture) DeleteDevice(context.Context, *waStore.Device) error {
	f.called = true
	return f.cause
}

func TestSessionLogoutJournalDoesNotTreatLocalDeleteFailureAsNoEffectBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			c := openRuntimeConnectionFixture(t, f)
			original := c.state.currentOccurrence()
			fault := &logoutDeletionFaultFixture{DeviceContainer: original.stores.container, cause: errors.New("exact private deletion fixture refusal")}
			original.stores.container = fault
			connectRuntimeConnectionFixture(t, f, c)
			op := reserveRuntimeLogoutFixture(t, f, c)
			ctx := logoutJournalFixture(t, f, op)
			done := make(chan error, 1)
			go func() { done <- c.DispatchSessionLogout(ctx, op) }()
			frame := f.peer.next(t)
			if frame.node.Tag != "iq" || frame.node.Attrs["xmlns"] != "md" {
				t.Fatal("local deletion proof did not reach actual remote unlink", frame.node)
			}
			f.peer.acknowledge(t, frame)
			if err := awaitOccurrenceProbe(t, f.peer.ctx, done); !errors.Is(err, fault.cause) || !fault.called || original.client.Store.Deleted {
				t.Fatal("remote success/local failure lost evidence or deleted pairing", err, fault.called)
			}
			identity, _ := effects.ChannelLogoutOperationID(op.TeardownID)
			outcome, found, err := f.selected.(effects.OutcomeStore).GetExternalEffectOutcome(ctx, identity)
			if err != nil || !found || outcome.AttemptState != effects.StateOutcomeUncertain {
				t.Fatal("local deletion error was misclassified as no effect", outcome, err)
			}
			retained, err := f.selected.GetChannelTeardown(ctx, op.TeardownID)
			if err != nil || retained.Phase != channelonboarding.TeardownFailed || retained.FailureCode != "logout_outcome_uncertain" || *retained.Logout != *op.Logout {
				t.Fatal("uncertain logout lost exact frozen responsibility", retained, err)
			}
			if err := c.DispatchSessionLogout(ctx, op); err == nil {
				t.Fatal("local deletion failure authorized another remote unlink")
			}
			select {
			case extra := <-f.peer.frames:
				t.Fatal("local deletion failure replayed remote unlink", extra.node)
			default:
			}
		})
	}
}

func TestSessionLogoutJournalCanceledAfterLaunchRemainsUncertainBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			c := openRuntimeConnectionFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			original := c.state.currentOccurrence()
			op := reserveRuntimeLogoutFixture(t, f, c)
			ctx := logoutJournalFixture(t, f, op)
			observed := &logoutSettlementObservationFixture{Store: f.selected.(effects.Store), results: make(chan error, 1)}
			ctx = effects.WithController(ctx, effects.NewController(observed).WithExecutionPosture(executionposture.Live))
			caller, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- c.DispatchSessionLogout(caller, op) }()
			frame := f.peer.next(t)
			if frame.node.Tag != "iq" || frame.node.Attrs["xmlns"] != "md" {
				t.Fatal("cancellation proof did not reach actual unlink", frame.node)
			}
			cancel()
			if err := awaitOccurrenceProbe(t, f.peer.ctx, done); !errors.Is(err, context.Canceled) {
				t.Fatal("postlaunch cancellation did not release caller wait", err)
			}
			wait, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			if _, err := f.workOwner.RetireAndWait(wait); err != nil {
				t.Fatal("cancellation abandoned journal settlement or original cleanup", err)
			}
			select {
			case err := <-observed.results:
				if err != nil {
					t.Fatal("original counted logout tail could not settle after cancellation/retirement", err)
				}
			default:
				t.Fatal("original counted logout tail did not attempt settlement")
			}
			identity, _ := effects.ChannelLogoutOperationID(op.TeardownID)
			outcome, found, err := f.selected.(effects.OutcomeStore).GetExternalEffectOutcome(wait, identity)
			if err != nil || !found || outcome.AttemptState != effects.StateOutcomeUncertain || original.client.Store.Deleted {
				t.Fatal("canceled launched unlink became no-effect/success or deleted pairing", outcome, err)
			}
			retained, err := f.selected.GetChannelTeardown(wait, op.TeardownID)
			if err != nil || retained.Phase != channelonboarding.TeardownFailed || retained.FailureCode != "logout_outcome_uncertain" {
				t.Fatal("canceled unlink lost truthful readback", retained, err)
			}
		})
	}
}

func TestSessionLogoutRequiresAcceptedProcessTailBeforeAuthorizationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			c := openRuntimeConnectionFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			original := c.state.currentOccurrence()
			op := reserveRuntimeLogoutFixture(t, f, c)
			ctx := logoutJournalFixture(t, f, op)
			process, ok := worklifetime.ProcessFromContext(c.ctx)
			if !ok {
				t.Fatal("original connection lost its process owner")
			}
			before := process.ActiveCount()
			if err := process.Fence(); err != nil {
				t.Fatal(err)
			}
			if err := c.DispatchSessionLogout(ctx, op); !errors.Is(err, worklifetime.ErrAdmissionFenced) {
				t.Fatal("logout did not refuse unowned settlement before authorization", err)
			}
			identity, _ := effects.ChannelLogoutOperationID(op.TeardownID)
			if _, found, err := f.selected.(effects.OutcomeStore).GetExternalEffectOutcome(ctx, identity); err != nil || found {
				t.Fatal("fenced process authorized a logout attempt", err, found)
			}
			retained, err := f.selected.GetChannelTeardown(ctx, op.TeardownID)
			if err != nil || retained.Phase != op.Phase || retained.Revision != op.Revision || original.client.Store.Deleted || process.ActiveCount() != before {
				t.Fatal("failed tail admission changed responsibility, pairing or work accounting", retained, err)
			}
			select {
			case frame := <-f.peer.frames:
				t.Fatal("fenced process sent an unlink", frame.node)
			default:
			}
		})
	}
}

func reserveRuntimeLogoutFixture(t *testing.T, f *activeInputFixture, c *RuntimeConnection) channelonboarding.TeardownOperation {
	t.Helper()
	target, err := c.PrepareSessionLogout(f.ctx, f.operation)
	if err != nil {
		t.Fatal(err)
	}
	op, err := f.selected.ReserveChannelTeardown(f.ctx, channelonboarding.ReserveTeardownRequest{
		TeardownID: uuid.NewString(), RequestKeyHash: uuid.NewString(), RequestHash: uuid.NewString(),
		Kind: channelonboarding.TeardownLogout, PrincipalID: f.operation.PrincipalID,
		Scope: channelonboarding.TeardownScope{Interface: f.operation.Interface}, Logout: &target, RequestedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func logoutJournalFixture(t *testing.T, f *activeInputFixture, op channelonboarding.TeardownOperation) context.Context {
	t.Helper()
	raw, err := json.Marshal(op.Logout)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := effects.ChannelLogoutOperationID(op.TeardownID)
	if err != nil {
		t.Fatal(err)
	}
	a := effects.Authority{Kind: effects.AuthorityChannelLogout, ID: identity,
		ExecutionOwner: "channel-logout:" + op.Logout.OccurrenceID, LeaseExpiresAt: time.Now().UTC().Add(time.Minute),
		FenceGeneration: uint64(op.Revision), ExecutionMode: effects.ExecutionModeLive,
		ChannelLogout: effects.ChannelLogoutAuthority{EffectOperationID: identity, TeardownID: op.TeardownID,
			TeardownRevision: op.Revision, PrincipalID: op.PrincipalID, BundleHash: op.Logout.Coordinate.BundleHash,
			RuntimeInstanceID: op.Logout.Coordinate.RuntimeInstanceID, TargetFingerprint: effects.Fingerprint(raw)}}
	ctx := effects.WithController(f.ctx, effects.NewController(f.selected.(effects.Store)).WithExecutionPosture(executionposture.Live))
	return effects.WithAuthority(effects.WithExecutionMode(ctx, effects.ExecutionModeLive), a)
}

func TestSessionLogoutJournalBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, acknowledged := range []bool{true, false} {
			name := "lost_result"
			if acknowledged {
				name = "acknowledged"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				c := openRuntimeConnectionFixture(t, f)
				connectRuntimeConnectionFixture(t, f, c)
				op := reserveRuntimeLogoutFixture(t, f, c)
				ctx := logoutJournalFixture(t, f, op)
				done := make(chan error, 1)
				go func() { done <- c.DispatchSessionLogout(ctx, op) }()
				frame := f.peer.next(t)
				if frame.node.Tag != "iq" || frame.node.Attrs["xmlns"] != "md" {
					t.Fatal("logout selected another SDK primitive", frame.node)
				}
				identity, _ := effects.ChannelLogoutOperationID(op.TeardownID)
				journal := f.selected.(effects.OutcomeStore)
				before, found, err := journal.GetExternalEffectOutcome(ctx, identity)
				if err != nil || !found || before.AttemptState != effects.StateLaunched {
					t.Fatalf("unlink preceded committed journal launch: %+v, %v", before, err)
				}
				if acknowledged {
					f.peer.acknowledge(t, frame)
				} else if err := frame.peer.conn.CloseNow(); err != nil {
					t.Fatal(err)
				}
				err = awaitOccurrenceProbe(t, f.peer.ctx, done)
				if (err == nil) != acknowledged {
					t.Fatal("logout result misclassified", err)
				}
				want, phase := effects.StateOutcomeUncertain, channelonboarding.TeardownFailed
				if acknowledged {
					want, phase = effects.StateSettled, channelonboarding.TeardownSucceeded
				}
				after, found, err := journal.GetExternalEffectOutcome(ctx, identity)
				if err != nil || !found || after.AttemptState != want {
					t.Fatalf("logout journal lost exact result: %+v, %v", after, err)
				}
				settled, err := f.selected.GetChannelTeardown(ctx, op.TeardownID)
				if err != nil || settled.Phase != phase || settled.Revision != op.Revision+1 || *settled.Logout != *op.Logout {
					t.Fatalf("logout result and responsibility did not commit together: %+v, %v", settled, err)
				}
				if c.state.currentOccurrence().client.Store.Deleted != acknowledged {
					t.Fatal("logout result contradicts retained SDK pairing")
				}
				if err := c.DispatchSessionLogout(ctx, op); err == nil {
					t.Fatal("terminal logout replayed its original unlink")
				}
				select {
				case extra := <-f.peer.frames:
					t.Fatal("logout replay sent another frame", extra.node)
				default:
				}
			})
		}
	}
}

func TestSessionLogoutJournalRecoveryAndRollbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, phase := range []effects.State{effects.StateAuthorized, effects.StateLaunched, effects.StateResponseObserved} {
			t.Run(backend+"/"+string(phase), func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				c := openRuntimeConnectionFixture(t, f)
				connectRuntimeConnectionFixture(t, f, c)
				op := reserveRuntimeLogoutFixture(t, f, c)
				ctx := logoutJournalFixture(t, f, op)
				raw, _ := json.Marshal(op.Logout)
				handle, err := effects.BeginChannelLogout(ctx, raw)
				if err != nil {
					t.Fatal(err)
				}
				if phase != effects.StateAuthorized {
					if err := handle.MarkLaunched(ctx); err != nil {
						t.Fatal(err)
					}
					if err := handle.Succeed(ctx, map[string]any{"remote_unlinked": true}); err == nil {
						t.Fatal("incomplete evidence fabricated logout success")
					}
					current, err := f.selected.GetChannelTeardown(ctx, op.TeardownID)
					if err != nil || current.Phase != op.Phase || current.Revision != op.Revision {
						t.Fatal("rejected settlement did not roll back responsibility", err)
					}
				}
				if phase == effects.StateResponseObserved {
					if err := handle.MarkResponseObserved(ctx, map[string]any{"remote_unlinked": true}); err != nil {
						t.Fatal(err)
					}
				}
				recovery := f.selected.(effects.RecoveryStore)
				for i := 0; i < 2; i++ {
					if _, err := recovery.ReconcileExternalEffectAttempts(ctx,
						effects.NewRecoveryRequest(time.Now().UTC().Add(time.Hour), executionposture.Live)); err != nil {
						t.Fatal(err)
					}
				}
				outcome, found, err := f.selected.(effects.OutcomeStore).GetExternalEffectOutcome(ctx, handle.Attempt().OperationID)
				want := effects.StateOutcomeUncertain
				if phase == effects.StateAuthorized {
					want = effects.StateTerminalFailure
				}
				if err != nil || !found || outcome.AttemptState != want {
					t.Fatalf("startup logout settlement: %+v, %v", outcome, err)
				}
				current, err := f.selected.GetChannelTeardown(ctx, op.TeardownID)
				if err != nil || current.Phase != channelonboarding.TeardownFailed || current.Revision != op.Revision+1 || *current.Logout != *op.Logout {
					t.Fatalf("startup recovery lost frozen responsibility or repeated completion: %+v, %v", current, err)
				}
				if _, err := effects.BeginChannelLogout(ctx, raw); err == nil {
					t.Fatal("recovered logout authorized redispatch")
				}
				if c.state.currentOccurrence().client.Store.Deleted {
					t.Fatal("journal recovery deleted retained provider state")
				}
				select {
				case frame := <-f.peer.frames:
					t.Fatal("journal recovery sent an unlink", frame.node)
				default:
				}
			})
		}
	}
}

func TestSessionLogoutJournalRejectsForeignAuthorityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			c := openRuntimeConnectionFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			op := reserveRuntimeLogoutFixture(t, f, c)
			ctx := logoutJournalFixture(t, f, op)
			original, _ := effects.AuthorityFromContext(ctx)
			raw, _ := json.Marshal(op.Logout)
			for _, tc := range []struct {
				name string
				edit func(*effects.Authority)
			}{
				{"owner", func(a *effects.Authority) { a.ExecutionOwner += "-successor" }},
				{"principal", func(a *effects.Authority) { a.ChannelLogout.PrincipalID = uuid.NewString() }},
				{"revision", func(a *effects.Authority) { a.ChannelLogout.TeardownRevision++; a.FenceGeneration++ }},
				{"runtime", func(a *effects.Authority) { a.ChannelLogout.RuntimeInstanceID = uuid.NewString() }},
				{"bundle", func(a *effects.Authority) { a.ChannelLogout.BundleHash += "-other" }},
				{"fingerprint", func(a *effects.Authority) { a.ChannelLogout.TargetFingerprint = effects.Fingerprint([]byte("other")) }},
				{"responsibility", func(a *effects.Authority) {
					a.ChannelLogout.TeardownID = uuid.NewString()
					a.ID, _ = effects.ChannelLogoutOperationID(a.ChannelLogout.TeardownID)
					a.ChannelLogout.EffectOperationID = a.ID
				}},
				{"mixed-authority", func(a *effects.Authority) { a.ChannelDelivery.EffectOperationID = uuid.NewString() }},
				{"confirmation", func(a *effects.Authority) { a.Kind = effects.AuthorityChannelConfirmation }},
				{"mock", func(a *effects.Authority) { a.ExecutionMode = effects.ExecutionModeMock }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					foreign := original
					tc.edit(&foreign)
					if _, err := effects.BeginChannelLogout(effects.WithAuthority(ctx, foreign), raw); err == nil {
						t.Fatal("foreign authority admitted exact logout")
					}
					if _, found, err := f.selected.(effects.OutcomeStore).GetExternalEffectOutcome(ctx, foreign.ID); err != nil || found {
						t.Fatal("foreign admission persisted an effect", err)
					}
				})
			}
			if _, err := effects.BeginChannelLogout(ctx, append(raw, ' ')); err == nil {
				t.Fatal("different request bytes borrowed the frozen target fingerprint")
			}
			if c.state.currentOccurrence().client.Store.Deleted {
				t.Fatal("rejected effect deleted provider state")
			}
			select {
			case frame := <-f.peer.frames:
				t.Fatal("rejected effect sent an unlink", frame.node)
			default:
			}
		})
	}
}

func TestSessionLogoutJournalDrainsAndCancelsWaitBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cancelWait := range []bool{false, true} {
			name := "complete_drain"
			if cancelWait {
				name = "cancel_original_wait"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				f := newActiveInputFixture(t, backend)
				c := openRuntimeConnectionFixture(t, f)
				connectRuntimeConnectionFixture(t, f, c)
				original := c.state.currentOccurrence()
				op := reserveRuntimeLogoutFixture(t, f, c)
				ctx := logoutJournalFixture(t, f, op)
				entered, finish := make(chan struct{}), make(chan struct{})
				var release sync.Once
				t.Cleanup(func() { release.Do(func() { close(finish) }) })
				transaction := make(chan error, 1)
				go func() {
					transaction <- original.client.Store.EventBuffer.DoDecryptionTxn(f.ctx, func(ctx context.Context) error {
						close(entered)
						<-finish
						return original.client.Store.Sessions.PutSession(ctx, "admitted_before_journal_logout.0", []byte("completed"))
					})
				}()
				select {
				case <-entered:
				case err := <-transaction:
					t.Fatal("private transaction refused before logout", err)
				case <-f.peer.ctx.Done():
					t.Fatal("private transaction never entered")
				}
				caller, cancel := context.WithCancel(ctx)
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- c.DispatchSessionLogout(caller, op) }()
				requireLogoutAdmissionFenced(t, f.peer, original)
				identity, _ := effects.ChannelLogoutOperationID(op.TeardownID)
				outcome, found, err := f.selected.(effects.OutcomeStore).GetExternalEffectOutcome(ctx, identity)
				if err != nil || !found || outcome.AttemptState != effects.StateAuthorized || original.client.Store.Deleted {
					t.Fatalf("journal launch/deletion preceded the admitted private transaction: %+v, %v", outcome, err)
				}
				select {
				case frame := <-f.peer.frames:
					t.Fatal("unlink preceded private transaction drain", frame.node)
				default:
				}
				if cancelWait {
					cancel()
					if err := awaitOccurrenceProbe(t, f.peer.ctx, done); !errors.Is(err, context.Canceled) {
						t.Fatal("original caller cancellation did not release its wait", err)
					}
					join, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
					if err := original.stores.fence.join(join); !errors.Is(err, context.DeadlineExceeded) {
						t.Fatal("unresolved original private work did not retain cleanup possession", err)
					}
					stop()
				}
				release.Do(func() { close(finish) })
				if err := awaitOccurrenceProbe(t, f.peer.ctx, transaction); err != nil {
					t.Fatal("logout lost admitted private transaction", err)
				}
				if !cancelWait {
					frame := f.peer.next(t)
					launched, found, err := f.selected.(effects.OutcomeStore).GetExternalEffectOutcome(ctx, identity)
					if err != nil || !found || launched.AttemptState != effects.StateLaunched {
						t.Fatal("drained unlink did not have committed launch", err)
					}
					f.peer.acknowledge(t, frame)
					if err := awaitOccurrenceProbe(t, f.peer.ctx, done); err != nil {
						t.Fatal(err)
					}
				} else {
					// The returned caller does not own the journal/cleanup tail.
					wait, stop := context.WithTimeout(context.Background(), 5*time.Second)
					defer stop()
					if err := c.Close(wait); err != nil {
						t.Fatal("original cleanup did not remain joinable", err)
					}
					if _, err := f.workOwner.RetireAndWait(wait); err != nil {
						t.Fatal("counted logout settlement was abandoned on caller cancellation", err)
					}
					settled, found, err := f.selected.(effects.OutcomeStore).GetExternalEffectOutcome(ctx, identity)
					if err != nil || !found || settled.AttemptState != effects.StateTerminalFailure {
						t.Fatal("canceled prelaunch work lost truthful settlement", settled, err)
					}
					select {
					case frame := <-f.peer.frames:
						t.Fatal("canceled original wait sent an unlink", frame.node)
					default:
					}
				}
			})
		}
	}
}

func TestWhatsAppRuntimeLogoutPreparationRejectsReconstruction(t *testing.T) {
	var empty RuntimeConnection
	var absent *RuntimeConnection
	for _, c := range []*RuntimeConnection{&empty, absent} {
		if _, err := c.PrepareSessionLogout(context.Background(), channelonboarding.Operation{}); err == nil {
			t.Fatal("reconstructed connection manufactured a logout target")
		}
	}
}
