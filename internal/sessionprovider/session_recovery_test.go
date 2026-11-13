//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/registration"
	"github.com/google/uuid"
	waBinary "go.mau.fi/whatsmeow/binary"
)

func recoveryNativeFixture(t *testing.T, backend string, registered bool) (*activeInputFixture, *RuntimeConnection, func() (*RuntimeConnection, error)) {
	t.Helper()
	var f *activeInputFixture
	if registered {
		f = newActiveInputFixtureWithTarget(t, backend, false, false, 0, true)
	} else {
		f = newNativeConfirmationFixture(t, backend)
		ctx := f.confirmationContext(t)
		executor := sessionChannelExecutor{owner: f.authority, plan: f.channel}
		proveNativeJournalWrite(t, f, ctx, "Swarm channel connected.", true, func() (registration.DeliveryResult, error) {
			return executor.DeliverChannelConfirmation(ctx, "deliver", map[string]any{"destination": f.binding.ConversationRef, "text": "Swarm channel connected."}, nil)
		})
		var err error
		f.operation, err = f.selected.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{OperationID: f.operation.OperationID, ExpectedRevision: f.operation.Revision, Phase: channelonboarding.PhaseSucceeded, Now: time.Now().UTC()})
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := f.state.close(f.ctx); err != nil {
		t.Fatal(err)
	}
	principal, err := f.selected.EnsureOperatorPrincipal(f.ctx, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	process := worklifetime.NewProcess()
	t.Cleanup(func() {
		process.Retire()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := process.Join(ctx); err != nil {
			t.Error(err)
		}
	})
	open := func() (*RuntimeConnection, error) {
		if !registered {
			return OpenRuntimeConnection(f.ctx, RuntimeConnectionOptions{Directory: f.basePath, Store: f.selected, OperationID: f.operation.OperationID, Plan: f.channel})
		}
		operations := f.selected.(interface {
			DeploymentChannelOperations() channelonboarding.OperationStore
		}).DeploymentChannelOperations()
		return OpenDeploymentConnection(context.Background(), DeploymentConnectionOptions{Directory: f.basePath, Store: f.selected, Operations: operations, OperationID: f.operation.OperationID, Principal: principal, Process: process, Plan: f.channel, Trigger: f.trigger})
	}
	c, err := open()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	connectRuntimeConnectionFixture(t, f, c)
	awaitDiagnostic(t, func() bool { _, healthy, err := c.SessionRecovery(f.ctx, f.operation); return err == nil && healthy })
	return f, c, open
}

// Invariant: genuine registered AND learned socket cuts preserve eligibility, exact device and joined replacement, both stores.
func TestWhatsAppRecoverySocketAndCurrentnessBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, registered := range []bool{false, true} {
			if !t.Run(fmt.Sprintf("%s/registered=%t", backend, registered), func(t *testing.T) {
				f, c, open := recoveryNativeFixture(t, backend, registered)
				original := c.state.currentOccurrence()
				for _, change := range []func(*channelonboarding.Operation){
					func(op *channelonboarding.Operation) { op.PrincipalID = uuid.NewString() }, func(op *channelonboarding.Operation) { op.Revision++ },
					func(op *channelonboarding.Operation) { op.Provider = "telegram" }, func(op *channelonboarding.Operation) { op.SessionAccount.AdmissionID = uuid.NewString() },
				} {
					op := f.operation
					change(&op)
					if _, _, err := c.SessionRecovery(f.ctx, op); err == nil {
						t.Fatal("foreign expected tuple admitted")
					}
				}
				storage := errors.New("independent currentness failure")
				operations := c.operations
				c.operations = diagnosticOperationFault{OperationStore: operations, cause: storage}
				_, _, err := c.SessionRecovery(f.ctx, f.operation)
				c.operations = operations
				if !errors.Is(err, storage) {
					t.Fatal("read failure became recovery", err)
				}
				ctx, cancel := context.WithCancel(f.ctx)
				cancel()
				if _, _, err := c.SessionRecovery(ctx, f.operation); err == nil {
					t.Fatal("canceled recovery")
				}
				second, err := open()
				if second != nil {
					if closeErr := second.Close(context.Background()); closeErr != nil {
						t.Fatal(closeErr)
					}
				}
				if err == nil || channelonboarding.SessionUnavailableOnly(err) {
					t.Fatal("physical competitor queued for retry", err)
				}
				f.peer.mu.Lock()
				wire := f.peer.peers[len(f.peer.peers)-1]
				f.peer.mu.Unlock()
				if err := wire.conn.CloseNow(); err != nil {
					t.Fatal(err)
				}
				awaitDiagnostic(t, func() bool { return original.diagnosticSnapshot().Down })
				at, healthy, err := c.SessionRecovery(f.ctx, f.operation)
				if err != nil || healthy || at.IsZero() {
					t.Fatal("actual cut lost positive native eligibility", at, err)
				}
				if channelonboarding.SessionUnavailableOnly(c.RecoveryFailure(f.ctx, f.operation, errors.Join(errClientOccurrenceFenced, storage))) {
					t.Fatal("mixed independent failure was hidden")
				}
				if err := c.Close(f.ctx); err != nil {
					t.Fatal(err)
				}
				if original.transientAdmissionRefusal(errClientOccurrenceFenced) {
					t.Fatal("retirement retained eligibility")
				}
				next, err := open()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := next.Close(context.Background()); err != nil {
						t.Error(err)
					}
				})
				connectRuntimeConnectionFixture(t, f, next)
				awaitDiagnostic(t, func() bool { _, ok, err := next.SessionRecovery(f.ctx, f.operation); return ok && err == nil })
				if next.ConnectionID() != c.ConnectionID() || next.state.currentOccurrence().occurrenceID == original.occurrenceID {
					t.Fatal("replacement changed device parent or reused occurrence")
				}
				select {
				case frame := <-f.peer.frames:
					t.Fatal("replacement replayed application send", frame.node.Tag)
				default:
				}
				next.state.currentOccurrence().callbacks.fail("capture_failed", errors.New("synthetic capture failure"))
				if at, _, err := next.SessionRecovery(f.ctx, f.operation); err == nil || !at.IsZero() {
					t.Fatal("capture failure permitted recovery", at, err)
				}
			}) {
				t.Fatal("first recovery backend/kind failed")
			}
		}
	}
}

// Invariant: permanent SDK lifecycle events never authorize recovery; real wire events, both stores.
func TestWhatsAppRecoveryPermanentEventsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, kind := range []string{"logout", "replacement", "ban"} {
			if !t.Run(backend+"/"+kind, func(t *testing.T) {
				f, c, _ := recoveryNativeFixture(t, backend, true)
				node := waBinary.Node{Tag: "stream:error", Attrs: waBinary.Attrs{"code": "401"}, Content: []waBinary.Node{{Tag: "conflict", Attrs: waBinary.Attrs{"type": "device_removed"}}}}
				if kind == "replacement" {
					node.Content = []waBinary.Node{{Tag: "conflict", Attrs: waBinary.Attrs{"type": "replaced"}}}
				}
				if kind == "ban" {
					node = waBinary.Node{Tag: "failure", Attrs: waBinary.Attrs{"reason": "402", "code": "101", "expire": "60"}}
				}
				f.peer.mu.Lock()
				wire := f.peer.peers[len(f.peer.peers)-1]
				f.peer.mu.Unlock()
				if err := wire.send(f.ctx, node); err != nil {
					t.Fatal(err)
				}
				awaitDiagnostic(t, func() bool {
					fact := c.state.currentOccurrence().diagnosticSnapshot()
					return fact.Down && fact.Lifecycle != ""
				})
				at, healthy, err := c.SessionRecovery(f.ctx, f.operation)
				if err == nil || healthy {
					t.Fatal("permanent event admitted recovery", at, err)
				}
				if channelonboarding.SessionUnavailableOnly(c.RecoveryFailure(f.ctx, f.operation, errClientOccurrenceFenced)) {
					t.Fatal("permanent fact became transient")
				}
				if kind == "logout" && !errors.Is(err, channelonboarding.ErrSessionNotLinked) {
					t.Fatal("positive revocation missing", err)
				}
			}) {
				t.Fatal("first permanent backend failed")
			}
		}
	}
}

// Invariant: only actual network-start leaves qualify; mixed storage/cancel/fence failures never do.
func TestWhatsAppRecoveryStartErrorBranches(t *testing.T) {
	for _, err := range []error{syscall.ECONNREFUSED, fmt.Errorf("dial: %w", syscall.ENETUNREACH), syscall.ETIMEDOUT} {
		if !transientConnectError(err) {
			t.Fatal("network start refused", err)
		}
	}
	for _, err := range []error{errClientOccurrenceFenced, context.Canceled, errors.Join(syscall.ECONNREFUSED, errSessionAccount), errors.New("unknown")} {
		if transientConnectError(err) {
			t.Fatal("unqualified error admitted", err)
		}
	}
}

// Invariant: positively absent SDK row is distinct from wrong/multiple/unsafe files; fixture deletion is NOT unlink.
func TestWhatsAppRecoveryMissingDeviceAndPhysicalGuardsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		if !t.Run(backend, func(t *testing.T) {
			f, c, open := recoveryNativeFixture(t, backend, true)
			expected := c.state.expectedAccount
			c.state.expectedAccount = "199999999999@s.whatsapp.net"
			_, _, err := c.SessionRecovery(f.ctx, f.operation)
			c.state.expectedAccount = expected
			if err == nil || errors.Is(err, channelonboarding.ErrSessionNotLinked) {
				t.Fatal("wrong device became absent", err)
			}
			device, err := c.state.device(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			owned, err := c.state.database.GetDevice(f.ctx, *device.ID)
			if err != nil || owned == nil {
				t.Fatal("native fixture device", err)
			}
			if err := owned.Delete(f.ctx); err != nil {
				t.Fatal(err)
			}
			if _, _, err := c.SessionRecovery(f.ctx, f.operation); !errors.Is(err, channelonboarding.ErrSessionNotLinked) {
				t.Fatal("positive absence not classified", err)
			}
			if err := c.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			missing, err := open()
			if missing != nil {
				if closeErr := missing.Close(context.Background()); closeErr != nil {
					t.Fatal(closeErr)
				}
			}
			if !errors.Is(err, channelonboarding.ErrSessionNotLinked) {
				t.Fatal("missing retained row did not refuse scan", err)
			}
		}) {
			t.Fatal("first missing-device backend failed")
		}
		if !t.Run(backend+"/unsafe_file", func(t *testing.T) {
			f, c, open := recoveryNativeFixture(t, backend, true)
			file := filepath.Join(c.state.directory.path, "provider.db")
			if err := c.Close(f.ctx); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(file, 0644); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := os.Chmod(file, 0600); err != nil {
					t.Error(err)
				}
			})
			unsafe, err := open()
			if unsafe != nil {
				if closeErr := unsafe.Close(context.Background()); closeErr != nil {
					t.Fatal(closeErr)
				}
			}
			if err == nil || channelonboarding.SessionUnavailableOnly(err) {
				t.Fatal("unsafe file became unavailable", err)
			}
		}) {
			t.Fatal("first unsafe-file backend failed")
		}
	}
}
