//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func awaitDiagnostic(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("diagnostic observation did not arrive")
		}
		time.Sleep(time.Millisecond)
	}
}

// Invariant: only fixed facts survive SDK errors; real timeout/restoration do not fence.
func TestWhatsAppDiagnosticCaptureAndNativeJoin(t *testing.T) {
	for _, cell := range []struct {
		err    error
		socket string
	}{
		{websocket.CloseError{Code: websocket.StatusNormalClosure, Reason: "PRIVATE-CANARY"}, "peer_close"},
		{websocket.CloseError{Code: websocket.StatusNoStatusRcvd}, "peer_close"},
		{websocket.CloseError{Code: websocket.StatusAbnormalClosure}, "unknown"},
		{fmt.Errorf("PRIVATE-CANARY: %w", syscall.ECONNRESET), "reset"},
		{syscall.ENETUNREACH, "unreachable"}, {syscall.EHOSTUNREACH, "unreachable"},
		{syscall.ETIMEDOUT, "timeout"}, {context.DeadlineExceeded, "timeout"},
		{io.EOF, "eof"}, {errors.New("PRIVATE-CANARY"), "unknown"},
	} {
		o := &clientOccurrence{}
		log := sdkDiagnosticLog{Logger: waLog.Noop, owner: o}.Sub("Socket")
		log.Errorf("Error reading from websocket: %v", cell.err)
		log.Errorf("Error reading from websocket: %v", syscall.ECONNRESET)
		fact := o.diagnosticSnapshot()
		if fact.Socket != cell.socket || strings.Contains(fmt.Sprint(fact), "PRIVATE-CANARY") {
			t.Fatal("capture leaked raw data, changed first fact or misclassified", fact)
		}
	}
	for _, cause := range []string{"incoming capture failed", "incoming callback panic", "runtime release failed", "connection attempt failed", "connection attempt cancelled", "PRIVATE-CANARY"} {
		t.Run(cause, func(t *testing.T) {
			peer := newSDKPeer(t)
			_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
			o := newOccurrenceFixture(t, peer, newSDKDeviceFixture(t, container), container, uuid.NewString(), nil)
			o.mu.Lock()
			o.diagnostic.Socket, o.diagnostic.Lifecycle = "reset", "logged out"
			o.mu.Unlock()
			o.fenceFailure(cause)
			if err := o.join(peer.ctx); err != nil {
				t.Fatal(err)
			}
			fact := o.diagnosticSnapshot()
			if !fact.Failed() || fact.Disconnected || fact.Cause() != fixedSessionFailure(cause) || strings.Contains(fmt.Sprint(fact), "PRIVATE-CANARY") {
				t.Fatal("local origin borrowed older socket/lifecycle facts or cleanup erased it", fact)
			}
		})
	}
	for _, interrupted := range []bool{false, true} {
		t.Run(fmt.Sprintf("intentional_join/interrupted=%t", interrupted), func(t *testing.T) {
			peer := newSDKPeer(t)
			_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
			o := newOccurrenceFixture(t, peer, newSDKDeviceFixture(t, container), container, uuid.NewString(), nil)
			ctx := peer.ctx
			if interrupted {
				_, release, err := o.acquire(peer.ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer release()
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			err := o.join(ctx)
			fact := o.diagnosticSnapshot()
			if interrupted {
				if err == nil || !fact.Failed() || fact.Disconnected || fact.Cause() != "runtime release failed" {
					t.Fatal("failed intentional cleanup hid its actual failure", fact, err)
				}
			} else if err != nil || fact.Failed() || !fact.Down || !fact.Intentional {
				t.Fatal("normal shutdown became an error", fact, err)
			}
		})
	}
	min, max, response := whatsmeow.KeepAliveIntervalMin, whatsmeow.KeepAliveIntervalMax, whatsmeow.KeepAliveResponseDeadline
	whatsmeow.KeepAliveIntervalMin, whatsmeow.KeepAliveIntervalMax, whatsmeow.KeepAliveResponseDeadline = 20*time.Millisecond, 21*time.Millisecond, 25*time.Millisecond
	t.Cleanup(func() {
		whatsmeow.KeepAliveIntervalMin, whatsmeow.KeepAliveIntervalMax, whatsmeow.KeepAliveResponseDeadline = min, max, response
	})
	peer := newSDKPeer(t)
	peer.dropPing.Store(true)
	_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	o := newOccurrenceFixture(t, peer, newSDKDeviceFixture(t, container), container, uuid.NewString(), nil)
	awaitDiagnostic(t, func() bool { return o.diagnosticSnapshot().KeepAliveTimeout })
	if fact := o.diagnosticSnapshot(); fact.Down || fact.Cause() != "cause unknown" || !o.client.IsConnected() {
		t.Fatal("keepalive observation became health or disconnect cause", fact)
	}
	peer.dropPing.Store(false)
	awaitDiagnostic(t, func() bool { return o.diagnosticSnapshot().KeepAliveRestored })
	peer.mu.Lock()
	socket := peer.peers[0]
	peer.mu.Unlock()
	if err := socket.conn.Close(websocket.StatusNormalClosure, "PRIVATE-CANARY"); err != nil {
		t.Fatal(err)
	}
	awaitDiagnostic(t, func() bool { return o.diagnosticSnapshot().Down })
	if err := o.join(peer.ctx); err != nil {
		t.Fatal("native cleanup did not join", err)
	}
	if fact := o.diagnosticSnapshot(); fact.Socket != "peer_close" || fact.Cause() != "closed by the other side" || strings.Contains(fmt.Sprint(fact), "PRIVATE-CANARY") {
		t.Fatal("real socket/generic fence lost safe original cause", fact)
	}
}

type diagnosticOperationFault struct {
	channelonboarding.OperationStore
	cause error
}

func (s diagnosticOperationFault) GetChannelOnboarding(context.Context, string) (channelonboarding.Operation, error) {
	return channelonboarding.Operation{}, s.cause
}

// Invariant: captured historical logout is readable, never native execution authority.
func TestWhatsAppDiagnosticOwnedLogoutAndReadGuardsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		if !t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixture(t, backend)
			c := openRuntimeConnectionFixture(t, f)
			connectRuntimeConnectionFixture(t, f, c)
			o := c.state.currentOccurrence()
			fact, account, err := c.SessionDiagnostic(f.ctx)
			if err != nil || fact.Down || fact.OccurrenceID != o.occurrenceID || account == "" {
				t.Fatal("healthy native observation", fact, err)
			}
			original := c.state.expectedAccount
			c.state.expectedAccount = "199999999999@s.whatsapp.net"
			_, _, err = c.SessionDiagnostic(f.ctx)
			c.state.expectedAccount = original
			if !errors.Is(err, errSessionAccount) {
				t.Fatal("no-fact read lost device validation", err)
			}
			f.peer.mu.Lock()
			wire := f.peer.peers[len(f.peer.peers)-1]
			f.peer.mu.Unlock()
			if err := wire.send(f.ctx, waBinary.Node{Tag: "stream:error", Attrs: waBinary.Attrs{"code": "401"}, Content: []waBinary.Node{{Tag: "conflict", Attrs: waBinary.Attrs{"type": "device_removed"}}}}); err != nil {
				t.Fatal(err)
			}
			awaitDiagnostic(t, func() bool {
				fact := o.diagnosticSnapshot()
				return fact.Lifecycle == "logged out" && fact.Down
			})
			if err := o.join(f.ctx); err != nil {
				t.Fatal("captured logout did not join native work", err)
			}
			device, err := c.state.device(f.ctx)
			if err != nil || device.ID == nil {
				t.Fatal("unsolicited SDK deletion bypassed explicit-logout guard", err)
			}
			// Fixture-controlled native-owner deletion, NOT provider unlink or implicit SDK deletion.
			jid, err := types.ParseJID(f.operation.SessionAccount.AccountRef)
			if err != nil {
				t.Fatal(err)
			}
			device, err = c.state.database.GetDevice(f.ctx, jid)
			if err != nil || device == nil {
				t.Fatal("fixture native device", err)
			}
			if err := device.Delete(f.ctx); err != nil {
				t.Fatal(err)
			}
			fact, got, err := c.SessionDiagnostic(f.ctx)
			if err != nil || !fact.Failed() || !fact.Disconnected || fact.Cause() != "logged out" || got != account || c.FailureCause() != "logged out" || c.DisconnectionReason() != "logged out" {
				t.Fatal("deleted device hid original occurrence fact", fact, err)
			}
			if _, _, err := c.AccountPresentation(f.ctx); err == nil {
				t.Fatal("ordinary read lost native device guard")
			}
			admission, err := c.AdmitSessionAccount(f.ctx, f.operation.SessionAccount)
			admission.Close()
			if err == nil {
				t.Fatal("historical cause granted native admission")
			}
			for _, cell := range []string{"connection", "occurrence", "tuple", "storage", "cancel"} {
				connectionID, occurrenceID, operation, operations := o.connectionID, o.occurrenceID, c.operation, c.operations
				ctx, cancel := context.WithCancel(f.ctx)
				cause := errors.New("diagnostic storage fault")
				switch cell {
				case "connection":
					o.connectionID = uuid.NewString()
				case "occurrence":
					o.occurrenceID = "invalid"
				case "tuple":
					c.operation.PrincipalID = uuid.NewString()
				case "storage":
					c.operations = diagnosticOperationFault{operations, cause}
				case "cancel":
					cancel()
				}
				_, _, err := c.SessionDiagnostic(ctx)
				cancel()
				o.connectionID, o.occurrenceID, c.operation, c.operations = connectionID, occurrenceID, operation, operations
				if err == nil || cell == "storage" && !errors.Is(err, cause) {
					t.Fatal("observation bypassed refusal", cell, err)
				}
			}
		}) {
			t.Fatal("first backend failure; later backend not run")
		}
	}
}
