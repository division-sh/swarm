package whatsapp

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

func newOccurrenceFixture(t *testing.T, peer *sdkPeer, device *store.Device, container *sqlstore.Container, connectionID string, log waLog.Logger) *clientOccurrence {
	t.Helper()
	o, err := newClientOccurrence(peer.ctx, connectionID, uuid.NewString(), device, container.LIDMap, log)
	if err != nil {
		t.Fatal(err)
	}
	peer.attach(t, o.client)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := o.join(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := o.connect(); err != nil {
		t.Fatal(err)
	}
	if !o.client.WaitForConnection(5 * time.Second) {
		t.Fatal("synthetic occurrence authentication did not complete")
	}
	return o
}

func runOccurrenceProbe(ctx context.Context, o *clientOccurrence, operation, id string) error {
	if operation == "logout" {
		return o.logout(ctx)
	}
	_, err := o.send(ctx, types.NewJID("100000000002", types.NewsletterServer),
		&waE2E.Message{Conversation: proto.String("occurrence acceptance evidence")}, id)
	return err
}

func awaitOccurrenceProbe(t *testing.T, ctx context.Context, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		t.Fatal("SDK operation did not finish")
	}
	return nil
}

func TestWhatsAppClientOccurrenceNormalSendAndExplicitLogout(t *testing.T) {
	for _, operation := range []string{"send", "logout"} {
		t.Run(operation, func(t *testing.T) {
			peer := newSDKPeer(t)
			_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
			device := newSDKDeviceFixture(t, container)
			o := newOccurrenceFixture(t, peer, device, container, uuid.NewString(), nil)
			done := make(chan error, 1)
			go func() { done <- runOccurrenceProbe(peer.ctx, o, operation, "CURRENT_SEND") }()
			frame := peer.next(t)
			peer.acknowledge(t, frame)
			if err := awaitOccurrenceProbe(t, peer.ctx, done); err != nil {
				t.Fatal(err)
			}
			if device.Deleted != (operation == "logout") {
				t.Fatalf("wrong explicit deletion outcome: %v", device.Deleted)
			}
			if err := o.connect(); err == nil {
				t.Fatal("used SDK occurrence accepted another connect")
			}
		})
	}
}

func TestWhatsAppClientOccurrenceLostResultCannotReplayOnSuccessor(t *testing.T) {
	for _, operation := range []string{"send", "logout"} {
		for _, boundary := range []string{"remote_disconnect", "retirement_before_result"} {
			t.Run(operation+"/"+boundary, func(t *testing.T) {
				peer := newSDKPeer(t)
				_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
				device := newSDKDeviceFixture(t, container)
				jid := *device.ID
				connectionID := uuid.NewString()
				o := newOccurrenceFixture(t, peer, device, container, connectionID, nil)
				done := make(chan error, 1)
				go func() { done <- runOccurrenceProbe(peer.ctx, o, operation, "UNCERTAIN_SEND") }()
				first := peer.next(t) // The provider accepted; no result reaches the caller.
				if boundary == "remote_disconnect" {
					if err := first.peer.conn.CloseNow(); err != nil {
						t.Fatal(err)
					}
				} else {
					o.fence()
				}
				select {
				case <-o.ctx.Done():
				case <-peer.ctx.Done():
					t.Fatal("disconnected occurrence was not fenced")
				}
				if err := awaitOccurrenceProbe(t, peer.ctx, done); err == nil {
					t.Fatal("lost result became successful settlement")
				}
				if err := o.connect(); !errors.Is(err, errClientOccurrenceFenced) {
					t.Fatalf("old occurrence could reconnect: %v", err)
				}
				if err := runOccurrenceProbe(peer.ctx, o, operation, "FORBIDDEN_RETRY"); !errors.Is(err, errClientOccurrenceFenced) {
					t.Fatalf("old occurrence accepted another effect: %v", err)
				}
				if err := o.join(peer.ctx); err != nil {
					t.Fatal(err)
				}
				if device.Deleted {
					t.Fatal("uncertain logout or retirement deleted pairing")
				}
				retained, err := container.GetDevice(peer.ctx, jid)
				if err != nil || retained == nil {
					t.Fatalf("uncertain cleanup lost provider state: %v", err)
				}
				select {
				case extra := <-peer.frames:
					t.Fatalf("old SDK replayed accepted frame: %v", extra.node)
				default:
				}
				// This is a new explicit operation, not an automatic retry. The later
				// selected-store journey must prove its own authorization and journal.
				successor := newOccurrenceFixture(t, peer, retained, container, connectionID, nil)
				if successor.client == o.client || successor.occurrenceID == o.occurrenceID {
					t.Fatal("successor reused the old client occurrence")
				}
				go func() { done <- runOccurrenceProbe(peer.ctx, successor, operation, "EXPLICIT_NEW_SEND") }()
				fresh := peer.next(t)
				if fresh.peer == first.peer || fresh.node.Attrs["id"] == first.node.Attrs["id"] {
					t.Fatal("new operation was the original uncertain frame")
				}
				peer.acknowledge(t, fresh)
				if err := awaitOccurrenceProbe(t, peer.ctx, done); err != nil {
					t.Fatal(err)
				}
				if err := successor.join(peer.ctx); err != nil {
					t.Fatal(err)
				}
				select {
				case extra := <-peer.frames:
					t.Fatalf("unexpected second business/destructive frame: %v", extra.node)
				default:
				}
			})
		}
	}
}

func TestWhatsAppClientOccurrenceJoinRetainsDelayedSDKWork(t *testing.T) {
	peer := newSDKPeer(t)
	_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	device := newSDKDeviceFixture(t, container)
	o := newOccurrenceFixture(t, peer, device, container, uuid.NewString(), nil)
	entered, finish := make(chan struct{}), make(chan struct{})
	var finishOnce sync.Once
	defer finishOnce.Do(func() { close(finish) })
	done := make(chan error, 1)
	go func() {
		done <- device.EventBuffer.DoDecryptionTxn(peer.ctx, func(ctx context.Context) error {
			close(entered)
			<-finish
			return device.Sessions.PutSession(ctx, "joined_protocol.0", []byte("retained"))
		})
	}()
	<-entered
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := o.join(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("unfinished SDK work was released: %v", err)
	}
	if err := device.Sessions.PutSession(context.Background(), "late_protocol.0", []byte("refused")); !errors.Is(err, errSDKStoreFenced) {
		t.Fatalf("fenced protocol work entered provider state: %v", err)
	}
	finishOnce.Do(func() { close(finish) })
	if err := awaitOccurrenceProbe(t, peer.ctx, done); err != nil {
		t.Fatal(err)
	}
	if err := o.join(peer.ctx); err != nil {
		t.Fatal(err)
	}
}

func TestWhatsAppClientOccurrencePinnedConfigurationInventory(t *testing.T) {
	_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	device := newSDKDeviceFixture(t, container)
	o, err := newClientOccurrence(context.Background(), uuid.NewString(), uuid.NewString(), device, container.LIDMap, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := o.join(context.Background()); err != nil {
			t.Error(err)
		}
	})
	for name, enabled := range map[string]bool{
		"auto_reconnect":      o.client.EnableAutoReconnect,
		"initial_reconnect":   o.client.InitialAutoReconnect,
		"decrypted_buffer":    o.client.EnableDecryptedEventBuffer,
		"outgoing_store":      o.client.UseRetryMessageStore,
		"identity_auto_trust": o.client.AutoTrustIdentity,
		"phone_rerequest":     o.client.AutomaticMessageRerequestFromPhone,
	} {
		if enabled {
			t.Errorf("unsafe pinned SDK default enabled: %s", name)
		}
	}
	if !o.client.DisableLoginAutoReconnect || !o.client.SynchronousAck || !o.client.ManualHistorySyncDownload ||
		!o.client.DisableManualHistorySyncReceipt || o.client.BackgroundEventCtx != o.ctx ||
		o.client.PreRetryCallback == nil || o.client.PreRetryCallback(nil, "", 1, nil) {
		t.Fatal("incomplete pinned SDK occurrence posture")
	}
}

func TestWhatsAppClientOccurrenceRejectsSDKReceiptReplay(t *testing.T) {
	peer := newSDKPeer(t)
	_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	device := newSDKDeviceFixture(t, container)
	log := replayProbeLog{retryReceiptRefused: make(chan struct{}, 1)}
	o := newOccurrenceFixture(t, peer, device, container, uuid.NewString(), log)
	done := make(chan error, 1)
	go func() { done <- runOccurrenceProbe(peer.ctx, o, "send", "RECEIPT_PROBE") }()
	first := peer.next(t)
	peer.acknowledge(t, first)
	if err := awaitOccurrenceProbe(t, peer.ctx, done); err != nil {
		t.Fatal(err)
	}
	if err := first.peer.send(peer.ctx, waBinary.Node{
		Tag: "receipt", Attrs: waBinary.Attrs{
			"from": types.NewJID("100000000002", types.NewsletterServer),
			"id":   "RECEIPT_PROBE", "t": time.Now().Unix(), "type": "retry",
		}, Content: []waBinary.Node{{Tag: "retry", Attrs: waBinary.Attrs{
			"id": "RECEIPT_PROBE", "t": time.Now().Unix(), "count": "1",
		}}},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-log.retryReceiptRefused:
	case <-peer.ctx.Done():
		t.Fatal("SDK receipt retry did not traverse the refusal owner")
	}
	select {
	case frame := <-peer.protocol:
		if frame.node.Attrs["id"] != "RECEIPT_PROBE" || frame.node.Attrs["class"] != "receipt" {
			t.Fatalf("wrong protocol acknowledgment: %v", frame.node)
		}
	case <-peer.ctx.Done():
		t.Fatal("SDK retry handler did not finish its protocol acknowledgment")
	}
	if err := o.join(peer.ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case frame := <-peer.frames:
		t.Fatalf("refused receipt produced a business frame: %v", frame.node)
	default:
	}
}

func TestWhatsAppClientOccurrenceLoginReconnectAndUnlinkRetainState(t *testing.T) {
	for _, code := range []string{"515", "401"} {
		t.Run(code, func(t *testing.T) {
			peer := newSDKPeer(t)
			_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
			device := newSDKDeviceFixture(t, container)
			jid := *device.ID
			o := newOccurrenceFixture(t, peer, device, container, uuid.NewString(), nil)
			done := make(chan error, 1)
			go func() { done <- runOccurrenceProbe(peer.ctx, o, "send", "LOGIN_FENCE_PROBE") }()
			first := peer.next(t)
			failure := waBinary.Node{Tag: "stream:error", Attrs: waBinary.Attrs{"code": code}}
			if code == "401" {
				failure.Content = []waBinary.Node{{Tag: "conflict", Attrs: waBinary.Attrs{"type": "device_removed"}}}
			}
			if err := first.peer.send(peer.ctx, failure); err != nil {
				t.Fatal(err)
			}
			select {
			case <-o.ctx.Done():
			case <-peer.ctx.Done():
				t.Fatal("SDK lifetime event did not fence its occurrence")
			}
			if err := awaitOccurrenceProbe(t, peer.ctx, done); err == nil {
				t.Fatal("stream failure became a successful business effect")
			}
			if err := o.join(peer.ctx); err != nil {
				t.Fatal(err)
			}
			retained, err := container.GetDevice(peer.ctx, jid)
			if err != nil || retained == nil || device.Deleted {
				t.Fatalf("login/unlink event deleted local pairing: %v", err)
			}
			peer.mu.Lock()
			connections := len(peer.peers)
			peer.mu.Unlock()
			if connections != 1 {
				t.Fatalf("SDK reconnected the old occurrence: %d sockets", connections)
			}
		})
	}
}

func TestWhatsAppSDKClientLifetimeConsumerInventory(t *testing.T) {
	want := map[string]int{"NewClient": 1, "ConnectContext": 1, "Connect": 0,
		"ResetConnection": 0, "SendMessage": 1, "Logout": 1, "Disconnect": 1,
		"GetQRChannel": 0, "DangerousInternals": 0,
		"SendPasskeyResponse": 0, "SendPasskeyConfirmation": 0}
	got := make(map[string]int, len(want))
	for name := range want {
		got[name] = 0
	}
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range files {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), entry.Name(), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			method, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if _, listed := want[method.Sel.Name]; !listed {
				return true
			}
			if entry.Name() != "client_occurrence.go" {
				t.Errorf("SDK lifetime consumer bypasses its occurrence owner: %s/%s", entry.Name(), method.Sel.Name)
			}
			got[method.Sel.Name]++
			return true
		})
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("pinned SDK lifetime consumer inventory changed: got %v want %v", got, want)
	}
}
