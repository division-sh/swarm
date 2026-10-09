package sessionprovider

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mau.fi/libsignal/ecc"
	"go.mau.fi/libsignal/keys/identity"
	"go.mau.fi/libsignal/keys/prekey"
	"go.mau.fi/libsignal/session"
	"go.mau.fi/libsignal/util/optional"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type encryptedCaptureLog struct{ handlerFailed chan struct{} }

func (encryptedCaptureLog) Debugf(string, ...any) {}
func (encryptedCaptureLog) Infof(string, ...any)  {}
func (encryptedCaptureLog) Errorf(string, ...any) {}
func (l encryptedCaptureLog) Warnf(format string, args ...any) {
	if strings.Contains(fmt.Sprintf(format, args...), "Handler for ENCRYPTED_CAPTURE failed") {
		select {
		case l.handlerFailed <- struct{}{}:
		default:
		}
	}
}
func (l encryptedCaptureLog) Sub(string) waLog.Logger { return l }

// The sender uses the same pinned Signal implementation and real private stores.
// Only the account and transport peer are synthetic; decryption is not mocked.
func encryptedMessageFixture(t *testing.T, receiver *store.Device, from types.JID, message *waE2E.Message) waBinary.Node {
	t.Helper()
	ctx := context.Background()
	_, senderContainer := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "sender.db"))
	sender := newSDKDeviceFixture(t, senderContainer)
	keys, err := receiver.PreKeys.GetOrGenPreKeys(ctx, 1)
	if err != nil || len(keys) != 1 {
		t.Fatalf("recipient prekey: %v", err)
	}
	bundle := prekey.NewBundle(receiver.RegistrationID, uint32(receiver.ID.Device),
		optional.NewOptionalUint32(keys[0].KeyID), receiver.SignedPreKey.KeyID,
		ecc.NewDjbECPublicKey(*keys[0].Pub), ecc.NewDjbECPublicKey(*receiver.SignedPreKey.Pub),
		*receiver.SignedPreKey.Signature, identity.NewKey(ecc.NewDjbECPublicKey(*receiver.IdentityKey.Pub)))
	builder := session.NewBuilderFromSignal(sender, receiver.ID.SignalAddress(), store.SignalProtobufSerializer)
	if err := builder.ProcessBundle(ctx, bundle); err != nil {
		t.Fatal(err)
	}
	plain, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	cipher := session.NewCipher(builder, receiver.ID.SignalAddress())
	// One byte of valid v2 padding; no SDK source implementation is copied.
	encrypted, err := cipher.Encrypt(ctx, append(plain, 1))
	if err != nil {
		t.Fatal(err)
	}
	return waBinary.Node{Tag: "message", Attrs: waBinary.Attrs{
		"from": from, "id": "ENCRYPTED_CAPTURE", "t": time.Now().Unix(), "type": "text",
	}, Content: []waBinary.Node{{Tag: "enc", Attrs: waBinary.Attrs{"type": "pkmsg", "v": "2"}, Content: encrypted.Serialize()}}}
}

func TestWhatsAppEncryptedCaptureCommitControlsSDKReceipt(t *testing.T) {
	for _, outcome := range []string{"committed", "write_failure", "callback_panic"} {
		t.Run(outcome, func(t *testing.T) {
			peer := newSDKPeer(t)
			_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
			device := newSDKDeviceFixture(t, container)
			event := captureFixture(t)
			event.Scope.Session.AccountRef = device.ID.String()
			captureDB, capture := openCaptureFixture(t, filepath.Join(t.TempDir(), "incoming.db"), event.Scope.Session.ConnectionID)
			if outcome == "write_failure" {
				if _, err := captureDB.Exec(`CREATE TRIGGER refuse_capture BEFORE INSERT ON whatsapp_incoming_capture BEGIN SELECT RAISE(ABORT,'test capture refusal'); END`); err != nil {
					t.Fatal(err)
				}
			}
			log := encryptedCaptureLog{handlerFailed: make(chan struct{}, 1)}
			o, err := newClientOccurrence(peer.ctx, event.Scope.Session.ConnectionID, event.OccurrenceID, device, container.LIDMap, log)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := o.join(ctx); err != nil {
					t.Error(err)
				}
			})
			from := types.NewJID("100000000003", types.DefaultUserServer)
			from.Device = 1
			want := &waE2E.Message{Conversation: proto.String("private encrypted capture fixture")}
			message := encryptedMessageFixture(t, device, from, want)
			handled := make(chan capturedEvent, 1)
			guard, err := o.bindCallbacks(func(ctx context.Context, raw any) error {
				message, ok := raw.(*events.Message)
				if !ok {
					return nil
				}
				captured := event
				captured.Conversation, captured.EventID = message.Info.Chat.String(), message.Info.ID
				if message.Info.Sender != from || !proto.Equal(message.Message, want) {
					return errors.New("SDK decrypted different sender or content")
				}
				var err error
				captured.Body, err = protojson.Marshal(message.Message)
				if err != nil {
					return err
				}
				handled <- captured
				if outcome == "callback_panic" {
					panic("private callback payload must not escape")
				}
				return capture.capture(ctx, captured)
			}, capture.recordFailure)
			if err != nil {
				t.Fatal(err)
			}
			peer.attach(t, o.client)
			if err := o.connect(); err != nil {
				t.Fatal(err)
			}
			if !o.client.WaitForConnection(5 * time.Second) {
				t.Fatal("synthetic SDK authentication did not complete")
			}
			peer.mu.Lock()
			socket := peer.peers[0]
			peer.mu.Unlock()
			if err := socket.send(peer.ctx, message); err != nil {
				t.Fatal(err)
			}
			var delivered capturedEvent
			select {
			case delivered = <-handled:
			case <-peer.ctx.Done():
				t.Fatal("pinned SDK did not decrypt and dispatch message")
			}
			if outcome == "committed" {
				select {
				case frame := <-peer.protocol:
					if frame.node.Tag != "receipt" || frame.node.Attrs["id"] != delivered.EventID {
						t.Fatalf("wrong SDK delivery receipt: %v", frame.node)
					}
				case <-peer.ctx.Done():
					t.Fatal("committed capture did not permit SDK receipt")
				}
			} else {
				select {
				case <-log.handlerFailed:
				case <-peer.ctx.Done():
					t.Fatal("SDK did not consume callback failure status")
				}
			}
			if err := o.join(peer.ctx); err != nil {
				t.Fatal(err)
			}
			pending, err := capture.pending(peer.ctx)
			if err != nil {
				t.Fatal(err)
			}
			if outcome == "committed" {
				if len(pending) != 1 || pending[0].Scope != event.Scope || !bytes.Equal(pending[0].Body, delivered.Body) {
					t.Fatalf("receipt preceded exact durable capture: %v", pending)
				}
			} else {
				if len(pending) != 0 || guard.currentFailure() == nil {
					t.Fatal("failed capture was retained as success")
				}
				select {
				case frame := <-peer.protocol:
					t.Fatalf("failed capture emitted SDK acknowledgment/receipt: %v", frame.node)
				default:
				}
				var count int
				if err := captureDB.QueryRow(`SELECT COUNT(*) FROM whatsapp_callback_failures WHERE connection_id=? AND occurrence_id=?`,
					event.Scope.Session.ConnectionID, event.OccurrenceID).Scan(&count); err != nil || count != 1 {
					t.Fatalf("failure evidence missing: %d %v", count, err)
				}
				if strings.Contains(guard.currentFailure().Error(), "private callback payload") {
					t.Fatal("private panic payload escaped")
				}
			}
		})
	}
}

func TestWhatsAppClientOccurrenceCallbackBindingCannotChangeAfterConnect(t *testing.T) {
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
	handle := func(context.Context, any) error { return nil }
	record := func(context.Context, callbackFailure) error { return nil }
	if _, err := o.bindCallbacks(nil, record); err == nil {
		t.Fatal("missing capture owner accepted")
	}
	guard, err := o.bindCallbacks(handle, record)
	if err != nil || guard.connectionID != o.connectionID || guard.occurrenceID != o.occurrenceID || guard.ctx != o.ctx {
		t.Fatalf("callback did not bind to owned scope: %v", err)
	}
	if _, err := o.bindCallbacks(handle, record); err == nil {
		t.Fatal("second callback owner accepted")
	}
	o.mu.Lock()
	o.started = true
	o.mu.Unlock()
	if _, err := o.bindCallbacks(handle, record); err == nil {
		t.Fatal("callback owner changed after connection admission")
	}
	o.fence()
	if _, err := o.bindCallbacks(handle, record); !errors.Is(err, errClientOccurrenceFenced) {
		t.Fatalf("retired occurrence accepted callbacks: %v", err)
	}
}
