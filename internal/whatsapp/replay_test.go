package whatsapp

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

type replayProbeLog struct {
	retryEntered        chan struct{}
	retryReceiptRefused chan struct{}
}

func (l replayProbeLog) Debugf(format string, args ...any) {
	if strings.Contains(fmt.Sprintf(format, args...), "waiting for reconnect to retry") {
		select {
		case l.retryEntered <- struct{}{}:
		default:
		}
	}
	if strings.Contains(fmt.Sprintf(format, args...), "Cancelled retry receipt in PreRetryCallback") {
		select {
		case l.retryReceiptRefused <- struct{}{}:
		default:
		}
	}
}
func (replayProbeLog) Errorf(string, ...any)     {}
func (replayProbeLog) Warnf(string, ...any)      {}
func (replayProbeLog) Infof(string, ...any)      {}
func (l replayProbeLog) Sub(string) waLog.Logger { return l }

func sendReplayProbe(ctx context.Context, client *whatsmeow.Client) error {
	// Newsletters reach the same SendMessage response/retryFrame interpreter
	// without fabricating a Signal recipient. This is not a DM/interop proof.
	_, err := client.SendMessage(ctx, types.NewJID("100000000002", types.NewsletterServer),
		&waE2E.Message{Conversation: proto.String("frame acceptance evidence")},
		whatsmeow.SendRequestExtra{ID: "REPLAY_PROBE", Timeout: 10 * time.Second})
	return err
}

func TestWhatsAppPinnedSDKFrameReplayBypassesRetryReceiptHook(t *testing.T) {
	for _, operation := range []string{"send", "logout"} {
		t.Run(operation, func(t *testing.T) {
			peer := newSDKPeer(t)
			_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
			device := newSDKDeviceFixture(t, container)
			ctx, cancel := context.WithCancel(peer.ctx)
			log := replayProbeLog{retryEntered: make(chan struct{}, 1)}
			client := whatsmeow.NewClient(device, log)
			client.EnableAutoReconnect = false
			client.BackgroundEventCtx = ctx
			var hookCalls atomic.Int32
			client.PreRetryCallback = func(*events.Receipt, types.MessageID, int, *waE2E.Message) bool {
				hookCalls.Add(1)
				return false
			}
			peer.attach(t, client)
			t.Cleanup(func() { cancel(); client.Disconnect() })
			if err := client.ConnectContext(ctx); err != nil {
				t.Fatal(err)
			}
			if !client.WaitForConnection(5 * time.Second) {
				t.Fatal("synthetic SDK authentication did not complete")
			}
			done := make(chan error, 1)
			go func() {
				if operation == "logout" {
					done <- client.Logout(ctx)
				} else {
					done <- sendReplayProbe(ctx, client)
				}
			}()
			first := peer.next(t)
			if err := first.peer.conn.CloseNow(); err != nil {
				t.Fatal(err)
			}
			select {
			case <-log.retryEntered:
			case <-ctx.Done():
				t.Fatal("SDK retryFrame path not reached")
			}
			if err := client.ConnectContext(ctx); err != nil {
				t.Fatal(err)
			}
			second := peer.next(t)
			if !reflect.DeepEqual(first.node, second.node) || first.peer == second.peer {
				t.Fatalf("unsafe control did not replay the same frame on a new socket: %v / %v", first.node, second.node)
			}
			peer.acknowledge(t, second)
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("SDK replay did not finish")
			}
			if hookCalls.Load() != 0 {
				t.Fatal("control unexpectedly traversed the retry-receipt hook")
			}
			if operation == "logout" && !device.Deleted {
				t.Fatal("unsafe replay did not reach local destructive cleanup")
			}
		})
	}
}
