package sessionprovider

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestWhatsAppClientOccurrenceShutdownInterleavings(t *testing.T) {
	for _, boundary := range []string{"retirement", "parent_cancellation", "all_three"} {
		for iteration := 0; iteration < 8; iteration++ {
			t.Run(fmt.Sprintf("%s/%02d", boundary, iteration), func(t *testing.T) {
				peer := newSDKPeer(t)
				_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
				device := newSDKDeviceFixture(t, container)
				ctx, cancel := context.WithCancel(peer.ctx)
				defer cancel()
				o, err := newClientOccurrence(ctx, uuid.NewString(), uuid.NewString(), device, container.LIDMap(), nil)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					joinCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
					defer stop()
					if err := o.join(joinCtx); err != nil {
						t.Error(err)
					}
				})
				peer.attach(t, o.client)
				if err := o.connect(o.ctx); err != nil || !o.client.WaitForConnection(5*time.Second) {
					t.Fatalf("synthetic SDK authentication: %v", err)
				}
				done := make(chan error, 1)
				go func() { done <- runOccurrenceProbe(peer.ctx, o, "send", "SHUTDOWN_PROBE") }()
				accepted := peer.next(t)
				start := make(chan struct{})
				var workers sync.WaitGroup
				for _, work := range []func(){
					func() {
						if boundary != "parent_cancellation" {
							o.fence()
						}
					},
					func() {
						if boundary != "retirement" {
							cancel()
						}
					},
					func() {
						if boundary == "all_three" {
							_ = accepted.peer.conn.CloseNow()
						}
					},
				} {
					workers.Add(1)
					go func() { defer workers.Done(); <-start; work() }()
				}
				close(start)
				workers.Wait()
				if err := awaitOccurrenceProbe(t, peer.ctx, done); err == nil {
					t.Fatal("lost effect result became success during retirement")
				}
				if err := o.join(peer.ctx); err != nil {
					t.Fatal(err)
				}
				if device.Deleted || o.client.IsConnected() {
					t.Fatal("shutdown deleted pairing or kept the old socket live")
				}
				select {
				case frame := <-peer.frames:
					t.Fatalf("shutdown replayed accepted work: %v", frame.node)
				default:
				}
			})
		}
	}
}

func TestWhatsAppClientOccurrenceLogoutResultAndRemoteClose(t *testing.T) {
	for iteration := 0; iteration < 16; iteration++ {
		t.Run(fmt.Sprintf("%02d", iteration), func(t *testing.T) {
			peer := newSDKPeer(t)
			_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
			device := newSDKDeviceFixture(t, container)
			o := newOccurrenceFixture(t, peer, device, container, uuid.NewString(), nil)
			done := make(chan error, 1)
			go func() { done <- o.logout(peer.ctx) }()
			accepted := peer.next(t)
			peer.acknowledge(t, accepted)
			_ = accepted.peer.conn.CloseNow()
			err := awaitOccurrenceProbe(t, peer.ctx, done)
			if err := o.join(peer.ctx); err != nil {
				t.Fatal(err)
			}
			if err != nil && device.Deleted {
				t.Fatal("failed logout deleted retained pairing")
			}
		})
	}
}
