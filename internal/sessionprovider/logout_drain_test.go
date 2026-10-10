package sessionprovider

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestWhatsAppExplicitLogoutDrainsOriginalWorkBeforeUnlink(t *testing.T) {
	peer := newSDKPeer(t)
	_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	device := newSDKDeviceFixture(t, container)
	o := newOccurrenceFixture(t, peer, device, container, uuid.NewString(), nil)
	sent := make(chan error, 1)
	go func() { sent <- runOccurrenceProbe(peer.ctx, o, "send", "BEFORE_LOGOUT") }()
	accepted := peer.next(t)
	loggedOut := make(chan error, 1)
	go func() { loggedOut <- o.logout(peer.ctx) }()
	requireLogoutAdmissionFenced(t, peer, o)
	if device.Deleted || !o.client.IsConnected() {
		t.Fatal("logout discarded pairing/transport before original work drained")
	}
	peer.acknowledge(t, accepted)
	if err := awaitOccurrenceProbe(t, peer.ctx, sent); err != nil {
		t.Fatal("logout canceled the previously admitted send", err)
	}
	unlink := peer.next(t)
	if unlink.node.Tag != "iq" || unlink.node.Attrs["xmlns"] != "md" {
		t.Fatal("logout did not use its original unlink transport", unlink.node)
	}
	peer.acknowledge(t, unlink)
	if err := awaitOccurrenceProbe(t, peer.ctx, loggedOut); err != nil || !device.Deleted {
		t.Fatal("exact drained logout failed", err, device.Deleted)
	}
	if err := o.logout(peer.ctx); err == nil {
		t.Fatal("same occurrence repeated explicit destruction")
	}
	select {
	case duplicate := <-peer.frames:
		t.Fatal("logout launched more than once", duplicate.node)
	default:
	}
}

func TestWhatsAppExplicitLogoutDrainsSDKTransactionBeforeUnlink(t *testing.T) {
	peer := newSDKPeer(t)
	_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	device := newSDKDeviceFixture(t, container)
	if _, err := device.PreKeys.GetOrGenPreKeys(peer.ctx, 812); err != nil {
		t.Fatal("prepare genuine SDK private-state fixture", err)
	}
	o := newOccurrenceFixture(t, peer, device, container, uuid.NewString(), nil)
	entered, finish := make(chan struct{}), make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(finish) }) })
	transaction := make(chan error, 1)
	go func() {
		transaction <- device.EventBuffer.DoDecryptionTxn(peer.ctx, func(ctx context.Context) error {
			close(entered)
			<-finish
			return device.Sessions.PutSession(ctx, "admitted_before_logout.0", []byte("completed"))
		})
	}()
	<-entered
	loggedOut := make(chan error, 1)
	go func() { loggedOut <- o.logout(peer.ctx) }()
	requireLogoutAdmissionFenced(t, peer, o)
	select {
	case frame := <-peer.frames:
		t.Fatal("unlink preceded admitted SDK transaction completion", frame.node)
	case <-time.After(200 * time.Millisecond):
	}
	if device.Deleted || !o.client.IsConnected() {
		t.Fatal("SDK drain lost original pairing or transport")
	}
	release.Do(func() { close(finish) })
	if err := awaitOccurrenceProbe(t, peer.ctx, transaction); err != nil {
		t.Fatal("logout interrupted its previously admitted SDK transaction", err)
	}
	unlink := peer.next(t)
	peer.acknowledge(t, unlink)
	if err := awaitOccurrenceProbe(t, peer.ctx, loggedOut); err != nil || !device.Deleted {
		t.Fatal("SDK-drained unlink failed", err, device.Deleted)
	}
}

func TestWhatsAppExplicitLogoutDrainsCallbacksBeforeUnlink(t *testing.T) {
	peer := newSDKPeer(t)
	_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	device := newSDKDeviceFixture(t, container)
	if _, err := device.PreKeys.GetOrGenPreKeys(peer.ctx, 812); err != nil {
		t.Fatal(err)
	}
	o, err := newClientOccurrence(peer.ctx, uuid.NewString(), uuid.NewString(), device, container.LIDMap(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := o.join(peer.ctx); err != nil {
			t.Error(err)
		}
	})
	entered, finish := make(chan struct{}), make(chan struct{})
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(finish) }) })
	callbacks, err := o.bindCallbacks(func(_ context.Context, event any) error {
		if marker, ok := event.(string); ok && marker == "held callback" {
			close(entered)
			<-finish
		}
		return nil
	}, func(context.Context, callbackFailure) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	peer.attach(t, o.client)
	if err := o.connect(peer.ctx); err != nil || !o.client.WaitForConnection(5*time.Second) {
		t.Fatal("SDK callback fixture did not connect", err)
	}
	_, releaseOutbound, err := o.acquire(peer.ctx)
	if err != nil {
		t.Fatal(err)
	}
	var outbound sync.Once
	t.Cleanup(func() { outbound.Do(releaseOutbound) })
	captured := make(chan bool, 1)
	go func() { captured <- callbacks.receive("held callback") }()
	<-entered
	loggedOut := make(chan error, 1)
	go func() { loggedOut <- o.logout(peer.ctx) }()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		callbacks.mu.Lock()
		fenced := callbacks.fenced
		callbacks.mu.Unlock()
		if fenced {
			break
		}
		select {
		case frame := <-peer.frames:
			t.Fatal("unlink preceded admitted callback completion", frame.node)
		case <-deadline.C:
			t.Fatal("logout did not fence callback admission")
		case <-time.After(time.Millisecond):
		}
	}
	if callbacks.receive("late callback") || device.Deleted || !o.client.IsConnected() {
		t.Fatal("logout admitted a new callback or discarded original transport/state")
	}
	outbound.Do(releaseOutbound)
	release.Do(func() { close(finish) })
	select {
	case success := <-captured:
		if success {
			t.Fatal("callback completed after fencing but reported SDK success")
		}
	case <-peer.ctx.Done():
		t.Fatal("original callback did not join")
	}
	unlink := peer.next(t)
	peer.acknowledge(t, unlink)
	if err := awaitOccurrenceProbe(t, peer.ctx, loggedOut); err != nil || !device.Deleted {
		t.Fatal("callback-drained unlink failed", err, device.Deleted)
	}
}

func requireLogoutAdmissionFenced(t *testing.T, peer *sdkPeer, o *clientOccurrence) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for {
		select {
		case frame := <-peer.frames:
			t.Fatal("logout launched before original work drained", frame.node)
		case <-deadline.C:
			t.Fatal("logout never fenced new business admission")
		default:
		}
		_, release, err := o.acquire(peer.ctx)
		if errors.Is(err, errClientOccurrenceFenced) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		release()
		time.Sleep(time.Millisecond)
	}
}

func TestWhatsAppExplicitLogoutCanceledDrainDoesNotSendOrDelete(t *testing.T) {
	peer := newSDKPeer(t)
	_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	device := newSDKDeviceFixture(t, container)
	o := newOccurrenceFixture(t, peer, device, container, uuid.NewString(), nil)
	_, release, err := o.acquire(peer.ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if release != nil {
			release()
		}
	})
	caller, cancel := context.WithCancel(peer.ctx)
	done := make(chan error, 1)
	go func() { done <- o.logout(caller) }()
	requireLogoutAdmissionFenced(t, peer, o)
	cancel()
	if err := awaitOccurrenceProbe(t, peer.ctx, done); !errors.Is(err, context.Canceled) {
		t.Fatal("pending unlink lost its bounded cancellation", err)
	}
	release()
	release = nil
	if device.Deleted || !o.client.IsConnected() {
		t.Fatal("prelaunch cancellation destroyed pairing or transport")
	}
	if err := o.logout(peer.ctx); err == nil {
		t.Fatal("another caller silently adopted the reserved unlink")
	}
	select {
	case frame := <-peer.frames:
		t.Fatal("canceled unlink emitted an effect", frame.node)
	default:
	}
}

func TestWhatsAppExplicitLogoutRetirementDuringDrainRetainsPairing(t *testing.T) {
	peer := newSDKPeer(t)
	_, container := openSDKStoreFixture(t, filepath.Join(t.TempDir(), "provider.db"))
	device := newSDKDeviceFixture(t, container)
	o := newOccurrenceFixture(t, peer, device, container, uuid.NewString(), nil)
	_, release, err := o.acquire(peer.ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if release != nil {
			release()
		}
	})
	done := make(chan error, 1)
	go func() { done <- o.logout(peer.ctx) }()
	requireLogoutAdmissionFenced(t, peer, o)
	joined := make(chan error, 1)
	go func() { joined <- o.join(peer.ctx) }()
	if err := awaitOccurrenceProbe(t, peer.ctx, done); err == nil {
		t.Fatal("retirement allowed a pending destructive effect")
	}
	select {
	case err := <-joined:
		t.Fatal("retirement released possession before admitted work drained", err)
	default:
	}
	release()
	release = nil
	if err := awaitOccurrenceProbe(t, peer.ctx, joined); err != nil || device.Deleted {
		t.Fatal("ordinary retirement destroyed pairing or failed its join", err, device.Deleted)
	}
	select {
	case frame := <-peer.frames:
		t.Fatal("retirement launched a reserved unlink", frame.node)
	default:
	}
}
