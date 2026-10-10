package sessionprovider

import (
	"context"
	"errors"
	"path/filepath"
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
