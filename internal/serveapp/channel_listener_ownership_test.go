package serveapp

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"

	"github.com/division-sh/swarm/internal/cliapp"
)

func assertChannelListenerReserved(t *testing.T, address string) {
	t.Helper()
	competitor, err := net.Listen("tcp", address)
	if competitor != nil {
		_ = competitor.Close()
	}
	if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("fixture released its address before cleanup: %v", err)
	}
}

func TestChannelListenerReservationSurvivesDescriptorHandoffs(t *testing.T) {
	var address string
	t.Run("fixture_lifetime", func(t *testing.T) {
		listener := reserveChannelOnboardingListener(t)
		address = listener.Addr().String()
		for range 2 {
			assertChannelListenerReserved(t, address)
			file, err := listener.File()
			if err != nil {
				t.Fatal(err)
			}
			child := channelOnboardingListenerFromFile(t, file)
			if child.Addr().String() != address {
				t.Fatal("handoff changed listener identity")
			}
			if err := child.Close(); err != nil {
				t.Fatal(err)
			}
			assertChannelListenerReserved(t, address)
		}
	})
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("fixture cleanup retained socket: %v", err)
	}
	_ = listener.Close()
}

func TestServeBoundPublicListenerClosesOnAdmissionFailure(t *testing.T) {
	listener := reserveChannelOnboardingListener(t)
	address := listener.Addr().String()
	if code := runFrom(context.Background(), repoRootForTest(), cliapp.ServeOptions{
		PublicWebhookListener: listener, NoFeed: true,
	}); code != 2 {
		t.Fatalf("invalid serve admission exit=%d, want2", code)
	}
	replacement, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("pre-controller failure leaked transferred socket: %v", err)
	}
	_ = replacement.Close()
}
