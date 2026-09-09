package publicingress

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
)

func requireListenerOwned(t *testing.T, address string) {
	t.Helper()
	competitor, err := net.Listen("tcp", address)
	if competitor != nil {
		_ = competitor.Close()
	}
	if !errors.Is(err, syscall.EADDRINUSE) {
		t.Fatalf("address %s is not exclusively owned: %v", address, err)
	}
}

func requireListenerReleased(t *testing.T, address string) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("address %s remained owned: %v", address, err)
	}
	_ = listener.Close()
}

func TestBoundPublicIngressListenerOwnsEntireStartupAndShutdown(t *testing.T) {
	for _, phase := range []string{"before_start", "probe_failure", "ready"} {
		t.Run(phase, func(t *testing.T) {
			listener := reserveTestListener(t)
			address := listener.Addr().String()
			requireListenerOwned(t, address)
			client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				if phase == "probe_failure" {
					return nil, errors.New("controlled public route failure")
				}
				clone := request.Clone(request.Context())
				clone.URL.Scheme, clone.URL.Host = "http", address
				return http.DefaultTransport.RoundTrip(clone)
			})}
			opts := Options{Mode: ModeExternalOrigin, PublicOrigin: "https://hooks.example.test", ListenAddress: address, HTTPClient: client, Handler: http.NotFoundHandler()}
			// Without handoff, the real startup path cannot bind the retained socket.
			unowned, err := NewController(opts)
			if err != nil {
				t.Fatal(err)
			}
			if err := unowned.Start(context.Background()); !errors.Is(err, syscall.EADDRINUSE) {
				t.Fatalf("negative control did not reproduce bind failure: %v", err)
			}
			opts.Listener = listener
			controller, err := NewController(opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = controller.Stop(context.Background()) })
			requireListenerOwned(t, address)
			if phase != "before_start" {
				err = controller.Start(context.Background())
				if phase == "probe_failure" {
					if err == nil || !strings.Contains(err.Error(), "controlled public route failure") || controller.Generation().ID != "" {
						t.Fatalf("failed probe admitted generation: %v, %+v", err, controller.Generation())
					}
					requireListenerReleased(t, address)
				} else {
					if err != nil || controller.Generation().ListenAddress != address || controller.Generation().ID == "" {
						t.Fatalf("handoff startup: %v, %+v", err, controller.Generation())
					}
					requireListenerOwned(t, address)
				}
			}
			if err := controller.Stop(context.Background()); err != nil {
				t.Fatal(err)
			}
			requireListenerReleased(t, address)
		})
	}
}

func TestBoundPublicIngressListenerRejectsDifferentAddressOrMode(t *testing.T) {
	for _, mode := range []string{ModeExternalOrigin, ModeManagedQuickTunnel} {
		t.Run(mode, func(t *testing.T) {
			listener := reserveTestListener(t)
			opts := Options{Mode: mode, Listener: listener, Handler: http.NotFoundHandler()}
			if mode == ModeExternalOrigin {
				opts.PublicOrigin = "https://hooks.example.test"
				opts.ListenAddress = reserveTestListener(t).Addr().String()
			}
			if _, err := NewController(opts); err == nil {
				t.Fatal("mismatched handoff accepted")
			}
			// Rejected construction must not take ownership from its caller.
			requireListenerOwned(t, listener.Addr().String())
		})
	}
}
