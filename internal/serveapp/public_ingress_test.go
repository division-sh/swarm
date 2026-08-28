package serveapp

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/publicingress"
)

func TestResolveServePublicIngressMode(t *testing.T) {
	tests := []struct {
		name    string
		opts    cliapp.ServeOptions
		mode    string
		enabled bool
		ok      bool
	}{
		{name: "disabled", opts: cliapp.ServeOptions{}, ok: true},
		{name: "managed", opts: cliapp.ServeOptions{Dev: true, Expose: true}, mode: publicingress.ModeManagedQuickTunnel, enabled: true, ok: true},
		{name: "external in dev", opts: cliapp.ServeOptions{Dev: true, PublicWebhookBaseURL: "https://hooks.example.test", PublicWebhookListen: "127.0.0.1:8443"}, mode: publicingress.ModeExternalOrigin, enabled: true, ok: true},
		{name: "external in retained serve", opts: cliapp.ServeOptions{PublicWebhookBaseURL: "https://hooks.example.test", PublicWebhookListen: "127.0.0.1:8443"}, mode: publicingress.ModeExternalOrigin, enabled: true, ok: true},
		{name: "managed outside dev", opts: cliapp.ServeOptions{Expose: true}},
		{name: "mixed modes", opts: cliapp.ServeOptions{Dev: true, Expose: true, PublicWebhookBaseURL: "https://hooks.example.test", PublicWebhookListen: "127.0.0.1:8443"}},
		{name: "mixed modes outside dev", opts: cliapp.ServeOptions{Expose: true, PublicWebhookBaseURL: "https://hooks.example.test", PublicWebhookListen: "127.0.0.1:8443"}},
		{name: "partial external", opts: cliapp.ServeOptions{Dev: true, PublicWebhookBaseURL: "https://hooks.example.test"}},
		{name: "partial external outside dev", opts: cliapp.ServeOptions{PublicWebhookBaseURL: "https://hooks.example.test"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mode, enabled, err := resolveServePublicIngressMode(tc.opts)
			if (err == nil) != tc.ok || mode != tc.mode || enabled != tc.enabled {
				t.Fatalf("resolveServePublicIngressMode = %q,%v,%v want %q,%v,ok=%v", mode, enabled, err, tc.mode, tc.enabled, tc.ok)
			}
		})
	}
}

type servePublicIngressRoundTripper func(*http.Request) (*http.Response, error)

func (f servePublicIngressRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestRetainedServeExternalOriginExposesOnlyWebhookRoutes(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	listenAddress := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	mode, enabled, err := resolveServePublicIngressMode(cliapp.ServeOptions{
		PublicWebhookBaseURL: "https://hooks.example.test", PublicWebhookListen: listenAddress,
	})
	if err != nil || !enabled || mode != publicingress.ModeExternalOrigin {
		t.Fatalf("retained external-origin admission = %q,%v,%v", mode, enabled, err)
	}
	client := &http.Client{Transport: servePublicIngressRoundTripper(func(request *http.Request) (*http.Response, error) {
		clone := request.Clone(request.Context())
		clone.URL.Scheme = "http"
		clone.URL.Host = listenAddress
		return http.DefaultTransport.RoundTrip(clone)
	})}
	var admitted int
	controller, err := publicingress.NewController(publicingress.Options{
		Mode: mode, PublicOrigin: "https://hooks.example.test", ListenAddress: listenAddress, HTTPClient: client,
		Handler: http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			if !strings.HasPrefix(request.URL.Path, "/webhooks/") {
				t.Fatalf("ingress handler received control-plane path %q", request.URL.Path)
			}
			admitted++
			response.WriteHeader(http.StatusAccepted)
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := controller.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := controller.Stop(stopCtx); err != nil {
			t.Errorf("stop retained external-origin ingress: %v", err)
		}
	})
	for _, route := range []string{"/v1/rpc", "/v1/ws", "/mcp", "/tools", "/healthz", "/readyz"} {
		response, err := client.Get("https://hooks.example.test" + route)
		if err != nil {
			t.Fatalf("GET %s: %v", route, err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusNotFound {
			t.Fatalf("GET %s status = %d, want 404", route, response.StatusCode)
		}
	}
	response, err := client.Post("https://hooks.example.test/webhooks/support/telegram", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusAccepted || admitted != 1 {
		t.Fatalf("webhook status=%d admitted=%d, want 202/1", response.StatusCode, admitted)
	}
}

func TestPublicIngressDegradationLowersReadyzWithoutBlockingLocalAPI(t *testing.T) {
	readiness := publicingress.NewReadinessOwner(true)
	readiness.SetRuntimeReady(true)
	server := newAPIServer(readiness, http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusNoContent)
	}), nil)

	api := httptest.NewRecorder()
	server.Handler.ServeHTTP(api, httptest.NewRequest(http.MethodPost, "/v1/rpc", nil))
	if api.Code != http.StatusNoContent {
		t.Fatalf("local API status = %d, want %d while public ingress is degraded", api.Code, http.StatusNoContent)
	}
	ready := httptest.NewRecorder()
	server.Handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz status = %d, want %d", ready.Code, http.StatusServiceUnavailable)
	}
}
