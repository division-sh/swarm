package serveapp

import (
	"net/http"
	"net/http/httptest"
	"testing"

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
		{name: "external", opts: cliapp.ServeOptions{Dev: true, PublicWebhookBaseURL: "https://hooks.example.test", PublicWebhookListen: "127.0.0.1:8443"}, mode: publicingress.ModeExternalOrigin, enabled: true, ok: true},
		{name: "managed outside dev", opts: cliapp.ServeOptions{Expose: true}},
		{name: "external outside dev", opts: cliapp.ServeOptions{PublicWebhookBaseURL: "https://hooks.example.test", PublicWebhookListen: "127.0.0.1:8443"}},
		{name: "mixed modes", opts: cliapp.ServeOptions{Dev: true, Expose: true, PublicWebhookBaseURL: "https://hooks.example.test", PublicWebhookListen: "127.0.0.1:8443"}},
		{name: "partial external", opts: cliapp.ServeOptions{Dev: true, PublicWebhookBaseURL: "https://hooks.example.test"}},
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
