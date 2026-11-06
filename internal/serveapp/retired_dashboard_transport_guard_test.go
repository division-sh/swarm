package serveapp

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
)

var retiredDashboardPaths = []string{
	"/api", "/api/health", "/api/healthz", "/api/status", "/api/events",
	"/api/events/id", "/api/events/stream", "/api/runtime/logs",
	"/api/runtime/logs/stream", "/api/runtime/incidents", "/api/runtime/actions",
	"/api/agents", "/api/agents/id", "/api/agents/id/directive",
	"/api/agents/id/restart", "/api/conversations", "/api/conversations/id",
	"/api/instances", "/api/instances/id", "/api/mailbox", "/api/mailbox/id",
	"/rpc", "/api/rpc", "/ws", "/api/ws",
	"/api/runs/run-1/trace", "/api/events/flow", "/api/instances/aggregate",
	"/api/agents/id/actions/directive", "/api/agents/id/actions/restart",
}

func assertServedRetiredDashboardRefusal(t *testing.T, rt servedControlProofRuntime) {
	t.Helper()
	counts := func() [5]int {
		t.Helper()
		var out [5]int
		for i, table := range []string{"runs", "entity_state", "agents", "mailbox", "api_idempotency"} {
			if err := rt.DB.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+table).Scan(&out[i]); err != nil {
				t.Fatalf("retired route state snapshot: %v", err)
			}
		}
		return out
	}
	before := counts()
	client := http.Client{Timeout: 5 * time.Second}
	for _, path := range retiredDashboardPaths {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			for _, auth := range []string{"", "Bearer invalid", "Bearer " + apiv1.DefaultLoopbackAPIToken, "Bearer retired"} {
				req, err := http.NewRequest(method, strings.TrimSuffix(rt.Endpoint, "/v1/rpc")+path, strings.NewReader(`{"action":"reset_state"}`))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Authorization", auth)
				response, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				_, readErr := io.Copy(io.Discard, response.Body)
				closeErr := response.Body.Close()
				if readErr != nil || closeErr != nil || response.StatusCode != http.StatusNotFound {
					t.Fatalf("%s retired %s %s: %d read=%v close=%v", rt.Backend, method, path, response.StatusCode, readErr, closeErr)
				}
			}
		}
	}
	if after := counts(); after != before {
		t.Fatalf("%s retired routes mutated domain state: %v -> %v", rt.Backend, before, after)
	}
}

func TestRetiredDashboardTransportCannotReturn(t *testing.T) {
	var hits atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusAccepted)
	})
	for _, isReady := range []bool{false, true} {
		var ready atomic.Bool
		ready.Store(isReady)
		mux := newAPIServer(&ready, handler, handler).Handler
		for _, path := range retiredDashboardPaths {
			for _, transport := range []string{"GET", "POST", "upgrade"} {
				for _, auth := range []string{"", "Bearer invalid", "Bearer configured", "Bearer retired"} {
					method := transport
					if transport == "upgrade" {
						method = http.MethodGet
					}
					req := httptest.NewRequest(method, path, strings.NewReader(`{"action":"reset_state"}`))
					req.Header.Set("Authorization", auth)
					if transport == "upgrade" {
						req.Header.Set("Connection", "Upgrade")
						req.Header.Set("Upgrade", "websocket")
					}
					rec := httptest.NewRecorder()
					mux.ServeHTTP(rec, req)
					if rec.Code != http.StatusNotFound || hits.Load() != 0 {
						t.Fatalf("ready=%t %s %s auth=%q: status=%d downstream hits=%d", isReady, transport, path, auth, rec.Code, hits.Load())
					}
				}
			}
		}
	}
}
