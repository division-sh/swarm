package builder

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
)

type stubBuilderRuntimeControl struct{}

func (stubBuilderRuntimeControl) PauseIngress() error {
	return nil
}

func (stubBuilderRuntimeControl) ResumeIngress() error {
	return nil
}

func TestHandler_LegacyRoutesFailClosed(t *testing.T) {
	handler := NewHandler(Options{
		AuthToken: testBuilderAuthToken,
	})

	for _, tc := range []struct {
		method string
		path   string
		body   string
	}{
		{method: http.MethodPost, path: "/rpc", body: `{"jsonrpc":"2.0","id":"1","method":"engine.ping"}`},
		{method: http.MethodPost, path: "/api/rpc", body: `{"jsonrpc":"2.0","id":"1","method":"engine.ping"}`},
		{method: http.MethodGet, path: "/ws"},
		{method: http.MethodGet, path: "/api/ws"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer "+testBuilderAuthToken)
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("%s %s status = %d, want %d body=%s", tc.method, tc.path, rec.Code, http.StatusNotFound, rec.Body.String())
			}
		})
	}
}

func TestHandler_LegacyWebSocketRoutesDoNotUpgrade(t *testing.T) {
	ts := httptest.NewServer(NewHandler(Options{AuthToken: testBuilderAuthToken}))
	defer ts.Close()

	for _, path := range []string{"/ws", "/api/ws"} {
		t.Run(path, func(t *testing.T) {
			wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + path
			_, resp, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Authorization": []string{"Bearer " + testBuilderAuthToken}})
			if err == nil {
				t.Fatal("expected websocket upgrade denial")
			}
			if resp == nil || resp.StatusCode != http.StatusNotFound {
				t.Fatalf("upgrade response = %#v, want 404", resp)
			}
		})
	}
}

func TestHandler_RuntimeControllerHasNoResetCallback(t *testing.T) {
	eb, err := runtimebus.NewEventBus(runtimebus.InMemoryEventStore{})
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: eb}
	h := NewHandler(Options{
		AuthToken:       testBuilderAuthToken,
		Runtime:         stubBuilderRuntimeControl{},
		RuntimeAcquirer: newTestRuntimeAcquirer(t, rt),
	})
	typed, ok := h.(*handler)
	if !ok || typed.runHub == nil {
		t.Fatalf("builder handler runHub = %#v, want configured runHub", typed)
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"1","method":"runtime.reset_state"}`))
	req.Header.Set("Authorization", "Bearer "+testBuilderAuthToken)
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("legacy reset RPC status = %d, want %d body=%s", rec.Code, http.StatusNotFound, rec.Body.String())
	}
}
