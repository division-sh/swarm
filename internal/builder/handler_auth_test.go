package builder

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHandler_RPCRejectsUnauthenticatedAccess(t *testing.T) {
	handler := NewHandler(Options{
		AuthToken: testBuilderAuthToken,
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"1","method":"engine.ping"}`))
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusUnauthorized, rec.Body.String())
	}
	if got := rec.Header().Get("WWW-Authenticate"); !strings.Contains(got, "Bearer") {
		t.Fatalf("WWW-Authenticate = %q, want bearer challenge", got)
	}
	var resp RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode rpc denial: %v", err)
	}
	if resp.Error == nil || resp.Error.Code != builderRPCAuthErrorCode {
		t.Fatalf("rpc error = %#v, want auth denial code", resp.Error)
	}
	if !strings.Contains(resp.Error.Message, "authorization bearer token") {
		t.Fatalf("rpc denial message = %q", resp.Error.Message)
	}
}

func TestHandler_RPCRejectsWhenControlPlaneAuthIsNotConfigured(t *testing.T) {
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/rpc", strings.NewReader(`{"jsonrpc":"2.0","id":"1","method":"engine.ping"}`))
	req.Header.Set("Authorization", "Bearer "+testBuilderAuthToken)
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	var resp RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode rpc denial: %v", err)
	}
	if resp.Error == nil || !strings.Contains(resp.Error.Message, "not configured") {
		t.Fatalf("rpc error = %#v, want explicit unconfigured denial", resp.Error)
	}
}

func TestHandler_WSRejectsUnauthenticatedUpgrade(t *testing.T) {
	ts := httptest.NewServer(NewHandler(Options{
		AuthToken: testBuilderAuthToken,
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
	}))
	defer ts.Close()

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatal("expected websocket upgrade denial")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("upgrade response = %#v, want 401", resp)
	}
}

func TestHandler_WSAllowsAuthenticatedUpgrade(t *testing.T) {
	restore := SetHealthHeartbeatIntervalForTest(20 * time.Millisecond)
	defer restore()
	ts := httptest.NewServer(NewHandler(Options{
		AuthToken: testBuilderAuthToken,
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "builder-auth-test",
	}))
	defer ts.Close()

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	headers := http.Header{"Authorization": []string{"Bearer " + testBuilderAuthToken}}
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, headers)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "channel": "engine:health"}); err != nil {
		t.Fatalf("subscribe write: %v", err)
	}
	var frame WSEventFrame
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if frame.Channel != "engine:health" {
		t.Fatalf("channel = %q, want engine:health", frame.Channel)
	}
}
