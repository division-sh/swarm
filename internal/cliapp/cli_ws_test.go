package cliapp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

func TestCLIAPIReadWebSocketJSONClassifiesMalformedPayloadAsProtocol(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer conn.Close()
		if err := conn.WriteMessage(websocket.TextMessage, []byte(`{`)); err != nil {
			t.Errorf("write malformed payload: %v", err)
		}
	}))
	defer server.Close()

	endpoint := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.DialContext(context.Background(), endpoint, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	var out map[string]any
	err = cliAPIReadWebSocketJSON(conn, "runtime event stream", endpoint, "subscription response", &out)
	if err == nil {
		t.Fatal("expected malformed websocket payload error")
	}
	var protocolErr *cliAPIProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("error = %T %v, want cliAPIProtocolError", err, err)
	}
	if cliAPIIsTransportFailure(err) {
		t.Fatalf("malformed websocket payload must not be classified as transport: %v", err)
	}
	if traceFollowRetryableSubscribeError(err) || traceFollowRetryableStreamError(err) {
		t.Fatalf("malformed websocket payload must not be retryable transport: %v", err)
	}
	got := FormatCLIAPIError(err)
	if want := "ERROR: the Swarm runtime at " + strings.TrimPrefix(endpoint, "ws://") + " returned an invalid API response."; !strings.Contains(got, want) {
		t.Fatalf("diagnostic = %q, want substring %q", got, want)
	}
	if strings.Contains(got, "cannot reach") {
		t.Fatalf("diagnostic = %q, must not render as transport failure", got)
	}
}
