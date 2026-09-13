package serveapp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
)

// The real server executes the command. The proxy observes its response, then
// closes the caller's socket without delivering any bytes. This is response
// loss, not process death; the separate process proof covers that boundary.
func mailboxCompletionLoseHTTPResponse(t *testing.T, endpoint, method string, params map[string]any) map[string]any {
	t.Helper()
	type observation struct {
		response servedJSONRPCEnvelope
		err      error
	}
	observed := make(chan observation, 1)
	var calls atomic.Int32
	client := &http.Client{Timeout: 20 * time.Second}
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		request, err := http.NewRequestWithContext(r.Context(), http.MethodPost, endpoint, r.Body)
		if err != nil {
			observed <- observation{err: err}
			http.Error(w, "upstream request failed", http.StatusBadGateway)
			return
		}
		request.Header = r.Header.Clone()
		response, err := client.Do(request)
		if err != nil {
			observed <- observation{err: err}
			http.Error(w, "upstream execution failed", http.StatusBadGateway)
			return
		}
		defer response.Body.Close()
		var capture observation
		capture.err = json.NewDecoder(response.Body).Decode(&capture.response)
		if response.StatusCode != http.StatusOK {
			capture.err = errors.Join(capture.err, fmt.Errorf("upstream HTTP status %d", response.StatusCode))
		}
		connection, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			capture.err = errors.Join(capture.err, err)
			observed <- capture
			return
		}
		capture.err = errors.Join(capture.err, connection.Close())
		observed <- capture
	}))
	defer proxy.Close()
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "lost-mailbox-response", "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodPost, proxy.URL, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	response, err := client.Do(request)
	if response != nil {
		response.Body.Close()
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected lost HTTP response, got %v", err)
	}
	var capture observation
	select {
	case capture = <-observed:
	case <-time.After(time.Second):
		t.Fatal("response loss occurred without the completed upstream request")
	}
	if calls.Load() != 1 || capture.err != nil || capture.response.Error != nil {
		t.Fatalf("wrong response-loss cut: requests=%d err=%v response=%+v", calls.Load(), capture.err, capture.response)
	}
	var result map[string]any
	if err := json.Unmarshal(capture.response.Result, &result); err != nil {
		t.Fatal(err)
	}
	if result["idempotency_replayed"] != false {
		t.Fatalf("response-loss cut did not execute a fresh command: %v", result)
	}
	return result
}
