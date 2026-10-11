package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/division-sh/swarm/internal/apiv1"
	"net/http"
	"testing"
	"time"
)

func issue642ObserveCommittedFork(t *testing.T, endpoint string, source any) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	read := func(method string, params map[string]any) (json.RawMessage, bool) {
		body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "a-fork-diagnostic-read", "method": method, "params": params})
		if err != nil {
			t.Logf("A_FORK_READ method=%s marshal_error=%v", method, err)
			return nil, false
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			t.Logf("A_FORK_READ method=%s request_error=%v", method, err)
			return nil, false
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
		began := time.Now()
		response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
		if err != nil {
			t.Logf("A_FORK_READ method=%s utc=%s elapsed=%s error=%v", method, time.Now().UTC().Format(time.RFC3339Nano), time.Since(began), err)
			return nil, false
		}
		defer response.Body.Close()
		var result servedJSONRPCEnvelope
		err = json.NewDecoder(response.Body).Decode(&result)
		t.Logf("A_FORK_READ method=%s utc=%s elapsed=%s status=%d result=%s rpc_error=%v decode_error=%v", method, time.Now().UTC().Format(time.RFC3339Nano), time.Since(began), response.StatusCode, result.Result, result.Error, err)
		return result.Result, err == nil && result.Error == nil && response.StatusCode == http.StatusOK
	}
	read("run.get", map[string]any{"run_id": source})
	data, ok := read("run.list", map[string]any{"limit": 3})
	if !ok {
		return
	}
	var inventory struct {
		Runs []struct {
			RunID string `json:"run_id"`
		} `json:"runs"`
	}
	if err := json.Unmarshal(data, &inventory); err != nil {
		t.Logf("A_FORK_RUN_INVENTORY decode_error=%v", err)
		return
	}
	for _, run := range inventory.Runs {
		read("event.list", map[string]any{"filter": map[string]any{"run_id": run.RunID}, "limit": 10})
	}
}

func r7P27ReadCommittedOutcome(t *testing.T, endpoint string, bundleHash any) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	read := func(method string, params map[string]any) (json.RawMessage, bool) {
		body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "p27-diagnostic-read", "method": method, "params": params})
		if err != nil {
			t.Logf("P27_READ method=%s marshal_error=%v", method, err)
			return nil, false
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			t.Logf("P27_READ method=%s request_error=%v", method, err)
			return nil, false
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
		started := time.Now()
		response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
		if err != nil {
			t.Logf("P27_READ method=%s utc=%s elapsed=%s error=%v", method, time.Now().UTC().Format(time.RFC3339Nano), time.Since(started), err)
			return nil, false
		}
		defer response.Body.Close()
		var result servedJSONRPCEnvelope
		err = json.NewDecoder(response.Body).Decode(&result)
		t.Logf("P27_READ method=%s utc=%s elapsed=%s status=%d result=%s rpc_error=%v decode_error=%v", method, time.Now().UTC().Format(time.RFC3339Nano), time.Since(started), response.StatusCode, result.Result, result.Error, err)
		return result.Result, err == nil && result.Error == nil && response.StatusCode == http.StatusOK
	}
	data, ok := read("run.list", map[string]any{"bundle_hash": bundleHash, "limit": 2})
	if !ok {
		return
	}
	var inventory struct {
		Runs []struct {
			RunID string `json:"run_id"`
		} `json:"runs"`
	}
	if err := json.Unmarshal(data, &inventory); err != nil {
		t.Logf("P27_RUN_INVENTORY decode_error=%v", err)
		return
	}
	for _, run := range inventory.Runs {
		read("run.get", map[string]any{"run_id": run.RunID})
		read("event.list", map[string]any{"filter": map[string]any{"run_id": run.RunID}, "limit": 100})
		read("entity.list", map[string]any{"run_id": run.RunID, "limit": 100})
	}
}
