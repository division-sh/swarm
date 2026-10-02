package releasee2e

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// The internal mock-agent lifecycle executes the root -> singleton -> private
// template workload. Public CLI commands and HTTP/WS readbacks inspect its
// recorded results; this is not a claim of public live-agent startup.
func TestSelectedRootEventBoundarySQLitePostgres(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s required for both-store public boundary proof", goldenPostgresEnv)
	}
	root := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, root)
	lifecycleBinary := buildOwnedMockLifecycleBinary(t, root)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			base := filepath.Join(root, backend)
			store := goldenSQLiteStore(base)
			if backend == "postgres" {
				store = goldenPostgresStore(t, dsn)
			}
			runGoldenAgentWorkload(t, binary, base, store, false, goldenWorkloadOptions{
				lifecycleBinary: lifecycleBinary, candidateIDs: goldenSmokeCandidateIDs, publicBoundaryProof: true,
			})
		})
	}
}

func assertSelectedRootRejectsPrivatePublication(t *testing.T, process *releaseServeProcess, hash, binary, cwd, config, token string, env []string, payload map[string]any, eventNames ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
	defer cancel()
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var before, after json.RawMessage
	if err := process.rpc.call(ctx, "run.list", map[string]any{"bundle_hash": hash}, &before); err != nil {
		t.Fatal(err)
	}
	for _, event := range eventNames {
		params := map[string]any{"bundle_hash": hash, "event_name": event,
			"payload": payload, "idempotency_key": "reject-http-" + event}
		var result json.RawMessage
		if err := process.rpc.call(ctx, "event.publish", params, &result); err == nil || !strings.Contains(err.Error(), "EVENT_NOT_DECLARED") {
			t.Fatalf("private HTTP input %s: result=%s err=%v", event, result, err)
		}
		cli := runReleaseCommand(t, goldenStartupTimeout, cwd, env, "", binary,
			"event", "publish", event, "--payload-json", string(payloadJSON),
			"--idempotency-key", "reject-cli-"+event,
			"--config", config, "--api-server", process.apiBase, "--api-token-file", token)
		if cli.err == nil || !strings.Contains(cli.output, "EVENT_NOT_DECLARED") {
			t.Fatalf("private CLI input %s: %v\n%s", event, cli.err, cli.output)
		}
	}
	if err := process.rpc.call(ctx, "run.list", map[string]any{"bundle_hash": hash}, &after); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("rejected input changed public run inventory: before=%s after=%s", before, after)
	}
}

func assertSelectedRootConnectedReadbacks(t *testing.T, process *releaseServeProcess, runID, binary, cwd, config, token string, env []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
	defer cancel()
	recorded, err := listGoldenEvents(ctx, process.rpc, runID)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]goldenEvent{}
	for _, event := range recorded {
		want[event.EventID] = event
	}
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, "ws"+strings.TrimPrefix(process.apiBase, "http")+"/v1/ws", http.Header{"Authorization": {"Bearer " + goldenAPIToken}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err := conn.SetReadDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetWriteDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": "observe", "method": "event.subscribe", "params": map[string]any{
		"filter": map[string]any{"run_id": runID}, "replay_since": time.Unix(0, 0).UTC().Format(time.RFC3339Nano),
	}}); err != nil {
		t.Fatal(err)
	}
	for len(want) > 0 {
		var message struct {
			ID     json.RawMessage `json:"id"`
			Error  json.RawMessage `json:"error"`
			Method string          `json:"method"`
			Params struct {
				Result goldenEvent `json:"result"`
			} `json:"params"`
		}
		if err := conn.ReadJSON(&message); err != nil {
			t.Fatalf("WebSocket replay left %d recorded events unseen: %v", len(want), err)
		}
		if len(message.Error) > 0 && string(message.Error) != "null" {
			t.Fatalf("event subscription: %s", message.Error)
		}
		if len(message.ID) > 0 {
			continue
		}
		got := message.Params.Result
		expected, exists := want[got.EventID]
		if !exists {
			continue
		} // At-least-once notifications can repeat.
		if message.Method != "rpc.subscription" || got.EventName != expected.EventName || got.RunID != runID || !reflect.DeepEqual(got.Payload, expected.Payload) {
			t.Fatalf("WebSocket record differs from HTTP record: got=%#v want=%#v", got, expected)
		}
		delete(want, got.EventID)
	}
	for _, event := range recorded {
		if !strings.Contains(event.EventName, "candidate.requested") {
			continue
		}
		read := runReleaseCommand(t, goldenStartupTimeout, cwd, env, "", binary,
			"event", "view", event.EventID, "--config", config, "--api-server", process.apiBase, "--api-token-file", token)
		if read.err != nil || !strings.Contains(read.output, event.EventID) || !strings.Contains(read.output, event.EventName) {
			t.Fatalf("connected event CLI readback: %v\n%s", read.err, read.output)
		}
		return
	}
	t.Fatal("connected private-template request missing from recorded execution")
}

func assertFullEventPayloadReadback(t *testing.T, process *releaseServeProcess, expected goldenEvent, binary, cwd, config, token string, env []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
	defer cancel()
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, "ws"+strings.TrimPrefix(process.apiBase, "http")+"/v1/ws", http.Header{"Authorization": {"Bearer " + goldenAPIToken}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err := conn.SetReadDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetWriteDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": "full-payload", "method": "event.subscribe", "params": map[string]any{
		"filter": map[string]any{"run_id": expected.RunID}, "replay_since": time.Unix(0, 0).UTC().Format(time.RFC3339Nano),
	}}); err != nil {
		t.Fatal(err)
	}
	for {
		var response struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Error  json.RawMessage `json:"error"`
			Params struct {
				Result goldenEvent `json:"result"`
			} `json:"params"`
		}
		if err := conn.ReadJSON(&response); err != nil {
			t.Fatal(err)
		}
		if len(response.Error) != 0 && string(response.Error) != "null" {
			t.Fatalf("WebSocket subscription: %s", response.Error)
		}
		if len(response.ID) != 0 || response.Params.Result.EventID != expected.EventID {
			continue
		}
		got := response.Params.Result
		if response.Method != "rpc.subscription" || got.EventName != expected.EventName || got.RunID != expected.RunID || !reflect.DeepEqual(got.Payload, expected.Payload) {
			t.Fatalf("WebSocket changed complete HTTP payload: %+v", response)
		}
		break
	}
	read := runReleaseCommand(t, goldenStartupTimeout, cwd, env, "", binary, "event", "view", expected.EventID,
		"--config", config, "--api-server", process.apiBase, "--api-token-file", token)
	payload, err := json.Marshal(expected.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if read.err != nil || !strings.Contains(read.output, string(payload)) {
		t.Fatalf("CLI changed complete event payload: %v\n%s", read.err, read.output)
	}
}
