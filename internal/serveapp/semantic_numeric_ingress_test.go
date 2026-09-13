package serveapp

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

func TestServedSemanticNumericIdempotencyReplay(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			root := semanticNumericIngressFixture(t)
			bundleHash := servedEventPublishFixtureBundleHash(t, root)
			unsetStoreSelectorEnv(t)
			stubServeRuntimeWorkspaceLifecycle(t)
			opts := cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
			var db *sql.DB
			if backend == servedparity.BackendExplicitPostgres {
				_, db, _ = installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle { return serveRuntimeWorkspaceStub{} })
				opts.ConfigPath = writeServeRuntimeTestConfig(t)
				opts.StoreMode = "postgres"
				opts.StoreModeSet = true
			} else {
				opts.ConfigPath = writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "semantic.db"))
			}
			captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { db, _, _ = selectedRuntimeStoreForTest(t, p) })
			start := func() (*serveRuntimeTestProcess, string) {
				p := startServeRuntimeTestProcess(t, opts)
				p.waitForReadyLine()
				return p, "http://" + serveRuntimeAPIListenerFromOutput(t, p.outputString()) + "/v1/rpc"
			}
			process, endpoint := start()
			type replayCase struct {
				method, transport, run, key string
				result                      json.RawMessage
			}
			var cases []replayCase
			for _, method := range []string{"event.publish", "run.start"} {
				for _, transport := range []string{"http"} {
					c := replayCase{method: method, transport: transport, run: uuid.NewString(), key: uuid.NewString()}
					body := semanticNumericRequest(bundleHash, c.method, c.run, c.key, "7")
					resp := semanticNumericRPC(t, endpoint, c.transport, body)
					if resp.Error != nil {
						t.Fatalf("%s/%s: %#v", method, transport, resp.Error)
					}
					c.result = resp.Result
					var result struct {
						RunID string `json:"run_id"`
					}
					if err := json.Unmarshal(resp.Result, &result); err != nil || result.RunID == "" {
						t.Fatalf("run result %s: %v", resp.Result, err)
					}
					c.run = result.RunID
					for _, equivalent := range []string{"7.0", "7e0"} {
						replay := semanticNumericRPC(t, endpoint, c.transport, semanticNumericRequest(bundleHash, c.method, c.run, c.key, equivalent))
						requireSemanticReplay(t, c.result, replay)
					}
					semanticNumericOutput(t, endpoint, db, c.run)
					conflict := semanticNumericRPC(t, endpoint, c.transport, semanticNumericRequest(bundleHash, c.method, c.run, c.key, "8"))
					if conflict.Error == nil || conflict.Error.Data["code"] != apiv1.IdempotencyConflictCode {
						t.Fatalf("conflict = %#v", conflict)
					}
					semanticNumericOutput(t, endpoint, db, c.run)
					cases = append(cases, c)
				}
			}
			if code := process.stop(); code != 0 {
				t.Fatalf("stop = %d: %s", code, process.outputString())
			}
			process, endpoint = start()
			for _, c := range cases {
				replay := semanticNumericRPC(t, endpoint, c.transport, semanticNumericRequest(bundleHash, c.method, c.run, c.key, "7e0"))
				requireSemanticReplay(t, c.result, replay)
				semanticNumericOutput(t, endpoint, db, c.run)
			}
			if code := process.stop(); code != 0 {
				t.Fatalf("restart stop=%d", code)
			}
		})
	}
}

func TestServedSemanticNumericWebSocketCommandRejection(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, semanticNumericIngressFixture(t))
			before := semanticNumericDomainCounts(t, rt.DB)
			for _, method := range []string{"event.publish", "run.start"} {
				t.Run(method, func(t *testing.T) {
					response := semanticNumericRPC(t, rt.Endpoint, "ws", semanticNumericRequest(rt.BundleHash, method, uuid.NewString(), uuid.NewString(), "7.0"))
					if response.Error == nil || response.Error.Code != -32601 {
						t.Fatalf("wrong-transport response = %#v", response)
					}
					details, _ := response.Error.Data["details"].(map[string]any)
					if details["method"] != method || details["transport"] != "websocket" {
						t.Fatalf("transport details = %#v", details)
					}
					if got := semanticNumericDomainCounts(t, rt.DB); !reflect.DeepEqual(got, before) {
						t.Fatalf("wrong-transport mutation: before=%v after=%v", before, got)
					}
				})
			}
		})
	}
}

func TestServedSemanticIngressRejectsBeforeMutation(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, semanticNumericIngressFixture(t))
			before := semanticNumericDomainCounts(t, rt.DB)
			for _, transport := range []string{"http", "ws"} {
				for _, hostile := range []string{"9007199254740992", "-9007199254740992", "-0", "1e999", "1e-999", "NaN"} {
					response := semanticNumericRPC(t, rt.Endpoint, transport, semanticNumericRequest(rt.BundleHash, "run.start", uuid.NewString(), uuid.NewString(), hostile))
					if response.Error == nil || response.Error.Code == -32601 {
						t.Fatalf("%s/%s did not fail envelope admission: %#v", transport, hostile, response)
					}
					if got := semanticNumericDomainCounts(t, rt.DB); !reflect.DeepEqual(got, before) {
						t.Fatalf("hostile envelope mutation: before=%v after=%v", before, got)
					}
				}
				duplicate := strings.Replace(semanticNumericRequest(rt.BundleHash, "run.start", uuid.NewString(), uuid.NewString(), "7"), `"value":7`, `"value":7,"value":8`, 1)
				if response := semanticNumericRPC(t, rt.Endpoint, transport, duplicate); response.Error == nil || response.Error.Code == -32601 {
					t.Fatalf("duplicate key admission = %#v", response)
				}
				if got := semanticNumericDomainCounts(t, rt.DB); !reflect.DeepEqual(got, before) {
					t.Fatalf("duplicate key mutation: %v -> %v", before, got)
				}
			}
		})
	}
}

func TestServedSemanticNumericEventSubscription(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rt := startServedTestSetupEntitiesProofRuntimeFromSource(t, backend, semanticNumericIngressFixture(t))
			watermark := time.Now().UTC().Add(-time.Second)
			response := semanticNumericRPC(t, rt.Endpoint, "http", semanticNumericRequest(rt.BundleHash, "run.start", uuid.NewString(), uuid.NewString(), "7e0"))
			if response.Error != nil {
				t.Fatalf("publication: %#v", response.Error)
			}
			var started struct {
				RunID string `json:"run_id"`
			}
			if err := json.Unmarshal(response.Result, &started); err != nil {
				t.Fatal(err)
			}
			semanticNumericOutput(t, rt.Endpoint, rt.DB, started.RunID)
			url := "ws" + strings.TrimPrefix(strings.TrimSuffix(rt.Endpoint, "/v1/rpc"), "http") + "/v1/ws"
			conn, _, err := websocket.DefaultDialer.Dial(url, http.Header{"Authorization": []string{"Bearer " + apiv1.DefaultLoopbackAPIToken}})
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if err := conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": "subscribe", "method": "event.subscribe", "params": map[string]any{"filter": map[string]any{"run_id": started.RunID}, "replay_since": watermark.Format(time.RFC3339Nano)}}); err != nil {
				t.Fatal(err)
			}
			var ack servedJSONRPCEnvelope
			if err := conn.ReadJSON(&ack); err != nil {
				t.Fatal(err)
			}
			if ack.Error != nil {
				t.Fatalf("subscribe: %#v", ack.Error)
			}
			var sub struct {
				ID string `json:"subscription_id"`
			}
			if err := json.Unmarshal(ack.Result, &sub); err != nil || sub.ID == "" {
				t.Fatalf("subscription acknowledgment %s: %v", ack.Result, err)
			}
			found := false
			for !found {
				var notification struct {
					Method string `json:"method"`
					Params struct {
						Subscription string `json:"subscription"`
						Result       struct {
							RunID     string         `json:"run_id"`
							EventName string         `json:"event_name"`
							Payload   map[string]any `json:"payload"`
						} `json:"result"`
					} `json:"params"`
				}
				if err := conn.ReadJSON(&notification); err != nil {
					t.Fatal(err)
				}
				if notification.Method != "rpc.subscription" || notification.Params.Subscription != sub.ID {
					t.Fatalf("unexpected notification %#v", notification)
				}
				event := notification.Params.Result
				if event.RunID != started.RunID {
					t.Fatalf("subscription leaked run %s", event.RunID)
				}
				if event.EventName != "numeric.completed" {
					continue
				}
				want := map[string]any{"value": float64(15), "fraction": float64(8), "explicit_double": float64(8)}
				if !reflect.DeepEqual(event.Payload, want) {
					t.Fatalf("public semantic payload %#v, want %#v", event.Payload, want)
				}
				found = true
			}
		})
	}
}

func semanticNumericDomainCounts(t *testing.T, db *sql.DB) map[string]int {
	t.Helper()
	counts := map[string]int{}
	for _, table := range []string{"runs", "events", "event_deliveries", "entity_state", "api_idempotency"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		counts[table] = count
	}
	return counts
}

func semanticNumericIngressFixture(t *testing.T) string {
	t.Helper()
	return canonicalrouting.CopySemanticNumericIngress(t)
}

func semanticNumericRequest(bundleHash, method, run, key, number string) string {
	if method == "event.publish" {
		run = ""
	}
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"numeric","method":%q,"params":{"bundle_hash":%q,"run_id":%q,"idempotency_key":%q,"event_name":"numeric.requested","payload":{"value":%s,"nested":{"numbers":[%s,7.5]}}}}`, method, bundleHash, run, key, number, number)
}

func semanticNumericRPC(t *testing.T, endpoint, transport, body string) servedJSONRPCEnvelope {
	t.Helper()
	if transport == "http" {
		return requestServedRawJSONRPC(t, endpoint, body)
	}
	url := "ws" + strings.TrimPrefix(strings.TrimSuffix(endpoint, "/v1/rpc"), "http") + "/v1/ws"
	conn, _, err := websocket.DefaultDialer.Dial(url, http.Header{"Authorization": []string{"Bearer " + apiv1.DefaultLoopbackAPIToken}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte(body)); err != nil {
		t.Fatal(err)
	}
	var response servedJSONRPCEnvelope
	if err := conn.ReadJSON(&response); err != nil {
		t.Fatal(err)
	}
	return response
}

func requireSemanticReplay(t *testing.T, want json.RawMessage, got servedJSONRPCEnvelope) {
	t.Helper()
	if got.Error != nil {
		t.Fatalf("replay error: %#v", got.Error)
	}
	a, err := canonicaljson.HashRaw(want)
	if err != nil {
		t.Fatal(err)
	}
	b, err := canonicaljson.HashRaw(got.Result)
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Fatalf("replay changed result: %s -> %s", want, got.Result)
	}
}

func semanticNumericOutput(t *testing.T, endpoint string, db *sql.DB, run string) {
	t.Helper()
	var raw string
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		err := db.QueryRow(`SELECT CAST(payload AS TEXT) FROM events WHERE CAST(run_id AS TEXT)=$1 AND event_name='numeric.completed'`, run).Scan(&raw)
		if err == nil {
			break
		}
		if err != sql.ErrNoRows {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var payload map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes([]byte(raw), &payload); err != nil {
		t.Fatalf("numeric completion missing/invalid: %s, %v", raw, err)
	}
	projected, err := workflowexpr.ProjectCELValue(payload)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"value": int64(15), "fraction": float64(8), "explicit_double": float64(8)}
	if !reflect.DeepEqual(projected, want) {
		t.Fatalf("execution output = %#v, want %#v (raw %s)", projected, want, raw)
	}
	for _, event := range []string{"numeric.requested", "numeric.completed"} {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM events WHERE CAST(run_id AS TEXT)=$1 AND event_name=$2`, run, event).Scan(&n); err != nil || n != 1 {
			t.Fatalf("%s count=%d err=%v", event, n, err)
		}
	}
	// Check settlement after observing output, not before a future schedule fires.
	requireServedParitySettlementPostconditions(t, endpoint, db, "", run, servedparity.Scenario{
		ID: "semantic-numeric-ingress",
		Postconditions: []servedparity.Postcondition{
			servedparity.PostconditionNoNonTerminalDeliveries,
			servedparity.PostconditionNoPendingPipelineEvents,
			servedparity.PostconditionNoUnfiredDueTimers,
		},
	})
	var fields, state string
	if err := db.QueryRow(`SELECT CAST(fields AS TEXT), current_state FROM entity_state WHERE CAST(run_id AS TEXT)=$1`, run).Scan(&fields, &state); err != nil {
		t.Fatal(err)
	}
	var entity map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes([]byte(fields), &entity); err != nil {
		t.Fatal(err)
	}
	projectedFields, err := workflowexpr.ProjectCELValue(entity)
	if err != nil || state != "done" || projectedFields.(map[string]any)["score"] != int64(15) {
		t.Fatalf("consumer state=%s fields=%#v err=%v", state, projectedFields, err)
	}
}
