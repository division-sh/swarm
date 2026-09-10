package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
)

type lifecycleTimerContenderDiagnostic struct {
	server                       lockedBuffer
	runID, entityID, seedEventID string
	due                          time.Time
	requestJSON, responseJSON    []byte
	status                       int
	err                          error
}

// Keep the existing PostgreSQL serve setup, but retain its output until the
// failure snapshot runs. Cleanup registration keeps reads ahead of shutdown/drop.
func startLifecycleTimerContenderDiagnostic(t *testing.T, root string) (servedControlProofRuntime, *lifecycleTimerContenderDiagnostic) {
	t.Helper()
	d := &lifecycleTimerContenderDiagnostic{}
	_, db, _ := installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle { return serveRuntimeWorkspaceStub{} })
	bundleHash := servedEventPublishFixtureBundleHash(t, root)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	ready := make(chan *runtimepkg.Runtime, 1)
	opts := cliapp.ServeOptions{
		ConfigPath: writeServeRuntimeTestConfig(t), SourceRoot: root,
		PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath),
		StoreMode:        "postgres", StoreModeSet: true,
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
		SelfCheck: true, Verbose: true, Output: &d.server,
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(),
		TestRuntimeReadyHook: func(rt *runtimepkg.Runtime) {
			select {
			case ready <- rt:
			default:
			}
		},
	}
	go func() { done <- runFrom(ctx, repoRootForTest(), opts) }()
	t.Cleanup(func() {
		cancel()
		select {
		case code := <-done:
			if code != 0 {
				t.Errorf("diagnostic serve exit=%d", code)
			}
		case <-time.After(servedProofPollDeadline):
			t.Error("diagnostic serve shutdown timed out")
		}
		if t.Failed() {
			t.Logf("server output after shutdown:\n%s", d.server.String())
		}
	})
	rt := servedControlProofRuntime{DB: db, Backend: "postgres", BundleHash: bundleHash}
	t.Cleanup(func() {
		if t.Failed() {
			d.dump(t, rt)
		}
	})
	waitForServeReadyLine(t, &d.server, done)
	select {
	case rt.Runtime = <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for generated serve runtime")
	}
	rt.Endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, d.server.String()) + "/v1/rpc"
	return rt, d
}

// Same request ID, auth, five-second HTTP timeout and envelope assertions as
// requestServedJSONRPC; only retain the full request/response bytes for failure.
func (d *lifecycleTimerContenderDiagnostic) decide(t *testing.T, endpoint string, params map[string]any) servedJSONRPCEnvelope {
	t.Helper()
	d.requestJSON, d.err = json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "mailbox.decide-proof", "method": "mailbox.decide", "params": params})
	if d.err != nil {
		t.Fatal(d.err)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint, bytes.NewReader(d.requestJSON))
	d.err = err
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	d.err = err
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	d.status = resp.StatusCode
	d.responseJSON, d.err = io.ReadAll(resp.Body)
	if d.err != nil {
		t.Fatal(d.err)
	}
	if d.status != http.StatusOK {
		t.Fatalf("mailbox.decide HTTP status = %d, want 200", d.status)
	}
	var reply servedJSONRPCEnvelope
	d.err = json.Unmarshal(d.responseJSON, &reply)
	if d.err != nil {
		t.Fatal(d.err)
	}
	return reply
}

func (d *lifecycleTimerContenderDiagnostic) dump(t *testing.T, rt servedControlProofRuntime) {
	t.Helper()
	t.Logf("timer contender failure BEFORE cleanup: run=%s entity=%s seed_event=%s due=%s HTTP=%d error=%v\nrequest=%s\nresponse=%s\nserver output:\n%s", d.runID, d.entityID, d.seedEventID, d.due.Format(time.RFC3339Nano), d.status, d.err, d.requestJSON, d.responseJSON, d.server.String())
	if d.runID == "" {
		return
	}
	queries := []struct{ name, sql string }{
		{"run", `SELECT * FROM runs WHERE run_id=$1`},
		{"entity", `SELECT * FROM entity_state WHERE run_id=$1`},
		{"transition_and_activation", `SELECT * FROM flow_instances WHERE run_id=$1 AND instance_path IN (SELECT flow_instance FROM entity_state WHERE run_id=$1)`},
		{"cards", `SELECT * FROM decision_cards WHERE run_id=$1`},
		{"card_changes", `SELECT * FROM decision_card_changes WHERE run_id=$1`},
		{"card_routes", `SELECT * FROM decision_card_route_obligations WHERE card_id IN (SELECT card_id FROM decision_cards WHERE run_id=$1)`},
		{"timers", `SELECT * FROM timers WHERE run_id=$1`},
		{"publications_and_runtime_logs", `SELECT * FROM events WHERE run_id=$1`},
		{"deliveries", `SELECT * FROM event_deliveries WHERE run_id=$1`},
		{"delivery_outcomes", `SELECT * FROM event_delivery_outcomes WHERE delivery_id IN (SELECT delivery_id FROM event_deliveries WHERE run_id=$1)`},
		{"receipts", `SELECT * FROM event_receipts WHERE event_id IN (SELECT event_id FROM events WHERE run_id=$1)`},
		{"api_completions", `SELECT * FROM api_idempotency WHERE resource_id=$1::text OR resource_id IN (SELECT card_id::text FROM decision_cards WHERE run_id=$1::uuid)`},
	}
	for _, query := range queries {
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			rows, err := rt.DB.QueryContext(ctx, `SELECT row_to_json(facts)::text FROM (`+query.sql+`) facts`, d.runID)
			if err != nil {
				t.Logf("diagnostic %s query error: %v", query.name, err)
				return
			}
			defer rows.Close()
			count := 0
			for rows.Next() {
				var raw string
				if err := rows.Scan(&raw); err != nil {
					t.Logf("diagnostic %s scan error: %v", query.name, err)
					return
				}
				t.Logf("diagnostic %s[%d]=%s", query.name, count, raw)
				count++
			}
			t.Logf("diagnostic %s rows=%d error=%v", query.name, count, rows.Err())
		}()
	}
	if rt.Endpoint != "" && d.entityID != "" {
		result := lifecycleContenderRPC(rt.Endpoint, "entity.get", map[string]any{"run_id": d.runID, "entity_id": d.entityID})
		raw, err := json.Marshal(result.reply)
		t.Logf("diagnostic public entity response=%s transport=%v encoding=%v", raw, result.err, err)
	}
	t.Logf("server output after failure reads:\n%s", d.server.String())
}
