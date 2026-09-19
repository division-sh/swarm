package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/events"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

// This is a real serve/HTTP/selected-store cancellation journey. The twenty
// durable obligations are a bounded fixture, not proof of authored creation or
// a worker executing under the seeded held claim. Those have separate proofs.
func TestIssue2394ServedStopTwentyBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rig := newIssue2394StopRig(t, backend)
			rt := rig.start(t)
			runID, eventID, _ := createServedControlWaitingRun(t, rt, "stop-twenty-"+uuid.NewString())
			requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": runID})
			seedIssue2394StopIntents(t, rt, runID, eventID)
			before := readIssue2394StopSnapshot(t, rt.DB, runID)
			assertIssue2394StopMatrix(t, before, false)

			key := uuid.NewString()
			body := fmt.Sprintf(`{"jsonrpc":"2.0","id":"stop-twenty","method":"run.stop","params":{"run_id":%q,"idempotency_key":%q}}`, runID, key)
			request, err := http.NewRequest(http.MethodPost, rt.Endpoint, strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/json")
			unauthorized, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
			if err != nil {
				t.Fatal(err)
			}
			unauthorized.Body.Close()
			if unauthorized.StatusCode != http.StatusUnauthorized {
				t.Fatalf("unauthenticated stop HTTP=%d, want 401", unauthorized.StatusCode)
			}
			assertIssue2394StopUnchanged(t, before, rt.DB, runID)
			wrongTransport := semanticNumericRPC(t, rt.Endpoint, "ws", body)
			if wrongTransport.Error == nil || wrongTransport.Error.Code != -32601 {
				t.Fatalf("WebSocket stop must be refused: %+v", wrongTransport)
			}
			assertIssue2394StopUnchanged(t, before, rt.DB, runID)

			// Lose the successful server reply at a proxy, after the actual HTTP
			// command completed, rather than issuing a second command on timeout.
			target, err := url.Parse(rt.Endpoint)
			if err != nil {
				t.Fatal(err)
			}
			target.Path = ""
			proxy := httputil.NewSingleHostReverseProxy(target)
			proxy.ModifyResponse = func(response *http.Response) error {
				_, readErr := io.Copy(io.Discard, response.Body)
				return errors.Join(readErr, response.Body.Close(), errors.New("test drops stop response"))
			}
			proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) { w.WriteHeader(http.StatusBadGateway) }
			lostReply := httptest.NewServer(proxy)
			t.Cleanup(lostReply.Close)
			request, err = http.NewRequest(http.MethodPost, lostReply.URL+"/v1/rpc", strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
			response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != http.StatusBadGateway {
				t.Fatalf("lost reply HTTP=%d, want 502", response.StatusCode)
			}
			lostReply.Close()
			var stored []byte
			if err := rt.DB.QueryRow(`SELECT response FROM api_idempotency WHERE method='run.stop' AND idempotency_key=$1`, key).Scan(&stored); err != nil {
				t.Fatalf("stop must have committed despite lost response: %v", err)
			}
			var result struct {
				OK       bool `json:"ok"`
				Recovery struct {
					Status string `json:"status"`
				} `json:"recovery"`
			}
			if err := json.Unmarshal(stored, &result); err != nil || !result.OK || result.Recovery.Status != "complete" {
				t.Fatalf("stored stop response=%s err=%v", stored, err)
			}
			after := readIssue2394StopSnapshot(t, rt.DB, runID)
			assertIssue2394StopMatrix(t, after, true)
			if !reflect.DeepEqual(before.Outcomes, after.Outcomes) || after.EventCount != before.EventCount {
				t.Fatal("stop changed committed prefix event identities or fabricated suffix events")
			}
			if after.Revisions != before.Revisions+1 {
				t.Fatalf("stop revision delta=%d, want one atomic revision", after.Revisions-before.Revisions)
			}
			requireServedRunStatus(t, rt.Endpoint, runID, "cancelled")
			requireSemanticReplay(t, stored, requestServedRawJSONRPC(t, rt.Endpoint, body))
			assertIssue2394StopUnchanged(t, after, rt.DB, runID)
			rig.stop(t)
			rt = rig.start(t)
			requireSemanticReplay(t, stored, requestServedRawJSONRPC(t, rt.Endpoint, body))
			assertIssue2394StopUnchanged(t, after, rt.DB, runID)
			conflict := requestServedJSONRPC(t, rt.Endpoint, "run.stop", map[string]any{"run_id": uuid.NewString(), "idempotency_key": key})
			if conflict.Error == nil || conflict.Error.Data["code"] != apiv1.IdempotencyConflictCode {
				t.Fatalf("same key with different run must conflict: %+v", conflict)
			}
			fresh := requestServedJSONRPC(t, rt.Endpoint, "run.stop", map[string]any{"run_id": runID, "idempotency_key": uuid.NewString()})
			if fresh.Error == nil || fresh.Error.Data["code"] != apiv1.RunAlreadyTerminalCode {
				t.Fatalf("fresh stop is not committed replay: %+v", fresh)
			}
			assertIssue2394StopUnchanged(t, after, rt.DB, runID)
			requireServedControlAPIIdempotencyRows(t, rt.DB, rt.Backend, "run.stop", key, 1)
			rig.stop(t)
		})
	}
}

func TestIssue2394SelectedStopTwentyBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			rig := newIssue2394StopRig(t, backend)
			rt := rig.start(t)
			runID, eventID, _ := createServedControlWaitingRun(t, rt, "selected-stop-twenty-"+uuid.NewString())
			requireServedOKJSONRPC(t, rt.Endpoint, "run.pause", map[string]any{"run_id": runID})
			seedIssue2394StopIntents(t, rt, runID, eventID)
			_, err := rt.Runtime.RunControl.Stop(servedControlProofAuthorActivityContext(t, rt), runcontrol.TransitionRequest{RunID: runID})
			if err != nil {
				t.Fatalf("selected stop twenty: %v", err)
			}
			assertIssue2394StopMatrix(t, readIssue2394StopSnapshot(t, rt.DB, runID), true)
			rig.stop(t)
		})
	}
}

type issue2394StopRig struct {
	opts                cliapp.ServeOptions
	process             *serveRuntimeTestProcess
	db                  *sql.DB
	backend, bundleHash string
}

func newIssue2394StopRig(t *testing.T, backend servedparity.Backend) *issue2394StopRig {
	t.Helper()
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	root := writeServedEventPublishFollowUpFixture(t)
	rig := &issue2394StopRig{bundleHash: servedEventPublishFixtureBundleHash(t, root), backend: "sqlite"}
	rig.opts = cliapp.ServeOptions{
		SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath,
		APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true,
		TestLifecycleProbe:      lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout)),
		TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig(), TestLLMRuntime: servedNoopLLMRuntime{},
	}
	if backend == servedparity.BackendExplicitPostgres {
		_, rig.db, _ = installServeRuntimeEmptyPostgresTestStores(t, func() cliapp.ServeWorkspaceLifecycle { return serveRuntimeWorkspaceStub{} })
		rig.backend, rig.opts.StoreMode, rig.opts.StoreModeSet = "postgres", "postgres", true
		rig.opts.ConfigPath = writeServeRuntimeTestConfig(t)
	} else {
		rig.opts.ConfigPath = writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "stop-twenty.sqlite"))
	}
	captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { rig.db, _, _ = selectedRuntimeStoreForTest(t, p) })
	return rig
}

func (r *issue2394StopRig) start(t *testing.T) servedControlProofRuntime {
	t.Helper()
	r.process = startServeRuntimeTestProcess(t, r.opts)
	r.process.waitForReadyLine()
	r.process.mu.Lock()
	rt := r.process.runtime
	r.process.mu.Unlock()
	return servedControlProofRuntime{
		Endpoint: "http://" + serveRuntimeAPIListenerFromOutput(t, r.process.outputString()) + "/v1/rpc",
		DB:       r.db, Backend: r.backend, BundleHash: r.bundleHash, Runtime: rt,
		Probe: r.opts.TestLifecycleProbe.(*lifecycletest.Probe),
	}
}

func (r *issue2394StopRig) stop(t *testing.T) {
	t.Helper()
	if code := r.process.stop(); code != 0 {
		t.Fatalf("serve stop=%d: %s", code, r.process.outputString())
	}
}

func seedIssue2394StopIntents(t *testing.T, rt servedControlProofRuntime, runID, eventID string) {
	t.Helper()
	// Pausing through the real owner prevents the worker from executing these
	// fixture rows; cancellation itself still crosses the production HTTP path.
	requireServedRunControlState(t, rt.DB, rt.Backend, runID, "paused", "paused")
	var prefixes []string
	for i := 0; i < 4; i++ {
		published := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{
			"event_name": "item.processed", "run_id": runID, "source_event_id": eventID,
			"payload": map[string]any{"item_id": fmt.Sprintf("prefix-%d", i)}, "idempotency_key": uuid.NewString(),
		})
		prefixes = append(prefixes, published.EventID)
	}
	var deliveryID string
	if err := rt.DB.QueryRow(`SELECT delivery_id FROM event_deliveries WHERE event_id=$1 AND subscriber_type='node' ORDER BY delivery_id LIMIT 1`, eventID).Scan(&deliveryID); err != nil {
		t.Fatal(err)
	}
	producer, err := events.NewRootRoutingSource(uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	capsule, err := fanoutobligation.MarshalCapsule(fanoutobligation.Capsule{
		NodeKey: "root.item-handler", ExecutionFlowID: "root", Route: runtimeflowidentity.StoredRoute("root", "root", "root"),
		HandlerEventKey: "item.received", ProducerSource: producer,
		Lineage: events.EventLineage{RunID: runID, ParentEventID: eventID, ExecutionMode: executionmode.Live},
	})
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := runtimefailures.MarshalEnvelope(runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassTargetUnreachable, "not_found", "runtime.fan_out", "serve", nil), "runtime.fan_out", "serve"))
	if err != nil {
		t.Fatal(err)
	}
	retry, err := runtimefailures.MarshalEnvelope(runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassDependencyUnavailable, "dependency_down", "runtime.fan_out", "serve", nil), "runtime.fan_out", "serve"))
	if err != nil {
		t.Fatal(err)
	}
	tx, err := rt.DB.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	at := time.Now().UTC()
	for i := 0; i < 20; i++ {
		path := fmt.Sprintf("stop-fixture-%02d", i)
		status, cursor, generation := "open", 0, 0
		var claim, lease, reason, retryAt, retryFailure any
		switch i % 5 {
		case 1:
			cursor = 1
		case 2:
			claim, lease, generation = "held-fixture", at.Add(time.Hour), 7
		case 3:
			status, reason = "blocked", string(blocked)
		case 4:
			retryAt, retryFailure = at.Add(time.Hour), string(retry)
		}
		_, err := tx.Exec(`INSERT INTO fan_out_intents
		(run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,bundle_hash,semantic_digest,source_kind,source_event_id,source_field,cardinality,cursor,status,next_chunk_size,capsule,created_at,updated_at,claim_owner,claim_generation,lease_expires_at,blocked_reason,retry_ready_at,retry_failure)
		VALUES ($1,$2,'root','fan_out',$3,$4,$5,'event_payload_field',$6,'items',25,$7,$8,32,$9,$10,$10,$11,$12,$13,$14,$15,$16)`,
			runID, deliveryID, path, rt.BundleHash, "sha256:"+strings.Repeat("2", 64), eventID, cursor, status, string(capsule), at, claim, generation, lease, reason, retryAt, retryFailure)
		if err != nil {
			t.Fatal(err)
		}
		if cursor == 1 {
			if _, err := tx.Exec(`INSERT INTO fan_out_outcomes (run_id,triggering_delivery_id,flow_path,declaration_family,semantic_path,ordinal,outcome_kind,event_id,created_at) VALUES ($1,$2,'root','fan_out',$3,0,'committed',$4,$5)`, runID, deliveryID, path, prefixes[i/5], at); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

type issue2394StopIntent struct {
	Path                                          string
	Cardinality, Cursor                           int
	Status, Claim                                 string
	Generation                                    int
	Lease, Reason, RetryAt, RetryFailure, Updated string
}

type issue2394StopSnapshot struct {
	Intents                                                 []issue2394StopIntent
	Outcomes                                                []string
	EventCount, PendingDeliveries, Revisions, FactRevisions int
	RunStatus, ControlStatus, ControlUpdated                string
}

func readIssue2394StopSnapshot(t *testing.T, db *sql.DB, runID string) issue2394StopSnapshot {
	t.Helper()
	var snapshot issue2394StopSnapshot
	rows, err := db.Query(`SELECT semantic_path,cardinality,cursor,status,COALESCE(claim_owner,''),claim_generation,COALESCE(CAST(lease_expires_at AS TEXT),''),COALESCE(blocked_reason,''),COALESCE(CAST(retry_ready_at AS TEXT),''),COALESCE(CAST(retry_failure AS TEXT),''),CAST(updated_at AS TEXT) FROM fan_out_intents WHERE run_id=$1 ORDER BY semantic_path`, runID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var intent issue2394StopIntent
		if err := rows.Scan(&intent.Path, &intent.Cardinality, &intent.Cursor, &intent.Status, &intent.Claim, &intent.Generation, &intent.Lease, &intent.Reason, &intent.RetryAt, &intent.RetryFailure, &intent.Updated); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		snapshot.Intents = append(snapshot.Intents, intent)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	rows, err = db.Query(`SELECT semantic_path,ordinal,outcome_kind,CAST(event_id AS TEXT),CAST(created_at AS TEXT) FROM fan_out_outcomes WHERE run_id=$1 ORDER BY semantic_path,ordinal`, runID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var path, kind, eventID, created string
		var ordinal int
		if err := rows.Scan(&path, &ordinal, &kind, &eventID, &created); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		snapshot.Outcomes = append(snapshot.Outcomes, fmt.Sprintf("%s/%d/%s/%s/%s", path, ordinal, kind, eventID, created))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	for table, target := range map[string]*int{"events": &snapshot.EventCount, "run_fork_revisions": &snapshot.Revisions, "run_fork_fact_revisions": &snapshot.FactRevisions} {
		if err := db.QueryRow(`SELECT COUNT(*) FROM `+table+` WHERE run_id=$1`, runID).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('pending','processing','retrying')`, runID).Scan(&snapshot.PendingDeliveries); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT r.status,c.control_status,CAST(c.updated_at AS TEXT) FROM runs r JOIN run_control_state c ON c.run_id=r.run_id WHERE r.run_id=$1`, runID).Scan(&snapshot.RunStatus, &snapshot.ControlStatus, &snapshot.ControlUpdated); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func assertIssue2394StopUnchanged(t *testing.T, want issue2394StopSnapshot, db *sql.DB, runID string) {
	t.Helper()
	if got := readIssue2394StopSnapshot(t, db, runID); !reflect.DeepEqual(got, want) {
		t.Fatalf("stop replay/refusal changed durable state:\nwant=%+v\ngot=%+v", want, got)
	}
}

func assertIssue2394StopMatrix(t *testing.T, snapshot issue2394StopSnapshot, canceled bool) {
	t.Helper()
	if len(snapshot.Intents) != 20 || len(snapshot.Outcomes) != 4 {
		t.Fatalf("intent/outcome counts=%d/%d, want 20/4", len(snapshot.Intents), len(snapshot.Outcomes))
	}
	for i, intent := range snapshot.Intents {
		wantCursor := 0
		if i%5 == 1 {
			wantCursor = 1
		}
		if intent.Cardinality != 25 || intent.Cursor != wantCursor {
			t.Fatalf("prefix/cardinality changed: %+v", intent)
		}
		if canceled {
			if intent.Status != "canceled" || intent.Reason != "run_stopped" || intent.Claim != "" || intent.Lease != "" || intent.RetryAt != "" || intent.RetryFailure != "" {
				t.Fatalf("uncanceled suffix/operational state: %+v", intent)
			}
			continue
		}
		if i%5 == 3 {
			if intent.Status != "blocked" || intent.Reason == "" {
				t.Fatalf("blocked fixture missing: %+v", intent)
			}
		} else if intent.Status != "open" {
			t.Fatalf("open fixture missing: %+v", intent)
		}
		if (intent.Claim != "") != (i%5 == 2) || (intent.Lease != "") != (i%5 == 2) || (intent.RetryAt != "") != (i%5 == 4) || (intent.RetryFailure != "") != (i%5 == 4) {
			t.Fatalf("fixture state partition differs: %+v", intent)
		}
	}
	if canceled && (snapshot.RunStatus != "cancelled" || snapshot.ControlStatus != "stopped" || snapshot.PendingDeliveries != 0) {
		t.Fatalf("stop did not settle run work: %+v", snapshot)
	}
}
