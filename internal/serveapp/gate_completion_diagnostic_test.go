package serveapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

type gateCompletionDomain struct {
	Method, Actor, Key, Hash string
}

type gateCompletionRow struct {
	Method, Actor, Key, Hash, ResourceID, Response string
}

type gateCompletionSnapshot struct {
	Phase                                               string
	Domain                                              gateCompletionDomain
	CardID, Status, Verdict, DecidedBy, DecisionEventID string
	Anchor, State, Accumulator, History                 string
	DecisionEvents, OutcomeEvents                       int
	Changes, Receipts                                   []string
	API                                                 []gateCompletionRow
}

type gateCompletionHTTPResult struct {
	Envelope servedJSONRPCEnvelope
	Err      error
}

func gateCompletionRequestDomain(t *testing.T, params map[string]any) gateCompletionDomain {
	t.Helper()
	actor := sha256.Sum256([]byte(apiv1.DefaultLoopbackAPIToken))
	raw, err := canonicaljson.Bytes(map[string]any{"method": "mailbox.decide", "params": params})
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(raw)
	key, _ := params["idempotency_key"].(string)
	return gateCompletionDomain{Method: "mailbox.decide", Actor: "sha256:" + hex.EncodeToString(actor[:]), Key: key, Hash: "sha256:" + hex.EncodeToString(hash[:])}
}

// This returns transport failures separately from a received application error.
func gateCompletionHTTP(ctx context.Context, endpoint string, params map[string]any) gateCompletionHTTPResult {
	raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "gate-completion-diagnostic", "method": "mailbox.decide", "params": params})
	if err != nil {
		return gateCompletionHTTPResult{Err: err}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if err != nil {
		return gateCompletionHTTPResult{Err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	// Runtime shutdown at the durable handler barrier owns the interruption,
	// not a client timeout. The enclosing test timeout remains a safety bound.
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return gateCompletionHTTPResult{Err: err}
	}
	defer resp.Body.Close()
	var result gateCompletionHTTPResult
	result.Err = json.NewDecoder(resp.Body).Decode(&result.Envelope)
	return result
}

func gateCompletionRead(t *testing.T, rt servedControlProofRuntime, phase, runID, cardID string, domain gateCompletionDomain) gateCompletionSnapshot {
	t.Helper()
	out := gateCompletionSnapshot{Phase: phase, Domain: domain, CardID: cardID}
	err := rt.DB.QueryRow(`SELECT status, COALESCE(verdict,''), COALESCE(decided_by,''), COALESCE(CAST(decision_event_id AS TEXT),''), CAST(anchor AS TEXT) FROM decision_cards WHERE card_id=$1`, cardID).Scan(&out.Status, &out.Verdict, &out.DecidedBy, &out.DecisionEventID, &out.Anchor)
	if err != nil {
		t.Fatal(err)
	}
	err = rt.DB.QueryRow(`SELECT e.current_state, CAST(e.accumulator AS TEXT), CAST(f.config AS TEXT) FROM entity_state e JOIN flow_instances f ON e.flow_instance=f.instance_id WHERE e.run_id=$1`, runID).Scan(&out.State, &out.Accumulator, &out.History)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='mailbox.card_decided'`, runID).Scan(&out.DecisionEvents); err != nil {
		t.Fatal(err)
	}
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='work.completed'`, runID).Scan(&out.OutcomeEvents); err != nil {
		t.Fatal(err)
	}
	rows, err := rt.DB.Query(`SELECT CAST(change_id AS TEXT), change_type, CAST(payload AS TEXT) FROM decision_card_changes WHERE card_id=$1 ORDER BY change_id`, cardID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id, kind, payload string
		if err := rows.Scan(&id, &kind, &payload); err != nil {
			t.Fatal(err)
		}
		out.Changes = append(out.Changes, id+" "+kind+" "+payload)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	rows, err = rt.DB.Query(`SELECT event_id, subscriber_type, subscriber_id, outcome, reason_code FROM event_receipts WHERE event_id IN (SELECT event_id FROM events WHERE run_id=$1) ORDER BY event_id, subscriber_id`, runID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id, kind, subscriber, outcome, reason string
		if err := rows.Scan(&id, &kind, &subscriber, &outcome, &reason); err != nil {
			t.Fatal(err)
		}
		out.Receipts = append(out.Receipts, id+" "+kind+" "+subscriber+" "+outcome+" "+reason)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	// Include any wrong actor/hash domain associated with this card or key;
	// absence cannot be inferred from only the expected tuple.
	rows, err = rt.DB.Query(`SELECT method, actor_token_id, idempotency_key, request_hash, COALESCE(resource_id,''), CAST(response AS TEXT) FROM api_idempotency WHERE resource_id=$1 OR (method=$2 AND idempotency_key=$3) ORDER BY method, actor_token_id, idempotency_key`, cardID, domain.Method, domain.Key)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var row gateCompletionRow
		if err := rows.Scan(&row.Method, &row.Actor, &row.Key, &row.Hash, &row.ResourceID, &row.Response); err != nil {
			t.Fatal(err)
		}
		out.API = append(out.API, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("GATE_COMPLETION_RECORD %s", raw)
	return out
}

func gateCompletionAssertResponse(t *testing.T, result gateCompletionHTTPResult, snapshot gateCompletionSnapshot, replay bool) {
	t.Helper()
	if result.Err != nil || result.Envelope.Error != nil {
		t.Errorf("original-response replay=%t: transport=%v application=%#v", replay, result.Err, result.Envelope.Error)
		return
	}
	var response map[string]any
	if err := json.Unmarshal(result.Envelope.Result, &response); err != nil {
		t.Fatal(err)
	}
	if response["decision_event_id"] != snapshot.DecisionEventID || response["card_id"] != snapshot.CardID || response["idempotency_replayed"] != replay {
		t.Errorf("original-response identity/replay mismatch: response=%#v snapshot=%#v", response, snapshot)
	}
	if len(snapshot.API) != 1 {
		t.Errorf("exact API completion rows=%d, want one", len(snapshot.API))
		return
	}
	row := snapshot.API[0]
	if row.Method != snapshot.Domain.Method || row.Actor != snapshot.Domain.Actor || row.Key != snapshot.Domain.Key || row.Hash != snapshot.Domain.Hash || row.ResourceID != snapshot.CardID {
		t.Errorf("wrong stored completion domain: %#v want %#v", row, snapshot.Domain)
	}
	var stored map[string]any
	if err := json.Unmarshal([]byte(row.Response), &stored); err != nil {
		t.Fatal(err)
	}
	delete(response, "idempotency_replayed")
	if !reflect.DeepEqual(response, stored) {
		t.Errorf("HTTP response does not replay original stored response: response=%#v stored=%#v", response, stored)
	}
}

func gateCompletionHarness(t *testing.T, backend, root string) (*cliapp.ServeOptions, func() (*serveRuntimeTestProcess, servedControlProofRuntime)) {
	t.Helper()
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	opts := &cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
	var db *sql.DB
	if backend == "sqlite" {
		opts.ConfigPath = writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "gate-completion.sqlite"))
		captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { db, _, _ = selectedRuntimeStoreForTest(t, p) })
	} else {
		dsn, _, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		original := buildStoresForServe
		buildStoresForServe = func(_ context.Context, _ storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
			pg, err := store.NewPostgresStore(dsn)
			if err != nil {
				return nil, err
			}
			storetest.BootstrapPostgresRuntimeStore(t, pg)
			db = storetest.DatabaseForTest(pg)
			return openSelectedPostgresOwner(t, dsn, db, cfg), nil
		}
		t.Cleanup(func() { buildStoresForServe = original })
		opts.ConfigPath = writeServeRuntimeTestConfig(t)
		opts.StoreMode, opts.StoreModeSet = "postgres", true
	}
	return opts, func() (*serveRuntimeTestProcess, servedControlProofRuntime) {
		p := startServeRuntimeTestProcess(t, *opts)
		p.waitForReadyLine()
		return p, servedControlProofRuntime{Endpoint: "http://" + serveRuntimeAPIListenerFromOutput(t, p.outputString()) + "/v1/rpc", DB: db, Backend: backend, BundleHash: servedEventPublishFixtureBundleHash(t, root)}
	}
}

func TestServedGateCompletionInterruptionDiagnosticOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/interrupted_%t", backend, cut), func(t *testing.T) {
				opts, start := gateCompletionHarness(t, backend, canonicalrouting.CopyGateCompletionDiagnostic(t))
				reached := make(chan struct{}, 1)
				if cut {
					opts.TestWorkflowNodeHandlerStartHook = func(ctx context.Context, _ string, event events.Event) error {
						if event.Type() != "work.completed" {
							return nil
						}
						var payload struct {
							Result string `json:"result"`
						}
						if err := json.Unmarshal(event.Payload(), &payload); err != nil {
							return err
						}
						if payload.Result == "ready" {
							return nil
						}
						select {
						case reached <- struct{}{}:
						default:
						}
						<-ctx.Done()
						return ctx.Err()
					}
				}
				first, rt := start()
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "completion-seed"})
				entityID := requireServedEventPublishEntityState(t, rt.DB, backend, seed.RunID, "", "review")
				var cardID, hash string
				if err := rt.DB.QueryRow(`SELECT card_id,card_content_hash FROM decision_cards WHERE run_id=$1 AND status='pending'`, seed.RunID).Scan(&cardID, &hash); err != nil {
					t.Fatal(err)
				}
				params := map[string]any{"card_id": cardID, "verdict": "approve", "observed_content_hash": hash, "idempotency_key": "gate-completion"}
				domain := gateCompletionRequestDomain(t, params)
				initial := gateCompletionRead(t, rt, "before_domain_commit", seed.RunID, cardID, domain)
				if initial.Status != "pending" || initial.State != "review" || initial.DecisionEvents != 0 || len(initial.API) != 0 {
					t.Fatalf("invalid initial cut: %#v", initial)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan gateCompletionHTTPResult, 1)
				endpoint := rt.Endpoint
				go func() { done <- gateCompletionHTTP(ctx, endpoint, params) }()
				var firstReply gateCompletionHTTPResult
				if cut {
					select {
					case <-reached:
					case <-time.After(15 * time.Second):
						t.Fatal("durable outcome-consumer barrier not reached")
					}
					select {
					case reply := <-done:
						t.Fatalf("HTTP completed before semantic cut: %#v", reply)
					default:
					}
					committed := gateCompletionRead(t, rt, "domain_committed_before_dispatch_completion", seed.RunID, cardID, domain)
					if committed.Status != "decided" || committed.State != "approved" || committed.DecisionEventID == "" || committed.DecisionEvents != 1 || committed.OutcomeEvents != 2 {
						t.Fatalf("barrier lacks durable decision: %#v", committed)
					}
					if len(committed.API) != 1 {
						t.Errorf("atomic mailbox response absent at durable domain cut: rows=%d", len(committed.API))
					}
					t.Log("INTERRUPTION runtime shutdown begins after measured durable barrier; request context remains uncancelled")
				} else {
					select {
					case firstReply = <-done:
					case <-time.After(15 * time.Second):
						t.Fatal("acknowledged control did not answer")
					}
					requireServedEventPublishEntityState(t, rt.DB, backend, seed.RunID, entityID, "done")
					waitServedRunDeliveryQuiescence(t, rt.DB, backend, seed.RunID)
					ack := gateCompletionRead(t, rt, "acknowledged_success", seed.RunID, cardID, domain)
					gateCompletionAssertResponse(t, firstReply, ack, false)
				}
				if code := first.stop(); code != 0 {
					t.Fatalf("first stop=%d", code)
				}
				if cut {
					select {
					case firstReply = <-done:
					case <-time.After(5 * time.Second):
						t.Fatal("HTTP did not finish after runtime shutdown")
					}
					t.Logf("INTERRUPTED_HTTP transport=%v application=%#v request_context=%v", firstReply.Err, firstReply.Envelope.Error, ctx.Err())
				}
				opts.TestWorkflowNodeHandlerStartHook = nil
				setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
				second, rt := start()
				requireServedEventPublishEntityState(t, rt.DB, backend, seed.RunID, entityID, "done")
				waitServedRunDeliveryQuiescence(t, rt.DB, backend, seed.RunID)
				after := gateCompletionRead(t, rt, "after_restart_before_retry", seed.RunID, cardID, domain)
				retry := gateCompletionHTTP(ctx, rt.Endpoint, params)
				final := gateCompletionRead(t, rt, "after_same_key_retry", seed.RunID, cardID, domain)
				gateCompletionAssertResponse(t, retry, final, true)
				if after.Status != final.Status || after.DecisionEventID != final.DecisionEventID || after.History != final.History || after.Accumulator != final.Accumulator || !reflect.DeepEqual(after.Changes, final.Changes) || final.DecisionEvents != 1 || final.OutcomeEvents != 2 {
					t.Error("same-key retry mutated durable decision/outcome/history")
				}
				if !cut {
					conflict := map[string]any{"card_id": cardID, "verdict": "reject", "observed_content_hash": hash, "idempotency_key": "gate-completion"}
					reply := gateCompletionHTTP(ctx, rt.Endpoint, conflict)
					t.Logf("ACK_CONTROL same-key-conflict domain=%#v reply=%#v", gateCompletionRequestDomain(t, conflict), reply)
					if reply.Err != nil || reply.Envelope.Error == nil || reply.Envelope.Error.Data["code"] != "IDEMPOTENCY_CONFLICT" {
						t.Errorf("same-key conflict=%#v", reply)
					}
				}
				if code := second.stop(); code != 0 {
					t.Fatalf("second stop=%d", code)
				}
			})
		}
	}
}
