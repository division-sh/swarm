package serveapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
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
	Method, ActorKind, Actor, Key, Hash, ResourceID, Response string
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

func gateCompletionRequestDomain(t *testing.T, db *sql.DB, params map[string]any) gateCompletionDomain {
	t.Helper()
	var principal string
	if err := db.QueryRow(`SELECT principal_id FROM operator_principals`).Scan(&principal); err != nil {
		t.Fatal(err)
	}
	raw, err := canonicaljson.Bytes(map[string]any{"method": "mailbox.decide", "params": params})
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(raw)
	key, _ := params["idempotency_key"].(string)
	return gateCompletionDomain{Method: "mailbox.decide", Actor: principal, Key: key, Hash: "sha256:" + hex.EncodeToString(hash[:])}
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
	err = rt.DB.QueryRow(`SELECT e.current_state, CAST(e.accumulator AS TEXT), CAST(f.config AS TEXT) FROM entity_state e JOIN flow_instances f ON e.run_id=f.run_id AND e.flow_instance=f.instance_path WHERE e.run_id=$1`, runID).Scan(&out.State, &out.Accumulator, &out.History)
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
	rows, err = rt.DB.Query(`SELECT method, actor_kind, actor_id, idempotency_key, request_hash, COALESCE(resource_id,''), CAST(response AS TEXT) FROM api_idempotency WHERE resource_id=$1 OR (method=$2 AND idempotency_key=$3) ORDER BY method, actor_kind, actor_id, idempotency_key`, cardID, domain.Method, domain.Key)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var row gateCompletionRow
		if err := rows.Scan(&row.Method, &row.ActorKind, &row.Actor, &row.Key, &row.Hash, &row.ResourceID, &row.Response); err != nil {
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
	if row.Method != snapshot.Domain.Method || row.ActorKind != "operator_principal" || row.Actor != snapshot.Domain.Actor || row.Key != snapshot.Domain.Key || row.Hash != snapshot.Domain.Hash || row.ResourceID != snapshot.CardID {
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

func TestServedMailboxCompletionInsertRollbackAndRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			opts, start := gateCompletionHarness(t, backend, canonicalrouting.CopyGateCompletionDiagnostic(t))
			first, rt := start()
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "completion-seed"})
			entityID := requireServedEventPublishEntityState(t, rt.DB, backend, seed.RunID, "", "review")
			waitServedRunDeliveryQuiescence(t, rt.DB, backend, seed.RunID)
			var cardID, hash string
			if err := rt.DB.QueryRow(`SELECT card_id, card_content_hash FROM decision_cards WHERE run_id=$1 AND status='pending'`, seed.RunID).Scan(&cardID, &hash); err != nil {
				t.Fatal(err)
			}
			params := map[string]any{"card_id": cardID, "verdict": "approve", "observed_content_hash": hash, "idempotency_key": "completion-insert"}
			domain := gateCompletionRequestDomain(t, rt.DB, params)
			before := gateCompletionRead(t, rt, "before_insert_failure", seed.RunID, cardID, domain)
			install := []string{`CREATE TRIGGER fail_mailbox_completion BEFORE INSERT ON api_idempotency WHEN NEW.method = 'mailbox.decide' BEGIN SELECT RAISE(ABORT, 'mailbox completion fault'); END`}
			remove := []string{`DROP TRIGGER fail_mailbox_completion`}
			if backend == "postgres" {
				install = []string{`CREATE FUNCTION fail_mailbox_completion() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.method = 'mailbox.decide' THEN RAISE EXCEPTION 'mailbox completion fault'; END IF; RETURN NEW; END $$`, `CREATE TRIGGER fail_mailbox_completion BEFORE INSERT ON api_idempotency FOR EACH ROW EXECUTE FUNCTION fail_mailbox_completion()`}
				remove = []string{`DROP TRIGGER fail_mailbox_completion ON api_idempotency`, `DROP FUNCTION fail_mailbox_completion()`}
			}
			for _, stmt := range install {
				if _, err := rt.DB.Exec(stmt); err != nil {
					t.Fatal(err)
				}
			}
			response := gateCompletionHTTP(context.Background(), rt.Endpoint, params)
			if response.Err != nil || response.Envelope.Error == nil {
				t.Fatalf("completion INSERT failure must return an application error: %#v", response)
			}
			after := gateCompletionRead(t, rt, "after_failed_completion_insert", seed.RunID, cardID, domain)
			if after.Status != before.Status || after.DecisionEventID != before.DecisionEventID || after.State != before.State || after.History != before.History || after.Accumulator != before.Accumulator || !reflect.DeepEqual(after.Changes, before.Changes) || after.DecisionEvents != 0 || after.OutcomeEvents != 1 || len(after.API) != 0 {
				t.Errorf("completion INSERT failure left partial domain or response state: before=%#v after=%#v", before, after)
			}
			for _, stmt := range remove {
				if _, err := rt.DB.Exec(stmt); err != nil {
					t.Fatal(err)
				}
			}
			if code := first.stop(); code != 0 {
				t.Fatalf("first stop=%d", code)
			}
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			second, rt := start()
			firstSuccess := gateCompletionHTTP(context.Background(), rt.Endpoint, params)
			if firstSuccess.Err != nil || firstSuccess.Envelope.Error != nil {
				t.Fatalf("first execution after removing completion fault: %#v", firstSuccess)
			}
			requireServedEventPublishEntityState(t, rt.DB, backend, seed.RunID, entityID, "done")
			waitServedRunDeliveryQuiescence(t, rt.DB, backend, seed.RunID)
			committed := gateCompletionRead(t, rt, "first_success_after_restart", seed.RunID, cardID, domain)
			gateCompletionAssertResponse(t, firstSuccess, committed, false)
			replay := gateCompletionHTTP(context.Background(), rt.Endpoint, params)
			final := gateCompletionRead(t, rt, "exact_replay", seed.RunID, cardID, domain)
			gateCompletionAssertResponse(t, replay, final, true)
			if committed.History != final.History || committed.Accumulator != final.Accumulator || !reflect.DeepEqual(committed.Changes, final.Changes) || final.DecisionEvents != 1 || final.OutcomeEvents != 2 {
				t.Error("exact replay mutated committed domain")
			}
			if code := second.stop(); code != 0 {
				t.Fatalf("second stop=%d", code)
			}
		})
	}
}
