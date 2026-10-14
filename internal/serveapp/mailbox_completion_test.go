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
	storeselected "github.com/division-sh/swarm/internal/store/selected"
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

func gateCompletionRequestDomain(t *testing.T, reader receiverProofStateReader, params map[string]any) gateCompletionDomain {
	t.Helper()
	var principal string
	if err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		principal, err = storetest.ReadGateCompletionPrincipalStorage(ctx, reader)
		return err
	}); err != nil {
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

func gateCompletionRead(t *testing.T, reader receiverProofStateReader, phase, runID, cardID string, domain gateCompletionDomain) gateCompletionSnapshot {
	t.Helper()
	out := gateCompletionSnapshot{Phase: phase, Domain: domain, CardID: cardID}
	var card storetest.GateCompletionCardStorage
	err := reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		card, err = storetest.ReadGateCompletionCardStorage(ctx, reader, cardID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	out.Status, out.Verdict, out.DecidedBy, out.DecisionEventID, out.Anchor = card.Status, card.Verdict, card.DecidedBy, card.DecisionEventID, card.Anchor
	var flow storetest.GateCompletionFlowStorage
	err = reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		flow, err = storetest.ReadGateCompletionFlowStorage(ctx, reader, runID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	out.State, out.Accumulator, out.History = flow.State, flow.Accumulator, flow.Config
	{
		var err error
		out.DecisionEvents, err = readServedEventNameCountForRun(context.Background(), reader, runID, "mailbox.card_decided")
		if err != nil {
			t.Fatal(err)
		}
	}
	{
		var err error
		out.OutcomeEvents, err = readServedEventNameCountForRun(context.Background(), reader, runID, "work.completed")
		if err != nil {
			t.Fatal(err)
		}
	}
	var changes [][3]string
	err = reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		changes, err = storetest.ReadGateCompletionChangesStorage(ctx, reader, cardID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range changes {
		out.Changes = append(out.Changes, row[0]+" "+row[1]+" "+row[2])
	}
	var receipts [][5]string
	err = reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		receipts, err = storetest.ReadGateCompletionReceiptsStorage(ctx, reader, runID)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range receipts {
		out.Receipts = append(out.Receipts, row[0]+" "+row[1]+" "+row[2]+" "+row[3]+" "+row[4])
	}
	var api []storetest.GateCompletionAPIStorage
	err = reader.InspectSnapshot(context.Background(), func(ctx context.Context) error {
		var err error
		api, err = storetest.ReadGateCompletionAPIStorage(ctx, reader, cardID, domain.Method, domain.Key)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range api {
		out.API = append(out.API, gateCompletionRow(row))
	}
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
	var deliveryReader servedRunDeliveryReader
	var projected any
	captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) {
		deliveryReader = p.deps.DeliveryStore.SummarizeRun
		projected = p.deps.EventStore
	})
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	opts := &cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
	var db *sql.DB
	var selectedPG *store.PostgresStore
	var selectedSQLite *store.SQLiteRuntimeStore
	captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { db, selectedPG, selectedSQLite = selectedRuntimeStoreForTest(t, p) })
	if backend == "sqlite" {
		opts.ConfigPath = writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "gate-completion.sqlite"))
	} else {
		dsn, _, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		original := buildStoresForServe
		buildStoresForServe = func(ctx context.Context, selection storebackend.Selection, cfg *config.Config) (*selectedStoreOwner, error) {
			return storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{Selection: selection, PostgresDSN: dsn, SessionLockTTL: runtimeSessionLockTTL(cfg)})
		}
		t.Cleanup(func() { buildStoresForServe = original })
		opts.ConfigPath = writeServeRuntimeTestConfig(t)
		opts.StoreMode, opts.StoreModeSet = "postgres", true
	}
	return opts, func() (*serveRuntimeTestProcess, servedControlProofRuntime) {
		p := startServeRuntimeTestProcess(t, *opts)
		p.waitForReadyLine()
		if projected == nil || backend == "sqlite" && projected != selectedSQLite || backend != "sqlite" && projected != selectedPG {
			t.Fatalf("gate completion fixture lost actual current owner: projected=%p postgres=%p sqlite=%p", projected, selectedPG, selectedSQLite)
		}
		return p, servedControlProofRuntime{ReadRunDeliveries: deliveryReader, Endpoint: "http://" + serveRuntimeAPIListenerFromOutput(t, p.outputString()) + "/v1/rpc", DB: db, Backend: backend, BundleHash: servedEventPublishFixtureBundleHash(t, root), Postgres: selectedPG, SQLite: selectedSQLite, ReceiverStateReader: requireReceiverProofStateReader(t, projected)}
	}
}

func TestServedMailboxCompletionInsertRollbackAndRestartBothStores(t *testing.T) {
	requireMailboxCompletionFaultFunction(t)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			opts, start := gateCompletionHarness(t, backend, canonicalrouting.CopyGateCompletionDiagnostic(t))
			first, rt := start()
			seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "work.requested", "bundle_hash": rt.BundleHash, "payload": map[string]any{"seed": true}, "idempotency_key": "completion-seed"})
			entityID := requireServedEventPublishEntityState(t, rt.ReceiverStateReader, backend, seed.RunID, "", "review")
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
			var cardID, hash string
			if err := rt.DB.QueryRow(`SELECT card_id, card_content_hash FROM decision_cards WHERE run_id=$1 AND status='pending'`, seed.RunID).Scan(&cardID, &hash); err != nil {
				t.Fatal(err)
			}
			params := map[string]any{"card_id": cardID, "verdict": "approve", "observed_content_hash": hash, "idempotency_key": "completion-insert"}
			domain := gateCompletionRequestDomain(t, rt.ReceiverStateReader, params)
			before := gateCompletionRead(t, rt.ReceiverStateReader, "before_insert_failure", seed.RunID, cardID, domain)
			assertCut, remove := installMailboxCompletionFaultWitness(t, rt, "completion-insert")
			response := gateCompletionHTTP(context.Background(), rt.Endpoint, params)
			if response.Err != nil || response.Envelope.Error == nil {
				t.Fatalf("completion INSERT failure must return an application error: %#v", response)
			}
			assertCut()
			after := gateCompletionRead(t, rt.ReceiverStateReader, "after_failed_completion_insert", seed.RunID, cardID, domain)
			if after.Status != before.Status || after.DecisionEventID != before.DecisionEventID || after.State != before.State || after.History != before.History || after.Accumulator != before.Accumulator || !reflect.DeepEqual(after.Changes, before.Changes) || after.DecisionEvents != 0 || after.OutcomeEvents != 1 || len(after.API) != 0 {
				t.Errorf("completion INSERT failure left partial domain or response state: before=%#v after=%#v", before, after)
			}
			remove()
			if code := first.stop(); code != 0 {
				t.Fatalf("first stop=%d", code)
			}
			setServeRuntimeRecovery(t, opts.ConfigPath, false, true)
			second, rt := start()
			firstSuccess := gateCompletionHTTP(context.Background(), rt.Endpoint, params)
			if firstSuccess.Err != nil || firstSuccess.Envelope.Error != nil {
				t.Fatalf("first execution after removing completion fault: %#v", firstSuccess)
			}
			requireServedEventPublishEntityState(t, rt.ReceiverStateReader, backend, seed.RunID, entityID, "done")
			waitServedRunDeliveryQuiescence(t, rt.ReadRunDeliveries, seed.RunID)
			committed := gateCompletionRead(t, rt.ReceiverStateReader, "first_success_after_restart", seed.RunID, cardID, domain)
			gateCompletionAssertResponse(t, firstSuccess, committed, false)
			replay := gateCompletionHTTP(context.Background(), rt.Endpoint, params)
			final := gateCompletionRead(t, rt.ReceiverStateReader, "exact_replay", seed.RunID, cardID, domain)
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
