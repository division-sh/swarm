package apiv1

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apispec"
	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/store"
	"github.com/gorilla/websocket"
)

const webSocketRuntimeProbeTestName = "TestOpenRPCWebSocketRuntimeProbes"

func TestOpenRPCWebSocketRuntimeProbes(t *testing.T) {
	root := repoRoot(t)
	api := loadComplianceAPISpec(t, root)
	openRPC, _ := loadComplianceOpenRPC(t, complianceOpenRPCPath(root))
	matrix := loadComplianceMatrix(t, filepath.Join(root, "internal", "apiv1", "testdata", "openrpc_compliance_matrix.yaml"))

	wsMethods, httpMethods := transportAdmissionRuntimeMethods(t, api, openRPC, matrix)
	assertStringList(t, "websocket runtime method set", wsMethods, approvedWebSocketRuntimeMethods())
	assertStringList(t, "http runtime transport method set", httpMethods, approvedHTTPRuntimeTransportMethods())
	assertWebSocketRuntimeMatrixProofRefs(t, api, matrix, wsMethods)

	t.Run("http rejects websocket methods before params validation or dispatch", func(t *testing.T) {
		base := time.Unix(1700001400, 0).UTC()
		handler, calls := newWebSocketRuntimeProbeHandler(t, webSocketRuntimeProbeObservability(base), time.Hour)
		fixtures := webSocketRuntimeProbeFixtures(base)
		for _, methodName := range wsMethods {
			methodName := methodName
			t.Run(methodName, func(t *testing.T) {
				params := cloneProbeParams(fixtures[methodName].Params)
				status, resp, body := callTransportProbeHTTP(t, handler, methodName, params)
				if status != http.StatusOK {
					t.Fatalf("status = %d, want 200 body=%s", status, body)
				}
				assertTransportProbeMethodNotFound(t, methodName, resp)
				if calls[methodName] != 0 {
					t.Fatalf("%s handler calls = %d, want 0 for wrong HTTP transport", methodName, calls[methodName])
				}
			})
		}
	})

	t.Run("websocket rejects http methods before params validation or dispatch", func(t *testing.T) {
		base := time.Unix(1700001400, 0).UTC()
		handler, calls := newWebSocketRuntimeProbeHandler(t, webSocketRuntimeProbeObservability(base), time.Hour)
		server := httptest.NewServer(handler)
		defer server.Close()
		conn := dialTestWS(t, server.URL)
		defer conn.Close()

		for _, methodName := range httpMethods {
			writeWSRequest(t, conn, map[string]any{
				"jsonrpc": "2.0",
				"id":      webSocketRuntimeProbeID(methodName),
				"method":  methodName,
				"params":  map[string]any{},
			})
			resp := readWSResponse(t, conn)
			assertTransportProbeMethodNotFound(t, methodName, resp)
		}
		for _, methodName := range httpMethods {
			if calls[methodName] != 0 {
				t.Fatalf("%s handler calls = %d, want 0 for wrong WebSocket transport", methodName, calls[methodName])
			}
		}
	})

	t.Run("websocket auth boundary", func(t *testing.T) {
		base := time.Unix(1700001400, 0).UTC()
		handler, _ := newWebSocketRuntimeProbeHandler(t, webSocketRuntimeProbeObservability(base), time.Hour)
		server := httptest.NewServer(handler)
		defer server.Close()
		wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/ws"

		_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err == nil {
			t.Fatal("missing auth websocket dial unexpectedly succeeded")
		}
		if resp == nil || resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("missing auth response = %#v, want 401", resp)
		}

		_, resp, err = websocket.DefaultDialer.Dial(wsURL, http.Header{"Authorization": []string{"Bearer wrong"}})
		if err == nil {
			t.Fatal("invalid auth websocket dial unexpectedly succeeded")
		}
		if resp == nil || resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("invalid auth response = %#v, want 401", resp)
		}

		conn, _, err := websocket.DefaultDialer.Dial(wsURL, http.Header{"Authorization": []string{"Bearer " + testToken}})
		if err != nil {
			t.Fatalf("valid websocket dial: %v", err)
		}
		conn.Close()
	})

	fixtures := webSocketRuntimeProbeFixtures(time.Unix(1700001400, 0).UTC())
	for _, methodName := range wsMethods {
		methodName := methodName
		method := api.MethodCatalog[methodName]

		t.Run(methodName+"/unknown_params_key", func(t *testing.T) {
			base := time.Unix(1700001400, 0).UTC()
			handler, _ := newWebSocketRuntimeProbeHandler(t, webSocketRuntimeProbeObservability(base), time.Hour)
			server := httptest.NewServer(handler)
			defer server.Close()
			conn := dialTestWS(t, server.URL)
			defer conn.Close()

			params := cloneProbeParams(fixtures[methodName].Params)
			params["_unexpected"] = true
			writeWSRequest(t, conn, map[string]any{
				"jsonrpc": "2.0",
				"id":      webSocketRuntimeProbeID(methodName),
				"method":  methodName,
				"params":  params,
			})
			resp := readWSResponse(t, conn)
			assertTransportProbeInvalidParams(t, methodName, resp, "_unexpected")
		})

		for _, paramName := range requiredParamNames(method) {
			paramName := paramName
			t.Run(methodName+"/missing_required_"+paramName, func(t *testing.T) {
				base := time.Unix(1700001400, 0).UTC()
				handler, _ := newWebSocketRuntimeProbeHandler(t, webSocketRuntimeProbeObservability(base), time.Hour)
				server := httptest.NewServer(handler)
				defer server.Close()
				conn := dialTestWS(t, server.URL)
				defer conn.Close()

				params := cloneProbeParams(fixtures[methodName].Params)
				delete(params, paramName)
				writeWSRequest(t, conn, map[string]any{
					"jsonrpc": "2.0",
					"id":      webSocketRuntimeProbeID(methodName),
					"method":  methodName,
					"params":  params,
				})
				resp := readWSResponse(t, conn)
				assertTransportProbeInvalidParams(t, methodName, resp, paramName)
			})
		}
	}

	for _, methodName := range []string{"health.subscribe", "event.subscribe", "mailbox.subscribe", "run.subscribe_trace", "runtime.subscribe_logs"} {
		methodName := methodName
		t.Run(methodName+"/success_and_notification_envelope", func(t *testing.T) {
			base := time.Unix(1700001400, 0).UTC()
			handler, _ := newWebSocketRuntimeProbeHandler(t, webSocketRuntimeProbeObservability(base), time.Hour)
			server := httptest.NewServer(handler)
			defer server.Close()
			conn := dialTestWS(t, server.URL)
			defer conn.Close()

			writeWSRequest(t, conn, map[string]any{
				"jsonrpc": "2.0",
				"id":      webSocketRuntimeProbeID(methodName),
				"method":  methodName,
				"params":  webSocketRuntimeProbeFixtures(base)[methodName].Params,
			})
			resp := readWSResponse(t, conn)
			subscriptionID := assertWebSocketRuntimeSubscribeSuccess(t, methodName, resp)
			notification := readWSNotification(t, conn)
			assertWebSocketRuntimeNotification(t, methodName, subscriptionID, notification)
		})
	}

	t.Run("run.subscribe_trace declared RUN_NOT_FOUND", func(t *testing.T) {
		base := time.Unix(1700001400, 0).UTC()
		observability := webSocketRuntimeProbeObservability(base)
		observability.traceErr = store.ErrRunNotFound
		handler, _ := newWebSocketRuntimeProbeHandler(t, observability, time.Hour)
		server := httptest.NewServer(handler)
		defer server.Close()
		conn := dialTestWS(t, server.URL)
		defer conn.Close()

		writeWSRequest(t, conn, map[string]any{
			"jsonrpc": "2.0",
			"id":      webSocketRuntimeProbeID("run.subscribe_trace"),
			"method":  "run.subscribe_trace",
			"params": map[string]any{
				"run_id":       "missing-run",
				"replay_since": base.Format(time.RFC3339Nano),
			},
		})
		resp := readWSResponse(t, conn)
		assertWebSocketRuntimeApplicationError(t, "run.subscribe_trace", resp, RunNotFoundCode)
	})

	t.Run("rpc.unsubscribe same connection closeout", func(t *testing.T) {
		base := time.Unix(1700001400, 0).UTC()
		handler, _ := newWebSocketRuntimeProbeHandler(t, webSocketRuntimeProbeObservability(base), time.Hour)
		server := httptest.NewServer(handler)
		defer server.Close()
		conn := dialTestWS(t, server.URL)
		defer conn.Close()

		writeWSRequest(t, conn, map[string]any{
			"jsonrpc": "2.0",
			"id":      "ws-sub-for-unsubscribe",
			"method":  "health.subscribe",
			"params":  map[string]any{},
		})
		subscribe := readWSResponse(t, conn)
		subscriptionID := assertWebSocketRuntimeSubscribeSuccess(t, "health.subscribe", subscribe)
		notification := readWSNotification(t, conn)
		assertWebSocketRuntimeNotification(t, "health.subscribe", subscriptionID, notification)

		writeWSRequest(t, conn, map[string]any{
			"jsonrpc": "2.0",
			"id":      webSocketRuntimeProbeID("rpc.unsubscribe"),
			"method":  "rpc.unsubscribe",
			"params":  map[string]any{"subscription_id": subscriptionID},
		})
		unsubscribe := readWSResponse(t, conn)
		if unsubscribe.Error != nil {
			t.Fatalf("rpc.unsubscribe error = %#v, want success", unsubscribe.Error)
		}
		if result := asMap(t, unsubscribe.Result); result["ok"] != true {
			t.Fatalf("rpc.unsubscribe result = %#v, want ok true", unsubscribe.Result)
		}
	})
}

func TestMailboxWebSocketSubscriptionPreservesHumanTaskAnchorKind(t *testing.T) {
	base := time.Unix(1700001400, 0).UTC()
	state := &mutatingRuntimeProbeState{now: base}
	cards := newMutatingProbeDecisionCardStore(state)
	anchor, err := decisioncard.NewHumanTaskAnchor(decisioncard.HumanTaskAnchor{
		RequesterAgentID: "requester-agent", OperationID: "provider-turn/tool-call-1", Category: "review",
		Scope: decisioncard.Scope{Kind: decisioncard.ScopeFlow, FlowInstance: "provider/instance-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	cards.card.Anchor = anchor
	handler, _ := newWebSocketRuntimeProbeHandler(t, webSocketRuntimeProbeObservability(base), time.Hour, cards)
	server := httptest.NewServer(handler)
	defer server.Close()
	conn := dialTestWS(t, server.URL)
	defer conn.Close()
	writeWSRequest(t, conn, map[string]any{
		"jsonrpc": "2.0", "id": "human-task-mailbox-subscribe", "method": "mailbox.subscribe", "params": map[string]any{},
	})
	response := readWSResponse(t, conn)
	subscriptionID := assertWebSocketRuntimeSubscribeSuccess(t, "mailbox.subscribe", response)
	notification := readWSNotification(t, conn)
	assertWebSocketRuntimeNotification(t, "mailbox.subscribe", subscriptionID, notification)
	result := asMap(t, notification.Params.Result)
	if result["card_id"] != cards.card.CardID {
		t.Fatalf("human-task subscription card_id = %#v, want %q", result["card_id"], cards.card.CardID)
	}
	payload := asMap(t, result["payload"])
	if payload["anchor_kind"] != string(decisioncard.AnchorKindHumanTask) {
		t.Fatalf("human-task subscription payload = %#v", payload)
	}
}

func TestMailboxWebSocketSubscriptionProjectsProposedEffectDispatchState(t *testing.T) {
	base := time.Unix(1700001500, 0).UTC()
	state := &mutatingRuntimeProbeState{now: base}
	cards := newMutatingProbeDecisionCardStore(state)
	anchor, err := decisioncard.NewProposedEffectAnchor(decisioncard.ProposedEffectAnchor{
		RequestEventID: "00000000-0000-0000-0000-000000000303", ActivityID: "send_support_reply", Decision: "support_reply",
		Scope: decisioncard.Scope{Kind: decisioncard.ScopeEntity, FlowInstance: "root", EntityID: "entity-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	cards.card.Anchor = anchor
	handler, _ := newWebSocketRuntimeProbeHandler(t, webSocketRuntimeProbeObservability(base), time.Hour, cards)
	server := httptest.NewServer(handler)
	defer server.Close()
	conn := dialTestWS(t, server.URL)
	defer conn.Close()
	writeWSRequest(t, conn, map[string]any{
		"jsonrpc": "2.0", "id": "proposed-effect-mailbox-subscribe", "method": "mailbox.subscribe", "params": map[string]any{},
	})
	response := readWSResponse(t, conn)
	subscriptionID := assertWebSocketRuntimeSubscribeSuccess(t, "mailbox.subscribe", response)
	notification := readWSNotification(t, conn)
	assertWebSocketRuntimeNotification(t, "mailbox.subscribe", subscriptionID, notification)
	result := asMap(t, notification.Params.Result)
	effect := asMap(t, result["effect"])
	if effect["dispatch_state"] != "held" || effect["request_event_id"] != "00000000-0000-0000-0000-000000000303" {
		t.Fatalf("proposed-effect subscription dispatch state = %#v", effect)
	}
}

func transportAdmissionRuntimeMethods(t *testing.T, api *apispec.APISpecification, openRPC apispec.OpenRPCDocument, matrix openRPCComplianceMatrix) ([]string, []string) {
	t.Helper()
	openRPCMethods := map[string]struct{}{}
	for _, method := range openRPC.Methods {
		openRPCMethods[method.Name] = struct{}{}
	}
	matrixRows := map[string]openRPCMethodMatrix{}
	for _, row := range matrix.Methods {
		matrixRows[row.Method] = row
	}

	var wsMethods []string
	var httpMethods []string
	for methodName, method := range api.MethodCatalog {
		if _, ok := openRPCMethods[methodName]; !ok {
			t.Fatalf("%s missing from generated OpenRPC artifact", methodName)
		}
		row, ok := matrixRows[methodName]
		if !ok {
			t.Fatalf("%s missing from OpenRPC compliance matrix", methodName)
		}
		wantMatrixTransport := expectedComplianceTransport(methodName, method)
		if row.Transport != wantMatrixTransport {
			t.Fatalf("%s matrix transport = %q, want %q", methodName, row.Transport, wantMatrixTransport)
		}
		switch runtimeMethodTransport(methodName, method) {
		case transportWebSocket:
			if row.Transport != "ws" {
				t.Fatalf("%s runtime transport is websocket but matrix transport = %q", methodName, row.Transport)
			}
			wsMethods = append(wsMethods, methodName)
		case transportHTTP:
			if row.Transport != "http" {
				t.Fatalf("%s runtime transport is http but matrix transport = %q", methodName, row.Transport)
			}
			httpMethods = append(httpMethods, methodName)
		default:
			t.Fatalf("%s has unsupported runtime transport", methodName)
		}
	}
	sort.Strings(wsMethods)
	sort.Strings(httpMethods)
	return wsMethods, httpMethods
}

func approvedWebSocketRuntimeMethods() []string {
	return []string{
		"event.subscribe",
		"health.subscribe",
		"mailbox.subscribe",
		"rpc.unsubscribe",
		"run.subscribe_trace",
		"runtime.subscribe_logs",
	}
}

func approvedHTTPRuntimeTransportMethods() []string {
	out := append([]string{}, approvedReadOnlyHTTPRuntimeMethods()...)
	out = append(out, approvedMutatingHTTPRuntimeMethods()...)
	sort.Strings(out)
	return out
}

func assertWebSocketRuntimeMatrixProofRefs(t *testing.T, api *apispec.APISpecification, matrix openRPCComplianceMatrix, methods []string) {
	t.Helper()
	wsMethods := complianceStringSet(methods)
	rows := map[string]openRPCMethodMatrix{}
	for _, row := range matrix.Methods {
		rows[row.Method] = row
		if _, ok := wsMethods[row.Method]; !ok && rowHasWebSocketRuntimeProof(row) {
			t.Fatalf("%s has %s proof_ref but is outside the approved WebSocket runtime probe set", row.Method, webSocketRuntimeProbeTestName)
		}
	}

	for _, methodName := range methods {
		row, ok := rows[methodName]
		if !ok {
			t.Fatalf("%s missing from compliance matrix", methodName)
		}
		assertEvidenceHasWebSocketRuntimeProof(t, methodName, "happy_path", row.HappyPath)
		assertEvidenceHasWebSocketRuntimeProof(t, methodName, "unknown_top_level_param_validation", row.UnknownTopLevelParamValidation)
		assertEvidenceHasWebSocketRuntimeProof(t, methodName, "auth", row.Auth)
		assertEvidenceHasWebSocketRuntimeProof(t, methodName, "result_schema", row.ResultSchema)
		if len(requiredParamNames(api.MethodCatalog[methodName])) > 0 {
			assertEvidenceHasWebSocketRuntimeProof(t, methodName, "required_param_validation", row.RequiredParamValidation)
		}
		if len(api.MethodCatalog[methodName].Errors) > 0 {
			assertEvidenceHasWebSocketRuntimeProof(t, methodName, "declared_error_tests", row.DeclaredErrorTests)
		}
	}
}

func assertEvidenceHasWebSocketRuntimeProof(t *testing.T, methodName, field string, evidence complianceEvidence) {
	t.Helper()
	if !evidenceHasGoTest(evidence, webSocketRuntimeProbeTestName) {
		t.Fatalf("%s %s missing go_test proof_ref %s", methodName, field, webSocketRuntimeProbeTestName)
	}
}

func rowHasWebSocketRuntimeProof(row openRPCMethodMatrix) bool {
	for _, evidence := range complianceEvidenceFields(row) {
		if evidenceHasGoTest(evidence.evidence, webSocketRuntimeProbeTestName) {
			return true
		}
	}
	return false
}

func newWebSocketRuntimeProbeHandler(t *testing.T, observability *fakeObservabilityReadStore, healthInterval time.Duration, cardStores ...decisioncard.Store) (*Handler, map[string]int) {
	t.Helper()
	if observability == nil {
		observability = webSocketRuntimeProbeObservability(time.Unix(1700001400, 0).UTC())
	}
	if healthInterval <= 0 {
		healthInterval = time.Hour
	}
	var cards decisioncard.Store = newMutatingProbeDecisionCardStore(&mutatingRuntimeProbeState{now: time.Unix(1700001410, 0).UTC()})
	if len(cardStores) > 0 && cardStores[0] != nil {
		cards = cardStores[0]
	}
	readOpts := OperatorReadOptions{
		Now:           func() time.Time { return time.Unix(1700001410, 0).UTC() },
		Ready:         func() bool { return true },
		Database:      fakePinger{err: nil},
		Observability: observability,
		DecisionCards: cards,
		Bundle: runtimecontracts.BundleIdentity{
			WorkflowName:    "review",
			WorkflowVersion: "1.2.3",
			Fingerprint:     "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
	}
	registry := testRegistry(t)
	calls := map[string]int{}
	handlers := map[string]MethodHandler{}
	for _, methodName := range registry.MethodNames() {
		method, ok := registry.Method(methodName)
		if !ok {
			t.Fatalf("%s missing from registry", methodName)
		}
		if runtimeMethodTransport(methodName, method) != transportHTTP {
			continue
		}
		methodName := methodName
		handlers[methodName] = func(context.Context, Request) (any, error) {
			calls[methodName]++
			return map[string]any{"ok": true}, nil
		}
	}
	return testHandler(t, Options{
		AuthTokens:    []string{testToken},
		Handlers:      handlers,
		Subscriptions: OperatorSubscriptions(readOpts, SubscriptionRuntimeOptions{HealthInterval: healthInterval, PollInterval: time.Hour, QueueSize: 16}),
	}), calls
}

func webSocketRuntimeProbeObservability(base time.Time) *fakeObservabilityReadStore {
	return &fakeObservabilityReadStore{
		events: map[string]store.OperatorEventFull{
			"evt-1": {
				EventID:       "evt-1",
				EventName:     "scan.requested",
				ExecutionMode: "live",
				RunID:         "run-1",
				EntityID:      "entity-1",
				CreatedAt:     base.Add(time.Second),
				Source:        "runtime",
				ProducerType:  events.EventProducerPlatform,
				Payload:       map[string]any{"ok": true},
				Deliveries:    []store.OperatorEventDelivery{},
				DeadLetters:   []store.OperatorDeadLetterRecord{},
			},
		},
		traceRows: map[string][]store.RunDebugTraceRow{
			"run-1": {{
				EventID:        "evt-1",
				EventName:      "scan.requested",
				EventCreatedAt: base.Add(time.Second),
			}},
		},
		logs: []store.OperatorRuntimeLogEntry{{
			LogID:     "log-1",
			TS:        base.Add(time.Second),
			Level:     "error",
			Component: "scheduler",
			Source:    "agent-1",
			RunID:     "run-1",
			EntityID:  "entity-1",
			SessionID: "sess-1",
			ErrorCode: "E_RUNTIME",
			Message:   "runtime failed",
		}},
	}
}

type webSocketRuntimeProbeFixture struct {
	Params map[string]any
}

func webSocketRuntimeProbeFixtures(base time.Time) map[string]webSocketRuntimeProbeFixture {
	return map[string]webSocketRuntimeProbeFixture{
		"health.subscribe": {Params: map[string]any{}},
		"event.subscribe": {Params: map[string]any{
			"filter":       map[string]any{"run_id": "run-1"},
			"replay_since": base.Format(time.RFC3339Nano),
		}},
		"mailbox.subscribe": {Params: map[string]any{}},
		"run.subscribe_trace": {Params: map[string]any{
			"run_id":       "run-1",
			"replay_since": base.Format(time.RFC3339Nano),
		}},
		"runtime.subscribe_logs": {Params: map[string]any{
			"run_id":       "run-1",
			"entity_id":    "entity-1",
			"session_id":   "sess-1",
			"component":    "scheduler",
			"level":        "error",
			"error_code":   "E_RUNTIME",
			"source":       "agent-1",
			"replay_since": base.Format(time.RFC3339Nano),
		}},
		"rpc.unsubscribe": {Params: map[string]any{"subscription_id": "sub-1"}},
	}
}

func callTransportProbeHTTP(t *testing.T, handler *Handler, methodName string, params map[string]any) (int, rpcResponse, string) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      webSocketRuntimeProbeID(methodName),
		"method":  methodName,
		"params":  params,
	})
	if err != nil {
		t.Fatalf("marshal %s transport probe request: %v", methodName, err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/rpc", bytes.NewReader(raw))
	req.Header.Set("Authorization", "Bearer "+testToken)
	handler.ServeHTTP(rec, testAuthorActivityRequest(req))

	if rec.Code != http.StatusOK {
		return rec.Code, rpcResponse{}, rec.Body.String()
	}
	var resp rpcResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %s transport probe response: %v body=%s", methodName, err, rec.Body.String())
	}
	return rec.Code, resp, rec.Body.String()
}

func assertTransportProbeMethodNotFound(t *testing.T, methodName string, resp rpcResponse) {
	t.Helper()
	if resp.Error == nil {
		t.Fatalf("%s error = nil, want method not found", methodName)
	}
	if resp.Error.Code != codeMethodNotFound {
		t.Fatalf("%s error code = %d, want %d: %#v", methodName, resp.Error.Code, codeMethodNotFound, resp.Error)
	}
	data := asMap(t, resp.Error.Data)
	if _, ok := data["correlation_id"].(string); !ok {
		t.Fatalf("%s method-not-found data missing correlation_id: %#v", methodName, data)
	}
	details := asMap(t, data["details"])
	if details["method"] != methodName {
		t.Fatalf("%s method-not-found details = %#v, want method %q", methodName, details, methodName)
	}
}

func assertTransportProbeInvalidParams(t *testing.T, methodName string, resp rpcResponse, field string) {
	t.Helper()
	if resp.Error == nil || resp.Error.Code != codeInvalidParams {
		t.Fatalf("%s error = %#v, want invalid params", methodName, resp.Error)
	}
	details := asMap(t, asMap(t, resp.Error.Data)["details"])
	if details["field"] != field {
		t.Fatalf("%s invalid params details = %#v, want field %q", methodName, details, field)
	}
}

func assertWebSocketRuntimeSubscribeSuccess(t *testing.T, methodName string, resp rpcResponse) string {
	t.Helper()
	if resp.JSONRPC != jsonRPCVersion {
		t.Fatalf("%s jsonrpc = %q, want %q", methodName, resp.JSONRPC, jsonRPCVersion)
	}
	if resp.Error != nil {
		t.Fatalf("%s error = %#v, want success", methodName, resp.Error)
	}
	subscriptionID, ok := asMap(t, resp.Result)["subscription_id"].(string)
	if !ok || strings.TrimSpace(subscriptionID) == "" {
		t.Fatalf("%s result = %#v, want subscription_id", methodName, resp.Result)
	}
	return subscriptionID
}

func assertWebSocketRuntimeNotification(t *testing.T, methodName, subscriptionID string, notification rpcSubscriptionNotification) {
	t.Helper()
	if notification.JSONRPC != jsonRPCVersion || notification.Method != "rpc.subscription" {
		t.Fatalf("%s notification = %#v, want rpc.subscription", methodName, notification)
	}
	if notification.Params.Subscription != subscriptionID {
		t.Fatalf("%s notification subscription = %q, want %q", methodName, notification.Params.Subscription, subscriptionID)
	}
	if len(asMap(t, notification.Params.Result)) == 0 {
		t.Fatalf("%s notification result = %#v, want non-empty object", methodName, notification.Params.Result)
	}
}

func assertWebSocketRuntimeApplicationError(t *testing.T, methodName string, resp rpcResponse, code string) {
	t.Helper()
	if resp.Error == nil {
		t.Fatalf("%s error = nil, want %s", methodName, code)
	}
	data := asMap(t, resp.Error.Data)
	if data["code"] != code {
		t.Fatalf("%s application error data = %#v, want code %s", methodName, data, code)
	}
	if _, ok := data["correlation_id"].(string); !ok {
		t.Fatalf("%s application error missing correlation_id: %#v", methodName, data)
	}
}

func webSocketRuntimeProbeID(methodName string) string {
	return "ws-runtime-" + strings.ReplaceAll(methodName, ".", "-")
}
