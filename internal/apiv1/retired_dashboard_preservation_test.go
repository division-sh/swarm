package apiv1

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimepipelineobligation "github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/store/storetest"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func withNativeObservabilityStores(t *testing.T, proof func(*testing.T, context.Context, observabilityFixtureStore, *sql.DB, authoractivityfixture.Dialect)) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := testAuthorActivityContext(context.Background())
			var selected observabilityFixtureStore
			var db *sql.DB
			dialect := authoractivityfixture.DialectSQLite
			if backend == "sqlite" {
				s := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
				selected, db = s, storetest.DatabaseForTest(s)
			} else {
				_, db, _ = testutil.StartPostgres(t)
				selected = storetest.AdmitPostgresRuntimeStore(t, db)
				dialect = authoractivityfixture.DialectPostgres
			}
			proof(t, ctx, selected, db, dialect)
		})
	}
}

func TestCanonicalEventReadbackOverridesConflictingReceiptsBothStores(t *testing.T) {
	withNativeObservabilityStores(t, func(t *testing.T, ctx context.Context, selected observabilityFixtureStore, db *sql.DB, dialect authoractivityfixture.Dialect) {
		runID, eventID, entityID := uuid.NewString(), uuid.NewString(), uuid.NewString()
		fixture := storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: runID}
		if dialect == authoractivityfixture.DialectPostgres {
			storetest.RequirePostgresRun(t, ctx, db, fixture)
		} else {
			storetest.RequireSQLiteRun(t, ctx, db, fixture)
		}
		event := eventtest.ExistingRunRootIngress(eventID, "task.completed", "runtime", "", []byte(`{"entity_id":"business-only"}`), 0, runID,
			events.EventEnvelope{EntityID: entityID, Scope: events.EventScopeEntity}, time.Now().UTC())
		statuses := []string{"pending", "in_progress", "delivered", "failed", "dead_letter"}
		routes := make([]events.DeliveryRoute, len(statuses))
		for i, status := range statuses {
			agentID := "agent-" + status
			routes[i] = events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(agentID), AgentIdentity: agentidentitytest.RootRuntimeForRun(t, runID, agentID, "canonical-readback")}
		}
		storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, routes, runtimepipelineobligation.ScopeSubscribed)
		for i := 1; i < len(routes); i++ {
			claimed, err := storetest.ClaimDelivery(ctx, selected, event, routes[i])
			if err != nil {
				t.Fatalf("claim %s: %v", statuses[i], err)
			}
			switch statuses[i] {
			case "delivered":
				_, err = selected.SettleSuccess(ctx, claimed.Claim, nil, time.Second, runtimedelivery.NotApplicableHandlerRuleSelection())
			case "failed", "dead_letter":
				disposition := runtimedelivery.FailureRetry
				if statuses[i] == "dead_letter" {
					disposition = runtimedelivery.FailureDeadLetter
				}
				_, err = selected.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{Disposition: disposition, ReasonCode: "delivery_wins", Failure: testFailure("delivery_wins"), RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation()})
			}
			if err != nil {
				t.Fatalf("settle %s: %v", statuses[i], err)
			}
		}
		// Deliberately conflicting legacy evidence must not replace delivery truth.
		query := `INSERT INTO event_receipts (event_id, subscriber_type, subscriber_id, outcome, side_effects, failure, processed_at) VALUES (?, 'platform', ?, ?, ?, ?, ?)`
		if dialect == authoractivityfixture.DialectPostgres {
			query = `INSERT INTO event_receipts (event_id, subscriber_type, subscriber_id, outcome, side_effects, failure, processed_at) VALUES ($1::uuid, 'platform', $2, $3, $4::jsonb, $5::jsonb, $6)`
		}
		failure, err := json.Marshal(testFailure("receipt_should_not_win"))
		if err != nil {
			t.Fatal(err)
		}
		for _, receipt := range []struct {
			agent, outcome, effects string
			failure                 any
		}{
			{"agent-pending", "dead_letter", `{"retry_count":9}`, string(failure)},
			{"agent-failed", "success", `{"retry_count":0}`, nil},
		} {
			if _, err := db.ExecContext(ctx, query, eventID, receipt.agent, receipt.outcome, receipt.effects, receipt.failure, time.Now().UTC()); err != nil {
				t.Fatalf("conflicting receipt: %v", err)
			}
		}
		assertEvent := func(got operatorread.OperatorEventFull) {
			t.Helper()
			if got.EntityID != entityID || got.Payload["entity_id"] != "business-only" || len(got.Deliveries) != len(statuses) {
				t.Fatalf("event identity/deliveries: %#v", got)
			}
			for _, item := range got.Deliveries {
				if item.DeliveryID == "" || item.SubscriberType != "agent" {
					t.Fatalf("typed delivery: %#v", item)
				}
				want := strings.TrimPrefix(item.SubscriberID, "agent-")
				if item.Status != want {
					t.Fatalf("delivery %s status %s, want %s", item.SubscriberID, item.Status, want)
				}
				if want == "failed" && (item.RetryCount != 1 || item.Failure == nil || item.Failure.Detail.Code != "delivery_wins") {
					t.Fatalf("receipt overrode failure: %#v", item)
				}
				if want == "pending" && (item.RetryCount != 0 || item.Failure != nil) {
					t.Fatalf("receipt overrode pending: %#v", item)
				}
			}
		}
		full, err := selected.LoadOperatorEvent(ctx, eventID)
		if err != nil {
			t.Fatal(err)
		}
		assertEvent(full)
		page, err := selected.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: runID}, Limit: 10})
		if err != nil || len(page.Events) != 1 {
			t.Fatalf("event list: %#v, %v", page, err)
		}
		assertEvent(page.Events[0])
		handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: testOperatorHandlers(testOperatorCapabilities{Observability: selected})})
		for _, method := range []string{"event.get", "event.list"} {
			params := fmt.Sprintf(`{"event_id":%q}`, eventID)
			if method == "event.list" {
				params = fmt.Sprintf(`{"filter":{"run_id":%q},"limit":10}`, runID)
			}
			response := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"read","method":%q,"params":%s}`, method, params))
			if response.Error != nil {
				t.Fatalf("%s: %#v", method, response.Error)
			}
			raw, err := json.Marshal(response.Result)
			if err != nil {
				t.Fatal(err)
			}
			if method == "event.list" {
				var result operatorread.OperatorEventListResult
				if err := json.Unmarshal(raw, &result); err != nil || len(result.Events) != 1 {
					t.Fatalf("API list: %s, %v", raw, err)
				}
				assertEvent(result.Events[0])
			} else {
				var result operatorread.OperatorEventFull
				if err := json.Unmarshal(raw, &result); err != nil {
					t.Fatal(err)
				}
				assertEvent(result)
			}
		}
		// The original detail-reader counterexample contradicts failure with a
		// dead-letter receipt, not merely a success receipt.
		detailID := uuid.NewString()
		detailEvent := eventtest.ExistingRunRootIngress(detailID, "detail.completed", "runtime", "", []byte(`{}`), 0, runID, events.EventEnvelope{Scope: events.EventScopeGlobal}, time.Now().UTC())
		storetest.CommitSemanticEventWithRoutes(t, ctx, selected, detailEvent, routes[3:4], runtimepipelineobligation.ScopeSubscribed)
		claimed, err := storetest.ClaimDelivery(ctx, selected, detailEvent, routes[3])
		if err != nil {
			t.Fatal(err)
		}
		if _, err := selected.SettleFailure(ctx, claimed.Claim, runtimedelivery.Settlement{Disposition: runtimedelivery.FailureRetry, Failure: testFailure("delivery_wins"), RuleSelection: runtimedelivery.NotApplicableHandlerRuleObservation()}); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, query, detailID, "agent-failed", "dead_letter", `{"retry_count":7,"error":"receipt-loses"}`, nil, time.Now().UTC()); err != nil {
			t.Fatal(err)
		}
		detail, err := selected.LoadOperatorEvent(ctx, detailID)
		if err != nil || len(detail.Deliveries) != 1 || detail.Deliveries[0].Status != "failed" || detail.Deliveries[0].RetryCount != 1 || detail.Deliveries[0].Failure == nil || detail.Deliveries[0].Failure.Detail.Code != "delivery_wins" {
			t.Fatalf("detail conflicting receipt: %#v, %v", detail, err)
		}
		response := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"detail","method":"event.get","params":{"event_id":%q}}`, detailID))
		if response.Error != nil {
			t.Fatalf("detail API: %#v", response.Error)
		}
		row := asMap(t, asMap(t, response.Result)["deliveries"].([]any)[0])
		if row["status"] != "failed" || row["retry_count"] != float64(1) {
			t.Fatalf("detail receipt won through API: %#v", row)
		}
		payloadID := uuid.NewString()
		payloadEvent := eventtest.ExistingRunRootIngress(payloadID, "payload.only", "runtime", "", []byte(fmt.Sprintf(`{"entity_id":%q}`, entityID)), 0, runID, events.EventEnvelope{Scope: events.EventScopeGlobal}, time.Now().UTC())
		storetest.CommitSemanticEventWithRoutes(t, ctx, selected, payloadEvent, routes[:1], runtimepipelineobligation.ScopeSubscribed)
		payloadOnly, err := selected.LoadOperatorEvent(ctx, payloadID)
		if err != nil || payloadOnly.EntityID != "" || payloadOnly.Payload["entity_id"] != entityID {
			t.Fatalf("payload promoted to authority: %#v, %v", payloadOnly, err)
		}
		byEntity, err := selected.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{EntityID: entityID}, Limit: 10})
		if err != nil || len(byEntity.Events) != 1 || byEntity.Events[0].EventID != eventID {
			t.Fatalf("payload invented entity filter match: %#v, %v", byEntity, err)
		}
		response = rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"payload","method":"event.get","params":{"event_id":%q}}`, payloadID))
		if response.Error != nil {
			t.Fatalf("payload API: %#v", response.Error)
		}
		payloadRead := asMap(t, response.Result)
		if _, present := payloadRead["entity_id"]; present || asMap(t, payloadRead["payload"])["entity_id"] != entityID {
			t.Fatalf("API payload promoted: %#v", payloadRead)
		}
		// A pair filter must match one delivery, not the cross product of rows.
		for i, recipients := range [][]string{{"agent"}, {"node"}, {"node", "other-agent"}} {
			id := uuid.NewString()
			e := eventtest.ExistingRunRootIngress(id, events.EventType(fmt.Sprintf("typed.%d", i)), "runtime", "", []byte(`{}`), 0, runID, events.EventEnvelope{Scope: events.EventScopeGlobal}, time.Now().UTC())
			var rs []events.DeliveryRoute
			for _, kind := range recipients {
				if kind == "node" {
					rs = append(rs, events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(identitytest.FlowNode(t, "fixture", "colliding-subscriber")), Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "fixture", FlowInstance: "fixture/colliding-subscriber"})})
				} else {
					id := "colliding-subscriber"
					if kind == "other-agent" {
						id = "different-subscriber"
					}
					rs = append(rs, events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(id), AgentIdentity: agentidentitytest.RootRuntimeForRun(t, runID, id, "canonical-readback")})
				}
			}
			storetest.CommitSemanticEventWithRoutes(t, ctx, selected, e, rs, runtimepipelineobligation.ScopeSubscribed)
		}
		filtered, err := selected.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: runID, SubscriberID: "colliding-subscriber", SubscriberType: "agent"}, Limit: 10})
		if err != nil || len(filtered.Events) != 1 || filtered.Events[0].EventName != "typed.0" {
			t.Fatalf("typed pair filter: %#v, %v", filtered, err)
		}
		response = rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"typed","method":"event.list","params":{"filter":{"run_id":%q,"subscriber_id":"colliding-subscriber","subscriber_type":"agent"},"limit":10}}`, runID))
		if response.Error != nil {
			t.Fatalf("typed filter API: %#v", response.Error)
		}
		rows := asMap(t, response.Result)["events"].([]any)
		if len(rows) != 1 || asMap(t, rows[0])["event_name"] != "typed.0" {
			t.Fatalf("typed filter API rows: %#v", rows)
		}
	})
}

func TestRetiredDashboardObservabilityAssertionsThroughV1BothStores(t *testing.T) {
	withNativeObservabilityStores(t, func(t *testing.T, ctx context.Context, selected observabilityFixtureStore, db *sql.DB, dialect authoractivityfixture.Dialect) {
		base := time.Now().UTC().Truncate(time.Second).Add(-time.Minute)
		insert := func(component, action, code, agent string, at time.Time) {
			t.Helper()
			class := runtimefailures.ClassInternalFailure
			if code == "retry_exhausted" {
				class = runtimefailures.ClassRetryExhausted
			}
			failure := runtimefailures.Normalize(runtimefailures.New(class, code, "test-runtime", action, nil), "test-runtime", action)
			payload, err := json.Marshal(map[string]any{"log_level": "error", "message": "runtime incident", "details": map[string]any{"component": component, "action": action, "agent_id": agent, "failure": failure}})
			if err != nil {
				t.Fatal(err)
			}
			storetest.InsertDiagnosticDirectEventRecord(t, ctx, db, dialect, uuid.NewString(), "runtime", payload, at)
		}
		insert("mcp-gateway", "request_failed", "retry_exhausted", "agent-a", base)
		insert("mcp-gateway", "request_failed", "retry_exhausted", "agent-b", base.Add(time.Second))
		insert("diagnostics", "same_second_order", "older_code", "", base.Add(100*time.Millisecond))
		insert("diagnostics", "same_second_order", "newer_code", "", base.Add(900*time.Millisecond))
		storetest.InsertDiagnosticDirectEventRecord(t, ctx, db, dialect, uuid.NewString(), "runtime", []byte(`{"log_level":"error","message":"message fallback","details":{"component":"no-failure","action":"fallback_case"}}`), base)
		grouped, err := selected.ListOperatorRuntimeIncidents(ctx, operatorread.OperatorRuntimeIncidentListOptions{SinceHours: 24, MCPOnly: true})
		if err != nil || len(grouped.Incidents) != 1 {
			t.Fatalf("grouped: %#v, %v", grouped, err)
		}
		got := grouped.Incidents[0]
		if got.ErrorCode != "retry_exhausted" || got.Count != 2 || got.Component != "mcp-gateway" || got.Level != "error" || got.SampleMessage != "Retry policy was exhausted (retry_exhausted)." || strings.Join(got.Agents, ",") != "agent-a,agent-b" || strings.Join(got.Actions, ",") != "request_failed" {
			t.Fatalf("canonical incident: %#v", got)
		}
		latest, err := selected.ListOperatorRuntimeIncidents(ctx, operatorread.OperatorRuntimeIncidentListOptions{SinceHours: 24, Component: "diagnostics", Limit: 1})
		if err != nil || len(latest.Incidents) != 1 || latest.Incidents[0].ErrorCode != "newer_code" || !latest.Incidents[0].LastSeen.Equal(base.Add(900*time.Millisecond)) {
			t.Fatalf("subsecond before limit: %#v, %v", latest, err)
		}
		absent, err := selected.ListOperatorRuntimeIncidents(ctx, operatorread.OperatorRuntimeIncidentListOptions{SinceHours: 24, Component: "no-failure"})
		if err != nil || len(absent.Incidents) != 0 {
			t.Fatalf("prose is not failure: %#v, %v", absent, err)
		}
		handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: testOperatorHandlers(testOperatorCapabilities{Observability: selected})})
		for _, cell := range []struct {
			component, code string
			count           int
		}{{"mcp-gateway", "retry_exhausted", 2}, {"diagnostics", "newer_code", 1}, {"no-failure", "", 0}} {
			response := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"incident","method":"runtime.incidents","params":{"component":%q,"limit":1}}`, cell.component))
			if response.Error != nil {
				t.Fatalf("incident API: %#v", response.Error)
			}
			rows, _ := asMap(t, response.Result)["incidents"].([]any)
			if cell.count == 0 {
				if len(rows) != 0 {
					t.Fatalf("invented incident: %#v", rows)
				}
				continue
			}
			if len(rows) != 1 || asMap(t, rows[0])["error_code"] != cell.code || asMap(t, rows[0])["count"] != float64(cell.count) {
				t.Fatalf("API incident: %#v", rows)
			}
		}
		for i, cell := range []struct {
			state, prev, reason, terminal string
			retries                       int
		}{{"retrying", "active", "boom", "", 1}, {"exhausted", "retrying", "boom", "retry_exhausted", 2}, {"exhausted", "active", "cancelled_by_kill_previous", "cancelled_by_kill_previous", 0}} {
			payload, err := json.Marshal(map[string]any{"log_level": "debug", "message": "delivery lifecycle", "details": map[string]any{"component": "agent-manager", "action": "delivery_lifecycle_transition", "event_id": fmt.Sprintf("evt-%d", i), "agent_id": "agent-1", "delivery_state": cell.state, "delivery_previous_state": cell.prev, "delivery_transition": cell.state, "delivery_reason": cell.reason, "delivery_terminal_outcome": cell.terminal, "retry_count": cell.retries}})
			if err != nil {
				t.Fatal(err)
			}
			storetest.InsertDiagnosticDirectEventRecord(t, ctx, db, dialect, uuid.NewString(), "runtime", payload, base.Add(time.Duration(i)*time.Second))
		}
		logs, err := selected.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{Component: "agent-manager", Limit: 10})
		if err != nil || len(logs.Logs) != 3 {
			t.Fatalf("lifecycle logs: %#v, %v", logs, err)
		}
		wantStates := []string{"exhausted", "exhausted", "retrying"}
		wantPrevious := []string{"active", "retrying", "active"}
		wantReasons := []string{"cancelled_by_kill_previous", "boom", "boom"}
		wantTerminal := []string{"cancelled_by_kill_previous", "retry_exhausted", ""}
		wantRetries := []int{0, 2, 1}
		for i, log := range logs.Logs {
			if log.DeliveryState != wantStates[i] || log.PreviousState != wantPrevious[i] || log.Reason != wantReasons[i] || log.Terminal != wantTerminal[i] || log.RetryCount != wantRetries[i] || log.EventID != fmt.Sprintf("evt-%d", 2-i) || log.AgentID != "agent-1" {
				t.Fatalf("lifecycle %d: %#v", i, log)
			}
		}
		response := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"lifecycle","method":"runtime.logs","params":{"component":"agent-manager","limit":10}}`)
		if response.Error != nil {
			t.Fatalf("lifecycle API: %#v", response.Error)
		}
		raw, err := json.Marshal(response.Result)
		if err != nil {
			t.Fatal(err)
		}
		var apiLogs operatorread.OperatorRuntimeLogListResult
		if err := json.Unmarshal(raw, &apiLogs); err != nil || len(apiLogs.Logs) != 3 {
			t.Fatalf("lifecycle API rows: %s, %v", raw, err)
		}
		for i, log := range apiLogs.Logs {
			if log.DeliveryState != wantStates[i] || log.PreviousState != wantPrevious[i] || log.Reason != wantReasons[i] || log.Terminal != wantTerminal[i] || log.RetryCount != wantRetries[i] || log.EventID != fmt.Sprintf("evt-%d", 2-i) || log.AgentID != "agent-1" {
				t.Fatalf("lifecycle API %d: %#v", i, log)
			}
		}
	})
}

func TestCanonicalObservabilityCorruptionRefusesBothStores(t *testing.T) {
	for _, cell := range []struct{ name, payload, method, message string }{
		{"details", `{"log_level":"error","message":"malformed runtime log","details":"not-an-object"}`, "runtime.logs", "runtime log details must be an object"},
		{"component", `{"log_level":"error","message":"incomplete runtime incident","details":{"action":"request_failed","failure":null}}`, "runtime.incidents", "runtime log component is required"},
	} {
		t.Run(cell.name, func(t *testing.T) {
			withNativeObservabilityStores(t, func(t *testing.T, ctx context.Context, selected observabilityFixtureStore, db *sql.DB, dialect authoractivityfixture.Dialect) {
				payload := cell.payload
				if cell.name == "component" {
					raw, err := json.Marshal(testFailure("retry_exhausted"))
					if err != nil {
						t.Fatal(err)
					}
					payload = strings.Replace(payload, "null", string(raw), 1)
				}
				storetest.InsertDiagnosticDirectEventRecord(t, ctx, db, dialect, uuid.NewString(), "runtime", []byte(payload), time.Now().UTC())
				var err error
				if cell.method == "runtime.logs" {
					_, err = selected.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{Limit: 10})
				} else {
					_, err = selected.ListOperatorRuntimeIncidents(ctx, operatorread.OperatorRuntimeIncidentListOptions{SinceHours: 24})
				}
				if err == nil || !strings.Contains(err.Error(), cell.message) {
					t.Fatalf("canonical refusal: %v, want %s", err, cell.message)
				}
				handler := testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: testOperatorHandlers(testOperatorCapabilities{Observability: selected})})
				response := rpcCall(t, handler, fmt.Sprintf(`{"jsonrpc":"2.0","id":"corrupt","method":%q,"params":{"limit":10}}`, cell.method))
				if response.Error == nil {
					t.Fatalf("corrupt API read returned success: %#v", response.Result)
				}
				requireRPCFailure(t, response.Error, runtimefailures.ClassInternalFailure, "unclassified_runtime_error")
			})
		})
	}
}
