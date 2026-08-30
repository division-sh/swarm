package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	operatorread "github.com/division-sh/swarm/internal/operatorread"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

const testRetiredTransportAuthToken = "builder-test-token"
const testOperatorAuthToken = "operator-secret"

func setOperatorAuth(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+testOperatorAuthToken)
}

func TestDashboardEventFilterFromRequestPreservesTypedSubscriberIdentity(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/events?type=task.completed&source=runtime&entity_id=entity-1&subscriber_id=worker-1&subscriber_type=node", nil)

	filter, err := dashboardEventFilterFromRequest(req)
	if err != nil {
		t.Fatalf("dashboardEventFilterFromRequest: %v", err)
	}
	if filter.Type != "task.completed" ||
		filter.Source != "runtime" ||
		filter.EntityID != "entity-1" ||
		filter.SubscriberID != "worker-1" ||
		filter.SubscriberType != "node" {
		t.Fatalf("filter = %#v", filter)
	}
}

func TestDashboardEventFilterFromRequestRejectsInvalidSubscriberType(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/events?subscriber_id=worker-1&subscriber_type=platform", nil)

	if _, err := dashboardEventFilterFromRequest(req); err == nil || !strings.Contains(err.Error(), "subscriber_type") {
		t.Fatalf("dashboardEventFilterFromRequest error = %v, want subscriber_type rejection", err)
	}
}

func TestDashboardRejectsLegacySubscriberParameter(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/events?subscriber=worker-1&subscriber_id=worker-1&subscriber_type=agent", nil)

	if _, err := dashboardEventFilterFromRequest(req); err == nil || !strings.Contains(err.Error(), "subscriber is unsupported") {
		t.Fatalf("dashboardEventFilterFromRequest error = %v, want legacy subscriber rejection", err)
	}
}

type stubInstances struct {
	rows          []operatorread.OperatorEntitySummary
	byID          map[string]operatorread.OperatorEntityFull
	lastAggregate *operatorread.OperatorEntityAggregateOptions
}

func (s stubInstances) ListOperatorEntities(_ context.Context, opts operatorread.OperatorEntityListOptions) (operatorread.OperatorEntityListResult, error) {
	rows := make([]operatorread.OperatorEntitySummary, 0, len(s.rows))
	for _, row := range s.rows {
		if opts.RunID != "" && row.RunID != opts.RunID {
			continue
		}
		if opts.EntityID != "" && row.EntityID != opts.EntityID {
			continue
		}
		if opts.Flow != "" && row.FlowInstance != opts.Flow && !strings.HasPrefix(row.FlowInstance, opts.Flow+"/") {
			continue
		}
		if opts.Type != "" && row.EntityType != opts.Type {
			continue
		}
		if opts.CurrentState != "" && row.CurrentState != opts.CurrentState {
			continue
		}
		rows = append(rows, row)
	}
	if opts.Limit > 0 && len(rows) > opts.Limit {
		rows = rows[:opts.Limit]
	}
	return operatorread.OperatorEntityListResult{Entities: rows}, nil
}

func (s stubInstances) LoadOperatorEntity(_ context.Context, entityID, runID string) (operatorread.OperatorEntityFull, error) {
	item, ok := s.byID[entityID]
	if !ok {
		return operatorread.OperatorEntityFull{}, operatorread.ErrEntityNotFound
	}
	if runID != "" && item.Entity.RunID != runID {
		return operatorread.OperatorEntityFull{}, operatorread.ErrEntityNotFound
	}
	return item, nil
}

func (s stubInstances) AggregateOperatorEntities(_ context.Context, opts operatorread.OperatorEntityAggregateOptions) (operatorread.OperatorEntityAggregateResult, error) {
	if s.lastAggregate != nil {
		*s.lastAggregate = opts
	}
	counts := map[string]int{}
	for _, row := range s.rows {
		if opts.RunID != "" && row.RunID != opts.RunID {
			continue
		}
		if opts.Type != "" && row.EntityType != opts.Type {
			continue
		}
		key := row.CurrentState
		switch opts.GroupBy {
		case "flow", "flow_instance":
			key = row.FlowInstance
		case "type", "entity_type":
			key = row.EntityType
		case "slug":
			key = row.Slug
		case "name":
			key = row.Name
		}
		if strings.TrimSpace(key) == "" {
			key = "unknown"
		}
		counts[key]++
	}
	return operatorread.OperatorEntityAggregateResult{Counts: counts}, nil
}

func TestHandler_InstanceHandlersReturnCanonicalEntityProjection(t *testing.T) {
	entityID := runtimeflowidentity.EntityID("wf-1")
	lastAggregate := &operatorread.OperatorEntityAggregateOptions{}
	h := &Handler{
		entities: stubInstances{
			rows: []operatorread.OperatorEntitySummary{{
				EntityID:     entityID,
				RunID:        "run-1",
				FlowInstance: "order/wf-1",
				EntityType:   "order",
				CurrentState: "reviewing",
			}},
			byID: map[string]operatorread.OperatorEntityFull{
				entityID: {
					Entity: operatorread.OperatorEntitySummary{
						EntityID:     entityID,
						RunID:        "run-1",
						FlowInstance: "order/wf-1",
						EntityType:   "order",
						CurrentState: "reviewing",
					},
					Fields:      map[string]any{"business_status": "approved", "activation": "manual"},
					Bookkeeping: map[string]any{"activation": "standing"},
					Gates:       map[string]bool{"review_gate": true},
					Accumulated: map[string]any{
						"score":       float64(9),
						"accumulator": map[string]any{"count": float64(2)},
						"notes":       []any{"a", map[string]any{"text": "probe"}},
					},
				},
			},
			lastAggregate: lastAggregate,
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/instances?current_state=reviewing&type=order&limit=1", nil)
	h.handleInstances(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("handleInstances status=%d body=%s", rec.Code, rec.Body.String())
	}
	var listPayload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &listPayload); err != nil {
		t.Fatalf("unmarshal instances: %v", err)
	}
	rows, ok := listPayload["instances"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("instances payload = %#v", listPayload)
	}
	row := rows[0].(map[string]any)
	if row["current_state"] != "reviewing" {
		t.Fatalf("instances current_state = %#v, want reviewing", row["current_state"])
	}
	if _, ok := row["state"]; ok {
		t.Fatalf("instances leaked legacy state field: %#v", row)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/instances/wf-1?run_id=run-1", nil)
	req.SetPathValue("id", "wf-1")
	h.handleInstanceDetail(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("handleInstanceDetail status=%d body=%s", rec.Code, rec.Body.String())
	}
	var detail operatorread.OperatorEntityFull
	if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
		t.Fatalf("unmarshal instance detail: %v", err)
	}
	if detail.Entity.CurrentState != "reviewing" || detail.Fields["business_status"] != "approved" || detail.Fields["activation"] != "manual" || detail.Bookkeeping["activation"] != "standing" || !detail.Gates["review_gate"] || detail.Accumulated["score"] != float64(9) {
		t.Fatalf("detail payload = %#v", detail)
	}
	if bucket, ok := detail.Accumulated["accumulator"].(map[string]any); !ok || bucket["count"] != float64(2) {
		t.Fatalf("detail accumulated bucket = %#v, want count", detail.Accumulated["accumulator"])
	}
	if _, ok := detail.Fields["status"]; ok {
		t.Fatalf("detail leaked control status field: %#v", detail.Fields)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/instances/aggregate?group_by=current_state&type=order", nil)
	h.handleInstanceAggregate(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("handleInstanceAggregate status=%d body=%s", rec.Code, rec.Body.String())
	}
	var aggregate map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &aggregate); err != nil {
		t.Fatalf("unmarshal aggregate: %v", err)
	}
	groups, _ := aggregate["groups"].([]any)
	if lastAggregate.GroupBy != "current_state" || len(groups) != 1 {
		t.Fatalf("aggregate = %#v opts=%#v, want current_state reviewing=1", aggregate, *lastAggregate)
	}
	group, _ := groups[0].(map[string]any)
	if group["key"] != "reviewing" || group["count"] != float64(1) {
		t.Fatalf("aggregate group = %#v, want reviewing=1", group)
	}
}

type stubObservability struct {
	events      []eventRecord
	eventDetail map[string]eventRecord
	runtimeLogs []runtimeLogRecord
	incidents   []incidentRecord
	err         error
}

type stubDashboardConversationHTTPReader struct {
	listResult operatorread.OperatorConversationListResult
	turnPages  map[string]operatorread.OperatorConversationTurnListResult
	listOpts   operatorread.OperatorConversationListOptions
	turnOpts   []operatorread.OperatorConversationTurnListOptions
}

func (s *stubDashboardConversationHTTPReader) ListOperatorConversations(_ context.Context, opts operatorread.OperatorConversationListOptions) (operatorread.OperatorConversationListResult, error) {
	s.listOpts = opts
	return s.listResult, nil
}

func (s *stubDashboardConversationHTTPReader) ListOperatorConversationTurns(_ context.Context, opts operatorread.OperatorConversationTurnListOptions) (operatorread.OperatorConversationTurnListResult, error) {
	s.turnOpts = append(s.turnOpts, opts)
	return s.turnPages[strings.TrimSpace(opts.Cursor)], nil
}

func (s *stubDashboardConversationHTTPReader) LoadOperatorPublicConversationTurn(context.Context, string, string) (operatorread.OperatorPublicConversationTurnDetail, error) {
	return operatorread.OperatorPublicConversationTurnDetail{}, operatorread.ErrTurnNotFound
}

func (s stubObservability) ListEvents(context.Context, EventFilter, int) ([]eventRecord, error) {
	return s.events, s.err
}

func (s stubObservability) GetEvent(_ context.Context, id string) (eventRecord, bool, error) {
	if s.err != nil {
		return eventRecord{}, false, s.err
	}
	item, ok := s.eventDetail[id]
	return item, ok, nil
}

func (s stubObservability) ListRuntimeLogs(context.Context, RuntimeLogFilter, int) ([]runtimeLogRecord, error) {
	return s.runtimeLogs, s.err
}

func (s stubObservability) ListIncidents(context.Context, IncidentFilter) ([]incidentRecord, error) {
	return s.incidents, s.err
}

func TestDashboardMissingCapabilitiesFailClosed(t *testing.T) {
	h := &Handler{}
	tests := []struct {
		name string
		call func(http.ResponseWriter, *http.Request)
		path string
	}{
		{name: "health", call: h.handleHealth, path: "/healthz"},
		{name: "event list", call: h.handleEvents, path: "/api/events"},
		{name: "event detail", call: h.handleEventDetail, path: "/api/events/evt-1"},
		{name: "flow stream", call: h.handleFlowEvents, path: "/api/flow/events"},
		{name: "event stream", call: h.handleEventStream, path: "/api/events?stream=true"},
		{name: "runtime logs", call: h.handleRuntimeLogs, path: "/api/runtime/logs"},
		{name: "runtime incidents", call: h.handleRuntimeIncidents, path: "/api/runtime/incidents"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			req.SetPathValue("id", "evt-1")
			tc.call(rec, req)
			if rec.Code != http.StatusNotImplemented {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusNotImplemented, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "not configured") {
				t.Fatalf("body = %s, want explicit missing capability", rec.Body.String())
			}
		})
	}
}

func TestDashboardStreamsSurfaceReadFailureAndTerminate(t *testing.T) {
	readErr := errors.New("projection read failed")
	h := &Handler{observability: stubObservability{err: readErr}}
	for _, tc := range []struct {
		name string
		call func(http.ResponseWriter, *http.Request)
		path string
	}{
		{name: "flow", call: h.handleFlowEvents, path: "/api/flow/events"},
		{name: "event", call: h.handleEventStream, path: "/api/events?stream=true&include_runtime=true"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			tc.call(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))
			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want stream status 200; body=%s", rec.Code, rec.Body.String())
			}
			body := rec.Body.String()
			if !strings.Contains(body, "event: error") || !strings.Contains(body, readErr.Error()) {
				t.Fatalf("stream body = %q, want explicit terminal error event", body)
			}
		})
	}
}

type stubRuntimeControl struct {
	pauseCalls  int
	resumeCalls int
}

func (s *stubRuntimeControl) PauseIngress() error {
	s.pauseCalls++
	return nil
}
func (s *stubRuntimeControl) ResumeIngress() error {
	s.resumeCalls++
	return nil
}

func TestHandler_LegacyDashboardRoutesFailClosedWithoutAuthBoundary(t *testing.T) {
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		AuthToken: testOperatorAuthToken,
		Runtime:   &stubRuntimeControl{},
	})

	for _, tc := range []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{
			name:   "dashboard agents",
			method: http.MethodGet,
			path:   "/api/agents",
		},
		{
			name:   "dashboard runtime logs",
			method: http.MethodGet,
			path:   "/api/runtime/logs",
		},
		{
			name:   "runtime control",
			method: http.MethodPost,
			path:   "/api/runtime/actions",
			body:   `{"action":"pause"}`,
		},
		{
			name:   "runtime reset_state",
			method: http.MethodPost,
			path:   "/api/runtime/actions",
			body:   `{"action":"reset_state"}`,
		},
		{
			name:   "run trace",
			method: http.MethodGet,
			path:   "/api/runs/run-1/trace",
		},
		{
			name:   "retired rpc",
			method: http.MethodPost,
			path:   "/rpc",
			body:   `{"jsonrpc":"2.0","id":"1","method":"engine.ping"}`,
		},
		{
			name:   "retired rpc api alias",
			method: http.MethodPost,
			path:   "/api/rpc",
			body:   `{"jsonrpc":"2.0","id":"1","method":"engine.ping"}`,
		},
		{
			name:   "retired websocket",
			method: http.MethodGet,
			path:   "/ws",
		},
		{
			name:   "retired websocket api alias",
			method: http.MethodGet,
			path:   "/api/ws",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			setOperatorAuth(req)
			req.Header.Set("Authorization", "Bearer "+testRetiredTransportAuthToken)
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusNotFound {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestHandler_RuntimeResetStateActionIsRetired(t *testing.T) {
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Runtime: runtimeCtl,
	}).(*Handler)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/runtime/actions", strings.NewReader(`{"action":"reset_state"}`))
	handler.handleRuntimeAction(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("reset_state runtime action status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
	if runtimeCtl.pauseCalls != 0 || runtimeCtl.resumeCalls != 0 {
		t.Fatalf("runtime control calls = pause:%d resume:%d, want no calls", runtimeCtl.pauseCalls, runtimeCtl.resumeCalls)
	}
}

func TestHandler_DashboardRoutesFailClosedWhenAuthIsNotConfigured(t *testing.T) {
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandler_HealthzOnlyKeepsProcessProbeAndRetiresAliases(t *testing.T) {
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		AuthToken: testOperatorAuthToken,
		Version:   "swarm-test",
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz status=%d body=%s", rec.Code, rec.Body.String())
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal /healthz: %v", err)
	}
	if payload["ok"] != true {
		t.Fatalf("unexpected /healthz payload: %#v", payload)
	}

	for _, path := range []string{"/api/healthz", "/api/health"} {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		setOperatorAuth(req)
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s status=%d body=%s, want 404", path, rec.Code, rec.Body.String())
		}
	}
}

func TestDashboardConversationListHTTPUsesCanonicalPageAndCursor(t *testing.T) {
	reader := &stubDashboardConversationHTTPReader{listResult: operatorread.OperatorConversationListResult{
		Conversations: []operatorread.OperatorConversationSummary{{SessionID: "sess-1", AgentID: "agent-1", Status: "active"}},
		NextCursor:    "conversation-page-3",
	}}
	handler := NewHandler(Options{Conversations: reader}).(*Handler)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/conversations?agent_id=agent-1&run_id=run-1&limit=7&cursor=conversation-page-2", nil)
	handler.handleConversations(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("conversation list status = %d body=%s", rec.Code, rec.Body.String())
	}
	if reader.listOpts.AgentID != "agent-1" || reader.listOpts.RunID != "run-1" || reader.listOpts.Limit != 7 || reader.listOpts.Cursor != "conversation-page-2" {
		t.Fatalf("conversation list options = %#v", reader.listOpts)
	}
	if !strings.Contains(rec.Body.String(), `"next_cursor":"conversation-page-3"`) {
		t.Fatalf("conversation list body = %s, want consumable next cursor", rec.Body.String())
	}
}

func TestDashboardConversationDetailHTTPUsesCompactSafeCursorPages(t *testing.T) {
	completedAt := time.Date(2026, 7, 14, 1, 2, 3, 0, time.UTC)
	reader := &stubDashboardConversationHTTPReader{turnPages: map[string]operatorread.OperatorConversationTurnListResult{
		"": {
			Conversation: operatorread.OperatorConversationSummary{SessionID: "sess-1", AgentID: "agent-1", Status: "active"},
			Turns: []operatorread.OperatorConversationTurnListItem{{
				TurnID: "turn-2", Ordinal: 2, CompletedAt: completedAt, DurationMS: 42,
				ActivityCounts: operatorread.OperatorConversationActivityCounts{Dispatch: 1, Tool: 1, ToolResult: 1, Publish: 1, Output: 1, Failure: 1},
				ParseOK:        false, Outcome: "failed",
			}},
			NextCursor: "turn-page-2",
		},
		"turn-page-2": {
			Conversation: operatorread.OperatorConversationSummary{SessionID: "sess-1", AgentID: "agent-1", Status: "active"},
			Turns:        []operatorread.OperatorConversationTurnListItem{{TurnID: "turn-1", Ordinal: 1, CompletedAt: completedAt.Add(-time.Minute), DurationMS: 21, ParseOK: true}},
		},
	}}
	handler := NewHandler(Options{Conversations: reader}).(*Handler)

	requestPage := func(rawURL, cursor, wantTurn string) string {
		t.Helper()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, rawURL, nil)
		req.SetPathValue("sessionID", "sess-1")
		handler.handleConversationDetail(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("conversation detail status = %d body=%s", rec.Code, rec.Body.String())
		}
		last := reader.turnOpts[len(reader.turnOpts)-1]
		if last.SessionID != "sess-1" || last.Limit != 1 || last.Cursor != cursor {
			t.Fatalf("conversation detail options = %#v", last)
		}
		if !strings.Contains(rec.Body.String(), `"turn_id":"`+wantTurn+`"`) {
			t.Fatalf("conversation detail body = %s, want %s", rec.Body.String(), wantTurn)
		}
		return rec.Body.String()
	}

	first := requestPage("/api/conversations/sess-1?limit=1", "", "turn-2")
	if !strings.Contains(first, `"next_cursor":"turn-page-2"`) || !strings.Contains(first, `"activity_counts"`) {
		t.Fatalf("conversation detail first page = %s", first)
	}
	second := requestPage("/api/conversations/sess-1?limit=1&cursor=turn-page-2", "turn-page-2", "turn-1")
	for _, body := range []string{first, second} {
		for _, forbidden := range []string{`"activity":`, `"assistant_visible_output":`, `"entity_id":`, `"task_id":`, `"retry_count":`, "request_payload", "response_payload", "available_tools", "mcp_servers", "mcp_tools_listed", "mcp_tools_visible"} {
			if strings.Contains(body, forbidden) {
				t.Fatalf("conversation detail leaked %q: %s", forbidden, body)
			}
		}
	}
}
