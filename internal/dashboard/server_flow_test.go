package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestHandlePipelineGraph_RuntimeIncludesFlowEvents(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	now := time.Date(2026, 2, 26, 14, 0, 0, 0, time.UTC)
	s := NewServer(db, nil, nil, nil, nil)
	s.now = func() time.Time { return now }

	mock.ExpectQuery(`(?s)SELECT.*FROM events e.*LEFT JOIN verticals v ON v.id = e\.vertical_id.*ORDER BY e\.created_at ASC.*LIMIT \$\d+`).
		WithArgs(now.Add(-15*time.Minute), 20).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "type", "source_agent", "task_id", "vertical_id", "created_at", "payload", "targets",
		}).AddRow(
			"evt-1",
			"vertical.shortlisted",
			"runtime",
			"",
			"",
			now.Add(-2*time.Minute),
			[]byte(`{"result":"shortlisted","vertical_id":"v-1"}`),
			[]byte(`["business-research-agent"]`),
		))

	req := httptest.NewRequest(http.MethodGet, "/api/pipeline/graph?view=runtime&limit=5", nil)
	w := httptest.NewRecorder()
	s.handlePipelineGraph(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	var resp struct {
		View      string                   `json:"view"`
		FlowCount int                      `json:"flow_count"`
		Flow      []map[string]interface{} `json:"flow_events"`
		Meta      map[string]interface{}   `json:"meta"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.View != "runtime" {
		t.Fatalf("expected view runtime, got %q", resp.View)
	}
	if resp.FlowCount != 1 || len(resp.Flow) != 1 {
		t.Fatalf("expected one flow event, got flow_count=%d len=%d", resp.FlowCount, len(resp.Flow))
	}
	if resp.Meta == nil || resp.Meta["design_version"] == nil {
		t.Fatalf("expected graph metadata with design_version")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestHandleFlowEvents_JSON(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	now := time.Date(2026, 2, 26, 15, 0, 0, 0, time.UTC)
	s := NewServer(db, nil, nil, nil, nil)
	s.now = func() time.Time { return now }

	mock.ExpectQuery(`(?s)SELECT.*FROM events e.*LEFT JOIN verticals v ON v.id = e\.vertical_id.*ORDER BY e\.created_at DESC.*LIMIT \$\d+`).
		WithArgs(10).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "type", "source_agent", "task_id", "vertical_id", "created_at", "payload", "targets",
		}).AddRow(
			"evt-2",
			"vertical.scored",
			"runtime",
			"task-1",
			"",
			now.Add(-30*time.Second),
			[]byte(`{"result":"marginal"}`),
			[]byte(`[]`),
		))

	req := httptest.NewRequest(http.MethodGet, "/api/events/flow?limit=10", nil)
	w := httptest.NewRecorder()
	s.handleFlowEvents(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	var resp struct {
		Count int `json:"count"`
		Flow  []struct {
			EventType   string   `json:"event_type"`
			Intercepted bool     `json:"intercepted"`
			Passthrough bool     `json:"passthrough"`
			TargetNodes []string `json:"target_nodes"`
		} `json:"flow_events"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Count != 1 || len(resp.Flow) != 1 {
		t.Fatalf("expected one flow event, got count=%d len=%d", resp.Count, len(resp.Flow))
	}
	if resp.Flow[0].EventType != "vertical.scored" {
		t.Fatalf("unexpected event type: %q", resp.Flow[0].EventType)
	}
	if !resp.Flow[0].Intercepted || !resp.Flow[0].Passthrough {
		t.Fatalf("expected vertical.scored(marginal) to be intercepted+passthrough")
	}
	if len(resp.Flow[0].TargetNodes) != 1 || resp.Flow[0].TargetNodes[0] != "pipeline-coordinator" {
		t.Fatalf("expected pipeline-coordinator target fallback, got %#v", resp.Flow[0].TargetNodes)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestParseFlowRange_AcceptsDateTimeLocal(t *testing.T) {
	start, end := parseFlowRange("2026-02-26T09:15", "2026-02-26T10:45:30")
	if start.IsZero() || end.IsZero() {
		t.Fatalf("expected datetime-local values to parse: start=%v end=%v", start, end)
	}

	startRFC, endRFC := parseFlowRange("2026-02-26T09:15:00Z", "2026-02-26T10:45:30Z")
	if startRFC.IsZero() || endRFC.IsZero() {
		t.Fatalf("expected RFC3339 values to parse")
	}

	if !regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T`).MatchString(start.Format(time.RFC3339)) {
		t.Fatalf("unexpected normalized timestamp: %s", start.Format(time.RFC3339))
	}
}

func TestHandlePipelineGraph_DesignAnnotatesEventEdges(t *testing.T) {
	s := NewServer(nil, nil, nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/pipeline/graph?view=design", nil)
	w := httptest.NewRecorder()

	s.handlePipelineGraph(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	var resp struct {
		Edges []struct {
			Kind              string   `json:"kind"`
			EventType         string   `json:"event_type"`
			SchemaRequired    []string `json:"schema_required"`
			InterceptorHandle string   `json:"interceptor_handler"`
		} `json:"edges"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	found := false
	for _, e := range resp.Edges {
		if e.Kind == "producer" && e.EventType == "scan.requested" {
			found = true
			if len(e.SchemaRequired) == 0 {
				t.Fatalf("expected schema_required for scan.requested edge")
			}
			if strings.TrimSpace(e.InterceptorHandle) != "pipeline_coordinator.go:handleScanRequested" {
				t.Fatalf("unexpected interceptor handler: %q", e.InterceptorHandle)
			}
			break
		}
	}
	if !found {
		t.Fatalf("expected at least one producer edge annotated for scan.requested")
	}
}

func TestHandlePipelineGraph_DesignIncludesStageAndRubricMetadata(t *testing.T) {
	s := NewServer(nil, nil, nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/pipeline/graph?view=design", nil)
	w := httptest.NewRecorder()

	s.handlePipelineGraph(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	var resp struct {
		Meta  map[string]any `json:"meta"`
		Edges []struct {
			EventType string   `json:"event_type"`
			Stages    []string `json:"stages"`
			Rubrics   []string `json:"rubrics"`
		} `json:"edges"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Meta == nil {
		t.Fatalf("expected non-empty graph meta")
	}
	if _, ok := resp.Meta["stages"]; !ok {
		t.Fatalf("expected stages metadata")
	}
	if _, ok := resp.Meta["rubrics"]; !ok {
		t.Fatalf("expected rubrics metadata")
	}
	if got, _ := resp.Meta["workflow_version"].(string); strings.TrimSpace(got) != "2.1.0" {
		t.Fatalf("expected workflow_version 2.1.0, got %q", got)
	}
	if got, _ := resp.Meta["platform_version"].(string); strings.TrimSpace(got) == "" {
		t.Fatalf("expected platform_version metadata")
	}
	if _, ok := resp.Meta["event_stage_map"]; !ok {
		t.Fatalf("expected event_stage_map metadata")
	}
	if _, ok := resp.Meta["stage_phase_map"]; !ok {
		t.Fatalf("expected stage_phase_map metadata")
	}
	if sources, ok := resp.Meta["sources"].([]any); !ok || len(sources) == 0 {
		t.Fatalf("expected sources metadata, got %#v", resp.Meta["sources"])
	}

	found := false
	for _, e := range resp.Edges {
		if strings.TrimSpace(e.EventType) != "scoring.requested" {
			continue
		}
		found = true
		if len(e.Stages) == 0 || e.Stages[0] != "scoring" {
			t.Fatalf("expected scoring.requested stage metadata, got %#v", e.Stages)
		}
		if len(e.Rubrics) == 0 {
			t.Fatalf("expected scoring.requested rubric metadata")
		}
		break
	}
	if !found {
		t.Fatalf("expected scoring.requested edge in design graph")
	}
}

func TestHandlePipelineGraph_DesignIncludesTransitionAndTimerMetadata(t *testing.T) {
	s := NewServer(nil, nil, nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/api/pipeline/graph?view=design", nil)
	w := httptest.NewRecorder()

	s.handlePipelineGraph(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}

	var resp struct {
		Edges []struct {
			EventType         string           `json:"event_type"`
			TransitionIDs     []string         `json:"transition_ids"`
			TransitionDetails []map[string]any `json:"transition_details"`
			TimerIDs          []string         `json:"timer_ids"`
			TimerDetails      []map[string]any `json:"timer_details"`
		} `json:"edges"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	foundTransitionEdge := false
	foundTimerEdge := false
	for _, edge := range resp.Edges {
		switch strings.TrimSpace(edge.EventType) {
		case "vertical.shortlisted":
			foundTransitionEdge = true
			if len(edge.TransitionIDs) == 0 || len(edge.TransitionDetails) == 0 {
				t.Fatalf("expected transition metadata on vertical.shortlisted edge, got ids=%v details=%v", edge.TransitionIDs, edge.TransitionDetails)
			}
		case "timer.portfolio_digest":
			foundTimerEdge = true
			if len(edge.TimerIDs) == 0 || len(edge.TimerDetails) == 0 {
				t.Fatalf("expected timer metadata on timer.portfolio_digest edge, got ids=%v details=%v", edge.TimerIDs, edge.TimerDetails)
			}
		}
	}
	if !foundTransitionEdge {
		t.Fatalf("expected vertical.shortlisted edge in design graph")
	}
	if !foundTimerEdge {
		t.Fatalf("expected timer.portfolio_digest edge in design graph")
	}
}
