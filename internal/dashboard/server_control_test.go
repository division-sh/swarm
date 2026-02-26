package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"empireai/internal/events"
	"empireai/internal/models"
	"empireai/internal/runtime"
	"github.com/DATA-DOG/go-sqlmock"
)

type stubEventStore struct {
	appendFn  func(ctx context.Context, evt events.Event) error
	deliverFn func(ctx context.Context, eventID string, agentIDs []string) error
}

func (s stubEventStore) AppendEvent(ctx context.Context, evt events.Event) error {
	if s.appendFn != nil {
		return s.appendFn(ctx, evt)
	}
	return nil
}

func (s stubEventStore) InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error {
	if s.deliverFn != nil {
		return s.deliverFn(ctx, eventID, agentIDs)
	}
	return nil
}

type stubBoardAgent struct {
	id string
}

func (a *stubBoardAgent) ID() string                        { return a.id }
func (a *stubBoardAgent) Type() string                      { return "stub" }
func (a *stubBoardAgent) Subscriptions() []events.EventType { return nil }
func (a *stubBoardAgent) OnEvent(context.Context, events.Event) ([]events.Event, error) {
	return nil, nil
}
func (a *stubBoardAgent) BoardStep(context.Context, string) (string, error) { return "ACK", nil }

func TestHandleControlSeedOrg_LoadErrorFromYAML(t *testing.T) {
	agentsDir := t.TempDir()
	t.Setenv("EMPIREAI_GLOBAL_AGENTS_DIR", agentsDir)

	bus := runtime.NewEventBus(runtime.InMemoryEventStore{})
	manager := runtime.NewAgentManager(bus, nil)
	s := NewServer(nil, nil, nil, nil, manager)

	req := httptest.NewRequest(http.MethodPost, "/dashboard/api/control/seed-org", nil)
	w := httptest.NewRecorder()
	s.handleControlSeedOrg(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !strings.Contains(asString(resp["error"]), "load global agents from YAML") {
		t.Fatalf("unexpected error: %v", resp["error"])
	}
}

func TestHandleControlSeedOrg_SuccessFromYAML(t *testing.T) {
	agentsDir := t.TempDir()
	t.Setenv("EMPIREAI_GLOBAL_AGENTS_DIR", agentsDir)
	roster := []byte(strings.Join([]string{
		"agents:",
		"  empire_coordinator:",
		"    config_path: ./empire-coordinator.yaml",
		"  factory_cto:",
		"    config_path: ./factory-cto.yaml",
	}, "\n"))
	if err := os.WriteFile(filepath.Join(agentsDir, "roster.yaml"), roster, 0o644); err != nil {
		t.Fatalf("write roster: %v", err)
	}
	a1 := []byte(strings.Join([]string{
		"id: empire-coordinator",
		"role: empire-coordinator",
		"mode: holding",
		"model_tier: sonnet",
		"system_prompt: |",
		"  You are empire coordinator.",
	}, "\n"))
	a2 := []byte(strings.Join([]string{
		"id: factory-cto",
		"role: factory-cto",
		"mode: factory",
		"model_tier: sonnet",
		"system_prompt: |",
		"  You are factory cto.",
	}, "\n"))
	if err := os.WriteFile(filepath.Join(agentsDir, "empire-coordinator.yaml"), a1, 0o644); err != nil {
		t.Fatalf("write empire-coordinator: %v", err)
	}
	if err := os.WriteFile(filepath.Join(agentsDir, "factory-cto.yaml"), a2, 0o644); err != nil {
		t.Fatalf("write factory-cto: %v", err)
	}

	bus := runtime.NewEventBus(runtime.InMemoryEventStore{})
	manager := runtime.NewAgentManager(bus, nil)
	s := NewServer(nil, nil, nil, nil, manager)

	req := httptest.NewRequest(http.MethodPost, "/dashboard/api/control/seed-org", nil)
	w := httptest.NewRecorder()
	s.handleControlSeedOrg(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		OK           bool     `json:"ok"`
		Created      []string `json:"created"`
		AgentsSource string   `json:"agents_source"`
		Total        int      `json:"total"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !resp.OK {
		t.Fatalf("expected ok response: %s", w.Body.String())
	}
	if resp.AgentsSource != "yaml" {
		t.Fatalf("expected agents_source=yaml, got %q", resp.AgentsSource)
	}
	if resp.Total != 2 {
		t.Fatalf("expected total=2, got %d", resp.Total)
	}
	if manager.Count() != 2 {
		t.Fatalf("expected manager count 2, got %d", manager.Count())
	}
}

func TestHandleControlRuntime_ResetDB_LoadErrorFromYAML(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	agentsDir := t.TempDir() // no roster.yaml => load failure
	t.Setenv("EMPIREAI_GLOBAL_AGENTS_DIR", agentsDir)

	// resetState enumerates public tables dynamically.
	mock.ExpectQuery(`SELECT tablename FROM pg_tables WHERE schemaname = 'public'`).
		WillReturnRows(sqlmock.NewRows([]string{"tablename"}))
	// ensureInitialTemplate short-circuits when template exists.
	mock.ExpectQuery(`SELECT EXISTS\(SELECT 1 FROM org_templates\)`).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))

	bus := runtime.NewEventBus(runtime.InMemoryEventStore{})
	manager := runtime.NewAgentManager(bus, nil)
	s := NewServer(db, nil, nil, nil, manager)

	body := []byte(`{"action":"reset_db","confirm":"RESET","seed_org":true}`)
	req := httptest.NewRequest(http.MethodPost, "/dashboard/api/control/runtime", bytes.NewReader(body))
	w := httptest.NewRecorder()
	s.handleControlRuntime(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !strings.Contains(asString(resp["error"]), "load global agents from YAML") {
		t.Fatalf("unexpected error: %v", resp["error"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestHandleControlRuntime_InvalidAction(t *testing.T) {
	s := NewServer(nil, nil, nil, nil, nil)
	body := []byte(`{"action":"nope"}`)
	req := httptest.NewRequest(http.MethodPost, "/dashboard/api/control/runtime", bytes.NewReader(body))
	w := httptest.NewRecorder()
	s.handleControlRuntime(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestHandleControlRuntime_PauseResume(t *testing.T) {
	runtime.ResumeRuntimeIngress()
	defer runtime.ResumeRuntimeIngress()
	bus := runtime.NewEventBus(runtime.InMemoryEventStore{})
	manager := runtime.NewAgentManager(bus, nil)
	s := NewServer(nil, nil, nil, nil, manager)

	resumeReq := httptest.NewRequest(http.MethodPost, "/dashboard/api/control/runtime", bytes.NewReader([]byte(`{"action":"resume"}`)))
	resumeW := httptest.NewRecorder()
	s.handleControlRuntime(resumeW, resumeReq)
	if resumeW.Code != http.StatusOK {
		t.Fatalf("resume status=%d body=%s", resumeW.Code, resumeW.Body.String())
	}
	if !manager.IsRunning() {
		t.Fatal("expected manager running after resume")
	}
	if runtime.RuntimeIngressPaused() {
		t.Fatal("expected ingress resumed after resume action")
	}

	pauseReq := httptest.NewRequest(http.MethodPost, "/dashboard/api/control/runtime", bytes.NewReader([]byte(`{"action":"pause"}`)))
	pauseW := httptest.NewRecorder()
	s.handleControlRuntime(pauseW, pauseReq)
	if pauseW.Code != http.StatusOK {
		t.Fatalf("pause status=%d body=%s", pauseW.Code, pauseW.Body.String())
	}
	if manager.IsRunning() {
		t.Fatal("expected manager stopped after pause")
	}
	if !runtime.RuntimeIngressPaused() {
		t.Fatal("expected ingress paused after pause action")
	}
}

func TestHandleControlChat_LiveMarksReceiptProcessed(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// lookupControlTarget query.
	mock.ExpectQuery(`SELECT\s+a\.id,`).
		WithArgs("empire-coordinator").
		WillReturnRows(sqlmock.NewRows([]string{"id", "role", "vertical_id", "slug", "status"}).
			AddRow("empire-coordinator", "empire-coordinator", "", "", "active"))

	// upsertEventReceipt query (event id is generated; match args loosely).
	mock.ExpectExec(`INSERT INTO event_receipts`).
		WithArgs(sqlmock.AnyArg(), "empire-coordinator", "processed", "").
		WillReturnResult(sqlmock.NewResult(1, 1))

	bus := runtime.NewEventBus(runtime.InMemoryEventStore{})
	factory := func(cfg models.AgentConfig) (runtime.Agent, error) {
		return &stubBoardAgent{id: cfg.ID}, nil
	}
	manager := runtime.NewAgentManager(bus, factory)
	if err := manager.SpawnAgent(models.AgentConfig{ID: "empire-coordinator", Role: "empire-coordinator"}); err != nil {
		t.Fatalf("spawn: %v", err)
	}

	es := stubEventStore{}
	s := NewServer(db, nil, es, nil, manager)

	body := []byte(`{"agent_id":"empire-coordinator","message":"hi","mode":"live"}`)
	req := httptest.NewRequest(http.MethodPost, "/dashboard/api/control/chat", bytes.NewReader(body))
	w := httptest.NewRecorder()
	s.handleControlChat(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if asString(resp["response"]) != "ACK" {
		t.Fatalf("expected ACK response, got %v", resp["response"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}
