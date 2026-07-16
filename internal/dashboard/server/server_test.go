package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	builderpkg "github.com/division-sh/swarm/internal/builder"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeagents "github.com/division-sh/swarm/internal/runtime/agents"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

type builderRPCResponse = builderpkg.RPCResponse
type builderWSEventFrame = builderpkg.WSEventFrame

const testBuilderAuthToken = "builder-test-token"
const testOperatorAuthToken = "operator-secret"

func asString(v any) string {
	switch typed := v.(type) {
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return ""
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func cloneAnyMap(values map[string]any) map[string]any {
	if values == nil {
		return nil
	}
	out := make(map[string]any, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func ptrTime(v time.Time) *time.Time { return &v }

func parseTestTime(raw string) time.Time {
	ts, _ := time.Parse(time.RFC3339Nano, strings.TrimSpace(raw))
	return ts
}

func canonicalEventAndConversationCaps() store.StoreSchemaCapabilities {
	return store.StoreSchemaCapabilities{
		Events: store.EventSchemaCapabilities{
			Log:        store.SchemaFlavorCanonical,
			Deliveries: store.SchemaFlavorCanonical,
			Receipts:   store.SchemaFlavorCanonical,
		},
		Conversations: store.ConversationSchemaCapabilities{
			Sessions:   store.SchemaFlavorCanonical,
			Audits:     store.SchemaFlavorCanonical,
			Turns:      store.SchemaFlavorCanonical,
			TurnBlocks: true,
		},
	}
}

func canonicalAgentProjectionColumns() []string {
	return []string{
		"agent_id", "status", "session_id", "session_started_at", "turn_count", "lease_holder", "lease_expires_at", "runtime_state", "pending_count", "oldest_pending_age_sec",
	}
}

func setOperatorAuth(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+testOperatorAuthToken)
}

func TestDashboardEventFilterFromRequestPreservesTypedSubscriberIdentity(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/events?type=task.completed&source=runtime&entity_id=entity-1&subscriber=worker-1&subscriber_id=worker-1&subscriber_type=node", nil)

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
	req := httptest.NewRequest(http.MethodGet, "/api/events?subscriber=worker-1&subscriber_type=platform", nil)

	if _, err := dashboardEventFilterFromRequest(req); err == nil || !strings.Contains(err.Error(), "subscriber_type") {
		t.Fatalf("dashboardEventFilterFromRequest error = %v, want subscriber_type rejection", err)
	}
}

func TestDashboardEventFilterFromRequestRejectsConflictingSubscriberAliases(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/events?subscriber=worker-1&subscriber_id=worker-2&subscriber_type=agent", nil)

	if _, err := dashboardEventFilterFromRequest(req); err == nil || !strings.Contains(err.Error(), "subscriber and subscriber_id must match") {
		t.Fatalf("dashboardEventFilterFromRequest error = %v, want alias conflict rejection", err)
	}
}

type stubAgents struct {
	rows []runtimemanager.PersistedAgent
}

func (s stubAgents) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return s.rows, nil
}

func (s stubAgents) ListOperatorAgents(_ context.Context, opts store.OperatorAgentListOptions) (store.OperatorAgentListResult, error) {
	out := store.OperatorAgentListResult{Agents: []store.OperatorAgentSummary{}}
	for _, row := range s.rows {
		if opts.Role != "" && row.Config.Role != opts.Role {
			continue
		}
		item := store.OperatorAgentSummary{
			AgentID:         row.Config.ID,
			Role:            row.Config.Role,
			Type:            row.Config.Type,
			Model:           row.Config.Model,
			Memory:          row.Config.Memory.Enabled,
			MemorySource:    string(row.Config.Memory.Source),
			Status:          row.Status,
			RuntimeFlowID:   row.Config.FlowID,
			FlowInstance:    row.Config.CanonicalFlowPath(),
			EntityID:        row.Config.EffectiveEntityID(),
			ParentAgentID:   row.ParentAgentID,
			CoordinatorID:   row.CoordinatorID,
			HiredBy:         row.HiredBy,
			TemplateVersion: row.TemplateVersion,
			BudgetEnvelope:  row.Config.BudgetEnvelope,
			Subscriptions:   append([]string(nil), row.Config.Subscriptions...),
			Permissions:     append([]string(nil), row.Config.Permissions...),
			StartedAt:       row.StartedAt,
			DashboardStatus: row.Status,
			DashboardState:  row.Status,
		}
		out.Agents = append(out.Agents, item)
	}
	return out, nil
}

func (s stubAgents) LoadOperatorAgent(ctx context.Context, agentID string) (store.OperatorAgentDetail, error) {
	result, err := s.ListOperatorAgents(ctx, store.OperatorAgentListOptions{})
	if err != nil {
		return store.OperatorAgentDetail{}, err
	}
	for _, row := range result.Agents {
		if row.AgentID == agentID {
			return store.OperatorAgentDetail{Agent: row}, nil
		}
	}
	return store.OperatorAgentDetail{}, store.ErrAgentNotFound
}

type stubMailbox struct {
	items map[string]runtimetools.MailboxItem
	list  []runtimetools.MailboxItem
}

func (s stubMailbox) ListMailboxItems(context.Context, string, int) ([]runtimetools.MailboxItem, error) {
	return s.list, nil
}

func (s stubMailbox) GetMailboxItem(_ context.Context, id string) (runtimetools.MailboxItem, error) {
	return s.items[id], nil
}

type stubInstances struct {
	rows          []store.OperatorEntitySummary
	byID          map[string]store.OperatorEntityFull
	lastAggregate *store.OperatorEntityAggregateOptions
}

func (s stubInstances) ListOperatorEntities(_ context.Context, opts store.OperatorEntityListOptions) (store.OperatorEntityListResult, error) {
	rows := make([]store.OperatorEntitySummary, 0, len(s.rows))
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
	return store.OperatorEntityListResult{Entities: rows}, nil
}

func (s stubInstances) LoadOperatorEntity(_ context.Context, entityID, runID string) (store.OperatorEntityFull, error) {
	item, ok := s.byID[entityID]
	if !ok {
		return store.OperatorEntityFull{}, store.ErrEntityNotFound
	}
	if runID != "" && item.Entity.RunID != runID {
		return store.OperatorEntityFull{}, store.ErrEntityNotFound
	}
	return item, nil
}

func (s stubInstances) AggregateOperatorEntities(_ context.Context, opts store.OperatorEntityAggregateOptions) (store.OperatorEntityAggregateResult, error) {
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
	return store.OperatorEntityAggregateResult{Counts: counts}, nil
}

func TestHandler_InstanceHandlersReturnCanonicalEntityProjection(t *testing.T) {
	entityID := runtimeflowidentity.EntityID("wf-1")
	lastAggregate := &store.OperatorEntityAggregateOptions{}
	h := &Handler{
		entities: stubInstances{
			rows: []store.OperatorEntitySummary{{
				EntityID:     entityID,
				RunID:        "run-1",
				FlowInstance: "order/wf-1",
				EntityType:   "order",
				CurrentState: "reviewing",
			}},
			byID: map[string]store.OperatorEntityFull{
				entityID: {
					Entity: store.OperatorEntitySummary{
						EntityID:     entityID,
						RunID:        "run-1",
						FlowInstance: "order/wf-1",
						EntityType:   "order",
						CurrentState: "reviewing",
					},
					Fields: map[string]any{"business_status": "approved"},
					Gates:  map[string]bool{"review_gate": true},
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
	var detail store.OperatorEntityFull
	if err := json.Unmarshal(rec.Body.Bytes(), &detail); err != nil {
		t.Fatalf("unmarshal instance detail: %v", err)
	}
	if detail.Entity.CurrentState != "reviewing" || detail.Fields["business_status"] != "approved" || !detail.Gates["review_gate"] || detail.Accumulated["score"] != float64(9) {
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

func TestHandler_AgentHandlersDoNotExposeUnsupportedMetricStubs(t *testing.T) {
	now := time.Date(2026, 5, 22, 3, 30, 0, 0, time.UTC)
	h := &Handler{
		agents: stubAgents{rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ExecutionMode: "live",
				ID:            "agent-1",
				Role:          "worker",
				Type:          "managed",
				Model:         "cheap",
				Memory:        agentmemory.Authored(true),
				FlowPath:      "research/inst-1",
			},
			Status:    "active",
			StartedAt: now,
		}}},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/agents", nil)
	h.handleAgents(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("handleAgents status=%d body=%s", rec.Code, rec.Body.String())
	}
	var listPayload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &listPayload); err != nil {
		t.Fatalf("unmarshal agents: %v", err)
	}
	rows, ok := listPayload["agents"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("agents payload = %#v", listPayload)
	}
	assertUnsupportedAgentMetricStubsAbsent(t, rows[0].(map[string]any))

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/agents/agent-1", nil)
	req.SetPathValue("id", "agent-1")
	h.handleAgentDetail(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("handleAgentDetail status=%d body=%s", rec.Code, rec.Body.String())
	}
	var detailPayload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &detailPayload); err != nil {
		t.Fatalf("unmarshal agent detail: %v", err)
	}
	assertUnsupportedAgentMetricStubsAbsent(t, detailPayload)
}

func assertUnsupportedAgentMetricStubsAbsent(t *testing.T, payload map[string]any) {
	t.Helper()
	for _, key := range []string{"turns_24h", "in_flight_seconds"} {
		if _, ok := payload[key]; ok {
			t.Fatalf("agent payload exposed unsupported metric stub %q: %#v", key, payload)
		}
	}
}

type stubObservability struct {
	events      []eventRecord
	eventDetail map[string]eventRecord
	runtimeLogs []runtimeLogRecord
	incidents   []incidentRecord
}

type stubRunTrace struct {
	rows map[string][]store.RunDebugTraceRow
	err  error
}

func (s stubRunTrace) LoadRunDebugTrace(_ context.Context, runID string, _ store.RunDebugTraceQueryOptions) ([]store.RunDebugTraceRow, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.rows[strings.TrimSpace(runID)], nil
}

type stubBuilderRunStore struct {
	mu          sync.Mutex
	events      []events.Event
	snapshots   map[string]runtimebus.RunLifecycleSnapshot
	runControls map[string]string
}

func (s *stubBuilderRunStore) AppendEvent(_ context.Context, evt events.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, evt)
	return nil
}
func (*stubBuilderRunStore) InsertEventDeliveries(context.Context, string, []string) error {
	return nil
}
func (*stubBuilderRunStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return []string{}, nil
}
func (s *stubBuilderRunStore) MarkRunTerminal(_ context.Context, runID, status string, failure *runtimefailures.Envelope, endedAt time.Time) (runtimebus.RunLifecycleSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshots == nil {
		s.snapshots = map[string]runtimebus.RunLifecycleSnapshot{}
	}
	snap := s.snapshots[runID]
	snap.RunID = runID
	snap.Status = status
	snap.Failure = runtimefailures.CloneEnvelope(failure)
	ended := endedAt
	snap.EndedAt = &ended
	seenEntities := map[string]struct{}{}
	eventCount := 0
	var startedAt time.Time
	for _, evt := range s.events {
		if strings.TrimSpace(evt.RunID()) != strings.TrimSpace(runID) {
			continue
		}
		eventCount++
		if startedAt.IsZero() || evt.CreatedAt().Before(startedAt) {
			startedAt = evt.CreatedAt()
		}
		if entityID := strings.TrimSpace(evt.EntityID()); entityID != "" {
			seenEntities[entityID] = struct{}{}
		}
	}
	snap.EventCount = eventCount
	snap.EntityCount = len(seenEntities)
	snap.StartedAt = startedAt
	s.snapshots[runID] = snap
	return snap, nil
}
func (s *stubBuilderRunStore) LoadRunLifecycleSnapshot(_ context.Context, runID string) (runtimebus.RunLifecycleSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshots[runID], nil
}

func (s *stubBuilderRunStore) StopRunControl(_ context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	runID := strings.TrimSpace(req.RunID)
	status, ok := s.stubRunStatusLocked(runID)
	if !ok {
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrRunNotFound, RunID: runID}
	}
	if stubRunTerminal(status) {
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrAlreadyTerminal, RunID: runID, CurrentStatus: status}
	}
	if s.snapshots == nil {
		s.snapshots = map[string]runtimebus.RunLifecycleSnapshot{}
	}
	if s.runControls == nil {
		s.runControls = map[string]string{}
	}
	snap := s.snapshots[runID]
	snap.RunID = runID
	snap.Status = "cancelled"
	ended := req.Now
	if ended.IsZero() {
		ended = time.Now().UTC()
	}
	snap.EndedAt = &ended
	s.snapshots[runID] = snap
	s.runControls[runID] = "stopped"
	return runtimeruncontrol.State{RunID: runID, Status: "cancelled", ControlStatus: "stopped"}, nil
}

func (s *stubBuilderRunStore) PauseRunControl(_ context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	runID := strings.TrimSpace(req.RunID)
	status, ok := s.stubRunStatusLocked(runID)
	if !ok {
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrRunNotFound, RunID: runID}
	}
	if stubRunTerminal(status) {
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrAlreadyTerminal, RunID: runID, CurrentStatus: status}
	}
	if status == "paused" && s.runControls[runID] == "paused" {
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrAlreadyPaused, RunID: runID, CurrentStatus: status}
	}
	if s.snapshots == nil {
		s.snapshots = map[string]runtimebus.RunLifecycleSnapshot{}
	}
	if s.runControls == nil {
		s.runControls = map[string]string{}
	}
	snap := s.snapshots[runID]
	snap.RunID = runID
	snap.Status = "paused"
	s.snapshots[runID] = snap
	s.runControls[runID] = "paused"
	return runtimeruncontrol.State{RunID: runID, Status: "paused", ControlStatus: "paused"}, nil
}

func (s *stubBuilderRunStore) ContinueRunControl(_ context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	runID := strings.TrimSpace(req.RunID)
	status, ok := s.stubRunStatusLocked(runID)
	if !ok {
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrRunNotFound, RunID: runID}
	}
	if stubRunTerminal(status) {
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrAlreadyTerminal, RunID: runID, CurrentStatus: status}
	}
	if status != "paused" || s.runControls[runID] != "paused" {
		return runtimeruncontrol.State{}, &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrNotPaused, RunID: runID, CurrentStatus: status}
	}
	snap := s.snapshots[runID]
	snap.RunID = runID
	snap.Status = "running"
	s.snapshots[runID] = snap
	s.runControls[runID] = "running"
	return runtimeruncontrol.State{RunID: runID, Status: "running", ControlStatus: "running"}, nil
}

func (s *stubBuilderRunStore) RunDispatchBlocked(_ context.Context, runID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.runControls[strings.TrimSpace(runID)] == "paused", nil
}

func (s *stubBuilderRunStore) stubRunStatusLocked(runID string) (string, bool) {
	if snap, ok := s.snapshots[runID]; ok && strings.TrimSpace(snap.Status) != "" {
		return strings.TrimSpace(snap.Status), true
	}
	for _, evt := range s.events {
		if strings.TrimSpace(evt.RunID()) == runID {
			return "running", true
		}
	}
	return "", false
}

func stubRunTerminal(status string) bool {
	switch strings.TrimSpace(status) {
	case "completed", "failed", "cancelled", "stopped", "abandoned":
		return true
	default:
		return false
	}
}

func (s *stubBuilderRunStore) LoadRunDebugReport(_ context.Context, runID string, _ store.RunDebugQueryOptions) (store.RunDebugReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	report := store.RunDebugReport{
		RunID:             strings.TrimSpace(runID),
		EventCounts:       []store.RunDebugEventCount{},
		Deliveries:        []store.RunDebugDeliveryCount{},
		Events:            []store.RunDebugEvent{},
		DeadLetters:       []store.RunDebugDeadLetter{},
		AgentTurns:        []store.RunDebugAgentTurn{},
		Mutations:         []store.RunDebugMutation{},
		RuntimeLogs:       []store.RunDebugRuntimeLog{},
		RuntimeLogSummary: []store.RunDebugRuntimeSummary{},
	}
	if snap, ok := s.snapshots[runID]; ok {
		report.RunTableStatus = snap.Status
		report.Failure = runtimefailures.CloneEnvelope(snap.Failure)
		report.EntityCount = snap.EntityCount
		if snap.EndedAt != nil {
			ended := snap.EndedAt.UTC()
			report.EndedAt = &ended
		}
		if !snap.StartedAt.IsZero() {
			report.StartedAt = snap.StartedAt.UTC()
		}
	}
	counts := map[string]int{}
	for _, evt := range s.events {
		if strings.TrimSpace(evt.RunID()) != report.RunID {
			continue
		}
		report.EventCount++
		if report.StartedAt.IsZero() || evt.CreatedAt().Before(report.StartedAt) {
			report.StartedAt = evt.CreatedAt().UTC()
		}
		if evt.CreatedAt().After(report.LastEventAt) {
			report.LastEventAt = evt.CreatedAt().UTC()
		}
		counts[string(evt.Type())]++
		if report.RootEventID == "" || evt.CreatedAt().Before(report.StartedAt) {
			report.RootEventID = strings.TrimSpace(evt.ID())
			report.RootEventType = strings.TrimSpace(string(evt.Type()))
		}
		if evt.Type() == events.EventType("platform.runtime_log") {
			payload := map[string]any{}
			_ = json.Unmarshal(evt.Payload(), &payload)
			details, _ := payload["details"].(map[string]any)
			detailJSON, _ := json.Marshal(details)
			var failure *runtimefailures.Envelope
			if raw, err := json.Marshal(details["failure"]); err == nil && string(raw) != "null" {
				if decoded, err := runtimefailures.UnmarshalEnvelope(raw); err == nil {
					failure = &decoded
				}
			}
			report.RuntimeLogs = append(report.RuntimeLogs, store.RunDebugRuntimeLog{
				EventID:   strings.TrimSpace(evt.ID()),
				Level:     strings.TrimSpace(asString(payload["log_level"])),
				Message:   strings.TrimSpace(asString(payload["message"])),
				Component: strings.TrimSpace(asString(details["component"])),
				Action:    strings.TrimSpace(asString(details["action"])),
				EventType: strings.TrimSpace(asString(details["event_type"])),
				AgentID:   strings.TrimSpace(asString(details["agent_id"])),
				EntityID:  strings.TrimSpace(asString(details["entity_id"])),
				Failure:   failure,
				Detail:    append(json.RawMessage(nil), detailJSON...),
				CreatedAt: evt.CreatedAt().UTC(),
			})
			continue
		}
		payload := append(json.RawMessage(nil), evt.Payload()...)
		report.Events = append(report.Events, store.RunDebugEvent{
			EventID:    strings.TrimSpace(evt.ID()),
			EventName:  strings.TrimSpace(string(evt.Type())),
			EntityID:   strings.TrimSpace(evt.EntityID()),
			CreatedAt:  evt.CreatedAt().UTC(),
			Source:     strings.TrimSpace(evt.SourceAgent()),
			SourceType: "agent",
			Payload:    payload,
		})
	}
	for eventName, count := range counts {
		report.EventCounts = append(report.EventCounts, store.RunDebugEventCount{EventName: eventName, Count: count})
	}
	slices.SortFunc(report.Events, func(a, b store.RunDebugEvent) int { return b.CreatedAt.Compare(a.CreatedAt) })
	slices.SortFunc(report.RuntimeLogs, func(a, b store.RunDebugRuntimeLog) int { return b.CreatedAt.Compare(a.CreatedAt) })
	slices.SortFunc(report.EventCounts, func(a, b store.RunDebugEventCount) int { return strings.Compare(a.EventName, b.EventName) })
	if report.RootEventID == "" && len(report.Events) > 0 {
		root := report.Events[len(report.Events)-1]
		report.RootEventID = root.EventID
		report.RootEventType = root.EventName
	}
	return report, nil
}

func (s *stubBuilderRunStore) LoadRunDebugTracePage(_ context.Context, runID string, opts store.RunDebugTraceQueryOptions) ([]store.RunDebugTraceRow, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	runID = strings.TrimSpace(runID)
	rows := []store.RunDebugTraceRow{}
	for _, evt := range s.events {
		if strings.TrimSpace(evt.RunID()) != runID {
			continue
		}
		if opts.Since != nil && !evt.CreatedAt().After(opts.Since.UTC()) {
			continue
		}
		rows = append(rows, store.RunDebugTraceRow{
			EventID:         strings.TrimSpace(evt.ID()),
			EventName:       strings.TrimSpace(string(evt.Type())),
			SourceEventID:   strings.TrimSpace(evt.ParentEventID()),
			EntityID:        strings.TrimSpace(evt.EntityID()),
			EventSource:     strings.TrimSpace(evt.SourceAgent()),
			EventSourceType: "agent",
			EventCreatedAt:  evt.CreatedAt().UTC(),
		})
	}
	slices.SortFunc(rows, func(a, b store.RunDebugTraceRow) int {
		if cmp := a.EventCreatedAt.Compare(b.EventCreatedAt); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.EventID, b.EventID)
	})
	limit := opts.Limit
	if limit <= 0 || limit > len(rows) {
		limit = len(rows)
	}
	return append([]store.RunDebugTraceRow(nil), rows[:limit]...), "", nil
}

func (s *stubBuilderRunStore) ListOperatorEvents(_ context.Context, opts store.OperatorEventListOptions) (store.OperatorEventListResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	eventsOut := []store.OperatorEventFull{}
	for _, evt := range s.events {
		if strings.TrimSpace(evt.RunID()) != strings.TrimSpace(opts.Filter.RunID) {
			continue
		}
		if opts.ExcludeRuntimeLogs && evt.Type() == events.EventType("platform.runtime_log") {
			continue
		}
		if opts.Since != nil && !evt.CreatedAt().After(opts.Since.UTC()) {
			continue
		}
		payload := map[string]any{}
		_ = json.Unmarshal(evt.Payload(), &payload)
		eventsOut = append(eventsOut, store.OperatorEventFull{
			EventID:       strings.TrimSpace(evt.ID()),
			EventName:     strings.TrimSpace(string(evt.Type())),
			ExecutionMode: evt.ExecutionMode(),
			EntityID:      strings.TrimSpace(evt.EntityID()),
			RunID:         strings.TrimSpace(evt.RunID()),
			CreatedAt:     evt.CreatedAt().UTC(),
			Source:        strings.TrimSpace(evt.SourceAgent()),
			ProducerType:  evt.ProducerType(),
			Payload:       payload,
		})
	}
	slices.SortFunc(eventsOut, func(a, b store.OperatorEventFull) int {
		if cmp := a.CreatedAt.Compare(b.CreatedAt); cmp != 0 {
			if opts.Order == "asc" {
				return cmp
			}
			return -cmp
		}
		if opts.Order == "asc" {
			return strings.Compare(a.EventID, b.EventID)
		}
		return strings.Compare(b.EventID, a.EventID)
	})
	limit := opts.Limit
	if limit <= 0 || limit > len(eventsOut) {
		limit = len(eventsOut)
	}
	return store.OperatorEventListResult{Events: append([]store.OperatorEventFull(nil), eventsOut[:limit]...)}, nil
}

func (s *stubBuilderRunStore) ListOperatorRuntimeLogs(_ context.Context, opts store.OperatorRuntimeLogListOptions) (store.OperatorRuntimeLogListResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	logs := []store.OperatorRuntimeLogEntry{}
	for _, evt := range s.events {
		if strings.TrimSpace(evt.RunID()) != strings.TrimSpace(opts.RunID) || evt.Type() != events.EventType("platform.runtime_log") {
			continue
		}
		if opts.Since != nil && !evt.CreatedAt().After(opts.Since.UTC()) {
			continue
		}
		payload := map[string]any{}
		_ = json.Unmarshal(evt.Payload(), &payload)
		details, _ := payload["details"].(map[string]any)
		logs = append(logs, store.OperatorRuntimeLogEntry{
			LogID:     strings.TrimSpace(evt.ID()),
			TS:        evt.CreatedAt().UTC(),
			Level:     strings.TrimSpace(asString(payload["log_level"])),
			Component: strings.TrimSpace(asString(details["component"])),
			Source:    strings.TrimSpace(firstNonEmpty(asString(details["agent_id"]), evt.SourceAgent())),
			RunID:     strings.TrimSpace(evt.RunID()),
			EntityID:  strings.TrimSpace(firstNonEmpty(evt.EntityID(), asString(details["entity_id"]))),
			ErrorCode: strings.TrimSpace(asString(details["error_code"])),
			Message:   strings.TrimSpace(asString(payload["message"])),
			Details:   cloneAnyMap(details),
		})
	}
	slices.SortFunc(logs, func(a, b store.OperatorRuntimeLogEntry) int {
		if cmp := a.TS.Compare(b.TS); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.LogID, b.LogID)
	})
	limit := opts.Limit
	if limit <= 0 || limit > len(logs) {
		limit = len(logs)
	}
	return store.OperatorRuntimeLogListResult{Logs: append([]store.OperatorRuntimeLogEntry(nil), logs[:limit]...)}, nil
}

type stubConversationCaps struct {
	caps store.StoreSchemaCapabilities
	err  error
}

type stubDashboardConversationHTTPReader struct {
	listResult store.OperatorConversationListResult
	turnPages  map[string]store.OperatorConversationTurnListResult
	listOpts   store.OperatorConversationListOptions
	turnOpts   []store.OperatorConversationTurnListOptions
}

func (s *stubDashboardConversationHTTPReader) ListOperatorConversations(_ context.Context, opts store.OperatorConversationListOptions) (store.OperatorConversationListResult, error) {
	s.listOpts = opts
	return s.listResult, nil
}

func (s *stubDashboardConversationHTTPReader) ListOperatorConversationTurns(_ context.Context, opts store.OperatorConversationTurnListOptions) (store.OperatorConversationTurnListResult, error) {
	s.turnOpts = append(s.turnOpts, opts)
	return s.turnPages[strings.TrimSpace(opts.Cursor)], nil
}

func (s *stubDashboardConversationHTTPReader) LoadOperatorPublicConversationTurn(context.Context, string, string) (store.OperatorPublicConversationTurnDetail, error) {
	return store.OperatorPublicConversationTurnDetail{}, store.ErrTurnNotFound
}

type stubDashboardAgentHTTPReader struct {
	result store.OperatorAgentListResult
	detail store.OperatorAgentDetail
}

func (s stubDashboardAgentHTTPReader) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return nil, nil
}

func (s stubDashboardAgentHTTPReader) ListOperatorAgents(context.Context, store.OperatorAgentListOptions) (store.OperatorAgentListResult, error) {
	return s.result, nil
}

func (s stubDashboardAgentHTTPReader) LoadOperatorAgent(_ context.Context, agentID string) (store.OperatorAgentDetail, error) {
	if s.detail.Agent.AgentID != strings.TrimSpace(agentID) {
		return store.OperatorAgentDetail{}, store.ErrAgentNotFound
	}
	return s.detail, nil
}

func (s stubConversationCaps) ResolveSchemaCapabilities(context.Context) (store.StoreSchemaCapabilities, error) {
	return s.caps, s.err
}

func (s stubConversationCaps) ListOperatorConversationTurns(_ context.Context, _ store.OperatorConversationTurnListOptions) (store.OperatorConversationTurnListResult, error) {
	return store.OperatorConversationTurnListResult{Turns: []store.OperatorConversationTurnListItem{}}, nil
}

func (s stubConversationCaps) LoadOperatorPublicConversationTurn(context.Context, string, string) (store.OperatorPublicConversationTurnDetail, error) {
	return store.OperatorPublicConversationTurnDetail{}, store.ErrTurnNotFound
}

type stubSQLAgents struct {
	rows           []runtimemanager.PersistedAgent
	caps           store.StoreSchemaCapabilities
	err            error
	facts          map[string]store.PendingAgentDeliveryFacts
	lifecycleFacts map[string]store.AgentDeliveryLifecycleFacts
	turns          map[string][]store.OperatorPublicConversationTurn
	turnErr        error
}

type stubSQLAgentsWithoutLifecycle struct {
	rows  []runtimemanager.PersistedAgent
	caps  store.StoreSchemaCapabilities
	facts map[string]store.PendingAgentDeliveryFacts
}

func (s stubSQLAgentsWithoutLifecycle) ListOperatorConversationTurns(_ context.Context, opts store.OperatorConversationTurnListOptions) (store.OperatorConversationTurnListResult, error) {
	return store.OperatorConversationTurnListResult{Turns: []store.OperatorConversationTurnListItem{}}, nil
}

func (s stubSQLAgentsWithoutLifecycle) LoadOperatorPublicConversationTurn(context.Context, string, string) (store.OperatorPublicConversationTurnDetail, error) {
	return store.OperatorPublicConversationTurnDetail{}, store.ErrTurnNotFound
}

func (s stubSQLAgentsWithoutLifecycle) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return s.rows, nil
}

func (s stubSQLAgentsWithoutLifecycle) ResolveSchemaCapabilities(context.Context) (store.StoreSchemaCapabilities, error) {
	return s.caps, nil
}

func (s stubSQLAgentsWithoutLifecycle) ListPendingAgentDeliveryFacts(_ context.Context, agentIDs []string, _ time.Time) (map[string]store.PendingAgentDeliveryFacts, error) {
	out := make(map[string]store.PendingAgentDeliveryFacts, len(agentIDs))
	for _, agentID := range agentIDs {
		out[agentID] = s.facts[agentID]
	}
	return out, nil
}

func (s stubSQLAgents) LoadAgents(context.Context) ([]runtimemanager.PersistedAgent, error) {
	return s.rows, nil
}

func (s stubSQLAgents) ResolveSchemaCapabilities(context.Context) (store.StoreSchemaCapabilities, error) {
	return s.caps, s.err
}

func (s stubSQLAgents) ListPendingAgentDeliveryFacts(_ context.Context, agentIDs []string, _ time.Time) (map[string]store.PendingAgentDeliveryFacts, error) {
	out := make(map[string]store.PendingAgentDeliveryFacts, len(agentIDs))
	for _, agentID := range agentIDs {
		out[agentID] = s.facts[agentID]
	}
	return out, nil
}

func (s stubSQLAgents) ListAgentDeliveryLifecycleFacts(_ context.Context, agentIDs []string) (map[string]store.AgentDeliveryLifecycleFacts, error) {
	out := make(map[string]store.AgentDeliveryLifecycleFacts, len(agentIDs))
	for _, agentID := range agentIDs {
		out[agentID] = s.lifecycleFacts[agentID]
	}
	return out, nil
}

func (s stubSQLAgents) ListOperatorConversationTurns(_ context.Context, opts store.OperatorConversationTurnListOptions) (store.OperatorConversationTurnListResult, error) {
	if s.turnErr != nil {
		return store.OperatorConversationTurnListResult{}, s.turnErr
	}
	publicTurns := s.turns[strings.TrimSpace(opts.SessionID)]
	page := store.OperatorConversationTurnListResult{Turns: []store.OperatorConversationTurnListItem{}}
	for _, turn := range publicTurns {
		page.Turns = append(page.Turns, store.OperatorConversationTurnListItem{
			TurnID: turn.TurnID, Ordinal: turn.Ordinal, CompletedAt: turn.CompletedAt,
			DurationMS: turn.DurationMS, TriggerEventID: turn.TriggerEventID,
			TriggerEventType: turn.TriggerEventType, Tokens: turn.Tokens,
			Outcome: turn.Outcome, ParseOK: turn.ParseOK, Failure: turn.Failure,
		})
	}
	return page, nil
}

func (s stubSQLAgents) LoadOperatorPublicConversationTurn(_ context.Context, sessionID, turnID string) (store.OperatorPublicConversationTurnDetail, error) {
	if s.turnErr != nil {
		return store.OperatorPublicConversationTurnDetail{}, s.turnErr
	}
	for _, turn := range s.turns[strings.TrimSpace(sessionID)] {
		if turn.TurnID == strings.TrimSpace(turnID) {
			return store.OperatorPublicConversationTurnDetail{Turn: turn}, nil
		}
	}
	return store.OperatorPublicConversationTurnDetail{}, store.ErrTurnNotFound
}

func (s stubObservability) ListEvents(context.Context, EventFilter, int) ([]eventRecord, error) {
	return s.events, nil
}

func (s stubObservability) GetEvent(_ context.Context, id string) (eventRecord, bool, error) {
	item, ok := s.eventDetail[id]
	return item, ok, nil
}

func (s stubObservability) ListRuntimeLogs(context.Context, RuntimeLogFilter, int) ([]runtimeLogRecord, error) {
	return s.runtimeLogs, nil
}

func (s stubObservability) ListIncidents(context.Context, IncidentFilter) ([]incidentRecord, error) {
	return s.incidents, nil
}

type stubAgentControl struct {
	lastDirective  runtimeagentcontrol.SendDirectiveRequest
	directiveCalls int
	restartCalls   int
	replayCalls    int
}

func (s *stubAgentControl) Restart(_ context.Context, req runtimeagentcontrol.RestartRequest) (runtimeagentcontrol.RestartResult, error) {
	s.restartCalls++
	return runtimeagentcontrol.RestartResult{AgentID: req.AgentID}, nil
}

func (s *stubAgentControl) ReplayBacklog(_ context.Context, req runtimeagentcontrol.ReplayBacklogRequest) (runtimeagentcontrol.ReplayBacklogResult, error) {
	s.replayCalls++
	return runtimeagentcontrol.ReplayBacklogResult{AgentID: req.AgentID, ReplayedCount: 3}, nil
}

func (s *stubAgentControl) SendDirective(_ context.Context, req runtimeagentcontrol.SendDirectiveRequest) (runtimeagentcontrol.SendDirectiveResult, error) {
	s.directiveCalls++
	s.lastDirective = req
	return runtimeagentcontrol.SendDirectiveResult{AgentID: req.AgentID, Response: "ok"}, nil
}

type directiveSurfaceRuntime struct {
	requiredTool string
}

func (r *directiveSurfaceRuntime) StartSession(_ context.Context, agentID, systemPrompt string, tools []runtimellm.ToolDefinition) (*runtimellm.Session, error) {
	return &runtimellm.Session{
		ID:           "sess-1",
		AgentID:      agentID,
		SystemPrompt: systemPrompt,
		Tools:        tools,
	}, nil
}

func (r *directiveSurfaceRuntime) ContinueSession(_ context.Context, s *runtimellm.Session, message runtimellm.Message) (*runtimellm.Response, error) {
	if strings.TrimSpace(message.Role) == "tool" {
		if testToolsContainName(s.Tools, r.requiredTool) {
			return &runtimellm.Response{Message: runtimellm.Message{Role: "assistant", Content: "workflow dispatched"}}, nil
		}
		return &runtimellm.Response{Message: runtimellm.Message{Role: "assistant", Content: "The required emit_scan_requested tool is not registered as a callable function in the runtime tool environment."}}, nil
	}
	if testToolsContainName(s.Tools, r.requiredTool) {
		return &runtimellm.Response{
			Message: runtimellm.Message{Role: "assistant", Content: "Dispatching workflow now."},
			ToolCalls: []runtimellm.ToolCall{{
				Name:      r.requiredTool,
				Arguments: map[string]any{"entity_id": "entity-1"},
			}},
		}, nil
	}
	return &runtimellm.Response{
		Message: runtimellm.Message{Role: "assistant", Content: "Checking state before acting."},
		ToolCalls: []runtimellm.ToolCall{{
			Name:      "query_entities",
			Arguments: map[string]any{"entity_id": "entity-1"},
		}},
	}, nil
}

type directiveSurfaceToolExecutor struct {
	defs     *runtimetools.Executor
	executed []string
}

func (e *directiveSurfaceToolExecutor) Execute(ctx context.Context, name string, input any) (any, error) {
	set, ok := toolcapabilities.FromContext(ctx)
	if !ok {
		return nil, errors.New("missing tool capabilities")
	}
	cap, ok := set.Capability(name)
	if !ok || !cap.Callable {
		return nil, errors.New("tool not callable in this turn")
	}
	e.executed = append(e.executed, strings.TrimSpace(name))
	if strings.HasPrefix(strings.TrimSpace(name), "emit_") {
		if rec, ok := runtimebus.EmittedEventsRecorderFromContext(ctx); ok && rec != nil {
			rec.Append(eventtest.PersistedProjection("", events.EventType(strings.TrimPrefix(strings.TrimSpace(name), "emit_")), "", "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
		}
		return map[string]any{"status": "published"}, nil
	}
	return map[string]any{"ok": true, "input": input}, nil
}

func (e *directiveSurfaceToolExecutor) ToolDefinitionsForActor(cfg runtimeactors.AgentConfig) []runtimellm.ToolDefinition {
	return e.defs.ToolDefinitionsForActor(cfg)
}

func (e *directiveSurfaceToolExecutor) ToolCapabilitiesForActor(actor runtimeactors.AgentConfig, names []string, requestAllowed map[string]struct{}) toolcapabilities.Set {
	return e.defs.ToolCapabilitiesForActor(actor, names, requestAllowed)
}

func testToolsContainName(tools []runtimellm.ToolDefinition, want string) bool {
	for _, tool := range tools {
		if strings.TrimSpace(tool.Name) == strings.TrimSpace(want) {
			return true
		}
	}
	return false
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

type stubProjectControl struct {
	current builderpkg.ProjectStatus
}

func (s *stubProjectControl) OpenProject(_ context.Context, projectDir string) (builderpkg.ProjectStatus, error) {
	s.current = builderpkg.ProjectStatus{
		ProjectDir:      strings.TrimSpace(projectDir),
		Loaded:          true,
		WorkflowName:    "sample",
		WorkflowVersion: "v1",
	}
	return s.current, nil
}

func (s *stubProjectControl) ReloadProject(_ context.Context, projectDir string) (builderpkg.ProjectStatus, error) {
	if strings.TrimSpace(projectDir) != "" {
		s.current.ProjectDir = strings.TrimSpace(projectDir)
	}
	s.current.Loaded = true
	return s.current, nil
}

func (s *stubProjectControl) CloseProject(context.Context) (builderpkg.ProjectStatus, error) {
	s.current = builderpkg.ProjectStatus{}
	return s.current, nil
}

func (s *stubProjectControl) CurrentProject() builderpkg.ProjectStatus {
	return s.current
}

func newBuilderHandlerForTest(
	health HealthChecker,
	entities EntityReader,
	version string,
	runtimeCtl RuntimeController,
	rt *runtimepkg.Runtime,
	projectCtl builderpkg.ProjectController,
) http.Handler {
	var runtimeProvider builderpkg.RuntimeProvider
	var runDebug builderpkg.RunDebugReader
	if rt != nil {
		runtimeProvider = func() *runtimepkg.Runtime { return rt }
		if typed, ok := rt.Bus.Store().(*stubBuilderRunStore); ok {
			runDebug = typed
			if rt.RunControl == nil {
				rt.RunControl = runtimeruncontrol.NewController(typed, nil, runtimeruncontrol.Options{})
			}
		}
	}
	return builderpkg.NewHandler(builderpkg.Options{
		Health:         builderpkg.HealthChecker(health),
		Entities:       entities,
		Runtime:        runtimeCtl,
		AuthToken:      testBuilderAuthToken,
		Version:        version,
		CurrentRuntime: runtimeProvider,
		ProjectControl: projectCtl,
		RunDebug:       runDebug,
	})
}

func builderAuthRequest(method, path, body string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testBuilderAuthToken)
	return req
}

func builderAuthHeader() http.Header {
	return http.Header{"Authorization": []string{"Bearer " + testBuilderAuthToken}}
}

func TestHandler_AgentDirective_UsesLiveFactoryCreatedEmitToolSurface(t *testing.T) {
	t.Skip("legacy dashboard/Builder operator endpoint retired under #731; canonical v1 owner tests cover this behavior")
	reviewFlow := runtimecontracts.FlowContractView{
		Paths: runtimecontracts.FlowContractPaths{
			ID:   "review",
			Flow: "review",
		},
		Path: "review",
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.requested": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"entity_id": {Type: "string"},
					},
					Required: []string{"entity_id"},
				},
			},
		},
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		FlowTree: flowmodel.Tree[runtimecontracts.FlowContractView]{
			Root: &runtimecontracts.FlowContractView{
				Children: []runtimecontracts.FlowContractView{reviewFlow},
			},
			ByID: map[string]*runtimecontracts.FlowContractView{
				"review": &reviewFlow,
			},
		},
	})
	exec := &directiveSurfaceToolExecutor{
		defs: runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{
			WorkflowSource: source,
			EmitRegistry:   runtimetools.NewEmitRegistry(source, nil),
		}),
	}
	factory := runtimeagents.NewLLMAgentFactory(&directiveSurfaceRuntime{requiredTool: "emit_scan_requested"}, exec, nil, runtimeagents.LLMAgentOptions{})
	manager := runtimemanager.NewAgentManager(nil, factory)
	if err := manager.SpawnAgent(runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            "review-coordinator-inst-1",
		Role:          "review_coordinator",
		FlowID:        "review",
		FlowPath:      "review/inst-1",
		EmitEvents:    []string{"review/inst-1/scan.requested"},
		Config:        runtimemanager.MustJSON(map[string]any{"system_prompt": "Coordinate review startup."}),
	}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}

	handler := NewHandler(Options{
		AuthToken:    testOperatorAuthToken,
		AgentControl: manager,
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/agents/review-coordinator-inst-1/actions/directive", strings.NewReader(`{"message":"start the review workflow"}`))
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("directive status=%d body=%s", rec.Code, rec.Body.String())
	}
	if !slices.Contains(exec.executed, "emit_scan_requested") {
		t.Fatalf("executed tools = %#v, want emit_scan_requested", exec.executed)
	}
	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal directive response: %v", err)
	}
	if ok, _ := payload["ok"].(bool); !ok {
		t.Fatalf("directive response = %#v, want ok=true", payload)
	}
}

func TestHandler_LegacyDashboardRoutesFailClosedWithoutAuthBoundary(t *testing.T) {
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		AuthToken: testOperatorAuthToken,
		Agents: stubAgents{rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{ExecutionMode: "live", ID: "agent-1"},
		}}},
		Runtime: &stubRuntimeControl{},
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
			name:   "builder rpc",
			method: http.MethodPost,
			path:   "/rpc",
			body:   `{"jsonrpc":"2.0","id":"1","method":"engine.ping"}`,
		},
		{
			name:   "builder rpc api alias",
			method: http.MethodPost,
			path:   "/api/rpc",
			body:   `{"jsonrpc":"2.0","id":"1","method":"engine.ping"}`,
		},
		{
			name:   "builder ws",
			method: http.MethodGet,
			path:   "/ws",
		},
		{
			name:   "builder ws api alias",
			method: http.MethodGet,
			path:   "/api/ws",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			setOperatorAuth(req)
			req.Header.Set("Authorization", "Bearer "+testBuilderAuthToken)
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

func TestDashboardConversationListHTTPUsesCanonicalPageAndCursor(t *testing.T) {
	reader := &stubDashboardConversationHTTPReader{listResult: store.OperatorConversationListResult{
		Conversations: []store.OperatorConversationSummary{{SessionID: "sess-1", AgentID: "agent-1", Status: "active"}},
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
	reader := &stubDashboardConversationHTTPReader{turnPages: map[string]store.OperatorConversationTurnListResult{
		"": {
			Conversation: store.OperatorConversationSummary{SessionID: "sess-1", AgentID: "agent-1", Status: "active"},
			Turns: []store.OperatorConversationTurnListItem{{
				TurnID: "turn-2", Ordinal: 2, CompletedAt: completedAt, DurationMS: 42,
				ActivityCounts: store.OperatorConversationActivityCounts{Dispatch: 1, Tool: 1, ToolResult: 1, Publish: 1, Output: 1, Failure: 1},
				ParseOK:        false, Outcome: "failed",
			}},
			NextCursor: "turn-page-2",
		},
		"turn-page-2": {
			Conversation: store.OperatorConversationSummary{SessionID: "sess-1", AgentID: "agent-1", Status: "active"},
			Turns:        []store.OperatorConversationTurnListItem{{TurnID: "turn-1", Ordinal: 1, CompletedAt: completedAt.Add(-time.Minute), DurationMS: 21, ParseOK: true}},
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

func TestDashboardAgentListHTTPExposesOnlySafeLatestTurn(t *testing.T) {
	row := store.OperatorAgentSummary{
		AgentID: "agent-1", Role: "researcher", Type: "managed", Memory: true, MemorySource: "authored", Status: "active",
		FlowInstance: "research/inst-1",
		LiveTurn: &store.OperatorLiveTurn{
			TurnID: "turn-1", TaskID: "task-1", ParseOK: true, AssistantVisibleOutput: "safe output", Outcome: "completed",
			LastTool: &store.OperatorAgentTool{Name: "inspect", ToolUseID: "toolu-1", OK: true},
		},
	}
	handler := NewHandler(Options{Agents: stubDashboardAgentHTTPReader{result: store.OperatorAgentListResult{Agents: []store.OperatorAgentSummary{row}}}}).(*Handler)
	rec := httptest.NewRecorder()
	handler.handleAgents(rec, httptest.NewRequest(http.MethodGet, "/api/agents", nil))
	assertDashboardAgentHTTPSafe(t, rec, "turn-1")
}

func TestDashboardAgentDetailHTTPExposesOnlySafeLatestTurn(t *testing.T) {
	row := store.OperatorAgentSummary{
		AgentID: "agent-1", Role: "researcher", Type: "managed", Memory: true, MemorySource: "authored", Status: "active",
		FlowInstance: "research/inst-1",
		LiveTurn: &store.OperatorLiveTurn{
			TurnID: "turn-1", TaskID: "task-1", ParseOK: true, AssistantVisibleOutput: "safe output", Outcome: "completed",
			LastTool: &store.OperatorAgentTool{Name: "inspect", ToolUseID: "toolu-1", OK: true},
		},
	}
	handler := NewHandler(Options{Agents: stubDashboardAgentHTTPReader{detail: store.OperatorAgentDetail{Agent: row}}}).(*Handler)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/agents/agent-1", nil)
	req.SetPathValue("id", "agent-1")
	handler.handleAgentDetail(rec, req)
	assertDashboardAgentHTTPSafe(t, rec, "turn-1")
}

func assertDashboardAgentHTTPSafe(t *testing.T, rec *httptest.ResponseRecorder, wantTurn string) {
	t.Helper()
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"turn_id":"`+wantTurn+`"`) || !strings.Contains(rec.Body.String(), `"assistant_visible_output":"safe output"`) || !strings.Contains(rec.Body.String(), `"memory":true`) || !strings.Contains(rec.Body.String(), `"memory_source":"authored"`) || !strings.Contains(rec.Body.String(), `"flow_instance":"research/inst-1"`) {
		t.Fatalf("dashboard agent response status=%d body=%s", rec.Code, rec.Body.String())
	}
	for _, forbidden := range []string{"request_payload", "response_payload", "available_tools", "mcp_servers", "mcp_tools_listed", "mcp_tools_visible", `"result":`} {
		if strings.Contains(rec.Body.String(), forbidden) {
			t.Fatalf("dashboard agent response leaked %q: %s", forbidden, rec.Body.String())
		}
	}
}

func TestSQLAgentReader_ListGenericAgents_UsesCanonicalTurnSummary(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	toolOK := true
	reader := NewSQLAgentReader(db, stubSQLAgents{
		rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ExecutionMode: "live",
				ID:            "agent-1",
				Role:          "researcher",
				FlowID:        "global",
				Type:          "managed",
			},
			Status: "active",
		}},
		caps: canonicalEventAndConversationCaps(),
		turns: map[string][]store.OperatorPublicConversationTurn{
			"sess-1": {{
				TurnID: "turn-1", TaskID: "task-1", ParseOK: true, CompletedAt: time.Now(),
				Activity: []store.OperatorConversationActivity{{Kind: "tool_result", ToolName: "schedule", ToolUseID: "toolu_1", OK: &toolOK}},
			}},
		},
	}, 12)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WillReturnRows(sqlmock.NewRows(canonicalAgentProjectionColumns()).
			AddRow("agent-1", "active", "sess-1", time.Now(), 3, "", nil, []byte(`{"provider_session_id":"provider-sess-1"}`), 0, 0))

	items, err := reader.ListGenericAgents(context.Background())
	if err != nil {
		t.Fatalf("ListGenericAgents: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one agent, got %+v", items)
	}
	if items[0].CurrentTaskID != "task-1" {
		t.Fatalf("current_task_id = %q", items[0].CurrentTaskID)
	}
	if items[0].SessionID != "sess-1" || items[0].ProviderSessionID != "provider-sess-1" {
		t.Fatalf("expected canonical session linkage, got %+v", items[0])
	}
	if items[0].LiveTurn == nil || items[0].LiveTurn.TurnID != "turn-1" || items[0].LiveTurn.TaskID != "task-1" {
		t.Fatalf("live_turn = %#v", items[0].LiveTurn)
	}
	if items[0].LastTool == nil || items[0].LastTool.Name != "schedule" {
		t.Fatalf("last_tool = %#v", items[0].LastTool)
	}
	if items[0].LastTool.ToolUseID != "toolu_1" {
		t.Fatalf("last_tool.tool_use_id = %#v", items[0].LastTool)
	}
	if items[0].LastTool.OK != true {
		t.Fatalf("last_tool.ok = %#v", items[0].LastTool.OK)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLAgentReader_ListGenericAgents_ConsumesSafeTurnWithoutSummaryBlock(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLAgentReader(db, stubSQLAgents{
		rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ExecutionMode: "live",
				ID:            "agent-1",
				Role:          "researcher",
				FlowID:        "global",
				Type:          "managed",
			},
			Status: "active",
		}},
		caps: canonicalEventAndConversationCaps(),
		turns: map[string][]store.OperatorPublicConversationTurn{
			"sess-1": {{TurnID: "turn-1", TaskID: "task-1", ParseOK: true, CompletedAt: time.Now(), Activity: []store.OperatorConversationActivity{{Kind: "output", Text: "safe output"}}}},
		},
	}, 12)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WillReturnRows(sqlmock.NewRows(canonicalAgentProjectionColumns()).
			AddRow("agent-1", "active", "sess-1", time.Now(), 3, "", nil, []byte(`{}`), 0, 0))

	items, err := reader.ListGenericAgents(context.Background())
	if err != nil {
		t.Fatalf("ListGenericAgents: %v", err)
	}
	if len(items) != 1 || items[0].LiveTurn == nil || items[0].LiveTurn.TurnID != "turn-1" {
		t.Fatalf("safe latest turn = %#v", items)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLAgentReader_ListGenericAgents_PropagatesSafeTurnProjectionFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLAgentReader(db, stubSQLAgents{
		rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ExecutionMode: "live",
				ID:            "agent-1",
				Role:          "researcher",
				FlowID:        "global",
				Type:          "managed",
			},
			Status: "active",
		}},
		caps:    canonicalEventAndConversationCaps(),
		turnErr: errors.New("decode public conversation turn blocks: malformed"),
	}, 12)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WillReturnRows(sqlmock.NewRows(canonicalAgentProjectionColumns()).
			AddRow("agent-1", "active", "sess-1", time.Now(), 3, "", nil, []byte(`{}`), 0, 0))

	if _, err := reader.ListGenericAgents(context.Background()); err == nil || !strings.Contains(err.Error(), "decode public conversation turn blocks") {
		t.Fatalf("expected safe turn projection failure, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLAgentReader_ListGenericAgents_PropagatesMalformedPublicActivityFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLAgentReader(db, stubSQLAgents{
		rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ExecutionMode: "live",
				ID:            "agent-1",
				Role:          "researcher",
				FlowID:        "global",
				Type:          "managed",
			},
			Status: "active",
		}},
		caps:    canonicalEventAndConversationCaps(),
		turnErr: errors.New("decode public tool result activity: tool_name is required"),
	}, 12)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WillReturnRows(sqlmock.NewRows(canonicalAgentProjectionColumns()).
			AddRow("agent-1", "active", "sess-1", time.Now(), 3, "", nil, []byte(`{}`), 0, 0))

	if _, err := reader.ListGenericAgents(context.Background()); err == nil || !strings.Contains(err.Error(), "decode public tool result activity") {
		t.Fatalf("expected malformed public activity error, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLAgentReader_ListGenericAgents_UsesOperatorProjectionAsCanonicalOwner(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLAgentReader(db, stubSQLAgents{
		rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ExecutionMode: "live",
				ID:            "agent-1",
				Role:          "researcher",
				FlowID:        "global",
				Type:          "managed",
			},
			Status: "terminated",
		}},
		caps: canonicalEventAndConversationCaps(),
		facts: map[string]store.PendingAgentDeliveryFacts{
			"agent-1": {PendingCount: 2, OldestPendingAgeSec: 45},
		},
		lifecycleFacts: map[string]store.AgentDeliveryLifecycleFacts{
			"agent-1": {CurrentState: "launching", BlockingLayer: "session_launch"},
		},
		turns: map[string][]store.OperatorPublicConversationTurn{
			"sess-7": {{TurnID: "turn-7", TaskID: "task-7", ParseOK: false, CompletedAt: time.Now(), Activity: []store.OperatorConversationActivity{}}},
		},
	}, 12)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WillReturnRows(sqlmock.NewRows(canonicalAgentProjectionColumns()).
			AddRow("agent-1", "active", "sess-7", time.Now(), 7, "lease-owner", time.Now().Add(time.Minute), []byte(`{"provider_session_id":"provider-sess-7"}`), 2, 45))

	items, err := reader.ListGenericAgents(context.Background())
	if err != nil {
		t.Fatalf("ListGenericAgents: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one agent, got %+v", items)
	}
	if items[0].Status != "active" {
		t.Fatalf("status = %q, want active from operator projection", items[0].Status)
	}
	if items[0].State != "launching" {
		t.Fatalf("state = %q, want launching from canonical lifecycle owner", items[0].State)
	}
	if items[0].BlockingLayer != "session_launch" {
		t.Fatalf("blocking_layer = %q, want session_launch", items[0].BlockingLayer)
	}
	if items[0].PendingEvents != 2 {
		t.Fatalf("pending_events = %d, want 2", items[0].PendingEvents)
	}
	if items[0].CurrentTaskID != "task-7" {
		t.Fatalf("current_task_id = %q, want task-7", items[0].CurrentTaskID)
	}
	if items[0].SessionID != "sess-7" || items[0].ProviderSessionID != "provider-sess-7" {
		t.Fatalf("session linkage = %+v", items[0])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLAgentReader_GetGenericAgent_UsesCanonicalLifecycleProjection(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLAgentReader(db, stubSQLAgents{
		rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ExecutionMode: "live",
				ID:            "agent-1",
				Role:          "researcher",
				FlowID:        "global",
				Type:          "managed",
			},
			Status: "active",
		}},
		caps: canonicalEventAndConversationCaps(),
		facts: map[string]store.PendingAgentDeliveryFacts{
			"agent-1": {PendingCount: 1, OldestPendingAgeSec: 12},
		},
		lifecycleFacts: map[string]store.AgentDeliveryLifecycleFacts{
			"agent-1": {CurrentState: "active", BlockingLayer: "session_execution"},
		},
	}, 12)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WillReturnRows(sqlmock.NewRows(canonicalAgentProjectionColumns()).
			AddRow("agent-1", "active", "sess-1", time.Now(), 3, "lease-owner", time.Now().Add(time.Minute), []byte(`{"provider_session_id":"provider-sess-1"}`), 0, 0))

	item, ok, err := reader.GetGenericAgent(context.Background(), "agent-1")
	if err != nil {
		t.Fatalf("GetGenericAgent: %v", err)
	}
	if !ok {
		t.Fatalf("expected agent to exist")
	}
	if item.State != "active" {
		t.Fatalf("state = %q, want active", item.State)
	}
	if item.BlockingLayer != "session_execution" {
		t.Fatalf("blocking_layer = %q, want session_execution", item.BlockingLayer)
	}
	if item.SessionID != "sess-1" || item.ProviderSessionID != "provider-sess-1" {
		t.Fatalf("session linkage = %+v", item)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLAgentReader_ListGenericAgents_DoesNotDeriveLifecycleFromActiveLeaseWhenFactsAbsent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLAgentReader(db, stubSQLAgents{
		rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ExecutionMode: "live",
				ID:            "agent-1",
				Role:          "researcher",
				FlowID:        "global",
				Type:          "managed",
			},
			Status: "active",
		}},
		caps: canonicalEventAndConversationCaps(),
		facts: map[string]store.PendingAgentDeliveryFacts{
			"agent-1": {},
		},
		lifecycleFacts: map[string]store.AgentDeliveryLifecycleFacts{
			"agent-1": {},
		},
	}, 12)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WillReturnRows(sqlmock.NewRows(canonicalAgentProjectionColumns()).
			AddRow("agent-1", "active", "sess-1", time.Now(), 2, "lease-owner", time.Now().Add(time.Minute), []byte(`{}`), 0, 0))

	items, err := reader.ListGenericAgents(context.Background())
	if err != nil {
		t.Fatalf("ListGenericAgents: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one agent, got %+v", items)
	}
	if items[0].State != "idle" {
		t.Fatalf("state = %q, want idle from empty canonical lifecycle facts", items[0].State)
	}
	if items[0].BlockingLayer != "" {
		t.Fatalf("blocking_layer = %q, want empty without canonical lifecycle blocker", items[0].BlockingLayer)
	}
	if items[0].LockOwner != "lease-owner" || items[0].LockExpiresAt == "" {
		t.Fatalf("raw lock metadata not preserved as debug data: %+v", items[0])
	}
	assertGenericAgentJSONFieldAbsent(t, items[0], "in_flight_turn")
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLAgentReader_GetGenericAgent_DoesNotDeriveLifecycleFromActiveLeaseWhenFactsAbsent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLAgentReader(db, stubSQLAgents{
		rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ExecutionMode: "live",
				ID:            "agent-1",
				Role:          "researcher",
				FlowID:        "global",
				Type:          "managed",
			},
			Status: "active",
		}},
		caps: canonicalEventAndConversationCaps(),
		facts: map[string]store.PendingAgentDeliveryFacts{
			"agent-1": {},
		},
		lifecycleFacts: map[string]store.AgentDeliveryLifecycleFacts{
			"agent-1": {},
		},
	}, 12)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WillReturnRows(sqlmock.NewRows(canonicalAgentProjectionColumns()).
			AddRow("agent-1", "active", "sess-1", time.Now(), 2, "lease-owner", time.Now().Add(time.Minute), []byte(`{}`), 0, 0))

	item, ok, err := reader.GetGenericAgent(context.Background(), "agent-1")
	if err != nil {
		t.Fatalf("GetGenericAgent: %v", err)
	}
	if !ok {
		t.Fatalf("expected agent to exist")
	}
	if item.State != "idle" {
		t.Fatalf("state = %q, want idle from empty canonical lifecycle facts", item.State)
	}
	if item.BlockingLayer != "" {
		t.Fatalf("blocking_layer = %q, want empty without canonical lifecycle blocker", item.BlockingLayer)
	}
	assertGenericAgentJSONFieldAbsent(t, item, "in_flight_turn")
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func assertGenericAgentJSONFieldAbsent(t *testing.T, item genericAgent, field string) {
	t.Helper()
	raw, err := json.Marshal(item)
	if err != nil {
		t.Fatalf("marshal generic agent: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("unmarshal generic agent: %v", err)
	}
	if _, ok := payload[field]; ok {
		t.Fatalf("generic agent payload exposed %q: %#v", field, payload)
	}
}

func TestSQLAgentReader_ListGenericAgents_FailsClosedWithoutOperatorProjection(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLAgentReader(db, stubSQLAgents{
		rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ExecutionMode: "live",
				ID:            "agent-1",
				Role:          "researcher",
				FlowID:        "global",
				Type:          "managed",
			},
			Status: "active",
		}},
		caps: canonicalEventAndConversationCaps(),
		lifecycleFacts: map[string]store.AgentDeliveryLifecycleFacts{
			"agent-1": {},
		},
	}, 12)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WillReturnRows(sqlmock.NewRows(canonicalAgentProjectionColumns()))

	if _, err := reader.ListGenericAgents(context.Background()); err == nil || !strings.Contains(err.Error(), "missing agent operator projection") {
		t.Fatalf("expected missing agent operator projection error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLAgentReader_ListGenericAgents_FailsClosedWithoutCanonicalReceiptCapability(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	caps := canonicalEventAndConversationCaps()
	caps.Events.Receipts = store.SchemaFlavorLegacy

	reader := NewSQLAgentReader(db, stubSQLAgents{
		rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ExecutionMode: "live",
				ID:            "agent-1",
				Role:          "researcher",
				FlowID:        "global",
				Type:          "managed",
			},
			Status: "active",
		}},
		caps: caps,
		lifecycleFacts: map[string]store.AgentDeliveryLifecycleFacts{
			"agent-1": {},
		},
	}, 12)

	if _, err := reader.ListGenericAgents(context.Background()); err == nil || !strings.Contains(err.Error(), "event_receipts schema is unsupported") {
		t.Fatalf("expected explicit unsupported receipt capability error, got %v", err)
	}
}

func TestSQLAgentReader_ListGenericAgents_FailsClosedWithoutCapabilityOwner(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLAgentReader(db, stubAgents{rows: []runtimemanager.PersistedAgent{{
		Config: runtimeactors.AgentConfig{
			ExecutionMode: "live",
			ID:            "agent-1",
			Role:          "researcher",
			FlowID:        "global",
			Type:          "managed",
		},
		Status: "active",
	}}}, 12)

	if _, err := reader.ListGenericAgents(context.Background()); err == nil || !strings.Contains(err.Error(), "agent reader requires explicit schema capability owner") {
		t.Fatalf("expected missing capability owner error, got %v", err)
	}
}

func TestSQLAgentReader_ListGenericAgents_FailsClosedWithoutCanonicalTurnCapability(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	caps := canonicalEventAndConversationCaps()
	caps.Conversations.Turns = store.SchemaFlavorLegacy

	reader := NewSQLAgentReader(db, stubSQLAgents{
		rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ExecutionMode: "live",
				ID:            "agent-1",
				Role:          "researcher",
				FlowID:        "global",
				Type:          "managed",
			},
			Status: "active",
		}},
		caps: caps,
		lifecycleFacts: map[string]store.AgentDeliveryLifecycleFacts{
			"agent-1": {},
		},
	}, 12)

	if _, err := reader.ListGenericAgents(context.Background()); err == nil || !strings.Contains(err.Error(), "agent_turns schema is unsupported") {
		t.Fatalf("expected explicit unsupported turn capability error, got %v", err)
	}
}

func TestSQLAgentReader_ListGenericAgents_FailsClosedWithoutLifecycleFactOwner(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLAgentReader(db, stubSQLAgentsWithoutLifecycle{
		rows: []runtimemanager.PersistedAgent{{
			Config: runtimeactors.AgentConfig{
				ExecutionMode: "live",
				ID:            "agent-1",
				Role:          "researcher",
				FlowID:        "global",
				Type:          "managed",
			},
			Status: "active",
		}},
		caps: canonicalEventAndConversationCaps(),
		facts: map[string]store.PendingAgentDeliveryFacts{
			"agent-1": {PendingCount: 1, OldestPendingAgeSec: 10},
		},
	}, 12)

	mock.ExpectQuery("(?s)SELECT\\s+a\\.agent_id,.*FROM agents a").
		WillReturnRows(sqlmock.NewRows(canonicalAgentProjectionColumns()).
			AddRow("agent-1", "active", "", nil, 0, "", nil, []byte(`{}`), 0, 0))

	if _, err := reader.ListGenericAgents(context.Background()); err == nil || !strings.Contains(err.Error(), "missing agent lifecycle fact source") {
		t.Fatalf("expected explicit lifecycle fact source error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLAgentReader_ListGenericAgents_AlignsBacklogWithCanonicalPendingSelector(t *testing.T) {
	dsn, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg, err := store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            "agent-1",
			Role:          "researcher",
			FlowID:        "global",
			Type:          "managed",
			Model:         "regular",
			ExecutionMode: "live",
			Config:        json.RawMessage(`{"system_prompt":"You are an operator agent."}`),
		},
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}

	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	pendingEventID := uuid.NewString()
	failedEventID := uuid.NewString()
	inProgressNoReceiptEventID := uuid.NewString()
	deadEventID := uuid.NewString()
	for _, eventID := range []string{pendingEventID, failedEventID, inProgressNoReceiptEventID, deadEventID} {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO events (execution_mode,
				event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at
			) VALUES ('live',
				$1::uuid, $2::uuid, 'task.completed', 'global', '{}'::jsonb, 'runtime', 'agent', now() - interval '5 minutes'
			)
		`, eventID, runID); err != nil {
			t.Fatalf("seed event %s: %v", eventID, err)
		}
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, failure, delivered_at, created_at
		) VALUES
			($1::uuid, $2::uuid, 'agent', 'agent-1', 'pending', 0, NULL, NULL, now() - interval '7 minutes'),
			($1::uuid, $3::uuid, 'agent', 'agent-1', 'failed', 1, $6::jsonb, now() - interval '2 minutes', now() - interval '5 minutes'),
			($1::uuid, $4::uuid, 'agent', 'agent-1', 'in_progress', 0, NULL, NULL, now() - interval '6 minutes'),
			($1::uuid, $5::uuid, 'agent', 'agent-1', 'dead_letter', 2, $7::jsonb, now() - interval '1 minute', now() - interval '8 minutes')
	`, runID, pendingEventID, failedEventID, inProgressNoReceiptEventID, deadEventID,
		mustMarshalFailure(t, testFailure("retryable_failure")), mustMarshalFailure(t, testFailure("terminal_dead_letter"))); err != nil {
		t.Fatalf("seed deliveries: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, outcome, side_effects, failure, processed_at
		) VALUES
			($1::uuid, 'agent', 'agent-1', 'dead_letter', '{"manager_status":"error","retry_count":1}'::jsonb, $3::jsonb, now() - interval '2 minutes'),
			($2::uuid, 'agent', 'agent-1', 'success', '{"manager_status":"dead_letter","retry_count":2}'::jsonb, NULL, now())
	`, failedEventID, deadEventID, mustMarshalFailure(t, testFailure("retryable_failure"))); err != nil {
		t.Fatalf("seed conflicting receipts: %v", err)
	}

	pending, err := pg.ListPendingEventsForAgent(ctx, "agent-1", time.Now().Add(-time.Hour), 20)
	if err != nil {
		t.Fatalf("ListPendingEventsForAgent: %v", err)
	}
	factsByAgent, err := pg.ListPendingAgentDeliveryFacts(ctx, []string{"agent-1"}, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("ListPendingAgentDeliveryFacts: %v", err)
	}
	gotPendingIDs := make([]string, 0, len(pending))
	for _, evt := range pending {
		gotPendingIDs = append(gotPendingIDs, evt.ID())
	}
	slices.Sort(gotPendingIDs)
	wantPendingIDs := []string{failedEventID, inProgressNoReceiptEventID, pendingEventID}
	slices.Sort(wantPendingIDs)
	if !slices.Equal(gotPendingIDs, wantPendingIDs) {
		t.Fatalf("pending event ids = %#v, want %#v", gotPendingIDs, wantPendingIDs)
	}

	reader := NewSQLAgentReader(db, pendingFactsOverrideStore{PostgresStore: pg, facts: factsByAgent}, 12)
	items, err := reader.ListGenericAgents(ctx)
	if err != nil {
		t.Fatalf("ListGenericAgents: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one agent, got %+v", items)
	}
	if items[0].PendingEvents != len(pending) {
		t.Fatalf("pending_events = %d, want %d canonical pending deliveries", items[0].PendingEvents, len(pending))
	}
	wantOldestAge := factsByAgent["agent-1"].OldestPendingAgeSec
	if diff := items[0].OldestPendingAgeSec - wantOldestAge; diff < -1 || diff > 1 {
		t.Fatalf("oldest_pending_age_sec = %d, want %d canonical pending age (+/-1s)", items[0].OldestPendingAgeSec, wantOldestAge)
	}
	if items[0].State != "retrying" {
		t.Fatalf("state = %q, want retrying", items[0].State)
	}
	if items[0].BlockingLayer != "delivery_retry" {
		t.Fatalf("blocking_layer = %q, want delivery_retry", items[0].BlockingLayer)
	}
}

func TestSQLAgentReader_ListGenericAgents_UsesFullPendingDeliveryFactHorizon(t *testing.T) {
	dsn, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg, err := store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            "agent-1",
			Role:          "researcher",
			FlowID:        "global",
			Type:          "managed",
			Model:         "regular",
			ExecutionMode: "live",
			Config:        json.RawMessage(`{"system_prompt":"You are an operator agent."}`),
		},
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}

	runID := uuid.NewString()
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (execution_mode,
			event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		) VALUES ('live',
			$1::uuid, $2::uuid, 'task.completed', 'global', '{}'::jsonb, 'runtime', 'agent', now() - interval '45 days'
		)
	`, eventID, runID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, delivered_at, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'agent', 'agent-1', 'pending', 0, NULL, now() - interval '45 days'
		)
	`, runID, eventID); err != nil {
		t.Fatalf("seed delivery: %v", err)
	}

	factsByAgent, err := pg.ListPendingAgentDeliveryFacts(ctx, []string{"agent-1"}, time.Time{})
	if err != nil {
		t.Fatalf("ListPendingAgentDeliveryFacts: %v", err)
	}
	reader := NewSQLAgentReader(db, pendingFactsOverrideStore{PostgresStore: pg, facts: factsByAgent}, 12)
	items, err := reader.ListGenericAgents(ctx)
	if err != nil {
		t.Fatalf("ListGenericAgents: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one agent, got %+v", items)
	}
	if items[0].PendingEvents != 1 {
		t.Fatalf("pending_events = %d, want 1 full-horizon pending delivery", items[0].PendingEvents)
	}
	wantOldestAge := factsByAgent["agent-1"].OldestPendingAgeSec
	if diff := items[0].OldestPendingAgeSec - wantOldestAge; diff < -1 || diff > 1 {
		t.Fatalf("oldest_pending_age_sec = %d, want %d canonical full-horizon age (+/-1s)", items[0].OldestPendingAgeSec, wantOldestAge)
	}
	if items[0].OldestPendingAgeSec < 30*24*60*60 {
		t.Fatalf("oldest_pending_age_sec = %d, want at least 30 days", items[0].OldestPendingAgeSec)
	}
	if items[0].State != "queued" {
		t.Fatalf("state = %q, want queued", items[0].State)
	}
	if items[0].BlockingLayer != "delivery_queue" {
		t.Fatalf("blocking_layer = %q, want delivery_queue", items[0].BlockingLayer)
	}
}

func TestSQLAgentReader_ListGenericAgents_ScopesLiveTurnToSelectedActiveSession(t *testing.T) {
	dsn, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)

	pg, err := store.NewPostgresStore(dsn)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(func() { _ = pg.DB.Close() })

	ctx := context.Background()
	if err := pg.UpsertAgent(ctx, runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID:            "agent-1",
			Role:          "researcher",
			FlowID:        "entity",
			FlowPath:      "entity",
			Type:          "managed",
			Model:         "regular",
			ExecutionMode: "live",
			Memory:        agentmemory.Authored(true),
		},
		Status:    "active",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}

	sessionOlder := uuid.NewString()
	sessionSelected := uuid.NewString()
	runID := uuid.NewString()
	olderUpdatedAt := time.Date(2026, 4, 15, 3, 0, 0, 0, time.UTC)
	selectedUpdatedAt := olderUpdatedAt.Add(5 * time.Minute)
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', $2)`, runID, olderUpdatedAt); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, flow_instance, memory_enabled, memory_source, conversation, turn_count, runtime_state, status, created_at, updated_at
		) VALUES
			($1::uuid, $3::uuid, 'agent-1', 'entity/older', TRUE, 'authored', '[]'::jsonb, 1, '{"provider_session_id":"provider-older"}'::jsonb, 'active', $4, $4),
			($2::uuid, $3::uuid, 'agent-1', 'entity/selected', TRUE, 'authored', '[]'::jsonb, 7, '{"provider_session_id":"provider-selected"}'::jsonb, 'active', $5, $5)
	`, sessionOlder, sessionSelected, runID, olderUpdatedAt, selectedUpdatedAt); err != nil {
		t.Fatalf("seed agent_sessions: %v", err)
	}

	olderTurnBlocks := `[{"kind":"tool_result","tool_name":"older_tool","output":{"status":"old"},"data":{"tool_use_id":"toolu-old"}},{"kind":"turn_summary","data":{"assistant_visible_output":"older session turn","outcome":"working","tool_results":[{"tool_name":"older_tool","tool_use_id":"toolu-old","output":{"status":"old"}}]}}]`
	selectedTurnBlocks := `[{"kind":"tool_result","tool_name":"selected_tool","output":{"status":"selected"},"data":{"tool_use_id":"toolu-selected"}},{"kind":"turn_summary","data":{"assistant_visible_output":"selected session turn","outcome":"waiting","tool_results":[{"tool_name":"selected_tool","tool_use_id":"toolu-selected","output":{"status":"selected"}}]}}]`
	olderTurnID := uuid.NewString()
	selectedTurnID := uuid.NewString()
	olderCapabilityID := seedDashboardTurnCapabilitySurface(t, ctx, pg, olderTurnID, sessionOlder, "agent-1", "session_per_entity")
	selectedCapabilityID := seedDashboardTurnCapabilitySurface(t, ctx, pg, selectedTurnID, sessionSelected, "agent-1", "session_per_entity")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, flow_instance, memory_enabled, memory_source, task_id, capability_surface_id, turn_blocks, parse_ok, execution_mode, created_at
		) VALUES
			($1::uuid, $11::uuid, 'agent-1', $2::uuid, 'entity/older', TRUE, 'authored', 'task-older', $3::uuid, $4::jsonb, true, 'live', $5),
			($6::uuid, $11::uuid, 'agent-1', $7::uuid, 'entity/selected', TRUE, 'authored', 'task-selected', $8::uuid, $9::jsonb, true, 'live', $10)
	`, olderTurnID, sessionOlder, olderCapabilityID, olderTurnBlocks, selectedUpdatedAt.Add(2*time.Minute), selectedTurnID, sessionSelected, selectedCapabilityID, selectedTurnBlocks, selectedUpdatedAt.Add(-1*time.Minute), runID); err != nil {
		t.Fatalf("seed agent_turns: %v", err)
	}

	reader := NewSQLAgentReader(db, pg, 12)
	items, err := reader.ListGenericAgents(ctx)
	if err != nil {
		t.Fatalf("ListGenericAgents: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one agent, got %+v", items)
	}
	if items[0].SessionID != sessionSelected {
		t.Fatalf("session_id = %q, want %q", items[0].SessionID, sessionSelected)
	}
	if items[0].ProviderSessionID != "provider-selected" {
		t.Fatalf("provider_session_id = %q, want provider-selected", items[0].ProviderSessionID)
	}
	if items[0].CurrentTaskID != "task-selected" {
		t.Fatalf("current_task_id = %q, want task-selected", items[0].CurrentTaskID)
	}
	if items[0].LiveTurn == nil {
		t.Fatalf("expected live_turn, got nil")
	}
	if items[0].LiveTurn.TaskID != "task-selected" || items[0].LiveTurn.AssistantVisibleOutput != "selected session turn" {
		t.Fatalf("live_turn = %+v", items[0].LiveTurn)
	}
	if items[0].LastTool == nil || items[0].LastTool.Name != "selected_tool" || items[0].LastTool.ToolUseID != "toolu-selected" {
		t.Fatalf("last_tool = %#v", items[0].LastTool)
	}
}

func seedDashboardTurnCapabilitySurface(t testing.TB, ctx context.Context, pg *store.PostgresStore, turnID, sessionID, agentID, runtimeMode string) string {
	t.Helper()
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorID: agentID, RuntimeMode: runtimeMode, Provider: "test", Transport: "api", ProviderContract: "dashboard-test",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: turnID,
			ExecutionKind: managedcapabilities.ExecutionNormalAgent, ExecutionAuthorityID: "dashboard-test-runtime",
			SessionID: sessionID, TurnOrdinal: 1,
		},
		CreatedAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatalf("build dashboard capability surface: %v", err)
	}
	if err := pg.SaveManagedCapabilitySurface(ctx, surface); err != nil {
		t.Fatalf("persist dashboard capability surface: %v", err)
	}
	return surface.ID
}

type pendingFactsOverrideStore struct {
	*store.PostgresStore
	facts map[string]store.PendingAgentDeliveryFacts
	err   error
}

func (s pendingFactsOverrideStore) ListPendingAgentDeliveryFacts(context.Context, []string, time.Time) (map[string]store.PendingAgentDeliveryFacts, error) {
	if s.err != nil {
		return nil, s.err
	}
	out := make(map[string]store.PendingAgentDeliveryFacts, len(s.facts))
	for agentID, facts := range s.facts {
		out[agentID] = facts
	}
	return out, nil
}

func TestSQLConversationReader_ListFailsOnMalformedCanonicalSessionWatchdogState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLConversationReader(db, stubConversationCaps{caps: store.StoreSchemaCapabilities{
		Conversations: store.ConversationSchemaCapabilities{
			Sessions:   store.SchemaFlavorCanonical,
			Turns:      store.SchemaFlavorCanonical,
			TurnBlocks: true,
		},
	}})

	mock.ExpectQuery("SELECT\\s+conversations\\.session_id,\\s+conversations\\.agent_id,\\s+conversations\\.run_id,.*FROM \\(").
		WithArgs(11).
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "agent_id", "run_id", "kind", "flow_instance", "memory_enabled", "memory_source", "status", "turn_count", "message_count", "runtime_state", "started_at", "ended_at", "updated_at",
		}).AddRow("sess-1", "agent-1", "", "live_session", "global", true, "authored", "active", 3, 0, []byte(`{"summary":"ok","watchdog":{"state":"mystery","blocking_layer":"session_execution","action":"turn_long_running","outcome":"observed","recorded_at":"2026-04-10T12:00:30Z"}}`), time.Now().UTC(), nil, time.Now().UTC()))

	if _, err := reader.ListOperatorConversations(context.Background(), store.OperatorConversationListOptions{Limit: 10}); err == nil {
		t.Fatal("expected malformed canonical session watchdog state to fail")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestSQLConversationReader_ListProjectsCanonicalSessionWatchdogState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	reader := NewSQLConversationReader(db, stubConversationCaps{caps: store.StoreSchemaCapabilities{
		Conversations: store.ConversationSchemaCapabilities{
			Sessions:   store.SchemaFlavorCanonical,
			Turns:      store.SchemaFlavorCanonical,
			TurnBlocks: true,
		},
	}})

	mock.ExpectQuery("SELECT\\s+conversations\\.session_id,\\s+conversations\\.agent_id,\\s+conversations\\.run_id,.*FROM \\(").
		WithArgs(11).
		WillReturnRows(sqlmock.NewRows([]string{
			"session_id", "agent_id", "run_id", "kind", "flow_instance", "memory_enabled", "memory_source", "status", "turn_count", "message_count", "runtime_state", "started_at", "ended_at", "updated_at",
		}).AddRow("sess-1", "agent-1", "", "live_session", "global", true, "authored", "active", 3, 0, []byte(`{"summary":"ok","watchdog":{"state":"healthy_long_running","blocking_layer":"session_execution","action":"turn_long_running","outcome":"observed","last_output_at":"2026-04-10T12:00:00Z","recorded_at":"2026-04-10T12:00:30Z"}}`), time.Now().UTC(), nil, time.Now().UTC()))

	page, err := reader.ListOperatorConversations(context.Background(), store.OperatorConversationListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListOperatorConversations: %v", err)
	}
	items := page.Conversations
	if len(items) != 1 || items[0].Metadata.Watchdog == nil {
		t.Fatalf("expected one watchdog-bearing row, got %+v", items)
	}
	if items[0].Metadata.Watchdog.State != "healthy_long_running" || items[0].Metadata.Watchdog.Action != "turn_long_running" {
		t.Fatalf("unexpected summary watchdog: %+v", items[0].Metadata.Watchdog)
	}
}

func TestHandler_BuilderRPC(t *testing.T) {
	t.Skip("legacy dashboard/Builder operator endpoint retired under #731; canonical v1 owner tests cover this behavior")
	projectCtl := &stubProjectControl{}
	entityID := runtimeflowidentity.EntityID("wf-1")
	lastAggregate := &store.OperatorEntityAggregateOptions{}
	instances := stubInstances{
		rows: []store.OperatorEntitySummary{
			{EntityID: entityID, FlowInstance: "order", CurrentState: "active"},
		},
		byID: map[string]store.OperatorEntityFull{
			entityID: {
				Entity: store.OperatorEntitySummary{
					EntityID:     entityID,
					FlowInstance: "order",
					CurrentState: "active",
					Slug:         "order-1",
				},
				Fields:      map[string]any{"score": 3.7},
				Gates:       map[string]bool{"review_gate": true},
				Accumulated: map[string]any{"accumulator": map[string]any{"count": 2}},
			},
		},
		lastAggregate: lastAggregate,
	}
	health := func(context.Context) (map[string]any, error) {
		return map[string]any{"runtime": map[string]any{"ready": true}}, nil
	}
	handler := NewHandler(Options{
		Health:    health,
		Entities:  instances,
		AuthToken: testOperatorAuthToken,
		Version:   "swarm-test",
		Builder:   newBuilderHandlerForTest(health, instances, "swarm-test", nil, nil, projectCtl),
	})

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"1","method":"engine.ping"}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("engine.ping status=%d body=%s", rec.Code, rec.Body.String())
	}
	var pingResp builderpkg.RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &pingResp); err != nil {
		t.Fatalf("unmarshal ping response: %v", err)
	}
	result, ok := pingResp.Result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected ping result: %#v", pingResp.Result)
	}
	if result["status"] != "ok" || result["version"] != "swarm-test" {
		t.Fatalf("unexpected ping result: %#v", result)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/instances?workflow_name=order&current_state=active&limit=1", nil)
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("dashboard instances status=%d body=%s", rec.Code, rec.Body.String())
	}
	var dashboardInstances map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &dashboardInstances); err != nil {
		t.Fatalf("unmarshal dashboard instances: %v", err)
	}
	if rows, ok := dashboardInstances["instances"].([]any); !ok || len(rows) != 1 {
		t.Fatalf("unexpected dashboard instances payload: %#v", dashboardInstances)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/instances/wf-1", nil)
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("dashboard entity detail status=%d body=%s", rec.Code, rec.Body.String())
	}
	var dashboardEntity store.OperatorEntityFull
	if err := json.Unmarshal(rec.Body.Bytes(), &dashboardEntity); err != nil {
		t.Fatalf("unmarshal dashboard entity detail: %v", err)
	}
	if dashboardEntity.Entity.EntityID != entityID || dashboardEntity.Fields["score"] != float64(3.7) || !dashboardEntity.Gates["review_gate"] {
		t.Fatalf("unexpected dashboard entity detail: %#v", dashboardEntity)
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/instances/aggregate?group_by=workflow_version", nil)
	setOperatorAuth(req)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("dashboard workflow_version aggregate status=%d body=%s", rec.Code, rec.Body.String())
	}
	if lastAggregate.GroupBy != "workflow_version" {
		t.Fatalf("dashboard workflow_version aggregate group_by = %q, want workflow_version", lastAggregate.GroupBy)
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"2","method":"state.list_instances"}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("state.list_instances status=%d body=%s", rec.Code, rec.Body.String())
	}
	var instancesResp builderpkg.RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &instancesResp); err != nil {
		t.Fatalf("unmarshal instances response: %v", err)
	}
	result, ok = instancesResp.Result.(map[string]any)
	if !ok {
		t.Fatalf("unexpected instances result: %#v", instancesResp.Result)
	}
	instanceRows, ok := result["instances"].([]any)
	if !ok || len(instanceRows) != 1 {
		t.Fatalf("unexpected instances payload: %#v", result)
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"3","method":"state.get_instances"}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("state.get_instances status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"4","method":"state.get_entity","params":{"instance_id":"wf-1"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("state.get_entity status=%d body=%s", rec.Code, rec.Body.String())
	}
	var entityResp builderpkg.RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &entityResp); err != nil {
		t.Fatalf("unmarshal entity response: %v", err)
	}
	rawEntity, err := json.Marshal(entityResp.Result)
	if err != nil {
		t.Fatalf("marshal entity result: %v", err)
	}
	var builderEntity store.OperatorEntityFull
	if err := json.Unmarshal(rawEntity, &builderEntity); err != nil {
		t.Fatalf("unmarshal canonical entity result: %v", err)
	}
	if builderEntity.Entity.CurrentState != "active" || builderEntity.Fields["score"] != float64(3.7) {
		t.Fatalf("unexpected canonical entity payload: %#v", builderEntity)
	}
	if !builderEntity.Gates["review_gate"] {
		t.Fatalf("unexpected gates payload: %#v", builderEntity.Gates)
	}
	accBucket, ok := builderEntity.Accumulated["accumulator"].(map[string]any)
	if !ok || accBucket["count"] != float64(2) {
		t.Fatalf("unexpected accumulated payload: %#v", builderEntity.Accumulated)
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"5","method":"project.open","params":{"project_dir":"/tmp/builder-project"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("project.open status=%d body=%s", rec.Code, rec.Body.String())
	}
	var projectResp builderpkg.RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &projectResp); err != nil {
		t.Fatalf("unmarshal project.open response: %v", err)
	}
	result, ok = projectResp.Result.(map[string]any)
	if !ok || result["project_dir"] != "/tmp/builder-project" || result["loaded"] != true {
		t.Fatalf("unexpected project.open payload: %#v", projectResp.Result)
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"6","method":"engine.ping"}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/api/rpc engine.ping status=%d body=%s", rec.Code, rec.Body.String())
	}
	var apiPingResp builderpkg.RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &apiPingResp); err != nil {
		t.Fatalf("unmarshal /api/rpc response: %v", err)
	}
	result, ok = apiPingResp.Result.(map[string]any)
	if !ok || result["status"] != "ok" || result["version"] != "swarm-test" {
		t.Fatalf("unexpected /api/rpc result: %#v", apiPingResp.Result)
	}
}

func TestHandler_BuilderWSHealthHeartbeat(t *testing.T) {
	t.Skip("legacy dashboard/Builder operator endpoint retired under #731; canonical v1 owner tests cover this behavior")
	restore := builderpkg.SetHealthHeartbeatIntervalForTest(20 * time.Millisecond)
	defer restore()
	health := func(context.Context) (map[string]any, error) {
		return map[string]any{"runtime": map[string]any{"ready": true}}, nil
	}
	ts := httptest.NewServer(NewHandler(Options{
		Health:  health,
		Version: "swarm-test",
		Builder: newBuilderHandlerForTest(health, nil, "swarm-test", nil, nil, nil),
	}))
	defer ts.Close()

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "channel": "engine:health"}); err != nil {
		t.Fatalf("subscribe write: %v", err)
	}

	var frame builderpkg.WSEventFrame
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatalf("read first event: %v", err)
	}
	if frame.Channel != "engine:health" {
		t.Fatalf("unexpected channel: %#v", frame.Channel)
	}
	data, ok := frame.Data.(map[string]any)
	if !ok {
		t.Fatalf("unexpected event payload: %#v", frame.Data)
	}
	if data["status"] != "ok" || data["version"] != "swarm-test" {
		t.Fatalf("unexpected health payload: %#v", data)
	}
}

func TestHandler_BuilderWSHealthHeartbeat_APIAlias(t *testing.T) {
	t.Skip("legacy dashboard/Builder operator endpoint retired under #731; canonical v1 owner tests cover this behavior")
	restore := builderpkg.SetHealthHeartbeatIntervalForTest(20 * time.Millisecond)
	defer restore()
	health := func(context.Context) (map[string]any, error) {
		return map[string]any{"runtime": map[string]any{"ready": true}}, nil
	}
	ts := httptest.NewServer(NewHandler(Options{
		Health:  health,
		Version: "swarm-test",
		Builder: newBuilderHandlerForTest(health, nil, "swarm-test", nil, nil, nil),
	}))
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial builder ws alias: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{
		"type":    "subscribe",
		"channel": "engine:health",
	}); err != nil {
		t.Fatalf("subscribe health alias: %v", err)
	}

	var frame builderpkg.WSEventFrame
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatalf("read health alias frame: %v", err)
	}
	if frame.Channel != "engine:health" {
		t.Fatalf("unexpected alias channel: %#v", frame.Channel)
	}
	payload, ok := frame.Data.(map[string]any)
	if !ok {
		t.Fatalf("unexpected alias payload: %#v", frame.Data)
	}
	if payload["version"] != "swarm-test" {
		t.Fatalf("unexpected alias payload: %#v", payload)
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

func TestHandler_RunStartStreamsRunEvents(t *testing.T) {
	t.Skip("legacy dashboard/Builder operator endpoint retired under #731; canonical v1 owner tests cover this behavior")
	bus, err := runtimebus.NewEventBus(&stubBuilderRunStore{})
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	runID := "run_test_001"
	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "channel": "run:events:" + runID}); err != nil {
		t.Fatalf("subscribe run events: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_001","inputs":{"intake.requested":{"topic":"sample"}}}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start status=%d body=%s", rec.Code, rec.Body.String())
	}

	receivedTypes := map[string]struct{}{}
	done := make(chan map[string]struct{}, 1)
	go func() {
		defer close(done)
		for {
			var frame builderWSEventFrame
			if err := conn.ReadJSON(&frame); err != nil {
				done <- receivedTypes
				return
			}
			if frame.Channel != "run:events:"+runID {
				continue
			}
			payload, ok := frame.Data.(map[string]any)
			if !ok {
				continue
			}
			eventType, _ := payload["type"].(string)
			if eventType != "" {
				receivedTypes[eventType] = struct{}{}
			}
			if _, ok := receivedTypes["run.started"]; ok {
				if _, ok := receivedTypes["event.fired"]; ok {
					if _, ok := receivedTypes["run.completed"]; ok {
						done <- receivedTypes
						return
					}
				}
			}
		}
	}()

	select {
	case got := <-done:
		if _, ok := got["run.started"]; !ok {
			t.Fatalf("expected run.started, got %#v", got)
		}
		if _, ok := got["event.fired"]; !ok {
			t.Fatalf("expected event.fired, got %#v", got)
		}
		if _, ok := got["run.completed"]; !ok {
			t.Fatalf("expected run.completed, got %#v", got)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for run events")
	}
}

func TestHandler_RunEventReplayUsesCanonicalPersistedRunDebugOwner(t *testing.T) {
	t.Skip("legacy dashboard/Builder operator endpoint retired under #731; canonical v1 owner tests cover this behavior")
	now := time.Unix(1700000000, 0).UTC()
	runID := "run_replay_001"
	rootEvent := eventtest.PersistedProjection(
		"evt-root",
		events.EventType("scan.requested"),
		"builder",
		"",
		json.RawMessage(`{"topic":"sample"}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, runID),
		now,
	)

	storeStub := &stubBuilderRunStore{
		events: []events.Event{
			rootEvent,
			eventtest.PersistedProjection("evt-log", events.EventType("platform.runtime_log"), "runtime", "", json.RawMessage(`{"log_level":"warn","message":"runtime log","details":{"component":"scheduler","action":"checkpoint","error":"boom"}}`), 0, runID, "", events.EventEnvelope{}, now.Add(2*time.Second)),
		},
		snapshots: map[string]runtimebus.RunLifecycleSnapshot{
			runID: {
				RunID:       runID,
				Status:      "completed",
				EventCount:  2,
				EntityCount: 1,
				StartedAt:   now,
				EndedAt:     ptrTime(now.Add(3 * time.Second)),
			},
		},
	}
	bus, err := runtimebus.NewEventBus(storeStub)
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: &stubRuntimeControl{},
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			nil,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "channel": "run:events:" + runID}); err != nil {
		t.Fatalf("subscribe run events: %v", err)
	}

	gotTypes := map[string]map[string]any{}
	deadline := time.After(1 * time.Second)
	for len(gotTypes) < 4 {
		_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for canonical replay, got %#v", gotTypes)
		default:
		}
		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, _ := frame.Data.(map[string]any)
		eventType, _ := payload["type"].(string)
		if eventType != "" {
			gotTypes[eventType] = payload
		}
	}

	if gotTypes["run.started"]["timestamp"] != now.Format(time.RFC3339) {
		t.Fatalf("run.started timestamp = %#v, want %q", gotTypes["run.started"]["timestamp"], now.Format(time.RFC3339))
	}
	if gotTypes["event.fired"]["timestamp"] != now.Format(time.RFC3339) {
		t.Fatalf("event.fired timestamp = %#v, want %q", gotTypes["event.fired"]["timestamp"], now.Format(time.RFC3339))
	}
	eventPayload, _ := gotTypes["event.fired"]["payload"].(map[string]any)
	rawEventPayload, _ := eventPayload["payload"].(map[string]any)
	if rawEventPayload["topic"] != "sample" {
		t.Fatalf("event.fired payload = %#v", eventPayload)
	}
	if gotTypes["runtime.log"]["timestamp"] != now.Add(2*time.Second).Format(time.RFC3339) {
		t.Fatalf("runtime.log timestamp = %#v, want %q", gotTypes["runtime.log"]["timestamp"], now.Add(2*time.Second).Format(time.RFC3339))
	}
	runtimePayload, _ := gotTypes["runtime.log"]["payload"].(map[string]any)
	if runtimePayload["component"] != "scheduler" || runtimePayload["action"] != "checkpoint" {
		t.Fatalf("runtime.log payload = %#v", runtimePayload)
	}
	if runtimePayload["error"] != "boom" {
		t.Fatalf("runtime.log payload.error = %#v, want boom", runtimePayload["error"])
	}
	donePayload, _ := gotTypes["run.completed"]["payload"].(map[string]any)
	summary, _ := donePayload["summary"].(map[string]any)
	if summary["entity_count"] != float64(1) && summary["entity_count"] != 1 {
		t.Fatalf("run.completed summary = %#v", summary)
	}
}

func TestHandler_RunTraceUsesCanonicalPersistedRunDebugOwner(t *testing.T) {
	t.Skip("legacy dashboard/Builder operator endpoint retired under #731; canonical v1 owner tests cover this behavior")
	now := time.Unix(1700000200, 0).UTC()
	runID := "run_trace_001"
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		AuthToken: testOperatorAuthToken,
		RunTrace: stubRunTrace{rows: map[string][]store.RunDebugTraceRow{
			runID: {{
				EventID:              "evt-1",
				EventName:            "scan.requested",
				EventCreatedAt:       now,
				DeliveryID:           "del-1",
				SubscriberType:       "agent",
				SubscriberID:         "agent-source",
				DeliveryStatus:       "in_progress",
				ActiveSessionID:      "sess-1",
				SessionID:            "sess-1",
				SessionKind:          "live_session",
				SessionMemory:        true,
				SessionMemorySource:  "authored",
				SessionStatus:        "active",
				TurnID:               "turn-1",
				TurnTriggerEventID:   "evt-1",
				TurnTriggerEventType: "scan.requested",
				TurnFlowInstance:     "research/inst-1",
				TurnMemory:           true,
				TurnMemorySource:     "authored",
				TurnTaskID:           "task-1",
				TurnCreatedAt:        ptrTime(now.Add(2 * time.Second)),
			}},
		}},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/runs/"+runID+"/trace", nil)
	setOperatorAuth(req)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/runs/{runID}/trace status=%d body=%s", rec.Code, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got, _ := body["run_id"].(string); got != runID {
		t.Fatalf("run_id = %q, want %q", got, runID)
	}
	rows, _ := body["trace"].([]any)
	if len(rows) != 1 {
		t.Fatalf("trace len = %d, want 1", len(rows))
	}
	row, _ := rows[0].(map[string]any)
	if row["event_id"] != "evt-1" || row["delivery_id"] != "del-1" || row["session_id"] != "sess-1" || row["turn_id"] != "turn-1" {
		t.Fatalf("trace row = %#v", row)
	}
	if row["turn_trigger_event_id"] != "evt-1" {
		t.Fatalf("turn_trigger_event_id = %#v", row["turn_trigger_event_id"])
	}
}

func TestHandler_RunEventStreamPreservesCanonicalRuntimeLogWithoutEntityID(t *testing.T) {
	t.Skip("legacy dashboard/Builder operator endpoint retired under #731; canonical v1 owner tests cover this behavior")
	now := time.Unix(1700000100, 0).UTC()
	runID := "run_live_001"
	storeStub := &stubBuilderRunStore{}
	bus, err := runtimebus.NewEventBus(storeStub)
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "channel": "run:events:" + runID}); err != nil {
		t.Fatalf("subscribe run events: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_live_001","inputs":{"intake.requested":{"topic":"sample"}}}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start status=%d body=%s", rec.Code, rec.Body.String())
	}

	deadline := time.After(1 * time.Second)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		select {
		case <-deadline:
			t.Fatal("timed out draining initial run events")
		default:
		}
		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read initial run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, _ := frame.Data.(map[string]any)
		if payload["type"] == "run.completed" {
			break
		}
	}

	logPayload := json.RawMessage(`{"log_level":"warn","message":"runtime log","details":{"component":"scheduler","action":"canonical-owner","error":"boom"}}`)
	if err := storeStub.AppendEvent(context.Background(), eventtest.PersistedProjection("evt-runtime-log",

		events.EventType("platform.runtime_log"),
		"runtime", "", logPayload, 0, runID, "", events.EventEnvelope{}, now)); err != nil {
		t.Fatalf("append canonical runtime log: %v", err)
	}

	typedHandler, ok := handler.(*Handler)
	if !ok || !builderpkg.HandleRuntimeLogForTest(typedHandler.builder, runtimepkg.RuntimeLogEntry{
		Level:     "warn",
		Component: "scheduler",
		Action:    "canonical-owner",
	}) {
		t.Fatalf("expected typed handler with builder runtime-log hook")
	}

	deadline = time.After(1 * time.Second)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		select {
		case <-deadline:
			t.Fatal("timed out waiting for canonical runtime.log event")
		default:
		}
		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, _ := frame.Data.(map[string]any)
		if payload["type"] != "runtime.log" {
			continue
		}
		if payload["timestamp"] != now.Format(time.RFC3339) {
			t.Fatalf("runtime.log timestamp = %#v, want %q", payload["timestamp"], now.Format(time.RFC3339))
		}
		return
	}
}

func TestHandler_RunStopUsesRunControlOwnerAndStreamsStopped(t *testing.T) {
	t.Skip("legacy dashboard/Builder operator endpoint retired under #731; canonical v1 owner tests cover this behavior")
	bus, err := runtimebus.NewEventBus(&stubBuilderRunStore{})
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	runID := "run_test_stop_001"
	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "channel": "run:events:" + runID}); err != nil {
		t.Fatalf("subscribe run events: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_stop_001","inputs":{"intake.requested":{"topic":"sample"}}}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"11","method":"run.stop","params":{"run_id":"run_test_stop_001"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.stop status=%d body=%s", rec.Code, rec.Body.String())
	}

	deadline := time.After(1 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for run.stopped")
		default:
		}

		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, ok := frame.Data.(map[string]any)
		if !ok {
			continue
		}
		eventType, _ := payload["type"].(string)
		if eventType != "run.stopped" {
			continue
		}
		if runtimeCtl.pauseCalls != 0 || runtimeCtl.resumeCalls != 0 {
			t.Fatalf("expected run.stop not to change runtime ingress, got pause:%d resume:%d", runtimeCtl.pauseCalls, runtimeCtl.resumeCalls)
		}
		return
	}
}

func TestHandler_RunPauseAndContinueStreamStateChanges(t *testing.T) {
	t.Skip("legacy dashboard/Builder operator endpoint retired under #731; canonical v1 owner tests cover this behavior")
	bus, err := runtimebus.NewEventBus(&stubBuilderRunStore{})
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	runID := "run_test_pause_001"
	if err := conn.WriteJSON(map[string]any{"type": "subscribe", "channel": "run:events:" + runID}); err != nil {
		t.Fatalf("subscribe run events: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_pause_001","inputs":{"intake.requested":{"topic":"sample"}}}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"11","method":"run.pause","params":{"run_id":"run_test_pause_001"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.pause status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/rpc", `{"jsonrpc":"2.0","id":"12","method":"run.continue","params":{"run_id":"run_test_pause_001"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.continue status=%d body=%s", rec.Code, rec.Body.String())
	}

	received := map[string]struct{}{}
	deadline := time.After(1 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for pause/resume events: %#v", received)
		default:
		}

		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, ok := frame.Data.(map[string]any)
		if !ok {
			continue
		}
		eventType, _ := payload["type"].(string)
		if eventType == "" {
			continue
		}
		received[eventType] = struct{}{}
		if _, ok := received["run.paused"]; ok {
			if _, ok := received["run.resumed"]; ok {
				break
			}
		}
	}

	if runtimeCtl.pauseCalls != 0 {
		t.Fatalf("expected run.pause not to pause runtime ingress, got %d calls", runtimeCtl.pauseCalls)
	}
	if runtimeCtl.resumeCalls != 0 {
		t.Fatalf("expected run.continue not to resume runtime ingress, got %d calls", runtimeCtl.resumeCalls)
	}
}

func TestHandler_RunLifecycleOverAPIAliases(t *testing.T) {
	t.Skip("legacy dashboard/Builder operator endpoint retired under #731; canonical v1 owner tests cover this behavior")
	bus, err := runtimebus.NewEventBus(&stubBuilderRunStore{})
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket alias: %v", err)
	}
	defer conn.Close()

	runID := "run_test_api_alias_001"
	if err := conn.WriteJSON(map[string]any{
		"type":    "subscribe",
		"channel": "run:events:" + runID,
	}); err != nil {
		t.Fatalf("subscribe run events alias: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_api_alias_001","inputs":{"intake.requested":{"topic":"sample"}}}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"11","method":"run.pause","params":{"run_id":"run_test_api_alias_001"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.pause alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"12","method":"run.continue","params":{"run_id":"run_test_api_alias_001"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.continue alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	received := map[string]struct{}{}
	deadline := time.After(1 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for alias run events: %#v", received)
		default:
		}

		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read alias run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, ok := frame.Data.(map[string]any)
		if !ok {
			continue
		}
		eventType, _ := payload["type"].(string)
		if eventType == "" {
			continue
		}
		received[eventType] = struct{}{}
		if _, ok := received["run.started"]; ok {
			if _, ok := received["event.fired"]; ok {
				if _, ok := received["run.paused"]; ok {
					if _, ok := received["run.resumed"]; ok {
						if _, ok := received["run.completed"]; ok {
							break
						}
					}
				}
			}
		}
	}

	if runtimeCtl.pauseCalls != 0 {
		t.Fatalf("expected alias run.pause not to pause runtime ingress, got %d calls", runtimeCtl.pauseCalls)
	}
	if runtimeCtl.resumeCalls != 0 {
		t.Fatalf("expected alias run.continue not to resume runtime ingress, got %d calls", runtimeCtl.resumeCalls)
	}
}

func TestHandler_RunBreakpointHitPausesRuntime(t *testing.T) {
	t.Skip("legacy dashboard/Builder operator endpoint retired under #731; canonical v1 owner tests cover this behavior")
	bus, err := runtimebus.NewEventBus(&stubBuilderRunStore{})
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket alias: %v", err)
	}
	defer conn.Close()

	runID := "run_test_breakpoint_001"
	if err := conn.WriteJSON(map[string]any{
		"type":    "subscribe",
		"channel": "run:events:" + runID,
	}); err != nil {
		t.Fatalf("subscribe run events alias: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_breakpoint_001","inputs":{"intake.requested":{"topic":"sample"}},"breakpoints":["agent-source"]}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	typedHandler, ok := handler.(*Handler)
	if !ok || !builderpkg.HandleRuntimeLogForTest(typedHandler.builder, runtimepkg.RuntimeLogEntry{
		Level:     "info",
		Component: "pipeline",
		Action:    "handled",
		AgentID:   "agent-source",
		EntityID:  runID,
		EventID:   "evt-breakpoint",
	}) {
		t.Fatalf("expected typed handler with builder runtime-log hook")
	}

	deadline := time.After(1 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for breakpoint event")
		default:
		}

		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, ok := frame.Data.(map[string]any)
		if !ok {
			continue
		}
		if payload["type"] != "run.breakpoint_hit" {
			continue
		}
		if payload["node_id"] != "agent-source" {
			t.Fatalf("unexpected node_id: %#v", payload)
		}
		if payload["instance_id"] != runID {
			t.Fatalf("unexpected instance_id: %#v", payload)
		}
		break
	}

	if runtimeCtl.pauseCalls != 1 {
		t.Fatalf("expected runtime pause once, got %d", runtimeCtl.pauseCalls)
	}
}

func TestHandler_HumanTaskWaitingAndDecisionResume(t *testing.T) {
	t.Skip("legacy dashboard/Builder operator endpoint retired under #731; canonical v1 owner tests cover this behavior")
	bus, err := runtimebus.NewEventBus(&stubBuilderRunStore{})
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket alias: %v", err)
	}
	defer conn.Close()

	runID := "run_test_human_001"
	if err := conn.WriteJSON(map[string]any{
		"type":    "subscribe",
		"channel": "run:events:" + runID,
	}); err != nil {
		t.Fatalf("subscribe run events alias: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_human_001","inputs":{"intake.requested":{"topic":"sample"}}}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	typedHandler, ok := handler.(*Handler)
	if !ok || !builderpkg.HandleRuntimeLogForTest(typedHandler.builder, runtimepkg.RuntimeLogEntry{
		Level:     "info",
		Component: "eventbus",
		Action:    "published",
		AgentID:   "agent-source",
		EntityID:  runID,
		EventType: "human_task.requested",
		EventID:   "evt-human",
		Detail: map[string]any{
			"type":   "human_task.requested",
			"source": "agent-source",
		},
	}) {
		t.Fatalf("expected typed handler with builder runtime-log hook")
	}

	receivedWaiting := false
	deadline := time.After(1 * time.Second)
	for !receivedWaiting {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for human.task_waiting")
		default:
		}

		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, ok := frame.Data.(map[string]any)
		if !ok {
			continue
		}
		switch payload["type"] {
		case "human.task_waiting":
			receivedWaiting = true
		}
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"11","method":"run.continue","params":{"run_id":"run_test_human_001","decision":"approved","instance_ids":["run_test_human_001"]}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.continue alias status=%d body=%s", rec.Code, rec.Body.String())
	}
	var rpcResp builderRPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &rpcResp); err != nil {
		t.Fatalf("decode run.continue rejection: %v", err)
	}
	if rpcResp.Error == nil || rpcResp.Error.Code != -32602 {
		t.Fatalf("expected invalid-params rejection for legacy human-decision run.continue, got %#v body=%s", rpcResp.Error, rec.Body.String())
	}

	if runtimeCtl.pauseCalls != 1 {
		t.Fatalf("expected runtime pause once, got %d", runtimeCtl.pauseCalls)
	}
	if runtimeCtl.resumeCalls != 0 {
		t.Fatalf("expected legacy human-decision run.continue not to resume runtime ingress, got %d", runtimeCtl.resumeCalls)
	}
}

func TestHandler_RunStepPausesAfterNextRuntimeEvent(t *testing.T) {
	t.Skip("legacy dashboard/Builder operator endpoint retired under #731; canonical v1 owner tests cover this behavior")
	bus, err := runtimebus.NewEventBus(&stubBuilderRunStore{})
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket alias: %v", err)
	}
	defer conn.Close()

	runID := "run_test_step_001"
	if err := conn.WriteJSON(map[string]any{
		"type":    "subscribe",
		"channel": "run:events:" + runID,
	}); err != nil {
		t.Fatalf("subscribe run events alias: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_step_001","inputs":{"intake.requested":{"topic":"sample"}}}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"11","method":"run.step","params":{"run_id":"run_test_step_001","node_id":"agent-source","instance_id":"run_test_step_001"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.step alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	typedHandler, ok := handler.(*Handler)
	if !ok || !builderpkg.HandleRuntimeLogForTest(typedHandler.builder, runtimepkg.RuntimeLogEntry{
		Level:     "info",
		Component: "pipeline",
		Action:    "handled",
		AgentID:   "agent-source",
		EntityID:  runID,
		EventID:   "evt-step",
	}) {
		t.Fatalf("expected typed handler with builder runtime-log hook")
	}

	receivedResumed := false
	receivedPaused := false
	deadline := time.After(1 * time.Second)
	for !(receivedResumed && receivedPaused) {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for step events")
		default:
		}

		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, ok := frame.Data.(map[string]any)
		if !ok {
			continue
		}
		switch payload["type"] {
		case "run.resumed":
			if payload["node_id"] == "agent-source" {
				receivedResumed = true
			}
		case "run.paused":
			stepPayload, _ := payload["payload"].(map[string]any)
			if stepPayload["reason"] == "step_complete" {
				receivedPaused = true
			}
		}
	}

	if runtimeCtl.resumeCalls != 1 {
		t.Fatalf("expected runtime resume once, got %d", runtimeCtl.resumeCalls)
	}
	if runtimeCtl.pauseCalls != 1 {
		t.Fatalf("expected runtime pause once from step completion, got %d", runtimeCtl.pauseCalls)
	}
}

func TestHandler_RunRetryEmitsRetriedAndResumed(t *testing.T) {
	t.Skip("legacy dashboard/Builder operator endpoint retired under #731; canonical v1 owner tests cover this behavior")
	bus, err := runtimebus.NewEventBus(&stubBuilderRunStore{})
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket alias: %v", err)
	}
	defer conn.Close()

	runID := "run_test_retry_001"
	if err := conn.WriteJSON(map[string]any{
		"type":    "subscribe",
		"channel": "run:events:" + runID,
	}); err != nil {
		t.Fatalf("subscribe run events alias: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_retry_001","inputs":{"intake.requested":{"topic":"sample"}}}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"11","method":"run.retry","params":{"run_id":"run_test_retry_001","node_id":"agent-source","instance_id":"run_test_retry_001"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.retry alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	receivedRetried := false
	receivedResumed := false
	deadline := time.After(1 * time.Second)
	for !(receivedRetried && receivedResumed) {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for retry events")
		default:
		}

		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, ok := frame.Data.(map[string]any)
		if !ok {
			continue
		}
		switch payload["type"] {
		case "handler.retried":
			receivedRetried = true
		case "run.resumed":
			modePayload, _ := payload["payload"].(map[string]any)
			if modePayload["mode"] == "retry" {
				receivedResumed = true
			}
		}
	}

	if runtimeCtl.resumeCalls != 1 {
		t.Fatalf("expected runtime resume once, got %d", runtimeCtl.resumeCalls)
	}
}

func TestHandler_RunSkipEmitsSkippedAndResumed(t *testing.T) {
	t.Skip("legacy dashboard/Builder operator endpoint retired under #731; canonical v1 owner tests cover this behavior")
	bus, err := runtimebus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("new event bus: %v", err)
	}
	rt := &runtimepkg.Runtime{Bus: bus}
	runtimeCtl := &stubRuntimeControl{}
	handler := NewHandler(Options{
		Health: func(context.Context) (map[string]any, error) {
			return map[string]any{"runtime": map[string]any{"ready": true}}, nil
		},
		Version: "swarm-test",
		Runtime: runtimeCtl,
		Builder: newBuilderHandlerForTest(
			func(context.Context) (map[string]any, error) {
				return map[string]any{"runtime": map[string]any{"ready": true}}, nil
			},
			nil,
			"swarm-test",
			runtimeCtl,
			rt,
			nil,
		),
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/api/ws"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, builderAuthHeader())
	if err != nil {
		t.Fatalf("dial websocket alias: %v", err)
	}
	defer conn.Close()

	runID := "run_test_skip_001"
	if err := conn.WriteJSON(map[string]any{
		"type":    "subscribe",
		"channel": "run:events:" + runID,
	}); err != nil {
		t.Fatalf("subscribe run events alias: %v", err)
	}

	rec := httptest.NewRecorder()
	req := builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"10","method":"run.start","params":{"run_id":"run_test_skip_001","inputs":{"intake.requested":{"topic":"sample"}}}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.start alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	req = builderAuthRequest(http.MethodPost, "/api/rpc", `{"jsonrpc":"2.0","id":"11","method":"run.skip","params":{"run_id":"run_test_skip_001","node_id":"agent-source","instance_id":"run_test_skip_001"}}`)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("run.skip alias status=%d body=%s", rec.Code, rec.Body.String())
	}

	receivedSkipped := false
	receivedResumed := false
	deadline := time.After(1 * time.Second)
	for !(receivedSkipped && receivedResumed) {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for skip events")
		default:
		}

		var frame builderWSEventFrame
		if err := conn.ReadJSON(&frame); err != nil {
			t.Fatalf("read run event: %v", err)
		}
		if frame.Channel != "run:events:"+runID {
			continue
		}
		payload, ok := frame.Data.(map[string]any)
		if !ok {
			continue
		}
		switch payload["type"] {
		case "handler.skipped":
			receivedSkipped = true
		case "run.resumed":
			modePayload, _ := payload["payload"].(map[string]any)
			if modePayload["mode"] == "skip" {
				receivedResumed = true
			}
		}
	}

	if runtimeCtl.resumeCalls != 1 {
		t.Fatalf("expected runtime resume once, got %d", runtimeCtl.resumeCalls)
	}
}
