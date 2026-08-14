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

	operatorread "github.com/division-sh/swarm/internal/operatorread"

	builderpkg "github.com/division-sh/swarm/internal/builder"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
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

type stubRunTrace struct {
	rows map[string][]operatorread.RunDebugTraceRow
	err  error
}

func (s stubRunTrace) LoadRunDebugTrace(_ context.Context, runID string, _ operatorread.RunDebugTraceQueryOptions) ([]operatorread.RunDebugTraceRow, error) {
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

func (s *stubBuilderRunStore) CommitPublication(ctx context.Context, command runtimebus.PublicationCommand) (runtimebus.CommittedPublication, error) {
	return runtimebustest.CommitPublish(ctx, command, nil, func(_ context.Context, req runtimebus.CommitPublishRequest) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.events = append(s.events, req.Event.Event())
		return nil
	})
}
func (*stubBuilderRunStore) ListEventDeliveryRecipients(context.Context, string) ([]string, error) {
	return []string{}, nil
}
func (*stubBuilderRunStore) SupportsPersistedReplay() bool { return false }
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

func (s *stubBuilderRunStore) LoadRunDebugReport(_ context.Context, runID string, _ operatorread.RunDebugQueryOptions) (operatorread.RunDebugReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	report := operatorread.RunDebugReport{
		RunID:             strings.TrimSpace(runID),
		EventCounts:       []operatorread.RunDebugEventCount{},
		Deliveries:        []operatorread.RunDebugDeliveryCount{},
		Events:            []operatorread.RunDebugEvent{},
		DeadLetters:       []operatorread.RunDebugDeadLetter{},
		AgentTurns:        []operatorread.RunDebugAgentTurn{},
		Mutations:         []operatorread.RunDebugMutation{},
		RuntimeLogs:       []operatorread.RunDebugRuntimeLog{},
		RuntimeLogSummary: []operatorread.RunDebugRuntimeSummary{},
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
			report.RuntimeLogs = append(report.RuntimeLogs, operatorread.RunDebugRuntimeLog{
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
		report.Events = append(report.Events, operatorread.RunDebugEvent{
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
		report.EventCounts = append(report.EventCounts, operatorread.RunDebugEventCount{EventName: eventName, Count: count})
	}
	slices.SortFunc(report.Events, func(a, b operatorread.RunDebugEvent) int { return b.CreatedAt.Compare(a.CreatedAt) })
	slices.SortFunc(report.RuntimeLogs, func(a, b operatorread.RunDebugRuntimeLog) int { return b.CreatedAt.Compare(a.CreatedAt) })
	slices.SortFunc(report.EventCounts, func(a, b operatorread.RunDebugEventCount) int { return strings.Compare(a.EventName, b.EventName) })
	if report.RootEventID == "" && len(report.Events) > 0 {
		root := report.Events[len(report.Events)-1]
		report.RootEventID = root.EventID
		report.RootEventType = root.EventName
	}
	return report, nil
}

func (s *stubBuilderRunStore) LoadRunDebugTracePage(_ context.Context, runID string, opts operatorread.RunDebugTraceQueryOptions) ([]operatorread.RunDebugTraceRow, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	runID = strings.TrimSpace(runID)
	rows := []operatorread.RunDebugTraceRow{}
	for _, evt := range s.events {
		if strings.TrimSpace(evt.RunID()) != runID {
			continue
		}
		if opts.Since != nil && !evt.CreatedAt().After(opts.Since.UTC()) {
			continue
		}
		rows = append(rows, operatorread.RunDebugTraceRow{
			EventID:         strings.TrimSpace(evt.ID()),
			EventName:       strings.TrimSpace(string(evt.Type())),
			SourceEventID:   strings.TrimSpace(evt.ParentEventID()),
			EntityID:        strings.TrimSpace(evt.EntityID()),
			EventSource:     strings.TrimSpace(evt.SourceAgent()),
			EventSourceType: "agent",
			EventCreatedAt:  evt.CreatedAt().UTC(),
		})
	}
	slices.SortFunc(rows, func(a, b operatorread.RunDebugTraceRow) int {
		if cmp := a.EventCreatedAt.Compare(b.EventCreatedAt); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.EventID, b.EventID)
	})
	limit := opts.Limit
	if limit <= 0 || limit > len(rows) {
		limit = len(rows)
	}
	return append([]operatorread.RunDebugTraceRow(nil), rows[:limit]...), "", nil
}

func (s *stubBuilderRunStore) ListOperatorEvents(_ context.Context, opts operatorread.OperatorEventListOptions) (operatorread.OperatorEventListResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	eventsOut := []operatorread.OperatorEventFull{}
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
		eventsOut = append(eventsOut, operatorread.OperatorEventFull{
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
	slices.SortFunc(eventsOut, func(a, b operatorread.OperatorEventFull) int {
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
	return operatorread.OperatorEventListResult{Events: append([]operatorread.OperatorEventFull(nil), eventsOut[:limit]...)}, nil
}

func (s *stubBuilderRunStore) ListOperatorRuntimeLogs(_ context.Context, opts operatorread.OperatorRuntimeLogListOptions) (operatorread.OperatorRuntimeLogListResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	logs := []operatorread.OperatorRuntimeLogEntry{}
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
		logs = append(logs, operatorread.OperatorRuntimeLogEntry{
			LogID:           strings.TrimSpace(evt.ID()),
			TS:              evt.CreatedAt().UTC(),
			Level:           strings.TrimSpace(asString(payload["log_level"])),
			Component:       strings.TrimSpace(asString(details["component"])),
			Source:          strings.TrimSpace(firstNonEmpty(asString(details["agent_id"]), evt.SourceAgent())),
			RunID:           strings.TrimSpace(evt.RunID()),
			EntityID:        strings.TrimSpace(firstNonEmpty(evt.EntityID(), asString(details["entity_id"]))),
			ErrorCode:       strings.TrimSpace(asString(details["error_code"])),
			Message:         strings.TrimSpace(asString(payload["message"])),
			Action:          strings.TrimSpace(asString(details["action"])),
			EventType:       strings.TrimSpace(firstNonEmpty(asString(details["event_name"]), asString(details["event_type"]))),
			AgentID:         strings.TrimSpace(asString(details["agent_id"])),
			CanonicalDetail: cloneAnyMap(details),
		})
	}
	slices.SortFunc(logs, func(a, b operatorread.OperatorRuntimeLogEntry) int {
		if cmp := a.TS.Compare(b.TS); cmp != 0 {
			return cmp
		}
		return strings.Compare(a.LogID, b.LogID)
	})
	limit := opts.Limit
	if limit <= 0 || limit > len(logs) {
		limit = len(logs)
	}
	return operatorread.OperatorRuntimeLogListResult{Logs: append([]operatorread.OperatorRuntimeLogEntry(nil), logs[:limit]...)}, nil
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
	t *testing.T,
	health HealthChecker,
	entities EntityReader,
	version string,
	runtimeCtl RuntimeController,
	rt *runtimepkg.Runtime,
	projectCtl builderpkg.ProjectController,
) http.Handler {
	processOwner := worklifetime.NewProcess()
	var runtimeAcquirer builderpkg.RuntimeAcquirer
	var runDebug builderpkg.RunDebugReader
	if rt != nil {
		runtimeAcquirer = newDashboardBuilderRuntimeAcquirer(t, processOwner, rt)
		if typed, ok := rt.Bus.Store().(*stubBuilderRunStore); ok {
			runDebug = typed
			if rt.RunControl == nil {
				rt.RunControl = runtimeruncontrol.NewController(typed, nil, runtimeruncontrol.Options{})
			}
		}
	}
	return builderpkg.NewHandler(builderpkg.Options{
		Health:           builderpkg.HealthChecker(health),
		Entities:         entities,
		Runtime:          runtimeCtl,
		AuthToken:        testBuilderAuthToken,
		Version:          version,
		RuntimeAcquirer:  runtimeAcquirer,
		ProcessWorkOwner: processOwner,
		ProjectControl:   projectCtl,
		RunDebug:         runDebug,
	})
}

type dashboardBuilderRuntimeAcquirer struct {
	runtime *runtimepkg.Runtime
	owner   *worklifetime.RuntimeOccurrence
	process *worklifetime.Process
}

type dashboardBuilderRuntimeUse struct {
	runtime *runtimepkg.Runtime
	lease   *worklifetime.Lease
	ctx     context.Context
}

func newDashboardBuilderRuntimeAcquirer(t *testing.T, process *worklifetime.Process, rt *runtimepkg.Runtime) builderpkg.RuntimeAcquirer {
	t.Helper()
	owner, err := process.NewRuntime(context.Background(), worklifetime.RuntimeIdentity{
		RuntimeInstanceID: uuid.NewString(),
		BundleHash:        "dashboard-builder-test-bundle",
	})
	if err != nil {
		t.Fatalf("new dashboard builder runtime occurrence: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := owner.RetireAndWait(ctx); err != nil {
			t.Errorf("retire dashboard builder runtime occurrence: %v", err)
			return
		}
		if _, err := process.Join(ctx); err != nil {
			t.Errorf("join dashboard builder process: %v", err)
		}
	})
	return &dashboardBuilderRuntimeAcquirer{runtime: rt, owner: owner, process: process}
}

func (a *dashboardBuilderRuntimeAcquirer) AcquireCurrentRuntime(ctx context.Context) (builderpkg.RuntimeUse, error) {
	return a.acquire(ctx)
}

func (a *dashboardBuilderRuntimeAcquirer) AcquireRunRuntime(ctx context.Context, _ string) (builderpkg.RuntimeUse, error) {
	return a.acquire(ctx)
}

func (a *dashboardBuilderRuntimeAcquirer) acquire(ctx context.Context) (builderpkg.RuntimeUse, error) {
	ctx = worklifetime.WithProcess(ctx, a.process)
	ctx = worklifetime.WithOccurrence(ctx, a.owner)
	lease, err := a.owner.Begin(ctx)
	if err != nil {
		return nil, err
	}
	workCtx := worklifetime.WithProcess(lease.Context(), a.process)
	workCtx = worklifetime.WithOccurrence(workCtx, a.owner)
	return &dashboardBuilderRuntimeUse{runtime: a.runtime, lease: lease, ctx: workCtx}, nil
}

func (u *dashboardBuilderRuntimeUse) Runtime() *runtimepkg.Runtime { return u.runtime }
func (u *dashboardBuilderRuntimeUse) WorkContext() context.Context { return u.ctx }
func (u *dashboardBuilderRuntimeUse) Done() error                  { return u.lease.Done() }

func builderAuthRequest(method, path, body string) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testBuilderAuthToken)
	return req
}

func builderAuthHeader() http.Header {
	return http.Header{"Authorization": []string{"Bearer " + testBuilderAuthToken}}
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

func TestHandler_BuilderRPC(t *testing.T) {
	t.Skip("legacy dashboard/Builder operator endpoint retired under #731; canonical v1 owner tests cover this behavior")
	projectCtl := &stubProjectControl{}
	entityID := runtimeflowidentity.EntityID("wf-1")
	lastAggregate := &operatorread.OperatorEntityAggregateOptions{}
	instances := stubInstances{
		rows: []operatorread.OperatorEntitySummary{
			{EntityID: entityID, FlowInstance: "order", CurrentState: "active"},
		},
		byID: map[string]operatorread.OperatorEntityFull{
			entityID: {
				Entity: operatorread.OperatorEntitySummary{
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
		Builder:   newBuilderHandlerForTest(t, health, instances, "swarm-test", nil, nil, projectCtl),
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
	var dashboardEntity operatorread.OperatorEntityFull
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
	var builderEntity operatorread.OperatorEntityFull
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
		Builder: newBuilderHandlerForTest(t, health, nil, "swarm-test", nil, nil, nil),
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
		Builder: newBuilderHandlerForTest(t, health, nil, "swarm-test", nil, nil, nil),
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
	bus, err := runtimebus.NewEphemeralEventBus(&stubBuilderRunStore{})
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
		Builder: newBuilderHandlerForTest(t,
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
	bus, err := runtimebus.NewEphemeralEventBus(storeStub)
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
		Builder: newBuilderHandlerForTest(t,
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
		RunTrace: stubRunTrace{rows: map[string][]operatorread.RunDebugTraceRow{
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
	bus, err := runtimebus.NewEphemeralEventBus(storeStub)
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
		Builder: newBuilderHandlerForTest(t,
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
	if err := bus.Publish(context.Background(), eventtest.RuntimeDiagnostic(uuid.NewString(),
		events.EventType("platform.runtime_log"),
		"runtime", "", logPayload, 0, runID, "", events.EventEnvelope{}, now)); err != nil {
		t.Fatalf("publish runtime-log fixture: %v", err)
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
	bus, err := runtimebus.NewEphemeralEventBus(&stubBuilderRunStore{})
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
		Builder: newBuilderHandlerForTest(t,
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
	bus, err := runtimebus.NewEphemeralEventBus(&stubBuilderRunStore{})
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
		Builder: newBuilderHandlerForTest(t,
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
	bus, err := runtimebus.NewEphemeralEventBus(&stubBuilderRunStore{})
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
		Builder: newBuilderHandlerForTest(t,
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
	bus, err := runtimebus.NewEphemeralEventBus(&stubBuilderRunStore{})
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
		Builder: newBuilderHandlerForTest(t,
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
	bus, err := runtimebus.NewEphemeralEventBus(&stubBuilderRunStore{})
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
		Builder: newBuilderHandlerForTest(t,
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
	bus, err := runtimebus.NewEphemeralEventBus(&stubBuilderRunStore{})
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
		Builder: newBuilderHandlerForTest(t,
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
	bus, err := runtimebus.NewEphemeralEventBus(&stubBuilderRunStore{})
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
		Builder: newBuilderHandlerForTest(t,
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
	bus, err := runtimebus.NewEphemeralEventBus(nil)
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
		Builder: newBuilderHandlerForTest(t,
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
