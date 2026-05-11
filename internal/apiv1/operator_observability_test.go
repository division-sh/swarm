package apiv1

import (
	"context"
	"testing"
	"time"

	"swarm/internal/store"
)

func TestOperatorObservabilityHandlersExposePersistedReadMethods(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	observability := &fakeObservabilityReadStore{
		traceRows: map[string][]store.RunDebugTraceRow{
			"run-1": {{
				EventID:        "evt-1",
				EventName:      "scan.requested",
				EventCreatedAt: now,
			}},
		},
		events: map[string]store.OperatorEventFull{
			"evt-1": {
				EventID:   "evt-1",
				EventName: "scan.requested",
				RunID:     "run-1",
				CreatedAt: now,
				Source:    "runtime",
				Payload:   map[string]any{"ok": true},
				Deliveries: []store.OperatorEventDelivery{{
					DeliveryID:     "del-1",
					SubscriberType: "agent",
					SubscriberID:   "agent-1",
					Status:         "delivered",
				}},
				DeadLetters: []store.OperatorDeadLetterRecord{},
			},
		},
		logs: []store.OperatorRuntimeLogEntry{{
			LogID:     "log-1",
			TS:        now,
			Level:     "warn",
			Component: "scheduler",
			Source:    "runtime",
			RunID:     "run-1",
			ErrorCode: "retry_exhausted",
			Message:   "retry exhausted",
			Details:   map[string]any{"action": "dispatch"},
		}},
		incidents: []store.OperatorRuntimeIncident{{
			IncidentID:    "inc-1",
			FirstSeen:     now,
			LastSeen:      now,
			Count:         1,
			Level:         "warn",
			Component:     "scheduler",
			ErrorCode:     "retry_exhausted",
			SampleMessage: "retry exhausted",
			SampleLogIDs:  []string{"log-1"},
		}},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Observability: observability,
		}),
	})

	trace := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"trace","method":"run.trace","params":{"run_id":"run-1","limit":1}}`)
	if trace.Error != nil {
		t.Fatalf("run.trace error = %#v", trace.Error)
	}
	if rows, _ := asMap(t, trace.Result)["trace"].([]any); len(rows) != 1 {
		t.Fatalf("run.trace rows = %#v", asMap(t, trace.Result)["trace"])
	}

	list := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"events","method":"event.list","params":{"filter":{"run_id":"run-1","event_name":"scan.requested","has_dead_letter":false},"limit":10}}`)
	if list.Error != nil {
		t.Fatalf("event.list error = %#v", list.Error)
	}
	if events, _ := asMap(t, list.Result)["events"].([]any); len(events) != 1 {
		t.Fatalf("event.list events = %#v", asMap(t, list.Result)["events"])
	}
	if observability.lastEventList.Filter.RunID != "run-1" || observability.lastEventList.Filter.EventName != "scan.requested" || observability.lastEventList.Filter.HasDeadLetter == nil || *observability.lastEventList.Filter.HasDeadLetter {
		t.Fatalf("event.list filter = %#v", observability.lastEventList.Filter)
	}

	eventGet := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"event","method":"event.get","params":{"event_id":"evt-1"}}`)
	if eventGet.Error != nil {
		t.Fatalf("event.get error = %#v", eventGet.Error)
	}
	if got := asMap(t, eventGet.Result)["event_id"]; got != "evt-1" {
		t.Fatalf("event.get event_id = %#v", got)
	}

	logs := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"logs","method":"runtime.logs","params":{"run_id":"run-1","component":"scheduler","level":"warn","limit":5}}`)
	if logs.Error != nil {
		t.Fatalf("runtime.logs error = %#v", logs.Error)
	}
	if rows, _ := asMap(t, logs.Result)["logs"].([]any); len(rows) != 1 {
		t.Fatalf("runtime.logs rows = %#v", asMap(t, logs.Result)["logs"])
	}
	if observability.lastRuntimeLogs.RunID != "run-1" || observability.lastRuntimeLogs.Component != "scheduler" {
		t.Fatalf("runtime.logs options = %#v", observability.lastRuntimeLogs)
	}

	incidents := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"incidents","method":"runtime.incidents","params":{"since_hours":24,"component":"scheduler","limit":5}}`)
	if incidents.Error != nil {
		t.Fatalf("runtime.incidents error = %#v", incidents.Error)
	}
	if rows, _ := asMap(t, incidents.Result)["incidents"].([]any); len(rows) != 1 {
		t.Fatalf("runtime.incidents rows = %#v", asMap(t, incidents.Result)["incidents"])
	}
	if observability.lastIncidents.Component != "scheduler" || observability.lastIncidents.SinceHours != 24 {
		t.Fatalf("runtime.incidents options = %#v", observability.lastIncidents)
	}
}

func TestOperatorObservabilityHandlersTypedErrors(t *testing.T) {
	observability := &fakeObservabilityReadStore{
		traceErr: store.ErrRunNotFound,
		eventErr: store.ErrEventNotFound,
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Observability: observability,
		}),
	})

	trace := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"trace","method":"run.trace","params":{"run_id":"missing"}}`)
	if trace.Error == nil {
		t.Fatal("run.trace error = nil, want RUN_NOT_FOUND")
	}
	if data := asMap(t, trace.Error.Data); data["code"] != RunNotFoundCode {
		t.Fatalf("run.trace error data = %#v", data)
	}

	eventGet := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"event","method":"event.get","params":{"event_id":"missing"}}`)
	if eventGet.Error == nil {
		t.Fatal("event.get error = nil, want EVENT_NOT_FOUND")
	}
	if data := asMap(t, eventGet.Error.Data); data["code"] != EventNotFoundCode {
		t.Fatalf("event.get error data = %#v", data)
	}

	observability.listErr = store.ErrInvalidObservabilityCursor
	list := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"events","method":"event.list","params":{"cursor":"bad"}}`)
	if list.Error == nil || list.Error.Code != codeInvalidParams {
		t.Fatalf("event.list error = %#v, want invalid params", list.Error)
	}
}

type fakeObservabilityReadStore struct {
	traceRows map[string][]store.RunDebugTraceRow
	traceErr  error

	events   map[string]store.OperatorEventFull
	eventErr error
	listErr  error

	logs      []store.OperatorRuntimeLogEntry
	incidents []store.OperatorRuntimeIncident

	lastEventList   store.OperatorEventListOptions
	lastRuntimeLogs store.OperatorRuntimeLogListOptions
	lastIncidents   store.OperatorRuntimeIncidentListOptions
}

func (s *fakeObservabilityReadStore) LoadRunDebugTracePage(_ context.Context, runID string, _ store.RunDebugTraceQueryOptions) ([]store.RunDebugTraceRow, string, error) {
	if s.traceErr != nil {
		return nil, "", s.traceErr
	}
	return s.traceRows[runID], "", nil
}

func (s *fakeObservabilityReadStore) ListOperatorEvents(_ context.Context, opts store.OperatorEventListOptions) (store.OperatorEventListResult, error) {
	s.lastEventList = opts
	if s.listErr != nil {
		return store.OperatorEventListResult{}, s.listErr
	}
	out := make([]store.OperatorEventFull, 0, len(s.events))
	for _, event := range s.events {
		out = append(out, event)
	}
	return store.OperatorEventListResult{Events: out}, nil
}

func (s *fakeObservabilityReadStore) LoadOperatorEvent(_ context.Context, eventID string) (store.OperatorEventFull, error) {
	if s.eventErr != nil {
		return store.OperatorEventFull{}, s.eventErr
	}
	event, ok := s.events[eventID]
	if !ok {
		return store.OperatorEventFull{}, store.ErrEventNotFound
	}
	return event, nil
}

func (s *fakeObservabilityReadStore) ListOperatorRuntimeLogs(_ context.Context, opts store.OperatorRuntimeLogListOptions) (store.OperatorRuntimeLogListResult, error) {
	s.lastRuntimeLogs = opts
	return store.OperatorRuntimeLogListResult{Logs: s.logs}, nil
}

func (s *fakeObservabilityReadStore) ListOperatorRuntimeIncidents(_ context.Context, opts store.OperatorRuntimeIncidentListOptions) (store.OperatorRuntimeIncidentListResult, error) {
	s.lastIncidents = opts
	return store.OperatorRuntimeIncidentListResult{Incidents: s.incidents}, nil
}

var _ ObservabilityReadStore = (*fakeObservabilityReadStore)(nil)
