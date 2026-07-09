package apiv1

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/store"
)

func TestOperatorObservabilityHandlersExposePersistedReadMethods(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	observability := &fakeObservabilityReadStore{
		traceRows: map[string][]store.RunDebugTraceRow{
			"run-1": {{
				EventID:        "evt-1",
				EventName:      "scan.requested",
				EntityID:       "entity-1",
				EventCreatedAt: now,
				SubscriberType: "agent",
				SubscriberID:   "agent-1",
				DeliveryStatus: "delivered",
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
			SessionID: "sess-1",
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

	trace := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"trace","method":"run.trace","params":{"run_id":"run-1","limit":1,"since":"2023-11-14T22:12:00Z","until":"2023-11-14T22:14:00Z","filter":{"event_name":["scan.requested"],"entity_id":["entity-1"],"delivery_status":["delivered"],"subscriber_id":["agent-1"],"subscriber_type":["agent"]}}}`)
	if trace.Error != nil {
		t.Fatalf("run.trace error = %#v", trace.Error)
	}
	if rows, _ := asMap(t, trace.Result)["trace"].([]any); len(rows) != 1 {
		t.Fatalf("run.trace rows = %#v", asMap(t, trace.Result)["trace"])
	}
	if observability.lastTrace.Since == nil || observability.lastTrace.Until == nil || observability.lastTrace.Limit != 1 {
		t.Fatalf("run.trace options = %#v", observability.lastTrace)
	}
	if !observability.lastTrace.ExcludeRuntimeLogs {
		t.Fatalf("run.trace ExcludeRuntimeLogs = false, want default true")
	}
	if got := observability.lastTrace.Filter; len(got.EventNames) != 1 || got.EventNames[0] != "scan.requested" || len(got.EntityIDs) != 1 || got.EntityIDs[0] != "entity-1" || len(got.DeliveryStatuses) != 1 || got.DeliveryStatuses[0] != "delivered" || len(got.SubscriberIDs) != 1 || got.SubscriberIDs[0] != "agent-1" || len(got.SubscriberTypes) != 1 || got.SubscriberTypes[0] != "agent" {
		t.Fatalf("run.trace filter = %#v", got)
	}
	traceVerbose := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"trace-verbose","method":"run.trace","params":{"run_id":"run-1","include_internal":true}}`)
	if traceVerbose.Error != nil {
		t.Fatalf("run.trace include_internal error = %#v", traceVerbose.Error)
	}
	if observability.lastTrace.ExcludeRuntimeLogs {
		t.Fatalf("run.trace include_internal ExcludeRuntimeLogs = true, want false")
	}

	invalidTraceSince := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"trace-since","method":"run.trace","params":{"run_id":"run-1","since":"not-a-time"}}`)
	if invalidTraceSince.Error == nil || invalidTraceSince.Error.Code != codeInvalidParams {
		t.Fatalf("run.trace invalid since error = %#v, want invalid params", invalidTraceSince.Error)
	}
	invalidTraceUntil := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"trace-until","method":"run.trace","params":{"run_id":"run-1","until":"not-a-time"}}`)
	if invalidTraceUntil.Error == nil || invalidTraceUntil.Error.Code != codeInvalidParams {
		t.Fatalf("run.trace invalid until error = %#v, want invalid params", invalidTraceUntil.Error)
	}
	invalidTraceWindow := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"trace-window","method":"run.trace","params":{"run_id":"run-1","since":"2023-11-14T22:14:00Z","until":"2023-11-14T22:12:00Z"}}`)
	if invalidTraceWindow.Error == nil || invalidTraceWindow.Error.Code != codeInvalidParams {
		t.Fatalf("run.trace invalid window error = %#v, want invalid params", invalidTraceWindow.Error)
	}
	if details := asMap(t, asMap(t, invalidTraceWindow.Error.Data)["details"]); details["field"] != "until" {
		t.Fatalf("run.trace invalid window details = %#v, want until field", details)
	}
	invalidTraceFilter := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"trace-filter","method":"run.trace","params":{"run_id":"run-1","filter":{"delivery_status":["done"]}}}`)
	if invalidTraceFilter.Error == nil || invalidTraceFilter.Error.Code != codeInvalidParams {
		t.Fatalf("run.trace invalid filter error = %#v, want invalid params", invalidTraceFilter.Error)
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

	bundleHash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	logs := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"logs","method":"runtime.logs","params":{"bundle_hash":"`+bundleHash+`","run_id":"run-1","session_id":"sess-1","component":"scheduler","level":"warn","since":"2023-11-14T22:12:00Z","until":"2023-11-14T22:14:00Z","limit":5}}`)
	if logs.Error != nil {
		t.Fatalf("runtime.logs error = %#v", logs.Error)
	}
	if rows, _ := asMap(t, logs.Result)["logs"].([]any); len(rows) != 1 {
		t.Fatalf("runtime.logs rows = %#v", asMap(t, logs.Result)["logs"])
	}
	if observability.lastRuntimeLogs.BundleHash != bundleHash || observability.lastRuntimeLogs.RunID != "run-1" || observability.lastRuntimeLogs.SessionID != "sess-1" || observability.lastRuntimeLogs.Component != "scheduler" {
		t.Fatalf("runtime.logs options = %#v", observability.lastRuntimeLogs)
	}
	if observability.lastRuntimeLogs.Since == nil || observability.lastRuntimeLogs.Until == nil {
		t.Fatalf("runtime.logs time window missing: %#v", observability.lastRuntimeLogs)
	}
	invalidWindow := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"logs-window","method":"runtime.logs","params":{"since":"2023-11-14T22:14:00Z","until":"2023-11-14T22:12:00Z"}}`)
	if invalidWindow.Error == nil || invalidWindow.Error.Code != codeInvalidParams {
		t.Fatalf("runtime.logs invalid window error = %#v, want invalid params", invalidWindow.Error)
	}

	invalidBundleHash := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"logs-invalid-bundle","method":"runtime.logs","params":{"bundle_hash":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`)
	if invalidBundleHash.Error == nil || invalidBundleHash.Error.Code != codeInvalidParams {
		t.Fatalf("runtime.logs invalid bundle_hash error = %#v, want invalid params", invalidBundleHash.Error)
	}

	incidents := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"incidents","method":"runtime.incidents","params":{"bundle_hash":"`+bundleHash+`","since_hours":24,"component":"scheduler","limit":5}}`)
	if incidents.Error != nil {
		t.Fatalf("runtime.incidents error = %#v", incidents.Error)
	}
	if rows, _ := asMap(t, incidents.Result)["incidents"].([]any); len(rows) != 1 {
		t.Fatalf("runtime.incidents rows = %#v", asMap(t, incidents.Result)["incidents"])
	}
	if observability.lastIncidents.BundleHash != bundleHash || observability.lastIncidents.Component != "scheduler" || observability.lastIncidents.SinceHours != 24 {
		t.Fatalf("runtime.incidents options = %#v", observability.lastIncidents)
	}

	missingEventRun := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"events-missing-run","method":"event.list","params":{"filter":{"event_name":"scan.requested"},"limit":10}}`)
	if missingEventRun.Error == nil || missingEventRun.Error.Code != codeInvalidParams {
		t.Fatalf("event.list missing run scope error = %#v, want invalid params", missingEventRun.Error)
	}
	if details := asMap(t, asMap(t, missingEventRun.Error.Data)["details"]); details["field"] != "filter.run_id" {
		t.Fatalf("event.list missing run scope details = %#v, want filter.run_id", details)
	}
}

func TestOperatorObservabilityHandlersKeepPayloadEntityOutOfTopLevelEventIdentity(t *testing.T) {
	now := time.Unix(1700001200, 0).UTC()
	payloadEntityID := "11111111-1111-4111-8111-111111111111"
	observability := &fakeObservabilityReadStore{
		events: map[string]store.OperatorEventFull{
			"evt-payload-only": {
				EventID:     "evt-payload-only",
				EventName:   "task.payload_only",
				RunID:       "run-1",
				CreatedAt:   now,
				Source:      "runtime",
				Payload:     map[string]any{"entity_id": payloadEntityID, "marker": "payload-only"},
				Deliveries:  []store.OperatorEventDelivery{},
				DeadLetters: []store.OperatorDeadLetterRecord{},
			},
		},
	}
	handler := testHandler(t, Options{
		AuthTokens: []string{testToken},
		Handlers: OperatorReadHandlers(OperatorReadOptions{
			Observability: observability,
		}),
	})

	list := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"events","method":"event.list","params":{"filter":{"run_id":"run-1"},"limit":10}}`)
	if list.Error != nil {
		t.Fatalf("event.list error = %#v", list.Error)
	}
	events := asSlice(t, asMap(t, list.Result)["events"])
	if len(events) != 1 {
		t.Fatalf("event.list events = %#v, want one payload-only event", events)
	}
	listEvent := asMap(t, events[0])
	if _, ok := listEvent["entity_id"]; ok {
		t.Fatalf("event.list top-level entity_id = %#v, want absent", listEvent["entity_id"])
	}
	if payload := asMap(t, listEvent["payload"]); payload["entity_id"] != payloadEntityID {
		t.Fatalf("event.list payload = %#v, want preserved entity_id", payload)
	}

	filtered := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"events-filtered","method":"event.list","params":{"filter":{"run_id":"run-1","entity_id":"`+payloadEntityID+`"},"limit":10}}`)
	if filtered.Error != nil {
		t.Fatalf("event.list filtered error = %#v", filtered.Error)
	}
	if got := asSlice(t, asMap(t, filtered.Result)["events"]); len(got) != 0 {
		t.Fatalf("event.list filtered events = %#v, want payload-only event excluded", got)
	}

	eventGet := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"event","method":"event.get","params":{"event_id":"evt-payload-only"}}`)
	if eventGet.Error != nil {
		t.Fatalf("event.get error = %#v", eventGet.Error)
	}
	detail := asMap(t, eventGet.Result)
	if _, ok := detail["entity_id"]; ok {
		t.Fatalf("event.get top-level entity_id = %#v, want absent", detail["entity_id"])
	}
	if payload := asMap(t, detail["payload"]); payload["entity_id"] != payloadEntityID {
		t.Fatalf("event.get payload = %#v, want preserved entity_id", payload)
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
	list := rpcCall(t, handler, `{"jsonrpc":"2.0","id":"events","method":"event.list","params":{"filter":{"run_id":"run-1"},"cursor":"bad"}}`)
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

	logs           []store.OperatorRuntimeLogEntry
	runtimeLogsErr error
	incidents      []store.OperatorRuntimeIncident

	lastEventList   store.OperatorEventListOptions
	lastTrace       store.RunDebugTraceQueryOptions
	lastRuntimeLogs store.OperatorRuntimeLogListOptions
	lastIncidents   store.OperatorRuntimeIncidentListOptions
}

func (s *fakeObservabilityReadStore) LoadRunDebugTracePage(_ context.Context, runID string, opts store.RunDebugTraceQueryOptions) ([]store.RunDebugTraceRow, string, error) {
	s.lastTrace = opts
	if s.traceErr != nil {
		return nil, "", s.traceErr
	}
	rows := []store.RunDebugTraceRow{}
	for _, row := range s.traceRows[runID] {
		if opts.ExcludeRuntimeLogs && row.EventName == "platform.runtime_log" {
			continue
		}
		if opts.Since != nil && !row.EventCreatedAt.After(opts.Since.UTC()) {
			continue
		}
		if opts.Until != nil && row.EventCreatedAt.After(opts.Until.UTC()) {
			continue
		}
		rows = append(rows, row)
	}
	return rows, "", nil
}

func (s *fakeObservabilityReadStore) ListOperatorEvents(_ context.Context, opts store.OperatorEventListOptions) (store.OperatorEventListResult, error) {
	s.lastEventList = opts
	if s.listErr != nil {
		return store.OperatorEventListResult{}, s.listErr
	}
	out := make([]store.OperatorEventFull, 0, len(s.events))
	for _, event := range s.events {
		if opts.Since != nil && !event.CreatedAt.After(opts.Since.UTC()) {
			continue
		}
		if opts.Filter.RunID != "" && event.RunID != opts.Filter.RunID {
			continue
		}
		if opts.Filter.EntityID != "" && event.EntityID != opts.Filter.EntityID {
			continue
		}
		if opts.Filter.EventName != "" && event.EventName != opts.Filter.EventName {
			continue
		}
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
	if s.runtimeLogsErr != nil {
		return store.OperatorRuntimeLogListResult{}, s.runtimeLogsErr
	}
	out := make([]store.OperatorRuntimeLogEntry, 0, len(s.logs))
	for _, log := range s.logs {
		if opts.Since != nil && !log.TS.After(opts.Since.UTC()) {
			continue
		}
		if opts.RunID != "" && log.RunID != opts.RunID {
			continue
		}
		if opts.EntityID != "" && log.EntityID != opts.EntityID {
			continue
		}
		if opts.SessionID != "" && log.SessionID != opts.SessionID {
			continue
		}
		if opts.Component != "" && log.Component != opts.Component {
			continue
		}
		if opts.Level != "" && log.Level != opts.Level {
			continue
		}
		if opts.ErrorCode != "" && log.ErrorCode != opts.ErrorCode {
			continue
		}
		if opts.Source != "" && log.Source != opts.Source {
			continue
		}
		if opts.Until != nil && log.TS.After(opts.Until.UTC()) {
			continue
		}
		out = append(out, log)
	}
	return store.OperatorRuntimeLogListResult{Logs: out}, nil
}

func (s *fakeObservabilityReadStore) ListOperatorRuntimeIncidents(_ context.Context, opts store.OperatorRuntimeIncidentListOptions) (store.OperatorRuntimeIncidentListResult, error) {
	s.lastIncidents = opts
	return store.OperatorRuntimeIncidentListResult{Incidents: s.incidents}, nil
}

var _ ObservabilityReadStore = (*fakeObservabilityReadStore)(nil)
