package cataloge2e

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	forkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/google/uuid"
)

type activityLineageProofStore struct {
	forkexecution.SelectedContractForkLifecycle
	t           *testing.T
	h           *runtimeHarness
	calls       *atomic.Int32
	hostile     bool
	rejectFinal bool
}

func (p *activityLineageProofStore) ActivateRunForkForSelectedContractExecution(ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	t := p.t
	observed := activityLineageEvents(t, ctx, p.h, req.ForkRunID)
	var request, result, diagnostic operatorread.OperatorEventFull
	for _, event := range observed {
		switch {
		case event.EventName == "platform.activity_requested":
			request = event
		case strings.HasSuffix(event.EventName, "/terminal_probe.succeeded"), strings.HasSuffix(event.EventName, "/terminal_probe.failed"):
			result = event
		case event.EventName == "platform.runtime_log":
			if details, ok := event.Payload["details"].(map[string]any); ok && details["component"] == "activity" {
				diagnostic = event
			}
		}
	}
	if request.EventID == "" || result.EventID == "" || diagnostic.EventID == "" || p.calls.Load() != 1 {
		t.Fatalf("activity pre-verification proof missing: request=%s result=%s diagnostic=%s calls=%d", request.EventID, result.EventID, diagnostic.EventID, p.calls.Load())
	}
	if p.rejectFinal {
		p.writePayload(ctx, request.EventID, map[string]any{})
		return p.SelectedContractForkLifecycle.ActivateRunForkForSelectedContractExecution(ctx, req)
	}
	if p.hostile {
		tests := []struct {
			name, eventID, key string
			value              any
		}{
			{"malformed_request", request.EventID, "", nil},
			{"missing_activity_id", request.EventID, "activity_id", ""},
			{"malformed_owner", request.EventID, "node_id", "node:invalid"},
			{"foreign_node", request.EventID, "node_id", "agent:Zm9yZWlnbg"},
			{"missing_input", request.EventID, "input", nil},
			{"wrong_run", request.EventID, "source_run_id", uuid.NewString()},
			{"source_run", request.EventID, "source_run_id", catalogRuntimeRunID},
			{"wrong_source_event", request.EventID, "source_event_id", uuid.NewString()},
			{"wrong_grandparent", request.EventID, "parent_event_id", uuid.NewString()},
			{"wrong_entity", request.EventID, "entity_id", uuid.NewString()},
			{"wrong_flow", request.EventID, "flow_id", "other-flow"},
			{"wrong_flow_instance", request.EventID, "flow_instance", "worker-flow/foreign"},
			{"wrong_contract", request.EventID, "bundle_hash", "bundle-v2:sha256:" + strings.Repeat("a", 64)},
			{"wrong_handler", request.EventID, "handler_event_key", "worker.ready"},
			{"wrong_tool", request.EventID, "tool", "foreign-tool"},
			{"wrong_result_contract", request.EventID, "success_event", "foreign.succeeded"},
			{"unadmitted_write", request.EventID, "effect_class", "non_idempotent_write"},
			{"invalid_attempt", request.EventID, "attempt", 0},
			{"foreign_result", result.EventID, "activity_id", "foreign"},
			{"malformed_result", result.EventID, "", nil},
			{"fabricated_diagnostic_subject", diagnostic.EventID, "details", map[string]any{"component": "activity", "event_id": uuid.NewString()}},
			{"conflicting_diagnostic_subject", diagnostic.EventID, "details", map[string]any{"component": "activity", "event_id": request.EventID, "request_event_id": uuid.NewString()}},
			{"missing_diagnostic_component", diagnostic.EventID, "details", map[string]any{"event_id": request.EventID, "request_event_id": request.EventID}},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				original := observed[test.eventID].Payload
				payload := map[string]any{}
				if test.key != "" {
					for key, value := range original {
						payload[key] = value
					}
					payload[test.key] = test.value
				}
				p.writePayload(ctx, test.eventID, payload)
				defer p.writePayload(ctx, test.eventID, original)
				p.requireRejected(t, ctx, req)
			})
		}
		for _, test := range []struct{ name, id string }{
			{"missing_parent", uuid.NewString()}, {"source_parent", req.AllowedSourceEventIDs[0]}, {"unrelated_parent", diagnostic.EventID},
		} {
			t.Run(test.name, func(t *testing.T) {
				p.update(ctx, `UPDATE events SET source_event_id=$1 WHERE event_id=$2`, test.id, request.EventID)
				defer p.update(ctx, `UPDATE events SET source_event_id=$1 WHERE event_id=$2`, request.SourceEventID, request.EventID)
				p.requireRejected(t, ctx, req)
			})
		}
		t.Run("unknown_platform", func(t *testing.T) {
			p.update(ctx, `UPDATE events SET event_name=$1 WHERE event_id=$2`, "platform.reset", request.EventID)
			defer p.update(ctx, `UPDATE events SET event_name=$1 WHERE event_id=$2`, request.EventName, request.EventID)
			p.requireRejected(t, ctx, req)
		})
		t.Run("generic_runtime_producer", func(t *testing.T) {
			p.update(ctx, `UPDATE events SET produced_by=$1 WHERE event_id=$2`, "runtime", request.EventID)
			defer p.update(ctx, `UPDATE events SET produced_by=$1 WHERE event_id=$2`, request.Source, request.EventID)
			p.requireRejected(t, ctx, req)
		})
		t.Run("missing_selected_source", func(t *testing.T) {
			copy := req
			copy.ExecutionSource = nil
			p.requireRejected(t, ctx, copy)
		})
		t.Run("foreign_selected_source", func(t *testing.T) {
			var hash string
			if err := p.h.db.QueryRowContext(ctx, `SELECT bundle_hash FROM runs WHERE run_id=$1`, req.ForkRunID).Scan(&hash); err != nil {
				t.Fatal(err)
			}
			p.update(ctx, `UPDATE runs SET bundle_hash=$1 WHERE run_id=$2`, "bundle-v2:sha256:"+strings.Repeat("a", 64), req.ForkRunID)
			defer p.update(ctx, `UPDATE runs SET bundle_hash=$1 WHERE run_id=$2`, hash, req.ForkRunID)
			p.requireRejected(t, ctx, req)
		})
	}
	return p.SelectedContractForkLifecycle.ActivateRunForkForSelectedContractExecution(ctx, req)
}

func (p *activityLineageProofStore) requireRejected(t *testing.T, ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) {
	t.Helper()
	before := activityLineageStateSnapshot(t, ctx, p.h, req.ForkRunID)
	for attempt := 0; attempt < 2; attempt++ {
		result, err := p.SelectedContractForkLifecycle.ActivateRunForkForSelectedContractExecution(ctx, req)
		if err == nil || result.Activated || (!strings.Contains(err.Error(), "fork activity lineage") && !strings.Contains(err.Error(), "fork_events_not_selected_contract_lineage")) {
			t.Fatalf("lineage must reject before activation: activated=%t err=%v", result.Activated, err)
		}
		if p.calls.Load() != 1 {
			t.Fatalf("reverification repeated HTTP: %d", p.calls.Load())
		}
		if after := activityLineageStateSnapshot(t, ctx, p.h, req.ForkRunID); before != after {
			t.Fatal("rejected activation mutated durable state")
		}
	}
}

func (p *activityLineageProofStore) update(ctx context.Context, query string, args ...any) {
	p.t.Helper()
	if _, err := p.h.db.ExecContext(ctx, query, args...); err != nil {
		p.t.Fatal(err)
	}
}

func (p *activityLineageProofStore) writePayload(ctx context.Context, id string, payload map[string]any) {
	p.t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		p.t.Fatal(err)
	}
	p.update(ctx, `UPDATE events SET payload=$1, payload_bytes=$2 WHERE event_id=$3`, string(encoded), encoded, id)
}

func activityLineageStateSnapshot(t *testing.T, ctx context.Context, h *runtimeHarness, runID string) string {
	t.Helper()
	state, err := runScopedCatalogStore(t, h).LoadRunLifecycleSnapshot(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, table := range []string{"events", "event_deliveries", "entity_state", "agent_sessions", "agent_turns"} {
		var count int
		if err := h.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table+` WHERE run_id=$1`, runID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		counts[table] = count
	}
	instances, err := h.workflow.ListWorkflowInstances(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal([]any{state, counts, instances})
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func activityLineageEvents(t *testing.T, ctx context.Context, h *runtimeHarness, runID string) map[string]operatorread.OperatorEventFull {
	t.Helper()
	lister, err := h.catalogOperatorEventLister()
	if err != nil {
		t.Fatal(err)
	}
	page, err := lister.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{
		Filter: operatorread.OperatorEventListFilter{RunID: runID}, Limit: 1000, Order: "asc",
	})
	if err != nil || page.NextCursor != "" {
		t.Fatalf("bounded activity fixture readback: cursor=%q err=%v", page.NextCursor, err)
	}
	result := map[string]operatorread.OperatorEventFull{}
	for _, event := range page.Events {
		result[event.EventID] = event
	}
	return result
}

func assertCatalogActivityLineage(t *testing.T, observed map[string]operatorread.OperatorEventFull, failure bool) {
	t.Helper()
	requests, results, diagnostics := 0, 0, 0
	var request operatorread.OperatorEventFull
	for _, event := range observed {
		if event.EventName == "platform.activity_requested" {
			request = event
			requests++
		}
	}
	for _, event := range observed {
		if strings.HasSuffix(event.EventName, "/terminal_probe.succeeded") || strings.HasSuffix(event.EventName, "/terminal_probe.failed") {
			results++
			want := "succeeded"
			if failure {
				want = "failed"
			}
			if !strings.HasSuffix(event.EventName, "/terminal_probe."+want) || event.SourceEventID != request.SourceEventID || event.RunID != request.RunID {
				t.Fatalf("activity result lineage differs: request=%+v result=%+v", request, event)
			}
			if failure && event.Payload["failure"] == nil {
				t.Fatal("activity failure has no typed failure evidence")
			}
		}
		if event.EventName != "platform.runtime_log" {
			continue
		}
		details, ok := event.Payload["details"].(map[string]any)
		if !ok || details["component"] != "activity" {
			continue
		}
		diagnostics++
		if event.SourceEventID != request.EventID || event.RunID != request.RunID || details["request_event_id"] != request.EventID || details["event_id"] != request.EventID {
			t.Fatalf("diagnostic does not reference actual persisted request: request=%s event=%+v", request.EventID, event)
		}
	}
	wantDiagnostics := 2
	if failure {
		wantDiagnostics = 3
	}
	if request.Payload["effect_class"] == "non_idempotent_write" {
		wantDiagnostics = 1 // Journaled execution emits result_published, not read-attempt diagnostics.
	}
	if !reflect.DeepEqual([]int{requests, results, diagnostics}, []int{1, 1, wantDiagnostics}) {
		t.Fatal(fmt.Sprintf("activity facts = %d requests, %d results, %d diagnostics; want 1,1,%d", requests, results, diagnostics, wantDiagnostics))
	}
}

func assertCatalogActivityJournal(t *testing.T, ctx context.Context, h *runtimeHarness, runID string, failure bool) {
	t.Helper()
	var journal pipeline.ActivityAttemptJournal = h.sqlite
	if h.pg != nil {
		journal = h.pg
	}
	observed := activityLineageEvents(t, ctx, h, runID)
	for _, event := range observed {
		if event.EventName != "platform.activity_requested" {
			continue
		}
		record, found, err := journal.LoadActivityAttempt(ctx, event.EventID)
		wantStatus := pipeline.ActivityAttemptStatusSucceeded
		if failure {
			wantStatus = pipeline.ActivityAttemptStatusFailed
		}
		result, resultFound := observed[record.ResultEventID]
		if err != nil || !found || !resultFound || record.Status != wantStatus || record.Attempt != 1 ||
			record.RunID != runID || record.SourceEventID != event.SourceEventID || record.CompletedAt == nil ||
			record.ResultEventType != result.EventName || record.EffectClass != "non_idempotent_write" {
			t.Fatalf("journal/public readback differs: record=%+v found=%t result=%+v err=%v", record, found, result, err)
		}
		return
	}
	t.Fatal("journal proof requires a persisted activity request")
}
