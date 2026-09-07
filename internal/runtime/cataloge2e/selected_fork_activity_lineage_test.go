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
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
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
	hostileLoop bool
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
	if p.hostileLoop {
		requestOwner, err := activityidentity.ParseOwnerKey(request.Payload["node_id"].(string))
		if err != nil {
			t.Fatal(err)
		}
		node, ok := requestOwner.Node()
		if !ok {
			t.Fatal("expected node activity")
		}
		for _, site := range runtimecontracts.ActivitySitesForNode(node, req.ExecutionSource.ExecutableNodeEventHandlers(node)) {
			if site.Spec.ID != "unselected_probe" {
				continue
			}
			t.Run("reminted_unselected_rule_activity", func(t *testing.T) {
				payload := make(map[string]any, len(request.Payload))
				for key, value := range request.Payload {
					payload[key] = value
				}
				result := runtimecontracts.ActivityResultEventsForSite(site)
				payload["activity_id"], payload["success_event"], payload["failure_event"] = result.ActivityID, result.SuccessEvent, result.FailureEvent
				payload["revision_event"], payload["rejected_event"] = result.RevisionRequested, result.Rejected
				restore := p.remintRequest(ctx, request, payload)
				defer restore()
				p.requireRejected(t, ctx, req)
			})
		}
		instancePath, ok := request.Payload["flow_instance"].(string)
		if !ok {
			t.Fatal("loop request has no concrete flow instance")
		}
		owner, err := flowidentity.NewRunScopedFlowInstance(req.ForkRunID, flowidentity.RouteForInstancePath(instancePath))
		if err != nil {
			t.Fatal(err)
		}
		instance, found, err := p.h.workflow.Load(ctx, owner)
		if err != nil || !found {
			t.Fatalf("loop activity state: found=%t err=%v", found, err)
		}
		carrier, err := runtimeengine.StateCarrierFromPersisted(nil, nil, nil, instance.StateBuckets)
		if err != nil {
			t.Fatal(err)
		}
		activations, err := loopruntime.List(carrier.StateBuckets)
		if err != nil || len(activations) != 1 || activations[0].Status != loopruntime.StatusClosed || activations[0].CurrentStage != "complete" {
			t.Fatalf("result must close the real loop before activation: %+v err=%v", activations, err)
		}
		for _, test := range []struct {
			name, key string
			value     any
		}{
			{"loop_flow", "flow_id", "foreign"},
			{"loop_id", "loop_id", "foreign"},
			{"loop_activation", "activation_id", uuid.NewString()},
			{"loop_revision", "revision_id", uuid.NewString()},
			{"loop_revision_field", "revision_field", "foreign_revision"},
			{"loop_attempt", "attempt", 2},
			{"loop_attempt_zero", "attempt", 0},
			{"loop_stage_foreign", "loop_stage", "foreign"},
			{"loop_stage_before_transition", "loop_stage", "working"},
			{"loop_stage_after_close", "loop_stage", "complete"},
			{"loop_stage_absent", "loop_stage", ""},
			{"loop_generation_absent", "", nil},
		} {
			t.Run("reminted_"+test.name, func(t *testing.T) {
				payload := make(map[string]any, len(request.Payload))
				for key, value := range request.Payload {
					payload[key] = value
				}
				generation := map[string]any{}
				original, ok := request.Payload["loop_generation"].(map[string]any)
				if !ok {
					t.Fatal("positive control did not execute a loop activity")
				}
				for key, value := range original {
					generation[key] = value
				}
				payload["loop_generation"] = generation
				switch test.key {
				case "":
					delete(payload, "loop_generation")
				case "loop_stage":
					payload["loop_stage"] = test.value
				default:
					generation[test.key] = test.value
				}
				restore := p.remintRequest(ctx, request, payload)
				defer restore()
				p.requireRejected(t, ctx, req)
			})
		}
		t.Run("foreign_result_loop_revision", func(t *testing.T) {
			payload := make(map[string]any, len(result.Payload))
			for key, value := range result.Payload {
				payload[key] = value
			}
			payload[activations[0].RevisionField] = uuid.NewString()
			p.writePayload(ctx, result.EventID, payload)
			defer p.writePayload(ctx, result.EventID, result.Payload)
			p.requireRejected(t, ctx, req)
		})
		t.Run("foreign_diagnostic_loop_stage", func(t *testing.T) {
			payload := make(map[string]any, len(diagnostic.Payload))
			for key, value := range diagnostic.Payload {
				payload[key] = value
			}
			details := map[string]any{}
			for key, value := range diagnostic.Payload["details"].(map[string]any) {
				details[key] = value
			}
			details["loop_stage"] = "foreign"
			payload["details"] = details
			p.writePayload(ctx, diagnostic.EventID, payload)
			defer p.writePayload(ctx, diagnostic.EventID, diagnostic.Payload)
			p.requireRejected(t, ctx, req)
		})
	}
	if p.hostile {
		for _, test := range []struct {
			name       string
			attempt    int
			generation map[string]any
			stage      string
		}{
			{name: "reminted_request_attempt_2", attempt: 2},
			{name: "reminted_request_attempt_99", attempt: 99},
			{name: "reminted_non_loop_stage", attempt: 1, stage: "foreign"},
			{name: "reminted_non_loop_generation", attempt: 1, stage: "foreign", generation: map[string]any{
				"flow_id": "foreign", "loop_id": "foreign", "activation_id": "foreign", "revision_field": "revision", "revision_id": "foreign", "attempt": 1,
			}},
			{name: "reminted_malformed_generation", attempt: 1, generation: map[string]any{"loop_id": "partial"}},
		} {
			t.Run(test.name, func(t *testing.T) {
				payload := make(map[string]any, len(request.Payload))
				for key, value := range request.Payload {
					payload[key] = value
				}
				payload["attempt"] = test.attempt
				if test.generation != nil {
					payload["loop_generation"] = test.generation
				}
				if test.stage != "" {
					payload["loop_stage"] = test.stage
				}
				restore := p.remintRequest(ctx, request, payload)
				defer restore()
				p.requireRejected(t, ctx, req)
			})
		}
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

// Preserve every admitted envelope coordinate while reminting the request's
// canonical ID. This catches forged construction, not just an ID mismatch.
func (p *activityLineageProofStore) remintRequest(ctx context.Context, request operatorread.OperatorEventFull, payload map[string]any) func() {
	t := p.t
	t.Helper()
	str := func(key string) string { value, _ := payload[key].(string); return value }
	owner, err := activityidentity.ParseOwnerKey(str("node_id"))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var identityFacts struct {
		Attempt    int `json:"attempt"`
		Generation struct {
			RevisionID string `json:"revision_id"`
		} `json:"loop_generation"`
	}
	if err := json.Unmarshal(encoded, &identityFacts); err != nil {
		t.Fatal(err)
	}
	id := activityidentity.RequestEventID(activityidentity.Fact{
		RunID: str("source_run_id"), SourceEventID: str("source_event_id"), ParentEventID: str("parent_event_id"),
		EntityID: str("entity_id"), ExecutionFlowID: str("flow_id"), Owner: owner, HandlerEventKey: str("handler_event_key"),
		ActivityID: str("activity_id"), Tool: str("tool"), Attempt: identityFacts.Attempt, RevisionID: identityFacts.Generation.RevisionID,
	})
	if id == request.EventID {
		p.writePayload(ctx, id, payload)
		return func() { p.writePayload(ctx, id, request.Payload) }
	}
	rows, err := p.h.db.QueryContext(ctx, "SELECT * FROM events WHERE 1=0")
	if err != nil {
		t.Fatal(err)
	}
	columns, err := rows.Columns()
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	retained, selected := []string{}, []string{}
	for _, column := range columns {
		if column == "insertion_sequence" {
			continue
		}
		retained = append(retained, column)
		switch column {
		case "event_id":
			selected = append(selected, "$1")
		case "payload":
			selected = append(selected, "$2")
		case "payload_bytes":
			selected = append(selected, "$3")
		default:
			selected = append(selected, column)
		}
	}
	p.update(ctx, "INSERT INTO events ("+strings.Join(retained, ",")+") SELECT "+strings.Join(selected, ",")+" FROM events WHERE event_id=$4", id, string(encoded), encoded, request.EventID)
	return func() { p.update(ctx, "DELETE FROM events WHERE event_id=$1", id) }
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
