package apiv1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apispec"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
	"github.com/division-sh/swarm/internal/runtime/bundledelete"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	runtimeingress "github.com/division-sh/swarm/internal/runtime/ingress"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/runbundle"
)

const mutatingRuntimeProbeTestName = "TestOpenRPCMutatingHTTPRuntimeProbes"

func TestOpenRPCMutatingHTTPRuntimeProbes(t *testing.T) {
	root := repoRoot(t)
	api := loadComplianceAPISpec(t, root)
	openRPC, _ := loadComplianceOpenRPC(t, complianceOpenRPCPath(root))
	matrix := loadComplianceMatrix(t, filepath.Join(root, "internal", "apiv1", "testdata", "openrpc_compliance_matrix.yaml"))

	methods := mutatingHTTPRuntimeMethods(t, api, openRPC, matrix)
	assertStringList(t, "mutating HTTP runtime method set", methods, approvedMutatingHTTPRuntimeMethods())

	fixtures := mutatingHTTPRuntimeFixtures()
	assertStringList(t, "mutating HTTP runtime fixture methods", sortedMutatingProbeFixtureMethods(fixtures), methods)
	assertMutatingRuntimeMatrixProofRefs(t, api, matrix, methods)
	assertMutatingRuntimeDeclaredErrorCoverage(t, api, methods)

	t.Run("classification excludes sibling classes", func(t *testing.T) {
		mutating := complianceStringSet(methods)
		for _, sibling := range []string{"agent.get", "event.subscribe", "rpc.unsubscribe", "health.check"} {
			if _, ok := mutating[sibling]; ok {
				t.Fatalf("%s classified into mutating HTTP runtime probes; sibling methods belong to their approved probe class", sibling)
			}
		}
	})

	for _, methodName := range methods {
		methodName := methodName
		fixture := fixtures[methodName]
		method := api.MethodCatalog[methodName]

		t.Run(methodName+"/success_idempotency_and_conflict", func(t *testing.T) {
			handler, calls, state := newMutatingRuntimeProbeHandler(t, methodName)
			key := "idem-" + strings.ReplaceAll(methodName, ".", "-")
			params := mutatingProbeParamsWithIdempotency(fixture.Params, key)
			status, resp, body := callMutatingProbeRPC(t, handler, methodName, params, "Bearer "+testToken)
			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200 body=%s", status, body)
			}
			if calls[methodName] != 1 {
				t.Fatalf("%s handler calls = %d, want 1", methodName, calls[methodName])
			}
			assertMutatingProbeSuccess(t, methodName, resp, fixture.ResultKeys)
			if got := state.effectCount(); got != fixture.SuccessEffects {
				t.Fatalf("%s side effects after success = %d, want %d", methodName, got, fixture.SuccessEffects)
			}
			if got := state.idempotency.calls; got != 1 {
				t.Fatalf("%s idempotency calls after success = %d, want 1", methodName, got)
			}

			replayStatus, replayResp, replayBody := callMutatingProbeRPC(t, handler, methodName, params, "Bearer "+testToken)
			if replayStatus != http.StatusOK {
				t.Fatalf("replay status = %d, want 200 body=%s", replayStatus, replayBody)
			}
			if calls[methodName] != 2 {
				t.Fatalf("%s handler calls after replay = %d, want 2", methodName, calls[methodName])
			}
			assertMutatingProbeSuccess(t, methodName, replayResp, fixture.ResultKeys)
			if got := state.effectCount(); got != fixture.SuccessEffects {
				t.Fatalf("%s side effects after replay = %d, want %d", methodName, got, fixture.SuccessEffects)
			}
			if got := state.idempotency.calls; got != 2 {
				t.Fatalf("%s idempotency calls after replay = %d, want 2", methodName, got)
			}

			conflictParams := mutatingProbeConflictParams(fixture, key)
			conflictStatus, conflictResp, conflictBody := callMutatingProbeRPC(t, handler, methodName, conflictParams, "Bearer "+testToken)
			if conflictStatus != http.StatusOK {
				t.Fatalf("conflict status = %d, want 200 body=%s", conflictStatus, conflictBody)
			}
			if calls[methodName] != 3 {
				t.Fatalf("%s handler calls after conflict = %d, want 3", methodName, calls[methodName])
			}
			assertMutatingProbeApplicationError(t, testRegistry(t), methodName, conflictResp, IdempotencyConflictCode)
			if got := state.effectCount(); got != fixture.SuccessEffects {
				t.Fatalf("%s side effects after conflict = %d, want %d", methodName, got, fixture.SuccessEffects)
			}
			if got := state.idempotency.calls; got != 3 {
				t.Fatalf("%s idempotency calls after conflict = %d, want 3", methodName, got)
			}
		})

		t.Run(methodName+"/unknown_params_key", func(t *testing.T) {
			handler, calls, state := newMutatingRuntimeProbeHandler(t, methodName)
			params := cloneProbeParams(fixture.Params)
			params["_unexpected"] = true
			status, resp, body := callMutatingProbeRPC(t, handler, methodName, params, "Bearer "+testToken)
			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200 body=%s", status, body)
			}
			assertMutatingProbeInvalidParams(t, methodName, resp, "_unexpected")
			assertMutatingProbeNoExecution(t, methodName, calls, state, "unknown params-object key")
		})

		for _, paramName := range requiredParamNames(method) {
			paramName := paramName
			t.Run(methodName+"/missing_required_"+paramName, func(t *testing.T) {
				handler, calls, state := newMutatingRuntimeProbeHandler(t, methodName)
				params := cloneProbeParams(fixture.Params)
				delete(params, paramName)
				status, resp, body := callMutatingProbeRPC(t, handler, methodName, params, "Bearer "+testToken)
				if status != http.StatusOK {
					t.Fatalf("status = %d, want 200 body=%s", status, body)
				}
				assertMutatingProbeInvalidParams(t, methodName, resp, paramName)
				assertMutatingProbeNoExecution(t, methodName, calls, state, "missing required param")
			})
		}

		for _, authCase := range []struct {
			name   string
			header string
		}{
			{name: "missing_auth"},
			{name: "invalid_auth", header: "Bearer wrong"},
		} {
			authCase := authCase
			t.Run(methodName+"/"+authCase.name, func(t *testing.T) {
				handler, calls, state := newMutatingRuntimeProbeHandler(t, methodName)
				status, _, body := callMutatingProbeRPC(t, handler, methodName, fixture.Params, authCase.header)
				if status != http.StatusUnauthorized {
					t.Fatalf("status = %d, want 401 body=%s", status, body)
				}
				assertMutatingProbeNoExecution(t, methodName, calls, state, authCase.name)
			})
		}
	}

	for _, probe := range mutatingHTTPRuntimeErrorProbes() {
		probe := probe
		t.Run(probe.Method+"/"+probe.Code, func(t *testing.T) {
			method := api.MethodCatalog[probe.Method]
			if _, ok := complianceStringSet(method.Errors)[probe.Code]; !ok {
				t.Fatalf("%s error probe uses %s, absent from declared errors %v", probe.Method, probe.Code, method.Errors)
			}
			handler, calls, state := newMutatingRuntimeProbeHandler(t, probe.Method, probe.Modifiers...)
			status, resp, body := callMutatingProbeRPC(t, handler, probe.Method, probe.Params, "Bearer "+testToken)
			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200 body=%s", status, body)
			}
			if calls[probe.Method] != 1 {
				t.Fatalf("%s handler calls = %d, want 1 for declared application error", probe.Method, calls[probe.Method])
			}
			assertMutatingProbeApplicationError(t, testRegistry(t), probe.Method, resp, probe.Code)
			if got := state.effectCount(); got != probe.WantEffects {
				t.Fatalf("%s side effects after %s = %d, want %d", probe.Method, probe.Code, got, probe.WantEffects)
			}
		})
	}
}

func mutatingHTTPRuntimeMethods(t *testing.T, api *apispec.APISpecification, openRPC apispec.OpenRPCDocument, matrix openRPCComplianceMatrix) []string {
	t.Helper()
	openRPCMethods := map[string]struct{}{}
	for _, method := range openRPC.Methods {
		openRPCMethods[method.Name] = struct{}{}
	}
	matrixRows := map[string]openRPCMethodMatrix{}
	for _, row := range matrix.Methods {
		matrixRows[row.Method] = row
	}

	var out []string
	for _, methodName := range api.Conventions.Idempotency.MutatingMethods {
		method, ok := api.MethodCatalog[methodName]
		if !ok {
			t.Fatalf("%s listed in mutating_methods but missing from platform spec method_catalog", methodName)
		}
		if _, ok := openRPCMethods[methodName]; !ok {
			t.Fatalf("%s missing from generated OpenRPC artifact", methodName)
		}
		row, ok := matrixRows[methodName]
		if !ok {
			t.Fatalf("%s missing from OpenRPC compliance matrix", methodName)
		}
		if row.Transport != expectedComplianceTransport(methodName, method) {
			t.Fatalf("%s matrix transport = %q, want %q", methodName, row.Transport, expectedComplianceTransport(methodName, method))
		}
		if expectedComplianceTransport(methodName, method) != "http" {
			t.Fatalf("%s mutating runtime probe expected HTTP transport, got %q", methodName, expectedComplianceTransport(methodName, method))
		}
		out = append(out, methodName)
	}
	sort.Strings(out)
	return out
}

func approvedMutatingHTTPRuntimeMethods() []string {
	return []string{
		"agent.replay",
		"agent.replay_backlog",
		"agent.restart",
		"agent.send_directive",
		"bundle.delete",
		"bundle.register",
		"conversation.fork",
		"conversation.fork_chat",
		"conversation.fork_delete",
		"event.publish",
		"event.replay",
		"mailbox.approve",
		"mailbox.defer",
		"mailbox.reject",
		"run.continue",
		"run.fork",
		"run.pause",
		"run.start",
		"run.stop",
		"runtime.nuke",
		"runtime.pause",
		"runtime.resume",
	}
}

type mutatingHTTPRuntimeFixture struct {
	Params                         map[string]any
	ConflictParams                 map[string]any
	TrimEquivalentConflictKeyValue bool
	ResultKeys                     []string
	SuccessEffects                 int
}

func mutatingHTTPRuntimeFixtures() map[string]mutatingHTTPRuntimeFixture {
	runID := "00000000-0000-0000-0000-000000000101"
	otherRunID := "00000000-0000-0000-0000-000000000102"
	sourceSessionID := "00000000-0000-0000-0000-000000000201"
	forkID := "00000000-0000-0000-0000-000000000301"
	until := time.Unix(1700003600, 0).UTC().Format(time.RFC3339Nano)
	later := time.Unix(1700007200, 0).UTC().Format(time.RFC3339Nano)
	return map[string]mutatingHTTPRuntimeFixture{
		"agent.replay": {
			Params:         map[string]any{"event_id": "evt-1", "agent_id": "agent-a"},
			ConflictParams: map[string]any{"event_id": "evt-1", "agent_id": "agent-b"},
			ResultKeys:     []string{"event_id", "agent_id", "replay_event_id", "audit_event_id", "original_delivery", "new_delivery"},
			SuccessEffects: 2,
		},
		"agent.replay_backlog": {
			Params:         map[string]any{"agent_id": "agent-a"},
			ConflictParams: map[string]any{"agent_id": "agent-b"},
			ResultKeys:     []string{"ok", "replayed_count"},
			SuccessEffects: 1,
		},
		"agent.restart": {
			Params:         map[string]any{"agent_id": "agent-a"},
			ConflictParams: map[string]any{"agent_id": "agent-b"},
			ResultKeys:     []string{"ok"},
			SuccessEffects: 1,
		},
		"agent.send_directive": {
			Params:         map[string]any{"agent_id": "agent-a", "directive": "continue", "run_id": runID},
			ConflictParams: map[string]any{"agent_id": "agent-a", "directive": "pause", "run_id": runID},
			ResultKeys:     []string{"ok", "run_id", "run_id_resolution", "directive_event_id", "directive_event_type"},
			SuccessEffects: 1,
		},
		"bundle.delete": {
			Params:         map[string]any{"bundle_hash": runStartTestBundleHash, "force": true, "dry_run": false},
			ConflictParams: map[string]any{"bundle_hash": runStartTestBundleHash, "force": true, "dry_run": true},
			ResultKeys:     []string{"ok", "status", "operation_name", "bundle_hash", "force", "deleted", "dry_run", "active_runs_stopped", "deliveries_cancelled", "containers_stopped", "plan", "cleanup", "containers", "final_mutation"},
			SuccessEffects: 1,
		},
		"conversation.fork": {
			Params:         map[string]any{"source_session_id": sourceSessionID, "fork_point": map[string]any{"kind": "turn", "turn_index": float64(1)}},
			ConflictParams: map[string]any{"source_session_id": sourceSessionID, "fork_point": map[string]any{"kind": "turn", "turn_index": float64(2)}},
			ResultKeys:     []string{"fork", "idempotency_replayed"},
			SuccessEffects: 1,
		},
		"conversation.fork_chat": {
			Params:         map[string]any{"fork_id": forkID, "message": "inspect fork"},
			ConflictParams: map[string]any{"fork_id": forkID, "message": "different message"},
			ResultKeys:     []string{"fork_id", "turn", "snapshot", "sandbox_policy", "idempotency_replayed"},
			SuccessEffects: 1,
		},
		"conversation.fork_delete": {
			Params:         map[string]any{"fork_id": forkID},
			ConflictParams: map[string]any{"fork_id": "00000000-0000-0000-0000-000000000302"},
			ResultKeys:     []string{"ok", "fork_id", "deleted", "already_deleted", "idempotency_replayed"},
			SuccessEffects: 1,
		},
		"bundle.register": {
			Params:         map[string]any{"content_yaml": testBundleRegistrationEnvelope()},
			ConflictParams: map[string]any{"content_yaml": strings.Replace(testBundleRegistrationEnvelope(), "name: registered", "name: registered-conflict", 1)},
			ResultKeys:     []string{"bundle_hash", "registered", "has_data", "data_size_bytes"},
			SuccessEffects: 1,
		},
		"event.publish": {
			Params:         map[string]any{"bundle_hash": runStartTestBundleHash, "event_name": "scan.requested", "payload": map[string]any{"topic": "medicine"}},
			ConflictParams: map[string]any{"bundle_hash": runStartTestBundleHash, "event_name": "scan.requested", "payload": map[string]any{"topic": "dentistry"}},
			ResultKeys:     []string{"event_id", "run_id", "new_run_created", "deliveries"},
			SuccessEffects: 1,
		},
		"event.replay": {
			Params:         map[string]any{"event_id": "evt-1"},
			ConflictParams: map[string]any{"event_id": "evt-1", "subscribers": []any{"agent-a"}},
			ResultKeys:     []string{"event_id", "replay_event_id", "audit_event_id", "subscribers_replayed", "original_deliveries", "new_deliveries"},
			SuccessEffects: 2,
		},
		"mailbox.approve": {
			Params:         map[string]any{"mailbox_id": "mailbox-1", "decision_payload": map[string]any{"approved": true}},
			ConflictParams: map[string]any{"mailbox_id": "mailbox-1", "decision_payload": map[string]any{"approved": false}},
			ResultKeys:     []string{"ok", "mailbox_decision_id", "status", "idempotency_replayed", "downstream_event_id", "downstream_event_name", "downstream_subscribers", "downstream_subscriber_source"},
			SuccessEffects: 2,
		},
		"mailbox.defer": {
			Params:         map[string]any{"mailbox_id": "mailbox-1", "until": until},
			ConflictParams: map[string]any{"mailbox_id": "mailbox-1", "until": later},
			ResultKeys:     []string{"ok", "mailbox_decision_id", "status", "idempotency_replayed"},
			SuccessEffects: 1,
		},
		"mailbox.reject": {
			Params:         map[string]any{"mailbox_id": "mailbox-1", "reason": "not enough evidence"},
			ConflictParams: map[string]any{"mailbox_id": "mailbox-1", "reason": "duplicate request"},
			ResultKeys:     []string{"ok", "mailbox_decision_id", "status", "idempotency_replayed"},
			SuccessEffects: 1,
		},
		"run.fork": {
			Params:         map[string]any{"source_run_id": runID, "fork_event_id": runForkTestEventID},
			ConflictParams: map[string]any{"source_run_id": otherRunID, "fork_event_id": runForkTestEventID},
			ResultKeys:     []string{"owner", "source_run_id", "fork_run_id", "fork_event_id", "fork_run_status", "bundle_hash", "executed_event_count"},
			SuccessEffects: 1,
		},
		"run.continue": {
			Params:         map[string]any{"run_id": runID},
			ConflictParams: map[string]any{"run_id": otherRunID},
			ResultKeys:     []string{"ok"},
			SuccessEffects: 1,
		},
		"run.pause": {
			Params:         map[string]any{"run_id": runID},
			ConflictParams: map[string]any{"run_id": otherRunID},
			ResultKeys:     []string{"ok"},
			SuccessEffects: 1,
		},
		"run.start": {
			Params:         map[string]any{"bundle_hash": runStartTestBundleHash, "event_name": "scan.requested", "payload": map[string]any{"topic": "medicine"}, "run_id": runID},
			ConflictParams: map[string]any{"bundle_hash": runStartTestBundleHash, "event_name": "scan.requested", "payload": map[string]any{"topic": "dentistry"}, "run_id": runID},
			ResultKeys:     []string{"run_id", "status"},
			SuccessEffects: 1,
		},
		"run.stop": {
			Params:         map[string]any{"run_id": runID},
			ConflictParams: map[string]any{"run_id": otherRunID},
			ResultKeys:     []string{"ok"},
			SuccessEffects: 1,
		},
		"runtime.nuke": {
			Params:         map[string]any{"dry_run": false},
			ConflictParams: map[string]any{"dry_run": true},
			ResultKeys:     []string{"ok", "status", "dry_run", "include_bundles", "operation_name", "plan", "quiescence", "cleanup", "containers"},
			SuccessEffects: 4,
		},
		"runtime.pause": {
			Params:                         map[string]any{},
			ConflictParams:                 map[string]any{},
			TrimEquivalentConflictKeyValue: true,
			ResultKeys:                     []string{"ok"},
			SuccessEffects:                 1,
		},
		"runtime.resume": {
			Params:                         map[string]any{},
			ConflictParams:                 map[string]any{},
			TrimEquivalentConflictKeyValue: true,
			ResultKeys:                     []string{"ok"},
			SuccessEffects:                 1,
		},
	}
}

type mutatingHTTPRuntimeErrorProbe struct {
	Method      string
	Params      map[string]any
	Code        string
	WantEffects int
	Modifiers   []func(*mutatingRuntimeProbeState)
}

func mutatingHTTPRuntimeErrorProbes() []mutatingHTTPRuntimeErrorProbe {
	runID := "00000000-0000-0000-0000-000000000101"
	missingRunID := "00000000-0000-0000-0000-000000000999"
	otherBundleHash := "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	validEvent := map[string]any{"bundle_hash": runStartTestBundleHash, "event_name": "scan.requested", "payload": map[string]any{"topic": "medicine"}, "idempotency_key": "idem-error"}
	legacyOnlyEvent := map[string]any{"bundle_ref": map[string]any{"fingerprint": runStartTestFingerprint}, "event_name": "scan.requested", "payload": map[string]any{"topic": "medicine"}, "idempotency_key": "idem-error"}
	invalidBundleHashEvent := mergeProbeParams(validEvent, map[string]any{"bundle_hash": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"})
	sourceSessionID := "00000000-0000-0000-0000-000000000201"
	forkID := "00000000-0000-0000-0000-000000000301"
	return []mutatingHTTPRuntimeErrorProbe{
		{Method: "event.publish", Params: legacyOnlyEvent, Code: BundleScopeRequiredCode},
		{Method: "event.publish", Params: mergeProbeParams(validEvent, map[string]any{"bundle_ref": map[string]any{"fingerprint": runStartTestFingerprint}}), Code: UnsupportedBundleHashCode},
		{Method: "event.publish", Params: mergeProbeParams(validEvent, map[string]any{"bundle_hash": otherBundleHash}), Code: BundleUnavailableCode},
		{Method: "event.publish", Params: mergeProbeParams(validEvent, map[string]any{"bundle_hash": otherBundleHash, "run_id": runID}), Code: BundleMismatchCode},
		{Method: "event.publish", Params: invalidBundleHashEvent, Code: UnsupportedBundleHashCode},
		{Method: "event.publish", Params: mergeProbeParams(legacyOnlyEvent, map[string]any{"bundle_ref": map[string]any{"label": "latest"}}), Code: UnsupportedBundleRefCode},
		{Method: "event.publish", Params: mergeProbeParams(validEvent, map[string]any{"event_name": "scan.missing"}), Code: EventNotDeclaredCode},
		{Method: "event.publish", Params: validEvent, Code: EventPublishFailedCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) { s.events.publishErr = errors.New("simulated publish failure") }}},
		{Method: "event.publish", Params: validEvent, Code: PayloadValidationFailedCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) { s.events.publishErr = runtimebus.ErrPayloadValidation }}},
		{Method: "event.publish", Params: mergeProbeParams(validEvent, map[string]any{"run_id": missingRunID}), Code: RunNotFoundCode},
		{Method: "event.publish", Params: mergeProbeParams(validEvent, map[string]any{"run_id": runID, "source_event_id": "00000000-0000-0000-0000-000000000998"}), Code: EventNotFoundCode},
		{Method: "event.publish", Params: mergeProbeParams(validEvent, map[string]any{"run_id": runID}), Code: BundleDataIntegrityErrorCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.runForkAvailability.rows[runID] = runForkDataIntegrity(runID, runStartTestBundleHash)
		}}},
		{Method: "event.publish", Params: mergeProbeParams(validEvent, map[string]any{"run_id": runID}), Code: RunAlreadyTerminalCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.runs.headers[runID] = store.RunHeader{RunID: runID, Status: "completed"}
		}}},

		{Method: "event.replay", Params: map[string]any{"event_id": "missing", "idempotency_key": "idem-error"}, Code: EventNotFoundCode},
		{Method: "event.replay", Params: map[string]any{"event_id": "evt-empty", "idempotency_key": "idem-error"}, Code: EventReplayNoDeliveryHistoryCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.observability.events["evt-empty"] = mutatingProbeOriginalEvent("evt-empty", nil, eventReplayStatusDelivered)
		}}},
		{Method: "event.replay", Params: map[string]any{"event_id": "evt-1", "subscribers": []any{"agent-b"}, "idempotency_key": "idem-error"}, Code: EventReplaySubscriberNotOriginalCode},
		{Method: "event.replay", Params: map[string]any{"event_id": "evt-1", "idempotency_key": "idem-error"}, Code: EventReplaySubscriberUnavailableCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) { s.events.missingRecipients = []string{"agent-a"} }}},
		{Method: "event.replay", Params: map[string]any{"event_id": "evt-pending", "idempotency_key": "idem-error"}, Code: EventReplayNotEligibleCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.observability.events["evt-pending"] = mutatingProbeOriginalEvent("evt-pending", []string{"agent-a"}, eventReplayStatusPending)
		}}},
		{Method: "event.replay", Params: map[string]any{"event_id": "evt-1", "idempotency_key": "idem-error"}, Code: PayloadValidationFailedCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) { s.events.checkErr = runtimebus.ErrPayloadValidation }}},

		{Method: "agent.replay", Params: map[string]any{"event_id": "missing", "agent_id": "agent-a", "idempotency_key": "idem-error"}, Code: EventNotFoundCode},
		{Method: "agent.replay", Params: map[string]any{"event_id": "evt-empty", "agent_id": "agent-a", "idempotency_key": "idem-error"}, Code: EventReplayNoDeliveryHistoryCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.observability.events["evt-empty"] = mutatingProbeOriginalEvent("evt-empty", nil, eventReplayStatusDelivered)
		}}},
		{Method: "agent.replay", Params: map[string]any{"event_id": "evt-1", "agent_id": "agent-b", "idempotency_key": "idem-error"}, Code: EventReplaySubscriberNotOriginalCode},
		{Method: "agent.replay", Params: map[string]any{"event_id": "evt-1", "agent_id": "agent-a", "idempotency_key": "idem-error"}, Code: EventReplaySubscriberUnavailableCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) { s.events.missingRecipients = []string{"agent-a"} }}},
		{Method: "agent.replay", Params: map[string]any{"event_id": "evt-pending", "agent_id": "agent-a", "idempotency_key": "idem-error"}, Code: EventReplayNotEligibleCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.observability.events["evt-pending"] = mutatingProbeOriginalEvent("evt-pending", []string{"agent-a"}, eventReplayStatusPending)
		}}},
		{Method: "agent.replay", Params: map[string]any{"event_id": "evt-1", "agent_id": "agent-a", "idempotency_key": "idem-error"}, Code: PayloadValidationFailedCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) { s.events.checkErr = runtimebus.ErrPayloadValidation }}},

		{Method: "conversation.fork", Params: map[string]any{"source_session_id": sourceSessionID, "fork_point": map[string]any{"kind": "turn", "turn_index": float64(1)}, "idempotency_key": "idem-error"}, Code: SessionNotFoundCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.forks.createErr = store.ErrSessionNotFound
		}}},
		{Method: "conversation.fork", Params: map[string]any{"source_session_id": sourceSessionID, "fork_point": map[string]any{"kind": "turn", "turn_index": float64(1)}, "idempotency_key": "idem-error"}, Code: TurnNotFoundCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.forks.createErr = store.ErrTurnNotFound
		}}},
		{Method: "conversation.fork", Params: map[string]any{"source_session_id": sourceSessionID, "fork_point": map[string]any{"kind": "event", "event_id": "00000000-0000-0000-0000-000000000901"}, "idempotency_key": "idem-error"}, Code: EventNotFoundCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.forks.createErr = store.ErrEventNotFound
		}}},
		{Method: "conversation.fork_chat", Params: map[string]any{"fork_id": forkID, "message": "inspect fork", "idempotency_key": "idem-error"}, Code: ForkNotFoundCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.forks.prepareErr = store.ErrConversationForkNotFound
		}}},
		{Method: "conversation.fork_delete", Params: map[string]any{"fork_id": forkID, "idempotency_key": "idem-error"}, Code: ForkNotFoundCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.forks.deleteErr = store.ErrConversationForkNotFound
		}}},
		{Method: "bundle.register", Params: map[string]any{"content_yaml": testBundleRegistrationEnvelope(), "idempotency_key": "idem-error"}, Code: BundleRegisterConflictCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.bundleCatalog.conflict = true
		}}},

		{Method: "run.fork", Params: map[string]any{"source_run_id": missingRunID, "fork_event_id": runForkTestEventID, "idempotency_key": "idem-error"}, Code: RunNotFoundCode},
		{Method: "run.fork", Params: map[string]any{"source_run_id": runForkTestSourceRunID, "fork_event_id": runForkTestEventID, "idempotency_key": "idem-error"}, Code: EventNotFoundCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.runFork.err = errors.New("fork point event " + runForkTestEventID + " not found in source run " + runForkTestSourceRunID)
		}}},
		{Method: "run.fork", Params: map[string]any{"source_run_id": runForkTestSourceRunID, "fork_event_id": runForkTestEventID, "bundle_hash": "bundle-v1:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", "idempotency_key": "idem-error"}, Code: BundleUnavailableCode},
		{Method: "run.fork", Params: map[string]any{"source_run_id": runForkTestSourceRunID, "fork_event_id": runForkTestEventID, "idempotency_key": "idem-error"}, Code: BundleUnavailableCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.runForkAvailability.rows[runForkTestSourceRunID] = runForkUnavailable(runForkTestSourceRunID, runForkTestBundleHash, "legacy")
		}}},
		{Method: "run.fork", Params: map[string]any{"source_run_id": runForkTestSourceRunID, "fork_event_id": runForkTestEventID, "idempotency_key": "idem-error"}, Code: BundleDataIntegrityErrorCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.runForkAvailability.rows[runForkTestSourceRunID] = runForkDataIntegrity(runForkTestSourceRunID, runForkTestBundleHash)
		}}},

		{Method: "run.start", Params: legacyOnlyEvent, Code: BundleScopeRequiredCode},
		{Method: "run.start", Params: mergeProbeParams(validEvent, map[string]any{"bundle_hash": otherBundleHash}), Code: BundleUnavailableCode},
		{Method: "run.start", Params: mergeProbeParams(validEvent, map[string]any{"bundle_hash": otherBundleHash, "run_id": runID}), Code: BundleMismatchCode},
		{Method: "run.start", Params: invalidBundleHashEvent, Code: UnsupportedBundleHashCode},
		{Method: "run.start", Params: mergeProbeParams(legacyOnlyEvent, map[string]any{"bundle_ref": map[string]any{"label": "latest"}, "run_id": runID}), Code: UnsupportedBundleRefCode},
		{Method: "run.start", Params: mergeProbeParams(validEvent, map[string]any{"run_id": runID}), Code: BundleDataIntegrityErrorCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.runForkAvailability.rows[runID] = runForkDataIntegrity(runID, runStartTestBundleHash)
		}}},
		{Method: "run.start", Params: mergeProbeParams(validEvent, map[string]any{"event_name": "scan.missing"}), Code: EventNotDeclaredCode},
		{Method: "run.start", Params: validEvent, Code: EventPublishFailedCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) { s.events.publishErr = errors.New("simulated publish failure") }}},
		{Method: "run.start", Params: validEvent, Code: PayloadValidationFailedCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) { s.events.publishErr = runtimebus.ErrPayloadValidation }}},

		{Method: "run.stop", Params: map[string]any{"run_id": runID, "idempotency_key": "idem-error"}, Code: RunNotFoundCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.runControl.errs["stop"] = &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrRunNotFound, RunID: runID}
		}}},
		{Method: "run.stop", Params: map[string]any{"run_id": runID, "idempotency_key": "idem-error"}, Code: RunAlreadyTerminalCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.runControl.errs["stop"] = &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrAlreadyTerminal, RunID: runID, CurrentStatus: "completed"}
		}}},
		{Method: "run.pause", Params: map[string]any{"run_id": runID, "idempotency_key": "idem-error"}, Code: RunNotFoundCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.runControl.errs["pause"] = &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrRunNotFound, RunID: runID}
		}}},
		{Method: "run.pause", Params: map[string]any{"run_id": runID, "idempotency_key": "idem-error"}, Code: RunAlreadyTerminalCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.runControl.errs["pause"] = &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrAlreadyTerminal, RunID: runID, CurrentStatus: "completed"}
		}}},
		{Method: "run.pause", Params: map[string]any{"run_id": runID, "idempotency_key": "idem-error"}, Code: RunAlreadyPausedCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.runControl.errs["pause"] = &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrAlreadyPaused, RunID: runID, CurrentStatus: "paused"}
		}}},
		{Method: "run.continue", Params: map[string]any{"run_id": runID, "idempotency_key": "idem-error"}, Code: RunNotFoundCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.runControl.errs["continue"] = &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrRunNotFound, RunID: runID}
		}}},
		{Method: "run.continue", Params: map[string]any{"run_id": runID, "idempotency_key": "idem-error"}, Code: RunNotPausedCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.runControl.errs["continue"] = &runtimeruncontrol.StateError{Err: runtimeruncontrol.ErrNotPaused, RunID: runID, CurrentStatus: "running"}
		}}},

		{Method: "mailbox.approve", Params: map[string]any{"mailbox_id": "missing", "idempotency_key": "idem-error"}, Code: MailboxNotFoundCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) { s.mailbox.decisionErr = store.ErrMailboxV1NotFound }}},
		{Method: "mailbox.approve", Params: map[string]any{"mailbox_id": "mailbox-1", "idempotency_key": "idem-error"}, Code: MailboxAlreadyDecidedCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.mailbox.decisionErr = &store.MailboxV1AlreadyDecidedError{MailboxID: "mailbox-1", ExistingDecision: "approved", DecidedAt: s.now}
		}}},
		{Method: "mailbox.approve", Params: map[string]any{"mailbox_id": "mailbox-1", "idempotency_key": "idem-error"}, Code: MailboxApprovalEventUnconfiguredCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.mailbox.decisionErr = &store.MailboxV1ApprovalRouteError{MailboxID: "mailbox-1", ItemType: "review_request"}
		}}},
		{Method: "mailbox.reject", Params: map[string]any{"mailbox_id": "missing", "reason": "no", "idempotency_key": "idem-error"}, Code: MailboxNotFoundCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) { s.mailbox.decisionErr = store.ErrMailboxV1NotFound }}},
		{Method: "mailbox.reject", Params: map[string]any{"mailbox_id": "mailbox-1", "reason": "no", "idempotency_key": "idem-error"}, Code: MailboxAlreadyDecidedCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.mailbox.decisionErr = &store.MailboxV1AlreadyDecidedError{MailboxID: "mailbox-1", ExistingDecision: "rejected", DecidedAt: s.now}
		}}},
		{Method: "mailbox.defer", Params: map[string]any{"mailbox_id": "missing", "until": time.Unix(1700003600, 0).UTC().Format(time.RFC3339Nano), "idempotency_key": "idem-error"}, Code: MailboxNotFoundCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) { s.mailbox.decisionErr = store.ErrMailboxV1NotFound }}},
		{Method: "mailbox.defer", Params: map[string]any{"mailbox_id": "mailbox-1", "until": time.Unix(1700003600, 0).UTC().Format(time.RFC3339Nano), "idempotency_key": "idem-error"}, Code: MailboxAlreadyDecidedCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.mailbox.decisionErr = &store.MailboxV1AlreadyDecidedError{MailboxID: "mailbox-1", ExistingDecision: "deferred", DecidedAt: s.now}
		}}},
		{Method: "mailbox.defer", Params: map[string]any{"mailbox_id": "mailbox-1", "until": time.Unix(1700003600, 0).UTC().Format(time.RFC3339Nano), "idempotency_key": "idem-error"}, Code: InvalidDeferUntilCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.mailbox.decisionErr = &store.MailboxV1InvalidDeferUntilError{Reason: "too far in future"}
		}}},

		{Method: "agent.send_directive", Params: map[string]any{"agent_id": "missing", "directive": "continue", "idempotency_key": "idem-error"}, Code: AgentNotFoundCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.agentControl.errs["agent.send_directive"] = &runtimeagentcontrol.StateError{Err: runtimeagentcontrol.ErrAgentNotFound, AgentID: "missing"}
		}}},
		{Method: "agent.send_directive", Params: map[string]any{"agent_id": "agent-a", "directive": "continue", "idempotency_key": "idem-error"}, Code: AgentNotRunningCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.agentControl.errs["agent.send_directive"] = &runtimeagentcontrol.StateError{Err: runtimeagentcontrol.ErrAgentNotRunning, AgentID: "agent-a", CurrentStatus: runtimeagentcontrol.StatusTerminated}
		}}},
		{Method: "agent.send_directive", Params: map[string]any{"agent_id": "agent-a", "directive": "continue", "run_id": missingRunID, "idempotency_key": "idem-error"}, Code: RunNotFoundCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.agentControl.errs["agent.send_directive"] = &runtimeagentcontrol.StateError{Err: runtimeagentcontrol.ErrRunNotFound, AgentID: "agent-a", RunID: missingRunID}
		}}},
		{Method: "agent.send_directive", Params: map[string]any{"agent_id": "agent-a", "directive": "continue", "run_id": runID, "idempotency_key": "idem-error"}, Code: RunAlreadyTerminalCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.agentControl.errs["agent.send_directive"] = &runtimeagentcontrol.StateError{Err: runtimeagentcontrol.ErrRunAlreadyTerminal, AgentID: "agent-a", RunID: runID, CurrentStatus: "completed"}
		}}},
		{Method: "agent.send_directive", Params: map[string]any{"agent_id": "agent-a", "directive": "continue", "idempotency_key": "idem-error"}, Code: AmbiguousRunTargetCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.agentControl.errs["agent.send_directive"] = &runtimeagentcontrol.StateError{Err: runtimeagentcontrol.ErrAmbiguousRunTarget, AgentID: "agent-a", ActiveSessions: []runtimeagentcontrol.ActiveSessionTarget{{SessionID: "sess-1", RunID: runID}}}
		}}},
		{Method: "agent.restart", Params: map[string]any{"agent_id": "missing", "idempotency_key": "idem-error"}, Code: AgentNotFoundCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.agentControl.errs["agent.restart"] = &runtimeagentcontrol.StateError{Err: runtimeagentcontrol.ErrAgentNotFound, AgentID: "missing"}
		}}},
		{Method: "agent.replay_backlog", Params: map[string]any{"agent_id": "missing", "idempotency_key": "idem-error"}, Code: AgentNotFoundCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.agentControl.errs["agent.replay_backlog"] = &runtimeagentcontrol.StateError{Err: runtimeagentcontrol.ErrAgentNotFound, AgentID: "missing"}
		}}},

		{Method: "runtime.pause", Params: map[string]any{"idempotency_key": "idem-error"}, Code: RuntimeAlreadyPausedCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.runtimeIngress.errs["runtime.pause"] = runtimeingress.ErrAlreadyPaused
		}}},
		{Method: "runtime.resume", Params: map[string]any{"idempotency_key": "idem-error"}, Code: RuntimeNotPausedCode, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.runtimeIngress.errs["runtime.resume"] = runtimeingress.ErrNotPaused
		}}},
		{Method: "runtime.nuke", Params: map[string]any{"dry_run": false, "idempotency_key": "idem-error"}, Code: RuntimeNukeInProgressCode, WantEffects: 1, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) { s.nuke.planErr = destructivereset.ErrOperationInProgress }}},
		{Method: "bundle.delete", Params: map[string]any{"bundle_hash": runStartTestBundleHash, "force": true, "idempotency_key": "idem-error"}, Code: BundleNotFoundCode, WantEffects: 1, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) { s.bundleDelete.err = store.ErrBundleNotFound }}},
		{Method: "bundle.delete", Params: map[string]any{"bundle_hash": runStartTestBundleHash, "force": true, "idempotency_key": "idem-error"}, Code: BundleDeleteInProgressCode, WantEffects: 1, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) { s.bundleDelete.err = bundledelete.ErrOperationInProgress }}},
		{Method: "bundle.delete", Params: map[string]any{"bundle_hash": runStartTestBundleHash, "idempotency_key": "idem-error"}, Code: BundleHasActiveRunsCode, WantEffects: 1, Modifiers: []func(*mutatingRuntimeProbeState){func(s *mutatingRuntimeProbeState) {
			s.bundleDelete.err = &bundledelete.ActiveRunsRemainError{
				BundleHash: runStartTestBundleHash,
				ActiveRuns: []bundledelete.RunRef{{
					RunID:        "00000000-0000-0000-0000-000000000101",
					Status:       "running",
					BundleHash:   runStartTestBundleHash,
					BundleSource: "persisted",
				}},
			}
		}}},
	}
}

func assertMutatingRuntimeDeclaredErrorCoverage(t *testing.T, api *apispec.APISpecification, methods []string) {
	t.Helper()
	covered := map[string]map[string]struct{}{}
	for _, methodName := range methods {
		covered[methodName] = map[string]struct{}{IdempotencyConflictCode: {}}
	}
	for _, probe := range mutatingHTTPRuntimeErrorProbes() {
		if _, ok := covered[probe.Method]; !ok {
			t.Fatalf("%s error probe is outside the approved mutating HTTP method set", probe.Method)
		}
		covered[probe.Method][probe.Code] = struct{}{}
	}
	for _, methodName := range methods {
		for _, code := range api.MethodCatalog[methodName].Errors {
			if _, ok := covered[methodName][code]; !ok {
				t.Fatalf("%s declared error %s lacks generated mutating runtime probe coverage", methodName, code)
			}
		}
	}
}

func assertMutatingRuntimeMatrixProofRefs(t *testing.T, api *apispec.APISpecification, matrix openRPCComplianceMatrix, methods []string) {
	t.Helper()
	mutatingMethods := complianceStringSet(methods)
	rows := map[string]openRPCMethodMatrix{}
	for _, row := range matrix.Methods {
		rows[row.Method] = row
		if _, ok := mutatingMethods[row.Method]; !ok && rowHasMutatingRuntimeProof(row) {
			t.Fatalf("%s has %s proof_ref but is outside the approved mutating HTTP runtime probe set", row.Method, mutatingRuntimeProbeTestName)
		}
	}

	for _, methodName := range methods {
		row, ok := rows[methodName]
		if !ok {
			t.Fatalf("%s missing from compliance matrix", methodName)
		}
		assertEvidenceHasMutatingRuntimeProof(t, methodName, "happy_path", row.HappyPath)
		assertEvidenceHasMutatingRuntimeProof(t, methodName, "unknown_top_level_param_validation", row.UnknownTopLevelParamValidation)
		assertEvidenceHasMutatingRuntimeProof(t, methodName, "auth", row.Auth)
		assertEvidenceHasMutatingRuntimeProof(t, methodName, "declared_error_tests", row.DeclaredErrorTests)
		assertEvidenceHasMutatingRuntimeProof(t, methodName, "idempotency", row.Idempotency)
		assertEvidenceHasMutatingRuntimeProof(t, methodName, "result_schema", row.ResultSchema)
		if len(requiredParamNames(api.MethodCatalog[methodName])) > 0 {
			assertEvidenceHasMutatingRuntimeProof(t, methodName, "required_param_validation", row.RequiredParamValidation)
		}
	}
}

func assertEvidenceHasMutatingRuntimeProof(t *testing.T, methodName, field string, evidence complianceEvidence) {
	t.Helper()
	if !evidenceHasGoTest(evidence, mutatingRuntimeProbeTestName) {
		t.Fatalf("%s %s missing go_test proof_ref %s", methodName, field, mutatingRuntimeProbeTestName)
	}
}

func rowHasMutatingRuntimeProof(row openRPCMethodMatrix) bool {
	for _, evidence := range complianceEvidenceFields(row) {
		if evidenceHasGoTest(evidence.evidence, mutatingRuntimeProbeTestName) {
			return true
		}
	}
	return false
}

func newMutatingRuntimeProbeHandler(t *testing.T, methodName string, modifiers ...func(*mutatingRuntimeProbeState)) (*Handler, map[string]int, *mutatingRuntimeProbeState) {
	t.Helper()
	state := newMutatingRuntimeProbeState(t, methodName)
	for _, modifier := range modifiers {
		modifier(state)
	}
	allHandlers := OperatorReadHandlers(state.options(t))
	calls := map[string]int{}
	handlers := map[string]MethodHandler{}
	for _, name := range approvedMutatingHTTPRuntimeMethods() {
		handler, ok := allHandlers[name]
		if !ok {
			t.Fatalf("OperatorReadHandlers missing mutating method %s", name)
		}
		name, handler := name, handler
		handlers[name] = func(ctx context.Context, req Request) (any, error) {
			calls[name]++
			return handler(ctx, req)
		}
	}
	return testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: handlers}), calls, state
}

type mutatingRuntimeProbeState struct {
	method              string
	now                 time.Time
	idempotency         *mutatingProbeIdempotencyStore
	runs                *fakeRunReadStore
	observability       *fakeObservabilityReadStore
	events              *mutatingProbeEventPublisher
	runFork             *mutatingProbeRunForkExecutor
	runForkAvailability *recordingRunForkAvailability
	runControl          *mutatingProbeRunControl
	agentControl        *mutatingProbeAgentControl
	runtimeIngress      *mutatingProbeRuntimeIngress
	mailbox             *mutatingProbeMailboxStore
	bundleCatalog       *mutatingProbeBundleCatalog
	forks               *fakeConversationForkLifecycleStore
	nuke                *recordingRuntimeNukeOwners
	bundleDelete        *recordingBundleDeleteExecutor
	effects             int
}

func newMutatingRuntimeProbeState(t *testing.T, methodName string) *mutatingRuntimeProbeState {
	t.Helper()
	now := time.Unix(1700000000, 0).UTC()
	runID := "00000000-0000-0000-0000-000000000101"
	otherRunID := "00000000-0000-0000-0000-000000000102"
	state := &mutatingRuntimeProbeState{
		method:      methodName,
		now:         now,
		idempotency: newMutatingProbeIdempotencyStore(),
		runs: &fakeRunReadStore{
			headers: map[string]store.RunHeader{
				runID: {RunID: runID, Status: "running", TriggerEventType: "scan.requested", TriggerEventID: "evt-1", StartedAt: now},
			},
		},
		observability: &fakeObservabilityReadStore{events: map[string]store.OperatorEventFull{}},
		runControl:    &mutatingProbeRunControl{errs: map[string]error{}},
		agentControl:  &mutatingProbeAgentControl{errs: map[string]error{}},
		runtimeIngress: &mutatingProbeRuntimeIngress{
			errs: map[string]error{},
		},
		nuke: newRecordingRuntimeNukeOwners(),
		bundleDelete: &recordingBundleDeleteExecutor{
			bundleHash: runStartTestBundleHash,
		},
	}
	state.observability.events["evt-1"] = mutatingProbeOriginalEvent("evt-1", []string{"agent-a"}, eventReplayStatusDelivered)
	state.events = &mutatingProbeEventPublisher{state: state}
	state.runFork = &mutatingProbeRunForkExecutor{state: state}
	state.runForkAvailability = &recordingRunForkAvailability{
		rows: map[string]runbundle.Availability{
			runID:                  runForkAvailable(runID, runStartTestBundleHash),
			otherRunID:             runForkAvailable(otherRunID, runStartTestBundleHash),
			runForkTestSourceRunID: runForkAvailable(runForkTestSourceRunID, runForkTestBundleHash),
		},
	}
	state.runControl.state = state
	state.agentControl.state = state
	state.runtimeIngress.state = state
	state.mailbox = newMutatingProbeMailboxStore(state)
	state.bundleCatalog = &mutatingProbeBundleCatalog{state: state, details: map[string]store.BundleCatalogDetail{}}
	state.forks = &fakeConversationForkLifecycleStore{
		createResult: store.OperatorConversationForkSession{
			ForkID:          "00000000-0000-0000-0000-000000000301",
			SourceSessionID: "00000000-0000-0000-0000-000000000201",
			SourceRunID:     runID,
			SourceAgentID:   "agent-a",
			ForkPoint: store.ConversationForkPointDescriptor{
				Kind:       "turn",
				TurnIndex:  1,
				TurnID:     "00000000-0000-0000-0000-000000000401",
				SelectedAt: now,
			},
			CreatedBy: "token",
			CreatedAt: now,
			ExpiresAt: now.Add(store.ConversationForkLifecycleTTL),
			State:     "active",
			Turns:     []store.OperatorConversationTurn{},
		},
		prepareResult: store.ConversationForkChatPrepared{
			Fork: store.OperatorConversationForkSession{
				ForkID:          "00000000-0000-0000-0000-000000000301",
				SourceSessionID: "00000000-0000-0000-0000-000000000201",
				SourceRunID:     runID,
				SourceAgentID:   "agent-a",
				ForkPoint: store.ConversationForkPointDescriptor{
					Kind:       "turn",
					TurnIndex:  1,
					TurnID:     "00000000-0000-0000-0000-000000000401",
					SelectedAt: now,
				},
				CreatedBy: "token",
				CreatedAt: now,
				ExpiresAt: now.Add(store.ConversationForkLifecycleTTL),
				State:     "active",
				Turns:     []store.OperatorConversationTurn{},
			},
			Snapshot: store.ConversationForkSnapshot{
				ForkID:          "00000000-0000-0000-0000-000000000301",
				SourceSessionID: "00000000-0000-0000-0000-000000000201",
				SourceRunID:     runID,
				SourceAgentID:   "agent-a",
				SourceTurn: store.ConversationForkSourceTurn{
					TurnID:     "00000000-0000-0000-0000-000000000401",
					TurnIndex:  1,
					SelectedAt: now,
					CreatedAt:  now,
				},
				EntitySnapshot: []store.ConversationForkEntitySnapshot{},
				SnapshotOwner:  store.ConversationForkChatSnapshotOwner,
				CreatedAt:      now,
			},
			SandboxPolicy: store.ConversationForkSandboxPolicy{
				Owner:              store.ConversationForkChatSandboxOwner,
				ReadPolicy:         "fork_snapshot_only",
				WritePolicy:        "stub_record_only_no_live_mutation",
				SideEffectingTools: []string{"save_entity_field"},
				StubbedTools:       []string{"save_entity_field"},
			},
			AvailableTools: []string{"fork_snapshot_read_entities", "save_entity_field"},
		},
		recordResult: store.ConversationForkChatResult{
			ForkID: "00000000-0000-0000-0000-000000000301",
			Turn: store.OperatorConversationTurn{
				TurnIndex:       1,
				TurnID:          "00000000-0000-0000-0000-000000000402",
				RequestPayload:  []byte(`{"message":"inspect fork"}`),
				ResponsePayload: []byte(`{"message":"forkchat sandbox response: inspect fork"}`),
				ParseOK:         true,
			},
			Snapshot: store.ConversationForkSnapshot{
				ForkID:          "00000000-0000-0000-0000-000000000301",
				SourceSessionID: "00000000-0000-0000-0000-000000000201",
				SourceRunID:     runID,
				SourceAgentID:   "agent-a",
				SourceTurn: store.ConversationForkSourceTurn{
					TurnID:     "00000000-0000-0000-0000-000000000401",
					TurnIndex:  1,
					SelectedAt: now,
					CreatedAt:  now,
				},
				EntitySnapshot: []store.ConversationForkEntitySnapshot{},
				SnapshotOwner:  store.ConversationForkChatSnapshotOwner,
				CreatedAt:      now,
			},
			SandboxPolicy: store.ConversationForkSandboxPolicy{
				Owner:              store.ConversationForkChatSandboxOwner,
				ReadPolicy:         "fork_snapshot_only",
				WritePolicy:        "stub_record_only_no_live_mutation",
				SideEffectingTools: []string{"save_entity_field"},
				StubbedTools:       []string{"save_entity_field"},
			},
		},
		deleteResult: store.ConversationForkDeleteResult{ForkID: "00000000-0000-0000-0000-000000000301", Deleted: true},
		recordEffect: state.recordEffect,
	}
	return state
}

func (s *mutatingRuntimeProbeState) options(t *testing.T) OperatorReadOptions {
	t.Helper()
	source := semanticview.Wrap(runStartTestBundle("scan.requested"))
	return OperatorReadOptions{
		RepoRoot:          t.TempDir(),
		PlatformSpecPath:  testBundleRegistrationPlatformSpec(t),
		Now:               func() time.Time { return s.now },
		Ready:             func() bool { return true },
		Database:          fakePinger{},
		Runs:              s.runs,
		Observability:     s.observability,
		AgentControl:      s.agentControl,
		ConversationForks: s.forks,
		ForkChatExecutor: &fakeForkChatExecutor{result: store.ConversationForkChatExecution{
			AssistantMessage: "forkchat sandbox response: inspect fork",
		}},
		Mailbox:               s.mailbox,
		BundleCatalog:         s.bundleCatalog,
		Idempotency:           s.idempotency,
		Events:                s.events,
		RunBundleContext:      s.runForkAvailability,
		RunForkAvailability:   s.runForkAvailability,
		RunFork:               s.runFork,
		RunControl:            s.runControl,
		RuntimeIngress:        s.runtimeIngress,
		ResetCoordinator:      s.nuke,
		ResetQuiescer:         recordingRuntimeNukeQuiescer{s.nuke},
		ResetCleaner:          recordingRuntimeNukeCleaner{s.nuke},
		ResetContainers:       recordingRuntimeNukeContainerStopper{s.nuke},
		BundleDelete:          s.bundleDelete,
		Source:                source,
		MailboxApprovalRoutes: map[string]string{"review_request": "mailbox.item_decided"},
		Bundle: runtimecontracts.BundleIdentity{
			WorkflowName:    "review",
			WorkflowVersion: "1.0.0",
			Fingerprint:     runStartTestFingerprint,
		},
	}
}

func (s *mutatingRuntimeProbeState) recordEffect() {
	s.effects++
}

func (s *mutatingRuntimeProbeState) effectCount() int {
	return s.effects + len(s.nuke.calls) + len(s.bundleDelete.calls)
}

type recordingBundleDeleteExecutor struct {
	calls      []bundledelete.Request
	err        error
	bundleHash string
}

func (e *recordingBundleDeleteExecutor) Execute(_ context.Context, req bundledelete.Request) (bundledelete.Result, error) {
	e.calls = append(e.calls, req)
	if e.err != nil {
		return bundledelete.Result{}, e.err
	}
	bundleHash := strings.TrimSpace(req.BundleHash)
	if bundleHash == "" {
		bundleHash = strings.TrimSpace(e.bundleHash)
	}
	status := "completed"
	if req.DryRun {
		status = "dry_run"
	}
	activeRunsStopped := 0
	deliveriesCancelled := 0
	containersStopped := 0
	var activeRuns []bundledelete.RunRef
	var nonActiveRuns []bundledelete.RunRef
	if req.Force {
		activeRunsStopped = 1
		deliveriesCancelled = 1
		containersStopped = 1
		activeRuns = []bundledelete.RunRef{{
			RunID:        "00000000-0000-0000-0000-000000000101",
			Status:       "running",
			BundleHash:   bundleHash,
			BundleSource: "persisted",
		}}
	} else {
		nonActiveRuns = []bundledelete.RunRef{{
			RunID:        "00000000-0000-0000-0000-000000000102",
			Status:       "completed",
			BundleHash:   bundleHash,
			BundleSource: "persisted",
		}}
	}
	return bundledelete.Result{
		OK:                  true,
		Status:              status,
		OperationName:       bundledelete.DefaultOperationName,
		BundleHash:          bundleHash,
		Force:               req.Force,
		Deleted:             !req.DryRun,
		DryRun:              req.DryRun,
		ActiveRunsStopped:   activeRunsStopped,
		DeliveriesCancelled: deliveriesCancelled,
		ContainersStopped:   containersStopped,
		Plan: bundledelete.Plan{
			BundleHash:    bundleHash,
			ActiveRuns:    activeRuns,
			NonActiveRuns: nonActiveRuns,
			AffectedRuns:  append(activeRuns, nonActiveRuns...),
		},
	}, nil
}

type mutatingProbeIdempotencyStore struct {
	calls   int
	records map[string]store.APIIdempotencyCompletion
	hashes  map[string]string
}

func newMutatingProbeIdempotencyStore() *mutatingProbeIdempotencyStore {
	return &mutatingProbeIdempotencyStore{
		records: map[string]store.APIIdempotencyCompletion{},
		hashes:  map[string]string{},
	}
}

func (s *mutatingProbeIdempotencyStore) WithAPIIdempotency(
	ctx context.Context,
	req store.APIIdempotencyRequest,
	execute func(context.Context) (store.APIIdempotencyCompletion, error),
) (store.APIIdempotencyCompletion, bool, error) {
	s.calls++
	if strings.TrimSpace(req.IdempotencyKey) == "" {
		completion, err := execute(ctx)
		return completion, false, err
	}
	key := strings.Join([]string{req.Method, req.ActorTokenID, strings.TrimSpace(req.IdempotencyKey)}, "|")
	if completion, ok := s.records[key]; ok {
		if s.hashes[key] != req.RequestHash {
			return store.APIIdempotencyCompletion{}, false, &store.APIIdempotencyConflictError{
				OriginalRequestHash:    s.hashes[key],
				ConflictingRequestHash: req.RequestHash,
				Method:                 req.Method,
				ResourceID:             completion.ResourceID,
			}
		}
		copied := completion
		copied.Response = append(json.RawMessage(nil), completion.Response...)
		return copied, true, nil
	}
	completion, err := execute(ctx)
	if err != nil {
		return store.APIIdempotencyCompletion{}, false, err
	}
	copied := completion
	copied.Response = append(json.RawMessage(nil), completion.Response...)
	s.records[key] = copied
	s.hashes[key] = req.RequestHash
	return completion, false, nil
}

type mutatingProbeBundleCatalog struct {
	state    *mutatingRuntimeProbeState
	details  map[string]store.BundleCatalogDetail
	conflict bool
}

func (s *mutatingProbeBundleCatalog) ListBundleCatalog(context.Context, store.BundleCatalogListOptions) (store.BundleCatalogListResult, error) {
	return store.BundleCatalogListResult{Bundles: []store.BundleCatalogSummary{}}, nil
}

func (s *mutatingProbeBundleCatalog) LoadBundleCatalog(_ context.Context, bundleHash string) (store.BundleCatalogDetail, error) {
	detail, ok := s.details[strings.TrimSpace(bundleHash)]
	if !ok {
		return store.BundleCatalogDetail{}, store.ErrBundleNotFound
	}
	return detail, nil
}

func (s *mutatingProbeBundleCatalog) ListBundleCatalogAgents(context.Context, string) (store.BundleCatalogAgentsResult, error) {
	return store.BundleCatalogAgentsResult{Agents: []store.BundleCatalogAgentDefinition{}}, nil
}

func (s *mutatingProbeBundleCatalog) UpsertBundleCatalog(_ context.Context, req store.BundleCatalogUpsert) (store.BundleCatalogUpsertResult, error) {
	if s.conflict {
		return store.BundleCatalogUpsertResult{}, &store.BundleCatalogConflictError{BundleHash: req.BundleHash}
	}
	if s.details == nil {
		s.details = map[string]store.BundleCatalogDetail{}
	}
	_, exists := s.details[req.BundleHash]
	if !exists {
		s.state.recordEffect()
		s.details[req.BundleHash] = store.BundleCatalogDetail{
			BundleHash:    req.BundleHash,
			ContentYAML:   req.ContentYAML,
			ParsedJSON:    req.ParsedJSON,
			Metadata:      req.Metadata,
			AgentCount:    1,
			HasData:       len(req.DataBlob) > 0,
			DataSizeBytes: int64(len(req.DataBlob)),
			IngestedAt:    s.state.now,
		}
	}
	return store.BundleCatalogUpsertResult{Detail: s.details[req.BundleHash], Registered: !exists}, nil
}

var _ BundleCatalogReadStore = (*mutatingProbeBundleCatalog)(nil)
var _ BundleCatalogRegisterStore = (*mutatingProbeBundleCatalog)(nil)

type mutatingProbeEventPublisher struct {
	state             *mutatingRuntimeProbeState
	publishErr        error
	directErr         error
	checkErr          error
	missingRecipients []string
}

func (p *mutatingProbeEventPublisher) Publish(_ context.Context, evt events.Event) error {
	if p.publishErr != nil {
		return p.publishErr
	}
	p.state.recordEffect()
	p.state.storeEvent(evt, nil)
	return nil
}

func (p *mutatingProbeEventPublisher) PublishAcknowledged(ctx context.Context, evt events.Event) error {
	return p.Publish(ctx, evt)
}

func (p *mutatingProbeEventPublisher) WithBundleFingerprint(ctx context.Context) context.Context {
	return runtimecorrelation.WithBundleSourceFact(ctx, runStartTestBundleSourceFact())
}

func (p *mutatingProbeEventPublisher) PublishInMutation(ctx context.Context, evt events.Event) error {
	return p.Publish(ctx, evt)
}

func (p *mutatingProbeEventPublisher) PublishDirect(_ context.Context, evt events.Event, recipients []string) error {
	if p.directErr != nil {
		return p.directErr
	}
	p.state.recordEffect()
	deliveries := make([]store.OperatorEventDelivery, 0, len(recipients))
	for i, recipient := range recipients {
		deliveries = append(deliveries, store.OperatorEventDelivery{
			DeliveryID:     "delivery-" + recipient,
			SubscriberType: eventReplaySubscriberTypeAgent,
			SubscriberID:   recipient,
			SessionID:      "sess-" + recipient,
			Status:         eventReplayStatusDelivered,
			RetryCount:     i,
		})
	}
	p.state.storeEvent(evt, deliveries)
	return nil
}

func (p *mutatingProbeEventPublisher) CheckDirectRecipients(_ context.Context, _ events.Event, recipients []string) (runtimebus.DirectRecipientStatus, error) {
	status := runtimebus.DirectRecipientStatus{Requested: append([]string(nil), recipients...)}
	if p.checkErr != nil {
		return status, p.checkErr
	}
	if len(p.missingRecipients) > 0 {
		status.Missing = append([]string(nil), p.missingRecipients...)
		return status, nil
	}
	status.Recipients = append([]string(nil), recipients...)
	return status, nil
}

type mutatingProbeRunForkExecutor struct {
	state *mutatingRuntimeProbeState
	err   error
}

func (e *mutatingProbeRunForkExecutor) ExecuteRunFork(_ context.Context, req RunForkExecutionRequest) (RunForkExecutionResult, error) {
	if e.err != nil {
		return RunForkExecutionResult{}, e.err
	}
	e.state.recordEffect()
	return RunForkExecutionResult{
		Owner:              "runtime.run_fork.selected_contract_execution",
		SourceRunID:        strings.TrimSpace(req.SourceRunID),
		ForkRunID:          runForkTestForkRunID,
		ForkEventID:        strings.TrimSpace(req.ForkEventID),
		ForkRunStatus:      "running",
		BundleHash:         strings.TrimSpace(req.BundleHash),
		ExecutedEventCount: 1,
	}, nil
}

func (s *mutatingRuntimeProbeState) storeEvent(evt events.Event, deliveries []store.OperatorEventDelivery) {
	payload := map[string]any{}
	if len(evt.Payload()) > 0 {
		_ = json.Unmarshal(evt.Payload(), &payload)
	}
	s.observability.events[evt.ID()] = store.OperatorEventFull{
		EventID:       evt.ID(),
		EventName:     strings.TrimSpace(string(evt.Type())),
		EntityID:      evt.EntityID(),
		RunID:         evt.RunID(),
		SourceEventID: strings.TrimSpace(evt.ParentEventID()),
		CreatedAt:     evt.CreatedAt().UTC(),
		Source:        evt.SourceAgent(),
		Payload:       payload,
		Deliveries:    deliveries,
	}
}

func mutatingProbeOriginalEvent(eventID string, subscribers []string, status string) store.OperatorEventFull {
	deliveries := make([]store.OperatorEventDelivery, 0, len(subscribers))
	for _, subscriber := range subscribers {
		deliveries = append(deliveries, store.OperatorEventDelivery{
			DeliveryID:     "original-" + subscriber,
			SubscriberType: eventReplaySubscriberTypeAgent,
			SubscriberID:   subscriber,
			SessionID:      "sess-" + subscriber,
			Status:         status,
		})
	}
	return store.OperatorEventFull{
		EventID:    eventID,
		EventName:  "scan.requested",
		EntityID:   "entity-1",
		RunID:      "00000000-0000-0000-0000-000000000101",
		CreatedAt:  time.Unix(1700000000, 0).UTC(),
		Source:     "origin-agent",
		Payload:    map[string]any{"topic": "medicine"},
		Deliveries: deliveries,
	}
}

type mutatingProbeRunControl struct {
	state *mutatingRuntimeProbeState
	errs  map[string]error
}

func (c *mutatingProbeRunControl) Stop(_ context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.TransitionResult, error) {
	return c.transition("stop", req)
}

func (c *mutatingProbeRunControl) Pause(_ context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.TransitionResult, error) {
	return c.transition("pause", req)
}

func (c *mutatingProbeRunControl) Continue(_ context.Context, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.TransitionResult, error) {
	return c.transition("continue", req)
}

func (c *mutatingProbeRunControl) transition(action string, req runtimeruncontrol.TransitionRequest) (runtimeruncontrol.TransitionResult, error) {
	if err := c.errs[action]; err != nil {
		return runtimeruncontrol.TransitionResult{}, err
	}
	c.state.recordEffect()
	return runtimeruncontrol.TransitionResult{RunID: req.RunID, Status: action}, nil
}

type mutatingProbeAgentControl struct {
	state *mutatingRuntimeProbeState
	errs  map[string]error
}

func (c *mutatingProbeAgentControl) SendDirective(_ context.Context, req runtimeagentcontrol.SendDirectiveRequest) (runtimeagentcontrol.SendDirectiveResult, error) {
	if err := c.errs["agent.send_directive"]; err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	c.state.recordEffect()
	runID := strings.TrimSpace(req.RunID)
	if runID == "" {
		runID = "00000000-0000-0000-0000-000000000201"
	}
	return runtimeagentcontrol.SendDirectiveResult{
		AgentID:            req.AgentID,
		Response:           "accepted",
		RunID:              runID,
		RunIDResolution:    runtimeagentcontrol.RunResolutionSpecified,
		DirectiveEventID:   "00000000-0000-0000-0000-000000000202",
		DirectiveEventType: runtimeagentcontrol.DirectiveEventType,
	}, nil
}

func (c *mutatingProbeAgentControl) Restart(_ context.Context, req runtimeagentcontrol.RestartRequest) (runtimeagentcontrol.RestartResult, error) {
	if err := c.errs["agent.restart"]; err != nil {
		return runtimeagentcontrol.RestartResult{}, err
	}
	c.state.recordEffect()
	return runtimeagentcontrol.RestartResult{AgentID: req.AgentID}, nil
}

func (c *mutatingProbeAgentControl) ReplayBacklog(_ context.Context, req runtimeagentcontrol.ReplayBacklogRequest) (runtimeagentcontrol.ReplayBacklogResult, error) {
	if err := c.errs["agent.replay_backlog"]; err != nil {
		return runtimeagentcontrol.ReplayBacklogResult{}, err
	}
	c.state.recordEffect()
	return runtimeagentcontrol.ReplayBacklogResult{AgentID: req.AgentID, ReplayedCount: 3}, nil
}

type mutatingProbeRuntimeIngress struct {
	state *mutatingRuntimeProbeState
	errs  map[string]error
}

func (c *mutatingProbeRuntimeIngress) Pause(_ context.Context, _ runtimeingress.TransitionRequest) (runtimeingress.TransitionResult, error) {
	if err := c.errs["runtime.pause"]; err != nil {
		return runtimeingress.TransitionResult{}, err
	}
	c.state.recordEffect()
	return runtimeingress.TransitionResult{Status: runtimeingress.StatusPaused, TransitionID: "pause-1"}, nil
}

func (c *mutatingProbeRuntimeIngress) Resume(_ context.Context, _ runtimeingress.TransitionRequest) (runtimeingress.TransitionResult, error) {
	if err := c.errs["runtime.resume"]; err != nil {
		return runtimeingress.TransitionResult{}, err
	}
	c.state.recordEffect()
	return runtimeingress.TransitionResult{Status: runtimeingress.StatusRunning, TransitionID: "resume-1"}, nil
}

type mutatingProbeMailboxStore struct {
	state       *mutatingRuntimeProbeState
	item        store.MailboxV1Item
	decisionErr error
}

func newMutatingProbeMailboxStore(state *mutatingRuntimeProbeState) *mutatingProbeMailboxStore {
	return &mutatingProbeMailboxStore{
		state: state,
		item: store.MailboxV1Item{
			MailboxID:     "mailbox-1",
			Type:          "review_request",
			Status:        "pending",
			Priority:      "high",
			SourceEventID: "evt-1",
			SourceFlow:    "review/primary",
			Payload:       map[string]any{"title": "probe"},
			CreatedAt:     state.now.Format(time.RFC3339Nano),
		},
	}
}

func (s *mutatingProbeMailboxStore) ListV1MailboxItems(context.Context, store.MailboxV1ListOptions) ([]store.MailboxV1Item, string, error) {
	return []store.MailboxV1Item{s.item}, "", nil
}

func (s *mutatingProbeMailboxStore) GetV1MailboxItem(_ context.Context, mailboxID string) (store.MailboxV1ItemDetail, error) {
	if strings.TrimSpace(mailboxID) != s.item.MailboxID {
		return store.MailboxV1ItemDetail{}, store.ErrMailboxV1NotFound
	}
	return store.MailboxV1ItemDetail{Item: s.item, Payload: s.item.Payload}, nil
}

func (s *mutatingProbeMailboxStore) DecideV1MailboxItem(ctx context.Context, decision store.MailboxV1DecisionRequest) (store.MailboxV1DecisionOutcome, error) {
	execute := func(context.Context) (store.APIIdempotencyCompletion, error) {
		if s.decisionErr != nil {
			return store.APIIdempotencyCompletion{}, s.decisionErr
		}
		s.state.recordEffect()
		status := "decided"
		if strings.TrimSpace(decision.Action) == "deferred" {
			status = "deferred"
		}
		result := store.MailboxV1DecisionResult{
			OK:                true,
			MailboxDecisionID: "decision-" + strings.TrimSpace(decision.Action),
			Status:            status,
		}
		if decision.Action == "approved" && strings.TrimSpace(decision.ApprovalEventType) != "" && decision.ApprovalEventPublish != nil {
			eventID := "mailbox-event-1"
			result.DownstreamEventID = eventID
			result.DownstreamEventName = strings.TrimSpace(decision.ApprovalEventType)
			subscribers := append([]string(nil), decision.ApprovalEventSubscribers...)
			if subscribers == nil {
				subscribers = []string{}
			}
			result.DownstreamSubscribers = &subscribers
			result.DownstreamSubscriberSource = strings.TrimSpace(decision.ApprovalEventSubscriberSource)
			if err := decision.ApprovalEventPublish(ctx, eventtest.Projection(eventID,
				events.EventType(decision.ApprovalEventType),
				"mailbox", "", json.RawMessage(`{"mailbox_id":"mailbox-1"}`), 0, "", "", events.EventEnvelope{}, decision.Now)); err != nil {
				return store.APIIdempotencyCompletion{}, err
			}
		}
		raw, err := json.Marshal(result)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		return store.APIIdempotencyCompletion{ResourceID: decision.MailboxID, Response: raw}, nil
	}
	var completion store.APIIdempotencyCompletion
	var replay bool
	var err error
	if decision.Idempotency != nil {
		completion, replay, err = s.state.idempotency.WithAPIIdempotency(ctx, *decision.Idempotency, execute)
	} else {
		completion, err = execute(ctx)
	}
	if err != nil {
		return store.MailboxV1DecisionOutcome{}, err
	}
	var result store.MailboxV1DecisionResult
	if err := json.Unmarshal(completion.Response, &result); err != nil {
		return store.MailboxV1DecisionOutcome{}, err
	}
	return store.MailboxV1DecisionOutcome{Result: result, Replayed: replay}, nil
}

func callMutatingProbeRPC(t *testing.T, handler *Handler, methodName string, params map[string]any, authHeader string) (int, rpcResponse, string) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      mutatingProbeID(methodName),
		"method":  methodName,
		"params":  params,
	})
	if err != nil {
		t.Fatalf("marshal probe request: %v", err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/rpc", bytes.NewReader(raw))
	if strings.TrimSpace(authHeader) != "" {
		req.Header.Set("Authorization", authHeader)
	}
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		return rec.Code, rpcResponse{}, rec.Body.String()
	}
	var resp rpcResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %s RPC response: %v body=%s", methodName, err, rec.Body.String())
	}
	return rec.Code, resp, rec.Body.String()
}

func assertMutatingProbeSuccess(t *testing.T, methodName string, resp rpcResponse, resultKeys []string) {
	t.Helper()
	if resp.JSONRPC != jsonRPCVersion {
		t.Fatalf("%s jsonrpc = %q, want %q", methodName, resp.JSONRPC, jsonRPCVersion)
	}
	if resp.ID != mutatingProbeID(methodName) {
		t.Fatalf("%s id = %#v, want %q", methodName, resp.ID, mutatingProbeID(methodName))
	}
	if resp.Error != nil {
		t.Fatalf("%s error = %#v, want success", methodName, resp.Error)
	}
	result := asMap(t, resp.Result)
	for _, key := range resultKeys {
		if _, ok := result[key]; !ok {
			t.Fatalf("%s result missing top-level key %q: %#v", methodName, key, result)
		}
	}
}

func assertMutatingProbeInvalidParams(t *testing.T, methodName string, resp rpcResponse, field string) {
	t.Helper()
	if resp.JSONRPC != jsonRPCVersion {
		t.Fatalf("%s jsonrpc = %q, want %q", methodName, resp.JSONRPC, jsonRPCVersion)
	}
	if resp.ID != mutatingProbeID(methodName) {
		t.Fatalf("%s id = %#v, want %q", methodName, resp.ID, mutatingProbeID(methodName))
	}
	if resp.Error == nil {
		t.Fatalf("%s error = nil, want invalid params for %s", methodName, field)
	}
	if resp.Error.Code != codeInvalidParams {
		t.Fatalf("%s error code = %d, want %d body=%#v", methodName, resp.Error.Code, codeInvalidParams, resp.Error)
	}
	data := asMap(t, resp.Error.Data)
	details := asMap(t, data["details"])
	if details["field"] != field {
		t.Fatalf("%s invalid params field = %#v, want %s details=%#v", methodName, details["field"], field, details)
	}
	if _, ok := data["correlation_id"].(string); !ok {
		t.Fatalf("%s invalid params missing correlation_id: %#v", methodName, data)
	}
}

func assertMutatingProbeApplicationError(t *testing.T, registry *Registry, methodName string, resp rpcResponse, code string) {
	t.Helper()
	if resp.JSONRPC != jsonRPCVersion {
		t.Fatalf("%s jsonrpc = %q, want %q", methodName, resp.JSONRPC, jsonRPCVersion)
	}
	if resp.ID != mutatingProbeID(methodName) {
		t.Fatalf("%s id = %#v, want %q", methodName, resp.ID, mutatingProbeID(methodName))
	}
	if resp.Error == nil {
		t.Fatalf("%s error = nil, want %s", methodName, code)
	}
	numeric, ok := registry.ApplicationErrorCode(code)
	if !ok {
		t.Fatalf("registry missing application error code %s", code)
	}
	if resp.Error.Code != numeric {
		t.Fatalf("%s error code = %d, want generated numeric %d for %s", methodName, resp.Error.Code, numeric, code)
	}
	data := asMap(t, resp.Error.Data)
	if data["code"] != code {
		t.Fatalf("%s data.code = %#v, want %s", methodName, data["code"], code)
	}
	if _, ok := data["details"].(map[string]any); !ok && data["details"] != nil {
		t.Fatalf("%s data.details = %#v, want object or null", methodName, data["details"])
	}
	if _, ok := data["retryable"].(bool); !ok {
		t.Fatalf("%s data.retryable = %#v, want bool", methodName, data["retryable"])
	}
	if _, ok := data["correlation_id"].(string); !ok {
		t.Fatalf("%s data.correlation_id = %#v, want string", methodName, data["correlation_id"])
	}
}

func assertMutatingProbeNoExecution(t *testing.T, methodName string, calls map[string]int, state *mutatingRuntimeProbeState, reason string) {
	t.Helper()
	if calls[methodName] != 0 {
		t.Fatalf("%s handler calls = %d, want 0 for %s", methodName, calls[methodName], reason)
	}
	if state.idempotency.calls != 0 {
		t.Fatalf("%s idempotency calls = %d, want 0 for %s", methodName, state.idempotency.calls, reason)
	}
	if got := state.effectCount(); got != 0 {
		t.Fatalf("%s side effects = %d, want 0 for %s", methodName, got, reason)
	}
}

func mutatingProbeParamsWithIdempotency(params map[string]any, key string) map[string]any {
	out := cloneProbeParams(params)
	if _, ok := out["idempotency_key"]; !ok {
		out["idempotency_key"] = key
	}
	return out
}

func mutatingProbeConflictParams(fixture mutatingHTTPRuntimeFixture, key string) map[string]any {
	out := cloneProbeParams(fixture.ConflictParams)
	if fixture.TrimEquivalentConflictKeyValue {
		out["idempotency_key"] = " " + key + " "
		return out
	}
	if _, ok := out["idempotency_key"]; !ok {
		out["idempotency_key"] = key
	}
	return out
}

func mergeProbeParams(base map[string]any, override map[string]any) map[string]any {
	out := cloneProbeParams(base)
	for key, value := range override {
		out[key] = value
	}
	return out
}

func mutatingProbeID(methodName string) string {
	return "mutating-probe-" + strings.ReplaceAll(methodName, ".", "-")
}

func sortedMutatingProbeFixtureMethods(fixtures map[string]mutatingHTTPRuntimeFixture) []string {
	methods := make([]string, 0, len(fixtures))
	for methodName := range fixtures {
		methods = append(methods, methodName)
	}
	sort.Strings(methods)
	return methods
}

var _ APIIdempotencyStore = (*mutatingProbeIdempotencyStore)(nil)
var _ EventPublisher = (*mutatingProbeEventPublisher)(nil)
var _ EventMutationPublisher = (*mutatingProbeEventPublisher)(nil)
var _ eventReplayPublisher = (*mutatingProbeEventPublisher)(nil)
var _ RunControlController = (*mutatingProbeRunControl)(nil)
var _ AgentControlController = (*mutatingProbeAgentControl)(nil)
var _ RuntimeIngressController = (*mutatingProbeRuntimeIngress)(nil)
var _ MailboxAPIStore = (*mutatingProbeMailboxStore)(nil)
