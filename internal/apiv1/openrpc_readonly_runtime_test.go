package apiv1

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/bundlecatalog"
	"github.com/division-sh/swarm/internal/mailbox"
	operatorread "github.com/division-sh/swarm/internal/operatorread"

	"github.com/division-sh/swarm/internal/apispec"
	"github.com/division-sh/swarm/internal/events"
	runtimeagentintent "github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
)

const readOnlyRuntimeProbeTestName = "TestOpenRPCReadOnlyHTTPRuntimeProbes"
const readOnlyProbeBundleHash = "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const readOnlyProbeMissingBundleHash = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func TestOpenRPCReadOnlyHTTPRuntimeProbes(t *testing.T) {
	root := repoRoot(t)
	api := loadComplianceAPISpec(t, root)
	openRPC, _ := loadComplianceOpenRPC(t, complianceOpenRPCPath(root))
	errorSchemaValidator := newOpenRPCResultSchemaValidator(t, openRPC)
	matrix := loadComplianceMatrix(t, filepath.Join(root, "internal", "apiv1", "testdata", "openrpc_compliance_matrix.yaml"))

	methods := readOnlyHTTPRuntimeMethods(t, api, openRPC, matrix)
	assertStringList(t, "read-only HTTP runtime method set", methods, approvedReadOnlyHTTPRuntimeMethods())

	fixtures := readOnlyHTTPRuntimeFixtures()
	assertStringList(t, "read-only HTTP runtime fixture methods", sortedProbeFixtureMethods(fixtures), methods)
	assertReadOnlyRuntimeMatrixProofRefs(t, api, matrix, methods)

	t.Run("classification excludes sibling classes", func(t *testing.T) {
		readOnly := complianceStringSet(methods)
		for _, sibling := range []string{"event.publish", "mailbox.decide", "event.subscribe", "rpc.unsubscribe"} {
			if _, ok := readOnly[sibling]; ok {
				t.Fatalf("%s classified into read-only HTTP runtime probes; sibling methods belong to their approved probe class", sibling)
			}
		}
	})

	for _, methodName := range methods {
		methodName := methodName
		fixture := fixtures[methodName]
		method := api.MethodCatalog[methodName]

		t.Run(methodName+"/success_smoke", func(t *testing.T) {
			handler, calls := newReadOnlyRuntimeProbeHandler(t, readOnlyRuntimeProbeOptions(t))
			status, resp, body := callReadOnlyProbeRPC(t, handler, methodName, fixture.Params, "Bearer "+testToken)
			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200 body=%s", status, body)
			}
			if calls[methodName] != 1 {
				t.Fatalf("%s handler calls = %d, want 1", methodName, calls[methodName])
			}
			assertReadOnlyProbeSuccess(t, methodName, resp, fixture.ResultKeys)
		})

		t.Run(methodName+"/unknown_params_key", func(t *testing.T) {
			handler, calls := newReadOnlyRuntimeProbeHandler(t, readOnlyRuntimeProbeOptions(t))
			params := cloneProbeParams(fixture.Params)
			params["_unexpected"] = true
			status, resp, body := callReadOnlyProbeRPC(t, handler, methodName, params, "Bearer "+testToken)
			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200 body=%s", status, body)
			}
			assertReadOnlyProbeInvalidParams(t, methodName, resp, "_unexpected")
			if calls[methodName] != 0 {
				t.Fatalf("%s handler calls = %d, want 0 for params validation failure", methodName, calls[methodName])
			}
		})

		for _, paramName := range requiredParamNames(method) {
			paramName := paramName
			t.Run(methodName+"/missing_required_"+paramName, func(t *testing.T) {
				handler, calls := newReadOnlyRuntimeProbeHandler(t, readOnlyRuntimeProbeOptions(t))
				params := cloneProbeParams(fixture.Params)
				delete(params, paramName)
				status, resp, body := callReadOnlyProbeRPC(t, handler, methodName, params, "Bearer "+testToken)
				if status != http.StatusOK {
					t.Fatalf("status = %d, want 200 body=%s", status, body)
				}
				assertReadOnlyProbeInvalidParams(t, methodName, resp, paramName)
				if calls[methodName] != 0 {
					t.Fatalf("%s handler calls = %d, want 0 for required param failure", methodName, calls[methodName])
				}
			})
		}

		t.Run(methodName+"/missing_auth", func(t *testing.T) {
			handler, calls := newReadOnlyRuntimeProbeHandler(t, readOnlyRuntimeProbeOptions(t))
			status, _, body := callReadOnlyProbeRPC(t, handler, methodName, fixture.Params, "")
			if status != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401 body=%s", status, body)
			}
			if calls[methodName] != 0 {
				t.Fatalf("%s handler calls = %d, want 0 for auth failure", methodName, calls[methodName])
			}
		})

		t.Run(methodName+"/invalid_auth", func(t *testing.T) {
			handler, calls := newReadOnlyRuntimeProbeHandler(t, readOnlyRuntimeProbeOptions(t))
			status, _, body := callReadOnlyProbeRPC(t, handler, methodName, fixture.Params, "Bearer wrong")
			if status != http.StatusUnauthorized {
				t.Fatalf("status = %d, want 401 body=%s", status, body)
			}
			if calls[methodName] != 0 {
				t.Fatalf("%s handler calls = %d, want 0 for auth failure", methodName, calls[methodName])
			}
		})
	}

	for _, probe := range readOnlyHTTPRuntimeErrorProbes() {
		probe := probe
		t.Run(probe.Method+"/"+probe.Code, func(t *testing.T) {
			method := api.MethodCatalog[probe.Method]
			if _, ok := complianceStringSet(method.Errors)[probe.Code]; !ok {
				t.Fatalf("%s error probe uses %s, absent from declared errors %v", probe.Method, probe.Code, method.Errors)
			}
			handler, calls := newReadOnlyRuntimeProbeHandler(t, probe.Options(t))
			status, resp, body := callReadOnlyProbeRPC(t, handler, probe.Method, probe.Params, "Bearer "+testToken)
			if status != http.StatusOK {
				t.Fatalf("status = %d, want 200 body=%s", status, body)
			}
			if calls[probe.Method] != 1 {
				t.Fatalf("%s handler calls = %d, want 1 for declared application error", probe.Method, calls[probe.Method])
			}
			assertReadOnlyProbeApplicationError(t, testRegistry(t), errorSchemaValidator, probe.Method, resp, probe.Code)
		})
	}
}

func assertReadOnlyRuntimeMatrixProofRefs(t *testing.T, api *apispec.APISpecification, matrix openRPCComplianceMatrix, methods []string) {
	t.Helper()
	readOnlyMethods := complianceStringSet(methods)
	rows := map[string]openRPCMethodMatrix{}
	for _, row := range matrix.Methods {
		rows[row.Method] = row
		if _, ok := readOnlyMethods[row.Method]; !ok && rowHasReadOnlyRuntimeProof(row) {
			t.Fatalf("%s has %s proof_ref but is outside the approved read-only HTTP runtime probe set", row.Method, readOnlyRuntimeProbeTestName)
		}
	}

	for _, methodName := range methods {
		row, ok := rows[methodName]
		if !ok {
			t.Fatalf("%s missing from compliance matrix", methodName)
		}
		assertEvidenceHasReadOnlyRuntimeProof(t, methodName, "happy_path", row.HappyPath)
		assertEvidenceHasReadOnlyRuntimeProof(t, methodName, "unknown_top_level_param_validation", row.UnknownTopLevelParamValidation)
		assertEvidenceHasReadOnlyRuntimeProof(t, methodName, "auth", row.Auth)
		assertEvidenceHasReadOnlyRuntimeProof(t, methodName, "result_schema", row.ResultSchema)
		if len(requiredParamNames(api.MethodCatalog[methodName])) > 0 {
			assertEvidenceHasReadOnlyRuntimeProof(t, methodName, "required_param_validation", row.RequiredParamValidation)
		}
		if len(api.MethodCatalog[methodName].Errors) > 0 {
			assertEvidenceHasReadOnlyRuntimeProof(t, methodName, "declared_error_tests", row.DeclaredErrorTests)
		}
	}
}

func assertEvidenceHasReadOnlyRuntimeProof(t *testing.T, methodName, field string, evidence complianceEvidence) {
	t.Helper()
	if !evidenceHasGoTest(evidence, readOnlyRuntimeProbeTestName) {
		t.Fatalf("%s %s missing go_test proof_ref %s", methodName, field, readOnlyRuntimeProbeTestName)
	}
}

func rowHasReadOnlyRuntimeProof(row openRPCMethodMatrix) bool {
	for _, evidence := range complianceEvidenceFields(row) {
		if evidenceHasGoTest(evidence.evidence, readOnlyRuntimeProbeTestName) {
			return true
		}
	}
	return false
}

func evidenceHasGoTest(evidence complianceEvidence, name string) bool {
	for _, ref := range evidence.ProofRefs {
		if ref.Kind == "go_test" && ref.Name == name {
			return true
		}
	}
	return false
}

type readOnlyHTTPRuntimeFixture struct {
	Params     map[string]any
	ResultKeys []string
}

type readOnlyHTTPRuntimeErrorProbe struct {
	Method  string
	Params  map[string]any
	Code    string
	Options func(*testing.T) testOperatorCapabilities
}

func readOnlyHTTPRuntimeMethods(t *testing.T, api *apispec.APISpecification, openRPC apispec.OpenRPCDocument, matrix openRPCComplianceMatrix) []string {
	t.Helper()
	openRPCMethods := map[string]struct{}{}
	for _, method := range openRPC.Methods {
		openRPCMethods[method.Name] = struct{}{}
	}
	matrixRows := map[string]openRPCMethodMatrix{}
	for _, row := range matrix.Methods {
		matrixRows[row.Method] = row
	}
	mutating := complianceStringSet(api.Conventions.Idempotency.MutatingMethods)

	var out []string
	for methodName, method := range api.MethodCatalog {
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
		if _, ok := mutating[methodName]; ok {
			continue
		}
		if expectedComplianceTransport(methodName, method) != "http" {
			continue
		}
		out = append(out, methodName)
	}
	sort.Strings(out)
	return out
}

func approvedReadOnlyHTTPRuntimeMethods() []string {
	return []string{
		"agent.delivery_diagnostics",
		"agent.delivery_lifecycle",
		"agent.diagnose",
		"agent.frame",
		"agent.get",
		"agent.list",
		"agent.usage",
		"bundle.agents",
		"bundle.get",
		"bundle.list",
		"conversation.fork_list",
		"conversation.fork_view",
		"conversation.get_turn",
		"conversation.list",
		"conversation.list_turns",
		"entity.aggregate",
		"entity.get",
		"entity.list",
		"event.get",
		"event.list",
		"health.check",
		"health.ping",
		"mailbox.get",
		"mailbox.list",
		"run.diagnose",
		"run.get",
		"run.list",
		"run.trace",
		"runtime.identity",
		"runtime.incidents",
		"runtime.logs",
	}
}

func readOnlyHTTPRuntimeFixtures() map[string]readOnlyHTTPRuntimeFixture {
	return map[string]readOnlyHTTPRuntimeFixture{
		"agent.delivery_diagnostics": {Params: map[string]any{"agent_id": "agent-1"}, ResultKeys: []string{"agent_id", "summary", "failures", "dead_letters"}},
		"agent.delivery_lifecycle":   {Params: map[string]any{"agent_id": "agent-1"}, ResultKeys: []string{"agent_id", "deliveries"}},
		"agent.diagnose":             {Params: map[string]any{"agent_id": "agent-1"}, ResultKeys: []string{"agent_id", "status", "queue", "runtime_state", "active", "last_tool_outcome"}},
		"agent.get":                  {Params: map[string]any{"agent_id": "agent-1"}, ResultKeys: []string{"agent"}},
		"agent.frame":                {Params: map[string]any{"scope": "static", "agent_id": "researcher", "bundle_hash": readOnlyProbeBundleHash, "flow": "research/inst-1"}, ResultKeys: []string{"version", "scope", "selector", "session_contract", "turn_context"}},
		"agent.list":                 {Params: map[string]any{}, ResultKeys: []string{"agents"}},
		"agent.usage":                {Params: map[string]any{"agent_id": "agent-1", "since": "2026-05-21T09:00:00Z", "until": "2026-05-21T10:00:00Z"}, ResultKeys: []string{"agent_id", "window", "usage", "breakdown"}},
		"bundle.agents":              {Params: map[string]any{"bundle_hash": readOnlyProbeBundleHash}, ResultKeys: []string{"agents"}},
		"bundle.get":                 {Params: map[string]any{"bundle_hash": readOnlyProbeBundleHash}, ResultKeys: []string{"bundle_hash", "content_yaml", "parsed_json", "metadata", "agent_count", "has_data", "data_size_bytes", "ingested_at"}},
		"bundle.list":                {Params: map[string]any{}, ResultKeys: []string{"bundles"}},
		"conversation.fork_list":     {Params: map[string]any{}, ResultKeys: []string{"forks"}},
		"conversation.fork_view":     {Params: map[string]any{"fork_id": "00000000-0000-0000-0000-000000000301"}, ResultKeys: []string{"fork_id", "source_session_id", "source_agent_id", "fork_point", "created_by", "created_at", "expires_at", "state", "turns"}},
		"conversation.get_turn":      {Params: map[string]any{"session_id": "sess-1", "turn_id": "turn-1"}, ResultKeys: []string{"session", "turn"}},
		"conversation.list":          {Params: map[string]any{}, ResultKeys: []string{"conversations"}},
		"conversation.list_turns":    {Params: map[string]any{"session_id": "sess-1"}, ResultKeys: []string{"conversation", "turns"}},
		"entity.aggregate":           {Params: map[string]any{}, ResultKeys: []string{"counts"}},
		"entity.get":                 {Params: map[string]any{"entity_id": "entity-1"}, ResultKeys: []string{"entity", "fields", "gates", "accumulated"}},
		"entity.list":                {Params: map[string]any{}, ResultKeys: []string{"entities"}},
		"event.get":                  {Params: map[string]any{"event_id": "evt-1"}, ResultKeys: []string{"event_id", "event_name", "payload", "deliveries", "dead_letters"}},
		"event.list":                 {Params: map[string]any{"filter": map[string]any{"run_id": "run-1"}}, ResultKeys: []string{"events"}},
		"health.check":               {Params: map[string]any{}, ResultKeys: []string{"alive", "ready", "db_ok", "runtime_ok", "execution_posture", "bundle"}},
		"health.ping":                {Params: map[string]any{}, ResultKeys: []string{"ok", "ts"}},
		"mailbox.get":                {Params: map[string]any{"mailbox_id": "card-1"}, ResultKeys: []string{"kind", "decision_card"}},
		"mailbox.list":               {Params: map[string]any{}, ResultKeys: []string{"items"}},
		"run.diagnose":               {Params: map[string]any{"run_id": "run-1"}, ResultKeys: []string{"run", "operational_state", "blocking_layer", "blocking_reason", "heuristics", "test_quiescence"}},
		"run.get":                    {Params: map[string]any{"run_id": "run-1"}, ResultKeys: []string{"run"}},
		"run.list":                   {Params: map[string]any{}, ResultKeys: []string{"runs"}},
		"run.trace":                  {Params: map[string]any{"run_id": "run-1"}, ResultKeys: []string{"trace"}},
		"runtime.identity":           {Params: map[string]any{}, ResultKeys: []string{"runtime_instance_id", "started_at", "api_version", "supported_transports"}},
		"runtime.incidents":          {Params: map[string]any{}, ResultKeys: []string{"incidents"}},
		"runtime.logs":               {Params: map[string]any{}, ResultKeys: []string{"logs"}},
	}
}

func readOnlyHTTPRuntimeErrorProbes() []readOnlyHTTPRuntimeErrorProbe {
	return []readOnlyHTTPRuntimeErrorProbe{
		{
			Method: "agent.frame",
			Params: map[string]any{"scope": "static", "agent_id": "missing", "bundle_hash": readOnlyProbeBundleHash, "flow": "research/inst-1"},
			Code:   AgentNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				return readOnlyRuntimeProbeOptions(t)
			},
		},
		{
			Method: "agent.frame",
			Params: map[string]any{"scope": "static", "agent_id": "researcher", "bundle_hash": readOnlyProbeMissingBundleHash, "flow": "research/inst-1"},
			Code:   BundleNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				opts := readOnlyRuntimeProbeOptions(t)
				opts.BundleCatalog = &fakeBundleCatalogReadStore{missing: map[string]bool{readOnlyProbeMissingBundleHash: true}}
				return opts
			},
		},
		{
			Method: "agent.delivery_diagnostics",
			Params: map[string]any{"agent_id": "missing"},
			Code:   AgentNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				opts := readOnlyRuntimeProbeOptions(t)
				opts.AgentConversations = &fakeAgentConversationReadStore{agentDeliveryDiagnosticsErr: operatorread.ErrAgentNotFound}
				return opts
			},
		},
		{
			Method: "agent.delivery_lifecycle",
			Params: map[string]any{"agent_id": "missing"},
			Code:   AgentNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				opts := readOnlyRuntimeProbeOptions(t)
				opts.AgentDeliveryLifecycle = &fakeAgentConversationReadStore{agentDeliveryLifecycleErr: operatorread.ErrAgentNotFound}
				return opts
			},
		},
		{
			Method: "agent.diagnose",
			Params: map[string]any{"agent_id": "missing"},
			Code:   AgentNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				opts := readOnlyRuntimeProbeOptions(t)
				opts.AgentConversations = &fakeAgentConversationReadStore{agentDiagnosisErr: operatorread.ErrAgentNotFound}
				return opts
			},
		},
		{
			Method: "agent.usage",
			Params: map[string]any{"agent_id": "missing"},
			Code:   AgentNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				opts := readOnlyRuntimeProbeOptions(t)
				opts.AgentUsage = &fakeAgentConversationReadStore{agentUsageErr: operatorread.ErrAgentNotFound}
				return opts
			},
		},
		{
			Method: "agent.get",
			Params: map[string]any{"agent_id": "missing"},
			Code:   AgentNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				opts := readOnlyRuntimeProbeOptions(t)
				opts.AgentConversations = &fakeAgentConversationReadStore{agentErr: operatorread.ErrAgentNotFound}
				return opts
			},
		},
		{
			Method: "bundle.agents",
			Params: map[string]any{"bundle_hash": readOnlyProbeMissingBundleHash},
			Code:   BundleNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				opts := readOnlyRuntimeProbeOptions(t)
				opts.BundleCatalog = &fakeBundleCatalogReadStore{missing: map[string]bool{readOnlyProbeMissingBundleHash: true}}
				return opts
			},
		},
		{
			Method: "bundle.get",
			Params: map[string]any{"bundle_hash": readOnlyProbeMissingBundleHash},
			Code:   BundleNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				opts := readOnlyRuntimeProbeOptions(t)
				opts.BundleCatalog = &fakeBundleCatalogReadStore{missing: map[string]bool{readOnlyProbeMissingBundleHash: true}}
				return opts
			},
		},
		{
			Method: "conversation.list_turns",
			Params: map[string]any{"session_id": "missing"},
			Code:   SessionNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				opts := readOnlyRuntimeProbeOptions(t)
				opts.AgentConversations = &fakeAgentConversationReadStore{conversationTurnsErr: operatorread.ErrSessionNotFound}
				return opts
			},
		},
		{
			Method: "conversation.get_turn",
			Params: map[string]any{"session_id": "missing", "turn_id": "turn-1"},
			Code:   SessionNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				opts := readOnlyRuntimeProbeOptions(t)
				opts.AgentConversations = &fakeAgentConversationReadStore{conversationTurnErr: operatorread.ErrSessionNotFound}
				return opts
			},
		},
		{
			Method: "conversation.get_turn",
			Params: map[string]any{"session_id": "sess-1", "turn_id": "missing-turn"},
			Code:   TurnNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				opts := readOnlyRuntimeProbeOptions(t)
				opts.AgentConversations = &fakeAgentConversationReadStore{conversationTurnErr: operatorread.ErrTurnNotFound}
				return opts
			},
		},
		{
			Method: "conversation.fork_view",
			Params: map[string]any{"fork_id": "00000000-0000-0000-0000-000000000999"},
			Code:   ForkNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				opts := readOnlyRuntimeProbeOptions(t)
				opts.ConversationForks = &fakeConversationForkLifecycleStore{viewErr: runfork.ErrConversationForkNotFound}
				return opts
			},
		},
		{
			Method: "entity.get",
			Params: map[string]any{"entity_id": "missing"},
			Code:   EntityNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				opts := readOnlyRuntimeProbeOptions(t)
				opts.Entities = &fakeEntityReadStore{getErr: operatorread.ErrEntityNotFound}
				return opts
			},
		},
		{
			Method: "event.get",
			Params: map[string]any{"event_id": "missing"},
			Code:   EventNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				return readOnlyRuntimeProbeOptions(t)
			},
		},
		{
			Method: "event.list",
			Params: map[string]any{"filter": map[string]any{}},
			Code:   EventObservationRunScopeRequiredCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				return readOnlyRuntimeProbeOptions(t)
			},
		},
		{
			Method: "mailbox.get",
			Params: map[string]any{"mailbox_id": "missing"},
			Code:   MailboxNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				return readOnlyRuntimeProbeOptions(t)
			},
		},
		{
			Method: "run.diagnose",
			Params: map[string]any{"run_id": "missing"},
			Code:   RunNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				return readOnlyRuntimeProbeOptions(t)
			},
		},
		{
			Method: "run.get",
			Params: map[string]any{"run_id": "missing"},
			Code:   RunNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				return readOnlyRuntimeProbeOptions(t)
			},
		},
		{
			Method: "run.trace",
			Params: map[string]any{"run_id": "missing"},
			Code:   RunNotFoundCode,
			Options: func(t *testing.T) testOperatorCapabilities {
				opts := readOnlyRuntimeProbeOptions(t)
				opts.Observability = &fakeObservabilityReadStore{traceErr: operatorread.ErrRunNotFound}
				return opts
			},
		},
	}
}

func readOnlyRuntimeProbeOptions(t *testing.T) testOperatorCapabilities {
	t.Helper()
	catalogIntent := apiTestResolvedIntent(t, "researcher", "Research the requested market signal.")
	catalogDerivedPrompt, err := runtimeagentintent.IntentOnlyPrompt(catalogIntent)
	if err != nil {
		t.Fatal(err)
	}
	catalogProviderPrompt, err := runtimeagentintent.AssembleProviderPrompt(catalogIntent, nil, catalogDerivedPrompt, runtimeagentintent.RuntimeEnvironmentContext())
	if err != nil {
		t.Fatal(err)
	}
	catalogProviderPromptText, err := catalogProviderPrompt.Text()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1700000000, 0).UTC()
	runID := "run-1"
	eventID := "evt-1"
	sessionID := "sess-1"
	forkID := "00000000-0000-0000-0000-000000000301"
	forkSessionID := "00000000-0000-0000-0000-000000000201"
	forkTurnID := "00000000-0000-0000-0000-000000000401"
	fork := runfork.OperatorConversationForkSession{
		ForkID:          forkID,
		SourceSessionID: forkSessionID,
		SourceRunID:     "00000000-0000-0000-0000-000000000501",
		SourceAgentID:   "agent-1",
		ForkPoint: runfork.ConversationForkPointDescriptor{
			Kind:       "turn",
			TurnIndex:  1,
			TurnID:     forkTurnID,
			SelectedAt: now,
		},
		CreatedBy: "token",
		CreatedAt: now,
		ExpiresAt: now.Add(runfork.ConversationForkLifecycleTTL),
		State:     "active",
		Turns:     []operatorread.OperatorConversationTurn{},
	}
	decisionProbeState := &mutatingRuntimeProbeState{now: now}
	return testOperatorCapabilities{
		ExecutionPosture: executionposture.Live,
		Now:              func() time.Time { return now },
		Ready:            func() bool { return true },
		Database:         fakePinger{},
		RuntimeIdentity: RuntimeIdentityResult{
			RuntimeInstanceID:   "runtime-1",
			StartedAt:           now.Format(time.RFC3339Nano),
			APIVersion:          "v1",
			SupportedTransports: []string{"tcp"},
		},
		Runs: &fakeRunReadStore{
			headers: map[string]operatorread.RunHeader{
				runID: {
					RunID:       runID,
					Status:      "running",
					Origin:      mustEventRunOrigin(t, eventID, "scan.requested"),
					EntityCount: 1,
					EventCount:  1,
					StartedAt:   now.Add(-time.Hour),
				},
			},
			reports: map[string]operatorread.RunDebugReport{
				runID: {
					RunID:          runID,
					RunTableStatus: "running",
					RootEventID:    eventID,
					RootEventType:  "scan.requested",
					StartedAt:      now.Add(-time.Hour),
					LastEventAt:    now.Add(-time.Minute),
					EventCount:     1,
					EntityCount:    1,
				},
			},
		},
		Observability: &fakeObservabilityReadStore{
			traceRows: map[string][]operatorread.RunDebugTraceRow{
				runID: {{
					EventID:        eventID,
					EventName:      "scan.requested",
					EventCreatedAt: now,
				}},
			},
			events: map[string]operatorread.OperatorEventFull{
				eventID: {
					EventID:       eventID,
					EventName:     "scan.requested",
					ExecutionMode: "live",
					RunID:         runID,
					CreatedAt:     now,
					Source:        "runtime",
					ProducerType:  events.EventProducerPlatform,
					Payload:       map[string]any{"ok": true},
					Deliveries:    []operatorread.OperatorEventDelivery{},
					DeadLetters:   []operatorread.OperatorDeadLetterRecord{},
				},
			},
			logs: []operatorread.OperatorRuntimeLogEntry{{
				LogID:     "log-1",
				TS:        now,
				Level:     "info",
				Component: "scheduler",
				Source:    "runtime",
				RunID:     runID,
				SessionID: sessionID,
				Message:   "probe",
			}},
			incidents: []operatorread.OperatorRuntimeIncident{{
				IncidentID:    "inc-1",
				FirstSeen:     now,
				LastSeen:      now,
				Count:         1,
				Level:         "warn",
				Component:     "scheduler",
				SampleMessage: "probe",
				SampleLogIDs:  []string{"log-1"},
			}},
		},
		Entities: &fakeEntityReadStore{
			listResult: operatorread.OperatorEntityListResult{Entities: []operatorread.OperatorEntitySummary{{
				EntityID:     "entity-1",
				RunID:        runID,
				FlowInstance: "review/primary",
				EntityType:   "mvp_spec",
				CurrentState: "collecting",
				Revision:     1,
				CreatedAt:    now,
				UpdatedAt:    now,
			}}},
			getResult: operatorread.OperatorEntityFull{
				Entity: operatorread.OperatorEntitySummary{EntityID: "entity-1", RunID: runID, CurrentState: "collecting"},
				Fields: map[string]any{"priority": "high"},
				Gates:  map[string]bool{"approved": true},
				Accumulated: map[string]any{
					"score":       float64(3),
					"accumulator": map[string]any{"count": float64(2)},
					"notes":       []any{"a", map[string]any{"text": "probe"}},
				},
			},
			aggregate: operatorread.OperatorEntityAggregateResult{Counts: map[string]int{"collecting": 1}},
		},
		BundleCatalog: &fakeBundleCatalogReadStore{
			listResult: bundlecatalog.ListResult{Bundles: []bundlecatalog.Summary{{
				BundleHash:    readOnlyProbeBundleHash,
				AgentCount:    1,
				HasData:       false,
				DataSizeBytes: 0,
				Metadata:      map[string]any{"source": "probe"},
				IngestedAt:    now,
			}}},
			details: map[string]bundlecatalog.Detail{
				readOnlyProbeBundleHash: {
					BundleHash:    readOnlyProbeBundleHash,
					ContentYAML:   "name: probe",
					ParsedJSON:    map[string]any{"agents": map[string]any{}},
					Metadata:      map[string]any{"source": "probe"},
					AgentCount:    1,
					HasData:       false,
					DataSizeBytes: 0,
					IngestedAt:    now,
				},
			},
			agents: map[string]bundlecatalog.AgentsResult{
				readOnlyProbeBundleHash: {
					Agents: []bundlecatalog.AgentDefinition{{
						AgentID:           "researcher",
						AgentNameOwner:    "swarm://flows/research/agent/researcher",
						Role:              "research",
						Type:              "managed",
						Model:             "cheap",
						LLMBackend:        "claude",
						Memory:            true,
						MemorySource:      "authored",
						FlowInstance:      "research/inst-1",
						IntentKind:        string(catalogIntent.Kind),
						IntentSource:      catalogIntent.Coordinate,
						IntentProvenance:  catalogIntent.Provenance,
						IntentContentHash: catalogIntent.ContentHash,
						IntentIdentity:    catalogIntent.Identity,
						IntentContent:     catalogIntent.Content,
						ProviderPrompt:    catalogProviderPromptText,
						Subscriptions:     []string{"scan.requested"},
						Tools:             []string{"web_search"},
					}},
				},
			},
		},
		AgentConversations: &fakeAgentConversationReadStore{
			listAgentsResult: operatorread.OperatorAgentListResult{Agents: []operatorread.OperatorAgentSummary{{
				AgentID:       "agent-1",
				Role:          "researcher",
				Type:          "managed",
				Model:         "cheap",
				ExecutionMode: "live",
				Memory:        true,
				MemorySource:  "authored",
				FlowInstance:  "research/inst-1",
				Status:        "running",
			}}},
			agentResult: operatorread.OperatorAgentDetail{Agent: operatorread.OperatorAgentSummary{
				AgentID:       "agent-1",
				Role:          "researcher",
				Type:          "managed",
				Model:         "cheap",
				ExecutionMode: "live",
				Memory:        true,
				MemorySource:  "authored",
				FlowInstance:  "research/inst-1",
				Status:        "running",
			}},
			agentDiagnosisResult: operatorread.OperatorAgentDiagnosis{
				AgentID: "agent-1",
				Status:  "running",
				Queue: operatorread.OperatorAgentDiagnosisQueue{
					PendingCount:            2,
					OldestPendingAgeSeconds: 30,
					PendingDeliveries: []operatorread.OperatorAgentPendingDelivery{{
						DeliveryID: "delivery-1",
						EventID:    "event-1",
						EventName:  "task.ready",
						EnqueuedAt: time.Date(2026, 5, 21, 10, 0, 0, 0, time.UTC),
						Attempts:   1,
					}},
				},
				DeliveryLifecycle: &operatorread.OperatorAgentDeliveryLifecycle{
					State:         "active",
					BlockingLayer: "session_execution",
				},
				Active: &operatorread.OperatorAgentDiagnosisActive{
					TurnID:   "22222222-2222-2222-2222-222222222222",
					TaskID:   "task-1",
					EntityID: "33333333-3333-3333-3333-333333333333",
				},
				LastToolOutcome: &operatorread.OperatorAgentLastToolOutcome{
					TurnID:    "22222222-2222-2222-2222-222222222222",
					ToolName:  "selected_tool",
					ToolUseID: "toolu-selected",
					OK:        true,
				},
				RuntimeState: &operatorread.OperatorAgentDiagnosisRuntimeState{
					Watchdog: &operatorread.OperatorAgentDiagnosisWatchdog{
						State:         "no_output",
						BlockingLayer: "session_execution",
						Action:        "session_no_output",
						Outcome:       "warning_emitted",
						RecordedAt:    "2026-05-21T10:01:00Z",
					},
				},
			},
			agentUsageResult: operatorread.OperatorAgentUsage{
				AgentID: "agent-1",
				Window: operatorread.OperatorAgentUsageWindow{
					Since: ptrTime(now.Add(-time.Hour)),
					Until: ptrTime(now),
				},
				Usage: operatorread.OperatorAgentUsageByAccounting{
					Exact: operatorread.OperatorAgentUsageTotals{
						LedgerEntries:    1,
						InputTokens:      100,
						OutputTokens:     25,
						EstimatedCostUSD: 0.000675,
					},
					Estimated: operatorread.OperatorAgentUsageTotals{
						LedgerEntries:    1,
						InputTokens:      50,
						OutputTokens:     10,
						EstimatedCostUSD: 0.000300,
					},
				},
				Breakdown: []operatorread.OperatorAgentUsageBreakdown{{
					ExecutionMode:   "live",
					UsageAccounting: operatorread.AgentUsageAccountingExact,
					InvocationType:  "anthropic",
					Model:           "claude-3-5-sonnet",
					ModelAlias:      "regular",
					BackendProfile:  "anthropic",
					Provider:        "anthropic",
					Transport:       "api",
					ResolvedModel:   "claude-3-5-sonnet",
					CostDisplay:     "$0.000675 estimated",
					Totals: operatorread.OperatorAgentUsageTotals{
						LedgerEntries:    1,
						InputTokens:      100,
						OutputTokens:     25,
						EstimatedCostUSD: 0.000675,
					},
				}, {
					ExecutionMode:   "live",
					UsageAccounting: operatorread.AgentUsageAccountingEstimated,
					InvocationType:  "claude_cli",
					Model:           "sonnet",
					ModelAlias:      "regular",
					BackendProfile:  "claude_cli",
					Provider:        "claude",
					Transport:       "cli",
					ResolvedModel:   "sonnet",
					CostDisplay:     "$0.000300 estimated",
					Totals: operatorread.OperatorAgentUsageTotals{
						LedgerEntries:    1,
						InputTokens:      50,
						OutputTokens:     10,
						EstimatedCostUSD: 0.000300,
					},
				}},
			},
			agentDeliveryLifecycleResult: operatorread.OperatorAgentDeliveryLifecycleList{
				AgentID: "agent-1",
				Deliveries: []operatorread.OperatorAgentDeliveryLifecycleRow{{
					DeliveryID:        "delivery-lifecycle-1",
					EventID:           "event-lifecycle-1",
					EventName:         "task.ready",
					RunID:             runID,
					EntityID:          "entity-1",
					Status:            "pending",
					ReasonCode:        "retry_scheduled",
					Failure:           testFailure("temporary_failure"),
					RetryCount:        1,
					DeliveryCreatedAt: now.Add(-3 * time.Minute),
				}},
			},
			agentDeliveryDiagnosticsResult: operatorread.OperatorAgentDeliveryDiagnostics{
				AgentID: "agent-1",
				Summary: operatorread.OperatorAgentDeliveryDiagnosticsSummary{
					Failures24h:    1,
					DeadLetters24h: 1,
				},
				Failures: []operatorread.OperatorAgentDeliveryFailure{{
					DeliveryID: "delivery-failed-1",
					EventID:    "event-failed-1",
					EventName:  "task.failed",
					RunID:      runID,
					EntityID:   "entity-1",
					Status:     "failed",
					ReasonCode: "handler_error",
					Failure:    testFailure("handler_failed"),
					RetryCount: 2,
					OccurredAt: now.Add(-time.Minute),
				}},
				DeadLetters: []operatorread.OperatorAgentDeadLetterDelivery{{
					DeliveryID: "delivery-dead-1",
					EventID:    "event-dead-1",
					EventName:  "task.dead",
					RunID:      runID,
					EntityID:   "entity-1",
					Status:     "dead_letter",
					ReasonCode: "retry_exhausted",
					Failure:    testFailure("retry_exhausted"),
					RetryCount: 3,
					OccurredAt: now.Add(-2 * time.Minute),
					DeadLetterRecords: []operatorread.OperatorDeadLetterRecord{{
						DeadLetterID: "dead-letter-1",
						Failure:      *testFailure("retry_exhausted"),
						RetryCount:   3,
						ChainDepth:   0,
						HandlerNode:  "agent-1",
						CreatedAt:    now.Add(-time.Minute),
					}},
				}},
			},
			listConversationsResult: operatorread.OperatorConversationListResult{Conversations: []operatorread.OperatorConversationSummary{{
				SessionID:    sessionID,
				AgentID:      "agent-1",
				RunID:        runID,
				StartedAt:    now,
				TurnCount:    1,
				MessageCount: 2,
				Status:       "active",
			}}},
			conversationTurnsResult: operatorread.OperatorConversationTurnListResult{
				Conversation: operatorread.OperatorConversationSummary{SessionID: sessionID, AgentID: "agent-1", RunID: runID, StartedAt: now, Status: "active"},
				Turns: []operatorread.OperatorConversationTurnListItem{{TurnID: "turn-1", ExecutionMode: "live", Ordinal: 1, CompletedAt: now, DurationMS: 25,
					TriggerEventID: eventID, TriggerEventType: "scan.requested", ParseOK: true}},
			},
			conversationTurnResult: operatorread.OperatorPublicConversationTurnDetail{
				Session: operatorread.OperatorConversationSummary{SessionID: sessionID, AgentID: "agent-1", RunID: runID, StartedAt: now, Status: "active"},
				Turn: operatorread.OperatorPublicConversationTurn{TurnID: "turn-1", ExecutionMode: "live", Ordinal: 1, CompletedAt: now.Add(time.Second), DurationMS: 25,
					TriggerEventID: eventID, TriggerEventType: "scan.requested", ParseOK: true, Activity: []operatorread.OperatorConversationActivity{}},
			},
		},
		AgentDeliveryLifecycle: &fakeAgentConversationReadStore{
			agentDeliveryLifecycleResult: operatorread.OperatorAgentDeliveryLifecycleList{
				AgentID: "agent-1",
				Deliveries: []operatorread.OperatorAgentDeliveryLifecycleRow{{
					DeliveryID:        "delivery-lifecycle-1",
					EventID:           "event-lifecycle-1",
					EventName:         "task.ready",
					RunID:             runID,
					EntityID:          "entity-1",
					Status:            "pending",
					ReasonCode:        "retry_scheduled",
					Failure:           testFailure("temporary_failure"),
					RetryCount:        1,
					DeliveryCreatedAt: now.Add(-3 * time.Minute),
				}},
			},
		},
		AgentUsage: &fakeAgentConversationReadStore{
			agentUsageResult: operatorread.OperatorAgentUsage{
				AgentID: "agent-1",
				Window: operatorread.OperatorAgentUsageWindow{
					Since: ptrTime(now.Add(-time.Hour)),
					Until: ptrTime(now),
				},
				Usage: operatorread.OperatorAgentUsageByAccounting{
					Exact: operatorread.OperatorAgentUsageTotals{
						LedgerEntries:    1,
						InputTokens:      100,
						OutputTokens:     25,
						EstimatedCostUSD: 0.000675,
					},
					Estimated: operatorread.OperatorAgentUsageTotals{
						LedgerEntries:    1,
						InputTokens:      50,
						OutputTokens:     10,
						EstimatedCostUSD: 0.000300,
					},
				},
				Breakdown: []operatorread.OperatorAgentUsageBreakdown{{
					ExecutionMode:   "live",
					UsageAccounting: operatorread.AgentUsageAccountingExact,
					InvocationType:  "anthropic",
					Model:           "claude-3-5-sonnet",
					ModelAlias:      "regular",
					BackendProfile:  "anthropic",
					Provider:        "anthropic",
					Transport:       "api",
					ResolvedModel:   "claude-3-5-sonnet",
					CostDisplay:     "$0.000675 estimated",
					Totals: operatorread.OperatorAgentUsageTotals{
						LedgerEntries:    1,
						InputTokens:      100,
						OutputTokens:     25,
						EstimatedCostUSD: 0.000675,
					},
				}, {
					ExecutionMode:   "live",
					UsageAccounting: operatorread.AgentUsageAccountingEstimated,
					InvocationType:  "claude_cli",
					Model:           "sonnet",
					ModelAlias:      "regular",
					BackendProfile:  "claude_cli",
					Provider:        "claude",
					Transport:       "cli",
					ResolvedModel:   "sonnet",
					CostDisplay:     "$0.000300 estimated",
					Totals: operatorread.OperatorAgentUsageTotals{
						LedgerEntries:    1,
						InputTokens:      50,
						OutputTokens:     10,
						EstimatedCostUSD: 0.000300,
					},
				}},
			},
		},
		ConversationForks: &fakeConversationForkLifecycleStore{
			listResult: runfork.ConversationForkListResult{Forks: []runfork.OperatorConversationForkSession{fork}},
			viewResult: fork,
		},
		Idempotency:   newMutatingProbeIdempotencyStore(),
		Mailbox:       newReadOnlyMailboxProbeStore(now),
		DecisionCards: newMutatingProbeDecisionCardStore(decisionProbeState),
		Bundle: runtimecontracts.BundleIdentity{
			WorkflowName:    "review",
			WorkflowVersion: "1.0.0",
			BundleHash:      "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
	}
}

func newReadOnlyRuntimeProbeHandler(t *testing.T, opts testOperatorCapabilities) (*Handler, map[string]int) {
	t.Helper()
	allHandlers := testOperatorHandlers(opts)
	calls := map[string]int{}
	handlers := map[string]MethodHandler{}
	for _, methodName := range approvedReadOnlyHTTPRuntimeMethods() {
		handler, ok := allHandlers[methodName]
		if !ok {
			t.Fatalf("OperatorReadHandlers missing read-only method %s", methodName)
		}
		methodName, handler := methodName, handler
		handlers[methodName] = func(ctx context.Context, req Request) (any, error) {
			calls[methodName]++
			return handler(ctx, req)
		}
	}
	return testHandler(t, Options{AuthTokens: []string{testToken}, Handlers: handlers}), calls
}

func callReadOnlyProbeRPC(t *testing.T, handler *Handler, methodName string, params map[string]any, authHeader string) (int, rpcResponse, string) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      readOnlyProbeID(methodName),
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
	handler.ServeHTTP(rec, testAuthorActivityRequest(req))

	if rec.Code != http.StatusOK {
		return rec.Code, rpcResponse{}, rec.Body.String()
	}
	var resp rpcResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %s RPC response: %v body=%s", methodName, err, rec.Body.String())
	}
	return rec.Code, resp, rec.Body.String()
}

func assertReadOnlyProbeSuccess(t *testing.T, methodName string, resp rpcResponse, resultKeys []string) {
	t.Helper()
	if resp.JSONRPC != jsonRPCVersion {
		t.Fatalf("%s jsonrpc = %q, want %q", methodName, resp.JSONRPC, jsonRPCVersion)
	}
	if resp.ID != readOnlyProbeID(methodName) {
		t.Fatalf("%s id = %#v, want %q", methodName, resp.ID, readOnlyProbeID(methodName))
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
	if methodName == "run.diagnose" {
		heuristics, ok := result["heuristics"].([]any)
		if !ok {
			t.Fatalf("run.diagnose heuristics = %#v, want array", result["heuristics"])
		}
		if len(heuristics) != 0 {
			t.Fatalf("run.diagnose heuristics = %#v, want empty array", heuristics)
		}
		quiescence := asMap(t, result["test_quiescence"])
		if quiescence["ready"] != true {
			t.Fatalf("run.diagnose test_quiescence = %#v, want ready", quiescence)
		}
	}
	if methodName == "conversation.list_turns" {
		turns, ok := result["turns"].([]any)
		if !ok || len(turns) == 0 {
			t.Fatalf("%s turns = %#v, want non-empty array", methodName, result["turns"])
		}
		if got := asMap(t, turns[0])["ordinal"]; got != float64(1) {
			t.Fatalf("%s first ordinal = %#v, want 1", methodName, got)
		}
	}
	if methodName == "agent.delivery_lifecycle" {
		deliveries, ok := result["deliveries"].([]any)
		if !ok || len(deliveries) != 1 {
			t.Fatalf("agent.delivery_lifecycle deliveries = %#v", result["deliveries"])
		}
		delivery := asMap(t, deliveries[0])
		if delivery["delivery_id"] != "delivery-lifecycle-1" || delivery["status"] != "pending" || delivery["retry_count"] != float64(1) {
			t.Fatalf("agent.delivery_lifecycle delivery = %#v", delivery)
		}
		for _, forbidden := range []string{"dead_letter_records", "failures", "dead_letters", "summary"} {
			if _, ok := delivery[forbidden]; ok {
				t.Fatalf("agent.delivery_lifecycle exposed diagnostics field %q: %#v", forbidden, delivery)
			}
		}
	}
	if methodName == "agent.delivery_diagnostics" {
		summary := asMap(t, result["summary"])
		if summary["failures_24h"] != float64(1) || summary["dead_letters_24h"] != float64(1) {
			t.Fatalf("agent.delivery_diagnostics summary = %#v", summary)
		}
		failures, ok := result["failures"].([]any)
		if !ok || len(failures) != 1 {
			t.Fatalf("agent.delivery_diagnostics failures = %#v", result["failures"])
		}
		failure := asMap(t, failures[0])
		if failure["status"] != "failed" || failure["delivery_id"] != "delivery-failed-1" || failure["retry_count"] != float64(2) {
			t.Fatalf("agent.delivery_diagnostics failure = %#v", failure)
		}
		deadLetters, ok := result["dead_letters"].([]any)
		if !ok || len(deadLetters) != 1 {
			t.Fatalf("agent.delivery_diagnostics dead_letters = %#v", result["dead_letters"])
		}
		deadLetter := asMap(t, deadLetters[0])
		if deadLetter["status"] != "dead_letter" || deadLetter["delivery_id"] != "delivery-dead-1" || deadLetter["retry_count"] != float64(3) {
			t.Fatalf("agent.delivery_diagnostics dead_letter = %#v", deadLetter)
		}
		records, ok := deadLetter["dead_letter_records"].([]any)
		if !ok || len(records) != 1 || asMap(t, records[0])["dead_letter_id"] != "dead-letter-1" {
			t.Fatalf("agent.delivery_diagnostics dead_letter_records = %#v", deadLetter["dead_letter_records"])
		}
	}
	if methodName == "agent.diagnose" {
		queue := asMap(t, result["queue"])
		if queue["pending_count"] != float64(2) || queue["oldest_pending_age_seconds"] != float64(30) {
			t.Fatalf("agent.diagnose queue = %#v", queue)
		}
		deliveries, ok := queue["pending_deliveries"].([]any)
		if !ok || len(deliveries) != 1 {
			t.Fatalf("agent.diagnose pending_deliveries = %#v", queue["pending_deliveries"])
		}
		if delivery := asMap(t, deliveries[0]); delivery["event_id"] != "event-1" || delivery["event_name"] != "task.ready" || delivery["attempts"] != float64(1) {
			t.Fatalf("agent.diagnose pending delivery = %#v", deliveries[0])
		}
		active := asMap(t, result["active"])
		if active["turn_id"] != "22222222-2222-2222-2222-222222222222" || active["task_id"] != "task-1" || active["entity_id"] != "33333333-3333-3333-3333-333333333333" {
			t.Fatalf("agent.diagnose active = %#v", active)
		}
		runtimeState := asMap(t, result["runtime_state"])
		watchdog := asMap(t, runtimeState["watchdog"])
		if watchdog["state"] != "no_output" || watchdog["blocking_layer"] != "session_execution" || watchdog["action"] != "session_no_output" || watchdog["outcome"] != "warning_emitted" {
			t.Fatalf("agent.diagnose runtime_state.watchdog = %#v", watchdog)
		}
		if watchdog["recorded_at"] != "2026-05-21T10:01:00Z" {
			t.Fatalf("agent.diagnose runtime_state.watchdog.recorded_at = %#v", watchdog["recorded_at"])
		}
		lastTool := asMap(t, result["last_tool_outcome"])
		if lastTool["turn_id"] != "22222222-2222-2222-2222-222222222222" || lastTool["tool_name"] != "selected_tool" || lastTool["tool_use_id"] != "toolu-selected" || lastTool["ok"] != true {
			t.Fatalf("agent.diagnose last_tool_outcome = %#v", lastTool)
		}
		if _, ok := lastTool["result"]; ok {
			t.Fatalf("agent.diagnose last_tool_outcome exposed raw result = %#v", lastTool)
		}
		for _, splitField := range []string{"bundle_version", "watchdog", "token_usage", "failures_recent", "dead_letters_recent"} {
			if _, ok := result[splitField]; ok {
				t.Fatalf("agent.diagnose exposed split field %q: %#v", splitField, result)
			}
		}
		if methodName == "mailbox.get" {
			sheet := asMap(t, result["decision_sheet"])
			entityContext := asMap(t, sheet["entity_context"])
			if entityContext["available"] != false || entityContext["reason"] != "no_source_entity" {
				t.Fatalf("mailbox.get decision_sheet.entity_context = %#v", entityContext)
			}
			downstream := asMap(t, sheet["downstream_preview"])
			if downstream["available"] != false || downstream["reason"] != "no_approval_route" || downstream["subscriber_source"] != "none" {
				t.Fatalf("mailbox.get decision_sheet.downstream_preview = %#v", downstream)
			}
			if subscribers, ok := downstream["subscribers"].([]any); !ok || len(subscribers) != 0 {
				t.Fatalf("mailbox.get decision_sheet.downstream_preview.subscribers = %#v", downstream["subscribers"])
			}
		}
	}
	if methodName == "agent.usage" {
		usage := asMap(t, result["usage"])
		exact := asMap(t, usage["exact"])
		estimated := asMap(t, usage["estimated"])
		if exact["input_tokens"] != float64(100) || estimated["input_tokens"] != float64(50) {
			t.Fatalf("agent.usage exact/estimated totals = %#v", usage)
		}
		breakdown, ok := result["breakdown"].([]any)
		if !ok || len(breakdown) != 2 {
			t.Fatalf("agent.usage breakdown = %#v", result["breakdown"])
		}
		first := asMap(t, breakdown[0])
		if first["usage_accounting"] != "exact" || first["invocation_type"] != "anthropic" || first["model_alias"] != "regular" || first["backend_profile"] != "anthropic" || first["provider"] != "anthropic" || first["transport"] != "api" || first["resolved_model"] != "claude-3-5-sonnet" {
			t.Fatalf("agent.usage first breakdown = %#v", first)
		}
		for _, forbidden := range []string{"token_usage", "run_id", "session_id", "turn_id"} {
			if _, ok := result[forbidden]; ok {
				t.Fatalf("agent.usage exposed forbidden field %q: %#v", forbidden, result)
			}
		}
	}
	if methodName == "bundle.agents" {
		agents, ok := result["agents"].([]any)
		if !ok || len(agents) != 1 {
			t.Fatalf("bundle.agents agents = %#v, want one definition", result["agents"])
		}
		agent := asMap(t, agents[0])
		for _, runtimeKey := range []string{"status", "runtime_state", "queue", "active", "session_id"} {
			if _, ok := agent[runtimeKey]; ok {
				t.Fatalf("bundle.agents leaked runtime field %q: %#v", runtimeKey, agent)
			}
		}
	}
}

func assertReadOnlyProbeInvalidParams(t *testing.T, methodName string, resp rpcResponse, field string) {
	t.Helper()
	if resp.JSONRPC != jsonRPCVersion {
		t.Fatalf("%s jsonrpc = %q, want %q", methodName, resp.JSONRPC, jsonRPCVersion)
	}
	if resp.ID != readOnlyProbeID(methodName) {
		t.Fatalf("%s id = %#v, want %q", methodName, resp.ID, readOnlyProbeID(methodName))
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

func assertReadOnlyProbeApplicationError(t *testing.T, registry *Registry, schemaValidator openRPCResultSchemaValidator, methodName string, resp rpcResponse, code string) {
	t.Helper()
	if resp.JSONRPC != jsonRPCVersion {
		t.Fatalf("%s jsonrpc = %q, want %q", methodName, resp.JSONRPC, jsonRPCVersion)
	}
	if resp.ID != readOnlyProbeID(methodName) {
		t.Fatalf("%s id = %#v, want %q", methodName, resp.ID, readOnlyProbeID(methodName))
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
	if _, ok := data["details"].(map[string]any); !ok {
		t.Fatalf("%s data.details = %#v, want object", methodName, data["details"])
	}
	if _, ok := data["retryable"].(bool); !ok {
		t.Fatalf("%s data.retryable = %#v, want bool", methodName, data["retryable"])
	}
	if _, ok := data["correlation_id"].(string); !ok {
		t.Fatalf("%s data.correlation_id = %#v, want string", methodName, data["correlation_id"])
	}
	schemaValidator.validateMethodErrorData(t, methodName, resp.Error.Code, data)
}

func cloneProbeParams(params map[string]any) map[string]any {
	out := make(map[string]any, len(params))
	for key, value := range params {
		out[key] = value
	}
	return out
}

func readOnlyProbeID(methodName string) string {
	return "probe-" + strings.ReplaceAll(methodName, ".", "-")
}

func sortedProbeFixtureMethods(fixtures map[string]readOnlyHTTPRuntimeFixture) []string {
	methods := make([]string, 0, len(fixtures))
	for methodName := range fixtures {
		methods = append(methods, methodName)
	}
	sort.Strings(methods)
	return methods
}

type readOnlyMailboxProbeStore struct {
	items   []mailbox.V1Item
	details map[string]mailbox.V1ItemDetail
}

func newReadOnlyMailboxProbeStore(now time.Time) *readOnlyMailboxProbeStore {
	item := mailbox.V1Item{
		MailboxID:     "mailbox-1",
		Type:          "review_request",
		Status:        "pending",
		Priority:      "high",
		SourceEventID: "evt-1",
		ExecutionMode: "live",
		SourceFlow:    "review/primary",
		Payload:       map[string]any{"title": "probe"},
		CreatedAt:     now.Format(time.RFC3339Nano),
	}
	return &readOnlyMailboxProbeStore{
		items: []mailbox.V1Item{item},
		details: map[string]mailbox.V1ItemDetail{
			item.MailboxID: {
				Item:    item,
				Payload: map[string]any{"title": "probe"},
				History: []mailbox.V1HistoryEntry{{
					Action:       "created",
					ActorTokenID: "system",
					TS:           now.Format(time.RFC3339Nano),
				}},
			},
		},
	}
}

func (s *readOnlyMailboxProbeStore) ListV1MailboxItems(context.Context, mailbox.V1ListOptions) ([]mailbox.V1Item, string, error) {
	return append([]mailbox.V1Item(nil), s.items...), "", nil
}

func (s *readOnlyMailboxProbeStore) GetV1MailboxItem(_ context.Context, mailboxID string) (mailbox.V1ItemDetail, error) {
	detail, ok := s.details[mailboxID]
	if !ok {
		return mailbox.V1ItemDetail{}, mailbox.ErrV1NotFound
	}
	return detail, nil
}

var _ MailboxAPIStore = (*readOnlyMailboxProbeStore)(nil)
