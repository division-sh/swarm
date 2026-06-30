package bootverify

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"gopkg.in/yaml.v3"
)

func TestRun_MapsMissingToolToToolResolutionWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-tool-missing")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "tool_resolution", "nonexistent_tool") {
		t.Fatalf("expected tool_resolution warning, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForBuiltinRuntimeToolReference(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "test"
	bundle.Agents = map[string]runtimecontracts.AgentRegistryEntry{
		"agent-1": {ID: "agent-1", Tools: []string{"schedule"}, Permissions: []string{"schedule"}},
	}
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "tool_resolution", "schedule") {
		t.Fatalf("unexpected tool_resolution warning for builtin runtime tool, got %#v", report.Warnings())
	}
}

func TestRun_FailsClosedForInvalidExternalDispatchRateLimit(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"bad_http": {
				HandlerType: "http",
				HTTP:        &runtimecontracts.HTTPToolSpec{Method: "GET", URL: "https://example.test"},
				RateLimit:   "1/s",
			},
		},
	})

	report := Run(context.Background(), source, Options{})
	if !reportContains(report.Errors(), "invalid_field_detection", "rate_limit requires rate_limit_max_wait") {
		t.Fatalf("expected invalid rate_limit hard invalidity, got %#v", report.Errors())
	}
}

func TestRun_FailsClosedForMissingDiscoveredMCPTool(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("Decode request: %v", err)
		}
		switch req["method"] {
		case "initialize":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"protocolVersion": "2025-03-26",
					"capabilities":    map[string]any{"tools": map[string]any{}},
					"serverInfo":      map[string]any{"name": "infra", "version": "1.0.0"},
				},
			})
		case "notifications/initialized":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"result":  map[string]any{},
			})
		case "tools/list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tools": []map[string]any{{
						"name": "ping",
					}},
				},
			})
		default:
			t.Fatalf("unexpected mcp method %v", req["method"])
		}
	}))
	defer server.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {
				Tools: []string{"infra.missing"},
			},
		},
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {
				Value: map[string]any{
					"infra": map[string]any{
						"transport": "http",
						"url":       server.URL,
						"prefix":    "infra",
					},
				},
			},
		}},
	})

	report := Run(context.Background(), source, Options{CheckMCPReachable: true})

	if !reportContains(report.Errors(), "required_mcp_tool_availability", "infra.missing") {
		t.Fatalf("expected required_mcp_tool_availability hard invalidity for undiscovered mcp tool, got %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "tool_resolution", "infra.missing") {
		t.Fatalf("did not expect required mcp tool to fall back to tool_resolution warning, got %#v", report.Warnings())
	}
}

func TestRun_FailsClosedForMissingContractMCPTool(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("Decode request: %v", err)
		}
		switch req["method"] {
		case "initialize":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"protocolVersion": "2025-03-26",
					"capabilities":    map[string]any{"tools": map[string]any{}},
					"serverInfo":      map[string]any{"name": "infra", "version": "1.0.0"},
				},
			})
		case "notifications/initialized":
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "result": map[string]any{}})
		case "tools/list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"tools": []map[string]any{{
						"name": "ping",
					}},
				},
			})
		default:
			t.Fatalf("unexpected mcp method %v", req["method"])
		}
	}))
	defer server.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {Tools: []string{"infra.missing"}},
		},
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"infra.missing": {HandlerType: "mcp"},
		},
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {
				Value: map[string]any{
					"infra": map[string]any{
						"transport": "http",
						"url":       server.URL,
						"prefix":    "infra",
					},
				},
			},
		}},
	})

	report := Run(context.Background(), source, Options{CheckMCPReachable: true})

	if !reportContains(report.Errors(), "required_mcp_tool_availability", "infra.missing") {
		t.Fatalf("expected required_mcp_tool_availability hard invalidity for undiscovered contract mcp tool, got %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "tool_resolution", "infra.missing") {
		t.Fatalf("did not expect required contract mcp tool to fall back to tool_resolution warning, got %#v", report.Warnings())
	}
}

func TestRun_FailsClosedForRequiredMCPToolWhenDiscoveryFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("Decode request: %v", err)
		}
		switch req["method"] {
		case "initialize":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"protocolVersion": "2025-03-26",
					"capabilities":    map[string]any{"tools": map[string]any{}},
					"serverInfo":      map[string]any{"name": "infra", "version": "1.0.0"},
				},
			})
		case "notifications/initialized":
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "result": map[string]any{}})
		case "tools/list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error": map[string]any{
					"code":    -32000,
					"message": "catalog unavailable",
				},
			})
		default:
			t.Fatalf("unexpected mcp method %v", req["method"])
		}
	}))
	defer server.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {Tools: []string{"infra.ping"}},
		},
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {
				Value: map[string]any{
					"infra": map[string]any{
						"transport": "http",
						"url":       server.URL,
						"prefix":    "infra",
					},
				},
			},
		}},
	})

	report := Run(context.Background(), source, Options{CheckMCPReachable: true})

	if !reportContains(report.Errors(), "required_mcp_tool_availability", "infra.ping") {
		t.Fatalf("expected required_mcp_tool_availability hard invalidity for failed required mcp discovery, got %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "mcp_server_reachable", "catalog unavailable") {
		t.Fatalf("expected optional inventory reachability warning to remain visible, got %#v", report.Warnings())
	}
}

func TestRun_FailsClosedForPrefixOnlyRequiredMCPToolWhenDiscoveryDisabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("MCP server should not be contacted when CheckMCPReachable is false")
	}))
	defer server.Close()

	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {Tools: []string{"infra.ping"}},
		},
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {
				Value: map[string]any{
					"infra": map[string]any{
						"transport": "http",
						"url":       server.URL,
						"prefix":    "infra",
					},
				},
			},
		}},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "required_mcp_tool_availability", "infra.ping") {
		t.Fatalf("expected required_mcp_tool_availability hard invalidity without catalog proof, got %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "tool_resolution", "infra.ping") {
		t.Fatalf("did not expect prefix-only required mcp tool to fall back to tool_resolution warning, got %#v", report.Warnings())
	}
}

func TestRun_MapsMissingRuntimeExternalCredentialsToCredentialKeyExistsWarnings(t *testing.T) {
	source := runtimeExternalResourceSource("http://127.0.0.1:1")

	report := Run(context.Background(), source, Options{Credentials: bootverifyCredentialStore{}})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	for _, expected := range []string{
		"credential sendgrid_api_key is missing (required by tool email_api)",
		"credential infra_mcp_token is missing (required by mcp_server infra)",
		"credential brave_search_api_key is missing (required by web_search_provider brave)",
	} {
		if !reportContains(report.Warnings(), "credential_key_exists", expected) {
			t.Fatalf("expected credential_key_exists warning containing %q, got %#v", expected, report.Warnings())
		}
	}
}

func TestRun_SkipsCredentialKeyExistsWhenNoCredentialStoreIsSupplied(t *testing.T) {
	source := runtimeExternalResourceSource("http://127.0.0.1:1")

	report := Run(context.Background(), source, Options{})

	if reportContains(report.Warnings(), "credential_key_exists", "credential") {
		t.Fatalf("expected no credential_key_exists warning without credential store, got %#v", report.Warnings())
	}
}

func TestRun_MapsCredentialStoreErrorsToCredentialKeyExistsError(t *testing.T) {
	source := runtimeExternalResourceSource("http://127.0.0.1:1")

	report := Run(context.Background(), source, Options{
		Credentials: bootverifyCredentialStore{listErr: errors.New("credential store unavailable")},
	})

	if !reportContains(report.Errors(), "credential_key_exists", "credential store unavailable") {
		t.Fatalf("expected credential_key_exists error for store failure, got %#v", report.Errors())
	}
}

func TestRun_MapsMCPDiscoveryFailureToMCPServerReachableWarning(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("Decode request: %v", err)
		}
		switch req["method"] {
		case "initialize":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"result": map[string]any{
					"protocolVersion": "2025-03-26",
					"capabilities":    map[string]any{"tools": map[string]any{}},
					"serverInfo":      map[string]any{"name": "infra", "version": "1.0.0"},
				},
			})
		case "notifications/initialized":
			_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "result": map[string]any{}})
		case "tools/list":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"jsonrpc": "2.0",
				"id":      req["id"],
				"error": map[string]any{
					"code":    -32000,
					"message": "catalog unavailable",
				},
			})
		default:
			t.Fatalf("unexpected mcp method %v", req["method"])
		}
	}))
	defer server.Close()
	source := runtimeExternalResourceSource(server.URL)

	report := Run(context.Background(), source, Options{CheckMCPReachable: true})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "mcp_server_reachable", "mcp server infra: catalog unavailable") {
		t.Fatalf("expected mcp_server_reachable warning, got %#v", report.Warnings())
	}
}

func TestRun_SkipsMCPServerReachableWhenReachabilityCheckDisabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("MCP server should not be contacted when CheckMCPReachable is false")
	}))
	defer server.Close()
	source := runtimeExternalResourceSource(server.URL)

	report := Run(context.Background(), source, Options{})

	if reportContains(report.Warnings(), "mcp_server_reachable", "mcp server") {
		t.Fatalf("expected no mcp_server_reachable warning with disabled reachability check, got %#v", report.Warnings())
	}
}

func TestRun_MapsPlatformToolUsageHintCoverageToBootCheck(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"custom_platform_tool": {HandlerType: "platform_builtin"},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "platform_tool_usage_hints", "custom_platform_tool") {
		t.Fatalf("expected platform_tool_usage_hints hard invalidity, got %#v", report.Errors())
	}
}

func TestRun_MapsGeneratedToolSchemaClosureToBootCheck(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"agent-1": {ID: "agent-1", Role: "agent", EmitEvents: []string{"ready.event"}},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"ready.event": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"unsupported": {Type: "NotDeclared"},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "generated_tool_schema_closure", "NotDeclared") {
		t.Fatalf("expected generated_tool_schema_closure hard invalidity, got %#v", report.Errors())
	}
}

func TestRun_MapsEventNoSchemaToEventChainIntegrityWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-event-no-schema")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "event_chain_integrity", "orphan.event") {
		t.Fatalf("expected event_chain_integrity warning, got %#v", report.Warnings())
	}
}

func TestRun_MapsEventNoConsumerToNamedWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-event-no-consumer")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "event_consumer_exists", "orphan.unconsumed") {
		t.Fatalf("expected event_consumer_exists warning, got %#v", report.Warnings())
	}
}

func TestRun_MapsEventNoProducerToNamedWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-event-no-producer")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "event_producer_exists", "task.requested") {
		t.Fatalf("expected event_producer_exists warning, got %#v", report.Warnings())
	}
}

func TestRun_MapsDeadDeclaredEventSchemaToNamedWarning(t *testing.T) {
	root := writeDeadEventSchemaFixture(t, deadEventSchemaFixtureOptions{
		name:       "dead-event-schema-warning",
		rootEvents: "root.unused: {}\n",
	})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "semantic_drift_dead_event_schema", "root.unused") {
		t.Fatalf("expected semantic_drift_dead_event_schema warning for root.unused, got %#v", report.Warnings())
	}
	for _, want := range []string{
		"has no active role in the authored bundle",
		"Handler emits: 0",
		"External source metadata (swarm.source): no",
	} {
		if !reportContains(report.Warnings(), "semantic_drift_dead_event_schema", want) {
			t.Fatalf("expected semantic_drift_dead_event_schema warning containing %q, got %#v", want, report.Warnings())
		}
	}
}

func TestRun_DoesNotWarnWhenDeclaredEventHasAcceptedActiveRoleCarrier(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		target string
		opts   deadEventSchemaFixtureOptions
	}{
		{
			name:   "same-flow handler emit",
			target: "support/ticket.ready",
			opts: deadEventSchemaFixtureOptions{
				name: "dead-event-schema-same-flow-handler",
				flows: map[string]deadEventSchemaFlowFiles{
					"support": {
						events: "ticket.ready: {}\n",
						nodes: `
support-node:
  id: support-node
  execution_type: system_node
  subscribes_to:
    - start
  event_handlers:
    start:
      emit: ticket.ready
`,
					},
				},
			},
		},
		{
			name:   "cross-flow qualified usage",
			target: "producer/ticket.ready",
			opts: deadEventSchemaFixtureOptions{
				name: "dead-event-schema-cross-flow-qualified",
				flows: map[string]deadEventSchemaFlowFiles{
					"producer": {
						events: "ticket.ready: {}\n",
					},
					"consumer": {
						nodes: `
consumer-node:
  id: consumer-node
  execution_type: system_node
  subscribes_to:
    - producer/ticket.ready
  event_handlers:
    producer/ticket.ready: {}
`,
					},
				},
			},
		},
		{
			name:   "root-local root declaration",
			target: "ticket.ready",
			opts: deadEventSchemaFixtureOptions{
				name:       "dead-event-schema-root-local",
				rootEvents: "ticket.ready: {}\n",
				rootNodes: `
root-node:
  id: root-node
  execution_type: system_node
  subscribes_to:
    - ticket.ready
  event_handlers:
    ticket.ready: {}
`,
			},
		},
		{
			name:   "external source metadata",
			target: "support/ticket.ready",
			opts: deadEventSchemaFixtureOptions{
				name: "dead-event-schema-external-source",
				flows: map[string]deadEventSchemaFlowFiles{
					"support": {
						events: "ticket.ready:\n  swarm:\n    source: external (manual handoff)\n",
					},
				},
			},
		},
		{
			name:   "external consumer metadata",
			target: "support/ticket.ready",
			opts: deadEventSchemaFixtureOptions{
				name: "dead-event-schema-external-consumer",
				flows: map[string]deadEventSchemaFlowFiles{
					"support": {
						events: "ticket.ready:\n  swarm:\n    consumer: mailbox_system\n",
					},
				},
			},
		},
		{
			name:   "same-flow timer reference",
			target: "support/ticket.ready",
			opts: deadEventSchemaFixtureOptions{
				name: "dead-event-schema-timer-reference",
				flows: map[string]deadEventSchemaFlowFiles{
					"support": {
						events: "ticket.ready: {}\nstart.signal: {}\n",
						nodes: `
timer-owner:
  id: timer-owner
  execution_type: system_node
  timers:
    - id: reminder
      owner: timer-owner
      event: ticket.ready
      start_on: event:start.signal
      cancel_on: event:ticket.ready
`,
					},
				},
			},
		},
		{
			name:   "fan-out emit",
			target: "support/ticket.ready",
			opts: deadEventSchemaFixtureOptions{
				name: "dead-event-schema-fanout",
				flows: map[string]deadEventSchemaFlowFiles{
					"support": {
						events: "ticket.ready: {}\nstart: {}\n",
						nodes: `
fanout-node:
  id: fanout-node
  execution_type: system_node
  subscribes_to:
    - start
  event_handlers:
    start:
      fan_out:
        emit: ticket.ready
`,
					},
				},
			},
		},
		{
			name:   "accumulate on_complete emit",
			target: "support/ticket.ready",
			opts: deadEventSchemaFixtureOptions{
				name: "dead-event-schema-accumulate-on-complete-emit",
				flows: map[string]deadEventSchemaFlowFiles{
					"support": {
						events: "ticket.ready: {}\nstart: {}\n",
						nodes: `
accumulate-node:
  id: accumulate-node
  execution_type: system_node
  subscribes_to:
    - start
  event_handlers:
    start:
      accumulate:
        expected_from: payload.items
        threshold: 1
        on_complete:
          - emit: ticket.ready
`,
					},
				},
			},
		},
		{
			name:   "accumulate on_timeout fan-out",
			target: "support/ticket.ready",
			opts: deadEventSchemaFixtureOptions{
				name: "dead-event-schema-accumulate-on-timeout-fanout",
				flows: map[string]deadEventSchemaFlowFiles{
					"support": {
						events: "ticket.ready: {}\nstart: {}\n",
						nodes: `
accumulate-timeout-node:
  id: accumulate-timeout-node
  execution_type: system_node
  subscribes_to:
    - start
  event_handlers:
    start:
      accumulate:
        expected_from: payload.items
        threshold: 2
        timeout_ms: 1000
        on_timeout:
          fan_out:
            emit: ticket.ready
`,
					},
				},
			},
		},
		{
			name:   "auto emit on create",
			target: "support/ticket.ready",
			opts: deadEventSchemaFixtureOptions{
				name: "dead-event-schema-auto-emit",
				flows: map[string]deadEventSchemaFlowFiles{
					"support": {
						schema: `
name: support
mode: template
auto_emit_on_create:
  event: ticket.ready
initial_state: idle
terminal_states: [done]
states: [idle, done]
pins:
  inputs:
    events: []
  outputs:
    events: []
`,
						events: "ticket.ready: {}\n",
					},
				},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			root := writeDeadEventSchemaFixture(t, tc.opts)
			bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if reportContains(report.Warnings(), "semantic_drift_dead_event_schema", tc.target) {
				t.Fatalf("unexpected semantic_drift_dead_event_schema warning for %s, got %#v", tc.target, report.Warnings())
			}
		})
	}
}

func TestRun_DoesNotUseSameLocalNameAcrossFlowsByCoincidenceForDeadEventSchema(t *testing.T) {
	root := writeDeadEventSchemaFixture(t, deadEventSchemaFixtureOptions{
		name: "dead-event-schema-coincidental-name",
		flows: map[string]deadEventSchemaFlowFiles{
			"alpha": {
				events: "task.completed: {}\n",
			},
			"beta": {
				events: "task.completed: {}\n",
				nodes: `
beta-node:
  id: beta-node
  execution_type: system_node
  subscribes_to:
    - task.completed
  event_handlers:
    task.completed: {}
`,
			},
		},
	})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Warnings(), "semantic_drift_dead_event_schema", "alpha/task.completed") {
		t.Fatalf("expected semantic_drift_dead_event_schema warning for alpha/task.completed, got %#v", report.Warnings())
	}
	if reportContains(report.Warnings(), "semantic_drift_dead_event_schema", "beta/task.completed") {
		t.Fatalf("unexpected semantic_drift_dead_event_schema warning for beta/task.completed, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotUseRootLocalReferenceAsProofForChildFlowDeadEventSchema(t *testing.T) {
	root := writeDeadEventSchemaFixture(t, deadEventSchemaFixtureOptions{
		name: "dead-event-schema-root-local-child-flow",
		rootNodes: `
root-node:
  id: root-node
  execution_type: system_node
  subscribes_to:
    - ticket.ready
  event_handlers:
    ticket.ready: {}
`,
		flows: map[string]deadEventSchemaFlowFiles{
			"scoring": {
				events: "ticket.ready: {}\n",
			},
		},
	})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Warnings(), "semantic_drift_dead_event_schema", "scoring/ticket.ready") {
		t.Fatalf("expected semantic_drift_dead_event_schema warning for scoring/ticket.ready, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotUsePlatformCatalogOverlapAsProofForDeadEventSchema(t *testing.T) {
	root := writeDeadEventSchemaFixture(t, deadEventSchemaFixtureOptions{
		name:       "dead-event-schema-platform-overlap",
		rootEvents: "platform.runtime_log: {}\n",
	})
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Warnings(), "semantic_drift_dead_event_schema", "platform.runtime_log") {
		t.Fatalf("expected semantic_drift_dead_event_schema warning for platform.runtime_log, got %#v", report.Warnings())
	}
	if !reportContains(report.Errors(), "platform_namespace_violation", "platform.runtime_log") {
		t.Fatalf("expected platform_namespace_violation error for platform.runtime_log, got %#v", report.Errors())
	}
}

func TestRun_DoesNotWarnForEventConsumerExistsWhenCatalogDeclaresConsumerMetadata(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"producer": {
					"task.start": {Emit: runtimecontracts.EmitSpec{Event: "task.done"}},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"producer": {
				SubscribesTo: []string{"task.start"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"task.start": {
						Emit: runtimecontracts.EmitSpec{Event: "task.done"},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.start": {Swarm: runtimecontracts.EventSwarmMetadata{Source: "external"}},
			"task.done":  {Swarm: runtimecontracts.EventSwarmMetadata{Consumer: []string{"dashboard"}}},
		},
	}
	bundle.Platform.Platform.Name = "test"
	bundle.Platform.Platform.Version = "1.0.0"
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if reportContains(report.Warnings(), "event_consumer_exists", "task.done") {
		t.Fatalf("unexpected event_consumer_exists warning with consumer metadata, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForEventProducerExistsWhenCatalogDeclaresExternalOrPlannedSource(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		entry runtimecontracts.EventCatalogEntry
	}{
		{
			name:  "external source",
			entry: runtimecontracts.EventCatalogEntry{Swarm: runtimecontracts.EventSwarmMetadata{Source: "external system"}},
		},
		{
			name:  "planned status",
			entry: runtimecontracts.EventCatalogEntry{Swarm: runtimecontracts.EventSwarmMetadata{Status: "planned"}},
		},
		{
			name:  "exceptional non-agent producer metadata",
			entry: runtimecontracts.EventCatalogEntry{Swarm: runtimecontracts.EventSwarmMetadata{Producer: []string{"mailbox_human"}}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bundle := &runtimecontracts.WorkflowContractBundle{
				Platform: runtimecontracts.PlatformSpecDocument{},
				Semantics: runtimecontracts.WorkflowSemanticView{
					NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
						"consumer": {
							"task.requested": {},
						},
					},
				},
				Nodes: map[string]runtimecontracts.SystemNodeContract{
					"consumer": {
						SubscribesTo: []string{"task.requested"},
						EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
							"task.requested": {},
						},
					},
				},
				Events: map[string]runtimecontracts.EventCatalogEntry{
					"task.requested": tc.entry,
				},
			}
			bundle.Platform.Platform.Name = "test"
			bundle.Platform.Platform.Version = "1.0.0"
			source := semanticview.Wrap(bundle)

			report := Run(context.Background(), source, Options{})

			if reportContains(report.Warnings(), "event_producer_exists", "task.requested") {
				t.Fatalf("unexpected event_producer_exists warning for %s, got %#v", tc.name, report.Warnings())
			}
		})
	}
}

func TestRun_DoesNotWarnForPlatformEmittedEventCatalogSubscription(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		eventType string
		nodeID    string
		node      runtimecontracts.SystemNodeContract
		catalog   yaml.Node
	}{
		{
			name:      "mailbox item decided",
			eventType: "mailbox.item_decided",
			nodeID:    "approval-handler",
			node: runtimecontracts.SystemNodeContract{
				ID:           "approval-handler",
				SubscribesTo: []string{"mailbox.item_decided"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"mailbox.item_decided": {},
				},
			},
			catalog: platformEventCatalogTestNode(t, `
payload:
  mailbox_id: uuid
  mailbox_decision_id: uuid
  decision: text
  decision_payload: object
  item_type: text
  mailbox_payload: object
  source_event_id: uuid
  source_flow: text
  source_entity_id: uuid
  decided_by: text
  decided_at: timestamp
required:
  - mailbox_id
  - mailbox_decision_id
  - decision
  - decision_payload
  - item_type
  - mailbox_payload
  - source_event_id
  - source_flow
  - source_entity_id
  - decided_by
  - decided_at
`),
		},
		{
			name:      "platform paused",
			eventType: "platform.paused",
			nodeID:    "pause-handler",
			node: runtimecontracts.SystemNodeContract{
				ID:           "pause-handler",
				SubscribesTo: []string{"platform.paused"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"platform.paused": {},
				},
			},
			catalog: platformEventCatalogTestNode(t, `
payload:
  reason: text
  paused_by: text
  timestamp: timestamp
`),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bundle := &runtimecontracts.WorkflowContractBundle{
				Semantics: runtimecontracts.WorkflowSemanticView{
					Name:    "platform-event-test",
					Version: "1.0.0",
					NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
						tc.nodeID: tc.node.EventHandlers,
					},
				},
				Nodes: map[string]runtimecontracts.SystemNodeContract{
					tc.nodeID: tc.node,
				},
			}
			bundle.Platform.Platform.Name = "test"
			bundle.Platform.Platform.Version = "1.0.0"
			bundle.Platform.PlatformEvents.Catalog = map[string]yaml.Node{
				tc.eventType: tc.catalog,
			}
			source := semanticview.Wrap(bundle)

			report := Run(context.Background(), source, Options{})

			if reportContains(report.Warnings(), "event_producer_exists", tc.eventType) {
				t.Fatalf("unexpected event_producer_exists warning for platform catalog event, got %#v", report.Warnings())
			}
		})
	}
}

func TestRun_RejectsProductRedeclarationOfPlatformEmittedEvent(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"mailbox.item_decided": {
				Swarm: runtimecontracts.EventSwarmMetadata{Source: "external"},
			},
		},
	}
	bundle.Platform.Platform.Name = "test"
	bundle.Platform.Platform.Version = "1.0.0"
	bundle.Platform.PlatformEvents.Catalog = map[string]yaml.Node{
		"mailbox.item_decided": platformEventCatalogTestNode(t, `
payload:
  mailbox_id: uuid
`),
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "platform_namespace_violation", "Event mailbox.item_decided is platform-emitted and auto-registered; remove the local redeclaration.") {
		t.Fatalf("expected platform-emitted event redeclaration error, got %#v", report.Errors())
	}
}

func TestRun_RejectsFlowOutputPinClaimOfPlatformEmittedEvent(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			Name: "approval",
			Pins: runtimecontracts.FlowPins{
				Outputs: runtimecontracts.FlowOutputPins{
					Events: []string{"mailbox.item_decided"},
				},
			},
		},
	}
	bundle.Platform.Platform.Name = "test"
	bundle.Platform.Platform.Version = "1.0.0"
	bundle.Platform.PlatformEvents.Catalog = map[string]yaml.Node{
		"mailbox.item_decided": platformEventCatalogTestNode(t, `
payload:
  mailbox_id: uuid
`),
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "platform_namespace_violation", "root schema pins.outputs.events references platform-emitted event mailbox.item_decided; platform owns this event") {
		t.Fatalf("expected platform-emitted event output pin error, got %#v", report.Errors())
	}
}

func TestRun_MapsMissingPromptToPromptExistsWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-prompt-missing")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "prompt_exists", "promptless-agent") {
		t.Fatalf("expected prompt_exists warning, got %#v", report.Warnings())
	}
}

func TestRun_PromptRefSatisfiesPromptExistsWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-prompt-ref")

	report := Run(context.Background(), source, Options{})

	if reportContains(report.Warnings(), "prompt_exists", "prompt-ref-agent") {
		t.Fatalf("unexpected prompt_exists warning for prompt_ref backed agent, got %#v", report.Warnings())
	}
}

func TestEventProducedExternallyLocal_AllowsAnnotatedSourceText(t *testing.T) {
	t.Parallel()

	entry := runtimecontracts.EventCatalogEntry{Swarm: runtimecontracts.EventSwarmMetadata{Source: "platform (timer system)"}}
	if !eventProducedExternallyLocal(entry) {
		t.Fatal("expected platform source annotation to count as externally produced")
	}
}

func TestRun_ReportsRecordEvidenceMissingEvidenceTarget(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"node-a": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"task.completed": {
						Action: runtimecontracts.ActionSpec{ID: "record_evidence"},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "handler_field_compliance", "record_evidence is missing evidence_target") {
		t.Fatalf("expected handler_field_compliance error, got %#v", report.Errors())
	}
}

func TestRun_ReportsMailboxWriteMissingMailboxSpec(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"mailbox-node": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"mailbox.review_requested": {
						Action: runtimecontracts.ActionSpec{ID: "mailbox_write"},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "handler_field_compliance", "mailbox_write is missing mailbox") {
		t.Fatalf("expected handler_field_compliance missing mailbox error, got %#v", report.Errors())
	}
}

func TestRun_ReportsRuleMailboxWriteMissingMailboxSpec(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"mailbox-node": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"mailbox.review_requested": {
						Rules: []runtimecontracts.HandlerRuleEntry{{
							ID:     "needs-human",
							Action: runtimecontracts.ActionSpec{ID: "mailbox_write"},
						}},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "handler_field_compliance", "handler mailbox.review_requested rule needs-human mailbox_write is missing mailbox") {
		t.Fatalf("expected rule handler_field_compliance missing mailbox error, got %#v", report.Errors())
	}
}

func TestRun_ReportsMailboxWriteMissingRequiredFields(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"mailbox-node": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"mailbox.review_requested": {
						Action: runtimecontracts.ActionSpec{
							ID:      "mailbox_write",
							Mailbox: &runtimecontracts.MailboxWriteSpec{},
						},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "handler_field_compliance", "mailbox_write is missing mailbox.item_type") {
		t.Fatalf("expected handler_field_compliance missing mailbox.item_type error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "handler_field_compliance", "mailbox_write is missing mailbox.summary") {
		t.Fatalf("expected handler_field_compliance missing mailbox.summary error, got %#v", report.Errors())
	}
}

func TestRun_ReportsRuleMailboxWriteMissingRequiredFields(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"mailbox-node": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"mailbox.review_requested": {
						Rules: []runtimecontracts.HandlerRuleEntry{{
							ID: "needs-human",
							Action: runtimecontracts.ActionSpec{
								ID:      "mailbox_write",
								Mailbox: &runtimecontracts.MailboxWriteSpec{},
							},
						}},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "handler_field_compliance", "handler mailbox.review_requested rule needs-human mailbox_write is missing mailbox.item_type") {
		t.Fatalf("expected rule handler_field_compliance missing mailbox.item_type error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "handler_field_compliance", "handler mailbox.review_requested rule needs-human mailbox_write is missing mailbox.summary") {
		t.Fatalf("expected rule handler_field_compliance missing mailbox.summary error, got %#v", report.Errors())
	}
}

func TestRun_ReportsHandlerLevelActionWithRules(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"mailbox-node": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"mailbox.review_requested": {
						Action: runtimecontracts.ActionSpec{ID: "mailbox_write"},
						Rules: []runtimecontracts.HandlerRuleEntry{{
							ID:        "needs-human",
							Condition: "else",
						}},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "handler_field_compliance", "handler-level action is invalid when rules are present") {
		t.Fatalf("expected ambiguous handler-level action error, got %#v", report.Errors())
	}
}

func TestRun_ReportsUnsupportedRuleActionContexts(t *testing.T) {
	cases := []struct {
		name    string
		handler runtimecontracts.SystemNodeEventHandler
		want    string
	}{
		{
			name: "on_complete",
			handler: runtimecontracts.SystemNodeEventHandler{
				OnComplete: []runtimecontracts.HandlerRuleEntry{{
					ID:     "complete",
					Action: runtimecontracts.ActionSpec{ID: "mailbox_write"},
				}},
			},
			want: "handler.on_complete[complete] action is unsupported",
		},
		{
			name: "accumulate on_complete",
			handler: runtimecontracts.SystemNodeEventHandler{
				Accumulate: &runtimecontracts.AccumulateSpec{
					OnComplete: []runtimecontracts.HandlerRuleEntry{{
						ID:     "complete",
						Action: runtimecontracts.ActionSpec{ID: "mailbox_write"},
					}},
				},
			},
			want: "handler.accumulate.on_complete[complete] action is unsupported",
		},
		{
			name: "accumulate on_timeout",
			handler: runtimecontracts.SystemNodeEventHandler{
				Accumulate: &runtimecontracts.AccumulateSpec{
					OnTimeout: &runtimecontracts.HandlerRuleEntry{
						ID:     "timeout",
						Action: runtimecontracts.ActionSpec{ID: "mailbox_write"},
					},
				},
			},
			want: "handler.accumulate.on_timeout[timeout] action is unsupported",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				Nodes: map[string]runtimecontracts.SystemNodeContract{
					"mailbox-node": {
						EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
							"mailbox.review_requested": tc.handler,
						},
					},
				},
			})

			report := Run(context.Background(), source, Options{})

			if !reportContains(report.Errors(), "handler_field_compliance", tc.want) {
				t.Fatalf("expected unsupported rule action context error %q, got %#v", tc.want, report.Errors())
			}
		})
	}
}

func TestRun_ReportsMailboxDeclarationOnNonMailboxAction(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"mailbox-node": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"mailbox.review_requested": {
						Action: runtimecontracts.ActionSpec{
							ID: "record_evidence",
							Mailbox: &runtimecontracts.MailboxWriteSpec{
								ItemType: runtimecontracts.LiteralExpression("review_request"),
								Summary:  runtimecontracts.LiteralExpression("review"),
							},
						},
						EvidenceTarget: "evidence",
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "handler_field_compliance", "mailbox declaration requires action mailbox_write") {
		t.Fatalf("expected handler_field_compliance mailbox/action mismatch error, got %#v", report.Errors())
	}
}

func TestRun_ReportsArtifactRepoCommitMissingSpec(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"artifact-node": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"artifact.commit_requested": {
						Action: runtimecontracts.ActionSpec{ID: "artifact_repo_commit"},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "handler_field_compliance", "artifact_repo_commit is missing artifact_repo") {
		t.Fatalf("expected handler_field_compliance missing artifact_repo error, got %#v", report.Errors())
	}
}

func TestRun_ReportsArtifactRepoCommitInvalidShape(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"artifact-node": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"artifact.commit_requested": {
						Action: runtimecontracts.ActionSpec{
							ID: "artifact_repo_commit",
							ArtifactRepo: &runtimecontracts.ArtifactRepoSpec{
								Provider:    "s3",
								RepoID:      runtimecontracts.RefExpression("entity.repo_id"),
								RequestID:   runtimecontracts.RefExpression("payload.request_id"),
								Namespace:   runtimecontracts.LiteralExpression("../escape"),
								DisplaySlug: runtimecontracts.LiteralExpression("../escape"),
								Provenance: map[string]runtimecontracts.ExpressionValue{
									"bad/key": {},
								},
								AllowedPaths: []string{"../escape.yaml"},
								Files: []runtimecontracts.ArtifactRepoFileSpec{{
									Path:        runtimecontracts.LiteralExpression("specs/mvp.yaml"),
									Content:     runtimecontracts.RefExpression("payload.mvp_yaml"),
									ContentType: "json",
								}},
								Output: runtimecontracts.ArtifactRepoOutputSpec{
									RepoURL: "repo_url",
									Status:  "status",
								},
								SuccessEvent: "artifact_repo.commit_completed",
								SuccessPayload: map[string]runtimecontracts.ExpressionValue{
									"repo_id": runtimecontracts.RefExpression("entity.repo_id"),
								},
								FailurePayload: map[string]runtimecontracts.ExpressionValue{
									"producer": {},
								},
							},
						},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "handler_field_compliance", "provider s3 is unsupported") {
		t.Fatalf("expected unsupported provider error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "handler_field_compliance", "artifact_repo.namespace") {
		t.Fatalf("expected invalid namespace error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "handler_field_compliance", "artifact_repo.display_slug") {
		t.Fatalf("expected invalid display_slug error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "handler_field_compliance", "artifact_repo.provenance key") {
		t.Fatalf("expected invalid provenance key error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "handler_field_compliance", "artifact_repo.provenance.bad/key is missing value") {
		t.Fatalf("expected missing provenance value error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "handler_field_compliance", "path traversal is not allowed") {
		t.Fatalf("expected traversal allowlist error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "handler_field_compliance", "content_type \"json\" is unsupported") {
		t.Fatalf("expected unsupported content_type error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "handler_field_compliance", "missing artifact_repo.output.current_ref") {
		t.Fatalf("expected missing current_ref output error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "handler_field_compliance", "success_event artifact_repo.commit_completed does not resolve") {
		t.Fatalf("expected unresolved success_event error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "handler_field_compliance", "success_payload must not override runtime-owned field repo_id") {
		t.Fatalf("expected reserved success_payload field error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "handler_field_compliance", "failure_payload requires artifact_repo.failure_event") {
		t.Fatalf("expected failure_payload without failure_event error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "handler_field_compliance", "failure_payload.producer is missing value") {
		t.Fatalf("expected missing failure_payload value error, got %#v", report.Errors())
	}
}

func TestRun_ReportsArtifactRepoCommitResultEventSchemaMismatch(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"artifact-node": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"artifact.commit_requested": {
						Action: runtimecontracts.ActionSpec{
							ID: "artifact_repo_commit",
							ArtifactRepo: &runtimecontracts.ArtifactRepoSpec{
								Provider:     "local_git",
								RepoID:       runtimecontracts.RefExpression("entity.repo_id"),
								RequestID:    runtimecontracts.RefExpression("payload.request_id"),
								Namespace:    runtimecontracts.RefExpression("payload.namespace"),
								AllowedPaths: []string{"readme.md"},
								Files: []runtimecontracts.ArtifactRepoFileSpec{{
									Path:        runtimecontracts.LiteralExpression("readme.md"),
									Content:     runtimecontracts.RefExpression("payload.readme"),
									ContentType: "markdown",
								}},
								Output: runtimecontracts.ArtifactRepoOutputSpec{
									RepoURL:           "repo_url",
									CurrentRef:        "current_ref",
									FileManifest:      "file_manifest",
									Status:            "status",
									FailureReason:     "failure_reason",
									LastRequestID:     "last_request_id",
									LastSourceEventID: "last_source_event_id",
								},
								SuccessEvent: "artifact_repo.commit_completed",
								FailureEvent: "artifact_repo.commit_failed",
							},
						},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"artifact.commit_requested": {},
			"artifact_repo.commit_completed": {
				Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
					"repo_id":         {Type: "string"},
					"namespace":       {Type: "string"},
					"request_id":      {Type: "string"},
					"source_event_id": {Type: "string"},
					"repo_url":        {Type: "string"},
					"current_ref":     {Type: "string"},
					"file_manifest":   {Type: "object"},
					"provenance":      {Type: "object"},
					"result_kind":     {Type: "string"},
				}},
				Required: []string{"repo_id", "namespace", "request_id", "source_event_id", "repo_url", "current_ref", "file_manifest", "provenance", "result_kind"},
			},
			"artifact_repo.commit_failed": {
				Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
					"repo_id":         {Type: "string"},
					"namespace":       {Type: "string"},
					"request_id":      {Type: "string"},
					"source_event_id": {Type: "string"},
					"failure_reason":  {Type: "string"},
					"provenance":      {Type: "object"},
					"request_copy":    {Type: "string"},
				}},
				Required: []string{"repo_id", "namespace", "request_id", "source_event_id", "failure_reason", "provenance", "request_copy"},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "handler_field_compliance", "success_event artifact_repo.commit_completed requires payload field result_kind") {
		t.Fatalf("expected missing success result required payload field error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "handler_field_compliance", "failure_event artifact_repo.commit_failed requires payload field request_copy") {
		t.Fatalf("expected missing failure result required payload field error, got %#v", report.Errors())
	}
}

func TestRun_ReportsArtifactRepoCommitResultEventRuntimeOwnedTypeMismatch(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"artifact-node": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"artifact.commit_requested": {
						Action: runtimecontracts.ActionSpec{
							ID: "artifact_repo_commit",
							ArtifactRepo: &runtimecontracts.ArtifactRepoSpec{
								Provider:     "local_git",
								RepoID:       runtimecontracts.RefExpression("entity.repo_id"),
								RequestID:    runtimecontracts.RefExpression("payload.request_id"),
								Namespace:    runtimecontracts.RefExpression("payload.namespace"),
								AllowedPaths: []string{"readme.md"},
								Files: []runtimecontracts.ArtifactRepoFileSpec{{
									Path:        runtimecontracts.LiteralExpression("readme.md"),
									Content:     runtimecontracts.RefExpression("payload.readme"),
									ContentType: "markdown",
								}},
								Output: runtimecontracts.ArtifactRepoOutputSpec{
									RepoURL:           "repo_url",
									CurrentRef:        "current_ref",
									FileManifest:      "file_manifest",
									Status:            "status",
									FailureReason:     "failure_reason",
									LastRequestID:     "last_request_id",
									LastSourceEventID: "last_source_event_id",
								},
								SuccessEvent: "artifact_repo.commit_completed",
								SuccessPayload: map[string]runtimecontracts.ExpressionValue{
									"result_kind": runtimecontracts.LiteralExpression("success"),
								},
								FailureEvent: "artifact_repo.commit_failed",
								FailurePayload: map[string]runtimecontracts.ExpressionValue{
									"request_copy": runtimecontracts.RefExpression("payload.request_id"),
								},
							},
						},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"artifact.commit_requested": {},
			"artifact_repo.commit_completed": {
				Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
					"repo_id":         {Type: "string"},
					"namespace":       {Type: "string"},
					"request_id":      {Type: "string"},
					"source_event_id": {Type: "string"},
					"repo_url":        {Type: "string"},
					"current_ref":     {Type: "object"},
					"file_manifest":   {Type: "string"},
					"provenance":      {Type: "object"},
					"result_kind":     {Type: "string"},
				}},
				Required: []string{"repo_id", "namespace", "request_id", "source_event_id", "repo_url", "current_ref", "file_manifest", "provenance", "result_kind"},
			},
			"artifact_repo.commit_failed": {
				Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{
					"repo_id":         {Type: "string"},
					"namespace":       {Type: "string"},
					"request_id":      {Type: "string"},
					"source_event_id": {Type: "string"},
					"failure_reason":  {Type: "object"},
					"provenance":      {Type: "string"},
					"request_copy":    {Type: "string"},
				}},
				Required: []string{"repo_id", "namespace", "request_id", "source_event_id", "failure_reason", "provenance", "request_copy"},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	for _, want := range []string{
		"success_event artifact_repo.commit_completed runtime-owned field current_ref must be string-compatible, got object",
		"success_event artifact_repo.commit_completed runtime-owned field file_manifest must be object-compatible, got string",
		"failure_event artifact_repo.commit_failed runtime-owned field failure_reason must be string-compatible, got object",
		"failure_event artifact_repo.commit_failed runtime-owned field provenance must be object-compatible, got string",
	} {
		if !reportContains(report.Errors(), "handler_field_compliance", want) {
			t.Fatalf("expected handler_field_compliance error containing %q, got %#v", want, report.Errors())
		}
	}
}

func TestRun_ReportsArtifactRepoCommitYAMLFileMissingSchema(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"artifact-node": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"artifact.commit_requested": {
						Action: runtimecontracts.ActionSpec{
							ID: "artifact_repo_commit",
							ArtifactRepo: &runtimecontracts.ArtifactRepoSpec{
								Provider:     "local_git",
								RepoID:       runtimecontracts.RefExpression("entity.repo_id"),
								Namespace:    runtimecontracts.LiteralExpression("tenant.alpha"),
								RequestID:    runtimecontracts.RefExpression("payload.request_id"),
								AllowedPaths: []string{"specs/mvp.yaml"},
								Files: []runtimecontracts.ArtifactRepoFileSpec{{
									Path:        runtimecontracts.LiteralExpression("specs/mvp.yaml"),
									Content:     runtimecontracts.RefExpression("payload.mvp_yaml"),
									ContentType: "yaml",
								}},
								Output: runtimecontracts.ArtifactRepoOutputSpec{
									RepoURL:           "repo_url",
									CurrentRef:        "current_ref",
									FileManifest:      "file_manifest",
									Status:            "status",
									FailureReason:     "failure_reason",
									LastRequestID:     "last_request_id",
									LastSourceEventID: "last_source_event_id",
								},
							},
						},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "handler_field_compliance", "schema.type is required for yaml content") {
		t.Fatalf("expected yaml schema requirement error, got %#v", report.Errors())
	}
}

func TestRun_ReportsArtifactRepoDeclarationOnNonArtifactAction(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"artifact-node": {
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"artifact.commit_requested": {
						Action: runtimecontracts.ActionSpec{
							ID: "record_evidence",
							ArtifactRepo: &runtimecontracts.ArtifactRepoSpec{
								Provider: "local_git",
							},
						},
						EvidenceTarget: "evidence",
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "handler_field_compliance", "artifact_repo declaration requires action artifact_repo_commit") {
		t.Fatalf("expected handler_field_compliance artifact/action mismatch error, got %#v", report.Errors())
	}
}

func TestRun_MapsPromptStubToPromptExistsWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-prompt-stub")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "prompt_exists", "TODO") {
		t.Fatalf("expected prompt_exists warning for stub, got %#v", report.Warnings())
	}
}

func TestRun_MapsPromptRefStubToPromptExistsWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-prompt-ref-stub")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "prompt_exists", "TODO") {
		t.Fatalf("expected prompt_exists warning for resolved prompt_ref stub, got %#v", report.Warnings())
	}
}

func TestRun_MapsPolicyConflictToNamedWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-policy-conflict")

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Warnings(), "policy_conflict_detection", "max_retries") {
		t.Fatalf("expected policy_conflict_detection warning, got %#v", report.Warnings())
	}
}

func TestRun_MapsConditionPolicyToNamedWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-condition-policy")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "condition_policy_alignment", "policy.nonexistent_key") {
		t.Fatalf("expected condition_policy_alignment warning, got %#v", report.Warnings())
	}
}

func TestRun_AllowsNestedConditionPolicyReferencePresentInResolvedPolicy(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	nodeID, eventType, handler, ok := firstBundleHandler(bundle)
	if !ok {
		t.Fatal("expected at least one handler")
	}
	handler.Guard = &runtimecontracts.GuardSpec{Check: "policy.retry.max_attempts > 0"}
	node := bundle.Nodes[nodeID]
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node
	bundle.Policy = runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
		"retry": {
			Value: map[string]any{
				"max_attempts": 3,
			},
		},
	}}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "condition_policy_alignment", "policy.retry.max_attempts") {
		t.Fatalf("expected nested policy reference to be accepted, got %#v", report.Warnings())
	}
}

func TestRun_MapsRequiredAgentMismatchToNamedError(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-required-agent-missing")

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "required_agents_match", "missing-agent") {
		t.Fatalf("expected required_agents_match error, got %#v", report.Errors())
	}
}

func TestRun_RejectsRequiredAgentRoleFallbackWithoutMapKey(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			RequiredAgents: []runtimecontracts.FlowRequiredAgent{{
				Role:  "worker",
				Emits: []string{"task.completed"},
			}},
		},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"worker-alias": {
				ID:         "worker",
				Role:       "worker",
				EmitEvents: []string{"task.completed"},
			},
		},
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "required_agents_match", "worker") {
		t.Fatalf("expected required_agents_match missing worker error, got %#v", report.Errors())
	}
}

func TestRun_ReportsRequiredAgentSubscriptionMismatchForTemplateFlow(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-pin-wiring"))
	schema := bundle.FlowSchemas["child"]
	schema.Mode = "template"
	schema.RequiredAgents = []runtimecontracts.FlowRequiredAgent{{
		Role:         "worker",
		SubscribesTo: []string{"work.completed"},
		Emits:        []string{"work.completed"},
	}}
	bundle.FlowSchemas["child"] = schema
	bundle.Semantics.FlowAgents["child"] = append([]runtimecontracts.FlowRequiredAgent(nil), schema.RequiredAgents...)
	worker := runtimecontracts.AgentRegistryEntry{
		ID:               "worker",
		Role:             "worker",
		Model:            "small",
		ConversationMode: "task",
		Subscriptions:    []string{"work.requested"},
		EmitEvents:       []string{"work.completed"},
	}
	bundle.Agents["worker"] = worker
	if view := bundle.FlowTree.ByID["child"]; view != nil {
		if view.Agents == nil {
			view.Agents = map[string]runtimecontracts.AgentRegistryEntry{}
		}
		view.Agents["worker"] = worker
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "required_agents_match", "subscriptions mismatch") {
		t.Fatalf("expected template-flow required_agents_match subscriptions error, got %#v", report.Errors())
	}
}

func TestRun_ReportsRootRequiredAgentSubscriptionMismatch(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			RequiredAgents: []runtimecontracts.FlowRequiredAgent{{
				Role:         "worker",
				SubscribesTo: []string{"task.completed"},
				Emits:        []string{"task.completed"},
			}},
		},
		Agents: map[string]runtimecontracts.AgentRegistryEntry{
			"worker": {
				ID:               "worker",
				Role:             "worker",
				Model:            "small",
				ConversationMode: "task",
				Subscriptions:    []string{"task.requested"},
				EmitEvents:       []string{"task.completed"},
			},
		},
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "required_agents_match", "root required agent worker subscriptions mismatch") {
		t.Fatalf("expected root required_agents_match subscriptions error, got %#v", report.Errors())
	}
}

func TestRun_MapsConditionPayloadMismatchToNamedError(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-condition-payload-mismatch")

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "condition_payload_alignment", "payload.nonexistent_field") {
		t.Fatalf("expected condition_payload_alignment error, got %#v", report.Errors())
	}
}

func TestRun_MapsUnknownSetsGateToGateSchemaValidationError(t *testing.T) {
	bundle := gateSchemaValidationBundle(runtimecontracts.NodeGateStateSchema{
		Gates: []runtimecontracts.NodeGateField{{Name: "approved"}},
	}, runtimecontracts.SystemNodeEventHandler{
		SetsGate: &runtimecontracts.GateSpec{Name: "rejected", Value: true},
	})

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "gate_schema_validation", "sets_gate rejected") {
		t.Fatalf("expected gate_schema_validation error, got %#v", report.Errors())
	}
}

func TestRun_MapsMissingOrEmptyGateStateToGateSchemaValidationError(t *testing.T) {
	cases := []struct {
		name      string
		gateState runtimecontracts.NodeGateStateSchema
	}{
		{name: "missing gate_state"},
		{name: "empty gate_state", gateState: runtimecontracts.NodeGateStateSchema{Gates: []runtimecontracts.NodeGateField{}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bundle := gateSchemaValidationBundle(tc.gateState, runtimecontracts.SystemNodeEventHandler{
				SetsGate: &runtimecontracts.GateSpec{Name: "approved", Value: true},
			})

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if !reportContains(report.Errors(), "gate_schema_validation", "sets_gate approved") {
				t.Fatalf("expected gate_schema_validation error, got %#v", report.Errors())
			}
		})
	}
}

func TestRun_AllowsDeclaredSetsGateFromScalarAndStructuredForms(t *testing.T) {
	cases := []struct {
		name string
		yaml string
	}{
		{
			name: "scalar",
			yaml: "sets_gate: approved\n",
		},
		{
			name: "structured",
			yaml: "sets_gate:\n  name: approved\n  value: true\n",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := decodeGateSchemaHandler(t, tc.yaml)
			bundle := gateSchemaValidationBundle(runtimecontracts.NodeGateStateSchema{
				Gates: []runtimecontracts.NodeGateField{{Name: "approved"}},
			}, handler)

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if reportContains(report.Errors(), "gate_schema_validation", "sets_gate approved") {
				t.Fatalf("unexpected gate_schema_validation error, got %#v", report.Errors())
			}
		})
	}
}

func TestRun_MapsEmptyEventPayloadSchemaConditionRefsToNamedError(t *testing.T) {
	cases := []struct {
		name    string
		handler runtimecontracts.SystemNodeEventHandler
	}{
		{
			name: "guard check",
			handler: runtimecontracts.SystemNodeEventHandler{
				Guard: &runtimecontracts.GuardSpec{Check: `payload.missing == "x"`},
			},
		},
		{
			name: "guard checks",
			handler: runtimecontracts.SystemNodeEventHandler{
				Guard: &runtimecontracts.GuardSpec{Checks: []runtimecontracts.GuardCheck{{
					ID:    "missing",
					Check: `payload.missing == "x"`,
				}}},
			},
		},
		{
			name: "rule",
			handler: runtimecontracts.SystemNodeEventHandler{
				Rules: []runtimecontracts.HandlerRuleEntry{{
					ID:        "missing",
					Condition: `payload.missing == "x"`,
				}},
			},
		},
		{
			name: "on complete",
			handler: runtimecontracts.SystemNodeEventHandler{
				OnComplete: []runtimecontracts.HandlerRuleEntry{{
					Condition: `payload.missing == "x"`,
				}},
			},
		},
		{
			name: "accumulate on complete",
			handler: runtimecontracts.SystemNodeEventHandler{
				Accumulate: &runtimecontracts.AccumulateSpec{
					OnComplete: []runtimecontracts.HandlerRuleEntry{{
						Condition: `payload.missing == "x"`,
					}},
				},
			},
		},
		{
			name: "filter",
			handler: runtimecontracts.SystemNodeEventHandler{
				Filter: &runtimecontracts.FilterSpec{Condition: `payload.missing == "x"`},
			},
		},
		{
			name: "count",
			handler: runtimecontracts.SystemNodeEventHandler{
				Count: &runtimecontracts.CountSpec{Condition: `payload.missing == "x"`},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handler := tc.handler
			handler.AdvancesTo = "done"
			handler.Emit = runtimecontracts.EmitSpec{Event: "task.completed", Broadcast: true}
			bundle := &runtimecontracts.WorkflowContractBundle{
				Events: map[string]runtimecontracts.EventCatalogEntry{
					"task.requested": {},
					"task.completed": {
						Payload: runtimecontracts.EventPayloadSpec{
							Properties: map[string]runtimecontracts.EventFieldSpec{
								"entity_id": {Type: "string"},
							},
						},
					},
				},
				Nodes: map[string]runtimecontracts.SystemNodeContract{
					"complete-task": {
						ID:            "complete-task",
						ExecutionType: "system_node",
						SubscribesTo:  []string{"task.requested"},
						Produces:      []string{"task.completed"},
						EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
							"task.requested": handler,
						},
					},
				},
				RootSchema: &runtimecontracts.FlowSchemaDocument{
					InitialState:   "pending",
					TerminalStates: []string{"done"},
					States:         []string{"pending", "done"},
				},
			}
			bundle.Platform.Platform.Name = "swarm"
			bundle.Platform.Platform.Version = "test"
			source := semanticview.Wrap(bundle)

			report := Run(context.Background(), source, Options{})

			if !reportContains(report.Errors(), "condition_payload_alignment", "payload.missing") {
				t.Fatalf("expected empty payload schema condition_payload_alignment error, got %#v", report.Errors())
			}
		})
	}
}

func TestRun_DoesNotMapMissingEventSchemaToConditionPayloadAlignment(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.completed": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"entity_id": {Type: "string"},
					},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"complete-task": {
				ID:            "complete-task",
				ExecutionType: "system_node",
				SubscribesTo:  []string{"task.requested"},
				Produces:      []string{"task.completed"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"task.requested": {
						Guard:      &runtimecontracts.GuardSpec{Check: `payload.missing == "x"`},
						AdvancesTo: "done",
						Emit:       runtimecontracts.EmitSpec{Event: "task.completed", Broadcast: true},
					},
				},
			},
		},
		RootSchema: &runtimecontracts.FlowSchemaDocument{
			InitialState:   "pending",
			TerminalStates: []string{"done"},
			States:         []string{"pending", "done"},
		},
	}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "test"

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "condition_payload_alignment", "payload.missing") {
		t.Fatalf("missing event schema should stay outside condition_payload_alignment, got %#v", report.Errors())
	}
}

func TestRun_AllowsNestedConditionPayloadReferenceWithinEventPayloadSchema(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	nodeID, eventType, handler, ok := firstBundleHandler(bundle)
	if !ok {
		t.Fatal("expected at least one handler")
	}
	handler.Guard = &runtimecontracts.GuardSpec{Check: `payload.task.id != ""`}
	node := bundle.Nodes[nodeID]
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node
	entry := bundle.Events[eventType]
	entry.Payload.Properties = map[string]runtimecontracts.EventFieldSpec{
		"task": {Type: "object"},
	}
	bundle.Events[eventType] = entry

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "condition_payload_alignment", "payload.task.id") {
		t.Fatalf("expected nested payload reference to be accepted, got %#v", report.Errors())
	}
}

func TestRun_MapsDataAccumulationSourcePayloadMismatchToNamedError(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"item.received": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"score": {Type: "integer"},
					},
				},
			},
		},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"subject": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"score": {Type: "integer"},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"node-1": {
				ID: "node-1",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": {
						DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
							Writes: []runtimecontracts.WorkflowDataWrite{{
								SourceField: "missing_score",
								TargetField: "score",
							}},
						},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "payload_field_coverage", `source field "missing_score"`) {
		t.Fatalf("expected payload_field_coverage error, got %#v", report.Errors())
	}
}

func TestRun_MapsEmptyDataAccumulationSourcePayloadSchemaToNamedError(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"item.received": {},
		},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"subject": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"score": {Type: "integer"},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"node-1": {
				ID: "node-1",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": {
						DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
							Writes: []runtimecontracts.WorkflowDataWrite{{
								SourceField: "foo",
								TargetField: "score",
							}},
						},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "payload_field_coverage", `source field "foo"`) {
		t.Fatalf("expected empty payload schema payload_field_coverage error, got %#v", report.Errors())
	}
}

func TestRun_AllowsDeclaredDataAccumulationSourcePayloadField(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"item.received": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"score": {Type: "integer"},
					},
				},
			},
		},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"subject": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"score": {Type: "integer"},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"node-1": {
				ID: "node-1",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": {
						DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
							Writes: []runtimecontracts.WorkflowDataWrite{{
								SourceField: "score",
								TargetField: "score",
							}},
						},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if reportContains(report.Errors(), "payload_field_coverage", "score") {
		t.Fatalf("unexpected payload_field_coverage error, got %#v", report.Errors())
	}
}

func TestRun_MapsUndeclaredNestedEntityWriteTargetToEntityWriteTargetComplianceError(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootTypes: runtimecontracts.TypeCatalogDocument{
			Types: map[string]runtimecontracts.NamedTypeDecl{
				"Analysis": {
					Fields: map[string]runtimecontracts.TypeFieldSpec{
						"summary": {Type: "text"},
					},
				},
			},
		},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"subject": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"analysis": {Type: "Analysis"},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"node-1": {
				ID: "node-1",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"task.completed": {
						Compute: &runtimecontracts.ComputeSpec{
							Operation: runtimecontracts.ComputeOpCount,
							StoreAs:   "entity.analysis.missing",
						},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "entity_write_target_compliance", `entity.analysis.missing`) {
		t.Fatalf("expected nested entity_write_target_compliance error, got %#v", report.Errors())
	}
}

func TestRun_MapsConfigFromPayloadMismatchToNamedError(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	bundle.Semantics.HandlerTransitions = []runtimecontracts.HandlerTransitionSemantic{{
		ID:        "transition-1",
		EventType: "task.requested",
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			SourceEvent: "other.event",
		},
	}}
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "config_from_payload_alignment", "source_event other.event does not match handler event task.requested") {
		t.Fatalf("expected config_from_payload_alignment error, got %#v", report.Errors())
	}
}

func TestRun_AllowsFanOutDerivedAccumulationSourceEvent(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	bundle.Semantics.HandlerTransitions = []runtimecontracts.HandlerTransitionSemantic{{
		ID:        "transition-1",
		EventType: "task.requested",
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			SourceEvent: "fan_out.child_completed",
		},
	}}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "config_from_payload_alignment", "fan_out.child_completed") {
		t.Fatalf("expected fan_out source_event to be accepted, got %#v", report.Errors())
	}
}

func TestRun_ReportsCreateFlowInstanceMissingInstanceIDFrom(t *testing.T) {
	repoRoot := repoRootForBootverifyTest(t)
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier9-composition-patterns", "test-compose-create-instance-config")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle := loadFixtureBundleAt(t, repoRoot, fixtureRoot, platformSpec)
	node := bundle.Nodes["spawner"]
	handler := node.EventHandlers["spawn.requested"]
	handler.Action.InstanceIDFrom = ""
	handler.Action.InstanceIDPath = paths.Path{}
	node.EventHandlers["spawn.requested"] = handler
	bundle.Nodes["spawner"] = node

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "handler_field_compliance", "create_flow_instance is missing instance_id_from") {
		t.Fatalf("expected handler_field_compliance missing instance_id_from error, got %#v", report.Errors())
	}
}

func TestRun_ReportsCreateFlowInstanceMissingConfigFrom(t *testing.T) {
	repoRoot := repoRootForBootverifyTest(t)
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier9-composition-patterns", "test-compose-create-instance-config")
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle := loadFixtureBundleAt(t, repoRoot, fixtureRoot, platformSpec)
	node := bundle.Nodes["spawner"]
	handler := node.EventHandlers["spawn.requested"]
	handler.Action.ConfigFrom = &runtimecontracts.ConfigFromSpec{Bindings: map[string]string{}}
	node.EventHandlers["spawn.requested"] = handler
	bundle.Nodes["spawner"] = node

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "handler_field_compliance", "create_flow_instance is missing config_from") {
		t.Fatalf("expected handler_field_compliance missing config_from error, got %#v", report.Errors())
	}
}

func TestRun_MapsStateMachineMismatchToNamedError(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-state-machine-invalid")

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "state_machine_coherence", "bogus_state") {
		t.Fatalf("expected state_machine_coherence error, got %#v", report.Errors())
	}
}

func TestRun_WarnsWhenDeclaredStateIsUnreachable(t *testing.T) {
	root := writeStateReachabilityFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "semantic_drift_unreachable_state", "declares state review but no transition path from initial_state waiting reaches review") {
		t.Fatalf("expected semantic_drift_unreachable_state warning, got %#v", report.Warnings())
	}
	if !reportContains(report.Warnings(), "semantic_drift_unreachable_state", "Reachable states: active, done, waiting") {
		t.Fatalf("expected reachable-state summary, got %#v", report.Warnings())
	}
	if !reportContains(report.Warnings(), "semantic_drift_unreachable_state", "Unreachable states: review") {
		t.Fatalf("expected unreachable-state summary, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnWhenOnCompleteBranchReachesDeclaredState(t *testing.T) {
	root := writeStateReachabilityFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))
	node := bundle.Nodes["support-node"]
	handler := node.EventHandlers["ticket.closed"]
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{{
		Condition:  "true",
		AdvancesTo: "review",
	}}
	node.EventHandlers["ticket.closed"] = handler
	bundle.Nodes["support-node"] = node

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "semantic_drift_unreachable_state", "review") {
		t.Fatalf("unexpected semantic_drift_unreachable_state warning when on_complete reaches review, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnWhenRuleBranchReachesDeclaredState(t *testing.T) {
	root := writeStateReachabilityFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))
	node := bundle.Nodes["support-node"]
	handler := node.EventHandlers["ticket.closed"]
	handler.Rules = []runtimecontracts.HandlerRuleEntry{{
		Condition:  "true",
		AdvancesTo: "review",
	}}
	node.EventHandlers["ticket.closed"] = handler
	bundle.Nodes["support-node"] = node

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "semantic_drift_unreachable_state", "review") {
		t.Fatalf("unexpected semantic_drift_unreachable_state warning when rules reach review, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotUseAccumulateOnCompleteAsReachabilityProof(t *testing.T) {
	root := writeStateReachabilityFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))
	node := bundle.Nodes["support-node"]
	handler := node.EventHandlers["ticket.closed"]
	handler.Accumulate = &runtimecontracts.AccumulateSpec{
		OnComplete: []runtimecontracts.HandlerRuleEntry{{
			Condition:  "true",
			AdvancesTo: "review",
		}},
	}
	node.EventHandlers["ticket.closed"] = handler
	bundle.Nodes["support-node"] = node

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Warnings(), "semantic_drift_unreachable_state", "review") {
		t.Fatalf("expected semantic_drift_unreachable_state warning when only accumulate.on_complete reaches review, got %#v", report.Warnings())
	}
}

func TestRun_PreservesStateMachineCoherenceErrorWhenInvalidTargetExists(t *testing.T) {
	root := writeStateReachabilityFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))
	node := bundle.Nodes["support-node"]
	handler := node.EventHandlers["ticket.closed"]
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{{
		Condition:  "true",
		AdvancesTo: "review",
	}}
	handler.Rules = []runtimecontracts.HandlerRuleEntry{{
		Condition:  "true",
		AdvancesTo: "bogus_state",
	}}
	node.EventHandlers["ticket.closed"] = handler
	bundle.Nodes["support-node"] = node

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "state_machine_coherence", "bogus_state") {
		t.Fatalf("expected state_machine_coherence error, got %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "semantic_drift_unreachable_state", "review") {
		t.Fatalf("unexpected semantic_drift_unreachable_state warning when declared states remain reachable, got %#v", report.Warnings())
	}
}

func TestRun_MapsDialectDualToNamedError(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	nodeID, eventType, handler, ok := firstBundleHandler(bundle)
	if !ok {
		t.Fatal("expected at least one handler")
	}
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{{Condition: "true"}}
	handler.Rules = []runtimecontracts.HandlerRuleEntry{{Condition: "true"}}
	node := bundle.Nodes[nodeID]
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "dialect_compliance", "declares both on_complete and rules") {
		t.Fatalf("expected dialect_compliance error, got %#v", report.Errors())
	}
}

func TestRun_MapsCreateEntityPlusAccumulateToNamedError(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-create-entity-plus-accumulate")

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "dialect_compliance", "declares both create_entity and accumulate") {
		t.Fatalf("expected dialect_compliance create_entity/accumulate error, got %#v", report.Errors())
	}
}

func TestRun_MapsInvalidFieldDetectionToNamedError(t *testing.T) {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{
			"flow-a": {
				InitialState: "pending",
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{},
		Nodes:  map[string]runtimecontracts.SystemNodeContract{},
		Tools:  map[string]runtimecontracts.ToolSchemaEntry{},
	}
	bundle.Platform.Platform.Name = "Swarm Platform"
	bundle.Platform.Platform.Version = "1.0.0"
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "invalid_field_detection", "flow schema flow-a missing required field name") {
		t.Fatalf("expected invalid_field_detection error, got %#v", report.Errors())
	}
}

func TestRun_RejectsRootAgentFlowSessionScope(t *testing.T) {
	root := writeSessionScopeValidationFixture(t, `
root-flow:
  id: root-flow
  model: regular
  conversation_mode: session
  session_scope: flow
  subscriptions:
    - item.created
`, "", "")

	report := Run(context.Background(), loadSessionScopeValidationFixture(t, root), Options{})

	if !reportContains(report.Errors(), "invalid_field_detection", "session_scope flow requires flow-scoped declaration") {
		t.Fatalf("expected session_scope flow declaration error, got %#v", report.Errors())
	}
}

func TestRun_RejectsEntitySessionScopeInStatelessFlow(t *testing.T) {
	root := writeSessionScopeValidationFixture(t, "{}\n", `
name: support
`, `
entity-agent:
  id: entity-agent
  model: regular
  conversation_mode: session_per_entity
  session_scope: entity
  subscriptions:
    - item.created
`)

	report := Run(context.Background(), loadSessionScopeValidationFixture(t, root), Options{})

	if !reportContains(report.Errors(), "invalid_field_detection", "session_scope entity requires stateful flow support") {
		t.Fatalf("expected stateful flow session_scope error, got %#v", report.Errors())
	}
}

func TestRun_AcceptsExplicitSessionScopeDeclarations(t *testing.T) {
	root := writeSessionScopeValidationFixture(t, "{}\n", `
name: support
initial_state: waiting
states:
  - waiting
  - done
`, `
flow-agent:
  id: flow-agent
  model: regular
  conversation_mode: session
  session_scope: flow
  subscriptions:
    - support/item.created
entity-agent:
  id: entity-agent
  model: regular
  conversation_mode: session_per_entity
  session_scope: entity
  subscriptions:
    - support/item.created
`)

	report := Run(context.Background(), loadSessionScopeValidationFixture(t, root), Options{})

	for _, finding := range report.Errors() {
		if finding.CheckID == "invalid_field_detection" && strings.Contains(finding.Message, "session_scope") {
			t.Fatalf("unexpected session_scope error: %#v", report.Errors())
		}
	}
}

func TestRun_RejectsAuthoredGlobalSessionScope(t *testing.T) {
	root := writeSessionScopeValidationFixture(t, `
root-global:
  id: root-global
  model: regular
  conversation_mode: session
  session_scope: global
  subscriptions:
    - item.created
`, "", "")

	report := Run(context.Background(), loadSessionScopeValidationFixture(t, root), Options{})

	if !reportContains(report.Errors(), "invalid_field_detection", "authored normal agents cannot declare session_scope global") {
		t.Fatalf("expected authored global session_scope error, got %#v", report.Errors())
	}
}

func TestRun_AcceptsPackageBackedFlowSessionScopeDeclarations(t *testing.T) {
	root := writePackageBackedSessionScopeValidationFixture(t, `
name: support
initial_state: waiting
states:
  - waiting
  - done
`, `
flow-agent:
  id: flow-agent
  model: regular
  conversation_mode: session
  session_scope: flow
  subscriptions:
    - support/item.created
entity-agent:
  id: entity-agent
  model: regular
  conversation_mode: session_per_entity
  session_scope: entity
  subscriptions:
    - support/item.created
`)

	source := loadSessionScopeValidationFixture(t, root)
	report := Run(context.Background(), source, Options{})

	for _, finding := range report.Errors() {
		if finding.CheckID == "invalid_field_detection" && strings.Contains(finding.Message, "session_scope") {
			t.Fatalf("unexpected package-backed session_scope error: %#v", report.Errors())
		}
	}
}

func TestRun_RejectsPackageBackedEntitySessionScopeInStatelessFlow(t *testing.T) {
	root := writePackageBackedSessionScopeValidationFixture(t, `
name: support
`, `
entity-agent:
  id: entity-agent
  model: regular
  conversation_mode: session_per_entity
  session_scope: entity
  subscriptions:
    - support/item.created
`)

	report := Run(context.Background(), loadSessionScopeValidationFixture(t, root), Options{})

	if !reportContains(report.Errors(), "invalid_field_detection", "session_scope entity requires stateful flow support") {
		t.Fatalf("expected stateful flow session_scope error for package-backed flow, got %#v", report.Errors())
	}
}

func TestRun_MapsHandlerFieldComplianceToNamedError(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	nodeID, eventType, handler, ok := firstBundleHandler(bundle)
	if !ok {
		t.Fatal("expected at least one handler")
	}
	node := bundle.Nodes[nodeID]
	handler.Action = runtimecontracts.ActionSpec{ID: "missing.handler.action"}
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "handler_field_compliance", "action missing.handler.action is not executable") {
		t.Fatalf("expected handler_field_compliance error, got %#v", report.Errors())
	}
}

func TestRun_MapsSelfEmitToEventCycleDetection(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-self-emit")

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected error report, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "event_cycle_detection", "emits its own trigger event") {
		t.Fatalf("expected event_cycle_detection error, got %#v", report.Errors())
	}
}

func TestRun_MapsSelfEmitToEventCycleDetectionForFlowLocalHandlers(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-local-events"))
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.Emit = runtimecontracts.EmitSpec{Event: eventType}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "event_cycle_detection", "emits its own trigger event") {
		t.Fatalf("expected event_cycle_detection error for flow-local handler, got %#v", report.Errors())
	}
}

func TestRun_ReportsSemanticModelMultiHopEventCycle(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-self-emit")
	bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{
		"node-a": {
			"task.a": {Emit: runtimecontracts.EmitSpec{Event: "task.b"}},
		},
		"node-b": {
			"task.b": {Emit: runtimecontracts.EmitSpec{Event: "task.a"}},
		},
	}
	bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{
		"node-a": {
			ID:           "node-a",
			SubscribesTo: []string{"task.a"},
			Produces:     []string{"task.b"},
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"task.a": {Emit: runtimecontracts.EmitSpec{Event: "task.b"}},
			},
		},
		"node-b": {
			ID:           "node-b",
			SubscribesTo: []string{"task.b"},
			Produces:     []string{"task.a"},
			EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
				"task.b": {Emit: runtimecontracts.EmitSpec{Event: "task.a"}},
			},
		},
	}
	bundle.Events = map[string]runtimecontracts.EventCatalogEntry{
		"task.a": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{"entity_id": {Type: "string"}}}},
		"task.b": {Payload: runtimecontracts.EventPayloadSpec{Properties: map[string]runtimecontracts.EventFieldSpec{"entity_id": {Type: "string"}}}},
	}
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "event_cycle_detection", "EVENT-CYCLE") {
		t.Fatalf("expected semantic-model event_cycle_detection error, got %#v", report.Errors())
	}
}

func TestRun_MapsBareConditionToConditionExpressionValidation(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-bare-condition")

	report := Run(context.Background(), source, Options{})

	if !report.HasErrors() {
		t.Fatalf("expected validation errors, got %#v", report.Findings)
	}
	if !reportContains(report.Errors(), "condition_expression_validation", "missing required prefix") {
		t.Fatalf("expected condition_expression_validation error, got %#v", report.Errors())
	}
}

func TestRun_RejectsUnsupportedGuardOnFail(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"test-node": {
				ID: "test-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": {
						Guard: &runtimecontracts.GuardSpec{
							Check:  "entity.entity_id != null",
							OnFail: "explode",
						},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "condition_expression_validation", `unsupported guard on_fail action "explode"`) {
		t.Fatalf("expected unsupported on_fail error, got %#v", report.Errors())
	}
}

func TestRun_RejectsMalformedConditionCELAfterRecognizedPrefix(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"test-node": {
				ID: "test-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": {
						Guard: &runtimecontracts.GuardSpec{Check: "entity.entity_id =="},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "condition_expression_validation", `CEL parse failed for "entity.entity_id =="`) {
		t.Fatalf("expected CEL parse failure, got %#v", report.Errors())
	}
}

func TestRun_RejectsFanOutNamespaceInGuardConditions(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"test-node": {
				ID: "test-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": {
						Guard: &runtimecontracts.GuardSpec{Check: "fan_out.count > 0"},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "condition_expression_validation", "fan_out.count > 0") {
		t.Fatalf("expected fan_out guard condition to fail validation, got %#v", report.Errors())
	}
}

func TestRun_RejectsItemNamespaceOutsideFilterConditions(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"test-node": {
				ID: "test-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": {
						Rules: []runtimecontracts.HandlerRuleEntry{{
							Condition: "item.score > 0",
						}},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "condition_expression_validation", "item.score > 0") {
		t.Fatalf("expected rule item condition to fail validation, got %#v", report.Errors())
	}
}

func TestRun_RejectsAccumulatedNamespaceInDataAccumulationExpressions(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		RootEntities: runtimecontracts.EntityContractsDocument{
			"tracking": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"expected_count": {Type: "integer"},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"test-node": {
				ID: "test-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": {
						DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
							Writes: []runtimecontracts.WorkflowDataWrite{
								{TargetField: "expected_count", Value: runtimecontracts.CELExpression("accumulated.size()")},
							},
						},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "data_accumulation_expression_validation", "accumulated.size()") {
		t.Fatalf("expected accumulated namespace in data_accumulation expression to fail validation, got %#v", report.Errors())
	}
}

func TestRun_RejectsAccumulatedNamespaceInEmitFieldExpressions(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"test-node": {
				ID: "test-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": {
						Emit: runtimecontracts.EmitSpec{
							Event: "item.scored",
							Fields: map[string]runtimecontracts.ExpressionValue{
								"bad": runtimecontracts.CELExpression("accumulated.size()"),
							},
						},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "emit_field_expression_validation", "accumulated.size()") {
		t.Fatalf("expected accumulated namespace in emit.fields expression to fail validation, got %#v", report.Errors())
	}
}

func TestRun_RejectsDisallowedRefNamespaceInEmitFieldExpressions(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"test-node": {
				ID: "test-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": {
						Emit: runtimecontracts.EmitSpec{
							Event: "item.scored",
							Fields: map[string]runtimecontracts.ExpressionValue{
								"bad": runtimecontracts.RefExpression("accumulated.count"),
							},
						},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "emit_field_expression_validation", "accumulated.count") {
		t.Fatalf("expected accumulated namespace in emit.fields ref to fail validation, got %#v", report.Errors())
	}
}

func TestRun_RejectsYAMLScalarEmitFieldsAsExpressions(t *testing.T) {
	var handler runtimecontracts.SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
emit:
  event: item.scored
  fields:
    bad: accumulated.size()
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"test-node": {
				ID: "test-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": handler,
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "emit_field_expression_validation", "accumulated.size()") {
		t.Fatalf("expected YAML scalar emit.fields expression to fail validation, got %#v", report.Errors())
	}
}

func TestRun_AcceptsYAMLScalarFanOutEmitItemExpressions(t *testing.T) {
	var handler runtimecontracts.SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
fan_out:
  items_from: payload.industries
  target: market-research-agent
  emit:
    event: market_research.industry_assigned
    fields:
      industry: item
      taxonomy_categories: "[item]"
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"market_research.industry_assigned": {
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"industry":            {Type: "text"},
						"taxonomy_categories": {Type: "text[]"},
					},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"scan-orchestrator": {
				ID: "scan-orchestrator",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"scan.requested": handler,
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if reportContains(report.Errors(), "emit_field_expression_validation", "item") {
		t.Fatalf("unexpected item expression validation error, got %#v", report.Errors())
	}
}

func TestRun_RejectsBareItemInHandlerEmitFields(t *testing.T) {
	var handler runtimecontracts.SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(`
emit:
  event: item.scored
  fields:
    bad: item
`), &handler); err != nil {
		t.Fatalf("yaml.Unmarshal: %v", err)
	}
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"test-node": {
				ID: "test-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": handler,
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "emit_field_expression_validation", "item") {
		t.Fatalf("expected bare item in handler emit.fields to fail validation, got %#v", report.Errors())
	}
}

func TestRun_RejectsBareItemInDataAccumulationExpressions(t *testing.T) {
	source := semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"test-node": {
				ID: "test-node",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"item.received": {
						DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
							Writes: []runtimecontracts.WorkflowDataWrite{
								{TargetField: "last_item", Value: runtimecontracts.CELExpression("item")},
							},
						},
					},
				},
			},
		},
	})

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "data_accumulation_expression_validation", "item") {
		t.Fatalf("expected bare item in data_accumulation expression to fail validation, got %#v", report.Errors())
	}
}

func TestRun_PreservesPermissionMismatchWarningsDuringMigration(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-permission-tool-mismatch")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "agent_permission_validation", "lookup_data") {
		t.Fatalf("expected agent_permission_validation warning, got %#v", report.Warnings())
	}
}

func TestRun_MapsReservedPlatformNamespaceToNamedCheck(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	bundle.Events["platform.forbidden"] = runtimecontracts.EventCatalogEntry{}
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "platform_namespace_violation", "reserved platform.* namespace") {
		t.Fatalf("expected platform_namespace_violation error, got %#v", report.Errors())
	}
}

func TestRun_MapsReservedPlatformNamespaceInAgentEmitEventsToNamedCheck(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	agent := bundle.Agents["intake-agent"]
	agent.EmitEvents = []string{"platform.forbidden"}
	bundle.Agents["intake-agent"] = agent
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "platform_namespace_violation", "emit_events references reserved platform.* namespace") {
		t.Fatalf("expected platform_namespace_violation error, got %#v", report.Errors())
	}
}

func TestRun_MapsInvalidNativeToolsToNamedCheck(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	agent := bundle.Agents["intake-agent"]
	agent.NativeTools = map[string]any{"mystery_tool": true, "bash": "yes"}
	bundle.Agents["intake-agent"] = agent
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Errors(), "native_tools_valid", "native_tools.mystery_tool") {
		t.Fatalf("expected native_tools_valid error for unknown capability, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "native_tools_valid", "native_tools.bash must be boolean") {
		t.Fatalf("expected native_tools_valid error for non-boolean value, got %#v", report.Errors())
	}
}

func TestRun_DoesNotRequireWebSearchFallbackPolicyForNativeTools(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	agent := bundle.Agents["intake-agent"]
	agent.NativeTools = map[string]any{"web_search": true}
	bundle.Agents["intake-agent"] = agent
	delete(bundle.Policy.Values, "web_search_provider")
	source := semanticview.Wrap(bundle)

	report := Run(context.Background(), source, Options{})

	for _, finding := range report.Errors() {
		if finding.CheckID != "native_tools_valid" {
			continue
		}
		if strings.Contains(finding.Message, "web_search_provider") {
			t.Fatalf("unexpected fallback policy error: %#v", report.Errors())
		}
	}
}

func TestRun_MapsProducesDriftToNamedWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-produces-drift")

	report := Run(context.Background(), source, Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "produces_drift", "outside produces list") {
		t.Fatalf("expected produces_drift warning, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForProducesDriftWhenDeclaredEventsMatchEmits(t *testing.T) {
	report := Run(context.Background(), semanticview.Wrap(bootverifyDeclarationDriftBundle()), Options{})

	if reportContains(report.Warnings(), "produces_drift", "outside produces list") {
		t.Fatalf("unexpected produces_drift warning for matching declaration/emission, got %#v", report.Warnings())
	}
}

func TestRun_MapsPhantomProducesToNamedWarning(t *testing.T) {
	bundle := bootverifyDeclarationDriftBundle()
	bundle.Nodes["producer"] = runtimecontracts.SystemNodeContract{
		Produces: []string{"task.done", "task.unused"},
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
			"task.start": {Emit: runtimecontracts.EmitSpec{Event: "task.done"}},
		},
		SubscribesTo: []string{"task.start"},
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if report.HasErrors() {
		t.Fatalf("expected warning-only report, got errors: %#v", report.Errors())
	}
	if !reportContains(report.Warnings(), "phantom_produces", "task.unused") {
		t.Fatalf("expected phantom_produces warning, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForPhantomProducesWhenDeclaredEventsMatchEmits(t *testing.T) {
	report := Run(context.Background(), semanticview.Wrap(bootverifyDeclarationDriftBundle()), Options{})

	if reportContains(report.Warnings(), "phantom_produces", "no handler emits") {
		t.Fatalf("unexpected phantom_produces warning for matching declaration/emission, got %#v", report.Warnings())
	}
}

func TestRun_ErrorsWhenEmitFieldsOmitRequiredEmittedField(t *testing.T) {
	bundle := bootverifyPayloadCompletenessBundle()
	node := bundle.Nodes["dispatcher"]
	handler := node.EventHandlers["scan.corpus_dispatch"]
	handler.Emit = runtimecontracts.EmitSpec{
		Event: "market_research.scan_assigned",
		Fields: map[string]runtimecontracts.ExpressionValue{
			"geography": runtimecontracts.RefExpression("payload.geography"),
			"mode":      runtimecontracts.RefExpression("payload.mode"),
		},
	}
	node.EventHandlers["scan.corpus_dispatch"] = handler
	bundle.Nodes["dispatcher"] = node
	bundle.Semantics.NodeHandlers["dispatcher"]["scan.corpus_dispatch"] = handler

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "scan_id is not statically provable") {
		t.Fatalf("expected payload completeness error for scan_id, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "emit.fields covers: geography, mode") {
		t.Fatalf("expected payload completeness error to mention emit.fields coverage, got %#v", report.Errors())
	}
}

func TestRun_ErrorsWithoutEmitFieldsEvenWhenContextSuggestsPassthrough(t *testing.T) {
	bundle := bootverifyPayloadCompletenessBundle()
	entry := bundle.Events["market_research.scan_assigned"]
	entry.Required = []string{"entity_id", "scan_id"}
	bundle.Events["market_research.scan_assigned"] = entry

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "scan_id is not statically provable") {
		t.Fatalf("expected payload completeness error for scan_id, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "emit.fields: absent") {
		t.Fatalf("expected payload completeness error to mention missing emit.fields, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "trigger schema declares scan_id: yes (required)") {
		t.Fatalf("expected payload completeness error to mention trigger schema context, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "entity schema declares scan_id: yes") {
		t.Fatalf("expected payload completeness error to mention entity schema context, got %#v", report.Errors())
	}
}

func TestRun_DoesNotWarnWhenEmitFieldsCoverRequiredPayload(t *testing.T) {
	bundle := bootverifyPayloadCompletenessBundle()
	node := bundle.Nodes["dispatcher"]
	handler := node.EventHandlers["scan.corpus_dispatch"]
	handler.Emit = runtimecontracts.EmitSpec{
		Event: "market_research.scan_assigned",
		Fields: map[string]runtimecontracts.ExpressionValue{
			"scan_id":   runtimecontracts.RefExpression("payload.scan_id"),
			"geography": runtimecontracts.RefExpression("payload.geography"),
		},
	}
	node.EventHandlers["scan.corpus_dispatch"] = handler
	bundle.Nodes["dispatcher"] = node
	bundle.Semantics.NodeHandlers["dispatcher"]["scan.corpus_dispatch"] = handler

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "semantic_drift_payload_completeness", "scan_id is not statically provable") {
		t.Fatalf("unexpected payload completeness error when transform covers required fields, got %#v", report.Errors())
	}
}

func TestRun_DoesNotWarnWhenEmitFieldsCoverRequiredPayloadAcrossExpressionKinds(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		fields map[string]runtimecontracts.ExpressionValue
	}{
		{
			name: "ref",
			fields: map[string]runtimecontracts.ExpressionValue{
				"scan_id": runtimecontracts.RefExpression("payload.scan_id"),
			},
		},
		{
			name: "cel",
			fields: map[string]runtimecontracts.ExpressionValue{
				"scan_id": runtimecontracts.CELExpression("payload.scan_id"),
			},
		},
		{
			name: "literal",
			fields: map[string]runtimecontracts.ExpressionValue{
				"scan_id": runtimecontracts.LiteralExpression("scan-1"),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bundle := bootverifyPayloadCompletenessBundle()
			node := bundle.Nodes["dispatcher"]
			handler := node.EventHandlers["scan.corpus_dispatch"]
			handler.Emit = runtimecontracts.EmitSpec{
				Event:  "market_research.scan_assigned",
				Fields: tc.fields,
			}
			node.EventHandlers["scan.corpus_dispatch"] = handler
			bundle.Nodes["dispatcher"] = node
			bundle.Semantics.NodeHandlers["dispatcher"]["scan.corpus_dispatch"] = handler

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if reportContains(report.Errors(), "semantic_drift_payload_completeness", "scan_id is not statically provable") {
				t.Fatalf("unexpected payload completeness error for %s transform form, got %#v", tc.name, report.Errors())
			}
		})
	}
}

func TestRun_ErrorsWhenRequiredPayloadContainsEnvelopeOwnedFields(t *testing.T) {
	bundle := bootverifyPayloadCompletenessBundle()
	entry := bundle.Events["market_research.scan_assigned"]
	entry.Required = []string{"entity_id", "current_state"}
	bundle.Events["market_research.scan_assigned"] = entry

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "entity_id is not statically provable") {
		t.Fatalf("expected payload completeness error for envelope-owned required field entity_id, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "current_state is not statically provable") {
		t.Fatalf("expected payload completeness error for envelope-owned required field current_state, got %#v", report.Errors())
	}
}

func TestRun_ErrorsWhenEmitFieldsAuthorEnvelopeOwnedFieldWithoutRequiredPayload(t *testing.T) {
	bundle := bootverifyPayloadCompletenessBundle()
	entry := bundle.Events["market_research.scan_assigned"]
	entry.Required = nil
	bundle.Events["market_research.scan_assigned"] = entry
	node := bundle.Nodes["dispatcher"]
	handler := node.EventHandlers["scan.corpus_dispatch"]
	handler.Emit = runtimecontracts.EmitSpec{
		Event: "market_research.scan_assigned",
		Fields: map[string]runtimecontracts.ExpressionValue{
			"entity_id": runtimecontracts.RefExpression("payload.scan_id"),
		},
	}
	node.EventHandlers["scan.corpus_dispatch"] = handler
	bundle.Nodes["dispatcher"] = node
	bundle.Semantics.NodeHandlers["dispatcher"]["scan.corpus_dispatch"] = handler

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "authors envelope-owned field entity_id in emit.fields") {
		t.Fatalf("expected authored envelope field error even without required payload fields, got %#v", report.Errors())
	}
}

func TestRun_ErrorsPerEmitSiteWhenSameEventIsUnderspecifiedOnOneRuleOnly(t *testing.T) {
	bundle := bootverifyPayloadCompletenessBundle()
	node := bundle.Nodes["dispatcher"]
	handler := node.EventHandlers["scan.corpus_dispatch"]
	handler.Emit = runtimecontracts.EmitSpec{}
	handler.Rules = []runtimecontracts.HandlerRuleEntry{
		{
			ID:        "complete",
			Condition: "payload.mode == 'full'",
			Emit: runtimecontracts.EmitSpec{
				Event: "market_research.scan_assigned",
				Fields: map[string]runtimecontracts.ExpressionValue{
					"scan_id": runtimecontracts.RefExpression("payload.scan_id"),
				},
			},
		},
		{
			ID:        "partial",
			Condition: "payload.mode == 'partial'",
			Emit: runtimecontracts.EmitSpec{
				Event: "market_research.scan_assigned",
				Fields: map[string]runtimecontracts.ExpressionValue{
					"geography": runtimecontracts.RefExpression("payload.geography"),
				},
			},
		},
	}
	node.EventHandlers["scan.corpus_dispatch"] = handler
	bundle.Nodes["dispatcher"] = node
	bundle.Semantics.NodeHandlers["dispatcher"]["scan.corpus_dispatch"] = handler

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "rules[partial].emit") {
		t.Fatalf("expected site-specific payload completeness error for partial rule, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "scan_id is not statically provable") {
		t.Fatalf("expected missing scan_id error for underspecified rule, got %#v", report.Errors())
	}
	if reportContains(report.Errors(), "semantic_drift_payload_completeness", "rules[complete].emit") {
		t.Fatalf("unexpected payload completeness error for fully specified rule, got %#v", report.Errors())
	}
}

func TestRun_ErrorsForOnCompleteEmitSitePayloadDrift(t *testing.T) {
	bundle := bootverifyPayloadCompletenessBundle()
	node := bundle.Nodes["dispatcher"]
	handler := node.EventHandlers["scan.corpus_dispatch"]
	handler.Emit = runtimecontracts.EmitSpec{}
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{
		{
			ID: "complete",
			Emit: runtimecontracts.EmitSpec{
				Event: "market_research.scan_assigned",
				Fields: map[string]runtimecontracts.ExpressionValue{
					"scan_id": runtimecontracts.RefExpression("payload.scan_id"),
				},
			},
		},
		{
			ID: "partial",
			Emit: runtimecontracts.EmitSpec{
				Event: "market_research.scan_assigned",
				Fields: map[string]runtimecontracts.ExpressionValue{
					"geography": runtimecontracts.RefExpression("payload.geography"),
				},
			},
		},
	}
	node.EventHandlers["scan.corpus_dispatch"] = handler
	bundle.Nodes["dispatcher"] = node
	bundle.Semantics.NodeHandlers["dispatcher"]["scan.corpus_dispatch"] = handler

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "on_complete[partial].emit") {
		t.Fatalf("expected on_complete payload completeness error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "scan_id is not statically provable") {
		t.Fatalf("expected missing scan_id error for underspecified on_complete branch, got %#v", report.Errors())
	}
	if reportContains(report.Errors(), "semantic_drift_payload_completeness", "on_complete[complete].emit") {
		t.Fatalf("unexpected payload completeness error for fully specified on_complete branch, got %#v", report.Errors())
	}
}

func TestRun_ErrorsForAccumulateOnTimeoutEmitSitePayloadDrift(t *testing.T) {
	bundle := bootverifyPayloadCompletenessBundle()
	node := bundle.Nodes["dispatcher"]
	handler := node.EventHandlers["scan.corpus_dispatch"]
	handler.Emit = runtimecontracts.EmitSpec{}
	handler.Accumulate = &runtimecontracts.AccumulateSpec{
		OnTimeout: &runtimecontracts.HandlerRuleEntry{
			ID: "timeout",
			Emit: runtimecontracts.EmitSpec{
				Event: "market_research.scan_assigned",
				Fields: map[string]runtimecontracts.ExpressionValue{
					"geography": runtimecontracts.RefExpression("payload.geography"),
				},
			},
		},
	}
	node.EventHandlers["scan.corpus_dispatch"] = handler
	bundle.Nodes["dispatcher"] = node
	bundle.Semantics.NodeHandlers["dispatcher"]["scan.corpus_dispatch"] = handler

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "accumulate.on_timeout[timeout].emit") {
		t.Fatalf("expected accumulate.on_timeout payload completeness error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "scan_id is not statically provable") {
		t.Fatalf("expected missing scan_id error for accumulate.on_timeout emit, got %#v", report.Errors())
	}
}

func TestRun_ErrorsForFanOutEmitSitePayloadDrift(t *testing.T) {
	bundle := bootverifyPayloadCompletenessBundle()
	node := bundle.Nodes["dispatcher"]
	handler := node.EventHandlers["scan.corpus_dispatch"]
	handler.Emit = runtimecontracts.EmitSpec{}
	handler.FanOut = &runtimecontracts.FanOutSpec{
		Emit: runtimecontracts.EmitSpec{
			Event: "market_research.scan_assigned",
			Fields: map[string]runtimecontracts.ExpressionValue{
				"geography": runtimecontracts.RefExpression("payload.geography"),
			},
		},
	}
	node.EventHandlers["scan.corpus_dispatch"] = handler
	bundle.Nodes["dispatcher"] = node
	bundle.Semantics.NodeHandlers["dispatcher"]["scan.corpus_dispatch"] = handler

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "handler.fan_out.emit") {
		t.Fatalf("expected fan_out payload completeness error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "scan_id is not statically provable") {
		t.Fatalf("expected missing scan_id error for fan_out emit, got %#v", report.Errors())
	}
}

func TestRun_ErrorsForGuardEscalatePayloadDrift(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier1-primitives", "test-guard-escalate"))
	entry := bundle.Events["check.escalated"]
	entry.Payload.Properties["reason"] = runtimecontracts.EventFieldSpec{Type: "string"}
	entry.Required = []string{"entity_id", "reason"}
	bundle.Events["check.escalated"] = entry

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "guard.on_fail.escalate") {
		t.Fatalf("expected guard escalation payload completeness error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "reason is not statically provable") {
		t.Fatalf("expected missing reason error for guard escalation emit, got %#v", report.Errors())
	}
}

func TestRun_ErrorsForGuardEscalateWhenRequiredPayloadContainsEnvelopeOwnedFields(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier1-primitives", "test-guard-escalate"))
	entry := bundle.Events["check.escalated"]
	entry.Required = []string{"entity_id"}
	bundle.Events["check.escalated"] = entry

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "guard.on_fail.escalate") {
		t.Fatalf("expected payload completeness error for guard escalation event schema that still requires envelope-owned payload fields, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "semantic_drift_payload_completeness", "entity_id is not statically provable") {
		t.Fatalf("expected envelope-owned entity_id drift for guard escalation, got %#v", report.Errors())
	}
}

func TestRun_ReportsInputPinWiringWarning(t *testing.T) {
	source := loadTier8Fixture(t, "test-boot-missing-pin")

	report := Run(context.Background(), source, Options{})

	if !reportContains(report.Warnings(), "input_pin_wiring", "task.feedback") {
		t.Fatalf("expected input_pin_wiring warning, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForExternalInputPinWithoutEmitter(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	entry := bundle.Events["task.feedback"]
	entry.Swarm.Source = "external"
	bundle.Events["task.feedback"] = entry

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "input_pin_wiring", "task.feedback") {
		t.Fatalf("unexpected input_pin_wiring warning for external input, got %#v", report.Warnings())
	}
}

func TestRun_ConstrainsExternalInputProducerPathToConsumingScope(t *testing.T) {
	root := writeInputPinExternalScopeFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	var (
		externalCleared bool
		plainWarned     bool
	)
	for _, finding := range report.Warnings() {
		if finding.CheckID != "input_pin_wiring" || !strings.Contains(finding.Message, "ticket.ready") {
			continue
		}
		switch finding.Location {
		case "external_consumer":
			externalCleared = true
		case "plain_consumer":
			plainWarned = true
		}
	}
	if externalCleared {
		t.Fatalf("unexpected input_pin_wiring warning for external_consumer, got %#v", report.Warnings())
	}
	if !plainWarned {
		t.Fatalf("expected input_pin_wiring warning for plain_consumer, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForSiblingFlowOutputPinInputProducerPath(t *testing.T) {
	root := writeCrossFlowPinAmbiguityFixture(t, false)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))
	bundle.Semantics.FlowOutputs["producer_b"] = nil

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "input_pin_wiring", "ticket.ready") {
		t.Fatalf("unexpected input_pin_wiring warning for sibling output pin proof, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForRootAgentEmitInputProducerPath(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	bundle.Agents["lifecycle-coordinator"] = runtimecontracts.AgentRegistryEntry{
		ID:         "lifecycle-coordinator",
		EmitEvents: []string{"task.feedback"},
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "input_pin_wiring", "task.feedback") {
		t.Fatalf("unexpected input_pin_wiring warning for root agent emit proof, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForRootNodeHandlerEmitInputProducerPath(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	node := bundle.Nodes["dispatcher"]
	node.EventHandlers["task.requested"] = runtimecontracts.SystemNodeEventHandler{
		Emit: runtimecontracts.EmitSpec{Event: "child/task.feedback"},
	}
	bundle.Nodes["dispatcher"] = node
	bundle.Semantics.NodeHandlers["dispatcher"]["task.requested"] = node.EventHandlers["task.requested"]

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "input_pin_wiring", "task.feedback") {
		t.Fatalf("unexpected input_pin_wiring warning for root handler emit proof, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForPlatformEventCatalogInputProducerPath(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	bundle.Platform.PlatformEvents.Catalog = map[string]yaml.Node{
		"platform.runtime_log": {},
	}
	renameFlowHandlerEvent(t, bundle, "child", "worker", "task.feedback", "platform.runtime_log", runtimecontracts.SystemNodeEventHandler{
		CreateEntity: true,
		AdvancesTo:   "done",
		Emit:         runtimecontracts.EmitSpec{Event: "task.result"},
	})

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "input_pin_wiring", "platform.runtime_log") {
		t.Fatalf("unexpected input_pin_wiring warning for platform event proof, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForSameFlowTimerInputProducerPath(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	node := bundle.Nodes["worker"]
	node.Timers = append(node.Timers, runtimecontracts.WorkflowTimerContract{
		ID:     "feedback-timeout",
		Event:  "task.feedback",
		FlowID: "child",
		NodeID: "worker",
	})
	bundle.Nodes["worker"] = node
	bundle.Semantics.Timers = append(bundle.Semantics.Timers, runtimecontracts.WorkflowTimerContract{
		ID:     "feedback-timeout",
		Event:  "task.feedback",
		FlowID: "child",
		NodeID: "worker",
	})

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "input_pin_wiring", "task.feedback") {
		t.Fatalf("unexpected input_pin_wiring warning for same-flow timer proof, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotUseProducesOrPlannedAsInputProducerPathProof(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		mutate func(*runtimecontracts.WorkflowContractBundle)
	}{
		{
			name: "produces only",
			mutate: func(bundle *runtimecontracts.WorkflowContractBundle) {
				node := bundle.Nodes["dispatcher"]
				node.Produces = append(node.Produces, "child/task.feedback")
				bundle.Nodes["dispatcher"] = node
			},
		},
		{
			name: "planned status",
			mutate: func(bundle *runtimecontracts.WorkflowContractBundle) {
				entry := bundle.Events["task.feedback"]
				entry.Swarm.Status = "planned"
				bundle.Events["task.feedback"] = entry
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
			tc.mutate(bundle)

			report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

			if !reportContains(report.Warnings(), "input_pin_wiring", "task.feedback") {
				t.Fatalf("expected input_pin_wiring warning for %s, got %#v", tc.name, report.Warnings())
			}
		})
	}
}

func TestRun_ReportsConflictingWritePinOwners(t *testing.T) {
	root := writeCrossFlowPinAmbiguityFixture(t, false)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))
	bundle.Semantics.FlowWrites["producer_a"] = []string{"ticket.status"}
	bundle.Semantics.FlowWrites["producer_b"] = []string{"ticket.status"}
	bundle.Semantics.WritePinOwners["ticket.status"] = []string{"producer_a", "producer_b"}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "write_pin_ownership_validation", "ticket.status") {
		t.Fatalf("expected write_pin_ownership_validation error, got %#v", report.Errors())
	}
}

func TestRun_DoesNotWarnForLocalizedCrossFlowEventRouting(t *testing.T) {
	root := writeLocalizedEventRoutingFixture(t)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "event_producer_exists", "consumer/ticket.ready") {
		t.Fatalf("unexpected event_producer_exists warning for localized flow input, got %#v", report.Warnings())
	}
	if reportContains(report.Warnings(), "event_consumer_exists", "producer/ticket.ready") {
		t.Fatalf("unexpected event_consumer_exists warning for localized producer output, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForFlowLocalEmittedEventsWithOwningFlowSchemas(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-local-events"))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "event_chain_integrity", "child/child.internal") {
		t.Fatalf("unexpected event_chain_integrity warning for child/child.internal, got %#v", report.Warnings())
	}
	if reportContains(report.Warnings(), "event_chain_integrity", "child/child.done") {
		t.Fatalf("unexpected event_chain_integrity warning for child/child.done, got %#v", report.Warnings())
	}
}

func TestRun_DoesNotWarnForFlowOwnedAgentEmissionsDeclaredAsFlowOutputs(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-required-agents-child"))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Warnings(), "event_consumer_exists", "analysis.done") {
		t.Fatalf("unexpected event_consumer_exists warning for analysis.done flow output, got %#v", report.Warnings())
	}
}

func TestRun_ReportsExpressionFieldReferenceWarning(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.Guard = &runtimecontracts.GuardSpec{Check: "entity.missing_score >= 70"}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "expression_field_reference_validation", "entity.missing_score") {
		t.Fatalf("expected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsGuardReferenceToDeclaredFieldEvenWhenHandlerClearsItLater(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Guard = &runtimecontracts.GuardSpec{Check: "entity.revision_count > 0"}
	handler.Clear = &runtimecontracts.ClearSpec{Target: "revision_count"}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.revision_count") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_RejectsGuardReferenceToSparseFieldEvenWhenSameHandlerWritesFieldLater(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.Guard = &runtimecontracts.GuardSpec{Check: "entity.missing_score >= 70"}
	handler.DataAccumulation.Writes = append(handler.DataAccumulation.Writes, runtimecontracts.WorkflowDataWrite{
		TargetField: "missing_score",
		SourceField: "score",
	})
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "expression_field_reference_validation", "entity.missing_score") {
		t.Fatalf("expected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestHandlerEntityFieldWriters_TracksSetsGateAndClearTargets(t *testing.T) {
	handler := runtimecontracts.SystemNodeEventHandler{
		SetsGate: &runtimecontracts.GateSpec{Name: "approved", Value: true},
		Clear: &runtimecontracts.ClearSpec{
			Target:  "revision_count",
			Targets: []string{"entity.base_score"},
		},
	}

	writers := handlerEntityFieldWriters(handler)
	if _, ok := writers["gates"]; !ok {
		t.Fatalf("gates missing from handler writers: %#v", writers)
	}
	if _, ok := writers["revision_count"]; !ok {
		t.Fatalf("revision_count missing from handler writers: %#v", writers)
	}
	if _, ok := writers["base_score"]; !ok {
		t.Fatalf("base_score missing from handler writers: %#v", writers)
	}
}

func TestRun_RejectsFilterReferenceToSparseFieldEvenWhenSameHandlerComputesFieldLater(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.Filter = &runtimecontracts.FilterSpec{
		Source:    "payload.items",
		ItemsFrom: "payload.items",
		Condition: "entity.missing_filtered_score >= 70",
		StoreAs:   "entity.filtered_items",
	}
	handler.Compute = &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpCount,
		StoreAs:   "entity.missing_filtered_score",
	}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "expression_field_reference_validation", "entity.missing_filtered_score") {
		t.Fatalf("expected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_AttributesReaderCoverageToResolvedRootContractOwner(t *testing.T) {
	bundle := loadWave1RootReaderCoverageFixtureBundle(t)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.LintEvidence(), "entity_reader_coverage", "flow root entity_type case declares field priority") {
		t.Fatalf("unexpected root entity_reader_coverage lint, got %#v", report.LintEvidence())
	}
}

func TestRun_EntityWriterCoverageCountsExplicitAgentEntityWritesList(t *testing.T) {
	root := writePromptWriterCoverageFixture(t, `
writer:
  id: writer
  role: writer
  prompt_ref: writer
  workspace_class: factory
  manager_fallback: ops
  entity_writes:
    case:
      save:
      - business_brief
`, `
case:
  business_brief:
    type: text
  untouched:
    type: text
    _unused_reason: prompt coverage proof
`, "")
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "entity_writer_coverage", "business_brief") {
		t.Fatalf("unexpected entity_writer_coverage error, got %#v", report.Errors())
	}
}

func TestRun_ReportsPromptCreateEntityWithoutEntityWritesAuthorization(t *testing.T) {
	root := writePromptWriterCoverageFixture(t, `
writer:
  id: writer
  role: writer
  prompt_ref: writer
  workspace_class: factory
  manager_fallback: ops
`, `
case:
  business_brief:
    type: text
    initial: seeded
`, "Call create_entity using the delivered schema.\n")
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "entity_writer_coverage", "prompt declares create_entity") {
		t.Fatalf("expected prompt create_entity authorization error, got %#v", report.Errors())
	}
}

func TestRun_ReportsPromptSaveEntityFieldWithoutMatchingEntityWritesAuthorization(t *testing.T) {
	root := writePromptWriterCoverageFixture(t, `
writer:
  id: writer
  role: writer
  prompt_ref: writer
  workspace_class: factory
  manager_fallback: ops
  entity_writes:
    case:
      save:
      - research_context
`, `
case:
  business_brief:
    type: text
    _unused_reason: prompt save auth proof
  research_context:
    type: text
    _unused_reason: prompt save auth proof
`, "Use save_entity_field for `business_brief`.\n")
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "entity_writer_coverage", "business_brief") {
		t.Fatalf("expected prompt save_entity_field authorization error, got %#v", report.Errors())
	}
}

func TestRun_PromptEntityWritesPrefersFlowScopedAuthorization(t *testing.T) {
	root := writePromptWriterCoverageFixture(t, `
writer:
  id: writer
  type: factory
  role: writer
  prompt_ref: writer
  model: regular
  conversation_mode: task
  subscriptions: []
  entity_writes:
    case:
      save:
      - research_context
    child.case:
      save:
      - business_brief
`, `
case:
  business_brief:
    type: text
    _unused_reason: scoped auth precedence proof
  research_context:
    type: text
    _unused_reason: scoped auth precedence proof
`, "Use `save_entity_field` for `business_brief`.\n")
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "entity_writer_coverage", "business_brief") {
		t.Fatalf("unexpected entity_writer_coverage error, got %#v", report.Errors())
	}
}

func TestRun_EntityWriterCoverageCountsExplicitAgentEntityWritesForScopedDuplicateIDs(t *testing.T) {
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: duplicate-agent-writer-coverage
version: "1.0.0"
platform: ">=1.6.0"
flows:
  - id: alpha
    flow: alpha
    mode: static
  - id: beta
    flow: beta
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: duplicate-agent-writer-coverage\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")

	for _, flowID := range []string{"alpha", "beta"} {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), "name: "+flowID+"\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "policy.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "nodes.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "entities.yaml"), `
case:
  business_brief:
    type: text
`)
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "agents.yaml"), `
writer:
  id: writer
  type: factory
  role: writer
  prompt_ref: writer
  model: regular
  conversation_mode: task
  subscriptions: []
  entity_writes:
    case:
      save:
      - business_brief
`)
	}

	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "entity_writer_coverage", "flow alpha entity_type case declares field business_brief") {
		t.Fatalf("unexpected alpha entity_writer_coverage error, got %#v", report.Errors())
	}
	if reportContains(report.Errors(), "entity_writer_coverage", "flow beta entity_type case declares field business_brief") {
		t.Fatalf("unexpected beta entity_writer_coverage error, got %#v", report.Errors())
	}
}

func TestRun_AllowsExpressionFieldReferenceForDeclaredFieldWrittenBySiblingStep(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{
		{
			TargetField: "base_score",
			SourceField: "score",
		},
		{
			TargetField: "adjusted_score",
			Value:       runtimecontracts.CELExpression("entity.base_score + 1"),
		},
	}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.base_score") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsExpressionFieldReferenceForSelfTargetEntityUpdate(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier9-composition-patterns", "test-compose-guard-counter-escalate"))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.retry_count") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "expression_field_reference_validation", "entity.retry_count") {
		t.Fatalf("unexpected expression_field_reference_validation warning, got %#v", report.Warnings())
	}
}

func TestRun_AllowsGuardReferenceToPersistedFieldEvenWhenHandlerWritesFieldLater(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.Guard = &runtimecontracts.GuardSpec{Check: "entity.retry_count < 3"}
	handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{{
		TargetField: "retry_count",
		Value:       runtimecontracts.CELExpression("entity.retry_count + 1"),
	}}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.retry_count") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "expression_field_reference_validation", "entity.retry_count") {
		t.Fatalf("unexpected expression_field_reference_validation warning, got %#v", report.Warnings())
	}
}

func TestRun_AllowsOnCompleteReferenceToPersistedFieldEvenWhenHandlerWritesFieldLater(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{{
		TargetField: "revision_count",
		Value:       runtimecontracts.CELExpression("entity.revision_count + 1"),
	}}
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{{
		ID:        "retry",
		Condition: "entity.revision_count < 3",
	}}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.revision_count") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "expression_field_reference_validation", "entity.revision_count") {
		t.Fatalf("unexpected expression_field_reference_validation warning, got %#v", report.Warnings())
	}
}

func TestRun_AllowsCreateEntityGuardReferenceToSchemaInitializedField(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Guard = &runtimecontracts.GuardSpec{Check: "entity.revision_count == 0"}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.revision_count") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsSparsePresenceChecksWithoutInitializer(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Guard = &runtimecontracts.GuardSpec{Check: "has(entity.kill_reason) || entity.kill_reason == null"}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.kill_reason") {
		t.Fatalf("unexpected sparse-field validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsHasGuardedTernaryReadWithoutInitializer(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Guard = &runtimecontracts.GuardSpec{Check: `has(entity.kill_reason) ? entity.kill_reason == "manual" : true`}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.kill_reason") {
		t.Fatalf("unexpected guarded ternary sparse-field validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsCreateEntityGuardReferenceToDeclaredFieldEvenWhenSameHandlerAlsoWritesIt(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Guard = &runtimecontracts.GuardSpec{Check: "entity.revision_count == 0"}
	handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{{
		TargetField: "revision_count",
		Value:       runtimecontracts.LiteralExpression(0),
	}}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.revision_count") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsCreateEntityEmitFieldReadOfSameHandlerTopLevelWrite(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{{
		TargetField: "revision_count",
		Value:       runtimecontracts.LiteralExpression(0),
	}}
	handler.Emit = runtimecontracts.EmitSpec{
		Event: "testing.revision_count_read",
		Fields: map[string]runtimecontracts.ExpressionValue{
			"revision_count": runtimecontracts.CELExpression("entity.revision_count"),
		},
	}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.revision_count") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsCreateEntityEmitFieldReadOfDeclaredFieldEvenWhenOnlyRuleWritesIt(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Rules = []runtimecontracts.HandlerRuleEntry{{
		Condition: "entity.entity_id != null",
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{{
				TargetField: "revision_count",
				Value:       runtimecontracts.LiteralExpression(0),
			}},
		},
	}}
	handler.Emit = runtimecontracts.EmitSpec{
		Event: "testing.revision_count_read",
		Fields: map[string]runtimecontracts.ExpressionValue{
			"revision_count": runtimecontracts.CELExpression("entity.revision_count"),
		},
	}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.revision_count") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsCreateEntityEmitFieldReadOfDeclaredFieldEvenWhenOnlyRuleComputeWritesIt(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Rules = []runtimecontracts.HandlerRuleEntry{{
		Condition: "entity.entity_id != null",
		Compute: &runtimecontracts.ComputeSpec{
			Operation: runtimecontracts.ComputeOpCount,
			StoreAs:   "entity.revision_count",
		},
	}}
	handler.Emit = runtimecontracts.EmitSpec{
		Event: "testing.revision_count_read",
		Fields: map[string]runtimecontracts.ExpressionValue{
			"revision_count": runtimecontracts.CELExpression("entity.revision_count"),
		},
	}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.revision_count") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsCreateEntityEmitFieldReadWhenRuleAlsoWritesUnconditionallyAvailableField(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Compute = &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpCount,
		StoreAs:   "entity.revision_count",
	}
	handler.Rules = []runtimecontracts.HandlerRuleEntry{{
		Condition: "entity.entity_id != null",
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{{
				TargetField: "revision_count",
				Value:       runtimecontracts.LiteralExpression(0),
			}},
		},
	}}
	handler.Emit = runtimecontracts.EmitSpec{
		Event: "testing.revision_count_read",
		Fields: map[string]runtimecontracts.ExpressionValue{
			"revision_count": runtimecontracts.CELExpression("entity.revision_count"),
		},
	}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.revision_count") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsDeclaredFieldReadWhenSameHandlerAlsoWritesIt(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{
		{
			TargetField: "base_score",
			Value:       runtimecontracts.CELExpression("entity.base_score + 1"),
		},
		{
			TargetField: "adjusted_score",
			Value:       runtimecontracts.CELExpression("entity.base_score + 1"),
		},
	}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.base_score") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_RejectsUndeclaredFieldReadEvenWhenSiblingWriteAlsoExists(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.Guard = &runtimecontracts.GuardSpec{Check: "entity.missing_retry_count < 3"}
	handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{
		{
			TargetField: "missing_retry_count",
			Value:       runtimecontracts.CELExpression("entity.missing_retry_count + 1"),
		},
		{
			TargetField: "missing_adjusted_score",
			Value:       runtimecontracts.CELExpression("entity.missing_retry_count + entity.missing_base_score"),
		},
		{
			TargetField: "missing_base_score",
			SourceField: "score",
		},
	}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "expression_field_reference_validation", "entity.missing_retry_count") {
		t.Fatalf("expected undeclared-field guard error, got %#v", report.Errors())
	}
	if !reportContains(report.Errors(), "expression_field_reference_validation", "entity.missing_base_score") {
		t.Fatalf("expected undeclared sibling-read error, got %#v", report.Errors())
	}
}

func TestRun_AllowsTopLevelDataAccumulationExpressionToReadRuleProducedField(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.Rules = []runtimecontracts.HandlerRuleEntry{{
		Condition: "payload.score >= 70",
		DataAccumulation: runtimecontracts.WorkflowDataAccumulation{
			Writes: []runtimecontracts.WorkflowDataWrite{{
				TargetField: "base_score",
				SourceField: "score",
			}},
		},
	}}
	handler.DataAccumulation.Writes = []runtimecontracts.WorkflowDataWrite{{
		TargetField: "adjusted_score",
		Value:       runtimecontracts.CELExpression("entity.base_score + 1"),
	}}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.base_score") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_SuppressesExpressionFieldReferenceFindingWhenComputeMakesFieldAvailableBeforeOnComplete(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.Compute = &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpCount,
		StoreAs:   "entity.composite_score",
	}
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{{
		Condition: "entity.composite_score >= 0",
	}}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "expression_field_reference_validation", "entity.composite_score") {
		t.Fatalf("unexpected expression_field_reference_validation error, got %#v", report.Errors())
	}
	if reportContains(report.Warnings(), "expression_field_reference_validation", "entity.composite_score") {
		t.Fatalf("unexpected expression_field_reference_validation warning, got %#v", report.Warnings())
	}
}

func TestRun_RejectsCreateEntityAccumulateWhenDynamicComputeProofWouldOtherwiseWarn(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Accumulate = &runtimecontracts.AccumulateSpec{ExpectedFrom: "entity.expected_count"}
	handler.Compute = &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpCount,
		StoreAs:   "entity.composite_score",
	}
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{{
		Condition: "entity.composite_score >= 0",
	}}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "dialect_compliance", "declares both create_entity and accumulate") {
		t.Fatalf("expected dialect_compliance create_entity/accumulate error, got %#v", report.Errors())
	}
}

func TestRun_RejectsCreateEntityAccumulateWhenExpectedFromIsNotDynamicEntityField(t *testing.T) {
	bundle := loadWave1ExpressionFixtureBundle(t)
	flowID, nodeID, eventType, handler := firstFlowHandlerInFlowView(t, bundle)
	handler.CreateEntity = true
	handler.Accumulate = &runtimecontracts.AccumulateSpec{Threshold: 1}
	handler.Compute = &runtimecontracts.ComputeSpec{
		Operation: runtimecontracts.ComputeOpCount,
		StoreAs:   "entity.composite_score",
	}
	handler.OnComplete = []runtimecontracts.HandlerRuleEntry{{
		Condition: "entity.composite_score >= 0",
	}}
	writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "dialect_compliance", "declares both create_entity and accumulate") {
		t.Fatalf("expected dialect_compliance create_entity/accumulate error, got %#v", report.Errors())
	}
}

func TestRun_RequiresCreateEntityForStatefulInputPinHandlers(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-pin-wiring"))
	flowID := "child"
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	for nodeID, node := range flowView.Nodes {
		for eventType, handler := range node.EventHandlers {
			handler.CreateEntity = false
			writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)
		}
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "flow_boundary_create_entity_validation", "must declare create_entity: true") {
		t.Fatalf("expected flow_boundary_create_entity_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsCreateEntityForStatefulInputPinHandlers(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-pin-wiring"))
	flowID := "child"
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	for nodeID, node := range flowView.Nodes {
		for eventType, handler := range node.EventHandlers {
			handler.CreateEntity = true
			writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)
		}
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "flow_boundary_create_entity_validation", "must declare create_entity: true") {
		t.Fatalf("unexpected flow_boundary_create_entity_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsSelectEntityForStatefulInputPinHandlers(t *testing.T) {
	root := writeSelectEntityInputPinFixture(t, `
treasury-node:
  id: treasury-node
  execution_type: system_node
  subscribes_to: [opco.spend_requested]
  event_handlers:
    opco.spend_requested:
      select_entity:
        by:
          vertical_id: payload.vertical_id
      data_accumulation:
        writes:
          - source_field: amount_usd
            target_field: spent_usd
`)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "flow_boundary_create_entity_validation", "must declare create_entity: true") {
		t.Fatalf("unexpected flow_boundary_create_entity_validation error, got %#v", report.Errors())
	}
	if reportContains(report.Errors(), "select_entity_validation", "") {
		t.Fatalf("unexpected select_entity_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsSelectOrCreateEntityForStatefulInputPinHandlers(t *testing.T) {
	root := writeSelectEntityInputPinFixture(t, `
treasury-node:
  id: treasury-node
  execution_type: system_node
  subscribes_to: [opco.spend_requested]
  event_handlers:
    opco.spend_requested:
      select_or_create_entity:
        by:
          vertical_id: payload.vertical_id
      data_accumulation:
        writes:
          - source_field: amount_usd
            target_field: spent_usd
`)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "flow_boundary_create_entity_validation", "must declare create_entity: true") {
		t.Fatalf("unexpected flow_boundary_create_entity_validation error, got %#v", report.Errors())
	}
	if reportContains(report.Errors(), "select_entity_validation", "") {
		t.Fatalf("unexpected select_entity_validation error, got %#v", report.Errors())
	}
}

func TestRun_RejectsSelectEntityWithSourceEnvelopeAuthority(t *testing.T) {
	root := writeSelectEntityInputPinFixture(t, `
treasury-node:
  id: treasury-node
  execution_type: system_node
  subscribes_to: [opco.spend_requested]
  event_handlers:
    opco.spend_requested:
      select_entity:
        by:
          vertical_id: payload.entity_id
`)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "select_entity_validation", "must not use source envelope authority") {
		t.Fatalf("expected select_entity source authority error, got %#v", report.Errors())
	}
}

func TestRun_RejectsSelectOrCreateEntityWithSourceEnvelopeAuthority(t *testing.T) {
	root := writeSelectEntityInputPinFixture(t, `
treasury-node:
  id: treasury-node
  execution_type: system_node
  subscribes_to: [opco.spend_requested]
  event_handlers:
    opco.spend_requested:
      select_or_create_entity:
        by:
          vertical_id: payload.entity_id
`)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "select_entity_validation", "select_or_create_entity") || !reportContains(report.Errors(), "select_entity_validation", "must not use source envelope authority") {
		t.Fatalf("expected select_or_create_entity source authority error, got %#v", report.Errors())
	}
}

func TestRun_RejectsSelectEntityWithEnvelopeTargetField(t *testing.T) {
	root := writeSelectEntityInputPinFixture(t, `
treasury-node:
  id: treasury-node
  execution_type: system_node
  subscribes_to: [opco.spend_requested]
  event_handlers:
    opco.spend_requested:
      select_entity:
        by:
          entity_id: payload.vertical_id
`)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "select_entity_validation", "is not an entity contract field selection target") {
		t.Fatalf("expected select_entity envelope target field error, got %#v", report.Errors())
	}
}

func TestRun_RejectsSelectOrCreateEntityWithUndeclaredPayloadRef(t *testing.T) {
	root := writeSelectEntityInputPinFixture(t, `
treasury-node:
  id: treasury-node
  execution_type: system_node
  subscribes_to: [opco.spend_requested]
  event_handlers:
    opco.spend_requested:
      select_or_create_entity:
        by:
          vertical_id: payload.missing_vertical_id
`)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "select_entity_validation", "select_or_create_entity") || !reportContains(report.Errors(), "select_entity_validation", "references undeclared payload field") {
		t.Fatalf("expected select_or_create_entity undeclared payload field error, got %#v", report.Errors())
	}
}

func TestRun_RejectsSelectEntityWithUndeclaredPayloadRef(t *testing.T) {
	root := writeSelectEntityInputPinFixture(t, `
treasury-node:
  id: treasury-node
  execution_type: system_node
  subscribes_to: [opco.spend_requested]
  event_handlers:
    opco.spend_requested:
      select_entity:
        by:
          vertical_id: payload.missing_vertical_id
`)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "select_entity_validation", "references undeclared payload field") {
		t.Fatalf("expected select_entity undeclared payload field error, got %#v", report.Errors())
	}
}

func TestRun_RejectsSelectEntityWithCreateEntity(t *testing.T) {
	root := writeSelectEntityInputPinFixture(t, `
treasury-node:
  id: treasury-node
  execution_type: system_node
  subscribes_to: [opco.spend_requested]
  event_handlers:
    opco.spend_requested:
      create_entity: true
      select_entity:
        by:
          vertical_id: payload.vertical_id
`)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "select_entity_validation", "must not declare create_entity with select_entity or select_or_create_entity") {
		t.Fatalf("expected create_entity/select_entity error, got %#v", report.Errors())
	}
}

func TestRun_RejectsSelectOrCreateEntityWithCreateEntity(t *testing.T) {
	root := writeSelectEntityInputPinFixture(t, `
treasury-node:
  id: treasury-node
  execution_type: system_node
  subscribes_to: [opco.spend_requested]
  event_handlers:
    opco.spend_requested:
      create_entity: true
      select_or_create_entity:
        by:
          vertical_id: payload.vertical_id
`)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "select_entity_validation", "must not declare create_entity with select_entity or select_or_create_entity") {
		t.Fatalf("expected create_entity/select_or_create_entity error, got %#v", report.Errors())
	}
}

func TestRun_RejectsSelectEntityWithSelectOrCreateEntity(t *testing.T) {
	root := writeSelectEntityInputPinFixture(t, `
treasury-node:
  id: treasury-node
  execution_type: system_node
  subscribes_to: [opco.spend_requested]
  event_handlers:
    opco.spend_requested:
      select_entity:
        by:
          vertical_id: payload.vertical_id
      select_or_create_entity:
        by:
          vertical_id: payload.vertical_id
`)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "select_entity_validation", "must not declare both select_entity and select_or_create_entity") {
		t.Fatalf("expected select_entity/select_or_create_entity error, got %#v", report.Errors())
	}
}

func TestRun_AllowsTemplateFlowInputPinHandlersWithoutCreateEntity(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-pin-wiring"))
	flowID := "child"
	schema, ok := bundle.FlowSchemas[flowID]
	if !ok {
		t.Fatalf("flow schema %s missing", flowID)
	}
	schema.Mode = "template"
	bundle.FlowSchemas[flowID] = schema
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	for nodeID, node := range flowView.Nodes {
		for eventType, handler := range node.EventHandlers {
			handler.CreateEntity = false
			writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)
		}
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "flow_boundary_create_entity_validation", "must declare create_entity: true") {
		t.Fatalf("unexpected flow_boundary_create_entity_validation error, got %#v", report.Errors())
	}
}

func TestRun_ReportsCrossFlowPinAmbiguityWithoutEscapeHatch(t *testing.T) {
	root := writeCrossFlowPinAmbiguityFixture(t, false)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "cross_flow_pin_ambiguity_validation", "ticket.ready") {
		t.Fatalf("expected cross_flow_pin_ambiguity_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsCrossFlowPinAmbiguityWithScopedEscapeHatch(t *testing.T) {
	root := writeCrossFlowPinAmbiguityFixture(t, true)
	bundle := loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "cross_flow_pin_ambiguity_validation", "ticket.ready") {
		t.Fatalf("unexpected cross_flow_pin_ambiguity_validation error, got %#v", report.Errors())
	}
}

func TestRun_AllowsStatelessFlowInputPinHandlersWithoutCreateEntity(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-pin-wiring"))
	flowID := "child"
	schema, ok := bundle.FlowSchemas[flowID]
	if !ok {
		t.Fatalf("flow schema %s missing", flowID)
	}
	schema.InitialState = ""
	bundle.FlowSchemas[flowID] = schema
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	for nodeID, node := range flowView.Nodes {
		for eventType, handler := range node.EventHandlers {
			handler.CreateEntity = false
			writeFlowHandler(t, bundle, flowID, nodeID, eventType, handler)
		}
	}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "flow_boundary_create_entity_validation", "must declare create_entity: true") {
		t.Fatalf("unexpected flow_boundary_create_entity_validation error, got %#v", report.Errors())
	}
}

func TestRun_RequiresCreateEntityForBackpropInputPinHandlers(t *testing.T) {
	bundle := loadFixtureBundle(t, filepath.Join("tests", "tier11-flow-composition", "test-child-flow-pin-wiring"))
	flowID := "child"
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	var (
		nodeID    string
		eventType string
		handler   runtimecontracts.SystemNodeEventHandler
		found     bool
	)
	for candidateNodeID, node := range flowView.Nodes {
		for candidateEventType, candidateHandler := range node.EventHandlers {
			nodeID = candidateNodeID
			eventType = candidateEventType
			handler = candidateHandler
			found = true
			break
		}
		if found {
			break
		}
	}
	if !found {
		t.Fatal("expected child flow handler")
	}
	newEventType := "child.killed_backprop"
	handler.CreateEntity = false
	renameFlowHandlerEvent(t, bundle, flowID, nodeID, eventType, newEventType, handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "flow_boundary_create_entity_validation", "must declare create_entity: true") {
		t.Fatalf("expected flow_boundary_create_entity_validation error, got %#v", report.Errors())
	}
}

func TestRun_UsesCompiledOwnersForEquivalentSingleNodePerEventRoutes(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	rootNode := bundle.Nodes["dispatcher"]
	rootNode.EventHandlers["child/task.feedback"] = runtimecontracts.SystemNodeEventHandler{}
	bundle.Nodes["dispatcher"] = rootNode
	bundle.Semantics.NodeHandlers["dispatcher"]["child/task.feedback"] = runtimecontracts.SystemNodeEventHandler{}

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "single_node_per_event", "child/task.feedback") {
		t.Fatalf("expected single_node_per_event error, got %#v", report.Errors())
	}
}

func TestRun_ReportsExactDuplicateSingleNodePerEventOwnership(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-success")
	handler := bundle.Nodes["complete-task"].EventHandlers["task.requested"]
	addProjectHandler(t, bundle, "shadow-complete-task", "task.requested", handler)

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "single_node_per_event", "task.requested") {
		t.Fatalf("expected single_node_per_event duplicate-owner error, got %#v", report.Errors())
	}
}

func TestRun_DoesNotReportSingleNodePerEventForWildcardOnlyOverlap(t *testing.T) {
	bundle := loadTier8FixtureBundle(t, "test-boot-missing-pin")
	addProjectHandler(t, bundle, "dispatcher-shadow", "child/*", runtimecontracts.SystemNodeEventHandler{})

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if reportContains(report.Errors(), "single_node_per_event", "child/task.feedback") {
		t.Fatalf("unexpected single_node_per_event wildcard collision, got %#v", report.Errors())
	}
}

func TestRun_ReportsMissingTransitionTriggerEvent(t *testing.T) {
	bundle := bootverifyTransitionRuntimeOwnershipBundle()
	bundle.Semantics.Transitions[0].Trigger = "ticket.missing"

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "transition_reference_validation", "trigger ticket.missing missing from event catalog") {
		t.Fatalf("expected transition_reference_validation error, got %#v", report.Errors())
	}
}

func TestRun_ReportsTransitionOwnershipMismatch(t *testing.T) {
	bundle := bootverifyTransitionRuntimeOwnershipBundle()
	bundle.Semantics.Transitions[0].Node = "projector"

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "transition_ownership_validation", "workflow owner is projector") {
		t.Fatalf("expected transition_ownership_validation error, got %#v", report.Errors())
	}
}

func TestRun_ReportsMissingSemanticHandlerForOwnedRuntimeEvent(t *testing.T) {
	bundle := bootverifyTransitionRuntimeOwnershipBundle()
	event := bundle.Events["ticket.opened"]
	event.OwningNode = "dispatcher"
	bundle.Events["ticket.opened"] = event

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "event_runtime_wiring_validation", "owning_node dispatcher missing semantic event_handler") {
		t.Fatalf("expected event_runtime_wiring_validation error, got %#v", report.Errors())
	}
}

func TestRun_ReportsMissingRuntimeExecutorForOwnedRuntimeEvent(t *testing.T) {
	bundle := bootverifyTransitionRuntimeOwnershipBundle()
	bundle.Nodes["idle-owner"] = runtimecontracts.SystemNodeContract{ID: "idle-owner"}
	event := bundle.Events["ticket.audit"]
	event.RuntimeHandling = "projection"
	event.OwningNode = "idle-owner"
	bundle.Events["ticket.audit"] = event

	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})

	if !reportContains(report.Errors(), "handler_field_compliance", "event ticket.audit owning_node idle-owner has no runtime executor") {
		t.Fatalf("expected handler_field_compliance runtime executor error, got %#v", report.Errors())
	}
}

func TestBootCheckRegistry_HasSpecCheckCount(t *testing.T) {
	if got := len(bootCheckRegistry); got != 64 {
		t.Fatalf("bootCheckRegistry count = %d, want 64", got)
	}
	if got := len(supplementalChecks); got != 3 {
		t.Fatalf("supplementalChecks count = %d, want 3", got)
	}
}

func TestRun_ReportsErrorForUnprefixedTimerStartOn(t *testing.T) {
	root := writeTimerValidationFixture(t, "ticket.opened", "")
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	report := Run(context.Background(), semanticview.Wrap(loadFixtureBundleAt(t, repoRoot, root, platformSpec)), Options{})
	if !reportContains(report.Errors(), "timer_validation", "start_on") {
		t.Fatalf("expected timer_validation start_on error, got %#v", report.Errors())
	}
}

func TestRun_ReportsErrorForTimerCancelOnBoot(t *testing.T) {
	root := writeTimerValidationFixture(t, "event:ticket.opened", "boot")
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	report := Run(context.Background(), semanticview.Wrap(loadFixtureBundleAt(t, repoRoot, root, platformSpec)), Options{})
	if !reportContains(report.Errors(), "timer_validation", "cancel_on") {
		t.Fatalf("expected timer_validation cancel_on error, got %#v", report.Errors())
	}
}

func TestRun_ReportsErrorForUnknownTimerTriggerState(t *testing.T) {
	root := writeTimerValidationFixture(t, "state:missing_state", "")
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	report := Run(context.Background(), semanticview.Wrap(loadFixtureBundleAt(t, repoRoot, root, platformSpec)), Options{})
	if !reportContains(report.Errors(), "timer_validation", "unknown state") {
		t.Fatalf("expected timer_validation unknown state error, got %#v", report.Errors())
	}
}

func TestRun_ReportsWarningForUnknownTimerTriggerEvent(t *testing.T) {
	root := writeTimerValidationFixture(t, "event:ticket.unknown", "")
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	report := Run(context.Background(), semanticview.Wrap(loadFixtureBundleAt(t, repoRoot, root, platformSpec)), Options{})
	if !reportContains(report.Warnings(), "timer_validation", "unknown event") {
		t.Fatalf("expected timer_validation unknown event warning, got %#v", report.Warnings())
	}
}

func TestRun_ReportsErrorForTimerMissingOwner(t *testing.T) {
	root := writeTimerValidationFixture(t, "event:ticket.opened", "")
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle := loadFixtureBundleAt(t, repoRoot, root, platformSpec)
	bundle.Semantics.Timers[0].Owner = ""
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "timer_validation", "missing owner") {
		t.Fatalf("expected timer_validation missing owner error, got %#v", report.Errors())
	}
}

func TestRun_ReportsErrorForTimerOwnerMissingFromParticipants(t *testing.T) {
	root := writeTimerValidationFixture(t, "event:ticket.opened", "")
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle := loadFixtureBundleAt(t, repoRoot, root, platformSpec)
	bundle.Semantics.Timers[0].Owner = "missing-owner"
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "timer_validation", "owner missing-owner missing from participants") {
		t.Fatalf("expected timer_validation missing participant owner error, got %#v", report.Errors())
	}
}

func TestRun_ReportsErrorForTimerEventMissingFromCatalog(t *testing.T) {
	root := writeTimerValidationFixture(t, "event:ticket.opened", "")
	repoRoot := repoRootForBootverifyTest(t)
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle := loadFixtureBundleAt(t, repoRoot, root, platformSpec)
	bundle.Semantics.Timers[0].Event = "timer.missing"
	report := Run(context.Background(), semanticview.Wrap(bundle), Options{})
	if !reportContains(report.Errors(), "timer_validation", "event timer.missing missing from event catalog") {
		t.Fatalf("expected timer_validation missing timer event error, got %#v", report.Errors())
	}
}

func repoRootForBootverifyTest(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Clean(filepath.Join(wd, "..", "..", ".."))
}

func writeSelectEntityInputPinFixture(t *testing.T, treasuryNodes string) string {
	t.Helper()
	root := t.TempDir()
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: select-entity-fixture
version: 1.0.0
platform_version: ">=1.1.0"
flows:
  - id: treasury
    flow: treasury
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: select-entity-fixture\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), `
opco.spend_requested:
  vertical_id: string
  amount_usd: number
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "treasury", "schema.yaml"), `
name: treasury
mode: static
initial_state: active
states: [active]
pins:
  inputs:
    events: [opco.spend_requested]
  outputs:
    events: []
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "treasury", "events.yaml"), `
opco.spend_requested:
  vertical_id: string
  amount_usd: number
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "treasury", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "treasury", "entities.yaml"), `
opco_budget:
  vertical_id:
    type: text
  spent_usd:
    type: number
    initial: 0
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "treasury", "nodes.yaml"), treasuryNodes)
	return root
}

func writeBootverifyFixtureFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func loadSessionScopeValidationFixture(t *testing.T, fixtureRoot string) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	return semanticview.Wrap(loadFixtureBundleAt(t, repoRoot, fixtureRoot, platformSpec))
}

func writeSessionScopeValidationFixture(t *testing.T, rootAgents, flowSchema, flowAgents string) string {
	t.Helper()
	root := t.TempDir()
	flows := " []"
	if strings.TrimSpace(flowSchema) != "" || strings.TrimSpace(flowAgents) != "" {
		flows = "\n  - id: support\n    flow: support\n    mode: static"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: session-scope-validation
version: "1.0.0"
platform_version: ">=1.0.0"
flows:`+flows+`
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item:
  item_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-validation\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.created:
  entity_id: string
`)
	if strings.TrimSpace(rootAgents) == "" {
		rootAgents = "{}\n"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), rootAgents)
	if strings.TrimSpace(flowSchema) != "" || strings.TrimSpace(flowAgents) != "" {
		if strings.TrimSpace(flowSchema) == "" {
			flowSchema = "name: support\n"
		}
		if strings.TrimSpace(flowAgents) == "" {
			flowAgents = "{}\n"
		}
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), flowSchema)
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
support/item.created:
  entity_id: string
`)
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), flowAgents)
	}
	return root
}

func writePackageBackedSessionScopeValidationFixture(t *testing.T, flowSchema, packageAgents string) string {
	t.Helper()
	root := t.TempDir()

	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: session-scope-validation
version: "1.0.0"
platform_version: ">=1.0.0"
flows:
  - id: support
    flow: support
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item:
  item_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: session-scope-validation\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), `
item.created:
  entity_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "package.yaml"), `
name: support
version: "1.0.0"
flows: []
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), flowSchema)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
support/item.created:
  entity_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "agents.yaml"), packageAgents)
	return root
}

type timerValidationFixtureOptions struct {
	startOn           string
	cancelOn          string
	owner             string
	event             string
	includeTimerEvent bool
}

func writeTimerValidationFixture(t *testing.T, startOn, cancelOn string) string {
	return writeTimerValidationFixtureWithOptions(t, timerValidationFixtureOptions{
		startOn:           startOn,
		cancelOn:          cancelOn,
		owner:             "support-node",
		event:             "timer.reminder",
		includeTimerEvent: true,
	})
}

func writeTimerValidationFixtureWithOptions(t *testing.T, opts timerValidationFixtureOptions) string {
	t.Helper()
	root := t.TempDir()
	if strings.TrimSpace(opts.event) == "" {
		opts.event = "timer.reminder"
	}

	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: timer-validation
version: "1.0.0"
platform: ">=1.6.0"
flows:
  - id: support
    flow: support
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "entities.yaml"), `
ticket:
  ticket_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: timer-validation\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	timerEventBlock := ""
	if opts.includeTimerEvent {
		timerEventBlock = strings.TrimSpace(`
` + opts.event + `:
  entity_id: string
`)
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: waiting
terminal_states: [done]
states: [waiting, active, done]
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
ticket.opened:
  entity_id: string
ticket.closed:
  entity_id: string
`+timerEventBlock+`
`)
	timerBlock := `
    - id: reminder
      owner: ` + opts.owner + `
      event: ` + opts.event + `
      delay: 1m
      start_on: ` + opts.startOn + "\n"
	if strings.TrimSpace(opts.cancelOn) != "" {
		timerBlock += "      cancel_on: " + opts.cancelOn + "\n"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "nodes.yaml"), `
support-node:
  id: support-node
  execution_type: system_node
  subscribes_to:
    - ticket.opened
    - ticket.closed
    - timer.reminder
  timers:
`+timerBlock+`  event_handlers:
    ticket.opened:
      create_entity: true
      advances_to: active
    ticket.closed:
      advances_to: done
    timer.reminder:
      advances_to: done
`)
	return root
}

func writeCrossFlowPinAmbiguityFixture(t *testing.T, scoped bool) string {
	t.Helper()
	root := t.TempDir()

	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: pin-ambiguity
version: "1.0.0"
platform: ">=1.6.0"
flows:
  - id: producer_a
    flow: producer_a
    mode: static
  - id: producer_b
    flow: producer_b
    mode: static
  - id: consumer
    flow: consumer
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item:
  item_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: pin-ambiguity\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")

	for _, flowID := range []string{"producer_a", "producer_b"} {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), `
name: `+flowID+`
initial_state: idle
terminal_states: [done]
states: [idle, done]
pins:
  inputs:
    events: []
  outputs:
    events:
      - ticket.ready
`)
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "policy.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), `
ticket.ready:
  entity_id: string
`)
	}

	subscription := "ticket.ready"
	if scoped {
		subscription = "producer_a/ticket.ready"
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
initial_state: waiting
terminal_states: [done]
states: [waiting, done]
pins:
  inputs:
    events:
      - ticket.ready
  outputs:
    events:
      - consumer.started
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), `
ticket.ready:
  entity_id: string
consumer.started:
  entity_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), `
consumer-node:
  id: consumer-node
  execution_type: system_node
  subscribes_to:
    - `+subscription+`
  event_handlers:
    ticket.ready:
      create_entity: true
      advances_to: done
      emit: consumer.started
`)

	return root
}

func writeInputPinExternalScopeFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: input-pin-external-scope
version: "1.0.0"
platform: ">=1.6.0"
flows:
  - id: external_consumer
    flow: external_consumer
    mode: static
  - id: plain_consumer
    flow: plain_consumer
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: input-pin-external-scope\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")

	for _, flowID := range []string{"external_consumer", "plain_consumer"} {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), `
name: `+flowID+`
initial_state: idle
terminal_states: [done]
states: [idle, done]
pins:
  inputs:
    events:
      - ticket.ready
  outputs:
    events: []
`)
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "policy.yaml"), "{}\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "nodes.yaml"), "{}\n")
		entry := "ticket.ready:\n  payload:\n    entity_id: string\n"
		if flowID == "external_consumer" {
			entry = "ticket.ready:\n  swarm:\n    source: external (manual handoff)\n  entity_id: string\n"
		} else {
			entry = "ticket.ready:\n  entity_id: string\n"
		}
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), entry)
	}

	return root
}

func writeLocalizedEventRoutingFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: localized-event-routing
version: "1.0.0"
platform: ">=1.6.0"
flows:
  - id: producer
    flow: producer
    mode: static
  - id: consumer
    flow: consumer
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "entities.yaml"), `
item:
  item_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: localized-event-routing\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")

	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "schema.yaml"), `
name: producer
initial_state: idle
terminal_states: [done]
states: [idle, done]
pins:
  inputs:
    events: []
  outputs:
    events:
      - ticket.ready
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "events.yaml"), `
ticket.ready:
  entity_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "producer", "nodes.yaml"), `
producer-node:
  id: producer-node
  execution_type: system_node
  subscribes_to:
    - start
  produces:
    - ticket.ready
  event_handlers:
    start:
      emit: ticket.ready
`)

	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "schema.yaml"), `
name: consumer
initial_state: waiting
terminal_states: [done]
states: [waiting, done]
pins:
  inputs:
    events:
      - ticket.ready
  outputs:
    events: []
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "events.yaml"), `
ticket.ready:
  entity_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "consumer", "nodes.yaml"), `
consumer-node:
  id: consumer-node
  execution_type: system_node
  subscribes_to:
    - ticket.ready
  event_handlers:
    ticket.ready:
      create_entity: true
      advances_to: done
`)

	return root
}

func writeStateReachabilityFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: state-reachability
version: "1.0.0"
platform: ">=1.6.0"
flows:
  - id: support
    flow: support
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: state-reachability\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")

	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "schema.yaml"), `
name: support
initial_state: waiting
terminal_states: [done]
states: [waiting, active, review, done]
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "entities.yaml"), `
ticket: {}
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "events.yaml"), `
ticket.opened:
  entity_id: string
ticket.closed:
  entity_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "support", "nodes.yaml"), `
support-node:
  id: support-node
  execution_type: system_node
  subscribes_to:
    - ticket.opened
    - ticket.closed
  event_handlers:
    ticket.opened:
      create_entity: true
      advances_to: active
    ticket.closed:
      advances_to: done
`)

	return root
}

func writeWave1ExpressionFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: wave1-expression-fixture
version: "1.0.0"
platform: ">=1.6.0"
flows:
  - id: child
    flow: child
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: wave1-expression-fixture\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")

	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "schema.yaml"), `
name: child
namespace: child
initial_state: idle
terminal_states: [done]
states: [idle, working, done]
pins:
  inputs:
    events: [task.assigned, task.feedback]
  outputs:
    events: [task.result]
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "entities.yaml"), `
task:
  retry_count:
    type: integer
    initial: 0
  revision_count:
    type: integer
    initial: 0
  kill_reason:
    type: text
    _unused_reason: optional test surface field
  base_score:
    type: numeric
    _unused_reason: optional test surface field
  adjusted_score:
    type: numeric
    _unused_reason: optional test surface field
  filtered_score:
    type: numeric
    _unused_reason: optional test surface field
  filtered_items:
    type: text
    _unused_reason: optional test surface field
  composite_score:
    type: numeric
    _unused_reason: optional test surface field
  expected_count:
    type: integer
    initial: 1
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "events.yaml"), `
task.assigned:
  entity_id: string
  score: numeric
task.feedback:
  entity_id: string
  comment: string
task.result:
  entity_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "nodes.yaml"), `
worker:
  id: worker
  execution_type: system_node
  subscribes_to: [task.assigned, task.feedback]
  produces: [task.result]
  event_handlers:
    task.assigned:
      create_entity: true
      advances_to: working
    task.feedback:
      create_entity: true
      advances_to: done
      emit: task.result
`)

	return root
}

func loadWave1ExpressionFixtureBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := writeWave1ExpressionFixture(t)
	return loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))
}

func writeWave1RootReaderCoverageFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()

	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: wave1-root-reader-coverage
version: "1.0.0"
platform: ">=1.6.0"
flows:
  - id: child
    flow: child
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: wave1-root-reader-coverage\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "entities.yaml"), `
case:
  priority:
    type: integer
    _unused_reason: child read-pin coverage proof field
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")

	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "schema.yaml"), `
name: child
initial_state: idle
terminal_states: [done]
states: [idle, done]
pins:
  inputs:
    events: [task.assigned]
    reads: [priority]
  outputs:
    events: []
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "events.yaml"), `
task.assigned:
  entity_id: string
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "nodes.yaml"), `
reader:
  id: reader
  execution_type: system_node
  subscribes_to: [task.assigned]
  event_handlers:
    task.assigned:
      guard:
        check: "entity.priority >= 0"
      advances_to: done
`)

	return root
}

func loadWave1RootReaderCoverageFixtureBundle(t *testing.T) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	root := writeWave1RootReaderCoverageFixture(t)
	return loadFixtureBundleAt(t, repoRootForBootverifyTest(t), root, runtimecontracts.DefaultPlatformSpecFile(repoRootForBootverifyTest(t)))
}

func writePromptWriterCoverageFixture(t *testing.T, agentsYAML, entitiesYAML, promptText string) string {
	t.Helper()
	root := t.TempDir()

	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), `
name: prompt-writer-coverage
version: "1.0.0"
platform: ">=1.6.0"
flows:
  - id: child
    flow: child
    mode: static
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: prompt-writer-coverage\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "schema.yaml"), `
name: child
initial_state: idle
terminal_states: [done]
states: [idle, done]
`)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "entities.yaml"), entitiesYAML)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "policy.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "agents.yaml"), agentsYAML)
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "events.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "nodes.yaml"), "{}\n")
	if strings.TrimSpace(promptText) != "" {
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", "child", "prompts", "writer.md"), promptText)
	}
	return root
}

func loadTier8Fixture(t *testing.T, fixture string) semanticview.Source {
	t.Helper()
	return semanticview.Wrap(loadTier8FixtureBundle(t, fixture))
}

func loadTier8FixtureBundle(t *testing.T, fixture string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier8-boot-verification", fixture)
	return loadFixtureBundleAt(t, repoRoot, fixtureRoot, platformSpec)
}

func loadFixtureBundle(t *testing.T, relativeRoot string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	platformSpec := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	fixtureRoot := filepath.Join(repoRoot, relativeRoot)
	return loadFixtureBundleAt(t, repoRoot, fixtureRoot, platformSpec)
}

func loadFixtureBundleAt(t *testing.T, repoRoot, fixtureRoot, platformSpec string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, platformSpec)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides(%s): %v", fixtureRoot, err)
	}
	return bundle
}

func bootverifyTransitionRuntimeOwnershipBundle() *runtimecontracts.WorkflowContractBundle {
	return &runtimecontracts.WorkflowContractBundle{
		Semantics: runtimecontracts.WorkflowSemanticView{
			Transitions: []runtimecontracts.WorkflowTransitionContract{{
				ID:      "ticket-open",
				Trigger: "ticket.created",
				Node:    "dispatcher",
				Actions: []string{"emit_opened"},
				Guards:  []string{"allow_ticket"},
			}},
			ActionByID: map[string]runtimecontracts.GuardActionEntry{
				"emit_opened": {
					ID:    "emit_opened",
					Emits: "ticket.opened",
				},
			},
			GuardByID: map[string]runtimecontracts.GuardActionEntry{
				"allow_ticket": {
					ID:    "allow_ticket",
					Check: "true",
				},
			},
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"dispatcher": {
					"ticket.created": {},
				},
				"projector": {
					"ticket.opened": {},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"dispatcher": {
				ID:               "dispatcher",
				OwnedTransitions: []string{"ticket-open"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"ticket.created": {},
				},
			},
			"projector": {
				ID: "projector",
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"ticket.opened": {},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"ticket.created": {
				RuntimeHandling: "consuming",
				OwningNode:      "dispatcher",
			},
			"ticket.opened": {
				RuntimeHandling: "projection",
				OwningNode:      "projector",
			},
			"ticket.audit": {},
		},
	}
}

func bootverifyDeclarationDriftBundle() *runtimecontracts.WorkflowContractBundle {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"producer": {
					"task.start": {Emit: runtimecontracts.EmitSpec{Event: "task.done"}},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"producer": {
				SubscribesTo: []string{"task.start"},
				Produces:     []string{"task.done"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"task.start": {Emit: runtimecontracts.EmitSpec{Event: "task.done"}},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"task.start": {Swarm: runtimecontracts.EventSwarmMetadata{Source: "external"}},
			"task.done":  {Swarm: runtimecontracts.EventSwarmMetadata{Consumer: []string{"dashboard"}}},
		},
	}
	bundle.Platform.Platform.Name = "test"
	bundle.Platform.Platform.Version = "1.0.0"
	return bundle
}

func bootverifyPayloadCompletenessBundle() *runtimecontracts.WorkflowContractBundle {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Platform: runtimecontracts.PlatformSpecDocument{},
		RootEntities: runtimecontracts.EntityContractsDocument{
			"scan": {
				Fields: map[string]runtimecontracts.EntityFieldDecl{
					"scan_id":   {Type: "string"},
					"geography": {Type: "string"},
				},
			},
		},
		Semantics: runtimecontracts.WorkflowSemanticView{
			NodeHandlers: map[string]map[string]runtimecontracts.SystemNodeEventHandler{
				"dispatcher": {
					"scan.corpus_dispatch": {
						Emit: runtimecontracts.EmitSpec{Event: "market_research.scan_assigned"},
					},
				},
			},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			"dispatcher": {
				SubscribesTo: []string{"scan.corpus_dispatch"},
				EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
					"scan.corpus_dispatch": {
						Emit: runtimecontracts.EmitSpec{Event: "market_research.scan_assigned"},
					},
				},
			},
		},
		Events: map[string]runtimecontracts.EventCatalogEntry{
			"scan.corpus_dispatch": {
				Swarm: runtimecontracts.EventSwarmMetadata{Source: "external"},
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"scan_id":   {Type: "string"},
						"geography": {Type: "string"},
						"mode":      {Type: "string"},
					},
				},
				Required: []string{"scan_id", "geography"},
			},
			"market_research.scan_assigned": {
				Swarm: runtimecontracts.EventSwarmMetadata{Consumer: []string{"dashboard"}},
				Payload: runtimecontracts.EventPayloadSpec{
					Properties: map[string]runtimecontracts.EventFieldSpec{
						"scan_id":   {Type: "string"},
						"geography": {Type: "string"},
					},
				},
				Required: []string{"scan_id"},
			},
		},
	}
	bundle.Platform.Platform.Name = "test"
	bundle.Platform.Platform.Version = "1.0.0"
	return bundle
}

type deadEventSchemaFlowFiles struct {
	schema string
	events string
	agents string
	nodes  string
	policy string
}

type deadEventSchemaFixtureOptions struct {
	name       string
	rootSchema string
	rootEvents string
	rootAgents string
	rootNodes  string
	rootPolicy string
	flows      map[string]deadEventSchemaFlowFiles
}

func writeDeadEventSchemaFixture(t *testing.T, opts deadEventSchemaFixtureOptions) string {
	t.Helper()
	root := t.TempDir()
	name := strings.TrimSpace(opts.name)
	if name == "" {
		name = "dead-event-schema"
	}

	flowIDs := make([]string, 0, len(opts.flows))
	for flowID := range opts.flows {
		flowID = strings.TrimSpace(flowID)
		if flowID != "" {
			flowIDs = append(flowIDs, flowID)
		}
	}
	sort.Strings(flowIDs)

	packageYAML := "name: " + name + "\nversion: \"1.0.0\"\nplatform: \">=1.6.0\"\n"
	if len(flowIDs) > 0 {
		packageYAML += "flows:\n"
		for _, flowID := range flowIDs {
			packageYAML += "  - id: " + flowID + "\n    flow: " + flowID + "\n    mode: static\n"
		}
	}

	writeBootverifyFixtureFile(t, filepath.Join(root, "package.yaml"), packageYAML)
	rootSchema := strings.TrimSpace(opts.rootSchema)
	if rootSchema == "" {
		rootSchema = "name: " + name
	}
	writeBootverifyFixtureFile(t, filepath.Join(root, "schema.yaml"), rootSchema+"\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "policy.yaml"), defaultFixtureYAML(opts.rootPolicy))
	writeBootverifyFixtureFile(t, filepath.Join(root, "tools.yaml"), "{}\n")
	writeBootverifyFixtureFile(t, filepath.Join(root, "agents.yaml"), defaultFixtureYAML(opts.rootAgents))
	writeBootverifyFixtureFile(t, filepath.Join(root, "events.yaml"), defaultFixtureYAML(opts.rootEvents))
	writeBootverifyFixtureFile(t, filepath.Join(root, "nodes.yaml"), defaultFixtureYAML(opts.rootNodes))

	for _, flowID := range flowIDs {
		files := opts.flows[flowID]
		schema := strings.TrimSpace(files.schema)
		if schema == "" {
			schema = "name: " + flowID + "\ninitial_state: idle\nterminal_states: [done]\nstates: [idle, done]\npins:\n  inputs:\n    events: []\n  outputs:\n    events: []"
		}
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "schema.yaml"), schema+"\n")
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "policy.yaml"), defaultFixtureYAML(files.policy))
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "agents.yaml"), defaultFixtureYAML(files.agents))
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "events.yaml"), defaultFixtureYAML(files.events))
		writeBootverifyFixtureFile(t, filepath.Join(root, "flows", flowID, "nodes.yaml"), defaultFixtureYAML(files.nodes))
	}

	return root
}

func defaultFixtureYAML(contents string) string {
	if strings.TrimSpace(contents) == "" {
		return "{}\n"
	}
	if strings.HasSuffix(contents, "\n") {
		return contents
	}
	return contents + "\n"
}

func gateSchemaValidationBundle(gateState runtimecontracts.NodeGateStateSchema, handler runtimecontracts.SystemNodeEventHandler) *runtimecontracts.WorkflowContractBundle {
	const (
		nodeID    = "validate-task"
		eventType = "task.requested"
	)
	node := runtimecontracts.SystemNodeContract{
		ID:            nodeID,
		ExecutionType: "system_node",
		SubscribesTo:  []string{eventType},
		GateState:     gateState,
		EventHandlers: map[string]runtimecontracts.SystemNodeEventHandler{
			eventType: handler,
		},
	}
	bundle := &runtimecontracts.WorkflowContractBundle{
		Events: map[string]runtimecontracts.EventCatalogEntry{
			eventType: {},
		},
		Nodes: map[string]runtimecontracts.SystemNodeContract{
			nodeID: node,
		},
	}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "test"
	bundle.Semantics.HandlerTransitions = []runtimecontracts.HandlerTransitionSemantic{{
		ID:        nodeID + ":" + eventType,
		NodeID:    nodeID,
		EventType: eventType,
		SetsGate:  handler.SetsGate,
	}}
	return bundle
}

func decodeGateSchemaHandler(t *testing.T, raw string) runtimecontracts.SystemNodeEventHandler {
	t.Helper()
	var handler runtimecontracts.SystemNodeEventHandler
	if err := yaml.Unmarshal([]byte(raw), &handler); err != nil {
		t.Fatalf("decode gate schema handler: %v", err)
	}
	return handler
}

func reportContains(items []Finding, checkID, contains string) bool {
	for _, item := range items {
		if item.CheckID == checkID && strings.Contains(item.Message, contains) {
			return true
		}
	}
	return false
}

type bootverifyCredentialStore struct {
	values  map[string]string
	listErr error
	getErr  error
}

func (s bootverifyCredentialStore) Get(_ context.Context, key string) (string, bool, error) {
	if s.getErr != nil {
		return "", false, s.getErr
	}
	value, ok := s.values[strings.TrimSpace(key)]
	return value, ok, nil
}

func (s bootverifyCredentialStore) Set(_ context.Context, key, value string) error {
	if s.values == nil {
		s.values = map[string]string{}
	}
	s.values[strings.TrimSpace(key)] = value
	return nil
}

func (s bootverifyCredentialStore) List(_ context.Context) ([]string, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	out := make([]string, 0, len(s.values))
	for key := range s.values {
		out = append(out, strings.TrimSpace(key))
	}
	sort.Strings(out)
	return out, nil
}

func (s bootverifyCredentialStore) Delete(_ context.Context, key string) error {
	delete(s.values, strings.TrimSpace(key))
	return nil
}

func runtimeExternalResourceSource(mcpURL string) semanticview.Source {
	bundle := &runtimecontracts.WorkflowContractBundle{
		Tools: map[string]runtimecontracts.ToolSchemaEntry{
			"email_api": {Credentials: []string{"sendgrid_api_key"}},
		},
		Policy: runtimecontracts.PolicyDocument{Values: map[string]runtimecontracts.PolicyValue{
			"mcp_servers": {
				Value: map[string]any{
					"infra": map[string]any{
						"transport":       "http",
						"url":             mcpURL,
						"prefix":          "infra",
						"credentials_key": "infra_mcp_token",
					},
				},
			},
			"web_search_provider": {
				Value: map[string]any{
					"provider":        "brave",
					"credentials_key": "brave_search_api_key",
				},
			},
		}},
	}
	bundle.Platform.Platform.Name = "swarm"
	bundle.Platform.Platform.Version = "test"
	return semanticview.Wrap(bundle)
}

func firstBundleHandler(bundle *runtimecontracts.WorkflowContractBundle) (string, string, runtimecontracts.SystemNodeEventHandler, bool) {
	for nodeID, node := range bundle.Nodes {
		for eventType, handler := range node.EventHandlers {
			return nodeID, eventType, handler, true
		}
	}
	return "", "", runtimecontracts.SystemNodeEventHandler{}, false
}

func firstFlowHandlerInFlowView(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle) (string, string, string, runtimecontracts.SystemNodeEventHandler) {
	t.Helper()
	views := bundle.FlowViews()
	sort.Slice(views, func(i, j int) bool {
		return strings.TrimSpace(views[i].Paths.ID) < strings.TrimSpace(views[j].Paths.ID)
	})
	for _, view := range views {
		flowID := strings.TrimSpace(view.Paths.ID)
		nodeIDs := make([]string, 0, len(view.Nodes))
		for nodeID := range view.Nodes {
			nodeIDs = append(nodeIDs, nodeID)
		}
		sort.Strings(nodeIDs)
		for _, nodeID := range nodeIDs {
			node := view.Nodes[nodeID]
			eventTypes := make([]string, 0, len(node.EventHandlers))
			for eventType := range node.EventHandlers {
				eventTypes = append(eventTypes, eventType)
			}
			sort.Strings(eventTypes)
			for _, eventType := range eventTypes {
				return flowID, nodeID, eventType, node.EventHandlers[eventType]
			}
		}
	}
	t.Fatal("expected fixture to include at least one flow handler")
	return "", "", "", runtimecontracts.SystemNodeEventHandler{}
}

func writeFlowHandler(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, flowID, nodeID, eventType string, handler runtimecontracts.SystemNodeEventHandler) {
	t.Helper()
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	node := flowView.Nodes[nodeID]
	node.EventHandlers[eventType] = handler
	flowView.Nodes[nodeID] = node
	if bundle.Nodes == nil {
		bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{}
	}
	bundle.Nodes[nodeID] = node
	if bundle.Semantics.NodeHandlers == nil {
		bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	if bundle.Semantics.NodeHandlers[nodeID] == nil {
		bundle.Semantics.NodeHandlers[nodeID] = map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	bundle.Semantics.NodeHandlers[nodeID][eventType] = handler
}

func renameFlowHandlerEvent(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, flowID, nodeID, oldEventType, newEventType string, handler runtimecontracts.SystemNodeEventHandler) {
	t.Helper()
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	node := flowView.Nodes[nodeID]
	delete(node.EventHandlers, oldEventType)
	node.EventHandlers[newEventType] = handler
	flowView.Nodes[nodeID] = node
	if bundle.Nodes == nil {
		bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{}
	}
	bundle.Nodes[nodeID] = node
	if bundle.Semantics.NodeHandlers == nil {
		bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	if bundle.Semantics.NodeHandlers[nodeID] == nil {
		bundle.Semantics.NodeHandlers[nodeID] = map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	delete(bundle.Semantics.NodeHandlers[nodeID], oldEventType)
	bundle.Semantics.NodeHandlers[nodeID][newEventType] = handler
	if len(bundle.Semantics.FlowInputs[flowID]) > 0 {
		inputs := append([]string{}, bundle.Semantics.FlowInputs[flowID]...)
		for idx, eventType := range inputs {
			if strings.TrimSpace(eventType) == strings.TrimSpace(oldEventType) {
				inputs[idx] = newEventType
			}
		}
		bundle.Semantics.FlowInputs[flowID] = inputs
	}
}

func flowEventEntry(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, flowID, eventType string) runtimecontracts.EventCatalogEntry {
	t.Helper()
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	entry, ok := flowView.Events[eventType]
	if !ok {
		t.Fatalf("flow %s event %s missing", flowID, eventType)
	}
	return entry
}

func writeFlowEventEntry(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, flowID, eventType string, entry runtimecontracts.EventCatalogEntry) {
	t.Helper()
	flowView, ok := bundle.FlowViewByID(flowID)
	if !ok || flowView == nil {
		t.Fatalf("flow view %s missing", flowID)
	}
	if flowView.Events == nil {
		flowView.Events = map[string]runtimecontracts.EventCatalogEntry{}
	}
	flowView.Events[eventType] = entry
}

func addProjectHandler(t *testing.T, bundle *runtimecontracts.WorkflowContractBundle, nodeID, eventType string, handler runtimecontracts.SystemNodeEventHandler) {
	t.Helper()
	if bundle.Nodes == nil {
		bundle.Nodes = map[string]runtimecontracts.SystemNodeContract{}
	}
	node := bundle.Nodes[nodeID]
	node.ID = nodeID
	node.ExecutionType = "system_node"
	if node.EventHandlers == nil {
		node.EventHandlers = map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	node.EventHandlers[eventType] = handler
	bundle.Nodes[nodeID] = node
	if bundle.Semantics.NodeHandlers == nil {
		bundle.Semantics.NodeHandlers = map[string]map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	if bundle.Semantics.NodeHandlers[nodeID] == nil {
		bundle.Semantics.NodeHandlers[nodeID] = map[string]runtimecontracts.SystemNodeEventHandler{}
	}
	bundle.Semantics.NodeHandlers[nodeID][eventType] = handler
}

func platformEventCatalogTestNode(t *testing.T, raw string) yaml.Node {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("yaml.Unmarshal platform event catalog node: %v", err)
	}
	if doc.Kind == yaml.DocumentNode && len(doc.Content) > 0 {
		return *doc.Content[0]
	}
	return doc
}
