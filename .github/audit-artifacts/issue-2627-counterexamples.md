# Audit-Only Counterexamples: Issue 2627

Source fc76c8b40e7b7d4adf9a0b7985a6e926939b9e49, effective Go1.26.8.
These disposable probes are characterization, NOT production repairs or passing
qualification. Temporary Go files are removed before committing this artifact.
Run each file in its corresponding existing test package with
`go test -race ./internal/runtime/llm ./internal/runtime/mcp -run '^TestIssue2627Audit' -count=3 -timeout=3m -json`.
The request/managed-HTTP/MCP/evidence assertions deliberately fail on baseline.
Real HTTP/stdio discovery controls pass, rejecting unsupported types upstream.
No paid provider, external messages, Docker or compiled public-route exploit.

The first attempt's invalid session UUID and second attempt's mismatched event
run/effective-unconfirmed capabilities were probe-construction errors. They are
retained in the original logs but are NOT defect proof. Final probes use a valid
session UUID, exact run/event context and admitted visible/callable capability
inputs. Anthropic reaches one real local HTTP request and settles the simulated
response; MCP actually returns the unsupported schema in exact tools/list; Mock
request and target-observed matching identity accept the same bad schema.

## llm/issue2627_audit_probe_test.go

```go
package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/google/uuid"
)

func TestIssue2627AuditBuilderCounterexamples(t *testing.T) {
	bad := map[string]any{"type": "unsupported-provider-type"}
	if err := ValidateProviderToolSchema("bad", bad); err == nil {
		t.Fatal("canonical validator did not reject the counterexample")
	}
	session := &Session{Tools: []ToolDefinition{{Name: "bad", Schema: bad}}}
	t.Run("anthropic", func(t *testing.T) {
		request, err := (&AnthropicAPIRuntime{}).buildRequest(session, Message{Role: "user", Content: "test"}, "model")
		if err == nil {
			raw, _ := json.Marshal(request.Tools)
			t.Fatalf("canonical refusal bypassed by Anthropic: tools=%s", raw)
		}
	})
	t.Run("mock-request", func(t *testing.T) {
		request, err := buildMockRequest(context.Background(), session, Message{Role: "user", Content: "test"})
		if err == nil {
			raw, _ := json.Marshal(request.Tools)
			t.Fatalf("canonical refusal bypassed by Mock request projection: tools=%s", raw)
		}
	})
}

func TestIssue2627AuditWorkspaceSchemaEvidenceRefusal(t *testing.T) {
	bad := ToolDefinition{Name: "bad", Schema: map[string]any{"type": "unsupported-provider-type"}}
	if err := ValidateProviderToolSchema(bad.Name, bad.Schema); err == nil {
		t.Fatal("canonical counterexample was admitted")
	}
	raw, err := json.Marshal(bad.Schema)
	if err != nil {
		t.Fatal(err)
	}
	err = compareWorkspaceMCPDefinitions(map[string]string{bad.Name: ToolDefinitionIdentity(bad)}, []toolgateway.ListedDefinition{{Name: bad.Name, InputSchema: raw}})
	if err == nil {
		t.Fatal("matching identity confirmed canonically invalid target-observed schema")
	}
}

func TestIssue2627AuditAnthropicManagedHTTPRefusal(t *testing.T) {
	var sends atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		sends.Add(1)
		t.Logf("actual local Anthropic HTTP request tools=%v", payload["tools"])
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"model":"effect-test-model","usage":{"input_tokens":1,"output_tokens":1},"content":[{"type":"text","text":"done"}]}`))
	}))
	defer server.Close()
	harness := effecttest.New()
	runtime := NewAnthropicAPIRuntime(&config.Config{}, nil, "audit-owner", nil, nil)
	runtime.apiURL = server.URL
	runtime.credentialCache.value = "audit-local-token"
	runtime.completionController = liveTestCompletionController(harness, harness, harness, harness)
	ctx := testManagedConversationContext(t, harness, "audit-agent", "audit/instance", "worker")
	actor, _ := runtimeactors.ActorFromContext(ctx)
	ctx = runtimecorrelation.WithRunID(ctx, actor.Identity.RunID)
	ctx = agentmemory.WithExecution(ctx, agentmemory.Plan{}, actor.Identity)
	bad := map[string]any{"type": "unsupported-provider-type"}
	if err := ValidateProviderToolSchema("bad", bad); err == nil {
		t.Fatal("canonical counterexample was admitted")
	}
	session := &Session{ID: uuid.NewString(), AgentID: actor.ID, MemoryIdentity: actor.Identity,
		Tools: []ToolDefinition{{Name: "bad", Schema: bad}}}
	ctx = managedProviderTestContext(t, ctx, runtime, session, session.Tools)
	response, err := runtime.continueSession(ctx, session, Message{Role: "user", Content: "test"}, managedProviderCallForEffectTest(t, ctx))
	if got := sends.Load(); got != 0 {
		t.Fatalf("canonically invalid schema reached real HTTP %d times: response=%v error=%v", got, response, err)
	}
	if err == nil {
		t.Fatal("invalid managed request did not refuse")
	}
}

```

## mcp/issue2627_audit_probe_test.go

```go
package mcp

import (
	"encoding/json"
	"testing"
	"time"

	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/llm"
)

func TestIssue2627AuditMCPToolsListSchemaRefusal(t *testing.T) {
	bad := map[string]any{"type": "unsupported-provider-type"}
	if err := llm.ValidateProviderToolSchema("bad", bad); err == nil {
		t.Fatal("canonical counterexample was admitted")
	}
	ctx, original, _ := managedClaudeProviderTurnTestContext(t, managedcapabilities.ExecutionNormalAgent)
	actor, _ := models.ActorFromContext(ctx)
	definition := llm.ToolDefinition{Name: "write_file", Schema: bad}
	caps := toolcapabilities.NewSet([]toolcapabilities.Capability{{Name: definition.Name, Visible: true, Callable: true}})
	plan, err := llm.CompileManagedCapabilityAdmission(actor, llm.ClaudeCLIProviderContract(), []llm.ToolDefinition{definition}, caps)
	if err != nil {
		t.Fatalf("earlier capability admission refused: %v", err)
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorIdentity: actor.Identity, RuntimeMode: original.RuntimeMode, Provider: original.Provider,
		Transport: original.Transport, ProviderContract: original.ProviderContract,
		Authority: original.Authority, Tools: plan, CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx = managedcapabilities.WithContext(ctx, surface)
	registry := NewTurnContextRegistry(models.ActorFromContext)
	token := registry.RegisterTurnContextWithCapabilitySurface(ctx, time.Minute, surface)
	if token == "" {
		t.Fatal("actual context registration refused")
	}
	defer registry.UnregisterTurnContext(token)
	calls := 0
	gateway := NewGateway(actorScopedToolExecutorStub{actorDefs: []llm.ToolDefinition{definition}, callCount: &calls}, testGatewayToken, managedCLIGatewayHooks(registry))
	response := callMCPGateway(t, gateway, token, RPCRequest{JSONRPC: "2.0", Method: "tools/list", ID: "audit-list"})
	if calls != 0 {
		t.Fatalf("tools/list unexpectedly executed %d calls", calls)
	}
	if response.Error == nil {
		raw, _ := json.Marshal(response.Result)
		t.Fatalf("canonically invalid schema escaped exact managed tools/list: %s", raw)
	}
}

func TestIssue2627AuditExternalDiscoveryRefusesUnsupportedType(t *testing.T) {
	for _, transport := range []string{"http", "stdio"} {
		t.Run(transport, func(t *testing.T) {
			client, server := remoteSchemaClient(t, transport, `{"tools":[{"name":"bad","inputSchema":{"type":"unsupported-provider-type"}}]}`)
			tools, err := client.discoverServerTools(unmanagedMCPTestContext(), nil, server)
			if err == nil || len(tools) != 0 {
				t.Fatalf("upstream discovery admitted malformed schema: tools=%v error=%v", tools, err)
			}
			t.Logf("real %s discovery gate refused malformed type: %v", transport, err)
		})
	}
}

```
