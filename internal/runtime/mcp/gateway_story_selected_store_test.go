package mcp_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const gatewayStoryAuthToken = "gateway-story-token"

type gatewayStoryStore interface {
	runtimeeffects.Store
	ListAuthorActivity(context.Context, runtimeauthoractivity.ListOptions) (runtimeauthoractivity.ListResult, error)
}

type gatewayStorySelectedStore struct {
	backend  gatewayStoryStore
	db       *sql.DB
	postgres bool
}

func TestGatewayTurnContextEffectStoryScopeSelectedStoreParity(t *testing.T) {
	for _, tc := range []struct {
		name  string
		start func(*testing.T) gatewayStorySelectedStore
	}{
		{
			name: "sqlite",
			start: func(t *testing.T) gatewayStorySelectedStore {
				backend := storetest.StartSQLiteRuntimeStore(t)
				return gatewayStorySelectedStore{backend: backend, db: backend.DB}
			},
		},
		{
			name: "postgres",
			start: func(t *testing.T) gatewayStorySelectedStore {
				_, db, _ := testutil.StartPostgres(t)
				return gatewayStorySelectedStore{backend: &store.PostgresStore{DB: db}, db: db, postgres: true}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := tc.start(t)
			var dispatches atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				dispatches.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"ok":true}`))
			}))
			t.Cleanup(server.Close)

			runID := uuid.NewString()
			runtimeInstanceID := uuid.NewString()
			sourceFact := runtimecorrelation.BundleSourceFact{
				BundleHash:        "bundle-v1:sha256:" + strings.Repeat("c", 64),
				BundleSource:      "ephemeral",
				BundleFingerprint: "sha256:" + strings.Repeat("c", 64),
			}
			scope := runtimeauthoractivity.BundleScope(runtimeInstanceID, sourceFact.BundleHash)
			actor := models.AgentConfig{
				ExecutionMode: "live",
				ID:            "story-writer",
				Type:          "internal",
				Role:          "story-writer",
				FlowID:        "story",
				FlowPath:      "story/instance-1",
				Tools:         []string{"send_story"},
			}
			seedGatewayStoryRuntime(t, selected, runID, actor.ID, sourceFact)
			source := loadGatewayStorySource(t, server.URL)
			executor := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{WorkflowSource: source})
			if !gatewayStoryToolOffered(executor, actor, "send_story") {
				t.Fatalf("send_story is not offered to actor: %#v", executor.ToolDefinitionsForActor(actor))
			}
			if gatewayStoryToolOffered(executor, actor, "create_entity") {
				t.Fatal("retired create_entity surface unexpectedly reached the actor MCP catalog")
			}

			registry := runtimemcp.NewTurnContextRegistry(models.ActorFromContext)
			gateway := runtimemcp.NewGateway(executor, gatewayStoryAuthToken, runtimemcp.GatewayHooks{
				WithActor:                 models.WithActor,
				WithInboundEvent:          runtimebus.WithInboundEvent,
				ActorFromContext:          models.ActorFromContext,
				ResolveTurnContext:        registry.ResolveTurnContext,
				ObserveCapabilityEvidence: registry.ObserveCapabilityEvidence,
				ObserveCapabilityMismatch: registry.ObserveCapabilityMismatch,
				ObserveMCPProviderCall:    registry.ObserveMCPProviderCall,
				MarkEmitKeyUsed:           registry.MarkEmitKeyUsed,
			})
			surface, authority, admission := gatewayStoryCapabilitySurface(t, gateway, actor, runID)

			successCtx := gatewayStoryManagedTurnContext(context.Background(), selected, actor, runID, scope, sourceFact, authority, admission, "gateway-http-success")
			successToken := registry.RegisterTurnContextWithCapabilitySurface(successCtx, time.Hour, surface)
			response := callGatewayStoryTool(t, gateway, successToken, "send_story", map[string]any{})
			if gatewayStoryResponseIsError(response) {
				t.Fatalf("send_story response = %#v", response)
			}
			assertGatewayStoryEffectAndOccurrence(t, selected, scope, 1)
			if got := dispatches.Load(); got != 1 {
				t.Fatalf("HTTP dispatches = %d, want 1", got)
			}

			missingScopeCtx := gatewayStoryManagedTurnContext(context.Background(), selected, actor, runID, runtimeauthoractivity.Scope{}, sourceFact, authority, admission, "gateway-http-missing-scope")
			missingScopeToken := registry.RegisterTurnContextWithCapabilitySurface(missingScopeCtx, time.Hour, surface)
			failed := callGatewayStoryTool(t, gateway, missingScopeToken, "send_story", map[string]any{})
			if !gatewayStoryResponseIsError(failed) {
				t.Fatalf("scope-less send_story response = %#v, want error", failed)
			}
			assertGatewayStoryEffectAndOccurrence(t, selected, scope, 1)
			if got := dispatches.Load(); got != 1 {
				t.Fatalf("scope-less call dispatched HTTP request; dispatches = %d, want 1", got)
			}
		})
	}
}

func gatewayStoryManagedTurnContext(ctx context.Context, selected gatewayStorySelectedStore, actor models.AgentConfig, runID string, scope runtimeauthoractivity.Scope, sourceFact runtimecorrelation.BundleSourceFact, authority runtimeeffects.Authority, admission managedexecution.Admission, identity string) context.Context {
	ctx = models.WithActor(ctx, actor)
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, sourceFact)
	ctx = runtimeeffects.WithExecutionMode(ctx, runtimeeffects.ExecutionModeLive)
	if scope.Kind != "" {
		ctx = runtimeauthoractivity.WithScope(ctx, scope)
	}
	ctx = runtimebus.WithInboundEvent(ctx, gatewayStoryInboundEvent(runID, actor))
	ctx = runtimeeffects.WithLifecycleToken(ctx, runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: actor.ID, Generation: 3})
	ctx = runtimeeffects.WithAuthority(ctx, authority)
	ctx = managedexecution.WithAdmission(ctx, admission)
	ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(selected.backend))
	return runtimeeffects.WithLogicalOperationIdentity(ctx, identity)
}

func gatewayStoryCapabilitySurface(t *testing.T, gateway *runtimemcp.Gateway, actor models.AgentConfig, runID string) (managedcapabilities.Surface, runtimeeffects.Authority, managedexecution.Admission) {
	t.Helper()
	var definition *runtimemcp.ToolDef
	for _, candidate := range gateway.MCPToolsForActor(actor) {
		if candidate.Name == "send_story" {
			copy := candidate
			definition = &copy
			break
		}
	}
	if definition == nil {
		t.Fatal("send_story is absent from the live MCP catalog")
	}
	turnID := uuid.NewString()
	sessionID := uuid.NewString()
	token := runtimeeffects.LifecycleToken{RuntimeEpoch: 7, AgentID: actor.ID, Generation: 3}
	authority := runtimeeffects.NormalAgentAuthority(token, "gateway-story-owner", time.Now().UTC().Add(time.Hour))
	authority.Target = runtimeeffects.UsageTarget{
		Kind: runtimeeffects.UsageTargetAgentTurn, ID: turnID, RunID: runID, AgentID: actor.ID,
		SessionID: sessionID, Memory: agentmemory.PlatformDefault(), FlowInstance: actor.CanonicalFlowPath(),
	}
	binding := managedcapabilities.DeliveryBinding{
		Kind: managedcapabilities.BindingMCPTool, ExactName: "mcp__runtime-tools__send_story", RequiredEvidenceKind: "mcp_listed",
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorID: actor.ID, RuntimeMode: "task", Provider: "test", Transport: "cli", ProviderContract: "gateway-story-test",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityProviderTurn, ID: turnID, ExecutionKind: managedcapabilities.ExecutionNormalAgent,
			ExecutionAuthorityID: actor.ID, RunID: runID, SessionID: sessionID, TurnOrdinal: 1,
		},
		Tools: []managedcapabilities.PlannedTool{{
			Name: "send_story",
			DefinitionHash: runtimellm.ToolDefinitionIdentity(runtimellm.ToolDefinition{
				Name: definition.Name, Description: definition.Description, Schema: definition.InputSchema,
			}),
			Capability: toolcapabilities.Capability{Name: "send_story", Kind: toolcapabilities.KindStandard, Visible: true, Callable: true},
			Bindings:   []managedcapabilities.DeliveryBinding{binding},
		}},
	})
	if err != nil {
		t.Fatalf("build gateway story capability surface: %v", err)
	}
	surface, err = surface.Observe(managedcapabilities.DeliveryEvidence{
		BindingKind: binding.Kind, ExactName: binding.ExactName, Kind: binding.RequiredEvidenceKind, Status: managedcapabilities.EvidenceConfirmed,
	})
	if err != nil {
		t.Fatalf("confirm gateway story MCP delivery: %v", err)
	}
	admission, err := managedexecution.New(
		managedexecution.KindNormalRuntime,
		actor.ID,
		uint64(token.Generation),
		"",
		"gateway-story-actors",
		"gateway-story-bundle",
		[]string{surface.ID},
	)
	if err != nil {
		t.Fatalf("build gateway story managed execution admission: %v", err)
	}
	return surface, authority, admission
}

func gatewayStoryToolOffered(executor *runtimetools.Executor, actor models.AgentConfig, name string) bool {
	for _, definition := range executor.ToolDefinitionsForActor(actor) {
		if strings.TrimSpace(definition.Name) == strings.TrimSpace(name) {
			return true
		}
	}
	return false
}

func loadGatewayStorySource(t *testing.T, serverURL string) semanticview.Source {
	t.Helper()
	repoRoot := runtimepipeline.WorkflowRepoRoot()
	root := t.TempDir()
	writeGatewayStoryFixture(t, filepath.Join(root, "package.yaml"), `
name: mcp-gateway-story
version: "1.0.0"
platform_version: ">=0.7.0 <0.8.0"
flows:
  - id: story
    flow: story
    mode: static
`)
	writeGatewayStoryFixture(t, filepath.Join(root, "schema.yaml"), "name: mcp-gateway-story\n")
	writeGatewayStoryFixture(t, filepath.Join(root, "flows", "story", "schema.yaml"), `
name: story
mode: static
initial_state: queued
states: [queued, done]
terminal_states: [done]
`)
	writeGatewayStoryFixture(t, filepath.Join(root, "flows", "story", "agents.yaml"), `
story-writer:
  id: story-writer
  role: story-writer
  memory: false
  tools: [send_story]
`)
	writeGatewayStoryFixture(t, filepath.Join(root, "flows", "story", "tools.yaml"), `
send_story:
  description: Commit a selected-store story effect.
  handler_type: http
  input_schema:
    type: object
    additionalProperties: false
  http:
    method: GET
    url: `+serverURL+`
`)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repoRoot,
		root,
		runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	return semanticview.Wrap(bundle)
}

func writeGatewayStoryFixture(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s): %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(strings.TrimLeft(contents, "\n")), 0o644); err != nil {
		t.Fatalf("WriteFile(%s): %v", path, err)
	}
}

func seedGatewayStoryRuntime(t *testing.T, selected gatewayStorySelectedStore, runID, agentID string, source runtimecorrelation.BundleSourceFact) {
	t.Helper()
	now := time.Now().UTC()
	if selected.postgres {
		if _, err := selected.db.ExecContext(context.Background(), `
			INSERT INTO runs (run_id, status, bundle_hash, bundle_source, bundle_fingerprint, started_at)
			VALUES ($1::uuid, 'running', $2, $3, $4, $5)
		`, runID, source.BundleHash, source.BundleSource, source.BundleFingerprint, now); err != nil {
			t.Fatalf("seed selected-store run: %v", err)
		}
		if _, err := selected.db.ExecContext(context.Background(), `
			INSERT INTO agents (agent_id, flow_instance, role, model, llm_backend, memory_enabled, memory_source, status, lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase, created_at)
			VALUES ($1, 'story/instance-1', 'story-writer', 'regular', 'mock', FALSE, 'platform_default', 'active', 7, 3, 'running', $2)
		`, agentID, now); err != nil {
			t.Fatalf("seed selected-store agent: %v", err)
		}
		return
	}
	if _, err := selected.db.ExecContext(context.Background(), `
			INSERT INTO runs (run_id, status, bundle_hash, bundle_source, bundle_fingerprint, started_at)
			VALUES (?, 'running', ?, ?, ?, ?)
		`, runID, source.BundleHash, source.BundleSource, source.BundleFingerprint, now); err != nil {
		t.Fatalf("seed selected-store run: %v", err)
	}
	if _, err := selected.db.ExecContext(context.Background(), `
			INSERT INTO agents (agent_id, flow_instance, role, model, llm_backend, memory_enabled, memory_source, status, lifecycle_runtime_epoch, lifecycle_generation, lifecycle_phase, created_at)
			VALUES (?, 'story/instance-1', 'story-writer', 'regular', 'mock', 0, 'platform_default', 'active', 7, 3, 'running', ?)
		`, agentID, now); err != nil {
		t.Fatalf("seed selected-store agent: %v", err)
	}
}

func gatewayStoryInboundEvent(runID string, actor models.AgentConfig) events.Event {
	return eventtest.RootIngress(
		uuid.NewString(),
		events.EventType("story.review_requested"),
		"runtime",
		"",
		[]byte(`{"request":"review"}`),
		0,
		runID,
		"",
		events.EnvelopeForFlowInstance(events.EventEnvelope{}, actor.CanonicalFlowPath()),
		time.Now().UTC(),
	)
}

func callGatewayStoryTool(t *testing.T, gateway *runtimemcp.Gateway, token, toolName string, arguments map[string]any) map[string]any {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      uuid.NewString(),
		"method":  "tools/call",
		"params": map[string]any{
			"name":      toolName,
			"arguments": arguments,
			"_meta": map[string]any{
				"claudecode/toolUseId": "toolu-" + uuid.NewString(),
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal tools/call request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer "+gatewayStoryAuthToken)
	req.Header.Set("X-SWARM-Context-Token", token)
	rec := httptest.NewRecorder()
	gateway.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("tools/call %s status = %d body=%s", toolName, rec.Code, rec.Body.String())
	}
	var rpc map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &rpc); err != nil {
		t.Fatalf("decode tools/call %s response: %v", toolName, err)
	}
	result, ok := rpc["result"].(map[string]any)
	if !ok {
		t.Fatalf("tools/call %s response = %#v", toolName, rpc)
	}
	return result
}

func gatewayStoryResponseIsError(result map[string]any) bool {
	isError, _ := result["isError"].(bool)
	return isError
}

func assertGatewayStoryEffectAndOccurrence(t *testing.T, selected gatewayStorySelectedStore, scope runtimeauthoractivity.Scope, want int) {
	t.Helper()
	query := `SELECT o.bundle_hash, o.execution_mode, a.execution_mode FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id WHERE a.adapter = ? AND a.state = 'settled'`
	if selected.postgres {
		query = `SELECT o.bundle_hash, o.execution_mode, a.execution_mode FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id WHERE a.adapter = $1 AND a.state = 'settled'`
	}
	rows, err := selected.db.QueryContext(context.Background(), query, "authored_http_tool")
	if err != nil {
		t.Fatalf("read authored HTTP effects: %v", err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var bundleHash, operationMode, attemptMode string
		if err := rows.Scan(&bundleHash, &operationMode, &attemptMode); err != nil {
			t.Fatalf("scan authored HTTP effect: %v", err)
		}
		if bundleHash != scope.BundleHash || operationMode != string(runtimeeffects.ExecutionModeLive) || attemptMode != string(runtimeeffects.ExecutionModeLive) {
			t.Fatalf("authored HTTP effect semantics = bundle:%q operation_mode:%q attempt_mode:%q, want %q/live/live", bundleHash, operationMode, attemptMode, scope.BundleHash)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate authored HTTP effects: %v", err)
	}
	if count != want {
		t.Fatalf("settled authored HTTP effects = %d, want %d", count, want)
	}
	result, err := selected.backend.ListAuthorActivity(context.Background(), runtimeauthoractivity.ListOptions{
		RuntimeInstanceID: scope.RuntimeInstanceID,
		BundleHashes:      []string{scope.BundleHash},
		Limit:             100,
	})
	if err != nil {
		t.Fatalf("ListAuthorActivity: %v", err)
	}
	count = 0
	for _, occurrence := range result.Occurrences {
		if occurrence.Kind != runtimeauthoractivity.KindEffectLifecycle || occurrence.Transition != "launched" || occurrence.Projection.Adapter != "authored_http_tool" {
			continue
		}
		if occurrence.Scope != scope {
			t.Fatalf("effect occurrence scope = %#v, want %#v", occurrence.Scope, scope)
		}
		count++
	}
	if count != want {
		t.Fatalf("effect.lifecycle/launched occurrences = %d, want %d", count, want)
	}
}
