package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	"github.com/division-sh/swarm/internal/runtime/core/toolresultpolicy"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	"github.com/division-sh/swarm/internal/runtime/failures"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/google/uuid"
)

const testGatewayToken = "gateway-token"

func authorizeGatewayRequest(req *http.Request) {
	if req != nil {
		req.Header.Set("Authorization", "Bearer "+testGatewayToken)
	}
}

func withContextToken(req *http.Request, token string) *http.Request {
	if req != nil {
		req.Header.Set(contextTokenHeader, strings.TrimSpace(token))
	}
	return req
}

func mustRPCResponse(t *testing.T, rec *httptest.ResponseRecorder) RPCResponse {
	t.Helper()
	var resp RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	return resp
}

func mustMCPToolsForRequest(t *testing.T, g *Gateway, req *http.Request) []ToolDef {
	t.Helper()
	tools, err := g.mcpToolsForRequest(req)
	if err != nil {
		t.Fatalf("mcpToolsForRequest: %v", err)
	}
	return tools
}

func roleScopedTypedReadContext(name string) context.Context {
	return toolcapabilities.WithContext(unmanagedMCPTestContext(), toolcapabilities.NewSet([]toolcapabilities.Capability{{
		Name:               name,
		Visible:            true,
		Callable:           true,
		AuthorizationClass: toolresultpolicy.RoleScopedEntityToolAuthorizationClass,
	}}))
}

func largeValidationCasePayloadForTypedReadTest() map[string]any {
	problem := strings.Repeat("problem statement ", 350)
	return map[string]any{
		"entity_id":              "validation-case-1",
		"entity_type":            "validation_case",
		"flow_instance":          "validation/inst-1",
		"mvp_problem_statement":  problem,
		"current_state":          "mvp_speccing",
		"revision":               float64(3),
		"business_brief_padding": strings.Repeat("business brief ", 900),
		"fields": map[string]any{
			"business_brief": map[string]any{
				"summary":    strings.Repeat("business brief ", 900),
				"confidence": float64(10),
			},
			"mvp_spec": map[string]any{
				"problem_statement":  problem,
				"technical_approach": strings.Repeat("technical approach ", 350),
			},
		},
	}
}

func asStringForGatewayTest(value any) string {
	if text, ok := value.(string); ok {
		return text
	}
	return ""
}

type testToolExecutor func(context.Context, string, any) (any, error)

func (f testToolExecutor) Execute(ctx context.Context, name string, input any) (any, error) {
	return f(ctx, name, input)
}

func (f testToolExecutor) ToolDefinitions() []llm.ToolDefinition {
	return []llm.ToolDefinition{
		{Name: "query_entities"},
		{Name: "read_file"},
		{Name: "write_file"},
		{Name: "emit_scan_requested"},
		{Name: "emit_score_dimension_complete"},
	}
}

func (f testToolExecutor) ToolDefinitionsForActor(models.AgentConfig) []llm.ToolDefinition {
	return f.ToolDefinitions()
}

func (f testToolExecutor) ToolCapabilitiesForActor(_ models.AgentConfig, names []string, requestAllowed map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, raw := range names {
		name := toolidentity.CanonicalName(raw)
		if name == "" {
			continue
		}
		visible := true
		callable := true
		if len(requestAllowed) > 0 {
			_, visible = requestAllowed[name]
			callable = visible
		}
		requirement := toolcapabilities.ContextRequirementTurnContext
		switch name {
		case "query_entities", "read_file":
			requirement = toolcapabilities.ContextRequirementActorContext
		}
		kind := toolcapabilities.KindStandard
		if toolidentity.IsEmitToolName(name) {
			kind = toolcapabilities.KindEmit
		}
		caps = append(caps, toolcapabilities.Capability{
			Name:               name,
			Kind:               kind,
			Visible:            visible,
			Callable:           callable,
			ContextRequirement: requirement,
		})
	}
	return toolcapabilities.NewSet(caps)
}

type relayAwareToolExecutorStub struct {
	execFn    func(context.Context, string, any) (any, error)
	relayRef  ToolResultRelayRef
	relayErr  error
	relayRaw  []byte
	relayTool string
}

func (s *relayAwareToolExecutorStub) Execute(ctx context.Context, name string, input any) (any, error) {
	if s.execFn != nil {
		return s.execFn(ctx, name, input)
	}
	return map[string]any{"ok": true}, nil
}

func (s *relayAwareToolExecutorStub) ToolDefinitions() []llm.ToolDefinition {
	return []llm.ToolDefinition{{Name: "read_file"}, {Name: "query_entities"}}
}

func (s *relayAwareToolExecutorStub) ToolDefinitionsForActor(models.AgentConfig) []llm.ToolDefinition {
	return s.ToolDefinitions()
}

func (s *relayAwareToolExecutorStub) ToolCapabilitiesForActor(_ models.AgentConfig, names []string, requestAllowed map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, raw := range names {
		name := toolidentity.CanonicalName(raw)
		if name == "" {
			continue
		}
		visible := true
		if len(requestAllowed) > 0 {
			_, visible = requestAllowed[name]
		}
		caps = append(caps, toolcapabilities.Capability{
			Name:               name,
			Visible:            visible,
			Callable:           visible,
			ContextRequirement: toolcapabilities.ContextRequirementTurnContext,
		})
	}
	return toolcapabilities.NewSet(caps)
}

func (s *relayAwareToolExecutorStub) PersistOversizedToolResultRelay(_ context.Context, toolName string, rawJSON []byte) (ToolResultRelayRef, error) {
	s.relayTool = strings.TrimSpace(toolName)
	s.relayRaw = append([]byte(nil), rawJSON...)
	if s.relayErr != nil {
		return ToolResultRelayRef{}, s.relayErr
	}
	return s.relayRef, nil
}

type actorScopedToolExecutorStub struct {
	defs      []llm.ToolDefinition
	actorDefs []llm.ToolDefinition
	callCount *int
}

func (s actorScopedToolExecutorStub) Execute(context.Context, string, any) (any, error) {
	if s.callCount != nil {
		*s.callCount++
	}
	return map[string]any{"ok": true}, nil
}

func (s actorScopedToolExecutorStub) ToolDefinitions() []llm.ToolDefinition {
	return append([]llm.ToolDefinition(nil), s.defs...)
}

func (s actorScopedToolExecutorStub) ToolDefinitionsForActor(models.AgentConfig) []llm.ToolDefinition {
	return append([]llm.ToolDefinition(nil), s.actorDefs...)
}

func (s actorScopedToolExecutorStub) ToolCapabilitiesForActor(_ models.AgentConfig, names []string, requestAllowed map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, raw := range names {
		name := toolidentity.CanonicalName(raw)
		if name == "" {
			continue
		}
		visible := true
		if len(requestAllowed) > 0 {
			_, visible = requestAllowed[name]
		}
		caps = append(caps, toolcapabilities.Capability{
			Name:     name,
			Visible:  visible,
			Callable: visible,
		})
	}
	return toolcapabilities.NewSet(caps)
}

type contextAwareRoleScopedExecutorStub struct {
	callCount int
}

func (s *contextAwareRoleScopedExecutorStub) Execute(context.Context, string, any) (any, error) {
	s.callCount++
	return map[string]any{"ok": true}, nil
}

func (s *contextAwareRoleScopedExecutorStub) ToolDefinitions() []llm.ToolDefinition {
	return []llm.ToolDefinition{{Name: "read_scan_campaign"}, {Name: "save_scan_campaign_mode"}, {Name: "emit_market_research_scan_complete"}}
}

func (s *contextAwareRoleScopedExecutorStub) ToolDefinitionsForActor(models.AgentConfig) []llm.ToolDefinition {
	return s.ToolDefinitions()
}

func (s *contextAwareRoleScopedExecutorStub) ToolDefinitionsForActorInContext(ctx context.Context, actor models.AgentConfig) []llm.ToolDefinition {
	if roleScopedCurrentEntityEligibleInGatewayTest(ctx) {
		return s.ToolDefinitionsForActor(actor)
	}
	return []llm.ToolDefinition{{Name: "emit_market_research_scan_complete"}}
}

func (s *contextAwareRoleScopedExecutorStub) ToolCapabilitiesForActor(actor models.AgentConfig, names []string, requestAllowed map[string]struct{}) toolcapabilities.Set {
	return roleScopedGatewayCapabilities(names, requestAllowed, true)
}

func (s *contextAwareRoleScopedExecutorStub) ToolCapabilitiesForActorInContext(ctx context.Context, actor models.AgentConfig, names []string, requestAllowed map[string]struct{}) toolcapabilities.Set {
	return roleScopedGatewayCapabilities(names, requestAllowed, roleScopedCurrentEntityEligibleInGatewayTest(ctx))
}

func roleScopedCurrentEntityEligibleInGatewayTest(ctx context.Context) bool {
	inbound, ok := runtimebus.InboundEventFromContext(ctx)
	return ok && strings.HasPrefix(inbound.EntityID(), "valid-")
}

func roleScopedGatewayCapabilities(names []string, requestAllowed map[string]struct{}, currentEntityEligible bool) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, raw := range names {
		name := toolidentity.CanonicalName(raw)
		if name == "" {
			continue
		}
		visible := true
		callable := true
		if len(requestAllowed) > 0 {
			_, visible = requestAllowed[name]
			callable = visible
		}
		if strings.HasPrefix(name, "read_scan_campaign") || strings.HasPrefix(name, "save_scan_campaign") {
			if !currentEntityEligible {
				visible = false
				callable = false
			}
		}
		caps = append(caps, toolcapabilities.Capability{
			Name:     name,
			Visible:  visible,
			Callable: callable,
		})
	}
	return toolcapabilities.NewSet(caps)
}

type capabilityAwareExecutorStub struct {
	callCount int
}

func (s *capabilityAwareExecutorStub) Execute(context.Context, string, any) (any, error) {
	s.callCount++
	return map[string]any{"ok": true}, nil
}

func (s *capabilityAwareExecutorStub) ToolDefinitions() []llm.ToolDefinition {
	return []llm.ToolDefinition{
		{Name: "emit_score_dimension_complete", Description: "emit"},
		{Name: "query_entities", Description: "query"},
	}
}

func (s *capabilityAwareExecutorStub) ToolDefinitionsForActor(models.AgentConfig) []llm.ToolDefinition {
	return s.ToolDefinitions()
}

func (s *capabilityAwareExecutorStub) ToolCapabilitiesForActor(_ models.AgentConfig, names []string, requestAllowed map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, raw := range names {
		name := toolidentity.CanonicalName(raw)
		callable := len(requestAllowed) == 0
		if len(requestAllowed) > 0 {
			_, callable = requestAllowed[name]
		}
		kind := toolcapabilities.KindStandard
		if toolidentity.IsEmitToolName(name) {
			kind = toolcapabilities.KindEmit
		}
		caps = append(caps, toolcapabilities.Capability{
			Name:     name,
			Kind:     kind,
			Visible:  callable,
			Callable: callable,
		})
	}
	return toolcapabilities.NewSet(caps)
}

type actorAwareToolExecutorStub struct {
	callCount int
}

func (s *actorAwareToolExecutorStub) Execute(context.Context, string, any) (any, error) {
	s.callCount++
	return map[string]any{"ok": true}, nil
}

func (s *actorAwareToolExecutorStub) ToolDefinitions() []llm.ToolDefinition {
	return []llm.ToolDefinition{
		{Name: "query_entities"},
		{Name: "emit_score_dimension_complete"},
	}
}

func (s *actorAwareToolExecutorStub) ToolDefinitionsForActor(models.AgentConfig) []llm.ToolDefinition {
	return s.ToolDefinitions()
}

func (s *actorAwareToolExecutorStub) ToolCapabilitiesForActor(actor models.AgentConfig, names []string, _ map[string]struct{}) toolcapabilities.Set {
	caps := make([]toolcapabilities.Capability, 0, len(names))
	for _, raw := range names {
		name := toolidentity.CanonicalName(raw)
		if name == "" {
			continue
		}
		visible := true
		callable := true
		if name == "emit_score_dimension_complete" && strings.TrimSpace(actor.Role) != "campaign_coordinator" {
			visible = false
			callable = false
		}
		kind := toolcapabilities.KindStandard
		if toolidentity.IsEmitToolName(name) {
			kind = toolcapabilities.KindEmit
		}
		caps = append(caps, toolcapabilities.Capability{
			Name:               name,
			Kind:               kind,
			Visible:            visible,
			Callable:           callable,
			ContextRequirement: toolcapabilities.ContextRequirementTurnContext,
		})
	}
	return toolcapabilities.NewSet(caps)
}

func newTestTurnContextRegistry() *TurnContextRegistry {
	return NewTurnContextRegistry(nil)
}

func putTestTurnContext(t testing.TB, registry *TurnContextRegistry, token string, turn TurnContext) {
	t.Helper()
	if turn.CapabilitySurface != nil && !turn.HasExecutionAdmission {
		generation := uint64(1)
		if turn.LifecycleToken.Generation > 0 {
			generation = uint64(turn.LifecycleToken.Generation)
		}
		admission, err := managedexecution.New(
			managedexecution.KindNormalRuntime,
			turn.CapabilitySurface.Authority.ExecutionAuthorityID,
			generation,
			"",
			"gateway-test-actors",
			"gateway-test-bundle",
			nil,
		)
		if err != nil {
			t.Fatalf("build gateway test execution admission: %v", err)
		}
		turn.ExecutionAdmission = admission
		turn.HasExecutionAdmission = true
	}
	if turn.CapabilitySurface != nil && turn.HasLifecycleToken && !turn.HasEffectAuthority {
		authority := runtimeeffects.NormalAgentAuthority(
			turn.LifecycleToken,
			"gateway-test:"+turn.LifecycleToken.AgentID,
			time.Now().UTC().Add(time.Minute),
		)
		authority.Target = runtimeeffects.UsageTarget{
			Kind: runtimeeffects.UsageTargetAgentTurn, ID: turn.CapabilitySurface.Authority.ID,
			RunID: turn.CapabilitySurface.Authority.RunID, AgentID: turn.CapabilitySurface.ActorID,
			SessionID: turn.CapabilitySurface.Authority.SessionID, Memory: agentmemory.PlatformDefault(),
			FlowInstance: strings.TrimSpace(turn.Actor.CanonicalFlowPath()),
		}
		turn.EffectAuthority = authority
		turn.HasEffectAuthority = true
	}
	registry.PutTurnContextForTest(token, turn)
	t.Cleanup(func() {
		registry.UnregisterTurnContext(token)
	})
}

func testCapabilitySurface(t testing.TB, actor models.AgentConfig, names ...string) *managedcapabilities.Surface {
	t.Helper()
	definitions := make([]llm.ToolDefinition, 0, len(names))
	for _, name := range names {
		definitions = append(definitions, llm.ToolDefinition{Name: toolidentity.CanonicalName(name)})
	}
	return testCapabilitySurfaceForDefinitions(t, actor, definitions...)
}

func testCapabilitySurfaceForDefinitions(t testing.TB, actor models.AgentConfig, definitions ...llm.ToolDefinition) *managedcapabilities.Surface {
	t.Helper()
	planned := make([]managedcapabilities.PlannedTool, 0, len(definitions))
	for _, definition := range definitions {
		canonical := toolidentity.CanonicalName(definition.Name)
		kind := toolcapabilities.KindStandard
		if toolidentity.IsEmitToolName(canonical) {
			kind = toolcapabilities.KindEmit
		}
		planned = append(planned, managedcapabilities.PlannedTool{
			Name: canonical, DefinitionHash: llm.ToolDefinitionIdentity(llmToolDefinitionForMCP(mcpToolDefinition(canonical, definition))),
			Capability: toolcapabilities.Capability{Name: canonical, Kind: kind, Visible: true, Callable: true},
			Bindings:   []managedcapabilities.DeliveryBinding{{Kind: managedcapabilities.BindingMCPTool, ExactName: toolidentity.RuntimeToolsMCPPrefix + canonical, RequiredEvidenceKind: "mcp_listed"}},
		})
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorID: actor.ID, RuntimeMode: "task", Provider: "test", Transport: "cli", ProviderContract: "test-contract",
		Authority: managedcapabilities.Authority{Kind: managedcapabilities.AuthorityProviderTurn, ID: uuid.NewString(), ExecutionKind: managedcapabilities.ExecutionNormalAgent, ExecutionAuthorityID: actor.ID, RunID: uuid.NewString(), SessionID: uuid.NewString(), TurnOrdinal: 1},
		Tools:     planned,
	})
	if err != nil {
		t.Fatalf("build test capability surface: %v", err)
	}
	var evidence []managedcapabilities.DeliveryEvidence
	for _, tool := range surface.Tools {
		for _, binding := range tool.Bindings {
			evidence = append(evidence, managedcapabilities.DeliveryEvidence{BindingKind: binding.Kind, ExactName: binding.ExactName, Kind: "mcp_listed", Status: managedcapabilities.EvidenceConfirmed})
		}
	}
	surface, err = surface.Observe(evidence...)
	if err != nil {
		t.Fatalf("observe test capability surface: %v", err)
	}
	return &surface
}

func managedCLIGatewayHooks(registry *TurnContextRegistry) GatewayHooks {
	return GatewayHooks{
		ResolveTurnContext:        registry.ResolveTurnContext,
		ObserveCapabilityEvidence: registry.ObserveCapabilityEvidence,
		ObserveCapabilityMismatch: registry.ObserveCapabilityMismatch,
		ObserveMCPProviderCall:    registry.ObserveMCPProviderCall,
		WithActor:                 models.WithActor,
	}
}

func callMCPGateway(t *testing.T, gateway *Gateway, contextToken string, request RPCRequest) RPCResponse {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal MCP request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body)), contextToken)
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	gateway.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("gateway status = %d body=%s", rec.Code, rec.Body.String())
	}
	return mustRPCResponse(t, rec)
}

func TestGatewayHandleMCP_ProviderCallCoordinateSeparatesSiblingsAndFencesReplay(t *testing.T) {
	harness := effecttest.New()
	registry := newTestTurnContextRegistry()
	controller := runtimeeffects.NewController(harness)
	putTurn := func(token, identity string) {
		putTestTurnContext(t, registry, token, TurnContext{
			Actor:              models.AgentConfig{ExecutionMode: "live", ID: harness.Token.AgentID},
			LifecycleToken:     harness.Token,
			HasLifecycleToken:  true,
			EffectController:   controller,
			LogicalIdentity:    identity,
			HasLogicalIdentity: true,
			CapabilitySurface:  testCapabilitySurface(t, models.AgentConfig{ID: harness.Token.AgentID}, "write_file"),
		})
	}
	putTurn("ctx-provider-turn-1", "provider-turn-1")
	putTurn("ctx-provider-turn-2", "provider-turn-2")

	primitiveDispatches := 0
	gateway := NewGateway(testToolExecutor(func(ctx context.Context, name string, input any) (any, error) {
		request, err := json.Marshal(map[string]any{"name": name, "arguments": input})
		if err != nil {
			return nil, err
		}
		attempt, err := runtimeeffects.Begin(ctx, "authored_http_tool", request, map[string]string{"tool": name})
		if err != nil {
			return nil, err
		}
		if err := attempt.MarkLaunched(ctx); err != nil {
			return nil, err
		}
		primitiveDispatches++
		if err := attempt.Succeed(ctx, map[string]any{"ok": true}); err != nil {
			return nil, err
		}
		return map[string]any{"ok": true}, nil
	}), testGatewayToken, GatewayHooks{ResolveTurnContext: registry.ResolveTurnContext})

	call := func(contextToken string, transportID any, toolUseID string, progressToken any) RPCResponse {
		t.Helper()
		body, err := json.Marshal(RPCRequest{
			JSONRPC: "2.0",
			Method:  "tools/call",
			ID:      transportID,
			Params: map[string]any{
				"name":      "write_file",
				"arguments": map[string]any{"path": "/workspace/result.txt", "content": "same"},
				"_meta": map[string]any{
					claudeCodeToolUseIDMetaKey: toolUseID,
					"progressToken":            progressToken,
				},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body)), contextToken)
		authorizeGatewayRequest(req)
		rec := httptest.NewRecorder()
		gateway.Handler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("gateway status = %d body=%s", rec.Code, rec.Body.String())
		}
		return mustRPCResponse(t, rec)
	}

	if resp := call("ctx-provider-turn-1", float64(1), "toolu-call-1", float64(1)); resp.Error != nil {
		t.Fatalf("first sibling failed: %#v", resp.Error)
	} else if result, ok := resp.Result.(map[string]any); ok && result["isError"] == true {
		t.Fatalf("first sibling returned tool error: %#v", result)
	}
	if resp := call("ctx-provider-turn-1", float64(2), "toolu-call-2", float64(2)); resp.Error != nil {
		t.Fatalf("second identical sibling failed: %#v", resp.Error)
	}
	if primitiveDispatches != 2 || len(harness.Attempts) != 2 {
		t.Fatalf("same-turn sibling dispatches=%d attempts=%d, want 2/2", primitiveDispatches, len(harness.Attempts))
	}

	replay := call("ctx-provider-turn-1", "replacement-json-rpc-id", "toolu-call-1", "replacement-progress-token")
	replayResult, ok := replay.Result.(map[string]any)
	if !ok || replayResult["isError"] != true {
		t.Fatalf("replay result = %#v, want tool error result", replay.Result)
	}
	if primitiveDispatches != 2 || len(harness.Attempts) != 2 {
		t.Fatalf("transport-correlation replay redispatched: dispatches=%d attempts=%d", primitiveDispatches, len(harness.Attempts))
	}

	if resp := call("ctx-provider-turn-2", float64(1), "toolu-call-1", float64(1)); resp.Error != nil {
		t.Fatalf("cross-turn sibling failed: %#v", resp.Error)
	}
	if primitiveDispatches != 3 || len(harness.Attempts) != 3 {
		t.Fatalf("cross-turn sibling dispatches=%d attempts=%d, want 3/3", primitiveDispatches, len(harness.Attempts))
	}
}

func TestGatewayHandleMCP_ManagedClaudeCLIChronologyNormalAndSelectedFork(t *testing.T) {
	for _, executionKind := range []managedcapabilities.ExecutionKind{
		managedcapabilities.ExecutionNormalAgent,
		managedcapabilities.ExecutionSelectedContractFork,
	} {
		t.Run(string(executionKind), func(t *testing.T) {
			ctx, surface, harness := managedClaudeProviderTurnTestContext(t, executionKind)
			if names := surface.BindingNames(managedcapabilities.BindingMCPTool); !slices.Equal(names, []string{"mcp__runtime-tools__write_file"}) {
				t.Fatalf("managed CLI MCP catalog bindings = %#v", names)
			}
			if names := surface.BindingNames(managedcapabilities.BindingMCPProvider); !slices.Equal(names, []string{"mcp__runtime-tools__write_file"}) {
				t.Fatalf("managed CLI MCP provider bindings = %#v", names)
			}
			if len(surface.Tools) != 1 || len(surface.Tools[0].Evidence) != 0 {
				t.Fatalf("managed CLI registration pre-confirmed evidence: %#v", surface.Tools)
			}
			registry := NewTurnContextRegistry(models.ActorFromContext)
			authority, ok := runtimeeffects.AuthorityFromContext(ctx)
			if !ok {
				t.Fatal("managed Claude effect authority is missing")
			}

			for _, hostile := range []struct {
				name   string
				mutate func(*runtimeeffects.Authority)
			}{
				{name: "missing target", mutate: func(a *runtimeeffects.Authority) { a.Target = runtimeeffects.UsageTarget{} }},
				{name: "different actor", mutate: func(a *runtimeeffects.Authority) { a.Target.AgentID = "different-agent" }},
				{name: "different turn", mutate: func(a *runtimeeffects.Authority) { a.Target.ID = uuid.NewString() }},
				{name: "different session", mutate: func(a *runtimeeffects.Authority) { a.Target.SessionID = uuid.NewString() }},
				{name: "different run", mutate: func(a *runtimeeffects.Authority) { a.Target.RunID = uuid.NewString() }},
			} {
				hostileAuthority := authority
				hostile.mutate(&hostileAuthority)
				if token := registry.RegisterTurnContextWithCapabilitySurface(runtimeeffects.WithAuthority(ctx, hostileAuthority), time.Minute, surface); token != "" {
					t.Fatalf("%s registration returned token %q", hostile.name, token)
				}
			}
			if len(harness.Attempts) != 0 {
				t.Fatalf("hostile registrations authorized %d attempts, want zero", len(harness.Attempts))
			}

			token := registry.RegisterTurnContextWithCapabilitySurface(ctx, time.Minute, surface)
			if token == "" {
				t.Fatal("exact managed Claude provider turn was not registered")
			}
			turn, ok := registry.ResolveTurnContext(token)
			if !ok || !turn.HasEffectAuthority ||
				!runtimeeffects.ProviderTurnTargetMatchesCapabilitySurface(turn.EffectAuthority.Target, surface) {
				t.Fatalf("registered turn context lost exact provider target: %#v", turn)
			}

			executorCalls := 0
			dispatches := 0
			gateway := NewGateway(testToolExecutor(func(callCtx context.Context, name string, input any) (any, error) {
				executorCalls++
				request, err := json.Marshal(map[string]any{"name": name, "arguments": input})
				if err != nil {
					return nil, err
				}
				handle, err := runtimeeffects.Begin(callCtx, "authored_http_tool", request, map[string]string{"tool": name})
				if err != nil {
					return nil, err
				}
				if err := handle.MarkLaunched(callCtx); err != nil {
					return nil, err
				}
				dispatches++
				if err := handle.Succeed(callCtx, map[string]any{"ok": true}); err != nil {
					return nil, err
				}
				return map[string]any{"ok": true}, nil
			}), testGatewayToken, managedCLIGatewayHooks(registry))

			listed := callMCPGateway(t, gateway, token, RPCRequest{JSONRPC: "2.0", Method: "tools/list", ID: "list-1", Params: map[string]any{}})
			if listed.Error != nil {
				t.Fatalf("managed Claude tools/list error = %#v", listed.Error)
			}
			listedTurn, ok := registry.ResolveTurnContext(token)
			if !ok || listedTurn.CapabilitySurface == nil {
				t.Fatal("managed Claude tools/list did not preserve the turn surface")
			}
			if names := listedTurn.CapabilitySurface.EffectiveNames(); len(names) != 0 {
				t.Fatalf("tools/list pre-confirmed provider visibility: effective names = %#v", names)
			}
			listedEvidence := listedTurn.CapabilitySurface.Tools[0].Evidence
			if len(listedEvidence) != 1 || listedEvidence[0].BindingKind != managedcapabilities.BindingMCPTool || listedEvidence[0].Kind != "mcp_listed" {
				t.Fatalf("tools/list evidence = %#v, want only exact MCP catalog confirmation", listedEvidence)
			}

			call := RPCRequest{
				JSONRPC: "2.0", Method: "tools/call", ID: float64(1),
				Params: map[string]any{
					"name": "write_file", "arguments": map[string]any{"path": "/workspace/result.txt", "content": "ok"},
					"_meta": map[string]any{claudeCodeToolUseIDMetaKey: "toolu-exact-provider-turn"},
				},
			}
			resp := callMCPGateway(t, gateway, token, call)
			if resp.Error != nil {
				t.Fatalf("managed Claude tools/call error = %#v", resp.Error)
			}
			if result, ok := resp.Result.(map[string]any); ok && result["isError"] == true {
				t.Fatalf("managed Claude tools/call result = %#v", result)
			}
			if executorCalls != 1 || dispatches != 1 || len(harness.Attempts) != 1 {
				t.Fatalf("managed Claude executor=%d dispatches=%d persisted attempts=%d, want 1/1/1", executorCalls, dispatches, len(harness.Attempts))
			}
			if state, ok := harness.StateForAdapter("authored_http_tool"); !ok || state != runtimeeffects.StateSettled {
				t.Fatalf("managed Claude effect state=%q present=%t, want settled", state, ok)
			}
			settledTurn, ok := registry.ResolveTurnContext(token)
			if !ok || settledTurn.CapabilitySurface == nil || !slices.Equal(settledTurn.CapabilitySurface.EffectiveNames(), []string{"write_file"}) {
				t.Fatalf("provider call did not settle exact MCP visibility: %#v", settledTurn.CapabilitySurface)
			}
			settledEvidence := settledTurn.CapabilitySurface.Tools[0].Evidence
			providerConfirmed := false
			for _, evidence := range settledEvidence {
				providerConfirmed = providerConfirmed || (evidence.BindingKind == managedcapabilities.BindingMCPProvider && evidence.Kind == "mcp_visible")
			}
			if len(settledEvidence) != 2 || !providerConfirmed {
				t.Fatalf("provider-call evidence = %#v, want exact MCP provider confirmation", settledEvidence)
			}
			if _, err := llm.ObserveCLIResponseCapabilitySurface(settledTurn.CapabilitySurface.Clone(), &llm.Response{
				MCPServers: map[string]string{"runtime-tools": "connected"}, MCPVisibleTools: []string{"mcp__runtime-tools__write_file"},
			}); err != nil {
				t.Fatalf("provider response could not confirm the call-settled canonical evidence: %v", err)
			}

			call.ID = "replacement-transport-id"
			replay := callMCPGateway(t, gateway, token, call)
			if replayResult, ok := replay.Result.(map[string]any); !ok || replayResult["isError"] != true {
				t.Fatalf("replayed provider occurrence = %#v, want fail-closed tool error", replay)
			}
			if executorCalls != 1 || dispatches != 1 || len(harness.Attempts) != 1 {
				t.Fatalf("replay reached execution: executor=%d dispatches=%d attempts=%d", executorCalls, dispatches, len(harness.Attempts))
			}
			replayedTurn, ok := registry.ResolveTurnContext(token)
			if !ok || replayedTurn.CapabilitySurface == nil || !replayedTurn.CapabilitySurface.HasMismatch() || len(replayedTurn.CapabilitySurface.EffectiveNames()) != 0 {
				t.Fatalf("replay did not fail the canonical turn surface closed: %#v", replayedTurn.CapabilitySurface)
			}
		})
	}
}

func TestGatewayHandleMCP_ManagedClaudeCLIRejectsHostileCallsBeforeExecutor(t *testing.T) {
	for _, hostile := range []struct {
		name          string
		toolName      string
		providerID    string
		mutateContext func(*TurnContext)
	}{
		{name: "unplanned tool", toolName: "read_file", providerID: "toolu-unplanned"},
		{name: "wrong authority", toolName: "write_file", providerID: "toolu-wrong-authority", mutateContext: func(turn *TurnContext) {
			turn.EffectAuthority.Target.RunID = uuid.NewString()
		}},
		{name: "missing call coordinate", toolName: "write_file"},
	} {
		t.Run(hostile.name, func(t *testing.T) {
			ctx, surface, harness := managedClaudeProviderTurnTestContext(t, managedcapabilities.ExecutionNormalAgent)
			registry := NewTurnContextRegistry(models.ActorFromContext)
			token := registry.RegisterTurnContextWithCapabilitySurface(ctx, time.Minute, surface)
			if token == "" {
				t.Fatal("register production-shaped managed CLI turn")
			}
			executorCalls := 0
			gateway := NewGateway(testToolExecutor(func(context.Context, string, any) (any, error) {
				executorCalls++
				return map[string]any{"ok": true}, nil
			}), testGatewayToken, managedCLIGatewayHooks(registry))
			if resp := callMCPGateway(t, gateway, token, RPCRequest{JSONRPC: "2.0", Method: "tools/list", ID: "list-hostile", Params: map[string]any{}}); resp.Error != nil {
				t.Fatalf("hostile setup tools/list error = %#v", resp.Error)
			}
			if hostile.mutateContext != nil {
				turn, ok := registry.ResolveTurnContext(token)
				if !ok {
					t.Fatal("resolve hostile turn")
				}
				hostile.mutateContext(&turn)
				registry.PutTurnContextForTest(token, turn)
			}
			params := map[string]any{"name": hostile.toolName, "arguments": map[string]any{}}
			if hostile.providerID != "" {
				params["_meta"] = map[string]any{claudeCodeToolUseIDMetaKey: hostile.providerID}
			}
			resp := callMCPGateway(t, gateway, token, RPCRequest{JSONRPC: "2.0", Method: "tools/call", ID: "hostile-call", Params: params})
			if result, ok := resp.Result.(map[string]any); resp.Error == nil && (!ok || result["isError"] != true) {
				t.Fatalf("hostile call was not rejected: %#v", resp)
			}
			if executorCalls != 0 || len(harness.Attempts) != 0 {
				t.Fatalf("hostile call reached execution: executor=%d attempts=%d", executorCalls, len(harness.Attempts))
			}
			turn, ok := registry.ResolveTurnContext(token)
			if !ok || turn.CapabilitySurface == nil || !turn.CapabilitySurface.HasMismatch() || len(turn.CapabilitySurface.EffectiveNames()) != 0 {
				t.Fatalf("hostile call did not fail the canonical turn surface closed: %#v", turn.CapabilitySurface)
			}
		})
	}
}

func TestGatewayHandleMCP_ToolsListRejectsSameNameDefinitionIdentityDrift(t *testing.T) {
	for _, changedDefinition := range []llm.ToolDefinition{
		{Name: "write_file", Description: "changed description"},
		{Name: "write_file", Schema: map[string]any{"type": "object", "required": []any{"path"}}},
	} {
		name := "description"
		if changedDefinition.Schema != nil {
			name = "schema"
		}
		t.Run(name, func(t *testing.T) {
			ctx, surface, _ := managedClaudeProviderTurnTestContext(t, managedcapabilities.ExecutionNormalAgent)
			registry := NewTurnContextRegistry(models.ActorFromContext)
			token := registry.RegisterTurnContextWithCapabilitySurface(ctx, time.Minute, surface)
			if token == "" {
				t.Fatal("register production-shaped managed CLI turn")
			}
			executorCalls := 0
			gateway := NewGateway(actorScopedToolExecutorStub{
				actorDefs: []llm.ToolDefinition{changedDefinition}, callCount: &executorCalls,
			}, testGatewayToken, managedCLIGatewayHooks(registry))
			resp := callMCPGateway(t, gateway, token, RPCRequest{JSONRPC: "2.0", Method: "tools/list", ID: "list-drift", Params: map[string]any{}})
			if resp.Error == nil {
				t.Fatalf("same-name changed %s definition was accepted: %#v", name, resp)
			}
			turn, ok := registry.ResolveTurnContext(token)
			if !ok || turn.CapabilitySurface == nil || !turn.CapabilitySurface.HasMismatch() {
				t.Fatalf("definition drift was not settled on canonical surface: %#v", turn.CapabilitySurface)
			}
			if len(turn.CapabilitySurface.Mismatches) != 1 || turn.CapabilitySurface.Mismatches[0].Kind != "mcp_definition_identity_mismatch" {
				t.Fatalf("definition drift mismatch = %#v", turn.CapabilitySurface.Mismatches)
			}
			if executorCalls != 0 {
				t.Fatalf("definition drift reached executor %d times", executorCalls)
			}
		})
	}
}

func TestGatewayHandleMCP_ManagedCallWithoutProviderCoordinateFailsBeforeExecutor(t *testing.T) {
	harness := effecttest.New()
	registry := newTestTurnContextRegistry()
	putTestTurnContext(t, registry, "ctx-managed", TurnContext{
		Actor:              models.AgentConfig{ExecutionMode: "live", ID: harness.Token.AgentID},
		LifecycleToken:     harness.Token,
		HasLifecycleToken:  true,
		EffectController:   runtimeeffects.NewController(harness),
		LogicalIdentity:    "provider-turn",
		HasLogicalIdentity: true,
		CapabilitySurface:  testCapabilitySurface(t, models.AgentConfig{ID: harness.Token.AgentID}, "write_file"),
	})
	executed := false
	gateway := NewGateway(testToolExecutor(func(context.Context, string, any) (any, error) {
		executed = true
		return nil, nil
	}), testGatewayToken, GatewayHooks{ResolveTurnContext: registry.ResolveTurnContext})
	body, err := json.Marshal(RPCRequest{JSONRPC: "2.0", Method: "tools/call", ID: float64(1), Params: map[string]any{
		"name": "write_file", "arguments": map[string]any{"path": "/workspace/result.txt"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body)), "ctx-managed")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	gateway.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("gateway status = %d body=%s", rec.Code, rec.Body.String())
	}
	resp := mustRPCResponse(t, rec)
	if resp.Error == nil && resp.Result == nil {
		t.Fatalf("missing-coordinate response = %#v", resp)
	}
	if executed {
		t.Fatal("managed call without provider coordinate reached executor")
	}
}

type gatewayCtxProbeKey string

type gatewayCtxObservation struct {
	value       string
	hasDeadline bool
	deadline    time.Time
	err         error
}

func runGatewayTransportPair(t *testing.T, reqCtx context.Context) (gatewayCtxObservation, gatewayCtxObservation) {
	t.Helper()

	registry := newTestTurnContextRegistry()
	observations := make([]gatewayCtxObservation, 0, 2)
	g := NewGateway(testToolExecutor(func(ctx context.Context, name string, input any) (any, error) {
		t.Helper()
		if name != "query_entities" {
			t.Fatalf("tool name = %q, want query_entities", name)
		}
		value, _ := ctx.Value(gatewayCtxProbeKey("probe")).(string)
		deadline, hasDeadline := ctx.Deadline()
		observations = append(observations, gatewayCtxObservation{
			value:       value,
			hasDeadline: hasDeadline,
			deadline:    deadline,
			err:         ctx.Err(),
		})
		return map[string]any{"ok": true}, nil
	}), testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-transport-probe", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, "query_entities"),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	mcpBody, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "query_entities",
			"arguments": map[string]any{"query": "kind = 'vertical'"},
		},
	})
	if err != nil {
		t.Fatalf("marshal mcp request: %v", err)
	}
	mcpReq := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(string(mcpBody))), "ctx-transport-probe").WithContext(reqCtx)
	authorizeGatewayRequest(mcpReq)
	mcpRec := httptest.NewRecorder()
	g.Handler().ServeHTTP(mcpRec, mcpReq)
	if mcpRec.Code != http.StatusOK {
		t.Fatalf("mcp status = %d body=%s", mcpRec.Code, mcpRec.Body.String())
	}

	toolBody, err := json.Marshal(ToolGatewayRequest{
		Input: map[string]any{"query": "kind = 'vertical'"},
	})
	if err != nil {
		t.Fatalf("marshal tool request: %v", err)
	}
	toolReq := withContextToken(httptest.NewRequest(http.MethodPost, "/tools/query_entities", strings.NewReader(string(toolBody))), "ctx-transport-probe").WithContext(reqCtx)
	authorizeGatewayRequest(toolReq)
	toolRec := httptest.NewRecorder()
	g.Handler().ServeHTTP(toolRec, toolReq)
	if toolRec.Code != http.StatusOK {
		t.Fatalf("tool status = %d body=%s", toolRec.Code, toolRec.Body.String())
	}

	if len(observations) != 2 {
		t.Fatalf("observation count = %d, want 2", len(observations))
	}
	return observations[0], observations[1]
}

func TestGatewayHydrateActor_PrefersResolvedRuntimeConfig(t *testing.T) {
	g := NewGateway(nil, "", GatewayHooks{
		ResolveActorConfig: func(agentID string) (models.AgentConfig, bool) {
			if agentID != "market-research-agent" {
				return models.AgentConfig{}, false
			}
			return models.AgentConfig{
				ExecutionMode: "live",
				ID:            "market-research-agent",
				Role:          "market_research",
				FlowID:        "discovery",
				EntityID:      "entity-1",
				Permissions:   []string{"schedule"},
				EmitEvents:    []string{"category.assessed", "market_research.scan_complete"},
			}, true
		},
	})

	hydrated := g.hydrateActor(models.AgentConfig{
		ExecutionMode: "live",
		ID:            "market-research-agent",
		Role:          "spoofed_role",
		FlowID:        "spoofed_mode",
	})

	if len(hydrated.EmitEvents) != 2 {
		t.Fatalf("emit_events = %#v, want two resolved events", hydrated.EmitEvents)
	}
	if hydrated.FlowID != "discovery" {
		t.Fatalf("flow_id = %q, want discovery", hydrated.FlowID)
	}
	if hydrated.Role != "market_research" {
		t.Fatalf("role = %q, want market_research", hydrated.Role)
	}
	if len(hydrated.Permissions) != 1 || hydrated.Permissions[0] != "schedule" {
		t.Fatalf("permissions = %#v", hydrated.Permissions)
	}
}

func TestNormalizeGatewayToolNameCanonicalAliases(t *testing.T) {
	tests := map[string]string{
		"":                                   "",
		"bash":                               "bash",
		"Bash":                               "bash",
		"web_search":                         "web_search",
		"WebFetch":                           "web_search",
		"WebSearch":                          "web_search",
		"Read":                               "read_file",
		"read_file":                          "read_file",
		"Write":                              "write_file",
		"Edit":                               "write_file",
		"mcp__runtime-tools__read_file":      "read_file",
		"mcp__runtime-tools__write_file":     "write_file",
		"mcp__runtime-tools__emit_scan_done": "emit_scan_done",
	}

	for raw, want := range tests {
		if got := normalizeGatewayToolName(raw); got != want {
			t.Fatalf("normalizeGatewayToolName(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestParseToolListHeaderCanonicalizesAliases(t *testing.T) {
	allowed := ParseToolListHeader("Read, mcp__runtime-tools__write_file, emit_scan_done")
	if len(allowed) != 3 {
		t.Fatalf("allowed size = %d, want 3", len(allowed))
	}
	for _, name := range []string{"read_file", "write_file", "emit_scan_done"} {
		if _, ok := allowed[name]; !ok {
			t.Fatalf("expected canonical allowed tool %q in %#v", name, allowed)
		}
	}
}

func TestGatewayMCPToolsForRequest_UsesHydratedActorRoleForEmitTools(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(nil, "", GatewayHooks{
		ResolveActorConfig: func(agentID string) (models.AgentConfig, bool) {
			if agentID != "campaign-coordinator" {
				return models.AgentConfig{}, false
			}
			return models.AgentConfig{
				ExecutionMode: "live",
				ID:            "campaign-coordinator",
				Role:          "campaign_coordinator",
			}, true
		},
		EmitTools: func(role string) []llm.ToolDefinition {
			if role != "campaign_coordinator" {
				return nil
			}
			return []llm.ToolDefinition{{
				Name:        "emit_scan_requested",
				Description: "Emit scan.requested",
				Schema:      map[string]any{"type": "object"},
			}}
		},
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-hydrated-role", TurnContext{
		Actor:             models.AgentConfig{ID: "campaign-coordinator"},
		CapabilitySurface: testCapabilitySurfaceForDefinitions(t, models.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"}, llm.ToolDefinition{Name: "emit_scan_requested", Description: "Emit scan.requested", Schema: map[string]any{"type": "object"}}),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	req := withContextToken(httptest.NewRequest("POST", "/mcp", nil), "ctx-hydrated-role")
	tools := mustMCPToolsForRequest(t, g, req)
	for _, tool := range tools {
		if tool.Name == "emit_scan_requested" {
			return
		}
	}
	t.Fatalf("emit_scan_requested not found in MCP tools: %#v", tools)
}

func TestGatewayMCPToolsForRequest_KeepsEmitToolsForDirectMCPContext(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(nil, "", GatewayHooks{
		ResolveActorConfig: func(agentID string) (models.AgentConfig, bool) {
			if agentID != "campaign-coordinator" {
				return models.AgentConfig{}, false
			}
			return models.AgentConfig{
				ExecutionMode: "live",
				ID:            "campaign-coordinator",
				Role:          "campaign_coordinator",
			}, true
		},
		EmitTools: func(role string) []llm.ToolDefinition {
			if role != "campaign_coordinator" {
				return nil
			}
			return []llm.ToolDefinition{{
				Name:        "emit_scan_requested",
				Description: "Emit scan.requested",
				Schema:      map[string]any{"type": "object"},
			}}
		},
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-1", TurnContext{
		Actor:             models.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"},
		CapabilitySurface: testCapabilitySurfaceForDefinitions(t, models.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"}, llm.ToolDefinition{Name: "emit_scan_requested", Description: "Emit scan.requested", Schema: map[string]any{"type": "object"}}),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	req := withContextToken(httptest.NewRequest("POST", "/mcp", nil), "ctx-1")
	tools := mustMCPToolsForRequest(t, g, req)
	for _, tool := range tools {
		if tool.Name == "emit_scan_requested" {
			return
		}
	}
	t.Fatalf("emit_scan_requested should remain visible for direct MCP context: %#v", tools)
}

func TestGatewayMCPToolsForRequest_PrefersActorScopedToolDefinitions(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(actorScopedToolExecutorStub{
		defs: []llm.ToolDefinition{
			{Name: "workflow_custom_tool", Description: "global"},
		},
		actorDefs: []llm.ToolDefinition{
			{Name: "query_entities", Description: "actor scoped", Usage: "Use CEL equality with ==."},
		},
	}, "", GatewayHooks{
		ResolveActorConfig: func(agentID string) (models.AgentConfig, bool) {
			return models.AgentConfig{
				ExecutionMode: "live",
				ID:            agentID,
				Role:          "analysis",
			}, true
		},
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-actor-scoped", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurfaceForDefinitions(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, llm.ToolDefinition{Name: "query_entities", Description: "actor scoped", Usage: "Use CEL equality with ==."}),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	req := withContextToken(httptest.NewRequest("POST", "/mcp", nil), "ctx-actor-scoped")
	tools := mustMCPToolsForRequest(t, g, req)
	if len(tools) != 1 {
		t.Fatalf("tool count = %d, want 1 (%#v)", len(tools), tools)
	}
	if tools[0].Name != "query_entities" {
		t.Fatalf("tool name = %q, want query_entities", tools[0].Name)
	}
	if !strings.Contains(tools[0].Description, "actor scoped\n\nUsage:\nUse CEL equality with ==.") {
		t.Fatalf("tool description = %q, want usage appended", tools[0].Description)
	}
}

func TestGatewayMCPToolsForRequest_ExposesFlowDataOnlyFromActorScopedCatalog(t *testing.T) {
	registry := newTestTurnContextRegistry()
	putTestTurnContext(t, registry, "ctx-flow-data", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurfaceForDefinitions(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, llm.ToolDefinition{Name: "read_flow_data", Description: "actor declared flow data", GeneratedSchema: true}),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})
	req := withContextToken(httptest.NewRequest("POST", "/mcp", nil), "ctx-flow-data")

	declaredGateway := NewGateway(actorScopedToolExecutorStub{
		defs: []llm.ToolDefinition{
			{Name: "read_flow_data", Description: "runtime-wide fallback must not leak"},
		},
		actorDefs: []llm.ToolDefinition{
			{Name: "read_flow_data", Description: "actor declared flow data", GeneratedSchema: true},
		},
	}, "", GatewayHooks{
		ResolveActorConfig: func(agentID string) (models.AgentConfig, bool) {
			return models.AgentConfig{ExecutionMode: "live", ID: agentID, Role: "analysis"}, true
		},
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	tools := mustMCPToolsForRequest(t, declaredGateway, req)
	if len(tools) != 1 {
		t.Fatalf("tool count = %d, want 1 (%#v)", len(tools), tools)
	}
	if tools[0].Name != "read_flow_data" || !strings.Contains(tools[0].Description, "actor declared flow data") {
		t.Fatalf("flow data tool = %#v, want actor-scoped read_flow_data definition", tools[0])
	}

	undeclaredGateway := NewGateway(actorScopedToolExecutorStub{
		defs: []llm.ToolDefinition{
			{Name: "read_flow_data", Description: "runtime-wide fallback must not leak"},
		},
		actorDefs: nil,
	}, "", GatewayHooks{
		ResolveActorConfig: func(agentID string) (models.AgentConfig, bool) {
			return models.AgentConfig{ExecutionMode: "live", ID: agentID, Role: "analysis"}, true
		},
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	if tools, err := undeclaredGateway.mcpToolsForRequest(req); err == nil {
		t.Fatalf("undeclared actor resolved tools %#v, want fail-closed planned-definition mismatch", tools)
	}
}

func TestGatewayMCPToolsForRequest_DoesNotFallbackToRuntimeWideToolsForEmptyActorCatalog(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(actorScopedToolExecutorStub{
		defs: []llm.ToolDefinition{
			{Name: "get_entity", Description: "runtime-wide legacy entity tool"},
		},
		actorDefs: nil,
	}, "", GatewayHooks{
		ResolveActorConfig: func(agentID string) (models.AgentConfig, bool) {
			return models.AgentConfig{ExecutionMode: "live", ID: agentID, Role: "validation_orchestrator"}, true
		},
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-role-scoped-empty", TurnContext{
		Actor:             models.AgentConfig{ID: "validation-orchestrator", Role: "validation_orchestrator"},
		CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "validation-orchestrator", Role: "validation_orchestrator"}),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	req := withContextToken(httptest.NewRequest("POST", "/mcp", nil), "ctx-role-scoped-empty")
	tools := mustMCPToolsForRequest(t, g, req)
	if len(tools) != 0 {
		t.Fatalf("empty actor catalog fell back to runtime-wide tools: %#v", tools)
	}
}

func TestGatewayMCPToolsForRoleScopedActor_RetiresLegacyEntitySurface(t *testing.T) {
	registry := newTestTurnContextRegistry()
	executeCount := 0
	legacyDefs := []llm.ToolDefinition{
		{Name: "create_entity", Description: "legacy create"},
		{Name: "get_entity", Description: "legacy get"},
		{Name: "get_subject_status", Description: "legacy subject"},
		{Name: "query_entities", Description: "legacy query"},
		{Name: "query_metrics", Description: "legacy metrics"},
		{Name: "save_entity_field", Description: "legacy save"},
		{Name: "search_entities", Description: "legacy search"},
	}
	generatedDefs := []llm.ToolDefinition{
		{Name: "read_validation_case", Description: "role scoped read"},
		{Name: "save_validation_case_business_brief", Description: "role scoped save"},
	}
	g := NewGateway(actorScopedToolExecutorStub{
		defs:      legacyDefs,
		actorDefs: generatedDefs,
		callCount: &executeCount,
	}, testGatewayToken, GatewayHooks{
		ResolveActorConfig: func(agentID string) (models.AgentConfig, bool) {
			return models.AgentConfig{ExecutionMode: "live", ID: agentID, Role: "validation_orchestrator"}, true
		},
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	actor := models.AgentConfig{ExecutionMode: "live", ID: "validation-orchestrator", Role: "validation_orchestrator"}
	putTestTurnContext(t, registry, "ctx-role-scoped-generated", TurnContext{
		Actor:             actor,
		CapabilitySurface: testCapabilitySurfaceForDefinitions(t, actor, generatedDefs...),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", nil), "ctx-role-scoped-generated")
	tools := mustMCPToolsForRequest(t, g, req)
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		names = append(names, tool.Name)
	}
	slices.Sort(names)
	if !slices.Equal(names, []string{"read_validation_case", "save_validation_case_business_brief"}) {
		t.Fatalf("role-scoped tool catalog = %#v, want generated-only tools", names)
	}

	legacyAllowed := map[string]struct{}{}
	for _, def := range legacyDefs {
		legacyAllowed[def.Name] = struct{}{}
	}
	putTestTurnContext(t, registry, "ctx-role-scoped-legacy-allowed", TurnContext{
		Actor:             actor,
		CapabilitySurface: testCapabilitySurface(t, actor, slices.Collect(maps.Keys(legacyAllowed))...),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})
	allowedReq := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", nil), "ctx-role-scoped-legacy-allowed")
	if tools, err := g.mcpToolsForRequest(allowedReq); err == nil {
		t.Fatalf("legacy capability surface resolved tools %#v, want fail-closed planned-definition mismatch", tools)
	}

	for _, def := range legacyDefs {
		body, err := json.Marshal(map[string]any{
			"id":     "req-" + def.Name,
			"method": "tools/call",
			"params": map[string]any{
				"name":      def.Name,
				"arguments": map[string]any{},
			},
		})
		if err != nil {
			t.Fatalf("marshal request for %s: %v", def.Name, err)
		}
		callReq := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(string(body))), "ctx-role-scoped-generated")
		authorizeGatewayRequest(callReq)
		rec := httptest.NewRecorder()
		g.Handler().ServeHTTP(rec, callReq)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s status = %d, want 200", def.Name, rec.Code)
		}
		resp := mustRPCResponse(t, rec)
		result, ok := resp.Result.(map[string]any)
		if !ok {
			t.Fatalf("%s result type = %T, want map", def.Name, resp.Result)
		}
		if isError, _ := result["isError"].(bool); !isError {
			t.Fatalf("%s isError = %#v, want true", def.Name, result["isError"])
		}
		if !strings.Contains(rec.Body.String(), "Authorization was denied") {
			t.Fatalf("%s response = %s, want tool-not-allowed", def.Name, rec.Body.String())
		}
	}
	if executeCount != 0 {
		t.Fatalf("MCP denied legacy tool calls reached executor %d times, want 0", executeCount)
	}

	for _, def := range legacyDefs {
		body, err := json.Marshal(ToolGatewayRequest{Input: map[string]any{}})
		if err != nil {
			t.Fatalf("marshal direct tool request for %s: %v", def.Name, err)
		}
		toolReq := withContextToken(httptest.NewRequest(http.MethodPost, "/tools/"+def.Name, strings.NewReader(string(body))), "ctx-role-scoped-generated")
		authorizeGatewayRequest(toolReq)
		rec := httptest.NewRecorder()
		g.Handler().ServeHTTP(rec, toolReq)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("direct %s status = %d, want 400", def.Name, rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "Authorization was denied") {
			t.Fatalf("direct %s response = %s, want tool-not-allowed", def.Name, rec.Body.String())
		}
	}
	if executeCount != 0 {
		t.Fatalf("direct denied legacy tool calls reached executor %d times, want 0", executeCount)
	}
}

func TestGatewayMCPToolsForRequest_FiltersRoleScopedToolsByTurnEntityEligibility(t *testing.T) {
	registry := newTestTurnContextRegistry()
	exec := &contextAwareRoleScopedExecutorStub{}
	g := NewGateway(exec, testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
		WithActor:          models.WithActor,
		WithInboundEvent:   runtimebus.WithInboundEvent,
	})
	actor := models.AgentConfig{ExecutionMode: "live", ID: "market-research-agent", Role: "market_research"}
	putTestTurnContext(t, registry, "ctx-invalid-current-entity", TurnContext{
		Actor:             actor,
		CapabilitySurface: testCapabilitySurface(t, actor, "emit_market_research_scan_complete"),
		Inbound: eventtest.RootIngress(
			"evt-root",
			events.EventType("discovery/market_research.corpus_file_assigned"),
			"",
			"",
			nil,
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "root-run-id"),
			time.Time{},
		),

		HasInbound: true,
		CreatedAt:  time.Now().UTC(),
		ExpiresAt:  time.Now().UTC().Add(time.Hour),
	})
	putTestTurnContext(t, registry, "ctx-valid-current-entity", TurnContext{
		Actor:             actor,
		CapabilitySurface: testCapabilitySurface(t, actor, "read_scan_campaign", "save_scan_campaign_mode", "emit_market_research_scan_complete"),
		Inbound: eventtest.RootIngress(
			"evt-scan",
			events.EventType("discovery/market_research.corpus_file_assigned"),
			"",
			"",
			nil,
			0,
			"",
			"",
			events.EnvelopeForEntityID(events.EventEnvelope{}, "valid-scan-campaign-id"),
			time.Time{},
		),

		HasInbound: true,
		CreatedAt:  time.Now().UTC(),
		ExpiresAt:  time.Now().UTC().Add(time.Hour),
	})

	invalidReq := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", nil), "ctx-invalid-current-entity")
	invalidTools := mustMCPToolsForRequest(t, g, invalidReq)
	invalidNames := make([]string, 0, len(invalidTools))
	for _, tool := range invalidTools {
		invalidNames = append(invalidNames, tool.Name)
	}
	if slices.Contains(invalidNames, "read_scan_campaign") || slices.Contains(invalidNames, "save_scan_campaign_mode") {
		t.Fatalf("invalid current entity exposed role-scoped tools: %#v", invalidNames)
	}
	if !slices.Contains(invalidNames, "emit_market_research_scan_complete") {
		t.Fatalf("invalid current entity filtered non-entity tool: %#v", invalidNames)
	}

	validReq := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", nil), "ctx-valid-current-entity")
	validTools := mustMCPToolsForRequest(t, g, validReq)
	validNames := make([]string, 0, len(validTools))
	for _, tool := range validTools {
		validNames = append(validNames, tool.Name)
	}
	if !slices.Contains(validNames, "read_scan_campaign") || !slices.Contains(validNames, "save_scan_campaign_mode") {
		t.Fatalf("valid current entity did not expose role-scoped tools: %#v", validNames)
	}

	body, err := json.Marshal(map[string]any{
		"id":     "req-read",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "read_scan_campaign",
			"arguments": map[string]any{},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	callReq := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(string(body))), "ctx-invalid-current-entity")
	authorizeGatewayRequest(callReq)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, callReq)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if exec.callCount != 0 {
		t.Fatalf("ineligible role-scoped MCP call reached executor %d times", exec.callCount)
	}
	if !strings.Contains(rec.Body.String(), "Authorization was denied") {
		t.Fatalf("response = %s, want tool-not-allowed", rec.Body.String())
	}
}

func TestGatewayMCPToolsForRequest_IgnoresCallerAllowlist(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(actorScopedToolExecutorStub{
		actorDefs: []llm.ToolDefinition{
			{Name: "query_entities", Description: "actor scoped"},
			{Name: "read_file", Description: "reader"},
		},
	}, "", GatewayHooks{
		ResolveActorConfig: func(agentID string) (models.AgentConfig, bool) {
			return models.AgentConfig{ExecutionMode: "live", ID: agentID, Role: "analysis"}, true
		},
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-ignore-allowlist", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurfaceForDefinitions(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, llm.ToolDefinition{Name: "query_entities", Description: "actor scoped"}, llm.ToolDefinition{Name: "read_file", Description: "reader"}),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	req := withContextToken(httptest.NewRequest("POST", "/mcp?allowed_tools=Read", nil), "ctx-ignore-allowlist")
	tools := mustMCPToolsForRequest(t, g, req)
	if len(tools) != 2 {
		t.Fatalf("tool count = %d, want 2 (%#v)", len(tools), tools)
	}
	names := []string{tools[0].Name, tools[1].Name}
	slices.Sort(names)
	if !slices.Equal(names, []string{"query_entities", "read_file"}) {
		t.Fatalf("tool names = %#v", names)
	}
}

func TestGatewayMCPToolsForRequest_DoesNotTrustUnknownCallerAllowlist(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(actorScopedToolExecutorStub{
		actorDefs: []llm.ToolDefinition{
			{Name: "query_entities", Description: "actor scoped"},
		},
	}, "", GatewayHooks{
		ResolveActorConfig: func(agentID string) (models.AgentConfig, bool) {
			return models.AgentConfig{ExecutionMode: "live", ID: agentID, Role: "analysis"}, true
		},
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-unknown-allowlist", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurfaceForDefinitions(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, llm.ToolDefinition{Name: "query_entities", Description: "actor scoped"}),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	req := withContextToken(httptest.NewRequest("POST", "/mcp?allowed_tools=does_not_exist", nil), "ctx-unknown-allowlist")
	tools := mustMCPToolsForRequest(t, g, req)
	if len(tools) != 1 {
		t.Fatalf("tool count = %d, want 1 (%#v)", len(tools), tools)
	}
	if tools[0].Name != "query_entities" {
		t.Fatalf("tool name = %q, want query_entities", tools[0].Name)
	}
}

func TestGatewayHandleMCP_ToolsListRejectsMissingOrInvalidContextToken(t *testing.T) {
	for _, rawQuery := range []string{"", "?ctx_token=missing&agent_id=analysis-agent"} {
		t.Run(rawQuery, func(t *testing.T) {
			g := NewGateway(actorScopedToolExecutorStub{
				actorDefs: []llm.ToolDefinition{
					{Name: "query_entities", Description: "actor scoped"},
				},
			}, testGatewayToken, GatewayHooks{})

			body, err := json.Marshal(map[string]any{
				"id":     "req-1",
				"method": "tools/list",
				"params": map[string]any{},
			})
			if err != nil {
				t.Fatalf("marshal request: %v", err)
			}
			req := httptest.NewRequest(http.MethodPost, "/mcp"+rawQuery, strings.NewReader(string(body)))
			authorizeGatewayRequest(req)
			rec := httptest.NewRecorder()
			g.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rec.Code)
			}
			var resp RPCResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("unmarshal response: %v", err)
			}
			if resp.Error == nil {
				t.Fatalf("response error = nil, want context error in %s", rec.Body.String())
			}
			if !strings.Contains(resp.Error.Message, "MCP context token is required") {
				t.Fatalf("error message = %q", resp.Error.Message)
			}
		})
	}
}

func TestGatewayHandleMCP_ToolsListUsesResolvedTurnContext(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(actorScopedToolExecutorStub{
		actorDefs: []llm.ToolDefinition{
			{Name: "query_entities", Description: "actor scoped"},
		},
	}, testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-list", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurfaceForDefinitions(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, llm.ToolDefinition{Name: "query_entities", Description: "actor scoped"}),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/list",
		"params": map[string]any{},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(string(body))), "ctx-list")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp RPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("response error = %#v", resp.Error)
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", resp.Result)
	}
	rawTools, ok := result["tools"].([]any)
	if !ok || len(rawTools) != 1 {
		t.Fatalf("tools = %#v, want one visible tool", result["tools"])
	}
	tool, ok := rawTools[0].(map[string]any)
	if !ok || strings.TrimSpace(asString(tool["name"])) != "query_entities" {
		t.Fatalf("tool payload = %#v", rawTools[0])
	}
}

func TestMarkEmitUsed(t *testing.T) {
	registry := newTestTurnContextRegistry()
	putTestTurnContext(t, registry, "ctx-emit", TurnContext{
		Actor:     models.AgentConfig{ExecutionMode: "live", ID: "campaign-coordinator", Role: "campaign_coordinator"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	if already := registry.MarkEmitUsed("ctx-emit"); already {
		t.Fatal("first emit use should not report already used")
	}
	if already := registry.MarkEmitUsed("ctx-emit"); !already {
		t.Fatal("second emit use should report already used")
	}
}

func TestMarkEmitKeyUsed_AllowsDistinctKeysPerTurn(t *testing.T) {
	registry := newTestTurnContextRegistry()
	putTestTurnContext(t, registry, "ctx-emit-keys", TurnContext{
		Actor:     models.AgentConfig{ExecutionMode: "live", ID: "analysis-agent", Role: "analysis"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	if already := registry.MarkEmitKeyUsed("ctx-emit-keys", "emit_a\n{\"dimension\":\"one\"}"); already {
		t.Fatal("first unique emit key should not report already used")
	}
	if already := registry.MarkEmitKeyUsed("ctx-emit-keys", "emit_a\n{\"dimension\":\"two\"}"); already {
		t.Fatal("second distinct emit key should be allowed")
	}
	if already := registry.MarkEmitKeyUsed("ctx-emit-keys", "emit_a\n{\"dimension\":\"one\"}"); !already {
		t.Fatal("duplicate emit key should report already used")
	}
}

func TestGatewayHandleMCP_AllowsDistinctEmitPayloadsPerTurn(t *testing.T) {
	registry := newTestTurnContextRegistry()
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
		MarkEmitKeyUsed:    registry.MarkEmitKeyUsed,
	})

	putTestTurnContext(t, registry, "ctx-emit-gateway", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, "emit_score_dimension_complete"),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	handler := g.Handler()
	makeReq := func(arguments any) *httptest.ResponseRecorder {
		body, err := json.Marshal(map[string]any{
			"id":     "req-1",
			"method": "tools/call",
			"params": map[string]any{
				"name":      "emit_score_dimension_complete",
				"arguments": arguments,
			},
		})
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(string(body))), "ctx-emit-gateway")
		authorizeGatewayRequest(req)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	first := makeReq(map[string]any{"dimension": "build_complexity", "score": 32})
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d", first.Code)
	}
	second := makeReq(map[string]any{"dimension": "icp_crispness", "score": 70})
	if second.Code != http.StatusOK {
		t.Fatalf("second status = %d", second.Code)
	}
	duplicate := makeReq(map[string]any{"dimension": "build_complexity", "score": 32})
	if duplicate.Code != http.StatusOK {
		t.Fatalf("duplicate status = %d", duplicate.Code)
	}
	if callCount != 2 {
		t.Fatalf("executor call count = %d, want 2", callCount)
	}
	if !strings.Contains(duplicate.Body.String(), "duplicate emit already executed this turn") {
		t.Fatalf("duplicate body = %s", duplicate.Body.String())
	}
}

func TestGatewayHandleMCP_AllowsPrefixedToolNameFromRuntimeOwnedTurnContext(t *testing.T) {
	registry := newTestTurnContextRegistry()
	exec := &capabilityAwareExecutorStub{}
	g := NewGateway(exec, testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
		MarkEmitKeyUsed:    registry.MarkEmitKeyUsed,
	})

	putTestTurnContext(t, registry, "ctx-prefixed-emit", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, "emit_score_dimension_complete"),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "mcp__runtime-tools__emit_score_dimension_complete",
			"arguments": map[string]any{"dimension": "build_complexity", "score": 32},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp?allowed_tools=query_entities", strings.NewReader(string(body))), "ctx-prefixed-emit")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if exec.callCount != 1 {
		t.Fatalf("executor call count = %d, want 1", exec.callCount)
	}
	if strings.Contains(rec.Body.String(), "Authorization was denied") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestGatewayHandleMCP_DoesNotLetCallerAllowlistGrantToolAccess(t *testing.T) {
	registry := newTestTurnContextRegistry()
	exec := &capabilityAwareExecutorStub{}
	var loggedAction string
	var denialLayer string
	g := NewGateway(exec, testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
		MarkEmitKeyUsed:    registry.MarkEmitKeyUsed,
		Log: func(_ context.Context, level, action, agentID, entityID string, detail map[string]any, failure *failures.Envelope) {
			loggedAction = action
			denialLayer = strings.TrimSpace(asString(detail["denial_layer"]))
		},
	})

	putTestTurnContext(t, registry, "ctx-denied-allowlist", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, "query_entities"),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "mcp__runtime-tools__emit_score_dimension_complete",
			"arguments": map[string]any{"dimension": "build_complexity", "score": 32},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp?allowed_tools=emit_score_dimension_complete", strings.NewReader(string(body))), "ctx-denied-allowlist")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if exec.callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", exec.callCount)
	}
	if !strings.Contains(rec.Body.String(), "Authorization was denied") {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if loggedAction != "mcp.tools.call.denied" {
		t.Fatalf("logged action = %q", loggedAction)
	}
	if denialLayer != "gateway" {
		t.Fatalf("denial_layer = %q, want gateway", denialLayer)
	}
}

func TestGatewayHandleMCP_ToolsCallIncludesStructuredRuntimeErrorPayload(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(testToolExecutor(func(_ context.Context, _ string, _ any) (any, error) {
		return nil, failures.NewDetail("invalid_tool_input", "tool-executor", "exec_query_entities.filter", map[string]any{"field": "query"})
	}), testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-runtime-error", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, "query_entities"),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "query_entities",
			"arguments": map[string]any{},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(string(body))), "ctx-runtime-error")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	resp := mustRPCResponse(t, rec)
	if resp.Error != nil {
		t.Fatalf("rpc error = %#v", resp.Error)
	}
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", resp.Result)
	}
	if isError, _ := result["isError"].(bool); !isError {
		t.Fatalf("isError = %#v, want true", result["isError"])
	}
	runtimeErr, err := DecodeRuntimeErrorPayload(result["runtimeError"])
	if err != nil {
		t.Fatalf("DecodeRuntimeErrorPayload: %v", err)
	}
	if runtimeErr.Failure == nil || runtimeErr.Failure.Class != failures.ClassSchemaInvalid || runtimeErr.Failure.Detail.Code != "invalid_tool_input" {
		t.Fatalf("runtimeError.failure = %#v, want schema_invalid/invalid_tool_input", runtimeErr.Failure)
	}
}

func TestGatewayExecutionFailureEnvelopeParityAcrossToolsAndMCP(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{"authentication", failures.New(failures.ClassAuthenticationNeeded, "provider_unauthorized", "tool-executor", "execute", map[string]any{"auth_kind": "provider"})},
		{"authorization", failures.New(failures.ClassAuthorizationDenied, "provider_forbidden", "tool-executor", "execute", map[string]any{"action": "tool_execute"})},
		{"connector", failures.New(failures.ClassConnectorFailure, "provider_rate_limited", "tool-executor", "execute", map[string]any{"status": 429})},
		{"data limit", failures.New(failures.ClassDataLimitExceeded, "typed_read_result_too_large", "tool-executor", "execute", map[string]any{"limit_kind": "bytes", "limit": 1024, "actual": 2048})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := newTestTurnContextRegistry()
			g := NewGateway(testToolExecutor(func(context.Context, string, any) (any, error) {
				return nil, tt.err
			}), testGatewayToken, GatewayHooks{ResolveTurnContext: registry.ResolveTurnContext})
			putTestTurnContext(t, registry, "ctx-parity", TurnContext{
				Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
				CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, "query_entities"),
				CreatedAt:         time.Now().UTC(),
				ExpiresAt:         time.Now().UTC().Add(time.Hour),
			})

			toolReq := withContextToken(httptest.NewRequest(http.MethodPost, "/tools/query_entities", strings.NewReader(`{"input":{}}`)), "ctx-parity")
			authorizeGatewayRequest(toolReq)
			toolRec := httptest.NewRecorder()
			g.Handler().ServeHTTP(toolRec, toolReq)
			var toolResponse ToolGatewayResponse
			if err := json.Unmarshal(toolRec.Body.Bytes(), &toolResponse); err != nil {
				t.Fatalf("decode /tools response: %v", err)
			}
			if toolResponse.OK || toolResponse.Error != nil || toolResponse.RuntimeError == nil || toolResponse.RuntimeError.Failure == nil {
				t.Fatalf("/tools response = %#v, want canonical runtimeError only", toolResponse)
			}

			mcpBody, err := json.Marshal(map[string]any{
				"jsonrpc": "2.0", "id": "req-parity", "method": "tools/call",
				"params": map[string]any{"name": "query_entities", "arguments": map[string]any{}},
			})
			if err != nil {
				t.Fatal(err)
			}
			mcpReq := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(mcpBody)), "ctx-parity")
			authorizeGatewayRequest(mcpReq)
			mcpRec := httptest.NewRecorder()
			g.Handler().ServeHTTP(mcpRec, mcpReq)
			mcpResponse := mustRPCResponse(t, mcpRec)
			mcpResult, ok := mcpResponse.Result.(map[string]any)
			if !ok {
				t.Fatalf("/mcp result = %#v", mcpResponse.Result)
			}
			mcpRuntimeError, err := DecodeRuntimeErrorPayload(mcpResult["runtimeError"])
			if err != nil || mcpRuntimeError.Failure == nil {
				t.Fatalf("decode /mcp runtimeError = %#v, %v", mcpRuntimeError, err)
			}

			direct, ok := failures.EnvelopeFromError(tt.err)
			if !ok {
				t.Fatalf("direct error has no envelope: %v", tt.err)
			}
			want, _ := failures.MarshalEnvelope(direct)
			gotTool, _ := failures.MarshalEnvelope(*toolResponse.RuntimeError.Failure)
			gotMCP, _ := failures.MarshalEnvelope(*mcpRuntimeError.Failure)
			if !bytes.Equal(gotTool, want) || !bytes.Equal(gotMCP, want) {
				t.Fatalf("failure parity mismatch\ndirect=%s\n/tools=%s\n/mcp=%s", want, gotTool, gotMCP)
			}
		})
	}
}

func TestGatewayHandleMCP_ToolsCallRelaysOversizedReadFileResultsForHelperPath(t *testing.T) {
	registry := newTestTurnContextRegistry()
	exec := &relayAwareToolExecutorStub{
		execFn: func(_ context.Context, _ string, _ any) (any, error) {
			return map[string]any{
				"content":    strings.Repeat("a", maxToolResultBytes+512),
				"size_bytes": maxToolResultBytes + 512,
			}, nil
		},
		relayRef: ToolResultRelayRef{
			Chunks:     []string{"/workspace/.swarm/tool-results/agent/read-file-chunk-001.txt", "/workspace/.swarm/tool-results/agent/read-file-chunk-002.txt"},
			ReadTool:   "read_file",
			Format:     "text",
			Visibility: "workspace_mount",
		},
	}
	g := NewGateway(exec, testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-relay-read-file", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, "read_file"),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "read_file",
			"arguments": map[string]any{"path": "/data/big.json"},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(string(body))), "ctx-relay-read-file")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	resp := mustRPCResponse(t, rec)
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", resp.Result)
	}
	content, ok := result["content"].([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("content = %#v, want single text entry", result["content"])
	}
	entry, ok := content[0].(map[string]any)
	if !ok {
		t.Fatalf("content[0] = %#v, want map", content[0])
	}
	text, _ := entry["text"].(string)
	var projected map[string]any
	if err := json.Unmarshal([]byte(text), &projected); err != nil {
		t.Fatalf("unmarshal projected text: %v", err)
	}
	if truncated, _ := projected["truncated"].(bool); !truncated {
		t.Fatalf("truncated = %#v, want true", projected["truncated"])
	}
	followUp, _ := projected["follow_up"].(map[string]any)
	if followUp == nil || followUp["tool"] != "read_file" {
		t.Fatalf("follow_up = %#v, want read_file chunk metadata", followUp)
	}
	chunks, _ := followUp["chunks"].([]any)
	if len(chunks) != len(exec.relayRef.Chunks) {
		t.Fatalf("chunks = %#v, want %d chunk paths", followUp["chunks"], len(exec.relayRef.Chunks))
	}
	if exec.relayTool != "read_file" {
		t.Fatalf("relay tool = %q, want read_file", exec.relayTool)
	}
	if len(exec.relayRaw) == 0 {
		t.Fatalf("expected raw relay payload to be persisted")
	}
}

func TestGatewayHandleMCP_ToolsCallSuppressesRuntimeReadFileFollowUpWithoutReadFileSurface(t *testing.T) {
	registry := newTestTurnContextRegistry()
	exec := &relayAwareToolExecutorStub{
		execFn: func(_ context.Context, _ string, _ any) (any, error) {
			return map[string]any{
				"content":    strings.Repeat("q", maxToolResultBytes+512),
				"size_bytes": maxToolResultBytes + 512,
			}, nil
		},
		relayRef: ToolResultRelayRef{
			Path:       "/workspace/.swarm/tool-results/agent/query-entities-1.json",
			ReadTool:   "read_file",
			Format:     "json",
			Visibility: "workspace_mount",
		},
	}
	g := NewGateway(exec, testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-query-only", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, "query_entities"),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "query_entities",
			"arguments": map[string]any{"entity_type": "company"},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(string(body))), "ctx-query-only")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	resp := mustRPCResponse(t, rec)
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", resp.Result)
	}
	content, ok := result["content"].([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("content = %#v, want single text entry", result["content"])
	}
	entry, ok := content[0].(map[string]any)
	if !ok {
		t.Fatalf("content[0] = %#v, want map", content[0])
	}
	text, _ := entry["text"].(string)
	var projected map[string]any
	if err := json.Unmarshal([]byte(text), &projected); err != nil {
		t.Fatalf("unmarshal projected text: %v", err)
	}
	if truncated, _ := projected["truncated"].(bool); !truncated {
		t.Fatalf("truncated = %#v, want true", projected["truncated"])
	}
	if _, ok := projected["follow_up"]; ok {
		t.Fatalf("follow_up = %#v, want absent on no-read_file turn", projected["follow_up"])
	}
	if exec.relayTool != "" {
		t.Fatalf("relay tool = %q, want no relay write", exec.relayTool)
	}
	if len(exec.relayRaw) != 0 {
		t.Fatalf("relay raw = %q, want no relay payload write", string(exec.relayRaw))
	}
}

func TestProjectToolCallSuccessText_RoleScopedTypedReadPreservesLargeValidationCase(t *testing.T) {
	payload := largeValidationCasePayloadForTypedReadTest()
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	if len(rawPayload) < 40*1024 || len(rawPayload) > toolresultpolicy.MaxCompleteTypedReadResultBytes {
		t.Fatalf("payload size = %d, want >=40KB and <=%d", len(rawPayload), toolresultpolicy.MaxCompleteTypedReadResultBytes)
	}
	ctx := roleScopedTypedReadContext("read_validation_case")

	text, err := projectToolCallSuccessText(ctx, testToolExecutor(func(context.Context, string, any) (any, error) {
		return payload, nil
	}), "read_validation_case", map[string]any{}, payload)
	if err != nil {
		t.Fatalf("projectToolCallSuccessText: %v", err)
	}
	if strings.Contains(text, `"truncated"`) || strings.Contains(text, `"preview"`) || strings.Contains(text, `"follow_up"`) {
		t.Fatalf("typed read projection used lossy metadata: %s", text)
	}
	var projected map[string]any
	if err := json.Unmarshal([]byte(text), &projected); err != nil {
		t.Fatalf("unmarshal projected text: %v", err)
	}
	fields, _ := projected["fields"].(map[string]any)
	mvp, _ := fields["mvp_spec"].(map[string]any)
	if got := asStringForGatewayTest(mvp["problem_statement"]); got != asStringForGatewayTest(payload["mvp_problem_statement"]) {
		t.Fatalf("mvp_spec.problem_statement length = %d, want %d", len(got), len(asStringForGatewayTest(payload["mvp_problem_statement"])))
	}
}

func TestProjectToolCallSuccessText_RoleScopedTypedReadFieldPreservesCompleteField(t *testing.T) {
	field := map[string]any{
		"problem_statement":  strings.Repeat("problem statement ", 900),
		"technical_approach": strings.Repeat("technical approach ", 900),
	}
	ctx := roleScopedTypedReadContext("read_validation_case_mvp_spec")

	text, err := projectToolCallSuccessText(ctx, testToolExecutor(func(context.Context, string, any) (any, error) {
		return field, nil
	}), "read_validation_case_mvp_spec", map[string]any{}, field)
	if err != nil {
		t.Fatalf("projectToolCallSuccessText: %v", err)
	}
	if strings.Contains(text, `"truncated"`) || strings.Contains(text, `"preview"`) || strings.Contains(text, `"follow_up"`) {
		t.Fatalf("typed field read projection used lossy metadata: %s", text)
	}
	var projected map[string]any
	if err := json.Unmarshal([]byte(text), &projected); err != nil {
		t.Fatalf("unmarshal projected text: %v", err)
	}
	if got := asStringForGatewayTest(projected["technical_approach"]); got != field["technical_approach"].(string) {
		t.Fatalf("technical_approach length = %d, want %d", len(got), len(field["technical_approach"].(string)))
	}
}

func TestProjectToolCallSuccessText_RoleScopedTypedReadFailsClosedWhenTooLarge(t *testing.T) {
	payload := map[string]any{"blob": strings.Repeat("x", toolresultpolicy.MaxCompleteTypedReadResultBytes+1024)}
	ctx := roleScopedTypedReadContext("read_validation_case")

	text, err := projectToolCallSuccessText(ctx, testToolExecutor(func(context.Context, string, any) (any, error) {
		return payload, nil
	}), "read_validation_case", map[string]any{}, payload)
	if err == nil {
		t.Fatalf("projectToolCallSuccessText returned nil error and text %s", text)
	}
	runtimeErr, ok := failures.As(err)
	if !ok || runtimeErr.Failure.Class != failures.ClassDataLimitExceeded || runtimeErr.Failure.Detail.Code != toolresultpolicy.TypedReadResultTooLargeCode {
		t.Fatalf("error = %#v, want runtime code %s", err, toolresultpolicy.TypedReadResultTooLargeCode)
	}
}

func TestProjectToolCallSuccessText_ReadPrefixedNonRoleScopedToolKeepsLegacyProjection(t *testing.T) {
	payload := map[string]any{"blob": strings.Repeat("x", maxToolResultBytes+1024)}

	text, err := projectToolCallSuccessText(unmanagedMCPTestContext(), testToolExecutor(func(context.Context, string, any) (any, error) {
		return payload, nil
	}), "read_custom_report", map[string]any{}, payload)
	if err != nil {
		t.Fatalf("projectToolCallSuccessText: %v", err)
	}
	var projected map[string]any
	if err := json.Unmarshal([]byte(text), &projected); err != nil {
		t.Fatalf("unmarshal projected text: %v", err)
	}
	if truncated, _ := projected["truncated"].(bool); !truncated {
		t.Fatalf("truncated = %#v, want legacy projection for non-role-scoped read-prefixed tool", projected["truncated"])
	}
}

func TestGatewayHandleMCP_ToolsCallPreservesLargeRelayPathReadInline(t *testing.T) {
	registry := newTestTurnContextRegistry()
	exec := &relayAwareToolExecutorStub{
		execFn: func(_ context.Context, _ string, _ any) (any, error) {
			return map[string]any{
				"content":    strings.Repeat("b", maxToolResultBytes+1024),
				"size_bytes": maxToolResultBytes + 1024,
			}, nil
		},
		relayRef: ToolResultRelayRef{
			Path:       "/workspace/.swarm/tool-results/agent/read-file-1.json",
			ReadTool:   "read_file",
			Format:     "json",
			Visibility: "workspace_mount",
		},
	}
	g := NewGateway(exec, testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-read-relay-file", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, "read_file"),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "read_file",
			"arguments": map[string]any{"path": "/workspace/.swarm/tool-results/agent/read-file-1.json"},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(string(body))), "ctx-read-relay-file")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	resp := mustRPCResponse(t, rec)
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", resp.Result)
	}
	content, ok := result["content"].([]any)
	if !ok || len(content) != 1 {
		t.Fatalf("content = %#v, want single text entry", result["content"])
	}
	entry, ok := content[0].(map[string]any)
	if !ok {
		t.Fatalf("content[0] = %#v, want map", content[0])
	}
	text, _ := entry["text"].(string)
	if strings.Contains(text, "\"follow_up\"") {
		t.Fatalf("did not expect relay follow_up when reading runtime relay path, got %s", text)
	}
	if exec.relayTool != "" {
		t.Fatalf("did not expect relay writer call, got %q", exec.relayTool)
	}
}

func TestGatewayHandleMCP_ToolsCallIncludesExplicitStartupProbeSuccessOutcome(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(testToolExecutor(func(_ context.Context, _ string, _ any) (any, error) {
		return map[string]any{"ok": true}, nil
	}), testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-startup-probe-success", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, "query_entities"),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "query_entities",
			"arguments": map[string]any{},
			"swarmProbe": map[string]any{
				"contract": StartupProbeContractManagedAgentCallable,
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(string(body))), "ctx-startup-probe-success")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	resp := mustRPCResponse(t, rec)
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", resp.Result)
	}
	probeResult, err := DecodeStartupProbeResult(result["swarmStartupProbe"])
	if err != nil {
		t.Fatalf("DecodeStartupProbeResult: %v", err)
	}
	if probeResult.Outcome != StartupProbeOutcomeSuccess {
		t.Fatalf("startup probe outcome = %q, want success", probeResult.Outcome)
	}
}

func TestGatewayHandleMCP_ToolsCallIncludesExplicitStartupProbeValidationOnlyOutcome(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(testToolExecutor(func(_ context.Context, _ string, _ any) (any, error) {
		return nil, failures.NewDetail("invalid_tool_input", "tool-executor", "exec_query_entities.filter", map[string]any{"field": "query"})
	}), testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-startup-probe-validation", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, "query_entities"),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "query_entities",
			"arguments": map[string]any{},
			"swarmProbe": map[string]any{
				"contract": StartupProbeContractManagedAgentCallable,
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(string(body))), "ctx-startup-probe-validation")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	resp := mustRPCResponse(t, rec)
	result, ok := resp.Result.(map[string]any)
	if !ok {
		t.Fatalf("result type = %T, want map[string]any", resp.Result)
	}
	probeResult, err := DecodeStartupProbeResult(result["swarmStartupProbe"])
	if err != nil {
		t.Fatalf("DecodeStartupProbeResult: %v", err)
	}
	if probeResult.Outcome != StartupProbeOutcomeValidationOnly {
		t.Fatalf("startup probe outcome = %q, want validation_only", probeResult.Outcome)
	}
	runtimeErr, err := DecodeRuntimeErrorPayload(result["runtimeError"])
	if err != nil {
		t.Fatalf("DecodeRuntimeErrorPayload: %v", err)
	}
	if runtimeErr.Failure == nil || runtimeErr.Failure.Class != failures.ClassSchemaInvalid || runtimeErr.Failure.Detail.Code != "invalid_tool_input" {
		t.Fatalf("startup probe failure = %#v, want schema_invalid/invalid_tool_input", runtimeErr.Failure)
	}
}

func TestGatewayHandleMCP_RejectsToolWhenContextTokenMisses(t *testing.T) {
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "emit_score_dimension_complete",
			"arguments": map[string]any{"dimension": "build_complexity", "score": 72},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/mcp?agent_id=analysis-agent&agent_role=analysis", strings.NewReader(string(body)))
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", callCount)
	}
	if !strings.Contains(rec.Body.String(), "MCP context token is required") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestGatewayMCPExecutionContext_RejectsPrefixedMutatingToolOnContextMiss(t *testing.T) {
	g := NewGateway(nil, "", GatewayHooks{})
	req := httptest.NewRequest(http.MethodPost, "/mcp?agent_id=analysis-agent&agent_role=analysis", nil)
	if _, err := g.mcpExecutionContext(req, "mcp__runtime-tools__emit_score_dimension_complete"); err == nil {
		t.Fatal("expected context miss error for prefixed mutating tool")
	}
}

func TestGatewayExecutionContext_UsesInboundTraceNotRequestTraceOnResolvedTurn(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(nil, "", GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
		WithActor: func(ctx context.Context, actor models.AgentConfig) context.Context {
			return models.WithActor(ctx, actor)
		},
		WithCurrentRuntimeEpoch: func(ctx context.Context) context.Context {
			return runtimebus.WithCurrentRuntimeEpoch(ctx)
		},
		WithInboundEvent: func(ctx context.Context, evt events.Event) context.Context {
			return runtimebus.WithInboundEvent(ctx, evt)
		},
	})
	putTestTurnContext(t, registry, "ctx-trace", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, "get_entity"),
		Inbound:           eventtest.RootIngress("evt-1", events.EventType(""), "", "", nil, 0, "run-1", "", events.EventEnvelope{}, time.Time{}),
		HasInbound:        true,
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", nil), "ctx-trace")
	ctx, err := g.mcpExecutionContext(req, "get_entity")
	if err != nil {
		t.Fatalf("mcpExecutionContext: %v", err)
	}
	_ = ctx
}

func TestGatewayExecutionContext_RestoresTypedRuntimeLineageOnResolvedTurn(t *testing.T) {
	registry := newTestTurnContextRegistry()
	g := NewGateway(nil, "", GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
		WithActor:          models.WithActor,
		WithInboundEvent:   runtimebus.WithInboundEvent,
	})
	putTestTurnContext(t, registry, "ctx-lineage", TurnContext{
		Actor:             models.AgentConfig{ID: "validation-coordinator", Role: "validation"},
		CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "validation-coordinator", Role: "validation"}, "get_entity"),
		Inbound: eventtest.RootIngress("3134bdf0-2ce0-4260-93bd-f0a45371b7d7",
			events.EventType("validation/validation.package_ready"), "", "", nil, 0, "a6f6861a-d154-4d38-a2d6-1388f5bb6daf", "", events.EventEnvelope{}, time.Time{}),

		HasInbound: true,
		RuntimeLineage: runtimecorrelation.RuntimeLineage{
			Owner:               "runtime.run_fork.selected_contract_execution.fork_local_runtime_typed_lineage",
			RunID:               "a6f6861a-d154-4d38-a2d6-1388f5bb6daf",
			SubjectEventID:      "3134bdf0-2ce0-4260-93bd-f0a45371b7d7",
			SubjectEventType:    "validation/validation.package_ready",
			ParentEventID:       "3134bdf0-2ce0-4260-93bd-f0a45371b7d7",
			RowCategory:         runtimecorrelation.RuntimeLineageRowCategoryRuntimeContainer,
			SelectedForkOwner:   "runtime.run_fork.selected_contract_execution.fork_local_runtime_container",
			Classification:      runtimecorrelation.RuntimeLineageClassificationForkLocal,
			SelectedForkContext: true,
		},
		HasRuntimeLineage: true,
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", nil), "ctx-lineage")
	ctx, err := g.mcpExecutionContext(req, "get_entity")
	if err != nil {
		t.Fatalf("mcpExecutionContext: %v", err)
	}
	lineage, ok := runtimecorrelation.RuntimeLineageFromContext(ctx)
	if !ok {
		t.Fatal("runtime lineage missing from gateway execution context")
	}
	if lineage.Owner != "runtime.run_fork.selected_contract_execution.fork_local_runtime_typed_lineage" ||
		lineage.SubjectEventID != "3134bdf0-2ce0-4260-93bd-f0a45371b7d7" ||
		lineage.ParentEventID != "3134bdf0-2ce0-4260-93bd-f0a45371b7d7" ||
		lineage.RowCategory != runtimecorrelation.RuntimeLineageRowCategoryRuntimeContainer ||
		lineage.Classification != runtimecorrelation.RuntimeLineageClassificationForkLocal ||
		!lineage.SelectedForkContext {
		t.Fatalf("runtime lineage = %#v", lineage)
	}
}

func TestGatewayMCPExecutionContext_KeepsOtherRegistryTokensValidAfterGlobalEpochBump(t *testing.T) {
	registryA := newTestTurnContextRegistry()
	registryB := newTestTurnContextRegistry()
	putTestTurnContext(t, registryB, "ctx-b", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, "query_entities"),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	gatewayB := NewGateway(nil, "", GatewayHooks{
		ResolveTurnContext:      registryB.ResolveTurnContext,
		WithActor:               models.WithActor,
		WithCurrentRuntimeEpoch: runtimebus.WithCurrentRuntimeEpoch,
	})

	registryA.Reset()
	runtimebus.EnterRuntimeResetMode()
	runtimebus.ExitRuntimeResetMode()

	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", nil), "ctx-b")
	ctx, err := gatewayB.mcpExecutionContext(req, "query_entities")
	if err != nil {
		t.Fatalf("mcpExecutionContext after unrelated reset: %v", err)
	}
	if epoch, ok := runtimebus.RuntimeEpochFromContext(ctx); !ok || epoch != runtimebus.CurrentRuntimeEpoch() {
		t.Fatalf("context epoch = %d ok=%v, want current epoch %d", epoch, ok, runtimebus.CurrentRuntimeEpoch())
	}
}

func TestGatewayAuthorize_FailsClosedWithoutConfiguredToken(t *testing.T) {
	g := NewGateway(nil, "", GatewayHooks{})
	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	err := g.AuthorizeForTest(req)
	if err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("AuthorizeForTest err = %v, want unconfigured auth error", err)
	}
}

func TestGatewayAuthorize_DeniesMissingBearer(t *testing.T) {
	g := NewGateway(nil, testGatewayToken, GatewayHooks{})
	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	err := g.AuthorizeForTest(req)
	if err == nil || !strings.Contains(err.Error(), "Authorization bearer token is required") {
		t.Fatalf("AuthorizeForTest err = %v, want missing bearer error", err)
	}
}

func TestGatewayAuthorize_DeniesInvalidBearer(t *testing.T) {
	g := NewGateway(nil, testGatewayToken, GatewayHooks{})
	req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	err := g.AuthorizeForTest(req)
	if err == nil || !strings.Contains(err.Error(), "Authorization bearer token is invalid") {
		t.Fatalf("AuthorizeForTest err = %v, want Authorization bearer token is invalid error", err)
	}
}

func TestGatewayHandleTool_DeniesWhenRuntimeShutdownAdmissionClosed(t *testing.T) {
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{
		RuntimeShutdownAdmissionClosed: func() bool { return true },
	})

	req := httptest.NewRequest(http.MethodPost, "/tools/query_entities", strings.NewReader(`{"input":{"query":"kind='vertical'"}}`))
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "runtime shutting down") {
		t.Fatalf("body = %s, want runtime shutting down", rec.Body.String())
	}
	if callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", callCount)
	}
}

func TestGatewayHandleTool_DeniesThroughRuntimeIngressOwnerRead(t *testing.T) {
	callCount := 0
	readCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{
		RuntimeIngressRequestPaused: func(context.Context) (bool, error) {
			readCount++
			return true, nil
		},
	})

	req := httptest.NewRequest(http.MethodPost, "/tools/query_entities", strings.NewReader(`{"input":{"query":"kind='vertical'"}}`))
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "runtime reset in progress") {
		t.Fatalf("body = %s, want runtime reset in progress", rec.Body.String())
	}
	if readCount != 1 {
		t.Fatalf("runtime ingress owner reads = %d, want 1", readCount)
	}
	if callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", callCount)
	}
}

func TestGatewayHandleMCP_DeniesToolCallWhenRuntimeShutdownAdmissionClosed(t *testing.T) {
	registry := newTestTurnContextRegistry()
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{
		RuntimeShutdownAdmissionClosed: func() bool { return true },
		ResolveTurnContext:             registry.ResolveTurnContext,
		WithActor:                      models.WithActor,
		WithCurrentRuntimeEpoch:        runtimebus.WithCurrentRuntimeEpoch,
	})
	putTestTurnContext(t, registry, "ctx-shutdown", TurnContext{
		Actor:     models.AgentConfig{ExecutionMode: "live", ID: "analysis-agent", Role: "analysis"},
		CreatedAt: time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(time.Hour),
	})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "query_entities",
			"arguments": map[string]any{"query": "kind = 'vertical'"},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(string(body))), "ctx-shutdown")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	resp := mustRPCResponse(t, rec)
	if resp.Error == nil || !strings.Contains(resp.Error.Message, "runtime shutting down") {
		t.Fatalf("rpc error = %#v, want runtime shutting down", resp.Error)
	}
	if callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", callCount)
	}
}

func TestGatewayHandleMCP_RejectsReadOnlyToolWhenContextTokenMisses(t *testing.T) {
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "mcp__runtime-tools__query_entities",
			"arguments": map[string]any{"query": "kind = 'vertical'"},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/mcp?agent_id=analysis-agent&agent_role=analysis", strings.NewReader(string(body)))
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", callCount)
	}
	if !strings.Contains(rec.Body.String(), "MCP context token is required") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestGatewayHandleTool_RejectsMutatingToolWithoutContextToken(t *testing.T) {
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{})

	body, err := json.Marshal(ToolGatewayRequest{
		Input: map[string]any{"dimension": "build_complexity", "score": 72},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/tools/emit_score_dimension_complete", strings.NewReader(string(body)))
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", callCount)
	}
	if !strings.Contains(rec.Body.String(), "MCP context token is required") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestGatewayHandleTool_RejectsReadOnlyToolWithoutContextToken(t *testing.T) {
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{})

	body, err := json.Marshal(ToolGatewayRequest{
		Input: map[string]any{"query": "kind = 'vertical'"},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/tools/query_entities", strings.NewReader(string(body)))
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", callCount)
	}
	if !strings.Contains(rec.Body.String(), "MCP context token is required") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestGatewayHandleTool_IgnoresCallerSuppliedPrivilegeFields(t *testing.T) {
	registry := newTestTurnContextRegistry()
	exec := &actorAwareToolExecutorStub{}
	g := NewGateway(exec, testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-analysis", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, "query_entities"),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	body, err := json.Marshal(ToolGatewayRequest{
		Input: map[string]any{"dimension": "build_complexity", "score": 72},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/tools/emit_score_dimension_complete", strings.NewReader(string(body))), "ctx-analysis")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if exec.callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", exec.callCount)
	}
	if !strings.Contains(rec.Body.String(), "Authorization was denied") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestGatewayHandleTool_AllowsLegitimateRuntimeOwnedActorContext(t *testing.T) {
	registry := newTestTurnContextRegistry()
	exec := &actorAwareToolExecutorStub{}
	g := NewGateway(exec, testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-coordinator", TurnContext{
		Actor:             models.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"},
		CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "campaign-coordinator", Role: "campaign_coordinator"}, "emit_score_dimension_complete"),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	body, err := json.Marshal(ToolGatewayRequest{
		Input: map[string]any{"dimension": "build_complexity", "score": 72},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := withContextToken(httptest.NewRequest(http.MethodPost, "/tools/emit_score_dimension_complete", strings.NewReader(string(body))), "ctx-coordinator")
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if exec.callCount != 1 {
		t.Fatalf("executor call count = %d, want 1", exec.callCount)
	}
}

func TestGatewayTransports_AlignReadOnlyToolContextTokenFailures(t *testing.T) {
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{})

	mcpBody, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "query_entities",
			"arguments": map[string]any{"query": "kind = 'vertical'"},
		},
	})
	if err != nil {
		t.Fatalf("marshal mcp request: %v", err)
	}
	mcpReq := httptest.NewRequest(http.MethodPost, "/mcp?agent_id=analysis-agent&agent_role=analysis", strings.NewReader(string(mcpBody)))
	authorizeGatewayRequest(mcpReq)
	mcpRec := httptest.NewRecorder()
	g.Handler().ServeHTTP(mcpRec, mcpReq)

	toolBody, err := json.Marshal(ToolGatewayRequest{
		Input: map[string]any{"query": "kind = 'vertical'"},
	})
	if err != nil {
		t.Fatalf("marshal tool request: %v", err)
	}
	toolReq := httptest.NewRequest(http.MethodPost, "/tools/query_entities", strings.NewReader(string(toolBody)))
	authorizeGatewayRequest(toolReq)
	toolRec := httptest.NewRecorder()
	g.Handler().ServeHTTP(toolRec, toolReq)

	if callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", callCount)
	}
	if !strings.Contains(mcpRec.Body.String(), "MCP context token is required") {
		t.Fatalf("mcp body = %s", mcpRec.Body.String())
	}
	if !strings.Contains(toolRec.Body.String(), "MCP context token is required") {
		t.Fatalf("tool body = %s", toolRec.Body.String())
	}
}

func TestGatewayTransports_RejectLegacyQueryContextTokenCarrier(t *testing.T) {
	registry := newTestTurnContextRegistry()
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-query-only", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, "query_entities"),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	mcpBody, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "query_entities",
			"arguments": map[string]any{"query": "kind = 'vertical'"},
		},
	})
	if err != nil {
		t.Fatalf("marshal mcp request: %v", err)
	}
	mcpReq := httptest.NewRequest(http.MethodPost, "/mcp?ctx_token=ctx-query-only", strings.NewReader(string(mcpBody)))
	authorizeGatewayRequest(mcpReq)
	mcpRec := httptest.NewRecorder()
	g.Handler().ServeHTTP(mcpRec, mcpReq)

	toolBody, err := json.Marshal(ToolGatewayRequest{
		Input: map[string]any{"query": "kind = 'vertical'"},
	})
	if err != nil {
		t.Fatalf("marshal tool request: %v", err)
	}
	toolReq := httptest.NewRequest(http.MethodPost, "/tools/query_entities?ctx_token=ctx-query-only", strings.NewReader(string(toolBody)))
	authorizeGatewayRequest(toolReq)
	toolRec := httptest.NewRecorder()
	g.Handler().ServeHTTP(toolRec, toolReq)

	if callCount != 0 {
		t.Fatalf("executor call count = %d, want 0", callCount)
	}
	if !strings.Contains(mcpRec.Body.String(), "MCP context token is required") {
		t.Fatalf("mcp body = %s", mcpRec.Body.String())
	}
	if !strings.Contains(toolRec.Body.String(), "MCP context token is required") {
		t.Fatalf("tool body = %s", toolRec.Body.String())
	}
}

func TestGatewayTransports_AlignReadOnlyToolSuccessWithResolvedTurnContext(t *testing.T) {
	registry := newTestTurnContextRegistry()
	callCount := 0
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		callCount++
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{
		ResolveTurnContext: registry.ResolveTurnContext,
	})

	putTestTurnContext(t, registry, "ctx-query", TurnContext{
		Actor:             models.AgentConfig{ID: "analysis-agent", Role: "analysis"},
		CapabilitySurface: testCapabilitySurface(t, models.AgentConfig{ID: "analysis-agent", Role: "analysis"}, "query_entities"),
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().UTC().Add(time.Hour),
	})

	mcpBody, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "query_entities",
			"arguments": map[string]any{"query": "kind = 'vertical'"},
		},
	})
	if err != nil {
		t.Fatalf("marshal mcp request: %v", err)
	}
	mcpReq := withContextToken(httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(string(mcpBody))), "ctx-query")
	authorizeGatewayRequest(mcpReq)
	mcpRec := httptest.NewRecorder()
	g.Handler().ServeHTTP(mcpRec, mcpReq)

	toolBody, err := json.Marshal(ToolGatewayRequest{
		Input: map[string]any{"query": "kind = 'vertical'"},
	})
	if err != nil {
		t.Fatalf("marshal tool request: %v", err)
	}
	toolReq := withContextToken(httptest.NewRequest(http.MethodPost, "/tools/query_entities", strings.NewReader(string(toolBody))), "ctx-query")
	authorizeGatewayRequest(toolReq)
	toolRec := httptest.NewRecorder()
	g.Handler().ServeHTTP(toolRec, toolReq)

	if mcpRec.Code != http.StatusOK {
		t.Fatalf("mcp status = %d, want 200", mcpRec.Code)
	}
	if toolRec.Code != http.StatusOK {
		t.Fatalf("tool status = %d, want 200", toolRec.Code)
	}
	if callCount != 2 {
		t.Fatalf("executor call count = %d, want 2", callCount)
	}
	if strings.Contains(mcpRec.Body.String(), "MCP context token is required") {
		t.Fatalf("mcp body = %s", mcpRec.Body.String())
	}
	if strings.Contains(toolRec.Body.String(), "MCP context token is required") {
		t.Fatalf("tool body = %s", toolRec.Body.String())
	}
}

func TestGatewayTransports_PreserveRequestContextValueAfterResolvedTurn(t *testing.T) {
	base := context.WithValue(unmanagedMCPTestContext(), gatewayCtxProbeKey("probe"), "value-from-request")
	mcpObs, toolObs := runGatewayTransportPair(t, base)

	if mcpObs.value != "value-from-request" {
		t.Fatalf("mcp context value = %q, want propagated request value", mcpObs.value)
	}
	if toolObs.value != "value-from-request" {
		t.Fatalf("tool context value = %q, want propagated request value", toolObs.value)
	}
	if mcpObs.value != toolObs.value {
		t.Fatalf("transport value mismatch: mcp=%q tool=%q", mcpObs.value, toolObs.value)
	}
}

func TestGatewayTransports_PreserveRequestDeadlineAfterResolvedTurn(t *testing.T) {
	wantDeadline := time.Now().UTC().Add(5 * time.Minute).Round(0)
	base, cancel := context.WithDeadline(context.WithValue(unmanagedMCPTestContext(), gatewayCtxProbeKey("probe"), "deadline"), wantDeadline)
	defer cancel()

	mcpObs, toolObs := runGatewayTransportPair(t, base)

	if !mcpObs.hasDeadline {
		t.Fatal("mcp context missing propagated deadline")
	}
	if !toolObs.hasDeadline {
		t.Fatal("tool context missing propagated deadline")
	}
	if !mcpObs.deadline.Equal(wantDeadline) {
		t.Fatalf("mcp deadline = %s, want %s", mcpObs.deadline, wantDeadline)
	}
	if !toolObs.deadline.Equal(wantDeadline) {
		t.Fatalf("tool deadline = %s, want %s", toolObs.deadline, wantDeadline)
	}
}

func TestGatewayTransports_PreserveRequestCancellationAfterResolvedTurn(t *testing.T) {
	base, cancel := context.WithCancel(context.WithValue(unmanagedMCPTestContext(), gatewayCtxProbeKey("probe"), "cancel"))
	cancel()

	mcpObs, toolObs := runGatewayTransportPair(t, base)

	if mcpObs.err != context.Canceled {
		t.Fatalf("mcp ctx err = %v, want %v", mcpObs.err, context.Canceled)
	}
	if toolObs.err != context.Canceled {
		t.Fatalf("tool ctx err = %v, want %v", toolObs.err, context.Canceled)
	}
}

func TestGatewayHandleMCP_DoesNotLogFallbackUsedReason(t *testing.T) {
	var actions []string
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{
		Log: func(_ context.Context, level, action, agentID, entityID string, detail map[string]any, failure *failures.Envelope) {
			actions = append(actions, action)
		},
	})

	body, err := json.Marshal(map[string]any{
		"id":     "req-1",
		"method": "tools/call",
		"params": map[string]any{
			"name":      "mcp__runtime-tools__query_entities",
			"arguments": map[string]any{"query": "kind = 'vertical'"},
		},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/mcp?agent_id=analysis-agent&agent_role=analysis", strings.NewReader(string(body)))
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if slices.Contains(actions, "mcp.context.fallback_used") {
		t.Fatalf("actions = %#v, did not expect mcp.context.fallback_used", actions)
	}
}

func TestGatewayHandleTool_DoesNotLogFallbackBlockedReason(t *testing.T) {
	var actions []string
	g := NewGateway(testToolExecutor(func(_ context.Context, name string, input any) (any, error) {
		return map[string]any{"ok": true, "name": name, "input": input}, nil
	}), testGatewayToken, GatewayHooks{
		Log: func(_ context.Context, level, action, agentID, entityID string, detail map[string]any, failure *failures.Envelope) {
			actions = append(actions, action)
		},
	})

	body, err := json.Marshal(ToolGatewayRequest{
		Input: map[string]any{"dimension": "build_complexity", "score": 72},
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/tools/emit_score_dimension_complete", strings.NewReader(string(body)))
	authorizeGatewayRequest(req)
	rec := httptest.NewRecorder()
	g.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if slices.Contains(actions, "tool.context.fallback_blocked") {
		t.Fatalf("actions = %#v, did not expect tool.context.fallback_blocked", actions)
	}
}
