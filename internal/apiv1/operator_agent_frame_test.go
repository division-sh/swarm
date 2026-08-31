package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/bundlecatalog"
	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
)

type agentFrameEffectiveResolverStub struct {
	result runtimemanager.AgentFrameConfig
	err    error
	calls  int
}

type paginatedAgentFrameCatalog struct {
	*fakeBundleCatalogReadStore
	pages map[string]bundlecatalog.AgentsResult
	calls []bundlecatalog.AgentListOptions
}

func (s *paginatedAgentFrameCatalog) ListBundleCatalogAgents(_ context.Context, _ string, opts bundlecatalog.AgentListOptions) (bundlecatalog.AgentsResult, error) {
	s.calls = append(s.calls, opts)
	return s.pages[opts.Cursor], nil
}

func (s *agentFrameEffectiveResolverStub) ResolveAgentFrameConfig(runID, agentID, flowInstance string, root bool) (runtimemanager.AgentFrameConfig, error) {
	s.calls++
	return s.result, s.err
}

func TestOperatorAgentFrameInspectionScopesUseCanonicalProjection(t *testing.T) {
	bundleHash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	intent := apiTestResolvedIntent(t, "reviewer", "Review the admitted work.")
	providerPrompt := agentFrameTestProviderPrompt(t, intent)
	catalog := &fakeBundleCatalogReadStore{agents: map[string]bundlecatalog.AgentsResult{
		bundleHash: {Agents: []bundlecatalog.AgentDefinition{{
			AgentID: "reviewer", FlowInstance: "review", Role: "reviewer",
			IntentKind: string(intent.Kind), IntentSource: intent.Coordinate, IntentProvenance: intent.Provenance,
			IntentContent: intent.Content, IntentContentHash: intent.ContentHash, IntentIdentity: intent.Identity,
			ProviderPrompt: providerPrompt,
		}}},
	}}
	identity := agentidentitytest.RootRuntime(t, "reviewer", "agent-frame-api-test")
	resolver := &agentFrameEffectiveResolverStub{result: runtimemanager.AgentFrameConfig{
		BundleHash: bundleHash, BundleSource: "persisted",
		Config: runtimeactors.AgentConfig{
			ID: "reviewer", Identity: identity, Role: "reviewer", FlowID: "root", Intent: intent, Model: "regular",
			Prompt: agentFrameTestDerivedPrompt(t, intent), ResolvedLLMBackend: "claude_api",
			ResolvedLLMProvider: "anthropic", ResolvedLLMTransport: "api", ResolvedModel: "claude-sonnet",
		},
	}}
	handler := OperatorAgentFrameHandlers(AgentFrameHandlerOptions{Catalog: catalog, Effective: resolver})["agent.frame"]

	staticRaw, err := handler(context.Background(), Request{Params: map[string]any{
		"scope": "static", "agent_id": "reviewer", "bundle_hash": bundleHash, "flow": "review",
	}})
	if err != nil {
		t.Fatal(err)
	}
	static := staticRaw.(agentframe.Inspection)
	if static.Scope != agentframe.InspectionStatic || static.Session.AgentIdentity.Status != "unresolved" || static.Session.Provider.Status != "unresolved" {
		t.Fatalf("static inspection runtime facts = %#v", static.Session)
	}
	assertAgentFrameOccurrenceUnresolved(t, static)

	effectiveRaw, err := handler(context.Background(), Request{Params: map[string]any{
		"scope": "effective", "agent_id": "reviewer", "run_id": agentidentitytest.DefaultRunID, "root": true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	effective := effectiveRaw.(agentframe.Inspection)
	if effective.Scope != agentframe.InspectionEffective || effective.Session.AgentIdentity.Value == nil || effective.Session.Provider.Value == nil {
		t.Fatalf("effective inspection runtime facts = %#v", effective.Session)
	}
	if effective.Session.BundleSource.Value == nil || *effective.Session.BundleSource.Value != "persisted" {
		t.Fatalf("effective bundle source = %#v", effective.Session.BundleSource)
	}
	if effective.Session.Provider.Value.ModelAlias != "regular" || effective.Session.Provider.Value.Model != "claude-sonnet" {
		t.Fatalf("effective provider selection = %#v", effective.Session.Provider.Value)
	}
	root := repoRoot(t)
	openRPC, _ := loadComplianceOpenRPC(t, complianceOpenRPCPath(root))
	encoded, err := json.Marshal(effective)
	if err != nil {
		t.Fatalf("marshal effective inspection: %v", err)
	}
	var schemaResult any
	if err := json.Unmarshal(encoded, &schemaResult); err != nil {
		t.Fatalf("decode effective inspection: %v", err)
	}
	newOpenRPCResultSchemaValidator(t, openRPC).validateMethodResult(t, "agent.frame", schemaResult)
	assertAgentFrameOccurrenceUnresolved(t, effective)
}

func TestOperatorAgentFrameStaticInspectionConsumesBoundedCatalogPagesExhaustively(t *testing.T) {
	bundleHash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	intent := apiTestResolvedIntent(t, "reviewer", "Review the admitted work.")
	catalog := &paginatedAgentFrameCatalog{
		fakeBundleCatalogReadStore: &fakeBundleCatalogReadStore{},
		pages: map[string]bundlecatalog.AgentsResult{
			"": {Agents: []bundlecatalog.AgentDefinition{{AgentID: "other", FlowInstance: "review"}}, NextCursor: "page-2"},
			"page-2": {Agents: []bundlecatalog.AgentDefinition{{
				AgentID: "reviewer", FlowInstance: "review", Role: "reviewer",
				IntentKind: string(intent.Kind), IntentSource: intent.Coordinate, IntentProvenance: intent.Provenance,
				IntentContent: intent.Content, IntentContentHash: intent.ContentHash, IntentIdentity: intent.Identity,
				ProviderPrompt: agentFrameTestProviderPrompt(t, intent),
			}}},
		},
	}
	handler := OperatorAgentFrameHandlers(AgentFrameHandlerOptions{Catalog: catalog})["agent.frame"]
	result, err := handler(context.Background(), Request{Params: map[string]any{
		"scope": "static", "agent_id": "reviewer", "bundle_hash": bundleHash, "flow": "review",
	}})
	if err != nil {
		t.Fatal(err)
	}
	inspection := result.(agentframe.Inspection)
	if inspection.Session.AgentID != "reviewer" || len(catalog.calls) != 2 {
		t.Fatalf("inspection agent=%q calls=%#v, want reviewer across two pages", inspection.Session.AgentID, catalog.calls)
	}
	if catalog.calls[0].Limit != bundlecatalog.MaxAgentListLimit || catalog.calls[0].Cursor != "" || catalog.calls[1].Cursor != "page-2" {
		t.Fatalf("catalog page options = %#v, want bounded exhaustive cursor walk", catalog.calls)
	}
}

func TestOperatorAgentFrameRejectsMixedSelectorsBeforeEffectiveResolution(t *testing.T) {
	resolver := &agentFrameEffectiveResolverStub{}
	handler := OperatorAgentFrameHandlers(AgentFrameHandlerOptions{Effective: resolver})["agent.frame"]
	_, err := handler(context.Background(), Request{Params: map[string]any{
		"scope": "effective", "agent_id": "reviewer", "root": true, "flow_instance": "review/one",
	}})
	if err == nil {
		t.Fatal("expected selector rejection")
	}
	if resolver.calls != 0 {
		t.Fatalf("effective resolver calls = %d, want 0", resolver.calls)
	}
}

func TestOperatorAgentFrameRejectsNoncanonicalPathAliasesBeforeResolution(t *testing.T) {
	bundleHash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	catalog := &fakeBundleCatalogReadStore{}
	resolver := &agentFrameEffectiveResolverStub{}
	handler := OperatorAgentFrameHandlers(AgentFrameHandlerOptions{Catalog: catalog, Effective: resolver})["agent.frame"]
	cases := []map[string]any{
		{"scope": "static", "agent_id": "reviewer", "bundle_hash": bundleHash, "flow": "/review/"},
		{"scope": "static", "agent_id": "reviewer", "bundle_hash": bundleHash, "flow": " review "},
		{"scope": "effective", "agent_id": "reviewer", "flow_instance": "/review/one/"},
		{"scope": "effective", "agent_id": "reviewer", "flow_instance": "review/one/"},
	}
	for _, params := range cases {
		_, err := handler(context.Background(), Request{Params: params})
		var invalid *InvalidParamsError
		if !errors.As(err, &invalid) {
			t.Fatalf("params %#v error = %T %v, want invalid params", params, err, err)
		}
	}
	if resolver.calls != 0 {
		t.Fatalf("effective resolver calls = %d, want 0", resolver.calls)
	}
}

func TestOperatorAgentFrameRejectsNoncanonicalScalarAliasesBeforeResolution(t *testing.T) {
	bundleHash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	cases := []map[string]any{
		{"scope": " static ", "agent_id": "reviewer", "bundle_hash": bundleHash, "flow": "review"},
		{"scope": "static", "agent_id": " reviewer ", "bundle_hash": bundleHash, "flow": "review"},
		{"scope": "static", "agent_id": "reviewer", "bundle_hash": " " + bundleHash, "flow": "review"},
		{"scope": "effective", "agent_id": " reviewer ", "root": true},
		{"scope": "effective", "agent_id": "reviewer", "root": true, "bundle_hash": " "},
		{"scope": "effective", "agent_id": "reviewer", "root": true, "flow": " "},
	}
	for _, params := range cases {
		catalog := &paginatedAgentFrameCatalog{fakeBundleCatalogReadStore: &fakeBundleCatalogReadStore{}}
		resolver := &agentFrameEffectiveResolverStub{}
		handler := OperatorAgentFrameHandlers(AgentFrameHandlerOptions{Catalog: catalog, Effective: resolver})["agent.frame"]
		_, err := handler(context.Background(), Request{Params: params})
		var invalid *InvalidParamsError
		if !errors.As(err, &invalid) {
			t.Fatalf("params %#v error = %T %v, want invalid params", params, err, err)
		}
		if len(catalog.calls) != 0 {
			t.Fatalf("params %#v catalog calls = %d, want 0", params, len(catalog.calls))
		}
		if resolver.calls != 0 {
			t.Fatalf("params %#v resolver calls = %d, want 0", params, resolver.calls)
		}
	}
}

func TestOperatorAgentFrameNotFoundDetailsMatchDeclaredSchema(t *testing.T) {
	bundleHash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	catalog := &fakeBundleCatalogReadStore{agents: map[string]bundlecatalog.AgentsResult{bundleHash: {}}}
	resolver := &agentFrameEffectiveResolverStub{err: runtimemanager.ErrAgentNotFound}
	handler := OperatorAgentFrameHandlers(AgentFrameHandlerOptions{Catalog: catalog, Effective: resolver})["agent.frame"]
	for _, params := range []map[string]any{
		{"scope": "static", "agent_id": "missing", "bundle_hash": bundleHash, "flow": "review"},
		{"scope": "effective", "agent_id": "missing", "run_id": agentidentitytest.DefaultRunID, "root": true},
	} {
		_, err := handler(context.Background(), Request{Params: params})
		var appErr *ApplicationError
		if !errors.As(err, &appErr) || appErr.Code != AgentNotFoundCode {
			t.Fatalf("params %#v error = %T %v, want %s", params, err, err, AgentNotFoundCode)
		}
		want := map[string]any{"agent_id": "missing"}
		if !reflect.DeepEqual(appErr.Details, want) {
			t.Fatalf("params %#v details = %#v, want %#v", params, appErr.Details, want)
		}
	}
}

func assertAgentFrameOccurrenceUnresolved(t testing.TB, inspection agentframe.Inspection) {
	t.Helper()
	turn := inspection.Turn
	for name, status := range map[string]string{
		"frame_id": turn.FrameID.Status, "content_hash": turn.ContentHash.Status, "kind": turn.Kind.Status,
		"parent_frame_id": turn.ParentFrame.Status, "event": turn.Event.Status, "capability": turn.Capability.Status,
		"directive": turn.Directive.Status, "remediation": turn.Remediation.Status,
		"stage": turn.Lifecycle.Stage.Status, "loop_revision": turn.Lifecycle.LoopRevision.Status,
		"pack_provenance": turn.PackProvenance.Status,
	} {
		if status != "unresolved" {
			t.Fatalf("%s status = %q, want unresolved", name, status)
		}
	}
}

func agentFrameTestDerivedPrompt(t testing.TB, intent agentintent.Resolved) agentintent.DerivedPrompt {
	t.Helper()
	prompt, err := agentintent.IntentOnlyPrompt(intent)
	if err != nil {
		t.Fatal(err)
	}
	return prompt
}

func agentFrameTestProviderPrompt(t testing.TB, intent agentintent.Resolved) string {
	t.Helper()
	prompt, err := agentintent.AssembleProviderPrompt(intent, nil, agentFrameTestDerivedPrompt(t, intent), agentintent.RuntimeEnvironmentContext())
	if err != nil {
		t.Fatal(err)
	}
	text, err := prompt.Text()
	if err != nil {
		t.Fatal(err)
	}
	return text
}
