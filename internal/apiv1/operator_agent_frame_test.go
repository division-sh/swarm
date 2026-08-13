package apiv1

import (
	"context"
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

func (s *agentFrameEffectiveResolverStub) ResolveAgentFrameConfig(agentID, flowInstance string, root bool) (runtimemanager.AgentFrameConfig, error) {
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
			ID: "reviewer", Identity: identity, Role: "reviewer", FlowID: "root", Intent: intent,
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
		"scope": "effective", "agent_id": "reviewer", "root": true,
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
	assertAgentFrameOccurrenceUnresolved(t, effective)
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

func TestOperatorAgentFrameNotFoundDetailsMatchDeclaredSchema(t *testing.T) {
	bundleHash := "bundle-v1:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	catalog := &fakeBundleCatalogReadStore{agents: map[string]bundlecatalog.AgentsResult{bundleHash: {}}}
	resolver := &agentFrameEffectiveResolverStub{err: runtimemanager.ErrAgentNotFound}
	handler := OperatorAgentFrameHandlers(AgentFrameHandlerOptions{Catalog: catalog, Effective: resolver})["agent.frame"]
	for _, params := range []map[string]any{
		{"scope": "static", "agent_id": "missing", "bundle_hash": bundleHash, "flow": "review"},
		{"scope": "effective", "agent_id": "missing", "root": true},
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
