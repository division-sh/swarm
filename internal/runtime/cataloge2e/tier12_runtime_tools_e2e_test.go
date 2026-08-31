package cataloge2e

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimeagentidentity "github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/flowdata"
	llm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestTier12RuntimeTools_FlowDataAccessFixture(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.ArtifactID("tests/tier12-runtime-tools/test-flow-data-access"))
	fixtures := catalogRuntimeFixtures(t, "catalog.runtime.flow_data_access_tools")
	if len(fixtures) != 1 {
		t.Fatalf("flow data access runtime fixtures = %d, want 1", len(fixtures))
	}
	fixtureRoot := fixtures[0].Root

	h := newRuntimeHarness(t, fixtureRoot, true)
	bundleHash, bundleSource := h.rt.Options.BundleSourceFact.StorageValues()
	desired, err := h.rt.Manager.CompileStaticTopologyDesiredAgents(h.rt.Options.WorkflowModule.SemanticSource(), runtimeagenttopology.SourceCoordinate{
		BundleHash: bundleHash, BundleSource: bundleSource,
	})
	if err != nil {
		t.Fatalf("compile flow-data agent topology: %v", err)
	}
	var identity runtimeagentidentity.Identity
	for _, candidate := range desired {
		if candidate.Identity.AgentID() != "reference-agent" {
			continue
		}
		identity, err = candidate.Identity.Live(catalogRuntimeRunID)
		if err != nil {
			t.Fatalf("materialize flow-data agent identity: %v", err)
		}
		break
	}
	if err := identity.Validate(); err != nil {
		t.Fatalf("flow-data fixture has no reference-agent declaration: %v", err)
	}
	readinessEvent := eventtest.ExistingRunRootIngress(
		eventtest.UUID("flow-data-readiness"), "support.requested", "cataloge2e", "", []byte(`{}`), 0,
		catalogRuntimeRunID, events.EventEnvelope{}, time.Now().UTC(),
	)
	if err := h.rt.Manager.FinalizeCommittedAgentReadiness(h.ctx, readinessEvent, []events.DeliveryRoute{{
		Recipient: events.MustAgentDeliveryRecipient("reference-agent"), AgentIdentity: identity,
	}}); err != nil {
		t.Fatalf("finalize flow-data agent readiness: %v", err)
	}
	cfg, err := h.rt.Manager.ResolveAgentConfig(catalogRuntimeRunID, "reference-agent", "support")
	if err != nil {
		t.Fatalf("resolve reference-agent config: %v", err)
	}
	if cfg.FlowPath != "support" {
		t.Fatalf("flow path = %q, want support", cfg.FlowPath)
	}
	if len(cfg.FlowDataAccess) != 1 || cfg.FlowDataAccess[0] != "exclusions.yaml" {
		t.Fatalf("flow data access = %#v, want [exclusions.yaml]", cfg.FlowDataAccess)
	}

	defs := h.rt.ToolExecutor.ToolDefinitionsForActor(cfg)
	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, strings.TrimSpace(def.Name))
	}
	if !containsTier12String(names, "read_flow_data") {
		t.Fatalf("runtime tool definitions = %#v, want read_flow_data", names)
	}
	if containsTier12String(names, "read_file") || containsTier12String(names, "write_file") {
		t.Fatalf("flow_data_access exposed native file tools: %#v", names)
	}

	static := flowdata.AllowedStaticData(semanticview.Wrap(h.bundle), cfg)
	if len(static) != 1 || static[0].RelativePath != "exclusions.yaml" {
		t.Fatalf("compiled static data = %#v, want exact exclusions.yaml grant", static)
	}
	toolCtx := runtimecorrelation.WithRunID(testAuthorActivityContext(context.Background()), catalogRuntimeRunID)
	out, err := h.rt.ToolExecutor.Execute(models.WithActor(toolCtx, cfg), "read_flow_data", map[string]any{
		"kind": "static_file", "static_id": static[0].StaticID,
	})
	if err != nil {
		t.Fatalf("read_flow_data(exclusions.yaml): %v", err)
	}
	result, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("read_flow_data result = %T, want map[string]any", out)
	}
	content := strings.TrimSpace(asString(result["content"]))
	if !strings.Contains(content, "unmanaged-host-file-reads") || !strings.Contains(content, "cross-flow-data-access") {
		t.Fatalf("content = %q, want declared fixture data", content)
	}
	if got := strings.TrimSpace(asString(result["content_type"])); got != "yaml" {
		t.Fatalf("content_type = %q, want yaml", got)
	}

	if _, err := h.rt.ToolExecutor.Execute(models.WithActor(toolCtx, cfg), "read_flow_data", map[string]any{
		"filename": "undeclared.yaml",
	}); err == nil {
		t.Fatal("undeclared read unexpectedly succeeded")
	} else if failure, ok := runtimefailures.As(err); !ok || failure.Failure.Class != runtimefailures.ClassSchemaInvalid || failure.Failure.Detail.Code != "invalid_tool_input" {
		t.Fatalf("undeclared read failure = %#v, want fail-closed schema rejection", failure)
	}
	if _, err := h.rt.ToolExecutor.Execute(models.WithActor(toolCtx, cfg), "read_flow_data", map[string]any{
		"filename": "../support/data/exclusions.yaml",
	}); err == nil {
		t.Fatal("traversal read succeeded, want fail-closed error")
	}

	other := cfg
	other.ID = "other-agent"
	other.Role = "other"
	other.FlowDataAccess = nil
	if defs := h.rt.ToolExecutor.ToolDefinitionsForActor(other); containsTier12String(toolNamesForTier12RuntimeTools(defs), "read_flow_data") {
		t.Fatalf("undeclared actor saw read_flow_data definitions: %#v", toolNamesForTier12RuntimeTools(defs))
	}
	if _, err := h.rt.ToolExecutor.Execute(models.WithActor(toolCtx, other), "read_flow_data", map[string]any{
		"kind": "static_file", "static_id": static[0].StaticID,
	}); err == nil {
		t.Fatal("undeclared actor read flow data, want fail-closed error")
	}
}

func toolNamesForTier12RuntimeTools(defs []llm.ToolDefinition) []string {
	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, strings.TrimSpace(def.Name))
	}
	return names
}
