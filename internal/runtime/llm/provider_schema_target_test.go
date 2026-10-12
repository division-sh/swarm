package llm

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/google/uuid"
)

func TestProviderSchemaWorkspaceTargetRefusal(t *testing.T) {
	tool := ToolDefinition{Name: "write_file", Schema: map[string]any{"type": "unsupported-provider-type"}}
	actor := models.AgentConfig{ID: "schema-target", Identity: testAgentIdentity("schema-target", "")}
	tools, err := CompileManagedCapabilityAdmission(actor, MockProviderContract(), []ToolDefinition{tool}, toolcapabilities.NewSet([]toolcapabilities.Capability{{Name: tool.Name, Visible: true, Callable: true}}))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := actor.Identity.Plan()
	if err != nil {
		t.Fatal(err)
	}
	surface, err := managedcapabilities.New(managedcapabilities.Plan{ActorPlan: plan, RuntimeMode: "startup_probe", Provider: "mock", Transport: "cli", ProviderContract: "mock-schema-target",
		Authority: managedcapabilities.Authority{Kind: managedcapabilities.AuthorityStartupProbe, ID: uuid.NewString(), ExecutionKind: managedcapabilities.ExecutionNormalAgent,
			ExecutionAuthorityID: "schema-target-startup", StartupOwnerID: "schema-target-owner", StartupGeneration: 1}, Tools: tools})
	if err != nil {
		t.Fatal(err)
	}
	actor = models.AgentConfig{ID: plan.AgentID(), FlowPath: plan.FlowInstance()}
	ctx := managedcapabilities.WithContext(models.WithActor(llmTestWorkContext(t, context.Background()), actor), surface)
	binding, turns := nativeCLIProbeFixture(t, []ToolDefinition{tool})
	unregistered := 0
	turns.unregister = func(string) { unregistered++ }
	observed, err := ObserveWorkspaceGateway(ctx, &config.Config{}, turns, actor, []ToolDefinition{tool}, binding, mockHostRuntimeOptions(t).Workspaces)
	failure, ok := failures.EnvelopeFromError(err)
	if !ok || failure.Detail.Code != "managed_capability_mcp_definition_mismatch" || failure.Detail.Attributes["reason"] != "invalid_input_schema" || len(observed.Tools) != 0 || unregistered != 1 {
		t.Fatalf("native host probe confirmed invalid matching schema or leaked registration: observed=%+v err=%v unregister=%d", observed, err, unregistered)
	}
}
