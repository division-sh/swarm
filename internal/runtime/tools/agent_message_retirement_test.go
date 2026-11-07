package tools

import (
	"context"
	"strings"
	"testing"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/google/uuid"
)

func TestAgentMessageRetirementManagedSurfaceMatrix(t *testing.T) {
	exec := NewExecutorWithOptions(nil, ExecutorOptions{})
	for _, provider := range []llm.ProviderContract{
		llm.AnthropicAPIProviderContract(), llm.OpenAICompatibleProviderContract(),
		llm.OpenAIResponsesProviderContract(), llm.ClaudeCLIProviderContract(), llm.MockProviderContract(),
	} {
		for _, kind := range []managedcapabilities.ExecutionKind{managedcapabilities.ExecutionNormalAgent, managedcapabilities.ExecutionSelectedForkPreparation} {
			t.Run(provider.Provider+"/"+string(provider.Transport)+"/"+string(kind), func(t *testing.T) {
				identity := agentidentitytest.RootRuntime(t, "retirement-agent", "retirement-proof")
				actor := models.AgentConfig{ID: "retirement-agent", Identity: identity, ExecutionMode: "live",
					Tools:       []string{RetiredAgentMessageTool, "mcp__runtime-tools__" + RetiredAgentMessageTool},
					Permissions: []string{"message_flow", "message_peers", AskHumanToolName}}
				plan, err := identity.Plan()
				if err != nil {
					t.Fatal(err)
				}
				if kind == managedcapabilities.ExecutionSelectedForkPreparation {
					actor.Identity = agentidentity.Identity{}
				}
				ctx := WithActor(context.Background(), actor)
				definitions := exec.ToolDefinitionsForActor(actor)
				names := []string{NotifyHumanToolName, AskHumanToolName, RetiredAgentMessageTool, "mcp__runtime-tools__" + RetiredAgentMessageTool}
				capabilities := exec.ToolCapabilitiesForActor(actor, names, nil)
				authority := managedcapabilities.Authority{
					Kind: managedcapabilities.AuthorityStartupProbe, ID: uuid.NewString(), ExecutionKind: kind,
					ExecutionAuthorityID: uuid.NewString(), StartupOwnerID: "retirement-proof", StartupGeneration: 1,
				}
				if kind == managedcapabilities.ExecutionSelectedForkPreparation {
					fingerprint, err := plan.Fingerprint()
					if err != nil {
						t.Fatal(err)
					}
					authority.StartupOwnerID, authority.StartupGeneration = "", 0
					authority.Preparation = &managedcapabilities.PreparedSelectedForkProbeAuthority{
						SelectedForkPreparationCoordinates: managedcapabilities.SelectedForkPreparationCoordinates{
							ProcessAuthorityID: uuid.NewString(), ProcessOwnerID: "retirement-proof", ProcessBootID: uuid.NewString(),
							BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64), SourceFingerprint: strings.Repeat("b", 64),
							AdmittedPlanFingerprint: strings.Repeat("c", 64), ConfigurationFingerprint: strings.Repeat("d", 64), CatalogFingerprint: strings.Repeat("e", 64),
						}, ActorPlanFingerprint: fingerprint,
					}
				}
				surface, err := llm.ManagedCapabilitySurfaceForStartup(ctx, plan, llm.NewNoopRuntime(provider), definitions, capabilities, authority)
				if err != nil {
					t.Fatalf("plan real executor surface: %v", err)
				}
				active := map[string]bool{}
				for _, tool := range surface.Tools {
					if tool.Name == RetiredAgentMessageTool || strings.Contains(tool.Name, "__agent_message") {
						t.Fatalf("retired candidate planned: %+v", tool)
					}
					if tool.Capability.Visible && tool.Capability.Callable {
						active[tool.Name] = true
					}
				}
				if !active[NotifyHumanToolName] || !active[AskHumanToolName] {
					t.Fatalf("active human candidates lost: %+v", surface)
				}
				for _, name := range names[2:] {
					if _, err := exec.Execute(ctx, name, map[string]any{}); err == nil || !strings.Contains(err.Error(), `tool "agent_message" is unsupported`) {
						t.Fatalf("hostile execution %s = %v", name, err)
					}
				}
			})
		}
	}
}

func TestAgentMessageRetirementPermissionVocabularyCannotBeResurrected(t *testing.T) {
	for _, name := range []string{"message_flow", "message_peers"} {
		for _, extension := range []string{"bundle", "tool"} {
			t.Run(name+"/"+extension, func(t *testing.T) {
				known := map[string]struct{}{}
				if extension == "bundle" {
					if err := collectPermissionBundleExtensions(known, hitlLifecyclePermissionBundle(name)); err != nil {
						t.Fatal(err)
					}
				} else {
					entry := retiredToolEntry(runtimecontracts.WithToolPermission(name))
					collectToolPermissionExtensions(known, retiredToolSourceForScope("root", nil, map[string]runtimecontracts.ToolSchemaEntry{"lookup": entry}, runtimecontracts.PolicyDocument{}))
				}
				if _, present := known[name]; present {
					t.Fatalf("%s extension resurrected %s", extension, name)
				}
			})
		}
	}
}
