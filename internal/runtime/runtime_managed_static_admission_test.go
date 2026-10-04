package runtime

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime/agentintent"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
)

type staticAdmissionActorTools struct{ *startupProbeToolExecutor }

func (s staticAdmissionActorTools) ToolDefinitionsForActorInContext(ctx context.Context, actor actors.AgentConfig) []llm.ToolDefinition {
	definitions := s.startupProbeToolExecutor.ToolDefinitionsForActorInContext(ctx, actor)
	if actor.ID == "beta" {
		definitions = append(definitions, definitions[0])
	}
	return definitions
}

func TestManagedStartupStaticCensusRejectsBeforeExecutionAuthorityOrPersistence(t *testing.T) {
	for _, seam := range []string{"normal", "selected_catalog"} {
		for _, invalid := range []string{"prompt", "duplicate_tool"} {
			t.Run(seam+"/"+invalid, func(t *testing.T) {
				cfg := &config.Config{LLM: config.LLMConfig{Backend: "claude_cli"}}
				profile, err := cfg.LLMBackendProfile()
				if err != nil {
					t.Fatal(err)
				}
				probe := &preparedProtocolProbe{}
				runtimes, err := llm.NewAgentRuntimeSet(profile, llm.RuntimeFactory{}, startupProbeRuntime{ClaudeCLIRuntime: &llm.ClaudeCLIRuntime{}, probe: probe})
				if err != nil {
					t.Fatal(err)
				}
				blueprints, err := manager.StaticAgentMaterializationBlueprints(claudeStartupAgentSource("alpha", "beta"))
				if err != nil || len(blueprints) != 2 {
					t.Fatalf("blueprints: %#v %v", blueprints, err)
				}
				var candidates []managedProviderPreflightAgent
				for i := range blueprints {
					blueprints[i], err = manager.ResolveAgentMaterializationBlueprint(manager.AgentManagerOptions{ExecutionPosture: executionposture.Live, LLMBackend: "claude_cli"}, blueprints[i])
					if err != nil {
						t.Fatal(err)
					}
					if invalid == "prompt" && blueprints[i].Config.ID == "beta" {
						blueprints[i].Config.Prompt = agentintent.DerivedPrompt{}
					}
					candidates = append(candidates, managedProviderPreflightAgent{config: blueprints[i].Config, plan: blueprints[i].Identity})
				}
				executor := &startupProbeToolExecutor{defs: startupProbeDefs(), caps: startupProbeCaps()}
				var tools claudeStartupToolSource = executor
				if invalid == "duplicate_tool" {
					tools = staticAdmissionActorTools{executor}
				}
				if seam == "normal" {
					_, err = validateManagedProviderPreflightConfigs(testAuthorActivityContext(context.Background()), cfg, toolgateway.Binding{}, runtimes, nil, tools, candidates, ManagedProviderPreflightAuthority{})
				} else {
					_, err = PrepareSelectedForkProviderCatalog(testAuthorActivityContext(context.Background()), runtimes, tools, blueprints)
				}
				want := "derived prompt"
				if invalid == "duplicate_tool" {
					want = "duplicated"
				}
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("static %s refusal missing before unavailable execution ports: %v", invalid, err)
				}
				if len(probe.calls) != 0 || len(executor.executed) != 0 {
					t.Fatalf("static refusal executed a provider or tool: probes=%#v tools=%#v", probe.calls, executor.executed)
				}
			})
		}
	}
}
