package runforkexecution

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/config"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/mcp"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/toolgateway"
	"github.com/division-sh/swarm/internal/runtime/workspace"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/google/uuid"
)

type activationWorkspaceProcess struct {
	startupownership.ProcessCapability
	authority startupownership.Authority
}

func (p activationWorkspaceProcess) Evidence() (startupownership.Authority, error) {
	return p.authority, nil
}

func (activationWorkspaceProcess) ProveCurrent(ctx context.Context) error { return ctx.Err() }

type activationObservationWorkspace struct{ *preparationWorkspace }

func (w *activationObservationWorkspace) ResolveWorkspaceForCapabilityAdmission(ctx context.Context, actor actors.AgentConfig) (*workspace.Target, error) {
	if err := w.step("probe"); err != nil {
		return nil, err
	}
	return nil, errWorkspaceProbeReached
}

func (w *activationObservationWorkspace) RebindSourceProjection(p *sourceartifact.RuntimeProjection, source semanticview.Source) (workspace.Lifecycle, error) {
	bound, err := w.preparationWorkspace.RebindSourceProjection(p, source)
	if err != nil {
		return nil, err
	}
	return &activationObservationWorkspace{bound.(*preparationWorkspace)}, nil
}

func activationWorkspaceCatalog(t *testing.T, backend, id string) (*runtimepkg.PreparedSelectedForkProviderCatalog, actors.AgentConfig) {
	t.Helper()
	cfg := &config.Config{LLM: config.LLMConfig{Backend: backend,
		Models: selection.ModelAliases{selection.ModelAliasRegular: {backend: "test-model"}},
	}}
	profile, err := cfg.LLMBackendProfile()
	if err != nil {
		t.Fatal(err)
	}
	runtimes, err := llm.NewPreparedAgentRuntimeSet(profile, llm.RuntimeFactory{Cfg: cfg})
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		catalog, err := runtimepkg.PrepareSelectedForkProviderCatalog(context.Background(), runtimes, &selectedForkStartupProbeExecutor{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return catalog, actors.AgentConfig{}
	}
	identity := selectedContractTestRootAgentIdentity(t, id)
	plan, err := identity.Plan()
	if err != nil {
		t.Fatal(err)
	}
	actorConfig := selectedContractTestAgentConfig(t, actors.AgentConfig{
		ID: id, Identity: identity, Role: "selected_worker", Model: selection.ModelAliasRegular,
	})
	actorConfig.Identity = agentidentity.Identity{}
	blueprint, err := manager.ResolveAgentMaterializationBlueprint(manager.AgentManagerOptions{
		ExecutionPosture: executionposture.Live, LLMBackend: backend,
	}, manager.AgentMaterializationBlueprint{Identity: plan, Config: actorConfig})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := runtimepkg.PrepareSelectedForkProviderCatalog(context.Background(), runtimes, &selectedForkStartupProbeExecutor{}, []manager.AgentMaterializationBlueprint{blueprint})
	if err != nil {
		t.Fatal(err)
	}
	actor := blueprint.Config
	actor.Identity = identity
	return catalog, actor
}

// This exercises the selected hook's catalog observation path, without a
// provider launch or pretending that the catalog admits a constructed actor.
func TestSelectedActivationGatewayInitializesLaterProbingWorkspace(t *testing.T) {
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	repo := runForkExecutionRepoRoot(t)
	for _, initial := range []string{"empty", "api_only"} {
		for _, mode := range []string{"cold", "nonprobing", "prerequisite_failure", "partial_failure", "cancel_before", "cancel_after_prerequisites", "cancel_after_system", "released"} {
			t.Run(initial+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				initialID := ""
				if initial == "api_only" {
					initialID = "initial_api_actor"
				}
				original, _ := activationWorkspaceCatalog(t, selection.BackendAnthropic, initialID)
				originalFingerprint := original.Fingerprint()
				backend := selection.BackendClaudeCLI
				if mode == "nonprobing" {
					backend = selection.BackendAnthropic
				}
				catalog, actor := activationWorkspaceCatalog(t, backend, "later_actor")
				trace := &workspacePreparationTrace{mode: mode, cancel: cancel}
				source := &activationObservationWorkspace{&preparationWorkspace{Lifecycle: workspace.NewHostManager(), trace: trace}}
				loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo,
					SourceRoot:       filepath.Join(repo, "internal/runtime/runforkexecution/testdata/selected_fork_flow_scoped_mcp"),
					PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo)}
				loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
				if err != nil {
					t.Fatal(err)
				}
				artifact, err := sourceartifact.AdmitDirectory(loader.SourceRoot)
				if err != nil {
					t.Fatal(err)
				}
				loaded.RuntimeProjection, err = sourceartifact.MaterializeRuntimeProjection(artifact)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := loaded.RuntimeProjection.Release(); err != nil {
						t.Error(err)
					}
				})
				options, projection, err := bindSelectedContractWorkspaceProjection(loaded, SelectedContractAgentRuntimeOptions{
					Config: &config.Config{LLM: config.LLMConfig{Backend: backend}}, Workspace: source,
				})
				if err != nil || projection == nil {
					t.Fatalf("bind selected workspace: %v", err)
				}
				authority, err := startupownership.NewColdAuthority(startupownership.AcquireRequest{
					OwnerID: "selected-activation-test", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
				}, "selected_activation_test")
				if err != nil {
					t.Fatal(err)
				}
				options.ProcessCapability = activationWorkspaceProcess{authority: authority}
				plan := selectedContractAgentRuntimePlan{workspaceProjection: projection, Options: options}
				coordinates := managedcapabilities.SelectedForkPreparationCoordinates{
					ProcessAuthorityID: authority.AuthorityID, ProcessOwnerID: authority.OwnerID, ProcessBootID: authority.BootID,
					BundleHash: runForkTestBundleHash, SourceFingerprint: strings.Repeat("1", 64),
					AdmittedPlanFingerprint: strings.Repeat("2", 64), ConfigurationFingerprint: strings.Repeat("3", 64),
					CatalogFingerprint: catalog.Fingerprint(),
				}
				binding, err := toolgateway.NewRuntimeOwnedBinding(toolgateway.TransportHTTP,
					"http://127.0.0.1:12345", "http://host.docker.internal:12345", "selected-activation-test",
					toolgateway.LifecycleOwnerSelectedForkRuntime, toolgateway.SourceSelectedForkEphemeralGateway)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "released" {
					if err := projection.Release(); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "cancel_before" {
					cancel()
				}
				observe := func() error {
					return plan.observeActivationGateway(ctx, catalog, binding, mcp.NewTurnContextRegistry(actors.ActorFromContext), actor, uuid.NewString(), coordinates)
				}
				err = observe()
				wantSteps := []string{"prerequisites", "scaffold_created", "system_created", "probe"}
				switch mode {
				case "cold":
					if !errors.Is(err, errWorkspaceProbeReached) || !projection.initialized {
						t.Fatalf("cold selected observation: initialized=%v err=%v steps=%v", projection.initialized, err, trace.steps)
					}
					if err := observe(); !errors.Is(err, errWorkspaceProbeReached) {
						t.Fatal(err)
					}
					wantSteps = append(wantSteps, "probe")
				case "nonprobing":
					if err != nil || projection.initialized {
						t.Fatalf("API actor acquired workspace initialization: %v", err)
					}
					wantSteps = nil
				case "prerequisite_failure", "partial_failure":
					if !errors.Is(err, errWorkspaceConstruction) || projection.initialized {
						t.Fatalf("initialization failure lost: %v", err)
					}
					wantSteps = wantSteps[:1]
					if mode == "partial_failure" {
						wantSteps = []string{"prerequisites", "scaffold_created"}
					}
				case "released":
					if err == nil || projection.initialized {
						t.Fatal("released selected workspace accepted observation")
					}
					wantSteps = []string{"release"}
				default:
					if !errors.Is(err, context.Canceled) || projection.initialized {
						t.Fatalf("canceled initialization lost: %v", err)
					}
					wantSteps = wantSteps[:3]
					if mode == "cancel_before" {
						wantSteps = nil
					} else if mode == "cancel_after_prerequisites" {
						wantSteps = wantSteps[:1]
					}
				}
				if !reflect.DeepEqual(trace.steps, wantSteps) || source.selected || original.Fingerprint() != originalFingerprint {
					t.Fatalf("selected/source/catalog isolation: steps=%v want=%v", trace.steps, wantSteps)
				}
				if actor.Identity == (agentidentity.Identity{}) {
					t.Fatal("observation replaced the concrete actor with its runless snapshot")
				}
			})
		}
	}
}
