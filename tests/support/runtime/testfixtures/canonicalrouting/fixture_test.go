package canonicalrouting

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/packadmission"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthority "github.com/division-sh/swarm/internal/runtime/authority"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	models "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/tests/support/channelfixture"
)

func canonicalExampleNames() []ArtifactID {
	return []ArtifactID{
		RootIngress,
		ParentConnect,
		TemplateSelectExisting,
		TemplateSelectOrCreate,
		TemplateReply,
		TemplateCreateMintedKey,
		FanInStream,
		FanInBarrier,
		MapScatterGather,
		PolicyRules,
	}
}

func TestCanonicalPositiveFixtureOwnerSetIsClosed(t *testing.T) {
	for _, id := range canonicalExampleNames() {
		if _, ok := canonicalExamplePath(id); !ok {
			t.Fatalf("canonical artifact %q was rejected", id)
		}
	}
	for _, id := range []ArtifactID{
		"notify-all-children",
		"examples/routing/notify-all-children",
		"tests/tier7-composition/test-full-lifecycle",
	} {
		if root, ok := canonicalExamplePath(id); ok {
			t.Fatalf("non-canonical artifact %q resolved to positive owner %q", id, root)
		}
	}
}

func TestNumericFeedFixtureLoadsAndVerifies(t *testing.T) {
	const root = "tests/tier11-flow-composition/test-numeric-data-scatter-park"
	Prove(t, ArtifactID("tests/tier11-flow-composition/test-numeric-data-scatter-park"))
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(RepoRoot(t), filepath.Join(RepoRoot(t), root), runtimecontracts.DefaultPlatformSpecFile(RepoRoot(t)))
	if err != nil {
		t.Fatal(err)
	}
	if findings := runtimebootverify.Run(context.Background(), semanticview.Wrap(bundle), runtimebootverify.Options{}).HardInvalidities(); len(findings) != 0 {
		t.Fatalf("numeric fixture hard invalidities: %#v", findings)
	}
}

func TestFiniteSelectedDeploymentReceiverFixtureLoadsAndVerifies(t *testing.T) {
	root := CopySelectedDeploymentResourceFiniteSingleton(t)
	repo := RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	if findings := runtimebootverify.Run(context.Background(), semanticview.Wrap(bundle), runtimebootverify.Options{}).HardInvalidities(); len(findings) != 0 {
		t.Fatalf("finite selected deployment receiver hard invalidities: %#v", findings)
	}
}

func TestForkFanOutReceiverFixtureLoadsAndVerifies(t *testing.T) {
	root := CopyForkFanOutReceiverOwnership(t)
	repo := RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	source := semanticview.Wrap(bundle)
	if plans := source.FanOutPlans(); len(plans) != 1 || plans[0].ItemsFrom != "payload.tokens" {
		t.Fatalf("receiver fixture requires one real compiled handler source: %+v", plans)
	}
	if report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{}); len(report.Errors()) != 0 || len(report.Warnings()) != 0 {
		t.Fatalf("fan-out receiver fixture boot errors=%+v warnings=%+v", report.Errors(), report.Warnings())
	}
}

func TestCanonicalRoutingExamplesLoadAndVerify(t *testing.T) {
	Prove(t, RootIngress, ParentConnect, TemplateSelectExisting, TemplateSelectOrCreate, TemplateReply, TemplateCreateMintedKey, FanInStream, FanInBarrier, MapScatterGather, ArtifactID("examples/routing/policy-rules"))
	for _, name := range canonicalExampleNames() {
		t.Run(string(name), func(t *testing.T) {
			root := ExampleRoot(t, name)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
				RepoRoot(t),
				root,
				runtimecontracts.DefaultPlatformSpecFile(RepoRoot(t)),
			)
			if err != nil {
				t.Fatalf("load canonical example: %v", err)
			}
			report := runtimebootverify.Run(context.Background(), semanticview.Wrap(bundle), runtimebootverify.Options{})
			if findings := report.HardInvalidities(); len(findings) != 0 {
				t.Fatalf("hard invalidities: %#v", findings)
			}
		})
	}
}

func TestPublicationSitesLoadAndVerify(t *testing.T) {
	for _, mode := range []string{"root", "static", "template"} {
		t.Run(mode, func(t *testing.T) {
			repo := RepoRoot(t)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, CopyPublicationSites(t, mode), runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			if findings := runtimebootverify.Run(context.Background(), semanticview.Wrap(bundle), runtimebootverify.Options{}).HardInvalidities(); len(findings) != 0 {
				t.Fatalf("hard invalidities: %#v", findings)
			}
		})
	}
}

func TestClockDeploymentFixturesLoadAndVerify(t *testing.T) {
	Prove(t, ArtifactID("tests/support/runtime/testfixtures/canonicalrouting/testdata/clock-deployment"))
	for _, variant := range []string{"nested", "finite"} {
		t.Run(variant, func(t *testing.T) {
			repo := RepoRoot(t)
			root := filepath.Join(repo, "tests/support/runtime/testfixtures/canonicalrouting/testdata/clock-deployment", variant)
			bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, root, runtimecontracts.DefaultPlatformSpecFile(repo))
			if err != nil {
				t.Fatal(err)
			}
			if findings := runtimebootverify.Run(context.Background(), semanticview.Wrap(bundle), runtimebootverify.Options{}).HardInvalidities(); len(findings) != 0 {
				t.Fatalf("clock deployment hard invalidities: %#v", findings)
			}
		})
	}
}

func TestNodeIdentityFixtureLoadsAndVerifies(t *testing.T) {
	Prove(t, ArtifactID("internal/releasee2e/testdata/node_identity"))
	repo := RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(repo, "internal/releasee2e/testdata/node_identity"), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	report := runtimebootverify.Run(context.Background(), semanticview.Wrap(bundle), runtimebootverify.Options{})
	if findings := report.HardInvalidities(); len(findings) != 0 {
		t.Fatalf("hard invalidities: %#v", findings)
	}
}

func TestStaticDataInvocationFixtureLoadsAndVerifies(t *testing.T) {
	Prove(t, ArtifactID("internal/releasee2e/testdata/static_data_invocation"))
	repo := RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(repo, "internal/releasee2e/testdata/static_data_invocation"), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	report := runtimebootverify.Run(context.Background(), semanticview.Wrap(bundle), runtimebootverify.Options{})
	if findings := report.HardInvalidities(); len(findings) != 0 {
		t.Fatalf("hard invalidities: %#v", findings)
	}
}

func TestReleaseE2EClaudeLifecycleFixtureLoadsAndVerifies(t *testing.T) {
	const fixture = ArtifactID("internal/releasee2e/testdata/claude_cli_managed_lifecycle")
	Prove(t, ArtifactID("internal/releasee2e/testdata/claude_cli_managed_lifecycle"))
	repo := RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repo,
		filepath.Join(repo, filepath.FromSlash(string(fixture))),
		runtimecontracts.DefaultPlatformSpecFile(repo),
	)
	if err != nil {
		t.Fatalf("load release E2E Claude lifecycle fixture: %v", err)
	}
	source := semanticview.Wrap(bundle)
	report := runtimebootverify.Run(context.Background(), source, runtimebootverify.Options{})
	if findings := report.HardInvalidities(); len(findings) != 0 {
		t.Fatalf("release E2E Claude lifecycle fixture hard invalidities: %#v", findings)
	}
	registry := runtimetools.NewEmitRegistry(source, runtimeauthority.NewSourceProvider(source))
	actorDefinitions := registry.GenerateEmitToolsForActor(models.AgentConfig{
		ID:         "release-worker",
		Role:       "release-worker",
		FlowID:     "worker",
		FlowPath:   "worker",
		EmitEvents: []string{"worker/agent.completed"},
	}, nil)
	if len(actorDefinitions) != 1 {
		t.Fatalf("release E2E actor definitions = %#v, want one flow-scoped emit definition", actorDefinitions)
	}
	if definitions := registry.GenerateEmitToolsForActor(models.AgentConfig{
		ID: "undeclared", Role: "release-worker", FlowID: "worker", FlowPath: "worker",
		EmitEvents: []string{"worker/agent.completed"},
	}, nil); len(definitions) != 0 {
		t.Fatalf("release E2E role/global authority generated tools without the exact declaration: %#v", definitions)
	}
}

func TestReleaseE2EClaudeResourceReadFixtureLoadsAndVerifies(t *testing.T) {
	const fixture = ArtifactID("internal/releasee2e/testdata/claude_resource_read")
	Prove(t, ArtifactID("internal/releasee2e/testdata/claude_resource_read"))
	repo := RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repo,
		filepath.Join(repo, filepath.FromSlash(string(fixture))),
		runtimecontracts.DefaultPlatformSpecFile(repo),
	)
	if err != nil {
		t.Fatalf("load release E2E Claude resource fixture: %v", err)
	}
	report := runtimebootverify.Run(context.Background(), semanticview.Wrap(bundle), runtimebootverify.Options{})
	if findings := report.HardInvalidities(); len(findings) != 0 {
		t.Fatalf("release E2E Claude resource fixture hard invalidities: %#v", findings)
	}
}

func TestReleaseE2EChannelOnboardingFixtureIsRegistered(t *testing.T) {
	Prove(t, ArtifactID("internal/releasee2e/testdata/channel_onboarding_release"))
}

func TestReleaseE2EGoldenAgentWorkloadFixtureLoadsAndVerifies(t *testing.T) {
	Prove(t, ArtifactID("internal/releasee2e/testdata/golden_agent_workload"))
}

func TestWorkspaceMCPTransportFixtureLoadsAndVerifies(t *testing.T) {
	Prove(t, ArtifactID("internal/releasee2e/testdata/workspace_mcp"))
	repo := RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repo, filepath.Join(repo, "internal/releasee2e/testdata/workspace_mcp"), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	report := runtimebootverify.Run(context.Background(), semanticview.Wrap(bundle), runtimebootverify.Options{})
	if findings := report.HardInvalidities(); len(findings) != 0 {
		t.Fatalf("hard invalidities: %#v", findings)
	}
}

func TestReleaseE2EFullLifecycleFixtureLoadsAndVerifies(t *testing.T) {
	const fixture = ArtifactID("internal/releasee2e/testdata/full_lifecycle/standing_telegram")
	Prove(t, ArtifactID("internal/releasee2e/testdata/full_lifecycle/standing_telegram"))
	repo := RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOptions(
		repo,
		filepath.Join(repo, filepath.FromSlash(string(fixture))),
		runtimecontracts.DefaultPlatformSpecFile(repo),
		runtimecontracts.WorkflowContractLoadOptions{AdmitPackInventory: packadmission.AdmitInventory},
	)
	if err != nil {
		t.Fatalf("load release E2E full lifecycle fixture: %v", err)
	}
	admitted, err := packadmission.FromBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	bundle.PackAdmission = admitted.WithProviderFacts(channelfixture.For("telegram"))
	bundleHash, err := runtimecontracts.BundleHash(bundle)
	if err != nil {
		t.Fatalf("hash release E2E full lifecycle fixture: %v", err)
	}
	sourceFact, err := runtimecorrelation.NewSourceArtifactFact(bundleHash)
	if err != nil {
		t.Fatalf("create release E2E full lifecycle source fact: %v", err)
	}
	projection, err := runtimepkg.AdmitEffectiveSourceProjection(runtimepkg.EffectiveSourceProjectionRequest{
		Source: semanticview.Wrap(bundle), SourceArtifactFact: sourceFact,
	})
	if err != nil {
		t.Fatalf("admit release E2E full lifecycle effective source: %v", err)
	}
	report := runtimebootverify.Run(context.Background(), projection.Source(), runtimebootverify.Options{Purpose: runtimebootverify.StructuralValidation})
	if findings := report.HardInvalidities(); len(findings) != 0 {
		t.Fatalf("release E2E full lifecycle fixture hard invalidities: %#v", findings)
	}
	if report.AdmissionDecision(runtimebootverify.AdmissionFindingPolicy{}).Complete {
		t.Fatal("source-only fixture verification claimed complete deployment admission")
	}
	execution := runtimebootverify.Run(context.Background(), projection.Source(), runtimebootverify.Options{})
	for _, finding := range execution.HardInvalidities() {
		if finding.CheckID == "credential_key_exists" && finding.FailureClass == runtimefailures.ClassDependencyUnavailable {
			return
		}
	}
	t.Fatalf("execution-purpose fixture verification hid the missing credential reader: %#v", execution)
}

func TestSelectedForkFlowScopedMCPFixtureLoadsAndVerifies(t *testing.T) {
	const fixture = ArtifactID("internal/runtime/runforkexecution/testdata/selected_fork_flow_scoped_mcp")
	Prove(t, ArtifactID("internal/runtime/runforkexecution/testdata/selected_fork_flow_scoped_mcp"))
	repo := RepoRoot(t)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(
		repo,
		filepath.Join(repo, filepath.FromSlash(string(fixture))),
		runtimecontracts.DefaultPlatformSpecFile(repo),
	)
	if err != nil {
		t.Fatalf("load selected-fork flow-scoped MCP fixture: %v", err)
	}
	report := runtimebootverify.Run(context.Background(), semanticview.Wrap(bundle), runtimebootverify.Options{})
	if findings := report.HardInvalidities(); len(findings) != 0 {
		t.Fatalf("selected-fork flow-scoped MCP fixture hard invalidities: %#v", findings)
	}
}
