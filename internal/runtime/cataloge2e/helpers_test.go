package cataloge2e

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
	"swarm/internal/config"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
)

func repoRootFromCatalogE2E(t testing.TB) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve cataloge2e file path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func platformSpecPathFromCatalogE2E(t testing.TB) string {
	t.Helper()
	return filepath.Join(repoRootFromCatalogE2E(t), "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml")
}

func loadFixtureBundle(t testing.TB, fixtureRoot string) *runtimecontracts.WorkflowContractBundle {
	t.Helper()
	bundle, err := loadFixtureBundleMaybe(fixtureRoot)
	if err != nil {
		t.Fatalf("load workflow contract bundle %s: %v", fixtureRoot, err)
	}
	return bundle
}

func loadFixtureBundleMaybe(fixtureRoot string) (*runtimecontracts.WorkflowContractBundle, error) {
	repoRoot := repoRootFromCatalogE2ENonFatal()
	return runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, fixtureRoot, filepath.Join(repoRoot, "docs", "specs", "swarm-platform", "platform", "contracts", "platform-spec.yaml"))
}

func repoRootFromCatalogE2ENonFatal() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func loadYAML(t testing.TB, path string, out any) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if err := yaml.Unmarshal(b, out); err != nil {
		t.Fatalf("unmarshal %s: %v", path, err)
	}
}

func newFixtureWorkflowModule(bundle *runtimecontracts.WorkflowContractBundle) (runtimepipeline.WorkflowModule, error) {
	source := semanticview.Wrap(bundle)
	workflow, err := runtimepipeline.LoadWorkflowDefinition(source)
	if err != nil {
		return nil, err
	}
	workflowNodes, err := runtimepipeline.LoadWorkflowNodes(source)
	if err != nil {
		return nil, err
	}
	return &fixtureWorkflowModule{
		source:         source,
		workflow:       workflow,
		workflowNodes:  workflowNodes,
		guardRegistry:  runtimepipeline.NewContractGuardRegistry(source),
		actionRegistry: runtimepipeline.NewContractActionRegistry(source),
	}, nil
}

func testRuntimeConfig() *config.Config {
	return &config.Config{
		Runtime: config.RuntimeConfig{
			MaxConcurrentAgents: 4,
			EventPollInterval:   5 * time.Millisecond,
			RecoveryOnStartup:   false,
		},
		LLM: config.LLMConfig{
			RuntimeMode: "api",
			Session: config.LLMSessionConfig{
				LockTTL:               30 * time.Second,
				RotateAfterTurns:      8,
				RotateOnParseFailures: 2,
			},
			ClaudeAPI: config.ClaudeAPIConfig{
				DefaultModel: "test-model",
				HaikuModel:   "test-haiku",
				MaxRetries:   1,
				RetryBackoff: time.Millisecond,
			},
		},
	}
}

type catalogFixtureStartupPolicy struct {
	FatalBootWarnings bool
	StrictEmitSchemas bool
}

func (p catalogFixtureStartupPolicy) apply(t testing.TB) {
	t.Helper()
	if setter, ok := any(t).(interface{ Setenv(string, string) }); ok {
		if p.FatalBootWarnings {
			setter.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
		} else {
			setter.Setenv("SWARM_BOOT_WARNINGS_FATAL", "false")
		}
		if p.StrictEmitSchemas {
			setter.Setenv("SWARM_EMIT_SCHEMA_STRICT", "true")
		} else {
			setter.Setenv("SWARM_EMIT_SCHEMA_STRICT", "false")
		}
	}
}

func strictCatalogFixtureStartupPolicy() catalogFixtureStartupPolicy {
	return catalogFixtureStartupPolicy{
		FatalBootWarnings: true,
		StrictEmitSchemas: true,
	}
}

func runtimeCatalogHarnessStartupPolicy() catalogFixtureStartupPolicy {
	// Runtime-backed catalog fixtures still exercise post-boot runtime semantics for
	// flows that intentionally carry boot warnings, so only emit-schema strictness
	// stays forced here; "real runtime boot" fixtures use the strict policy instead.
	return catalogFixtureStartupPolicy{
		FatalBootWarnings: false,
		StrictEmitSchemas: true,
	}
}

type fixtureWorkflowModule struct {
	source         semanticview.Source
	workflow       *runtimepipeline.WorkflowDefinition
	workflowNodes  []runtimepipeline.WorkflowNode
	guardRegistry  runtimepipeline.GuardRegistry
	actionRegistry runtimepipeline.ActionRegistry
}

func (m *fixtureWorkflowModule) SemanticSource() semanticview.Source {
	return m.source
}

func (m *fixtureWorkflowModule) WorkflowDefinition() *runtimepipeline.WorkflowDefinition {
	return m.workflow
}

func (m *fixtureWorkflowModule) WorkflowNodes() []runtimepipeline.WorkflowNode {
	out := make([]runtimepipeline.WorkflowNode, 0, len(m.workflowNodes))
	out = append(out, m.workflowNodes...)
	return out
}

func (m *fixtureWorkflowModule) GuardRegistry() runtimepipeline.GuardRegistry {
	return m.guardRegistry
}

func (m *fixtureWorkflowModule) ActionRegistry() runtimepipeline.ActionRegistry {
	return m.actionRegistry
}
