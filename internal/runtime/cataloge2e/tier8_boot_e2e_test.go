package cataloge2e

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	runtime "swarm/internal/runtime"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
)

type tier8ExpectedDocument struct {
	Expected struct {
		BootResult    string `yaml:"boot_result"`
		ErrorCategory string `yaml:"error_category"`
		ErrorContains string `yaml:"error_contains"`
	} `yaml:"expected"`
}

type tier8ExcludedFixture struct {
	kind   string
	reason string
}

var tier8SupportedFixtures = []string{
	"test-boot-advances-to-list",
	"test-boot-bare-condition",
	"test-boot-cel-parse-error",
	"test-boot-condition-payload-mismatch",
	"test-boot-condition-policy",
	"test-boot-deprecated-field",
	"test-boot-dialect-dual",
	"test-boot-dialect-guard",
	"test-boot-emit-mismatch",
	"test-boot-event-cycle",
	"test-boot-event-no-consumer",
	"test-boot-event-no-producer",
	"test-boot-event-no-schema",
	"test-boot-handler-field-undefined",
	"test-boot-missing-pin",
	"test-boot-on-complete-state-invalid",
	"test-boot-on-complete-dict",
	"test-boot-payload-mismatch",
	"test-boot-permission-tool-mismatch",
	"test-boot-policy-conflict",
	"test-boot-prompt-missing",
	"test-boot-prompt-stub",
	"test-boot-produces-drift",
	"test-boot-required-agent-missing",
	"test-boot-self-emit",
	"test-boot-state-machine-invalid",
	"test-boot-success",
	"test-boot-tool-missing",
}

var tier8ExcludedFixtures = map[string]tier8ExcludedFixture{}

func TestTier8BootCatalogFixtures_RealRuntimeBoot(t *testing.T) {
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "false")

	repoRoot := repoRootFromCatalogE2E(t)
	for _, fixtureName := range tier8SupportedFixtures {
		fixtureRoot := filepath.Join(repoRoot, "tests", "tier8-boot-verification", fixtureName)
		t.Run(fixtureName, func(t *testing.T) {
			var expected tier8ExpectedDocument
			loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)

			bundle, loadErr := loadFixtureBundleMaybe(fixtureRoot)
			if strings.EqualFold(strings.TrimSpace(expected.Expected.BootResult), "error") && loadErr != nil {
				assertBootErrorMatches(t, loadErr, expected)
				return
			}
			if loadErr != nil {
				t.Fatalf("load workflow contract bundle %s: %v", fixtureRoot, loadErr)
			}
			source := semanticview.Wrap(bundle)
			warnings, validationErr := runtimepipeline.ValidateWorkflowContractsDetailed(source)

			switch strings.ToLower(strings.TrimSpace(expected.Expected.BootResult)) {
			case "", "success":
				if validationErr != nil {
					t.Fatalf("expected clean boot, got validation error: %v", validationErr)
				}
				if len(warnings) > 0 {
					t.Fatalf("expected clean boot warnings=[], got %#v", warnings)
				}
				rt, err := newTier8Runtime(bundle)
				if err != nil {
					t.Fatalf("NewRuntime: %v", err)
				}
				startRuntimeForBootTest(t, rt)
			case "warning":
				if validationErr != nil {
					t.Fatalf("expected warning boot result, got validation error: %v", validationErr)
				}
				if !warningsContain(warnings, expected.Expected.ErrorCategory, expected.Expected.ErrorContains) {
					t.Fatalf("expected warning %s containing %q, got %#v", expected.Expected.ErrorCategory, expected.Expected.ErrorContains, warnings)
				}
				rt, err := newTier8Runtime(bundle)
				if err != nil {
					t.Fatalf("NewRuntime: %v", err)
				}
				startRuntimeForBootTest(t, rt)
			case "error":
				if validationErr == nil {
					t.Fatal("expected validation error")
				}
				assertBootErrorMatches(t, validationErr, expected)
				if _, err := newTier8Runtime(bundle); err == nil {
					t.Fatal("expected NewRuntime to fail for invalid boot fixture")
				} else {
					assertBootErrorMatches(t, err, expected)
				}
			default:
				t.Fatalf("unsupported expected.boot_result %q", expected.Expected.BootResult)
			}
		})
	}
}

func TestTier8BootCatalogFixtures_AreExplicitlyClassified(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	entries, err := os.ReadDir(filepath.Join(repoRoot, "tests", "tier8-boot-verification"))
	if err != nil {
		t.Fatalf("read tier8 fixture dir: %v", err)
	}
	supported := make(map[string]struct{}, len(tier8SupportedFixtures))
	for _, name := range tier8SupportedFixtures {
		supported[name] = struct{}{}
	}
	found := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := strings.TrimSpace(entry.Name())
		if name == "" {
			continue
		}
		found = append(found, name)
		if _, ok := supported[name]; ok {
			continue
		}
		if _, ok := tier8ExcludedFixtures[name]; ok {
			continue
		}
		t.Fatalf("tier8 fixture %q is neither supported nor classified", name)
	}
	sort.Strings(found)
	expectedCount := len(tier8SupportedFixtures) + len(tier8ExcludedFixtures)
	if len(found) != expectedCount {
		t.Fatalf("tier8 fixture accounting mismatch: found=%d supported=%d excluded=%d", len(found), len(tier8SupportedFixtures), len(tier8ExcludedFixtures))
	}
}

func newTier8Runtime(bundle *runtimecontracts.WorkflowContractBundle) (*runtime.Runtime, error) {
	module, err := newFixtureWorkflowModule(bundle)
	if err != nil {
		return nil, err
	}
	return runtime.NewRuntime(context.Background(), testRuntimeConfig(), runtime.Stores{}, runtime.RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
	})
}

func startRuntimeForBootTest(t testing.TB, rt *runtime.Runtime) {
	t.Helper()
	if err := startRuntimeAndReturnError(rt); err != nil {
		t.Fatalf("runtime boot failed: %v", err)
	}
}

func startRuntimeAndReturnError(rt *runtime.Runtime) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rt.Start(ctx); err != nil {
		return err
	}
	return rt.Shutdown()
}

func warningsContain(warnings []runtimepipeline.WorkflowContractWarning, category, contains string) bool {
	for _, warning := range warnings {
		if strings.TrimSpace(category) != "" && strings.TrimSpace(warning.Category) != strings.TrimSpace(category) {
			continue
		}
		if strings.TrimSpace(contains) != "" && !strings.Contains(warning.Message, strings.TrimSpace(contains)) {
			continue
		}
		return true
	}
	return false
}

func assertBootErrorMatches(t testing.TB, err error, expected tier8ExpectedDocument) {
	t.Helper()
	if err == nil {
		t.Fatal("expected boot error, got nil")
	}
	text := strings.TrimSpace(expected.Expected.ErrorContains)
	if text == "" {
		text = strings.TrimSpace(expected.Expected.ErrorCategory)
	}
	if text == "" {
		return
	}
	errText := err.Error()
	if strings.Contains(errText, text) || strings.Contains(strings.ToLower(errText), strings.ToLower(text)) {
		return
	}
	if bootErrorContainsAllSignificantTokens(errText, text) {
		return
	}
	t.Fatalf("boot error = %q, want substring %q", err.Error(), text)
}

var bootErrorTokenPattern = regexp.MustCompile(`[A-Za-z0-9_.]+`)

func bootErrorContainsAllSignificantTokens(errText, want string) bool {
	lowerErr := strings.ToLower(errText)
	tokens := bootErrorTokenPattern.FindAllString(strings.ToLower(want), -1)
	if len(tokens) == 0 {
		return false
	}
	foundAny := false
	for _, token := range tokens {
		switch token {
		case "and", "both", "the", "a", "an":
			continue
		}
		foundAny = true
		if !strings.Contains(lowerErr, token) {
			return false
		}
	}
	return foundAny
}
