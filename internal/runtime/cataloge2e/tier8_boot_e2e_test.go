package cataloge2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	runtime "swarm/internal/runtime"
	runtimebootverify "swarm/internal/runtime/bootverify"
	runtimecontracts "swarm/internal/runtime/contracts"
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

var tier8ExcludedFixtures = map[string]tier8ExcludedFixture{
	"test-boot-create-entity-plus-accumulate":          {reason: "new conformance fixture not yet wired into runtime catalog execution set"},
	"test-boot-on-complete-and-rules-mutual-exclusion": {reason: "new conformance fixture not yet wired into runtime catalog execution set"},
}

func TestTier8BootCatalogFixtures_RealRuntimeBoot(t *testing.T) {
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
			report := runtimebootverify.Run(context.Background(), semanticview.Wrap(bundle), runtimebootverify.Options{})

			switch strings.ToLower(strings.TrimSpace(expected.Expected.BootResult)) {
			case "", "success":
				if report.HasErrors() {
					t.Fatalf("expected clean boot, got validation errors: %#v", report.Errors())
				}
				if len(report.Warnings()) > 0 {
					t.Fatalf("expected clean boot warnings=[], got %#v", report.Warnings())
				}
				rt, err := newTier8Runtime(t, bundle)
				if err != nil {
					t.Fatalf("NewRuntime: %v", err)
				}
				startRuntimeForBootTest(t, rt)
			case "warning":
				if report.HasErrors() {
					t.Fatalf("expected warning boot result, got validation errors: %#v", report.Errors())
				}
				if !findingsContain(report.Warnings(), expected.Expected.ErrorCategory, expected.Expected.ErrorContains) {
					t.Fatalf("expected warning %s containing %q, got %#v", expected.Expected.ErrorCategory, expected.Expected.ErrorContains, report.Warnings())
				}
				assertTier8RuntimeBootMatchesAuthoritativeStartupTruth(t, bundle, expected)
			case "error":
				if !report.HasErrors() {
					t.Fatal("expected validation error")
				}
				assertBootErrorMatches(t, findingsError(report.Errors()), expected)
				if _, err := newTier8Runtime(t, bundle); err == nil {
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

func newTier8Runtime(t testing.TB, bundle *runtimecontracts.WorkflowContractBundle) (*runtime.Runtime, error) {
	t.Helper()
	strictCatalogFixtureStartupPolicy().apply(t)
	module, err := newFixtureWorkflowModule(bundle)
	if err != nil {
		return nil, err
	}
	return runtime.NewRuntime(context.Background(), testRuntimeConfig(), runtime.Stores{}, runtime.RuntimeOptions{
		SelfCheck:      false,
		WorkflowModule: module,
	})
}

func assertTier8RuntimeBootMatchesAuthoritativeStartupTruth(t testing.TB, bundle *runtimecontracts.WorkflowContractBundle, expected tier8ExpectedDocument) {
	t.Helper()
	strictCatalogFixtureStartupPolicy().apply(t)
	source := semanticview.Wrap(bundle)
	_, validationErr := runtime.ValidateWorkflowContractSurface(context.Background(), source, runtime.DefaultWorkflowContractValidationOptions(nil))
	if validationErr != nil {
		if _, err := newTier8Runtime(t, bundle); err == nil {
			t.Fatal("expected NewRuntime to fail when authoritative startup validation fails")
		} else if !strings.Contains(err.Error(), validationErr.Error()) {
			t.Fatalf("newTier8Runtime error = %q, want authoritative validation error substring %q", err.Error(), validationErr.Error())
		}
		return
	}
	rt, err := newTier8Runtime(t, bundle)
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	startRuntimeForBootTest(t, rt)
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

func findingsContain(findings []runtimebootverify.Finding, category, contains string) bool {
	wantCheckID := legacyCategoryToCheckID(category)
	for _, finding := range findings {
		if wantCheckID != "" && strings.TrimSpace(finding.CheckID) != wantCheckID {
			continue
		}
		if strings.TrimSpace(contains) != "" && !strings.Contains(finding.Message, strings.TrimSpace(contains)) {
			continue
		}
		return true
	}
	return false
}

func findingsError(findings []runtimebootverify.Finding) error {
	if len(findings) == 0 {
		return nil
	}
	lines := make([]string, 0, len(findings))
	for _, finding := range findings {
		lines = append(lines, finding.Message)
	}
	return fmt.Errorf(strings.Join(lines, "\n"))
}

func legacyCategoryToCheckID(raw string) string {
	switch strings.TrimSpace(strings.ToUpper(raw)) {
	case "TOOL-MISSING":
		return "tool_resolution"
	case "PROMPT-MISSING", "PROMPT-STUB":
		return "prompt_exists"
	case "POLICY-CONFLICT":
		return "policy_conflict_detection"
	case "EVENT-NO-CONSUMER":
		return "event_consumer_exists"
	case "EVENT-NO-PRODUCER":
		return "event_producer_exists"
	case "EVENT-NO-SCHEMA":
		return "event_chain_integrity"
	case "PERMISSION-MISMATCH":
		return "agent_permission_validation"
	default:
		return ""
	}
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
