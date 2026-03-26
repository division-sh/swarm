package cataloge2e

import (
	"path/filepath"
	"strings"
	"testing"

	runtimepipeline "swarm/internal/runtime/pipeline"
	"swarm/internal/runtime/semanticview"
)

func runBootCatalogFixture(t *testing.T, fixtureRoot string) {
	t.Helper()

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
}
