package cataloge2e

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	runtime "github.com/division-sh/swarm/internal/runtime"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
		strictCatalogFixtureStartupPolicy().apply(t)
		_, validationErr := runtime.ValidateWorkflowContractSurface(context.Background(), semanticview.Wrap(bundle), runtime.DefaultWorkflowContractValidationOptions(nil))
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
}
