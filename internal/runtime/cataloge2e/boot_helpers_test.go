package cataloge2e

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	runtimebootverify "swarm/internal/runtime/bootverify"
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
	report := runtimebootverify.Run(context.Background(), semanticview.Wrap(bundle), runtimebootverify.Options{})

	switch strings.ToLower(strings.TrimSpace(expected.Expected.BootResult)) {
	case "", "success":
		if report.HasErrors() {
			t.Fatalf("expected clean boot, got validation errors: %#v", report.Errors())
		}
		if len(report.Warnings()) > 0 {
			t.Fatalf("expected clean boot warnings=[], got %#v", report.Warnings())
		}
		rt, err := newTier8Runtime(bundle)
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
		rt, err := newTier8Runtime(bundle)
		if err != nil {
			t.Fatalf("NewRuntime: %v", err)
		}
		startRuntimeForBootTest(t, rt)
	case "error":
		if !report.HasErrors() {
			t.Fatal("expected validation error")
		}
		assertBootErrorMatches(t, findingsError(report.Errors()), expected)
		if _, err := newTier8Runtime(bundle); err == nil {
			t.Fatal("expected NewRuntime to fail for invalid boot fixture")
		} else {
			assertBootErrorMatches(t, err, expected)
		}
	default:
		t.Fatalf("unsupported expected.boot_result %q", expected.Expected.BootResult)
	}
}
