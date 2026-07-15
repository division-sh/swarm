package cataloge2e

import (
	"context"
	"strings"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const staticMultiEntityRetirementDiagnostic = "static multi-row entity ownership is retired"

func assertCatalogStaticMultiEntityRetirement(t testing.TB, fixtureRoot string) {
	t.Helper()

	bundle, err := loadFixtureBundleMaybe(fixtureRoot)
	if err != nil {
		t.Fatalf("load workflow contract bundle %s: %v", fixtureRoot, err)
	}
	report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), runtimebootverify.Options{})
	if !findingsContain(report.Errors(), "flow_boundary_create_entity_validation", staticMultiEntityRetirementDiagnostic) &&
		!findingsContain(report.Errors(), "select_entity_validation", staticMultiEntityRetirementDiagnostic) {
		t.Fatalf("expected retired static multi-entity validation error, got %#v", report.Errors())
	}
	if _, err := newTier8Runtime(t, bundle); err == nil {
		t.Fatal("expected runtime startup to fail for retired static multi-entity fixture")
	} else if !strings.Contains(err.Error(), staticMultiEntityRetirementDiagnostic) {
		t.Fatalf("runtime startup error = %q, want %q", err.Error(), staticMultiEntityRetirementDiagnostic)
	}
}
