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

func TestCatalogFixtureStartupPolicies_AreExplicit(t *testing.T) {
	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "false")
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "false")

	strictCatalogFixtureStartupPolicy().apply(t)

	if got := runtime.DefaultWorkflowContractValidationOptions(nil).FatalBootWarnings; !got {
		t.Fatal("FatalBootWarnings = false, want true")
	}
	if got := runtime.DefaultWorkflowContractValidationOptions(nil).StrictEmitSchemas; !got {
		t.Fatal("StrictEmitSchemas = false, want true")
	}

	t.Setenv("SWARM_BOOT_WARNINGS_FATAL", "true")
	t.Setenv("SWARM_EMIT_SCHEMA_STRICT", "false")

	runtimeCatalogHarnessStartupPolicy().apply(t)

	if got := runtime.DefaultWorkflowContractValidationOptions(nil).FatalBootWarnings; got {
		t.Fatal("FatalBootWarnings = true, want false for runtime-backed catalog harness")
	}
	if got := runtime.DefaultWorkflowContractValidationOptions(nil).StrictEmitSchemas; !got {
		t.Fatal("StrictEmitSchemas = false, want true for runtime-backed catalog harness")
	}
}

func TestTier8RuntimeBootMatchesAuthoritativeStartupTruthForWarningFixtures(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	cases := []struct {
		name              string
		fixtureDir        string
		wantValidationErr bool
		wantBootWarnings  bool
	}{
		{
			name:              "event-no-schema warning follows authoritative startup failure",
			fixtureDir:        filepath.Join("tests", "tier8-boot-verification", "test-boot-event-no-schema"),
			wantValidationErr: true,
			wantBootWarnings:  true,
		},
		{
			name:              "tool-missing warning fixture no longer hides authoritative startup failure",
			fixtureDir:        filepath.Join("tests", "tier8-boot-verification", "test-boot-tool-missing"),
			wantValidationErr: true,
			wantBootWarnings:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			strictCatalogFixtureStartupPolicy().apply(t)

			fixtureRoot := filepath.Join(repoRoot, tc.fixtureDir)
			bundle, err := loadFixtureBundleMaybe(fixtureRoot)
			if err != nil {
				t.Fatalf("loadFixtureBundleMaybe(%s): %v", fixtureRoot, err)
			}
			source := semanticview.Wrap(bundle)

			report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), source, runtimebootverify.Options{})
			if got := len(report.Warnings()) > 0; got != tc.wantBootWarnings {
				t.Fatalf("boot warnings present = %v, want %v", got, tc.wantBootWarnings)
			}

			_, validationErr := runtime.ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), source, runtime.DefaultWorkflowContractValidationOptions(nil))
			if got := validationErr != nil; got != tc.wantValidationErr {
				t.Fatalf("ValidateWorkflowContractSurface error = %v, want error=%v", validationErr, tc.wantValidationErr)
			}

			rt, err := newTier8Runtime(t, bundle)
			if validationErr != nil {
				if err == nil {
					t.Fatal("newTier8Runtime unexpectedly succeeded under authoritative startup failure")
				}
				if got := err.Error(); !strings.Contains(got, validationErr.Error()) {
					t.Fatalf("newTier8Runtime error = %q, want authoritative validation error substring %q", got, validationErr.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("newTier8Runtime error = %v, want nil", err)
			}
			if err := startRuntimeAndReturnError(rt); err != nil {
				t.Fatalf("startRuntimeAndReturnError: %v", err)
			}
		})
	}
}
