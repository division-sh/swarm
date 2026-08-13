package cataloge2e

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"

	runtime "github.com/division-sh/swarm/internal/runtime"
	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testcatalog"
)

func TestTier8BootCatalogFixtures_RealRuntimeBoot(t *testing.T) {
	canonicalrouting.Prove(t,
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-advances-to-list"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-bare-condition"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-cel-parse-error"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-condition-payload-empty-schema-mismatch"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-condition-payload-empty-schema-rule-list-mismatch"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-condition-payload-mismatch"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-condition-policy"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-create-entity-plus-accumulate"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-deprecated-field"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-dialect-dual"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-dialect-guard"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-emit-mismatch"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-event-cycle"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-event-no-consumer"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-event-no-producer"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-event-no-schema"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-handler-field-undefined"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-missing-pin"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-on-complete-and-rules-mutual-exclusion"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-on-complete-dict"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-on-complete-state-invalid"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-payload-empty-schema-mismatch"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-payload-mismatch"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-permission-tool-mismatch"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-policy-conflict"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-produces-drift"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-prompt-missing"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-prompt-ref"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-prompt-ref-stub"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-prompt-stub"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-required-agent-missing"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-self-emit"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-state-machine-invalid"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-state-machine-unreachable"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-success"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-boot-tool-missing"),
		canonicalrouting.ArtifactID("tests/tier8-boot-verification/test-platform-mailbox-event-subscription"),
	)
	fixtures := catalogInventory(t).Select("catalog.verify.boot_diagnostics", testcatalog.DispositionVerifyOnly)
	if len(fixtures) != 35 {
		t.Fatalf("boot diagnostic fixtures = %d, want 35", len(fixtures))
	}
	for _, fixture := range fixtures {
		fixtureName, fixtureRoot := fixture.Name, fixture.Root
		t.Run(fixtureName, func(t *testing.T) {
			bundle, loadErr := loadFixtureBundleMaybe(fixtureRoot)
			if loadErr != nil {
				if fixture.Metadata.Verify != testcatalog.VerifyReject {
					t.Fatalf("load workflow contract bundle %s: %v", fixtureRoot, loadErr)
				}
				assertCatalogLoadDiagnostic(t, loadErr, fixture.Metadata)
				return
			}
			report := runtimebootverify.Run(testAuthorActivityContext(context.Background()), semanticview.Wrap(bundle), runtimebootverify.Options{})

			switch fixture.Metadata.Verify {
			case testcatalog.VerifyPass:
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
			case testcatalog.VerifyWarning:
				if report.HasErrors() {
					t.Fatalf("expected warning boot result, got validation errors: %#v", report.Errors())
				}
				assertCatalogFinding(t, report.Warnings(), fixture.Metadata)
				assertTier8RuntimeBootMatchesAuthoritativeStartupTruth(t, bundle)
			case testcatalog.VerifyReject:
				if !report.HasErrors() {
					t.Fatal("expected validation error")
				}
				assertCatalogFinding(t, report.Errors(), fixture.Metadata)
				if _, err := newTier8Runtime(t, bundle); err == nil {
					t.Fatal("expected NewRuntime to fail for invalid boot fixture")
				}
			default:
				t.Fatalf("unsupported conformance verify result %q", fixture.Metadata.Verify)
			}
		})
	}
}

func newTier8Runtime(t testing.TB, bundle *runtimecontracts.WorkflowContractBundle) (*runtime.Runtime, error) {
	t.Helper()
	strictCatalogFixtureStartupPolicy().apply(t)
	module, err := newFixtureWorkflowModule(bundle)
	if err != nil {
		return nil, err
	}
	processOwner := worklifetime.NewProcess()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := processOwner.Join(ctx); err != nil {
			t.Errorf("join tier-8 runtime process owner: %v", err)
		}
	})
	selected := storetest.StartSQLiteRuntimeStore(t)
	cfg := testRuntimeConfig()
	deps := catalogSQLiteRuntimeDeps(
		cfg,
		selected,
		runtimepipeline.NewWorkflowPersistence(selected),
		module,
		newScriptedLLMRuntime(),
		processOwner,
	)
	deps.Options.LLMRuntime = runtimellm.NewNoopRuntime(runtimellm.AnthropicAPIProviderContract())
	deps.Options.ProviderCredentials = tier8ProviderCredentialStore(t, "ANTHROPIC_API_KEY", "test-key")
	rt, err := runtime.NewRuntime(testAuthorActivityContext(context.Background()), deps)
	if err != nil {
		return nil, err
	}
	installCatalogRuntimeStartupGrant(t, testAuthorActivityContext(context.Background()), selected, rt)
	return rt, nil

}

func tier8ProviderCredentialStore(t testing.TB, key, value string) runtimecredentials.Store {
	t.Helper()
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "provider-credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := store.Set(testAuthorActivityContext(context.Background()), key, value); err != nil {
		t.Fatalf("Set provider credential: %v", err)
	}
	return store
}

func assertTier8RuntimeBootMatchesAuthoritativeStartupTruth(t testing.TB, bundle *runtimecontracts.WorkflowContractBundle) {
	t.Helper()
	strictCatalogFixtureStartupPolicy().apply(t)
	source := semanticview.Wrap(bundle)
	_, validationErr := runtime.ValidateWorkflowContractSurface(testAuthorActivityContext(context.Background()), source, runtime.DefaultWorkflowContractValidationOptions(nil, executionposture.Live))
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
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(context.Background()), 2*time.Second)
	defer cancel()
	if err := rt.Start(ctx); err != nil {
		return err
	}
	return rt.Shutdown()
}

func assertCatalogFinding(t testing.TB, findings []runtimebootverify.Finding, metadata testcatalog.Metadata) {
	t.Helper()
	want := metadata.Diagnostic
	if want == nil {
		t.Fatal("catalog diagnostic metadata is required")
	}
	for _, finding := range findings {
		if strings.TrimSpace(finding.CheckID) != strings.TrimSpace(want.Category) {
			continue
		}
		text := strings.Join(append([]string{finding.Message, finding.Remediation}, finding.Evidence...), "\n")
		if !strings.Contains(text, strings.TrimSpace(want.Contains)) {
			continue
		}
		return
	}
	t.Fatalf("expected diagnostic %s containing %q, got %#v", want.Category, want.Contains, findings)
}

func assertCatalogLoadDiagnostic(t testing.TB, err error, metadata testcatalog.Metadata) {
	t.Helper()
	if err == nil {
		t.Fatal("expected contract load error, got nil")
	}
	want := metadata.Diagnostic
	if want == nil {
		t.Fatal("catalog diagnostic metadata is required")
	}
	errText := err.Error()
	categoryMatched := strings.Contains(errText, strings.TrimSpace(want.Category))
	if diagnostic, ok := runtimecontracts.AsLoaderDiagnostic(err); ok {
		categoryMatched = diagnostic.Code == strings.TrimSpace(want.Category)
	}
	if !categoryMatched || !strings.Contains(errText, strings.TrimSpace(want.Contains)) {
		t.Fatalf("contract load error = %q, want category %q and teaching evidence %q", errText, want.Category, want.Contains)
	}
}
