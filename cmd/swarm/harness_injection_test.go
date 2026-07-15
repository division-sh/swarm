package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestVerifyHarnessInjectionLabelsNonProductionBundle(t *testing.T) {
	root := canonicalrouting.ExampleRoot(t, canonicalrouting.HarnessInjection)
	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithContractsOutputForTest(t, context.Background(), repoRoot(), root, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("verify exit = %d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	want := " -- 1 harness-injected input; not production-valid"
	if !strings.Contains(stdout.String(), want) {
		t.Fatalf("verify stdout missing %q:\n%s", want, stdout.String())
	}
	readme, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(readme), want) {
		t.Fatalf("canonical README does not capture verify label:\n%s", readme)
	}
}

func TestVerifyHarnessInjectionJSONMarksNonProductionBundle(t *testing.T) {
	opts := defaultVerifyCommandOptions()
	opts.contractsPath = canonicalrouting.ExampleRoot(t, canonicalrouting.HarnessInjection)
	opts.configPath = writeTestVerifyRuntimeConfig(t)
	opts.output.asJSON = true
	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithOutput(context.Background(), repoRoot(), opts, &stdout, &stderr)
	if code != 0 || strings.TrimSpace(stderr.String()) != "" {
		t.Fatalf("verify JSON exit = %d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	var output verifyCommandResult
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		t.Fatalf("decode verify JSON: %v\n%s", err, stdout.String())
	}
	if !output.OK || output.HarnessInjectedInputs != 1 || output.ProductionValid {
		t.Fatalf("verify JSON = %#v, want ok validation-only result", output)
	}
}

func TestVerifyHarnessInjectionMissingDeclarationRestoresProducerFailure(t *testing.T) {
	root := canonicalrouting.CopyHarnessInjectionWithoutSource(t)
	var stdout, stderr bytes.Buffer
	code := runVerifyCommandWithContractsOutputForTest(t, context.Background(), repoRoot(), root, &stdout, &stderr)
	want := "[BLOCKER] input_pin_wiring @ worker: Flow worker declares input pin event work.requested but no accepted producer source was found in the authored bundle. Expected a producer proof for input pin target worker.work_requested."
	if code == 0 || !strings.Contains(stderr.String(), want) {
		t.Fatalf("verify missing-source exit = %d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	readme, err := os.ReadFile(filepath.Join(canonicalrouting.ExampleRoot(t, canonicalrouting.HarnessInjection), "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(readme), want) {
		t.Fatalf("canonical README does not capture missing-source output:\n%s", readme)
	}
}

func TestServeRejectsHarnessInjectionBeforeRuntime(t *testing.T) {
	repo := repoRoot()
	root := canonicalrouting.ExampleRoot(t, canonicalrouting.HarnessInjection)
	loaded, err := loadServeRuntimeBundle(context.Background(), repo, storeBundle{}, cliContractPlatformSpecPaths{
		ContractsPath: root, PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repo),
	}, serveOptions{})
	if err != nil {
		t.Fatalf("loadServeRuntimeBundle: %v", err)
	}
	loaded.bundleSourceFact = runtimecorrelation.BundleSourceFact{BundleHash: loaded.bootIdentity.Fingerprint}
	contextDef, err := buildServeRuntimeBundleContext(serveRuntimeBundleContextRequest{
		Ctx: context.Background(), Loaded: loaded, StateStoreSummary: "test stores ready",
		WorkspaceBackend: workspaceBackendSelection{Backend: workspaceBackendNone, NoWorkspace: true, Source: "test"},
		BootStartedAt:    time.Now().UTC(),
	})
	if err == nil || !strings.Contains(err.Error(), "production validation rejects test-only input source: harness") {
		t.Fatalf("buildServeRuntimeBundleContext = %#v error=%v, want production rejection", contextDef, err)
	}
	if contextDef.runtime != nil {
		t.Fatal("serve materialized a runtime for harness input")
	}
}

func TestVerifyValidationPolicyIsTheOnlyHarnessOptIn(t *testing.T) {
	repo := repoRoot()
	source := loadHarnessSourceForCommandTest(t)
	opts, err := verifyWorkflowContractValidationOptions(repo, writeTestVerifyRuntimeConfig(t), source)
	if err != nil {
		t.Fatalf("verifyWorkflowContractValidationOptions: %v", err)
	}
	if !opts.AllowHarnessInputs {
		t.Fatal("verify policy did not opt into validation-only harness acceptance")
	}
	if runtimepkg.DefaultWorkflowContractValidationOptions(nil).AllowHarnessInputs {
		t.Fatal("default production policy inherited verify-only harness acceptance")
	}
}

func loadHarnessSourceForCommandTest(t *testing.T) semanticview.Source {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	_, bundle, err := newSwarmWorkflowModule(repo, canonicalrouting.ExampleRoot(t, canonicalrouting.HarnessInjection), runtimecontracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatalf("load harness source: %v", err)
	}
	return semanticview.Wrap(bundle)
}
