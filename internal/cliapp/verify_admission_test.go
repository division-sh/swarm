package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	runtimebootverify "github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestVerifyAdmissionRetainsFrozenConfigThroughSourceLoading(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	configPath := writeTestVerifyRuntimeConfig(t)
	configResult, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: RepoRoot(), ExplicitPath: configPath})
	if err != nil {
		t.Fatal(err)
	}
	// A second read would now fail: every downstream owner must use the admitted
	// value, not reload configuration during path/source/pack/model projection.
	if err := os.Remove(configPath); err != nil {
		t.Fatal(err)
	}
	_, bundle, paths, err := loadCLIWorkflowModuleWithRuntimeConfig(RepoRoot(), CLISourcePlatformSpecPathOptions{SourceRoot: root, ConfigPath: configPath}, configResult)
	if err != nil {
		t.Fatal(err)
	}
	if paths.SourceRoot != root {
		t.Fatalf("source path changed: %#v", paths)
	}
	source, opts, err := admitStructuralSource(configResult, bundle)
	if err != nil {
		t.Fatal(err)
	}
	result, err := verifyBundleResultWithOptions(context.Background(), source, opts)
	if err != nil || result.BootReport.SourceArtifactHash == "" {
		t.Fatalf("frozen admission failed: err=%v hash=%q", err, result.BootReport.SourceArtifactHash)
	}
	for _, observation := range result.BootReport.Observations {
		if observation.Subject != "source:"+result.BootReport.SourceArtifactHash {
			t.Fatalf("observation lost exact admitted source: %#v", observation)
		}
	}
	if _, err := verifyWorkflowContractValidationOptions(RuntimeConfigLoadResult{}, source); err == nil {
		t.Fatal("missing configuration was silently replaced with defaults")
	}
}

func TestVerifyPortableAdmissionOutputParity(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	t.Setenv("SWARM_CREDENTIALS_FILE", t.TempDir())
	t.Setenv("SWARM_MANAGED_CREDENTIALS_FILE", t.TempDir())
	var jsonOut, jsonErr bytes.Buffer
	if code := executeRootCommand(context.Background(), RepoRoot(), []string{"verify", root, "--json"}, &jsonOut, &jsonErr); code != 0 {
		t.Fatalf("JSON exit=%d stderr=%s", code, jsonErr.String())
	}
	var result verifyCommandResult
	if err := json.Unmarshal(jsonOut.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !result.OK || result.AdmissionComplete || result.ValidationScope != "structural" || result.LiveReadiness != "not_evaluated" || result.SourceArtifactHash == "" {
		t.Fatalf("portable result made a readiness claim: %#v", result)
	}
	if len(result.Observations) == 0 || result.ExecutionObligations == nil {
		t.Fatal("JSON omitted evidence or conflated execution with an absent field")
	}
	for _, mode := range []string{"text", "quiet"} {
		t.Run(mode, func(t *testing.T) {
			args := []string{"verify", root}
			if mode == "quiet" {
				args = append(args, "--quiet")
			}
			var out, errOut bytes.Buffer
			if code := executeRootCommand(context.Background(), RepoRoot(), args, &out, &errOut); code != 0 {
				t.Fatalf("exit=%d stderr=%s", code, errOut.String())
			}
			text := out.String()
			if !strings.Contains(text, "portable structural checks passed; deployment admission not evaluated") || strings.TrimSpace(text) == "ok" {
				t.Fatalf("unchecked readiness hidden: %q", text)
			}
			for _, observation := range result.Observations {
				if observation.Status == runtimebootverify.AdmissionNotRun && !strings.Contains(text, "not_run: "+observation.CheckID+" @ "+observation.Subject) {
					t.Fatalf("%s erased unperformed check %s: %s", mode, observation.CheckID, text)
				}
			}
		})
	}
}

func TestVerifyAdmissionExitCategories(t *testing.T) {
	for _, item := range []struct {
		decision runtimebootverify.AdmissionDecision
		want     int
	}{
		{runtimebootverify.AdmissionDecision{Complete: true}, 0},
		{runtimebootverify.AdmissionDecision{FailureClass: failures.ClassSchemaInvalid}, CLIExitValidation},
		{runtimebootverify.AdmissionDecision{FailureClass: failures.ClassAuthenticationNeeded}, cliExitAuth},
		{runtimebootverify.AdmissionDecision{FailureClass: failures.ClassLifecycleConflict}, cliExitConflict},
		{runtimebootverify.AdmissionDecision{FailureClass: failures.ClassDependencyUnavailable}, CLIExitRuntime},
		{runtimebootverify.AdmissionDecision{Interrupted: true, FailureClass: failures.ClassSchemaInvalid}, cliExitInterrupted},
	} {
		if got := verifyAdmissionExitCode(item.decision); got != item.want {
			t.Fatalf("decision=%#v exit=%d want=%d", item.decision, got, item.want)
		}
	}
}
