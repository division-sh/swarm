package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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
	if !result.OK || result.AdmissionComplete || result.ValidationScope != "structural" || result.LiveReadiness != "not_evaluated" || result.BundleHash == "" || result.SourceLabel == "" || len(result.Members) == 0 {
		t.Fatalf("portable result made a readiness claim: %#v", result)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(jsonOut.Bytes(), &wire); err != nil {
		t.Fatal(err)
	}
	if _, present := wire["source_artifact_hash"]; present {
		t.Fatal("verify published a competing source identity instead of bundle_hash")
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
				subject := observation.Subject
				if subject == "source:"+result.BundleHash {
					subject = "source:" + result.SourceLabel
				}
				if observation.Status == runtimebootverify.AdmissionNotRun && !strings.Contains(text, "not_run: "+observation.CheckID+" @ "+subject) {
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

func TestVerifyHumanAdmissionSubjectsPreserveExactMachineEvidence(t *testing.T) {
	hash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
	foreign := "bundle-v2:sha256:" + strings.Repeat("b", 64)
	output := verifyCommandResult{BundleHash: hash, SourceLabel: "Reception@1.2.3"}
	for _, row := range []struct{ subject, want string }{
		{"source:" + hash, "source:Reception@1.2.3"},
		{"fork:exact-id/source:" + hash, "fork:exact-id/source:Reception@1.2.3"},
		{"source:" + foreign, "source:" + foreign},
		{"project:/exact/project", "project:/exact/project"},
		{"source:" + hash + "/nested", "source:" + hash + "/nested"},
	} {
		output.Observations = []runtimebootverify.AdmissionObservation{{CheckID: "check", Status: runtimebootverify.AdmissionNotRun, Subject: row.subject}}
		if !strings.Contains(strings.Join(verifyAdmissionTextLines(output), "\n"), "not_run: check @ "+row.want+" (") {
			t.Fatalf("human display lost scoped identity for %q", row.subject)
		}
		data, err := json.Marshal(output)
		if err != nil {
			t.Fatal(err)
		}
		var machine verifyCommandResult
		if err := json.Unmarshal(data, &machine); err != nil {
			t.Fatal(err)
		}
		if machine.BundleHash != hash || machine.Observations[0].Subject != row.subject {
			t.Fatalf("human projection changed machine evidence: %s", data)
		}
	}
}

func TestVerifyEarlyRefusalPreservesIncompleteLedgerOnPublicOutputs(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	missing := filepath.Join(t.TempDir(), "missing-config.yaml")
	invalid := filepath.Join(t.TempDir(), "invalid-config.yaml")
	writeRuntimeConfigText(t, invalid, "llm: [\n")
	nonDirectory := filepath.Join(t.TempDir(), "not-a-directory")
	writeRuntimeConfigText(t, nonDirectory, "not a source directory\n")
	for _, item := range []struct {
		name, source, config, check string
	}{
		{"missing_config", root, missing, "runtime_configuration"},
		{"invalid_config", root, invalid, "runtime_configuration"},
		{"non_directory_source", nonDirectory, writeTestVerifyRuntimeConfig(t), "source_path_selection"},
	} {
		for _, mode := range []string{"json", "text", "quiet"} {
			t.Run(item.name+"/"+mode, func(t *testing.T) {
				args := []string{"verify", item.source, "--config", item.config}
				if mode != "text" {
					args = append(args, "--"+mode)
				}
				var out, errOut bytes.Buffer
				if code := executeRootCommand(context.Background(), RepoRoot(), args, &out, &errOut); code != CLIExitValidation {
					t.Fatalf("exit=%d stdout=%s stderr=%s", code, out.String(), errOut.String())
				}
				if mode == "text" || mode == "quiet" {
					if !strings.Contains(out.String(), "admission validation failed; startup execution not performed") || !strings.Contains(out.String(), "failed: "+item.check) || !strings.Contains(out.String(), "not_run:") {
						t.Fatalf("refusal or unobserved tail was hidden: %s", out.String())
					}
					return
				}
				if errOut.Len() != 0 {
					t.Fatalf("structured failure leaked to stderr: %s", errOut.String())
				}
				var result verifyCommandResult
				if err := json.Unmarshal(out.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				if result.OK || result.AdmissionComplete || result.ValidationScope != "deployment" || result.LiveReadiness != "not_evaluated" || result.Observations == nil || result.ExecutionObligations == nil || result.Members == nil {
					t.Fatalf("early refusal fabricated readiness or omitted evidence: %#v", result)
				}
				failed, blocked := false, false
				for _, observation := range result.Observations {
					if observation.CheckID == item.check && observation.Status == runtimebootverify.AdmissionFailed {
						failed = observation.Owner != "" && observation.Subject != "" && observation.Reason != "" && observation.FailureClass == failures.ClassSchemaInvalid
					}
					if observation.Status == runtimebootverify.AdmissionNotRun && len(observation.Dependencies) == 1 && observation.Dependencies[0] == item.check {
						blocked = true
					}
				}
				if !failed || !blocked {
					t.Fatalf("missing refusal or dependency evidence: %#v", result.Observations)
				}
			})
		}
	}
}

func TestVerifyEarlyRefusalCancellationWinsExitDisposition(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, errOut bytes.Buffer
	code := executeRootCommand(ctx, RepoRoot(), []string{"verify", ".", "--config", filepath.Join(t.TempDir(), "missing.yaml"), "--json"}, &out, &errOut)
	if code != cliExitInterrupted || errOut.Len() != 0 {
		t.Fatalf("cancellation lost precedence: exit=%d stdout=%s stderr=%s", code, out.String(), errOut.String())
	}
	var result verifyCommandResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.OK || result.AdmissionComplete || len(result.Observations) == 0 {
		t.Fatalf("canceled admission became success: %#v", result)
	}
}
