package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
)

func TestVerifyDeploymentDiscoveryAndPortableOverride(t *testing.T) {
	for _, layer := range []string{"explicit", "project", "no_deployment"} {
		for _, portable := range []bool{false, true} {
			name := layer + "/default"
			if portable {
				name = layer + "/portable"
			}
			t.Run(name, func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
				credentialDir := t.TempDir()
				t.Setenv("SWARM_CREDENTIALS_FILE", credentialDir)
				t.Setenv("SWARM_MANAGED_CREDENTIALS_FILE", credentialDir)
				args := []string{"verify", root, "--json"}
				switch layer {
				case "explicit":
					args = append(args, "--config", writeTestVerifyRuntimeConfig(t))
				case "project":
					writeRuntimeConfigText(t, filepath.Join(root, "swarm.yaml"), withTestProviderTriggerPlatformInventory(t, "llm: {backend: anthropic}\nserve: {api_listen_addr: '127.0.0.1:0', mcp_listen_addr: '127.0.0.1:0'}\n"))
				}
				if portable {
					args = append(args, "--portable")
				}
				var out, errOut bytes.Buffer
				code := executeRootCommand(context.Background(), root, args, &out, &errOut)
				var result verifyCommandResult
				if err := json.Unmarshal(out.Bytes(), &result); err != nil {
					t.Fatalf("result: %v exit=%d stdout=%s stderr=%s", err, code, out.String(), errOut.String())
				}
				if result.LiveReadiness != "not_evaluated" || result.AdmissionComplete {
					t.Fatalf("fabricated readiness or unobserved completeness: %#v", result)
				}
				if layer == "no_deployment" || portable {
					if code != 0 || !result.OK || result.ValidationScope != "structural" {
						t.Fatalf("portable admission failed: exit=%d result=%#v", code, result)
					}
				} else {
					if code == 0 || result.OK || result.ValidationScope != "deployment" {
						t.Fatalf("unchecked deployment became success: exit=%d result=%#v", code, result)
					}
				}
				if _, err := os.Stat(filepath.Join(root, ".swarm")); !os.IsNotExist(err) {
					t.Fatalf("verification created runtime state: %v", err)
				}
				if entries, err := os.ReadDir(credentialDir); err != nil || len(entries) != 0 {
					t.Fatalf("verification mutated credentials: entries=%v err=%v", entries, err)
				}
			})
		}
	}
}

func TestVerifyDeploymentObservationPreservesFailureAndCancellation(t *testing.T) {
	for _, item := range []struct {
		name   string
		cause  error
		cancel bool
		class  failures.Class
	}{
		{name: "authentication", cause: failures.New(failures.ClassAuthenticationNeeded, "provider_credential_missing", "llm-provider", "resolve_credential", map[string]any{"auth_kind": "provider_credential"}), class: failures.ClassAuthenticationNeeded},
		{name: "read_failure", cause: errors.New("read port unavailable"), class: failures.ClassDependencyUnavailable},
		{name: "canceled", cause: errors.New("later invalidity"), cancel: true, class: failures.ClassDependencyUnavailable},
	} {
		t.Run(item.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if item.cancel {
				cancel()
			}
			result := runtime.WorkflowContractValidationResult{BootReport: bootverify.Report{Purpose: bootverify.ExecutionValidation}}
			called := false
			if verifyDeploymentObservation(ctx, &result, "selected_read", "test.readOwner", "source:test", failures.ClassDependencyUnavailable, func(context.Context) error {
				called = true
				return item.cause
			}) {
				t.Fatal("failed observation returned success")
			}
			decision := result.BootReport.AdmissionDecision(bootverify.AdmissionFindingPolicy{})
			if decision.Complete || decision.FailureClass != item.class || decision.Interrupted != item.cancel || called == item.cancel {
				t.Fatalf("incorrect decision=%#v called=%v", decision, called)
			}
		})
	}
}

func TestVerifyListenerObservationReleasesPortAndRetainsExecutionObligation(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := t.TempDir()
	path := filepath.Join(root, "runtime.yaml")
	writeRuntimeConfigText(t, path, "serve: {api_listen_addr: '127.0.0.1:0', mcp_listen_addr: '127.0.0.1:0'}\n")
	cfg, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: path})
	if err != nil {
		t.Fatal(err)
	}
	result := runtime.WorkflowContractValidationResult{BootReport: bootverify.Report{Purpose: bootverify.ExecutionValidation}}
	inspectVerifyListeners(context.Background(), root, cfg, &result)
	decision := result.BootReport.AdmissionDecision(bootverify.AdmissionFindingPolicy{})
	if !decision.Complete || decision.FailureClass != "" || len(result.BootReport.ExecutionObligations) != 2 {
		t.Fatalf("listener observation failed or falsely settled startup: %#v %#v", decision, result.BootReport)
	}
	for _, kind := range []string{"api", "mcp"} {
		listener, err := ListenServeHTTPListener(kind, "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		observed, err := ListenServeHTTPListenerInContext(context.Background(), kind, addr)
		if err != nil {
			t.Fatal(err)
		}
		if err := observed.Close(); err != nil {
			t.Fatal(err)
		}
		reused, err := net.Listen("tcp", addr)
		if err != nil {
			t.Fatalf("observation retained a port: %v", err)
		}
		if err := reused.Close(); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ListenServeHTTPListenerInContext(ctx, "api", "127.0.0.1:0"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled listener acquired resources: %v", err)
	}
}

func TestVerifyDeploymentOptionsPreserveFrozenConfigAfterFileRemoval(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	path := writeTestVerifyRuntimeConfig(t)
	cfg, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: path})
	if err != nil {
		t.Fatal(err)
	}
	_, bundle, _, err := loadCLIWorkflowModuleWithRuntimeConfig(root, CLISourcePlatformSpecPathOptions{SourceRoot: root}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	opts, err := verifyDeploymentWorkflowOptions(context.Background(), cfg, bundle)
	if err != nil {
		t.Fatal(err)
	}
	if opts.Purpose != bootverify.ExecutionValidation || opts.Credentials == nil || opts.ProviderCredentials == nil || opts.ManagedCredentials == nil || opts.AllowHarnessInputs || opts.AllowHarnessOutputs || opts.MCPDiscoveryTimeout != verifyRemoteDeadline || !strings.EqualFold(opts.LLMProfile.ID, "anthropic") {
		t.Fatalf("deployment intent changed after admission: %#v", opts)
	}
}
