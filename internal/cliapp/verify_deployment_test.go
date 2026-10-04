package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/division-sh/swarm/internal/testutil"
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

func TestVerifyDeploymentObservationBudgetsAndCancellation(t *testing.T) {
	for _, budget := range []time.Duration{verifyLocalDeadline, verifyRemoteDeadline, verifyCleanupDeadline} {
		t.Run(budget.String(), func(t *testing.T) {
			result := runtime.WorkflowContractValidationResult{}
			start := time.Now()
			if !verifyDeploymentBoundedObservation(context.Background(), budget, &result, "bounded", "test.owner", "exact:subject", failures.ClassDependencyUnavailable, func(ctx context.Context) error {
				deadline, ok := ctx.Deadline()
				if !ok || deadline.After(time.Now().Add(budget)) || deadline.Before(start.Add(budget)) {
					t.Fatalf("observation deadline %v does not enforce %v", deadline, budget)
				}
				return nil
			}) {
				t.Fatalf("successful bounded observation failed: %+v", result.BootReport)
			}
		})
	}
	for _, cancelParent := range []bool{false, true} {
		t.Run(fmt.Sprintf("completion_after_deadline/cancel=%v", cancelParent), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := runtime.WorkflowContractValidationResult{}
			if verifyDeploymentBoundedObservation(ctx, 10*time.Millisecond, &result, "bounded", "test.owner", "exact:subject", failures.ClassDependencyUnavailable, func(ctx context.Context) error {
				if cancelParent {
					cancel()
				}
				<-ctx.Done()
				return nil
			}) {
				t.Fatal("completion after cancellation/deadline became success")
			}
			decision := result.BootReport.AdmissionDecision(bootverify.AdmissionFindingPolicy{})
			if decision.Complete || decision.Interrupted != cancelParent || decision.FailureClass != failures.ClassDependencyUnavailable || result.BootReport.Observations[0].Status != bootverify.AdmissionUnavailable {
				t.Fatalf("deadline/cancellation disposition: %+v %+v", decision, result.BootReport)
			}
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	result := runtime.WorkflowContractValidationResult{}
	verifyDeploymentBoundedObservation(ctx, verifyRemoteDeadline, &result, "bounded", "test.owner", "exact:subject", failures.ClassDependencyUnavailable, func(ctx context.Context) error {
		actual, _ := ctx.Deadline()
		if !actual.Equal(deadline) {
			t.Fatal("observation extended its caller's total deadline")
		}
		return nil
	})
}

func TestVerifyDeploymentManagedCatalogConsumesLoadedSourceWithoutExecution(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	configPath := filepath.Join(t.TempDir(), "runtime.yaml")
	writeRuntimeConfigText(t, configPath, withTestProviderTriggerPlatformInventory(t,
		"llm: {backend: anthropic}\nserve: {api_listen_addr: '127.0.0.1:0', mcp_listen_addr: '127.0.0.1:0'}\n"))
	var out, diagnostic bytes.Buffer
	code := executeRootCommand(context.Background(), root, []string{"verify", root, "--config", configPath, "--json"}, &out, &diagnostic)
	var result verifyCommandResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("public result: %v exit=%d stderr=%s", err, code, &diagnostic)
	}
	var catalog, selected *bootverify.AdmissionObservation
	for i := range result.Observations {
		switch result.Observations[i].CheckID {
		case "managed_provider_catalog_admission":
			catalog = &result.Observations[i]
		case "selected_store_access":
			selected = &result.Observations[i]
		}
	}
	if catalog == nil || catalog.Status != bootverify.AdmissionPassed {
		t.Fatalf("loaded static catalog did not complete: %+v stderr=%s", catalog, &diagnostic)
	}
	if code == 0 || result.OK || result.AdmissionComplete || selected == nil || selected.Status == bootverify.AdmissionPassed {
		t.Fatalf("catalog admission erased absent store observation: exit=%d result=%+v", code, result)
	}
	if result.LiveReadiness != "not_evaluated" {
		t.Fatalf("static catalog manufactured provider execution: %+v", result)
	}
	if _, err := os.Stat(filepath.Join(root, ".swarm")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("static catalog verification created runtime state: %v", err)
	}
}

func TestVerifyFreshSQLiteSchemaAccountsAbsenceWithoutClaimingPossession(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	storePath := filepath.Join(t.TempDir(), "fresh.db")
	if err := os.WriteFile(storePath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(t.TempDir(), "runtime.yaml")
	writeRuntimeConfigText(t, configPath, withTestProviderTriggerPlatformInventory(t, fmt.Sprintf(
		"llm: {backend: anthropic}\nserve: {api_listen_addr: '127.0.0.1:0', mcp_listen_addr: '127.0.0.1:0'}\nstore: {backend: sqlite, sqlite: {path: %q}}\n", storePath)))
	var out, diagnostic bytes.Buffer
	code := executeRootCommand(context.Background(), root, []string{"verify", root, "--config", configPath, "--json"}, &out, &diagnostic)
	var result verifyCommandResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("public result: %v exit=%d stderr=%s", err, code, &diagnostic)
	}
	assertVerifyFreshStoreAccounting(t, code, result, bootverify.AdmissionFailed)
	if data, err := os.ReadFile(storePath); err != nil || len(data) != 0 {
		t.Fatalf("inspection initialized the fresh database: bytes=%d err=%v", len(data), err)
	}
	for _, sidecar := range []string{"-wal", "-shm", ".startup.lock", ".possession"} {
		if _, err := os.Stat(storePath + sidecar); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("fresh inspection created %s: %v", sidecar, err)
		}
	}
}

func TestVerifyFreshPostgresSchemaAccountsAbsenceWithoutClaimingPossession(t *testing.T) {
	dsn, db, _ := testutil.StartEmptyPostgres(t)
	connection, err := testpostgres.ParseConnection(dsn)
	if err != nil {
		t.Fatal(err)
	}
	parameters := connection.Parameters()
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_VERIFY_TEST_PASSWORD", parameters.Password)
	root := canonicalrouting.CopyExample(t, canonicalrouting.TelegramAgent)
	configPath := filepath.Join(t.TempDir(), "runtime.yaml")
	writeRuntimeConfigText(t, configPath, withTestProviderTriggerPlatformInventory(t, fmt.Sprintf(
		"llm: {backend: anthropic}\nserve: {api_listen_addr: '127.0.0.1:0', mcp_listen_addr: '127.0.0.1:0'}\nstore: {backend: postgres}\ndatabase: {host: %q, port: %d, name: %q, user: %q, password_env: SWARM_VERIFY_TEST_PASSWORD, sslmode: %q}\n",
		parameters.Host, parameters.Port, parameters.Database, parameters.User, parameters.SSLMode)))
	var out, diagnostic bytes.Buffer
	code := executeRootCommand(context.Background(), root, []string{"verify", root, "--config", configPath, "--json"}, &out, &diagnostic)
	var result verifyCommandResult
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("public result: %v exit=%d stderr=%s", err, code, &diagnostic)
	}
	assertVerifyFreshStoreAccounting(t, code, result, bootverify.AdmissionPassed)
	var tables int
	if err := db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'`).Scan(&tables); err != nil || tables != 0 {
		t.Fatalf("verification initialized fresh PostgreSQL: tables=%d err=%v", tables, err)
	}
}

func assertVerifyFreshStoreAccounting(t *testing.T, code int, result verifyCommandResult, possession bootverify.AdmissionObservationStatus) {
	t.Helper()
	seen := map[string]bool{}
	for _, observation := range result.Observations {
		switch observation.CheckID {
		case "selected_store_access", "selected_store_schema", "selected_store_cleanup":
			if observation.Status != bootverify.AdmissionPassed {
				t.Fatalf("fresh store inspection failed: %+v", observation)
			}
		case "startup_process_possession":
			if observation.Status != possession {
				t.Fatalf("fresh schema fabricated or skipped independent possession: %+v", observation)
			}
		case "startup_authority_lineage", "pinned_source_admission", "retained_source_integrity", "startup_recovery_admission", "pending_reset_admission", "retained_route_admission", "retained_channel_admission", "selected_fork_recovery_admission", "retained_actor_admission", "selected_fork_source_dependencies", "retained_actor_provider_dependencies":
			if observation.Status != bootverify.AdmissionNotApplicable || !strings.Contains(observation.Reason, "canonical schema inspection") {
				t.Fatalf("proven absent domain mistaken for unavailable or observed rows: %+v", observation)
			}
		default:
			continue
		}
		seen[observation.CheckID] = true
	}
	if len(seen) != 15 || code == 0 || result.OK || result.AdmissionComplete {
		t.Fatalf("fresh-store accounting: seen=%v exit=%d result=%+v", seen, code, result)
	}
	preparation := false
	for _, obligation := range result.ExecutionObligations {
		preparation = preparation || obligation.ID == "store_schema_preparation"
	}
	if !preparation {
		t.Fatal("schema preparation was silently claimed completed")
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
	if opts.Purpose != bootverify.ExecutionValidation || !opts.ValidateLLMModelResolution || opts.Credentials == nil || opts.ProviderCredentials == nil || opts.ManagedCredentials == nil || opts.MCPDiscoveryTimeout != verifyRemoteDeadline || !strings.EqualFold(opts.LLMProfile.ID, "anthropic") {
		t.Fatalf("deployment intent changed after admission: %#v", opts)
	}
}

func TestVerifyConcreteModelObservationUsesSelectedDeploymentProfile(t *testing.T) {
	for _, backend := range []string{"anthropic", "openai_responses"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			root := filepath.Join(RepoRoot(), "internal/store/selected/testdata/retained_actor_admission")
			configPath := filepath.Join(t.TempDir(), "runtime.yaml")
			writeRuntimeConfigText(t, configPath, "llm: {backend: "+backend+"}\n")
			cfg, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: configPath})
			if err != nil {
				t.Fatal(err)
			}
			_, bundle, _, err := loadCLIWorkflowModuleWithRuntimeConfig(root, CLISourcePlatformSpecPathOptions{SourceRoot: root}, cfg)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			opts, err := verifyDeploymentWorkflowOptions(ctx, cfg, bundle)
			if err != nil {
				t.Fatal(err)
			}
			source, _, err := admitStructuralSource(cfg, bundle)
			if err != nil {
				t.Fatal(err)
			}
			if err := runtime.ValidateDeclaredAgentModelAdmission(executionposture.Live, cfg.Config, source); err != nil {
				t.Fatal(err)
			}
			result, err := runtime.ValidateWorkflowContractSurface(ctx, source, opts)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, observation := range result.BootReport.Observations {
				if observation.CheckID == "invalid_field_detection" && observation.Class == bootverify.AdmissionDeploymentObservation {
					found = true
					if observation.Status != bootverify.AdmissionPassed {
						t.Fatalf("the configured concrete model was not observed: %+v", observation)
					}
				}
			}
			if !found {
				t.Fatal("concrete model observation disappeared from the registry report")
			}
		})
	}
}

func TestVerifyListenerDriverProvesOccupiedAndReleasedExactBindings(t *testing.T) {
	for _, occupied := range []bool{false, true} {
		t.Run(fmt.Sprintf("occupied=%v", occupied), func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			root := t.TempDir()
			var addresses []string
			for range 2 {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				addresses = append(addresses, listener.Addr().String())
				if occupied {
					t.Cleanup(func() {
						if err := listener.Close(); err != nil {
							t.Error(err)
						}
					})
				} else if err := listener.Close(); err != nil {
					t.Fatal(err)
				}
			}
			path := filepath.Join(root, "runtime.yaml")
			writeRuntimeConfigText(t, path, fmt.Sprintf("serve: {api_listen_addr: '%s', mcp_listen_addr: '%s'}\n", addresses[0], addresses[1]))
			cfg, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: path})
			if err != nil {
				t.Fatal(err)
			}
			result := runtime.WorkflowContractValidationResult{BootReport: bootverify.Report{Purpose: bootverify.ExecutionValidation}}
			inspectVerifyListeners(context.Background(), root, cfg, &result)
			seen := map[string]bool{}
			for _, observation := range result.BootReport.Observations {
				if observation.CheckID != "listener_availability" {
					continue
				}
				wantStatus := bootverify.AdmissionPassed
				if occupied {
					wantStatus = bootverify.AdmissionFailed
				}
				if observation.Status != wantStatus || observation.StartedAt.IsZero() || observation.FinishedAt.Before(observation.StartedAt) {
					t.Fatalf("incorrect actual-binding evidence: %+v", observation)
				}
				seen[observation.Subject] = true
			}
			if !seen["api:"+addresses[0]] || !seen["mcp:"+addresses[1]] || len(seen) != 2 || len(result.BootReport.ExecutionObligations) != 2 {
				t.Fatalf("lost exact configured binding or execution obligation: %+v", result.BootReport)
			}
			if decision := result.BootReport.AdmissionDecision(bootverify.AdmissionFindingPolicy{}); decision.Complete == occupied {
				t.Fatalf("occupied bindings were admitted or valid observations incomplete: %+v", decision)
			}
			if !occupied {
				for _, address := range addresses {
					listener, err := net.Listen("tcp", address)
					if err != nil {
						t.Fatalf("verifier retained configured address %s: %v", address, err)
					}
					if err := listener.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}
