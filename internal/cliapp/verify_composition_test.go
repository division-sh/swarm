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
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/bootverify"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/destructivereset"
	"github.com/division-sh/swarm/internal/runtime/runbundle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/construction"
	"github.com/division-sh/swarm/internal/store/devscratch"
	"github.com/division-sh/swarm/internal/store/selected/selectedtest"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/tests/support/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/tests/support/testpostgres"
	"github.com/division-sh/swarm/tests/support/testutil"
	"github.com/division-sh/swarm/tests/support/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type verifyCompositionBootStore interface {
	store.SchemaBootstrapper
	storetest.RunFixtureStore
	sourceartifactfixture.Writer
	sourceartifactfixture.Reader
	startupownership.Store
	InspectSchema(context.Context, store.SchemaBootstrapRequest) (store.SchemaInspection, error)
	Close() error
}

func newVerifyCompositionBootStore(t *testing.T, backend, root string, schema store.SchemaBootstrapRequest) (verifyCompositionBootStore, string) {
	t.Helper()
	var owner verifyCompositionBootStore
	var configText string
	if backend == "sqlite" {
		path := filepath.Join(t.TempDir(), "runtime.db")
		selected, err := selectedtest.OpenProcessLeaseFixture(context.Background(), backendselection.Selection{Backend: backendselection.BackendSQLite, SQLitePath: path}, "")
		if err != nil {
			t.Fatal(err)
		}
		// Semantic runs and fixed mutation observation retain the original native
		// owner rather than the process-only lease projection.
		owner = selected.RunFixtureStore.(verifyCompositionBootStore)
		configText = fmt.Sprintf("store: {backend: sqlite, sqlite: {path: %q}}\n", path)
	} else {
		dsn, _, _ := testutil.StartEmptyPostgres(t)
		selected, _, err := construction.OpenPostgres(dsn)
		if err != nil {
			t.Fatal(err)
		}
		owner = selected
		connection, err := testpostgres.ParseConnection(dsn)
		if err != nil {
			t.Fatal(err)
		}
		p := connection.Parameters()
		t.Setenv("SWARM_VERIFY_COMPOSITION_PASSWORD", p.Password)
		configText = fmt.Sprintf("store: {backend: postgres}\ndatabase: {host: %q, port: %d, name: %q, user: %q, password_env: SWARM_VERIFY_COMPOSITION_PASSWORD, sslmode: %q}\n", p.Host, p.Port, p.Database, p.User, p.SSLMode)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx := context.Background()
	if err := owner.BootstrapSchema(ctx, schema); err != nil {
		t.Fatal(err)
	}
	capability, err := owner.AcquireProcessCapability(ctx, startupownership.AcquireRequest{
		OwnerID: "composition-proof", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := capability.Release(ctx); err != nil {
		t.Fatal(err)
	}
	return owner, configText
}

func TestVerifyPublicListenerSetMatchesBootAndReleasesBindings(t *testing.T) {
	t.Run("borrowed_api_handle", func(t *testing.T) {
		listener, err := ListenServeHTTPListener("api", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		address := listener.Addr().String()
		if err := inspectBorrowedAPIListener(address, listener); err != nil {
			t.Fatal(err)
		}
		if err := inspectBorrowedAPIListener("0.0.0.0:1", listener); err == nil {
			t.Fatal("borrowed handle bypassed exact address admission")
		}
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		if err := inspectBorrowedAPIListener(address, listener); err == nil {
			t.Fatal("closed borrowed handle was admitted")
		}
		reclaimed, err := ListenServeHTTPListener("api", address)
		if err != nil {
			t.Fatalf("borrowed-handle inspection retained its socket: %v", err)
		}
		if err := reclaimed.Close(); err != nil {
			t.Fatal(err)
		}
	})
	for _, mode := range []string{"same_fixed", "wildcard_specific", "specific_wildcard", "distinct_fixed", "ephemeral"} {
		t.Run(mode, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			root := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(RepoRoot(), root, filepath.Join(RepoRoot(), "platform-spec.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			plans, err := StateStoreSchemaPlans(bundle)
			if err != nil {
				t.Fatal(err)
			}
			schema, err := SchemaBootstrapRequest(bundle.Platform, plans.Platform)
			if err != nil {
				t.Fatal(err)
			}
			_, configText := newVerifyCompositionBootStore(t, "sqlite", root, schema)
			freeAddress := func() string {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				addr := listener.Addr().String()
				if err := listener.Close(); err != nil {
					t.Fatal(err)
				}
				return addr
			}
			api, mcp := freeAddress(), ""
			mcp = api
			_, port, err := net.SplitHostPort(api)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "wildcard_specific":
				api = "0.0.0.0:" + port
			case "specific_wildcard":
				mcp = "0.0.0.0:" + port
			case "distinct_fixed":
				mcp = freeAddress()
			case "ephemeral":
				api, mcp = "127.0.0.1:0", "127.0.0.1:0"
			}
			configPath := filepath.Join(t.TempDir(), "runtime.yaml")
			writeRuntimeConfigText(t, configPath, configText+fmt.Sprintf("llm: {backend: anthropic}\nserve: {api_listen_addr: %q, mcp_listen_addr: %q, api_token_file: %q}\n", api, mcp, writeCLIAPITokenFile(t, "composition-token")))
			var out, diagnostic bytes.Buffer
			code := renderServeChecksForTest(t, context.Background(), root, root, configPath, "json", &out, &diagnostic)
			var result verifyCommandResult
			if err := json.Unmarshal(out.Bytes(), &result); err != nil {
				t.Fatalf("public result: %v output=%s stderr=%s", err, &out, &diagnostic)
			}
			bootAPI, err := ListenServeHTTPListener("api", api)
			if err != nil {
				t.Fatalf("verify leaked its API listener: %v", err)
			}
			bootMCP, bootErr := ListenServeHTTPListener("mcp", mcp)
			if bootMCP != nil {
				if err := bootMCP.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if err := bootAPI.Close(); err != nil {
				t.Fatal(err)
			}
			conflict := mode == "same_fixed" || mode == "wildcard_specific" || mode == "specific_wildcard"
			if (bootErr != nil) != conflict || (code != 0) != conflict || result.OK == conflict || result.AdmissionComplete == conflict {
				t.Fatalf("verify/boot composition mismatch: mode=%s boot=%v code=%d result=%+v stderr=%s", mode, bootErr, code, result, &diagnostic)
			}
			for _, addr := range []string{api, mcp} {
				listener, err := ListenServeHTTPListener("after-cleanup", addr)
				if err != nil {
					t.Fatalf("composition retained %s: %v", addr, err)
				}
				if err := listener.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestVerifyPublicExistingStoreReportsDeploymentPreparationBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			root := canonicalrouting.CopyExample(t, canonicalrouting.RootIngress)
			path := filepath.Join(root, "nodes.yaml")
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			edited := string(body)
			bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(RepoRoot(), root, filepath.Join(RepoRoot(), "platform-spec.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			plans, err := StateStoreSchemaPlans(bundle)
			if err != nil {
				t.Fatal(err)
			}
			complete, err := SchemaBootstrapRequest(bundle.Platform, plans.Platform)
			if err != nil {
				t.Fatal(err)
			}
			platformOnly := complete
			platformOnly.PlatformPlans = nil
			for _, plan := range plans.Platform {
				if plan.TableName != "deployment_channel_operations" {
					platformOnly.PlatformPlans = append(platformOnly.PlatformPlans, plan)
				}
			}
			owner, configText := newVerifyCompositionBootStore(t, backend, root, platformOnly)
			configPath := filepath.Join(t.TempDir(), "runtime.yaml")
			writeRuntimeConfigText(t, configPath, configText+"llm: {backend: anthropic}\nserve: {api_listen_addr: '127.0.0.1:0', mcp_listen_addr: '127.0.0.1:0'}\n")
			ctx := context.Background()
			for _, prepared := range []bool{false, true} {
				before, err := owner.InspectSchema(ctx, complete)
				if err != nil || before.Fresh || (len(before.MissingDeploymentTables) == 0) != prepared {
					t.Fatalf("boot inspection: %+v %v", before, err)
				}
				t.Run(fmt.Sprintf("serve_stage_held=%t", prepared), func(t *testing.T) {
					cfg, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: configPath})
					if err != nil {
						t.Fatal(err)
					}
					paths, err := ResolveCLISourcePaths(root, CLISourcePathOptions{SourceRoot: root, ConfigPath: configPath})
					if err != nil {
						t.Fatal(err)
					}
					request, _, err := prepareVerifySelectedStoreInspection(ctx, root, paths, cfg, cliSwarmDirOptions{}, semanticview.Wrap(bundle))
					if err != nil {
						t.Fatal(err)
					}
					base, err := LoadConfiguredPlatformPackBase(root, cfg)
					if err != nil {
						t.Fatal(err)
					}
					bases, err := packartifact.NewPlatformPackBaseGenerationOwner(base)
					if err != nil {
						t.Fatal(err)
					}
					if prepared {
						capability, err := owner.AcquireProcessCapability(ctx, startupownership.AcquireRequest{OwnerID: "stage-held-control", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString()})
						if err != nil {
							t.Fatal(err)
						}
						before, err := capability.Evidence()
						if err != nil {
							t.Fatal(err)
						}
						defer func() {
							after, err := capability.Evidence()
							if err != nil || !reflect.DeepEqual(before, after) {
								t.Errorf("check changed held authority: %v", err)
							}
							if err := capability.Release(ctx); err != nil {
								t.Error(err)
							}
						}()
					}
					opts := DefaultServeOptions()
					opts.SourceRoot, opts.ConfigPath, opts.DryRun = root, configPath, true
					opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
					input := ServeCheckStageRequest{Repo: root, Paths: paths, Config: cfg, Options: opts, Selection: request.Selection, PackBases: bases}
					stage, err := InspectServeCheckStage(ctx, input)
					if err != nil || stage.Decision().FailureClass != "" {
						t.Fatalf("serve-owned stage failed: err=%v report=%+v", err, stage.Validation.BootReport)
					}
					if err := stage.Revalidate(ctx, input); err != nil {
						t.Fatalf("unchanged inputs refused: %v", err)
					}
					if err := os.WriteFile(path, append(append([]byte(nil), body...), '\n'), 0o600); err != nil {
						t.Fatal(err)
					}
					if err := stage.Revalidate(ctx, input); err == nil {
						t.Fatal("changed source reused check observations")
					}
					if err := os.WriteFile(path, body, 0o600); err != nil {
						t.Fatal(err)
					}
					after, err := owner.InspectSchema(ctx, complete)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("stage changed native schema: %v", err)
					}
					if backend == "sqlite" && !prepared {
						coordinate, err := devscratch.Resolve(root)
						if err != nil {
							t.Fatal(err)
						}
						if err := os.MkdirAll(filepath.Dir(coordinate.DatabasePath), 0700); err != nil {
							t.Fatal(err)
						}
						predecessor := []byte("disposable predecessor, not a retained SQLite source")
						if err := os.WriteFile(coordinate.DatabasePath, predecessor, 0600); err != nil {
							t.Fatal(err)
						}
						input.Options.Dev = true
						input.Selection = backendselection.Selection{Backend: backendselection.BackendSQLite, SQLitePath: coordinate.DatabasePath, SQLitePathSource: backendselection.SourceProjectScratch}
						scratch, err := InspectServeCheckStage(ctx, input)
						if err != nil || scratch.Failure() != nil || scratch.BootStoreInput() != nil {
							t.Fatalf("disposable predecessor became retained application state: %v %v", err, scratch.Failure())
						}
						if actual, err := os.ReadFile(coordinate.DatabasePath); err != nil || string(actual) != string(predecessor) {
							t.Fatalf("dry run replaced the predecessor: %v", err)
						}
					}
				})
				var out, diagnostic bytes.Buffer
				code := renderServeChecksForTest(t, ctx, root, root, configPath, "json", &out, &diagnostic)
				var result verifyCommandResult
				if err := json.Unmarshal(out.Bytes(), &result); err != nil || code != 0 || !result.OK || !result.AdmissionComplete {
					t.Fatalf("preparable state refused: code=%d result=%+v err=%v stderr=%s", code, result, err, &diagnostic)
				}
				found := false
				for _, obligation := range result.ExecutionObligations {
					if obligation.ID == "store_schema_preparation" {
						found = true
						if obligation.Owner != "internal/store.SchemaBootstrapper.BootstrapSchema" || !strings.Contains(obligation.Reason, "deployment_channel_operations") {
							t.Fatalf("missing exact boot preparation: %+v", obligation)
						}
					}
				}
				if found == prepared {
					t.Fatalf("preparation obligation=%v, boot prepared=%v", found, prepared)
				}
				after, err := owner.InspectSchema(ctx, complete)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("verify changed schema: before=%+v after=%+v err=%v", before, after, err)
				}
				for _, check := range []string{"selected_store_access", "selected_store_schema"} {
					passed := false
					for _, observation := range result.Observations {
						if observation.CheckID == check {
							if observation.Status != bootverify.AdmissionPassed {
								t.Fatalf("native store read did not pass: %+v", observation)
							}
							passed = true
						}
					}
					if !passed {
						t.Fatalf("missing explicit %s observation", check)
					}
				}
				if !prepared {
					if err := owner.BootstrapSchema(ctx, complete); err != nil {
						t.Fatal(err)
					}
				}
			}
			t.Run("compatible upgrade leaves the admitted run unchanged", func(t *testing.T) {
				process, err := owner.AcquireProcessCapability(ctx, startupownership.AcquireRequest{
					OwnerID: "upgrade-setup", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
				})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = process.Release(context.Background()) })
				original, err := startupownership.WithProcessMutationAuthority(ctx, process)
				if err != nil {
					t.Fatal(err)
				}
				runID := uuid.NewString()
				storetest.RequireRun(t, original, owner, storetest.RunFixture{
					RunID: runID, Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact,
				})
				reader := owner.(destructivereset.InventoryReader)
				operation, err := destructivereset.NewOperation(destructivereset.Request{
					OperationID: uuid.NewString(), ActorTokenID: "operator", RequestHash: "phase-aware-checks", RequestedAt: time.Now().UTC(), IncludeSourceArtifactsSet: true,
				})
				if err != nil {
					t.Fatal(err)
				}
				plan, err := (destructivereset.InventoryPlanner{Reader: reader}).BuildPlan(ctx, operation.Request)
				if err != nil {
					t.Fatal(err)
				}
				operation.Phase, operation.Revision = destructivereset.PhasePlanned, 2
				operation.Plan = &destructivereset.Result{OperationName: destructivereset.DefaultOperationName, PlannedAt: operation.Request.RequestedAt, Plan: plan}
				if _, err := inspectServeResetDiscard(ctx, reader, operation); err != nil {
					t.Fatalf("unchanged pre-cleanup population refused: %v", err)
				}
				storetest.RequireRun(t, original, owner, storetest.RunFixture{
					RunID: uuid.NewString(), Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact,
				})
				if _, err := inspectServeResetDiscard(ctx, reader, operation); !errors.Is(err, destructivereset.ErrOperationChanged) {
					t.Fatalf("pre-cleanup new membership was accepted: %v", err)
				}
				operation.Phase, operation.Revision = destructivereset.PhaseCleanupCommitted, 4
				operation.Quiescence = &destructivereset.QuiescenceResult{OperationName: destructivereset.DefaultOperationName}
				operation.Cleanup = &destructivereset.CleanupResult{OperationName: destructivereset.DefaultOperationName}
				beforeOperation := operation
				discard, err := inspectServeResetDiscard(ctx, reader, operation)
				if err != nil || len(discard.CleanupRuns) != 0 || !reflect.DeepEqual(beforeOperation, operation) {
					t.Fatalf("committed cleanup was reapplied or hid current survivors: %+v, %v", discard, err)
				}
				if err := process.Release(ctx); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(edited+"\n# compatible source revision\n"), 0600); err != nil {
					t.Fatal(err)
				}
				var out, diagnostic bytes.Buffer
				code := renderServeChecksForTest(t, ctx, root, root, configPath, "json", &out, &diagnostic)
				var result verifyCommandResult
				if err := json.Unmarshal(out.Bytes(), &result); err != nil || code != 0 || !result.OK || !result.AdmissionComplete {
					t.Fatalf("compatible retained run refused: code=%d result=%+v err=%v stderr=%s", code, result, err, &diagnostic)
				}
				if result.BundleHash == "" || result.BundleHash == bundle.SourceArtifact.BundleHash() {
					t.Fatalf("upgrade witness did not change the candidate artifact: %q", result.BundleHash)
				}
				availability, err := owner.(interface {
					LoadRunBundleAvailability(context.Context, string) (runbundle.Availability, error)
				}).LoadRunBundleAvailability(ctx, runID)
				if err != nil || availability.BundleHash != bundle.SourceArtifact.BundleHash() {
					t.Fatalf("read-only upgrade inspection changed admitted source: %+v, %v", availability, err)
				}
			})
			for _, survivor := range []string{"valid", "invalid_kept_source", "clear_to_empty"} {
				t.Run("committed_cleanup_snapshot_"+survivor, func(t *testing.T) {
					native, text := newVerifyCompositionBootStore(t, backend, root, complete)
					configPath := filepath.Join(t.TempDir(), "runtime.yaml")
					writeRuntimeConfigText(t, configPath, text+"llm: {backend: anthropic}\n")
					process, err := native.AcquireProcessCapability(ctx, startupownership.AcquireRequest{
						OwnerID: "cleanup-survivors", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
					})
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = process.Release(context.Background()) })
					original, err := startupownership.WithProcessMutationAuthority(ctx, process)
					if err != nil {
						t.Fatal(err)
					}
					runID := uuid.NewString()
					storetest.RequireRun(t, original, native, storetest.RunFixture{RunID: runID, Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact})
					operation, err := process.AdmitResetOperation(ctx, destructivereset.Request{
						OperationID: uuid.NewString(), ActorTokenID: "operator", RequestHash: "snapshot-survivors", RequestedAt: time.Now().UTC(), IncludeSourceArtifactsSet: true,
					})
					if err != nil {
						t.Fatal(err)
					}
					reader := native.(destructivereset.InventoryReader)
					plan, err := (destructivereset.InventoryPlanner{Reader: reader}).BuildPlan(ctx, operation.Request)
					if err != nil {
						t.Fatal(err)
					}
					planned := operation
					planned.Phase, planned.Revision = destructivereset.PhasePlanned, operation.Revision+1
					planned.Plan = &destructivereset.Result{OperationName: destructivereset.DefaultOperationName, PlannedAt: operation.Request.RequestedAt, Plan: plan}
					if err := process.AdvanceResetOperation(ctx, operation, planned); err != nil {
						t.Fatal(err)
					}
					quiescence, err := native.(destructivereset.QuiescenceStore).ApplyDestructiveResetQuiescence(original, destructivereset.QuiescenceRequest{
						OperationID: operation.Request.OperationID, ActorTokenID: operation.Request.ActorTokenID, RequestedAt: operation.Request.RequestedAt, Result: *planned.Plan,
					})
					if err != nil {
						t.Fatal(err)
					}
					if _, err := process.ApplyDestructiveResetCleanup(original, destructivereset.CleanupRequest{
						OperationID: operation.Request.OperationID, ActorTokenID: operation.Request.ActorTokenID, RequestedAt: operation.Request.RequestedAt, Result: *planned.Plan, Quiescence: quiescence,
					}, nil); err != nil {
						t.Fatal(err)
					}
					committed, err := process.ReadResetOperation(ctx, operation.Request.OperationID)
					if err != nil || committed.Phase != destructivereset.PhaseCleanupCommitted || committed.Cleanup == nil {
						t.Fatalf("cleanup did not commit through its owner: %+v, %v", committed, err)
					}
					artifact := bundle.SourceArtifact
					if survivor == "invalid_kept_source" {
						artifact = sourceartifactfixture.New("manifest.yaml", []byte("invalid: ["))
					}
					// Reuse a member of P after cleanup. Its current row must not be
					// discarded by the old cleanup receipt's membership list.
					storetest.RequireRun(t, original, native, storetest.RunFixture{RunID: runID, Origin: storetest.ScenarioSetupOrigin(), Artifact: artifact})
					before, err := reader.ReadResetInventory(ctx)
					if err != nil {
						t.Fatal(err)
					}
					cfg, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: configPath})
					if err != nil {
						t.Fatal(err)
					}
					paths, err := ResolveCLISourcePaths(root, CLISourcePathOptions{SourceRoot: root, ConfigPath: configPath})
					if err != nil {
						t.Fatal(err)
					}
					source := semanticview.Wrap(bundle)
					if survivor == "clear_to_empty" {
						source = nil
					}
					request, _, err := prepareVerifySelectedStoreInspection(ctx, root, paths, cfg, cliSwarmDirOptions{}, source)
					if err != nil {
						t.Fatal(err)
					}
					base, err := LoadConfiguredPlatformPackBase(root, cfg)
					if err != nil {
						t.Fatal(err)
					}
					bases, err := packartifact.NewPlatformPackBaseGenerationOwner(base)
					if err != nil {
						t.Fatal(err)
					}
					profile, err := cfg.Config.LLMBackendProfile()
					if err != nil {
						t.Fatal(err)
					}
					report := runtime.WorkflowContractValidationResult{BootReport: bootverify.Report{Purpose: bootverify.ExecutionValidation, SourceArtifactHash: bundle.SourceArtifact.BundleHash()}}
					inspectVerifySelectedStore(ctx, root, paths, cfg, cliSwarmDirOptions{}, source, runtime.WorkflowContractValidationOptions{LLMProfile: profile}, bases, nil, false, &report,
						serveStoreInspection{selection: request.Selection, dryRun: true, reset: &committed})
					failed, exactRefusal := false, false
					seen := map[string]bool{}
					for _, observation := range report.BootReport.Observations {
						failed = failed || observation.Status == bootverify.AdmissionFailed || observation.Status == bootverify.AdmissionUnavailable
						seen[observation.CheckID] = observation.Status == bootverify.AdmissionPassed
						if observation.Status == bootverify.AdmissionFailed {
							if survivor == "invalid_kept_source" && observation.CheckID == "retained_source_integrity" && strings.HasPrefix(observation.Subject, "run:"+runID+"/source:") {
								exactRefusal = true
							}
							if survivor == "clear_to_empty" && observation.CheckID == "pinned_source_admission" && strings.Contains(observation.Reason, "surviving runs or deliveries") {
								exactRefusal = true
							}
						}
					}
					if failed != (survivor != "valid") || (survivor != "valid" && !exactRefusal) {
						t.Fatalf("committed cleanup concealed or invented survivor refusal: %+v", report.BootReport)
					}
					if survivor == "valid" {
						for _, check := range bootverify.SelectedStoreDependentAdmissionChecks() {
							if !seen[check.ID] {
								t.Fatalf("surviving population did not pass %s: %+v", check.ID, report.BootReport)
							}
						}
					}
					after, err := reader.ReadResetInventory(ctx)
					if err != nil || !reflect.DeepEqual(before, after) {
						t.Fatalf("snapshot changed surviving inventory: %v", err)
					}
					afterOperation, err := process.ReadResetOperation(ctx, operation.Request.OperationID)
					if err != nil || !reflect.DeepEqual(committed, afterOperation) {
						t.Fatalf("snapshot advanced the committed operation: %v", err)
					}
				})
			}
		})
	}
}
