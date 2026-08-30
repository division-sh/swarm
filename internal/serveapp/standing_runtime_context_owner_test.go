package serveapp

import (
	"context"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type standingRuntimeContextOperationResult struct {
	ServiceID      string `json:"service_id"`
	RunID          string `json:"run_id"`
	Generation     int64  `json:"generation"`
	EffectiveState string `json:"effective_state"`
	Transition     string `json:"transition"`
}

func TestStandingServiceMutationsUseSelectedRuntimePipelineOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			primaryStores := openStandingRuntimeContextStore(t, backend, "primary")
			selectedStores := openStandingRuntimeContextStore(t, backend, "selected")
			process := worklifetime.NewProcess()
			t.Cleanup(func() {
				process.Retire()
				if _, err := process.Join(context.Background()); err != nil {
					t.Errorf("join standing runtime-context process: %v", err)
				}
			})

			catalog := testProviderTriggerCatalog(t)
			sourceRoot := writeStandingTelegramServeFixture(t, "http://127.0.0.1:1")
			repoRoot := repoRootForTest()
			selectedModule, selectedBundle, err := cliapp.NewSwarmWorkflowModule(
				repoRoot,
				sourceRoot,
				cliapp.ResolvePath(repoRoot, defaultPlatformSpecPath),
			)
			if err != nil {
				t.Fatalf("load selected standing module: %v", err)
			}
			selectedHash, err := runtimecontracts.BundleHash(selectedBundle)
			if err != nil {
				t.Fatalf("hash selected standing module: %v", err)
			}
			primaryHash := "bundle-v2:sha256:" + strings.Repeat("a", 64)
			if primaryHash == selectedHash {
				primaryHash = "bundle-v2:sha256:" + strings.Repeat("b", 64)
			}
			primaryFact := mustServeTestPersistedSourceArtifactFact(primaryHash)
			selectedFact := mustServeTestPersistedSourceArtifactFact(selectedHash)
			seedStandingRuntimeContextBundle(t, selectedStores.SourceArtifactWriter(), selectedBundle)
			runtimeInstanceID := uuid.NewString()
			primaryModule := stubWorkflowModule{source: semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{
				Platform: selectedBundle.Platform,
			})}
			primary := newStandingRuntimeContextRuntime(t, process, primaryStores, primaryModule, primaryFact, runtimeInstanceID, catalog)
			selected := newStandingRuntimeContextRuntime(t, process, selectedStores, selectedModule, selectedFact, runtimeInstanceID, catalog)

			selectedCtx := runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.BundleScope(runtimeInstanceID, selectedHash))
			selectedCtx = runtimecorrelation.WithSourceArtifactFact(selectedCtx, selectedFact)
			selectedCtx = worklifetime.WithRuntimeOccurrence(selectedCtx, selected.WorkOccurrence())
			targets, activations, err := selected.EnsureStandingTargets(selectedCtx)
			if err != nil {
				t.Fatalf("ensure selected standing targets: %v", err)
			}
			if len(targets) != 1 || len(activations) != 1 {
				t.Fatalf("selected standing targets/activations = %d/%d, want 1/1", len(targets), len(activations))
			}
			serviceID := targets[0].ServiceID

			installed, err := catalog.InstalledCapabilitySubjects()
			if err != nil {
				t.Fatalf("selected standing capability subjects: %v", err)
			}
			manager, err := runtimepkg.NewRuntimeContextManager(nil, completeServeTestPackContext(t, runtimepkg.BundleContext{
				SourceArtifactFact: primaryFact, Source: primaryModule.SemanticSource(), Runtime: primary, WorkOwner: primary.WorkOccurrence(),
				ProviderTriggerGeneration: catalog.Generation(), InstalledTriggerSubjects: installed,
			}), completeServeTestPackContext(t, runtimepkg.BundleContext{
				SourceArtifactFact: selectedFact, Source: selectedModule.SemanticSource(), Runtime: selected, WorkOwner: selected.WorkOccurrence(), StandingTargets: targets,
				ProviderTriggerGeneration: catalog.Generation(), InstalledTriggerSubjects: installed,
			}))
			if err != nil {
				t.Fatalf("build standing runtime-context manager: %v", err)
			}
			t.Cleanup(func() {
				if err := manager.QuiesceAllRuntimeContexts(context.Background()); err != nil {
					t.Errorf("quiesce standing runtime contexts: %v", err)
				}
			})

			var primarySignals, selectedSignals atomic.Int32
			primaryRegistration := registerStandingRuntimeContextSignal(t, primary.Pipeline, primaryFact, "primary", &primarySignals)
			selectedRegistration := registerStandingRuntimeContextSignal(t, selected.Pipeline, selectedFact, "selected", &selectedSignals)
			t.Cleanup(primaryRegistration.Release)
			t.Cleanup(selectedRegistration.Release)

			controller := &serveStandingServiceController{manager: manager}
			handlers := apiv1.OperatorStandingServiceHandlers(apiv1.StandingServiceHandlerOptions{
				Controller:  controller,
				Idempotency: selectedStores.Idempotency(),
			})
			invoke := func(action string) standingRuntimeContextOperationResult {
				t.Helper()
				method := "standing." + action
				handler := handlers[method]
				if handler == nil {
					t.Fatalf("%s handler is unavailable", method)
				}
				req := apiv1.Request{
					Method: method, ActorTokenID: "standing-owner-test", RequestHash: "request-" + action,
					Params: map[string]any{"service_id": serviceID, "reason": "selected-owner-test", "idempotency_key": "idem-" + action},
				}
				requestCtx := runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.RuntimeScope(runtimeInstanceID))
				requestCtx = runtimecorrelation.WithRuntimeInstanceID(requestCtx, runtimeInstanceID)
				first, err := handler(requestCtx, req)
				if err != nil {
					t.Fatalf("%s selected operation: %v", method, err)
				}
				replay, err := handler(requestCtx, req)
				if err != nil {
					t.Fatalf("%s selected replay: %v", method, err)
				}
				if !reflect.DeepEqual(first, replay) {
					t.Fatalf("%s replay = %#v, want %#v", method, replay, first)
				}
				body, err := json.Marshal(first)
				if err != nil {
					t.Fatalf("marshal %s result: %v", method, err)
				}
				var result standingRuntimeContextOperationResult
				if err := json.Unmarshal(body, &result); err != nil {
					t.Fatalf("decode %s result: %v", method, err)
				}
				return result
			}

			suspended := invoke("suspend")
			if suspended.Generation != 1 || suspended.EffectiveState != "suspended" || selectedSignals.Load() != 1 {
				t.Fatalf("selected suspend = %#v signals=%d, want generation 1 suspended and one signal", suspended, selectedSignals.Load())
			}
			resumed := invoke("resume")
			if resumed.RunID != suspended.RunID || resumed.Generation != 1 || resumed.EffectiveState != "active" || selectedSignals.Load() != 1 {
				t.Fatalf("selected resume = %#v signals=%d, want same generation active and no terminal signal", resumed, selectedSignals.Load())
			}
			reset := invoke("reset")
			if reset.RunID == resumed.RunID || reset.Generation != 2 || reset.EffectiveState != "active" || selectedSignals.Load() != 2 {
				t.Fatalf("selected reset = %#v signals=%d, want generation 2 active and second signal", reset, selectedSignals.Load())
			}
			if primarySignals.Load() != 0 {
				t.Fatalf("primary delivery continuation signals = %d, want 0", primarySignals.Load())
			}
			primaryStatuses, err := primary.Pipeline.ListStandingServiceStatuses(context.Background())
			if err != nil {
				t.Fatalf("list primary standing statuses: %v", err)
			}
			if len(primaryStatuses) != 0 {
				t.Fatalf("primary standing store was mutated: %#v", primaryStatuses)
			}
			selectedStatuses, err := selected.Pipeline.ListStandingServiceStatuses(selectedCtx)
			if err != nil {
				t.Fatalf("list selected standing statuses: %v", err)
			}
			if len(selectedStatuses) != 1 || selectedStatuses[0].BundleHash != selectedHash || selectedStatuses[0].Generation != 2 {
				t.Fatalf("selected standing source/generation = %#v", selectedStatuses)
			}
		})
	}
}

func seedStandingRuntimeContextBundle(t *testing.T, writer sourceArtifactDataWriter, bundle *runtimecontracts.WorkflowContractBundle) {
	t.Helper()
	catalog, err := runtimecontracts.BuildDurableDataCatalog(bundle)
	if err != nil {
		t.Fatalf("project standing runtime-context source data: %v", err)
	}
	if _, err := writer.EnsureSourceArtifactWithData(context.Background(), bundle.SourceArtifact, catalog); err != nil {
		t.Fatalf("persist standing runtime-context bundle: %v", err)
	}
}

func openStandingRuntimeContextStore(t *testing.T, backend, suffix string) *selectedStoreOwner {
	t.Helper()
	switch backend {
	case "sqlite":
		path := filepath.Join(t.TempDir(), suffix+".sqlite")
		stores := openSelectedSQLiteOwner(t, path, &config.Config{})
		t.Cleanup(func() { closeUnactivatedSelectedStore(t, stores) })
		spec, err := loadServePlatformSpecDocument(filepath.Join(repoRootForTest(), defaultPlatformSpecPath))
		if err != nil {
			t.Fatalf("load platform spec for %s SQLite store: %v", suffix, err)
		}
		plans, err := store.GeneratePlatformTableDDLs(spec)
		if err != nil {
			t.Fatalf("generate platform schema for %s SQLite store: %v", suffix, err)
		}
		request, err := schemaBootstrapRequest(spec, plans, nil)
		if err != nil {
			t.Fatalf("build schema request for %s SQLite store: %v", suffix, err)
		}
		if err := ensureServeSchemaTables(context.Background(), stores.Schema(), request); err != nil {
			t.Fatalf("bootstrap %s SQLite store: %v", suffix, err)
		}
		return stores
	case "postgres":
		dsn, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		stores := openSelectedPostgresOwner(t, dsn, db, &config.Config{})
		t.Cleanup(func() { closeUnactivatedSelectedStore(t, stores) })
		return stores
	default:
		t.Fatalf("unsupported standing runtime-context backend %q", backend)
		return nil
	}
}

func newStandingRuntimeContextRuntime(
	t *testing.T,
	process *worklifetime.Process,
	stores *selectedStoreOwner,
	module runtimepipeline.WorkflowModule,
	fact runtimecorrelation.SourceArtifactFact,
	runtimeInstanceID string,
	catalog *providertriggers.CatalogSnapshot,
) *runtimepkg.Runtime {
	t.Helper()
	credentials := processIngressCredentialStore{
		"telegram_bot_token":       "standing-owner-token",
		"webhook_signing.telegram": "standing-owner-signing-secret",
	}
	deps := runtimeDepsForServeTest(t, stores, &config.Config{}, runtimepkg.RuntimeOptions{
		WorkflowModule: module, SourceArtifactFact: fact, RuntimeInstanceID: runtimeInstanceID,
		ProcessWorkOwner: process, ProviderTriggerCatalog: catalog,
		Credentials: credentials, ProviderCredentials: credentials,
		DisablePersistentStartupRecovery: true, LLMRuntime: servedNoopLLMRuntime{},
	})
	rt, err := runtimepkg.NewRuntime(context.Background(), deps)
	if err != nil {
		t.Fatalf("build runtime context %s: %v", fact.BundleHash(), err)
	}
	if err := rt.PrepareAuthorActivityCatalog(); err != nil {
		t.Fatalf("prepare runtime context author activity %s: %v", fact.BundleHash(), err)
	}
	t.Cleanup(func() { _ = rt.Shutdown() })
	return rt
}

func registerStandingRuntimeContextSignal(
	t *testing.T,
	pipeline *runtimepipeline.PipelineCoordinator,
	fact runtimecorrelation.SourceArtifactFact,
	owner string,
	signals *atomic.Int32,
) *runtimepipeline.DeliveryContinuationSignalRegistration {
	t.Helper()
	authority, err := runtimedelivery.NewNormalExecutionAuthority(fact, "standing-runtime-context-"+owner, 1)
	if err != nil {
		t.Fatalf("build %s delivery authority: %v", owner, err)
	}
	registration, err := pipeline.RegisterDeliveryContinuationSignal(authority, func() { signals.Add(1) })
	if err != nil {
		t.Fatalf("register %s delivery continuation signal: %v", owner, err)
	}
	return registration
}
