package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
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

func TestStandingMutationsRemainFencedUntilResetConsumersConverge(t *testing.T) {
	manager, err := runtimepkg.NewRuntimeContextManager(nil)
	if err != nil {
		t.Fatal(err)
	}
	supervisor := newProcessLifecycleSupervisor(nil, nil)
	supervisor.resetting = true
	controller := &serveStandingServiceController{manager: manager, supervisor: supervisor}
	for _, call := range []func(context.Context, runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error){
		controller.SuspendStandingService, controller.ResumeStandingService, controller.ResetStandingService,
	} {
		if _, err := call(context.Background(), runtimepipeline.StandingServiceOperation{ServiceID: "unavailable"}); err == nil || !strings.Contains(err.Error(), "reset has not converged") {
			t.Fatalf("standing mutation bypassed reset fence: %v", err)
		}
	}
	supervisor.resetting = false
	release, err := controller.admitProcessTransition()
	if err != nil {
		t.Fatal(err)
	}
	locked := make(chan struct{})
	entering := make(chan struct{})
	go func() {
		close(entering)
		supervisor.mu.Lock()
		supervisor.resetting = true
		supervisor.mu.Unlock()
		close(locked)
	}()
	<-entering
	select {
	case <-locked:
		t.Fatal("reset passed an unsettled standing mutation")
	default:
	}
	release()
	select {
	case <-locked:
	case <-time.After(5 * time.Second):
		t.Fatal("reset did not acquire fence after standing mutation settled")
	}
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
			primaryBundle := &runtimecontracts.WorkflowContractBundle{
				Platform: selectedBundle.Platform,
			}
			if err := runtimecontracts.CompileWorkflowSemantics(primaryBundle); err != nil {
				t.Fatalf("admit primary control bundle: %v", err)
			}
			primaryModule := stubWorkflowModule{source: semanticview.Wrap(primaryBundle)}
			primary := newStandingRuntimeContextRuntime(t, process, primaryStores, primaryModule, primaryFact, runtimeInstanceID, catalog)
			faults := &standingRuntimeContextFaultOwner{}
			selected := newStandingRuntimeContextRuntime(t, process, selectedStores, selectedModule, selectedFact, runtimeInstanceID, catalog, faults)
			selected.InboundGateway = runtimepkg.NewInboundGateway(nil, nil, nil, executionposture.Live)
			capability, _, grant := installSelectedStoreTestProcessTopology(t, selectedStores, selected, selectedModule.SemanticSource(), selectedFact, runtimeInstanceID)
			t.Cleanup(func() {
				if err := selected.Shutdown(); err != nil {
					t.Error(err)
					return
				}
				if err := capability.Release(context.Background()); err != nil {
					t.Error(err)
				}
			})

			selectedCtx := runtimeauthoractivity.WithScope(context.Background(), runtimeauthoractivity.BundleScope(runtimeInstanceID, selectedHash))
			selectedCtx = runtimecorrelation.WithSourceArtifactFact(selectedCtx, selectedFact)
			selectedCtx = worklifetime.WithRuntimeOccurrence(selectedCtx, selected.WorkOccurrence())
			if _, err := grant.MarkProbesSettled(selectedCtx, nil); err != nil {
				t.Fatalf("settle selected standing probes: %v", err)
			}
			if _, err := grant.AdmitExecution(selectedCtx); err != nil {
				t.Fatalf("admit selected standing generation: %v", err)
			}
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

			controller := &serveStandingServiceController{manager: manager, supervisor: newProcessLifecycleSupervisor(nil, primary)}
			assertGateway := func(open bool) {
				t.Helper()
				wasOpen, err := selected.InboundGateway.FenceStandingServiceAdmission(serviceID)
				if err != nil || wasOpen != open {
					t.Fatalf("selected gateway admission: was_open=%t want=%t err=%v", wasOpen, open, err)
				}
				if open {
					if err := selected.InboundGateway.ReopenStandingServiceAdmission(serviceID); err != nil {
						t.Fatal(err)
					}
				}
			}
			assertTransitionFences := func(_ context.Context, operation runtimepipeline.StandingServiceOperation) error {
				if operation.Expected == nil {
					return errors.New("writer did not retain exact predecessor authority")
				}
				blocked, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				defer cancel()
				probe, err := selected.Bus.BeginPipelineParentTransition(blocked)
				if probe != nil {
					probe.Done()
					return errors.New("recovery entered during the standing writer transaction")
				}
				if !errors.Is(err, context.DeadlineExceeded) {
					return fmt.Errorf("observe selected pipeline exclusion: %w", err)
				}
				open, err := selected.InboundGateway.FenceStandingServiceAdmission(serviceID)
				if err != nil || open {
					return fmt.Errorf("writer reached an open gateway: open=%t err=%v", open, err)
				}
				return nil
			}
			faults.beforeCall = assertTransitionFences
			rollbackFault := errors.New("standing desired-state transaction refused")
			faults.before = rollbackFault
			if failed, err := controller.SuspendStandingService(selectedCtx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID}); !errors.Is(err, rollbackFault) || failed.CommittedMutation != "" {
				t.Fatalf("active rollback lost failure/outcome: result=%+v err=%v", failed, err)
			}
			faults.before = nil
			assertGateway(true)
			assertChild := func(present bool, runID string, generation int64) {
				t.Helper()
				origin, err := runtimerunlifecycle.StandingGenerationRunOrigin(serviceID, generation)
				if err != nil {
					t.Fatal(err)
				}
				lease, err := manager.BeginStandingRunRecovery(context.Background(), runID, origin)
				if present && err != nil {
					t.Fatalf("exact child is not admitted: %v", err)
				}
				if !present && err == nil {
					_ = lease.Done()
					t.Fatal("non-executable/failed publication acquired a child")
				}
				if lease != nil {
					if err := lease.Done(); err != nil {
						t.Fatal(err)
					}
				}
			}
			assertChild(true, targets[0].RunID, targets[0].Generation)
			origin, err := runtimerunlifecycle.StandingGenerationRunOrigin(serviceID, targets[0].Generation)
			if err != nil {
				t.Fatal(err)
			}
			held, err := manager.BeginStandingRunRecovery(selectedCtx, targets[0].RunID, origin)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = held.Done() })
			cancelCtx, cancel := context.WithCancel(selectedCtx)
			defer cancel()
			cancelledResult := make(chan error, 1)
			go func() {
				result, err := controller.SuspendStandingService(cancelCtx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID})
				if result.CommittedMutation != "" {
					err = errors.Join(err, fmt.Errorf("cancelled transition acknowledged %s", result.CommittedMutation))
				}
				cancelledResult <- err
			}()
			deadline := time.After(5 * time.Second)
			for {
				probe, err := manager.BeginStandingRunRecovery(selectedCtx, targets[0].RunID, origin)
				if errors.Is(err, worklifetime.ErrAdmissionFenced) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := probe.Done(); err != nil {
					t.Fatal(err)
				}
				select {
				case <-deadline:
					t.Fatal("standing command did not fence held work")
				case <-time.After(time.Millisecond):
				}
			}
			cancel()
			select {
			case err := <-cancelledResult:
				t.Fatalf("cancelled command returned before held child joined: %v", err)
			default:
			}
			if err := held.Done(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-cancelledResult:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled standing command = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("cancelled standing command did not settle after child joined")
			}
			assertChild(true, targets[0].RunID, targets[0].Generation)
			assertGateway(true)
			handlers := apiv1.OperatorStandingServiceHandlers(apiv1.StandingServiceHandlerOptions{
				Controller:  controller,
				Idempotency: selectedStores.Idempotency(),
			})
			serial := 0
			invoke := func(action string) standingRuntimeContextOperationResult {
				t.Helper()
				serial++
				method := "standing." + action
				handler := handlers[method]
				if handler == nil {
					t.Fatalf("%s handler is unavailable", method)
				}
				req := apiv1.Request{
					Method: method, ActorTokenID: "standing-owner-test", RequestHash: fmt.Sprintf("request-%s-%d", action, serial),
					Params: map[string]any{"service_id": serviceID, "reason": "selected-owner-test", "idempotency_key": fmt.Sprintf("idem-%s-%d", action, serial)},
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
			if repeated := invoke("suspend"); repeated.RunID != suspended.RunID || selectedSignals.Load() != 1 {
				t.Fatalf("fresh repeat suspend changed N or emitted another continuation: %+v", repeated)
			}
			assertChild(false, suspended.RunID, suspended.Generation)
			assertGateway(false)
			faults.before = rollbackFault
			if failed, err := controller.ResetStandingService(selectedCtx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID}); !errors.Is(err, rollbackFault) || failed.CommittedMutation != "" {
				t.Fatalf("no-child rollback lost failure/outcome: result=%+v err=%v", failed, err)
			}
			faults.before = nil
			assertChild(false, suspended.RunID, suspended.Generation)
			assertGateway(false)
			resetWhileSuspended := invoke("reset")
			if resetWhileSuspended.Generation != 2 || resetWhileSuspended.EffectiveState != "suspended" || selectedSignals.Load() != 2 {
				t.Fatalf("reset did not preserve suspension without a child: %+v", resetWhileSuspended)
			}
			resumed := invoke("resume")
			if resumed.RunID != resetWhileSuspended.RunID || resumed.Generation != 2 || resumed.EffectiveState != "active" || selectedSignals.Load() != 2 {
				t.Fatalf("selected resume = %#v signals=%d, want same suspended successor active and no terminal signal", resumed, selectedSignals.Load())
			}
			if repeated := invoke("resume"); repeated.RunID != resumed.RunID || selectedSignals.Load() != 2 {
				t.Fatalf("fresh repeat resume changed N or emitted another continuation: %+v", repeated)
			}
			assertGateway(true)
			reset := invoke("reset")
			if reset.RunID == resumed.RunID || reset.Generation != 3 || reset.EffectiveState != "active" || selectedSignals.Load() != 3 {
				t.Fatalf("selected reset = %#v signals=%d, want generation 3 active and third signal", reset, selectedSignals.Load())
			}
			assertChild(true, reset.RunID, reset.Generation)
			faults.beforeCall = func(ctx context.Context, operation runtimepipeline.StandingServiceOperation) error {
				if err := assertTransitionFences(ctx, operation); err != nil {
					return err
				}
				if operation.Expected == nil || operation.Expected.RunID != reset.RunID {
					return errors.New("terminal race did not capture the exact predecessor")
				}
				_, disposition, err := selectedStores.RuntimeDeps().EventBusDurable.RunLifecycle.MarkTerminalRun(ctx, runtimerunlifecycle.TerminalRequest{
					RunID: reset.RunID, State: runtimerunlifecycle.StateCancelled, EndedAt: time.Now().UTC(),
				})
				if err == nil && disposition != runtimerunlifecycle.MutationApplied {
					return fmt.Errorf("terminal race was not acknowledged: %s", disposition)
				}
				return err
			}
			refused, err := controller.SuspendStandingService(selectedCtx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID})
			faults.beforeCall = assertTransitionFences
			if err == nil || refused.CommittedMutation != "" || !strings.Contains(err.Error(), "admission remains closed") {
				t.Fatalf("terminal race revived the predecessor: result=%+v err=%v", refused, err)
			}
			assertChild(false, reset.RunID, reset.Generation)
			assertGateway(false)
			faults.before = rollbackFault
			if failed, err := controller.ResetStandingService(selectedCtx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID}); !errors.Is(err, rollbackFault) || failed.CommittedMutation != "" {
				t.Fatalf("terminal no-child rollback lost failure/outcome: result=%+v err=%v", failed, err)
			}
			faults.before = nil
			assertChild(false, reset.RunID, reset.Generation)
			assertGateway(false)
			reset = invoke("reset")
			if reset.Generation != 4 || reset.EffectiveState != "active" || selectedSignals.Load() != 3 {
				t.Fatalf("explicit reset did not replace the terminal predecessor: result=%+v signals=%d", reset, selectedSignals.Load())
			}
			assertChild(true, reset.RunID, reset.Generation)
			cleanupFault := errors.New("acknowledged standing cleanup failure")
			faults.after = cleanupFault
			committed, err := controller.SuspendStandingService(selectedCtx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID})
			if !errors.Is(err, cleanupFault) || committed.CommittedMutation != runtimerunlifecycle.MutationApplied || committed.RunID != reset.RunID || committed.Generation != reset.Generation || !committed.DeliveryContinuationRequired || selectedSignals.Load() != 4 {
				t.Fatalf("postcommit failure erased desired state: result=%+v err=%v signals=%d", committed, err, selectedSignals.Load())
			}
			faults.after = nil
			assertChild(false, reset.RunID, reset.Generation)
			assertGateway(false)
			publicationFault := errors.New("standing publication refused after acknowledged resume")
			faults.publication = publicationFault
			committed, err = controller.ResumeStandingService(selectedCtx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID})
			if !errors.Is(err, publicationFault) || committed.CommittedMutation != runtimerunlifecycle.MutationApplied || !committed.RestartDisposition.Executable() || committed.Generation != reset.Generation || committed.RunID != reset.RunID {
				t.Fatalf("publication failure erased committed resume: result=%+v err=%v", committed, err)
			}
			faults.publication = nil
			assertChild(false, reset.RunID, reset.Generation)
			assertGateway(false)
			if retried, err := controller.ResumeStandingService(selectedCtx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID}); err == nil || retried.CommittedMutation != "" {
				t.Fatalf("fresh retry hid missing executable child: result=%+v err=%v", retried, err)
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
			if len(selectedStatuses) != 1 || selectedStatuses[0].BundleHash != selectedHash || selectedStatuses[0].Generation != 4 {
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
	faults ...*standingRuntimeContextFaultOwner,
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
	if len(faults) != 0 {
		owner, ok := deps.RunBundleAvailability.(runtimepipeline.WorkflowPersistenceOwner)
		if !ok {
			t.Fatalf("selected store %T does not provide the complete workflow owner", deps.RunBundleAvailability)
		}
		faults[0].WorkflowPersistenceOwner = owner
		deps.WorkflowPersistence = runtimepipeline.NewWorkflowPersistence(faults[0])
	}
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

type standingRuntimeContextFaultOwner struct {
	runtimepipeline.WorkflowPersistenceOwner
	before, after, publication error
	beforeCall                 func(context.Context, runtimepipeline.StandingServiceOperation) error
}

func (o *standingRuntimeContextFaultOwner) SuspendStandingService(ctx context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	if o.beforeCall != nil {
		if err := o.beforeCall(ctx, operation); err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, err
		}
	}
	if o.before != nil {
		return runtimepipeline.StandingServiceReconciliation{}, o.before
	}
	result, err := o.WorkflowPersistenceOwner.SuspendStandingService(ctx, operation)
	return result, errors.Join(err, o.after)
}

func (o *standingRuntimeContextFaultOwner) ResetStandingService(ctx context.Context, operation runtimepipeline.StandingServiceOperation) (runtimepipeline.StandingServiceReconciliation, error) {
	if o.beforeCall != nil {
		if err := o.beforeCall(ctx, operation); err != nil {
			return runtimepipeline.StandingServiceReconciliation{}, err
		}
	}
	if o.before != nil {
		return runtimepipeline.StandingServiceReconciliation{}, o.before
	}
	return o.WorkflowPersistenceOwner.ResetStandingService(ctx, operation)
}

func (o *standingRuntimeContextFaultOwner) PublishStandingService(ctx context.Context, serviceID, runID string, generation int64) (int64, error) {
	if o.publication != nil {
		return 0, o.publication
	}
	return o.WorkflowPersistenceOwner.PublishStandingService(ctx, serviceID, runID, generation)
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
