package serveapp

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe/lifecycletest"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

// This is a real startup subscription barrier, not a replacement for Start or
// the selected-store lifecycle owners. Cancellation fails the actual startup.
type resetCandidateSubscriptionBarrier struct {
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
	hooks   []func()
}

func (b *resetCandidateSubscriptionBarrier) String() string { return "reset-candidate-barrier" }
func (b *resetCandidateSubscriptionBarrier) AddSubscriptionReadyHook(hook func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.hooks = append(b.hooks, hook)
}
func (b *resetCandidateSubscriptionBarrier) Run(ctx context.Context) {
	close(b.entered)
	select {
	case <-ctx.Done():
		return
	case <-b.release:
	}
	b.mu.Lock()
	hooks := append([]func(){}, b.hooks...)
	b.mu.Unlock()
	for _, hook := range hooks {
		hook()
	}
	<-ctx.Done()
}

func TestResetCandidateSetFencesConsumersWhileSecondPreparesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, failSecond := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/fail_second=%t", backend, failSecond), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				stores := openStandingRuntimeContextStore(t, backend, "reset-candidates")
				cfg, err := config.Load(writeServeRuntimeTestConfig(t))
				if err != nil {
					t.Fatal(err)
				}
				stubServeRuntimeWorkspaceLifecycle(t)
				process := worklifetime.NewProcess()
				instance := uuid.NewString()
				ctx = correlation.WithRuntimeInstanceID(ctx, instance)
				ctx = authoractivity.WithScope(ctx, authoractivity.RuntimeScope(instance))
				capability, err := stores.StartupOwnership().AcquireProcessCapability(ctx, startupownership.AcquireRequest{
					OwnerID: "reset-candidates:" + instance, BootID: uuid.NewString(), RuntimeInstanceID: instance,
				})
				if err != nil {
					t.Fatal(err)
				}
				var candidates []serveRuntimeBundleContext
				var manager *runtime.RuntimeContextManager
				managed := false
				t.Cleanup(func() {
					cancel()
					var joined = true
					if managed {
						if err := manager.QuiesceAllRuntimeContexts(context.Background()); err != nil {
							t.Error(err)
							joined = false
						}
					} else {
						for _, candidate := range candidates {
							if err := candidate.runtime.Shutdown(); err != nil {
								t.Error(err)
								joined = false
							}
						}
					}
					if err := closeSelectedStoreTestProcess(process, capability); err != nil {
						t.Error(err)
						joined = false
					}
					if joined {
						for _, candidate := range candidates {
							if err := candidate.loaded.cleanup(); err != nil {
								t.Error(err)
							}
						}
					}
				})
				credentials := processIngressCredentialStore{"telegram_bot_token": "reset-test-token", "webhook_signing.telegram": "reset-test-secret"}
				probe := lifecycletest.New(t, lifecycletest.WithTimeout(servedEventPublishLifecycleProbeWaitTimeout))
				for _, root := range []string{writeStandingTelegramServeFixture(t, "http://127.0.0.1:1"), writeServedEventPublishFollowUpFixture(t)} {
					loaded, err := loadServeRuntimeBundle(ctx, repoRootForTest(), stores.SourceArtifactStore(), cliapp.CLISourcePlatformSpecPaths{
						SourceRoot: root, PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath),
					}, cliapp.ServeOptions{}, testPlatformPackBaseGenerations(t))
					if err != nil {
						t.Fatal(err)
					}
					candidate, err := buildServeRuntimeBundleContext(serveRuntimeBundleContextRequest{
						Ctx: ctx, Stores: projectServeRuntimePersistence(stores), Config: cfg, Loaded: loaded,
						WorkspaceBackend:       cliapp.WorkspaceBackendSelection{Backend: "host"},
						ProviderTriggerCatalog: testProviderTriggerCatalog(t), ProcessWorkOwner: process, RuntimeInstanceID: instance,
						Credentials: credentials, ProviderCredentials: credentials,
						Options: cliapp.ServeOptions{TestLLMRuntime: servedNoopLLMRuntime{}, TestLifecycleProbe: probe, ShutdownGrace: 5 * time.Second},
					})
					if err != nil {
						_ = loaded.cleanup()
						t.Fatal(err)
					}
					candidates = append(candidates, candidate)
				}
				plan, err := compileServeSourceSetPlan(candidates)
				if err != nil {
					t.Fatal(err)
				}
				if err := installServeSourceSet(ctx, capability, plan); err != nil {
					t.Fatal(err)
				}
				for _, candidate := range candidates {
					installSelectedStoreTestGeneration(t, capability, candidate.runtime, plan, 1)
				}
				reconciled, err := reconcileServeStandingServices(ctx, candidates[0].runtime.Pipeline, candidates)
				if err != nil {
					t.Fatal(err)
				}
				for i := range candidates {
					targets, activations, err := reconcileServeRuntimeStandingTargets(candidates[i].runtime, reconciled)
					if err != nil {
						t.Fatal(err)
					}
					candidates[i].startupStandingTargets, candidates[i].startupStandingActivations = targets, activations
				}
				definitions, err := plannedServeRuntimeContexts(candidates)
				if err != nil {
					t.Fatal(err)
				}
				manager, err = runtime.NewRuntimeContextManager(stores.RunBundleAvailability())
				if err != nil {
					t.Fatal(err)
				}
				if err := manager.StageRecoveredRuntimeContexts(definitions...); err != nil {
					t.Fatal(err)
				}
				managed = true
				supervisor := newProcessLifecycleSupervisor(nil, candidates[0].runtime)
				supervisor.SetRuntimeContextManager(manager, candidates[0].sourceArtifactFact)
				supervisor.resetting = true
				primary := candidates[0].runtime
				supervisor.execution = apiv1.OperatorEventPublishHandlers(apiv1.EventPublishHandlerOptions{Publication: apiv1.EventPublicationOptions{
					ExecutionPosture: primary.ExecutionPosture, Idempotency: stores.Idempotency(),
					Events: primary.Bus, Acknowledged: primary.Bus, RecipientPlans: primary.Bus, SourceArtifact: primary.Bus,
					Runs: stores.Runs(), Entities: stores.Entities(), Observability: stores.Observability(),
					RunBundleContext: stores.RunBundleContext(), RuntimeContexts: manager,
					Source: candidates[0].loaded.source, Bundle: candidates[0].bootIdentity,
				}})
				if supervisor.execution["event.publish"] == nil {
					t.Fatal("canonical publication handler is not configured")
				}
				handler, err := apiv1.NewHandler(apiv1.Options{
					PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath),
					AuthTokens:       []string{apiv1.DefaultLoopbackAPIToken}, ProcessWorkOwner: process,
					Handlers: supervisor.executionDispatch(),
				})
				if err != nil {
					t.Fatal(err)
				}
				server := httptest.NewServer(handler)
				t.Cleanup(server.Close)
				barrier := &resetCandidateSubscriptionBarrier{entered: make(chan struct{}), release: make(chan struct{})}
				candidates[1].runtime.SystemNodes = append(candidates[1].runtime.SystemNodes, barrier)
				type result struct {
					starts []*runtime.PreparedStartup
					err    error
				}
				done := make(chan result, 1)
				finished := make(chan struct{})
				go func() {
					defer close(finished)
					starts, err := startResetServeRuntimeContexts(ctx, candidates, manager)
					done <- result{starts, err}
				}()
				t.Cleanup(func() {
					cancel()
					select {
					case <-finished:
					case <-time.After(20 * time.Second):
						t.Error("startup worker did not join before fixture cleanup")
					}
				})
				select {
				case <-barrier.entered:
				case result := <-done:
					t.Fatalf("startup returned before second barrier: %v", result.err)
				case <-time.After(20 * time.Second):
					t.Fatal("second candidate did not reach subscription readiness")
				}
				for _, candidate := range candidates {
					grant, err := candidate.runtime.CurrentStartupGrantEvidence()
					if err != nil || grant.State != startupownership.GrantPrepared {
						t.Fatalf("candidate admitted before complete-set convergence: %+v, %v", grant, err)
					}
					use, lookup, err := manager.AcquireBundleHash(ctx, candidate.sourceArtifactFact.BundleHash())
					if use != nil {
						_ = use.Done()
					}
					if use != nil || (lookup.Loaded() && err == nil) {
						t.Fatal("primary/secondary execution escaped the complete-set fence")
					}
					response := requestServedJSONRPC(t, server.URL+"/v1/rpc", "event.publish", map[string]any{
						"event_name": "item.received", "bundle_hash": candidate.sourceArtifactFact.BundleHash(),
						"payload": map[string]any{"item_id": "must-not-run"}, "idempotency_key": uuid.NewString(),
					})
					if response.Error == nil || response.Error.Data["code"] != apiv1.BundleUnavailableCode {
						t.Fatalf("API escaped reset fence: %+v", response.Error)
					}
				}
				if len(candidates[0].startupStandingTargets) == 0 {
					t.Fatal("standing probe has no actual service")
				}
				target := candidates[0].startupStandingTargets[0]
				if target.Alias == "" || target.Provider == "" {
					t.Fatal("channel probe lacks its real standing ingress identity")
				}
				if use, _, err := manager.AcquireIngress(ctx, target.Alias, target.Provider); use != nil {
					_ = use.Done()
					t.Fatalf("channel ingress escaped reset fence: %v", err)
				}
				controller := &serveStandingServiceController{manager: manager, supervisor: supervisor}
				if _, err := controller.SuspendStandingService(ctx, pipeline.StandingServiceOperation{ServiceID: candidates[0].startupStandingTargets[0].ServiceID}); err == nil {
					t.Fatal("standing control escaped reset")
				}
				if use, err := supervisor.acquireCurrentRuntime(ctx); err == nil || use != nil {
					if use != nil {
						use.Done()
					}
					t.Fatal("dynamic control escaped reset")
				}
				recorder := httptest.NewRecorder()
				supervisor.serveMCP(recorder, httptest.NewRequest(http.MethodPost, "/mcp", nil))
				if recorder.Code != http.StatusServiceUnavailable {
					t.Fatalf("MCP admitted while second candidate blocked: %d", recorder.Code)
				}
				var bootEvents int
				if err := selectedStoreDatabaseForTest(t, stores).QueryRow("SELECT COUNT(*) FROM events WHERE event_name = 'platform.boot'").Scan(&bootEvents); err != nil || bootEvents != 0 {
					t.Fatalf("autonomous boot escaped: %d, %v", bootEvents, err)
				}
				if failSecond {
					cancel()
				} else {
					close(barrier.release)
				}
				var prepared result
				select {
				case prepared = <-done:
				case <-time.After(20 * time.Second):
					t.Fatal("candidate preparation did not settle")
				}
				if failSecond {
					if prepared.err == nil {
						t.Fatal("canceled second candidate succeeded")
					}
					return
				}
				if prepared.err != nil {
					t.Fatal(prepared.err)
				}
				if err := manager.ReleaseResetExecution(definitions...); err != nil {
					t.Fatal(err)
				}
				for _, start := range prepared.starts {
					if err := start.Start(); err != nil {
						t.Fatal(err)
					}
				}
				supervisor.mu.Lock()
				supervisor.resetting = false
				supervisor.mu.Unlock()
				published := requireServedEventPublishRPCResult(t, server.URL+"/v1/rpc", map[string]any{
					"event_name": "item.received", "bundle_hash": candidates[1].sourceArtifactFact.BundleHash(),
					"payload": map[string]any{"item_id": "after-convergence"}, "idempotency_key": uuid.NewString(),
				})
				waitForServedEventPublishNodeDeliveryLifecycle(t, selectedStoreDatabaseForTest(t, stores), backend, published.RunID, published.EventID, probe)
				for _, candidate := range candidates {
					grant, err := candidate.runtime.CurrentStartupGrantEvidence()
					if err != nil || grant.State != startupownership.GrantAdmitted {
						t.Fatalf("converged candidate failed to execute: %+v, %v", grant, err)
					}
				}
			})
		}
	}
}
