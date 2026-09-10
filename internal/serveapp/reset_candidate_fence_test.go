package serveapp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
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

// Exercise typed transport routing independently of provider cost. This does
// not widen the explicit unsupported multi-context Claude CLI posture.
func assertResetCandidateStartupMCP(t *testing.T, supervisor *processLifecycleSupervisor, rt *runtime.Runtime, auth string, grant startupownership.GrantEvidence) {
	t.Helper()
	identity := agentidentitytest.RootDeclared(t, "probe-agent", "reset-mcp-test")
	plan, err := identity.Plan()
	if err != nil {
		t.Fatal(err)
	}
	probeID := uuid.NewString()
	surface, err := managedcapabilities.New(managedcapabilities.Plan{
		ActorPlan: plan, RuntimeMode: "startup_probe", Provider: "claude_cli", Transport: "cli", ProviderContract: "claude-cli-test",
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityStartupProbe, ID: probeID, ExecutionKind: managedcapabilities.ExecutionNormalAgent,
			ExecutionAuthorityID: grant.GrantID, StartupOwnerID: grant.ProcessOwnerID, StartupGeneration: grant.RuntimeGeneration,
		},
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := actors.WithActor(context.Background(), actors.AgentConfig{ID: plan.AgentID(), FlowPath: plan.FlowInstance()})
	ctx = effects.WithAuthority(ctx, effects.Authority{
		Kind: effects.AuthorityStartupProbe, ID: probeID, ExecutionMode: effects.ExecutionModeLive,
		ExecutionOwner: grant.ProcessOwnerID, LeaseExpiresAt: time.Now().Add(time.Minute), FenceGeneration: grant.RuntimeGeneration,
		StartupProbe: effects.StartupProbeAuthority{
			ProbeID: probeID, ActorID: plan.AgentID(), StartupAuthorityID: grant.GrantID, StartupStateVersion: grant.StateVersion,
			ExecutionKind: string(managedcapabilities.ExecutionNormalAgent), ExecutionAuthorityID: grant.GrantID,
		},
	})
	token := rt.MCPTurns.RegisterTurnContextWithCapabilitySurface(ctx, time.Minute, surface)
	if token == "" {
		t.Fatal("failed to register typed startup probe")
	}
	defer rt.MCPTurns.UnregisterTurnContext(token)
	request := func() *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize"}`))
		r.Header.Set("Authorization", "Bearer "+auth)
		r.Header.Set("X-SWARM-Context-Token", token)
		return r
	}
	response := httptest.NewRecorder()
	supervisor.serveMCP(response, request())
	if response.Code != http.StatusOK {
		t.Fatalf("candidate startup probe rejected before convergence: %d %s", response.Code, response.Body.String())
	}
	rt.MCPTurns.UnregisterTurnContext(token)
	response = httptest.NewRecorder()
	supervisor.serveMCP(response, request())
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unregistered candidate probe escaped the execution fence: %d", response.Code)
	}
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
		for _, failure := range []string{"none", "second_preparation", "registered_release", "normal_registered_release", "normal_active_release", "reset_active_release", "normal_second_preparation"} {
			t.Run(fmt.Sprintf("%s/failure=%s", backend, failure), func(t *testing.T) {
				failSecond := failure == "second_preparation"
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
				binding, err := createServeToolGatewayBinding(&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8081})
				if err != nil {
					t.Fatal(err)
				}
				for _, root := range []string{writeStandingTelegramServeFixture(t, "http://127.0.0.1:1"), writeServedEventPublishFollowUpFixture(t)} {
					loaded, err := loadServeRuntimeBundle(ctx, repoRootForTest(), stores.SourceArtifactStore(), cliapp.CLISourcePlatformSpecPaths{
						SourceRoot: root, PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath),
					}, cliapp.ServeOptions{}, testPlatformPackBaseGenerations(t))
					if err != nil {
						t.Fatal(err)
					}
					candidate, err := buildServeRuntimeBundleContext(serveRuntimeBundleContextRequest{
						ExecutionPosture: executionposture.Live,
						Ctx:              ctx, Stores: projectServeRuntimePersistence(stores), Config: cfg, Loaded: loaded,
						WorkspaceBackend:       cliapp.WorkspaceBackendSelection{Backend: "host"},
						EnableToolGateway:      true,
						ToolGatewayBinding:     binding,
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
				if failure == "normal_second_preparation" {
					managed = true
					// All lifecycle preparation has succeeded by the first candidate's
					// preflight. Fail the later release-preparation entrance without
					// canceling the caller or replacing either real runtime.
					candidates[0].runtime.Options.BootProgress = func(event runtime.BootProgressEvent) {
						if event.Step == 15 && event.Status == "ok" {
							candidates[1].runtime.CloseAdmission()
						}
					}
					release, err := prepareServeRuntimeContexts(ctx, candidates, manager)
					if err == nil || release != nil || !strings.Contains(err.Error(), "shutdown") {
						t.Fatalf("partial registration did not retain independent startup refusal: %v", err)
					}
					for index, candidate := range candidates {
						if got := candidate.runtime.WorkOccurrence().ActiveCount(); got != 0 {
							t.Errorf("partially registered candidate retains %d leases", got)
						}
						use, lookup, _ := manager.AcquireBundleHash(ctx, candidate.sourceArtifactFact.BundleHash())
						if index == 0 && !lookup.Found {
							t.Error("failure occurred before the first real registration")
						}
						if use != nil {
							_ = use.Done()
							t.Error("partially registered candidate remains selectable")
						}
					}
					return
				}
				if failure == "normal_registered_release" || failure == "normal_active_release" {
					managed = true
					release, err := prepareServeRuntimeContexts(ctx, candidates, manager)
					if err != nil {
						t.Fatal(err)
					}
					assertRegisteredStartupAbort(t, cancel, release, candidates, manager, failure == "normal_active_release")
					return
				}
				if err := manager.StageRecoveredRuntimeContexts(definitions...); err != nil {
					t.Fatal(err)
				}
				managed = true
				supervisor := newProcessLifecycleSupervisor(nil, candidates[0].runtime)
				supervisor.SetRuntimeContextManager(manager, candidates[0].sourceArtifactFact)
				supervisor.resetContexts = candidates
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
					release func() error
					err     error
				}
				done := make(chan result, 1)
				finished := make(chan struct{})
				go func() {
					defer close(finished)
					release, err := prepareResetServeRuntimeContexts(ctx, candidates, manager)
					done <- result{release, err}
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
					assertResetCandidateStartupMCP(t, supervisor, candidate.runtime, binding.AuthToken(), grant)
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
				if failure == "registered_release" {
					assertRegisteredStartupAbort(t, cancel, prepared.release, candidates, manager, false)
					return
				}
				if err := manager.ReleaseResetExecution(definitions...); err != nil {
					t.Fatal(err)
				}
				if failure == "reset_active_release" {
					assertRegisteredStartupAbort(t, cancel, prepared.release, candidates, manager, true)
					return
				}
				if err := prepared.release(); err != nil {
					t.Fatal(err)
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

func assertRegisteredStartupAbort(t *testing.T, cancel context.CancelFunc, release func() error, candidates []serveRuntimeBundleContext, manager *runtime.RuntimeContextManager, active bool) {
	t.Helper()
	if candidates[0].runtime.WorkOccurrence().ActiveCount() == 0 {
		t.Fatal("registered composition has no standing child lease")
	}
	var accepted *runtime.RuntimeContextUse
	if active {
		target := candidates[0].startupStandingTargets[0]
		var err error
		accepted, _, err = manager.AcquireIngress(context.Background(), target.Alias, target.Provider)
		if err != nil || accepted == nil {
			t.Fatalf("acquire actual standing ingress work: %v", err)
		}
		defer func() { _ = accepted.Done() }()
	}
	cancel()
	aborted := make(chan error, 1)
	go func() { aborted <- release() }()
	if accepted != nil {
		select {
		case <-accepted.WorkContext().Done():
		case <-time.After(10 * time.Second):
			t.Fatal("composed abort did not retire accepted standing work")
		}
		select {
		case err := <-aborted:
			t.Fatalf("abort returned before accepted work settled: %v", err)
		default:
		}
		for _, candidate := range candidates {
			use, _, _ := manager.AcquireBundleHash(context.Background(), candidate.sourceArtifactFact.BundleHash())
			if use != nil {
				_ = use.Done()
				t.Error("sibling remained selectable while abort joined standing work")
			}
		}
		if err := accepted.Done(); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-aborted:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("composed release lost cancellation: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("composed release could not retire registered standing children")
	}
	for _, candidate := range candidates {
		if got := candidate.runtime.WorkOccurrence().ActiveCount(); got != 0 {
			t.Errorf("aborted candidate retains %d leases", got)
		}
		use, _, _ := manager.AcquireBundleHash(context.Background(), candidate.sourceArtifactFact.BundleHash())
		if use != nil {
			_ = use.Done()
			t.Error("aborted candidate remains selectable")
		}
		if err := candidate.runtime.Shutdown(); err != nil {
			t.Errorf("repeated shutdown: %v", err)
		}
	}
}
