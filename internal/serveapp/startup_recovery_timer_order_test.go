package serveapp

import (
	"context"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

type startupTimerPublicationBarrier struct {
	entered chan lifecycleprobe.Signal
	release chan struct{}
	once    sync.Once
}

func (b *startupTimerPublicationBarrier) unblock() { b.once.Do(func() { close(b.release) }) }

func (b *startupTimerPublicationBarrier) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if signal.Kind != lifecycleprobe.EventPersisted || signal.EventType != "platform.stage_timer" {
		return
	}
	select {
	case b.entered <- signal:
	default:
	}
	select {
	case <-b.release:
	case <-ctx.Done():
	}
}

func TestComposedStartupWithholdsStandingTimerPublicationUntilRecoveryOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			stores := openStandingRuntimeContextStore(t, backend, "recovery-timer-order")
			cfg, err := config.Load(writeServeRuntimeTestConfig(t))
			if err != nil {
				t.Fatal(err)
			}
			cfg.Runtime.RecoveryOnStartup = true
			stubServeRuntimeWorkspaceLifecycle(t)
			process := worklifetime.NewProcess()
			instance := uuid.NewString()
			ctx = correlation.WithRuntimeInstanceID(ctx, instance)
			ctx = authoractivity.WithScope(ctx, authoractivity.RuntimeScope(instance))
			capability, err := stores.StartupOwnership().AcquireProcessCapability(ctx, startupownership.AcquireRequest{
				OwnerID: "recovery-timer:" + instance, BootID: uuid.NewString(), RuntimeInstanceID: instance,
			})
			if err != nil {
				t.Fatal(err)
			}
			barrier := &startupTimerPublicationBarrier{entered: make(chan lifecycleprobe.Signal, 1), release: make(chan struct{})}
			var candidates []serveRuntimeBundleContext
			manager, err := runtime.NewRuntimeContextManager(stores.RunBundleAvailability())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				barrier.unblock()
				cancel()
				if err := manager.QuiesceAllRuntimeContexts(context.Background()); err != nil {
					t.Error(err)
				}
				for _, candidate := range candidates {
					if err := candidate.runtime.Shutdown(); err != nil {
						t.Error(err)
					}
				}
				if err := closeSelectedStoreTestProcess(process, capability); err != nil {
					t.Error(err)
				}
				for _, candidate := range candidates {
					if err := candidate.loaded.cleanup(); err != nil {
						t.Error(err)
					}
				}
			})
			root := filepath.Join(repoRootForTest(), "internal/releasee2e/testdata/full_lifecycle/standing_telegram")
			loaded, err := loadServeRuntimeBundle(ctx, repoRootForTest(), stores.SourceArtifactStore(), cliapp.CLISourcePlatformSpecPaths{
				SourceRoot: root, PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath),
			}, cliapp.ServeOptions{}, testPlatformPackBaseGenerations(t))
			if err != nil {
				t.Fatal(err)
			}
			credentials := processIngressCredentialStore{"telegram_bot_token": "timer-test-token", "webhook_signing.telegram": "timer-test-secret"}
			binding, err := createServeToolGatewayBinding(&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8081})
			if err != nil {
				t.Fatal(err)
			}
			candidate, err := buildServeRuntimeBundleContext(serveRuntimeBundleContextRequest{
				UseStartupRecovery: true,
				ExecutionPosture:   executionposture.MockOnly, Ctx: ctx, Stores: projectServeRuntimePersistence(stores), Config: cfg, Loaded: loaded,
				WorkspaceBackend: cliapp.WorkspaceBackendSelection{Backend: "host"}, EnableToolGateway: true, ToolGatewayBinding: binding,
				ProviderTriggerCatalog: testProviderTriggerCatalog(t), ProcessWorkOwner: process, RuntimeInstanceID: instance,
				Credentials: credentials, ProviderCredentials: credentials,
				Options: cliapp.ServeOptions{TestLLMRuntime: servedNoopLLMRuntime{}, TestLifecycleProbe: barrier, ShutdownGrace: 5 * time.Second},
			})
			if err != nil {
				_ = loaded.cleanup()
				t.Fatal(err)
			}
			candidates = append(candidates, candidate)
			plan, err := compileServeSourceSetPlan(candidates)
			if err != nil {
				t.Fatal(err)
			}
			if err := installServeSourceSet(ctx, capability, plan); err != nil {
				t.Fatal(err)
			}
			installSelectedStoreTestGeneration(t, capability, candidate.runtime, plan, 1)
			reconciled, err := reconcileServeStandingServices(ctx, candidate.runtime.Pipeline, candidates)
			if err != nil {
				t.Fatal(err)
			}
			targets, activations, err := reconcileServeRuntimeStandingTargets(candidate.runtime, reconciled)
			if err != nil {
				t.Fatal(err)
			}
			candidates[0].startupStandingTargets, candidates[0].startupStandingActivations = targets, activations
			checked := false
			candidate.runtime.Options.BootProgress = func(event runtime.BootProgressEvent) {
				if event.Step == 10 && event.Status == "ok" {
					checked = true
					select {
					case signal := <-barrier.entered:
						t.Errorf("standing timer published before pipeline recovery: event_id=%s type=%s run_id=%s", signal.EventID, signal.EventType, activations[0].RunID)
					case <-time.After(500 * time.Millisecond):
					}
				}
				if event.Step == 16 && event.Status == "FAILED" {
					barrier.unblock()
				}
			}
			release, err := prepareServeRuntimeContexts(ctx, candidates, manager)
			if err != nil {
				t.Fatal(err)
			}
			if err := release(); err != nil {
				t.Fatalf("real composed startup recovery: %v", err)
			}
			if !checked {
				t.Fatal("pipeline recovery entrance was not observed")
			}
			select {
			case signal := <-barrier.entered:
				t.Logf("standing timer released after recovery: event_id=%s type=%s run_id=%s", signal.EventID, signal.EventType, activations[0].RunID)
			case <-time.After(5 * time.Second):
				t.Fatal("standing timer did not retain future execution authority")
			}
			barrier.unblock()
		})
	}
}
