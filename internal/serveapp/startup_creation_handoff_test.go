package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type startupCreationPublicationBarrier struct {
	entered   chan lifecycleprobe.Signal
	completed chan lifecycleprobe.Signal
	release   chan struct{}
	once      sync.Once
}

func (b *startupCreationPublicationBarrier) unblock() { b.once.Do(func() { close(b.release) }) }

func (b *startupCreationPublicationBarrier) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	if !strings.HasSuffix(signal.EventType, "/worker.created") {
		return
	}
	if signal.Kind == lifecycleprobe.PostCommitDispatchCompleted {
		select {
		case b.completed <- signal:
		default:
		}
		return
	}
	if signal.Kind != lifecycleprobe.PostCommitDispatchStarted {
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

type startupCreationDiagnostics struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *startupCreationDiagnostics) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}

func (b *startupCreationDiagnostics) evidence(t *testing.T, eventID string) {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, line := range strings.Split(b.Buffer.String(), "\n") {
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) == nil && record["event_id"] == eventID {
			t.Logf("exact creation diagnostic: %s", line)
		}
	}
}

func TestComposedStartupCreationPublicationHandoffOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, scenario := range []string{"startup_recovery", "ordinary_async", "recovery_disabled", "unknown_dispatch_mode"} {
			t.Run(backend+"/"+scenario, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				stores := openStandingRuntimeContextStore(t, backend, "startup-creation-handoff")
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
					OwnerID: "creation-handoff:" + instance, BootID: uuid.NewString(), RuntimeInstanceID: instance,
				})
				if err != nil {
					t.Fatal(err)
				}
				barrier := &startupCreationPublicationBarrier{entered: make(chan lifecycleprobe.Signal, 1), completed: make(chan lifecycleprobe.Signal, 1), release: make(chan struct{})}
				diagnostics := &startupCreationDiagnostics{}
				previousLogger := slog.Default()
				slog.SetDefault(slog.New(slog.NewJSONHandler(diagnostics, nil)))
				t.Cleanup(func() { slog.SetDefault(previousLogger) })
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
				root := t.TempDir()
				copyReleaseFixtureTree(t, filepath.Join(repoRootForTest(), "tests/tier5-flow-lifecycle/test-auto-emit-on-create"), root)
				writeStandingCandidateFile(t, filepath.Join(root, "nodes.yaml"), "spawner:\n  execution_type: system_node\n  subscribes_to: [flow.created]\n  produces: []\n  event_handlers:\n    flow.created:\n      advances_to: idle\n")
				for _, name := range []string{"schema.yaml", "events.yaml", "nodes.yaml"} {
					path := filepath.Join(root, "worker", name)
					raw, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					writeStandingCandidateFile(t, path, strings.ReplaceAll(string(raw), "auto.started", "worker.created"))
				}
				loaded, err := loadServeRuntimeBundle(ctx, repoRootForTest(), stores.SourceArtifactStore(), cliapp.CLISourcePlatformSpecPaths{
					SourceRoot: root, PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath),
				}, cliapp.ServeOptions{}, testPlatformPackBaseGenerations(t))
				if err != nil {
					t.Fatal(err)
				}
				binding, err := createServeToolGatewayBinding(&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 8081})
				if err != nil {
					t.Fatal(err)
				}
				persistence := projectServeRuntimePersistence(stores)
				credentials := processIngressCredentialStore{}
				buildRequest := serveRuntimeBundleContextRequest{
					UseStartupRecovery: true,
					ExecutionPosture:   executionposture.MockOnly, Ctx: ctx, Stores: persistence, Config: cfg, Loaded: loaded,
					WorkspaceBackend: cliapp.WorkspaceBackendSelection{Backend: "host"}, EnableToolGateway: true, ToolGatewayBinding: binding,
					ProviderTriggerCatalog: testProviderTriggerCatalog(t), ProcessWorkOwner: process, RuntimeInstanceID: instance,
					Credentials: credentials, ProviderCredentials: credentials,
					Options: cliapp.ServeOptions{TestLLMRuntime: servedNoopLLMRuntime{}, TestLifecycleProbe: barrier, ShutdownGrace: 5 * time.Second},
				}
				candidate, err := buildServeRuntimeBundleContext(buildRequest)
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
				startPredecessor, err := prepareServeRuntimeContexts(ctx, []serveRuntimeBundleContext{candidate}, manager)
				if err != nil {
					t.Fatalf("prepare predecessor: %v", err)
				}
				if err := startPredecessor(); err != nil {
					t.Fatalf("start predecessor: %v", err)
				}
				db, _, _ := selectedRuntimeStoreForTest(t, persistence)
				runID := uuid.NewString()
				fact := candidate.runtime.Options.SourceArtifactFact
				seed := runlifecyclefixture.Fixture{RunID: runID, Origin: runlifecyclefixture.ScenarioSetupOrigin(), Source: fact, Artifact: loaded.bundle.SourceArtifact}
				if backend == "sqlite" {
					runlifecyclefixture.RequireSQLite(t, ctx, db, seed)
				} else {
					runlifecyclefixture.RequirePostgres(t, ctx, db, seed)
				}
				activationCtx := correlation.WithRunID(correlation.WithSourceArtifactFact(ctx, fact), runID)
				activationCtx = authoractivity.WithScope(activationCtx, authoractivity.BundleScope(instance, fact.BundleHash()))
				trigger := eventtest.InExecutionMode(eventtest.RuntimeControl(uuid.NewString(), "flow.created", "test", "", json.RawMessage(`{"instance_id":"startup-worker"}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC()), executionmode.Mock)
				if err := candidate.runtime.Bus.Publish(activationCtx, trigger); err != nil {
					t.Fatalf("persist causal activation trigger: %v", err)
				}
				activation, err := candidate.runtime.Manager.PrepareFlowInstanceActivation(activationCtx, pipeline.FlowInstanceActivationRequest{
					ContractBundle: loaded.source,
					Instance:       flowidentity.Instance{TemplateID: "worker", ScopeKey: "worker", InstanceID: "startup-worker", InstancePath: "worker/startup-worker", EntityID: uuid.NewString(), HasStoredPath: true},
					Config:         map[string]any{"instance_id": "startup-worker"}, TriggerEvent: trigger, OccurredAt: trigger.CreatedAt(),
				})
				if err != nil {
					t.Fatalf("prepare interrupted activation: %v", err)
				}
				if activation.Readiness.CreationEvent == nil {
					t.Fatal("supported activation did not derive a creation occurrence")
				}
				committed, err := candidate.runtime.Bus.CommitFlowInstanceActivation(activationCtx, activation)
				if err != nil {
					t.Fatalf("commit interrupted activation: %v", err)
				}
				creationID := activation.Readiness.CreationEvent.EventID
				if scenario == "unknown_dispatch_mode" {
					creation := activation.Readiness.CreationEvent
					event := eventtest.ChildWithLineage(creation.EventID, events.EventType(creation.EventType), "flow-instance-activator", "", creation.Payload, 1,
						events.EventLineage{RunID: creation.RunID, ParentEventID: creation.ParentEventID, ExecutionMode: creation.ExecutionMode}, events.EventEnvelope{}, creation.CreatedAt)
					err := candidate.runtime.Bus.CommitDynamicFlowRuntimeCreationOccurrence(activationCtx, pipeline.DynamicFlowRuntimeCreationOccurrenceRequest{
						RunID: runID, InstancePath: activation.Identity.InstancePath, Plan: activation.Readiness, Event: event, OccurredAt: creation.CreatedAt,
						DispatchMode: pipeline.DynamicFlowRuntimeCreationDispatchMode(255),
					})
					if err == nil || !strings.Contains(err.Error(), "unknown dynamic flow creation dispatch mode") {
						t.Fatalf("unknown creation dispatch mode was not refused before publication: %v", err)
					}
					if counts := readStartupCreationCounts(t, db, creationID); counts != (startupCreationCounts{}) {
						t.Fatalf("unknown dispatch mode changed durable occurrence: %+v", counts)
					}
					return
				}
				if scenario == "ordinary_async" {
					finalized := make(chan error, 1)
					go func() {
						finalized <- candidate.runtime.Manager.FinalizeCommittedFlowInstanceActivation(activationCtx, committed)
					}()
					select {
					case signal := <-barrier.entered:
						if signal.EventID != creationID {
							t.Errorf("ordinary creation event=%s, want %s", signal.EventID, creationID)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("ordinary creation did not enter asynchronous dispatch")
					}
					select {
					case err := <-finalized:
						if err != nil {
							t.Fatal(err)
						}
					case <-time.After(5 * time.Second):
						barrier.unblock()
						t.Fatal("readiness waited circularly for its creation receiver")
					}
					if counts := readStartupCreationCounts(t, db, creationID); counts.receipts != 0 || counts.handedOff != 0 {
						t.Fatalf("blocked ordinary receiver already settled: %+v", counts)
					}
					barrier.unblock()
					select {
					case <-barrier.completed:
					case <-time.After(5 * time.Second):
						t.Fatal("ordinary creation did not complete after exact receiver release")
					}
					if counts := readStartupCreationCounts(t, db, creationID); counts.receipts != 1 || counts.handedOff != counts.deliveries {
						t.Fatalf("ordinary creation did not settle: %+v", counts)
					}
					return
				}
				// Deliberately omit post-commit finalization: startup must recover this
				// canonical persisted activation and publish its creation occurrence.
				if err := manager.QuiesceAllRuntimeContexts(context.Background()); err != nil {
					t.Fatalf("quiesce predecessor: %v", err)
				}
				if err := candidate.runtime.Shutdown(); err != nil {
					t.Fatalf("shutdown predecessor: %v", err)
				}
				pending, found, err := candidate.runtime.Pipeline.LoadDynamicFlowRuntimeReadiness(activationCtx, runID, activation.Identity.Route())
				if err != nil || !found || !pending.Pending() || pending.Plan.CreationEvent == nil || !pending.CreationEventEmittedAt.IsZero() {
					t.Fatalf("interrupted creation must remain pending: found=%t readiness=%+v err=%v", found, pending, err)
				}
				loaded, err = loadServeRuntimeBundle(ctx, repoRootForTest(), stores.SourceArtifactStore(), cliapp.CLISourcePlatformSpecPaths{
					SourceRoot: root, PlatformSpecPath: filepath.Join(repoRootForTest(), defaultPlatformSpecPath),
				}, cliapp.ServeOptions{}, testPlatformPackBaseGenerations(t))
				if err != nil {
					t.Fatal(err)
				}
				buildRequest.Loaded = loaded
				if scenario == "recovery_disabled" {
					cfg.Runtime.RecoveryOnStartup = false
				}
				candidate, err = buildServeRuntimeBundleContext(buildRequest)
				if err != nil {
					_ = loaded.cleanup()
					t.Fatalf("build successor: %v", err)
				}
				candidates = append(candidates, candidate)
				manager, err = runtime.NewRuntimeContextManager(stores.RunBundleAvailability())
				if err != nil {
					t.Fatal(err)
				}
				installSelectedStoreTestGeneration(t, capability, candidate.runtime, plan, 2)
				observed := false
				candidate.runtime.Options.BootProgress = func(event runtime.BootProgressEvent) {
					if event.Step == 10 && event.Status == "ok" {
						observed = true
						select {
						case signal := <-barrier.entered:
							t.Errorf("startup creation dispatched before recovery: event_id=%s type=%s run_id=%s", signal.EventID, signal.EventType, runID)
						default:
						}
						ready, found, readErr := candidate.runtime.Pipeline.LoadDynamicFlowRuntimeReadiness(activationCtx, runID, activation.Identity.Route())
						if readErr != nil || !found || ready.Pending() || ready.CreationEventEmittedAt.IsZero() {
							t.Errorf("creation must be committed before recovery: found=%t readiness=%+v err=%v", found, ready, readErr)
						}
						counts := readStartupCreationCounts(t, db, creationID)
						if counts.events != 1 || counts.receipts != 0 || counts.deliveries == 0 || counts.handedOff != 0 {
							t.Errorf("premature creation settlement before recovery: %+v", counts)
						}
						t.Logf("creation committed without dispatch at recovery entrance: event_id=%s run_id=%s counts=%+v", creationID, runID, counts)
					}
					if event.Step == 16 && event.Status == "FAILED" {
						t.Logf("phase16 failure: %+v", event)
						barrier.unblock()
					}
				}
				release, err := prepareServeRuntimeContexts(ctx, []serveRuntimeBundleContext{candidate}, manager)
				if scenario == "recovery_disabled" {
					if err == nil || !strings.Contains(err.Error(), "requires recovery") {
						t.Fatalf("pending creation without recovery was not refused: %v", err)
					}
					if counts := readStartupCreationCounts(t, db, creationID); counts != (startupCreationCounts{}) {
						t.Fatalf("disabled recovery published or settled pending creation: %+v", counts)
					}
					return
				}
				if err != nil {
					t.Fatalf("prepare composed startup: %v", err)
				}
				err = release()
				barrier.unblock()
				diagnostics.evidence(t, creationID)
				if err != nil {
					t.Fatalf("real composed creation handoff: %v", err)
				}
				if !observed {
					t.Fatal("creation handoff was not observed at recovery entrance")
				}
				counts := readStartupCreationCounts(t, db, creationID)
				if counts.events != 1 || counts.receipts != 1 || counts.deliveries == 0 || counts.handedOff != counts.deliveries {
					t.Fatalf("canonical creation recovery did not settle exact persisted event: %+v", counts)
				}
				t.Logf("creation recovered after readiness: event_id=%s counts=%+v", creationID, counts)
			})
		}
	}
}

type startupCreationCounts struct{ events, receipts, deliveries, handedOff int }

func readStartupCreationCounts(t *testing.T, db *sql.DB, eventID string) startupCreationCounts {
	t.Helper()
	var counts startupCreationCounts
	err := db.QueryRowContext(context.Background(), `SELECT
		(SELECT COUNT(*) FROM events WHERE event_id = $1),
		(SELECT COUNT(*) FROM event_receipts WHERE event_id = $1 AND subscriber_type = 'platform' AND subscriber_id = 'pipeline'),
		(SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1),
		(SELECT COUNT(*) FROM event_deliveries WHERE event_id = $1 AND continuation_handoff_at IS NOT NULL)`, eventID).Scan(
		&counts.events, &counts.receipts, &counts.deliveries, &counts.handedOff)
	if err != nil {
		t.Fatalf("read exact creation settlement: %v", err)
	}
	return counts
}
