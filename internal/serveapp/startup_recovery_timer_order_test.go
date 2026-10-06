package serveapp

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type startupTimerPublicationBarrier struct {
	entered   chan lifecycleprobe.Signal
	release   chan struct{}
	once      sync.Once
	eventType string
}

type startupClockReceiptFault struct {
	genericschedule.Store
	calls    int
	fault    error
	admitted []genericschedule.Activation
}

func (s *startupClockReceiptFault) AdmitGenericScheduleOutcome(ctx context.Context, command genericschedule.AdmissionCommand) (genericschedule.AdmissionCommit, error) {
	s.calls++
	commit, err := s.Store.AdmitGenericScheduleOutcome(ctx, command)
	if commit.Acknowledged {
		s.admitted = append(s.admitted, commit.Result.Activation)
		if s.calls == 2 {
			return genericschedule.AdmissionCommit{}, errors.Join(err, s.fault)
		}
	}
	return commit, err
}

func (b *startupTimerPublicationBarrier) unblock() { b.once.Do(func() { close(b.release) }) }

func (b *startupTimerPublicationBarrier) NotifyLifecycle(ctx context.Context, signal lifecycleprobe.Signal) {
	eventType := b.eventType
	if eventType == "" {
		eventType = "platform.stage_timer"
	}
	if signal.Kind != lifecycleprobe.EventPersisted || signal.EventType != eventType {
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
		for _, sourceCase := range []string{"workflow_timer", "root_clock", "child_clock", "partial_clock_receipt"} {
			t.Run(backend+"/"+sourceCase, func(t *testing.T) {
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
				if sourceCase != "workflow_timer" {
					root = canonicalrouting.CopyClockDeployment(t, sourceCase == "child_clock")
					barrier.eventType = "poll.tick"
					if sourceCase == "child_clock" {
						barrier.eventType = "clock/poll.tick"
					}
					if sourceCase == "partial_clock_receipt" {
						writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: clock-export\nstages: []\nschedules:\n  first: {every: 1h, emit: poll.tick}\n  second: {every: 1h, emit: poll.tick}\npins:\n  outputs: [poll.tick]\n")
					}
				}
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
				persistence := projectServeRuntimePersistence(stores)
				var receiptFault *startupClockReceiptFault
				if sourceCase == "partial_clock_receipt" {
					receiptFault = &startupClockReceiptFault{Store: persistence.deps.GenericScheduleStore, fault: errors.New("lost second real deployment clock receipt")}
					persistence.deps.GenericScheduleStore = receiptFault
				}
				candidate, err := buildServeRuntimeBundleContext(serveRuntimeBundleContextRequest{
					UseStartupRecovery: true,
					ExecutionPosture:   executionposture.MockOnly, Ctx: ctx, Stores: persistence, Config: cfg, Loaded: loaded,
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
				reconciled, err := reconcileServeStandingServices(ctx, candidate.runtime.Pipeline, candidates, true)
				if err != nil {
					t.Fatal(err)
				}
				targets, activations, err := reconcileServeRuntimeStandingTargets(candidate.runtime, reconciled)
				if err != nil {
					t.Fatal(err)
				}
				candidates[0].startupStandingTargets, candidates[0].startupStandingActivations = targets, activations
				frontierRun := ""
				for _, result := range reconciled {
					if result.RunID != "" {
						if frontierRun != "" && frontierRun != result.RunID {
							t.Fatal("clock frontier fixture requires one acknowledged standing generation")
						}
						frontierRun = result.RunID
					}
				}
				if frontierRun == "" {
					t.Fatal("startup frontier lacks its actual selected generation receipt")
				}
				checked := false
				candidate.runtime.Options.BootProgress = func(event runtime.BootProgressEvent) {
					if sourceCase != "workflow_timer" && event.Step == 12 && event.Status == "ok" {
						checked = true
						requireClockConstructionFrontier(t, candidate.runtime, frontierRun, sourceCase, "")
						if count, err := storetest.CountInstanceClockActivations(ctx, projectServeRuntimePersistence(stores).deps.EventStore); err != nil || count != 0 {
							t.Errorf("pre-construction clock rows=%d err=%v", count, err)
						}
						select {
						case signal := <-barrier.entered:
							t.Errorf("clock executed before construction/attachment: %+v", signal)
						case <-time.After(550 * time.Millisecond):
						}
					}
					if sourceCase == "workflow_timer" && event.Step == 10 && event.Status == "ok" {
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
				if sourceCase != "workflow_timer" {
					requireClockConstructionFrontier(t, candidate.runtime, frontierRun, sourceCase, pipeline.FlowAttachmentPlanned)
					if count, err := storetest.CountInstanceClockActivations(ctx, projectServeRuntimePersistence(stores).deps.EventStore); err != nil || count != 0 {
						t.Fatalf("prepared attachment acquired a clock before release: count=%d err=%v", count, err)
					}
				}
				releaseErr := release()
				if receiptFault != nil {
					if !errors.Is(releaseErr, receiptFault.fault) || receiptFault.calls != 2 || len(receiptFault.admitted) != 2 {
						t.Fatalf("composed partial arming lost its failure/evidence: calls=%d activations=%+v err=%v", receiptFault.calls, receiptFault.admitted, releaseErr)
					}
					if lookup := manager.LookupBundleHashStatus(candidate.sourceArtifactFact.BundleHash()); lookup.Loaded() || candidate.runtime.WorkOccurrence().ActiveCount() != 0 {
						t.Fatalf("failed clock startup retained executable process authority: lookup=%+v work=%d", lookup, candidate.runtime.WorkOccurrence().ActiveCount())
					}
					if count, err := storetest.CountInstanceClockActivations(ctx, projectServeRuntimePersistence(stores).deps.EventStore); err != nil || count != 2 {
						t.Fatalf("partial startup hid/replaced committed clocks: count=%d err=%v", count, err)
					}
					for _, original := range receiptFault.admitted {
						loaded, found, err := receiptFault.Store.LoadGenericScheduleActivation(ctx, original.ID)
						if err != nil || !found || loaded.ID != original.ID || loaded.ImmutableHash != original.ImmutableHash || !loaded.CurrentDueAt.Equal(original.CurrentDueAt) {
							t.Fatalf("startup rollback lost exact clock evidence: before=%+v after=%+v found=%t err=%v", original, loaded, found, err)
						}
					}
					return
				}
				if releaseErr != nil {
					t.Fatalf("real composed startup recovery: %v", releaseErr)
				}
				if sourceCase != "workflow_timer" {
					requireClockConstructionFrontier(t, candidate.runtime, frontierRun, sourceCase, pipeline.FlowAttachmentReady)
				}
				if !checked {
					t.Fatal("pipeline recovery entrance was not observed")
				}
				select {
				case signal := <-barrier.entered:
					t.Logf("standing timer released after recovery: event_id=%s type=%s run_id=%s", signal.EventID, signal.EventType, frontierRun)
				case <-time.After(5 * time.Second):
					t.Fatal("standing timer did not retain future execution authority")
				}
				barrier.unblock()
			})
		}
	}
}

func requireClockConstructionFrontier(t *testing.T, rt *runtime.Runtime, runID, sourceCase string, phase pipeline.FlowAttachmentPhase) {
	t.Helper()
	source := rt.Options.WorkflowModule.SemanticSource()
	flows := []string{"."}
	if sourceCase == "child_clock" {
		flows = append(flows, "clock", "consumer")
	}
	for _, flow := range flows {
		constructed := phase != ""
		instance, err := flowidentity.StandingForGeneration(source, flow, runID)
		if err != nil {
			t.Fatal(err)
		}
		owner, err := flowidentity.NewRunScopedFlowInstance(runID, instance.Route())
		if err != nil {
			t.Fatal(err)
		}
		stored, found, err := rt.Pipeline.LoadConstructedFlowInstance(t.Context(), owner, identity.NormalizeEntityID(instance.EntityID))
		if err != nil || found != constructed {
			t.Fatalf("construction frontier for %s: stored=%+v found=%t want=%t err=%v", flow, stored, found, constructed, err)
		}
		attachment, found, err := rt.Pipeline.LoadDynamicFlowRuntimeReadiness(t.Context(), runID, instance.Route())
		if err != nil || found != constructed || (constructed && attachment.Phase != phase) {
			t.Fatalf("attachment frontier for %s: state=%+v found=%t want=%s err=%v", flow, attachment, found, phase, err)
		}
	}
}
