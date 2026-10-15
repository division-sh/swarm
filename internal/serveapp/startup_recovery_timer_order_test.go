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
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/startuphandoff"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type startupTimerPublicationBarrier struct {
	entered   chan lifecycleprobe.Signal
	release   chan struct{}
	once      sync.Once
	eventType string
	handoff   *startuphandoff.Proof
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
	if b.handoff != nil {
		b.handoff.NotifyLifecycle(ctx, signal)
	}
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
		for _, sourceCase := range []string{"workflow_timer", "adopted_workflow_timer", "root_clock", "child_clock", "partial_clock_receipt"} {
			t.Run(backend+"/"+sourceCase, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				stores := openStandingRuntimeContextStore(t, backend, "recovery-timer-order")
				loadedConfig, err := cliapp.LoadRuntimeConfigWithOptions(cliapp.RuntimeConfigLoadOptions{
					RepoRoot: repoRootForTest(), ExplicitPath: writeServeRuntimeTestConfig(t),
				})
				if err != nil {
					t.Fatal(err)
				}
				cfg := loadedConfig.Config
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
				proof := startuphandoff.New()
				barrier := &startupTimerPublicationBarrier{entered: make(chan lifecycleprobe.Signal, 1), release: make(chan struct{}), handoff: proof}
				var candidates []serveRuntimeBundleContext
				manager, err := runtime.NewRuntimeContextManager(stores.RunBundleAvailability())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					proof.Release()
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
				workflowCase := sourceCase == "workflow_timer" || sourceCase == "adopted_workflow_timer"
				if workflowCase {
					fixture := t.TempDir()
					copyReleaseFixtureTree(t, root, fixture)
					root = fixture
					// Native timer coordinates use microsecond precision: 1ns is
					// due at construction, without sleeping or editing store rows.
					writeWorkflowValidationFixtureFile(t, filepath.Join(root, "telegram-ingress/schema.yaml"), `name: telegram-ingress
stages:
  warming:
    timers:
      - id: warmup
        after: 1ns
        advances_to: review
  review:
    gate:
      decision: lifecycle_ready
      title: Confirm lifecycle readiness
      outcomes:
        approve:
          label: Continue
          advances_to: serving
        reject:
          label: Retire
          advances_to: retired
  serving: {}
  retired:
    final: true
pins:
  inputs:
    - inbound.telegram
  outputs:
    - inbound.telegram.text_message
ingress:
  alias: chat
  providers:
    - provider: telegram
      signing_secret: webhook_signing.telegram
`)
				} else {
					root = canonicalrouting.CopyClockDeployment(t, sourceCase == "child_clock")
					barrier.eventType = "poll.tick"
					if sourceCase == "child_clock" {
						barrier.eventType = "clock/poll.tick"
					}
					if sourceCase == "partial_clock_receipt" {
						writeWorkflowValidationFixtureFile(t, filepath.Join(root, "schema.yaml"), "name: clock-export\nstages: []\nschedules:\n  first: {every: 1h, emit: poll.tick}\n  second: {every: 1h, emit: poll.tick}\npins:\n  outputs: [poll.tick]\n")
					}
				}
				loaded, err := loadServeRuntimeBundle(ctx, repoRootForTest(), stores.SourceArtifactStore(), cliapp.CLISourcePaths{
					SourceRoot: root,
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
				persistence.deps.PipelineObligations = proof.RecoveryStore(persistence.deps.PipelineObligations)
				persistence.deps.DeliveryStore = proof.DeliveryStore(persistence.deps.DeliveryStore)
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
				proof.WatchType("platform.stage_timer")
				if !workflowCase {
					proof.WatchType(barrier.eventType)
				}
				var completeAdoption func() error
				adoptionCompleted := make(chan struct{})
				candidate.runtime.Options.BootProgress = func(event runtime.BootProgressEvent) {
					if completeAdoption != nil && event.Step == 10 && event.Status == "ok" {
						if err := completeAdoption(); err != nil {
							t.Errorf("restore adopted standing timer under startup latch: %v", err)
						} else {
							close(adoptionCompleted)
						}
					}
					if !workflowCase && event.Step == 12 && event.Status == "ok" {
						requireClockConstructionFrontier(t, candidate.runtime, frontierRun, sourceCase, "")
						if count, err := storetest.CountInstanceClockActivations(ctx, projectServeRuntimePersistence(stores).deps.EventStore); err != nil || count != 0 {
							t.Errorf("pre-construction clock rows=%d err=%v", count, err)
						}
					}
					if event.Step == 16 && event.Status == "ok" {
						proof.MarkContinuationsSettled()
					}
					if event.Step == 16 && event.Status == "FAILED" {
						barrier.unblock()
					}
				}
				release, err := prepareServeRuntimeContexts(ctx, candidates, manager)
				if err != nil {
					t.Fatal(err)
				}
				if !workflowCase {
					requireClockConstructionFrontier(t, candidate.runtime, frontierRun, sourceCase, pipeline.FlowAttachmentPlanned)
					if count, err := storetest.CountInstanceClockActivations(ctx, projectServeRuntimePersistence(stores).deps.EventStore); err != nil || count != 0 {
						t.Fatalf("prepared attachment acquired a clock before release: count=%d err=%v", count, err)
					}
				}
				if sourceCase == "adopted_workflow_timer" {
					var adopted []runtime.StandingActivation
					_, adopted, completeAdoption, err = candidate.runtime.PrepareStandingTargets(ctx)
					if err != nil || len(adopted) == 0 {
						t.Fatalf("adopt constructed standing generation: %+v err=%v", adopted, err)
					}
					for _, activation := range adopted {
						if activation.Created || !activation.RestartDisposition.Executable() || activation.RunID != frontierRun {
							t.Fatalf("adoption did not retain exact executable generation: %+v", activation)
						}
					}
				}
				var dormant []pipeline.WorkflowTimerActivation
				timerReader, ok := persistence.deps.EventStore.(pipeline.WorkflowTimerActivationPersistence)
				if !ok {
					t.Fatal("selected timer activation owner is required")
				}
				if workflowCase {
					child, err := flowidentity.StandingForGeneration(candidate.loaded.source, "telegram-ingress", frontierRun)
					if err != nil {
						t.Fatal(err)
					}
					dormant, err = timerReader.ListWorkflowTimerActivations(ctx, frontierRun, child.EntityID, true)
					if err != nil || len(dormant) != 1 {
						t.Fatalf("initial/adopted registration omitted durable timer: %+v err=%v", dormant, err)
					}
					for _, timer := range dormant {
						wantDue := timer.CreatedAt.Add(time.Nanosecond).UTC().Truncate(time.Microsecond)
						if !timer.FireAt.Equal(wantDue) || timer.FireAt.After(time.Now()) || timer.Status != "active" {
							t.Fatalf("standing fixture must construct exact already-due work: timer=%+v want_due=%s", timer, wantDue)
						}
						t.Logf("standing occurrence due before startup: activation=%s created=%s due=%s", timer.Ref.ActivationID, timer.CreatedAt, timer.FireAt)
						proof.WatchID(timeridentity.WorkflowTimerOccurrenceEventID(timeridentity.WorkflowTimerOccurrenceRef{Activation: timer.Ref, DueAt: timer.FireAt}))
					}
				}
				finished := make(chan error, 1)
				joined := false
				go func() { finished <- release() }()
				defer func() {
					proof.Release()
					barrier.unblock()
					cancel()
					if !joined {
						select {
						case <-finished:
						case <-time.After(20 * time.Second):
							t.Error("composed startup did not join after releasing held handoffs")
						}
					}
				}()
				witnessCtx, witnessCancel := context.WithTimeout(t.Context(), 20*time.Second)
				defer witnessCancel()
				for _, handoff := range []*startuphandoff.Barrier{proof.Recovery, proof.Continuation} {
					if err := proof.Wait(witnessCtx, handoff); err != nil {
						t.Fatal(err)
					}
					if sourceCase == "adopted_workflow_timer" {
						select {
						case <-adoptionCompleted:
						default:
							t.Fatal("adopted timer restoration did not complete before recovery")
						}
					}
					if workflowCase {
						for _, original := range dormant {
							current, found, err := timerReader.LoadWorkflowTimerActivation(ctx, original.Ref.ActivationID)
							if err != nil || !found || current.Ref != original.Ref || !current.FireAt.Equal(original.FireAt) || current.FireAt.After(time.Now()) || current.Status != original.Status {
								t.Fatalf("held standing registration changed due work: before=%+v after=%+v found=%t err=%v", original, current, found, err)
							}
						}
					} else if count, err := storetest.CountInstanceClockActivations(ctx, projectServeRuntimePersistence(stores).deps.EventStore); err != nil || count != 0 {
						t.Fatalf("clock was admitted before both handoffs: rows=%d err=%v", count, err)
					}
					handoff.Release()
				}
				var releaseErr error
				select {
				case releaseErr = <-finished:
					joined = true
				case <-witnessCtx.Done():
					t.Fatal(witnessCtx.Err())
				}
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
				if !workflowCase {
					requireClockConstructionFrontier(t, candidate.runtime, frontierRun, sourceCase, pipeline.FlowAttachmentReady)
				}
				select {
				case signal := <-barrier.entered:
					if err := proof.CheckPublished(signal.EventID); err != nil {
						t.Fatal(err)
					}
					if workflowCase {
						original := dormant[0]
						want := timeridentity.WorkflowTimerOccurrenceEventID(timeridentity.WorkflowTimerOccurrenceRef{Activation: original.Ref, DueAt: original.FireAt})
						if signal.EventID != want {
							t.Fatalf("standing late publication changed occurrence: got=%s want=%s", signal.EventID, want)
						}
					}
					t.Logf("standing timer released after recovery: event_id=%s type=%s run_id=%s", signal.EventID, signal.EventType, frontierRun)
				case <-witnessCtx.Done():
					t.Fatal("standing timer did not retain future execution authority")
				}
				barrier.unblock()
				if workflowCase {
					if err := candidate.runtime.Scheduler.Wait(witnessCtx); err != nil {
						t.Fatalf("join exact standing timer callback: %v", err)
					}
					if err := proof.CheckPublished(timeridentity.WorkflowTimerOccurrenceEventID(dormant[0].Occurrence())); err != nil {
						t.Fatal(err)
					}
				}
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
