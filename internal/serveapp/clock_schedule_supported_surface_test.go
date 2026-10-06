package serveapp

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil"
)

type clockPublicRuntime struct {
	Endpoint, BundleHash string
	Runtime              *runtimepkg.Runtime
}

// The real serve entrypoint owns selection/bootstrap. This fixture exposes no
// database or construction handle to the clock journey.
func clockDeploymentHarness(t *testing.T, backend, root string) (*cliapp.ServeOptions, func() (*serveRuntimeTestProcess, clockPublicRuntime)) {
	t.Helper()
	unsetStoreSelectorEnv(t)
	stubServeRuntimeWorkspaceLifecycle(t)
	opts := &cliapp.ServeOptions{SourceRoot: root, PlatformSpecPath: defaultPlatformSpecPath, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0", SelfCheck: true, Verbose: true, TestOutboxSweeperConfig: servedEventPublishProofOutboxSweeperConfig()}
	if backend == "sqlite" {
		opts.ConfigPath = writeStoreBackendRuntimeConfig(t, "sqlite", filepath.Join(t.TempDir(), "lifecycle.sqlite"))
	} else {
		opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartPostgresDSN(t))
		opts.StoreMode, opts.StoreModeSet = "postgres", true
	}
	return opts, func() (*serveRuntimeTestProcess, clockPublicRuntime) {
		process := startServeRuntimeTestProcess(t, *opts)
		process.waitForReadyLine()
		endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
		return process, clockPublicRuntime{Endpoint: endpoint, BundleHash: servedEventPublishFixtureBundleHash(t, opts.SourceRoot)}
	}
}

func TestServedClockBindingExecutesOnBothStores(t *testing.T) {
	for _, backend := range []servedparity.Backend{servedparity.BackendDefaultSQLite, servedparity.BackendExplicitPostgres} {
		t.Run(string(backend), func(t *testing.T) {
			for _, nested := range []bool{false, true} {
				t.Run(map[bool]string{false: "root export", true: "connected keyless child"}[nested], func(t *testing.T) {
					backendName := "sqlite"
					if backend == servedparity.BackendExplicitPostgres {
						backendName = "postgres"
					}
					opts, start := clockDeploymentHarness(t, backendName, canonicalrouting.CopyClockDeployment(t, nested))
					ready := make(chan *runtimepkg.Runtime, 1)
					opts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) { ready <- rt }
					_, rt := start()
					select {
					case rt.Runtime = <-ready:
					case <-time.After(2 * time.Second):
						t.Fatal("public serve runtime hook did not join startup")
					}
					statuses, err := rt.Runtime.Pipeline.ListStandingServiceStatuses(t.Context())
					if err != nil || len(statuses) != 1 {
						t.Fatalf("clock deployment statuses=%+v err=%v", statuses, err)
					}
					status := statuses[0]
					if !status.RestartDisposition.Executable() || status.Generation != 1 || status.BundleHash != rt.BundleHash {
						t.Fatalf("clock deployment lacks actual generation: %+v", status)
					}
					var result struct {
						Run operatorread.RunHeader `json:"run"`
					}
					requireServedJSONRPCResult(t, rt.Endpoint, "run.get", map[string]any{"run_id": status.RunID}, &result)
					header := result.Run
					if len(header.ClockSchedules) != 1 || header.ClockSchedules[0].RunID != status.RunID ||
						header.ClockSchedules[0].Status != genericschedule.StatusActive || !header.ClockSchedules[0].RetainsRun || header.ClockSchedules[0].NextDueAt == nil {
						t.Fatalf("clock activation/readback=%+v", header)
					}
					clock := header.ClockSchedules[0]
					wantFlow, wantEvent := ".", "poll.tick"
					if nested {
						wantFlow, wantEvent = "clock", "clock/poll.tick"
					}
					if clock.FlowID != wantFlow || clock.Emit != wantEvent || clock.Name != "poll" || clock.Every != "250ms" {
						t.Fatalf("clock declaration/source readback=%+v", clock)
					}
					source := rt.Runtime.Options.WorkflowModule.SemanticSource()
					flows := []string{"."}
					if nested {
						flows = append(flows, "clock", "consumer")
					}
					for _, flow := range flows {
						instance, err := flowidentity.StandingForGeneration(source, flow, status.RunID)
						if err != nil {
							t.Fatal(err)
						}
						owner, err := flowidentity.NewRunScopedFlowInstance(status.RunID, instance.Route())
						if err != nil {
							t.Fatal(err)
						}
						stored, found, err := rt.Runtime.Pipeline.LoadConstructedFlowInstance(t.Context(), owner, identity.NormalizeEntityID(instance.EntityID))
						if err != nil || !found || stored.WorkflowName != flow || stored.ParentEntityID != instance.ParentEntityID {
							t.Fatalf("clock acquired fabricated/incomplete tree member=%+v err=%v", stored, err)
						}
						readiness, found, err := rt.Runtime.Pipeline.LoadDynamicFlowRuntimeReadiness(t.Context(), status.RunID, instance.Route())
						if err != nil || !found || readiness.Phase != pipeline.FlowAttachmentReady {
							t.Fatalf("clock armed before genuine attachment: %+v found=%t err=%v", readiness, found, err)
						}
					}
					deadline := time.Now().Add(5 * time.Second)
					for {
						var page operatorread.OperatorEventListResult
						requireServedJSONRPCResult(t, rt.Endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": status.RunID}, "limit": 100}, &page)
						settled := false
						for _, event := range page.Events {
							if event.EventName != wantEvent {
								continue
							}
							if event.ProducerType != events.EventProducerInstance || event.Source != wantFlow || len(event.Payload) != 0 || len(event.DeadLetters) != 0 {
								t.Fatalf("clock lost ordinary bare instance publication: %+v", event)
							}
							if !nested {
								if len(event.Deliveries) != 0 || event.NoDelivery == nil {
									t.Fatalf("root export acquired fake delivery or missing N=0 evidence: %+v", event)
								}
								settled = true
							} else if len(event.Deliveries) == 1 && event.Deliveries[0].Status == string(deliverylifecycle.StatusDelivered) && event.Deliveries[0].Terminal {
								if event.Deliveries[0].Target.FlowID != "consumer" || event.Deliveries[0].Target.FlowInstance != "consumer" {
									t.Fatalf("clock delivered outside its compiled connection: %+v", event)
								}
								settled = true
							}
						}
						if settled {
							break
						}
						if time.Now().After(deadline) {
							t.Fatalf("clock publication/settlement absent: %+v", page)
						}
						time.Sleep(10 * time.Millisecond)
					}
				})
			}
		})
	}
}

func TestServedClockBindingDisarmResetAndRestartBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			opts, start := clockDeploymentHarness(t, backend, canonicalrouting.CopyClockDeployment(t, false))
			ready := make(chan *runtimepkg.Runtime, 1)
			opts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) { ready <- rt }
			first, rt := start()
			rt.Runtime = <-ready
			statuses, err := rt.Runtime.Pipeline.ListStandingServiceStatuses(t.Context())
			if err != nil || len(statuses) != 1 {
				t.Fatalf("clock service inventory=%+v err=%v", statuses, err)
			}
			before := statuses[0]
			var result struct {
				Run operatorread.RunHeader `json:"run"`
			}
			requireServedJSONRPCResult(t, rt.Endpoint, "run.get", map[string]any{"run_id": before.RunID}, &result)
			if len(result.Run.ClockSchedules) != 1 {
				t.Fatalf("initial clock inventory=%+v", result)
			}
			initialClock := result.Run.ClockSchedules[0]
			suspended := invokeServedStandingOperation(t, rt.Endpoint, "standing.suspend", before.ServiceID, "clock-suspend")
			if suspended.RunID != before.RunID || suspended.Generation != before.Generation || suspended.EffectiveState != "suspended" {
				t.Fatalf("clock suspension lost generation=%+v", suspended)
			}
			result.Run = operatorread.RunHeader{}
			requireServedJSONRPCResult(t, rt.Endpoint, "run.get", map[string]any{"run_id": before.RunID}, &result)
			if len(result.Run.ClockSchedules) != 1 || result.Run.ClockSchedules[0].ActivationID != initialClock.ActivationID ||
				result.Run.ClockSchedules[0].Status != genericschedule.StatusParked || result.Run.ClockSchedules[0].RetainsRun || result.Run.ClockSchedules[0].NextDueAt != nil || result.Run.ClockSchedules[0].Suspension == nil {
				t.Fatalf("suspended clock remained executable/retaining=%+v", result.Run)
			}
			parkedAt := result.Run.ClockSchedules[0].Suspension.ParkedAt
			if code := first.stop(); code != 0 {
				t.Fatalf("suspended clock shutdown code=%d", code)
			}
			second, restarted := start()
			restarted.Runtime = <-ready
			result.Run = operatorread.RunHeader{}
			requireServedJSONRPCResult(t, restarted.Endpoint, "run.get", map[string]any{"run_id": before.RunID}, &result)
			if len(result.Run.ClockSchedules) != 1 || result.Run.ClockSchedules[0].ActivationID != initialClock.ActivationID ||
				result.Run.ClockSchedules[0].Status != genericschedule.StatusParked || result.Run.ClockSchedules[0].NextDueAt != nil || result.Run.ClockSchedules[0].Suspension == nil || !result.Run.ClockSchedules[0].Suspension.ParkedAt.Equal(parkedAt) {
				t.Fatalf("restart silently rearmed a suspended clock=%+v", result.Run)
			}
			time.Sleep(500 * time.Millisecond)
			resumedSame := invokeServedStandingOperation(t, restarted.Endpoint, "standing.resume", before.ServiceID, "clock-resume-same-generation")
			if resumedSame.RunID != before.RunID || resumedSame.Generation != before.Generation || resumedSame.EffectiveState != "active" {
				t.Fatalf("resume replaced the parked generation=%+v", resumedSame)
			}
			result.Run = operatorread.RunHeader{}
			requireServedJSONRPCResult(t, restarted.Endpoint, "run.get", map[string]any{"run_id": before.RunID}, &result)
			if len(result.Run.ClockSchedules) != 1 {
				t.Fatalf("resume lost clock inventory=%+v", result.Run)
			}
			armed := result.Run.ClockSchedules[0]
			if armed.ActivationID != initialClock.ActivationID || armed.Status != genericschedule.StatusActive || !armed.RetainsRun || armed.NextDueAt == nil ||
				armed.Suspension == nil || !armed.Suspension.SuspendedFrom.Equal(parkedAt) || armed.Suspension.ResumedAt.IsZero() || armed.Suspension.SkippedOccurrences < 1 || !armed.Suspension.ParkedAt.IsZero() ||
				!armed.InitialDueAt.Equal(initialClock.InitialDueAt) || !armed.NextDueAt.After(armed.Suspension.ResumedAt) {
				t.Fatalf("same-generation resume left a dead clock or invented catch-up=%+v", armed)
			}
			var page operatorread.OperatorEventListResult
			requireServedJSONRPCResult(t, restarted.Endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": before.RunID}, "limit": 100}, &page)
			for _, event := range page.Events {
				if event.EventName == "poll.tick" && !event.CreatedAt.Before(parkedAt) && !event.CreatedAt.After(armed.Suspension.ResumedAt) {
					t.Fatalf("resume published an elapsed suspended occurrence=%+v", event)
				}
			}
			invokeServedStandingOperation(t, restarted.Endpoint, "standing.suspend", before.ServiceID, "clock-suspend-again")
			reset := invokeServedStandingOperation(t, restarted.Endpoint, "standing.reset", before.ServiceID, "clock-reset")
			if reset.RunID == before.RunID || reset.Generation != before.Generation+1 || reset.EffectiveState != "suspended" {
				t.Fatalf("clock reset did not use fresh construction=%+v", reset)
			}
			resumed := invokeServedStandingOperation(t, restarted.Endpoint, "standing.resume", before.ServiceID, "clock-resume-fresh-generation")
			if resumed.RunID != reset.RunID || resumed.Generation != reset.Generation || resumed.EffectiveState != "active" {
				t.Fatalf("fresh clock generation failed resume=%+v", resumed)
			}
			result.Run = operatorread.RunHeader{}
			requireServedJSONRPCResult(t, restarted.Endpoint, "run.get", map[string]any{"run_id": reset.RunID}, &result)
			if len(result.Run.ClockSchedules) != 1 || result.Run.ClockSchedules[0].ActivationID == initialClock.ActivationID ||
				result.Run.ClockSchedules[0].Status != genericschedule.StatusActive || result.Run.ClockSchedules[0].FlowInstance != reset.RunID {
				t.Fatalf("clock reset borrowed predecessor schedule=%+v", result.Run)
			}
			result.Run = operatorread.RunHeader{}
			requireServedJSONRPCResult(t, restarted.Endpoint, "run.get", map[string]any{"run_id": before.RunID}, &result)
			if len(result.Run.ClockSchedules) != 1 || result.Run.ClockSchedules[0].Status != genericschedule.StatusCancelled || result.Run.ClockSchedules[0].CancelCause != "standing_reset" {
				t.Fatalf("reset preserved a parked predecessor=%+v", result.Run)
			}
			replayed := invokeServedStandingOperation(t, restarted.Endpoint, "standing.reset", before.ServiceID, "clock-reset")
			if replayed.RunID != reset.RunID || replayed.Generation != reset.Generation {
				t.Fatalf("clock reset replay repeated construction=%+v", replayed)
			}
			if code := second.stop(); code != 0 {
				t.Fatalf("reset clock shutdown code=%d", code)
			}
		})
	}
}
