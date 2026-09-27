package runforkexecution

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

type selectedRuntimeRetirementProbe struct {
	runtimestartupownership.GenerationGrant
	retireCalls int
}

func TestSelectedRetainedGatewayCleanupDrainsBoundOccurrence(t *testing.T) {
	for _, failures := range []int{1, 2} {
		t.Run(fmt.Sprintf("failures_%d", failures), func(t *testing.T) {
			process := worklifetime.NewProcess()
			op := selectedContractOperationForTest(t, worklifetime.WithProcess(context.Background(), process))
			identity := worklifetime.SelectedForkIdentity{ExecutionID: uuid.NewString(), RunID: uuid.NewString(), Generation: 1}
			if err := op.Bind(identity); err != nil {
				t.Fatal(err)
			}
			lease, err := op.selected.BeginStanding(op.Context())
			if err != nil {
				t.Fatal(err)
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			gateway := serveSelectedContractGateway(listener, http.NewServeMux(), lease)
			defer gateway.Close()
			calls := 0
			runtime := &selectedContractAgentRuntime{cleanup: func() {
				calls++
				if calls <= failures {
					panic("transient cleanup failure")
				}
				gateway.Close()
			}}
			prepared := &PreparedSelectedFork{operation: op, retainedRuntime: runtime}
			if err := runtime.Shutdown(); err == nil {
				t.Fatal("missing injected startup cleanup failure")
			}
			for i := 1; i < failures; i++ {
				if err := prepared.Close(); err == nil {
					t.Fatal("persistent cleanup failure released retained child")
				}
				if prepared.retainedRuntime != runtime || process.ActiveCount() == 0 {
					t.Fatal("failed cleanup lost bound resource ownership")
				}
			}
			closed := make(chan error, 1)
			go func() { closed <- prepared.Close() }()
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				gateway.Close()
				<-closed
				t.Fatal("preparation joined selected occurrence before releasing its retained gateway")
			}
			if err := process.Wait(context.Background()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSelectedRetainedManagerCleanupDrainsBoundOccurrence(t *testing.T) {
	process := worklifetime.NewProcess()
	op := selectedContractOperationForTest(t, worklifetime.WithProcess(context.Background(), process))
	identity := worklifetime.SelectedForkIdentity{ExecutionID: uuid.NewString(), RunID: uuid.NewString(), Generation: 1}
	if err := op.Bind(identity); err != nil {
		t.Fatal(err)
	}
	admission, err := managedexecution.New(managedexecution.KindSelectedContractFork, identity.ExecutionID, identity.Generation, identity.RunID, "bound-manager", runForkTestBundleHash, nil)
	if err != nil {
		t.Fatal(err)
	}
	manager := runtimemanager.NewAgentManagerWithOptions(nil, nil, runtimemanager.AgentManagerOptions{WorkOwner: op.selected, ReceiverExecution: eventreceiver.NormalExecution()})
	if err := manager.RunAuthoritativeDeliveryOnly(managedexecution.WithAdmission(op.Context(), admission)); err != nil {
		t.Fatal(err)
	}
	prepared := &PreparedSelectedFork{operation: op, retainedRuntime: &selectedContractAgentRuntime{manager: manager}}
	closed := make(chan error, 1)
	go func() { closed <- prepared.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("preparation joined selected occurrence before its Manager child")
	}
	if err := process.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type selectedFailOnceGrantCapability struct {
	runtimestartupownership.ProcessCapability
	issued *selectedFailOnceGrant
}

func (p *selectedFailOnceGrantCapability) IssueSelectedForkGenerationGrant(ctx context.Context, req runtimestartupownership.SelectedForkGrantRequest) (runtimestartupownership.GenerationGrant, error) {
	grant, err := p.ProcessCapability.IssueSelectedForkGenerationGrant(ctx, req)
	if err != nil {
		return nil, err
	}
	p.issued = &selectedFailOnceGrant{GenerationGrant: grant, failNext: true}
	return p.issued, nil
}

type selectedFailOnceGrant struct {
	runtimestartupownership.GenerationGrant
	failNext bool
}

func (g *selectedFailOnceGrant) Retire(ctx context.Context) error {
	if g.failNext {
		g.failNext = false
		return errors.New("injected selected grant retirement failure")
	}
	return g.GenerationGrant.Retire(ctx)
}

func (p *selectedRuntimeRetirementProbe) Retire(context.Context) error {
	p.retireCalls++
	return nil
}

type selectedRetainedActivationProbe struct {
	failAt   string
	failing  bool
	routes   int
	timers   int
	attempts int
}

func (p *selectedRetainedActivationProbe) Retire() error {
	p.routes++
	if p.failAt == "route" && p.failing {
		return errors.New("injected route retirement failure")
	}
	return nil
}

func (p *selectedRetainedActivationProbe) RetireInitialEntryTimerWakeups(context.Context, runtimeflowidentity.RunScopedFlowInstance) error {
	p.timers++
	if p.failAt == "timer" && p.failing {
		return errors.New("injected timer retirement failure")
	}
	return nil
}

func (p *selectedRetainedActivationProbe) AbandonDynamicFlowRuntimeActivationAttempt(context.Context, runtimepipeline.DynamicFlowRuntimeActivationAttempt) error {
	p.attempts++
	if p.failAt == "attempt" && p.failing {
		return errors.New("injected durable attempt abandonment failure")
	}
	return nil
}

func TestSelectedPartialActivationInventoryRetainsEveryFailedStage(t *testing.T) {
	for _, stage := range []string{"route", "timer", "attempt"} {
		t.Run(stage, func(t *testing.T) {
			grant := &selectedRuntimeRetirementProbe{}
			probe := &selectedRetainedActivationProbe{failAt: stage, failing: true}
			runtime := &selectedContractAgentRuntime{
				generationGrant: grant, pipeline: probe,
				pendingActivations: []selectedFlowActivation{{publication: probe, timersProjected: true}},
			}
			operation := selectedContractOperationForTest(t, worklifetime.WithProcess(context.Background(), worklifetime.NewProcess()))
			prepared := &PreparedSelectedFork{operation: operation, retainedRuntime: runtime}
			for i := 0; i < 2; i++ {
				if err := prepared.Close(); err == nil {
					t.Fatal("failed activation cleanup reported success")
				}
				if prepared.retainedRuntime != runtime || len(runtime.pendingActivations) != 1 || grant.retireCalls != 0 {
					t.Fatalf("failed %s stage released ownership or grant", stage)
				}
				waitCtx, cancel := context.WithCancel(context.Background())
				cancel()
				if err := operation.process.Wait(waitCtx); !errors.Is(err, context.Canceled) {
					t.Fatalf("failed %s stage allowed process/store release: %v", stage, err)
				}
			}
			probe.failing = false
			if err := prepared.Close(); err != nil {
				t.Fatalf("retry retained %s stage: %v", stage, err)
			}
			if len(runtime.pendingActivations) != 0 || grant.retireCalls != 1 || probe.attempts == 0 {
				t.Fatalf("successful %s retry did not settle activation before grant", stage)
			}
			if err := operation.process.Wait(context.Background()); err != nil {
				t.Fatalf("settled %s stage did not join process: %v", stage, err)
			}
		})
	}
}

func TestSelectedPartialRuntimeCleanupRemainsOwnedByPreparation(t *testing.T) {
	for _, adopted := range []bool{false, true} {
		for _, failures := range []int{1, 2} {
			name := "before_adoption"
			if adopted {
				name = "after_adoption"
			}
			if failures == 2 {
				name += "/persistent"
			} else {
				name += "/fail_once"
			}
			t.Run(name, func(t *testing.T) {
				grant := &selectedRuntimeRetirementProbe{}
				calls := 0
				runtime := &selectedContractAgentRuntime{generationGrant: grant, cleanup: func() {
					calls++
					if calls <= failures {
						panic("injected selected cleanup failure")
					}
				}}
				if adopted {
					runtime.manager = runtimemanager.NewAgentManagerWithOptions(nil, nil, runtimemanager.AgentManagerOptions{WorkOwner: testGatewayWorkOwner(t), ReceiverExecution: eventreceiver.NormalExecution()}, nil)
				}
				operation := selectedContractOperationForTest(t, worklifetime.WithProcess(context.Background(), worklifetime.NewProcess()))
				prepared := &PreparedSelectedFork{operation: operation, retainedRuntime: runtime}
				for i := 0; i < failures; i++ {
					if err := prepared.Close(); err == nil {
						t.Fatal("failed cleanup reported success")
					}
					if prepared.retainedRuntime != runtime || prepared.cleanupComplete || grant.retireCalls != 0 {
						t.Fatalf("failed cleanup lost exact owner or retired grant: retained=%v complete=%v retire=%d", prepared.retainedRuntime == runtime, prepared.cleanupComplete, grant.retireCalls)
					}
					waitCtx, cancel := context.WithCancel(context.Background())
					cancel()
					if err := operation.process.Wait(waitCtx); !errors.Is(err, context.Canceled) {
						t.Fatalf("failed cleanup allowed process/store release: %v", err)
					}
				}
				if err := prepared.Close(); err != nil {
					t.Fatalf("retry selected cleanup: %v", err)
				}
				if prepared.retainedRuntime != nil || !prepared.cleanupComplete || grant.retireCalls != 1 {
					t.Fatalf("successful cleanup did not release exact owner: retained=%v complete=%v retire=%d", prepared.retainedRuntime != nil, prepared.cleanupComplete, grant.retireCalls)
				}
				if err := operation.process.Wait(context.Background()); err != nil {
					t.Fatalf("successful cleanup did not join process: %v", err)
				}
			})
		}
	}
}
