package runforkexecution

import (
	"context"
	"errors"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

type selectedRuntimeRetirementProbe struct {
	runtimestartupownership.GenerationGrant
	retireCalls int
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
			prepared := &PreparedSelectedFork{operation: selectedContractOperationForTest(t, runForkTestContext(t)), retainedRuntime: runtime}
			for i := 0; i < 2; i++ {
				if err := prepared.Close(); err == nil {
					t.Fatal("failed activation cleanup reported success")
				}
				if prepared.retainedRuntime != runtime || len(runtime.pendingActivations) != 1 || grant.retireCalls != 0 {
					t.Fatalf("failed %s stage released ownership or grant", stage)
				}
			}
			probe.failing = false
			if err := prepared.Close(); err != nil {
				t.Fatalf("retry retained %s stage: %v", stage, err)
			}
			if len(runtime.pendingActivations) != 0 || grant.retireCalls != 1 || probe.attempts == 0 {
				t.Fatalf("successful %s retry did not settle activation before grant", stage)
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
				prepared := &PreparedSelectedFork{operation: selectedContractOperationForTest(t, runForkTestContext(t)), retainedRuntime: runtime}
				for i := 0; i < failures; i++ {
					if err := prepared.Close(); err == nil {
						t.Fatal("failed cleanup reported success")
					}
					if prepared.retainedRuntime != runtime || prepared.cleanupComplete || grant.retireCalls != 0 {
						t.Fatalf("failed cleanup lost exact owner or retired grant: retained=%v complete=%v retire=%d", prepared.retainedRuntime == runtime, prepared.cleanupComplete, grant.retireCalls)
					}
				}
				if err := prepared.Close(); err != nil {
					t.Fatalf("retry selected cleanup: %v", err)
				}
				if prepared.retainedRuntime != nil || !prepared.cleanupComplete || grant.retireCalls != 1 {
					t.Fatalf("successful cleanup did not release exact owner: retained=%v complete=%v retire=%d", prepared.retainedRuntime != nil, prepared.cleanupComplete, grant.retireCalls)
				}
			})
		}
	}
}
