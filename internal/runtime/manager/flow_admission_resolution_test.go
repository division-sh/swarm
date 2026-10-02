package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

var admissionResponseLost = errors.New("committed admission response lost")

type flowAdmissionResolutionProbe struct {
	*flowActivationTestInstanceStore
	manager                    *AgentManager
	lost, rollback, unresolved bool
	afterCommit                func()
}

func (s *flowAdmissionResolutionProbe) BeginDynamicFlowRuntimeActivation(ctx context.Context, request pipeline.DynamicFlowRuntimeActivationRequest) (pipeline.DynamicFlowRuntimeActivationAdmissionResult, error) {
	key, _ := newDynamicFlowRuntimeReadinessKey(request.Plan().RunID, request.Plan().Identity.InstancePath)
	s.manager.dynamicFlowReadinessMu.Lock()
	active := s.manager.dynamicFlowActiveAttempts[key]
	retained := active != nil && active.pending != nil && active.pending.ID() == request.ID() && active.receipt.Validate() != nil
	s.manager.dynamicFlowReadinessMu.Unlock()
	if !retained {
		return pipeline.DynamicFlowRuntimeActivationAdmissionResult{}, errors.New("admission called without retained pending responsibility")
	}
	if s.rollback {
		return pipeline.DynamicFlowRuntimeActivationAdmissionResult{}, admissionResponseLost
	}
	result, err := s.flowActivationTestInstanceStore.BeginDynamicFlowRuntimeActivation(ctx, request)
	if err == nil && result.Acknowledged && s.afterCommit != nil {
		s.afterCommit()
	}
	if err == nil && result.Acknowledged && s.lost {
		return pipeline.DynamicFlowRuntimeActivationAdmissionResult{}, admissionResponseLost
	}
	return result, err
}

func (s *flowAdmissionResolutionProbe) ResolveDynamicFlowRuntimeActivation(ctx context.Context, request pipeline.DynamicFlowRuntimeActivationRequest) (pipeline.DynamicFlowRuntimeActivationResolution, error) {
	if s.unresolved {
		return pipeline.DynamicFlowRuntimeActivationResolution{}, errors.New("resolution unavailable")
	}
	if err := ctx.Err(); err != nil {
		return pipeline.DynamicFlowRuntimeActivationResolution{}, err
	}
	return s.flowActivationTestInstanceStore.ResolveDynamicFlowRuntimeActivation(ctx, request)
}

func TestManagerRetainsAdmissionBeforeBeginAndResolvesLostResponse(t *testing.T) {
	for _, cut := range []string{"committed", "rolled_back", "unresolved"} {
		t.Run(cut, func(t *testing.T) {
			instances := &flowAdmissionResolutionProbe{flowActivationTestInstanceStore: &flowActivationTestInstanceStore{}, lost: true, rollback: cut == "rolled_back", unresolved: cut == "unresolved"}
			bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
			am := newFlowActivationManager(t, bus, instances)
			instances.manager = am
			ctx, cancel := context.WithTimeout(testAuthorActivityContext(context.Background()), 5*time.Second)
			defer cancel()
			req := testActivationRequest(testFlowBundle(t, ""), "review", "inst-1", "ent-1", "review/inst-1")
			err := activateFlowInstanceForTest(am, ctx, req)
			wantErr := admissionResponseLost
			if !errors.Is(err, wantErr) {
				t.Fatalf("response loss was hidden: %v", err)
			}
			key, _ := newDynamicFlowRuntimeReadinessKey(req.TriggerEvent.RunID(), req.Instance.InstancePath)
			am.dynamicFlowReadinessMu.Lock()
			active := am.dynamicFlowActiveAttempts[key]
			am.dynamicFlowReadinessMu.Unlock()
			if cut == "unresolved" {
				if active == nil || active.pending == nil || active.receipt.Validate() == nil {
					t.Fatalf("lost unresolved ownership: %+v", active)
				}
				if len(bus.addedPaths) != 0 || len(instances.armedEntries) != 0 {
					t.Fatal("unresolved admission executed topology")
				}
				instances.unresolved = false
				if err := am.retireDynamicFlowAttemptsAfterJoin(context.WithoutCancel(ctx)); err != nil {
					t.Fatal(err)
				}
				am.dynamicFlowReadinessMu.Lock()
				remaining := len(am.dynamicFlowActiveAttempts)
				am.dynamicFlowReadinessMu.Unlock()
				if remaining != 0 {
					t.Fatal("joined retirement retained a resolved request")
				}
			} else if cut == "rolled_back" {
				if active != nil || len(bus.addedPaths) != 0 {
					t.Fatal("unadmitted request retained execution or cleanup authority")
				}
			} else {
				if active == nil || active.pending != nil || active.receipt.Validate() != nil {
					t.Fatal("exact committed resolution lost its receipt")
				}
			}
			{
				instances.lost, instances.rollback = false, false
				created, err := am.EnsureFlowInstance(ctx, req)
				if err != nil || created || len(instances.creates) != 1 {
					t.Fatalf("retry repeated construction: created=%t count=%d err=%v", created, len(instances.creates), err)
				}
				row, found, err := instances.LoadDynamicFlowRuntimeReadiness(ctx, req.TriggerEvent.RunID(), req.Instance.Route())
				if err != nil || !found || row.Phase != pipeline.FlowAttachmentReady {
					t.Fatalf("retry readiness: %+v %t %v", row, found, err)
				}
			}
		})
	}
}

func TestManagerCancelledAdmissionRetainsResolutionUntilRetirement(t *testing.T) {
	instances := &flowAdmissionResolutionProbe{flowActivationTestInstanceStore: &flowActivationTestInstanceStore{}, lost: true}
	bus := &flowActivationTestBus{routeStore: &flowActivationTestRouteStore{}}
	am := newFlowActivationManager(t, bus, instances)
	instances.manager = am
	ctx, cancel := context.WithCancel(testAuthorActivityContext(context.Background()))
	defer cancel()
	req := testActivationRequest(testFlowBundle(t, ""), "review", "inst-1", "ent-1", "review/inst-1")
	setFlowActivationManagerSemanticSource(am, req.ContractBundle)
	plan, err := am.PrepareFlowInstanceActivation(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := am.roles.FlowActivation.CommitFlowInstanceActivation(ctx, plan)
	if err != nil || !committed.Acknowledged {
		t.Fatalf("constructor: %+v %v", committed, err)
	}
	instances.afterCommit = cancel
	key, _ := newDynamicFlowRuntimeReadinessKey(plan.Readiness.RunID, plan.Identity.InstancePath)
	active, _, err := am.beginDynamicFlowActiveAttempt(ctx, key, plan.Readiness, committed.ReadinessAttemptOrdinal, "planned")
	if active != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled admission executed: %+v %v", active, err)
	}
	am.dynamicFlowReadinessMu.Lock()
	pending := am.dynamicFlowActiveAttempts[key]
	am.dynamicFlowReadinessMu.Unlock()
	if pending == nil || pending.pending == nil || len(bus.addedPaths) != 0 || len(instances.armedEntries) != 0 {
		t.Fatal("cancelled admission lost non-executable responsibility")
	}
	if err := am.retireDynamicFlowAttemptsAfterJoin(context.WithoutCancel(ctx)); err != nil {
		t.Fatal(err)
	}
	row, found, err := instances.LoadDynamicFlowRuntimeReadiness(context.WithoutCancel(ctx), plan.Readiness.RunID, plan.Identity.Route())
	if err != nil || !found || row.AttemptState != "aborted" || row.Phase != pipeline.FlowAttachmentPlanned || len(instances.creates) != 1 {
		t.Fatalf("cancelled cleanup: %+v %t %v", row, found, err)
	}
}
