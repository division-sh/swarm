package manager

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type flowActorObservationFaultKey struct{}

type flowActorObservationStore struct {
	*flowActivationTestInstanceStore
}

func (s *flowActorObservationStore) LoadConstructedFlowInstance(ctx context.Context, owner flowidentity.RunScopedFlowInstance, entity identity.EntityID) (pipeline.WorkflowInstance, bool, error) {
	switch ctx.Value(flowActorObservationFaultKey{}) {
	case "missing_construction":
		return pipeline.WorkflowInstance{}, false, nil
	case "failed_construction":
		return pipeline.WorkflowInstance{}, false, errors.New("construction read failed")
	}
	row, found, err := s.flowActivationTestInstanceStore.LoadConstructedFlowInstance(ctx, owner, entity)
	if ctx.Value(flowActorObservationFaultKey{}) == "construction_parent" {
		row.ParentFlowInstance = "foreign"
	}
	return row, found, err
}

func (s *flowActorObservationStore) VerifyDynamicFlowRuntimeActivationAttempt(ctx context.Context, attempt pipeline.DynamicFlowRuntimeActivationAttempt) error {
	if ctx.Value(flowActorObservationFaultKey{}) == "native_stale" {
		return pipeline.ErrFlowAttachmentStale
	}
	return s.flowActivationTestInstanceStore.VerifyDynamicFlowRuntimeActivationAttempt(ctx, attempt)
}

func (s *flowActorObservationStore) LoadDynamicFlowRuntimeReadiness(ctx context.Context, runID string, route flowidentity.Route) (pipeline.DynamicFlowRuntimeReadiness, bool, error) {
	row, found, err := s.flowActivationTestInstanceStore.LoadDynamicFlowRuntimeReadiness(ctx, runID, route)
	row.Plan.Agents = append([]pipeline.DynamicFlowRuntimeAgentExpectation(nil), row.Plan.Agents...)
	switch ctx.Value(flowActorObservationFaultKey{}) {
	case "missing":
		found = false
	case "plan":
		row.PlanHash = "foreign"
	case "ordinal":
		row.AttemptOrdinal++
	case "state":
		row.AttemptState = "retired"
	case "terminal":
		row.RunStatus = "completed"
	case "source":
		row.Plan.BundleHash = "foreign"
	case "identity":
		row.Plan.Identity.InstanceID = "foreign"
	case "missing_actor":
		row.Plan.Agents = nil
	case "duplicate_actor":
		row.Plan.Agents = append(row.Plan.Agents, row.Plan.Agents[0])
	case "configuration":
		row.Plan.Agents[0].ConfigRevision = "foreign"
	case "entity":
		row.Plan.Agents[0].EntityID = "foreign"
	}
	return row, found, err
}

func TestWorkspaceGatewayConstructedActorUsesCurrentNativeAttempt(t *testing.T) {
	instances := &flowActorObservationStore{&flowActivationTestInstanceStore{}}
	bus := &flowActivationTestBus{creationStore: instances.flowActivationTestInstanceStore}
	am := newFlowActivationManager(t, bus, instances)
	am.factory = (&projectionTestFactory{handled: make(chan int, 1)}).Build
	bundle := testFlowBundle(t, "")
	setFlowActivationManagerSemanticSource(am, semanticview.Wrap(bundle))
	req := testActivationRequest(bundle, "review", "inst-1", "ent-1", "review/inst-1")
	ctx, cancel := context.WithCancel(testAuthorActivityContext(context.Background()))
	defer cancel()
	defer am.ShutdownWithOptions(ShutdownOptions{Grace: time.Second})
	observed := false
	am.workspaceGatewayAdmission = func(ctx context.Context, actor actors.AgentConfig) error {
		observed = true
		token, _ := effects.LifecycleTokenFromContext(ctx)
		admission, err := am.ProveUnpublishedFlowActor(ctx, token, actor)
		if err != nil || admission.RunID != actor.Identity.RunID || admission.InstancePath != req.Instance.InstancePath || admission.AttemptID == "" {
			t.Fatalf("exact constructed actor admission=%+v err=%v", admission, err)
		}
		for _, fault := range []string{"native_stale", "missing", "plan", "ordinal", "state", "terminal", "source", "identity", "missing_actor", "duplicate_actor", "configuration", "entity", "missing_construction", "failed_construction", "construction_parent"} {
			faultCtx := context.WithValue(ctx, flowActorObservationFaultKey{}, fault)
			if _, err := am.ProveUnpublishedFlowActor(faultCtx, token, actor); err == nil {
				t.Fatalf("%s borrowed current actor admission", fault)
			}
		}
		canceled, stop := context.WithCancel(ctx)
		stop()
		if _, err := am.ProveUnpublishedFlowActor(canceled, token, actor); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled current actor admission=%v", err)
		}
		return nil
	}
	if err := am.Run(managedExecutionTestContext(t, ctx)); err != nil {
		t.Fatal(err)
	}
	if err := activateFlowInstanceForTest(am, ctx, req); err != nil {
		t.Fatal(err)
	}
	if !observed {
		t.Fatal("actual flow activation skipped actor observation")
	}
}
