package bus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type scenarioSetupProbeStore struct {
	InMemoryEventStore
	acknowledged bool
	command      ScenarioSetupCommand
	calls        int
	fault        error
	cancel       context.CancelFunc
}

func (s *scenarioSetupProbeStore) CommitScenarioSetup(_ context.Context, command ScenarioSetupCommand) (pipeline.ScenarioSetupResult, error) {
	s.command = command
	s.calls++
	result := pipeline.ScenarioSetupResult{RunID: command.Setup.RunID, Acknowledged: s.acknowledged}
	for _, activation := range command.Activations {
		result.Activations = append(result.Activations, pipeline.CommittedFlowInstanceActivation{
			Plan: activation.Plan, Created: true, ReadinessAttemptOrdinal: 1, Acknowledged: s.acknowledged,
		})
	}
	if s.cancel != nil {
		s.cancel()
	}
	return result, s.fault
}

func TestScenarioSetupDispatchesOnlyAcknowledgedConstruction(t *testing.T) {
	for _, test := range []struct {
		name         string
		acknowledged bool
		cancel       bool
		root         bool
		wantCalls    int
	}{
		{"unknown-commit", false, false, true, 0},
		{"acknowledged-cleanup-error", true, false, true, 1},
		{"acknowledged-cancellation", true, true, true, 1},
		{"field-only-import", true, false, false, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			fault := errors.New("scenario post-commit diagnostic")
			store := &scenarioSetupProbeStore{acknowledged: test.acknowledged, fault: fault}
			source, _ := acknowledgedRootInputEndpoint(t)
			bundle, _ := semanticview.Bundle(source)
			bundle.Semantics.Version = "1"
			var calls int
			finalizer := pipeline.CommittedFlowInstanceActivationFinalizerFunc(func(ctx context.Context, activation pipeline.CommittedFlowInstanceActivation) error {
				calls++
				if !activation.Acknowledged || correlation.RunIDFromContext(ctx) != activation.Plan.Readiness.RunID {
					t.Fatal("scenario finalizer lost exact acknowledged run authority")
				}
				if test.cancel && ctx.Err() == nil {
					t.Fatal("scenario finalizer concealed cancellation")
				}
				return ctx.Err()
			})
			bus, err := newScopedTestEventBus(store, EventBusOptions{
				ContractBundle: source, TemplateInstancePlanner: newTestFlowInstanceActivationOwner(nil), FlowActivationFinalizer: finalizer,
			})
			if err != nil {
				t.Fatal(err)
			}
			bus.routeTable, err = DeriveRouteTable(source)
			if err != nil {
				t.Fatal(err)
			}
			bus.durable.ActiveFlows = &topologyOperationDescriptors{}
			bus.durable.ScenarioSetup = store
			runID, entityID := uuid.NewString(), uuid.NewString()
			if test.root {
				entityID = runID
			}
			request := pipeline.ScenarioSetupRequest{RunID: runID, CreatedAt: time.Now().UTC(), Entities: []pipeline.ScenarioSetupEntityRequest{{EntityID: entityID, EntityType: "record", CurrentState: "pending"}}}
			ctx, cancel := context.WithCancel(testAuthorActivityContext(context.Background()))
			defer cancel()
			if test.cancel {
				store.cancel = cancel
			}
			result, err := bus.SetupScenarioEntities(ctx, request)
			if !errors.Is(err, fault) || result.Acknowledged != test.acknowledged || calls != test.wantCalls || store.calls != 1 {
				t.Fatalf("setup evidence: result=%+v err=%v finalizations=%d commits=%d", result, err, calls, store.calls)
			}
			if test.root && store.command.Activations[0].Plan.CreatingInput != (pipeline.FlowConstructionInput{}) {
				t.Fatal("scenario setup fabricated a creating business occurrence")
			}
		})
	}
}
