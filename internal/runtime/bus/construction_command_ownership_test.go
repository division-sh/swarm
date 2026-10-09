package bus

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestConstructionCommandsDoNotCarryInstanceRouteMirror(t *testing.T) {
	for _, command := range []any{FlowInstanceActivationCommand{}, PublicationCommand{}, CommittedPublication{}, pipeline.WorkflowEngineMutationCommand{}, pipeline.CommittedWorkflowEngineMutation{}} {
		typeOf := reflect.TypeOf(command)
		if field, found := typeOf.FieldByName("RouteTopology"); found {
			t.Fatalf("%s restores non-authoritative instance-route persistence: %s", typeOf, field.Type)
		}
		if field, found := typeOf.FieldByName("RouteRetirement"); found {
			t.Fatalf("%s restores non-authoritative instance-route retirement: %s", typeOf, field.Type)
		}
	}
}

func TestEventBusDoesNotOwnInstanceRoutePublication(t *testing.T) {
	typeOf := reflect.TypeOf((*EventBus)(nil))
	for _, name := range []string{"PublishPersistedFlowInstanceRouteForAttempt", "RetireFlowInstanceRouteForAttempt", "ListFlowInstanceRoutes", "VerifyFlowInstanceRoute"} {
		if _, found := typeOf.MethodByName(name); found {
			t.Fatalf("EventBus restores parallel attachment ownership: %s", name)
		}
	}
	for _, name := range []string{"publications", "fencedPublications", "nextPublication", "generationMu", "generation"} {
		if _, found := reflect.TypeOf(RouteTable{}).FieldByName(name); found {
			t.Fatalf("RouteTable restores mutable publication ownership: %s", name)
		}
	}
}

func TestEventBusDoesNotRequireStandaloneInstanceRouteMutation(t *testing.T) {
	owner := reflect.TypeOf(DurableDependencies{})
	for _, name := range []string{"FlowRouteSets", "FlowRouteRollback", "FlowRoutes", "FlowRouteRecords"} {
		if _, present := owner.FieldByName(name); present {
			t.Fatalf("EventBus restores standalone instance-route mutation dependency %s", name)
		}
	}
}
