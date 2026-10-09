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
