package bus

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// These wrappers require explicit declaration/instance data and delegate only
// to the compiled owners. They do not observe existence or grant readiness.
func (rt *RouteTable) PubsubDeclarationDefinitionsFixture(t testing.TB, flowID string, keys ...string) []Subscriber {
	t.Helper()
	definitions, err := rt.PubsubDeclarationDefinitions(flowID, keys)
	if err != nil {
		t.Fatal(err)
	}
	return definitions
}

func AdmittedFlowInstanceObservationFixture(t testing.TB, source semanticview.Source, runID string, instance flowidentity.Instance, key string) pipeline.FlowInstanceObservation {
	return constructionIndexObservation(t, source, runID, instance, key)
}

func FlowInstanceIndexFixture(observations ...pipeline.FlowInstanceObservation) pipeline.FlowInstanceIndexReader {
	return constructionIndexTestReader{observations: observations}
}

func (rt *RouteTable) PubsubReceiverDefinitionsFixture(t testing.TB, runID string, instance flowidentity.Instance, keys ...string) []Subscriber {
	t.Helper()
	definitions, err := rt.PubsubReceiverDefinitions(runID, instance, keys)
	if err != nil {
		t.Fatal(err)
	}
	return definitions
}
