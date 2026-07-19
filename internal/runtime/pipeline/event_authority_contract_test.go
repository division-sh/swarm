package pipeline

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

func TestHumanDecisionProducerRequiresExactTypedExternalIdentity(t *testing.T) {
	external := eventtest.RootIngress(
		uuid.NewString(), "decision.recorded", "human", "", nil, 0,
		uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC(),
	)
	if !hasHumanDecisionProducer(external) {
		t.Fatal("typed external human producer was not recognized")
	}
	lineage := events.EventLineage{RunID: uuid.NewString(), ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live}
	agent := eventtest.ReplayForProducer(
		uuid.NewString(), "decision.recorded", eventtest.Producer(events.EventProducerAgent, "human"), "", nil, 0,
		lineage, events.EventEnvelope{}, time.Now().UTC(),
	)
	if hasHumanDecisionProducer(agent) {
		t.Fatal("agent producer reused a human ID to acquire decision authority")
	}
}
