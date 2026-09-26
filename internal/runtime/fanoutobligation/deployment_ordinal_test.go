package fanoutobligation

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
)

func TestDeploymentOrdinalRootIngressHasExactFeedIdentity(t *testing.T) {
	request := deploymentRequest()
	now := time.Now().UTC()
	intent := Intent{Request: request, Source: request.Source, Status: StatusOpen, NextChunkSize: InitialChunkSize, CreatedAt: now, UpdatedAt: now}
	first, err := PrepareDeploymentOrdinalEmission(intent, 0)
	if err != nil {
		t.Fatal(err)
	}
	facts := events.EventFacts{Payload: []byte(`{"value":1}`), Envelope: events.EventEnvelope{Scope: events.EventScopeGlobal}, ExecutionMode: executionmode.Live}
	event, err := first.NewEvent(facts)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.ValidateEvent(event); err != nil {
		t.Fatal(err)
	}
	committed := intent
	committed.Cursor = 1
	if err := ValidateCommittedDeploymentOrdinalEvent(committed, 0, event.ID()); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCommittedDeploymentOrdinalEvent(committed, 0, deploymentOrdinalEventID(IntentKey{DeploymentFeedID: "other"}, 0)); err == nil {
		t.Fatal("foreign feed event was accepted as the committed ordinal")
	}
	if err := ValidateCommittedDeploymentOrdinalEvent(committed, 1, event.ID()); err == nil {
		t.Fatal("event outside the committed cursor was accepted")
	}
	other, err := PrepareDeploymentOrdinalEmission(intent, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := other.ValidateEvent(event); err == nil {
		t.Fatal("different ordinal accepted the first event")
	}
	if _, err := PrepareDeploymentOrdinalEmission(intent, 2); err == nil {
		t.Fatal("out-of-range ordinal accepted")
	}
}
