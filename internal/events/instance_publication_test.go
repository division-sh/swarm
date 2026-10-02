package events

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

func instancePublicationInput(t *testing.T, flow string) InstancePublicationEventInput {
	t.Helper()
	route := RouteIdentity{FlowID: flow, FlowInstance: flow}
	if flow == "." {
		route.FlowInstance = testRunID
	}
	source, err := NewStaticFlowRoutingSource(route)
	if err != nil {
		t.Fatal(err)
	}
	eventType := "poll.tick"
	if flow != "." {
		eventType = flow + "/" + eventType
	}
	return InstancePublicationEventInput{RunID: testRunID, Facts: EventFacts{
		ID: uuid.NewString(), Type: EventType(eventType), Producer: ProducerClaim{Type: EventProducerInstance, ID: flow},
		Payload: []byte(`{}`), Envelope: EventEnvelope{FlowInstance: route.FlowInstance}, RoutingSource: source,
		CreatedAt: time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC), ExecutionMode: executionmode.Live,
	}}
}

func TestInstancePublicationAdmissionAndReadback(t *testing.T) {
	for _, flow := range []string{".", "account/poller"} {
		t.Run(flow, func(t *testing.T) {
			input := instancePublicationInput(t, flow)
			event, err := NewInstancePublicationEvent(input)
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := AdmitForPublish(event, AdmissionOptions{RequirePersistentUUIDIdentity: true})
			if err != nil || admitted.RunDisposition() != AdmittedRunRequireActive {
				t.Fatalf("ordinary admission = %v, %v", admitted.RunDisposition(), err)
			}
			payload := testPayloadAdmission(t, input.Facts.Payload)
			restored, err := RestoreAdmittedEvent(RestoredEventInput{Class: EventAdmissionInstancePublication, Facts: input.Facts, RunID: input.RunID, Payload: payload})
			if err != nil || restored.Event().Producer().ID() != flow || restored.Event().ParentEventID() != "" || restored.Event().RoutingSource() != event.RoutingSource() {
				t.Fatalf("readback lost exact parentless source: %#v, %v", restored, err)
			}
			if _, err := RestoreAdmittedEvent(RestoredEventInput{Class: EventAdmissionInstancePublication, Facts: input.Facts, RunID: input.RunID, ParentEventID: uuid.NewString(), Payload: payload}); err == nil {
				t.Fatal("readback fabricated a causal parent")
			}
		})
	}
}

func TestInstancePublicationRejectsAuthorityDrift(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*InstancePublicationEventInput)
	}{
		{"missing run", func(i *InstancePublicationEventInput) { i.RunID = "" }},
		{"platform producer", func(i *InstancePublicationEventInput) { i.Facts.Producer.Type = EventProducerPlatform }},
		{"node producer", func(i *InstancePublicationEventInput) { i.Facts.Producer.Type = EventProducerNode }},
		{"foreign declaration", func(i *InstancePublicationEventInput) { i.Facts.Producer.ID = "unrelated" }},
		{"absent source", func(i *InstancePublicationEventInput) { i.Facts.RoutingSource = NoRoutingSource() }},
		{"platform source", func(i *InstancePublicationEventInput) { i.Facts.RoutingSource = NewPlatformControlRoutingSource() }},
		{"platform event", func(i *InstancePublicationEventInput) { i.Facts.Type = "account/poller/platform.tick" }},
		{"unqualified child event", func(i *InstancePublicationEventInput) { i.Facts.Type = "poll.tick" }},
		{"foreign event", func(i *InstancePublicationEventInput) { i.Facts.Type = "unrelated/poll.tick" }},
		{"causal chain", func(i *InstancePublicationEventInput) { i.Facts.ChainDepth = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := instancePublicationInput(t, "account/poller")
			tc.mutate(&input)
			if _, err := NewInstancePublicationEvent(input); err == nil {
				t.Fatal("hostile instance publication admitted")
			}
		})
	}
	input := instancePublicationInput(t, "account/poller")
	if _, err := NewRunScopedRuntimeControlEvent(RunScopedRuntimeEventInput{Facts: input.Facts, RunID: input.RunID}); err == nil {
		t.Fatal("business instance acquired platform-control authority")
	}
}
