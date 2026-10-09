package bus

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

func TestSelectedInputValidationExactEvidence(t *testing.T) {
	source := selectedInputTestSource(t)
	runID := uuid.NewString()
	original := eventtest.OperatorInjectedWithRoutingSource(uuid.NewString(), "thing.created", "operator", "", []byte(`{}`), 0, runID, nil, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Now().UTC())
	var err error
	original, err = eventtest.AdmitPayload(original, ".", "thing.created")
	if err != nil {
		t.Fatal(err)
	}
	validation, err := RevalidateSelectedInput(source, original)
	if err != nil {
		t.Fatal(err)
	}
	makeForkEvent := func(sourceRun, sourceEvent string, eventType events.EventType, payload []byte) events.Event {
		t.Helper()
		lineage, err := events.NewSelectedForkLineage(uuid.NewString(), sourceRun, sourceEvent, "selected-owner", "", original.ExecutionMode())
		if err != nil {
			t.Fatal(err)
		}
		event, err := events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{Facts: events.EventFacts{
			ID: uuid.NewString(), Type: eventType, Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "selected-owner"}, Payload: payload,
			CreatedAt: time.Now().UTC(), ExecutionMode: original.ExecutionMode(),
		}, Lineage: lineage})
		if err != nil {
			t.Fatal(err)
		}
		return event
	}
	exact := makeForkEvent(original.RunID(), original.ID(), original.Type(), original.Payload())
	ctx, err := validation.bind(context.Background(), exact, validation.bundleHash)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := selectedInputValidationFromContext(ctx, exact); !ok {
		t.Fatal("exact selected event lost validation")
	}
	for _, tc := range []struct {
		name  string
		event events.Event
		hash  string
	}{
		{"source_run", makeForkEvent(uuid.NewString(), original.ID(), original.Type(), original.Payload()), validation.bundleHash},
		{"source_event", makeForkEvent(original.RunID(), uuid.NewString(), original.Type(), original.Payload()), validation.bundleHash},
		{"event_name", makeForkEvent(original.RunID(), original.ID(), "other.created", original.Payload()), validation.bundleHash},
		{"payload", makeForkEvent(original.RunID(), original.ID(), original.Type(), []byte(`{"extra":true}`)), validation.bundleHash},
		{"artifact", exact, "foreign-artifact"},
		{"operator", original, validation.bundleHash},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := validation.bind(context.Background(), tc.event, tc.hash); err == nil {
				t.Fatal("foreign selected evidence accepted")
			}
		})
	}
	if _, ok := selectedInputValidationFromContext(ctx, original); ok {
		t.Fatal("selected validation leaked into ordinary publication")
	}
	if _, err := RevalidateSelectedInput(source, exact); err == nil {
		t.Fatal("arbitrary replay became original input evidence")
	}
}

func TestSelectedInputValidationBindsStoreProjectedPayload(t *testing.T) {
	source := selectedInputTestSource(t)
	runID := uuid.NewString()
	original := eventtest.OperatorInjectedWithRoutingSource(uuid.NewString(), "thing.created", "operator", "", []byte(`{"revision_id":"source"}`), 0, runID, nil, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Now().UTC())
	var err error
	original, err = eventtest.AdmitPayload(original, ".", "thing.created")
	if err != nil {
		t.Fatal(err)
	}
	validation, err := RevalidateSelectedInput(source, original)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validation.WithStoreProjectedPayload([]byte(`{"revision_id":`)); err == nil {
		t.Fatal("invalid projected payload accepted")
	}
	projected := []byte(`{"revision_id":"fork"}`)
	bound, err := validation.WithStoreProjectedPayload(projected)
	if err != nil {
		t.Fatal(err)
	}
	projected[0] = '['
	makeEvent := func(payload []byte) events.Event {
		t.Helper()
		lineage, err := events.NewSelectedForkLineage(uuid.NewString(), original.RunID(), original.ID(), "selected-owner", "", original.ExecutionMode())
		if err != nil {
			t.Fatal(err)
		}
		event, err := events.NewSelectedForkReplayEvent(events.SelectedForkReplayEventInput{Facts: events.EventFacts{
			ID: uuid.NewString(), Type: original.Type(), Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: "selected-owner"}, Payload: payload,
			CreatedAt: time.Now().UTC(), ExecutionMode: original.ExecutionMode(),
		}, Lineage: lineage})
		if err != nil {
			t.Fatal(err)
		}
		return event
	}
	valid := makeEvent([]byte(`{"revision_id":"fork"}`))
	if _, err := bound.bind(context.Background(), valid, bound.bundleHash); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		validation SelectedInputValidation
		event      events.Event
	}{
		{"unprojected", validation, valid},
		{"source_payload", bound, makeEvent(original.Payload())},
		{"wrong_projected_payload", bound, makeEvent([]byte(`{"revision_id":"other"}`))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.validation.bind(context.Background(), tc.event, tc.validation.bundleHash); err == nil {
				t.Fatal("selected input accepted payload outside its exact binding")
			}
		})
	}
}

func TestSelectedInputValidationCannotWidenRecipientDisposition(t *testing.T) {
	source := selectedInputTestSource(t)
	runID := uuid.NewString()
	original := eventtest.OperatorInjectedWithRoutingSource(uuid.NewString(), "thing.created", "operator", "", []byte(`{}`), 0, runID, nil, events.EventEnvelope{}, eventtest.RootRoutingSource(runID), time.Now().UTC())
	original, err := eventtest.AdmitPayload(original, ".", "thing.created")
	if err != nil {
		t.Fatal(err)
	}
	validation, err := RevalidateSelectedInput(source, original)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := DeriveRouteTable(source)
	if err != nil {
		t.Fatal(err)
	}
	subscribers, err := routes.PubsubDeclarationDefinitions(".", []string{string(original.Type())})
	if err != nil {
		t.Fatal(err)
	}
	if len(validation.FilterSubscribers(subscribers)) == 0 {
		t.Fatal("fixture has no valid input recipients")
	}
	none, err := validation.SelectRecipients(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(none.FilterSubscribers(subscribers)) != 0 {
		t.Fatal("empty pending disposition recreated completed recipients")
	}
	if len(validation.FilterSubscribers(subscribers)) == 0 {
		t.Fatal("narrowing mutated original validation")
	}
}

func selectedInputTestSource(t *testing.T) semanticview.Source {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, canonicalrouting.CopySelectedInputValidationProbe(t), contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}
