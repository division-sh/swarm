package fanoutobligation

import (
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/google/uuid"
)

func ordinalEmissionFixture(t *testing.T, inherited bool) (Intent, events.Event, events.EventFacts) {
	t.Helper()
	request := validIntentRequest(t)
	node, err := identity.AdmitExecutableNodeDeclaration(".", "scatter")
	if err != nil {
		t.Fatal(err)
	}
	request.Capsule.NodeKey = node.Key()
	request.Capsule.ChainDepth = 3
	trigger := eventtest.RunCreatingRootIngress(request.Capsule.Lineage.ParentEventID, "items.ready", "", "", []byte(`{"items":[1,2,3]}`), 0,
		request.Capsule.Lineage.RunID, "", events.EventEnvelope{}, time.Now().UTC())
	if inherited {
		request.Key.RunID = uuid.NewString()
	}
	now := time.Now().UTC()
	intent := Intent{Request: request, Source: request.Source, Cursor: 1, Status: StatusOpen, NextChunkSize: InitialChunkSize, CreatedAt: now, UpdatedAt: now}
	facts := events.EventFacts{ID: uuid.NewString(), Type: "item.ready", Producer: events.ProducerClaim{Type: events.EventProducerNode, ID: node.Key()},
		Payload: []byte(`{"item":2}`), ChainDepth: 4, RoutingSource: request.Capsule.ProducerSource,
		Envelope: events.EnvelopeForSourceRoute(events.EventEnvelope{}, request.Capsule.ProducerSource.Route()), CreatedAt: now}
	return intent, trigger, facts
}

func TestOrdinalEmissionPreservesCausalityAndInheritedOriginSeparately(t *testing.T) {
	for _, inherited := range []bool{false, true} {
		t.Run(map[bool]string{false: "same_run", true: "ancestor_origin"}[inherited], func(t *testing.T) {
			intent, trigger, facts := ordinalEmissionFixture(t, inherited)
			before, err := events.IntegrityProjection(trigger)
			if err != nil {
				t.Fatal(err)
			}
			projection, err := PrepareOrdinalEmission(intent, trigger, 1)
			if err != nil {
				t.Fatal(err)
			}
			emitted, err := projection.NewEvent(facts)
			if err != nil {
				t.Fatal(err)
			}
			if err := projection.ValidateEvent(emitted); err != nil {
				t.Fatal(err)
			}
			if emitted.ID() == trigger.ID() || emitted.RunID() != intent.Request.Key.RunID {
				t.Fatal("new ordinal copied the trigger or changed its destination")
			}
			origin, exists := emitted.InheritedFanOutOrigin()
			if exists != inherited {
				t.Fatal("ordinary causality and inherited origin were conflated")
			}
			if inherited {
				declaration, _ := intent.Request.Key.ElementRef.DeclarationIdentity()
				if emitted.ParentEventID() != "" || origin.SourceRunID() != trigger.RunID() || origin.TriggerEventID() != trigger.ID() ||
					origin.TriggeringDeliveryID() != intent.Request.Key.TriggeringDeliveryID || origin.Declaration() != declaration ||
					origin.BundleHash() != intent.Request.PlanRef.BundleHash || origin.SemanticDigest() != intent.Request.PlanRef.SemanticDigest || origin.Ordinal() != 1 {
					t.Fatalf("lost exact origin: %#v", origin)
				}
				if err := events.ValidateGenericPublishEvent(emitted); err == nil {
					t.Fatal("generic publication authorized inherited origin")
				}
			} else if emitted.ParentEventID() != trigger.ID() || emitted.AdmissionClass() != events.EventAdmissionChild {
				t.Fatal("ordinary child lost its same-run parent")
			}
			after, err := events.IntegrityProjection(trigger)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("projection mutated the original trigger")
			}
		})
	}
}

func TestOrdinalEmissionRejectsWrongTriggerRangeAndExecutionFacts(t *testing.T) {
	for _, variant := range []string{"source_run", "source_event", "source_task", "source_mode", "consumed_ordinal", "past_end", "node", "producer_kind", "source", "depth", "zero_owner"} {
		t.Run(variant, func(t *testing.T) {
			intent, trigger, facts := ordinalEmissionFixture(t, true)
			ordinal := 1
			switch variant {
			case "source_run":
				intent.Request.Capsule.Lineage.RunID = uuid.NewString()
			case "source_event":
				intent.Request.Capsule.Lineage.ParentEventID = uuid.NewString()
				intent.Request.Source.EventID = intent.Request.Capsule.Lineage.ParentEventID
				intent.Source = intent.Request.Source
			case "source_task":
				intent.Request.Capsule.Lineage.TaskID = "wrong-task"
			case "source_mode":
				intent.Request.Capsule.Lineage.ExecutionMode = "mock"
			case "consumed_ordinal":
				ordinal = 0
			case "past_end":
				ordinal = 3
			case "node":
				facts.Producer.ID = "other"
			case "producer_kind":
				facts.Producer.Type = events.EventProducerAgent
			case "source":
				facts.RoutingSource = events.NoRoutingSource()
			case "depth":
				facts.ChainDepth++
			}
			projection, err := PrepareOrdinalEmission(intent, trigger, ordinal)
			if err == nil {
				if variant == "zero_owner" {
					projection = OrdinalEmission{}
				}
				_, err = projection.NewEvent(facts)
			}
			if err == nil {
				t.Fatal("contradictory ordinal emission accepted")
			}
		})
	}
}

func TestOrdinalEmissionValidatorRejectsEveryOriginCoordinate(t *testing.T) {
	intent, trigger, facts := ordinalEmissionFixture(t, true)
	exact, err := PrepareOrdinalEmission(intent, trigger, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"run", "delivery", "declaration", "bundle", "digest", "ordinal", "ordinary_parent"} {
		t.Run(variant, func(t *testing.T) {
			other := intent
			ordinal := 1
			switch variant {
			case "run":
				other.Request.Key.RunID = uuid.NewString()
			case "delivery":
				other.Request.Key.TriggeringDeliveryID = uuid.NewString()
			case "declaration":
				other.Request.Key.ElementRef.SemanticPath += "/sibling"
				other.Request.PlanRef.ElementRef = other.Request.Key.ElementRef
			case "bundle":
				other.Request.PlanRef.BundleHash += "-other"
			case "digest":
				other.Request.PlanRef.SemanticDigest += "-other"
			case "ordinal":
				ordinal = 2
			case "ordinary_parent":
				other.Request.Key.RunID = trigger.RunID()
			}
			projection, err := PrepareOrdinalEmission(other, trigger, ordinal)
			if err != nil {
				t.Fatal(err)
			}
			emitted, err := projection.NewEvent(facts)
			if err != nil {
				t.Fatal(err)
			}
			if err := exact.ValidateEvent(emitted); err == nil {
				t.Fatal("wrong origin matched exact ordinal")
			}
		})
	}
}
