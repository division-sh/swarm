package events

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/google/uuid"
)

func inheritedOriginFixture(t *testing.T) (InheritedFanOutOrigin, EventFacts) {
	t.Helper()
	declaration, err := identity.AdmitDeclarationIdentity(".", "fan_out", "scatter/handler/items.ready/fan_out")
	if err != nil {
		t.Fatal(err)
	}
	origin, err := NewInheritedFanOutOrigin(uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), declaration, "bundle:exact", "digest:exact", 0)
	if err != nil {
		t.Fatal(err)
	}
	node, err := identity.AdmitExecutableNodeDeclaration(".", "scatter")
	if err != nil {
		t.Fatal(err)
	}
	facts := validFacts()
	facts.Producer = ProducerClaim{Type: EventProducerNode, ID: node.Key()}
	return origin, facts
}

func TestInheritedFanOutOriginIsNotCausalParentOrReplay(t *testing.T) {
	origin, facts := inheritedOriginFixture(t)
	event, err := NewInheritedFanOutEvent(InheritedFanOutEventInput{Facts: facts, Origin: origin})
	if err != nil {
		t.Fatal(err)
	}
	if event.RunID() != origin.RunID() || event.ParentEventID() != "" || event.AdmissionClass() != EventAdmissionInheritedFanOut {
		t.Fatal("inherited origin became ordinary causality")
	}
	if _, replay := event.SelectedForkLineage(); replay {
		t.Fatal("new ordinal mislabeled as selected replay")
	}
	if err := ValidateGenericPublishEvent(event); err == nil {
		t.Fatal("generic publication self-authorized inherited origin")
	}
	admitted, err := RestoreAdmittedEvent(RestoredEventInput{Class: EventAdmissionInheritedFanOut, Facts: facts, RunID: origin.RunID(), InheritedFanOut: &origin})
	if err != nil {
		t.Fatal(err)
	}
	actual, found := admitted.Event().InheritedFanOutOrigin()
	if !found || actual != origin {
		t.Fatal("exact origin lost on semantic readback")
	}
	clone := event.Clone()
	clone.inheritedFanOut.ordinal++
	if original, _ := event.InheritedFanOutOrigin(); original != origin {
		t.Fatal("clone aliased origin")
	}
	before, err := IntegrityProjection(event)
	if err != nil {
		t.Fatal(err)
	}
	after, err := IntegrityProjection(clone)
	if err != nil {
		t.Fatal(err)
	}
	if reflect.DeepEqual(before, after) {
		t.Fatal("integrity omitted ordinal origin")
	}
	raw, err := json.Marshal(origin)
	if err != nil {
		t.Fatal(err)
	}
	var forged InheritedFanOutOrigin
	if err := json.Unmarshal(raw, &forged); err != nil {
		t.Fatal(err)
	}
	if err := forged.Validate(); err == nil {
		t.Fatal("wire projection manufactured origin admission")
	}
}

func TestInheritedFanOutOriginRejectsContradictoryIdentity(t *testing.T) {
	for _, variant := range []string{"same_run", "run_absent", "foreign_uuid", "trigger_absent", "delivery_absent", "declaration_absent", "wrong_family", "bundle_absent", "digest_absent", "negative_ordinal", "non_node", "causal_parent", "wrong_readback_run", "missing_readback_origin"} {
		t.Run(variant, func(t *testing.T) {
			origin, facts := inheritedOriginFixture(t)
			switch variant {
			case "same_run":
				origin.sourceRunID = origin.runID
			case "run_absent":
				origin.runID = ""
			case "foreign_uuid":
				origin.sourceRunID = "not-a-uuid"
			case "trigger_absent":
				origin.triggerEventID = ""
			case "delivery_absent":
				origin.triggeringDeliveryID = ""
			case "declaration_absent":
				origin.declaration = identity.DeclarationIdentity{}
			case "wrong_family":
				origin.declaration, _ = identity.AdmitDeclarationIdentity(".", "rule", "scatter/rule")
			case "bundle_absent":
				origin.bundleHash = ""
			case "digest_absent":
				origin.semanticDigest = ""
			case "negative_ordinal":
				origin.ordinal = -1
			case "non_node":
				facts.Producer = ProducerClaim{Type: EventProducerAgent, ID: "observer"}
			case "causal_parent", "wrong_readback_run", "missing_readback_origin":
				input := RestoredEventInput{Class: EventAdmissionInheritedFanOut, Facts: facts, RunID: origin.RunID(), InheritedFanOut: &origin}
				if variant == "causal_parent" {
					input.ParentEventID = origin.TriggerEventID()
				}
				if variant == "wrong_readback_run" {
					input.RunID = uuid.NewString()
				}
				if variant == "missing_readback_origin" {
					input.InheritedFanOut = nil
				}
				if _, err := RestoreAdmittedEvent(input); err == nil {
					t.Fatal("contradictory readback admitted")
				}
				return
			}
			if _, err := NewInheritedFanOutEvent(InheritedFanOutEventInput{Facts: facts, Origin: origin}); err == nil {
				t.Fatal("contradictory origin admitted")
			}
		})
	}
}
