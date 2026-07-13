package inboundpublication

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/google/uuid"
)

func TestEvidencePayloadOwnsExactOrderedCommittedBatch(t *testing.T) {
	request := evidenceProofRequest(t)
	rawID, err := DeterministicEventID(request.PublicationID, 0)
	if err != nil {
		t.Fatal(err)
	}
	normalizedID, err := DeterministicEventID(request.PublicationID, 1)
	if err != nil {
		t.Fatal(err)
	}
	eventIDs := []string{rawID, normalizedID}
	eventNames := []string{"inbound.github.push", "github.push.normalized"}
	payload, err := BuildEvidencePayload(request, eventIDs, eventNames)
	if err != nil {
		t.Fatalf("BuildEvidencePayload: %v", err)
	}
	evidence := evidenceProofEvent(request, payload)
	if err := ValidateEvidenceEvent(request, evidence, eventIDs, eventNames); err != nil {
		t.Fatalf("ValidateEvidenceEvent: %v", err)
	}

	testCases := []struct {
		name    string
		payload json.RawMessage
	}{
		{name: "reordered ids", payload: []byte(`{"publication_id":"` + request.PublicationID + `","provider":"github","provider_event_id":"delivery-1","entity_id":"` + request.EntityID + `","event_ids":["` + normalizedID + `","` + rawID + `"],"event_names":["inbound.github.push","github.push.normalized"],"output_count":2}`)},
		{name: "wrong count", payload: []byte(`{"publication_id":"` + request.PublicationID + `","provider":"github","provider_event_id":"delivery-1","entity_id":"` + request.EntityID + `","event_ids":["` + rawID + `","` + normalizedID + `"],"event_names":["inbound.github.push","github.push.normalized"],"output_count":1}`)},
		{name: "unknown field", payload: append(append([]byte{}, payload[:len(payload)-1]...), []byte(`,"legacy_event_id":"`+rawID+`"}`)...)},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateEvidenceEvent(request, evidenceProofEvent(request, tc.payload), eventIDs, eventNames); err == nil {
				t.Fatal("ValidateEvidenceEvent error = nil, want ordered evidence rejection")
			}
		})
	}
}

func TestCanonicalRecipientManifestIsOrderIndependent(t *testing.T) {
	reply := events.ReplyContextRef{ID: "reply-1"}
	routes := []events.DeliveryRoute{
		{
			SubscriberType: "node",
			SubscriberID:   "worker",
			Target:         events.RouteIdentity{FlowInstance: "flow/one", EntityID: "entity-1"},
			Context:        events.DeliveryContext{Reply: &reply},
		},
		{
			SubscriberType: "node",
			SubscriberID:   "workflow-runtime",
			Target:         events.RouteIdentity{FlowInstance: "flow/one", EntityID: "entity-1"},
		},
	}

	manifest, fingerprint, count, err := CanonicalRecipientManifest(routes)
	if err != nil {
		t.Fatalf("CanonicalRecipientManifest: %v", err)
	}
	reversedManifest, reversedFingerprint, reversedCount, err := CanonicalRecipientManifest([]events.DeliveryRoute{routes[1], routes[0]})
	if err != nil {
		t.Fatalf("CanonicalRecipientManifest reversed: %v", err)
	}
	if string(manifest) != string(reversedManifest) || fingerprint != reversedFingerprint || count != reversedCount {
		t.Fatalf("recipient manifest depends on route order: first=%s/%s/%d reversed=%s/%s/%d", manifest, fingerprint, count, reversedManifest, reversedFingerprint, reversedCount)
	}
}

func TestRequestRejectsNullTransportMetadata(t *testing.T) {
	request := evidenceProofRequest(t)
	request.OriginalTransportMetadata = json.RawMessage(`null`)
	if err := request.Validate(); err == nil {
		t.Fatal("Validate error = nil, want null transport metadata rejection")
	}
}

func evidenceProofRequest(t *testing.T) Request {
	t.Helper()
	entityID := uuid.NewString()
	publicationID, markerEventID := DeterministicIDs("github", entityID, "delivery-1")
	return Request{
		PublicationID: publicationID, Provider: "github", EntityID: entityID, ProviderEventID: "delivery-1",
		RequestFingerprint: strings.Repeat("a", 64), RequestProjectionVersion: RequestSemanticProjectionVersion,
		StableServiceID: uuid.NewString(), PackageKey: "proof", FlowID: "ingress", InstanceID: uuid.NewString(),
		TargetAlias: "github", TargetFlowInstance: "ingress/proof", ResolvedRunID: uuid.NewString(),
		MarkerEventID: markerEventID, AcknowledgementMode: AcknowledgementAfterPublish,
		OriginalReceivedAt: time.Unix(1, 0).UTC(), OriginalTransportMetadata: []byte(`{}`),
	}
}

func evidenceProofEvent(request Request, payload json.RawMessage) events.Event {
	return eventtest.DiagnosticDirect(
		request.MarkerEventID, events.EventTypePlatformInboundRecord, "runtime", "", payload, 0,
		request.ResolvedRunID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, request.EntityID), request.OriginalReceivedAt,
	)
}
