package inboundpublication

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/google/uuid"
)

func TestRequestCanonicalBytesRetainOriginalPublicationEvidence(t *testing.T) {
	request := evidenceProofRequest(t)
	request.ExpectedPublicationSequence = 7
	request.OriginalTransportMetadata = json.RawMessage(`{"second":2,"first":1}`)
	original, err := request.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	var decoded Request
	if err := json.Unmarshal(original, &decoded); err != nil || decoded.ExpectedPublicationSequence != 7 {
		t.Fatal("request encoding changed its original occurrence", err)
	}
	decoded.OriginalTransportMetadata = json.RawMessage(`{ "first": 1, "second": 2 }`)
	canonical, err := decoded.CanonicalBytes()
	if err != nil || !bytes.Equal(canonical, original) {
		t.Fatal("equivalent metadata changed canonical request evidence", err)
	}
	decoded.ExpectedPublicationSequence++
	changed, err := decoded.CanonicalBytes()
	if err != nil || bytes.Equal(changed, original) {
		t.Fatal("request encoding dropped its original sequence", err)
	}
	if got := (CommitCommand{Request: request}).PublicationSequence(); got != 7 {
		t.Fatalf("webhook request lost its existing exact fence: %d", got)
	}
}

func TestEventIntegrityFingerprintIncludesExactProducerAndClassAuthority(t *testing.T) {
	eventID := uuid.NewString()
	lineage := events.EventLineage{RunID: uuid.NewString(), ParentEventID: uuid.NewString(), ExecutionMode: executionmode.Live}
	buildReplay := func(producerType events.EventProducerType) events.Event {
		return eventtest.ReplayForProducer(
			eventID,
			"work.replayed",
			eventtest.Producer(producerType, "same-id"),
			"",
			nil,
			0,
			lineage,
			events.EventEnvelope{},
			time.Now().UTC(),
		)
	}
	left, err := EventIntegrityFingerprint(buildReplay(events.EventProducerAgent), runtimeprovideroutput.KindRaw, runtimeprovideroutput.Authorization{})
	if err != nil {
		t.Fatal(err)
	}
	right, err := EventIntegrityFingerprint(buildReplay(events.EventProducerNode), runtimeprovideroutput.KindRaw, runtimeprovideroutput.Authorization{})
	if err != nil {
		t.Fatal(err)
	}
	if left == right {
		t.Fatal("integrity fingerprint ignored producer_type")
	}
}

func TestEventIntegrityFingerprintIncludesExactPayloadBytes(t *testing.T) {
	eventID := uuid.NewString()
	runID := uuid.NewString()
	build := func(payload []byte) events.Event {
		return eventtest.ExistingRunRootIngress(
			eventID, "inbound.test", "gateway", "", payload, 0, runID,
			events.EventEnvelope{}, time.Date(2026, 7, 19, 12, 0, 0, 0, time.UTC),
		)
	}
	for _, tc := range []struct {
		name        string
		left, right []byte
	}{
		{name: "whitespace", left: []byte(`{"value":1}`), right: []byte(`{ "value": 1 }`)},
		{name: "key_order", left: []byte(`{"outer":{"a":1,"b":2},"value":true}`), right: []byte(`{"value":true,"outer":{"b":2,"a":1}}`)},
		{name: "numeric_lexeme", left: []byte(`{"value":1}`), right: []byte(`{"value":1.0}`)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leftFingerprint, err := EventIntegrityFingerprint(build(tc.left), runtimeprovideroutput.KindRaw, runtimeprovideroutput.Authorization{})
			if err != nil {
				t.Fatal(err)
			}
			rightFingerprint, err := EventIntegrityFingerprint(build(tc.right), runtimeprovideroutput.KindRaw, runtimeprovideroutput.Authorization{})
			if err != nil {
				t.Fatal(err)
			}
			if leftFingerprint == rightFingerprint {
				t.Fatalf("byte-distinct payloads share event integrity fingerprint %s", leftFingerprint)
			}
		})
	}
}

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
		{name: "reordered ids", payload: changedEvidencePayload(t, payload, "event_ids", []string{normalizedID, rawID})},
		{name: "wrong count", payload: changedEvidencePayload(t, payload, "output_count", 1)},
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
		{Recipient: events.MustNodeDeliveryRecipient(identitytest.RootNode(t, "worker")), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowInstance: "flow/one", EntityID: "entity-1"}),
			Context: events.DeliveryContext{Reply: &reply},
		},
		{Recipient: events.MustNodeDeliveryRecipient(identitytest.RootNode(t, "workflow-runtime")), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowInstance: "flow/one", EntityID: "entity-1"})},
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

func TestA9ReceiptRequestUsesExactAlias(t *testing.T) {
	request := evidenceProofRequest(t)
	request.ExpectedGeneration = 1
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, alias := range []string{" github", "github ", "/github", "github/", "github/child", ".github"} {
		changed := request
		changed.TargetAlias = alias
		if changed.Normalized().TargetAlias != alias {
			t.Fatalf("receipt coordinate %q was silently normalized", alias)
		}
		if err := changed.Validate(); err == nil {
			t.Fatalf("unadmitted alias %q entered receipt admission", alias)
		}
	}
}

func evidenceProofRequest(t *testing.T) Request {
	t.Helper()
	identity := Identity{ServiceID: flowidentity.StandingServiceID("ingress"), RunID: uuid.NewString(), Generation: 1, Provider: "github", ProviderEventID: "delivery-1"}
	publicationID, markerEventID, err := DeterministicIDs(identity)
	if err != nil {
		t.Fatal(err)
	}
	return Request{
		PublicationID: publicationID, Provider: "github", ProviderEventID: "delivery-1",
		RequestFingerprint: strings.Repeat("a", 64), RequestProjectionVersion: RequestSemanticProjectionVersion,
		StableServiceID: identity.ServiceID, FlowPath: "ingress", ExpectedGeneration: identity.Generation,
		TargetAlias: "github", ResolvedRunID: identity.RunID,
		MarkerEventID: markerEventID, AcknowledgementMode: AcknowledgementAfterPublish,
		OriginalReceivedAt: time.Unix(1, 0).UTC(), OriginalTransportMetadata: []byte(`{}`),
	}
}

func changedEvidencePayload(t *testing.T, payload json.RawMessage, field string, value any) json.RawMessage {
	t.Helper()
	var fields map[string]any
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	fields[field] = value
	result, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestA9InboundReceiptIdentity(t *testing.T) {
	request := evidenceProofRequest(t)
	if err := request.Validate(); err != nil {
		t.Fatal(err)
	}
	original := request.Identity()
	publication, marker, err := DeterministicIDs(original)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{publication: true}
	for _, mutate := range []func(*Identity){
		func(i *Identity) { i.ServiceID = flowidentity.StandingServiceID("other") },
		func(i *Identity) { i.RunID = uuid.NewString() },
		func(i *Identity) { i.Generation++ },
		func(i *Identity) { i.Provider = "telegram" },
		func(i *Identity) { i.ProviderEventID = "delivery-2" },
	} {
		changed := original
		mutate(&changed)
		id, evidence, err := DeterministicIDs(changed)
		if err != nil || seen[id] || evidence == marker {
			t.Fatalf("binding generation/delivery was collapsed: identity=%+v id=%s err=%v", changed, id, err)
		}
		seen[id] = true
	}
	for _, alias := range []string{"github", "other-alias"} {
		changed := request
		changed.TargetAlias = alias
		id, _, err := DeterministicIDs(changed.Identity())
		if err != nil || id != publication || changed.Validate() != nil {
			t.Fatalf("presentation became receipt identity: id=%s err=%v", id, err)
		}
	}
	for _, mutate := range []func(*Identity){
		func(i *Identity) { i.ServiceID = "" },
		func(i *Identity) { i.ServiceID = " " + i.ServiceID },
		func(i *Identity) { i.RunID = "" },
		func(i *Identity) { i.Generation = 0 },
		func(i *Identity) { i.Provider = "Telegram" },
		func(i *Identity) { i.ProviderEventID += " " },
	} {
		changed := original
		mutate(&changed)
		if id, evidence, err := DeterministicIDs(changed); err == nil || id != "" || evidence != "" {
			t.Fatalf("invalid identity acquired a reservation: %+v %s/%s %v", changed, id, evidence, err)
		}
	}
}

func evidenceProofEvent(request Request, payload json.RawMessage) events.Event {
	return eventtest.DiagnosticDirect(
		request.MarkerEventID, events.EventTypePlatformInboundRecord, "runtime", "", payload, 0,
		request.ResolvedRunID, "", events.EventEnvelope{}, request.OriginalReceivedAt,
	)
}
