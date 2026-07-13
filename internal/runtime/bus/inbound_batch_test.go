package bus

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
)

type inboundBatchPreflightProofStore struct {
	InMemoryEventStore
	mutationCalls int
}

func (s *inboundBatchPreflightProofStore) RunEventMutation(context.Context, func(EventMutation) error) error {
	s.mutationCalls++
	return errors.New("mutation must not start during authorization preflight proof")
}

type inboundBatchAuthorizationVerifier struct {
	expected runtimeprovideroutput.Authorization
}

func (v inboundBatchAuthorizationVerifier) VerifyProviderOutputAuthorization(actual runtimeprovideroutput.Authorization) error {
	if !v.expected.Matches(actual) {
		return errors.New("authorization does not match current compiled owner")
	}
	return nil
}

func TestPublishInboundDeliveryRejectsInvalidProviderOutputAuthorizationBeforeMutation(t *testing.T) {
	expected := runtimeprovideroutput.Authorization{
		Provider: "telegram", Event: "inbound.telegram.text_message",
		PackID: "provider.telegram", PackVersion: "1.0.0",
		ManifestHash: "sha256:" + strings.Repeat("a", 64), GenerationID: "generation-current",
	}
	testCases := []struct {
		name   string
		mutate func(*InboundDeliveryBatch)
	}{
		{name: "missing authorization", mutate: func(batch *InboundDeliveryBatch) {
			batch.Events[0].Authorization = runtimeprovideroutput.Authorization{}
		}},
		{name: "partial authorization", mutate: func(batch *InboundDeliveryBatch) {
			batch.Events[0].Authorization.ManifestHash = ""
		}},
		{name: "provider mismatch", mutate: func(batch *InboundDeliveryBatch) {
			batch.Claim.Provider = "telegram-stale"
			batch.Events[0].Authorization.Provider = "telegram-stale"
		}},
		{name: "event mismatch", mutate: func(batch *InboundDeliveryBatch) {
			batch.Events[0].Event = inboundBatchPreflightEvent("inbound.telegram.edited_message")
			batch.Events[0].Authorization.Event = "inbound.telegram.edited_message"
		}},
		{name: "pack id mismatch", mutate: func(batch *InboundDeliveryBatch) {
			batch.Events[0].Authorization.PackID = "provider.telegram.stale"
		}},
		{name: "pack version mismatch", mutate: func(batch *InboundDeliveryBatch) {
			batch.Events[0].Authorization.PackVersion = "0.9.0"
		}},
		{name: "manifest hash mismatch", mutate: func(batch *InboundDeliveryBatch) {
			batch.Events[0].Authorization.ManifestHash = "sha256:" + strings.Repeat("b", 64)
		}},
		{name: "stale generation", mutate: func(batch *InboundDeliveryBatch) {
			batch.Events[0].Authorization.GenerationID = "generation-stale"
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := &inboundBatchPreflightProofStore{}
			bus, err := NewEventBusWithOptions(store, EventBusOptions{
				ProviderOutputVerifier: inboundBatchAuthorizationVerifier{expected: expected},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			batch := inboundBatchPreflightBatch(expected)
			tc.mutate(&batch)
			if _, err := bus.PublishInboundDelivery(context.Background(), batch); err == nil {
				t.Fatal("PublishInboundDelivery error = nil, want fail-closed authorization rejection")
			}
			if store.mutationCalls != 0 {
				t.Fatalf("RunEventMutation calls = %d, want zero", store.mutationCalls)
			}
		})
	}
}

func TestPublishInboundDeliveryAcceptsOnlyExactCurrentProviderOutputAuthorizationIntoMutation(t *testing.T) {
	expected := runtimeprovideroutput.Authorization{
		Provider: "telegram", Event: "inbound.telegram.text_message",
		PackID: "provider.telegram", PackVersion: "1.0.0",
		ManifestHash: "sha256:" + strings.Repeat("a", 64), GenerationID: "generation-current",
	}
	store := &inboundBatchPreflightProofStore{}
	bus, err := NewEventBusWithOptions(store, EventBusOptions{
		ProviderOutputVerifier: inboundBatchAuthorizationVerifier{expected: expected},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if _, err := bus.PublishInboundDelivery(context.Background(), inboundBatchPreflightBatch(expected)); err == nil || !strings.Contains(err.Error(), "mutation must not start") {
		t.Fatalf("PublishInboundDelivery error = %v, want mutation sentinel", err)
	}
	if store.mutationCalls != 1 {
		t.Fatalf("RunEventMutation calls = %d, want one", store.mutationCalls)
	}
}

func inboundBatchPreflightBatch(authorization runtimeprovideroutput.Authorization) InboundDeliveryBatch {
	return InboundDeliveryBatch{
		Claim: InboundDeliveryClaim{ProviderEventID: "delivery-1", EntityID: "entity-1", Provider: "telegram"},
		Events: []InboundDeliveryEvent{{
			Event: inboundBatchPreflightEvent("inbound.telegram.text_message"), Kind: runtimeprovideroutput.KindNormalized,
			Authorization: authorization,
		}},
	}
}

func inboundBatchPreflightEvent(eventName string) events.Event {
	return eventtest.RootIngress(
		"event-1", events.EventType(eventName), "inbound-gateway", "", []byte(`{"chat_id":"42"}`), 0,
		"run-1", "", events.EventEnvelope{}, time.Unix(1, 0).UTC(),
	)
}
