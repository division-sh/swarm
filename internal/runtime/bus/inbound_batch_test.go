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
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

type inboundBatchAuthorizationVerifier struct {
	expected runtimeprovideroutput.Authorization
}

func (v inboundBatchAuthorizationVerifier) VerifyProviderOutputAuthorization(actual runtimeprovideroutput.Authorization) error {
	if !v.expected.Matches(actual) {
		return errors.New("authorization does not match current compiled owner")
	}
	return nil
}

func TestPrepareInboundDeliveryBatchRejectsInvalidProviderOutputAuthorizationBeforeMutation(t *testing.T) {
	expected := inboundBatchCurrentAuthorization()
	if _, err := runtimeprovideroutput.NewAuthorization(
		"telegram", "inbound.telegram.text_message", "provider.telegram", "1.0.0", "", expected.Generation(),
	); err == nil {
		t.Fatal("incomplete provider-output authorization acquired authority")
	}
	testCases := []struct {
		name   string
		mutate func(*InboundDeliveryBatch)
	}{
		{name: "missing authorization", mutate: func(batch *InboundDeliveryBatch) {
			batch.Events[1].Authorization = runtimeprovideroutput.Authorization{}
		}},
		{name: "provider mismatch", mutate: func(batch *InboundDeliveryBatch) {
			batch.Provider = "telegram-stale"
		}},
		{name: "event mismatch", mutate: func(batch *InboundDeliveryBatch) {
			batch.Events[1].Event = inboundBatchPreflightEvent("inbound.telegram.edited_message")
		}},
		{name: "pack id mismatch", mutate: func(batch *InboundDeliveryBatch) {
			a := batch.Events[1].Authorization
			batch.Events[1].Authorization = runtimeprovideroutput.MustAuthorization(
				a.Provider(), a.Event(), "provider.telegram.stale", a.PackVersion(), a.ManifestHash(), a.Generation(),
			)
		}},
		{name: "pack version mismatch", mutate: func(batch *InboundDeliveryBatch) {
			a := batch.Events[1].Authorization
			batch.Events[1].Authorization = runtimeprovideroutput.MustAuthorization(
				a.Provider(), a.Event(), a.PackID(), "0.9.0", a.ManifestHash(), a.Generation(),
			)
		}},
		{name: "manifest hash mismatch", mutate: func(batch *InboundDeliveryBatch) {
			a := batch.Events[1].Authorization
			batch.Events[1].Authorization = runtimeprovideroutput.MustAuthorization(
				a.Provider(), a.Event(), a.PackID(), a.PackVersion(), "sha256:"+strings.Repeat("b", 64), a.Generation(),
			)
		}},
		{name: "stale generation", mutate: func(batch *InboundDeliveryBatch) {
			a := batch.Events[1].Authorization
			batch.Events[1].Authorization = runtimeprovideroutput.MustAuthorization(
				a.Provider(), a.Event(), a.PackID(), a.PackVersion(), a.ManifestHash(),
				triggergeneration.FromCanonicalBytes([]byte("generation-stale")),
			)
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := &InMemoryEventStore{}
			bus, err := newScopedTestEventBus(store, EventBusOptions{
				ProviderOutputVerifier: inboundBatchAuthorizationVerifier{expected: expected},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			batch := inboundBatchPreflightBatch(expected)
			tc.mutate(&batch)
			if _, err := bus.PrepareInboundDeliveryBatch(context.Background(), batch); err == nil {
				t.Fatal("PrepareInboundDeliveryBatch error = nil, want fail-closed authorization rejection")
			}
		})
	}
}

func TestPrepareInboundDeliveryBatchAcceptsOnlyExactCurrentProviderOutputAuthorizationIntoMutation(t *testing.T) {
	expected := inboundBatchCurrentAuthorization()
	store := &InMemoryEventStore{}
	bus, err := newScopedTestEventBus(store, EventBusOptions{
		ProviderOutputVerifier: inboundBatchAuthorizationVerifier{expected: expected},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	batch := inboundBatchPreflightBatch(expected)
	plan, err := bus.PrepareInboundDeliveryBatch(context.Background(), batch)
	if err != nil {
		t.Fatalf("PrepareInboundDeliveryBatch: %v", err)
	}
	if got := len(plan.CommitCommands()); got != 2 {
		t.Fatalf("CommitCommands = %d, want 2", got)
	}
}

func TestPrepareInboundDeliveryBatchRejectsNonExclusiveOrMisorderedOutputsBeforeMutation(t *testing.T) {
	expected := inboundBatchCurrentAuthorization()
	testCases := []struct {
		name   string
		mutate func(*InboundDeliveryBatch)
	}{
		{name: "normalized only", mutate: func(batch *InboundDeliveryBatch) { batch.Events = batch.Events[1:] }},
		{name: "raw at ordinal one", mutate: func(batch *InboundDeliveryBatch) { batch.Events[0], batch.Events[1] = batch.Events[1], batch.Events[0] }},
		{name: "two normalized branches", mutate: func(batch *InboundDeliveryBatch) {
			second := batch.Events[1]
			second.Event = inboundBatchPreflightEvent("inbound.telegram.edited_message")
			a := second.Authorization
			second.Authorization = runtimeprovideroutput.MustAuthorization(
				a.Provider(), "inbound.telegram.edited_message", a.PackID(), a.PackVersion(), a.ManifestHash(), a.Generation(),
			)
			batch.Events = append(batch.Events, second)
		}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			store := &InMemoryEventStore{}
			bus, err := newScopedTestEventBus(store, EventBusOptions{
				ProviderOutputVerifier: inboundBatchAuthorizationVerifier{expected: expected},
			})
			if err != nil {
				t.Fatalf("NewEventBusWithOptions: %v", err)
			}
			batch := inboundBatchPreflightBatch(expected)
			tc.mutate(&batch)
			if _, err := bus.PrepareInboundDeliveryBatch(context.Background(), batch); err == nil {
				t.Fatal("PrepareInboundDeliveryBatch error = nil, want cardinality/order rejection")
			}
		})
	}
}

func inboundBatchPreflightBatch(authorization runtimeprovideroutput.Authorization) InboundDeliveryBatch {
	return InboundDeliveryBatch{
		Provider: "telegram",
		Events: []InboundDeliveryEvent{
			{Event: inboundBatchPreflightEvent("inbound.telegram"), Kind: runtimeprovideroutput.KindRaw},
			{
				Event: inboundBatchPreflightEvent("inbound.telegram.text_message"), Kind: runtimeprovideroutput.KindNormalized,
				Authorization: authorization,
			},
		},
	}
}

func inboundBatchCurrentAuthorization() runtimeprovideroutput.Authorization {
	return runtimeprovideroutput.MustAuthorization(
		"telegram",
		"inbound.telegram.text_message",
		"provider.telegram",
		"1.0.0",
		"sha256:"+strings.Repeat("a", 64),
		triggergeneration.FromCanonicalBytes([]byte("generation-current")),
	)
}

func inboundBatchPreflightEvent(eventName string) events.Event {
	return eventtest.ExistingRunRootIngress(
		eventtest.UUID("inbound-batch:"+eventName), events.EventType(eventName), "inbound-gateway", "", []byte(`{"chat_id":"42"}`), 0,
		eventtest.UUID("inbound-batch-run"), events.EventEnvelope{}, time.Unix(1, 0).UTC(),
	)
}
