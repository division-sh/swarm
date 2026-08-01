package bus

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
)

type inboundBatchPreflightMutation struct {
	appendCalls int
}

func (m *inboundBatchPreflightMutation) Context() context.Context {
	return WithCommitPublishTransaction(context.Background(), m)
}

func (*inboundBatchPreflightMutation) LoadPreparedPublishEvent(context.Context, string) (events.AdmittedEvent, bool, error) {
	return events.AdmittedEvent{}, false, nil
}

func (m *inboundBatchPreflightMutation) BeginPreparedPublish(context.Context, PreparedPublishEvent) (EventAppendOutcome, error) {
	m.appendCalls++
	return EventAppendOutcomeUnknown, errors.New("mutation append sentinel")
}

func (*inboundBatchPreflightMutation) FinalizePreparedPublish(context.Context, PreparedPublishFinalization) error {
	return errors.New("preflight sentinel must stop before finalization")
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
			mutation := &inboundBatchPreflightMutation{}
			if _, err := bus.PrepareInboundDeliveryBatchInMutation(inboundBatchProjectionContext(mutation.Context(), batch), batch); err == nil {
				t.Fatal("PrepareInboundDeliveryBatchInMutation error = nil, want fail-closed authorization rejection")
			}
			if mutation.appendCalls != 0 {
				t.Fatalf("AppendEvent calls = %d, want zero", mutation.appendCalls)
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
	mutation := &inboundBatchPreflightMutation{}
	batch := inboundBatchPreflightBatch(expected)
	if _, err := bus.PrepareInboundDeliveryBatchInMutation(inboundBatchProjectionContext(mutation.Context(), batch), batch); err == nil || !strings.Contains(err.Error(), "mutation append sentinel") {
		t.Fatalf("PrepareInboundDeliveryBatchInMutation error = %v, want mutation sentinel", err)
	}
	if mutation.appendCalls != 1 {
		t.Fatalf("AppendEvent calls = %d, want one", mutation.appendCalls)
	}
}

func TestPrepareInboundDeliveryBatchRequiresSingleMatchingProjectionContextBeforeMutation(t *testing.T) {
	expected := inboundBatchCurrentAuthorization()
	bus, err := newScopedTestEventBus(&InMemoryEventStore{}, EventBusOptions{
		ProviderOutputVerifier: inboundBatchAuthorizationVerifier{expected: expected},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	batch := inboundBatchPreflightBatch(expected)
	batch.AuthorSubjectType = "chat"
	batch.AuthorSubjectID = "42"
	batch.AuthorSummary = "hello"

	testCases := []struct {
		name string
		ctx  func(context.Context) context.Context
	}{
		{name: "missing", ctx: func(ctx context.Context) context.Context { return ctx }},
		{name: "mismatched", ctx: func(ctx context.Context) context.Context {
			return runtimeauthoractivity.WithInboundProjection(ctx, runtimeauthoractivity.InboundProjection{
				SubjectType: "chat", SubjectID: "different", Summary: "hello",
			})
		}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mutation := &inboundBatchPreflightMutation{}
			if _, err := bus.PrepareInboundDeliveryBatchInMutation(tc.ctx(mutation.Context()), batch); err == nil || !strings.Contains(err.Error(), "inbound author projection context") {
				t.Fatalf("PrepareInboundDeliveryBatchInMutation error = %v, want projection-context rejection", err)
			}
			if mutation.appendCalls != 0 {
				t.Fatalf("AppendEvent calls = %d, want zero", mutation.appendCalls)
			}
		})
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
			mutation := &inboundBatchPreflightMutation{}
			if _, err := bus.PrepareInboundDeliveryBatchInMutation(inboundBatchProjectionContext(mutation.Context(), batch), batch); err == nil {
				t.Fatal("PrepareInboundDeliveryBatchInMutation error = nil, want cardinality/order rejection")
			}
			if mutation.appendCalls != 0 {
				t.Fatalf("AppendEvent calls = %d, want zero", mutation.appendCalls)
			}
		})
	}
}

type inboundBatchOverlayMutation struct {
	ctx      context.Context
	overlays []*RouteTable
	active   []string
}

func (m *inboundBatchOverlayMutation) Context() context.Context {
	return WithCommitPublishTransaction(m.ctx, m)
}

func (*inboundBatchOverlayMutation) LoadPreparedPublishEvent(context.Context, string) (events.AdmittedEvent, bool, error) {
	return events.AdmittedEvent{}, false, nil
}

func (m *inboundBatchOverlayMutation) BeginPreparedPublish(ctx context.Context, prepared PreparedPublishEvent) (EventAppendOutcome, error) {
	m.overlays = append(m.overlays, transactionRouteTableFromContext(ctx))
	m.active = append(m.active, prepared.AdmittedEvent().ID())
	return EventAppendInserted, nil
}

func (m *inboundBatchOverlayMutation) FinalizePreparedPublish(_ context.Context, finalization PreparedPublishFinalization) error {
	if len(m.active) == 0 || m.active[len(m.active)-1] != finalization.Request().Event.ID() {
		return errors.New("prepared event finalization does not match active inbound event")
	}
	if err := finalization.Request().DeliveryReceipt.Record(nil); err != nil {
		return err
	}
	m.active = m.active[:len(m.active)-1]
	return nil
}

func TestPrepareInboundDeliveryBatchSharesTransactionRouteOverlayAcrossOrderedChildren(t *testing.T) {
	expected := inboundBatchCurrentAuthorization()
	bus, err := newScopedTestEventBus(&InMemoryEventStore{}, EventBusOptions{
		ProviderOutputVerifier: inboundBatchAuthorizationVerifier{expected: expected},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	mutation := &inboundBatchOverlayMutation{
		ctx: runtimepipeline.WithPipelineSQLTxContext(context.Background(), &sql.Tx{}),
	}
	batch := inboundBatchPreflightBatch(expected)
	if _, err := bus.PrepareInboundDeliveryBatchInMutation(inboundBatchProjectionContext(mutation.Context(), batch), batch); err != nil {
		t.Fatalf("PrepareInboundDeliveryBatchInMutation: %v", err)
	}
	if len(mutation.overlays) != 2 || mutation.overlays[0] == nil || mutation.overlays[1] == nil {
		t.Fatalf("transaction route overlays = %#v, want two non-nil observations", mutation.overlays)
	}
	if mutation.overlays[0] != mutation.overlays[1] {
		t.Fatal("ordered inbound children used different transaction route overlays")
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

func inboundBatchProjectionContext(ctx context.Context, batch InboundDeliveryBatch) context.Context {
	return runtimeauthoractivity.WithInboundProjection(ctx, runtimeauthoractivity.InboundProjection{
		SubjectType: batch.AuthorSubjectType,
		SubjectID:   batch.AuthorSubjectID,
		Summary:     batch.AuthorSummary,
	})
}

func inboundBatchPreflightEvent(eventName string) events.Event {
	return eventtest.ExistingRunRootIngress(
		eventtest.UUID("inbound-batch:"+eventName), events.EventType(eventName), "inbound-gateway", "", []byte(`{"chat_id":"42"}`), 0,
		eventtest.UUID("inbound-batch-run"), events.EventEnvelope{}, time.Unix(1, 0).UTC(),
	)
}
