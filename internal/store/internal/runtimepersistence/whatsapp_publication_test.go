package runtimepersistence

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

// This proves the selected-store publication component, not SDK authentication,
// pack normalization or a connected capture-to-publication runtime journey.
func TestWhatsAppInboundPublicationRollbackAndLostAcknowledgmentBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := openStandingDispositionParityFixture(t, backend)
			store := f.selected.(inboundPublicationProofStore)
			candidate := f.candidate("whatsapp-publication")
			ctx := runtimecorrelation.WithSourceArtifactFact(testAuthorActivityContextForBundle(candidate.Source.BundleHash()), candidate.Source)
			scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
			if !ok {
				t.Fatal("publication component fixture requires its exact source scope")
			}
			lease, err := store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope,
				[]runtimeauthoractivity.EventDescriptor{{EventType: "inbound.whatsapp.message", Disposition: runtimeauthoractivity.StoryDifferent}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(lease.Release)
			standing, err := f.workflow.ReconcileStandingService(ctx, candidate)
			if err != nil {
				t.Fatal(err)
			}
			sequence, err := f.workflow.PublishStandingService(ctx, standing.ServiceID, standing.RunID, standing.Generation)
			if err != nil {
				t.Fatal(err)
			}
			request := inboundPublicationProofRequest(t, candidate, standing.RunID, standing.Generation, sequence, "WHATSAPP_CAPTURED_EVENT")
			request.Provider, request.TargetAlias = "whatsapp", "whatsapp"
			request.PublicationID, request.MarkerEventID = runtimeinbound.DeterministicIDs(request.Provider, request.EntityID, request.ProviderEventID)
			request.OriginalTransportMetadata = []byte(`{"transport":"managed_session"}`)
			body := []byte(`{ "conversation" : "original", "text" : "exact captured bytes" }`)
			request.RequestFingerprint, err = runtimeinbound.SemanticFingerprint(struct {
				Provider, EventID string
				Body              []byte
			}{request.Provider, request.ProviderEventID, body})
			if err != nil {
				t.Fatal(err)
			}
			childID, err := runtimeinbound.DeterministicEventID(request.PublicationID, 0)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := runtimeinbound.BuildEvidencePayload(request, []string{childID}, []string{"inbound.whatsapp.message"})
			if err != nil {
				t.Fatal(err)
			}
			finalization := runtimeinbound.Finalization{
				EvidenceEvent: eventtest.DiagnosticDirect(request.MarkerEventID, events.EventTypePlatformInboundRecord,
					"runtime", "", payload, 0, request.ResolvedRunID, "",
					events.EnvelopeForEntityID(events.EventEnvelope{}, request.EntityID), request.OriginalReceivedAt),
				Events: []runtimeinbound.EventFinalization{{Ordinal: 0, Kind: runtimeprovideroutput.KindRaw,
					Event: eventtest.ExistingRunRootIngress(childID, "inbound.whatsapp.message", "inbound-gateway", "", body, 0,
						request.ResolvedRunID, events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{
							EntityID: request.EntityID, FlowInstance: request.TargetFlowInstance,
						}), request.OriginalReceivedAt)}},
			}
			eventBus, err := newStoreTestEventBus(t, store, runtimebus.EventBusOptions{
				SourceArtifactFact: candidate.Source, ProviderOutputVerifier: inboundPublicationProofAuthorizationVerifier{},
			})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := eventBus.PrepareInboundDeliveryBatch(ctx, runtimebus.InboundDeliveryBatch{
				Provider: request.Provider, Events: []runtimebus.InboundDeliveryEvent{
					{Event: finalization.Events[0].Event, Kind: runtimeprovideroutput.KindRaw},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = eventBus.AbandonInboundDeliveryPlan(context.WithoutCancel(ctx), plan) }()
			prepared := plan.PreparedPublications()[0]
			finalization.Events[0].Event = prepared.Event
			finalization.Events[0].RecipientManifest, _, _, err = runtimeinbound.CanonicalRecipientManifest(prepared.DeliveryRoutes())
			if err != nil {
				t.Fatal(err)
			}
			projection, _ := runtimeauthoractivity.InboundProjectionFromContext(ctx)
			command := runtimeinbound.CommitCommand{Finalization: finalization,
				Publications: plan.CommitCommands(), AuthorProjection: projection}
			// Fail in the actual selected-store transaction after event planning.
			drop := installOperatorChannelClaimFailureTrigger(t, f.db, backend == "sqlite", "inbound_publication_events", "INSERT")
			publish := func() (runtimeinbound.CommitResult, error) {
				command.Request = request
				return store.CommitInboundPublication(ctx, command)
			}
			if _, err := publish(); err == nil {
				drop()
				t.Fatal("failed publication transaction reported an acknowledgment")
			}
			drop()
			for _, check := range []struct{ prefix, identity string }{
				{`SELECT COUNT(*) FROM inbound_publications WHERE publication_id = `, request.PublicationID},
				{`SELECT COUNT(*) FROM events WHERE event_id = `, childID},
				{`SELECT COUNT(*) FROM events WHERE event_id = `, request.MarkerEventID},
			} {
				assertInboundPublicationProofCount(t, f.db, backend == "sqlite", check.prefix, check.identity, 0)
			}
			committed, err := publish()
			if err != nil || !committed.Acknowledged || !committed.Record.Created || committed.Record.OutputCount != 1 {
				t.Fatalf("clean publication retry: %+v, %v", committed, err)
			}
			// Model a lost application acknowledgment: reload through the canonical
			// durable owner and retry, without treating current health as evidence.
			recovered, found, err := store.LoadInboundPublicationByIdentity(context.Background(), request.Provider, request.EntityID, request.ProviderEventID)
			if err != nil || !found || recovered.State != "committed" || len(recovered.Events) != 1 || !bytes.Equal(recovered.Events[0].Event.Payload(), body) {
				t.Fatalf("durable exact-byte readback: %+v, found=%t, %v", recovered, found, err)
			}
			if _, err := f.workflow.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: candidate.ServiceID, Actor: "publication-component-proof"}); err != nil {
				t.Fatal(err)
			}
			duplicate, err := publish()
			if err != nil || !duplicate.Acknowledged || duplicate.Record.Created || !reflect.DeepEqual(duplicate.Record.Events, recovered.Events) {
				t.Fatalf("lost acknowledgment replay changed historical publication: %+v, %v", duplicate, err)
			}
			assertInboundPublicationProofCount(t, f.db, backend == "sqlite", `SELECT COUNT(*) FROM events WHERE event_id = `, childID, 1)
			assertInboundPublicationProofCount(t, f.db, backend == "sqlite", `SELECT COUNT(*) FROM events WHERE event_id = `, request.MarkerEventID, 1)
			request.RequestFingerprint, err = runtimeinbound.SemanticFingerprint("different captured bytes")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := publish(); !errors.Is(err, runtimeinbound.ErrRequestIdentityConflict) {
				t.Fatalf("changed capture adopted existing publication: %v", err)
			}
		})
	}
}
