package runtimepersistence

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/providertriggers"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/semanticviewtest"
)

// This proves the selected-store publication component, not SDK authentication,
// pack normalization or a connected capture-to-publication runtime journey.
func TestWhatsAppInboundPublicationRollbackAndLostAcknowledgmentBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := openStandingDispositionParityFixture(t, backend)
			store := f.selected.(inboundPublicationProofStore)
			candidate := f.candidate("whatsapp-publication")
			candidate.FlowPath = "."
			candidate.ServiceID = runtimeflowidentity.StandingServiceID(candidate.FlowPath)
			ctx := runtimecorrelation.WithSourceArtifactFact(testAuthorActivityContextForBundle(candidate.Source.BundleHash()), candidate.Source)
			scope, ok := runtimeauthoractivity.ScopeFromContext(ctx)
			if !ok {
				t.Fatal("publication component fixture requires its exact source scope")
			}
			lease, err := store.(testAuthorActivityCatalogRegistrar).RegisterAuthorActivityEventCatalog(scope,
				[]runtimeauthoractivity.EventDescriptor{{EventType: "inbound.whatsapp.message", Disposition: runtimeauthoractivity.StoryAuthored}})
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
			request.PublicationID, request.MarkerEventID, err = runtimeinbound.DeterministicIDs(request.Identity())
			if err != nil {
				t.Fatal(err)
			}
			request.OriginalTransportMetadata = []byte(`{"transport":"managed_session"}`)
			body := []byte(`{ "conversation" : "original", "text" : "exact captured bytes" }`)
			// This store-only fixture uses authenticated webhook admission. It
			// cannot manufacture a native session or stand in for the SDK journey.
			const manifestText = `provider: whatsapp
secret: {required: true}
signature: {type: hmac_sha256, header: X-Signature, prefix: "sha256=", signed_payload: raw_body}
delivery_id: {literal: WHATSAPP_CAPTURED_EVENT}
event_type: {literal: fixture}
event_name: {literal: inbound.whatsapp.message}
ack: {mode: durable_before_dispatch}
`
			manifest, err := providertriggers.ParseManifest([]byte(manifestText))
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256([]byte(manifestText))
			catalog, err := providertriggers.NewCatalogSnapshot(providertriggers.CatalogEntry{
				Manifest: manifest, Source: "authenticated store component fixture",
				Identity: providertriggers.PackIdentity{ID: "provider.whatsapp.fixture", Version: "1.0.0", ManifestHash: "sha256:" + hex.EncodeToString(digest[:]), Provenance: "test"},
			})
			if err != nil {
				t.Fatal(err)
			}
			trigger, err := catalog.CompileAdmission(providertriggers.CompileAdmissionRequest{Alias: request.TargetAlias, Provider: request.Provider, SigningSecret: "component-proof-secret"})
			if err != nil {
				t.Fatal(err)
			}
			var content map[string]any
			if err := json.Unmarshal(body, &content); err != nil {
				t.Fatal(err)
			}
			mac := hmac.New(sha256.New, []byte("component-proof-secret"))
			_, _ = mac.Write(body)
			authenticated, err := trigger.AdmitRequest(providertriggers.Request{Provider: request.Provider,
				Target:  providertriggers.Target{WebhookSecret: "component-proof-secret"},
				Headers: http.Header{"X-Signature": {"sha256=" + hex.EncodeToString(mac.Sum(nil))}},
				Body:    body, Payload: content, Received: request.OriginalReceivedAt})
			if err != nil {
				t.Fatal(err)
			}
			delivery, admission, err := trigger.ProjectPublication(authenticated, candidate.Source.BundleHash(), request.FlowPath)
			if err != nil {
				t.Fatal(err)
			}
			eventPayload, err := json.Marshal(delivery.Events[0].Payload)
			if err != nil {
				t.Fatal(err)
			}
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
			source, err := events.NewExternalIngressRoutingSource(request.FlowPath, events.RoutingSourceAuthorityProviderAdmissionPlan)
			if err != nil {
				t.Fatal(err)
			}
			finalization := runtimeinbound.Finalization{
				EvidenceEvent: eventtest.DiagnosticDirect(request.MarkerEventID, events.EventTypePlatformInboundRecord,
					"runtime", "", payload, 0, request.ResolvedRunID, "",
					events.EventEnvelope{}, request.OriginalReceivedAt),
				Events: []runtimeinbound.EventFinalization{{Ordinal: 0, Kind: runtimeprovideroutput.KindRaw,
					Event: eventtest.ExistingRunRootIngressWithRoutingSource(childID, "inbound.whatsapp.message", "inbound-gateway", "", eventPayload, 0,
						request.ResolvedRunID, events.EventEnvelope{}, source, request.OriginalReceivedAt)}},
			}
			eventBus, err := newStoreTestEventBus(t, store, runtimebus.EventBusOptions{
				SourceArtifactFact: candidate.Source,
				ContractBundle:     whatsappPublicationComponentSource(t),
			})
			if err != nil {
				t.Fatal(err)
			}
			// Managed-session publication admission is still required here; the
			// retired verifier double must not substitute webhook authority.
			plan, err := eventBus.PrepareInboundDeliveryBatch(ctx, runtimebus.InboundDeliveryBatch{
				Provider: request.Provider, Admission: admission, Events: []runtimebus.InboundDeliveryEvent{
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
			command := runtimeinbound.CommitCommand{Admission: plan.Admission(), Finalization: finalization,
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
			recovered, found, err := store.LoadInboundPublicationByIdentity(context.Background(), request.Identity())
			if err != nil || !found || recovered.State != "committed" || len(recovered.Events) != 1 || !bytes.Equal(recovered.Events[0].Event.Payload(), eventPayload) {
				t.Fatalf("durable exact-byte readback: %+v, found=%t, %v", recovered, found, err)
			}
			applied, err := eventBus.ApplyInboundDeliveryCommit(ctx, plan, committed.Publications)
			if err != nil {
				t.Fatal(err)
			}
			for _, publication := range applied {
				if err := eventBus.DispatchPreparedPublish(ctx, publication); err != nil {
					t.Fatal(err)
				}
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

func whatsappPublicationComponentSource(t *testing.T) semanticview.Source {
	t.Helper()
	root := &runtimecontracts.FlowContractView{Path: ".", Paths: runtimecontracts.FlowContractPaths{FlowPath: ".", SchemaFile: "schema.yaml"},
		Events: map[string]runtimecontracts.EventCatalogEntry{"inbound.whatsapp.message": {Payload: runtimecontracts.EventPayloadSpec{Type: "object"}}}}
	bundle := &runtimecontracts.WorkflowContractBundle{RootSchema: &root.Schema,
		FlowSources: map[string]runtimecontracts.FlowSource{".": {FlowPath: ".", Schema: "schema.yaml"}},
		FlowSchemas: map[string]runtimecontracts.FlowSchemaDocument{".": root.Schema},
		FlowTree:    flowmodel.Tree[runtimecontracts.FlowContractView]{Root: root, ByID: map[string]*runtimecontracts.FlowContractView{".": root}, ByPath: map[string]*runtimecontracts.FlowContractView{".": root}}}
	if err := runtimecontracts.CompileWorkflowSemantics(bundle); err != nil {
		t.Fatal(err)
	}
	return semanticviewtest.WithProviderIngress(semanticview.Wrap(bundle), map[string][]string{".": {"inbound.whatsapp.message"}})
}
