package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type inboundPublicationProofStore interface {
	runtimeinbound.Runner
}

func TestSQLiteInboundPublicationOperationCommitsRetriesAndRollsBackAtomically(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	store.SetEventPayloadValidator(currentPlatformPayloadValidatorForStoreTest(t))
	workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(store.DB, store)
	runInboundPublicationOperationProof(t, store.DB, true, store, workflowStore)
}

func TestPostgresInboundPublicationOperationCommitsRetriesAndRollsBackAtomically(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	store := &PostgresStore{DB: db}
	store.SetEventPayloadValidator(currentPlatformPayloadValidatorForStoreTest(t))
	runInboundPublicationOperationProof(t, db, false, store, runtimepipeline.NewWorkflowInstanceStore(db))
}

func runInboundPublicationOperationProof(t *testing.T, db *sql.DB, sqlite bool, store inboundPublicationProofStore, workflowStore *runtimepipeline.WorkflowInstanceStore) {
	t.Helper()
	ctx := context.Background()
	packageKey := "publication-proof"
	flowID := "ingress"
	serviceID := runtimeflowidentity.StandingServiceID(packageKey, flowID)
	instanceID := uuid.NewString()
	entityID := uuid.NewString()
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: packageKey, FlowID: flowID, InstanceID: instanceID, EntityID: entityID,
		Source: runtimecorrelation.BundleSourceFact{BundleHash: "bundle-v1:sha256:" + strings.Repeat("8", 64), BundleSource: "persisted"},
	}
	standing, err := workflowStore.ReconcileStandingService(ctx, candidate)
	if err != nil {
		t.Fatalf("ReconcileStandingService: %v", err)
	}
	sequence, err := workflowStore.PublishStandingService(ctx, serviceID, standing.RunID, standing.Generation)
	if err != nil {
		t.Fatalf("PublishStandingService: %v", err)
	}
	request := inboundPublicationProofRequest(t, candidate, standing.RunID, sequence, "delivery-1")
	callbackCalls := 0
	record, err := store.RunInboundPublicationMutation(ctx, request, func(mutation runtimeinbound.Mutation) error {
		callbackCalls++
		publication, evidence := inboundPublicationProofEvents(request)
		if err := mutation.AppendEvent(mutation.Context(), publication); err != nil {
			return err
		}
		if err := mutation.UpsertCommittedReplayScope(mutation.Context(), publication.ID(), runtimereplayclaim.CommittedReplayScopeSubscribed); err != nil {
			return err
		}
		return mutation.FinalizeInboundPublication(mutation.Context(), runtimeinbound.Finalization{
			EvidenceEvent: evidence, PublicationEvent: publication, RecipientManifest: []byte(`[]`),
		})
	})
	if err != nil {
		t.Fatalf("RunInboundPublicationMutation: %v", err)
	}
	if !record.Created || record.State != "committed" || callbackCalls != 1 {
		t.Fatalf("record = %#v calls=%d", record, callbackCalls)
	}
	if err := store.ValidateInboundPublicationIntegrity(ctx); err != nil {
		t.Fatalf("ValidateInboundPublicationIntegrity: %v", err)
	}
	duplicate, err := store.RunInboundPublicationMutation(ctx, request, func(runtimeinbound.Mutation) error {
		callbackCalls++
		return errors.New("exact retry invoked callback")
	})
	if err != nil {
		t.Fatalf("exact retry: %v", err)
	}
	if duplicate.Created || duplicate.PublicationEvent.ID() != request.PublicationEventID || callbackCalls != 1 {
		t.Fatalf("duplicate = %#v calls=%d", duplicate, callbackCalls)
	}
	conflict := request
	conflict.SemanticFingerprint = strings.Repeat("f", 64)
	if _, err := store.RunInboundPublicationMutation(ctx, conflict, func(runtimeinbound.Mutation) error { return nil }); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("conflicting retry error = %v", err)
	}

	failedRequest := inboundPublicationProofRequest(t, candidate, standing.RunID, sequence, "delivery-failure")
	publication, _ := inboundPublicationProofEvents(failedRequest)
	injected := errors.New("injected publication failure")
	if _, err := store.RunInboundPublicationMutation(ctx, failedRequest, func(mutation runtimeinbound.Mutation) error {
		if err := mutation.AppendEvent(mutation.Context(), publication); err != nil {
			return err
		}
		return injected
	}); !errors.Is(err, injected) {
		t.Fatalf("rollback error = %v", err)
	}
	assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM inbound_publications WHERE provider_event_id = `, failedRequest.ProviderEventID, 0)
	assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM events WHERE event_id = `, failedRequest.PublicationEventID, 0)

	assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM inbound_publications WHERE provider_event_id = `, request.ProviderEventID, 1)
	assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM events WHERE run_id = `, standing.RunID, 2)
}

func inboundPublicationProofRequest(t *testing.T, candidate runtimepipeline.StandingServiceCandidate, runID string, sequence int64, providerEventID string) runtimeinbound.Request {
	t.Helper()
	publicationID, markerEventID, publicationEventID := runtimeinbound.DeterministicIDs("github", candidate.EntityID, providerEventID)
	fingerprint, err := runtimeinbound.SemanticFingerprint(map[string]any{"provider": "github", "provider_event_id": providerEventID, "payload": map[string]any{"value": 1}})
	if err != nil {
		t.Fatal(err)
	}
	return runtimeinbound.Request{
		PublicationID: publicationID, Provider: "github", EntityID: candidate.EntityID, ProviderEventID: providerEventID,
		SemanticFingerprint: fingerprint, SemanticProjectionVersion: runtimeinbound.SemanticProjectionVersion,
		StableServiceID: candidate.ServiceID, PackageKey: candidate.PackageKey, FlowID: candidate.FlowID,
		InstanceID: candidate.InstanceID, TargetAlias: "github", TargetFlowInstance: candidate.FlowID + "/" + candidate.InstanceID,
		ExpectedPublicationSequence: sequence, ResolvedRunID: runID, MarkerEventID: markerEventID,
		PublicationEventID: publicationEventID, AcknowledgementMode: runtimeinbound.AcknowledgementDurableBeforeDispatch,
		OriginalReceivedAt: time.Now().UTC(), OriginalUserAgent: "proof", OriginalTransportMetadata: []byte(`{"method":"POST"}`),
	}
}

func inboundPublicationProofEvents(request runtimeinbound.Request) (events.Event, events.Event) {
	envelope := events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: request.EntityID, FlowInstance: request.TargetFlowInstance})
	publication := eventtest.RootIngress(request.PublicationEventID, "inbound.github.push", "inbound-gateway", "", []byte(`{"value":1}`), 0, request.ResolvedRunID, "", envelope, request.OriginalReceivedAt)
	evidence := eventtest.DiagnosticDirect(request.MarkerEventID, events.EventType(diagnosticDirectInboundRecord), "runtime", "", []byte(fmt.Sprintf(
		`{"publication_id":%q,"publication_event_id":%q,"provider":%q,"provider_event_id":%q,"entity_id":%q}`,
		request.PublicationID, request.PublicationEventID, request.Provider, request.ProviderEventID, request.EntityID,
	)), 0, request.ResolvedRunID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, request.EntityID), request.OriginalReceivedAt)
	return publication, evidence
}

func assertInboundPublicationProofCount(t *testing.T, db *sql.DB, sqlite bool, prefix, value string, want int) {
	t.Helper()
	placeholder := "$1"
	if sqlite {
		placeholder = "?"
	}
	var got int
	if err := db.QueryRow(prefix+placeholder, value).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("count for %q = %d, want %d", value, got, want)
	}
}
