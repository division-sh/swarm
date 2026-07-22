package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type inboundPublicationProofStore interface {
	runtimeinbound.Runner
	runtimebus.EventStore
}

func commitInboundPublicationTestEvent(t *testing.T, store runtimebus.EventStore, mutation runtimeinbound.Mutation, publication *runtimeinbound.EventFinalization) error {
	t.Helper()
	if publication == nil {
		return errors.New("inbound publication test event is required")
	}
	admitted, err := events.AdmitForPublish(publication.Event, events.AdmissionOptions{RequirePersistentUUIDIdentity: true})
	if err != nil {
		return err
	}
	if admitted.RunDisposition() != events.AdmittedRunRequireActive {
		return fmt.Errorf("standing inbound root disposition = %q, want require_active", admitted.RunDisposition())
	}
	eventBus, err := newStoreTestEventBus(t, store)
	if err != nil {
		return err
	}
	prepared, err := eventBus.PreparePublishInMutation(mutation.Context(), publication.Event)
	if err != nil {
		return err
	}
	publication.Event = prepared.Event
	return nil
}

func TestSQLiteInboundPublicationOperationCommitsRetriesAndRollsBackAtomically(t *testing.T) {
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	store.SetEventPayloadValidator(currentPlatformPayloadValidatorForStoreTest(t))
	workflowStore := runtimepipeline.NewSQLiteWorkflowInstanceStoreWithRuntimeMutationRunner(store.DB, store)
	workflowStore.ConfigureDeliveryLifecycleStore(store)
	runInboundPublicationOperationProof(t, store.DB, true, store, workflowStore)
}

func TestPostgresInboundPublicationOperationCommitsRetriesAndRollsBackAtomically(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	store := admitTestPostgresStore(t, db)
	store.SetEventPayloadValidator(currentPlatformPayloadValidatorForStoreTest(t))
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	workflowStore.ConfigureDeliveryLifecycleStore(store)
	runInboundPublicationOperationProof(t, db, false, store, workflowStore)
}

func runInboundPublicationOperationProof(t *testing.T, db *sql.DB, sqlite bool, store inboundPublicationProofStore, workflowStore *runtimepipeline.WorkflowInstanceStore) {
	t.Helper()
	packageKey := "publication-proof"
	flowID := "ingress"
	serviceID := runtimeflowidentity.StandingServiceID(packageKey, flowID)
	instanceID := uuid.NewString()
	entityID := uuid.NewString()
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: packageKey, FlowID: flowID, InstanceID: instanceID, EntityID: entityID,
		Source: runtimecorrelation.BundleSourceFact{BundleHash: "bundle-v1:sha256:" + strings.Repeat("8", 64), BundleSource: "persisted"},
	}
	ctx := testAuthorActivityContextForBundle(candidate.Source.BundleHash)
	registrar, ok := store.(testAuthorActivityCatalogRegistrar)
	if !ok {
		t.Fatalf("inbound publication proof store %T cannot register author activity catalog", store)
	}
	registerTestAuthorActivityCatalogForContext(t, registrar, ctx)
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
	var publications []runtimeinbound.EventFinalization
	record, err := store.RunInboundPublicationMutation(ctx, request, func(mutation runtimeinbound.Mutation) error {
		callbackCalls++
		var evidence events.Event
		publications, evidence = inboundPublicationProofEvents(t, request)
		for index := range publications {
			if err := commitInboundPublicationTestEvent(t, store, mutation, &publications[index]); err != nil {
				return err
			}
		}
		return mutation.FinalizeInboundPublication(mutation.Context(), runtimeinbound.Finalization{
			EvidenceEvent: evidence, Events: publications,
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
	eventID, _ := runtimeinbound.DeterministicEventID(request.PublicationID, 0)
	if duplicate.Created || len(duplicate.Events) != 2 || duplicate.Events[0].EventID != eventID || callbackCalls != 1 {
		t.Fatalf("duplicate = %#v calls=%d", duplicate, callbackCalls)
	}
	conflict := request
	conflict.RequestFingerprint = strings.Repeat("f", 64)
	if _, err := store.RunInboundPublicationMutation(ctx, conflict, func(runtimeinbound.Mutation) error { return nil }); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("conflicting retry error = %v", err)
	}

	failedRequest := inboundPublicationProofRequest(t, candidate, standing.RunID, sequence, "delivery-failure")
	failedPublications, _ := inboundPublicationProofEvents(t, failedRequest)
	injected := errors.New("injected publication failure")
	if _, err := store.RunInboundPublicationMutation(ctx, failedRequest, func(mutation runtimeinbound.Mutation) error {
		if err := commitInboundPublicationTestEvent(t, store, mutation, &failedPublications[0]); err != nil {
			return err
		}
		return injected
	}); !errors.Is(err, injected) {
		t.Fatalf("rollback error = %v", err)
	}
	assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM inbound_publications WHERE provider_event_id = `, failedRequest.ProviderEventID, 0)
	failedEventID, _ := runtimeinbound.DeterministicEventID(failedRequest.PublicationID, 0)
	assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM events WHERE event_id = `, failedEventID, 0)

	assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM inbound_publications WHERE provider_event_id = `, request.ProviderEventID, 1)
	assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM inbound_publication_events WHERE publication_id = `, request.PublicationID, 2)
	assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM events WHERE run_id = `, standing.RunID, 3)
	assertInboundEvidenceProducedByPlatform(t, db, sqlite, request.MarkerEventID)

	runInboundPublicationRawOnlyProof(t, ctx, db, sqlite, store, candidate, standing.RunID, sequence)
	runInboundPublicationOrdinalRollbackProof(t, ctx, db, sqlite, store, candidate, standing.RunID, sequence)
	runInboundPublicationConcurrentRetryProof(t, ctx, db, sqlite, store, candidate, standing.RunID, sequence)
	runInboundPublicationStandingGenerationRebindProof(t, db, sqlite, store, workflowStore)
	runInboundPublicationCorruptionProof(t, ctx, db, sqlite, store, candidate, standing.RunID, sequence)
}

func assertInboundEvidenceProducedByPlatform(t *testing.T, db *sql.DB, sqlite bool, eventID string) {
	t.Helper()
	query := `SELECT COALESCE(produced_by_type, '') FROM events WHERE event_id = $1::uuid`
	if sqlite {
		query = `SELECT COALESCE(produced_by_type, '') FROM events WHERE event_id = ?`
	}
	var producedByType string
	if err := db.QueryRow(query, eventID).Scan(&producedByType); err != nil {
		t.Fatalf("load inbound evidence producer classification: %v", err)
	}
	if producedByType != "platform" {
		t.Fatalf("inbound evidence produced_by_type = %q, want platform", producedByType)
	}
}

func runInboundPublicationStandingGenerationRebindProof(t *testing.T, db *sql.DB, sqlite bool, store inboundPublicationProofStore, workflowStore *runtimepipeline.WorkflowInstanceStore) {
	t.Helper()
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID:  runtimeflowidentity.StandingServiceID("publication-reset-proof", "ingress"),
		PackageKey: "publication-reset-proof",
		FlowID:     "ingress",
		InstanceID: uuid.NewString(),
		EntityID:   uuid.NewString(),
		Source: runtimecorrelation.BundleSourceFact{
			BundleHash:   "bundle-v1:sha256:" + strings.Repeat("9", 64),
			BundleSource: "persisted",
		},
	}
	ctx := testAuthorActivityContextForBundle(candidate.Source.BundleHash)
	registrar, ok := store.(testAuthorActivityCatalogRegistrar)
	if !ok {
		t.Fatalf("inbound publication proof store %T cannot register reset author activity catalog", store)
	}
	registerTestAuthorActivityCatalogForContext(t, registrar, ctx)
	first, err := workflowStore.ReconcileStandingService(ctx, candidate)
	if err != nil {
		t.Fatalf("reconcile reset proof standing service: %v", err)
	}
	firstSequence, err := workflowStore.PublishStandingService(ctx, candidate.ServiceID, first.RunID, first.Generation)
	if err != nil {
		t.Fatalf("publish reset proof standing service: %v", err)
	}
	const childPath = "child/stable"
	child := runtimepipeline.WorkflowInstance{
		InstanceID:      "stable",
		StorageRef:      childPath,
		WorkflowName:    "child",
		WorkflowVersion: "v1",
		CurrentState:    "created",
		EnteredStageAt:  time.Now().UTC(),
		Metadata:        map[string]any{"flow_path": childPath},
	}
	if err := workflowStore.Create(runtimecorrelation.WithRunID(ctx, first.RunID), child); err != nil {
		t.Fatalf("seed first-generation child: %v", err)
	}

	firstRequest := inboundPublicationProofRequest(t, candidate, first.RunID, firstSequence, "delivery-same-generation-rebind")
	firstCtx := runtimecorrelation.WithRunID(ctx, first.RunID)
	if _, err := store.RunInboundPublicationMutation(firstCtx, firstRequest, func(mutation runtimeinbound.Mutation) error {
		return workflowStore.Create(mutation.Context(), child)
	}); err == nil || !strings.Contains(err.Error(), "flow_instance_already_exists") {
		t.Fatalf("same-generation rebind error = %v", err)
	}
	assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM inbound_publications WHERE provider_event_id = `, firstRequest.ProviderEventID, 0)

	reset, err := workflowStore.ResetStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: candidate.ServiceID, Actor: "test"})
	if err != nil {
		t.Fatalf("reset standing service: %v", err)
	}
	resetSequence, err := workflowStore.PublishStandingService(ctx, candidate.ServiceID, reset.RunID, reset.Generation)
	if err != nil {
		t.Fatalf("publish reset standing generation: %v", err)
	}
	resetRequest := inboundPublicationProofRequest(t, candidate, reset.RunID, resetSequence, "delivery-reset-generation-rebind")
	publications, evidence := inboundPublicationProofEvents(t, resetRequest)
	resetCtx := runtimecorrelation.WithRunID(ctx, reset.RunID)
	if _, err := store.RunInboundPublicationMutation(resetCtx, resetRequest, func(mutation runtimeinbound.Mutation) error {
		if err := workflowStore.Create(mutation.Context(), child); err != nil {
			return err
		}
		for index := range publications {
			if err := commitInboundPublicationTestEvent(t, store, mutation, &publications[index]); err != nil {
				return err
			}
		}
		return mutation.FinalizeInboundPublication(mutation.Context(), runtimeinbound.Finalization{EvidenceEvent: evidence, Events: publications})
	}); err != nil {
		t.Fatalf("reset-generation inbound rebind: %v", err)
	}
	assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM entity_state WHERE run_id = `, reset.RunID, 1)
}

func runInboundPublicationRawOnlyProof(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, store inboundPublicationProofStore, candidate runtimepipeline.StandingServiceCandidate, runID string, sequence int64) {
	t.Helper()
	request := inboundPublicationProofRequest(t, candidate, runID, sequence, "delivery-raw-only")
	before := inboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM events WHERE run_id = `, runID)
	record := commitInboundPublicationProof(t, ctx, store, request, 1)
	if !record.Created || record.OutputCount != 1 || len(record.Events) != 1 || record.Events[0].Kind != runtimeprovideroutput.KindRaw {
		t.Fatalf("raw-only record = %#v", record)
	}
	duplicate, err := store.RunInboundPublicationMutation(ctx, request, func(runtimeinbound.Mutation) error {
		return errors.New("raw-only exact retry invoked callback")
	})
	if err != nil {
		t.Fatalf("raw-only exact retry: %v", err)
	}
	if duplicate.Created || duplicate.OutputCount != 1 || len(duplicate.Events) != 1 || duplicate.Events[0].EventID != record.Events[0].EventID {
		t.Fatalf("raw-only duplicate = %#v", duplicate)
	}
	after := inboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM events WHERE run_id = `, runID)
	if after-before != 2 {
		t.Fatalf("raw-only event delta = %d, want one executable plus one evidence", after-before)
	}
}

func runInboundPublicationOrdinalRollbackProof(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, store inboundPublicationProofStore, candidate runtimepipeline.StandingServiceCandidate, runID string, sequence int64) {
	t.Helper()
	for stage := 0; stage < 4; stage++ {
		providerEventID := fmt.Sprintf("delivery-rollback-%d", stage)
		request := inboundPublicationProofRequest(t, candidate, runID, sequence, providerEventID)
		publications, evidence := inboundPublicationProofEvents(t, request)
		injected := fmt.Errorf("injected rollback stage %d", stage)
		_, err := store.RunInboundPublicationMutation(ctx, request, func(mutation runtimeinbound.Mutation) error {
			appendCount := stage
			if appendCount > len(publications) {
				appendCount = len(publications)
			}
			for index := 0; index < appendCount; index++ {
				if err := commitInboundPublicationTestEvent(t, store, mutation, &publications[index]); err != nil {
					return err
				}
			}
			if stage == 3 {
				if err := mutation.FinalizeInboundPublication(mutation.Context(), runtimeinbound.Finalization{EvidenceEvent: evidence, Events: publications}); err != nil {
					return err
				}
			}
			return injected
		})
		if !errors.Is(err, injected) {
			t.Fatalf("rollback stage %d error = %v", stage, err)
		}
		assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM inbound_publications WHERE provider_event_id = `, providerEventID, 0)
		assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM inbound_publication_events WHERE publication_id = `, request.PublicationID, 0)
		for _, publication := range publications {
			assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM events WHERE event_id = `, publication.Event.ID(), 0)
		}
		assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM events WHERE event_id = `, request.MarkerEventID, 0)
	}

	request := inboundPublicationProofRequest(t, candidate, runID, sequence, "delivery-invalid-evidence")
	publications, evidence := inboundPublicationProofEvents(t, request)
	evidence = eventtest.DiagnosticDirect(
		request.MarkerEventID, events.EventTypePlatformInboundRecord, "runtime", "", []byte(`{}`), 0,
		request.ResolvedRunID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, request.EntityID), request.OriginalReceivedAt,
	)
	if _, err := store.RunInboundPublicationMutation(ctx, request, func(mutation runtimeinbound.Mutation) error {
		for index := range publications {
			if err := commitInboundPublicationTestEvent(t, store, mutation, &publications[index]); err != nil {
				return err
			}
		}
		return mutation.FinalizeInboundPublication(mutation.Context(), runtimeinbound.Finalization{EvidenceEvent: evidence, Events: publications})
	}); err == nil || !strings.Contains(err.Error(), "evidence payload") {
		t.Fatalf("invalid evidence error = %v", err)
	}
	assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM inbound_publications WHERE provider_event_id = `, request.ProviderEventID, 0)
}

func runInboundPublicationConcurrentRetryProof(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, store inboundPublicationProofStore, candidate runtimepipeline.StandingServiceCandidate, runID string, sequence int64) {
	t.Helper()
	request := inboundPublicationProofRequest(t, candidate, runID, sequence, "delivery-concurrent")
	publications, evidence := inboundPublicationProofEvents(t, request)
	var callbackCalls atomic.Int32
	start := make(chan struct{})
	results := make(chan struct {
		record runtimeinbound.Record
		err    error
	}, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			record, err := store.RunInboundPublicationMutation(ctx, request, func(mutation runtimeinbound.Mutation) error {
				callbackCalls.Add(1)
				for index := range publications {
					if err := commitInboundPublicationTestEvent(t, store, mutation, &publications[index]); err != nil {
						return err
					}
				}
				return mutation.FinalizeInboundPublication(mutation.Context(), runtimeinbound.Finalization{EvidenceEvent: evidence, Events: publications})
			})
			results <- struct {
				record runtimeinbound.Record
				err    error
			}{record: record, err: err}
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	created := 0
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent publication: %v", result.err)
		}
		if result.record.Created {
			created++
		}
		if len(result.record.Events) != 2 {
			t.Fatalf("concurrent record = %#v", result.record)
		}
	}
	if created != 1 || callbackCalls.Load() != 1 {
		t.Fatalf("concurrent created=%d callback_calls=%d, want one owner", created, callbackCalls.Load())
	}
	assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM inbound_publications WHERE provider_event_id = `, request.ProviderEventID, 1)
	assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM inbound_publication_events WHERE publication_id = `, request.PublicationID, 2)
}

func commitInboundPublicationProof(t *testing.T, ctx context.Context, store inboundPublicationProofStore, request runtimeinbound.Request, outputCount int) runtimeinbound.Record {
	t.Helper()
	publications, evidence := inboundPublicationProofEventsCount(t, request, outputCount)
	record, err := store.RunInboundPublicationMutation(ctx, request, func(mutation runtimeinbound.Mutation) error {
		for index := range publications {
			if err := commitInboundPublicationTestEvent(t, store, mutation, &publications[index]); err != nil {
				return err
			}
		}
		return mutation.FinalizeInboundPublication(mutation.Context(), runtimeinbound.Finalization{EvidenceEvent: evidence, Events: publications})
	})
	if err != nil {
		t.Fatalf("commit inbound publication %s: %v", request.ProviderEventID, err)
	}
	return record
}

func runInboundPublicationCorruptionProof(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, store inboundPublicationProofStore, candidate runtimepipeline.StandingServiceCandidate, runID string, sequence int64) {
	t.Helper()
	corruptions := []struct {
		name   string
		mutate func(*testing.T, runtimeinbound.Request)
	}{
		{name: "parent output count mismatch", mutate: func(t *testing.T, request runtimeinbound.Request) {
			execInboundPublicationProofSQL(t, db, sqlite,
				`UPDATE inbound_publications SET output_count = 1 WHERE publication_id = $1::uuid`,
				`UPDATE inbound_publications SET output_count = 1 WHERE publication_id = ?`, request.PublicationID)
		}},
		{name: "missing child", mutate: func(t *testing.T, request runtimeinbound.Request) {
			execInboundPublicationProofSQL(t, db, sqlite,
				`DELETE FROM inbound_publication_events WHERE publication_id = $1::uuid AND ordinal = 1`,
				`DELETE FROM inbound_publication_events WHERE publication_id = ? AND ordinal = 1`, request.PublicationID)
		}},
		{name: "noncontiguous ordinal", mutate: func(t *testing.T, request runtimeinbound.Request) {
			execInboundPublicationProofSQL(t, db, sqlite,
				`UPDATE inbound_publication_events SET ordinal = 2 WHERE publication_id = $1::uuid AND ordinal = 1`,
				`UPDATE inbound_publication_events SET ordinal = 2 WHERE publication_id = ? AND ordinal = 1`, request.PublicationID)
		}},
		{name: "event name mismatch", mutate: func(t *testing.T, request runtimeinbound.Request) {
			execInboundPublicationProofSQL(t, db, sqlite,
				`UPDATE inbound_publication_events SET event_name = 'inbound.corrupt' WHERE publication_id = $1::uuid AND ordinal = 1`,
				`UPDATE inbound_publication_events SET event_name = 'inbound.corrupt' WHERE publication_id = ? AND ordinal = 1`, request.PublicationID)
		}},
		{name: "event integrity mismatch", mutate: func(t *testing.T, request runtimeinbound.Request) {
			execInboundPublicationProofSQL(t, db, sqlite,
				`UPDATE inbound_publication_events SET event_integrity_fingerprint = $2 WHERE publication_id = $1::uuid AND ordinal = 1`,
				`UPDATE inbound_publication_events SET event_integrity_fingerprint = ? WHERE publication_id = ? AND ordinal = 1`,
				inboundProofArgs(sqlite, request.PublicationID, strings.Repeat("b", 64))...)
		}},
		{name: "recipient manifest mismatch", mutate: func(t *testing.T, request runtimeinbound.Request) {
			execInboundPublicationProofSQL(t, db, sqlite,
				`UPDATE inbound_publication_events SET recipient_manifest_fingerprint = $2 WHERE publication_id = $1::uuid AND ordinal = 1`,
				`UPDATE inbound_publication_events SET recipient_manifest_fingerprint = ? WHERE publication_id = ? AND ordinal = 1`,
				inboundProofArgs(sqlite, request.PublicationID, strings.Repeat("c", 64))...)
		}},
		{name: "normalized provenance missing", mutate: func(t *testing.T, request runtimeinbound.Request) {
			execInboundPublicationProofSQL(t, db, sqlite,
				`UPDATE inbound_publication_events SET pack_id = '' WHERE publication_id = $1::uuid AND ordinal = 1`,
				`UPDATE inbound_publication_events SET pack_id = '' WHERE publication_id = ? AND ordinal = 1`, request.PublicationID)
		}},
		{name: "evidence payload mismatch", mutate: func(t *testing.T, request runtimeinbound.Request) {
			execInboundPublicationProofSQL(t, db, sqlite,
				`UPDATE events SET payload = '{}'::jsonb WHERE event_id = $1::uuid`,
				`UPDATE events SET payload = '{}' WHERE event_id = ?`, request.MarkerEventID)
		}},
		{name: "replay scope missing", mutate: func(t *testing.T, request runtimeinbound.Request) {
			rawID, err := runtimeinbound.DeterministicEventID(request.PublicationID, 0)
			if err != nil {
				t.Fatal(err)
			}
			execInboundPublicationProofSQL(t, db, sqlite,
				`DELETE FROM committed_replay_scopes WHERE event_id = $1::uuid`,
				`DELETE FROM committed_replay_scopes WHERE event_id = ?`,
				rawID)
		}},
		{name: "durable prepared parent", mutate: func(t *testing.T, request runtimeinbound.Request) {
			execInboundPublicationProofSQL(t, db, sqlite,
				`UPDATE inbound_publications SET state = 'prepared', marker_event_id = NULL, output_count = 0, committed_at = NULL WHERE publication_id = $1::uuid`,
				`UPDATE inbound_publications SET state = 'prepared', marker_event_id = NULL, output_count = 0, committed_at = NULL WHERE publication_id = ?`, request.PublicationID)
		}},
	}

	for index, corruption := range corruptions {
		t.Run(corruption.name, func(t *testing.T) {
			request := inboundPublicationProofRequest(t, candidate, runID, sequence, fmt.Sprintf("delivery-corrupt-%d", index))
			commitInboundPublicationProof(t, ctx, store, request, 2)
			corruption.mutate(t, request)
			if _, _, err := store.LoadInboundPublicationByIdentity(context.Background(), request.Provider, request.EntityID, request.ProviderEventID); err == nil {
				t.Fatal("LoadInboundPublicationByIdentity error = nil, want corruption refusal")
			}
		})
	}

	t.Run("duplicate ordinal rejected by schema", func(t *testing.T) {
		request := inboundPublicationProofRequest(t, candidate, runID, sequence, "delivery-duplicate-ordinal")
		commitInboundPublicationProof(t, ctx, store, request, 2)
		if _, err := db.Exec(inboundPublicationProofQuery(sqlite,
			`INSERT INTO inbound_publication_events (publication_id, ordinal, event_id, event_name, output_kind, event_integrity_fingerprint, recipient_manifest_fingerprint, recipient_count) VALUES ($1::uuid, 0, $2::uuid, 'platform.inbound_recorded', 'raw', $3, $4, 0)`,
			`INSERT INTO inbound_publication_events (publication_id, ordinal, event_id, event_name, output_kind, event_integrity_fingerprint, recipient_manifest_fingerprint, recipient_count) VALUES (?, 0, ?, 'platform.inbound_recorded', 'raw', ?, ?, 0)`),
			request.PublicationID, request.MarkerEventID, strings.Repeat("d", 64), strings.Repeat("e", 64)); err == nil {
			t.Fatal("duplicate ordinal insert error = nil")
		}
	})

	t.Run("extra child rejected on read", func(t *testing.T) {
		request := inboundPublicationProofRequest(t, candidate, runID, sequence, "delivery-extra-child")
		commitInboundPublicationProof(t, ctx, store, request, 2)
		_, recipientFingerprint, _, err := runtimeinbound.CanonicalRecipientManifest(nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(inboundPublicationProofQuery(sqlite,
			`INSERT INTO inbound_publication_events (publication_id, ordinal, event_id, event_name, output_kind, event_integrity_fingerprint, recipient_manifest_fingerprint, recipient_count) VALUES ($1::uuid, 2, $2::uuid, 'platform.inbound_recorded', 'raw', $3, $4, 0)`,
			`INSERT INTO inbound_publication_events (publication_id, ordinal, event_id, event_name, output_kind, event_integrity_fingerprint, recipient_manifest_fingerprint, recipient_count) VALUES (?, 2, ?, 'platform.inbound_recorded', 'raw', ?, ?, 0)`),
			request.PublicationID, request.MarkerEventID, strings.Repeat("d", 64), recipientFingerprint); err != nil {
			t.Fatalf("insert extra child corruption: %v", err)
		}
		if _, _, err := store.LoadInboundPublicationByIdentity(context.Background(), request.Provider, request.EntityID, request.ProviderEventID); err == nil {
			t.Fatal("LoadInboundPublicationByIdentity error = nil, want extra-child refusal")
		}
	})
}

func execInboundPublicationProofSQL(t *testing.T, db *sql.DB, sqlite bool, postgresQuery, sqliteQuery string, args ...any) {
	t.Helper()
	if _, err := db.Exec(inboundPublicationProofQuery(sqlite, postgresQuery, sqliteQuery), args...); err != nil {
		t.Fatalf("corrupt inbound publication fixture: %v", err)
	}
}

func inboundPublicationProofQuery(sqlite bool, postgresQuery, sqliteQuery string) string {
	if sqlite {
		return sqliteQuery
	}
	return postgresQuery
}

func inboundProofArgs(sqlite bool, publicationID, value string) []any {
	if sqlite {
		return []any{value, publicationID}
	}
	return []any{publicationID, value}
}

func inboundPublicationProofRequest(t *testing.T, candidate runtimepipeline.StandingServiceCandidate, runID string, sequence int64, providerEventID string) runtimeinbound.Request {
	t.Helper()
	publicationID, markerEventID := runtimeinbound.DeterministicIDs("github", candidate.EntityID, providerEventID)
	fingerprint, err := runtimeinbound.SemanticFingerprint(map[string]any{"provider": "github", "provider_event_id": providerEventID, "payload": map[string]any{"value": 1}})
	if err != nil {
		t.Fatal(err)
	}
	return runtimeinbound.Request{
		PublicationID: publicationID, Provider: "github", EntityID: candidate.EntityID, ProviderEventID: providerEventID,
		RequestFingerprint: fingerprint, RequestProjectionVersion: runtimeinbound.RequestSemanticProjectionVersion,
		StableServiceID: candidate.ServiceID, PackageKey: candidate.PackageKey, FlowID: candidate.FlowID,
		InstanceID: candidate.InstanceID, TargetAlias: "github", TargetFlowInstance: candidate.FlowID + "/" + candidate.InstanceID,
		ExpectedPublicationSequence: sequence, ResolvedRunID: runID, MarkerEventID: markerEventID,
		AcknowledgementMode: runtimeinbound.AcknowledgementDurableBeforeDispatch,
		OriginalReceivedAt:  time.Now().UTC().Truncate(time.Microsecond), OriginalUserAgent: "proof", OriginalTransportMetadata: []byte(`{"method":"POST"}`),
	}
}

func inboundPublicationProofEvents(t *testing.T, request runtimeinbound.Request) ([]runtimeinbound.EventFinalization, events.Event) {
	return inboundPublicationProofEventsCount(t, request, 2)
}

func inboundPublicationProofEventsCount(t *testing.T, request runtimeinbound.Request, outputCount int) ([]runtimeinbound.EventFinalization, events.Event) {
	t.Helper()
	if outputCount < 1 || outputCount > 2 {
		t.Fatalf("unsupported inbound publication proof output count %d", outputCount)
	}
	envelope := events.EnvelopeForTargetRoute(events.EventEnvelope{}, events.RouteIdentity{EntityID: request.EntityID, FlowInstance: request.TargetFlowInstance})
	rawID, err := runtimeinbound.DeterministicEventID(request.PublicationID, 0)
	if err != nil {
		t.Fatal(err)
	}
	normalizedID, err := runtimeinbound.DeterministicEventID(request.PublicationID, 1)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"value":{"z":2,"a":1},"provider":"github"}`)
	raw := eventtest.ExistingRunRootIngress(rawID, "inbound.github.push", "inbound-gateway", "", payload, 0, request.ResolvedRunID, envelope, request.OriginalReceivedAt)
	normalized := eventtest.ExistingRunRootIngress(normalizedID, "github.push.normalized", "inbound-gateway", "", payload, 0, request.ResolvedRunID, events.EventEnvelope{}, request.OriginalReceivedAt)
	authorization := runtimeprovideroutput.Authorization{
		Provider: request.Provider, Event: string(normalized.Type()), PackID: "provider.github", PackVersion: "1.0.0",
		ManifestHash: "sha256:" + strings.Repeat("a", 64), GenerationID: "proof-generation",
	}
	publications := []runtimeinbound.EventFinalization{
		{Ordinal: 0, Event: raw, Kind: runtimeprovideroutput.KindRaw, RecipientManifest: []byte(`[]`)},
		{Ordinal: 1, Event: normalized, Kind: runtimeprovideroutput.KindNormalized, Authorization: authorization, RecipientManifest: []byte(`[]`)},
	}[:outputCount]
	eventIDs := make([]string, len(publications))
	eventNames := make([]string, len(publications))
	for index := range publications {
		eventIDs[index] = publications[index].Event.ID()
		eventNames[index] = string(publications[index].Event.Type())
	}
	evidencePayload, err := runtimeinbound.BuildEvidencePayload(request, eventIDs, eventNames)
	if err != nil {
		t.Fatalf("BuildEvidencePayload: %v", err)
	}
	evidence := eventtest.DiagnosticDirect(
		request.MarkerEventID, events.EventTypePlatformInboundRecord, "runtime", "", evidencePayload, 0,
		request.ResolvedRunID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, request.EntityID), request.OriginalReceivedAt,
	)
	return publications, evidence
}

func assertInboundPublicationProofCount(t *testing.T, db *sql.DB, sqlite bool, prefix, value string, want int) {
	t.Helper()
	got := inboundPublicationProofCount(t, db, sqlite, prefix, value)
	if got != want {
		t.Fatalf("count for %q = %d, want %d", value, got, want)
	}
}

func inboundPublicationProofCount(t *testing.T, db *sql.DB, sqlite bool, prefix, value string) int {
	t.Helper()
	placeholder := "$1"
	if sqlite {
		placeholder = "?"
	}
	var got int
	if err := db.QueryRow(prefix+placeholder, value).Scan(&got); err != nil {
		t.Fatal(err)
	}
	return got
}
