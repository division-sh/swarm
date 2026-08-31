package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeprovideroutput "github.com/division-sh/swarm/internal/runtime/core/provideroutput"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeinbound "github.com/division-sh/swarm/internal/runtime/inboundpublication"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type inboundPublicationProofStore interface {
	runtimeinbound.Runner
	storeTestDurableEventBusStore
	workflowTestSelectedStore
}

type inboundPublicationProofAuthorizationVerifier struct{}

func (inboundPublicationProofAuthorizationVerifier) VerifyProviderOutputAuthorization(actual runtimeprovideroutput.Authorization) error {
	if !actual.Valid() {
		return errors.New("inbound publication proof authorization is incomplete")
	}
	return nil
}

type inboundPublicationProofMutation interface {
	Context() context.Context
	FinalizeInboundPublication(context.Context, runtimeinbound.Finalization) error
}

type inboundPublicationProofBuilder struct {
	ctx          context.Context
	finalization runtimeinbound.Finalization
	finalized    bool
}

func (b *inboundPublicationProofBuilder) Context() context.Context { return b.ctx }

func (b *inboundPublicationProofBuilder) FinalizeInboundPublication(_ context.Context, finalization runtimeinbound.Finalization) error {
	if b.finalized {
		return errors.New("inbound publication proof was already finalized")
	}
	b.finalization = finalization
	b.finalized = true
	return nil
}

func runInboundPublicationProofMutation(t *testing.T, store inboundPublicationProofStore, ctx context.Context, request runtimeinbound.Request, fn func(inboundPublicationProofMutation) error) (runtimeinbound.Record, error) {
	t.Helper()
	if existing, found, err := store.LoadInboundPublicationByIdentity(ctx, request.Provider, request.EntityID, request.ProviderEventID); err != nil {
		return runtimeinbound.Record{}, err
	} else if found {
		if existing.RequestFingerprint != request.RequestFingerprint || existing.RequestProjectionVersion != request.RequestProjectionVersion {
			return runtimeinbound.Record{}, runtimeinbound.ErrRequestIdentityConflict
		}
		existing.Created = false
		return existing, nil
	}
	builder := &inboundPublicationProofBuilder{ctx: ctx}
	if err := fn(builder); err != nil {
		return runtimeinbound.Record{}, err
	}
	if !builder.finalized {
		return runtimeinbound.Record{}, errors.New("inbound publication proof did not finalize")
	}
	sourceFact, ok := runtimecorrelation.BundleSourceFactFromContext(ctx)
	if !ok {
		return runtimeinbound.Record{}, errors.New("inbound publication proof bundle source fact is required")
	}
	eventBus, err := newStoreTestEventBus(t, store, runtimebus.EventBusOptions{
		BundleSourceFact: sourceFact, ProviderOutputVerifier: inboundPublicationProofAuthorizationVerifier{},
	})
	if err != nil {
		return runtimeinbound.Record{}, err
	}
	batch := runtimebus.InboundDeliveryBatch{Provider: request.Provider, Events: make([]runtimebus.InboundDeliveryEvent, len(builder.finalization.Events))}
	for index, item := range builder.finalization.Events {
		batch.Events[index] = runtimebus.InboundDeliveryEvent{Event: item.Event, Kind: item.Kind, Authorization: item.Authorization}
	}
	plan, err := eventBus.PrepareInboundDeliveryBatch(ctx, batch)
	if err != nil {
		return runtimeinbound.Record{}, err
	}
	defer func() { _ = eventBus.AbandonInboundDeliveryPlan(context.WithoutCancel(ctx), plan) }()
	prepared := plan.PreparedPublications()
	for index := range prepared {
		manifest, _, _, err := runtimeinbound.CanonicalRecipientManifest(prepared[index].DeliveryRoutes())
		if err != nil {
			return runtimeinbound.Record{}, err
		}
		builder.finalization.Events[index].Event = prepared[index].Event
		builder.finalization.Events[index].RecipientManifest = manifest
	}
	projection, _ := runtimeauthoractivity.InboundProjectionFromContext(ctx)
	result, err := store.CommitInboundPublication(ctx, runtimeinbound.CommitCommand{
		Request: request, Finalization: builder.finalization,
		Publications: plan.CommitCommands(), AuthorProjection: projection,
	})
	if err == nil {
		for index, publication := range result.Publications {
			if validationErr := publication.Validate(); validationErr != nil {
				return runtimeinbound.Record{}, fmt.Errorf("validate committed publication %d: %w", index, validationErr)
			}
		}
	}
	return result.Record, err
}

func commitInboundPublicationTestEvent(t *testing.T, _ storeTestDurableEventBusStore, _ inboundPublicationProofMutation, publication *runtimeinbound.EventFinalization) error {
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
	publication.Event = admitted.Event()
	return nil
}

func TestInboundEvidencePersistsTypedNoSubscriberByDesign(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		store.SetEventPayloadValidator(currentPlatformPayloadValidatorForStoreTest(t))
		workflowStore := newSQLiteWorkflowTestCoordinator(t, store.backend.ConstructionHandle(), store)
		runInboundPublicationOperationProof(t, store.backend.ConstructionHandle(), true, store, workflowStore)
	})
	t.Run("postgres", func(t *testing.T) {
		_, db, cleanup := testutil.StartPostgres(t)
		t.Cleanup(cleanup)
		store := admitTestPostgresStore(t, db)
		store.SetEventPayloadValidator(currentPlatformPayloadValidatorForStoreTest(t))
		workflowStore := newPostgresWorkflowTestCoordinator(t, db, store)
		runInboundPublicationOperationProof(t, db, false, store, workflowStore)
	})
}

func runInboundPublicationOperationProof(t *testing.T, db *sql.DB, sqlite bool, store inboundPublicationProofStore, workflowStore *runtimepipeline.PipelineCoordinator) {
	t.Helper()
	packageKey := "publication-proof"
	flowID := "ingress"
	serviceID := runtimeflowidentity.StandingServiceID(packageKey, flowID)
	instanceID := uuid.NewString()
	entityID := uuid.NewString()
	candidate := runtimepipeline.StandingServiceCandidate{
		ServiceID: serviceID, PackageKey: packageKey, FlowID: flowID, InstanceID: instanceID, EntityID: entityID,
		Source: mustStoreTestPersistedBundleSourceFact("bundle-v1:sha256:" + strings.Repeat("8", 64)),
	}
	ctx := runtimecorrelation.WithBundleSourceFact(
		testAuthorActivityContextForBundle(candidate.Source.BundleHash()),
		candidate.Source,
	)
	seedStoreTestPersistedBundle(t, db, candidate.Source.BundleHash())
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
	request := inboundPublicationProofRequest(t, candidate, standing.RunID, standing.Generation, sequence, "delivery-1")
	callbackCalls := 0
	var publications []runtimeinbound.EventFinalization
	record, err := runInboundPublicationProofMutation(t, store, ctx, request, func(mutation inboundPublicationProofMutation) error {
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
	duplicate, err := runInboundPublicationProofMutation(t, store, ctx, request, func(inboundPublicationProofMutation) error {
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
	if _, err := runInboundPublicationProofMutation(t, store, ctx, conflict, func(inboundPublicationProofMutation) error { return nil }); !errors.Is(err, runtimeinbound.ErrRequestIdentityConflict) {
		t.Fatalf("conflicting retry error = %v", err)
	}

	failedRequest := inboundPublicationProofRequest(t, candidate, standing.RunID, standing.Generation, sequence, "delivery-failure")
	failedPublications, _ := inboundPublicationProofEvents(t, failedRequest)
	injected := errors.New("injected publication failure")
	if _, err := runInboundPublicationProofMutation(t, store, ctx, failedRequest, func(mutation inboundPublicationProofMutation) error {
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
	assertInboundEvidenceRouteSettlement(t, ctx, db, sqlite, request.MarkerEventID)

	runInboundPublicationRawOnlyProof(t, ctx, db, sqlite, store, candidate, standing.RunID, standing.Generation, sequence)
	runInboundPublicationOrdinalRollbackProof(t, ctx, db, sqlite, store, candidate, standing.RunID, standing.Generation, sequence)
	runInboundPublicationCorruptionProof(t, ctx, db, sqlite, store, candidate, standing.RunID, standing.Generation, sequence)
	runInboundPublicationOperatorChannelClaimProof(t, ctx, db, sqlite, store, candidate, standing.RunID, standing.Generation, sequence)
	if _, err := workflowStore.SuspendStandingService(ctx, runtimepipeline.StandingServiceOperation{ServiceID: serviceID, Actor: "inbound-proof"}); err != nil {
		t.Fatalf("suspend inbound standing service: %v", err)
	}
	blocked := inboundPublicationProofRequest(t, candidate, standing.RunID, standing.Generation, sequence, "delivery-suspended")
	if _, err := runInboundPublicationProofMutation(t, store, ctx, blocked, func(mutation inboundPublicationProofMutation) error {
		publications, evidence := inboundPublicationProofEvents(t, blocked)
		for index := range publications {
			if err := commitInboundPublicationTestEvent(t, store, mutation, &publications[index]); err != nil {
				return err
			}
		}
		return mutation.FinalizeInboundPublication(mutation.Context(), runtimeinbound.Finalization{EvidenceEvent: evidence, Events: publications})
	}); err == nil || !strings.Contains(err.Error(), "suspended") {
		t.Fatalf("suspended standing inbound error = %v, want typed suspended disposition", err)
	}
	assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM inbound_publications WHERE provider_event_id = `, blocked.ProviderEventID, 0)
}

func runInboundPublicationOperatorChannelClaimProof(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, store inboundPublicationProofStore, candidate runtimepipeline.StandingServiceCandidate, runID string, generation, sequence int64) {
	t.Helper()
	channelStore, ok := any(store).(operatorchannel.Store)
	if !ok {
		t.Fatalf("inbound publication store %T lacks operator-channel owner", store)
	}
	now := time.Date(2026, 8, 24, 16, 0, 0, 0, time.UTC)
	principal, err := channelStore.EnsureOperatorPrincipal(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	identity := operatorChannelContractIdentity("inbound-atomic-generation")
	begin := func(key string) operatorchannel.Operation {
		op, err := channelStore.BeginChannelBinding(ctx, operatorchannel.BeginRequest{ProviderCredential: operatorChannelProviderEvidence(),
			OperationID: uuid.NewString(), Kind: operatorchannel.OperationConnect, PrincipalID: principal.ID,
			Interface: identity, ExpectedRevision: 0, RequestKeyHash: key, RequestHash: key + "-body",
			RequestedAt: now, ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL),
		})
		if err != nil {
			t.Fatal(err)
		}
		return op
	}
	command := func(op operatorchannel.Operation, providerEventID string) runtimeinbound.CommitCommand {
		request := inboundPublicationProofRequest(t, candidate, runID, generation, sequence, providerEventID)
		request.OriginalReceivedAt = now.Add(time.Minute)
		claim := operatorChannelContractClaim(op, operatorchannel.ConversationScopeShared, "account-atomic", "conversation-atomic", request.PublicationID)
		claim.Provider = request.Provider
		claim.ProviderEventID = request.ProviderEventID
		claim.PublicationID = request.PublicationID
		projection, _ := runtimeauthoractivity.InboundProjectionFromContext(ctx)
		return runtimeinbound.CommitCommand{
			Request: request, Finalization: runtimeinbound.Finalization{EvidenceEvent: inboundPublicationZeroOutputEvidence(t, request)},
			AuthorProjection: projection, OperatorChannelClaim: &claim,
		}
	}
	for _, fault := range []struct {
		name      string
		table     string
		operation string
	}{
		{name: "operation update", table: "operator_channel_operations", operation: "UPDATE"},
		{name: "claim receipt insert", table: "operator_channel_claim_receipts", operation: "INSERT"},
	} {
		t.Run("rollback after "+fault.name, func(t *testing.T) {
			faultOp := begin("atomic-claim-fault-" + strings.ReplaceAll(fault.name, " ", "-"))
			faultCommand := command(faultOp, "operator-channel-fault-"+strings.ReplaceAll(fault.name, " ", "-"))
			drop := installOperatorChannelClaimFailureTrigger(t, db, sqlite, fault.table, fault.operation)
			if _, err := store.CommitInboundPublication(ctx, faultCommand); err == nil {
				drop()
				t.Fatalf("%s fault unexpectedly committed", fault.name)
			}
			drop()
			assertOperatorChannelOperationAwaitingClaim(t, ctx, channelStore, principal.ID, faultOp.OperationID)
			result, err := store.CommitInboundPublication(ctx, faultCommand)
			if err != nil || !result.Record.Created || result.OperatorChannelClaim == nil || result.OperatorChannelClaim.Disposition != operatorchannel.DispositionConsumedBinding {
				t.Fatalf("%s recovery result = %#v, %v", fault.name, result, err)
			}
		})
	}

	op := begin("atomic-claim-success")
	accepted := command(op, "operator-channel-claim-success")
	result, err := store.CommitInboundPublication(ctx, accepted)
	if err != nil {
		t.Fatalf("commit zero-event operator claim: %v", err)
	}
	if !result.Record.Created || result.Record.OutputCount != 0 || len(result.Record.Events) != 0 || len(result.Publications) != 0 || result.OperatorChannelClaim == nil || result.OperatorChannelClaim.Disposition != operatorchannel.DispositionConsumedBinding {
		t.Fatalf("zero-event operator claim result = %#v", result)
	}
	assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM inbound_publication_events WHERE publication_id = `, accepted.Request.PublicationID, 0)
	assertInboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM events WHERE event_id = `, accepted.Request.MarkerEventID, 1)

	replayed, err := store.CommitInboundPublication(ctx, accepted)
	if err != nil || replayed.Record.Created || replayed.Record.OutputCount != 0 || replayed.OperatorChannelClaim != nil {
		t.Fatalf("zero-event operator claim replay = %#v, %v", replayed, err)
	}
	changed := accepted
	changed.Request.RequestFingerprint = strings.Repeat("e", 64)
	if _, err := store.CommitInboundPublication(ctx, changed); !errors.Is(err, runtimeinbound.ErrRequestIdentityConflict) {
		t.Fatalf("changed zero-event operator claim retry error = %v", err)
	}

	concurrentOp := begin("atomic-claim-concurrent")
	concurrent := command(concurrentOp, "operator-channel-claim-concurrent")
	results := make(chan runtimeinbound.CommitResult, 2)
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := store.CommitInboundPublication(ctx, concurrent)
			results <- got
			errs <- err
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent zero-event operator claim: %v", err)
		}
	}
	created := 0
	for got := range results {
		if got.Record.Created {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("concurrent zero-event operator claim created results = %d, want 1", created)
	}

	failedOp := begin("atomic-claim-rollback")
	failed := command(failedOp, "operator-channel-claim-rollback")
	failed.Request.MarkerEventID = accepted.Request.MarkerEventID
	failed.Finalization.EvidenceEvent = inboundPublicationZeroOutputEvidence(t, failed.Request)
	if _, err := store.CommitInboundPublication(ctx, failed); err == nil {
		t.Fatal("late duplicate evidence write unexpectedly committed operator claim")
	}
	assertOperatorChannelOperationAwaitingClaim(t, ctx, channelStore, principal.ID, failedOp.OperationID)
}

func assertOperatorChannelOperationAwaitingClaim(t *testing.T, ctx context.Context, channelStore operatorchannel.Store, principalID, operationID string) {
	t.Helper()
	operations, err := channelStore.ListOperatorChannelOperations(ctx, principalID)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range operations {
		if candidate.OperationID == operationID {
			if candidate.State != operatorchannel.StateAwaitingClaim || candidate.Revision != 1 {
				t.Fatalf("failed atomic claim operation = %#v", candidate)
			}
			return
		}
	}
	t.Fatalf("failed atomic claim operation %s not found", operationID)
}

func installOperatorChannelClaimFailureTrigger(t *testing.T, db *sql.DB, sqlite bool, table, operation string) func() {
	t.Helper()
	const trigger = "operator_channel_claim_injected_failure"
	const function = "operator_channel_claim_injected_failure_fn"
	if sqlite {
		if _, err := db.Exec(fmt.Sprintf(`CREATE TRIGGER %s BEFORE %s ON %s BEGIN SELECT RAISE(ABORT, 'injected operator channel claim failure'); END`, trigger, operation, table)); err != nil {
			t.Fatalf("install SQLite operator-channel fault trigger: %v", err)
		}
		return func() {
			if _, err := db.Exec(`DROP TRIGGER ` + trigger); err != nil {
				t.Fatalf("drop SQLite operator-channel fault trigger: %v", err)
			}
		}
	}
	if _, err := db.Exec(fmt.Sprintf(`
CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'injected operator channel claim failure';
END
$$;
CREATE TRIGGER %s BEFORE %s ON %s FOR EACH ROW EXECUTE FUNCTION %s()`, function, trigger, operation, table, function)); err != nil {
		t.Fatalf("install Postgres operator-channel fault trigger: %v", err)
	}
	return func() {
		if _, err := db.Exec(fmt.Sprintf(`DROP TRIGGER %s ON %s; DROP FUNCTION %s()`, trigger, table, function)); err != nil {
			t.Fatalf("drop Postgres operator-channel fault trigger: %v", err)
		}
	}
}

func inboundPublicationZeroOutputEvidence(t *testing.T, request runtimeinbound.Request) events.Event {
	t.Helper()
	payload, err := runtimeinbound.BuildEvidencePayload(request, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return eventtest.DiagnosticDirect(
		request.MarkerEventID, events.EventTypePlatformInboundRecord, "runtime", "", payload, 0,
		request.ResolvedRunID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, request.EntityID), request.OriginalReceivedAt,
	)
}

func assertInboundEvidenceRouteSettlement(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, eventID string) {
	t.Helper()
	query := `SELECT route_settlement FROM events WHERE event_id = $1::uuid`
	if sqlite {
		query = `SELECT route_settlement FROM events WHERE event_id = ?`
	}
	var raw []byte
	if err := db.QueryRowContext(ctx, query, eventID).Scan(&raw); err != nil {
		t.Fatalf("load inbound evidence route settlement: %v", err)
	}
	var settlement events.RouteSettlement
	if err := json.Unmarshal(raw, &settlement); err != nil {
		t.Fatalf("decode inbound evidence route settlement: %v", err)
	}
	if settlement.WriteClass() != events.EventWriteInboundEvidenceDirect || !settlement.NoDelivery() || settlement.Reason() != events.NoDeliveryNoSubscriberByDesign || settlement.Ledger().Present() {
		t.Fatalf("inbound evidence route settlement = %#v", settlement)
	}
	if err := settlement.Validate(nil); err != nil {
		t.Fatalf("validate inbound evidence route settlement: %v", err)
	}
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

func runInboundPublicationRawOnlyProof(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, store inboundPublicationProofStore, candidate runtimepipeline.StandingServiceCandidate, runID string, generation, sequence int64) {
	t.Helper()
	request := inboundPublicationProofRequest(t, candidate, runID, generation, sequence, "delivery-raw-only")
	before := inboundPublicationProofCount(t, db, sqlite, `SELECT COUNT(*) FROM events WHERE run_id = `, runID)
	record := commitInboundPublicationProof(t, ctx, store, request, 1)
	if !record.Created || record.OutputCount != 1 || len(record.Events) != 1 || record.Events[0].Kind != runtimeprovideroutput.KindRaw {
		t.Fatalf("raw-only record = %#v", record)
	}
	duplicate, err := runInboundPublicationProofMutation(t, store, ctx, request, func(inboundPublicationProofMutation) error {
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

func runInboundPublicationOrdinalRollbackProof(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, store inboundPublicationProofStore, candidate runtimepipeline.StandingServiceCandidate, runID string, generation, sequence int64) {
	t.Helper()
	for stage := 0; stage < 4; stage++ {
		providerEventID := fmt.Sprintf("delivery-rollback-%d", stage)
		request := inboundPublicationProofRequest(t, candidate, runID, generation, sequence, providerEventID)
		publications, evidence := inboundPublicationProofEvents(t, request)
		injected := fmt.Errorf("injected rollback stage %d", stage)
		_, err := runInboundPublicationProofMutation(t, store, ctx, request, func(mutation inboundPublicationProofMutation) error {
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

	request := inboundPublicationProofRequest(t, candidate, runID, generation, sequence, "delivery-invalid-evidence")
	publications, evidence := inboundPublicationProofEvents(t, request)
	evidence = eventtest.DiagnosticDirect(
		request.MarkerEventID, events.EventTypePlatformInboundRecord, "runtime", "", []byte(`{}`), 0,
		request.ResolvedRunID, "", events.EnvelopeForEntityID(events.EventEnvelope{}, request.EntityID), request.OriginalReceivedAt,
	)
	if _, err := runInboundPublicationProofMutation(t, store, ctx, request, func(mutation inboundPublicationProofMutation) error {
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

func commitInboundPublicationProof(t *testing.T, ctx context.Context, store inboundPublicationProofStore, request runtimeinbound.Request, outputCount int) runtimeinbound.Record {
	t.Helper()
	publications, evidence := inboundPublicationProofEventsCount(t, request, outputCount)
	record, err := runInboundPublicationProofMutation(t, store, ctx, request, func(mutation inboundPublicationProofMutation) error {
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

func runInboundPublicationCorruptionProof(t *testing.T, ctx context.Context, db *sql.DB, sqlite bool, store inboundPublicationProofStore, candidate runtimepipeline.StandingServiceCandidate, runID string, generation, sequence int64) {
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
		{name: "evidence payload byte mismatch", mutate: func(t *testing.T, request runtimeinbound.Request) {
			execInboundPublicationProofSQL(t, db, sqlite,
				`UPDATE events SET payload_bytes = convert_to(' ' || convert_from(payload_bytes, 'UTF8'), 'UTF8') WHERE event_id = (SELECT event_id FROM inbound_publication_events WHERE publication_id = $1::uuid AND ordinal = 1)`,
				`UPDATE events SET payload_bytes = CAST(' ' || CAST(payload_bytes AS TEXT) AS BLOB) WHERE event_id = (SELECT event_id FROM inbound_publication_events WHERE publication_id = ? AND ordinal = 1)`, request.PublicationID)
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
			request := inboundPublicationProofRequest(t, candidate, runID, generation, sequence, fmt.Sprintf("delivery-corrupt-%d", index))
			commitInboundPublicationProof(t, ctx, store, request, 2)
			corruption.mutate(t, request)
			if _, _, err := store.LoadInboundPublicationByIdentity(context.Background(), request.Provider, request.EntityID, request.ProviderEventID); err == nil {
				t.Fatal("LoadInboundPublicationByIdentity error = nil, want corruption refusal")
			}
		})
	}

	t.Run("duplicate ordinal rejected by schema", func(t *testing.T) {
		request := inboundPublicationProofRequest(t, candidate, runID, generation, sequence, "delivery-duplicate-ordinal")
		commitInboundPublicationProof(t, ctx, store, request, 2)
		if _, err := db.Exec(inboundPublicationProofQuery(sqlite,
			`INSERT INTO inbound_publication_events (publication_id, ordinal, event_id, event_name, output_kind, event_integrity_fingerprint, recipient_manifest_fingerprint, recipient_count) VALUES ($1::uuid, 0, $2::uuid, 'platform.inbound_recorded', 'raw', $3, $4, 0)`,
			`INSERT INTO inbound_publication_events (publication_id, ordinal, event_id, event_name, output_kind, event_integrity_fingerprint, recipient_manifest_fingerprint, recipient_count) VALUES (?, 0, ?, 'platform.inbound_recorded', 'raw', ?, ?, 0)`),
			request.PublicationID, request.MarkerEventID, strings.Repeat("d", 64), strings.Repeat("e", 64)); err == nil {
			t.Fatal("duplicate ordinal insert error = nil")
		}
	})

	t.Run("extra child rejected on read", func(t *testing.T) {
		request := inboundPublicationProofRequest(t, candidate, runID, generation, sequence, "delivery-extra-child")
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

func inboundPublicationProofRequest(t *testing.T, candidate runtimepipeline.StandingServiceCandidate, runID string, generation, sequence int64, providerEventID string) runtimeinbound.Request {
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
		ExpectedPublicationSequence: sequence, ExpectedGeneration: generation,
		ResolvedRunID: runID, MarkerEventID: markerEventID,
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
	authorization := runtimeprovideroutput.MustAuthorization(
		request.Provider, string(normalized.Type()), "provider.github", "1.0.0",
		"sha256:"+strings.Repeat("a", 64),
		triggergeneration.FromCanonicalBytes([]byte("proof-generation")),
	)
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
