package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	storerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	privateauthoractivity "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSelectedContractExecutionMaterializationAllowsSelectedPendingNodeFrontier(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := newTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700002400, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)

	_, err := pg.MaterializeRunFork(ctx, runfork.RunForkMaterializeRequest{SourceRunID: sourceRunID, At: eventID})
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkBlockerNonAgentDeliveryReplayUnsupported) {
		t.Fatalf("MaterializeRunFork error = %v, want non-agent blocker", err)
	}

	request := runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	}
	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, request)
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	if materialized.ForkRunID == "" || materialized.SelectedContractBinding == nil || !materialized.DeliveryResumeBlocked {
		t.Fatalf("materialization = %#v", materialized)
	}
	replayed, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, request)
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution exact replay: %v", err)
	}
	replayedBindingAt := replayed.SelectedContractBinding.CreatedAt
	materializedBindingAt := materialized.SelectedContractBinding.CreatedAt
	replayedForkAt := replayed.ForkPoint.Timestamp
	materializedForkAt := materialized.ForkPoint.Timestamp
	replayed.SelectedContractBinding.CreatedAt = time.Time{}
	materialized.SelectedContractBinding.CreatedAt = time.Time{}
	replayed.ForkPoint.Timestamp = time.Time{}
	materialized.ForkPoint.Timestamp = time.Time{}
	if !replayedBindingAt.Equal(materializedBindingAt) ||
		!replayedForkAt.Equal(materializedForkAt) ||
		!reflect.DeepEqual(replayed, materialized) {
		t.Fatalf("selected-contract materialization replay = %#v, want %#v", replayed, materialized)
	}
	var forkBundleHash, forkBundleSource string
	if err := db.QueryRowContext(ctx, `
		SELECT bundle_hash, bundle_source
		FROM runs
		WHERE run_id = $1::uuid
	`, materialized.ForkRunID).Scan(&forkBundleHash, &forkBundleSource); err != nil {
		t.Fatalf("load selected fork bundle identity: %v", err)
	}
	if forkBundleHash != authorActivityTestBundleHash || forkBundleSource != storerunlifecycle.BundleSourceEphemeral {
		t.Fatalf("selected fork bundle identity = hash:%q source:%q, want inherited canonical identity", forkBundleHash, forkBundleSource)
	}
	var replayRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_delivery_event_replays WHERE fork_run_id = $1::uuid`, materialized.ForkRunID).Scan(&replayRows); err != nil {
		t.Fatalf("count replay rows: %v", err)
	}
	if replayRows != 0 {
		t.Fatalf("delivery replay rows = %d, want selected execution materialization to avoid #570 replay", replayRows)
	}
}

func TestRunForkSelectedContractBindingUsesPersistedPostgresTimestampPrecision(t *testing.T) {
	createdAt := time.Date(2026, time.July, 30, 1, 2, 3, 123456700, time.FixedZone("test", -4*60*60))
	binding, err := normalizeRunForkSelectedContractBinding(runfork.RunForkSelectedContractBindingRequest{
		ForkRunID:   uuid.NewString(),
		SourceRunID: uuid.NewString(),
		ForkEventID: uuid.NewString(),
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            runfork.RunForkContractSelectionModeSelectedContracts,
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	}, createdAt)
	if err != nil {
		t.Fatalf("normalizeRunForkSelectedContractBinding: %v", err)
	}
	want := createdAt.UTC().Round(time.Microsecond)
	if !binding.CreatedAt.Equal(want) || binding.CreatedAt.Location() != time.UTC {
		t.Fatalf("created_at = %s (%s), want %s (UTC)", binding.CreatedAt, binding.CreatedAt.Location(), want)
	}
}

func TestSelectedContractExecutionMaterializationStampsPersistedBundleIdentity(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700002402, 0).UTC()
	bundleHash := "bundle-v1:sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)
	seedStoreTestPersistedBundle(t, db, bundleHash)

	bundleSource, err := runtimecorrelation.NewPersistedBundleSourceFact(bundleHash)
	if err != nil {
		t.Fatalf("NewPersistedBundleSourceFact: %v", err)
	}
	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID:      sourceRunID,
		At:               eventID,
		BundleSourceFact: bundleSource,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	var forkBundleHash, forkBundleSource string
	if err := db.QueryRowContext(ctx, `
		SELECT bundle_hash, bundle_source
		FROM runs
		WHERE run_id = $1::uuid
	`, materialized.ForkRunID).Scan(&forkBundleHash, &forkBundleSource); err != nil {
		t.Fatalf("load selected fork bundle identity: %v", err)
	}
	if forkBundleHash != bundleHash || forkBundleSource != storerunlifecycle.BundleSourcePersisted {
		t.Fatalf("selected fork bundle identity = hash:%q source:%q, want persisted canonical identity", forkBundleHash, forkBundleSource)
	}
}

func TestSelectedContractExecutionMaterializationConsumesPlanSnapshotMetadata(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700002405, 0).UTC()
	seedSelectedContractExecutionStoreSourceUnpublished(t, db, sourceRunID, entityID, eventID, at)
	if _, err := db.ExecContext(ctx, `
		UPDATE events
		SET flow_instance = ''
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, sourceRunID, entityID); err != nil {
		t.Fatalf("clear event flow metadata: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE entity_state
		SET flow_instance = 'selected-state-flow/at-T',
		    entity_type = 'selected_case',
		    updated_at = $3
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, sourceRunID, entityID, at.Add(time.Minute)); err != nil {
		t.Fatalf("update source entity_state metadata: %v", err)
	}
	captureRunForkTestRevision(t, db, sourceRunID)

	plan, err := pg.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if len(plan.Entities) != 1 || plan.Entities[0].MaterializationMetadata == nil {
		t.Fatalf("plan entities = %#v, want materialization metadata", plan.Entities)
	}
	if got := plan.Entities[0].MaterializationMetadata.Source; got != runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState {
		t.Fatalf("metadata source = %q, want source entity_state", got)
	}

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	if materialized.ForkRunID == "" {
		t.Fatalf("materialized fork run_id is empty: %#v", materialized)
	}
	var flowInstance, entityType string
	if err := db.QueryRowContext(ctx, `
		SELECT flow_instance, entity_type
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, materialized.ForkRunID, entityID).Scan(&flowInstance, &entityType); err != nil {
		t.Fatalf("load selected fork entity_state: %v", err)
	}
	if flowInstance != "selected-state-flow/at-T" || entityType != "selected_case" {
		t.Fatalf("selected fork metadata = flow:%s type:%s", flowInstance, entityType)
	}
}

func TestLoadRunForkSelectedContractSourceEventsRestoresPersistedChronology(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	earlierEventID := "ffffffff-ffff-4fff-8fff-ffffffffffff"
	laterEventID := "00000000-0000-4000-8000-000000000001"
	earlierAt := time.Unix(1700002406, 0).UTC()
	laterAt := earlierAt.Add(time.Second)
	seedSelectedContractExecutionStoreSourceUnpublished(t, db, sourceRunID, entityID, earlierEventID, earlierAt)
	laterEvent := semanticEventRecordFixture(
		laterEventID, sourceRunID, "item.received", eventtest.Producer(events.EventProducerPlatform, "test"), []byte(`{}`),
		semanticEventRecordFixtureEnvelope(entityID, ""), laterAt,
	)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin later event transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	story, err := privateauthoractivity.Begin(ctx, tx, privateauthoractivity.DialectPostgres)
	if err != nil {
		t.Fatalf("begin later event story: %v", err)
	}
	if err := commitSemanticEventFixtureWithRoutesStoryTx(ctx, pg, tx, story, laterEvent, []events.DeliveryRoute{testEntitylessNodeDeliveryRoute("test-node")}); err != nil {
		t.Fatalf("seed later event and delivery: %v", err)
	}
	if err := story.Finalize(ctx); err != nil {
		t.Fatalf("finalize later event story: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit later event transaction: %v", err)
	}
	captureRunForkTestRevision(t, db, sourceRunID)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          laterEventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}

	loaded, err := pg.LoadRunForkSelectedContractSourceEvents(ctx, sourceRunID, materialized.ForkRunID, []string{earlierEventID, laterEventID}, nil)
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSourceEvents: %v", err)
	}
	if len(loaded) != 2 || loaded[0].SourceEventID != earlierEventID || loaded[1].SourceEventID != laterEventID {
		t.Fatalf("loaded source chronology = %#v, want [%s %s]", loaded, earlierEventID, laterEventID)
	}
}

func TestLoadRunForkSelectedContractSourceEventsPreservesExactPayloadBytes(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700002408, 0).UTC()
	payload := []byte("{\n  \"numeric\": 1.0, \"ordered\": {\"b\": 2, \"a\": 1}\n}")
	seedSelectedContractExecutionStoreSourceRawWithPayload(
		t, db, sourceRunID, entityID, eventID, at,
		[]events.DeliveryRoute{testEntitylessNodeDeliveryRoute("test-node")}, payload,
	)
	captureRunForkTestRevision(t, db, sourceRunID)

	var queryProjection []byte
	if err := db.QueryRowContext(ctx, `SELECT payload::text FROM events WHERE event_id=$1::uuid`, eventID).Scan(&queryProjection); err != nil {
		t.Fatalf("load derived payload query projection: %v", err)
	}
	if bytes.Equal(queryProjection, payload) {
		t.Fatalf("test fixture did not diverge JSONB query projection %q from raw bytes %q", queryProjection, payload)
	}

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	loaded, err := pg.LoadRunForkSelectedContractSourceEvents(ctx, sourceRunID, materialized.ForkRunID, []string{eventID}, nil)
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSourceEvents: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("loaded source events = %#v, want one", loaded)
	}
	if !bytes.Equal(loaded[0].Payload, payload) {
		t.Fatalf("selected-contract source payload = %q, want exact admitted bytes %q", loaded[0].Payload, payload)
	}
}

func TestSelectedContractExecutionMaterializationTreatsSourceConversationHistoryAsLineage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	sessionID := uuid.NewString()
	auditID := uuid.NewString()
	turnID := uuid.NewString()
	at := time.Unix(1700002410, 0).UTC()
	seedSelectedContractExecutionStoreSourceUnpublished(t, db, sourceRunID, entityID, eventID, at)
	seedSelectedContractSourceConversationHistory(t, db, sourceRunID, entityID, eventID, sessionID, auditID, turnID, at)
	captureRunForkTestRevision(t, db, sourceRunID)

	plan, err := pg.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	for _, code := range []string{
		runfork.RunForkBlockerSessionHistoryUnproven,
		runfork.RunForkBlockerConversationAuditUnproven,
		runfork.RunForkBlockerActiveTurnHistoryUnproven,
	} {
		if !runForkTestHasPlanBlocker(plan, code) {
			t.Fatalf("plan blockers = %#v, want %s", plan.UnsupportedBlockers, code)
		}
	}

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	if materialized.ForkRunID == "" {
		t.Fatalf("materialized fork run_id is empty: %#v", materialized)
	}
	for _, code := range []string{
		runfork.RunForkBlockerSessionHistoryUnproven,
		runfork.RunForkBlockerConversationAuditUnproven,
		runfork.RunForkBlockerActiveTurnHistoryUnproven,
	} {
		if runForkTestHasMaterializationBlocker(materialized, code) {
			t.Fatalf("selected-contract materialization kept %s: %#v", code, materialized.UnsupportedBlockers)
		}
	}
	for _, fact := range []string{
		runfork.RunForkReplayResumeFactSessionHistory,
		runfork.RunForkReplayResumeFactConversationAuditHistory,
		runfork.RunForkReplayResumeFactActiveTurnHistory,
	} {
		if !runForkTestHasLineageDispositionOwner(materialized.ReplayResumeAdmission, fact, runfork.RunForkSelectedContractSessionTurnAuditLineagePolicyOwner) {
			t.Fatalf("admission missing %s lineage owner: %#v", fact, materialized.ReplayResumeAdmission)
		}
	}
	assertNoCopiedConversationRows(t, db, materialized.ForkRunID, sessionID, auditID, turnID)
}

func TestSelectedContractExecutionMaterializationKeepsCanonicalReplayScopesSourceLocal(t *testing.T) {
	for _, tc := range []struct {
		name       string
		reasonCode string
	}{
		{name: "direct", reasonCode: replayScopeReasonDirect},
		{name: "subscribed", reasonCode: replayScopeReasonSubscribed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			pg := admitTestPostgresStore(t, db)
			ctx := testAuthorActivityContext()
			sourceRunID := uuid.NewString()
			entityID := uuid.NewString()
			eventID := uuid.NewString()
			at := time.Unix(1700002415, 0).UTC()
			if tc.reasonCode == replayScopeReasonDirect {
				seedSelectedContractExecutionStoreSourceWithoutDelivery(t, db, sourceRunID, entityID, eventID, at)
			} else {
				seedSelectedContractExecutionStoreSourceUnpublished(t, db, sourceRunID, entityID, eventID, at)
			}
			captureRunForkTestRevision(t, db, sourceRunID)
			var sourceScope string
			if err := db.QueryRowContext(ctx, `SELECT scope FROM committed_replay_scopes WHERE event_id = $1::uuid`, eventID).Scan(&sourceScope); err != nil {
				t.Fatalf("load source replay scope: %v", err)
			}
			wantScope, _ := committedReplayScopeFromReasonCode(tc.reasonCode)
			if sourceScope != string(wantScope) {
				t.Fatalf("source replay scope = %q, want %q", sourceScope, wantScope)
			}

			plan, err := pg.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
			if err != nil {
				t.Fatalf("PlanRunFork: %v", err)
			}
			if runForkTestHasPlanBlocker(plan, runfork.RunForkBlockerCommittedReplayScopeReplayUnsupported) {
				t.Fatalf("plan blockers = %#v, canonical replay scope is an admitted initial fact", plan.UnsupportedBlockers)
			}

			materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
				SourceRunID: sourceRunID,
				At:          eventID,
				ContractSelection: runfork.RunForkContractSelection{
					Mode:            "selected_contracts",
					ContractsRoot:   "/tmp/selected-contracts",
					WorkflowName:    "selected-workflow",
					WorkflowVersion: "v1",
				},
			})
			if err != nil {
				t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
			}
			if materialized.ForkRunID == "" {
				t.Fatalf("materialized fork run_id is empty: %#v", materialized)
			}
			if runForkTestHasMaterializationBlocker(materialized, runfork.RunForkBlockerCommittedReplayScopeReplayUnsupported) {
				t.Fatalf("selected-contract materialization kept committed replay-scope blocker: %#v", materialized.UnsupportedBlockers)
			}
			assertNoCopiedReplayScopeMarkers(t, db, materialized.ForkRunID)
		})
	}
}

func TestSelectedContractExecutionMaterializationKeepsActiveDeliverySessionCouplingFailClosed(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700002420, 0).UTC()
	seedSelectedContractExecutionStoreSourceUnpublished(t, db, sourceRunID, entityID, eventID, at)
	sessionID := uuid.NewString()
	activeClaim := seedSelectedContractSourceConversationHistoryWithActiveDelivery(t, db, sourceRunID, entityID, eventID, sessionID, uuid.NewString(), uuid.NewString(), at)
	if _, err := pg.BindAgentSession(ctx, activeClaim, sessionID); err != nil {
		t.Fatalf("bind active delivery session: %v", err)
	}
	captureRunForkTestRevision(t, db, sourceRunID)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err == nil || (!strings.Contains(err.Error(), runfork.RunForkBlockerSessionHistoryUnproven) && !strings.Contains(err.Error(), runfork.RunForkBlockerActiveTurnHistoryUnproven)) {
		t.Fatalf("materialization error = %v, want active session/turn blocker", err)
	}
	if materialized.ForkRunID != "" {
		t.Fatalf("materialized fork despite active coupling: %#v", materialized)
	}
	if !runForkTestHasMaterializationBlocker(materialized, runfork.RunForkBlockerSessionHistoryUnproven) ||
		!runForkTestHasMaterializationBlocker(materialized, runfork.RunForkBlockerActiveTurnHistoryUnproven) {
		t.Fatalf("materialization blockers = %#v, want active session and turn blockers", materialized.UnsupportedBlockers)
	}
	assertNoSelectedContractForkRows(t, db, sourceRunID)
}

func TestSelectedContractExecutionMaterializationAdmitsSameSourceDeliveryForkPointEmission(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	forkPointEventID := uuid.NewString()
	sessionID := uuid.NewString()
	auditID := uuid.NewString()
	turnID := uuid.NewString()
	at := time.Unix(1700002425, 0).UTC()
	forkAt := at.Add(30 * time.Second)
	seedSelectedContractExecutionStoreSourceWithoutDelivery(t, db, sourceRunID, entityID, sourceEventID, at)
	seedSelectedContractSourceConversationHistory(t, db, sourceRunID, entityID, sourceEventID, sessionID, auditID, turnID, at)
	sourceRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("validation-coordinator")}
	sourceEvent := commitPostgresDeliveryFixture(t, ctx, db, sourceEventID, sourceRoute)
	claimPostgresDeliveryFixture(t, ctx, db, sourceEvent, sourceRoute)
	seedPostgresChildEventRecordFixture(t, ctx, db, forkPointEventID, sourceRunID, sourceEventID,
		"validation/vertical.ready_for_review", events.EventProducerAgent, "validation-coordinator", entityID, "", []byte(`{}`), forkAt)
	captureRunForkTestRevision(t, db, sourceRunID)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          forkPointEventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	if materialized.ForkRunID == "" {
		t.Fatalf("materialized fork run_id is empty: %#v", materialized)
	}
	for _, code := range []string{
		runfork.RunForkBlockerDeliveryHistoryUnproven,
		runfork.RunForkBlockerSessionHistoryUnproven,
		runfork.RunForkBlockerConversationAuditUnproven,
		runfork.RunForkBlockerActiveTurnHistoryUnproven,
	} {
		if runForkTestHasMaterializationBlocker(materialized, code) {
			t.Fatalf("selected-contract materialization kept %s: %#v", code, materialized.UnsupportedBlockers)
		}
	}
	if !runForkTestHasLineageDispositionOwnerClassification(
		materialized.ReplayResumeAdmission,
		runfork.RunForkReplayResumeFactDeliveryInProgressHistory,
		runfork.RunForkSelectedContractActiveSourceDeliveryConversationCouplingPolicyOwner,
		runfork.RunForkSelectedContractActiveSourceDeliveryConversationCouplingClassification,
	) {
		t.Fatalf("admission missing #678 active delivery lineage owner: %#v", materialized.ReplayResumeAdmission)
	}
	for _, fact := range []string{
		runfork.RunForkReplayResumeFactSessionHistory,
		runfork.RunForkReplayResumeFactConversationAuditHistory,
		runfork.RunForkReplayResumeFactActiveTurnHistory,
	} {
		if !runForkTestHasLineageDispositionOwner(materialized.ReplayResumeAdmission, fact, runfork.RunForkSelectedContractSessionTurnAuditLineagePolicyOwner) {
			t.Fatalf("admission missing %s #661 lineage owner: %#v", fact, materialized.ReplayResumeAdmission)
		}
	}
	assertNoCopiedConversationRows(t, db, materialized.ForkRunID, sessionID, auditID, turnID)
}

func TestSelectedContractExecutionMaterializationKeepsUnrelatedInProgressDeliveryFailClosed(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	unrelatedEventID := uuid.NewString()
	forkPointEventID := uuid.NewString()
	sessionID := uuid.NewString()
	auditID := uuid.NewString()
	turnID := uuid.NewString()
	at := time.Unix(1700002427, 0).UTC()
	forkAt := at.Add(30 * time.Second)
	seedSelectedContractExecutionStoreSourceWithoutDelivery(t, db, sourceRunID, entityID, sourceEventID, at)
	seedSelectedContractSourceConversationHistory(t, db, sourceRunID, entityID, sourceEventID, sessionID, auditID, turnID, at)
	seedPostgresSemanticEventRecordFixture(t, ctx, db, unrelatedEventID, sourceRunID, "unrelated.started",
		events.EventProducerPlatform, "source-runtime", entityID, "", at.Add(10*time.Second))
	unrelatedRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("validation-coordinator")}
	unrelatedEvent := commitPostgresDeliveryFixture(t, ctx, db, unrelatedEventID, unrelatedRoute)
	claimPostgresDeliveryFixture(t, ctx, db, unrelatedEvent, unrelatedRoute)
	captureRunForkTestRevision(t, db, sourceRunID)
	seedPostgresChildEventRecordFixture(t, ctx, db, forkPointEventID, sourceRunID, sourceEventID,
		"validation/vertical.ready_for_review", events.EventProducerAgent, "validation-coordinator", entityID, "", []byte(`{}`), forkAt)
	captureRunForkTestRevision(t, db, sourceRunID)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          forkPointEventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkBlockerSessionHistoryUnproven) {
		t.Fatalf("materialization error = %v, want conversation history blocked by unrelated active delivery", err)
	}
	if materialized.ForkRunID != "" {
		t.Fatalf("materialized fork despite unrelated active coupling: %#v", materialized)
	}
	if !runForkTestHasMaterializationBlocker(materialized, runfork.RunForkBlockerSessionHistoryUnproven) ||
		!runForkTestHasMaterializationBlocker(materialized, runfork.RunForkBlockerActiveTurnHistoryUnproven) {
		t.Fatalf("materialization blockers = %#v, want conversation blockers", materialized.UnsupportedBlockers)
	}
	assertNoSelectedContractForkRows(t, db, sourceRunID)
}

func TestSelectedContractExecutionMaterializationKeepsUnrelatedInProgressDeliveryWithoutConversationHistoryFailClosed(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	unrelatedEventID := uuid.NewString()
	forkPointEventID := uuid.NewString()
	at := time.Unix(1700002428, 0).UTC()
	forkAt := at.Add(30 * time.Second)
	seedSelectedContractExecutionStoreSourceWithoutDelivery(t, db, sourceRunID, entityID, sourceEventID, at)
	seedPostgresSemanticEventRecordFixture(t, ctx, db, unrelatedEventID, sourceRunID, "unrelated.started",
		events.EventProducerPlatform, "source-runtime", entityID, "", at.Add(10*time.Second))
	unrelatedRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("unrelated-agent")}
	unrelatedEvent := commitPostgresDeliveryFixture(t, ctx, db, unrelatedEventID, unrelatedRoute)
	claimPostgresDeliveryFixture(t, ctx, db, unrelatedEvent, unrelatedRoute)
	seedPostgresChildEventRecordFixture(t, ctx, db, forkPointEventID, sourceRunID, sourceEventID,
		"validation/vertical.ready_for_review", events.EventProducerAgent, "validation-coordinator", entityID, "", []byte(`{}`), forkAt)
	captureRunForkTestRevision(t, db, sourceRunID)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          forkPointEventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkBlockerDeliveryHistoryUnproven) {
		t.Fatalf("materialization error = %v, want unrelated active delivery blocker", err)
	}
	if materialized.ForkRunID != "" {
		t.Fatalf("materialized fork despite unrelated active delivery: %#v", materialized)
	}
	if !runForkTestHasMaterializationBlocker(materialized, runfork.RunForkBlockerDeliveryHistoryUnproven) {
		t.Fatalf("materialization blockers = %#v, want delivery history blocker", materialized.UnsupportedBlockers)
	}
	assertNoSelectedContractForkRows(t, db, sourceRunID)
}

func TestSelectedContractExecutionMaterializationDoesNotTreatTerminalDeliveryAsActiveSessionCoupling(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	sessionID := uuid.NewString()
	auditID := uuid.NewString()
	turnID := uuid.NewString()
	at := time.Unix(1700002430, 0).UTC()
	seedSelectedContractExecutionStoreSourceUnpublished(t, db, sourceRunID, entityID, eventID, at)
	seedSelectedContractSourceConversationHistory(t, db, sourceRunID, entityID, eventID, sessionID, auditID, turnID, at)
	terminalRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("terminal-agent")}
	terminalEvent := commitPostgresDeliveryFixture(t, ctx, db, eventID, terminalRoute)
	terminalClaim := claimPostgresDeliveryFixture(t, ctx, db, terminalEvent, terminalRoute)
	failure := testFailureEnvelope(runtimefailures.ClassRetryExhausted, "terminal_source_delivery", nil)
	if _, err := pg.SettleFailure(ctx, terminalClaim.Claim, runtimedelivery.Settlement{
		Disposition: runtimedelivery.FailureDeadLetter,
		ReasonCode:  "terminal_source_delivery",
		Failure:     &failure, RuleSelection: runtimedelivery.NotApplicableHandlerRuleSelection(),
	}); err != nil {
		t.Fatalf("settle terminal delivery: %v", err)
	}
	captureRunForkTestRevision(t, db, sourceRunID)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	if materialized.ForkRunID == "" {
		t.Fatalf("materialized fork run_id is empty: %#v", materialized)
	}
	for _, code := range []string{
		runfork.RunForkBlockerSessionHistoryUnproven,
		runfork.RunForkBlockerConversationAuditUnproven,
		runfork.RunForkBlockerActiveTurnHistoryUnproven,
	} {
		if runForkTestHasMaterializationBlocker(materialized, code) {
			t.Fatalf("terminal delivery preserved conversation blocker %s: %#v", code, materialized.UnsupportedBlockers)
		}
	}
	assertNoCopiedConversationRows(t, db, materialized.ForkRunID, sessionID, auditID, turnID)
}

func TestSelectedContractExecutionActivationKeepsPostFrontierActiveDeliveryFailClosed(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	forkPointEventID := uuid.NewString()
	unrelatedEventID := uuid.NewString()
	at := time.Unix(1700002435, 0).UTC()
	forkAt := at.Add(30 * time.Second)
	seedSelectedContractExecutionStoreSourceWithoutDelivery(t, db, sourceRunID, entityID, sourceEventID, at)
	seedPostgresChildEventRecordFixture(t, ctx, db, forkPointEventID, sourceRunID, sourceEventID,
		"validation/vertical.ready_for_review", events.EventProducerAgent, "validation-coordinator", entityID, "", []byte(`{}`), forkAt)
	captureRunForkTestRevision(t, db, sourceRunID)
	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          forkPointEventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}

	seedPostgresSemanticEventRecordFixture(t, ctx, db, unrelatedEventID, sourceRunID, "unrelated.started",
		events.EventProducerPlatform, "source-runtime", entityID, "flow-a/1", at.Add(10*time.Second))
	unrelatedRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("unrelated-agent")}
	unrelatedEvent := commitPostgresDeliveryFixture(t, ctx, db, unrelatedEventID, unrelatedRoute)
	claimPostgresDeliveryFixture(t, ctx, db, unrelatedEvent, unrelatedRoute)
	captureRunForkTestRevision(t, db, sourceRunID)

	activation, err := pg.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		ForkRunID: materialized.ForkRunID,
	})
	if err == nil || !strings.Contains(err.Error(), "source_active_conversation_session_coupling_after_fork_point") {
		t.Fatalf("activation error = %v, want post-frontier active delivery blocker", err)
	}
	if activation.Activated || activation.BranchDivergence != nil {
		t.Fatalf("activation = %#v, want blocked before branch divergence", activation)
	}
	if !runForkTestHasActivationBlocker(activation, "source_active_conversation_session_coupling_after_fork_point") {
		t.Fatalf("activation blockers = %#v, want active delivery/session coupling blocker", activation.UnsupportedBlockers)
	}
}

func TestSelectedContractExecutionMaterializationRejectsActiveTimerBeforeMutation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	sourceTimerID := uuid.NewString()
	sourceRef := timeridentity.WorkflowTimerActivationRef{
		ActivationID:        sourceTimerID,
		DeclarationKey:      "selected-timer",
		DeclarationRevision: "sha256:selected-timer",
		Cause:               timeridentity.WorkflowTimerActivationCauseInitial,
	}
	at := time.Unix(1700002500, 0).UTC()
	seedSelectedContractExecutionStoreSourceUnpublished(t, db, sourceRunID, entityID, eventID, at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO timers (timer_id, run_id, timer_name, entity_id, flow_scope_key, flow_instance_id, flow_instance, fire_event, fire_payload, routing_source, execution_mode, fire_at, owner_agent, owner_kind, task_type, status, created_at)
		VALUES ($1::uuid, $2::uuid, $3, $4::uuid, 'flow-a', '1', 'flow-a/1', 'timer.selected', '{"source":true}'::jsonb,
		        jsonb_build_object('kind', 'flow_owned_control', 'route', jsonb_build_object('flow_id', 'flow-a', 'flow_instance', 'flow-a/1', 'entity_id', $4::text)),
		        'live', $5, 'agent-a', 'system', 'workflow_timer', 'active', $6)
	`, sourceTimerID, sourceRunID, sourceRef.TaskID(), entityID, at.Add(time.Hour), at); err != nil {
		t.Fatalf("seed timer: %v", err)
	}
	captureRunForkTestRevision(t, db, sourceRunID)
	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkBlockerTimerHistoryUnproven) {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution result=%#v error=%v, want timer blocker", materialized, err)
	}
	if materialized.ForkRunID != "" {
		t.Fatalf("materialized timer-bearing fork: %#v", materialized)
	}
	assertNoSelectedContractForkRows(t, db, sourceRunID)
	assertNoForkTimerCopiesForSource(t, db, sourceRunID)

	var sourceTimerCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM timers
		WHERE timer_id = $1::uuid
		  AND run_id = $2::uuid
		  AND status = 'active'
	`, sourceTimerID, sourceRunID).Scan(&sourceTimerCount); err != nil {
		t.Fatalf("count source timers: %v", err)
	}
	if sourceTimerCount != 1 {
		t.Fatalf("source timers = %d, want 1", sourceTimerCount)
	}
}

func TestSelectedContractExecutionMaterializationFailsClosedForUnsupportedTimerHistory(t *testing.T) {
	cases := []struct {
		name      string
		due       runtimegenericschedule.DueBasis
		cancelled bool
	}{
		{name: "active one-shot", due: runtimegenericschedule.AbsoluteDue(time.Unix(1700007125, 0).UTC())},
		{name: "cancelled one-shot", due: runtimegenericschedule.AbsoluteDue(time.Unix(1700007125, 0).UTC()), cancelled: true},
		{name: "active recurring", due: runtimegenericschedule.EveryDue(time.Hour)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			pg := admitTestPostgresStore(t, db)
			ctx := testAuthorActivityContext()
			sourceRunID := uuid.NewString()
			entityID := uuid.NewString()
			eventID := uuid.NewString()
			at := time.Unix(1700003525, 0).UTC()
			seedSelectedContractExecutionStoreSourceUnpublished(t, db, sourceRunID, entityID, eventID, at)
			activation := admitGenericScheduleFixture(t, ctx, pg, testAgentGenericScheduleCommand(
				t, sourceRunID, "agent-a", "flow-a/1", entityID, "selected-"+strings.ReplaceAll(tc.name, " ", "-"), tc.due,
			))
			if tc.cancelled {
				cancelGenericScheduleFixture(t, ctx, pg, activation, "test_cancelled", at.Add(time.Minute))
			}
			captureRunForkTestRevision(t, db, sourceRunID)

			materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
				SourceRunID: sourceRunID,
				At:          eventID,
				ContractSelection: runfork.RunForkContractSelection{
					Mode:            "selected_contracts",
					ContractsRoot:   "/tmp/selected-contracts",
					WorkflowName:    "selected-workflow",
					WorkflowVersion: "v1",
				},
			})
			if err == nil || !strings.Contains(err.Error(), runfork.RunForkBlockerTimerHistoryUnproven) {
				t.Fatalf("materialization error = %v, want %s", err, runfork.RunForkBlockerTimerHistoryUnproven)
			}
			if materialized.ForkRunID != "" {
				t.Fatalf("materialized fork despite unsupported timer history: %#v", materialized)
			}
			assertNoSelectedContractForkRows(t, db, sourceRunID)
			assertNoForkTimerCopiesForSource(t, db, sourceRunID)
		})
	}
}

func TestSelectedContractTimerBlockerRemainsFixedWhenSourceTimerIsDeletedLater(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700003550, 0).UTC()
	seedSelectedContractExecutionStoreSourceUnpublished(t, db, sourceRunID, entityID, eventID, at)
	timer := admitGenericScheduleFixture(t, ctx, pg, testAgentGenericScheduleCommand(
		t, sourceRunID, "agent-a", "flow-a/1", entityID, "selected-vanishing-timer", runtimegenericschedule.AbsoluteDue(at.Add(time.Hour)),
	))
	timerID := timer.ID
	captureRunForkTestRevision(t, db, sourceRunID)
	plan, err := pg.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	if !runForkTestHasPlanBlocker(plan, runfork.RunForkBlockerTimerHistoryUnproven) {
		t.Fatalf("plan missing timer blocker: %#v", plan.ReplayResumeAdmission)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM timers WHERE timer_id = $1::uuid`, timerID); err != nil {
		t.Fatalf("delete timer after planning: %v", err)
	}
	captureRunForkTestRevision(t, db, sourceRunID)
	repeatedPlan, err := pg.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: eventID})
	if err != nil {
		t.Fatalf("repeat PlanRunFork: %v", err)
	}
	if !runForkTestHasPlanBlocker(repeatedPlan, runfork.RunForkBlockerTimerHistoryUnproven) {
		t.Fatalf("repeated fixed-revision plan lost timer blocker: %#v", repeatedPlan.ReplayResumeAdmission)
	}
	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err == nil || !strings.Contains(err.Error(), runfork.RunForkBlockerTimerHistoryUnproven) || materialized.ForkRunID != "" {
		t.Fatalf("fixed timer blocker materialization result=%#v error=%v", materialized, err)
	}
	assertNoSelectedContractForkRows(t, db, sourceRunID)
}

func TestPostTSourceTimerActivatesAsSelectedBranchDivergence(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700003600, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	if materialized.ForkRunID == "" {
		t.Fatalf("materialized fork run_id is empty: %#v", materialized)
	}
	seedSelectedContractExecutionForkLineage(t, pg, db, sourceRunID, materialized.ForkRunID, eventID, entityID, at)

	admitGenericScheduleFixture(t, ctx, pg, testAgentGenericScheduleCommand(
		t, sourceRunID, "agent-a", "flow-a/1", entityID, "post-t-source-timer", runtimegenericschedule.AbsoluteDue(at.Add(time.Hour)),
	))
	captureRunForkTestRevision(t, db, sourceRunID)

	activation, err := pg.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		ForkRunID:             materialized.ForkRunID,
		AllowedSourceEventIDs: []string{eventID},
	})
	if err != nil {
		t.Fatalf("ActivateRunForkForSelectedContractExecution: %v", err)
	}
	if !activation.Activated || !activation.SourceAdvancedAfterFork || activation.BranchDivergence == nil {
		t.Fatalf("activation = %#v, want selected branch divergence", activation)
	}
	if runForkTestHasActivationBlocker(activation, "source_timers_advanced_after_fork_point") {
		t.Fatalf("activation blockers = %#v, selected source advancement should branch", activation.UnsupportedBlockers)
	}
	if !containsString(activation.BranchDivergence.SourceAdvancedFacts, "source_timers_advanced_after_fork_point") {
		t.Fatalf("branch divergence facts = %#v, want source_timers_advanced_after_fork_point", activation.BranchDivergence.SourceAdvancedFacts)
	}

	var sourceStatus, forkStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status: %v", err)
	}
	if sourceStatus != "running" || forkStatus != runfork.RunForkActivatedStatus {
		t.Fatalf("run statuses source=%q fork=%q, want live source branch and activated fork", sourceStatus, forkStatus)
	}

	var branchRows, forkTimerRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_branch_divergences
		WHERE fork_run_id = $1::uuid
	`, materialized.ForkRunID).Scan(&branchRows); err != nil {
		t.Fatalf("count branch divergences: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM timers WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkTimerRows); err != nil {
		t.Fatalf("count fork timers: %v", err)
	}
	if branchRows != 1 || forkTimerRows != 0 {
		t.Fatalf("branch rows=%d fork timer rows=%d, want one divergence and no post-frontier timer copies", branchRows, forkTimerRows)
	}
	assertNoForkTimerCopiesForSource(t, db, sourceRunID)
}

func TestPostTSourceSessionDoesNotChangeFixedEventMaterialization(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	sessionID := uuid.NewString()
	at := time.Unix(1700003605, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)
	identity := testAgentIdentity(t, "agent-a", "flow-a/1")
	fields := testAgentIdentityStorageFields(t, identity)
	seedTestAgentRow(t, ctx, db, true, identity, "active")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			memory_enabled, memory_source,
			status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored',
			'active', $10, $10)
	`, sessionID, sourceRunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		at.Add(time.Minute)); err != nil {
		t.Fatalf("seed post-T source session: %v", err)
	}
	captureRunForkTestRevision(t, db, sourceRunID)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	if materialized.ForkRunID == "" {
		t.Fatalf("materialized fork run_id is empty: %#v", materialized)
	}
	if runForkTestHasMaterializationBlocker(materialized, "source_sessions_advanced_after_fork_point") {
		t.Fatalf("selected-contract materialization kept post-T source session blocker: %#v", materialized.UnsupportedBlockers)
	}
	if runForkTestHasLineageDispositionOwnerClassification(materialized.ReplayResumeAdmission, runfork.RunForkReplayResumeFactSourceAdvanced, runfork.RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner, "source_sessions_advanced_after_fork_point") {
		t.Fatalf("materialization replay admission consumed a post-frontier source session: %#v", materialized.ReplayResumeAdmission)
	}
	var copiedSessions int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM agent_sessions
		WHERE run_id = $1::uuid
		   OR session_id = $2::uuid
	`, materialized.ForkRunID, sessionID).Scan(&copiedSessions); err != nil {
		t.Fatalf("count copied source session: %v", err)
	}
	if copiedSessions != 1 {
		t.Fatalf("conversation session fork/copy count = %d, want original source row only", copiedSessions)
	}
}

func TestPostTSourceConversationHistoryDoesNotChangeFixedEventMaterialization(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	sessionID := uuid.NewString()
	at := time.Unix(1700003608, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)
	seedPostTActiveConversationCoupling(t, db, sourceRunID, entityID, eventID, sessionID, at)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	if materialized.ForkRunID == "" {
		t.Fatalf("materialization = %#v, want fixed-event materialization", materialized)
	}
	if runForkTestHasMaterializationBlocker(materialized, "source_active_conversation_session_coupling_after_fork_point") {
		t.Fatalf("materialization blockers = %#v, post-frontier coupling belongs to fresh activation validation", materialized.UnsupportedBlockers)
	}
}

func TestPostTGlobalRoutingRuleDoesNotChangeSelectedContractActivation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700003610, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	if materialized.ForkRunID == "" {
		t.Fatalf("materialized fork run_id is empty: %#v", materialized)
	}
	seedSelectedContractExecutionForkLineage(t, pg, db, sourceRunID, materialized.ForkRunID, eventID, entityID, at)
	postForkRouteNode := mustPersistenceNode("flow-a", "post-t-source-route-node").Key()

	if _, err := db.ExecContext(ctx, `
		INSERT INTO routing_rules (
			event_pattern, subscriber_type, subscriber_id, flow_instance, source_flow,
			is_materialized, status, created_at
		)
		VALUES ('item.received', 'node', $1, 'flow-a/1', 'flow-a', true, 'active', $2)
	`, postForkRouteNode, at.Add(time.Minute)); err != nil {
		t.Fatalf("seed post-T route: %v", err)
	}

	activation, err := pg.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		ForkRunID:             materialized.ForkRunID,
		ConfirmSourceFreeze:   true,
		AllowedSourceEventIDs: []string{eventID},
	})
	if err != nil {
		t.Fatalf("ActivateRunForkForSelectedContractExecution: %v", err)
	}
	if !activation.Activated || activation.SourceAdvancedAfterFork || activation.BranchDivergence != nil {
		t.Fatalf("activation = %#v, want current global route to leave fixed source frontier unchanged", activation)
	}
	if runForkTestHasActivationBlocker(activation, "source_routes_advanced_after_fork_point") {
		t.Fatalf("activation blockers = %#v, current global routes are not historical source facts", activation.UnsupportedBlockers)
	}
	if runForkTestHasDispositionBlocker(activation.ReplayResumeAdmission, runfork.RunForkReplayResumeFactSourceAdvanced, "source_routes_advanced_after_fork_point") {
		t.Fatalf("activation replay admission = %#v, current global routes must not enter the fixed workset", activation.ReplayResumeAdmission)
	}

	var sourceStatus, forkStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status: %v", err)
	}
	if sourceStatus != "forked" || forkStatus != "running" {
		t.Fatalf("run statuses source=%q fork=%q, want source forked and fork running after activation", sourceStatus, forkStatus)
	}

	var branchRows, forkDeliveryRows, sourceRouteRows, routeRecoveryRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_branch_divergences
		WHERE fork_run_id = $1::uuid
	`, materialized.ForkRunID).Scan(&branchRows); err != nil {
		t.Fatalf("count branch divergences: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
	`, materialized.ForkRunID).Scan(&forkDeliveryRows); err != nil {
		t.Fatalf("count fork deliveries: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM routing_rules
		WHERE subscriber_id = $1
	`, postForkRouteNode).Scan(&sourceRouteRows); err != nil {
		t.Fatalf("count source route rows: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_route_recoveries
		WHERE fork_run_id = $1::uuid
	`, materialized.ForkRunID).Scan(&routeRecoveryRows); err != nil {
		t.Fatalf("count route recovery rows: %v", err)
	}
	if branchRows != 0 || sourceRouteRows != 1 || routeRecoveryRows != 0 {
		t.Fatalf("branch rows=%d fork delivery rows=%d source route rows=%d route recovery rows=%d, want no divergence, one untouched global route, and no invented recovery", branchRows, forkDeliveryRows, sourceRouteRows, routeRecoveryRows)
	}
}

func TestSelectedContractActivation_IgnoresExcludedSourceSessionColumnChanges(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	sessionID := uuid.NewString()
	at := time.Unix(1700003615, 0).UTC()
	seedSelectedContractExecutionStoreSourceUnpublished(t, db, sourceRunID, entityID, eventID, at)
	seedRunForkSessionProjection(t, db, sourceRunID, "selected-session-agent", sessionID, "terminated", at)
	selectedRevision := captureRunForkTestRevision(t, db, sourceRunID)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	seedSelectedContractExecutionForkLineage(t, pg, db, sourceRunID, materialized.ForkRunID, eventID, entityID, at)
	mutateRunForkSessionExcludedColumns(t, db, sourceRunID, sessionID, at.Add(time.Minute))
	var afterExcluded int64
	if err := db.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1::uuid`, sourceRunID).Scan(&afterExcluded); err != nil {
		t.Fatalf("load selected source revision after excluded session update: %v", err)
	}
	if afterExcluded != selectedRevision {
		t.Fatalf("selected source revision after excluded session update = %d, want %d", afterExcluded, selectedRevision)
	}

	activation, err := pg.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		ForkRunID:             materialized.ForkRunID,
		ConfirmSourceFreeze:   true,
		AllowedSourceEventIDs: []string{eventID},
	})
	if err != nil {
		t.Fatalf("ActivateRunForkForSelectedContractExecution after excluded session update: %v", err)
	}
	if !activation.Activated || activation.SourceAdvancedAfterFork || activation.BranchDivergence != nil {
		t.Fatalf("activation = %#v, want selected activation without branch divergence", activation)
	}
}

func TestPostTSourceConversationHistoryActivatesAsBranchDivergence(t *testing.T) {
	for _, tc := range []struct {
		name string
		code string
		seed func(context.Context, *PostgresStore, *sql.DB, string, string, string, time.Time) error
	}{
		{
			name: "session",
			code: "source_sessions_advanced_after_fork_point",
			seed: func(ctx context.Context, _ *PostgresStore, db *sql.DB, sourceRunID, entityID, eventID string, at time.Time) error {
				fields := testAgentIdentityStorageFields(t, testAgentIdentity(t, "agent-a", "flow-a/1"))
				_, err := db.ExecContext(ctx, `
					INSERT INTO agent_sessions (
						session_id, run_id, agent_id, agent_name_owner, agent_name_source,
						agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
						memory_enabled, memory_source,
						status, created_at, updated_at
					)
					VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored',
						'active', $10, $10)
				`, uuid.NewString(), sourceRunID, fields.AgentID, fields.NameOwner, fields.NameSource,
					fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
					at.Add(time.Minute))
				return err
			},
		},
		{
			name: "conversation audit",
			code: "source_conversation_audits_advanced_after_fork_point",
			seed: func(ctx context.Context, _ *PostgresStore, db *sql.DB, sourceRunID, entityID, eventID string, at time.Time) error {
				fields := testAgentIdentityStorageFields(t, testAgentIdentity(t, "agent-a", "flow-a/1"))
				_, err := db.ExecContext(ctx, `
					INSERT INTO agent_conversation_audits (
						session_id, run_id, agent_id, agent_name_owner, agent_name_source,
						agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
						entity_id, memory_enabled, memory_source,
						runtime_state, status, created_at, updated_at
					)
					VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10::uuid, FALSE, 'authored',
						'{}'::jsonb, 'active', $11, $11)
				`, uuid.NewString(), sourceRunID, fields.AgentID, fields.NameOwner, fields.NameSource,
					fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
					entityID, at.Add(time.Minute))
				return err
			},
		},
		{
			name: "turn",
			code: "source_turns_advanced_after_fork_point",
			seed: func(ctx context.Context, pg *PostgresStore, db *sql.DB, sourceRunID, entityID, eventID string, at time.Time) error {
				turnID := uuid.NewString()
				sessionID := uuid.NewString()
				identity := testAgentIdentity(t, "agent-a", "flow-a/1")
				event := loadPostgresDeliveryFixtureEvent(t, ctx, db, eventID)
				route := testAgentDeliveryRoute(t, identity.AgentID(), identity.FlowInstance())
				if err := commitDeliveryObligationFixture(ctx, pg, event, route); err != nil {
					return err
				}
				claimed, err := claimDeliveryFixture(ctx, pg, event, route)
				if err != nil {
					return err
				}
				if err := persistManagedAgentTurnReadbackFixtureWithOptions(t, runtimedelivery.WithClaim(ctx, claimed.Claim), pg, runtimellm.AgentTurnRecord{
					AgentID: identity.AgentID(), Identity: agentmemory.Identity{RunID: sourceRunID, Agent: identity},
					RunID: sourceRunID, FlowInstance: identity.FlowInstance(), Memory: agentmemory.Authored(false), SessionID: sessionID,
					EntityID: entityID, TriggerEventID: eventID, TriggerEventType: string(event.Type()), TaskID: "task-a", ParseOK: true,
				}, managedAgentTurnFixtureOptions{TurnID: turnID, Now: at.Add(time.Minute), OriginEvent: &event}); err != nil {
					return err
				}
				_, err = pg.SettleSuccess(ctx, claimed.Claim, nil, time.Millisecond)
				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			pg := admitTestPostgresStore(t, db)
			ctx := testAuthorActivityContext()
			sourceRunID := uuid.NewString()
			entityID := uuid.NewString()
			eventID := uuid.NewString()
			at := time.Unix(1700003620, 0).UTC()
			seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)
			seedTestAgentRow(t, ctx, db, true, testAgentIdentity(t, "agent-a", "flow-a/1"), "active")

			materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
				SourceRunID: sourceRunID,
				At:          eventID,
				ContractSelection: runfork.RunForkContractSelection{
					Mode:            "selected_contracts",
					ContractsRoot:   "/tmp/selected-contracts",
					WorkflowName:    "selected-workflow",
					WorkflowVersion: "v1",
				},
			})
			if err != nil {
				t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
			}
			seedSelectedContractExecutionForkLineage(t, pg, db, sourceRunID, materialized.ForkRunID, eventID, entityID, at)
			if err := tc.seed(ctx, pg, db, sourceRunID, entityID, eventID, at); err != nil {
				t.Fatalf("seed post-T %s: %v", tc.name, err)
			}
			captureRunForkTestRevision(t, db, sourceRunID)

			activation, err := pg.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
				ForkRunID:             materialized.ForkRunID,
				AllowedSourceEventIDs: []string{eventID},
			})
			if err != nil {
				t.Fatalf("ActivateRunForkForSelectedContractExecution: %v", err)
			}
			if !activation.Activated || !activation.SourceAdvancedAfterFork || activation.BranchDivergence == nil {
				t.Fatalf("activation = %#v, want source-advanced branch divergence", activation)
			}
			if runForkTestHasActivationBlocker(activation, tc.code) {
				t.Fatalf("activation blockers = %#v, did not expect %s", activation.UnsupportedBlockers, tc.code)
			}
			if !runForkTestHasLineageDispositionOwnerClassification(activation.ReplayResumeAdmission, runfork.RunForkReplayResumeFactSourceAdvanced, runfork.RunForkSelectedContractSourceAdvancedConversationHistoryPolicyOwner, tc.code) {
				t.Fatalf("activation replay admission = %#v, want source advanced lineage owner %s", activation.ReplayResumeAdmission, tc.code)
			}
			if !containsString(activation.BranchDivergence.SourceAdvancedFacts, tc.code) {
				t.Fatalf("branch divergence facts = %#v, want %s", activation.BranchDivergence.SourceAdvancedFacts, tc.code)
			}
			assertNoForkConversationRows(t, db, materialized.ForkRunID)
		})
	}
}

func TestSelectedContractExecutionActivationRecordsSameSourceDeliveryCouplingAsBranchDivergence(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	forkPointEventID := uuid.NewString()
	sessionID := uuid.NewString()
	auditID := uuid.NewString()
	turnID := uuid.NewString()
	at := time.Unix(1700003622, 0).UTC()
	forkAt := at.Add(30 * time.Second)
	seedSelectedContractExecutionStoreSourceWithoutDelivery(t, db, sourceRunID, entityID, sourceEventID, at)
	seedSelectedContractSourceConversationHistory(t, db, sourceRunID, entityID, sourceEventID, sessionID, auditID, turnID, at)
	sourceRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("validation-coordinator")}
	sourceEvent := commitPostgresDeliveryFixture(t, ctx, db, sourceEventID, sourceRoute)
	claimPostgresDeliveryFixture(t, ctx, db, sourceEvent, sourceRoute)
	seedPostgresChildEventRecordFixture(t, ctx, db, forkPointEventID, sourceRunID, sourceEventID,
		"validation/vertical.ready_for_review", events.EventProducerAgent, "validation-coordinator", entityID, "", []byte(`{}`), forkAt)
	captureRunForkTestRevision(t, db, sourceRunID)
	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          forkPointEventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	seedSelectedContractExecutionForkLineage(t, pg, db, sourceRunID, materialized.ForkRunID, forkPointEventID, entityID, forkAt)

	activation, err := pg.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		ForkRunID:             materialized.ForkRunID,
		AllowedSourceEventIDs: []string{forkPointEventID},
	})
	if err != nil {
		t.Fatalf("ActivateRunForkForSelectedContractExecution: %v", err)
	}
	if !activation.Activated || !activation.SourceAdvancedAfterFork || activation.BranchDivergence == nil {
		t.Fatalf("activation = %#v, want active-coupling branch divergence", activation)
	}
	if !runForkTestHasLineageDispositionOwnerClassification(
		activation.ReplayResumeAdmission,
		runfork.RunForkReplayResumeFactDeliveryInProgressHistory,
		runfork.RunForkSelectedContractActiveSourceDeliveryConversationCouplingPolicyOwner,
		runfork.RunForkSelectedContractActiveSourceDeliveryConversationCouplingClassification,
	) {
		t.Fatalf("activation replay admission missing #678 owner: %#v", activation.ReplayResumeAdmission)
	}
	if !containsString(activation.BranchDivergence.SourceAdvancedFacts, runfork.RunForkSelectedContractActiveSourceDeliveryConversationCouplingClassification) {
		t.Fatalf("branch divergence facts = %#v, want #678 classification", activation.BranchDivergence.SourceAdvancedFacts)
	}
	assertNoCopiedConversationRows(t, db, materialized.ForkRunID, sessionID, auditID, turnID)
}

func TestPostTSourceConversationHistoryActivationKeepsActiveCouplingFailClosed(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	sessionID := uuid.NewString()
	at := time.Unix(1700003623, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	seedPostTActiveConversationCoupling(t, db, sourceRunID, entityID, eventID, sessionID, at)

	activation, err := pg.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		ForkRunID:             materialized.ForkRunID,
		AllowedSourceEventIDs: []string{eventID},
	})
	if err == nil || !strings.Contains(err.Error(), "source_active_conversation_session_coupling_after_fork_point") {
		t.Fatalf("activation error = %v, want active post-T conversation coupling blocker", err)
	}
	if activation.Activated || activation.SourceAdvancedAfterFork || activation.BranchDivergence != nil {
		t.Fatalf("activation = %#v, want active coupling blocked before branch divergence", activation)
	}
	if !runForkTestHasActivationBlocker(activation, "source_active_conversation_session_coupling_after_fork_point") {
		t.Fatalf("activation blockers = %#v, want active post-T coupling blocker", activation.UnsupportedBlockers)
	}
}

func TestSelectedContractActivationAllowsFreshForkConversationRows(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700003627, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	forkEventID := seedSelectedContractExecutionForkLineage(t, pg, db, sourceRunID, materialized.ForkRunID, eventID, entityID, at)
	sessionID := uuid.NewString()
	identity := testAgentIdentity(t, "agent-a", "flow-a/1")
	fields := testAgentIdentityStorageFields(t, identity)
	seedTestAgentRow(t, ctx, db, true, identity, "active")
	forkRoute := testAgentDeliveryRoute(t, "agent-a", "flow-a/1")
	forkEvent := commitPostgresDeliveryFixture(t, ctx, db, forkEventID, forkRoute)
	forkClaim := claimPostgresDeliveryFixture(t, ctx, db, forkEvent, forkRoute)
	if _, err := pg.SettleSuccess(ctx, forkClaim.Claim, nil, time.Second, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
		t.Fatalf("settle selected agent delivery: %v", err)
	}
	followUp := eventtest.PersistedChildForProducer(
		uuid.NewString(),
		events.EventType("agent.follow_up"),
		eventtest.Producer(events.EventProducerAgent, "agent-a"),
		"",
		[]byte(`{}`),
		0,
		materialized.ForkRunID,
		forkEventID,
		events.EventEnvelope{
			EntityID:     entityID,
			FlowInstance: "flow-a/1",
			Scope:        events.EventScopeEntity,
			Source:       events.RouteIdentity{FlowID: "flow-a", FlowInstance: "flow-a/1", EntityID: entityID},
		},
		at.Add(4*time.Second),
	)
	if err := insertPostgresCanonicalEventRecordFixture(ctx, db, followUp); err != nil {
		t.Fatalf("seed fork-local agent follow-up: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			memory_enabled, memory_source,
			conversation, turn_count, runtime_state, status, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored',
			'[]'::jsonb, 0, '{}'::jsonb, 'active', $10, $10
		)
	`, sessionID, materialized.ForkRunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		at.Add(2*time.Second)); err != nil {
		t.Fatalf("seed fork session: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			entity_id, memory_enabled, memory_source,
			conversation, turn_count, runtime_state, status, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10::uuid, FALSE, 'authored',
			'[]'::jsonb, 1, '{}'::jsonb, 'active', $11, $11
		)
	`, uuid.NewString(), materialized.ForkRunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		entityID, at.Add(2*time.Second)); err != nil {
		t.Fatalf("seed fork conversation audit: %v", err)
	}
	turnID := uuid.NewString()
	turnEvent := loadPostgresDeliveryFixtureEvent(t, ctx, db, followUp.ID())
	turnRoute := testAgentDeliveryRoute(t, identity.AgentID(), identity.FlowInstance())
	if err := commitDeliveryObligationFixture(ctx, pg, turnEvent, turnRoute); err != nil {
		t.Fatalf("commit fork turn origin: %v", err)
	}
	turnClaim, err := claimDeliveryFixture(ctx, pg, turnEvent, turnRoute)
	if err != nil {
		t.Fatalf("claim fork turn origin: %v", err)
	}
	if err := persistManagedAgentTurnReadbackFixtureWithOptions(t, runtimedelivery.WithClaim(ctx, turnClaim.Claim), pg, runtimellm.AgentTurnRecord{
		AgentID: identity.AgentID(), Identity: agentmemory.Identity{RunID: materialized.ForkRunID, Agent: identity},
		RunID: materialized.ForkRunID, FlowInstance: identity.FlowInstance(), Memory: agentmemory.Authored(true), SessionID: sessionID,
		EntityID: entityID, TriggerEventID: turnEvent.ID(), TriggerEventType: string(turnEvent.Type()), ParseOK: true, Latency: time.Millisecond,
	}, managedAgentTurnFixtureOptions{TurnID: turnID, Now: at.Add(3 * time.Second), OriginEvent: &turnEvent}); err != nil {
		t.Fatalf("seed fork turn: %v", err)
	}
	if _, err := pg.SettleSuccess(ctx, turnClaim.Claim, nil, time.Millisecond); err != nil {
		t.Fatalf("settle fork turn origin: %v", err)
	}

	activation, err := pg.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		ForkRunID:             materialized.ForkRunID,
		ConfirmSourceFreeze:   true,
		AllowedSourceEventIDs: []string{eventID},
	})
	if err != nil {
		t.Fatalf("ActivateRunForkForSelectedContractExecution: %v", err)
	}
	if !activation.Activated {
		t.Fatalf("activation = %#v, want activated", activation)
	}
}

func TestSelectedContractActivationAllowsCausalForkLocalRuntimePlatformControlEvent(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700003630, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	forkEventID := seedSelectedContractExecutionForkLineage(t, pg, db, sourceRunID, materialized.ForkRunID, eventID, entityID, at)
	forkRoute := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient("agent-a")}
	forkEvent := commitPostgresDeliveryFixture(t, ctx, db, forkEventID, forkRoute)
	forkClaim := claimPostgresDeliveryFixture(t, ctx, db, forkEvent, forkRoute)
	if _, err := pg.SettleSuccess(ctx, forkClaim.Claim, nil, time.Second, runtimedelivery.NotApplicableHandlerRuleSelection()); err != nil {
		t.Fatalf("settle selected agent delivery: %v", err)
	}
	seedPostgresChildEventRecordFixture(t, ctx, db, uuid.NewString(), materialized.ForkRunID, forkEventID,
		"platform.auth_required", events.EventProducerPlatform, "runtime", entityID, "flow-a/1", []byte(`{}`), at.Add(3*time.Second))

	activation, err := pg.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		ForkRunID:             materialized.ForkRunID,
		ConfirmSourceFreeze:   true,
		AllowedSourceEventIDs: []string{eventID},
	})
	if err != nil {
		t.Fatalf("ActivateRunForkForSelectedContractExecution: %v", err)
	}
	if !activation.Activated {
		t.Fatalf("activation = %#v, want activated", activation)
	}
}

func TestSelectedContractActivationAllowsCausalForkLocalRuntimeLogDiagnostic(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700003631, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	forkEventID := seedSelectedContractExecutionForkLineage(t, pg, db, sourceRunID, materialized.ForkRunID, eventID, entityID, at)
	seedPostgresRuntimeLogEventRecordFixture(t, ctx, pg, uuid.NewString(), materialized.ForkRunID, forkEventID,
		[]byte(`{"log_level":"warn","message":"selected-fork diagnostic","details":{"component":"eventbus","action":"outbox_replay_scope_unavailable"}}`), at.Add(3*time.Second))

	activation, err := pg.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		ForkRunID:             materialized.ForkRunID,
		ConfirmSourceFreeze:   true,
		AllowedSourceEventIDs: []string{eventID},
	})
	if err != nil {
		t.Fatalf("ActivateRunForkForSelectedContractExecution: %v", err)
	}
	if !activation.Activated {
		t.Fatalf("activation = %#v, want activated", activation)
	}
}

func TestSelectedContractActivationRejectsUncausedForkLocalRuntimePlatformControlEvent(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700003632, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	seedSelectedContractExecutionForkLineage(t, pg, db, sourceRunID, materialized.ForkRunID, eventID, entityID, at)
	seedPostgresSemanticEventRecordFixture(t, ctx, db, uuid.NewString(), materialized.ForkRunID, "platform.auth_required",
		events.EventProducerPlatform, "runtime", entityID, "flow-a/1", at.Add(3*time.Second))

	activation, err := pg.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		ForkRunID:             materialized.ForkRunID,
		AllowedSourceEventIDs: []string{eventID},
	})
	if err == nil || !strings.Contains(err.Error(), "fork_events_not_selected_contract_lineage") {
		t.Fatalf("activation error = %v, want fork event lineage blocker", err)
	}
	if activation.Activated {
		t.Fatalf("activation = %#v, want blocked", activation)
	}
}

func TestSelectedContractActivationRejectsUncausedForkLocalRuntimeLogDiagnostic(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700003633, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	seedSelectedContractExecutionForkLineage(t, pg, db, sourceRunID, materialized.ForkRunID, eventID, entityID, at)
	seedPostgresRuntimeLogEventRecordFixture(t, ctx, pg, uuid.NewString(), materialized.ForkRunID, "",
		[]byte(`{"log_level":"warn","message":"uncorrelated diagnostic","details":{"component":"eventbus","action":"outbox_replay_scope_unavailable"}}`), at.Add(3*time.Second))

	activation, err := pg.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		ForkRunID:             materialized.ForkRunID,
		AllowedSourceEventIDs: []string{eventID},
	})
	if err == nil || !strings.Contains(err.Error(), "fork_events_not_selected_contract_lineage") {
		t.Fatalf("activation error = %v, want fork event lineage blocker", err)
	}
	if activation.Activated {
		t.Fatalf("activation = %#v, want blocked", activation)
	}
}

func TestSelectedContractActivationRejectsUncausedForkLocalToolExecutorRuntimeLogDiagnostic(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700003635, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	seedSelectedContractExecutionForkLineage(t, pg, db, sourceRunID, materialized.ForkRunID, eventID, entityID, at)
	seedPostgresRuntimeLogEventRecordFixture(t, ctx, pg, uuid.NewString(), materialized.ForkRunID, "",
		[]byte(`{"log_level":"info","message":"Tool read_validation_case_business_brief executed successfully","details":{"component":"tool-executor","action":"tool_execution_succeeded","tool_name":"read_validation_case_business_brief"}}`), at.Add(3*time.Second))

	activation, err := pg.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		ForkRunID:             materialized.ForkRunID,
		AllowedSourceEventIDs: []string{eventID},
	})
	if err == nil || !strings.Contains(err.Error(), "fork_events_not_selected_contract_lineage") {
		t.Fatalf("activation error = %v, want fork event lineage blocker", err)
	}
	if activation.Activated {
		t.Fatalf("activation = %#v, want blocked", activation)
	}
}

func TestSelectedContractActivationRejectsUnownedPlatformEventWithSelectedParent(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700003634, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	forkEventID := seedSelectedContractExecutionForkLineage(t, pg, db, sourceRunID, materialized.ForkRunID, eventID, entityID, at)
	seedPostgresChildEventRecordFixture(t, ctx, db, uuid.NewString(), materialized.ForkRunID, forkEventID,
		"platform.reset", events.EventProducerPlatform, "runtime", entityID, "flow-a/1", []byte(`{}`), at.Add(3*time.Second))

	activation, err := pg.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		ForkRunID:             materialized.ForkRunID,
		AllowedSourceEventIDs: []string{eventID},
	})
	if err == nil || !strings.Contains(err.Error(), "fork_events_not_selected_contract_lineage") {
		t.Fatalf("activation error = %v, want fork event lineage blocker", err)
	}
	if activation.Activated {
		t.Fatalf("activation = %#v, want blocked", activation)
	}
}

func TestPostTSourceReplayScopeMarkerFailsClosedForSelectedContractActivation(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := "00000000-0000-0000-0000-000000000001"
	afterEventID := "00000000-0000-0000-0000-000000000002"
	at := time.Unix(1700003626, 0).UTC()
	seedSelectedContractExecutionStoreSource(t, db, sourceRunID, entityID, eventID, at)

	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	if materialized.ForkRunID == "" {
		t.Fatalf("materialized fork run_id is empty: %#v", materialized)
	}
	seedSelectedContractPostForkSourceEvent(t, db, sourceRunID, afterEventID, entityID, at)
	seedSelectedContractSourceReplayScopeMarker(t, db, sourceRunID, afterEventID, replayScopeReasonDirect, at.Add(-time.Second))
	captureRunForkTestRevision(t, db, sourceRunID)

	activation, err := pg.ActivateRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionActivateRequest{
		ForkRunID:             materialized.ForkRunID,
		AllowedSourceEventIDs: []string{eventID},
	})
	if err == nil || !strings.Contains(err.Error(), "source_committed_replay_scope_advanced_after_fork_point") {
		t.Fatalf("activation error = %v, want post-T committed replay-scope marker blocker", err)
	}
	if activation.Activated || activation.BranchDivergence != nil || activation.SourceAdvancedAfterFork {
		t.Fatalf("activation = %#v, want marker post-T blocked before branch divergence", activation)
	}
	if !runForkTestHasDispositionBlocker(activation.ReplayResumeAdmission, runfork.RunForkReplayResumeFactSourceAdvanced, "source_committed_replay_scope_advanced_after_fork_point") {
		t.Fatalf("activation replay admission = %#v, want source advanced marker blocker", activation.ReplayResumeAdmission)
	}

	var sourceStatus, forkStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status: %v", err)
	}
	if sourceStatus != "running" || forkStatus != runfork.RunForkMaterializedStatus {
		t.Fatalf("run statuses source=%q fork=%q, want source running and fork materialized", sourceStatus, forkStatus)
	}
	assertNoCopiedReplayScopeMarkers(t, db, materialized.ForkRunID)
}

func TestSelectedContractExecutionMaterializationPreservesUnversionedRouteBlocker(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := admitTestPostgresStore(t, db)
	ctx := testAuthorActivityContext()
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700002525, 0).UTC()
	seedSelectedContractExecutionStoreSourceUnpublished(t, db, sourceRunID, entityID, eventID, at)
	if _, err := db.ExecContext(ctx, `
		UPDATE events
		SET flow_instance = 'flow-a/1'
		WHERE run_id = $1::uuid AND event_id = $2::uuid
	`, sourceRunID, eventID); err != nil {
		t.Fatalf("seed selected event route identity: %v", err)
	}
	captureRunForkTestRevision(t, db, sourceRunID)
	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID,
		At:          eventID,
		ContractSelection: runfork.RunForkContractSelection{
			Mode:            "selected_contracts",
			ContractsRoot:   "/tmp/selected-contracts",
			WorkflowName:    "selected-workflow",
			WorkflowVersion: "v1",
		},
	})
	blocker, fact, ok := runForkReplayResumeBlockerFromError(err)
	if err == nil || !ok || blocker.Code != runfork.RunForkBlockerFlowRouteHistoryUnproven || fact != runfork.RunForkReplayResumeFactRouteHistory {
		t.Fatalf("materialization error = %v blocker=%#v fact=%q, want typed route blocker", err, blocker, fact)
	}
	if materialized.ForkRunID != "" {
		t.Fatalf("materialized fork despite route blocker: %#v", materialized)
	}
	assertNoSelectedContractForkRows(t, db, sourceRunID)
}

func seedSelectedContractExecutionStoreSource(t *testing.T, db *sql.DB, sourceRunID, entityID, eventID string, at time.Time) {
	t.Helper()
	seedSelectedContractExecutionStoreSourceUnpublished(t, db, sourceRunID, entityID, eventID, at)
	captureRunForkTestRevision(t, db, sourceRunID)
}

func seedSelectedContractExecutionStoreSourceUnpublished(t *testing.T, db *sql.DB, sourceRunID, entityID, eventID string, at time.Time) {
	t.Helper()
	seedSelectedContractExecutionStoreSourceRaw(t, db, sourceRunID, entityID, eventID, at, []events.DeliveryRoute{testEntitylessNodeDeliveryRoute("test-node")})
}

func seedSelectedContractExecutionStoreSourceWithoutDelivery(t *testing.T, db *sql.DB, sourceRunID, entityID, eventID string, at time.Time) {
	t.Helper()
	seedSelectedContractExecutionStoreSourceRaw(t, db, sourceRunID, entityID, eventID, at, nil)
	captureRunForkTestRevision(t, db, sourceRunID)
}

func seedSelectedContractExecutionStoreSourceRaw(t *testing.T, db *sql.DB, sourceRunID, entityID, eventID string, at time.Time, routes []events.DeliveryRoute) {
	seedSelectedContractExecutionStoreSourceRawWithPayload(t, db, sourceRunID, entityID, eventID, at, routes, []byte(`{}`))
}

func seedSelectedContractExecutionStoreSourceRawWithPayload(t *testing.T, db *sql.DB, sourceRunID, entityID, eventID string, at time.Time, routes []events.DeliveryRoute, payload []byte) {
	t.Helper()
	ctx := testAuthorActivityContext()
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: sourceRunID, StartedAt: at.Add(-time.Minute), BundleHash: authorActivityTestBundleHash, BundleSource: storerunlifecycle.BundleSourceEphemeral})
	selected := newPostgresStoreWithBackend(mustPostgresBackend(db))
	selected.acceptCurrentSchemaForTest()
	event := semanticEventRecordFixture(
		eventID, sourceRunID, "item.received", eventtest.Producer(events.EventProducerPlatform, "test"), payload,
		semanticEventRecordFixtureEnvelope(entityID, ""), at,
	)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin source fact transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	story, err := privateauthoractivity.Begin(ctx, tx, privateauthoractivity.DialectPostgres)
	if err != nil {
		t.Fatalf("begin source story transaction: %v", err)
	}
	if err := commitSemanticEventFixtureWithRoutesStoryTx(ctx, selected, tx, story, event, routes); err != nil {
		t.Fatalf("seed source event and delivery obligations: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, domain, path, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'lifecycle_state', '', 'null'::jsonb, '"pending"'::jsonb, $3::uuid, 'platform', 'selected-store-test', 'seed', $4),
			($1::uuid, $2::uuid, 'authored_field', 'name', 'null'::jsonb, '"Selected Store Entity"'::jsonb, $3::uuid, 'platform', 'selected-store-test', 'seed', $4)
	`, sourceRunID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'Selected Store Entity',
			'pending', '{}'::jsonb, '{"name":"Selected Store Entity"}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, sourceRunID, entityID, at); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}
	if err := story.Finalize(ctx); err != nil {
		t.Fatalf("finalize source story transaction: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit source facts: %v", err)
	}
}

func seedSelectedContractSourceConversationHistory(t *testing.T, db *sql.DB, sourceRunID, entityID, eventID, sessionID, auditID, turnID string, at time.Time) {
	seedSelectedContractSourceConversationHistoryWithDelivery(t, db, sourceRunID, entityID, eventID, sessionID, auditID, turnID, at, false)
}

func seedSelectedContractSourceConversationHistoryWithActiveDelivery(t *testing.T, db *sql.DB, sourceRunID, entityID, eventID, sessionID, auditID, turnID string, at time.Time) runtimedelivery.Claim {
	return seedSelectedContractSourceConversationHistoryWithDelivery(t, db, sourceRunID, entityID, eventID, sessionID, auditID, turnID, at, true)
}

func seedSelectedContractSourceConversationHistoryWithDelivery(t *testing.T, db *sql.DB, sourceRunID, entityID, eventID, sessionID, auditID, turnID string, at time.Time, keepDelivery bool) runtimedelivery.Claim {
	t.Helper()
	ctx := testAuthorActivityContext()
	pg := admitTestPostgresStore(t, db)
	identity := testAgentIdentity(t, "agent-a", "flow-a/1")
	fields := testAgentIdentityStorageFields(t, identity)
	seedTestAgentRow(t, ctx, db, true, identity, "active")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			memory_enabled, memory_source,
			status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored',
			'active', $10, $10)
	`, sessionID, sourceRunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, at); err != nil {
		t.Fatalf("seed source session: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			entity_id, memory_enabled, memory_source,
			runtime_state, status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10::uuid, FALSE, 'authored',
			'{}'::jsonb, 'active', $11, $11)
	`, auditID, sourceRunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		entityID, at); err != nil {
		t.Fatalf("seed source conversation audit: %v", err)
	}
	event := loadPostgresDeliveryFixtureEvent(t, ctx, db, eventID)
	route := testAgentDeliveryRoute(t, identity.AgentID(), identity.FlowInstance())
	if err := commitDeliveryObligationFixture(ctx, pg, event, route); err != nil {
		t.Fatalf("commit source turn origin: %v", err)
	}
	claimed, err := claimDeliveryFixture(ctx, pg, event, route)
	if err != nil {
		t.Fatalf("claim source turn origin: %v", err)
	}
	if err := persistManagedAgentTurnReadbackFixtureWithOptions(t, runtimedelivery.WithClaim(ctx, claimed.Claim), pg, runtimellm.AgentTurnRecord{
		AgentID: identity.AgentID(), Identity: agentmemory.Identity{RunID: sourceRunID, Agent: identity},
		RunID: sourceRunID, FlowInstance: identity.FlowInstance(), Memory: agentmemory.Authored(true), SessionID: sessionID,
		EntityID: entityID, TriggerEventID: eventID, TriggerEventType: string(event.Type()), TaskID: "task-a", ParseOK: true,
	}, managedAgentTurnFixtureOptions{TurnID: turnID, Now: at, OriginEvent: &event}); err != nil {
		t.Fatalf("seed source turn: %v", err)
	}
	if !keepDelivery {
		if _, err := pg.SettleSuccess(ctx, claimed.Claim, nil, time.Millisecond); err != nil {
			t.Fatalf("settle source turn origin: %v", err)
		}
	}
	return claimed.Claim
}

func seedSelectedContractSourceReplayScopeMarker(t *testing.T, db execContextDB, sourceRunID, eventID, reasonCode string, at time.Time) {
	t.Helper()
	scope, ok := committedReplayScopeFromReasonCode(reasonCode)
	if !ok {
		t.Fatalf("invalid replay scope reason %q", reasonCode)
	}
	if _, err := db.ExecContext(testAuthorActivityContext(), `
		INSERT INTO committed_replay_scopes (event_id, run_id, scope, created_at, updated_at)
		VALUES ($1::uuid, $2::uuid, $3, $4, $4)
	`, eventID, sourceRunID, string(scope), at); err != nil {
		t.Fatalf("seed source committed replay scope: %v", err)
	}
}

func seedSelectedContractPostForkSourceEvent(t *testing.T, db *sql.DB, sourceRunID, eventID, entityID string, at time.Time) {
	t.Helper()
	seedPostgresSemanticEventRecordFixture(t, testAuthorActivityContext(), db, eventID, sourceRunID, "source.after",
		events.EventProducerPlatform, "source-runtime", entityID, "flow-a/1", at)
}

func seedPostTActiveConversationCoupling(t *testing.T, db *sql.DB, sourceRunID, entityID, eventID, sessionID string, at time.Time) {
	t.Helper()
	ctx := testAuthorActivityContext()
	identity := testAgentIdentity(t, "active-agent", "flow-a/1")
	fields := testAgentIdentityStorageFields(t, identity)
	seedTestAgentRow(t, ctx, db, true, identity, "active")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			memory_enabled, memory_source,
			status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, TRUE, 'authored',
			'active', $10, $10)
	`, sessionID, sourceRunID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath,
		at.Add(time.Minute)); err != nil {
		t.Fatalf("seed post-T active source session: %v", err)
	}
	activeRoute := testAgentDeliveryRoute(t, "active-agent", "flow-a/1")
	event := commitPostgresDeliveryFixture(t, ctx, db, eventID, activeRoute)
	claimed := claimPostgresDeliveryFixture(t, ctx, db, event, activeRoute)
	if _, err := postgresDeliveryFixtureStore(db).BindAgentSession(ctx, claimed.Claim, sessionID); err != nil {
		t.Fatalf("bind post-T active source delivery: %v", err)
	}
	captureRunForkTestRevision(t, db, sourceRunID)
}

func assertNoCopiedConversationRows(t *testing.T, db *sql.DB, forkRunID, sourceSessionID, sourceAuditID, sourceTurnID string) {
	t.Helper()
	ctx := testAuthorActivityContext()
	checks := map[string]struct {
		query string
		id    string
	}{
		"agent_sessions": {
			query: `SELECT COUNT(*) FROM agent_sessions WHERE run_id = $1::uuid OR session_id = $2::uuid`,
			id:    sourceSessionID,
		},
		"agent_conversation_audits": {
			query: `SELECT COUNT(*) FROM agent_conversation_audits WHERE run_id = $1::uuid OR session_id = $2::uuid`,
			id:    sourceAuditID,
		},
		"agent_turns": {
			query: `SELECT COUNT(*) FROM agent_turns WHERE run_id = $1::uuid OR turn_id = $2::uuid`,
			id:    sourceTurnID,
		},
	}
	for name, check := range checks {
		var count int
		if err := db.QueryRowContext(ctx, check.query, forkRunID, check.id).Scan(&count); err != nil {
			t.Fatalf("count copied %s: %v", name, err)
		}
		if count != 1 {
			t.Fatalf("%s fork/copy count = %d, want exactly the original source row only", name, count)
		}
	}
}

func assertNoForkConversationRows(t *testing.T, db *sql.DB, forkRunID string) {
	t.Helper()
	ctx := testAuthorActivityContext()
	checks := map[string]string{
		"agent_sessions":            `SELECT COUNT(*) FROM agent_sessions WHERE run_id = $1::uuid`,
		"agent_conversation_audits": `SELECT COUNT(*) FROM agent_conversation_audits WHERE run_id = $1::uuid`,
		"agent_turns":               `SELECT COUNT(*) FROM agent_turns WHERE run_id = $1::uuid`,
	}
	for name, query := range checks {
		var count int
		if err := db.QueryRowContext(ctx, query, forkRunID).Scan(&count); err != nil {
			t.Fatalf("count fork %s rows: %v", name, err)
		}
		if count != 0 {
			t.Fatalf("fork %s rows = %d, want 0 copied source conversation rows", name, count)
		}
	}
}

func assertNoCopiedReplayScopeMarkers(t *testing.T, db *sql.DB, forkRunID string) {
	t.Helper()
	var copied int
	if err := db.QueryRowContext(testAuthorActivityContext(), `
		SELECT COUNT(*)
		FROM committed_replay_scopes
		WHERE run_id = $1::uuid
	`, forkRunID).Scan(&copied); err != nil {
		t.Fatalf("count copied committed replay scopes: %v", err)
	}
	if copied != 0 {
		t.Fatalf("fork replay-scope marker rows = %d, want 0 copied source markers", copied)
	}
}

func assertOnlySelectedForkReplayScopeMarker(t *testing.T, db *sql.DB, forkRunID, forkEventID string) {
	t.Helper()
	var exact, copied int
	if err := db.QueryRowContext(testAuthorActivityContext(), `
		SELECT
			COUNT(*) FILTER (WHERE event_id = $2::uuid AND scope = 'direct'),
			COUNT(*) FILTER (WHERE event_id <> $2::uuid)
		FROM committed_replay_scopes
		WHERE run_id = $1::uuid
	`, forkRunID, forkEventID).Scan(&exact, &copied); err != nil {
		t.Fatalf("count selected-fork committed replay scopes: %v", err)
	}
	if exact != 1 || copied != 0 {
		t.Fatalf("selected-fork replay-scope markers exact=%d copied=%d, want exact=1 copied=0", exact, copied)
	}
}

func runForkTestHasPlanBlocker(plan runfork.RunForkPlan, code string) bool {
	for _, blocker := range plan.UnsupportedBlockers {
		if blocker.Code == code {
			return true
		}
	}
	return false
}

func runForkTestHasMaterializationBlocker(materialized runfork.RunForkMaterialization, code string) bool {
	for _, blocker := range materialized.UnsupportedBlockers {
		if blocker.Code == code {
			return true
		}
	}
	return false
}

func runForkTestHasLineageDispositionOwner(admission runfork.RunForkReplayResumeAdmission, fact, owner string) bool {
	for _, disposition := range admission.Dispositions {
		if disposition.Fact == fact &&
			disposition.Disposition == runfork.RunForkReplayResumeDispositionLineageOnly &&
			disposition.Owner == owner {
			return true
		}
	}
	return false
}

func runForkTestHasLineageDispositionOwnerClassification(admission runfork.RunForkReplayResumeAdmission, fact, owner, classification string) bool {
	for _, disposition := range admission.Dispositions {
		if disposition.Fact == fact &&
			disposition.Disposition == runfork.RunForkReplayResumeDispositionLineageOnly &&
			disposition.Owner == owner &&
			disposition.Classification == classification {
			return true
		}
	}
	return false
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func assertNoForkTimerCopiesForSource(t *testing.T, db *sql.DB, sourceRunID string) {
	t.Helper()
	var copied int
	if err := db.QueryRowContext(testAuthorActivityContext(), `
		SELECT COUNT(*)
		FROM timers
		WHERE forked_from_run_id = $1::uuid
		   OR source_timer_id IN (
				SELECT timer_id
				FROM timers
				WHERE run_id = $1::uuid
		   )
	`, sourceRunID).Scan(&copied); err != nil {
		t.Fatalf("count fork timer copies: %v", err)
	}
	if copied != 0 {
		t.Fatalf("fork timer copies for source run = %d, want 0", copied)
	}
}

func assertNoSelectedContractForkRows(t *testing.T, db *sql.DB, sourceRunID string) {
	t.Helper()
	ctx := testAuthorActivityContext()
	for name, query := range map[string]string{
		"runs": `
			SELECT COUNT(*)
			FROM runs
			WHERE forked_from_run_id = $1::uuid
		`,
		"selected_contract_bindings": `
			SELECT COUNT(*)
			FROM run_fork_selected_contract_bindings
			WHERE source_run_id = $1::uuid
		`,
		"selected_contract_executions": `
			SELECT COUNT(*)
			FROM run_fork_selected_contract_executions
			WHERE source_run_id = $1::uuid
		`,
		"selected_contract_branch_divergences": `
			SELECT COUNT(*)
			FROM run_fork_selected_contract_branch_divergences
			WHERE source_run_id = $1::uuid
		`,
	} {
		var count int
		if err := db.QueryRowContext(ctx, query, sourceRunID).Scan(&count); err != nil {
			t.Fatalf("count %s rows: %v", name, err)
		}
		if count != 0 {
			t.Fatalf("%s rows for blocked selected-contract fork = %d, want 0", name, count)
		}
	}
}

func seedSelectedContractExecutionForkLineage(t *testing.T, pg *PostgresStore, db execContextDB, sourceRunID, forkRunID, sourceEventID, entityID string, at time.Time) string {
	t.Helper()
	ctx := testAuthorActivityContext()
	forkEventID := uuid.NewString()
	lineage, err := events.NewSelectedForkLineage(
		forkRunID, sourceRunID, sourceEventID, runfork.RunForkSelectedContractExecutionOwner, "", "live",
	)
	if err != nil {
		t.Fatalf("construct selected fork lineage: %v", err)
	}
	constructed := eventtest.SelectedForkReplay(
		forkEventID, events.EventType("item.received"),
		eventtest.Producer(events.EventProducerPlatform, runfork.RunForkSelectedContractExecutionOwner), "", []byte(`{}`), 0,
		lineage, events.EventEnvelope{EntityID: entityID, FlowInstance: "flow-a/1", Scope: events.EventScopeEntity}, at.Add(time.Second),
	)
	if err := commitSelectedForkEventFixture(ctx, pg, constructed, runfork.RunForkSelectedContractExecutionLineage{
		ForkRunID: forkRunID, SourceRunID: sourceRunID, SourceEventID: sourceEventID,
		ForkEventID: forkEventID, EventName: "item.received",
		SelectionAuthority: runfork.RunForkSelectedContractExecutionOwner, CreatedAt: at.Add(time.Second),
	}); err != nil {
		t.Fatalf("commit selected fork event: %v", err)
	}
	return forkEventID
}
