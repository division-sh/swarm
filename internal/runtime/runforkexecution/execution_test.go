package runforkexecution

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimemcp "github.com/division-sh/swarm/internal/runtime/mcp"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestExecuteSelectedContractRunForkWritesForkLocalExecutionAndLineage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002200, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at)
	if _, err := db.ExecContext(ctx, `
		UPDATE event_deliveries
		SET subscriber_id = 'source-only-node'
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
		  AND subscriber_type = 'node'
	`, sourceRunID, sourceEventID); err != nil {
		t.Fatalf("stamp source-only delivery identity: %v", err)
	}
	seedSourceOutcomeThatMustNotSuppressFork(t, db, sourceEventID, entityID, at)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:  sourceRunID,
		At:           sourceEventID,
		Store:        pg,
		SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.Owner != store.RunForkSelectedContractExecutionOwner || result.ExecutedEventCount != 1 || len(result.ForkEvents) != 1 {
		t.Fatalf("result = %#v", result)
	}
	assertSelectedContractRuntimeContainerProof(t,
		result.ForkLocalRuntimeContainer,
		store.RunForkSelectedContractExecutionOwner,
		sourceRunID,
		result.Materialization.ForkRunID,
		sourceEventID,
		[]string{sourceEventID},
	)
	if result.SelectedContractExecutionAdmission.RecipientPlanning == nil ||
		result.SelectedContractExecutionAdmission.RecipientPlanning.Owner != store.RunForkSelectedContractRecipientPlanningOwner ||
		!result.SelectedContractExecutionAdmission.RecipientPlanning.RecipientPlanningSupported ||
		len(result.SelectedContractExecutionAdmission.RecipientPlanning.RecipientPlanEvents) != 1 {
		t.Fatalf("recipient planning admission = %#v", result.SelectedContractExecutionAdmission.RecipientPlanning)
	}
	forkEventID := result.ForkEvents[0].ForkEventID
	if forkEventID == "" || forkEventID == sourceEventID {
		t.Fatalf("fork event id = %q, source = %q", forkEventID, sourceEventID)
	}

	var forkEventRun, forkEventName, forkSourceEvent string
	if err := db.QueryRowContext(ctx, `
		SELECT run_id::text, event_name, COALESCE(source_event_id::text, '')
		FROM events
		WHERE event_id = $1::uuid
	`, forkEventID).Scan(&forkEventRun, &forkEventName, &forkSourceEvent); err != nil {
		t.Fatalf("load fork event: %v", err)
	}
	if forkEventRun != result.Materialization.ForkRunID || forkEventName != "item.received" {
		t.Fatalf("fork event = run:%s name:%s", forkEventRun, forkEventName)
	}
	if forkSourceEvent == sourceEventID {
		t.Fatalf("fork event source_event_id copied source event %s; lineage must be explicit table evidence", sourceEventID)
	}

	var lineageCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_executions
		WHERE fork_run_id = $1::uuid
		  AND source_run_id = $2::uuid
		  AND source_event_id = $3::uuid
		  AND fork_event_id = $4::uuid
	`, result.Materialization.ForkRunID, sourceRunID, sourceEventID, forkEventID).Scan(&lineageCount); err != nil {
		t.Fatalf("count selected execution lineage: %v", err)
	}
	if lineageCount != 1 {
		t.Fatalf("selected execution lineage rows = %d, want 1", lineageCount)
	}
	routeRecovery, ok, err := pg.LoadRunForkSelectedContractRouteRecovery(ctx, result.Materialization.ForkRunID)
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractRouteRecovery: %v", err)
	}
	if !ok {
		t.Fatal("selected-contract route recovery row missing")
	}
	if routeRecovery.Owner != store.RunForkSelectedContractRoutePersistenceOwner ||
		routeRecovery.RuntimeRecoveryOwner != store.RunForkSelectedContractRouteRecoveryOwner ||
		routeRecovery.RouteTopologyOwner != store.RunForkSelectedContractRouteTopologyOwner ||
		routeRecovery.RecipientPlanningOwner != store.RunForkSelectedContractRecipientPlanningOwner ||
		routeRecovery.ForkRunID != result.Materialization.ForkRunID ||
		routeRecovery.RecipientPlanEventCount != 1 ||
		routeRecovery.FrontierEvidenceFingerprint == "" ||
		routeRecovery.RouteTopologyFingerprint == "" ||
		routeRecovery.RecipientPlanningFingerprint == "" {
		t.Fatalf("route recovery = %#v", routeRecovery)
	}
	recoveredRoutes, err := RecoverSelectedContractRouteTruth(ctx, pg)
	if err != nil {
		t.Fatalf("RecoverSelectedContractRouteTruth: %v", err)
	}
	if len(recoveredRoutes) != 1 || recoveredRoutes[0].ForkRunID != result.Materialization.ForkRunID {
		t.Fatalf("recovered route truth = %#v", recoveredRoutes)
	}

	var copiedCurrentRoutes int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM routing_rules
		WHERE flow_instance = 'flow-a/1'
		  AND is_materialized = true
	`).Scan(&copiedCurrentRoutes); err != nil {
		t.Fatalf("count current route rows: %v", err)
	}
	if copiedCurrentRoutes != 0 {
		t.Fatalf("selected route recovery copied current routing_rules rows = %d, want 0", copiedCurrentRoutes)
	}

	var sourceCopiedEvents int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
	`, result.Materialization.ForkRunID, sourceEventID).Scan(&sourceCopiedEvents); err != nil {
		t.Fatalf("count copied source event ids: %v", err)
	}
	if sourceCopiedEvents != 0 {
		t.Fatalf("copied source event ids into fork run = %d, want 0", sourceCopiedEvents)
	}

	var forkReceipts, targetNodeDeliveries, sourceNodeDeliveries int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid`, forkEventID).Scan(&forkReceipts); err != nil {
		t.Fatalf("count fork receipts: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'test-node'
	`, result.Materialization.ForkRunID, forkEventID).Scan(&targetNodeDeliveries); err != nil {
		t.Fatalf("count target node fork deliveries: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'source-only-node'
	`, result.Materialization.ForkRunID, forkEventID).Scan(&sourceNodeDeliveries); err != nil {
		t.Fatalf("count source node fork deliveries: %v", err)
	}
	if forkReceipts == 0 || targetNodeDeliveries != 1 || sourceNodeDeliveries != 0 {
		t.Fatalf("fork outcomes = receipts:%d targetNodeDeliveries:%d sourceNodeDeliveries:%d, want target node only", forkReceipts, targetNodeDeliveries, sourceNodeDeliveries)
	}

	var emittedFollowUps int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'item.processed'
		  AND source_event_id = $2::uuid
	`, result.Materialization.ForkRunID, forkEventID).Scan(&emittedFollowUps); err != nil {
		t.Fatalf("count emitted follow-ups: %v", err)
	}
	if emittedFollowUps != 1 {
		t.Fatalf("fork follow-up events = %d, want 1", emittedFollowUps)
	}

	var sourceStatus, forkStatus, forkEntityState string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, result.Materialization.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT current_state FROM entity_state WHERE run_id = $1::uuid AND entity_id = $2::uuid`, result.Materialization.ForkRunID, entityID).Scan(&forkEntityState); err != nil {
		t.Fatalf("load fork entity state: %v", err)
	}
	if sourceStatus != store.RunForkSourceFrozenStatus || forkStatus != store.RunForkActivatedStatus || forkEntityState == "" {
		t.Fatalf("post execution = source:%s fork:%s entity:%s", sourceStatus, forkStatus, forkEntityState)
	}
}

func TestForkMintsFreshSyntheticCarryProjection(t *testing.T) {
	canonicalrouting.Prove(t, canonicalrouting.TemplateCreateMintedKey)
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := canonicalrouting.ExampleRoot(t, canonicalrouting.TemplateCreateMintedKey)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repoRoot)}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          store.RunForkContractSelectionModeSelectedContracts,
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status, started_at) VALUES ($1::uuid, 'running', now())`, sourceRunID); err != nil {
		t.Fatalf("seed source run: %v", err)
	}
	workflowStore := runtimepipeline.NewWorkflowInstanceStore(db)
	var manager *runtimemanager.AgentManager
	sourceBus, err := bus.NewEventBusWithOptions(pg, bus.EventBusOptions{
		ContractBundle: loaded.Source,
		InterceptorProvider: func() []bus.EventInterceptor {
			return nil
		},
		TemplateInstanceActivator: func(ctx context.Context, req runtimepipeline.FlowInstanceActivationRequest) error {
			if manager == nil {
				return errors.New("source lifecycle manager is not initialized")
			}
			return manager.ActivateFlowInstance(ctx, req)
		},
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	manager = runtimemanager.NewAgentManagerWithOptions(sourceBus, nil, runtimemanager.AgentManagerOptions{
		SemanticSource:    loaded.Source,
		WorkflowInstances: workflowStore,
	}, pg)
	t.Cleanup(func() { _ = manager.Shutdown() })
	sourcePipeline := newSelectedContractPipeline(sourceBus, pg, loaded, SelectedContractAgentRuntimeOptions{}, workflowStore, manager.ActivateFlowInstance)

	sourceEventID := uuid.NewString()
	payload := json.RawMessage(`{"candidate":"candidate-1"}`)
	sourceEvent := eventtest.RootIngress(
		sourceEventID,
		events.EventType(loaded.Source.ResolveFlowEventReference("producer", "validation.triggered")),
		sourceRunID,
		"",
		payload,
		0,
		sourceRunID,
		"",
		events.EventEnvelope{},
		time.Now().UTC(),
	)
	sourceCtx := runtimecorrelation.WithRunID(ctx, sourceRunID)
	preflight, err := sourceBus.CheckPublishRecipientPlan(sourceCtx, sourceEvent)
	if err != nil {
		t.Fatalf("plan source create event: %v", err)
	}
	if preflight.TargetFailure != "" || len(preflight.DeliveryRoutes) == 0 {
		t.Fatalf("source root preflight = failure:%s routes:%#v", preflight.TargetFailure, preflight.DeliveryRoutes)
	}
	if err := pg.PersistEventWithDeliveryRouteSetAndScope(sourceCtx, sourceEvent, preflight.DeliveryRoutes, runtimereplayclaim.CommittedReplayScopeSubscribed); err != nil {
		t.Fatalf("persist source fork frontier event: %v", err)
	}
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:  sourceRunID,
		At:           sourceEventID,
		Store:        pg,
		SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if len(result.ForkEvents) != 1 {
		t.Fatalf("fork events = %#v, want one", result.ForkEvents)
	}
	forkEventID := result.ForkEvents[0].ForkEventID
	var forkRequestedEventID string
	if err := db.QueryRowContext(ctx, `
		SELECT event_id::text
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'producer/validation.requested'
	`, result.Materialization.ForkRunID).Scan(&forkRequestedEventID); err != nil {
		t.Fatalf("load fork request event: %v", err)
	}
	forkRoute := requireSyntheticProjectionRoute(t, pg, forkRequestedEventID, "validator-node")
	forkKey := forkRoute.PayloadProjection.Fields()["validation_case_id"]
	if _, err := uuid.Parse(forkKey); err != nil {
		t.Fatalf("fork projection key = %q, want UUID: %v", forkKey, err)
	}

	// Execute the original pending delivery only after the fork committed. The
	// source and fork now traverse the same canonical constructor independently.
	sourceBus.SetInterceptors(sourcePipeline)
	if err := sourceBus.RecoverPersistedPipeline(sourceCtx, sourceEvent, nil); err != nil {
		t.Fatalf("execute source pending delivery: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := sourceBus.WaitForQuiescence(waitCtx); err != nil {
		t.Fatalf("wait for source pending delivery: %v", err)
	}
	var sourceRequestedEventID string
	if err := db.QueryRowContext(ctx, `
		SELECT event_id::text
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'producer/validation.requested'
	`, sourceRunID).Scan(&sourceRequestedEventID); err != nil {
		t.Fatalf("load source request event: %v", err)
	}
	sourceRoute := requireSyntheticProjectionRoute(t, pg, sourceRequestedEventID, "validator-node")
	sourceKey := sourceRoute.PayloadProjection.Fields()["validation_case_id"]
	if _, err := uuid.Parse(sourceKey); err != nil {
		t.Fatalf("source projection key = %q, want UUID: %v", sourceKey, err)
	}
	if forkKey == sourceKey || forkRoute.Target.FlowInstance == sourceRoute.Target.FlowInstance {
		t.Fatalf("fork reused source stamped identity: source=%s/%s fork=%s/%s", sourceKey, sourceRoute.Target.FlowInstance, forkKey, forkRoute.Target.FlowInstance)
	}
	var forkPayloadRaw string
	if err := db.QueryRowContext(ctx, `SELECT payload::text FROM events WHERE event_id = $1::uuid`, forkEventID).Scan(&forkPayloadRaw); err != nil {
		t.Fatalf("load fork event payload: %v", err)
	}
	var forkPayload map[string]any
	if err := json.Unmarshal([]byte(forkPayloadRaw), &forkPayload); err != nil {
		t.Fatalf("decode fork event payload: %v", err)
	}
	if _, exists := forkPayload["validation_case_id"]; exists {
		t.Fatalf("fork event payload was mutated with route projection: %#v", forkPayload)
	}
	var downstreamPayloadRaw string
	if err := db.QueryRowContext(ctx, `
		SELECT payload::text
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name LIKE 'validator/%/validation.started'
	`, result.Materialization.ForkRunID).Scan(&downstreamPayloadRaw); err != nil {
		t.Fatalf("load fork downstream event: %v", err)
	}
	var downstreamPayload map[string]any
	if err := json.Unmarshal([]byte(downstreamPayloadRaw), &downstreamPayload); err != nil {
		t.Fatalf("decode fork downstream event: %v", err)
	}
	if downstreamPayload["validation_case_id"] != forkKey {
		t.Fatalf("fork downstream payload = %#v, want fresh key %s", downstreamPayload, forkKey)
	}
}

func requireSyntheticProjectionRoute(t *testing.T, pg *store.PostgresStore, eventID, subscriberID string) events.DeliveryRoute {
	t.Helper()
	routes, err := pg.ListEventDeliveryRoutes(context.Background(), eventID)
	if err != nil {
		t.Fatalf("ListEventDeliveryRoutes(%s): %v", eventID, err)
	}
	for _, route := range routes {
		if route.SubscriberType == "node" && route.SubscriberID == subscriberID && !route.PayloadProjection.Empty() {
			return route
		}
	}
	t.Fatalf("delivery routes for %s = %#v, want projected node %s", eventID, routes, subscriberID)
	return events.DeliveryRoute{}
}

func TestExecuteSelectedContractRunForkLoadsDBBackedSourceAndStampsPersistedIdentity(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, contractsRoot, platformSpecPath)
	if err != nil {
		t.Fatalf("LoadWorkflowContractBundleWithOverrides: %v", err)
	}
	projection, err := runtimecontracts.BuildBundleCatalogProjection(bundle)
	if err != nil {
		t.Fatalf("BuildBundleCatalogProjection: %v", err)
	}
	if _, err := pg.UpsertBundleCatalog(ctx, store.BundleCatalogUpsert{
		BundleHash:  projection.BundleHash,
		ContentYAML: projection.ContentYAML,
		ParsedJSON:  projection.ParsedJSON,
		DataBlob:    projection.DataBlob,
		Metadata:    projection.Metadata,
	}); err != nil {
		t.Fatalf("UpsertBundleCatalog: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002202, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at)
	if _, err := db.ExecContext(ctx, `
		UPDATE runs
		SET bundle_hash = $2, bundle_source = $3
		WHERE run_id = $1::uuid
	`, sourceRunID, projection.BundleHash, storerunlifecycle.BundleSourcePersisted); err != nil {
		t.Fatalf("stamp source run bundle identity: %v", err)
	}
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:  sourceRunID,
		At:           sourceEventID,
		BundleHash:   projection.BundleHash,
		BundleSource: storerunlifecycle.BundleSourcePersisted,
		Store:        pg,
		SourceLoader: BundleCatalogSelectedContractSourceLoader{
			RepoRoot: repoRoot,
			Store:    pg,
		},
		ContractSelection: runforkadmission.SelectedContractSelection(
			semanticview.Wrap(bundle),
			"/stale/db-loaded/source-root",
		),
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.Owner != store.RunForkSelectedContractExecutionOwner || result.ExecutedEventCount != 1 || len(result.ForkEvents) != 1 {
		t.Fatalf("result = %#v", result)
	}
	assertSelectedContractRuntimeContainerProof(t,
		result.ForkLocalRuntimeContainer,
		store.RunForkSelectedContractExecutionOwner,
		sourceRunID,
		result.Materialization.ForkRunID,
		sourceEventID,
		[]string{sourceEventID},
	)
	var forkBundleHash, forkBundleSource, forkBundleFingerprint string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(bundle_hash, ''), bundle_source, COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, result.Materialization.ForkRunID).Scan(&forkBundleHash, &forkBundleSource, &forkBundleFingerprint); err != nil {
		t.Fatalf("load fork run bundle identity: %v", err)
	}
	if forkBundleHash != projection.BundleHash || forkBundleSource != storerunlifecycle.BundleSourcePersisted || forkBundleFingerprint != "" {
		t.Fatalf("fork run bundle identity = hash:%q source:%q fingerprint:%q", forkBundleHash, forkBundleSource, forkBundleFingerprint)
	}
}

func TestExecuteSelectedContractRunForkFailsClosedBeforeMaterializationForAgentRecipientWithoutHandlerMaterializer(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier7-composition/test-agent-emits-to-node")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002201, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "task.assigned", at)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:  sourceRunID,
		At:           sourceEventID,
		Store:        pg,
		SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err == nil ||
		!strings.Contains(err.Error(), store.RunForkBlockerSelectedContractAgentHandlerMaterializationUnsupported) ||
		!strings.Contains(err.Error(), store.RunForkSelectedContractAuthoritativeAgentDeliveryMaterializationOwner) ||
		!strings.Contains(err.Error(), "test-agent") {
		t.Fatalf("ExecuteSelectedContractRunFork error = %v, want selected agent materialization blocker for test-agent", err)
	}
	if result.Owner != store.RunForkSelectedContractExecutionOwner ||
		result.Materialization.ForkRunID != "" ||
		result.Activation.ForkRunID != "" ||
		result.ExecutedEventCount != 0 ||
		len(result.ForkEvents) != 0 {
		t.Fatalf("result mutated before selected agent materialization rejection: %#v", result)
	}
	assertNoSelectedContractExecutionMutationForSource(t, db, sourceRunID, sourceEventID)
}

func TestExecuteSelectedContractRunForkMaterializesAndExecutesForkLocalAgentRuntime(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier7-composition/test-agent-emits-to-node")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002202, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "task.assigned", at)
	seedSourceOutcomeThatMustNotSuppressFork(t, db, sourceEventID, entityID, at)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	agent := &selectedContractForkTestAgent{}
	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:  sourceRunID,
		At:           sourceEventID,
		Store:        pg,
		SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
		AgentRuntime: SelectedContractAgentRuntimeOptions{
			AgentFactory: func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
				agent.Configure(cfg)
				return agent, nil
			},
			QuiescenceTimeout: 5 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.AgentRuntimeMaterialization == nil ||
		result.AgentRuntimeMaterialization.Owner != store.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner ||
		result.AgentRuntimeMaterialization.RecipientPlanningOwner != store.RunForkSelectedContractRecipientPlanningOwner ||
		result.AgentRuntimeMaterialization.ExecutionOwner != store.RunForkSelectedContractExecutionOwner ||
		!result.AgentRuntimeMaterialization.MaterializationRequired ||
		!result.AgentRuntimeMaterialization.MaterializationSupported ||
		!result.AgentRuntimeMaterialization.EphemeralForkLocal ||
		!containsString(result.AgentRuntimeMaterialization.AgentRecipients, "test-agent") ||
		!containsString(result.AgentRuntimeMaterialization.ConfiguredAgentIDs, "test-agent") {
		t.Fatalf("agent runtime materialization = %#v", result.AgentRuntimeMaterialization)
	}
	if result.Owner != store.RunForkSelectedContractExecutionOwner ||
		result.Materialization.ForkRunID == "" ||
		!result.Activation.Activated ||
		result.ExecutedEventCount != 1 ||
		len(result.ForkEvents) != 1 {
		t.Fatalf("selected execution result = %#v", result)
	}
	assertSelectedContractRuntimeContainerProof(t,
		result.ForkLocalRuntimeContainer,
		store.RunForkSelectedContractExecutionOwner,
		sourceRunID,
		result.Materialization.ForkRunID,
		sourceEventID,
		[]string{sourceEventID},
	)
	if got := agent.SeenRunIDs(); len(got) != 1 || got[0] != result.Materialization.ForkRunID {
		t.Fatalf("agent saw run ids = %#v, want fork run %s", got, result.Materialization.ForkRunID)
	}
	if got := agent.SeenEventIDs(); len(got) != 1 || got[0] != result.ForkEvents[0].ForkEventID {
		t.Fatalf("agent saw event ids = %#v, want fork event %s", got, result.ForkEvents[0].ForkEventID)
	}

	var persistedAgents int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM agents
		WHERE agent_id = 'test-agent'
	`).Scan(&persistedAgents); err != nil {
		t.Fatalf("count persisted selected agent rows: %v", err)
	}
	if persistedAgents != 0 {
		t.Fatalf("selected-fork runtime persisted current-runtime agent rows = %d, want 0", persistedAgents)
	}

	forkEventID := result.ForkEvents[0].ForkEventID
	var sourceCopiedEvents int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
	`, result.Materialization.ForkRunID, sourceEventID).Scan(&sourceCopiedEvents); err != nil {
		t.Fatalf("count copied source event ids: %v", err)
	}
	if sourceCopiedEvents != 0 {
		t.Fatalf("copied source event ids into fork run = %d, want 0", sourceCopiedEvents)
	}

	var agentDeliveries, agentReceipts int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = 'test-agent'
	`, result.Materialization.ForkRunID, forkEventID).Scan(&agentDeliveries); err != nil {
		t.Fatalf("count fork agent deliveries: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = 'test-agent'
		  AND outcome = 'success'
	`, forkEventID).Scan(&agentReceipts); err != nil {
		t.Fatalf("count fork agent receipts: %v", err)
	}
	if agentDeliveries != 1 || agentReceipts != 1 {
		t.Fatalf("fork-local agent outcomes deliveries=%d receipts=%d, want 1/1", agentDeliveries, agentReceipts)
	}

	var agentFollowUps, finalizedEvents int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'task.completed'
		  AND source_event_id = $2::uuid
		  AND produced_by = 'test-agent'
	`, result.Materialization.ForkRunID, forkEventID).Scan(&agentFollowUps); err != nil {
		t.Fatalf("count fork-local agent follow-ups: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'task.finalized'
	`, result.Materialization.ForkRunID).Scan(&finalizedEvents); err != nil {
		t.Fatalf("count finalized events: %v", err)
	}
	if agentFollowUps != 1 || finalizedEvents != 1 {
		t.Fatalf("fork-local follow-ups task.completed=%d task.finalized=%d, want 1/1", agentFollowUps, finalizedEvents)
	}

	var typedRuntimeDiagnostics int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'platform.runtime_log'
		  AND source_event_id = $2::uuid
		  AND payload->'details'->>'runtime_lineage_owner' = $3
		  AND payload->'details'->>'runtime_lineage_row_category' = 'diagnostic'
		  AND payload->'details'->>'runtime_lineage_classification' = 'fork_local'
	`, result.Materialization.ForkRunID, forkEventID, store.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner).Scan(&typedRuntimeDiagnostics); err != nil {
		t.Fatalf("count typed runtime diagnostics: %v", err)
	}
	if typedRuntimeDiagnostics == 0 {
		t.Fatalf("typed runtime diagnostics parented to fork event = %d, want > 0", typedRuntimeDiagnostics)
	}
}

func TestExecuteSelectedContractRunForkProviderCompletionRecordsDurableAuthority(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier7-composition/test-agent-emits-to-node")
	loader := ContractBundleSourceLoader{
		RepoRoot:         repoRoot,
		PlatformSpecPath: runtimecontracts.DefaultPlatformSpecFile(repoRoot),
	}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	var providerCalls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		providerCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"model":"gpt-selected-fork",
			"choices":[{"message":{"role":"assistant","content":"completed","tool_calls":[{
				"id":"emit-1","type":"function","function":{"name":"emit_task_completed","arguments":"{}"}
			}]}}],
			"usage":{"prompt_tokens":11,"completion_tokens":3,"total_tokens":14}
		}`))
	}))
	defer provider.Close()
	providerCredentials, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "provider-credentials.json"))
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	if err := providerCredentials.Set(ctx, llmselection.OpenAICompatibleCredentialEnv, "test-key"); err != nil {
		t.Fatalf("store provider credential: %v", err)
	}
	cfg := &config.Config{LLM: config.LLMConfig{
		Backend: llmselection.BackendOpenAICompatible,
		Models: llmselection.ModelAliases{
			llmselection.ModelAliasRegular: {llmselection.BackendOpenAICompatible: "gpt-selected-fork"},
		},
		Session: config.LLMSessionConfig{LockTTL: time.Second, RotateAfterTurns: 40, RotateOnParseFailures: 3},
		OpenAICompatible: config.OpenAICompatibleConfig{
			BaseURL: provider.URL,
		},
	}}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002203, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "task.assigned", at)
	seedSourceOutcomeThatMustNotSuppressFork(t, db, sourceEventID, entityID, at)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)
	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:  sourceRunID,
		At:           sourceEventID,
		Store:        pg,
		SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
		AgentRuntime: SelectedContractAgentRuntimeOptions{
			Config:              cfg,
			ProviderCredentials: providerCredentials,
			QuiescenceTimeout:   5 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if providerCalls.Load() != 1 {
		t.Fatalf("provider calls = %d, want 1", providerCalls.Load())
	}
	proof := result.ForkLocalRuntimeContainer
	if proof == nil || proof.RuntimeExecutionID == "" || proof.RuntimeGeneration == 0 || proof.AuthorityExecutionOwner == "" {
		t.Fatalf("selected completion runtime authority proof = %#v", proof)
	}

	var operations, attempts, turns int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runtime_external_effect_operations
		WHERE effect_kind = 'provider_turn'
		  AND authority_kind = 'selected_contract_fork'
		  AND authority_id = $1
		  AND selected_execution_id = $1::uuid
		  AND generation = $2
		  AND authority_evidence->>'execution_id' = $1
		  AND (authority_evidence->>'generation')::bigint = $2
	`, proof.RuntimeExecutionID, proof.RuntimeGeneration).Scan(&operations); err != nil {
		t.Fatalf("count selected completion operations: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runtime_external_effect_attempts a
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		WHERE o.selected_execution_id = $1::uuid
		  AND a.adapter = 'openai_compatible'
		  AND a.generation = $2
		  AND a.execution_owner = $3
		  AND a.state = 'settled'
	`, proof.RuntimeExecutionID, proof.RuntimeGeneration, proof.AuthorityExecutionOwner).Scan(&attempts); err != nil {
		t.Fatalf("count selected completion attempts: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM agent_turns t
		JOIN runtime_external_effect_attempts a ON a.attempt_id = t.completion_attempt_id
		JOIN runtime_external_effect_operations o ON o.operation_id = a.operation_id
		WHERE o.selected_execution_id = $1::uuid
		  AND t.run_id = $2::uuid
		  AND t.agent_id = 'test-agent'
		  AND t.usage_exactness = 'exact'
		  AND t.input_tokens = 11
		  AND t.output_tokens = 3
	`, proof.RuntimeExecutionID, result.Materialization.ForkRunID).Scan(&turns); err != nil {
		t.Fatalf("count selected completion turns: %v", err)
	}
	if operations != 1 || attempts != 1 || turns != 1 {
		t.Fatalf("selected completion evidence operations=%d attempts=%d turns=%d, want 1/1/1", operations, attempts, turns)
	}
}

func TestSelectedContractServedAndStandaloneContainersCompeteForOnePostgresAuthority(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	now := time.Now().UTC()
	sourceRunID := uuid.NewString()
	forkRunID := uuid.NewString()
	forkEventID := uuid.NewString()
	bindingID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id,status,started_at) VALUES ($1::uuid,'running',$3),($2::uuid,'paused',$3)`, sourceRunID, forkRunID, now); err != nil {
		t.Fatalf("seed selected-contract container competition runs: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO events (event_id,run_id,event_name,scope,created_at) VALUES ($1::uuid,$2::uuid,'selected.test','global',$3)`, forkEventID, sourceRunID, now); err != nil {
		t.Fatalf("seed selected-contract container competition event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO run_fork_selected_contract_bindings
			(binding_id,fork_run_id,source_run_id,fork_event_id,mode,contracts_root,workflow_name,workflow_version,created_at)
		VALUES ($1::uuid,$2::uuid,$3::uuid,$4::uuid,'selected_contracts','/tmp/contracts','workflow','v1',$5)
	`, bindingID, forkRunID, sourceRunID, forkEventID, now); err != nil {
		t.Fatalf("seed selected-contract container competition binding: %v", err)
	}

	servedDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open served PostgreSQL handle: %v", err)
	}
	defer servedDB.Close()
	standaloneDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open standalone PostgreSQL handle: %v", err)
	}
	defer standaloneDB.Close()
	if err := servedDB.PingContext(ctx); err != nil {
		t.Fatalf("ping served PostgreSQL handle: %v", err)
	}
	if err := standaloneDB.PingContext(ctx); err != nil {
		t.Fatalf("ping standalone PostgreSQL handle: %v", err)
	}

	selection := store.RunForkContractSelection{
		Mode:            "selected_contracts",
		ContractsRoot:   "/tmp/contracts",
		WorkflowName:    "workflow",
		WorkflowVersion: "v1",
	}
	admission := store.RunForkSelectedContractExecutionAdmission{
		Owner:                 store.RunForkSelectedContractExecutionAdmissionOwner,
		FutureExecutionOwner:  store.RunForkSelectedContractExecutionOwner,
		NonMutating:           true,
		ExecutionSupported:    false,
		ForkRunID:             forkRunID,
		SourceRunID:           sourceRunID,
		ForkEventID:           forkEventID,
		ContractSelection:     selection,
		ContractBindingOwner:  store.RunForkSelectedContractBindingOwner,
		AdmissionOwner:        store.RunForkContractFrontierAdmissionOwner,
		AdmissionUse:          store.RunForkSelectedContractExecutionAdmissionUseDurableBinding,
		ExecutionModelOwner:   store.RunForkSelectedContractExecutionModelOwner,
		SourceWorkflowName:    "workflow",
		SourceWorkflowVersion: "v1",
	}
	planning := store.RunForkSelectedContractRecipientPlanning{
		Owner:                      store.RunForkSelectedContractRecipientPlanningOwner,
		FutureExecutionOwner:       store.RunForkSelectedContractExecutionOwner,
		NonMutating:                true,
		RecipientPlanningSupported: true,
		ContractSelection:          selection,
	}
	baseRequest := publishSelectedContractForkEventsRequest{
		Admission:         admission,
		RecipientPlanning: planning,
		SourceRunID:       sourceRunID,
		ForkRunID:         forkRunID,
		ForkEventID:       forkEventID,
		SourceEvents:      []string{forkEventID},
		ExecutionOwner:    store.RunForkSelectedContractExecutionOwner,
	}
	type contenderResult struct {
		surface   string
		container selectedContractForkLocalRuntimeContainer
		store     *store.PostgresStore
		err       error
	}
	start := make(chan struct{})
	results := make(chan contenderResult, 2)
	contenders := []struct {
		surface string
		db      *sql.DB
	}{
		{surface: "served", db: servedDB},
		{surface: "standalone", db: standaloneDB},
	}
	for _, contender := range contenders {
		contender := contender
		go func() {
			<-start
			pg := &store.PostgresStore{DB: contender.db}
			req := baseRequest
			req.Store = pg
			container, buildErr := buildSelectedContractForkLocalRuntimeContainer(ctx, req)
			results <- contenderResult{surface: contender.surface, container: container, store: pg, err: buildErr}
		}()
	}
	close(start)
	first, second := <-results, <-results
	var winner contenderResult
	var loser contenderResult
	if first.err == nil {
		winner, loser = first, second
	} else {
		winner, loser = second, first
	}
	if winner.err != nil || loser.err == nil {
		t.Fatalf("served/standalone authority race first=%s/%v second=%s/%v, want exactly one winner", first.surface, first.err, second.surface, second.err)
	}
	if winner.surface == loser.surface {
		t.Fatalf("served/standalone authority race returned duplicate surface %q", winner.surface)
	}
	proof := winner.container.Proof()
	if proof.RuntimeExecutionID == "" || proof.RuntimeGeneration != 1 || proof.AuthorityExecutionOwner == "" {
		t.Fatalf("winning %s authority proof = %#v", winner.surface, proof)
	}

	authority := winner.container.authority
	authority.Target = runtimeeffects.UsageTarget{
		Kind:        runtimeeffects.UsageTargetAgentTurn,
		ID:          uuid.NewString(),
		RunID:       forkRunID,
		AgentID:     "selected-agent",
		SessionID:   uuid.NewString(),
		RuntimeMode: "task",
		ScopeKey:    "selected-authority-race",
	}
	providerCtx := runtimeeffects.WithLogicalOperationIdentity(
		runtimeeffects.WithController(runtimeeffects.WithAuthority(ctx, authority), runtimeeffects.NewController(winner.store)),
		"served-standalone-authority-race",
	)
	handle, err := runtimeeffects.BeginCompletion(providerCtx, "openai_compatible", []byte("request"), nil)
	if err != nil {
		t.Fatalf("winning %s authorize provider completion: %v", winner.surface, err)
	}
	if err := handle.MarkLaunched(providerCtx); err != nil {
		t.Fatalf("winning %s launch provider completion: %v", winner.surface, err)
	}
	var providerLaunches atomic.Int32
	providerLaunches.Add(1)
	if err := handle.MarkResponseObserved(providerCtx, map[string]any{"surface": winner.surface}); err != nil {
		t.Fatalf("winning %s observe provider response: %v", winner.surface, err)
	}
	inputTokens, outputTokens := int64(1), int64(1)
	if err := handle.SettleCompletion(providerCtx, runtimeeffects.CompletionSettlement{
		Settlement: runtimeeffects.Settlement{State: runtimeeffects.StateSettled, Evidence: map[string]any{"surface": winner.surface}},
		Usage: runtimeeffects.CompletionUsage{
			ResolvedModel: "test-model",
			Exactness:     runtimeeffects.CompletionUsageExact,
			InputTokens:   &inputTokens,
			OutputTokens:  &outputTokens,
		},
		AgentTurn: &runtimeeffects.CompletionAgentTurn{
			TurnID: authority.Target.ID, RunID: forkRunID, AgentID: authority.Target.AgentID,
			SessionID: authority.Target.SessionID, RuntimeMode: authority.Target.RuntimeMode,
			ScopeKey: authority.Target.ScopeKey, ParseOK: true,
		},
		Spend: runtimeeffects.CompletionSpend{
			FlowInstance: authority.Target.ScopeKey, AgentID: authority.Target.AgentID,
			Model: "test-model", ModelAlias: "regular", BackendProfile: "test",
			Provider: "test", Transport: "http", ResolvedModel: "test-model",
			InvocationType: authority.Target.RuntimeMode,
		},
		Now: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("winning %s settle provider completion: %v", winner.surface, err)
	}
	if providerLaunches.Load() != 1 {
		t.Fatalf("provider launches = %d, want 1", providerLaunches.Load())
	}
	if err := winner.container.Quiesce(ctx); err != nil {
		t.Fatalf("quiesce winning %s container: %v", winner.surface, err)
	}
	if err := winner.container.Close(ctx); err != nil {
		t.Fatalf("close winning %s container: %v", winner.surface, err)
	}

	var authorities, attempts, reservations int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions WHERE fork_run_id=$1::uuid AND generation=1 AND state='closed'`, forkRunID).Scan(&authorities); err != nil {
		t.Fatalf("count served/standalone authority rows: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_external_effect_attempts a JOIN runtime_external_effect_operations o ON o.operation_id=a.operation_id WHERE o.selected_execution_id=$1::uuid AND a.state='settled'`, proof.RuntimeExecutionID).Scan(&attempts); err != nil {
		t.Fatalf("count served/standalone completion attempts: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_effect_budget_reservations WHERE attempt_id=$1::uuid`, handle.Attempt().AttemptID).Scan(&reservations); err != nil {
		t.Fatalf("count no-cap completion reservations: %v", err)
	}
	if authorities != 1 || attempts != 1 || reservations != 0 {
		t.Fatalf("served/standalone authority evidence authorities=%d attempts=%d reservations=%d, want 1/1/0", authorities, attempts, reservations)
	}
}

func TestStartSelectedContractAgentRuntimeGatewayReturnsGeneratedBinding(t *testing.T) {
	const staleHostURL = "http://127.0.0.1:9998"
	const staleContainerURL = "http://host.docker.internal:9998"
	t.Setenv("SWARM_TOOL_GATEWAY_URL", staleHostURL)
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", staleContainerURL)
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "")

	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{})
	turns := runtimemcp.NewTurnContextRegistry(runtimeactors.ActorFromContext)
	binding, cleanup, err := startSelectedContractAgentRuntimeGateway(exec, nil, turns, nil)
	if err != nil {
		t.Fatalf("startSelectedContractAgentRuntimeGateway: %v", err)
	}
	if cleanup == nil {
		t.Fatal("cleanup is nil")
	}
	defer cleanup()
	if binding.HostMCPURL() == "" || binding.WorkspaceMCPURL() == "" {
		t.Fatalf("binding endpoints were not populated: %#v", binding)
	}
	if binding.AuthToken() == "" {
		t.Fatalf("binding token was not generated: %#v", binding)
	}
	if strings.Contains(binding.HostEndpoint, ":9998") || strings.Contains(binding.WorkspaceEndpoint, ":9998") {
		t.Fatalf("binding endpoints used stale env: %#v", binding)
	}
	if got := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_URL")); got != staleHostURL {
		t.Fatalf("SWARM_TOOL_GATEWAY_URL = %q, want stale operator value unchanged %q", got, staleHostURL)
	}
	if got := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_CONTAINER_URL")); got != staleContainerURL {
		t.Fatalf("SWARM_TOOL_GATEWAY_CONTAINER_URL = %q, want stale operator value unchanged %q", got, staleContainerURL)
	}
	if got := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_TOKEN")); got != "" {
		t.Fatalf("SWARM_TOOL_GATEWAY_TOKEN = %q, want generated binding token to remain typed-only", got)
	}

	req, err := http.NewRequest(http.MethodPost, binding.HostMCPURL(), strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+binding.AuthToken())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post selected-fork gateway initialize: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("selected-fork gateway status = %d, want 200", resp.StatusCode)
	}
}

func TestStartSelectedContractAgentRuntimeGatewayRejectsRetiredTokenEnv(t *testing.T) {
	const staleHostURL = "http://127.0.0.1:9998"
	const staleContainerURL = "http://host.docker.internal:9998"
	t.Setenv("SWARM_TOOL_GATEWAY_URL", staleHostURL)
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", staleContainerURL)
	t.Setenv("SWARM_TOOL_GATEWAY_TOKEN", "operator-token")

	exec := runtimetools.NewExecutorWithOptions(nil, nil, runtimetools.ExecutorOptions{})
	turns := runtimemcp.NewTurnContextRegistry(runtimeactors.ActorFromContext)
	binding, cleanup, err := startSelectedContractAgentRuntimeGateway(exec, nil, turns, nil)
	if err == nil || !strings.Contains(err.Error(), "SWARM_TOOL_GATEWAY_TOKEN is retired") || !strings.Contains(err.Error(), "ToolGatewayBinding") {
		t.Fatalf("startSelectedContractAgentRuntimeGateway error = %v, want retired token env rejection", err)
	}
	if cleanup != nil {
		cleanup()
		t.Fatal("cleanup was returned for rejected retired token env")
	}
	if !binding.Empty() {
		t.Fatalf("binding = %#v, want empty rejected binding", binding)
	}
	if got := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_URL")); got != staleHostURL {
		t.Fatalf("SWARM_TOOL_GATEWAY_URL = %q, want unchanged %q", got, staleHostURL)
	}
	if got := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_CONTAINER_URL")); got != staleContainerURL {
		t.Fatalf("SWARM_TOOL_GATEWAY_CONTAINER_URL = %q, want unchanged %q", got, staleContainerURL)
	}
	if got := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_TOKEN")); got != "operator-token" {
		t.Fatalf("SWARM_TOOL_GATEWAY_TOKEN = %q, want explicit token env preserved", got)
	}
}

func TestStartSelectedContractAgentRuntimeCleansGatewayOnRegistrationFailure(t *testing.T) {
	const staleHostURL = "http://127.0.0.1:9998"
	const staleContainerURL = "http://host.docker.internal:9998"
	t.Setenv("SWARM_TOOL_GATEWAY_URL", staleHostURL)
	t.Setenv("SWARM_TOOL_GATEWAY_CONTAINER_URL", staleContainerURL)
	t.Setenv("SWARM_CLAUDE_USE_MCP", "1")
	eventBus, err := bus.NewEventBus(nil)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}

	_, err = startSelectedContractAgentRuntime(context.Background(), publishSelectedContractForkEventsRequest{
		Store: &store.PostgresStore{},
		AgentRuntime: selectedContractAgentRuntimePlan{
			Proof: SelectedContractAgentRuntimeMaterialization{
				AgentRecipients: []string{"bad-agent"},
			},
			Records: []runtimemanager.PersistedAgent{{
				Config: runtimeactors.AgentConfig{
					ID:            "bad-agent",
					Role:          "worker",
					LLMBackend:    "claude_cli",
					Model:         "regular",
					Subscriptions: []string{"item.received"},
				},
			}},
			Options: SelectedContractAgentRuntimeOptions{
				Config:     &config.Config{},
				LLMRuntime: selectedContractCleanupRuntime{},
			},
		},
	}, eventBus)
	if err == nil || !strings.Contains(err.Error(), "missing required system_prompt") {
		t.Fatalf("startSelectedContractAgentRuntime error = %v, want registration failure", err)
	}
	if got := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_URL")); got != staleHostURL {
		t.Fatalf("SWARM_TOOL_GATEWAY_URL = %q, want unchanged %q", got, staleHostURL)
	}
	if got := strings.TrimSpace(os.Getenv("SWARM_TOOL_GATEWAY_CONTAINER_URL")); got != staleContainerURL {
		t.Fatalf("SWARM_TOOL_GATEWAY_CONTAINER_URL = %q, want unchanged %q", got, staleContainerURL)
	}
}

type selectedContractCleanupRuntime struct{}

func (selectedContractCleanupRuntime) StartSession(context.Context, string, string, []runtimellm.ToolDefinition) (*runtimellm.Session, error) {
	return &runtimellm.Session{}, nil
}

func (selectedContractCleanupRuntime) ContinueSession(context.Context, *runtimellm.Session, runtimellm.Message) (*runtimellm.Response, error) {
	return &runtimellm.Response{}, nil
}

func TestExecuteSelectedContractRunForkTreatsDiagnosticPlatformOutcomeAsLineage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	diagnosticEventID := uuid.NewString()
	at := time.Unix(1700002215, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at)
	seedSelectedExecutionDiagnosticPlatformDeadLetter(t, db, sourceRunID, diagnosticEventID, at.Add(-time.Second))
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:  sourceRunID,
		At:           sourceEventID,
		Store:        pg,
		SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.Materialization.ForkRunID == "" || !result.Activation.Activated || result.ExecutedEventCount != 1 {
		t.Fatalf("selected execution result = %#v", result)
	}
	if result.SelectedContractExecutionAdmission == nil || result.SelectedContractExecutionAdmission.FrontierEventCount != 1 {
		t.Fatalf("selected execution admission = %#v, want only selected source frontier", result.SelectedContractExecutionAdmission)
	}
	if selectedExecutionResultHasBlocker(result, store.RunForkBlockerContractFrontierRouteUnresolved) {
		t.Fatalf("selected execution retained unresolved route blocker: materialization=%#v activation=%#v", result.Materialization.UnsupportedBlockers, result.Activation.UnsupportedBlockers)
	}

	var diagnosticCopies int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND (
			event_id = $2::uuid
			OR COALESCE(source_event_id::text, '') = $2::text
		  )
	`, result.Materialization.ForkRunID, diagnosticEventID).Scan(&diagnosticCopies); err != nil {
		t.Fatalf("count copied diagnostic events: %v", err)
	}
	if diagnosticCopies != 0 {
		t.Fatalf("diagnostic platform events copied into fork = %d, want 0", diagnosticCopies)
	}

	var diagnosticExecutionLineage int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_executions
		WHERE fork_run_id = $1::uuid
		  AND source_event_id = $2::uuid
	`, result.Materialization.ForkRunID, diagnosticEventID).Scan(&diagnosticExecutionLineage); err != nil {
		t.Fatalf("count diagnostic execution lineage: %v", err)
	}
	if diagnosticExecutionLineage != 0 {
		t.Fatalf("diagnostic platform execution lineage rows = %d, want 0", diagnosticExecutionLineage)
	}

	var selectedExecutionLineage int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_executions
		WHERE fork_run_id = $1::uuid
		  AND source_event_id = $2::uuid
	`, result.Materialization.ForkRunID, sourceEventID).Scan(&selectedExecutionLineage); err != nil {
		t.Fatalf("count selected execution lineage: %v", err)
	}
	if selectedExecutionLineage != 1 {
		t.Fatalf("selected source execution lineage rows = %d, want 1", selectedExecutionLineage)
	}
}

func TestActivateSelectedContractRunForkExecutesReplayReadyContractSwapThroughSelectedRecipients(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}
	selection := runforkadmission.SelectedContractSelection(loaded.Source, contractsRoot)

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002600, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at)
	seedSourceOutcomeThatMustNotSuppressFork(t, db, sourceEventID, entityID, at)
	if _, err := db.ExecContext(ctx, `
		UPDATE event_deliveries
		SET subscriber_type = 'agent',
		    subscriber_id = 'source-agent-that-must-not-route',
		    status = 'pending',
		    retry_count = 0,
		    reason_code = 'source_pending_agent_delivery',
		    active_session_id = NULL,
		    started_at = NULL,
		    delivered_at = NULL
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
	`, sourceRunID, sourceEventID); err != nil {
		t.Fatalf("seed replayable source agent delivery: %v", err)
	}
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	materialized := materializeSelectedExecutionForkForTest(t, ctx, pg, loaded.Source, selection, sourceRunID, sourceEventID)

	result, err := ActivateSelectedContractRunFork(ctx, SelectedContractActivationGateRequest{
		ForkRunID:    materialized.ForkRunID,
		Store:        pg,
		SourceLoader: loader,
	})
	if err != nil {
		t.Fatalf("ActivateSelectedContractRunFork: %v", err)
	}
	if result.ContractSwapBootResumeExecution == nil ||
		result.ContractSwapBootResumeExecution.Owner != store.RunForkHistoricalReplayContractSwapBootResumeOwner ||
		result.ContractSwapBootResumeExecution.ParentHistoricalReplayExecutionOwner != store.RunForkHistoricalReplayExecutionOwner ||
		len(result.ContractSwapBootResumeExecution.ExecutableWork) != 1 {
		t.Fatalf("contract-swap execution = %#v", result.ContractSwapBootResumeExecution)
	}
	if result.ExecutedEventCount != 1 || len(result.ForkEvents) != 1 || !result.Activated {
		t.Fatalf("activation result = %#v", result)
	}
	assertSelectedContractRuntimeContainerProof(t,
		result.ForkLocalRuntimeContainer,
		store.RunForkHistoricalReplayContractSwapBootResumeOwner,
		sourceRunID,
		materialized.ForkRunID,
		sourceEventID,
		[]string{sourceEventID},
	)
	forkEventID := result.ForkEvents[0].ForkEventID

	var sourceSubscriberDeliveries, forkEventDeliveries int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
		  AND subscriber_id = 'source-agent-that-must-not-route'
	`, materialized.ForkRunID, forkEventID).Scan(&sourceSubscriberDeliveries); err != nil {
		t.Fatalf("count source subscriber deliveries: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
	`, materialized.ForkRunID, forkEventID).Scan(&forkEventDeliveries); err != nil {
		t.Fatalf("count fork event deliveries: %v", err)
	}
	if sourceSubscriberDeliveries != 0 || forkEventDeliveries == 0 {
		t.Fatalf("fork delivery recipients source=%d total=%d, want selected recipient planning without source subscriber", sourceSubscriberDeliveries, forkEventDeliveries)
	}

	var genericReplayRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_delivery_event_replays
		WHERE fork_run_id = $1::uuid
	`, materialized.ForkRunID).Scan(&genericReplayRows); err != nil {
		t.Fatalf("count generic delivery replay rows: %v", err)
	}
	if genericReplayRows != 0 {
		t.Fatalf("generic delivery replay rows = %d, want contract-swap execution to avoid source-subscriber writer", genericReplayRows)
	}

	var forkReceipts, emittedFollowUps int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid`, forkEventID).Scan(&forkReceipts); err != nil {
		t.Fatalf("count fork receipts: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'item.processed'
		  AND source_event_id = $2::uuid
	`, materialized.ForkRunID, forkEventID).Scan(&emittedFollowUps); err != nil {
		t.Fatalf("count emitted follow-ups: %v", err)
	}
	if forkReceipts == 0 || emittedFollowUps != 1 {
		t.Fatalf("fork outcomes receipts=%d followUps=%d, want selected handler execution", forkReceipts, emittedFollowUps)
	}
}

func TestActivateSelectedContractRunForkFailsBeforePublishForPostTReplayScopeMarker(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}
	selection := runforkadmission.SelectedContractSelection(loaded.Source, contractsRoot)

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	afterEventID := uuid.NewString()
	at := time.Unix(1700002605, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at)
	seedSourceOutcomeThatMustNotSuppressFork(t, db, sourceEventID, entityID, at)
	if _, err := db.ExecContext(ctx, `
		UPDATE event_deliveries
		SET subscriber_type = 'agent',
		    subscriber_id = 'source-agent-that-must-not-route',
		    status = 'pending',
		    retry_count = 0,
		    reason_code = 'source_pending_agent_delivery',
		    active_session_id = NULL,
		    started_at = NULL,
		    delivered_at = NULL
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
	`, sourceRunID, sourceEventID); err != nil {
		t.Fatalf("seed replayable source agent delivery: %v", err)
	}
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	materialized := materializeSelectedExecutionForkForTest(t, ctx, pg, loaded.Source, selection, sourceRunID, sourceEventID)
	seedSelectedExecutionPostForkSourceEvent(t, db, sourceRunID, afterEventID, entityID, at.Add(time.Second))
	seedSelectedExecutionSourceReplayScopeMarker(t, db, sourceRunID, afterEventID, "replay_scope_direct", at.Add(time.Second))
	captureSelectedExecutionSourceRevision(t, db, sourceRunID, runforkrevision.FamilyEvents, runforkrevision.FamilyEventDeliveries)

	result, err := ActivateSelectedContractRunFork(ctx, SelectedContractActivationGateRequest{
		ForkRunID:    materialized.ForkRunID,
		Store:        pg,
		SourceLoader: loader,
	})
	if err == nil || !strings.Contains(err.Error(), "source_committed_replay_scope_advanced_after_fork_point") {
		t.Fatalf("ActivateSelectedContractRunFork error = %v, want post-T marker blocker", err)
	}
	if result.ExecutedEventCount != 0 || len(result.ForkEvents) != 0 || result.Activated {
		t.Fatalf("result = %#v, want no fork publish before marker block", result)
	}
	assertNoForkExecutionRowsForRun(t, db, materialized.ForkRunID)
}

func TestExecuteSelectedContractRunForkTreatsSourceConversationHistoryAsLineage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	sessionID := uuid.NewString()
	auditID := uuid.NewString()
	turnID := uuid.NewString()
	at := time.Unix(1700002300, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (agent_id, role, model, conversation_mode, status, created_at)
		VALUES ('agent-a', 'test-agent', 'tier1', 'session_per_entity', 'active', $1)
		ON CONFLICT (agent_id) DO NOTHING
	`, at); err != nil {
		t.Fatalf("seed source session agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
			runtime_mode, status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent-a', $3::uuid, 'flow-a/1', $3::text, 'entity',
			'session_per_entity', 'active', $4, $4)
	`, sessionID, sourceRunID, entityID, at); err != nil {
		t.Fatalf("seed source session: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
			runtime_mode, runtime_state, status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent-a', $3::uuid, 'flow-a/1', $3::text, 'entity',
			'task', '{}'::jsonb, 'active', $4, $4)
	`, auditID, sourceRunID, entityID, at); err != nil {
		t.Fatalf("seed source conversation audit: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, runtime_mode, scope_key, entity_id,
			trigger_event_id, trigger_event_type, task_id, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent-a', $3::uuid, 'session_per_entity', $4::text, $4::uuid,
			$5::uuid, 'item.received', 'task-a', $6)
	`, turnID, sourceRunID, sessionID, entityID, sourceEventID, at); err != nil {
		t.Fatalf("seed source turn: %v", err)
	}
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:  sourceRunID,
		At:           sourceEventID,
		Store:        pg,
		SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.Materialization.ForkRunID == "" || !result.Activation.Activated {
		t.Fatalf("selected execution result = %#v", result)
	}
	for _, code := range []string{
		store.RunForkBlockerSessionHistoryUnproven,
		store.RunForkBlockerConversationAuditUnproven,
		store.RunForkBlockerActiveTurnHistoryUnproven,
	} {
		if selectedExecutionResultHasBlocker(result, code) {
			t.Fatalf("selected execution retained %s: materialization=%#v activation=%#v", code, result.Materialization.UnsupportedBlockers, result.Activation.UnsupportedBlockers)
		}
	}
	var copiedSessions, copiedAudits, copiedTurns int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_sessions WHERE run_id = $1::uuid OR session_id = $2::uuid
	`, result.Materialization.ForkRunID, sessionID).Scan(&copiedSessions); err != nil {
		t.Fatalf("count session copies: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_conversation_audits WHERE run_id = $1::uuid OR session_id = $2::uuid
	`, result.Materialization.ForkRunID, auditID).Scan(&copiedAudits); err != nil {
		t.Fatalf("count audit copies: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_turns WHERE run_id = $1::uuid OR turn_id = $2::uuid
	`, result.Materialization.ForkRunID, turnID).Scan(&copiedTurns); err != nil {
		t.Fatalf("count turn copies: %v", err)
	}
	if copiedSessions != 1 || copiedAudits != 1 || copiedTurns != 1 {
		t.Fatalf("copied conversation rows sessions=%d audits=%d turns=%d, want source-only 1/1/1", copiedSessions, copiedAudits, copiedTurns)
	}
}

func TestExecuteSelectedContractRunForkAdmitsSameSourceActiveDeliveryForkPointEmission(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	forkPointEventID := uuid.NewString()
	sessionID := uuid.NewString()
	auditID := uuid.NewString()
	turnID := uuid.NewString()
	at := time.Unix(1700002303, 0).UTC()
	forkAt := at.Add(30 * time.Second)
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (agent_id, role, model, conversation_mode, status, created_at)
		VALUES ('validation-coordinator', 'test-agent', 'tier1', 'session_per_entity', 'active', $1)
		ON CONFLICT (agent_id) DO NOTHING
	`, at); err != nil {
		t.Fatalf("seed source session agent: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
			runtime_mode, status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, 'validation-coordinator', $3::uuid, 'flow-a/1', $3::text, 'entity',
			'session_per_entity', 'active', $4, $4)
	`, sessionID, sourceRunID, entityID, at); err != nil {
		t.Fatalf("seed source session: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
			runtime_mode, runtime_state, status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, 'validation-coordinator', $3::uuid, 'flow-a/1', $3::text, 'entity',
			'task', '{}'::jsonb, 'active', $4, $4)
	`, auditID, sourceRunID, entityID, at); err != nil {
		t.Fatalf("seed source conversation audit: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, runtime_mode, scope_key, entity_id,
			trigger_event_id, trigger_event_type, task_id, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'validation-coordinator', $3::uuid, 'session_per_entity', $4::text, $4::uuid,
			$5::uuid, 'item.received', 'task-a', $6)
	`, turnID, sourceRunID, sessionID, entityID, sourceEventID, at); err != nil {
		t.Fatalf("seed source turn: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, started_at, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent', 'validation-coordinator', 'in_progress', $3, $3)
	`, sourceRunID, sourceEventID, at.Add(5*time.Second)); err != nil {
		t.Fatalf("seed in-progress source delivery: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload,
			produced_by, produced_by_type, source_event_id, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'item.received', $3::uuid,
			'flow-a/1', 'entity', '{}'::jsonb, 'validation-coordinator', 'agent',
			$4::uuid, $5
		)
	`, sourceRunID, forkPointEventID, entityID, sourceEventID, forkAt); err != nil {
		t.Fatalf("seed fork point event: %v", err)
	}
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:  sourceRunID,
		At:           forkPointEventID,
		Store:        pg,
		SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.Materialization.ForkRunID == "" || !result.Activation.Activated {
		t.Fatalf("selected execution result = %#v", result)
	}
	for _, code := range []string{
		store.RunForkBlockerDeliveryHistoryUnproven,
		store.RunForkBlockerSessionHistoryUnproven,
		store.RunForkBlockerConversationAuditUnproven,
		store.RunForkBlockerActiveTurnHistoryUnproven,
	} {
		if selectedExecutionResultHasBlocker(result, code) {
			t.Fatalf("selected execution retained %s: materialization=%#v activation=%#v", code, result.Materialization.UnsupportedBlockers, result.Activation.UnsupportedBlockers)
		}
	}
	if !result.Activation.SourceAdvancedAfterFork ||
		result.Activation.BranchDivergence == nil ||
		!containsString(result.Activation.BranchDivergence.SourceAdvancedFacts, store.RunForkSelectedContractActiveSourceDeliveryConversationCouplingClassification) {
		t.Fatalf("activation branch divergence = %#v, want #678 same-source active delivery fact", result.Activation.BranchDivergence)
	}

	var copiedSessions, copiedAudits, copiedTurns, copiedSourceSubscriberDeliveries int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_sessions WHERE run_id = $1::uuid OR session_id = $2::uuid
	`, result.Materialization.ForkRunID, sessionID).Scan(&copiedSessions); err != nil {
		t.Fatalf("count session copies: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_conversation_audits WHERE run_id = $1::uuid OR session_id = $2::uuid
	`, result.Materialization.ForkRunID, auditID).Scan(&copiedAudits); err != nil {
		t.Fatalf("count audit copies: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_turns WHERE run_id = $1::uuid OR turn_id = $2::uuid
	`, result.Materialization.ForkRunID, turnID).Scan(&copiedTurns); err != nil {
		t.Fatalf("count turn copies: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND subscriber_id = 'validation-coordinator'
		  AND status = 'in_progress'
	`, result.Materialization.ForkRunID).Scan(&copiedSourceSubscriberDeliveries); err != nil {
		t.Fatalf("count copied source delivery: %v", err)
	}
	if copiedSessions != 1 || copiedAudits != 1 || copiedTurns != 1 || copiedSourceSubscriberDeliveries != 0 {
		t.Fatalf("copied source rows sessions=%d audits=%d turns=%d sourceDeliveries=%d, want source-only conversation rows and no source delivery copies", copiedSessions, copiedAudits, copiedTurns, copiedSourceSubscriberDeliveries)
	}
}

func TestExecuteSelectedContractRunForkTreatsPostTSourceConversationHistoryAsBranchDivergence(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	sessionID := uuid.NewString()
	auditID := uuid.NewString()
	turnID := uuid.NewString()
	at := time.Unix(1700002305, 0).UTC()
	after := at.Add(time.Minute)
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agents (agent_id, role, model, conversation_mode, status, created_at)
		VALUES ('agent-a', 'test-agent', 'tier1', 'session_per_entity', 'active', $1)
		ON CONFLICT (agent_id) DO NOTHING
	`, at); err != nil {
		t.Fatalf("seed source session agent: %v", err)
	}
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
			runtime_mode, status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent-a', $3::uuid, 'flow-a/1', $3::text, 'entity',
			'session_per_entity', 'active', $4, $4)
	`, sessionID, sourceRunID, entityID, after); err != nil {
		t.Fatalf("seed post-T source session: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_conversation_audits (
			session_id, run_id, agent_id, entity_id, flow_instance, scope_key, scope,
			runtime_mode, runtime_state, status, created_at, updated_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent-a', $3::uuid, 'flow-a/1', $3::text, 'entity',
			'task', '{}'::jsonb, 'active', $4, $4)
	`, auditID, sourceRunID, entityID, after); err != nil {
		t.Fatalf("seed post-T source conversation audit: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO agent_turns (
			turn_id, run_id, agent_id, session_id, runtime_mode, scope_key, entity_id,
			trigger_event_id, trigger_event_type, task_id, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent-a', $3::uuid, 'session_per_entity', $4::text, $4::uuid,
			$5::uuid, 'item.received', 'task-a', $6)
	`, turnID, sourceRunID, sessionID, entityID, sourceEventID, after); err != nil {
		t.Fatalf("seed post-T source turn: %v", err)
	}
	captureSelectedExecutionSourceRevision(t, db, sourceRunID,
		runforkrevision.FamilyAgentSessions,
		runforkrevision.FamilyAgentConversationAudits,
		runforkrevision.FamilyAgentTurns,
	)

	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:  sourceRunID,
		At:           sourceEventID,
		Store:        pg,
		SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.Materialization.ForkRunID == "" || !result.Activation.Activated {
		t.Fatalf("selected execution result = %#v", result)
	}
	if !result.Activation.SourceAdvancedAfterFork || result.Activation.BranchDivergence == nil {
		t.Fatalf("activation = %#v, want source-advanced branch divergence", result.Activation)
	}
	for _, code := range []string{
		"source_sessions_advanced_after_fork_point",
		"source_conversation_audits_advanced_after_fork_point",
		"source_turns_advanced_after_fork_point",
	} {
		if selectedExecutionResultHasBlocker(result, code) {
			t.Fatalf("selected execution retained %s: materialization=%#v activation=%#v", code, result.Materialization.UnsupportedBlockers, result.Activation.UnsupportedBlockers)
		}
		if !containsString(result.Activation.BranchDivergence.SourceAdvancedFacts, code) {
			t.Fatalf("branch facts = %#v, want %s", result.Activation.BranchDivergence.SourceAdvancedFacts, code)
		}
	}
	for _, code := range []string{
		store.RunForkBlockerSessionHistoryUnproven,
		store.RunForkBlockerConversationAuditUnproven,
		store.RunForkBlockerActiveTurnHistoryUnproven,
	} {
		if selectedExecutionResultHasBlocker(result, code) {
			t.Fatalf("selected execution retained old conversation-history blocker %s: materialization=%#v activation=%#v", code, result.Materialization.UnsupportedBlockers, result.Activation.UnsupportedBlockers)
		}
	}

	var copiedSessions, copiedAudits, copiedTurns int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_sessions WHERE run_id = $1::uuid OR session_id = $2::uuid
	`, result.Materialization.ForkRunID, sessionID).Scan(&copiedSessions); err != nil {
		t.Fatalf("count copied source sessions: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_conversation_audits WHERE run_id = $1::uuid OR session_id = $2::uuid
	`, result.Materialization.ForkRunID, auditID).Scan(&copiedAudits); err != nil {
		t.Fatalf("count copied source audits: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM agent_turns WHERE run_id = $1::uuid OR turn_id = $2::uuid
	`, result.Materialization.ForkRunID, turnID).Scan(&copiedTurns); err != nil {
		t.Fatalf("count copied source turns: %v", err)
	}
	if copiedSessions != 1 || copiedAudits != 1 || copiedTurns != 1 {
		t.Fatalf("copied post-T conversation rows sessions=%d audits=%d turns=%d, want source-only 1/1/1", copiedSessions, copiedAudits, copiedTurns)
	}
}

func TestExecuteSelectedContractRunForkTreatsSourceReplayScopeMarkerAsLineage(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002315, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at)
	seedSelectedExecutionSourceReplayScopeMarker(t, db, sourceRunID, sourceEventID, "replay_scope_subscribed", at)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:  sourceRunID,
		At:           sourceEventID,
		Store:        pg,
		SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.Materialization.ForkRunID == "" || !result.Activation.Activated {
		t.Fatalf("selected execution result = %#v", result)
	}
	if selectedExecutionResultHasBlocker(result, store.RunForkBlockerCommittedReplayScopeReplayUnsupported) {
		t.Fatalf("selected execution retained committed replay-scope blocker: materialization=%#v activation=%#v", result.Materialization.UnsupportedBlockers, result.Activation.UnsupportedBlockers)
	}

	var copiedSourceMarkers int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = '__runtime_replay_scope__'
		  AND reason_code = 'replay_scope_subscribed'
		  AND event_id = $2::uuid
	`, result.Materialization.ForkRunID, sourceEventID).Scan(&copiedSourceMarkers); err != nil {
		t.Fatalf("count copied source replay-scope markers: %v", err)
	}
	if copiedSourceMarkers != 0 {
		t.Fatalf("copied source replay-scope markers into fork = %d, want 0", copiedSourceMarkers)
	}
	var forkLocalMarkers int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = '__runtime_replay_scope__'
		  AND reason_code IN ('replay_scope_direct', 'replay_scope_subscribed')
		  AND event_id <> $2::uuid
	`, result.Materialization.ForkRunID, sourceEventID).Scan(&forkLocalMarkers); err != nil {
		t.Fatalf("count fork-local replay-scope markers: %v", err)
	}
	if forkLocalMarkers == 0 {
		t.Fatalf("fork-local replay-scope marker missing for selected execution result")
	}
}

func TestExecuteSelectedContractRunForkRejectsSameEventReplayScopeMarkerWriteSkew(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002320, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)
	if _, err := db.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION seed_post_t_replay_scope_marker_after_route_recovery()
		RETURNS trigger AS $$
		BEGIN
			INSERT INTO event_deliveries (
				run_id, event_id, subscriber_type, subscriber_id, status,
				retry_count, reason_code, delivered_at, created_at
			)
			VALUES (
				NEW.source_run_id, NEW.fork_event_id, 'node', '__runtime_replay_scope__', 'delivered',
				0, 'replay_scope_direct', NEW.created_at + interval '1 second', NEW.created_at + interval '1 second'
			);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;

		CREATE TRIGGER seed_post_t_replay_scope_marker_after_route_recovery
		AFTER INSERT ON run_fork_selected_contract_route_recoveries
		FOR EACH ROW EXECUTE FUNCTION seed_post_t_replay_scope_marker_after_route_recovery();
	`); err != nil {
		t.Fatalf("install post-T marker trigger: %v", err)
	}

	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:  sourceRunID,
		At:           sourceEventID,
		Store:        pg,
		SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err == nil || !strings.Contains(err.Error(), "source_committed_replay_scope_advanced_after_fork_point") {
		t.Fatalf("ExecuteSelectedContractRunFork error = %v, want committed replay-scope advancement rejection", err)
	}
	if result.Activation.Activated {
		t.Fatalf("activation = %#v, want rejection before activation", result.Activation)
	}
	assertSelectedContractExecutionCleanup(t, db, sourceRunID, result.Materialization.ForkRunID)
}

func TestExecuteSelectedContractRunForkRejectsUnresolvedFrontierBeforeMaterialization(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002325, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "ghost.event", at)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:  sourceRunID,
		At:           sourceEventID,
		Store:        pg,
		SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err == nil || !strings.Contains(err.Error(), store.RunForkBlockerContractFrontierRouteUnresolved) {
		t.Fatalf("ExecuteSelectedContractRunFork error = %v, want unresolved frontier blocker", err)
	}
	if result.Materialization.ForkRunID != "" || result.ExecutedEventCount != 0 {
		t.Fatalf("result mutated before unresolved frontier rejection: %#v", result)
	}

	var forkRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE forked_from_run_id = $1::uuid`, sourceRunID).Scan(&forkRows); err != nil {
		t.Fatalf("count fork rows: %v", err)
	}
	if forkRows != 0 {
		t.Fatalf("fork rows after unresolved frontier rejection = %d, want 0", forkRows)
	}
}

func TestExecuteSelectedContractRunForkCleansUpBeforeActivationOnPublishFailure(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		CREATE OR REPLACE FUNCTION fail_selected_contract_execution_lineage_insert()
		RETURNS trigger AS $$
		BEGIN
			RAISE EXCEPTION 'forced selected execution lineage failure';
		END;
		$$ LANGUAGE plpgsql;

		CREATE TRIGGER fail_selected_contract_execution_lineage_insert
		BEFORE INSERT ON run_fork_selected_contract_executions
		FOR EACH ROW EXECUTE FUNCTION fail_selected_contract_execution_lineage_insert();
	`); err != nil {
		t.Fatalf("install lineage failure trigger: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	at := time.Unix(1700002335, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)

	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:  sourceRunID,
		At:           sourceEventID,
		Store:        pg,
		SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err == nil || !strings.Contains(err.Error(), "forced selected execution lineage failure") {
		t.Fatalf("ExecuteSelectedContractRunFork error = %v, want forced lineage publish failure", err)
	}
	if result.Materialization.ForkRunID == "" {
		t.Fatalf("expected materialization before publish failure, got %#v", result.Materialization)
	}
	if result.Activation.SourceFrozen || result.Activation.ForkRunStatus == store.RunForkActivatedStatus {
		t.Fatalf("activation mutated before publish failure cleanup: %#v", result.Activation)
	}

	assertSelectedContractExecutionCleanup(t, db, sourceRunID, result.Materialization.ForkRunID)
}

func TestExecuteSelectedContractRunForkBranchesWhenSourceAdvancedAfterForkPoint(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()
	repoRoot := runForkExecutionRepoRoot(t)
	contractsRoot := filepath.Join(repoRoot, "tests/tier1-primitives/test-emits-multiple")
	platformSpecPath := runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	loader := ContractBundleSourceLoader{RepoRoot: repoRoot, PlatformSpecPath: platformSpecPath}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: contractsRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}

	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	afterEventID := uuid.NewString()
	afterDeliveryID := uuid.NewString()
	at := time.Unix(1700002350, 0).UTC()
	seedSelectedExecutionSourceRun(t, db, sourceRunID, entityID, sourceEventID, "item.received", at)
	captureSelectedExecutionSourceRevision(t, db, sourceRunID)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'source.after', $3::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'source-runtime', 'platform', $4)
	`, sourceRunID, afterEventID, entityID, at.Add(time.Second)); err != nil {
		t.Fatalf("seed post-fork source event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, status, reason_code, created_at
		)
		VALUES ($1::uuid, $2::uuid, $3::uuid, 'node', 'source-post-t-node', 'delivered', 'source_post_t_delivery', $4)
	`, afterDeliveryID, sourceRunID, afterEventID, at.Add(1500*time.Millisecond)); err != nil {
		t.Fatalf("seed post-fork source delivery: %v", err)
	}
	seedSourceOutcomeThatMustNotSuppressFork(t, db, afterEventID, entityID, at.Add(2*time.Second))
	if _, err := db.ExecContext(ctx, `
		UPDATE runs
		SET status = 'completed', ended_at = $2
		WHERE run_id = $1::uuid
	`, sourceRunID, at.Add(3*time.Second)); err != nil {
		t.Fatalf("mark source complete after fork point: %v", err)
	}
	captureSelectedExecutionSourceRevision(t, db, sourceRunID,
		runforkrevision.FamilyEvents,
		runforkrevision.FamilyEventDeliveries,
		runforkrevision.FamilyEventReceipts,
		runforkrevision.FamilyDeadLetters,
	)

	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID:  sourceRunID,
		At:           sourceEventID,
		Store:        pg,
		SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(
			loaded.Source,
			contractsRoot,
		),
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if !result.Activation.Activated || result.Activation.ForkRunStatus != store.RunForkActivatedStatus {
		t.Fatalf("activation = %#v, want activated fork", result.Activation)
	}
	if result.Activation.SourceFrozen || !result.Activation.SourceAdvancedAfterFork {
		t.Fatalf("branch activation flags = frozen:%v advanced:%v", result.Activation.SourceFrozen, result.Activation.SourceAdvancedAfterFork)
	}
	if result.Activation.BranchDivergence == nil {
		t.Fatalf("branch divergence missing from result: %#v", result.Activation)
	}
	if result.Activation.BranchDivergence.Owner != store.RunForkSelectedContractBranchDivergenceOwner ||
		result.Activation.BranchDivergence.Policy != store.RunForkSelectedContractSourceAdvancedBranchPolicy ||
		result.Activation.BranchDivergence.SourceFrozen {
		t.Fatalf("branch divergence = %#v", result.Activation.BranchDivergence)
	}
	for _, fact := range []string{
		"source_events_advanced_after_fork_point",
		"source_run_terminal_at_activation",
		"source_deliveries_advanced_after_fork_point",
		"source_receipts_advanced_after_fork_point",
		"source_dead_letters_advanced_after_fork_point",
	} {
		if !containsString(result.Activation.BranchDivergence.SourceAdvancedFacts, fact) {
			t.Fatalf("branch facts = %#v, want %s", result.Activation.BranchDivergence.SourceAdvancedFacts, fact)
		}
	}

	var sourceStatus, forkStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, result.Materialization.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status: %v", err)
	}
	if sourceStatus != "completed" || forkStatus != store.RunForkActivatedStatus {
		t.Fatalf("branch statuses source/fork = %s/%s, want completed/running", sourceStatus, forkStatus)
	}

	var branchRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_branch_divergences
		WHERE fork_run_id = $1::uuid
		  AND source_run_id = $2::uuid
		  AND fork_event_id = $3::uuid
		  AND policy = $4
		  AND source_frozen = false
		  AND source_run_status_at_activation = 'completed'
		  AND source_run_status_after_activation = 'completed'
		  AND source_advanced_facts @> ARRAY[
				'source_events_advanced_after_fork_point',
				'source_run_terminal_at_activation',
				'source_deliveries_advanced_after_fork_point',
				'source_receipts_advanced_after_fork_point',
				'source_dead_letters_advanced_after_fork_point'
		  ]::text[]
	`, result.Materialization.ForkRunID, sourceRunID, sourceEventID, store.RunForkSelectedContractSourceAdvancedBranchPolicy).Scan(&branchRows); err != nil {
		t.Fatalf("count branch divergence rows: %v", err)
	}
	if branchRows != 1 {
		t.Fatalf("branch divergence rows = %d, want 1", branchRows)
	}

	forkEventID := result.ForkEvents[0].ForkEventID
	var copiedPostTEvents, copiedPostTDeliveries, forkReceipts, emittedFollowUps int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_id = $2::uuid`, result.Materialization.ForkRunID, afterEventID).Scan(&copiedPostTEvents); err != nil {
		t.Fatalf("count copied post-T source event: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid AND event_id = $2::uuid`, result.Materialization.ForkRunID, afterEventID).Scan(&copiedPostTDeliveries); err != nil {
		t.Fatalf("count copied post-T source delivery: %v", err)
	}
	if copiedPostTEvents != 0 || copiedPostTDeliveries != 0 {
		t.Fatalf("copied post-T source rows into fork events=%d deliveries=%d", copiedPostTEvents, copiedPostTDeliveries)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts WHERE event_id = $1::uuid`, forkEventID).Scan(&forkReceipts); err != nil {
		t.Fatalf("count fork receipts: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'item.processed'
		  AND source_event_id = $2::uuid
	`, result.Materialization.ForkRunID, forkEventID).Scan(&emittedFollowUps); err != nil {
		t.Fatalf("count branch follow-ups: %v", err)
	}
	if forkReceipts == 0 || emittedFollowUps != 1 {
		t.Fatalf("branch fork-local outcomes receipts=%d followUps=%d, want receipts and one follow-up", forkReceipts, emittedFollowUps)
	}
}

func TestSelectedContractRecipientPlanPublishGuardAuthorizesCanonicalPlan(t *testing.T) {
	frontier := testContractFrontierAdmission(testContractSelection())
	sourceEventID := frontier.FrontierEvents[0].SourceEventID
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission)
	planning, err := BuildSelectedContractRecipientPlanning(SelectedContractRecipientPlanningRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractRecipientPlanning: %v", err)
	}
	guard, err := newSelectedContractRecipientPlanPublishGuard(planning)
	if err != nil {
		t.Fatalf("newSelectedContractRecipientPlanPublishGuard: %v", err)
	}
	guard.ExpectForkEvent("fork-event", sourceEventID)

	err = guard.AuthorizeEvent(context.Background(), eventtest.RootIngress("fork-event",
		"work.begin",
		store.RunForkSelectedContractExecutionOwner, "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}))
	if err != nil {
		t.Fatalf("AuthorizeEvent canonical recipient plan: %v", err)
	}

	err = guard.Authorize(context.Background(), eventtest.RootIngress("fork-event",
		"work.begin",
		store.RunForkSelectedContractExecutionOwner, "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),

		bus.PublishRecipientPlan{
			RoutedRecipients: []bus.PublishDiagnosticRecipient{{
				Type:        "node",
				ID:          "alpha-intake",
				Path:        "flow-a/alpha-intake",
				RouteSource: "selected_contracts",
			}},
		})
	if err != nil {
		t.Fatalf("Authorize canonical recipient plan: %v", err)
	}
}

func TestSelectedContractRecipientPlanPublishGuardScopesPathDriftToFreshCreateProjection(t *testing.T) {
	planning := store.RunForkSelectedContractRecipientPlanning{
		Owner:                      store.RunForkSelectedContractRecipientPlanningOwner,
		FutureExecutionOwner:       store.RunForkSelectedContractExecutionOwner,
		NonMutating:                true,
		RecipientPlanningSupported: true,
		DeliveryWritesSupported:    false,
		RecipientPlanEvents: []store.RunForkSelectedContractRecipientPlanEvent{{
			SourceEventID: "source-event",
			EventName:     "validation.requested",
			Recipients: []store.RunForkContractFrontierRecipient{{
				SubscriberType: "node",
				SubscriberID:   "validator-node",
				Path:           "validator/source-instance",
				RouteSource:    "canonical_connect",
			}},
			Disposition: store.RunForkSelectedContractDispositionForkLocalTruth,
		}},
	}
	guard, err := newSelectedContractRecipientPlanPublishGuard(planning)
	if err != nil {
		t.Fatalf("newSelectedContractRecipientPlanPublishGuard: %v", err)
	}
	guard.ExpectForkEvent("fork-event", "source-event")
	evt := eventtest.RootIngress("fork-event",
		"validation.requested",
		store.RunForkSelectedContractExecutionOwner, "", nil, 0, "", "", events.EventEnvelope{}, time.Time{})
	projection, err := events.NewDeliveryPayloadProjection(map[string]string{"validation_case_id": "fork-case"})
	if err != nil {
		t.Fatalf("NewDeliveryPayloadProjection: %v", err)
	}
	base := bus.PublishRecipientPlan{
		RoutedRecipients: []bus.PublishDiagnosticRecipient{{
			Type:        "node",
			ID:          "validator-node",
			Path:        "validator/fork-instance",
			RouteSource: "canonical_connect",
		}},
	}

	tests := []struct {
		name    string
		routes  []events.DeliveryRoute
		wantErr bool
	}{
		{
			name: "create fresh projected route accepts fork-local path",
			routes: []events.DeliveryRoute{{
				SubscriberType:    "node",
				SubscriberID:      "validator-node",
				Target:            events.RouteIdentity{FlowID: "validator", FlowInstance: "validator/fork-instance", EntityID: "fork-case"},
				PayloadProjection: projection,
			}},
		},
		{
			name: "select canonical path drift is rejected",
			routes: []events.DeliveryRoute{{
				SubscriberType: "node",
				SubscriberID:   "validator-node",
				Target:         events.RouteIdentity{FlowID: "validator", FlowInstance: "validator/fork-instance", EntityID: "fork-case"},
			}},
			wantErr: true,
		},
		{
			name: "select-or-create canonical path drift is rejected",
			routes: []events.DeliveryRoute{{
				SubscriberType: "node",
				SubscriberID:   "validator-node",
				Target:         events.RouteIdentity{FlowID: "validator", FlowInstance: "validator/fork-instance", EntityID: "fork-case"},
			}},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := base
			actual.DeliveryRoutes = tc.routes
			err := guard.Authorize(context.Background(), evt, actual)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "routed recipients do not match") {
					t.Fatalf("Authorize error = %v, want concrete-path mismatch", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Authorize fresh create projection: %v", err)
			}
		})
	}
}

func TestSelectedContractRecipientPlanPublishGuardMaterializesTargetNodeDeliveryRoutes(t *testing.T) {
	planning := store.RunForkSelectedContractRecipientPlanning{
		Owner:                      store.RunForkSelectedContractRecipientPlanningOwner,
		FutureExecutionOwner:       store.RunForkSelectedContractExecutionOwner,
		NonMutating:                true,
		RecipientPlanningSupported: true,
		DeliveryWritesSupported:    false,
		RecipientPlanEvents: []store.RunForkSelectedContractRecipientPlanEvent{{
			SourceEventID: "source-event",
			EventName:     "item.received",
			Recipients: []store.RunForkContractFrontierRecipient{
				{
					SubscriberType: "agent",
					SubscriberID:   "target-agent",
					RouteSource:    "selected_contracts",
				},
				{
					SubscriberType: "node",
					SubscriberID:   "test-node",
					RouteSource:    "selected_contracts",
				},
			},
			Disposition: store.RunForkSelectedContractDispositionForkLocalTruth,
		}},
	}
	guard, err := newSelectedContractRecipientPlanPublishGuard(planning)
	if err != nil {
		t.Fatalf("newSelectedContractRecipientPlanPublishGuard: %v", err)
	}
	guard.ExpectForkEvent("fork-event", "source-event")

	routes, err := guard.MaterializeNodeDeliveryRoutes(context.Background(), eventtest.RootIngress("fork-event",
		"item.received",
		store.RunForkSelectedContractExecutionOwner, "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),

		bus.PublishRecipientPlan{
			RoutedRecipients: []bus.PublishDiagnosticRecipient{
				{
					Type:        "agent",
					ID:          "target-agent",
					RouteSource: "selected_contracts",
				},
				{
					Type:        "node",
					ID:          "test-node",
					RouteSource: "selected_contracts",
				},
			},
		})
	if err != nil {
		t.Fatalf("MaterializeNodeDeliveryRoutes: %v", err)
	}
	if len(routes) != 1 ||
		routes[0].SubscriberType != "node" ||
		routes[0].SubscriberID != "test-node" ||
		!routes[0].Target.Empty() {
		t.Fatalf("materialized routes = %#v, want target node route only", routes)
	}
}

func TestSelectedContractRecipientPlanPublishGuardAuthorizesContractSwapOwner(t *testing.T) {
	frontier := testContractFrontierAdmission(testContractSelection())
	sourceEventID := frontier.FrontierEvents[0].SourceEventID
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission)
	planning, err := BuildSelectedContractRecipientPlanning(SelectedContractRecipientPlanningRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractRecipientPlanning: %v", err)
	}
	guard, err := newSelectedContractRecipientPlanPublishGuard(planning, store.RunForkHistoricalReplayContractSwapBootResumeOwner)
	if err != nil {
		t.Fatalf("newSelectedContractRecipientPlanPublishGuard: %v", err)
	}
	guard.ExpectForkEvent("fork-event", sourceEventID)

	err = guard.Authorize(context.Background(), eventtest.RootIngress("fork-event",
		"work.begin",
		store.RunForkHistoricalReplayContractSwapBootResumeOwner, "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),

		bus.PublishRecipientPlan{
			RoutedRecipients: []bus.PublishDiagnosticRecipient{{
				Type:        "node",
				ID:          "alpha-intake",
				Path:        "flow-a/alpha-intake",
				RouteSource: "selected_contracts",
			}},
		})
	if err != nil {
		t.Fatalf("Authorize contract-swap owner recipient plan: %v", err)
	}
}

func TestSelectedContractRecipientPlanPublishGuardRejectsBypassAndSubscriptions(t *testing.T) {
	frontier := testContractFrontierAdmission(testContractSelection())
	sourceEventID := frontier.FrontierEvents[0].SourceEventID
	routeAdmission := testSelectedContractRouteAdmission(frontier)
	routeTopology := testSelectedContractRouteTopologyFromAdmission(t, frontier, routeAdmission)
	planning, err := BuildSelectedContractRecipientPlanning(SelectedContractRecipientPlanningRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
		RouteTopology:  routeTopology,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractRecipientPlanning: %v", err)
	}
	guard, err := newSelectedContractRecipientPlanPublishGuard(planning)
	if err != nil {
		t.Fatalf("newSelectedContractRecipientPlanPublishGuard: %v", err)
	}

	err = guard.Authorize(context.Background(), eventtest.RootIngress("fork-event",
		"work.begin",
		store.RunForkSelectedContractExecutionOwner, "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),

		bus.PublishRecipientPlan{
			RoutedRecipients: []bus.PublishDiagnosticRecipient{{
				Type:        "node",
				ID:          "alpha-intake",
				Path:        "flow-a/alpha-intake",
				RouteSource: "selected_contracts",
			}},
		})
	if err == nil || !strings.Contains(err.Error(), store.RunForkSelectedContractRecipientPlanningOwner) {
		t.Fatalf("Authorize without expectation error = %v, want recipient-planning evidence failure", err)
	}

	guard.ExpectForkEvent("fork-event-subscription", sourceEventID)
	err = guard.Authorize(context.Background(), eventtest.RootIngress("fork-event-subscription",
		"work.begin",
		store.RunForkSelectedContractExecutionOwner, "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),

		bus.PublishRecipientPlan{
			RoutedRecipients: []bus.PublishDiagnosticRecipient{{
				Type:        "node",
				ID:          "alpha-intake",
				Path:        "flow-a/alpha-intake",
				RouteSource: "selected_contracts",
			}},
			SubscriptionRecipients: []string{"legacy-subscription"},
		})
	if err == nil || !strings.Contains(err.Error(), "live subscription") {
		t.Fatalf("Authorize subscription recipient error = %v, want live subscription rejection", err)
	}

	guard.ExpectForkEvent("fork-event-wrong-recipient", sourceEventID)
	err = guard.Authorize(context.Background(), eventtest.RootIngress("fork-event-wrong-recipient",
		"work.begin",
		store.RunForkSelectedContractExecutionOwner, "", nil, 0, "", "", events.EventEnvelope{}, time.Time{}),

		bus.PublishRecipientPlan{
			RoutedRecipients: []bus.PublishDiagnosticRecipient{{
				Type:        "node",
				ID:          "other-node",
				Path:        "flow-a/other-node",
				RouteSource: "selected_contracts",
			}},
		})
	if err == nil || !strings.Contains(err.Error(), "routed recipients do not match") {
		t.Fatalf("Authorize wrong recipient error = %v, want recipient-plan mismatch", err)
	}
}

func assertSelectedContractRuntimeContainerProof(t *testing.T, proof *SelectedContractForkLocalRuntimeContainer, executionOwner, sourceRunID, forkRunID, forkEventID string, sourceEventIDs []string) {
	t.Helper()
	if proof == nil {
		t.Fatal("fork-local runtime container proof missing")
	}
	if proof.Owner != store.RunForkSelectedContractForkLocalRuntimeContainerOwner ||
		proof.ExecutionOwner != executionOwner ||
		proof.SourceRunID != sourceRunID ||
		proof.ForkRunID != forkRunID ||
		proof.ForkEventID != forkEventID {
		t.Fatalf("runtime container identity = %#v", proof)
	}
	if proof.RecipientPlanningOwner != store.RunForkSelectedContractRecipientPlanningOwner ||
		proof.AuthoritativeAgentDeliveryMaterializationOwner != store.RunForkSelectedContractAuthoritativeAgentDeliveryMaterializationOwner ||
		proof.RuntimePlatformEventLineagePolicyOwner != store.RunForkSelectedContractForkLocalRuntimePlatformEventLineagePolicyOwner ||
		proof.TypedRuntimeLineageOwner != store.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner ||
		proof.RouteRecoveryOwner != store.RunForkSelectedContractRouteRecoveryOwner ||
		proof.ActivationGateOwner != store.RunForkSelectedContractExecutionActivationGateOwner {
		t.Fatalf("runtime container owner consumption = %#v", proof)
	}
	if !proof.EventBusRecipientPlanGuard ||
		!proof.RuntimeActiveAgentDescriptorsEphemeral ||
		!proof.EphemeralAgentRuntime ||
		!proof.QuiescenceRequired ||
		!proof.CleanupRequired {
		t.Fatalf("runtime container lifecycle proof = %#v", proof)
	}
	for _, sourceEventID := range sourceEventIDs {
		if !containsString(proof.SourceEventIDs, sourceEventID) {
			t.Fatalf("runtime container source events = %#v, want %s", proof.SourceEventIDs, sourceEventID)
		}
	}
	if !executionBoundaryHas(proof.InvalidPaths, "source_row_copy_as_execution_truth", store.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("runtime container invalid paths = %#v, want source-row-copy invalid", proof.InvalidPaths)
	}
	if executionBoundaryHas(proof.SplitSiblings, "typed_runtime_lineage", store.RunForkSelectedContractDispositionBlockedSibling) {
		t.Fatalf("runtime container split siblings = %#v, typed lineage should be implemented by #708", proof.SplitSiblings)
	}
}

func assertSelectedContractExecutionCleanup(t *testing.T, db *sql.DB, sourceRunID, forkRunID string) {
	t.Helper()
	ctx := context.Background()
	var sourceStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if sourceStatus != "running" {
		t.Fatalf("source status = %q, want running", sourceStatus)
	}
	var forkRows, forkEvents, forkDeliveries, forkReceipts, forkState, forkMutations, bindingRows, lineageRows, routeRecoveryRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE forked_from_run_id = $1::uuid`, sourceRunID).Scan(&forkRows); err != nil {
		t.Fatalf("count fork rows: %v", err)
	}
	if forkRunID != "" {
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid`, forkRunID).Scan(&forkEvents); err != nil {
			t.Fatalf("count fork events: %v", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid`, forkRunID).Scan(&forkDeliveries); err != nil {
			t.Fatalf("count fork deliveries: %v", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_receipts`).Scan(&forkReceipts); err != nil {
			t.Fatalf("count event receipts after cleanup: %v", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_state WHERE run_id = $1::uuid`, forkRunID).Scan(&forkState); err != nil {
			t.Fatalf("count fork state: %v", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM entity_mutations WHERE run_id = $1::uuid`, forkRunID).Scan(&forkMutations); err != nil {
			t.Fatalf("count fork mutations: %v", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_bindings WHERE fork_run_id = $1::uuid`, forkRunID).Scan(&bindingRows); err != nil {
			t.Fatalf("count fork binding: %v", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE fork_run_id = $1::uuid`, forkRunID).Scan(&lineageRows); err != nil {
			t.Fatalf("count fork lineage: %v", err)
		}
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_route_recoveries WHERE fork_run_id = $1::uuid`, forkRunID).Scan(&routeRecoveryRows); err != nil {
			t.Fatalf("count fork route recoveries: %v", err)
		}
	}
	var forkStatus string
	if forkRunID != "" {
		if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=$1::uuid`, forkRunID).Scan(&forkStatus); err != nil {
			t.Fatalf("load retained fork tombstone: %v", err)
		}
	}
	if forkRows != 1 || forkStatus != "cancelled" || forkEvents != 0 || forkDeliveries != 0 || forkReceipts != 0 || forkState != 0 || forkMutations != 0 || bindingRows != 1 || lineageRows != 0 || routeRecoveryRows != 0 {
		t.Fatalf("cleanup left fork rows runs:%d events:%d deliveries:%d receipts:%d state:%d mutations:%d bindings:%d lineage:%d route_recoveries:%d",
			forkRows, forkEvents, forkDeliveries, forkReceipts, forkState, forkMutations, bindingRows, lineageRows, routeRecoveryRows)
	}
}

type selectedContractForkTestAgent struct {
	mu       sync.Mutex
	cfg      runtimeactors.AgentConfig
	runIDs   []string
	eventIDs []string
}

func (a *selectedContractForkTestAgent) Configure(cfg runtimeactors.AgentConfig) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.cfg = cfg
}

func (a *selectedContractForkTestAgent) ID() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cfg.ID
}

func (a *selectedContractForkTestAgent) Type() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.cfg.Type
}

func (a *selectedContractForkTestAgent) Subscriptions() []events.EventType {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]events.EventType, 0, len(a.cfg.Subscriptions))
	for _, raw := range a.cfg.Subscriptions {
		if eventType := strings.TrimSpace(raw); eventType != "" {
			out = append(out, events.EventType(eventType))
		}
	}
	return out
}

func (a *selectedContractForkTestAgent) OnEvent(ctx context.Context, evt events.Event) ([]events.Event, error) {
	a.mu.Lock()
	a.runIDs = append(a.runIDs, strings.TrimSpace(evt.RunID()))
	a.eventIDs = append(a.eventIDs, strings.TrimSpace(evt.ID()))
	agentID := strings.TrimSpace(a.cfg.ID)
	a.mu.Unlock()

	return []events.Event{
		eventtest.RootIngress("", events.EventType("task.completed"), agentID, "", json.RawMessage(`{}`), 0, evt.RunID(), evt.ID(), evt.NormalizedEnvelope(), time.Now().UTC()),
	}, nil
}

func (a *selectedContractForkTestAgent) SeenRunIDs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.runIDs...)
}

func (a *selectedContractForkTestAgent) SeenEventIDs() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.eventIDs...)
}

func runForkExecutionRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatalf("repo root: %v", err)
	}
	return root
}

func captureSelectedExecutionSourceRevision(t *testing.T, db *sql.DB, runID string, families ...runforkrevision.Family) int64 {
	t.Helper()
	if len(families) == 0 {
		families = runforkrevision.AllFamilies()
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin selected execution source revision: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	revision, err := runforkrevision.Capture(context.Background(), tx, runID, families...)
	if err != nil {
		t.Fatalf("capture selected execution source revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit selected execution source revision: %v", err)
	}
	return revision
}

func materializeSelectedExecutionForkForTest(
	t *testing.T,
	ctx context.Context,
	pg *store.PostgresStore,
	source semanticview.Source,
	selection store.RunForkContractSelection,
	sourceRunID string,
	sourceEventID string,
) store.RunForkMaterialization {
	t.Helper()
	plan, err := pg.PlanRunFork(ctx, store.RunForkPlanRequest{SourceRunID: sourceRunID, At: sourceEventID})
	if err != nil {
		t.Fatalf("PlanRunFork: %v", err)
	}
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{
		Plan: plan, Source: source, ContractSelection: selection,
	})
	if err != nil {
		t.Fatalf("AdmitContractFrontier: %v", err)
	}
	routeAdmission, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{
		Plan: plan, Source: source, ContractSelection: selection, FrontierAdmission: frontier,
	})
	if err != nil {
		t.Fatalf("AdmitSelectedContractRouteHistory: %v", err)
	}
	topology, err := BuildSelectedContractRouteTopology(SelectedContractRouteTopologyRequest{
		Admission: frontier, RouteAdmission: routeAdmission,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractRouteTopology: %v", err)
	}
	model, err := BuildSelectedContractExecutionModel(SelectedContractExecutionModelRequest{
		Admission: frontier, RouteAdmission: routeAdmission, RouteTopology: topology,
	})
	if err != nil {
		t.Fatalf("BuildSelectedContractExecutionModel: %v", err)
	}
	materialized, err := pg.MaterializeRunForkForSelectedContractExecution(ctx, store.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID: sourceRunID, At: sourceEventID, ContractSelection: selection,
		FrontierAdmission: frontier, RouteTopology: topology, RecipientPlanning: *model.RecipientPlanning,
	})
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution: %v", err)
	}
	return materialized
}

func seedSelectedExecutionSourceRun(t *testing.T, db *sql.DB, sourceRunID, entityID, sourceEventID, eventName string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	payload, _ := json.Marshal(map[string]any{"entity_id": entityID})
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, sourceRunID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed source run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ($1::uuid, $2::uuid, $3, $4::uuid, 'flow-a/1', 'entity', $5::jsonb, 'source-runtime', 'platform', $6)
	`, sourceRunID, sourceEventID, eventName, entityID, string(payload), at); err != nil {
		t.Fatalf("seed source event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, reason_code, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'node', 'test-node', 'pending', 'source_pending_node_delivery', $3)
	`, sourceRunID, sourceEventID, at); err != nil {
		t.Fatalf("seed source delivery: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"pending"'::jsonb, $3::uuid, 'platform', 'selected-execution-test', 'seed', $4),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, '"Selected Execution Entity"'::jsonb, $3::uuid, 'platform', 'selected-execution-test', 'seed', $4)
	`, sourceRunID, entityID, sourceEventID, at); err != nil {
		t.Fatalf("seed source mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'Selected Execution Entity',
			'pending', '{}'::jsonb, '{"name":"Selected Execution Entity"}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, sourceRunID, entityID, at); err != nil {
		t.Fatalf("seed source entity_state: %v", err)
	}
}

func seedSourceOutcomeThatMustNotSuppressFork(t *testing.T, db *sql.DB, sourceEventID, entityID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassConnectorFailure, "source_dead_letter", "run-fork-test", "seed", nil), "run-fork-test", "seed")
	failureRaw, err := json.Marshal(failure)
	if err != nil {
		t.Fatalf("marshal source dead-letter failure: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance, outcome, reason_code, side_effects, processed_at
		)
		VALUES ($1::uuid, 'node', 'old-source-node', $2::uuid, 'flow-a/1', 'success', 'source_outcome_must_not_suppress_fork', '{}'::jsonb, $3)
	`, sourceEventID, entityID, at); err != nil {
		t.Fatalf("seed source receipt: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO dead_letters (
			original_event_id, original_event, entity_id, flow_instance, failure, handler_node, created_at
		)
		VALUES ($1::uuid, 'item.received', $2::uuid, 'flow-a/1', $3::jsonb, 'old-source-node', $4)
	`, sourceEventID, entityID, string(failureRaw), at); err != nil {
		t.Fatalf("seed source dead letter: %v", err)
	}
}

func seedSelectedExecutionDiagnosticPlatformDeadLetter(t *testing.T, db *sql.DB, sourceRunID, diagnosticEventID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	payload, _ := json.Marshal(map[string]any{
		"level":   "info",
		"message": "diagnostic platform row must remain lineage-only",
	})
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'platform.runtime_log', NULL, NULL, 'global',
			$3::jsonb, 'pipeline', 'platform', $4
		)
	`, sourceRunID, diagnosticEventID, string(payload), at); err != nil {
		t.Fatalf("seed diagnostic platform event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, entity_id, flow_instance, outcome, reason_code, side_effects, processed_at
		)
		VALUES (
			$1::uuid, 'platform', 'pipeline', NULL, NULL,
			'dead_letter', 'runtime_log_pipeline_dead_letter', '{}'::jsonb, $2
		)
	`, diagnosticEventID, at); err != nil {
		t.Fatalf("seed diagnostic platform receipt: %v", err)
	}
}

func seedSelectedExecutionSourceReplayScopeMarker(t *testing.T, db *sql.DB, sourceRunID, sourceEventID, reasonCode string, at time.Time) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status,
			retry_count, reason_code, delivered_at, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'node', '__runtime_replay_scope__', 'delivered',
			0, $3, $4, $4
		)
	`, sourceRunID, sourceEventID, reasonCode, at); err != nil {
		t.Fatalf("seed source replay-scope marker: %v", err)
	}
}

func seedSelectedExecutionPostForkSourceEvent(t *testing.T, db *sql.DB, sourceRunID, sourceEventID, entityID string, at time.Time) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO events (
			run_id, event_id, event_name, entity_id, flow_instance, scope,
			payload, produced_by, produced_by_type, created_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'source.after', $3::uuid, 'flow-a/1', 'entity',
			'{}'::jsonb, 'source-runtime', 'platform', $4
		)
	`, sourceRunID, sourceEventID, entityID, at); err != nil {
		t.Fatalf("seed post-fork source event: %v", err)
	}
}

func assertNoForkExecutionRowsForRun(t *testing.T, db *sql.DB, forkRunID string) {
	t.Helper()
	ctx := context.Background()
	for name, query := range map[string]string{
		"events":                                  `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid`,
		"event_deliveries":                        `SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid`,
		"run_fork_selected_contract_executions":   `SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE fork_run_id = $1::uuid`,
		"run_fork_selected_contract_divergences":  `SELECT COUNT(*) FROM run_fork_selected_contract_branch_divergences WHERE fork_run_id = $1::uuid`,
		"run_fork_selected_contract_route_rows":   `SELECT COUNT(*) FROM run_fork_selected_contract_route_recoveries WHERE fork_run_id = $1::uuid`,
		"run_fork_delivery_event_replay_lineages": `SELECT COUNT(*) FROM run_fork_delivery_event_replays WHERE fork_run_id = $1::uuid`,
	} {
		var count int
		if err := db.QueryRowContext(ctx, query, forkRunID).Scan(&count); err != nil {
			t.Fatalf("count %s rows: %v", name, err)
		}
		if count != 0 {
			t.Fatalf("%s rows for blocked selected fork = %d, want 0", name, count)
		}
	}
}

func assertNoSelectedContractExecutionMutationForSource(t *testing.T, db *sql.DB, sourceRunID, sourceEventID string) {
	t.Helper()
	ctx := context.Background()
	for name, query := range map[string]string{
		"fork_runs":                            `SELECT COUNT(*) FROM runs WHERE forked_from_run_id = $1::uuid`,
		"selected_contract_bindings":           `SELECT COUNT(*) FROM run_fork_selected_contract_bindings WHERE source_run_id = $1::uuid`,
		"selected_contract_executions":         `SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE source_run_id = $1::uuid`,
		"selected_contract_branch_divergences": `SELECT COUNT(*) FROM run_fork_selected_contract_branch_divergences WHERE source_run_id = $1::uuid`,
		"selected_contract_route_recoveries":   `SELECT COUNT(*) FROM run_fork_selected_contract_route_recoveries WHERE source_run_id = $1::uuid`,
		"delivery_event_replays":               `SELECT COUNT(*) FROM run_fork_delivery_event_replays WHERE source_run_id = $1::uuid`,
	} {
		var count int
		if err := db.QueryRowContext(ctx, query, sourceRunID).Scan(&count); err != nil {
			t.Fatalf("count %s rows: %v", name, err)
		}
		if count != 0 {
			t.Fatalf("%s rows for blocked selected fork source = %d, want 0", name, count)
		}
	}

	var forkEvents int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE source_event_id = $1::uuid
		  AND run_id <> $2::uuid
	`, sourceEventID, sourceRunID).Scan(&forkEvents); err != nil {
		t.Fatalf("count fork events: %v", err)
	}
	if forkEvents != 0 {
		t.Fatalf("fork event rows for blocked selected fork source event = %d, want 0", forkEvents)
	}
}

func assertNoCopiedSourceReplayScopeMarkers(t *testing.T, db *sql.DB, forkRunID, sourceEventID string) {
	t.Helper()
	var copied int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = '__runtime_replay_scope__'
		  AND reason_code IN ('replay_scope_direct', 'replay_scope_subscribed')
	`, forkRunID, sourceEventID).Scan(&copied); err != nil {
		t.Fatalf("count copied source replay-scope markers: %v", err)
	}
	if copied != 0 {
		t.Fatalf("copied source replay-scope markers = %d, want 0", copied)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func selectedExecutionResultHasBlocker(result SelectedContractExecutionResult, code string) bool {
	for _, blocker := range result.Materialization.UnsupportedBlockers {
		if blocker.Code == code {
			return true
		}
	}
	for _, blocker := range result.Activation.UnsupportedBlockers {
		if blocker.Code == code {
			return true
		}
	}
	return false
}
