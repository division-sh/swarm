package cataloge2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeagenttopology "github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimeruncontrol "github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	runtimerunforkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestTier12RuntimeFork_SelectedContractForkExecutionFixture(t *testing.T) {
	canonicalrouting.Prove(t,
		canonicalrouting.ArtifactID("tests/tier12-runtime-fork/test-non-agent-replay-fail-closed"),
		canonicalrouting.ArtifactID("tests/tier12-runtime-fork/test-selected-contract-fork-execution"),
	)
	repoRoot := repoRootFromCatalogE2E(t)
	fixtures := catalogRuntimeFixtures(t, "catalog.runtime.selected_contract_fork")
	if len(fixtures) != 2 {
		t.Fatalf("selected-contract fork runtime fixtures = %d, want 2", len(fixtures))
	}
	fixtureRoot := catalogRuntimeFixture(t, "catalog.runtime.selected_contract_fork", "test-selected-contract-fork-execution").Root

	var expected catalogExpectedDocument
	loadYAML(t, catalogExpectedPath(fixtureRoot), &expected)

	h := newRuntimeHarness(t, fixtureRoot, true)
	// Source execution is paused at T; register recipient evidence through runtime APIs before publishing.
	declarationOwner, ok := semanticview.AgentDeclarationOwner(semanticview.Wrap(h.bundle), ".", "test-agent")
	if !ok {
		t.Fatal("resolve test-agent declaration owner")
	}
	name, err := agentidentity.DeclaredName("test-agent", declarationOwner)
	if err != nil {
		t.Fatalf("build test-agent declaration name: %v", err)
	}
	identity, err := agentidentity.New(name, agentidentity.RootRoute())
	if err != nil {
		t.Fatalf("build test-agent identity: %v", err)
	}
	h.rt.Bus.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{
		Identity: identity,
	})
	h.seedInitialState(runtimepipeline.FlowInstanceEntityID(catalogRuntimeRunID))
	pauseCatalogRun(t, h)
	sourceEventID := publishCatalogTrigger(t, h, expected.triggerSequence()[0], 10*time.Second)
	sourceRunID := catalogRuntimeRunID
	assertSourcePendingAgentDelivery(t, h.db, sourceRunID, sourceEventID, "test-agent")
	forkAt := sourceEventID
	sourceBefore := selectedContractSourceRunCounts(t, h.db, sourceRunID)
	sourceRowsBefore := selectedContractSourceRowSnapshot(t, h.db, sourceRunID, sourceEventID)

	loader, selection, selectedSource := selectedContractForkFixtureSelection(t, h.ctx, repoRoot, fixtureRoot, h.pg)
	selectedBundle, ok := semanticview.Bundle(selectedSource.Source)
	if !ok {
		t.Fatal("selected-contract source has no loader-owned bundle")
	}
	storetest.RequireBundleDataCatalog(t, h.ctx, h.pg, selectedBundle)
	installCatalogSelectedSourceTopology(t, h.ctx, h, selectedSource)
	materialized, err := materializeSelectedContractForkCleanupProbe(t, h.ctx, h.pg, loader, selection, sourceRunID, forkAt)
	if err != nil {
		t.Fatalf("MaterializeRunForkForSelectedContractExecution cleanup probe: %v", err)
	}
	if materialized.ForkRunID == "" {
		t.Fatalf("cleanup probe materialization = %#v", materialized)
	}
	if err := h.pg.DiscardMaterializedSelectedContractExecutionFork(h.ctx, materialized.ForkRunID); err != nil {
		t.Fatalf("DiscardMaterializedSelectedContractExecutionFork: %v", err)
	}
	assertNoForkArtifacts(t, h.db, materialized.ForkRunID)
	assertRunCountsUnchanged(t, sourceBefore, selectedContractSourceRunCounts(t, h.db, sourceRunID), "source run after cleanup probe")
	assertSourceRowsFrozen(t, sourceRowsBefore, selectedContractSourceRowSnapshot(t, h.db, sourceRunID, sourceEventID), "source rows after cleanup probe")
	assertSourceRunLifecycle(t, h.db, sourceRunID, "paused", false)

	cfg := testRuntimeConfig()
	cfg.LLM.Backend = "anthropic"
	executionCtx := worklifetime.WithOccurrence(h.ctx, h.rt.WorkOccurrence())
	executionOwner := selectedContractExecutionOwnerForCatalogTest(t, h.db, h.pg)
	result, err := runtimerunforkexecution.ExecuteSelectedContractRunFork(executionCtx, runtimerunforkexecution.SelectedContractExecutionRequest{
		SourceRunID:         sourceRunID,
		At:                  forkAt,
		ConfirmSourceFreeze: true,
		Owner:               executionOwner,
		SourceLoader:        loader,
		ContractSelection:   selection,
		AgentRuntime: runtimerunforkexecution.SelectedContractAgentRuntimeOptions{
			Config:            cfg,
			ProcessCapability: h.processTopology,
			ExecutionPosture:  executionposture.Live,
			EntityStore:       h.pg,
			HumanTaskStore:    h.pg,
			SessionRegistry:   h.pg,
			ConversationStore: h.pg,
			MailboxStore:      h.pg,
			LLMRuntime:        h.llm,
			QuiescenceTimeout: 10 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.Owner != runfork.RunForkSelectedContractExecutionOwner ||
		result.Materialization.ForkRunID == "" ||
		!result.Activation.Activated ||
		result.ExecutedEventCount != 1 ||
		len(result.ForkEvents) != 1 {
		t.Fatalf("selected execution result = %#v", result)
	}
	if result.AgentRuntimeMaterialization == nil ||
		result.AgentRuntimeMaterialization.Owner != runfork.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner ||
		!result.AgentRuntimeMaterialization.MaterializationRequired ||
		!result.AgentRuntimeMaterialization.MaterializationSupported ||
		!containsTier12AgentID(result.AgentRuntimeMaterialization.AgentRecipients, "test-agent") ||
		!containsTier12AgentID(result.AgentRuntimeMaterialization.ConfiguredAgentIdentities, "test-agent") {
		t.Fatalf("agent runtime materialization = %#v", result.AgentRuntimeMaterialization)
	}
	if result.ForkLocalRuntimeContainer == nil ||
		result.ForkLocalRuntimeContainer.Owner != runfork.RunForkSelectedContractForkLocalRuntimeContainerOwner ||
		result.ForkLocalRuntimeContainer.TypedRuntimeLineageOwner != runfork.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner ||
		result.ForkLocalRuntimeContainer.ForkRunID != result.Materialization.ForkRunID {
		t.Fatalf("fork-local runtime container proof = %#v", result.ForkLocalRuntimeContainer)
	}

	forkRunID := result.Materialization.ForkRunID
	forkEventID := result.ForkEvents[0].ForkEventID
	if result.ForkEvents[0].SourceEventID != sourceEventID || forkEventID == "" || forkEventID == sourceEventID {
		t.Fatalf("fork event lineage = %#v, source event %s", result.ForkEvents[0], sourceEventID)
	}
	assertSelectedContractForkExecutionRows(t, h.db, sourceRunID, forkRunID, sourceEventID, forkEventID)
	assertSelectedContractForkRuntimeRows(t, h.db, forkRunID, forkEventID)
	assertSelectedContractForkSourceIsolation(t, h.db, sourceRunID, forkRunID, sourceEventID, forkEventID)
	assertRunCountsUnchanged(t, sourceBefore, selectedContractSourceRunCounts(t, h.db, sourceRunID), "source run after selected execution")
	assertSourceRowsFrozen(t, sourceRowsBefore, selectedContractSourceRowSnapshot(t, h.db, sourceRunID, sourceEventID), "source rows after selected execution")
	if result.Activation.SourceFrozen || !result.Activation.SourceAdvancedAfterFork || result.Activation.BranchDivergence == nil ||
		!containsTier12String(result.Activation.BranchDivergence.SourceAdvancedFacts, "source_events_advanced_after_fork_point") {
		t.Fatalf("selected-contract source-advanced branch activation = %#v", result.Activation)
	}
	assertSourceRunLifecycle(t, h.db, sourceRunID, "paused", false)
	negativeFixtureRoot := catalogRuntimeFixture(t, "catalog.runtime.selected_contract_fork", "test-non-agent-replay-fail-closed").Root
	assertUnsupportedHistoricalReplayFailsClosed(t, negativeFixtureRoot)
}

func selectedContractExecutionOwnerForCatalogTest(t testing.TB, db *sql.DB, selected *store.PostgresStore) runtimerunforkexecution.SelectedContractExecutionOwner {
	t.Helper()
	durable := runtimebus.DurableDependencies{
		ReplyContext: selected, RunLifecycle: selected, DeliveryLifecycle: selected,
		FlowRoutes: selected, FlowRouteRecords: selected, FlowRouteSets: selected, FlowRouteTopology: selected, FlowRouteRollback: selected,
		ActiveAgents: selected, ActiveFlows: selected, TargetOwners: selected, WorkflowInstances: selected, PreparedEvents: selected,
		TargetFailureRecorder: selected, RunOrigins: selected, StandingRestarts: selected,
	}
	managerRoles := runtimemanager.PersistenceRoles{
		LifecycleState: selected, LifecycleEffects: selected, LifecycleDiagnostics: selected, EffectsRecovery: selected,
		DeliveryQuiescence: selected, EventExistence: selected, DirectiveOperations: selected, DirectiveTargets: selected,
		FlowRoutes: selected, StandingRestarts: selected,
	}
	owner, err := runtimerunforkexecution.NewSelectedContractExecutionOwner(
		runtimepipeline.NewWorkflowPersistence(selected), selected, selected, selected,
		selected, durable, selected.PipelineObligations(), selected, managerRoles,
		selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected, selected,
	)
	if err != nil {
		t.Fatalf("NewSelectedContractExecutionOwner: %v", err)
	}
	return owner
}

func selectedContractForkFixtureSelection(t testing.TB, ctx context.Context, repoRoot, fixtureRoot string, selected *store.PostgresStore) (runtimerunforkexecution.SelectedContractSourceLoader, runfork.RunForkContractSelection, runtimerunforkexecution.LoadedSelectedContractSource) {
	t.Helper()
	bundle := loadFixtureBundle(t, fixtureRoot)
	storetest.RequireBundleDataCatalog(t, ctx, selected, bundle)
	loader := runtimerunforkexecution.SourceArtifactSelectedContractSourceLoader{
		RepoRoot:         repoRoot,
		PlatformSpecPath: platformSpecPathFromCatalogE2E(t),
		Store:            selected,
	}
	selection := runfork.RunForkContractSelection{
		Mode: runfork.RunForkContractSelectionModeBundleHash, BundleHash: bundle.SourceArtifact.BundleHash(),
	}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, selection)
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}
	if loaded.Cleanup != nil {
		t.Cleanup(func() {
			if err := loaded.Cleanup(); err != nil {
				t.Errorf("cleanup selected-contract fixture source: %v", err)
			}
		})
	}
	return loader, selection, loaded
}

func installCatalogSelectedSourceTopology(t testing.TB, ctx context.Context, h *runtimeHarness, loaded runtimerunforkexecution.LoadedSelectedContractSource) {
	t.Helper()
	if h == nil || h.processTopology == nil || h.rt == nil || h.rt.Manager == nil {
		t.Fatal("catalog selected source topology requires a live process capability")
	}
	bundleHash := loaded.SourceArtifactFact.BundleHash()
	coordinate := runtimeagenttopology.SourceCoordinate{BundleHash: bundleHash}
	desired, err := h.rt.Manager.CompileStaticTopologyDesiredAgents(loaded.Source, coordinate)
	if err != nil {
		t.Fatalf("compile selected-contract source topology: %v", err)
	}
	current, exists, err := h.processTopology.CurrentSourceSet(ctx)
	if err != nil || !exists {
		t.Fatalf("load catalog process source set: exists=%t err=%v", exists, err)
	}
	sources := append([]runtimeagenttopology.SourceCoordinate(nil), current.Sources...)
	sourcePresent := false
	for _, source := range sources {
		if source.Normalize().Key() == coordinate.Normalize().Key() {
			sourcePresent = true
			break
		}
	}
	if !sourcePresent {
		sources = append(sources, coordinate)
	}
	agents := append([]runtimeagenttopology.DesiredAgent(nil), current.Agents...)
	for _, candidate := range desired {
		key, keyErr := candidate.Key()
		if keyErr != nil {
			t.Fatalf("selected-contract desired agent key: %v", keyErr)
		}
		replaced := false
		for i := range agents {
			existingKey, existingErr := agents[i].Key()
			if existingErr != nil {
				t.Fatalf("current desired agent key: %v", existingErr)
			}
			if existingKey == key {
				agents[i] = candidate
				replaced = true
				break
			}
		}
		if !replaced {
			agents = append(agents, candidate)
		}
	}
	plan, err := runtimeagenttopology.NewSourceSetPlan(sources, agents)
	if err != nil {
		t.Fatalf("construct selected-contract complete source set: %v", err)
	}
	if plan.Revision == current.Revision {
		return
	}
	if _, err := h.processTopology.RestoreSourceSet(ctx, runtimeagenttopology.SourceSetCommitRequest{
		OperationID: uuid.NewString(), ExpectedRevision: current.Revision, Plan: plan,
	}); err != nil {
		t.Fatalf("commit selected-contract complete source set: %v", err)
	}
}

func materializeSelectedContractForkCleanupProbe(
	t testing.TB,
	ctx context.Context,
	pg *store.PostgresStore,
	loader runtimerunforkexecution.SelectedContractSourceLoader,
	selection runfork.RunForkContractSelection,
	sourceRunID,
	forkAt string,
) (runfork.RunForkMaterialization, error) {
	t.Helper()
	loaded, err := loader.LoadRunForkSelectedContractSourceForRequest(ctx, runtimerunforkexecution.SelectedContractSourceLoadRequest{
		Selection: selection,
	})
	if err != nil {
		t.Fatalf("load cleanup-probe selected source: %v", err)
	}
	if loaded.Cleanup != nil {
		defer func() {
			if err := loaded.Cleanup(); err != nil {
				t.Errorf("cleanup cleanup-probe selected source: %v", err)
			}
		}()
	}
	selection = loaded.Selection
	plan, err := pg.PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: sourceRunID, At: forkAt})
	if err != nil {
		t.Fatalf("plan cleanup-probe fork: %v", err)
	}
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{
		Plan:              plan,
		Source:            loaded.Source,
		ContractSelection: selection,
	})
	if err != nil {
		t.Fatalf("admit cleanup-probe frontier: %v", err)
	}
	routeAdmission, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{
		Plan:              plan,
		Source:            loaded.Source,
		ContractSelection: selection,
		FrontierAdmission: frontier,
	})
	if err != nil {
		t.Fatalf("admit cleanup-probe route history: %v", err)
	}
	topology, err := runtimerunforkexecution.BuildSelectedContractRouteTopology(runtimerunforkexecution.SelectedContractRouteTopologyRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
	})
	if err != nil {
		t.Fatalf("build cleanup-probe route topology: %v", err)
	}
	model, err := runtimerunforkexecution.BuildSelectedContractExecutionModel(runtimerunforkexecution.SelectedContractExecutionModelRequest{
		Admission:      frontier,
		RouteAdmission: routeAdmission,
		RouteTopology:  topology,
	})
	if err != nil {
		t.Fatalf("build cleanup-probe execution model: %v", err)
	}
	if model.RecipientPlanning == nil {
		t.Fatal("cleanup-probe execution model has no recipient planning")
	}
	return pg.MaterializeRunForkForSelectedContractExecution(ctx, runfork.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID:       sourceRunID,
		At:                forkAt,
		ContractSelection: selection,
		FrontierAdmission: frontier,
		RouteTopology:     topology,
		RecipientPlanning: *model.RecipientPlanning,
	})
}

func assertSourcePendingAgentDelivery(t testing.TB, db *sql.DB, runID, eventID, agentID string) {
	t.Helper()
	var rows int
	if err := db.QueryRowContext(testAuthorActivityContext(context.Background()), `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = $3
		  AND status = 'pending'
	`, runID, eventID, agentID).Scan(&rows); err != nil {
		t.Fatalf("count source pending agent delivery: %v", err)
	}
	if rows != 1 {
		t.Fatalf("source pending agent delivery rows = %d, want 1 for event %s agent %s", rows, eventID, agentID)
	}
}

func assertSelectedContractForkExecutionRows(t testing.TB, db *sql.DB, sourceRunID, forkRunID, sourceEventID, forkEventID string) {
	t.Helper()
	ctx := testAuthorActivityContext(context.Background())
	var lineageRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_executions
		WHERE source_run_id = $1::uuid
		  AND fork_run_id = $2::uuid
		  AND source_event_id = $3::uuid
		  AND fork_event_id = $4::uuid
	`, sourceRunID, forkRunID, sourceEventID, forkEventID).Scan(&lineageRows); err != nil {
		t.Fatalf("count selected execution lineage: %v", err)
	}
	if lineageRows != 1 {
		t.Fatalf("selected execution lineage rows = %d, want 1", lineageRows)
	}
}

func assertSelectedContractForkRuntimeRows(t testing.TB, db *sql.DB, forkRunID, forkEventID string) {
	t.Helper()
	ctx := testAuthorActivityContext(context.Background())
	counts := selectedContractForkRunCounts(t, db, forkRunID)
	for _, key := range []string{
		"runs",
		"events",
		"entity_state",
		"event_deliveries",
		"selected_contract_bindings",
		"selected_contract_executions",
		"selected_contract_route_recoveries",
	} {
		if counts[key] == 0 {
			t.Fatalf("%s rows for fork run %s = 0, counts=%#v", key, forkRunID, counts)
		}
	}

	var agentDeliveries, agentReceipts int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
		  AND subscriber_type = 'agent'
		  AND subscriber_id = 'test-agent'
		  AND status = 'delivered'
	`, forkRunID, forkEventID).Scan(&agentDeliveries); err != nil {
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
	if agentDeliveries != 1 || agentReceipts != 0 {
		t.Fatalf("fork runtime rows delivered_agent_obligations=%d agent_platform_receipts=%d, want 1/0", agentDeliveries, agentReceipts)
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
	`, forkRunID, forkEventID, runfork.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner).Scan(&typedRuntimeDiagnostics); err != nil {
		t.Fatalf("count typed fork runtime diagnostics: %v", err)
	}
	if typedRuntimeDiagnostics == 0 {
		t.Fatalf("typed fork runtime diagnostics parented to fork event = 0")
	}
}

func assertSelectedContractForkSourceIsolation(t testing.TB, db *sql.DB, sourceRunID, forkRunID, sourceEventID, forkEventID string) {
	t.Helper()
	ctx := testAuthorActivityContext(context.Background())
	var copiedSourceEvents, sourceIDReuse, sourceRunForkRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
	`, forkRunID, sourceEventID).Scan(&copiedSourceEvents); err != nil {
		t.Fatalf("count copied source event ids: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND source_event_id = $2::uuid
		  AND event_id <> $3::uuid
	`, forkRunID, sourceEventID, forkEventID).Scan(&sourceIDReuse); err != nil {
		t.Fatalf("count source_event_id reuse as fork truth: %v", err)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_id = $2::uuid
	`, sourceRunID, forkEventID).Scan(&sourceRunForkRows); err != nil {
		t.Fatalf("count fork rows in source run: %v", err)
	}
	if copiedSourceEvents != 0 || sourceIDReuse != 0 || sourceRunForkRows != 0 {
		t.Fatalf("source/fork isolation copied_source_events=%d source_id_reuse=%d source_run_fork_rows=%d, want 0/0/0",
			copiedSourceEvents, sourceIDReuse, sourceRunForkRows)
	}
}

func selectedContractSourceRowSnapshot(t testing.TB, db *sql.DB, sourceRunID, sourceEventID string) map[string]string {
	t.Helper()
	ctx := testAuthorActivityContext(context.Background())
	queries := map[string]string{
		"source_event": `
			SELECT COALESCE(jsonb_agg(to_jsonb(e) ORDER BY e.event_id), '[]'::jsonb)::text
			FROM events e
			WHERE e.run_id = $1::uuid
			  AND e.event_id = $2::uuid
		`,
		"source_delivery": `
			SELECT COALESCE(jsonb_agg(to_jsonb(d) ORDER BY d.delivery_id), '[]'::jsonb)::text
			FROM event_deliveries d
			WHERE d.run_id = $1::uuid
			  AND d.event_id = $2::uuid
		`,
		"source_event_receipts": `
			SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY r.receipt_id), '[]'::jsonb)::text
			FROM event_receipts r
			WHERE r.event_id = $2::uuid
			  AND EXISTS (
			    SELECT 1 FROM events e
			    WHERE e.run_id = $1::uuid
			      AND e.event_id = r.event_id
			  )
		`,
		"source_entity_state": `
			SELECT COALESCE(jsonb_agg(to_jsonb(es) ORDER BY es.entity_id), '[]'::jsonb)::text
			FROM entity_state es
			WHERE es.run_id = $1::uuid
		`,
		"source_agent_sessions": `
			SELECT COALESCE(jsonb_agg(to_jsonb(s) ORDER BY s.session_id), '[]'::jsonb)::text
			FROM agent_sessions s
			WHERE s.run_id = $1::uuid
		`,
		"source_agent_turns": `
			SELECT COALESCE(jsonb_agg(to_jsonb(trn) ORDER BY trn.turn_id), '[]'::jsonb)::text
			FROM agent_turns trn
			WHERE trn.run_id = $1::uuid
		`,
		"source_agent_conversation_audits": `
			SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY a.session_id), '[]'::jsonb)::text
			FROM agent_conversation_audits a
			WHERE a.run_id = $1::uuid
		`,
	}
	out := make(map[string]string, len(queries))
	for key, query := range queries {
		var value string
		args := []any{sourceRunID}
		if strings.Contains(query, "$2") {
			args = append(args, sourceEventID)
		}
		if err := db.QueryRowContext(ctx, query, args...).Scan(&value); err != nil {
			t.Fatalf("snapshot %s: %v", key, err)
		}
		out[key] = value
	}
	return out
}

func assertSourceRowsFrozen(t testing.TB, before, after map[string]string, label string) {
	t.Helper()
	if reflect.DeepEqual(before, after) {
		return
	}
	t.Fatalf("%s changed source row content:\nbefore=%#v\nafter=%#v", label, before, after)
}

func assertSourceRunLifecycle(t testing.TB, db *sql.DB, runID, wantStatus string, wantEnded bool) {
	t.Helper()
	var status string
	var ended bool
	if err := db.QueryRowContext(testAuthorActivityContext(context.Background()), `
		SELECT status, ended_at IS NOT NULL
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&status, &ended); err != nil {
		t.Fatalf("load source run lifecycle: %v", err)
	}
	if strings.TrimSpace(status) != wantStatus || ended != wantEnded {
		t.Fatalf("source run lifecycle = status:%q ended:%v, want status:%q ended:%v", status, ended, wantStatus, wantEnded)
	}
}

func assertUnsupportedHistoricalReplayFailsClosed(t *testing.T, fixtureRoot string) {
	t.Helper()
	var expected catalogExpectedDocument
	loadYAML(t, catalogExpectedPath(fixtureRoot), &expected)
	h := newRuntimeHarness(t, fixtureRoot, true)
	pauseCatalogRun(t, h)
	h.seedEntityFields(expected)
	sourceEventID := publishCatalogTrigger(t, h, expected.triggerSequence()[0], catalogRuntimePublishTimeout)
	plan, err := h.pg.PlanRunFork(h.ctx, runfork.RunForkPlanRequest{
		SourceRunID: catalogRuntimeRunID,
		At:          sourceEventID,
	})
	if err != nil {
		t.Fatalf("PlanRunFork negative replay proof: %v", err)
	}
	// Runtime bootstrap diagnostics can add committed replay-scope marker evidence
	// before this fixture's non-agent delivery reaches the planner on slower CI
	// runners. Both blockers are the same fail-closed historical-replay safety
	// gate this negative fixture is proving: selected-contract forks must not
	// silently replay source facts that lack a supported fork-local owner.
	if plan.ExecutionReady || !runForkPlanHasAnyTier12Blocker(plan.UnsupportedBlockers,
		runfork.RunForkBlockerNonAgentDeliveryReplayUnsupported,
		runfork.RunForkBlockerCommittedReplayScopeReplayUnsupported,
	) {
		t.Fatalf("negative replay plan ready=%v blockers=%#v, want fail-closed %s or %s",
			plan.ExecutionReady, plan.UnsupportedBlockers,
			runfork.RunForkBlockerNonAgentDeliveryReplayUnsupported,
			runfork.RunForkBlockerCommittedReplayScopeReplayUnsupported)
	}
}

func publishCatalogTrigger(t testing.TB, h *runtimeHarness, step catalogTriggerStep, timeout time.Duration) string {
	t.Helper()
	payload := cloneStringAnyMap(step.Payload)
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal trigger payload: %v", err)
	}
	eventID := uuid.NewString()
	eventEnvelope := events.EventEnvelope{}
	entityID := triggerPayloadEntityID(payload)
	if entityID == "" {
		entityID = runtimepipeline.FlowInstanceEntityID(catalogRuntimeRunID)
	}
	eventEnvelope = events.EnvelopeForEntityID(eventEnvelope, entityID)
	var flowInstance string
	err = h.db.QueryRowContext(h.ctx, `
		SELECT COALESCE(flow_instance, '')
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, catalogRuntimeRunID, entityID).Scan(&flowInstance)
	if err != nil && err != sql.ErrNoRows {
		t.Fatalf("load catalog trigger flow instance: %v", err)
	}
	if flowInstance = strings.TrimSpace(flowInstance); flowInstance != "" {
		eventEnvelope = events.EnvelopeForFlowInstance(eventEnvelope, flowInstance)
	}
	evt := eventtest.ExistingRunRootIngress(eventID,
		events.EventType(strings.TrimSpace(step.Event)),
		"cataloge2e", "", raw, 0, catalogRuntimeRunID, eventEnvelope, time.Now().UTC())
	ctx, cancel := context.WithTimeout(h.ctx, timeout)
	defer cancel()
	if err := h.publishBusEvent(ctx, evt); err != nil {
		t.Fatalf("Publish(%s): %v", strings.TrimSpace(step.Event), err)
	}
	h.mu.Lock()
	h.publishedIDs[eventID] = struct{}{}
	h.publishedOrder = append(h.publishedOrder, eventID)
	h.eventEntityIDs[eventID] = strings.TrimSpace(eventEnvelope.EntityID)
	h.mu.Unlock()
	return eventID
}

func pauseCatalogRun(t testing.TB, h *runtimeHarness) {
	t.Helper()
	if _, err := h.pg.PauseRunControl(h.ctx, runtimeruncontrol.TransitionRequest{
		RunID:        catalogRuntimeRunID,
		Reason:       "tier12_runtime_fork_fixture_boundary",
		ControlledBy: "cataloge2e",
		Now:          time.Now().UTC(),
	}); err != nil {
		t.Fatalf("PauseRunControl: %v", err)
	}
}

func selectedContractForkRunCounts(t testing.TB, db *sql.DB, runID string) map[string]int {
	t.Helper()
	return selectedContractRunCounts(t, db, runID, false)
}

func selectedContractSourceRunCounts(t testing.TB, db *sql.DB, runID string) map[string]int {
	t.Helper()
	return selectedContractRunCounts(t, db, runID, true)
}

func selectedContractRunCounts(t testing.TB, db *sql.DB, runID string, ignoreRuntimeDiagnosticEvents bool) map[string]int {
	t.Helper()
	ctx := testAuthorActivityContext(context.Background())
	eventFilter := ""
	if ignoreRuntimeDiagnosticEvents {
		eventFilter = " AND event_name NOT IN ('platform.runtime_log', 'platform.agent_started')"
	}
	queries := map[string]string{
		"runs":                                 `SELECT COUNT(*) FROM runs WHERE run_id = $1::uuid`,
		"events":                               `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid` + eventFilter,
		"event_deliveries":                     `SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid`,
		"event_receipts":                       `SELECT COUNT(*) FROM event_receipts WHERE event_id IN (SELECT event_id FROM events WHERE run_id = $1::uuid` + eventFilter + `)`,
		"entity_state":                         `SELECT COUNT(*) FROM entity_state WHERE run_id = $1::uuid`,
		"entity_mutations":                     `SELECT COUNT(*) FROM entity_mutations WHERE run_id = $1::uuid`,
		"agent_sessions":                       `SELECT COUNT(*) FROM agent_sessions WHERE run_id = $1::uuid`,
		"agent_turns":                          `SELECT COUNT(*) FROM agent_turns WHERE run_id = $1::uuid`,
		"agent_conversation_audits":            `SELECT COUNT(*) FROM agent_conversation_audits WHERE run_id = $1::uuid`,
		"selected_contract_bindings":           `SELECT COUNT(*) FROM run_fork_selected_contract_bindings WHERE fork_run_id = $1::uuid`,
		"selected_contract_executions":         `SELECT COUNT(*) FROM run_fork_selected_contract_executions WHERE fork_run_id = $1::uuid`,
		"selected_contract_branch_divergences": `SELECT COUNT(*) FROM run_fork_selected_contract_branch_divergences WHERE fork_run_id = $1::uuid`,
		"selected_contract_route_recoveries":   `SELECT COUNT(*) FROM run_fork_selected_contract_route_recoveries WHERE fork_run_id = $1::uuid`,
		"delivery_event_replays":               `SELECT COUNT(*) FROM run_fork_delivery_event_replays WHERE fork_run_id = $1::uuid`,
	}
	out := make(map[string]int, len(queries))
	for key, query := range queries {
		var count int
		if err := db.QueryRowContext(ctx, query, runID).Scan(&count); err != nil {
			t.Fatalf("count %s for run %s: %v", key, runID, err)
		}
		out[key] = count
	}
	return out
}

func assertRunCountsUnchanged(t testing.TB, before, after map[string]int, label string) {
	t.Helper()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("%s counts changed:\nbefore=%#v\nafter=%#v", label, before, after)
	}
}

func assertNoForkArtifacts(t testing.TB, db *sql.DB, forkRunID string) {
	t.Helper()
	counts := selectedContractForkRunCounts(t, db, forkRunID)
	for key, count := range counts {
		if count != 0 {
			t.Fatalf("fork artifact %s rows after cleanup = %d, counts=%#v", key, count, counts)
		}
	}
}

func containsTier12String(values []string, want string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == want {
			return true
		}
	}
	return false
}

func containsTier12AgentID(values []agentidentity.Identity, want string) bool {
	for _, value := range values {
		if value.AgentID() == want {
			return true
		}
	}
	return false
}

func runForkPlanHasTier12Blocker(blockers []runfork.RunForkUnsupportedBlocker, code string) bool {
	for _, blocker := range blockers {
		if strings.TrimSpace(blocker.Code) == code {
			return true
		}
	}
	return false
}

func runForkPlanHasAnyTier12Blocker(blockers []runfork.RunForkUnsupportedBlocker, codes ...string) bool {
	for _, code := range codes {
		if runForkPlanHasTier12Blocker(blockers, code) {
			return true
		}
	}
	return false
}
