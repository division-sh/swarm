package cataloge2e

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"swarm/internal/events"
	runtimebus "swarm/internal/runtime/bus"
	runtimeruncontrol "swarm/internal/runtime/runcontrol"
	"swarm/internal/runtime/runforkadmission"
	runtimerunforkexecution "swarm/internal/runtime/runforkexecution"
	"swarm/internal/runtime/sessions"
	"swarm/internal/store"
)

var tier12RuntimeForkFixtures = []string{
	"test-non-agent-replay-fail-closed",
	"test-selected-contract-fork-execution",
}

func TestTier12RuntimeFork_SelectedContractForkExecutionFixture(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	fixtureRoot := filepath.Join(repoRoot, "tests", "tier12-runtime-fork", "test-selected-contract-fork-execution")

	var expected catalogExpectedDocument
	loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)

	h := newRuntimeHarness(t, fixtureRoot, true)
	// Source execution is paused at T; register recipient evidence through runtime APIs before publishing.
	h.rt.Bus.RegisterRuntimeActiveAgentDescriptor(runtimebus.ActiveAgentDescriptor{AgentID: "test-agent"})
	_ = h.rt.Bus.Subscribe("test-agent", events.EventType("task.ready"))
	h.seedInitialState(triggerPayloadEntityID(expected.triggerSequence()[0].Payload))
	pauseCatalogRun(t, h)
	sourceEventID := publishCatalogTriggerAtFuture(t, h, expected.triggerSequence()[0], 10*time.Second)
	sourceRunID := catalogRuntimeRunID
	assertSourcePendingAgentDelivery(t, h.db, sourceRunID, sourceEventID, "test-agent")
	forkAt := sourceEventID
	sourceBefore := selectedContractSourceRunCounts(t, h.db, sourceRunID)
	sourceRowsBefore := selectedContractSourceRowSnapshot(t, h.db, sourceRunID, sourceEventID)

	loader, selection := selectedContractForkFixtureSelection(t, h.ctx, repoRoot, fixtureRoot)
	materialized, err := h.pg.MaterializeRunForkForSelectedContractExecution(h.ctx, store.RunForkSelectedContractExecutionMaterializeRequest{
		SourceRunID:       sourceRunID,
		At:                forkAt,
		ContractSelection: selection,
	})
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
	cfg.LLM.RuntimeMode = "api"
	result, err := runtimerunforkexecution.ExecuteSelectedContractRunFork(h.ctx, runtimerunforkexecution.SelectedContractExecutionRequest{
		SourceRunID:       sourceRunID,
		At:                forkAt,
		Store:             h.pg,
		SourceLoader:      loader,
		ContractSelection: selection,
		AgentRuntime: runtimerunforkexecution.SelectedContractAgentRuntimeOptions{
			Config:            cfg,
			SQLDB:             h.db,
			SessionRegistry:   sessions.NewPostgresRegistry(h.db, cfg.LLM.Session.LockTTL),
			ConversationStore: h.pg,
			TurnStore:         h.pg,
			ScheduleStore:     h.pg,
			MailboxStore:      h.pg,
			LLMRuntime:        h.llm,
			QuiescenceTimeout: 10 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteSelectedContractRunFork: %v", err)
	}
	if result.Owner != store.RunForkSelectedContractExecutionOwner ||
		result.Materialization.ForkRunID == "" ||
		!result.Activation.Activated ||
		result.ExecutedEventCount != 1 ||
		len(result.ForkEvents) != 1 {
		t.Fatalf("selected execution result = %#v", result)
	}
	if result.AgentRuntimeMaterialization == nil ||
		result.AgentRuntimeMaterialization.Owner != store.RunForkSelectedContractForkLocalAgentRuntimeMaterializerExecutorOwner ||
		!result.AgentRuntimeMaterialization.MaterializationRequired ||
		!result.AgentRuntimeMaterialization.MaterializationSupported ||
		!containsTier12String(result.AgentRuntimeMaterialization.AgentRecipients, "test-agent") ||
		!containsTier12String(result.AgentRuntimeMaterialization.ConfiguredAgentIDs, "test-agent") {
		t.Fatalf("agent runtime materialization = %#v", result.AgentRuntimeMaterialization)
	}
	if result.ForkLocalRuntimeContainer == nil ||
		result.ForkLocalRuntimeContainer.Owner != store.RunForkSelectedContractForkLocalRuntimeContainerOwner ||
		result.ForkLocalRuntimeContainer.TypedRuntimeLineageOwner != store.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner ||
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
	assertSourceRunLifecycle(t, h.db, sourceRunID, "forked", true)
	negativeFixtureRoot := filepath.Join(repoRoot, "tests", "tier12-runtime-fork", "test-non-agent-replay-fail-closed")
	assertUnsupportedNonAgentHistoricalReplayFailsClosed(t, negativeFixtureRoot)
}

func TestTier12RuntimeForkFixtures_AreExplicitlyClassified(t *testing.T) {
	repoRoot := repoRootFromCatalogE2E(t)
	entries, err := os.ReadDir(filepath.Join(repoRoot, "tests", "tier12-runtime-fork"))
	if err != nil {
		t.Fatalf("read tier12 fixture dir: %v", err)
	}
	supported := make(map[string]struct{}, len(tier12RuntimeForkFixtures))
	for _, name := range tier12RuntimeForkFixtures {
		supported[name] = struct{}{}
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := strings.TrimSpace(entry.Name())
		if name == "" {
			continue
		}
		if _, ok := supported[name]; !ok {
			t.Fatalf("tier12 runtime-fork fixture %q is not explicitly classified", name)
		}
	}
}

func selectedContractForkFixtureSelection(t testing.TB, ctx context.Context, repoRoot, fixtureRoot string) (runtimerunforkexecution.ContractBundleSourceLoader, store.RunForkContractSelection) {
	t.Helper()
	loader := runtimerunforkexecution.ContractBundleSourceLoader{
		RepoRoot:         repoRoot,
		PlatformSpecPath: platformSpecPathFromCatalogE2E(t),
	}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, store.RunForkContractSelection{
		Mode:          "selected_contracts",
		ContractsRoot: fixtureRoot,
	})
	if err != nil {
		t.Fatalf("LoadRunForkSelectedContractSource: %v", err)
	}
	return loader, runforkadmission.SelectedContractSelection(loaded.Source, fixtureRoot)
}

func assertSourcePendingAgentDelivery(t testing.TB, db *sql.DB, runID, eventID, agentID string) {
	t.Helper()
	var rows int
	if err := db.QueryRowContext(context.Background(), `
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
	ctx := context.Background()
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
	ctx := context.Background()
	counts := selectedContractForkRunCounts(t, db, forkRunID)
	for _, key := range []string{
		"runs",
		"events",
		"entity_state",
		"event_deliveries",
		"event_receipts",
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
	if agentDeliveries != 1 || agentReceipts != 1 {
		t.Fatalf("fork runtime rows deliveries=%d receipts=%d, want 1/1", agentDeliveries, agentReceipts)
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
	`, forkRunID, forkEventID, store.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner).Scan(&typedRuntimeDiagnostics); err != nil {
		t.Fatalf("count typed fork runtime diagnostics: %v", err)
	}
	if typedRuntimeDiagnostics == 0 {
		t.Fatalf("typed fork runtime diagnostics parented to fork event = 0")
	}
}

func assertSelectedContractForkSourceIsolation(t testing.TB, db *sql.DB, sourceRunID, forkRunID, sourceEventID, forkEventID string) {
	t.Helper()
	ctx := context.Background()
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
	ctx := context.Background()
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
	if err := db.QueryRowContext(context.Background(), `
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

func assertUnsupportedNonAgentHistoricalReplayFailsClosed(t *testing.T, fixtureRoot string) {
	t.Helper()
	var expected catalogExpectedDocument
	loadYAML(t, filepath.Join(fixtureRoot, "expected.yaml"), &expected)
	h := newRuntimeHarness(t, fixtureRoot, true)
	pauseCatalogRun(t, h)
	h.seedEntityFields(expected)
	sourceEventID := publishCatalogTriggerAtFuture(t, h, expected.triggerSequence()[0], 5*time.Second)
	plan, err := h.pg.PlanRunFork(h.ctx, store.RunForkPlanRequest{
		SourceRunID: catalogRuntimeRunID,
		At:          sourceEventID,
	})
	if err != nil {
		t.Fatalf("PlanRunFork negative replay proof: %v", err)
	}
	if plan.ExecutionReady || !runForkPlanHasTier12Blocker(plan.UnsupportedBlockers, store.RunForkBlockerNonAgentDeliveryReplayUnsupported) {
		t.Fatalf("negative replay plan ready=%v blockers=%#v, want fail-closed %s",
			plan.ExecutionReady, plan.UnsupportedBlockers, store.RunForkBlockerNonAgentDeliveryReplayUnsupported)
	}
}

func publishCatalogTriggerAtFuture(t testing.TB, h *runtimeHarness, step catalogTriggerStep, timeout time.Duration) string {
	t.Helper()
	payload := cloneStringAnyMap(step.Payload)
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal trigger payload: %v", err)
	}
	eventID := uuid.NewString()
	evt := events.Event{
		ID:          eventID,
		Type:        events.EventType(strings.TrimSpace(step.Event)),
		SourceAgent: "cataloge2e",
		RunID:       catalogRuntimeRunID,
		Payload:     raw,
		CreatedAt:   time.Now().UTC().Add(time.Second),
	}
	if entityID := triggerPayloadEntityID(payload); entityID != "" {
		evt = evt.WithEntityID(entityID)
	}
	ctx, cancel := context.WithTimeout(h.ctx, timeout)
	defer cancel()
	if err := h.publishBusEvent(ctx, evt); err != nil {
		t.Fatalf("Publish(%s): %v", strings.TrimSpace(step.Event), err)
	}
	h.mu.Lock()
	h.publishedIDs[eventID] = struct{}{}
	h.publishedOrder = append(h.publishedOrder, eventID)
	if entityID := triggerPayloadEntityID(payload); entityID != "" {
		h.eventEntityIDs[eventID] = entityID
	}
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

func selectedContractRunCounts(t testing.TB, db *sql.DB, runID string, ignoreRuntimeLogEvents bool) map[string]int {
	t.Helper()
	ctx := context.Background()
	eventFilter := ""
	if ignoreRuntimeLogEvents {
		eventFilter = " AND event_name <> 'platform.runtime_log'"
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

func runForkPlanHasTier12Blocker(blockers []store.RunForkUnsupportedBlocker, code string) bool {
	for _, blocker := range blockers {
		if strings.TrimSpace(blocker.Code) == code {
			return true
		}
	}
	return false
}
