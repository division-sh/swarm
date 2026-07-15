package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	runtimerunforkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRunForkRuntimeOwnerHarness_DryRunUsesCanonicalPlannerJSON(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000300, 0).UTC()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (execution_mode,
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ('live', $1::uuid, $2::uuid, 'fork.cli', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	captureRunForkCLIRevision(t, db, runID, runforkrevision.AllFamilies()...)
	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(ctx, t.TempDir(), []string{
		"--store", "postgres",
		"--dry-run",
		"--run", runID,
		"--at", eventID,
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d output=%s", code, buf.String())
	}
	var plan store.RunForkPlan
	if err := json.Unmarshal(buf.Bytes(), &plan); err != nil {
		t.Fatalf("decode fork plan json: %v\n%s", err, buf.String())
	}
	if plan.SourceRunID != runID {
		t.Fatalf("SourceRunID = %q, want %q", plan.SourceRunID, runID)
	}
	if plan.ForkPoint.EventID != eventID {
		t.Fatalf("ForkPoint.EventID = %q, want %q", plan.ForkPoint.EventID, eventID)
	}
	if plan.PendingWorkCount != 0 || len(plan.PendingWork) != 0 {
		t.Fatalf("pending work = %#v, want none", plan.PendingWork)
	}
	if !plan.ExecutionReady {
		t.Fatalf("ExecutionReady = false, want true for state-only dry-run; blockers=%#v", plan.UnsupportedBlockers)
	}
	if plan.UnsupportedBlockerCount != 0 {
		t.Fatalf("UnsupportedBlockerCount = %d, want 0; blockers=%#v", plan.UnsupportedBlockerCount, plan.UnsupportedBlockers)
	}
	if plan.ReplayResumeAdmission.Owner != store.RunForkReplayResumeAdmissionOwner || !plan.ReplayResumeAdmission.StateOnlyExecutionReady {
		t.Fatalf("taxonomy = %#v, want canonical owner and state-only ready", plan.ReplayResumeAdmission)
	}
}

func TestRunForkRuntimeOwnerHarness_DryRunJSONReportsDeliveryEventReplayReady(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000305, 0).UTC()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (execution_mode,
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ('live', $1::uuid, $2::uuid, 'fork.cli.pending', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent', 'cli-agent', 'pending', 0, 'matched_agent_subscription', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed pending delivery: %v", err)
	}
	captureRunForkCLIRevision(t, db, runID, runforkrevision.AllFamilies()...)

	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(ctx, t.TempDir(), []string{
		"--store", "postgres",
		"--dry-run",
		"--run", runID,
		"--at", eventID,
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d output=%s", code, buf.String())
	}
	var plan store.RunForkPlan
	if err := json.Unmarshal(buf.Bytes(), &plan); err != nil {
		t.Fatalf("decode fork plan json: %v\n%s", err, buf.String())
	}
	if !plan.ExecutionReady || !plan.ReplayResumeAdmission.DeliveryEventReplayReady || !plan.ReplayResumeAdmission.BoundedReplaySupported {
		t.Fatalf("dry-run replay admission = execution:%v admission:%#v", plan.ExecutionReady, plan.ReplayResumeAdmission)
	}
	if plan.ReplayResumeAdmission.StateOnlyExecutionReady {
		t.Fatalf("StateOnlyExecutionReady = true, want false for delivery/event replay dry-run")
	}
}

func TestRunForkRuntimeOwnerHarness_DryRunContractsAddsContractFrontierAdmissionJSON(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	runID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000307, 0).UTC()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (execution_mode,
			run_id, event_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ('live', $1::uuid, $2::uuid, 'flow-a/work.begin', 'global', '{}'::jsonb, 'test', 'platform', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'node', 'source-node', 'pending', 0, 'matched_node_subscription', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed pending node delivery: %v", err)
	}
	captureRunForkCLIRevision(t, db, runID, runforkrevision.AllFamilies()...)

	repo := cliapp.RepoRoot()
	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(ctx, repo, []string{
		"--store", "postgres",
		"--dry-run",
		"--run", runID,
		"--at", eventID,
		"--contracts", filepath.Join(repo, "tests", "tier11-flow-composition", "test-sibling-both-instantiated-isolated"),
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d output=%s", code, buf.String())
	}
	var plan store.RunForkPlan
	if err := json.Unmarshal(buf.Bytes(), &plan); err != nil {
		t.Fatalf("decode fork plan json: %v\n%s", err, buf.String())
	}
	if plan.ContractFrontierAdmission == nil {
		t.Fatalf("ContractFrontierAdmission = nil; output=%s", buf.String())
	}
	admission := plan.ContractFrontierAdmission
	if admission.Owner != store.RunForkContractFrontierAdmissionOwner || !admission.NonMutating || admission.HistoricalExecutionSupported {
		t.Fatalf("contract frontier admission = %#v", admission)
	}
	if admission.FrontierEventCount != 1 || len(admission.FrontierEvents) != 1 {
		t.Fatalf("frontier events = %#v", admission.FrontierEvents)
	}
	if !runForkPlanHasString(admission.FrontierEvents[0].RuntimeEventOwners, "alpha-intake") {
		t.Fatalf("runtime event owners = %#v, want alpha-intake from selected contract", admission.FrontierEvents[0].RuntimeEventOwners)
	}
	if !runForkPlanHasBlocker(admission.UnsupportedBlockers, store.RunForkBlockerContractFrontierExecutionUnsupported) {
		t.Fatalf("contract frontier blockers = %#v, want execution unsupported", admission.UnsupportedBlockers)
	}
	model := plan.SelectedContractExecution
	if model == nil {
		t.Fatalf("SelectedContractExecution = nil; output=%s", buf.String())
	}
	if model.Owner != store.RunForkSelectedContractExecutionModelOwner ||
		model.FutureExecutionOwner != store.RunForkSelectedContractExecutionOwner ||
		!model.NonMutating ||
		model.ExecutionSupported {
		t.Fatalf("selected-contract execution model = %#v", model)
	}
	if model.AdmissionOwner != store.RunForkContractFrontierAdmissionOwner ||
		model.AdmissionUse != store.RunForkSelectedContractExecutionAdmissionUseEvidenceOnly {
		t.Fatalf("selected-contract execution admission use = %#v", model)
	}
	if model.RouteTopology == nil ||
		model.RouteTopology.Owner != store.RunForkSelectedContractRouteTopologyOwner ||
		model.RouteTopology.RouteAdmissionOwner != store.RunForkSelectedContractRouteAdmissionOwner ||
		!model.RouteTopology.NonMutating ||
		model.RouteTopology.RoutePersistenceSupported ||
		model.RouteTopology.ExecutableRecipientsSupported {
		t.Fatalf("selected-contract route topology = %#v", model.RouteTopology)
	}
	if !runForkPlanHasBoundary(model.RouteTopology.InvalidPaths, "copy_source_routing_rules", store.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("selected-contract route invalid paths = %#v", model.RouteTopology.InvalidPaths)
	}
	if model.RecipientPlanning == nil ||
		model.RecipientPlanning.Owner != store.RunForkSelectedContractRecipientPlanningOwner ||
		!model.RecipientPlanning.NonMutating ||
		!model.RecipientPlanning.RecipientPlanningSupported ||
		model.RecipientPlanning.DeliveryWritesSupported {
		t.Fatalf("selected-contract recipient planning = %#v", model.RecipientPlanning)
	}
	if !runForkPlanHasBoundary(model.RecipientPlanning.RequiredConsumers, "eventbus_publish_recipient_guard", store.RunForkSelectedContractDispositionPrerequisite) {
		t.Fatalf("selected-contract recipient planning consumers = %#v", model.RecipientPlanning.RequiredConsumers)
	}
	if !runForkPlanHasBlocker(model.UnsupportedBlockers, store.RunForkBlockerSelectedContractRouteTopologyNonMutating) {
		t.Fatalf("selected-contract execution blockers = %#v, want route topology non-mutating blocker", model.UnsupportedBlockers)
	}
	if !runForkPlanHasBlocker(model.UnsupportedBlockers, store.RunForkBlockerSelectedContractRouteAdmissionNonMutating) {
		t.Fatalf("selected-contract execution blockers = %#v, want route admission non-mutating blocker", model.UnsupportedBlockers)
	}
	if !runForkPlanHasBoundary(model.InvalidPaths, "copy_source_event_deliveries", store.RunForkSelectedContractDispositionInvalid) {
		t.Fatalf("selected-contract execution invalid paths = %#v", model.InvalidPaths)
	}
	if !runForkPlanHasBoundary(model.RequiredConsumers, "fork_local_runtime_container", store.RunForkSelectedContractDispositionPrerequisite) ||
		!runForkPlanHasBoundary(model.RequiredConsumers, "fork_run_id_runtime_context", store.RunForkSelectedContractDispositionPrerequisite) ||
		!runForkPlanHasBoundary(model.RequiredConsumers, "fork_local_event_delivery_writes", store.RunForkSelectedContractDispositionPrerequisite) ||
		!runForkPlanHasBoundary(model.RequiredConsumers, "handler_execution", store.RunForkSelectedContractDispositionPrerequisite) ||
		!runForkPlanHasBoundary(model.RequiredConsumers, "emitted_follow_up_events", store.RunForkSelectedContractDispositionPrerequisite) {
		t.Fatalf("selected-contract execution consumers = %#v", model.RequiredConsumers)
	}
	if !runForkPlanHasBlocker(model.UnsupportedBlockers, store.RunForkBlockerSelectedContractExecutionModelNonMutating) {
		t.Fatalf("selected-contract execution blockers = %#v", model.UnsupportedBlockers)
	}
	readiness := plan.SelectedContractReadiness
	if readiness == nil {
		t.Fatalf("SelectedContractReadiness = nil; output=%s", buf.String())
	}
	if readiness.Owner != store.RunForkSelectedContractReadinessClassifierOwner ||
		!readiness.NonMutating ||
		readiness.PlannerOwner != store.RunForkPlanningOwner ||
		readiness.ReplayResumeAdmissionOwner != store.RunForkReplayResumeAdmissionOwner ||
		readiness.ContractFrontierAdmissionOwner != store.RunForkContractFrontierAdmissionOwner ||
		readiness.RouteTopologyOwner != store.RunForkSelectedContractRouteTopologyOwner ||
		readiness.RecipientPlanningOwner != store.RunForkSelectedContractRecipientPlanningOwner ||
		readiness.SelectedExecutionModelOwner != store.RunForkSelectedContractExecutionModelOwner ||
		readiness.FutureExecutionOwner != store.RunForkSelectedContractExecutionOwner {
		t.Fatalf("selected-contract readiness = %#v", readiness)
	}
	if len(readiness.FactMatrix) != 20 {
		t.Fatalf("readiness facts = %d, want complete matrix; facts=%#v", len(readiness.FactMatrix), readiness.FactMatrix)
	}
	for _, fact := range []string{
		store.RunForkSelectedContractReadinessFactSourceEvents,
		store.RunForkSelectedContractReadinessFactForkEvents,
		store.RunForkSelectedContractReadinessFactSourceDeliveries,
		store.RunForkSelectedContractReadinessFactForkDeliveries,
		store.RunForkSelectedContractReadinessFactSelectedRecipientsRoutes,
		store.RunForkSelectedContractReadinessFactTimers,
		store.RunForkSelectedContractReadinessFactSessions,
		store.RunForkSelectedContractReadinessFactTurns,
		store.RunForkSelectedContractReadinessFactAudits,
		store.RunForkSelectedContractReadinessFactCommittedReplayScopeMarkers,
		store.RunForkSelectedContractReadinessFactPlatformRuntimeDiagnostics,
		store.RunForkSelectedContractReadinessFactReceipts,
		store.RunForkSelectedContractReadinessFactDeadLetters,
		store.RunForkSelectedContractReadinessFactRetryIdempotency,
		store.RunForkSelectedContractReadinessFactEmittedFollowUps,
		store.RunForkSelectedContractReadinessFactSourcePostTFacts,
		store.RunForkSelectedContractReadinessFactCurrentStateSnapshots,
		store.RunForkSelectedContractReadinessFactNonAgentNodeSystemWork,
		store.RunForkSelectedContractReadinessFactRestartRecovery,
		store.RunForkSelectedContractReadinessFactOperatorConsumers,
	} {
		if !runForkReadinessFactHas(readiness.FactMatrix, fact) {
			t.Fatalf("readiness fact %q missing from %#v", fact, readiness.FactMatrix)
		}
	}
	if !runForkReadinessFactHasDisposition(readiness.FactMatrix, store.RunForkSelectedContractReadinessFactSourceDeliveries, store.RunForkSelectedContractReadinessDispositionFailClosedBlocker) {
		t.Fatalf("source delivery readiness = %#v, want fail-closed blocker for source node delivery", readiness.FactMatrix)
	}
	if !runForkReadinessFactHasDisposition(readiness.FactMatrix, store.RunForkSelectedContractReadinessFactSelectedRecipientsRoutes, store.RunForkSelectedContractReadinessDispositionReconstructedForkState) {
		t.Fatalf("route/recipient readiness = %#v, want reconstructed fork-local evidence", readiness.FactMatrix)
	}
}

func TestRunForkRuntimeOwnerHarness_ActivateWithContractsReachesSelectedActivationGate(t *testing.T) {
	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(context.Background(), t.TempDir(), []string{
		"--activate",
		"--contracts", t.TempDir(),
		"--run", uuid.NewString(),
	}, &buf)
	if code != 1 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d, want runtime failure after parsing; output=%s", code, buf.String())
	}
	if strings.Contains(buf.String(), "--contracts is only supported") {
		t.Fatalf("output = %q, want --activate to consume canonical selected activation gate rather than parse-level contract rejection", buf.String())
	}
}

func TestRunForkRuntimeOwnerHarness_SelectedContractsBorrowedRequireExplicitData(t *testing.T) {
	dsn, _, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	repo := cliapp.RepoRoot()
	borrowedRoot := t.TempDir()
	writeWorkflowValidationFixtureFile(t, filepath.Join(borrowedRoot, "package.yaml"), `
name: borrowed-selected-contracts
version: 1.0.0
flows: []
`)

	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(context.Background(), repo, []string{
		"--store", "postgres",
		"--contracts", borrowedRoot,
		"--run", uuid.NewString(),
		"--at", uuid.NewString(),
	}, &buf)
	if code != 1 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d, want missing data-source failure; output=%s", code, buf.String())
	}
	if !strings.Contains(buf.String(), "resolve workspace data source") ||
		!strings.Contains(buf.String(), "workspace data source is required") {
		t.Fatalf("output = %q, want borrowed selected contracts to require explicit workspace data", buf.String())
	}
	if _, err := os.Stat(filepath.Join(borrowedRoot, defaultWorkspaceDataSourceRelativePath)); !os.IsNotExist(err) {
		t.Fatalf("borrowed contracts data stat error = %v, want no default data source created", err)
	}
}

func TestRunForkRuntimeOwnerHarness_SelectedContractsExecutesExplicitHostRefusal(t *testing.T) {
	dsn, _, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	configPath := os.Getenv("SWARM_CONFIG")
	rawConfig, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read run-fork config: %v", err)
	}
	writeRuntimeConfigText(t, configPath, string(rawConfig)+fmt.Sprintf("workspace:\n  backend: host\n  data_source: %q\n", t.TempDir()))
	var out bytes.Buffer
	code := runForkRuntimeOwnerHarness(context.Background(), cliapp.RepoRoot(), []string{
		"--store", "postgres",
		"--config", configPath,
		"--contracts", doctorAgentContractsPath,
		"--run", uuid.NewString(),
		"--at", uuid.NewString(),
	}, &out)
	if code != 1 {
		t.Fatalf("selected-contract run-fork code = %d, want 1\n%s", code, out.String())
	}
	assertClaudeHostRefusal(t, out.String())
}

func TestRunForkRuntimeOwnerHarness_SelectedContractsExecuteThroughCanonicalOwnerJSON(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	repo := cliapp.RepoRoot()
	contractsRoot := filepath.Join(repo, "tests/tier1-primitives/test-emits-multiple")
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	diagnosticEventID := uuid.NewString()
	at := time.Unix(1700000312, 0).UTC()
	seedRunForkCLISelectedExecutionSource(t, db, sourceRunID, entityID, sourceEventID, at)
	seedRunForkCLISelectedExecutionDiagnosticPlatformDeadLetter(t, db, sourceRunID, diagnosticEventID, at.Add(-time.Second))

	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(context.Background(), repo, []string{
		"--store", "postgres",
		"--contracts", contractsRoot,
		"--run", sourceRunID,
		"--at", sourceEventID,
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d output=%s", code, buf.String())
	}
	var result runtimerunforkexecution.SelectedContractExecutionResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("decode selected execution json: %v\n%s", err, buf.String())
	}
	if result.Owner != store.RunForkSelectedContractExecutionOwner || result.ExecutedEventCount != 1 || len(result.ForkEvents) != 1 {
		t.Fatalf("selected execution result = %#v", result)
	}
	if result.SelectedContractExecutionAdmission == nil ||
		result.SelectedContractExecutionAdmission.RecipientPlanning == nil ||
		result.SelectedContractExecutionAdmission.RecipientPlanning.Owner != store.RunForkSelectedContractRecipientPlanningOwner {
		t.Fatalf("selected execution recipient planning admission = %#v", result.SelectedContractExecutionAdmission)
	}
	var lineageRows int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_executions
		WHERE fork_run_id = $1::uuid
		  AND source_event_id = $2::uuid
		  AND fork_event_id = $3::uuid
	`, result.Materialization.ForkRunID, sourceEventID, result.ForkEvents[0].ForkEventID).Scan(&lineageRows); err != nil {
		t.Fatalf("count selected execution lineage: %v", err)
	}
	if lineageRows != 1 {
		t.Fatalf("selected execution lineage rows = %d, want 1", lineageRows)
	}
	var diagnosticCopies int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND (
			event_id = $2::uuid
			OR COALESCE(source_event_id::text, '') = $2::text
		  )
	`, result.Materialization.ForkRunID, diagnosticEventID).Scan(&diagnosticCopies); err != nil {
		t.Fatalf("count copied diagnostic platform events: %v", err)
	}
	if diagnosticCopies != 0 {
		t.Fatalf("diagnostic platform events copied into fork = %d, want 0", diagnosticCopies)
	}
	var typedRuntimeDiagnostics int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM events
		WHERE run_id = $1::uuid
		  AND event_name = 'platform.runtime_log'
		  AND source_event_id = $2::uuid
		  AND payload->'details'->>'runtime_lineage_owner' = $3
	`, result.Materialization.ForkRunID, result.ForkEvents[0].ForkEventID, store.RunForkSelectedContractForkLocalRuntimeTypedLineageOwner).Scan(&typedRuntimeDiagnostics); err != nil {
		t.Fatalf("count typed fork-local runtime diagnostics: %v", err)
	}
	if typedRuntimeDiagnostics == 0 {
		t.Fatalf("typed fork-local runtime diagnostics = 0, want selected execution runtime logs parented to fork event")
	}
	var diagnosticLineageRows int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM run_fork_selected_contract_executions
		WHERE fork_run_id = $1::uuid
		  AND source_event_id = $2::uuid
	`, result.Materialization.ForkRunID, diagnosticEventID).Scan(&diagnosticLineageRows); err != nil {
		t.Fatalf("count diagnostic selected execution lineage: %v", err)
	}
	if diagnosticLineageRows != 0 {
		t.Fatalf("diagnostic platform execution lineage rows = %d, want 0", diagnosticLineageRows)
	}
}

func TestRunForkRuntimeOwnerHarness_SelectedContractsExecuteReportsSourceAdvancedBranchJSON(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	repo := cliapp.RepoRoot()
	contractsRoot := filepath.Join(repo, "tests/tier1-primitives/test-emits-multiple")
	sourceRunID := uuid.NewString()
	entityID := uuid.NewString()
	sourceEventID := uuid.NewString()
	afterEventID := uuid.NewString()
	at := time.Unix(1700000313, 0).UTC()
	seedRunForkCLISelectedExecutionSource(t, db, sourceRunID, entityID, sourceEventID, at)
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO events (execution_mode,
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ('live', $1::uuid, $2::uuid, 'source.after', $3::uuid, 'flow-a/1', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, sourceRunID, afterEventID, entityID, at.Add(time.Second)); err != nil {
		t.Fatalf("seed post-fork source event: %v", err)
	}
	captureRunForkCLIRevision(t, db, sourceRunID, runforkrevision.FamilyEvents)

	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(context.Background(), repo, []string{
		"--store", "postgres",
		"--contracts", contractsRoot,
		"--run", sourceRunID,
		"--at", sourceEventID,
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d output=%s", code, buf.String())
	}
	var result runtimerunforkexecution.SelectedContractExecutionResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("decode selected branch execution json: %v\n%s", err, buf.String())
	}
	if result.Activation.BranchDivergence == nil || result.Activation.SourceFrozen {
		t.Fatalf("branch activation = %#v", result.Activation)
	}
	if result.Activation.BranchDivergence.Owner != store.RunForkSelectedContractBranchDivergenceOwner ||
		result.Activation.BranchDivergence.Policy != store.RunForkSelectedContractSourceAdvancedBranchPolicy {
		t.Fatalf("branch divergence = %#v", result.Activation.BranchDivergence)
	}
	var sourceStatus string
	if err := db.QueryRowContext(context.Background(), `SELECT status FROM runs WHERE run_id = $1::uuid`, sourceRunID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if sourceStatus != "running" {
		t.Fatalf("source status = %q, want unchanged running", sourceStatus)
	}
}

func TestRunForkRuntimeOwnerHarness_MaterializeOnlyUsesCanonicalStoreOwnerJSON(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000310, 0).UTC()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (execution_mode,
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ('live', $1::uuid, $2::uuid, 'fork.cli.materialize', $3::uuid, '', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, runID, eventID, entityID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"ready"'::jsonb, $3::uuid, 'platform', 'cli-test', 'seed', $4),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, '"CLI Entity"'::jsonb, $3::uuid, 'platform', 'cli-test', 'seed', $4)
	`, runID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'CLI Entity',
			'ready', '{}'::jsonb, '{"name":"CLI Entity"}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, runID, entityID, at); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}
	captureRunForkCLIRevision(t, db, runID, runforkrevision.AllFamilies()...)
	repo := cliapp.RepoRoot()
	contractsRoot := filepath.Join(repo, "tests", "tier11-flow-composition", "test-sibling-both-instantiated-isolated")
	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(ctx, repo, []string{
		"--store", "postgres",
		"--materialize-only",
		"--run", runID,
		"--at", eventID,
		"--contracts", contractsRoot,
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d output=%s", code, buf.String())
	}
	var result store.RunForkMaterialization
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("decode fork materialization json: %v\n%s", err, buf.String())
	}
	if result.SourceRunID != runID || result.ForkRunID == "" || result.ForkRunStatus != store.RunForkMaterializedStatus {
		t.Fatalf("materialization result = %#v", result)
	}
	if result.ReplayResumeAdmission.Owner != store.RunForkReplayResumeAdmissionOwner || !result.ReplayResumeAdmission.StateOnlyExecutionReady {
		t.Fatalf("materialization taxonomy = %#v, want canonical owner and state-only ready", result.ReplayResumeAdmission)
	}
	if result.SelectedContractBinding == nil {
		t.Fatalf("SelectedContractBinding = nil; output=%s", buf.String())
	}
	if result.SelectedContractBinding.Owner != store.RunForkSelectedContractBindingOwner ||
		result.SelectedContractBinding.ForkRunID != result.ForkRunID ||
		result.SelectedContractBinding.SourceRunID != runID ||
		result.SelectedContractBinding.ForkEventID != eventID ||
		result.SelectedContractBinding.ContractSelection.ContractsRoot != contractsRoot {
		t.Fatalf("selected contract binding = %#v", result.SelectedContractBinding)
	}
	var forkState string
	if err := db.QueryRowContext(ctx, `
		SELECT current_state
		FROM entity_state
		WHERE run_id = $1::uuid AND entity_id = $2::uuid
	`, result.ForkRunID, entityID).Scan(&forkState); err != nil {
		t.Fatalf("load fork entity_state: %v", err)
	}
	if forkState != "ready" {
		t.Fatalf("fork state = %q, want ready", forkState)
	}
	var persistedBindingMode string
	if err := db.QueryRowContext(ctx, `
		SELECT mode
		FROM run_fork_selected_contract_bindings
		WHERE fork_run_id = $1::uuid
		  AND source_run_id = $2::uuid
		  AND fork_event_id = $3::uuid
	`, result.ForkRunID, runID, eventID).Scan(&persistedBindingMode); err != nil {
		t.Fatalf("load selected contract binding row: %v", err)
	}
	if persistedBindingMode != "selected_contracts" {
		t.Fatalf("binding mode = %q, want selected_contracts", persistedBindingMode)
	}
}

func TestRunForkRuntimeOwnerHarness_ActivateUsesCanonicalStoreOwnerJSON(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	pg := &store.PostgresStore{DB: db}
	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000320, 0).UTC()
	ctx := context.Background()
	seedRunForkCLIActivationSource(t, db, runID, entityID, eventID, at)
	materialized, err := pg.MaterializeRunFork(ctx, store.RunForkMaterializeRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}

	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(ctx, t.TempDir(), []string{
		"--store", "postgres",
		"--activate",
		"--run", materialized.ForkRunID,
		"--confirm-source-freeze",
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d output=%s", code, buf.String())
	}
	var result store.RunForkActivation
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("decode fork activation json: %v\n%s", err, buf.String())
	}
	if !result.Activated || !result.SourceFrozen || !result.ReplayResumeBlocked {
		t.Fatalf("activation result = %#v", result)
	}
	if result.ReplayResumeAdmission.Owner != store.RunForkReplayResumeAdmissionOwner || !result.ReplayResumeAdmission.StateOnlyExecutionReady {
		t.Fatalf("activation taxonomy = %#v, want canonical owner and state-only ready", result.ReplayResumeAdmission)
	}
	if result.SourceRunID != runID || result.ForkRunID != materialized.ForkRunID {
		t.Fatalf("activation lineage = %#v", result)
	}
	var sourceStatus, forkStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, runID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status: %v", err)
	}
	if sourceStatus != store.RunForkSourceFrozenStatus || forkStatus != store.RunForkActivatedStatus {
		t.Fatalf("source/fork status = %s/%s, want forked/running", sourceStatus, forkStatus)
	}
}

func TestRunForkRuntimeOwnerHarness_ActivateNonSelectedWithEmptySelectedAuthoritySchema(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	pg := &store.PostgresStore{DB: db}
	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000325, 0).UTC()
	ctx := context.Background()
	seedRunForkCLIActivationSource(t, db, runID, entityID, eventID, at)
	materialized, err := pg.MaterializeRunFork(ctx, store.RunForkMaterializeRequest{SourceRunID: runID, At: eventID})
	if err != nil {
		t.Fatalf("MaterializeRunFork: %v", err)
	}
	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(ctx, t.TempDir(), []string{
		"--store", "postgres",
		"--activate",
		"--run", materialized.ForkRunID,
		"--confirm-source-freeze",
		"--json",
	}, &buf)
	if code != 0 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d output=%s", code, buf.String())
	}
	var result store.RunForkActivation
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("decode fork activation json: %v\n%s", err, buf.String())
	}
	if !result.Activated || !result.SourceFrozen || result.ForkRunID != materialized.ForkRunID {
		t.Fatalf("activation result = %#v", result)
	}
}

func TestRunForkRuntimeOwnerHarness_ActivateSelectedBindingConsumesRuntimeAdmission(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000330, 0).UTC()
	ctx := context.Background()
	seedRunForkCLIActivationSource(t, db, runID, entityID, eventID, at)
	repo := cliapp.RepoRoot()
	contractsRoot := filepath.Join(repo, "tests", "tier11-flow-composition", "test-sibling-both-instantiated-isolated")

	var materializeOut bytes.Buffer
	materializeCode := runForkRuntimeOwnerHarness(ctx, repo, []string{
		"--store", "postgres",
		"--materialize-only",
		"--run", runID,
		"--at", eventID,
		"--contracts", contractsRoot,
		"--json",
	}, &materializeOut)
	if materializeCode != 0 {
		t.Fatalf("materialize code=%d output=%s", materializeCode, materializeOut.String())
	}
	var materialized store.RunForkMaterialization
	if err := json.Unmarshal(materializeOut.Bytes(), &materialized); err != nil {
		t.Fatalf("decode materialization: %v\n%s", err, materializeOut.String())
	}

	var activateOut bytes.Buffer
	activateCode := runForkRuntimeOwnerHarness(ctx, repo, []string{
		"--store", "postgres",
		"--activate",
		"--run", materialized.ForkRunID,
		"--confirm-source-freeze",
		"--json",
	}, &activateOut)
	if activateCode != 0 {
		t.Fatalf("activate code=%d output=%s", activateCode, activateOut.String())
	}
	var result struct {
		store.RunForkActivation
		Owner     string                                           `json:"selected_contract_activation_gate_owner"`
		Admission *store.RunForkSelectedContractExecutionAdmission `json:"selected_contract_execution_admission"`
	}
	if err := json.Unmarshal(activateOut.Bytes(), &result); err != nil {
		t.Fatalf("decode activation: %v\n%s", err, activateOut.String())
	}
	if !result.Activated || !result.SourceFrozen || result.ForkRunID != materialized.ForkRunID {
		t.Fatalf("activation = %#v", result.RunForkActivation)
	}
	if result.Owner != store.RunForkSelectedContractExecutionActivationGateOwner {
		t.Fatalf("selected activation owner = %q, want %q", result.Owner, store.RunForkSelectedContractExecutionActivationGateOwner)
	}
	if result.Admission == nil ||
		result.Admission.Owner != store.RunForkSelectedContractExecutionAdmissionOwner ||
		result.Admission.FrontierEventCount != 0 ||
		result.Admission.ContractBindingOwner != store.RunForkSelectedContractBindingOwner {
		t.Fatalf("selected admission = %#v", result.Admission)
	}
}

func TestRunForkRuntimeOwnerHarness_ActivateSelectedBindingRejectsDeliveryReplayWithoutPersistedRouteRecovery(t *testing.T) {
	dsn, db, _ := testutil.StartPostgres(t)
	setPostgresEnvFromDSN(t, dsn)
	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := uuid.NewString()
	at := time.Unix(1700000340, 0).UTC()
	ctx := context.Background()
	seedRunForkCLIActivationSourceWithoutRevision(t, db, runID, entityID, eventID, at)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, created_at
		)
		VALUES ($1::uuid, $2::uuid, 'agent', 'safe-agent', 'pending', 0, 'matched_agent_subscription', $3)
	`, runID, eventID, at); err != nil {
		t.Fatalf("seed safe pending delivery: %v", err)
	}
	captureRunForkCLIRevision(t, db, runID, runforkrevision.AllFamilies()...)
	repo := cliapp.RepoRoot()
	contractsRoot := filepath.Join(repo, "tests", "tier11-flow-composition", "test-sibling-both-instantiated-isolated")

	var materializeOut bytes.Buffer
	materializeCode := runForkRuntimeOwnerHarness(ctx, repo, []string{
		"--store", "postgres",
		"--materialize-only",
		"--run", runID,
		"--at", eventID,
		"--contracts", contractsRoot,
		"--json",
	}, &materializeOut)
	if materializeCode != 0 {
		t.Fatalf("materialize code=%d output=%s", materializeCode, materializeOut.String())
	}
	var materialized store.RunForkMaterialization
	if err := json.Unmarshal(materializeOut.Bytes(), &materialized); err != nil {
		t.Fatalf("decode materialization: %v\n%s", err, materializeOut.String())
	}

	var activateOut bytes.Buffer
	activateCode := runForkRuntimeOwnerHarness(ctx, repo, []string{
		"--store", "postgres",
		"--activate",
		"--run", materialized.ForkRunID,
		"--json",
	}, &activateOut)
	if activateCode != 1 {
		t.Fatalf("activate code=%d, want 1; output=%s", activateCode, activateOut.String())
	}
	if !strings.Contains(activateOut.String(), "requires persisted route recovery before delivery replay") {
		t.Fatalf("activate output = %q, want persisted route-recovery blocker", activateOut.String())
	}
	var sourceStatus, forkStatus string
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, runID).Scan(&sourceStatus); err != nil {
		t.Fatalf("load source status: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkStatus); err != nil {
		t.Fatalf("load fork status: %v", err)
	}
	if sourceStatus != "running" || forkStatus != store.RunForkMaterializedStatus {
		t.Fatalf("source/fork status = %s/%s, want running/paused", sourceStatus, forkStatus)
	}
	var replayRows, forkEvents, forkDeliveries int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_delivery_event_replays WHERE fork_run_id = $1::uuid`, materialized.ForkRunID).Scan(&replayRows); err != nil {
		t.Fatalf("count replay rows: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkEvents); err != nil {
		t.Fatalf("count fork events: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid`, materialized.ForkRunID).Scan(&forkDeliveries); err != nil {
		t.Fatalf("count fork deliveries: %v", err)
	}
	if replayRows != 0 || forkEvents != 0 || forkDeliveries != 0 {
		t.Fatalf("selected-bound replay mutation counts = replay:%d events:%d deliveries:%d, want 0/0/0", replayRows, forkEvents, forkDeliveries)
	}
}

func TestRunForkRuntimeOwnerHarness_NonDryRunWithoutMaterializeOnlyStaysFailClosed(t *testing.T) {
	var buf bytes.Buffer
	code := runForkRuntimeOwnerHarness(context.Background(), t.TempDir(), []string{
		"--run", uuid.NewString(),
		"--at", uuid.NewString(),
	}, &buf)
	if code != 2 {
		t.Fatalf("runForkRuntimeOwnerHarness code=%d, want 2; output=%s", code, buf.String())
	}
	if !strings.Contains(buf.String(), "mutating fork execution without --contracts is not implemented") {
		t.Fatalf("output = %q, want fail-closed fork execution message", buf.String())
	}
}

func seedRunForkCLIActivationSource(t *testing.T, db *sql.DB, runID, entityID, eventID string, at time.Time) {
	t.Helper()
	seedRunForkCLIActivationSourceWithoutRevision(t, db, runID, entityID, eventID, at)
	captureRunForkCLIRevision(t, db, runID, runforkrevision.AllFamilies()...)
}

func seedRunForkCLIActivationSourceWithoutRevision(t *testing.T, db *sql.DB, runID, entityID, eventID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', $2)
	`, runID, at.Add(-time.Minute)); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (execution_mode,
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ('live', $1::uuid, $2::uuid, 'fork.cli.activate', $3::uuid, '', 'entity', '{}'::jsonb, 'test', 'platform', $4)
	`, runID, eventID, entityID, at); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, old_value, new_value, caused_by_event, writer_type, writer_id, handler_step, created_at
		)
		VALUES
			($1::uuid, $2::uuid, 'current_state', 'null'::jsonb, '"ready"'::jsonb, $3::uuid, 'platform', 'cli-test', 'seed', $4),
			($1::uuid, $2::uuid, 'name', 'null'::jsonb, '"CLI Entity"'::jsonb, $3::uuid, 'platform', 'cli-test', 'seed', $4)
	`, runID, entityID, eventID, at); err != nil {
		t.Fatalf("seed mutations: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, name,
			current_state, gates, fields, accumulator, revision,
			entered_state_at, created_at, updated_at
		)
		VALUES (
			$1::uuid, $2::uuid, 'flow-a/1', 'default', 'CLI Entity',
			'ready', '{}'::jsonb, '{"name":"CLI Entity"}'::jsonb, '{}'::jsonb, 1,
			$3, $3, $3
		)
	`, runID, entityID, at); err != nil {
		t.Fatalf("seed entity_state: %v", err)
	}
}

func seedRunForkCLISelectedExecutionSource(t *testing.T, db *sql.DB, runID, entityID, eventID string, at time.Time) {
	t.Helper()
	seedRunForkSelectedExecutionSourceEvent(t, db, runID, entityID, eventID, "item.received", "test-node", "pending", "CLI Selected Execution Entity", "cli-selected-execution-test", at)
}

func seedRunForkCLISelectedExecutionDiagnosticPlatformDeadLetter(t *testing.T, db *sql.DB, runID, eventID string, at time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (execution_mode,
			run_id, event_id, event_name, entity_id, flow_instance, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ('live',
			$1::uuid, $2::uuid, 'platform.runtime_log', NULL, NULL, 'global',
			'{"level":"info","message":"diagnostic platform row must remain lineage-only"}'::jsonb,
			'pipeline', 'platform', $3
		)
	`, runID, eventID, at); err != nil {
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
	`, eventID, at); err != nil {
		t.Fatalf("seed diagnostic platform receipt: %v", err)
	}
	captureRunForkCLIRevision(t, db, runID, runforkrevision.FamilyEvents, runforkrevision.FamilyEventReceipts)
}

func runForkPlanHasBlocker(blockers []store.RunForkUnsupportedBlocker, code string) bool {
	for _, blocker := range blockers {
		if blocker.Code == code {
			return true
		}
	}
	return false
}

func runForkPlanHasString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func runForkPlanHasBoundary(values []store.RunForkSelectedContractExecutionBoundary, concept, disposition string) bool {
	for _, value := range values {
		if value.Concept == concept && value.Disposition == disposition {
			return true
		}
	}
	return false
}

func runForkReadinessFactHas(values []store.RunForkSelectedContractReadinessFact, fact string) bool {
	for _, value := range values {
		if value.Fact == fact {
			return true
		}
	}
	return false
}

func runForkReadinessFactHasDisposition(values []store.RunForkSelectedContractReadinessFact, fact, disposition string) bool {
	for _, value := range values {
		if value.Fact == fact && value.Disposition == disposition {
			return true
		}
	}
	return false
}

func setPostgresEnvFromDSN(t *testing.T, dsn string) {
	t.Helper()
	connection, err := testpostgres.ParseConnection(dsn)
	if err != nil {
		t.Fatalf("parse canonical test Postgres DSN: %v", err)
	}
	parsed := connection.Parameters()
	t.Setenv("PGPASSWORD", parsed.Password)
	configPath := filepath.Join(t.TempDir(), "swarm.yaml")
	t.Setenv("SWARM_CONFIG", configPath)
	writeRuntimeConfigText(t, configPath, fmt.Sprintf(`store:
  backend: postgres
database:
  host: %q
  port: %d
  name: %q
  user: %q
  password_env: PGPASSWORD
  sslmode: %q
  pool_size: 5
llm:
  backend: claude_cli
  session:
    lock_ttl: 10s
    rotate_after_turns: 40
    rotate_on_parse_failures: 3
  claude_cli:
    command: true
    timeout: 2s
    output_format: json
`, parsed.Host, parsed.Port, parsed.Database, parsed.User, parsed.SSLMode))
}

func writeRuntimeConfigText(t *testing.T, path, configText string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(configText), 0o644); err != nil {
		t.Fatalf("write runtime config %s: %v", path, err)
	}
}
