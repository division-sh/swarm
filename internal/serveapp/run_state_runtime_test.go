package serveapp

import (
	"bytes"
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimebustest "github.com/division-sh/swarm/internal/runtime/bus/bustest"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/eventreceiver"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/division-sh/swarm/internal/testutil/stagecatalogfixture"
	"github.com/google/uuid"
)

const runStatusTestRuntimeInstanceID = "22222222-2222-2222-2222-222222222222"

type runStatusManagerBus struct {
	*runtimebus.EventBus
	t *testing.T
}

func (b runStatusManagerBus) Publish(ctx context.Context, evt events.Event) error {
	err := b.EventBus.Publish(ctx, evt)
	if err != nil {
		b.t.Logf("run-status manager publication %s: %v", evt.Type(), err)
	}
	return err
}

func runStatusSubscriptionSource(t *testing.T, withAgent bool) semanticview.Source {
	t.Helper()
	root := t.TempDir()
	files := map[string]string{
		"schema.yaml":   "name: run-status\nrequired_agents: []\nstages:\n  ready: {}\npins:\n  inputs: [scan.requested]\n  outputs: [scan.completed]\n",
		"events.yaml":   "scan.requested:\n  topic: text\nscan.completed:\n",
		"entities.yaml": "default: {}\n",
	}
	if withAgent {
		files["agents.yaml"] = "agent-1:\n  role: worker\n  intent: {inline: Complete the scan.}\n  model: regular\n  subscriptions: [scan.requested]\n"
	}
	for name, body := range files {
		writeWorkflowValidationFixtureFile(t, filepath.Join(root, name), body)
	}
	return semanticview.Wrap(loadWorkflowValidationBundleAt(t, root))
}

func runStatusAgentConfig(t *testing.T, source semanticview.Source, runID, agentID string) runtimeactors.AgentConfig {
	t.Helper()
	scope, found := source.FlowScopeByID(".")
	if !found {
		t.Fatal("run-status root declaration is required")
	}
	plan, err := semanticview.FlowAgentNamePlan(source, scope, agentID)
	if err != nil {
		t.Fatal(err)
	}
	name, err := plan.Materialize()
	if err != nil {
		t.Fatal(err)
	}
	identity, err := agentidentity.New(runID, name, agentidentity.RootRoute())
	if err != nil {
		t.Fatal(err)
	}
	profile, err := llmselection.ResolveActiveBackend("anthropic")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := runtimellm.ResolveAgentExecution(executionposture.Live, profile, llmselection.BuiltInModelAliases(), serveTestAgentConfig(runtimeactors.AgentConfig{
		ID: agentID, Identity: identity,
		FlowID: ".", Role: "worker", Type: "stub", Model: "regular",
		Subscriptions: []string{"scan.requested"},
	}))
	if err != nil {
		t.Fatalf("resolve run-status agent before admitting its revision: %v", err)
	}
	return resolved.Actor
}

func runStatusAuthorActivityContext(source runtimecorrelation.SourceArtifactFact) context.Context {
	ctx := runtimecorrelation.WithRuntimeInstanceID(context.Background(), runStatusTestRuntimeInstanceID)
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, source)
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runStatusTestRuntimeInstanceID, source.BundleHash()))
}

type runStatusEventCatalogRegistrar interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

func registerRunStatusEventCatalog(t *testing.T, registrar runStatusEventCatalogRegistrar, source runtimecorrelation.SourceArtifactFact) {
	t.Helper()
	scope, ok := runtimeauthoractivity.ScopeFromContext(runStatusAuthorActivityContext(source))
	if !ok {
		t.Fatal("run status author activity scope is unavailable")
	}
	lease, err := registrar.RegisterAuthorActivityEventCatalog(scope, []runtimeauthoractivity.EventDescriptor{
		{EventType: "scan.completed", Disposition: runtimeauthoractivity.StoryAuthored},
		{EventType: "scan.requested", Disposition: runtimeauthoractivity.StoryAuthored},
	})
	if err != nil {
		t.Fatalf("register run status event catalog: %v", err)
	}
	t.Cleanup(lease.Release)
}

func newRunStatusEventBus(t *testing.T, pg *store.PostgresStore, runID string, withAgent bool) (*runtimebus.EventBus, *worklifetime.RuntimeOccurrence, runtimecorrelation.SourceArtifactFact, semanticview.Source) {
	t.Helper()
	workflow := runtimepipeline.NewWorkflowPersistence(pg)
	source := runStatusSubscriptionSource(t, withAgent)
	bundle, _ := semanticview.Bundle(source)
	sourceFact := sourceartifactfixture.RequireArtifact(t, context.Background(), pg, bundle.SourceArtifact)
	workOwner := newSupervisorTestRuntimeOccurrence(t, sourceFact.BundleHash())
	authority, err := runtimedelivery.NewNormalExecutionAuthority(sourceFact, runStatusTestRuntimeInstanceID, 1)
	if err != nil {
		t.Fatalf("construct run status delivery authority: %v", err)
	}
	if err := pg.ActivateDeliveryAuthority(runStatusAuthorActivityContext(sourceFact), authority); err != nil {
		t.Fatalf("activate run status delivery authority: %v", err)
	}
	bus, err := runtimebus.NewEventBusWithOptions(pg, runtimebus.EventBusOptions{
		ContractBundle:      source,
		ExecutionPosture:    executionposture.Live,
		RuntimeInstanceID:   runStatusTestRuntimeInstanceID,
		SourceArtifactFact:  sourceFact,
		WorkOwner:           workOwner,
		PipelineObligations: pg.PipelineObligations(),
		DeliveryAuthority:   authority,
		Durable: runtimebus.DurableDependencies{
			ReplyContext: pg, RunLifecycle: pg, DeliveryLifecycle: pg,
			FlowRouteTopology: pg,
			ActiveAgents:      pg, ActiveFlows: pg, TargetOwners: pg, PreparedEvents: pg,
			TargetFailureRecorder: pg, RunOrigins: pg, StandingRestarts: pg,
			Instances: workflow, ConstructionPublications: workflow,
		}, ReceiverExecution: eventreceiver.NormalExecution(),
	})
	if err != nil {
		t.Fatalf("NewEventBusWithOptions: %v", err)
	}
	if err := bus.SetDeliveryContinuationOwner(runtimebustest.NewDeliveryContinuationOwner(false)); err != nil {
		t.Fatalf("install run status delivery continuation owner: %v", err)
	}
	module, _, err := cliapp.NewSwarmWorkflowModuleForBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	coordinator := runtimepipeline.NewPipelineCoordinatorWithOptions(bus, runtimepipeline.PipelineCoordinatorOptions{
		Module: module, Persistence: workflow, SourceArtifactFact: sourceFact, WorkOwner: workOwner,
		ExecutionPosture: executionposture.Live, ReceiverExecution: eventreceiver.NormalExecution(),
		RunLifecycle: pg, PipelineObligations: pg.PipelineObligations(), DeliveryStore: pg, DeadLetters: pg,
		DecisionCards: pg, ProposedEffects: pg, HumanTasks: pg,
		DecisionCardDraftExpiry: pg, HumanTaskExpiry: pg, DeliveryRuntime: bus,
	})
	if coordinator == nil {
		t.Fatal("run-status constructor coordinator was not admitted")
	}
	planner := runtimemanager.NewAgentManagerWithOptions(bus, nil, runtimemanager.AgentManagerOptions{
		BaseContext: runStatusAuthorActivityContext(sourceFact), SemanticSource: source, SourceArtifactFact: sourceFact,
		WorkflowInstances: coordinator, WorkOwner: workOwner, ExecutionPosture: executionposture.Live,
		ReceiverExecution: eventreceiver.NormalExecution(),
	})
	// Completion controls use a prepared native root; they do not qualify
	// public launch or attachment. Publication consumes its exact index evidence.
	ctx := runtimeeffects.WithExecutionMode(runtimecorrelation.WithRunID(runStatusAuthorActivityContext(sourceFact), runID), runtimeeffects.ExecutionModeLive)
	storetest.RequireRun(t, ctx, pg, storetest.RunFixture{
		Origin: storetest.ScenarioSetupOrigin(), RunID: runID, BundleHash: sourceFact.BundleHash(), Artifact: bundle.SourceArtifact,
	})
	plan, err := planner.PrepareFlowInstanceActivation(ctx, runtimepipeline.FlowInstanceActivationRequest{
		ContractBundle: source, OccurredAt: time.Now().UTC(),
		Instance: flowidentity.Stored(source, ".", runID, runID, runID, ""),
	})
	if err != nil {
		t.Fatal(err)
	}
	committed, err := pg.CommitFlowInstanceActivation(ctx, runtimebus.FlowInstanceActivationCommand{Plan: plan})
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("prepare run-status root: acknowledged=%t created=%t err=%v", committed.Acknowledged, committed.Created, err)
	}
	return bus, workOwner, sourceFact, source
}

func publishRunStatusExistingRootEvent(t *testing.T, bus *runtimebus.EventBus, source runtimecorrelation.SourceArtifactFact, runID, entityID string) string {
	t.Helper()
	eventID := uuid.NewString()
	if err := bus.Publish(runStatusAuthorActivityContext(source), eventtest.ExistingRunRootIngress(
		eventID,
		events.EventType("scan.requested"),
		"api.v1",
		"",
		[]byte(`{"topic":"sample"}`),
		0,
		runID,
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("publish existing-run root event: %v", err)
	}
	return eventID
}

func markRunStatusCompleted(t *testing.T, pg *store.PostgresStore, source runtimecorrelation.SourceArtifactFact, eventID string) {
	t.Helper()
	var runID, bundleHash string
	if err := storetest.DatabaseForTest(pg).QueryRowContext(runStatusAuthorActivityContext(source), `
		SELECT r.run_id::text, r.bundle_hash
		FROM events e
		JOIN runs r ON r.run_id = e.run_id
		WHERE e.event_id = $1::uuid
	`, eventID).Scan(&runID, &bundleHash); err != nil {
		t.Fatalf("load completion candidate identity: %v", err)
	}
	if _, err := storetest.ExecuteRunCompletionCandidate(
		runStatusAuthorActivityContext(source), pg, bundleHash, runID,
		stagecatalogfixture.NewFinalCatalog([]string{"ready"}, map[string][]string{".": {"ready"}}),
	); err != nil {
		t.Fatalf("execute normal run completion candidate: %v", err)
	}
}

func TestCLI_ServeRetiredPlatformSpecWritesOnlyStderr(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	var stdout, stderr bytes.Buffer
	configPath := writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", filepath.Join(t.TempDir(), "retired-spec.sqlite"), nil)
	code := executeCLIFrom(context.Background(), repoRootForTest(), []string{
		"serve",
		filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		"--config", configPath,
		"--platform-spec", filepath.Join(t.TempDir(), "platform-spec.yaml"),
		"--store", "sqlite",
	}, &stdout, &stderr, Run)
	if code != cliapp.CLIExitValidation {
		t.Fatalf("serve code = %d, want %d\nstdout=%s\nstderr=%s", code, cliapp.CLIExitValidation, stdout.String(), stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("retired platform spec contaminated stdout: %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "unknown flag: --platform-spec") {
		t.Fatalf("retired platform spec stderr is incomplete:\n%s", stderr.String())
	}
}

func waitRunStatusEventSettlement(t *testing.T, db *sql.DB, runID string, wantEvents int) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var (
			eventCount       int
			activeDeliveries int
		)
		err := db.QueryRowContext(ctx, `
			SELECT
				(SELECT COUNT(*) FROM events WHERE run_id = $1::uuid AND event_name <> 'platform.runtime_log'),
				(SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid AND status IN ('pending', 'in_progress'))
		`, runID).Scan(&eventCount, &activeDeliveries)
		if err == nil && eventCount >= wantEvents && activeDeliveries == 0 {
			return
		}
		if time.Now().After(deadline) {
			rows, queryErr := db.QueryContext(ctx, `SELECT status, COALESCE(CAST(failure AS TEXT), '') FROM event_deliveries WHERE run_id=$1::uuid`, runID)
			if queryErr == nil {
				for rows.Next() {
					var status, failure string
					if scanErr := rows.Scan(&status, &failure); scanErr == nil {
						t.Logf("delivery status=%s failure=%s", status, failure)
					}
				}
				rows.Close()
			}
			t.Fatalf("run %s did not settle after release: last err=%v event_count=%d want_events=%d active_deliveries=%d", runID, err, eventCount, wantEvents, activeDeliveries)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunState_UsesDurableCompletedRunState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	runID := uuid.NewString()
	eb, _, source, _ := newRunStatusEventBus(t, pg, runID, false)
	registerRunStatusEventCatalog(t, pg, source)
	entityID := runID
	eventID := publishRunStatusExistingRootEvent(t, eb, source, runID, entityID)
	markRunStatusCompleted(t, pg, source, eventID)

	ctx := context.Background()
	deadline := time.Now().Add(2 * time.Second)
	var endedAt sql.NullTime
	for {
		var status string
		err := db.QueryRowContext(ctx, `
			SELECT COALESCE(status, ''), ended_at
			FROM runs
			WHERE run_id = $1::uuid
		`, runID).Scan(&status, &endedAt)
		if err == nil && status == "completed" && endedAt.Valid {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not reach durable completed state: last err=%v", runID, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if endedAt.Time.IsZero() {
		t.Fatal("expected durable ended_at for completed run")
	}
}

func TestRunState_KeepsSupportedRunRunningUntilManagerWorkSettles(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	runID := uuid.NewString()
	eb, workOwner, source, semanticSource := newRunStatusEventBus(t, pg, runID, true)
	registerRunStatusEventCatalog(t, pg, source)

	agentStarted := make(chan struct{}, 1)
	releaseAgent := make(chan struct{})
	testAgent := delayedRunStatusAgent{
		id:            "agent-1",
		subscriptions: []events.EventType{"scan.requested"},
		started:       agentStarted,
		release:       releaseAgent,
	}
	am := runtimemanager.NewAgentManagerWithOptions(runStatusManagerBus{eb, t}, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		if cfg.ID != testAgent.id {
			t.Fatalf("unexpected agent id: %q", cfg.ID)
		}
		return testAgent, nil
	}, runtimemanager.AgentManagerOptions{
		ExecutionPosture: executionposture.Live,
		LifecycleStore:   storetest.AgentLifecycleFixture(t, pg),
		SemanticSource:   semanticSource,
		DeliveryStore:    pg,
		SessionResetter:  pg,
		PersistenceRoles: selectedStoreManagerPersistenceRoles(pg, eb),
		WorkOwner:        workOwner, ReceiverExecution: eventreceiver.NormalExecution(),
	}, pg)
	config := runStatusAgentConfig(t, semanticSource, runID, testAgent.id)
	registerServeTestDurableAgent(t, runStatusAuthorActivityContext(source), pg, am, config, source)
	if err := am.Run(managedRuntimeAdmissionContextForTest(t, runStatusAuthorActivityContext(source))); err != nil {
		t.Fatalf("AgentManager.Run: %v", err)
	}
	installServeTestExactAgentReadiness(t, eb, config.Identity)
	defer func() { _ = am.Shutdown() }()

	entityID := runID
	eventID := publishRunStatusExistingRootEvent(t, eb, source, runID, entityID)

	select {
	case <-agentStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for agent work to start")
	}

	ctx := context.Background()
	var (
		status           string
		eventCount       int
		entityCount      int
		activeDeliveries int
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), event_count,
		       (SELECT COUNT(DISTINCT entity_id) FROM entity_state WHERE run_id = runs.run_id)
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&status, &eventCount, &entityCount); err != nil {
		t.Fatalf("load in-flight run row: %v", err)
	}
	if status != "running" {
		t.Fatalf("in-flight run status = %q, want running", status)
	}
	var semanticCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE run_id=$1::uuid AND event_name <> 'platform.runtime_log'`, runID).Scan(&semanticCount); err != nil {
		t.Fatal(err)
	}
	if semanticCount != 1 || eventCount < semanticCount {
		t.Fatalf("in-flight total/semantic events = %d/%d, want one semantic root plus durable diagnostics", eventCount, semanticCount)
	}
	if entityCount != 1 {
		t.Fatalf("in-flight entity_count = %d, want 1", entityCount)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND status IN ('pending', 'in_progress')
	`, runID).Scan(&activeDeliveries); err != nil {
		t.Fatalf("count active deliveries: %v", err)
	}
	if activeDeliveries == 0 {
		t.Fatal("expected active delivery while agent work is blocked")
	}

	close(releaseAgent)
	waitRunStatusEventSettlement(t, db, runID, 2)
	markRunStatusCompleted(t, pg, source, eventID)

	deadline := time.Now().Add(3 * time.Second)
	for {
		err := db.QueryRowContext(ctx, `
			SELECT COALESCE(status, ''), event_count,
			       (SELECT COUNT(DISTINCT entity_id) FROM entity_state WHERE run_id = runs.run_id)
			FROM runs
			WHERE run_id = $1::uuid
		`, runID).Scan(&status, &eventCount, &entityCount)
		if err == nil && status == "completed" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not reach coherent completed state: last err=%v status=%q event_count=%d entity_count=%d", runID, err, status, eventCount, entityCount)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if eventCount < 2 {
		t.Fatalf("completed event_count = %d, want downstream event activity", eventCount)
	}
	if entityCount != 1 {
		t.Fatalf("completed entity_count = %d, want 1", entityCount)
	}
	var extraRunningRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM runs
		WHERE run_id <> $1::uuid
		  AND status = 'running'
	`, runID).Scan(&extraRunningRows); err != nil {
		t.Fatalf("count extra running rows: %v", err)
	}
	if extraRunningRows != 0 {
		t.Fatalf("extra running rows = %d, want 0", extraRunningRows)
	}
}

func TestRunState_PreservesRunningTruthWhileManagerWorkIsActive(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	runID := uuid.NewString()
	eb, workOwner, source, semanticSource := newRunStatusEventBus(t, pg, runID, true)
	registerRunStatusEventCatalog(t, pg, source)

	agentStarted := make(chan struct{}, 1)
	releaseAgent := make(chan struct{})
	testAgent := delayedRunStatusAgent{
		id:            "agent-1",
		subscriptions: []events.EventType{"scan.requested"},
		started:       agentStarted,
		release:       releaseAgent,
	}
	am := runtimemanager.NewAgentManagerWithOptions(runStatusManagerBus{eb, t}, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		if cfg.ID != testAgent.id {
			t.Fatalf("unexpected agent id: %q", cfg.ID)
		}
		return testAgent, nil
	}, runtimemanager.AgentManagerOptions{
		ExecutionPosture: executionposture.Live,
		LifecycleStore:   storetest.AgentLifecycleFixture(t, pg),
		SemanticSource:   semanticSource,
		DeliveryStore:    pg,
		SessionResetter:  pg,
		PersistenceRoles: selectedStoreManagerPersistenceRoles(pg, eb),
		WorkOwner:        workOwner, ReceiverExecution: eventreceiver.NormalExecution(),
	}, pg)
	config := runStatusAgentConfig(t, semanticSource, runID, testAgent.id)
	registerServeTestDurableAgent(t, runStatusAuthorActivityContext(source), pg, am, config, source)
	if err := am.Run(managedRuntimeAdmissionContextForTest(t, runStatusAuthorActivityContext(source))); err != nil {
		t.Fatalf("AgentManager.Run: %v", err)
	}
	installServeTestExactAgentReadiness(t, eb, config.Identity)
	defer func() { _ = am.Shutdown() }()

	entityID := runID
	eventID := publishRunStatusExistingRootEvent(t, eb, source, runID, entityID)

	select {
	case <-agentStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for agent work to start")
	}

	time.Sleep(120 * time.Millisecond)

	ctx := context.Background()
	var (
		status           string
		activeDeliveries int
		endedAt          sql.NullTime
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&status, &endedAt); err != nil {
		t.Fatalf("load timed-out run row: %v", err)
	}
	if status != "running" {
		t.Fatalf("timed-out run status = %q, want running", status)
	}
	if endedAt.Valid {
		t.Fatalf("timed-out run ended_at = %s, want NULL while same-run work remains active", endedAt.Time)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE run_id = $1::uuid
		  AND status IN ('pending', 'in_progress')
	`, runID).Scan(&activeDeliveries); err != nil {
		t.Fatalf("count active deliveries after timeout window: %v", err)
	}
	if activeDeliveries == 0 {
		t.Fatal("expected same-run active delivery after shutdown timeout window")
	}
	if got := workOwner.ActiveCount(); got == 0 {
		t.Fatal("expected runtime occurrence to retain active manager work after shutdown timeout window")
	}
	close(releaseAgent)
	waitRunStatusEventSettlement(t, db, runID, 2)
	markRunStatusCompleted(t, pg, source, eventID)

	deadline := time.Now().Add(3 * time.Second)
	for {
		err := db.QueryRowContext(ctx, `
			SELECT COALESCE(status, ''), ended_at
			FROM runs
			WHERE run_id = $1::uuid
		`, runID).Scan(&status, &endedAt)
		if err == nil && status == "completed" && endedAt.Valid {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not reach coherent completed state after release: last err=%v status=%q ended_at_valid=%v", runID, err, status, endedAt.Valid)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
