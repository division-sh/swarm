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
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/store"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const runStatusTestRuntimeInstanceID = "22222222-2222-2222-2222-222222222222"
const runStatusTestBundleHash = "bundle-v1:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func runStatusAuthorActivityContext() context.Context {
	ctx := runtimecorrelation.WithRuntimeInstanceID(context.Background(), runStatusTestRuntimeInstanceID)
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, runtimecorrelation.BundleSourceFact{
		BundleHash:   runStatusTestBundleHash,
		BundleSource: storerunlifecycle.BundleSourceEphemeral,
	})
	return runtimeauthoractivity.WithScope(ctx, runtimeauthoractivity.BundleScope(runStatusTestRuntimeInstanceID, runStatusTestBundleHash))
}

type runStatusEventCatalogRegistrar interface {
	RegisterAuthorActivityEventCatalog(runtimeauthoractivity.Scope, []runtimeauthoractivity.EventDescriptor) (*runtimeauthoractivity.EventCatalogLease, error)
}

func registerRunStatusEventCatalog(t *testing.T, registrar runStatusEventCatalogRegistrar) {
	t.Helper()
	scope, ok := runtimeauthoractivity.ScopeFromContext(runStatusAuthorActivityContext())
	if !ok {
		t.Fatal("run status author activity scope is unavailable")
	}
	lease, err := registrar.RegisterAuthorActivityEventCatalog(scope, []runtimeauthoractivity.EventDescriptor{
		{EventType: "scan.completed", Disposition: runtimeauthoractivity.StoryDifferent},
		{EventType: "scan.requested", Disposition: runtimeauthoractivity.StoryDifferent},
	})
	if err != nil {
		t.Fatalf("register run status event catalog: %v", err)
	}
	t.Cleanup(lease.Release)
}

func publishRunStatusRootEvent(t *testing.T, bus *runtimebus.EventBus, runID, entityID string) string {
	t.Helper()
	eventID := uuid.NewString()
	if err := bus.Publish(runStatusAuthorActivityContext(), eventtest.RunCreatingRootIngress(
		eventID,
		events.EventType("scan.requested"),
		"api.v1",
		"",
		[]byte(`{"topic":"sample"}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now().UTC(),
	)); err != nil {
		t.Fatalf("publish root event: %v", err)
	}
	return eventID
}

func seedRunStatusEntityState(t *testing.T, db *sql.DB, runID, entityID string) {
	t.Helper()
	now := time.Now().UTC()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 'run-status-test', 'default', 'status-entity', 'Status Entity', 'ready',
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, $3, $3, $3
		)
	`, runID, entityID, now); err != nil {
		t.Fatalf("seed run status entity_state: %v", err)
	}
	if err := storerunlifecycle.SyncCounts(context.Background(), db, runID); err != nil {
		t.Fatalf("sync run status entity_count: %v", err)
	}
}

func markRunStatusCompleted(t *testing.T, pg *store.PostgresStore, eventID string) {
	t.Helper()
	if err := pg.ConvergeNormalRunCompletion(runStatusAuthorActivityContext(), eventID, []string{"ready"}, map[string][]string{"run-status-test": {"ready"}}); err != nil {
		t.Fatalf("converge normal run completion: %v", err)
	}
}

func TestCLI_ServeMissingPlatformSpecWritesOnlyStderr(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	var stdout, stderr bytes.Buffer
	missing := filepath.Join(t.TempDir(), "missing-platform-spec.yaml")
	configPath := writeStoreBackendRuntimeConfigWithWorkspaceFields(t, "sqlite", filepath.Join(t.TempDir(), "missing-spec.sqlite"), nil)
	code := cliapp.Execute(context.Background(), cliapp.RepoRoot(), []string{
		"serve",
		"--config", configPath,
		"--contracts", filepath.Join("tests", "tier8-boot-verification", "test-boot-success"),
		"--platform-spec", missing,
		"--store", "sqlite",
	}, &stdout, &stderr, Run)
	if code == 0 {
		t.Fatalf("serve code = 0, want startup failure\nstdout=%s\nstderr=%s", stdout.String(), stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("missing platform spec contaminated stdout: %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), "ERROR: serve failed") || !strings.Contains(stderr.String(), "missing-platform-spec.yaml") {
		t.Fatalf("missing platform spec stderr is incomplete:\n%s", stderr.String())
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
				(SELECT COUNT(*) FROM events WHERE run_id = $1::uuid),
				(SELECT COUNT(*) FROM event_deliveries WHERE run_id = $1::uuid AND status IN ('pending', 'in_progress'))
		`, runID).Scan(&eventCount, &activeDeliveries)
		if err == nil && eventCount >= wantEvents && activeDeliveries == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not settle after release: last err=%v event_count=%d want_events=%d active_deliveries=%d", runID, err, eventCount, wantEvents, activeDeliveries)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunState_UsesDurableCompletedRunState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := storetest.AdmitPostgresRuntimeStore(t, db)
	eb, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	registerRunStatusEventCatalog(t, pg)
	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := publishRunStatusRootEvent(t, eb, runID, entityID)
	seedRunStatusEntityState(t, db, runID, entityID)
	markRunStatusCompleted(t, pg, eventID)

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
	eb, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	registerRunStatusEventCatalog(t, pg)

	agentStarted := make(chan struct{}, 1)
	releaseAgent := make(chan struct{})
	testAgent := delayedRunStatusAgent{
		id:            "agent-1",
		subscriptions: []events.EventType{"scan.requested"},
		started:       agentStarted,
		release:       releaseAgent,
	}
	am := runtimemanager.NewAgentManager(eb, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		if cfg.ID != testAgent.id {
			t.Fatalf("unexpected agent id: %q", cfg.ID)
		}
		return testAgent, nil
	}, pg)
	if err := am.SpawnAgent(runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            testAgent.id,
		Model:         "regular",
		Subscriptions: []string{"scan.requested"},
	}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	if err := am.Run(managedRuntimeAdmissionContextForTest(t, runStatusAuthorActivityContext())); err != nil {
		t.Fatalf("AgentManager.Run: %v", err)
	}
	defer func() { _ = am.Shutdown() }()

	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := publishRunStatusRootEvent(t, eb, runID, entityID)
	seedRunStatusEntityState(t, db, runID, entityID)

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
		SELECT COALESCE(status, ''), event_count, entity_count
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&status, &eventCount, &entityCount); err != nil {
		t.Fatalf("load in-flight run row: %v", err)
	}
	if status != "running" {
		t.Fatalf("in-flight run status = %q, want running", status)
	}
	if eventCount != 1 {
		t.Fatalf("in-flight event_count = %d, want 1 root event", eventCount)
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
	markRunStatusCompleted(t, pg, eventID)

	deadline := time.Now().Add(3 * time.Second)
	for {
		err := db.QueryRowContext(ctx, `
			SELECT COALESCE(status, ''), event_count, entity_count
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
	eb, err := runtimebus.NewEventBus(pg)
	if err != nil {
		t.Fatalf("NewEventBus: %v", err)
	}
	registerRunStatusEventCatalog(t, pg)

	agentStarted := make(chan struct{}, 1)
	releaseAgent := make(chan struct{})
	testAgent := delayedRunStatusAgent{
		id:            "agent-1",
		subscriptions: []events.EventType{"scan.requested"},
		started:       agentStarted,
		release:       releaseAgent,
	}
	am := runtimemanager.NewAgentManager(eb, func(cfg runtimeactors.AgentConfig) (runtimemanager.Agent, error) {
		if cfg.ID != testAgent.id {
			t.Fatalf("unexpected agent id: %q", cfg.ID)
		}
		return testAgent, nil
	}, pg)
	if err := am.SpawnAgent(runtimeactors.AgentConfig{
		ExecutionMode: "live",
		ID:            testAgent.id,
		Model:         "regular",
		Subscriptions: []string{"scan.requested"},
	}); err != nil {
		t.Fatalf("SpawnAgent: %v", err)
	}
	if err := am.Run(managedRuntimeAdmissionContextForTest(t, runStatusAuthorActivityContext())); err != nil {
		t.Fatalf("AgentManager.Run: %v", err)
	}
	defer func() { _ = am.Shutdown() }()

	runID := uuid.NewString()
	entityID := uuid.NewString()
	eventID := publishRunStatusRootEvent(t, eb, runID, entityID)
	seedRunStatusEntityState(t, db, runID, entityID)

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
		t.Fatal("expected same-run active delivery after builder timeout window")
	}
	if got := am.InFlightCount(); got == 0 {
		t.Fatal("expected live in-flight manager work after builder timeout window")
	}
	close(releaseAgent)
	waitRunStatusEventSettlement(t, db, runID, 2)
	markRunStatusCompleted(t, pg, eventID)

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
