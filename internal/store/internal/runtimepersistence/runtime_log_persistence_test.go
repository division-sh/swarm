package runtimepersistence

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/computemodule"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSQLiteRuntimeLogPersistenceWritesLoggerRowsForObservability(t *testing.T) {
	ctx := runtimeeffects.WithExecutionMode(testAuthorActivityContext(), runtimeeffects.ExecutionModeLive)
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	subjectEventID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	if err := commitSemanticEventFixture(ctx, store, eventtest.RunCreatingRootIngress(subjectEventID,

		events.EventType("validation/validation.package_ready"),
		"agent-1", "", json.RawMessage(`{"ready":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("seed sqlite subject event: %v", err)
	}

	logger := runtimepkg.NewRuntimeLogger(store, executionposture.Live)
	if err := logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level:     "warn",
		Message:   "sqlite diagnostic persisted",
		Component: "eventbus",
		Action:    "lineage_lookup",
		EventID:   subjectEventID,
		EventType: "validation/validation.package_ready",
		SessionID: "session-1",
	}); err != nil {
		t.Fatalf("RuntimeLogger.Log sqlite: %v", err)
	}

	logs, err := store.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{
		RunID:     runID,
		Component: "eventbus",
		Level:     "warn",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs sqlite: %v", err)
	}
	if len(logs.Logs) != 1 {
		t.Fatalf("sqlite runtime logs = %#v, want one logger-written row", logs.Logs)
	}
	log := logs.Logs[0]
	if log.RunID != runID || log.SessionID != "session-1" || log.Message != "sqlite diagnostic persisted" {
		t.Fatalf("sqlite runtime log = %#v, want run/session/message", log)
	}
	if log.Source != "runtime" {
		t.Fatalf("sqlite runtime log source = %q, want runtime", log.Source)
	}
	if got := strings.TrimSpace(log.ParentEventID); got != subjectEventID {
		t.Fatalf("sqlite runtime log parent_event_id = %q, want %q", got, subjectEventID)
	}
	var sourceEventID string
	if err := store.backend.QueryRowContext(ctx, `
		SELECT COALESCE(source_event_id, '')
		FROM events
		WHERE event_id = ?
	`, log.LogID).Scan(&sourceEventID); err != nil {
		t.Fatalf("load sqlite runtime log source_event_id: %v", err)
	}
	if sourceEventID != subjectEventID {
		t.Fatalf("sqlite source_event_id = %q, want %q", sourceEventID, subjectEventID)
	}
}

func TestSQLiteRuntimeLogCarriesComputeModuleReplayEvidenceForReplayConsumer(t *testing.T) {
	ctx := runtimeeffects.WithExecutionMode(testAuthorActivityContext(), runtimeeffects.ExecutionModeLive)
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	requireRunFixtureForTest(t, ctx, NewSQLiteRuntimeStoreForTest(store.backend.ConstructionHandle()), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: time.Now().UTC()})

	envelope := computeModuleReplayEvidenceTestEnvelope()
	detail := computemodule.NewReplayEvidenceDetail([]computemodule.ReplayEnvelope{envelope})
	detail["node_id"] = "node-a"
	logger := runtimepkg.NewRuntimeLogger(store, executionposture.Live)
	if err := logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level:     "info",
		Message:   "Compute module replay evidence recorded",
		Component: "compute_module",
		Action:    computemodule.ReplayEvidenceAction,
		EventID:   "evt-a",
		Detail:    detail,
	}); err != nil {
		t.Fatalf("RuntimeLogger.Log: %v", err)
	}
	loaded, err := store.LoadComputeModuleReplayEvidenceForExecution(ctx, runID, "evt-a", "node-a")
	if err != nil {
		t.Fatalf("LoadComputeModuleReplayEvidenceForExecution: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("loaded replay evidence = %#v, want one envelope", loaded)
	}
	if loaded[0].Normalized() != envelope.Normalized() {
		t.Fatalf("loaded envelope = %#v, want %#v", loaded[0].Normalized(), envelope.Normalized())
	}

	actual := loaded[0]
	actual.OutputHash = "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	finding := computemodule.CompareReplayEnvelopes(loaded[0], actual)
	if finding == nil || finding.Kind != computemodule.ReplayFindingResultDivergence || finding.Field != "output_hash" {
		t.Fatalf("planted divergence finding = %#v, want result divergence on output_hash", finding)
	}
}

func TestPostgresRuntimeLogCarriesComputeModuleReplayEvidenceForReplayConsumer(t *testing.T) {
	ctx := runtimeeffects.WithExecutionMode(testAuthorActivityContext(), runtimeeffects.ExecutionModeLive)
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	pg := newTestPostgresStore(t, db)
	runID := uuid.NewString()
	requireRunningRunForTest(t, ctx, pg, runID, time.Now().UTC())
	envelope := computeModuleReplayEvidenceTestEnvelope()
	detail := computemodule.NewReplayEvidenceDetail([]computemodule.ReplayEnvelope{envelope})
	detail["component"] = "compute_module"
	detail["action"] = computemodule.ReplayEvidenceAction
	detail["event_id"] = "evt-a"
	detail["node_id"] = "node-a"
	payload, err := json.Marshal(map[string]any{
		"log_level": "info",
		"message":   "Compute module replay evidence recorded",
		"details":   detail,
	})
	if err != nil {
		t.Fatalf("marshal runtime log payload: %v", err)
	}
	if err := commitDiagnosticRuntimeLogFixture(ctx, pg, eventtest.DiagnosticDirect(
		uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "", payload, 0,
		runID, "", events.EventEnvelope{Scope: events.EventScopeGlobal}, time.Now().UTC(),
	)); err != nil {
		t.Fatalf("seed postgres runtime log: %v", err)
	}
	loaded, err := pg.LoadComputeModuleReplayEvidenceForExecution(ctx, runID, "evt-a", "node-a")
	if err != nil {
		t.Fatalf("LoadComputeModuleReplayEvidenceForExecution postgres: %v", err)
	}
	if len(loaded) != 1 {
		t.Fatalf("loaded postgres replay evidence = %#v, want one envelope", loaded)
	}
	if loaded[0].Normalized() != envelope.Normalized() {
		t.Fatalf("loaded postgres envelope = %#v, want %#v", loaded[0].Normalized(), envelope.Normalized())
	}
}

func computeModuleReplayEvidenceTestEnvelope() computemodule.ReplayEnvelope {
	return computemodule.ReplayEnvelope{
		ModuleID:     "structured_renderer",
		RowID:        "render_bundle",
		Kind:         "wasm",
		ABI:          computemodule.ABI,
		Entry:        computemodule.DefaultEntry,
		Digest:       "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		InputHash:    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		Outcome:      computemodule.ReplayOutcomeSuccess,
		OutputHash:   "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		FuelConsumed: 42,
		Limits: computemodule.ReplayLimits{
			Fuel:        1_000,
			MemoryPages: 17,
			OutputBytes: 1024,
		},
		Engine: "wasmtime-go:v46.0.0",
		Arch:   "arm64",
	}
}

func TestSQLiteRuntimeLogSourceProjectionAndFilterParity(t *testing.T) {
	ctx := runtimeeffects.WithExecutionMode(testAuthorActivityContext(), runtimeeffects.ExecutionModeLive)
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)

	requireRunFixtureForTest(t, ctx, NewSQLiteRuntimeStoreForTest(store.backend.ConstructionHandle()), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: time.Now().UTC()})
	direct := eventtest.DiagnosticDirect(
		uuid.NewString(), events.EventTypePlatformRuntimeLog, "runtime", "",
		json.RawMessage(`{"log_level":"warn","message":"direct fallback source","details":{"component":"source-parity","action":"direct_runtime_source"}}`),
		0, runID, "", events.EventEnvelope{}, time.Now().UTC(),
	)
	if err := commitDiagnosticRuntimeLogFixture(ctx, store, direct); err != nil {
		t.Fatalf("seed direct sqlite runtime log fallback row: %v", err)
	}

	logger := runtimepkg.NewRuntimeLogger(store, executionposture.Live)
	if err := logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level:     "warn",
		Message:   "runtime-owned source",
		Component: "source-parity",
		Action:    "runtime_source",
	}); err != nil {
		t.Fatalf("RuntimeLogger.Log runtime source: %v", err)
	}
	if err := logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level:     "warn",
		Message:   "agent-owned source",
		Component: "source-parity",
		Action:    "agent_source",
		AgentID:   "agent-1",
	}); err != nil {
		t.Fatalf("RuntimeLogger.Log agent source: %v", err)
	}

	all, err := store.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{
		RunID:     runID,
		Component: "source-parity",
		Level:     "warn",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs all: %v", err)
	}
	if len(all.Logs) != 3 {
		t.Fatalf("all runtime logs = %#v, want three", all.Logs)
	}

	runtimeRows, err := store.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{
		RunID:     runID,
		Component: "source-parity",
		Level:     "warn",
		Source:    "runtime",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs runtime source: %v", err)
	}
	if len(runtimeRows.Logs) != 2 {
		t.Fatalf("runtime source logs = %#v, want direct fallback and runtime-owned rows", runtimeRows.Logs)
	}
	runtimeMessages := map[string]bool{}
	for _, log := range runtimeRows.Logs {
		if log.Source != "runtime" {
			t.Fatalf("runtime source row = %#v, want source runtime", log)
		}
		runtimeMessages[log.Message] = true
	}
	if !runtimeMessages["direct fallback source"] || !runtimeMessages["runtime-owned source"] {
		t.Fatalf("runtime source messages = %#v, want direct fallback and runtime-owned rows", runtimeMessages)
	}

	agentRows, err := store.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{
		RunID:     runID,
		Component: "source-parity",
		Level:     "warn",
		Source:    "agent-1",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs agent source: %v", err)
	}
	if len(agentRows.Logs) != 1 || agentRows.Logs[0].Source != "agent-1" || agentRows.Logs[0].Message != "agent-owned source" {
		t.Fatalf("agent source logs = %#v, want only agent-owned row", agentRows.Logs)
	}

	missingRows, err := store.ListOperatorRuntimeLogs(ctx, operatorread.OperatorRuntimeLogListOptions{
		RunID:     runID,
		Component: "source-parity",
		Level:     "warn",
		Source:    "missing-source",
		Limit:     10,
	})
	if err != nil {
		t.Fatalf("ListOperatorRuntimeLogs missing source: %v", err)
	}
	if len(missingRows.Logs) != 0 {
		t.Fatalf("missing source logs = %#v, want none", missingRows.Logs)
	}
}

func TestPostgresRuntimeLogPersistencePreservesRunSourceAndLineage(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	pg := newTestPostgresStore(t, db)
	artifact := storeTestSourceArtifact("runtime-log-source-lineage")
	ctx := testAuthorActivityContextForBundle(artifact.BundleHash())
	registerTestAuthorActivityCatalogForContext(t, pg, ctx)

	runID := uuid.NewString()
	subjectEventID := uuid.NewString()
	sourceFact := mustStoreTestSourceArtifactFact(artifact.BundleHash())
	seedStoreTestPersistedArtifact(t, db, artifact)
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	ctx = runtimecorrelation.WithSourceArtifactFact(ctx, sourceFact)
	if err := commitSemanticEventFixture(ctx, pg, eventtest.RunCreatingRootIngress(subjectEventID,

		events.EventType("validation/validation.package_ready"),
		"agent-1", "", json.RawMessage(`{"ready":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("seed postgres subject event: %v", err)
	}

	logger := runtimepkg.NewRuntimeLogger(pg, executionposture.Live)
	if err := logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level:     "warn",
		Message:   "postgres diagnostic persisted",
		Component: "eventbus",
		Action:    "lineage_lookup",
		EventID:   subjectEventID,
		EventType: "validation/validation.package_ready",
	}); err != nil {
		t.Fatalf("RuntimeLogger.Log postgres: %v", err)
	}

	var gotHash, sourceEventID string
	if err := db.QueryRowContext(ctx, `
		SELECT bundle_hash
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&gotHash); err != nil {
		t.Fatalf("load postgres run bundle source: %v", err)
	}
	if gotHash != sourceFact.BundleHash() {
		t.Fatalf("postgres run bundle source = hash:%q, want %#v", gotHash, sourceFact)
	}
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(source_event_id::text, '')
		FROM events
		WHERE event_name = 'platform.runtime_log'
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&sourceEventID); err != nil {
		t.Fatalf("load postgres runtime log source_event_id: %v", err)
	}
	if sourceEventID != subjectEventID {
		t.Fatalf("postgres source_event_id = %q, want %q", sourceEventID, subjectEventID)
	}
}

func TestPostgresRuntimeLogPersistenceUsesClosedNamedCommits(t *testing.T) {
	ctx := runtimeeffects.WithExecutionMode(testAuthorActivityContext(), runtimeeffects.ExecutionModeLive)
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	pg := newTestPostgresStore(t, db)
	runID := uuid.NewString()
	subjectEventID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	logger := runtimepkg.NewRuntimeLogger(pg, executionposture.Live)

	subject := eventtest.RunCreatingRootIngress(
		subjectEventID, events.EventType("validation/validation.package_ready"),
		"agent-1", "", json.RawMessage(`{"ready":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticEventFixtureWithAgents(ctx, pg, subject, nil); err != nil {
		t.Fatalf("commit subject event: %v", err)
	}
	if err := logger.Log(ctx, runtimepkg.RuntimeLogEntry{
		Level: "warn", Message: "closed diagnostic", Component: "eventbus",
		Action: "closed_named_commit", EventID: subjectEventID, EventType: "validation/validation.package_ready",
	}); err != nil {
		t.Fatalf("persist runtime log: %v", err)
	}

	var sourceEventID string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(source_event_id::text, '')
		FROM events
		WHERE event_name = 'platform.runtime_log'
	`).Scan(&sourceEventID); err != nil {
		t.Fatalf("load transactional runtime log: %v", err)
	}
	if sourceEventID != subjectEventID {
		t.Fatalf("transactional runtime log source_event_id = %q, want %q", sourceEventID, subjectEventID)
	}
}
