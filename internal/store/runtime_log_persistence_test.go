package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/computemodule"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	storerunlifecycle "github.com/division-sh/swarm/internal/store/runlifecycle"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSQLiteRuntimeLogPersistenceWritesLoggerRowsForObservability(t *testing.T) {
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	subjectEventID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	if err := store.AppendEvent(ctx, eventtest.PersistedProjection(subjectEventID,

		events.EventType("validation/validation.package_ready"),
		"agent-1", "", json.RawMessage(`{"ready":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("seed sqlite subject event: %v", err)
	}

	logger := runtimepkg.NewRuntimeLogger(store)
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

	logs, err := store.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
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
	gotParentEventID, _ := log.Details["parent_event_id"].(string)
	if got := strings.TrimSpace(gotParentEventID); got != subjectEventID {
		t.Fatalf("sqlite runtime log parent_event_id = %q, want %q", got, subjectEventID)
	}
	var sourceEventID string
	if err := store.DB.QueryRowContext(ctx, `
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
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES (?, 'running', ?)
	`, runID, time.Now().UTC()); err != nil {
		t.Fatalf("seed sqlite run: %v", err)
	}

	envelope := computeModuleReplayEvidenceTestEnvelope()
	detail := computemodule.NewReplayEvidenceDetail([]computemodule.ReplayEnvelope{envelope})
	detail["node_id"] = "node-a"
	logger := runtimepkg.NewRuntimeLogger(store)
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
	ctx := testAuthorActivityContext()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	pg := newTestPostgresStore(t, db)
	runID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', NOW())
	`, runID); err != nil {
		t.Fatalf("seed postgres run: %v", err)
	}
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
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (execution_mode,
			event_id, run_id, event_name, scope, payload, produced_by, produced_by_type, created_at
		)
		VALUES ('live', gen_random_uuid(), $1::uuid, 'platform.runtime_log', 'global', $2::jsonb, 'runtime', 'platform', NOW())
	`, runID, string(payload)); err != nil {
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
	ctx := testAuthorActivityContext()
	store := newBootstrappedSQLiteRuntimeStoreForTest(t)
	runID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)

	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES (?, 'running', ?)
	`, runID, time.Now().UTC()); err != nil {
		t.Fatalf("seed sqlite run: %v", err)
	}
	if _, err := store.DB.ExecContext(ctx, `
		INSERT INTO events (execution_mode, event_id, run_id, event_name, scope, payload, produced_by_type, created_at)
		VALUES ('live', ?, ?, 'platform.runtime_log', 'global', ?, 'platform', ?)
	`, uuid.NewString(), runID, json.RawMessage(`{"log_level":"warn","message":"direct fallback source","details":{"component":"source-parity","action":"direct_runtime_source"}}`), time.Now().UTC()); err != nil {
		t.Fatalf("seed direct sqlite runtime log fallback row: %v", err)
	}

	logger := runtimepkg.NewRuntimeLogger(store)
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

	all, err := store.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
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

	runtimeRows, err := store.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
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

	agentRows, err := store.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
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

	missingRows, err := store.ListOperatorRuntimeLogs(ctx, OperatorRuntimeLogListOptions{
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
	ctx := testAuthorActivityContextForBundle("bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111")
	registerTestAuthorActivityCatalogForContext(t, pg, ctx)

	runID := uuid.NewString()
	subjectEventID := uuid.NewString()
	sourceFact := runtimecorrelation.BundleSourceFact{
		BundleHash:        "bundle-v1:sha256:1111111111111111111111111111111111111111111111111111111111111111",
		BundleSource:      storerunlifecycle.BundleSourcePersisted,
		BundleFingerprint: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO bundles (bundle_hash, content_yaml, parsed_json)
		VALUES ($1, 'name: test', '{}'::jsonb)
	`, sourceFact.BundleHash); err != nil {
		t.Fatalf("seed bundle row: %v", err)
	}
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	ctx = runtimecorrelation.WithBundleSourceFact(ctx, sourceFact)
	if err := pg.AppendEvent(ctx, eventtest.PersistedProjection(subjectEventID,

		events.EventType("validation/validation.package_ready"),
		"agent-1", "", json.RawMessage(`{"ready":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
		t.Fatalf("seed postgres subject event: %v", err)
	}

	logger := runtimepkg.NewRuntimeLogger(pg)
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

	var gotHash, gotSource, gotFingerprint, sourceEventID string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(bundle_hash, ''), bundle_source, COALESCE(bundle_fingerprint, '')
		FROM runs
		WHERE run_id = $1::uuid
	`, runID).Scan(&gotHash, &gotSource, &gotFingerprint); err != nil {
		t.Fatalf("load postgres run bundle source: %v", err)
	}
	if gotHash != sourceFact.BundleHash || gotSource != sourceFact.BundleSource || gotFingerprint != sourceFact.BundleFingerprint {
		t.Fatalf("postgres run bundle source = hash:%q source:%q fingerprint:%q, want %#v", gotHash, gotSource, gotFingerprint, sourceFact)
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

func TestPostgresRuntimeLogPersistenceReusesAmbientEventTransaction(t *testing.T) {
	ctx := testAuthorActivityContext()
	_, db, cleanup := testutil.StartPostgres(t)
	defer cleanup()
	pg := newTestPostgresStore(t, db)
	runID := uuid.NewString()
	subjectEventID := uuid.NewString()
	ctx = runtimecorrelation.WithRunID(ctx, runID)
	logger := runtimepkg.NewRuntimeLogger(pg)

	if err := pg.RunEventTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		if err := pg.AppendEventTx(txctx, tx, eventtest.PersistedProjection(
			subjectEventID, events.EventType("validation/validation.package_ready"),
			"agent-1", "", json.RawMessage(`{"ready":true}`), 0, runID, "", events.EventEnvelope{}, time.Now().UTC())); err != nil {
			return err
		}
		return logger.Log(txctx, runtimepkg.RuntimeLogEntry{
			Level: "warn", Message: "transactional diagnostic", Component: "eventbus",
			Action: "ambient_transaction", EventID: subjectEventID, EventType: "validation/validation.package_ready",
		})
	}); err != nil {
		t.Fatalf("RunEventTransaction: %v", err)
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
