package dashboard

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"empireai/internal/config"
	"empireai/internal/events"
	"empireai/internal/models"
	rt "empireai/internal/runtime"
	"empireai/internal/store"
	"empireai/internal/testutil"
	"github.com/google/uuid"
)

func TestDashboardServer_ControlRuntime_ResetDB_SeedsOrgAndTemplate(t *testing.T) {
	root := repoRoot(t)
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}

	t.Setenv("EMPIREAI_API_KEY", "test-key")
	t.Setenv("EMPIREAI_GLOBAL_AGENTS_DIR", filepath.Join(root, "configs", "agents"))
	t.Setenv("EMPIREAI_TEMPLATE_AGENTS_DIR", filepath.Join(root, "configs", "agents", "templates"))
	t.Setenv("EMPIREAI_TEMPLATE_ROUTES_YAML", filepath.Join(root, "configs", "agents", "templates", "routes.yaml"))
	t.Setenv("EMPIREAI_INITIAL_TEMPLATE_VERSION", "2.0.1")

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               5 * time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:              "true",
				Timeout:              2 * time.Second,
				OutputFormat:         "json",
				Retries:              1,
				NoSessionPersistence: false,
			},
		},
		Runtime: config.RuntimeConfig{
			MaxConcurrentAgents: 5,
			EventPollInterval:   100 * time.Millisecond,
		},
	}

	bus := rt.NewEventBus(rt.InMemoryEventStore{})
	manager := rt.NewAgentManager(bus, func(cfg models.AgentConfig) (rt.Agent, error) {
		return &stubAgent{id: cfg.ID, subs: []events.EventType{"*"}}, nil
	}, pg)
	manager.Run(context.Background())

	srv := NewServer(db, cfg, pg, pg, manager)
	h := srv.Handler()

	body := []byte(`{"action":"reset_db","confirm":"RESET","template_version":"2.0.1"}`)
	req := httptest.NewRequest(http.MethodPost, "/dashboard/api/control/runtime", bytes.NewReader(body))
	req.Header.Set("X-Empire-Key", "test-key")
	req.Header.Set("content-type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusOK && w.Code != http.StatusMultiStatus {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "reset_db") {
		t.Fatalf("expected reset_db response, got %s", w.Body.String())
	}

	// Sanity: org_templates should exist and at least one agent should have been seeded.
	var templateCount int
	_ = db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM org_templates`).Scan(&templateCount)
	if templateCount == 0 {
		t.Fatal("expected initial template to be ensured")
	}
	var agentCount int
	_ = db.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM agents`).Scan(&agentCount)
	if agentCount == 0 {
		t.Fatal("expected global agents to be seeded")
	}

	// Create a vertical to ensure reset_db didn't leave the system unusable.
	verticalID := uuid.NewString()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid,'After','after','us','operating','operating', now(), now())
	`, verticalID); err != nil {
		t.Fatalf("insert vertical after reset: %v", err)
	}
}

func TestDashboardServer_ResetState_TruncatesAllPublicRuntimeTables(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()

	// Some runtime tables arrive in later migrations in production. Create minimal
	// versions if they are absent so reset behavior is validated regardless of the
	// bootstrap migration used by tests.
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS scan_accumulators (
			scan_id TEXT PRIMARY KEY,
			campaign_id TEXT,
			mode TEXT,
			geography TEXT,
			expected INT,
			completed_by JSONB NOT NULL DEFAULT '{}'::jsonb,
			reports INT,
			discovered INT,
			skipped INT
		);
		CREATE TABLE IF NOT EXISTS pending_dedup_candidates (
			dedup_event_id TEXT PRIMARY KEY,
			scan_id TEXT,
			campaign_id TEXT,
			mode TEXT,
			geography TEXT,
			name TEXT,
			signal_strength DOUBLE PRECISION,
			payload JSONB NOT NULL DEFAULT '{}'::jsonb
		);
		CREATE TABLE IF NOT EXISTS pipeline_processed_events (
			event_id TEXT PRIMARY KEY,
			processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE TABLE IF NOT EXISTS validation_pipelines (
			vertical_id UUID PRIMARY KEY,
			status TEXT NOT NULL DEFAULT 'active'
		);
	`); err != nil {
		t.Fatalf("ensure optional runtime tables: %v", err)
	}

	// Seed rows in tables that were previously missed by the hardcoded reset list.
	if _, err := db.ExecContext(ctx, `
		INSERT INTO scan_accumulators (scan_id, campaign_id, mode, geography, expected, completed_by, reports, discovered, skipped)
		VALUES ('scan-zombie', 'camp-zombie', 'saas_gap', 'argentina', 1, '{}'::jsonb, 1, 1, 0)
	`); err != nil {
		t.Fatalf("seed scan_accumulators: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO pending_dedup_candidates (dedup_event_id, scan_id, campaign_id, mode, geography, name, signal_strength, payload)
		VALUES ('dedup-zombie', 'scan-zombie', 'camp-zombie', 'saas_gap', 'argentina', 'zombie', 77, '{}'::jsonb)
	`); err != nil {
		t.Fatalf("seed pending_dedup_candidates: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO pipeline_processed_events (event_id)
		VALUES ('evt-zombie')
	`); err != nil {
		t.Fatalf("seed pipeline_processed_events: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO validation_pipelines (vertical_id, status)
		VALUES ($1::uuid, 'active')
	`, uuid.NewString()); err != nil {
		t.Fatalf("seed validation_pipelines: %v", err)
	}

	// Probe table catches future "new table not in reset list" regressions.
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS reset_probe (
			id SERIAL PRIMARY KEY,
			note TEXT NOT NULL
		)
	`); err != nil {
		t.Fatalf("create reset_probe: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO reset_probe (note) VALUES ('stale')`); err != nil {
		t.Fatalf("seed reset_probe: %v", err)
	}

	srv := NewServer(db, nil, nil, nil, nil)
	if err := srv.resetState(ctx); err != nil {
		t.Fatalf("resetState: %v", err)
	}

	assertEmpty := func(table string) {
		t.Helper()
		var n int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if n != 0 {
			t.Fatalf("expected %s to be empty after reset, got %d rows", table, n)
		}
	}

	assertEmpty("scan_accumulators")
	assertEmpty("pending_dedup_candidates")
	assertEmpty("pipeline_processed_events")
	assertEmpty("validation_pipelines")
	assertEmpty("reset_probe")
}

func TestDashboardServer_ControlRuntime_ResetDB_ClearsPipelineInMemoryState(t *testing.T) {
	root := repoRoot(t)
	_, db, _ := testutil.StartPostgres(t)
	pg := &store.PostgresStore{DB: db}
	ctx := context.Background()

	t.Setenv("EMPIREAI_API_KEY", "test-key")
	t.Setenv("EMPIREAI_GLOBAL_AGENTS_DIR", filepath.Join(root, "configs", "agents"))
	t.Setenv("EMPIREAI_TEMPLATE_AGENTS_DIR", filepath.Join(root, "configs", "agents", "templates"))
	t.Setenv("EMPIREAI_TEMPLATE_ROUTES_YAML", filepath.Join(root, "configs", "agents", "templates", "routes.yaml"))
	t.Setenv("EMPIREAI_INITIAL_TEMPLATE_VERSION", "2.0.1")

	cfg := &config.Config{
		LLM: config.LLMConfig{
			RuntimeMode: "cli_test",
			Session: config.LLMSessionConfig{
				LockTTL:               5 * time.Second,
				RotateAfterTurns:      40,
				RotateOnParseFailures: 3,
			},
			ClaudeCLI: config.ClaudeCLIConfig{
				Command:      "true",
				Timeout:      2 * time.Second,
				OutputFormat: "json",
				Retries:      1,
			},
		},
	}

	bus := rt.NewEventBus(rt.InMemoryEventStore{})
	pc := rt.NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)

	manager := rt.NewAgentManager(bus, func(cfg models.AgentConfig) (rt.Agent, error) {
		return &stubAgent{id: cfg.ID, subs: []events.EventType{"*"}}, nil
	}, pg)
	manager.Run(context.Background())
	t.Cleanup(func() { _ = manager.Shutdown() })

	srv := NewServer(db, cfg, pg, pg, manager)
	h := srv.Handler()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS scan_accumulators (
			scan_id TEXT PRIMARY KEY
		);
		ALTER TABLE scan_accumulators
			ADD COLUMN IF NOT EXISTS campaign_id TEXT,
			ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT '',
			ADD COLUMN IF NOT EXISTS geography TEXT NOT NULL DEFAULT '',
			ADD COLUMN IF NOT EXISTS expected INT NOT NULL DEFAULT 0,
			ADD COLUMN IF NOT EXISTS completed_by JSONB NOT NULL DEFAULT '{}'::jsonb,
			ADD COLUMN IF NOT EXISTS reports INT NOT NULL DEFAULT 0,
			ADD COLUMN IF NOT EXISTS discovered INT NOT NULL DEFAULT 0,
			ADD COLUMN IF NOT EXISTS skipped INT NOT NULL DEFAULT 0,
			ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

		CREATE TABLE IF NOT EXISTS pending_dedup_candidates (
			dedup_event_id TEXT PRIMARY KEY
		);
		ALTER TABLE pending_dedup_candidates
			ADD COLUMN IF NOT EXISTS scan_id TEXT NOT NULL DEFAULT '',
			ADD COLUMN IF NOT EXISTS campaign_id TEXT,
			ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT '',
			ADD COLUMN IF NOT EXISTS geography TEXT NOT NULL DEFAULT '',
			ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT '',
			ADD COLUMN IF NOT EXISTS signal_strength DOUBLE PRECISION NOT NULL DEFAULT 0,
			ADD COLUMN IF NOT EXISTS payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();

		CREATE TABLE IF NOT EXISTS validation_pipelines (
			vertical_id UUID PRIMARY KEY
		);
		ALTER TABLE validation_pipelines
			ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active',
			ADD COLUMN IF NOT EXISTS g1_research BOOLEAN NOT NULL DEFAULT false,
			ADD COLUMN IF NOT EXISTS g2_spec BOOLEAN NOT NULL DEFAULT false,
			ADD COLUMN IF NOT EXISTS g3_cto BOOLEAN NOT NULL DEFAULT false,
			ADD COLUMN IF NOT EXISTS g4_brand BOOLEAN NOT NULL DEFAULT false,
			ADD COLUMN IF NOT EXISTS research_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			ADD COLUMN IF NOT EXISTS spec_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			ADD COLUMN IF NOT EXISTS cto_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			ADD COLUMN IF NOT EXISTS brand_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			ADD COLUMN IF NOT EXISTS scoring_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			ADD COLUMN IF NOT EXISTS revision_count INT NOT NULL DEFAULT 0,
			ADD COLUMN IF NOT EXISTS inner_revision_count INT NOT NULL DEFAULT 0,
			ADD COLUMN IF NOT EXISTS spec_version INT NOT NULL DEFAULT 0,
			ADD COLUMN IF NOT EXISTS packaging_requested BOOLEAN NOT NULL DEFAULT false,
			ADD COLUMN IF NOT EXISTS packaging_requested_at TIMESTAMPTZ,
			ADD COLUMN IF NOT EXISTS packaging_retries INT NOT NULL DEFAULT 0,
			ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

		CREATE TABLE IF NOT EXISTS pipeline_processed_events (
			event_id TEXT PRIMARY KEY,
			processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`); err != nil {
		t.Fatalf("ensure pipeline state tables: %v", err)
	}

	// Seed pipeline coordinator in-memory state by running a scan assignment through
	// the interceptor path. This also persists a scan accumulator row.
	scanID := "scan-zombie"
	if err := bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("scan.requested"),
		SourceAgent: "test",
		Payload:     []byte(`{"scan_id":"scan-zombie","mode":"local_services","geography":"argentina"}`),
		CreatedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("seed scan.requested: %v", err)
	}
	var before int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM scan_accumulators WHERE scan_id = $1`, scanID).Scan(&before); err != nil {
		t.Fatalf("count scan_accumulators before reset: %v", err)
	}
	if before == 0 {
		t.Fatalf("expected seeded scan_accumulators row for %s before reset", scanID)
	}

	body := []byte(`{"action":"reset_db","confirm":"RESET","seed_org":false,"template_version":"2.0.1"}`)
	req := httptest.NewRequest(http.MethodPost, "/dashboard/api/control/runtime", bytes.NewReader(body))
	req.Header.Set("X-Empire-Key", "test-key")
	req.Header.Set("content-type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK && w.Code != http.StatusMultiStatus {
		t.Fatalf("reset_db status=%d body=%s", w.Code, w.Body.String())
	}

	var afterReset int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM scan_accumulators WHERE scan_id = $1`, scanID).Scan(&afterReset); err != nil {
		t.Fatalf("count after reset: %v", err)
	}
	if afterReset != 0 {
		t.Fatalf("expected no scan_accumulators rows after reset, got %d", afterReset)
	}

	// Any later publish should not resurrect stale coordinator in-memory state.
	if err := bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("runtime.health_probe"),
		SourceAgent: "test",
		Payload:     []byte(`{}`),
		CreatedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("publish probe event: %v", err)
	}
	var afterProbe int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM scan_accumulators WHERE scan_id = $1`, scanID).Scan(&afterProbe); err != nil {
		t.Fatalf("count after probe: %v", err)
	}
	if afterProbe != 0 {
		t.Fatalf("stale scan state was repersisted after reset; got %d rows", afterProbe)
	}
}
