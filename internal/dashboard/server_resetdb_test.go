package dashboard

import (
	"bytes"
	"context"
	"fmt"
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
	runtimebus "empireai/internal/runtime/bus"
	runtimemanager "empireai/internal/runtime/manager"
	runtimepipeline "empireai/internal/runtime/pipeline"
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

	bus := rt.NewEventBus(runtimebus.InMemoryEventStore{})
	manager := runtimemanager.NewAgentManager(bus, func(cfg models.AgentConfig) (runtimemanager.Agent, error) {
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
			campaign_id TEXT NOT NULL,
			mode TEXT NOT NULL,
			geography TEXT NOT NULL,
			expected INT NOT NULL DEFAULT 0,
			complete INT NOT NULL DEFAULT 0,
			completed_by JSONB NOT NULL DEFAULT '{}'::jsonb,
			reports INT NOT NULL DEFAULT 0,
			discovered INT NOT NULL DEFAULT 0,
			skipped INT NOT NULL DEFAULT 0,
			pending_dedup INT NOT NULL DEFAULT 0,
			timeout_at TIMESTAMPTZ NOT NULL DEFAULT now() + interval '90 minutes',
			started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			completed_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		CREATE TABLE IF NOT EXISTS pending_dedup_candidates (
			dedup_event_id TEXT PRIMARY KEY,
			scan_id TEXT NOT NULL,
			campaign_id TEXT NOT NULL,
			mode TEXT NOT NULL,
			name TEXT NOT NULL,
			geography TEXT NOT NULL,
			discovery_mode TEXT NOT NULL,
			signal_strength DOUBLE PRECISION NOT NULL DEFAULT 0,
			payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			existing_id TEXT,
			status TEXT NOT NULL DEFAULT 'pending',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			resolved_at TIMESTAMPTZ
		);
		CREATE TABLE IF NOT EXISTS pipeline_processed_events (
			event_id UUID PRIMARY KEY,
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
	geoID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, region, created_at)
		VALUES ($1::uuid, 'argentina', 'AR', 'latam', now())
	`, geoID); err != nil {
		t.Fatalf("seed geography: %v", err)
	}
	campaignID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO scan_campaigns (id, geography_id, mode, priority, status, created_at)
		VALUES ($1::uuid, $2::uuid, 'saas_gap', 'normal', 'active', now())
	`, campaignID, geoID); err != nil {
		t.Fatalf("seed scan_campaign: %v", err)
	}
	scanID := uuid.NewString()
	existingVerticalID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, 'existing', 'existing-reset', 'argentina', 'operating', 'operating', now(), now())
	`, existingVerticalID); err != nil {
		t.Fatalf("seed existing vertical: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO scan_accumulators (
			scan_id, campaign_id, mode, geography, expected, complete,
			completed_by, reports, discovered, skipped, pending_dedup, timeout_at, started_at
		)
		VALUES ($1, $2, 'saas_gap', 'argentina', 1, 0, '{}'::jsonb, 0, 1, 0, 0, now() + interval '90 minutes', now())
	`, scanID, campaignID); err != nil {
		t.Fatalf("seed scan_accumulators: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO pending_dedup_candidates (
			dedup_event_id, scan_id, campaign_id, mode, name, geography, discovery_mode, signal_strength, payload, existing_id, status
		)
		VALUES ($1, $2, $3, 'saas_gap', 'candidate', 'argentina', 'saas_gap', 77, '{}'::jsonb, $4, 'pending')
	`, uuid.NewString(), scanID, campaignID, existingVerticalID); err != nil {
		t.Fatalf("seed pending_dedup_candidates: %v", err)
	}
	processedEventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, payload, created_at)
		VALUES ($1::uuid, 'scan.requested', 'pipeline-coordinator', '{}'::jsonb, now())
	`, processedEventID); err != nil {
		t.Fatalf("seed processed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO pipeline_processed_events (event_id)
		VALUES ($1::uuid)
	`, processedEventID); err != nil {
		t.Fatalf("seed pipeline_processed_events: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO validation_pipelines (vertical_id, status)
		VALUES ($1::uuid, 'active')
	`, existingVerticalID); err != nil {
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

	bus := rt.NewEventBus(runtimebus.InMemoryEventStore{})
	pc := runtimepipeline.NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)

	manager := runtimemanager.NewAgentManager(bus, func(cfg models.AgentConfig) (runtimemanager.Agent, error) {
		return &stubAgent{id: cfg.ID, subs: []events.EventType{"*"}}, nil
	}, pg)
	manager.Run(context.Background())
	t.Cleanup(func() { _ = manager.Shutdown() })

	srv := NewServer(db, cfg, pg, pg, manager)
	h := srv.Handler()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS scan_accumulators (
			scan_id TEXT PRIMARY KEY,
			campaign_id TEXT NOT NULL DEFAULT '',
			mode TEXT NOT NULL DEFAULT '',
			geography TEXT NOT NULL DEFAULT '',
			expected INT NOT NULL DEFAULT 0,
			complete INT NOT NULL DEFAULT 0,
			completed_by JSONB NOT NULL DEFAULT '{}'::jsonb,
			reports INT NOT NULL DEFAULT 0,
			discovered INT NOT NULL DEFAULT 0,
			skipped INT NOT NULL DEFAULT 0,
			pending_dedup INT NOT NULL DEFAULT 0,
			timeout_at TIMESTAMPTZ NOT NULL DEFAULT now() + interval '90 minutes',
			started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			completed_at TIMESTAMPTZ,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		CREATE TABLE IF NOT EXISTS pending_dedup_candidates (
			dedup_event_id TEXT PRIMARY KEY,
			scan_id TEXT NOT NULL,
			campaign_id TEXT NOT NULL DEFAULT '',
			mode TEXT NOT NULL DEFAULT '',
			name TEXT NOT NULL DEFAULT '',
			geography TEXT NOT NULL DEFAULT '',
			discovery_mode TEXT NOT NULL DEFAULT '',
			signal_strength DOUBLE PRECISION NOT NULL DEFAULT 0,
			payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			existing_id TEXT,
			status TEXT NOT NULL DEFAULT 'pending',
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			resolved_at TIMESTAMPTZ
		);

		CREATE TABLE IF NOT EXISTS validation_pipelines (
			vertical_id UUID PRIMARY KEY,
			status TEXT NOT NULL DEFAULT 'active',
			g1_research BOOLEAN NOT NULL DEFAULT false,
			g2_spec BOOLEAN NOT NULL DEFAULT false,
			g3_cto BOOLEAN NOT NULL DEFAULT false,
			g4_brand BOOLEAN NOT NULL DEFAULT false,
			research_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			spec_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			cto_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			brand_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			scoring_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
			revision_count INT NOT NULL DEFAULT 0,
			inner_revision_count INT NOT NULL DEFAULT 0,
			spec_version INT NOT NULL DEFAULT 0,
			packaging_requested BOOLEAN NOT NULL DEFAULT false,
			packaging_requested_at TIMESTAMPTZ,
			packaging_retries INT NOT NULL DEFAULT 0,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);

		CREATE TABLE IF NOT EXISTS pipeline_processed_events (
			event_id UUID PRIMARY KEY,
			processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`); err != nil {
		t.Fatalf("ensure pipeline state tables: %v", err)
	}

	// Seed pipeline coordinator in-memory state by running a scan assignment through
	// the interceptor path. This also persists a scan accumulator row.
	geoID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO geographies (id, name, country, region, created_at)
		VALUES ($1::uuid, 'argentina', 'AR', 'latam', now())
	`, geoID); err != nil {
		t.Fatalf("seed geography: %v", err)
	}
	campaignID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO scan_campaigns (id, geography_id, mode, priority, status, created_at)
		VALUES ($1::uuid, $2::uuid, 'local_services', 'normal', 'active', now())
	`, campaignID, geoID); err != nil {
		t.Fatalf("seed scan_campaign: %v", err)
	}
	scanID := uuid.NewString()
	if err := bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("scan.requested"),
		SourceAgent: "test",
		Payload:     []byte(fmt.Sprintf(`{"scan_id":"%s","campaign_id":"%s","mode":"local_services","geography":"argentina"}`, scanID, campaignID)),
		CreatedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("seed scan.requested: %v", err)
	}
	var before int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM scan_accumulators WHERE scan_id = $1`, scanID).Scan(&before); err != nil {
		t.Fatalf("count scan_accumulators before reset: %v", err)
	}
	if before == 0 {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO scan_accumulators (
				scan_id, campaign_id, mode, geography, expected, complete,
				completed_by, reports, discovered, skipped, pending_dedup, timeout_at, started_at
			)
			VALUES ($1, $2, 'local_services', 'argentina', 1, 0, '{}'::jsonb, 0, 0, 0, 0, now() + interval '90 minutes', now())
			ON CONFLICT (scan_id) DO NOTHING
		`, scanID, campaignID); err != nil {
			t.Fatalf("seed fallback scan_accumulator: %v", err)
		}
		before = 1
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
