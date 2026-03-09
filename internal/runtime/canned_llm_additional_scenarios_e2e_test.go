package runtime

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimeagents "empireai/internal/runtime/agents"
	runtimemanager "empireai/internal/runtime/manager"
	runtimepipeline "empireai/internal/runtime/pipeline"
	empirepipeline "empireai/internal/runtime/pipeline/empire"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/testutil"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type cannedE2EScenario struct {
	ID       string                   `yaml:"id"`
	Agents   []cannedE2EScenarioAgent `yaml:"agents"`
	Expected cannedE2EExpected        `yaml:"expected"`
}

type cannedE2EScenarioAgent struct {
	ID            string                  `yaml:"id"`
	Role          string                  `yaml:"role"`
	Mode          string                  `yaml:"mode"`
	Subscriptions []string                `yaml:"subscriptions"`
	Responses     []cannedFixtureResponse `yaml:"responses"`
}

type cannedE2EExpected struct {
	EventCounts  map[string]int `yaml:"event_counts"`
	AbsentEvents []string       `yaml:"absent_events"`
}

type captureScheduleStore struct {
	mu        sync.Mutex
	schedules []runtimepipeline.Schedule
}

func (s *captureScheduleStore) UpsertSchedule(_ context.Context, sc runtimepipeline.Schedule) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schedules = append(s.schedules, sc)
	return nil
}

func (s *captureScheduleStore) CancelSchedule(_ context.Context, agentID, eventType string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]runtimepipeline.Schedule, 0, len(s.schedules))
	for _, sc := range s.schedules {
		if strings.TrimSpace(sc.AgentID) == strings.TrimSpace(agentID) &&
			strings.TrimSpace(sc.EventType) == strings.TrimSpace(eventType) {
			continue
		}
		out = append(out, sc)
	}
	s.schedules = out
	return nil
}

func (s *captureScheduleStore) LoadActiveSchedules(context.Context) ([]runtimepipeline.Schedule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]runtimepipeline.Schedule, len(s.schedules))
	copy(out, s.schedules)
	return out, nil
}

func (s *captureScheduleStore) MarkScheduleFired(context.Context, runtimepipeline.Schedule) error {
	return nil
}

func (s *captureScheduleStore) Snapshot() []runtimepipeline.Schedule {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]runtimepipeline.Schedule, len(s.schedules))
	copy(out, s.schedules)
	return out
}

type e2eScenarioRig struct {
	ctx           context.Context
	cancel        context.CancelFunc
	db            *sql.DB
	bus           *EventBus
	eventStore    *postgresEventStore
	mailboxStore  *sqlMailboxStore
	pc            *runtimepipeline.FactoryPipelineCoordinator
	am            *runtimemanager.AgentManager
	scanMgr       *runtimepipeline.ScanCampaignManager
	scoringNode   runtimepipeline.BackgroundNode
	scheduler     *runtimepipeline.Scheduler
	scheduleStore *captureScheduleStore
	canned        *yamlCannedRuntime
}

func loadCannedScenario(t *testing.T, name string) cannedE2EScenario {
	t.Helper()
	path := projectPathFromThisFile("contracts", "test-vectors", "e2e", name+".yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read scenario %s: %v", name, err)
	}
	var sc cannedE2EScenario
	if err := yaml.Unmarshal(raw, &sc); err != nil {
		t.Fatalf("parse scenario %s: %v", name, err)
	}
	if strings.TrimSpace(sc.ID) == "" {
		t.Fatalf("scenario %s missing id", name)
	}
	return sc
}

func startE2EScenarioRig(t *testing.T, sc cannedE2EScenario, withScheduler bool) *e2eScenarioRig {
	t.Helper()
	_, db, _ := testutil.StartPostgres(t)
	eventStore := &postgresEventStore{db: db}
	bus := NewEventBus(eventStore)
	bus.SetRuntimeLogger(NewRuntimeLogger(db))
	pc := runtimepipeline.NewFactoryPipelineCoordinatorWithOptions(bus, db, runtimepipeline.FactoryPipelineCoordinatorOptions{Module: empirepipeline.NewModule()})
	bus.SetInterceptors(pc)

	fixtures := make(map[string]cannedRoleFixture, len(sc.Agents))
	for _, agent := range sc.Agents {
		id := strings.TrimSpace(agent.ID)
		if id == "" {
			t.Fatalf("scenario %s has agent with empty id", sc.ID)
		}
		fixtures[id] = cannedRoleFixture{
			Role:      id,
			Responses: append([]cannedFixtureResponse(nil), agent.Responses...),
		}
	}
	canned := newYAMLCannedRuntime(fixtures)

	var scheduler *runtimepipeline.Scheduler
	scheduleStore := &captureScheduleStore{}
	if withScheduler {
		scheduler = runtimepipeline.NewScheduler(func(sc runtimepipeline.Schedule) {
			payload := parsePayloadMap(sc.Payload)
			_ = bus.Publish(context.Background(), events.Event{
				ID:          uuid.NewString(),
				Type:        events.EventType(strings.TrimSpace(sc.EventType)),
				SourceAgent: strings.TrimSpace(sc.AgentID),
				VerticalID:  strings.TrimSpace(sc.VerticalID),
				TaskID:      strings.TrimSpace(sc.TaskID),
				Payload:     mustJSON(payload),
				CreatedAt:   time.Now().UTC(),
			})
		})
	}

	mailboxStore := &sqlMailboxStore{db: db}
	exec := runtimetools.NewExecutor(bus, scheduler, nil, scheduleStore)
	exec.SetMailboxStore(mailboxStore)
	baseFactory := runtimeagents.NewLLMAgentFactory(canned, exec, exec.ToolDefinitions())
	factory := func(cfg models.AgentConfig) (runtimemanager.Agent, error) {
		if strings.TrimSpace(extractSystemPromptForTest(cfg)) == "" {
			cfg.Config = runtimemanager.WithSystemPrompt(cfg.Config, "Canned scenario prompt for "+strings.TrimSpace(cfg.Role))
		}
		return baseFactory(cfg)
	}
	am := runtimemanager.NewAgentManager(bus, factory)
	exec.SetManager(am)

	for _, agent := range sc.Agents {
		if err := am.SpawnAgent(models.AgentConfig{
			ID:            strings.TrimSpace(agent.ID),
			Type:          "worker",
			Role:          strings.TrimSpace(agent.Role),
			Mode:          strings.TrimSpace(agent.Mode),
			Subscriptions: append([]string(nil), agent.Subscriptions...),
		}); err != nil {
			t.Fatalf("spawn scenario agent %s (%s): %v", agent.ID, agent.Role, err)
		}
	}

	scanMgr := runtimepipeline.NewScanCampaignManager(bus, &e2eCampaignStore{db: db}, newScanCampaignHooksForTest(), db)
	scoringNode := runtimepipeline.NewScoringNode(bus, pc, nil)
	if scoringNode == nil {
		t.Fatal("expected scoring node")
	}

	ctx, cancel := context.WithCancel(context.Background())
	am.Run(ctx)
	go scanMgr.Run(ctx)
	go scoringNode.Run(ctx)

	return &e2eScenarioRig{
		ctx:           ctx,
		cancel:        cancel,
		db:            db,
		bus:           bus,
		eventStore:    eventStore,
		mailboxStore:  mailboxStore,
		pc:            pc,
		am:            am,
		scanMgr:       scanMgr,
		scoringNode:   scoringNode,
		scheduler:     scheduler,
		scheduleStore: scheduleStore,
		canned:        canned,
	}
}

func (r *e2eScenarioRig) Close() {
	if r == nil {
		return
	}
	if r.cancel != nil {
		r.cancel()
	}
	if r.scheduler != nil {
		r.scheduler.Stop()
	}
	if r.am != nil {
		_ = r.am.Shutdown()
	}
}

func publishScenarioDirective(t *testing.T, r *e2eScenarioRig, text string) string {
	t.Helper()
	corpusPath := filepath.Join(t.TempDir(), "scenario-corpus.jsonl")
	if err := os.WriteFile(corpusPath, []byte("{\"signal\":\"scenario\"}\n"), 0o600); err != nil {
		t.Fatalf("write scenario corpus file: %v", err)
	}
	if err := r.bus.Publish(r.ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload: mustJSON(map[string]any{
			"directive_text": strings.TrimSpace(text),
			"corpus_path":    corpusPath,
		}),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish scenario directive: %v", err)
	}
	return corpusPath
}

func assertScenarioExpectedCounts(t *testing.T, db *sql.DB, expected cannedE2EExpected) {
	t.Helper()
	counts := dbEventTypeCounts(t, db)
	for eventType, want := range expected.EventCounts {
		if got := counts[strings.TrimSpace(eventType)]; got != want {
			t.Fatalf("event count mismatch type=%s got=%d want=%d counts=%v", eventType, got, want, counts)
		}
	}
	for _, eventType := range expected.AbsentEvents {
		if got := counts[strings.TrimSpace(eventType)]; got != 0 {
			t.Fatalf("expected event type %s to be absent, got=%d counts=%v", eventType, got, counts)
		}
	}
}

func waitForPendingDedupCount(t *testing.T, db *sql.DB, ctx context.Context, minCount int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var pending int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_dedup_candidates WHERE status='pending'`).Scan(&pending); err != nil {
			t.Fatalf("count pending_dedup_candidates: %v", err)
		}
		if pending >= minCount {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected pending_dedup_candidates >= %d, got %d", minCount, pending)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitForPendingDedupDrain(t *testing.T, db *sql.DB, ctx context.Context, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var pending int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM pending_dedup_candidates WHERE status='pending'`).Scan(&pending); err != nil {
			t.Fatalf("recount pending_dedup_candidates: %v", err)
		}
		if pending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected pending dedup queue to drain, got %d rows", pending)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func latestVerticalIDByEvent(t *testing.T, db *sql.DB, eventType string) string {
	t.Helper()
	var verticalID string
	if err := db.QueryRowContext(context.Background(), `
		SELECT COALESCE(vertical_id::text, '')
		FROM events
		WHERE type = $1
		ORDER BY created_at DESC, id DESC
		LIMIT 1
	`, strings.TrimSpace(eventType)).Scan(&verticalID); err != nil {
		t.Fatalf("load latest vertical id by event %s: %v", eventType, err)
	}
	return strings.TrimSpace(verticalID)
}

func TestCannedLLME2E_Scenario2_PrefilterRejectsAll(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping canned runtime e2e scenarios in -short mode")
	}
	sc := loadCannedScenario(t, "scenario2-prefilter-reject-all")
	rig := startE2EScenarioRig(t, sc, false)
	defer rig.Close()

	publishScenarioDirective(t, rig, "Corpus in Argentina")
	if err := waitForDBEventTypeCount(rig.eventStore, "campaign.completed", 1, 12*time.Second); err != nil {
		t.Fatalf("wait campaign.completed: %v", err)
	}

	scanPayload := latestEventPayload(t, rig.db, "scan.completed")
	if got := asInt(scanPayload["verticals_discovered"]); got != 0 {
		t.Fatalf("expected scan.completed verticals_discovered=0, got=%d payload=%v", got, scanPayload)
	}
	campaignPayload := latestEventPayload(t, rig.db, "campaign.completed")
	if got := asInt(campaignPayload["discoveries_count"]); got != 0 {
		t.Fatalf("expected campaign.completed discoveries_count=0, got=%d payload=%v", got, campaignPayload)
	}
	assertScenarioExpectedCounts(t, rig.db, sc.Expected)
}

func TestCannedLLME2E_Scenario3_MarginalPath(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping canned runtime e2e scenarios in -short mode")
	}
	sc := loadCannedScenario(t, "scenario3-marginal-path")
	rig := startE2EScenarioRig(t, sc, true)
	defer rig.Close()

	publishScenarioDirective(t, rig, "Corpus in Argentina")
	if err := waitForDBEventTypeCount(rig.eventStore, "campaign.completed", 1, 12*time.Second); err != nil {
		t.Fatalf("wait campaign.completed: %v", err)
	}
	if err := waitForDBEventTypeCount(rig.eventStore, "vertical.marginal", 1, 12*time.Second); err != nil {
		t.Fatalf("wait vertical.marginal: %v", err)
	}
	if err := waitForDBEventTypeCount(rig.eventStore, "vertical.rejected", 1, 12*time.Second); err != nil {
		t.Fatalf("wait vertical.rejected: %v", err)
	}

	marginalID := latestVerticalIDByEvent(t, rig.db, "vertical.marginal")
	if strings.TrimSpace(marginalID) == "" {
		t.Fatal("missing marginal vertical id")
	}
	var stage string
	var parkedAt sql.NullTime
	if err := rig.db.QueryRowContext(rig.ctx, `
		SELECT COALESCE(stage,''), parked_at
		FROM verticals
		WHERE id = $1::uuid
	`, marginalID).Scan(&stage, &parkedAt); err != nil {
		t.Fatalf("load marginal stage: %v", err)
	}
	if strings.TrimSpace(stage) != "marginal_review" {
		t.Fatalf("expected marginal_review stage before timer, got %q", stage)
	}
	if !parkedAt.Valid {
		t.Fatal("expected parked_at to be set for marginal vertical")
	}

	if err := rig.bus.Publish(rig.ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("timer.marginal_review"),
		SourceAgent: "runtime",
		VerticalID:  marginalID,
		Payload: mustJSON(map[string]any{
			"vertical_id":               marginalID,
			"parked_marginals_summary":  "scenario3 timer trigger",
			"scheduled_for_review_date": "2099-01-01",
		}),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish timer.marginal_review: %v", err)
	}
	if err := waitForDBEventTypeCount(rig.eventStore, "vertical.resumed", 1, cannedE2EWaitTimeout); err != nil {
		t.Fatalf("wait vertical.resumed: %v", err)
	}

	if err := rig.db.QueryRowContext(rig.ctx, `SELECT COALESCE(stage,'') FROM verticals WHERE id=$1::uuid`, marginalID).Scan(&stage); err != nil {
		t.Fatalf("reload marginal stage: %v", err)
	}
	if strings.TrimSpace(stage) != "researching" {
		t.Fatalf("expected researching stage after timer promote, got %q", stage)
	}

	foundTimer := false
	for _, sc := range rig.scheduleStore.Snapshot() {
		if strings.TrimSpace(sc.EventType) == "timer.marginal_review" && strings.TrimSpace(sc.VerticalID) == marginalID {
			foundTimer = true
			break
		}
	}
	if !foundTimer {
		t.Fatal("expected timer.marginal_review schedule to be created")
	}
	assertScenarioExpectedCounts(t, rig.db, sc.Expected)
}

func TestCannedLLME2E_Scenario4_DerivationLoop(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping canned runtime e2e scenarios in -short mode")
	}
	sc := loadCannedScenario(t, "scenario4-derivation-loop")
	rig := startE2EScenarioRig(t, sc, false)
	defer rig.Close()

	publishScenarioDirective(t, rig, "Corpus in Argentina")
	if err := waitForDBEventTypeCount(rig.eventStore, "vertical.derived", 1, 12*time.Second); err != nil {
		t.Fatalf("wait vertical.derived: %v", err)
	}
	if err := waitForDBEventTypeCount(rig.eventStore, "vertical.shortlisted", 1, 12*time.Second); err != nil {
		t.Fatalf("wait vertical.shortlisted: %v", err)
	}
	assertScenarioExpectedCounts(t, rig.db, sc.Expected)

	var derivedID, parentID, generator string
	var depth int
	if err := rig.db.QueryRowContext(rig.ctx, `
		SELECT id::text, COALESCE(parent_id::text,''), COALESCE(generation_depth,0), COALESCE(generator_agent_id,'')
		FROM verticals
		WHERE parent_id IS NOT NULL
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&derivedID, &parentID, &depth, &generator); err != nil {
		t.Fatalf("load derived vertical row: %v", err)
	}
	if strings.TrimSpace(parentID) == "" || strings.TrimSpace(derivedID) == "" {
		t.Fatalf("expected derived vertical with parent_id, got derived=%q parent=%q", derivedID, parentID)
	}
	if depth != 1 {
		t.Fatalf("expected generation_depth=1, got %d", depth)
	}
	if strings.TrimSpace(generator) != "analysis-agent" {
		t.Fatalf("expected generator_agent_id=analysis-agent, got %q", generator)
	}

	payload := latestEventForVertical(t, rig.db, "scoring.requested", derivedID)
	if got := strings.TrimSpace(asString(payload["excluded_analysis_agent_id"])); got != "analysis-agent" {
		t.Fatalf("expected excluded_analysis_agent_id=analysis-agent, got %q payload=%v", got, payload)
	}
	if got := strings.TrimSpace(asString(payload["assigned_analysis_agent_id"])); got != "analysis-agent-alt" {
		t.Fatalf("expected assigned_analysis_agent_id=analysis-agent-alt, got %q payload=%v", got, payload)
	}
}

func TestCannedLLME2E_Scenario5_ValidationFailureRevision(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping canned runtime e2e scenarios in -short mode")
	}
	sc := loadCannedScenario(t, "scenario5-validation-failure-revision")
	rig := startE2EScenarioRig(t, sc, false)
	defer rig.Close()

	publishScenarioDirective(t, rig, "Corpus in Argentina")
	if err := waitForDBEventTypeCount(rig.eventStore, "vertical.ready_for_review", 1, 15*time.Second); err != nil {
		t.Fatalf("wait vertical.ready_for_review: %v", err)
	}
	verticalID := latestVerticalIDByEvent(t, rig.db, "vertical.ready_for_review")
	if strings.TrimSpace(verticalID) == "" {
		t.Fatal("missing vertical id for scenario5 approval")
	}
	if err := rig.bus.Publish(rig.ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.approved"),
		SourceAgent: "human",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"vertical_id": verticalID}),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish vertical.approved: %v", err)
	}
	if err := waitForDBEventTypeCount(rig.eventStore, "vertical.approved", 1, cannedE2EWaitTimeout); err != nil {
		t.Fatalf("wait vertical.approved: %v", err)
	}
	assertScenarioExpectedCounts(t, rig.db, sc.Expected)

	var revisionCount int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		err := rig.db.QueryRowContext(rig.ctx, `
			SELECT COALESCE(revision_count,0)
			FROM validation_pipelines
			WHERE vertical_id = $1::uuid
		`, verticalID).Scan(&revisionCount)
		if err == nil && revisionCount >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if revisionCount < 1 {
		t.Fatalf("expected revision_count>=1 after validation failure, got %d", revisionCount)
	}
}

func TestCannedLLME2E_Scenario6_HumanRejectsMailboxThenApproves(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping canned runtime e2e scenarios in -short mode")
	}
	sc := loadCannedScenario(t, "scenario6-human-rejects-mailbox")
	rig := startE2EScenarioRig(t, sc, false)
	defer rig.Close()

	publishScenarioDirective(t, rig, "Corpus in Argentina")
	if err := waitForDBEventTypeCount(rig.eventStore, "vertical.ready_for_review", 1, 15*time.Second); err != nil {
		t.Fatalf("wait first vertical.ready_for_review: %v", err)
	}
	mailboxID, verticalID, err := waitForPendingMailboxApproval(rig.mailboxStore, 5*time.Second)
	if err != nil {
		t.Fatalf("wait first pending mailbox approval: %v", err)
	}
	if strings.TrimSpace(verticalID) == "" {
		t.Fatalf("missing vertical_id on pending mailbox item %s", mailboxID)
	}
	if _, err := rig.db.ExecContext(rig.ctx, `
		UPDATE mailbox
		SET status = 'more_data', decision='more_data', decision_notes='insufficient evidence'
		WHERE id = $1::uuid
	`, mailboxID); err != nil {
		t.Fatalf("update mailbox decision: %v", err)
	}
	if err := rig.bus.Publish(rig.ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.needs_more_data"),
		SourceAgent: "human",
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id": verticalID,
			"reason":      "needs_more_data",
		}),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish vertical.needs_more_data: %v", err)
	}
	if err := waitForDBEventTypeCount(rig.eventStore, "validation.more_data_needed", 1, cannedE2EWaitTimeout); err != nil {
		t.Fatalf("wait validation.more_data_needed: %v", err)
	}
	if err := rig.bus.Publish(rig.ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("scoring.requested"),
		SourceAgent: "human",
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id":   verticalID,
			"vertical_name": "Clinic Billing Exception Coordinator",
			"geography":     "argentina",
			"mode":          "corpus",
			"rubric":        "universal",
			"dimensions_requested": []string{
				"build_complexity",
				"automation_completeness",
				"icp_crispness",
				"distribution_leverage",
				"time_to_value",
				"operational_drag",
				"pain_severity",
				"competition_gap",
				"monetization_clarity",
				"retention_architecture",
				"expansion_potential",
			},
			"discovery_context": map[string]any{
				"source": "needs_more_data_rescore",
				"cycle":  2,
			},
		}),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish scoring.requested cycle2: %v", err)
	}
	if err := waitForDBEventTypeCount(rig.eventStore, "vertical.ready_for_review", 2, 20*time.Second); err != nil {
		t.Fatalf("wait second vertical.ready_for_review: %v", err)
	}
	if err := rig.bus.Publish(rig.ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.approved"),
		SourceAgent: "human",
		VerticalID:  verticalID,
		Payload:     mustJSON(map[string]any{"vertical_id": verticalID}),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish vertical.approved: %v", err)
	}
	if err := waitForDBEventTypeCount(rig.eventStore, "opco.spinup_requested", 1, 12*time.Second); err != nil {
		t.Fatalf("wait opco.spinup_requested: %v", err)
	}
	assertScenarioExpectedCounts(t, rig.db, sc.Expected)
}

func TestCannedLLME2E_Scenario7_CampaignMultiMode(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping canned runtime e2e scenarios in -short mode")
	}
	sc := loadCannedScenario(t, "scenario7-campaign-multi-mode")
	rig := startE2EScenarioRig(t, sc, false)
	defer rig.Close()

	if err := rig.bus.Publish(rig.ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSON(map[string]any{"directive_text": "run saas_gap in US"}),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish saas_gap directive: %v", err)
	}
	if err := rig.bus.Publish(rig.ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload:     mustJSON(map[string]any{"directive_text": "run saas_trend in US"}),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish saas_trend directive: %v", err)
	}
	if err := waitForDBEventTypeCount(rig.eventStore, "campaign.completed", 1, 15*time.Second); err != nil {
		t.Fatalf("wait campaign.completed: %v", err)
	}
	assertScenarioExpectedCounts(t, rig.db, sc.Expected)

	rows, err := rig.db.QueryContext(rig.ctx, `
		SELECT COALESCE(payload->>'mode','')
		FROM events
		WHERE type = 'scan.completed'
		ORDER BY created_at ASC, id ASC
	`)
	if err != nil {
		t.Fatalf("query scan.completed modes: %v", err)
	}
	defer rows.Close()
	modes := make([]string, 0, 4)
	for rows.Next() {
		var mode string
		if err := rows.Scan(&mode); err != nil {
			t.Fatalf("scan mode: %v", err)
		}
		modes = append(modes, runtimepipeline.NormalizeScanMode(strings.TrimSpace(mode)))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate scan.completed modes: %v", err)
	}
	if !(containsStringTrimmed(modes, "saas_gap") && containsStringTrimmed(modes, "saas_trend")) {
		t.Fatalf("expected scan.completed modes to include saas_gap and saas_trend, got %v", modes)
	}
}

func TestCannedLLME2E_Scenario8_BudgetThrottleEmergency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping canned runtime e2e scenarios in -short mode")
	}
	sc := loadCannedScenario(t, "scenario8-budget-throttle-emergency")
	rig := startE2EScenarioRig(t, sc, false)
	defer rig.Close()

	tracker := &BudgetTracker{lastState: map[string]string{}}
	rig.am.SetBudgetTracker(tracker)

	publishScenarioDirective(t, rig, "Corpus in Argentina")
	if err := waitForDBEventTypeCount(rig.eventStore, "scoring.requested", 1, 12*time.Second); err != nil {
		t.Fatalf("wait scoring.requested: %v", err)
	}

	if err := rig.bus.Publish(rig.ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("budget.threshold_crossed"),
		SourceAgent: "runtime",
		Payload: mustJSON(map[string]any{
			"state": "throttle",
			"level": 80,
		}),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish budget.threshold_crossed throttle: %v", err)
	}
	if err := waitForDBEventTypeCount(rig.eventStore, "budget.throttle", 1, cannedE2EWaitTimeout); err != nil {
		t.Fatalf("wait budget.throttle: %v", err)
	}

	verticalID := latestVerticalIDByEvent(t, rig.db, "vertical.discovered")
	if strings.TrimSpace(verticalID) == "" {
		t.Fatal("missing vertical id for budget suppression check")
	}
	tracker.mu.Lock()
	tracker.lastState["vertical|"+verticalID] = "emergency"
	tracker.mu.Unlock()

	if err := rig.bus.Publish(rig.ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("budget.threshold_crossed"),
		SourceAgent: "runtime",
		Payload: mustJSON(map[string]any{
			"state": "emergency",
			"level": 95,
		}),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish budget.threshold_crossed emergency: %v", err)
	}
	if err := waitForDBEventTypeCount(rig.eventStore, "budget.emergency", 1, cannedE2EWaitTimeout); err != nil {
		t.Fatalf("wait budget.emergency: %v", err)
	}

	if err := waitForDBEventTypeCount(rig.eventStore, "score.dimension_complete", 11, 12*time.Second); err != nil {
		t.Fatalf("wait scoring drain before emergency suppression check: %v", err)
	}
	scoreWatch := rig.bus.Subscribe("watch-budget-emergency-score", events.EventType("score.dimension_complete"))
	if err := rig.bus.Publish(rig.ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("scoring.requested"),
		SourceAgent: "manual-test",
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id": verticalID,
			"mode":        "corpus",
			"geography":   "argentina",
		}),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish scoring.requested during emergency: %v", err)
	}
	assertNoEventType(t, scoreWatch, "score.dimension_complete", 500*time.Millisecond)
	assertScenarioExpectedCounts(t, rig.db, sc.Expected)
}

func TestCannedLLME2E_Scenario9_DedupCollision(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping canned runtime e2e scenarios in -short mode")
	}
	sc := loadCannedScenario(t, "scenario9-dedup-collision")
	rig := startE2EScenarioRig(t, sc, false)
	defer rig.Close()

	publishScenarioDirective(t, rig, "Corpus in Argentina")
	if err := waitForDBEventTypeCount(rig.eventStore, "dedup.ambiguous", 1, 12*time.Second); err != nil {
		t.Fatalf("wait dedup.ambiguous: %v", err)
	}
	if err := waitForDBEventTypeCount(rig.eventStore, "scan.completed", 1, 12*time.Second); err != nil {
		t.Fatalf("wait scan.completed: %v", err)
	}
	if err := waitForDBEventTypeCount(rig.eventStore, "campaign.completed", 1, 12*time.Second); err != nil {
		t.Fatalf("wait campaign.completed: %v", err)
	}
	assertScenarioExpectedCounts(t, rig.db, sc.Expected)

	waitForPendingDedupCount(t, rig.db, rig.ctx, 1, 5*time.Second)

	payload := latestEventPayload(t, rig.db, "dedup.ambiguous")
	dedupEventID := strings.TrimSpace(asString(payload["dedup_event_id"]))
	if dedupEventID == "" {
		t.Fatalf("dedup.ambiguous missing dedup_event_id payload=%v", payload)
	}
	if err := rig.bus.Publish(rig.ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("dedup.resolved"),
		SourceAgent: "discovery-coordinator",
		Payload: mustJSON(map[string]any{
			"dedup_event_id": dedupEventID,
			"action":         "keep_existing",
		}),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish dedup.resolved: %v", err)
	}
	waitForPendingDedupDrain(t, rig.db, rig.ctx, 5*time.Second)
}

func TestCannedLLME2E_Scenario10_OpCoTeardown(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping canned runtime e2e scenarios in -short mode")
	}
	sc := loadCannedScenario(t, "scenario10-opco-teardown")
	rig := startE2EScenarioRig(t, sc, false)
	defer rig.Close()

	verticalID := uuid.NewString()
	if _, err := rig.db.ExecContext(rig.ctx, `
		INSERT INTO verticals (id, name, slug, geography, stage, mode, created_at, updated_at)
		VALUES ($1::uuid, $2, $3, $4, 'approved', 'factory', now(), now())
	`, verticalID, "Scenario10 Vertical", "scenario10-vertical", "us"); err != nil {
		t.Fatalf("seed scenario10 vertical: %v", err)
	}

	if err := rig.bus.Publish(rig.ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("opco.spinup_requested"),
		SourceAgent: "human",
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id": verticalID,
			"mandate": map[string]any{
				"vertical_id":   verticalID,
				"founder_notes": "spinup for teardown scenario",
			},
		}),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish opco.spinup_requested: %v", err)
	}
	if err := waitForDBEventTypeCount(rig.eventStore, "opco.ceo_ready", 1, 12*time.Second); err != nil {
		t.Fatalf("wait opco.ceo_ready: %v", err)
	}

	if err := rig.bus.Publish(rig.ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("opco.teardown_requested"),
		SourceAgent: "human",
		VerticalID:  verticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id": verticalID,
			"reason":      "scenario10 teardown",
		}),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish opco.teardown_requested: %v", err)
	}
	if err := waitForDBEventTypeCount(rig.eventStore, "opco.teardown_complete", 1, 12*time.Second); err != nil {
		t.Fatalf("wait opco.teardown_complete: %v", err)
	}
	if err := waitForDBEventTypeCount(rig.eventStore, "vertical.killed", 1, 12*time.Second); err != nil {
		t.Fatalf("wait vertical.killed: %v", err)
	}
	assertScenarioExpectedCounts(t, rig.db, sc.Expected)

	for _, role := range []string{
		"opco-ceo",
		"chief-of-staff",
		"vp-product",
		"vp-growth",
		"cto-agent",
		"pm-agent",
		"support-agent",
		"marketing-agent",
		"tech-writer",
		"backend-agent",
		"frontend-agent",
		"qa-agent",
		"devops-agent",
	} {
		if _, ok := rig.am.GetAgentConfig(runtimemanager.OpCoAgentID(role, verticalID)); ok {
			t.Fatalf("expected opco agent %s to be removed after teardown", role)
		}
	}
	var stage string
	if err := rig.db.QueryRowContext(rig.ctx, `SELECT COALESCE(stage,'') FROM verticals WHERE id=$1::uuid`, verticalID).Scan(&stage); err != nil {
		t.Fatalf("load scenario10 vertical stage: %v", err)
	}
	if strings.TrimSpace(stage) != "killed" {
		t.Fatalf("expected vertical stage killed after teardown, got %q", stage)
	}
}

func containsStringTrimmed(items []string, target string) bool {
	target = strings.TrimSpace(target)
	for _, item := range items {
		if strings.TrimSpace(item) == target {
			return true
		}
	}
	return false
}

func TestCannedLLME2E_ScenarioFilesExist(t *testing.T) {
	want := []string{
		"scenario2-prefilter-reject-all.yaml",
		"scenario3-marginal-path.yaml",
		"scenario4-derivation-loop.yaml",
		"scenario5-validation-failure-revision.yaml",
		"scenario6-human-rejects-mailbox.yaml",
		"scenario7-campaign-multi-mode.yaml",
		"scenario8-budget-throttle-emergency.yaml",
		"scenario9-dedup-collision.yaml",
		"scenario10-opco-teardown.yaml",
	}
	root := projectPathFromThisFile("contracts", "test-vectors", "e2e")
	for _, name := range want {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				t.Fatalf("missing scenario fixture file: %s", name)
			}
			t.Fatalf("stat scenario fixture %s: %v", name, err)
		}
	}
}
