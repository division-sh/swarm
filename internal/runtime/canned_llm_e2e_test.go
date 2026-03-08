package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"empireai/internal/events"
	"empireai/internal/models"
	runtimeagents "empireai/internal/runtime/agents"
	llm "empireai/internal/runtime/llm"
	runtimemanager "empireai/internal/runtime/manager"
	runtimepipeline "empireai/internal/runtime/pipeline"
	empirepipeline "empireai/internal/runtime/pipeline/empire"
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/testutil"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

const cannedE2EWaitTimeout = 20 * time.Second

type threadSafeEventStore struct {
	mu          sync.Mutex
	events      []events.Event
	deliveries  map[string][]string
	eventCounts map[string]int
	notifyCh    chan struct{}
}

func (s *threadSafeEventStore) AppendEvent(_ context.Context, evt events.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureNotifyLocked()
	s.events = append(s.events, evt)
	if s.eventCounts == nil {
		s.eventCounts = make(map[string]int)
	}
	s.eventCounts[strings.TrimSpace(string(evt.Type))]++
	s.signalLocked()
	return nil
}

func (s *threadSafeEventStore) InsertEventDeliveries(_ context.Context, eventID string, agentIDs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deliveries == nil {
		s.deliveries = make(map[string][]string)
	}
	s.deliveries[eventID] = append([]string(nil), agentIDs...)
	return nil
}

func (s *threadSafeEventStore) SnapshotEvents() []events.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]events.Event, len(s.events))
	copy(out, s.events)
	return out
}

func (s *threadSafeEventStore) ensureNotifyLocked() {
	if s.notifyCh == nil {
		s.notifyCh = make(chan struct{}, 1)
	}
}

func (s *threadSafeEventStore) signalLocked() {
	s.ensureNotifyLocked()
	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
}

func (s *threadSafeEventStore) WaitForEventTypeCount(eventType string, want int, timeout time.Duration) error {
	eventType = strings.TrimSpace(eventType)
	deadline := time.Now().Add(timeout)
	for {
		s.mu.Lock()
		s.ensureNotifyLocked()
		got := s.eventCounts[eventType]
		notifyCh := s.notifyCh
		s.mu.Unlock()
		if got >= want {
			return nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("timed out waiting for %s count>=%d (last=%d)", eventType, want, got)
		}
		select {
		case <-notifyCh:
		case <-time.After(remaining):
			return fmt.Errorf("timed out waiting for %s count>=%d (last=%d)", eventType, want, got)
		}
	}
}

func (s *threadSafeEventStore) WaitUntil(timeout time.Duration, condition func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for {
		ok, err := condition()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		s.mu.Lock()
		s.ensureNotifyLocked()
		notifyCh := s.notifyCh
		s.mu.Unlock()
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("condition not met within %s", timeout)
		}
		select {
		case <-notifyCh:
		case <-time.After(remaining):
			return fmt.Errorf("condition not met within %s", timeout)
		}
	}
}

type cannedRoleRuntime struct {
	mu             sync.Mutex
	userTurnCounts map[string]int
	prompts        map[string]string
	starts         map[string]int
}

func newCannedRoleRuntime() *cannedRoleRuntime {
	return &cannedRoleRuntime{
		userTurnCounts: map[string]int{},
		prompts:        map[string]string{},
		starts:         map[string]int{},
	}
}

func (r *cannedRoleRuntime) StartSession(_ context.Context, agentID string, systemPrompt string, tools []llm.ToolDefinition) (*llm.Session, error) {
	if err := validateToolDefinitionsDraft202012(tools); err != nil {
		return nil, fmt.Errorf("invalid tool schema for agent %s: %w", strings.TrimSpace(agentID), err)
	}
	r.mu.Lock()
	r.prompts[strings.TrimSpace(agentID)] = strings.TrimSpace(systemPrompt)
	r.starts[strings.TrimSpace(agentID)]++
	r.mu.Unlock()
	return &llm.Session{
		ID:               "sess-" + strings.TrimSpace(agentID) + "-" + uuid.NewString(),
		AgentID:          strings.TrimSpace(agentID),
		RuntimeMode:      "canned",
		ConversationMode: "task",
	}, nil
}

func (r *cannedRoleRuntime) ContinueSession(_ context.Context, session *llm.Session, msg llm.Message) (*llm.Response, error) {
	if session == nil {
		return nil, errors.New("session is required")
	}
	agentID := strings.TrimSpace(session.AgentID)
	if agentID == "" {
		return nil, errors.New("session.AgentID is required")
	}
	if strings.EqualFold(strings.TrimSpace(msg.Role), "tool") {
		return &llm.Response{Message: llm.Message{Role: "assistant", Content: "ack"}}, nil
	}

	switch agentID {
	case "market-research-agent":
		turn := r.nextUserTurn(agentID)
		if turn > 0 {
			return &llm.Response{Message: llm.Message{Role: "assistant", Content: "done"}}, nil
		}
		return buildCannedMRAResponse(msg.Content)
	case "analysis-agent":
		turn := r.nextUserTurn(agentID)
		if turn == 0 {
			return buildCannedAnalysisResponse(msg.Content, "shortlisted")
		}
		if turn == 1 {
			return buildCannedAnalysisResponse(msg.Content, "marginal")
		}
		return &llm.Response{Message: llm.Message{Role: "assistant", Content: "done"}}, nil
	default:
		return &llm.Response{Message: llm.Message{Role: "assistant", Content: "noop"}}, nil
	}
}

func (r *cannedRoleRuntime) nextUserTurn(agentID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	turn := r.userTurnCounts[agentID]
	r.userTurnCounts[agentID] = turn + 1
	return turn
}

func (r *cannedRoleRuntime) PromptFor(agentID string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.TrimSpace(r.prompts[strings.TrimSpace(agentID)])
}

func (r *cannedRoleRuntime) StartsFor(agentID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.starts[strings.TrimSpace(agentID)]
}

func buildCannedMRAResponse(input string) (*llm.Response, error) {
	scanID := extractMessageField(input, "scan_id")
	if scanID == "" {
		return nil, errors.New("canned MRA runtime could not extract scan_id from market_research.scan_assigned")
	}
	campaignID := extractMessageField(input, "campaign_id")
	mode := runtimepipeline.NormalizeScanMode(extractMessageField(input, "mode"))
	if mode == "" {
		mode = "corpus"
	}
	geography := strings.TrimSpace(extractMessageField(input, "geography"))
	if geography == "" {
		geography = "argentina"
	}

	calls := []llm.ToolCall{
		{
			Name: "emit_category_assessed",
			Arguments: map[string]any{
				"scan_id":             scanID,
				"campaign_id":         campaignID,
				"mode":                mode,
				"geography":           geography,
				"category":            "operations",
				"subcategory":         "dental_billing",
				"signal_strength":     68,
				"opportunity_name":    "Dental Insurance Reconciliation Copilot",
				"preliminary_icp":     "Billing manager at multi-dentist clinics handling invoice reconciliation workflow",
				"opportunity_pattern": "workflow_automation",
				"build_sketch": map[string]any{
					"core_features":    []any{"remittance parser", "claim mismatch queue", "exception routing"},
					"key_integrations": []any{"xero", "quickbooks"},
					"red_flags": []any{
						map[string]any{"type": "accuracy_liability"},
						map[string]any{"type": "one_time_setup"},
					},
				},
				"evidence": map[string]any{
					"competitors": []any{
						map[string]any{"name": "Dentalintel", "pricing": "$500/mo", "source_url": "https://example.com/dentalintel"},
					},
					"pain_signals": []any{
						map[string]any{"signal": "claims mismatch delays payouts", "source_url": "https://example.com/pain-claims"},
					},
					"regulatory": []any{
						map[string]any{"detail": "insurance coding audit cadence", "source_url": "https://example.com/reg-claims"},
					},
					"buyer_communities": []any{
						map[string]any{"name": "Dental Billing Managers", "source_url": "https://example.com/community-dental"},
					},
				},
				"opportunity_hypothesis": "Workflow automation that reconciles insurer invoice remittance files against practice ledgers.",
				"geographic_scope":       "local",
			},
		},
		{
			Name: "emit_category_assessed",
			Arguments: map[string]any{
				"scan_id":             scanID,
				"campaign_id":         campaignID,
				"mode":                mode,
				"geography":           geography,
				"category":            "operations",
				"subcategory":         "ap_ar",
				"signal_strength":     62,
				"opportunity_name":    "Trade Contractor AP Packet Validator",
				"preliminary_icp":     "AP manager at construction subcontractors processing invoice workflow",
				"opportunity_pattern": "workflow_automation",
				"build_sketch": map[string]any{
					"core_features":    []any{"packet completeness check", "approval workflow", "pay-app export"},
					"key_integrations": []any{"procore", "quickbooks"},
					"red_flags":        []any{},
				},
				"evidence": map[string]any{
					"competitors": []any{
						map[string]any{"name": "Procore Financials", "pricing": "$399/mo", "source_url": "https://example.com/procore-financials"},
					},
					"pain_signals": []any{
						map[string]any{"signal": "manual packet checks delay payments", "source_url": "https://example.com/pain-packets"},
					},
					"regulatory": []any{
						map[string]any{"detail": "lien waiver package requirements", "source_url": "https://example.com/reg-lien"},
					},
					"buyer_communities": []any{
						map[string]any{"name": "Construction AP Leaders", "source_url": "https://example.com/community-ap"},
					},
				},
				"opportunity_hypothesis": "Recurring AP workflow validator that checks compliance packet completeness before pay-application submission.",
				"geographic_scope":       "local",
			},
		},
		{
			Name: "emit_category_assessed",
			Arguments: map[string]any{
				"scan_id":             scanID,
				"campaign_id":         campaignID,
				"mode":                mode,
				"geography":           geography,
				"category":            "operations",
				"subcategory":         "consulting",
				"signal_strength":     54,
				"opportunity_name":    "General Admin Outsourcing Console",
				"preliminary_icp":     "Owner at small agencies",
				"opportunity_pattern": "unknown",
				"build_sketch": map[string]any{
					"core_features":    []any{"task board", "outsourcing roster", "manual handoff notes"},
					"key_integrations": []any{},
					"red_flags":        []any{},
				},
				"evidence": map[string]any{
					"competitors": []any{
						map[string]any{"name": "Generalist VA Marketplace", "pricing": "$149/mo", "source_url": "https://example.com/va-market"},
					},
					"pain_signals": []any{
						map[string]any{"signal": "owners mention admin overload", "source_url": "https://example.com/pain-admin"},
					},
					"regulatory": []any{},
					"buyer_communities": []any{
						map[string]any{"name": "Agency Owners Group", "source_url": "https://example.com/community-agency"},
					},
				},
				"opportunity_hypothesis": "A generic task handoff tool for occasional outsourcing.",
				"geographic_scope":       "local",
			},
		},
		{
			Name: "emit_market_research_scan_complete",
			Arguments: map[string]any{
				"scan_id":             scanID,
				"campaign_id":         campaignID,
				"mode":                mode,
				"geography":           geography,
				"categories_assessed": 3,
				"high_signal_count":   2,
			},
		},
	}
	return &llm.Response{
		Message:   llm.Message{Role: "assistant", Content: "emitting corpus findings"},
		ToolCalls: calls,
	}, nil
}

func buildCannedAnalysisResponse(input string, profile string) (*llm.Response, error) {
	verticalID := extractMessageField(input, "vertical_id")
	if verticalID == "" {
		return nil, errors.New("canned analysis runtime could not extract vertical_id from scoring.requested")
	}
	scores := map[string]int{
		"build_complexity":        90,
		"automation_completeness": 90,
		"icp_crispness":           90,
		"distribution_leverage":   90,
		"time_to_value":           90,
		"operational_drag":        90,
		"pain_severity":           90,
		"competition_gap":         90,
		"monetization_clarity":    90,
		"retention_architecture":  90,
		"expansion_potential":     90,
	}
	if strings.EqualFold(strings.TrimSpace(profile), "marginal") {
		scores = map[string]int{
			"build_complexity":        72,
			"automation_completeness": 72,
			"icp_crispness":           72,
			"distribution_leverage":   72,
			"time_to_value":           62,
			"operational_drag":        62,
			"pain_severity":           60,
			"competition_gap":         60,
			"monetization_clarity":    60,
			"retention_architecture":  60,
			"expansion_potential":     60,
		}
	}
	dims := expectedScoringDimensions("universal")
	calls := make([]llm.ToolCall, 0, len(dims))
	for _, dim := range dims {
		score := scores[dim]
		if score == 0 {
			score = 60
		}
		calls = append(calls, llm.ToolCall{
			Name: "emit_score_dimension_complete",
			Arguments: map[string]any{
				"vertical_id": verticalID,
				"dimension":   dim,
				"score":       score,
				"evidence":    "canned-e2e " + strings.TrimSpace(profile) + " evidence for " + dim,
				"confidence":  "high",
			},
		})
	}
	return &llm.Response{
		Message:   llm.Message{Role: "assistant", Content: "emitting scores"},
		ToolCalls: calls,
	}, nil
}

func extractMessageField(input, key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}
	if payload := extractInboundPayloadMap(input); len(payload) > 0 {
		if raw, ok := payload[key]; ok {
			if text := strings.TrimSpace(asString(raw)); text != "" {
				return text
			}
		}
	}
	jsonPattern := regexp.MustCompile(`"` + regexp.QuoteMeta(key) + `"\s*:\s*"([^"]+)"`)
	if m := jsonPattern.FindStringSubmatch(input); len(m) >= 2 {
		return strings.TrimSpace(m[1])
	}
	linePattern := regexp.MustCompile(`(?m)-\s*` + regexp.QuoteMeta(key) + `\s*:\s*([^\n]+)$`)
	if m := linePattern.FindStringSubmatch(input); len(m) >= 2 {
		return strings.Trim(strings.TrimSpace(m[1]), `"`)
	}
	return ""
}

func waitForEventTypeTimeout(ch <-chan events.Event, typ string, timeout time.Duration) (events.Event, error) {
	deadline := time.After(timeout)
	for {
		select {
		case evt := <-ch:
			if strings.TrimSpace(string(evt.Type)) == strings.TrimSpace(typ) {
				return evt, nil
			}
		case <-deadline:
			return events.Event{}, fmt.Errorf("timed out waiting for %s", strings.TrimSpace(typ))
		}
	}
}

func countEventTypes(eventsList []events.Event) map[string]int {
	out := make(map[string]int, len(eventsList))
	for _, evt := range eventsList {
		out[strings.TrimSpace(string(evt.Type))]++
	}
	return out
}

func waitForEventTypeCount(store *threadSafeEventStore, eventType string, want int, timeout time.Duration) error {
	return store.WaitForEventTypeCount(eventType, want, timeout)
}

func waitForStageSignals(t *testing.T, ch <-chan string, want []string, timeout time.Duration) {
	t.Helper()
	remaining := make(map[string]int, len(want))
	for _, stage := range want {
		remaining[strings.TrimSpace(stage)]++
	}
	deadline := time.After(timeout)
	for len(remaining) > 0 {
		select {
		case stage := <-ch:
			stage = strings.TrimSpace(stage)
			if count, ok := remaining[stage]; ok {
				if count <= 1 {
					delete(remaining, stage)
				} else {
					remaining[stage] = count - 1
				}
			}
		case <-deadline:
			t.Fatalf("timed out waiting for stage updates: remaining=%v", remaining)
		}
	}
}

var validDraft202012Types = map[string]struct{}{
	"string":  {},
	"number":  {},
	"integer": {},
	"boolean": {},
	"array":   {},
	"object":  {},
	"null":    {},
}

func validateToolDefinitionsDraft202012(tools []llm.ToolDefinition) error {
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Name)
		if name == "" {
			continue
		}
		raw, err := json.Marshal(tool.Schema)
		if err != nil {
			return fmt.Errorf("tool=%s marshal schema: %w", name, err)
		}
		var normalized any
		if err := json.Unmarshal(raw, &normalized); err != nil {
			return fmt.Errorf("tool=%s normalize schema: %w", name, err)
		}
		if err := validateDraftSchemaNode("tool="+name, "", normalized); err != nil {
			return err
		}
	}
	return nil
}

func validateDraftSchemaNode(path, parentKey string, v any) error {
	switch node := v.(type) {
	case map[string]any:
		interpretsTypeKeyword := parentKey != "properties" &&
			parentKey != "patternProperties" &&
			parentKey != "$defs" &&
			parentKey != "definitions"
		if interpretsTypeKeyword {
			if rawType, ok := node["type"]; ok {
				if err := validateDraftTypeValue(path+".type", rawType); err != nil {
					return err
				}
			}
		}
		for key, child := range node {
			if key == "type" && interpretsTypeKeyword {
				continue
			}
			if err := validateDraftSchemaNode(path+"."+key, key, child); err != nil {
				return err
			}
		}
	case []any:
		for i, child := range node {
			if err := validateDraftSchemaNode(fmt.Sprintf("%s[%d]", path, i), parentKey, child); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateDraftTypeValue(path string, raw any) error {
	validate := func(v string) error {
		v = strings.TrimSpace(v)
		if _, ok := validDraft202012Types[v]; !ok {
			return fmt.Errorf("invalid JSON Schema Draft 2020-12 type at %s: %q", path, v)
		}
		return nil
	}
	switch tv := raw.(type) {
	case string:
		return validate(tv)
	case []any:
		if len(tv) == 0 {
			return fmt.Errorf("invalid JSON Schema type array at %s: empty", path)
		}
		for i, entry := range tv {
			typeStr, ok := entry.(string)
			if !ok {
				return fmt.Errorf("invalid JSON Schema type array entry at %s[%d]", path, i)
			}
			if err := validate(typeStr); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("invalid JSON Schema type encoding at %s", path)
	}
}

func assertScanCampaignState(t *testing.T, db *sql.DB, campaignID string, discoveries int) {
	t.Helper()
	var status string
	var gotDiscoveries int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COALESCE(status,''), COALESCE(discoveries,0)
		FROM scan_campaigns
		WHERE id = $1::uuid
	`, campaignID).Scan(&status, &gotDiscoveries); err != nil {
		t.Fatalf("load scan campaign %s: %v", campaignID, err)
	}
	if strings.TrimSpace(status) != "completed" {
		t.Fatalf("expected campaign status completed, got %q", status)
	}
	if gotDiscoveries != discoveries {
		t.Fatalf("expected campaign discoveries=%d, got %d", discoveries, gotDiscoveries)
	}
}

type e2eCampaignStore struct {
	db *sql.DB
}

func (s *e2eCampaignStore) CreateScanCampaign(ctx context.Context, in runtimepipeline.CreateScanCampaignInput) (runtimepipeline.ScanCampaign, error) {
	id := uuid.NewString()
	priority := strings.TrimSpace(in.Priority)
	if priority == "" {
		priority = "normal"
	}
	status := strings.TrimSpace(in.Status)
	if status == "" {
		status = "queued"
	}
	strategic := strings.TrimSpace(string(in.StrategicContext))
	if strategic == "" {
		strategic = "{}"
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO scan_campaigns (
			id, geography_id, directive_id, mode, categories, priority, status, strategic_context, deadline_at, created_at
		) VALUES (
			$1::uuid, $2::uuid, NULLIF($3,'')::uuid, $4, $5::text[], $6, $7, $8::jsonb, $9, now()
		)
	`, id, in.GeographyID, strings.TrimSpace(in.DirectiveID), in.Mode, pq.Array(in.Categories), priority, status, strategic, in.DeadlineAt); err != nil {
		return runtimepipeline.ScanCampaign{}, err
	}
	return runtimepipeline.ScanCampaign{
		ID:               id,
		GeographyID:      in.GeographyID,
		DirectiveID:      strings.TrimSpace(in.DirectiveID),
		Mode:             in.Mode,
		Categories:       append([]string(nil), in.Categories...),
		Priority:         priority,
		Status:           status,
		StrategicContext: in.StrategicContext,
		DeadlineAt:       in.DeadlineAt,
		CreatedAt:        time.Now().UTC(),
	}, nil
}

func (s *e2eCampaignStore) ListScanCampaigns(context.Context, runtimepipeline.ScanCampaignFilter) ([]runtimepipeline.ScanCampaign, error) {
	return nil, nil
}

func (s *e2eCampaignStore) ClaimNextDueScanCampaign(ctx context.Context) (runtimepipeline.ScanCampaign, bool, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT
			id::text,
			geography_id::text,
			COALESCE(directive_id::text, ''),
			mode,
			COALESCE(categories, ARRAY[]::text[]),
			COALESCE(priority, 'normal'),
			COALESCE(strategic_context, '{}'::jsonb)
		FROM scan_campaigns
		WHERE status = 'queued'
		ORDER BY created_at ASC
		LIMIT 1
	`)
	var c runtimepipeline.ScanCampaign
	if err := row.Scan(&c.ID, &c.GeographyID, &c.DirectiveID, &c.Mode, pq.Array(&c.Categories), &c.Priority, &c.StrategicContext); err != nil {
		if err == sql.ErrNoRows {
			return runtimepipeline.ScanCampaign{}, false, nil
		}
		return runtimepipeline.ScanCampaign{}, false, err
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE scan_campaigns
		SET status = 'active', started_at = now()
		WHERE id = $1::uuid
	`, c.ID); err != nil {
		return runtimepipeline.ScanCampaign{}, false, err
	}
	c.Status = "active"
	return c, true, nil
}

func (s *e2eCampaignStore) LookupGeographyLabel(ctx context.Context, geographyID string) (string, error) {
	var name string
	if err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(name, '')
		FROM geographies
		WHERE id = $1::uuid
	`, geographyID).Scan(&name); err != nil {
		return "", err
	}
	return strings.TrimSpace(name), nil
}

func (s *e2eCampaignStore) MarkScanCampaignCompleted(ctx context.Context, campaignID string, discoveries int) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE scan_campaigns
		SET status = 'completed', discoveries = $2, completed_at = now()
		WHERE id = $1::uuid
	`, campaignID, discoveries)
	return err
}

func (s *e2eCampaignStore) RequeueDueRescans(context.Context, time.Time) (int, error) {
	return 0, nil
}

func (s *e2eCampaignStore) PauseQueuedScanCampaigns(ctx context.Context) (int, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE scan_campaigns SET status = 'paused' WHERE status = 'queued'`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func (s *e2eCampaignStore) ResumePausedScanCampaigns(ctx context.Context) (int, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE scan_campaigns SET status = 'queued' WHERE status = 'paused'`)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

func TestCannedLLME2E_CorpusDirectiveHappyPath(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping canned runtime e2e in -short mode")
	}
	_, db, _ := testutil.StartPostgres(t)
	eventStore := &threadSafeEventStore{}
	bus := NewEventBus(eventStore)
	bus.SetRuntimeLogger(NewRuntimeLogger(db))
	pc := runtimepipeline.NewFactoryPipelineCoordinatorWithOptions(bus, db, runtimepipeline.FactoryPipelineCoordinatorOptions{Module: empirepipeline.NewModule()})
	stageSignals := make(chan string, 8)
	pc.SetTestVerticalStageHook(func(_ string, stage string) {
		select {
		case stageSignals <- strings.TrimSpace(stage):
		default:
		}
	})
	bus.SetInterceptors(pc)

	canned := newCannedRoleRuntime()
	exec := runtimetools.NewExecutor(bus, nil, nil)
	factory := runtimeagents.NewLLMAgentFactory(canned, exec, exec.ToolDefinitions())
	am := runtimemanager.NewAgentManager(bus, factory)

	if err := am.SpawnAgent(models.AgentConfig{
		ID:            "market-research-agent",
		Type:          "worker",
		Role:          "market-research-agent",
		Mode:          "factory",
		Subscriptions: []string{"market_research.scan_assigned"},
	}); err != nil {
		t.Fatalf("spawn market-research-agent: %v", err)
	}
	if err := am.SpawnAgent(models.AgentConfig{
		ID:            "analysis-agent",
		Type:          "worker",
		Role:          "analysis-agent",
		Mode:          "factory",
		Subscriptions: []string{"scoring.requested"},
	}); err != nil {
		t.Fatalf("spawn analysis-agent: %v", err)
	}

	scanMgr := runtimepipeline.NewScanCampaignManager(bus, &e2eCampaignStore{db: db}, newScanCampaignHooksForTest(), db)
	scoringNode := runtimepipeline.NewScoringNode(bus, pc, nil)
	if scoringNode == nil {
		t.Fatal("expected scoring node")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	am.Run(ctx)
	defer func() { _ = am.Shutdown() }()
	go scanMgr.Run(ctx)
	go scoringNode.Run(ctx)

	campaignCompletedCh := bus.Subscribe("watch-canned-campaign", events.EventType("campaign.completed"))
	corpusPath := filepath.Join(t.TempDir(), "corpus-signals.jsonl")
	if err := os.WriteFile(corpusPath, []byte("{\"signal\":\"clinic billing automation\"}\n"), 0o600); err != nil {
		t.Fatalf("write temp corpus jsonl: %v", err)
	}

	if err := bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload: mustJSON(map[string]any{
			"directive_text": "Corpus in Argentina",
			"corpus_path":    corpusPath,
			"sent_by":        "runtime-e2e-test",
		}),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish directive: %v", err)
	}

	campaignCompleted, err := waitForEventTypeTimeout(campaignCompletedCh, "campaign.completed", cannedE2EWaitTimeout)
	if err != nil {
		t.Fatalf("waiting for campaign.completed: %v", err)
	}

	campaignPayload := parsePayloadMap(campaignCompleted.Payload)
	campaignID := strings.TrimSpace(asString(campaignPayload["campaign_id"]))
	if campaignID == "" {
		t.Fatalf("campaign.completed missing campaign_id payload=%v", campaignPayload)
	}
	eventsAtCompletion := eventStore.SnapshotEvents()
	countsAtCompletion := countEventTypes(eventsAtCompletion)
	if got := asInt(campaignPayload["discoveries_count"]); got != 2 {
		rows, _ := db.QueryContext(ctx, `
			SELECT
				COALESCE(detail->>'skip_reason',''),
				COALESCE(detail->>'opportunity_name','')
			FROM runtime_log
			WHERE component = 'prefilter'
			  AND action = 'skipped'
			ORDER BY ts DESC
			LIMIT 10
		`)
		skips := make([]string, 0, 10)
		if rows != nil {
			defer rows.Close()
			for rows.Next() {
				var reason, name string
				if err := rows.Scan(&reason, &name); err == nil {
					skips = append(skips, strings.TrimSpace(name)+":"+strings.TrimSpace(reason))
				}
			}
		}
		t.Fatalf("campaign.completed discoveries_count mismatch: got=%d want=2 payload=%v counts=%v skips=%v", got, campaignPayload, countsAtCompletion, skips)
	}
	if got := runtimepipeline.NormalizeScanMode(strings.TrimSpace(asString(campaignPayload["completed_mode"]))); got != "corpus" {
		t.Fatalf("campaign.completed completed_mode mismatch: got=%q want=%q payload=%v", got, "corpus", campaignPayload)
	}
	assertScanCampaignState(t, db, campaignID, 2)

	if err := waitForEventTypeCount(eventStore, "score.dimension_complete", 22, cannedE2EWaitTimeout); err != nil {
		t.Fatalf("waiting for score.dimension_complete fanout: %v", err)
	}
	if err := waitForEventTypeCount(eventStore, "vertical.scored", 2, cannedE2EWaitTimeout); err != nil {
		t.Fatalf("waiting for vertical.scored fanout: %v", err)
	}
	if err := waitForEventTypeCount(eventStore, "vertical.shortlisted", 1, cannedE2EWaitTimeout); err != nil {
		t.Fatalf("waiting for vertical.shortlisted fanout: %v", err)
	}
	if err := waitForEventTypeCount(eventStore, "vertical.marginal", 1, cannedE2EWaitTimeout); err != nil {
		t.Fatalf("waiting for vertical.marginal fanout: %v", err)
	}
	waitForStageSignals(t, stageSignals, []string{"shortlisted", "marginal_review"}, cannedE2EWaitTimeout)
	var shortlistedCount, marginalCount int
	if err := db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE stage = 'shortlisted') AS shortlisted,
			COUNT(*) FILTER (WHERE stage = 'marginal_review') AS marginal
		FROM verticals
	`).Scan(&shortlistedCount, &marginalCount); err != nil {
		t.Fatalf("query expected vertical stages: %v", err)
	}
	if shortlistedCount != 1 || marginalCount != 1 {
		rows, _ := db.QueryContext(ctx, `
			SELECT COALESCE(name,''), COALESCE(stage,''), COALESCE(geography,''), COALESCE(mode,'')
			FROM verticals
			ORDER BY created_at ASC
		`)
		summary := make([]string, 0, 8)
		if rows != nil {
			defer rows.Close()
			for rows.Next() {
				var name, stage, geo, mode string
				if err := rows.Scan(&name, &stage, &geo, &mode); err == nil {
					summary = append(summary, fmt.Sprintf("%s|%s|%s|%s", strings.TrimSpace(name), strings.TrimSpace(stage), strings.TrimSpace(geo), strings.TrimSpace(mode)))
				}
			}
		}
		t.Fatalf("expected 1 shortlisted and 1 marginal_review vertical, got shortlisted=%d marginal=%d rows=%v", shortlistedCount, marginalCount, summary)
	}

	if got := canned.StartsFor("market-research-agent"); got < 1 {
		t.Fatalf("expected market-research-agent session start, got=%d", got)
	}
	if got := canned.StartsFor("analysis-agent"); got < 1 {
		t.Fatalf("expected analysis-agent session start, got=%d", got)
	}

	mraPrompt := canned.PromptFor("market-research-agent")
	if !strings.Contains(mraPrompt, "CORPUS MODE") {
		t.Fatalf("expected corpus prompt selection for market-research-agent, got prompt=%q", mraPrompt)
	}
	if !strings.Contains(mraPrompt, "PASSTHROUGH red flags") {
		t.Fatalf("expected calibrated corpus prompt content, got prompt=%q", mraPrompt)
	}

	allEvents := eventStore.SnapshotEvents()
	counts := countEventTypes(allEvents)
	expectedCounts := map[string]int{
		"scan.requested":                1,
		"market_research.scan_assigned": 1,
		"category.assessed":             3,
		"market_research.scan_complete": 1,
		"vertical.discovered":           2,
		"scoring.requested":             2,
		"score.dimension_complete":      22,
		"vertical.scored":               2,
		"vertical.shortlisted":          1,
		"vertical.marginal":             1,
		"scan.completed":                1,
		"campaign.completed":            1,
	}
	for eventType, want := range expectedCounts {
		if got := counts[eventType]; got != want {
			t.Fatalf("event count mismatch type=%s got=%d want=%d counts=%v", eventType, got, want, counts)
		}
	}

	var gotShortlisted, gotMarginal int
	for _, evt := range allEvents {
		if strings.TrimSpace(string(evt.Type)) != "vertical.scored" {
			continue
		}
		payload := parsePayloadMap(evt.Payload)
		switch strings.TrimSpace(asString(payload["result"])) {
		case "shortlisted":
			gotShortlisted++
		case "marginal":
			gotMarginal++
		}
	}
	if gotShortlisted != 1 || gotMarginal != 1 {
		t.Fatalf("expected 1 shortlisted and 1 marginal vertical.scored result, got shortlisted=%d marginal=%d", gotShortlisted, gotMarginal)
	}

	var scanRequested events.Event
	for _, evt := range allEvents {
		if strings.TrimSpace(string(evt.Type)) == "scan.requested" {
			scanRequested = evt
			break
		}
	}
	if strings.TrimSpace(scanRequested.ID) == "" {
		t.Fatalf("missing scan.requested event in snapshot")
	}
	scanPayload := parsePayloadMap(scanRequested.Payload)
	if got := runtimepipeline.NormalizeScanMode(strings.TrimSpace(asString(scanPayload["mode"]))); got != "corpus" {
		t.Fatalf("scan.requested mode mismatch: got=%q want=%q payload=%v", got, "corpus", scanPayload)
	}
	if got := strings.TrimSpace(asString(scanPayload["corpus_path"])); got != corpusPath {
		t.Fatalf("scan.requested corpus_path mismatch: got=%q payload=%v", got, scanPayload)
	}
}
