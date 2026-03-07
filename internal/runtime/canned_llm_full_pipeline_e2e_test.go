package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	stdruntime "runtime"
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
	runtimetools "empireai/internal/runtime/tools"
	"empireai/internal/testutil"
	"github.com/google/uuid"
	"gopkg.in/yaml.v3"
)

type postgresEventStore struct {
	db          *sql.DB
	mu          sync.Mutex
	eventCounts map[string]int
	notifyCh    chan struct{}
}

func (s *postgresEventStore) AppendEvent(ctx context.Context, evt events.Event) error {
	if s == nil || s.db == nil {
		return errors.New("postgres event store db is nil")
	}
	id := strings.TrimSpace(evt.ID)
	if id == "" {
		id = uuid.NewString()
	}
	payload := evt.Payload
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	taskID := sanitizeOptionalUUIDForTest(evt.TaskID)
	verticalID := sanitizeOptionalUUIDForTest(evt.VerticalID)
	createdAt := evt.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO events (id, type, source_agent, task_id, vertical_id, payload, created_at)
		VALUES ($1::uuid, $2, $3, NULLIF($4,'')::uuid, NULLIF($5,'')::uuid, $6, $7)
		ON CONFLICT (id) DO NOTHING
	`, id, strings.TrimSpace(string(evt.Type)), strings.TrimSpace(evt.SourceAgent), taskID, verticalID, payload, createdAt)
	if err == nil {
		s.mu.Lock()
		s.ensureNotifyLocked()
		if s.eventCounts == nil {
			s.eventCounts = make(map[string]int)
		}
		s.eventCounts[strings.TrimSpace(string(evt.Type))]++
		s.signalLocked()
		s.mu.Unlock()
	}
	return err
}

func (s *postgresEventStore) ensureNotifyLocked() {
	if s.notifyCh == nil {
		s.notifyCh = make(chan struct{}, 1)
	}
}

func (s *postgresEventStore) signalLocked() {
	s.ensureNotifyLocked()
	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
}

func (s *postgresEventStore) WaitForEventTypeCount(eventType string, want int, timeout time.Duration) error {
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
			return fmt.Errorf("timed out waiting for events type=%s count>=%d", eventType, want)
		}
		select {
		case <-notifyCh:
		case <-time.After(remaining):
			return fmt.Errorf("timed out waiting for events type=%s count>=%d", eventType, want)
		}
	}
}

func (s *postgresEventStore) InsertEventDeliveries(ctx context.Context, eventID string, agentIDs []string) error {
	_ = ctx
	_ = eventID
	_ = agentIDs
	// Delivery persistence is not needed for this canned test and the fixture
	// agent set is in-memory only (no persisted agent rows for FK checks).
	return nil
}

func sanitizeOptionalUUIDForTest(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if _, err := uuid.Parse(raw); err != nil {
		return ""
	}
	return raw
}

type sqlMailboxStore struct {
	db       *sql.DB
	mu       sync.Mutex
	items    []MailboxItem
	notifyCh chan struct{}
}

func (s *sqlMailboxStore) InsertMailboxItem(ctx context.Context, item MailboxItem) (string, error) {
	if s == nil || s.db == nil {
		return "", errors.New("mailbox db is nil")
	}
	id := strings.TrimSpace(item.ID)
	if id == "" {
		id = uuid.NewString()
	}
	status := strings.TrimSpace(item.Status)
	if status == "" {
		status = "pending"
	}
	priority := strings.TrimSpace(item.Priority)
	if priority == "" {
		priority = "normal"
	}
	ctxPayload := item.Context
	if len(ctxPayload) == 0 {
		ctxPayload = []byte("{}")
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO mailbox (
			id, event_id, vertical_id, from_agent, type, priority, status, context, summary, timeout_at, decision, decision_notes, created_at
		) VALUES (
			$1::uuid,
			NULLIF($2,'')::uuid,
			NULLIF($3,'')::uuid,
			$4,
			$5,
			$6,
			$7,
			$8::jsonb,
			$9,
			NULLIF($10,'')::timestamptz,
			$11,
			$12,
			now()
		)
	`, id, strings.TrimSpace(item.EventID), sanitizeOptionalUUIDForTest(item.VerticalID), strings.TrimSpace(item.FromAgent), strings.TrimSpace(item.Type), priority, status, string(ctxPayload), strings.TrimSpace(item.Summary), "", strings.TrimSpace(item.Decision), strings.TrimSpace(item.DecisionNotes))
	if err != nil {
		return "", err
	}
	item.ID = id
	item.Status = status
	item.Priority = priority
	s.mu.Lock()
	s.ensureNotifyLocked()
	s.items = append(s.items, item)
	s.signalLocked()
	s.mu.Unlock()
	return id, nil
}

func (s *sqlMailboxStore) ensureNotifyLocked() {
	if s.notifyCh == nil {
		s.notifyCh = make(chan struct{}, 1)
	}
}

func (s *sqlMailboxStore) signalLocked() {
	s.ensureNotifyLocked()
	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
}

func (s *sqlMailboxStore) WaitForLatestMailboxItem(mailboxType string, timeout time.Duration) (mailboxID string, verticalID string, err error) {
	mailboxType = strings.TrimSpace(mailboxType)
	deadline := time.Now().Add(timeout)
	for {
		s.mu.Lock()
		s.ensureNotifyLocked()
		for i := len(s.items) - 1; i >= 0; i-- {
			item := s.items[i]
			if strings.TrimSpace(item.Type) == mailboxType && strings.TrimSpace(item.Status) == "pending" {
				s.mu.Unlock()
				return strings.TrimSpace(item.ID), strings.TrimSpace(item.VerticalID), nil
			}
		}
		notifyCh := s.notifyCh
		s.mu.Unlock()
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return "", "", sql.ErrNoRows
		}
		select {
		case <-notifyCh:
		case <-time.After(remaining):
			return "", "", sql.ErrNoRows
		}
	}
}

func (s *sqlMailboxStore) ListMailboxItems(context.Context, string, int) ([]MailboxItem, error) {
	return nil, nil
}

func (s *sqlMailboxStore) CountMailboxItems(context.Context, string) (int, error) {
	return 0, nil
}

func (s *sqlMailboxStore) GetMailboxItem(context.Context, string) (MailboxItem, error) {
	return MailboxItem{}, sql.ErrNoRows
}

func (s *sqlMailboxStore) DecideMailboxItem(context.Context, string, string, string, string) error {
	return nil
}

func (s *sqlMailboxStore) ExpireMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}

func (s *sqlMailboxStore) ListUnnotifiedCriticalMailboxItems(context.Context, int) ([]MailboxItem, error) {
	return nil, nil
}

func (s *sqlMailboxStore) MarkMailboxItemNotified(context.Context, string) error {
	return nil
}

type cannedRoleFixture struct {
	Role      string                  `yaml:"role"`
	Responses []cannedFixtureResponse `yaml:"responses"`
}

type cannedFixtureResponse struct {
	WhenEvent string                  `yaml:"when_event"`
	Message   string                  `yaml:"message"`
	ToolCalls []cannedFixtureToolCall `yaml:"tool_calls"`
}

type cannedFixtureToolCall struct {
	Name      string         `yaml:"name"`
	Arguments map[string]any `yaml:"arguments"`
}

type yamlCannedRuntime struct {
	mu       sync.Mutex
	fixtures map[string]cannedRoleFixture
	turns    map[string]int
	prompts  map[string]string
	starts   map[string]int
}

func newYAMLCannedRuntime(fixtures map[string]cannedRoleFixture) *yamlCannedRuntime {
	copyFixtures := make(map[string]cannedRoleFixture, len(fixtures))
	for role, fixture := range fixtures {
		copyFixtures[strings.TrimSpace(role)] = fixture
	}
	return &yamlCannedRuntime{
		fixtures: copyFixtures,
		turns:    map[string]int{},
		prompts:  map[string]string{},
		starts:   map[string]int{},
	}
}

func (r *yamlCannedRuntime) StartSession(_ context.Context, agentID string, systemPrompt string, tools []llm.ToolDefinition) (*llm.Session, error) {
	if err := validateToolDefinitionsDraft202012(tools); err != nil {
		return nil, fmt.Errorf("invalid tool schema for agent %s: %w", strings.TrimSpace(agentID), err)
	}
	agentID = strings.TrimSpace(agentID)
	r.mu.Lock()
	r.prompts[agentID] = strings.TrimSpace(systemPrompt)
	r.starts[agentID]++
	r.mu.Unlock()
	return &llm.Session{
		ID:               "sess-" + agentID + "-" + uuid.NewString(),
		AgentID:          agentID,
		RuntimeMode:      "canned-yaml",
		ConversationMode: "task",
	}, nil
}

func (r *yamlCannedRuntime) ContinueSession(_ context.Context, session *llm.Session, msg llm.Message) (*llm.Response, error) {
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
	fixture, ok := r.fixtures[agentID]
	if !ok {
		return &llm.Response{Message: llm.Message{Role: "assistant", Content: "noop"}}, nil
	}

	eventType := extractInboundEventType(msg.Content)
	r.mu.Lock()
	turn := r.turns[agentID]
	if turn >= len(fixture.Responses) {
		r.mu.Unlock()
		return &llm.Response{Message: llm.Message{Role: "assistant", Content: "done"}}, nil
	}
	respFixture := fixture.Responses[turn]
	r.turns[agentID] = turn + 1
	r.mu.Unlock()

	expectedEvent := strings.TrimSpace(respFixture.WhenEvent)
	if expectedEvent != "" && expectedEvent != eventType {
		return nil, fmt.Errorf("canned fixture mismatch agent=%s turn=%d expected_event=%s got_event=%s", agentID, turn, expectedEvent, eventType)
	}

	vars := extractTemplateVars(msg.Content, eventType)
	calls := make([]llm.ToolCall, 0, len(respFixture.ToolCalls))
	for _, tc := range respFixture.ToolCalls {
		args := applyTemplateVars(tc.Arguments, vars)
		argMap, _ := args.(map[string]any)
		if argMap == nil {
			argMap = map[string]any{}
		}
		calls = append(calls, llm.ToolCall{
			Name:      strings.TrimSpace(tc.Name),
			Arguments: argMap,
		})
	}
	content := strings.TrimSpace(respFixture.Message)
	if content == "" {
		content = "canned response"
	}
	return &llm.Response{Message: llm.Message{Role: "assistant", Content: content}, ToolCalls: calls}, nil
}

func (r *yamlCannedRuntime) StartsFor(agentID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.starts[strings.TrimSpace(agentID)]
}

func (r *yamlCannedRuntime) PromptFor(agentID string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.prompts[strings.TrimSpace(agentID)]
}

func loadCannedRoleFixtures(dir string) (map[string]cannedRoleFixture, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, errors.New("fixtures dir is required")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	fixtures := make(map[string]cannedRoleFixture, len(entries))
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(ent.Name()))
		if !strings.HasSuffix(name, ".yaml") && !strings.HasSuffix(name, ".yml") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, ent.Name()))
		if err != nil {
			return nil, fmt.Errorf("read fixture %s: %w", ent.Name(), err)
		}
		var fixture cannedRoleFixture
		if err := yaml.Unmarshal(raw, &fixture); err != nil {
			return nil, fmt.Errorf("parse fixture %s: %w", ent.Name(), err)
		}
		role := strings.TrimSpace(fixture.Role)
		if role == "" {
			return nil, fmt.Errorf("fixture %s missing role", ent.Name())
		}
		fixtures[role] = fixture
	}
	if len(fixtures) == 0 {
		return nil, fmt.Errorf("no fixtures found in %s", dir)
	}
	return fixtures, nil
}

func projectPathFromThisFile(parts ...string) string {
	_, thisFile, _, ok := stdruntime.Caller(0)
	if !ok {
		return filepath.Join(parts...)
	}
	base := filepath.Dir(thisFile)
	all := append([]string{base, "..", ".."}, parts...)
	return filepath.Clean(filepath.Join(all...))
}

var templateTokenPattern = regexp.MustCompile(`\{\{\s*([a-zA-Z0-9_]+)\s*\}\}`)

func applyTemplateVars(v any, vars map[string]any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = applyTemplateVars(val, vars)
		}
		return out
	case []any:
		out := make([]any, 0, len(t))
		for _, item := range t {
			out = append(out, applyTemplateVars(item, vars))
		}
		return out
	case string:
		trim := strings.TrimSpace(t)
		if m := templateTokenPattern.FindStringSubmatch(trim); len(m) == 2 && strings.TrimSpace(m[0]) == trim {
			if val, ok := vars[strings.TrimSpace(m[1])]; ok {
				return val
			}
			return ""
		}
		return templateTokenPattern.ReplaceAllStringFunc(t, func(token string) string {
			m := templateTokenPattern.FindStringSubmatch(token)
			if len(m) != 2 {
				return ""
			}
			val, ok := vars[strings.TrimSpace(m[1])]
			if !ok || val == nil {
				return ""
			}
			return fmt.Sprintf("%v", val)
		})
	default:
		return v
	}
}

func extractInboundEventType(input string) string {
	for _, line := range strings.Split(input, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "- type:") {
			continue
		}
		v := strings.TrimSpace(strings.TrimPrefix(line, "- type:"))
		return strings.Trim(v, `"`)
	}
	return strings.TrimSpace(extractMessageField(input, "type"))
}

func extractInboundPayloadMap(input string) map[string]any {
	for _, line := range strings.Split(input, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "- payload:") {
			continue
		}
		raw := strings.TrimSpace(strings.TrimPrefix(line, "- payload:"))
		if raw == "" {
			return map[string]any{}
		}
		out := map[string]any{}
		if err := json.Unmarshal([]byte(raw), &out); err == nil {
			return out
		}
		return map[string]any{}
	}
	return map[string]any{}
}

func extractTemplateVars(input, eventType string) map[string]any {
	vars := map[string]any{}
	eventType = strings.TrimSpace(eventType)
	if eventType != "" {
		vars["event_type"] = eventType
	}
	for _, key := range []string{"scan_id", "campaign_id", "mode", "geography", "vertical_id", "vertical_name", "task_id", "spec_version"} {
		if val := strings.TrimSpace(extractMessageField(input, key)); val != "" {
			vars[key] = val
		}
	}
	payload := extractInboundPayloadMap(input)
	for k, v := range payload {
		if _, exists := vars[k]; !exists {
			vars[k] = v
		}
	}
	if v := strings.TrimSpace(asString(payload["vertical_id"])); v != "" {
		vars["vertical_id"] = v
	}
	if v := strings.TrimSpace(asString(payload["scan_id"])); v != "" {
		vars["scan_id"] = v
	}
	if v := strings.TrimSpace(asString(payload["campaign_id"])); v != "" {
		vars["campaign_id"] = v
	}
	if v := strings.TrimSpace(asString(payload["mode"])); v != "" {
		vars["mode"] = v
	}
	if v := strings.TrimSpace(asString(payload["geography"])); v != "" {
		vars["geography"] = v
	}
	return vars
}

func waitForDBEventTypeCount(store *postgresEventStore, eventType string, want int, timeout time.Duration) error {
	return store.WaitForEventTypeCount(eventType, want, timeout)
}

func waitForPendingMailboxApproval(store *sqlMailboxStore, timeout time.Duration) (mailboxID string, verticalID string, err error) {
	return store.WaitForLatestMailboxItem("vertical_approval", timeout)
}

func dbEventTypeCounts(t *testing.T, db *sql.DB) map[string]int {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT type, COUNT(*) FROM events GROUP BY type`)
	if err != nil {
		t.Fatalf("query event counts: %v", err)
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var typ string
		var count int
		if err := rows.Scan(&typ, &count); err != nil {
			t.Fatalf("scan event counts: %v", err)
		}
		out[strings.TrimSpace(typ)] = count
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate event counts: %v", err)
	}
	return out
}

func recentEventTypes(t *testing.T, db *sql.DB, limit int) []string {
	t.Helper()
	if limit <= 0 {
		limit = 20
	}
	rows, err := db.QueryContext(context.Background(), `
		SELECT type
		FROM events
		ORDER BY created_at DESC, id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		t.Fatalf("query recent events: %v", err)
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var typ string
		if err := rows.Scan(&typ); err != nil {
			t.Fatalf("scan recent event type: %v", err)
		}
		out = append(out, strings.TrimSpace(typ))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate recent events: %v", err)
	}
	return out
}

func recentErrorReceipts(t *testing.T, db *sql.DB, limit int) []string {
	t.Helper()
	if limit <= 0 {
		limit = 20
	}
	rows, err := db.QueryContext(context.Background(), `
		SELECT COALESCE(agent_id,''), COALESCE(event_id::text,''), COALESCE(status,''), COALESCE(error,'')
		FROM event_receipts
		WHERE status = 'error'
		ORDER BY processed_at DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return []string{"event_receipts query failed: " + err.Error()}
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var agentID, eventID, status, errText string
		if err := rows.Scan(&agentID, &eventID, &status, &errText); err != nil {
			return []string{"event_receipts scan failed: " + err.Error()}
		}
		out = append(out, strings.TrimSpace(agentID)+"|"+strings.TrimSpace(eventID)+"|"+strings.TrimSpace(status)+"|"+strings.TrimSpace(errText))
	}
	return out
}

func recentRuntimeErrors(t *testing.T, db *sql.DB, limit int) []string {
	t.Helper()
	if limit <= 0 {
		limit = 20
	}
	rows, err := db.QueryContext(context.Background(), `
		SELECT COALESCE(level,''), COALESCE(component,''), COALESCE(action,''), COALESCE(error,''), COALESCE(detail::text,'')
		FROM runtime_log
		WHERE level IN ('error', 'warn')
		ORDER BY ts DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return []string{"runtime_log query failed: " + err.Error()}
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var level, component, action, errText, detail string
		if err := rows.Scan(&level, &component, &action, &errText, &detail); err != nil {
			return []string{"runtime_log scan failed: " + err.Error()}
		}
		out = append(out, strings.TrimSpace(level)+"|"+strings.TrimSpace(component)+"|"+strings.TrimSpace(action)+"|"+strings.TrimSpace(errText)+"|"+strings.TrimSpace(detail))
	}
	return out
}

func failedToolExecutions(t *testing.T, db *sql.DB, limit int) []string {
	t.Helper()
	if limit <= 0 {
		limit = 20
	}
	rows, err := db.QueryContext(context.Background(), `
		SELECT
			COALESCE(payload->>'agent_id',''),
			COALESCE(payload->>'tool_name',''),
			COALESCE(payload->>'error','')
		FROM events
		WHERE type = 'agent.tool_execution'
		  AND COALESCE(payload->>'ok','true') <> 'true'
		ORDER BY created_at DESC, id DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return []string{"failed tool query failed: " + err.Error()}
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var agentID, toolName, errText string
		if err := rows.Scan(&agentID, &toolName, &errText); err != nil {
			return []string{"failed tool scan failed: " + err.Error()}
		}
		out = append(out, strings.TrimSpace(agentID)+"|"+strings.TrimSpace(toolName)+"|"+strings.TrimSpace(errText))
	}
	return out
}

func latestEventPayload(t *testing.T, db *sql.DB, eventType string) map[string]any {
	t.Helper()
	var payload []byte
	if err := db.QueryRowContext(context.Background(), `
		SELECT payload
		FROM events
		WHERE type = $1
		ORDER BY created_at DESC, id DESC
		LIMIT 1
	`, strings.TrimSpace(eventType)).Scan(&payload); err != nil {
		t.Fatalf("load latest event payload type=%s: %v", eventType, err)
	}
	return parsePayloadMap(payload)
}

func latestEventForVertical(t *testing.T, db *sql.DB, eventType, verticalID string) map[string]any {
	t.Helper()
	var payload []byte
	if err := db.QueryRowContext(context.Background(), `
		SELECT payload
		FROM events
		WHERE type = $1 AND vertical_id = $2::uuid
		ORDER BY created_at DESC, id DESC
		LIMIT 1
	`, strings.TrimSpace(eventType), strings.TrimSpace(verticalID)).Scan(&payload); err != nil {
		t.Fatalf("load latest vertical event payload type=%s vertical=%s: %v", eventType, verticalID, err)
	}
	return parsePayloadMap(payload)
}

func eventTypesForVertical(t *testing.T, db *sql.DB, verticalID string) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `
		SELECT type
		FROM events
		WHERE vertical_id = $1::uuid
		ORDER BY created_at ASC, id ASC
	`, strings.TrimSpace(verticalID))
	if err != nil {
		t.Fatalf("query events for vertical %s: %v", verticalID, err)
	}
	defer rows.Close()
	out := make([]string, 0, 64)
	for rows.Next() {
		var typ string
		if err := rows.Scan(&typ); err != nil {
			t.Fatalf("scan event type: %v", err)
		}
		out = append(out, strings.TrimSpace(typ))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate event types: %v", err)
	}
	return out
}

func assertSubsequence(t *testing.T, got []string, want []string) {
	t.Helper()
	if len(want) == 0 {
		return
	}
	idx := 0
	for _, typ := range got {
		if typ == want[idx] {
			idx++
			if idx == len(want) {
				return
			}
		}
	}
	t.Fatalf("missing expected event subsequence\nwant=%v\ngot=%v", want, got)
}

func assertTransitionActionMinCount(t *testing.T, db *sql.DB, eventType, action string, wantMin int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM pipeline_transitions
		WHERE event_type = $1 AND action = $2
	`, strings.TrimSpace(eventType), strings.TrimSpace(action)).Scan(&got); err != nil {
		t.Fatalf("query pipeline_transitions for %s/%s: %v", eventType, action, err)
	}
	if got < wantMin {
		t.Fatalf("expected at least %d transitions for %s action=%s, got %d", wantMin, eventType, action, got)
	}
}

func assertTransitionEmitsEvent(t *testing.T, db *sql.DB, eventType, emitted string, wantMin int) {
	t.Helper()
	var got int
	if err := db.QueryRowContext(context.Background(), `
		SELECT COUNT(*)
		FROM pipeline_transitions
		WHERE event_type = $1 AND $2 = ANY(COALESCE(events_emitted, ARRAY[]::text[]))
	`, strings.TrimSpace(eventType), strings.TrimSpace(emitted)).Scan(&got); err != nil {
		t.Fatalf("query transition emitted for %s -> %s: %v", eventType, emitted, err)
	}
	if got < wantMin {
		t.Fatalf("expected at least %d transitions emitting %s from %s, got %d", wantMin, emitted, eventType, got)
	}
}

func TestCannedLLME2E_FullPipelineDirectiveToOpCo(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping canned full-pipeline e2e in -short mode")
	}
	_, db, _ := testutil.StartPostgres(t)
	eventStore := &postgresEventStore{db: db}
	bus := NewEventBus(eventStore)
	bus.SetRuntimeLogger(NewRuntimeLogger(db))
	pc := NewFactoryPipelineCoordinator(bus, db)
	bus.SetInterceptors(pc)

	fixturesDir := projectPathFromThisFile("contracts", "test-vectors", "e2e-happy-path")
	fixtures, err := loadCannedRoleFixtures(fixturesDir)
	if err != nil {
		t.Fatalf("load canned fixtures: %v", err)
	}
	canned := newYAMLCannedRuntime(fixtures)

	mailboxStore := &sqlMailboxStore{db: db}
	exec := runtimetools.NewExecutor(bus, nil, nil)
	exec.SetMailboxStore(mailboxStore)
	baseFactory := runtimeagents.NewLLMAgentFactory(canned, exec, exec.ToolDefinitions())
	factory := func(cfg models.AgentConfig) (Agent, error) {
		if strings.TrimSpace(extractSystemPromptForTest(cfg)) == "" {
			cfg.Config = withSystemPrompt(cfg.Config, "Canned runtime prompt for "+strings.TrimSpace(cfg.Role))
		}
		return baseFactory(cfg)
	}
	am := runtimemanager.NewAgentManager(bus, factory)

	spawn := func(id, role, mode string, subscriptions ...string) {
		t.Helper()
		if err := am.SpawnAgent(models.AgentConfig{
			ID:            strings.TrimSpace(id),
			Type:          "worker",
			Role:          strings.TrimSpace(role),
			Mode:          strings.TrimSpace(mode),
			Subscriptions: append([]string(nil), subscriptions...),
		}); err != nil {
			t.Fatalf("spawn agent %s (%s): %v", id, role, err)
		}
	}

	spawn("market-research-agent", "market-research-agent", "factory", "market_research.scan_assigned")
	spawn("analysis-agent", "analysis-agent", "factory", "scoring.requested")
	spawn("business-research-agent", "business-research-agent", "factory", "validation.started", "spec.draft_ready", "spec_review.passed")
	spawn("lightweight-spec-agent", "lightweight-spec-agent", "factory", "spec.requested")
	spawn("spec-reviewer", "spec-reviewer", "factory", "spec_review.requested")
	spawn("spec-auditor", "spec-auditor", "factory", "spec.validation_requested")
	spawn("factory-cto", "factory-cto", "factory", "cto.spec_review_requested")
	spawn("pre-brand-agent", "pre-brand-agent", "factory", "brand.requested")
	spawn("validation-coordinator", "validation-coordinator", "factory", "validation.package_ready")
	spawn("empire-coordinator", "empire-coordinator", "holding", "vertical.approved")

	scanMgr := runtimepipeline.NewScanCampaignManager(bus, &e2eCampaignStore{db: db}, newScanCampaignHooksForTest(), db)
	scoringNode := NewScoringNode(bus, pc, nil)
	if scoringNode == nil {
		t.Fatal("expected scoring node")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	am.Run(ctx)
	defer func() { _ = am.Shutdown() }()
	go scanMgr.Run(ctx)
	go scoringNode.Run(ctx)

	corpusPath := filepath.Join(t.TempDir(), "corpus-signals.jsonl")
	if err := os.WriteFile(corpusPath, []byte("{\"signal\":\"clinic billing automation\"}\n"), 0o600); err != nil {
		t.Fatalf("write corpus file: %v", err)
	}
	if err := bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("system.directive"),
		SourceAgent: "human",
		Payload: mustJSON(map[string]any{
			"directive_text": "Corpus in Argentina",
			"corpus_path":    corpusPath,
			"sent_by":        "canned-full-pipeline-e2e",
		}),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish system.directive: %v", err)
	}

	if err := waitForDBEventTypeCount(eventStore, "campaign.completed", 1, 12*time.Second); err != nil {
		t.Fatalf("wait campaign.completed: %v", err)
	}
	if err := waitForDBEventTypeCount(eventStore, "score.dimension_complete", 22, 12*time.Second); err != nil {
		t.Fatalf("wait score.dimension_complete: %v", err)
	}
	if err := waitForDBEventTypeCount(eventStore, "vertical.ready_for_review", 1, 12*time.Second); err != nil {
		starts := map[string]int{
			"market-research-agent":   canned.StartsFor("market-research-agent"),
			"analysis-agent":          canned.StartsFor("analysis-agent"),
			"business-research-agent": canned.StartsFor("business-research-agent"),
			"lightweight-spec-agent":  canned.StartsFor("lightweight-spec-agent"),
			"spec-reviewer":           canned.StartsFor("spec-reviewer"),
			"spec-auditor":            canned.StartsFor("spec-auditor"),
			"factory-cto":             canned.StartsFor("factory-cto"),
			"pre-brand-agent":         canned.StartsFor("pre-brand-agent"),
			"validation-coordinator":  canned.StartsFor("validation-coordinator"),
			"empire-coordinator":      canned.StartsFor("empire-coordinator"),
		}
		t.Fatalf(
			"wait vertical.ready_for_review: %v counts=%v recent=%v errors=%v runtime=%v failed_tools=%v starts=%v",
			err,
			dbEventTypeCounts(t, db),
			recentEventTypes(t, db, 40),
			recentErrorReceipts(t, db, 20),
			recentRuntimeErrors(t, db, 30),
			failedToolExecutions(t, db, 30),
			starts,
		)
	}

	pendingMailboxID, readyVerticalID, err := waitForPendingMailboxApproval(mailboxStore, 4*time.Second)
	if err != nil {
		t.Fatalf(
			"load pending mailbox item: %v counts=%v failed_tools=%v recent=%v",
			err,
			dbEventTypeCounts(t, db),
			failedToolExecutions(t, db, 20),
			recentEventTypes(t, db, 30),
		)
	}
	if strings.TrimSpace(readyVerticalID) == "" {
		t.Fatalf("pending mailbox item missing vertical_id id=%s", pendingMailboxID)
	}

	if _, err := db.ExecContext(ctx, `
		UPDATE mailbox
		SET status = 'approved', decision = 'approved', decision_notes = 'canned e2e approval'
		WHERE id = $1::uuid
	`, pendingMailboxID); err != nil {
		t.Fatalf("approve mailbox item: %v", err)
	}

	if err := bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("vertical.approved"),
		SourceAgent: "human",
		VerticalID:  readyVerticalID,
		Payload: mustJSON(map[string]any{
			"vertical_id":         readyVerticalID,
			"brand_choice":        "ReconcileFlow",
			"founder_directives":  "prioritize claim denial reduction; keep onboarding under 14 days",
			"mailbox_decision_id": pendingMailboxID,
		}),
		CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("publish vertical.approved: %v", err)
	}

	if err := waitForDBEventTypeCount(eventStore, "opco.spinup_requested", 1, 12*time.Second); err != nil {
		t.Fatalf("wait opco.spinup_requested: %v counts=%v failed_tools=%v starts=%v", err, dbEventTypeCounts(t, db), failedToolExecutions(t, db, 20), map[string]int{"empire-coordinator": canned.StartsFor("empire-coordinator")})
	}
	if err := waitForDBEventTypeCount(eventStore, "opco.ceo_ready", 1, 12*time.Second); err != nil {
		t.Fatalf("wait opco.ceo_ready: %v counts=%v failed_tools=%v starts=%v recent=%v", err, dbEventTypeCounts(t, db), failedToolExecutions(t, db, 20), map[string]int{"empire-coordinator": canned.StartsFor("empire-coordinator")}, recentEventTypes(t, db, 30))
	}

	counts := dbEventTypeCounts(t, db)
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
		"validation.started":            1,
		"brand.requested":               1,
		"research.completed":            1,
		"spec.requested":                1,
		"spec.draft_ready":              1,
		"spec_review.requested":         1,
		"spec_review.passed":            1,
		"spec.approved":                 1,
		"spec.validation_requested":     1,
		"spec.validation_passed":        1,
		"cto.spec_review_requested":     1,
		"cto.spec_approved":             1,
		"brand.candidates_ready":        1,
		"validation.package_ready":      1,
		"vertical.ready_for_review":     1,
		"vertical.approved":             1,
		"opco.spinup_requested":         1,
		"opco.ceo_ready":                1,
		"scan.completed":                1,
		"campaign.completed":            1,
	}
	for eventType, want := range expectedCounts {
		if got := counts[eventType]; got != want {
			t.Fatalf("event count mismatch type=%s got=%d want=%d counts=%v", eventType, got, want, counts)
		}
	}

	campaignPayload := latestEventPayload(t, db, "campaign.completed")
	campaignID := strings.TrimSpace(asString(campaignPayload["campaign_id"]))
	if campaignID == "" {
		t.Fatalf("campaign.completed missing campaign_id payload=%v", campaignPayload)
	}
	if got := asInt(campaignPayload["discoveries_count"]); got != 2 {
		t.Fatalf("campaign.completed discoveries_count mismatch got=%d payload=%v", got, campaignPayload)
	}
	assertScanCampaignState(t, db, campaignID, 2)

	rows, err := db.QueryContext(ctx, `
		SELECT payload
		FROM events
		WHERE type = 'vertical.scored'
		ORDER BY created_at ASC, id ASC
	`)
	if err != nil {
		t.Fatalf("query vertical.scored events: %v", err)
	}
	defer rows.Close()
	var shortlistedScore, marginalScore float64
	var shortlistedSeen, marginalSeen bool
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			t.Fatalf("scan vertical.scored payload: %v", err)
		}
		p := parsePayloadMap(payload)
		result := strings.TrimSpace(asString(p["result"]))
		score, _ := asFloat64(p["composite_score"])
		switch result {
		case "shortlisted":
			shortlistedSeen = true
			shortlistedScore = score
		case "marginal":
			marginalSeen = true
			marginalScore = score
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate vertical.scored events: %v", err)
	}
	if !shortlistedSeen || !marginalSeen {
		t.Fatalf("expected one shortlisted and one marginal scored event")
	}
	if math.Abs(shortlistedScore-78) > 0.001 {
		t.Fatalf("expected shortlisted composite_score=78, got %v", shortlistedScore)
	}
	if math.Abs(marginalScore-62) > 0.001 {
		t.Fatalf("expected marginal composite_score=62, got %v", marginalScore)
	}

	var shortlistedVerticalID string
	if err := db.QueryRowContext(ctx, `
		SELECT vertical_id::text
		FROM events
		WHERE type = 'vertical.shortlisted'
		ORDER BY created_at DESC, id DESC
		LIMIT 1
	`).Scan(&shortlistedVerticalID); err != nil {
		t.Fatalf("load shortlisted vertical_id: %v", err)
	}
	if strings.TrimSpace(shortlistedVerticalID) == "" {
		t.Fatal("shortlisted vertical_id is empty")
	}

	var stage string
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(stage,'') FROM verticals WHERE id = $1::uuid`, shortlistedVerticalID).Scan(&stage); err != nil {
		t.Fatalf("load final vertical stage: %v", err)
	}
	if strings.TrimSpace(stage) != "operating" {
		t.Fatalf("expected final stage operating, got %q", stage)
	}

	var status string
	var g1, g2, g3, g4 bool
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status,''), g1_research, g2_spec, g3_cto, g4_brand
		FROM validation_pipelines
		WHERE vertical_id = $1::uuid
	`, shortlistedVerticalID).Scan(&status, &g1, &g2, &g3, &g4); err != nil {
		t.Fatalf("query validation_pipelines: %v", err)
	}
	if strings.TrimSpace(status) != "approved" {
		t.Fatalf("expected validation status approved, got %q", status)
	}
	if !g1 || !g2 || !g3 || !g4 {
		t.Fatalf("expected validation gates all true, got g1=%v g2=%v g3=%v g4=%v", g1, g2, g3, g4)
	}

	assertTransitionActionMinCount(t, db, "scan.requested", "consumed", 1)
	assertTransitionActionMinCount(t, db, "category.assessed", "consumed", 3)
	assertTransitionActionMinCount(t, db, "vertical.shortlisted", "consumed", 1)
	assertTransitionActionMinCount(t, db, "research.completed", "consumed", 1)
	assertTransitionActionMinCount(t, db, "spec.approved", "consumed", 1)
	assertTransitionActionMinCount(t, db, "spec.validation_passed", "consumed", 1)
	assertTransitionActionMinCount(t, db, "cto.spec_approved", "consumed", 1)
	assertTransitionActionMinCount(t, db, "brand.candidates_ready", "consumed", 1)
	assertTransitionActionMinCount(t, db, "vertical.ready_for_review", "processed", 1)
	assertTransitionActionMinCount(t, db, "vertical.approved", "processed", 1)
	assertTransitionActionMinCount(t, db, "opco.ceo_ready", "processed", 1)

	assertTransitionEmitsEvent(t, db, "vertical.shortlisted", "validation.started", 1)
	assertTransitionEmitsEvent(t, db, "spec.approved", "spec.validation_requested", 1)
	assertTransitionEmitsEvent(t, db, "spec.validation_passed", "cto.spec_review_requested", 1)
	assertTransitionEmitsEvent(t, db, "cto.spec_approved", "validation.package_ready", 1)

	chain := eventTypesForVertical(t, db, shortlistedVerticalID)
	assertSubsequence(t, chain, []string{
		"vertical.discovered",
		"scoring.requested",
		"score.dimension_complete",
		"vertical.shortlisted",
		"validation.started",
		"research.completed",
		"brand.candidates_ready",
		"spec.requested",
		"spec.draft_ready",
		"spec_review.requested",
		"spec_review.passed",
		"spec.approved",
		"spec.validation_requested",
		"spec.validation_passed",
		"cto.spec_review_requested",
		"cto.spec_approved",
		"validation.package_ready",
		"vertical.ready_for_review",
		"vertical.approved",
		"opco.spinup_requested",
		"opco.ceo_ready",
	})

	readyPayload := latestEventForVertical(t, db, "opco.ceo_ready", shortlistedVerticalID)
	if got := asInt(readyPayload["agent_count"]); got != 13 {
		t.Fatalf("expected opco.ceo_ready agent_count=13, got %d payload=%v", got, readyPayload)
	}
	mandateMap, _ := readyPayload["mandate"].(map[string]any)
	if strings.TrimSpace(asString(mandateMap["vertical_id"])) != shortlistedVerticalID {
		t.Fatalf("expected opco mandate vertical_id=%s payload=%v", shortlistedVerticalID, readyPayload)
	}

	opcoRoles := []string{
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
	}
	for _, role := range opcoRoles {
		agentID := opCoAgentID(role, shortlistedVerticalID)
		if _, ok := am.GetAgentConfig(agentID); !ok {
			t.Fatalf("expected spawned opco agent config for %s", agentID)
		}
	}

	rt := bus.GetRoutingTable(shortlistedVerticalID)
	if rt == nil || len(rt.Routes) == 0 {
		t.Fatalf("expected bootstrap routing table for vertical %s", shortlistedVerticalID)
	}

	for _, agentID := range []string{
		"market-research-agent",
		"analysis-agent",
		"business-research-agent",
		"lightweight-spec-agent",
		"spec-reviewer",
		"spec-auditor",
		"factory-cto",
		"pre-brand-agent",
		"validation-coordinator",
		"empire-coordinator",
	} {
		if got := canned.StartsFor(agentID); got < 1 {
			t.Fatalf("expected session start for %s, got=%d", agentID, got)
		}
	}
	if prompt := canned.PromptFor("market-research-agent"); !strings.Contains(prompt, "CORPUS MODE") {
		t.Fatalf("expected corpus prompt for market-research-agent, got=%q", prompt)
	}
}
