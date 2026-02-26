package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
)

const (
	defaultOpCoCycleLimit      = 5
	defaultOpCoCycleWindow     = 4 * time.Hour
	spendNeededCycleLimit      = 3
	spendNeededCycleWindow     = 1 * time.Hour
	defaultCycleEscalationRole = "opco_cto"
)

type opcoCycleCounter struct {
	VerticalID   string
	EventPattern string
	Count        int
	WindowStart  time.Time
	LastEmitter  string
	Emitters     map[string]struct{}
}

// OpCoCycleTracker detects repeated event loops per vertical and pattern.
// Counters are kept in-memory and synchronized to cycle_counters for recovery.
type OpCoCycleTracker struct {
	mu       sync.Mutex
	db       *sql.DB
	counters map[string]*opcoCycleCounter
}

func NewOpCoCycleTracker(db *sql.DB) *OpCoCycleTracker {
	return &OpCoCycleTracker{
		db:       db,
		counters: make(map[string]*opcoCycleCounter, 128),
	}
}

func (t *OpCoCycleTracker) Check(ctx context.Context, evt events.Event) (bool, *events.Event) {
	if t == nil || !shouldTrackOpCoCycle(evt) {
		return false, nil
	}
	if strings.TrimSpace(string(evt.Type)) == "devops.deploy_complete" {
		t.ResetVertical(ctx, strings.TrimSpace(evt.VerticalID))
		return false, nil
	}

	verticalID := strings.TrimSpace(evt.VerticalID)
	eventPattern := strings.TrimSpace(string(evt.Type))
	limit, window := cycleLimitsForEvent(eventPattern)
	key := cycleCounterKey(verticalID, eventPattern)
	now := time.Now().UTC()

	t.mu.Lock()
	counter := t.loadCounterLocked(ctx, key, verticalID, eventPattern)
	if counter.WindowStart.IsZero() || now.Sub(counter.WindowStart) >= window {
		counter.Count = 0
		counter.WindowStart = now
		counter.Emitters = map[string]struct{}{}
	}
	counter.Count++
	counter.LastEmitter = strings.TrimSpace(evt.SourceAgent)
	if counter.Emitters == nil {
		counter.Emitters = map[string]struct{}{}
	}
	if counter.LastEmitter != "" {
		counter.Emitters[counter.LastEmitter] = struct{}{}
	}
	count := counter.Count
	windowStart := counter.WindowStart
	emitters := mapKeys(counter.Emitters)
	t.persistCounterLocked(ctx, counter)
	t.mu.Unlock()

	if count < limit {
		return false, nil
	}

	escalationTarget := defaultCycleEscalationRole
	if containsCTOEmitter(emitters) {
		escalationTarget = "mailbox"
	}
	recommendation := fmt.Sprintf(
		"Detected %d %s events within %s. Human review recommended.",
		count,
		eventPattern,
		window.String(),
	)
	payload := map[string]any{
		"vertical_id":       verticalID,
		"event_pattern":     eventPattern,
		"count":             count,
		"agents_involved":   emitters,
		"window_start":      windowStart.Format(time.RFC3339),
		"recommendation":    recommendation,
		"escalation_target": escalationTarget,
	}
	escalation := &events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("cycle_limit_reached"),
		SourceAgent: "runtime",
		VerticalID:  verticalID,
		Payload:     mustJSON(payload),
		CreatedAt:   now,
	}
	return true, escalation
}

func (t *OpCoCycleTracker) HandleResetEvent(ctx context.Context, evt events.Event) {
	if t == nil || strings.TrimSpace(string(evt.Type)) != "cycle_reset" {
		return
	}
	verticalID := strings.TrimSpace(evt.VerticalID)
	eventPattern := ""
	if len(evt.Payload) > 0 {
		var payload map[string]any
		if err := json.Unmarshal(evt.Payload, &payload); err == nil {
			if v := strings.TrimSpace(asString(payload["vertical_id"])); v != "" {
				verticalID = v
			}
			eventPattern = strings.TrimSpace(asString(payload["event_pattern"]))
		}
	}
	if verticalID == "" || eventPattern == "" {
		return
	}
	t.ResetCounter(ctx, verticalID, eventPattern)
}

func (t *OpCoCycleTracker) ResetCounter(ctx context.Context, verticalID, eventPattern string) {
	if t == nil {
		return
	}
	verticalID = strings.TrimSpace(verticalID)
	eventPattern = strings.TrimSpace(eventPattern)
	if verticalID == "" || eventPattern == "" {
		return
	}
	key := cycleCounterKey(verticalID, eventPattern)

	t.mu.Lock()
	delete(t.counters, key)
	t.mu.Unlock()

	if t.db != nil {
		if _, err := dbExecContext(ctx, t.db, `
			DELETE FROM cycle_counters
			WHERE vertical_id = $1::uuid AND event_pattern = $2
		`, verticalID, eventPattern); err != nil {
			runtimeWarn("cycle-tracker", "failed to delete cycle counter vertical=%s pattern=%s: %v", verticalID, eventPattern, err)
		}
	}
}

func (t *OpCoCycleTracker) ResetVertical(ctx context.Context, verticalID string) {
	if t == nil {
		return
	}
	verticalID = strings.TrimSpace(verticalID)
	if verticalID == "" {
		return
	}
	prefix := verticalID + ":"
	t.mu.Lock()
	for k := range t.counters {
		if strings.HasPrefix(k, prefix) {
			delete(t.counters, k)
		}
	}
	t.mu.Unlock()
	if t.db != nil {
		if _, err := dbExecContext(ctx, t.db, `DELETE FROM cycle_counters WHERE vertical_id = $1::uuid`, verticalID); err != nil {
			runtimeWarn("cycle-tracker", "failed to clear cycle counters for vertical=%s: %v", verticalID, err)
		}
	}
}

func (t *OpCoCycleTracker) ResetAll(ctx context.Context) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.counters = make(map[string]*opcoCycleCounter, 128)
	t.mu.Unlock()
	if t.db != nil {
		if _, err := dbExecContext(ctx, t.db, `DELETE FROM cycle_counters`); err != nil {
			runtimeWarn("cycle-tracker", "failed to clear cycle counters table: %v", err)
		}
	}
}

func (t *OpCoCycleTracker) loadCounterLocked(ctx context.Context, key, verticalID, eventPattern string) *opcoCycleCounter {
	if c, ok := t.counters[key]; ok {
		return c
	}
	c := &opcoCycleCounter{
		VerticalID:   verticalID,
		EventPattern: eventPattern,
		WindowStart:  time.Now().UTC(),
		Emitters:     map[string]struct{}{},
	}
	if t.db != nil {
		var dbCount int
		var dbWindow time.Time
		var dbLast sql.NullString
		err := dbQueryRowContext(ctx, t.db, `
			SELECT count, window_start, COALESCE(last_emitter, '')
			FROM cycle_counters
			WHERE vertical_id = $1::uuid
			  AND event_pattern = $2
		`, verticalID, eventPattern).Scan(&dbCount, &dbWindow, &dbLast)
		if err == nil {
			c.Count = dbCount
			c.WindowStart = dbWindow.UTC()
			if dbLast.Valid {
				c.LastEmitter = strings.TrimSpace(dbLast.String)
				if c.LastEmitter != "" {
					c.Emitters[c.LastEmitter] = struct{}{}
				}
			}
		}
	}
	t.counters[key] = c
	return c
}

func (t *OpCoCycleTracker) persistCounterLocked(ctx context.Context, c *opcoCycleCounter) {
	if t == nil || t.db == nil || c == nil {
		return
	}
	if _, err := dbExecContext(ctx, t.db, `
		INSERT INTO cycle_counters (vertical_id, event_pattern, count, window_start, last_emitter, updated_at)
		VALUES ($1::uuid, $2, $3, $4, NULLIF($5,''), now())
		ON CONFLICT (vertical_id, event_pattern) DO UPDATE
		SET count = EXCLUDED.count,
		    window_start = EXCLUDED.window_start,
		    last_emitter = EXCLUDED.last_emitter,
		    updated_at = now()
	`, c.VerticalID, c.EventPattern, c.Count, c.WindowStart, c.LastEmitter); err != nil {
		runtimeWarn("cycle-tracker", "failed to persist counter vertical=%s pattern=%s: %v", c.VerticalID, c.EventPattern, err)
	}
}

func shouldTrackOpCoCycle(evt events.Event) bool {
	verticalID := strings.TrimSpace(evt.VerticalID)
	source := strings.TrimSpace(evt.SourceAgent)
	eventPattern := strings.TrimSpace(string(evt.Type))
	if verticalID == "" || source == "" || eventPattern == "" {
		return false
	}
	if eventPattern == "cycle_limit_reached" || eventPattern == "cycle_reset" {
		return false
	}
	// OpCo agents use role-<vertical_uuid> ids.
	if !strings.HasSuffix(source, "-"+verticalID) {
		return false
	}
	return true
}

func cycleLimitsForEvent(eventPattern string) (limit int, window time.Duration) {
	eventPattern = strings.TrimSpace(eventPattern)
	if eventPattern == "spend_needed" {
		return spendNeededCycleLimit, spendNeededCycleWindow
	}
	return defaultOpCoCycleLimit, defaultOpCoCycleWindow
}

func cycleCounterKey(verticalID, eventPattern string) string {
	return strings.TrimSpace(verticalID) + ":" + strings.TrimSpace(eventPattern)
}

func mapKeys(in map[string]struct{}) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, 0, len(in))
	for key := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func containsCTOEmitter(emitters []string) bool {
	for _, emitter := range emitters {
		if strings.HasPrefix(strings.TrimSpace(emitter), "cto-agent-") {
			return true
		}
	}
	return false
}
