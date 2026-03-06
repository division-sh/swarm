package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"empireai/internal/events"
	"github.com/google/uuid"
)

const (
	ScoringNodeID               = "scoring-node"
	DefaultScoringNodeRetryLimit = 5
)

type ScoringPublisher interface {
	Subscribe(agentID string, eventTypes ...events.EventType) <-chan events.Event
	Publish(ctx context.Context, evt events.Event) error
}

type ScoringCoordinator interface {
	OnVerticalDiscovered(ctx context.Context, evt events.Event)
	OnVerticalDerived(ctx context.Context, evt events.Event)
	OnScoreDimensionComplete(ctx context.Context, evt events.Event)
	OnScoringContestResolved(ctx context.Context, evt events.Event)
}

type ScoringNode struct {
	bus ScoringPublisher
	pc  ScoringCoordinator
	db  *sql.DB

	retryLimit int
	backoffFn  func(int) time.Duration

	overrideHandle func(context.Context, events.Event) error
	onSubscribe    func()

	ledgerMu      sync.Mutex
	ledgerChecked bool
	ledgerEnabled bool
}

func NewScoringNode(bus ScoringPublisher, pc ScoringCoordinator, db *sql.DB) *ScoringNode {
	if bus == nil || pc == nil {
		return nil
	}
	return &ScoringNode{
		bus:        bus,
		pc:         pc,
		db:         db,
		retryLimit: DefaultScoringNodeRetryLimit,
		backoffFn:  defaultPanicBackoff,
	}
}

func (n *ScoringNode) Run(ctx context.Context) {
	if n == nil || n.bus == nil || n.pc == nil {
		return
	}
	ch := n.subscribe()
	if n.onSubscribe != nil {
		n.onSubscribe()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				ch = n.subscribe()
				if n.onSubscribe != nil {
					n.onSubscribe()
				}
				continue
			}
			n.ProcessEventForTest(ctx, evt)
		}
	}
}

func (n *ScoringNode) ProcessEventForTest(ctx context.Context, evt events.Event) {
	eventID := strings.TrimSpace(evt.ID)
	if eventID == "" {
		return
	}
	if n.isProcessed(ctx, eventID) {
		return
	}
	var lastErr error
	retryLimit := n.retryLimit
	if retryLimit <= 0 {
		retryLimit = DefaultScoringNodeRetryLimit
	}
	backoffFn := n.backoffFn
	if backoffFn == nil {
		backoffFn = defaultPanicBackoff
	}
	for attempt := 1; attempt <= retryLimit; attempt++ {
		if err := n.handle(ctx, evt); err == nil {
			n.markProcessed(ctx, eventID)
			return
		} else {
			lastErr = err
		}
		if attempt >= retryLimit {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoffFn(attempt)):
		}
	}
	n.emitDeadLetter(ctx, evt, lastErr)
	n.markProcessed(ctx, eventID)
}

func (n *ScoringNode) SetRetryPolicyForTest(limit int, backoff func(int) time.Duration) {
	if n == nil {
		return
	}
	n.retryLimit = limit
	n.backoffFn = backoff
}

func (n *ScoringNode) SetOverrideHandleForTest(fn func(context.Context, events.Event) error) {
	if n == nil {
		return
	}
	n.overrideHandle = fn
}

func (n *ScoringNode) SetOnSubscribeForTest(fn func()) {
	if n == nil {
		return
	}
	n.onSubscribe = fn
}

func (n *ScoringNode) subscribe() <-chan events.Event {
	if n == nil || n.bus == nil {
		return nil
	}
	return n.bus.Subscribe(ScoringNodeID,
		events.EventType("vertical.discovered"),
		events.EventType("vertical.derived"),
		events.EventType("score.dimension_complete"),
		events.EventType("scoring.contest_resolved"),
	)
}

func (n *ScoringNode) handle(ctx context.Context, evt events.Event) error {
	if n == nil || n.pc == nil {
		return errors.New("scoring node not initialized")
	}
	if n.overrideHandle != nil {
		return n.overrideHandle(ctx, evt)
	}
	switch strings.TrimSpace(string(evt.Type)) {
	case "vertical.discovered":
		n.pc.OnVerticalDiscovered(ctx, evt)
	case "vertical.derived":
		n.pc.OnVerticalDerived(ctx, evt)
	case "score.dimension_complete":
		n.pc.OnScoreDimensionComplete(ctx, evt)
	case "scoring.contest_resolved":
		n.pc.OnScoringContestResolved(ctx, evt)
	}
	return nil
}

func (n *ScoringNode) emitDeadLetter(ctx context.Context, evt events.Event, cause error) {
	if n == nil || n.bus == nil {
		return
	}
	msg := "unknown error"
	if cause != nil {
		msg = strings.TrimSpace(cause.Error())
		if msg == "" {
			msg = "unknown error"
		}
	}
	payload := map[string]any{
		"node_id":     ScoringNodeID,
		"event_id":    strings.TrimSpace(evt.ID),
		"event_type":  strings.TrimSpace(string(evt.Type)),
		"last_error":  msg,
		"retry_count": maxInt(1, n.retryLimit),
	}
	if strings.TrimSpace(evt.VerticalID) != "" {
		payload["vertical_id"] = strings.TrimSpace(evt.VerticalID)
	}
	if err := n.bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("pipeline.dead_letter"),
		SourceAgent: ScoringNodeID,
		VerticalID:  strings.TrimSpace(evt.VerticalID),
		Payload:     mustJSON(payload),
		CreatedAt:   time.Now().UTC(),
	}); err != nil {
		log.Printf("scoring-node: emit dead letter failed event=%s err=%v", strings.TrimSpace(evt.ID), err)
	}
}

func (n *ScoringNode) isProcessed(ctx context.Context, eventID string) bool {
	if n == nil || n.db == nil || strings.TrimSpace(eventID) == "" {
		return false
	}
	if !n.ledgerAvailable(ctx) {
		return false
	}
	var ok bool
	err := n.db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM system_node_ledger
			WHERE event_id = $1::uuid AND node_id = $2
		)
	`, strings.TrimSpace(eventID), ScoringNodeID).Scan(&ok)
	if err != nil {
		return false
	}
	return ok
}

func (n *ScoringNode) markProcessed(ctx context.Context, eventID string) {
	if n == nil || n.db == nil || strings.TrimSpace(eventID) == "" {
		return
	}
	if !n.ledgerAvailable(ctx) {
		return
	}
	if _, err := n.db.ExecContext(ctx, `
		INSERT INTO system_node_ledger (event_id, node_id, processed_at)
		VALUES ($1::uuid, $2, now())
		ON CONFLICT (event_id, node_id) DO NOTHING
	`, strings.TrimSpace(eventID), ScoringNodeID); err != nil {
		log.Printf("scoring-node: mark processed failed event=%s err=%v", strings.TrimSpace(eventID), err)
	}
}

func (n *ScoringNode) ledgerAvailable(ctx context.Context) bool {
	if n == nil || n.db == nil {
		return false
	}
	n.ledgerMu.Lock()
	defer n.ledgerMu.Unlock()
	if n.ledgerChecked {
		return n.ledgerEnabled
	}
	var exists bool
	if err := n.db.QueryRowContext(ctx, `SELECT to_regclass('public.system_node_ledger') IS NOT NULL`).Scan(&exists); err != nil {
		exists = false
	}
	n.ledgerChecked = true
	n.ledgerEnabled = exists
	return n.ledgerEnabled
}

func (n *ScoringNode) String() string {
	return fmt.Sprintf("%s", ScoringNodeID)
}

func defaultPanicBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := time.Duration(attempt*attempt) * 50 * time.Millisecond
	if d > 2*time.Second {
		return 2 * time.Second
	}
	return d
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	if len(b) == 0 {
		return []byte("{}")
	}
	return b
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
