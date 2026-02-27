package runtime

import (
	"context"
	"database/sql"
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
	scoringNodeID         = "scoring-node"
	scoringNodeRetryLimit = 5
)

// ScoringNode is a runtime-owned deterministic worker that handles scoring flow
// events via EventBus subscriptions instead of publish-path interception.
type ScoringNode struct {
	bus *EventBus
	pc  *FactoryPipelineCoordinator
	db  *sql.DB

	retryLimit int
	backoffFn  func(int) time.Duration

	overrideHandle func(context.Context, events.Event) error

	ledgerMu      sync.Mutex
	ledgerChecked bool
	ledgerEnabled bool
}

func NewScoringNode(bus *EventBus, pc *FactoryPipelineCoordinator, db *sql.DB) *ScoringNode {
	if bus == nil || pc == nil {
		return nil
	}
	return &ScoringNode{
		bus:        bus,
		pc:         pc,
		db:         db,
		retryLimit: scoringNodeRetryLimit,
		backoffFn:  panicBackoff,
	}
}

func (n *ScoringNode) Run(ctx context.Context) {
	if n == nil || n.bus == nil || n.pc == nil {
		return
	}
	ch := n.subscribe()
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				ch = n.subscribe()
				continue
			}
			n.processEvent(ctx, evt)
		}
	}
}

func (n *ScoringNode) subscribe() <-chan events.Event {
	if n == nil || n.bus == nil {
		return nil
	}
	return n.bus.Subscribe(scoringNodeID,
		events.EventType("vertical.discovered"),
		events.EventType("score.dimension_complete"),
		events.EventType("scoring.contest_resolved"),
	)
}

func (n *ScoringNode) processEvent(ctx context.Context, evt events.Event) {
	eventID := strings.TrimSpace(evt.ID)
	if eventID == "" {
		return
	}
	if n.isProcessed(ctx, eventID) {
		return
	}

	var lastErr error
	nodeCtx := withPipelineSourceAgent(ctx, scoringNodeID)
	retryLimit := n.retryLimit
	if retryLimit <= 0 {
		retryLimit = scoringNodeRetryLimit
	}
	backoffFn := n.backoffFn
	if backoffFn == nil {
		backoffFn = panicBackoff
	}
	for attempt := 1; attempt <= retryLimit; attempt++ {
		if err := n.handle(nodeCtx, evt); err == nil {
			n.markProcessed(nodeCtx, eventID)
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

func (n *ScoringNode) handle(ctx context.Context, evt events.Event) error {
	if n == nil || n.pc == nil {
		return errors.New("scoring node not initialized")
	}
	if n.overrideHandle != nil {
		return n.overrideHandle(ctx, evt)
	}
	switch strings.TrimSpace(string(evt.Type)) {
	case "vertical.discovered":
		n.pc.handleScoringRequested(ctx, evt)
		return nil
	case "score.dimension_complete":
		n.pc.handleScoreDimensionComplete(ctx, evt)
		return nil
	case "scoring.contest_resolved":
		n.pc.handleScoringContestResolved(ctx, evt)
		return nil
	default:
		return nil
	}
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
		"node_id":    scoringNodeID,
		"event_id":   strings.TrimSpace(evt.ID),
		"event_type": strings.TrimSpace(string(evt.Type)),
		"error":      msg,
		"retries":    maxInt(1, n.retryLimit),
	}
	if strings.TrimSpace(evt.VerticalID) != "" {
		payload["vertical_id"] = strings.TrimSpace(evt.VerticalID)
	}
	if err := n.bus.Publish(ctx, events.Event{
		ID:          uuid.NewString(),
		Type:        events.EventType("pipeline.dead_letter"),
		SourceAgent: scoringNodeID,
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
	err := dbQueryRowContext(ctx, n.db, `
		SELECT EXISTS(
			SELECT 1 FROM system_node_ledger
			WHERE event_id = $1::uuid AND node_id = $2
		)
	`, strings.TrimSpace(eventID), scoringNodeID).Scan(&ok)
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
	if _, err := dbExecContext(ctx, n.db, `
		INSERT INTO system_node_ledger (event_id, node_id, processed_at)
		VALUES ($1::uuid, $2, now())
		ON CONFLICT (event_id, node_id) DO NOTHING
	`, strings.TrimSpace(eventID), scoringNodeID); err != nil {
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
	if err := dbQueryRowContext(ctx, n.db, `SELECT to_regclass('public.system_node_ledger') IS NOT NULL`).Scan(&exists); err != nil {
		exists = false
	}
	n.ledgerChecked = true
	n.ledgerEnabled = exists
	return n.ledgerEnabled
}

func (n *ScoringNode) String() string {
	return fmt.Sprintf("%s", scoringNodeID)
}
