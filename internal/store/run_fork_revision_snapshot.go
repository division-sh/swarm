package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	"github.com/google/uuid"
)

type runForkRevisionedFact struct {
	FirstRevision int64
	Revision      int64
}

type runForkRevisionEvent struct {
	runForkRevisionedFact
	EventID        string               `json:"event_id"`
	EventName      string               `json:"event_name"`
	EntityID       string               `json:"entity_id"`
	FlowInstance   string               `json:"flow_instance"`
	SourceRoute    events.RouteIdentity `json:"source_route"`
	TargetRoute    json.RawMessage      `json:"target_route"`
	TargetSet      json.RawMessage      `json:"target_set"`
	Scope          string               `json:"scope"`
	Payload        json.RawMessage      `json:"payload"`
	ChainDepth     int                  `json:"chain_depth"`
	ProducedBy     string               `json:"produced_by"`
	ProducedByType string               `json:"produced_by_type"`
	HandlerNode    string               `json:"handler_node"`
	IdempotencyKey string               `json:"idempotency_key"`
	SourceEventID  string               `json:"source_event_id"`
	CreatedAt      time.Time            `json:"created_at"`
}

type runForkRevisionEntityMutation struct {
	runForkRevisionedFact
	MutationID    string          `json:"mutation_id"`
	EntityID      string          `json:"entity_id"`
	Field         string          `json:"field"`
	NewValue      json.RawMessage `json:"new_value"`
	CausedByEvent string          `json:"caused_by_event"`
	CreatedAt     time.Time       `json:"created_at"`
}

type runForkRevisionEntityMetadata struct {
	runForkRevisionedFact
	EntityID     string    `json:"entity_id"`
	FlowInstance string    `json:"flow_instance"`
	EntityType   string    `json:"entity_type"`
	CreatedAt    time.Time `json:"created_at"`
}

type runForkRevisionDelivery struct {
	runForkRevisionedFact
	DeliveryID                string          `json:"delivery_id"`
	EventID                   string          `json:"event_id"`
	SubscriberType            string          `json:"subscriber_type"`
	SubscriberID              string          `json:"subscriber_id"`
	DeliveryTargetRoute       json.RawMessage `json:"delivery_target_route"`
	DeliveryContext           json.RawMessage `json:"delivery_context"`
	DeliveryPayloadProjection json.RawMessage `json:"delivery_payload_projection"`
	Status                    string          `json:"status"`
	RetryCount                int             `json:"retry_count"`
	ReasonCode                string          `json:"reason_code"`
	ActiveSessionID           string          `json:"active_session_id"`
	StartedAt                 *time.Time      `json:"started_at"`
	DeliveredAt               *time.Time      `json:"delivered_at"`
	CreatedAt                 time.Time       `json:"created_at"`
}

type runForkRevisionReceipt struct {
	runForkRevisionedFact
	ReceiptID      string    `json:"receipt_id"`
	EventID        string    `json:"event_id"`
	SubscriberType string    `json:"subscriber_type"`
	SubscriberID   string    `json:"subscriber_id"`
	Outcome        string    `json:"outcome"`
	ReasonCode     string    `json:"reason_code"`
	ProcessedAt    time.Time `json:"processed_at"`
}

type runForkRevisionDeadLetter struct {
	runForkRevisionedFact
	DeadLetterID    string    `json:"dead_letter_id"`
	OriginalEventID string    `json:"original_event_id"`
	HandlerNode     string    `json:"handler_node"`
	CreatedAt       time.Time `json:"created_at"`
}

type runForkRevisionTimer struct {
	runForkRevisionedFact
	TimerID            string          `json:"timer_id"`
	TimerName          string          `json:"timer_name"`
	EntityID           string          `json:"entity_id"`
	FlowInstance       string          `json:"flow_instance"`
	FireEvent          string          `json:"fire_event"`
	FirePayload        json.RawMessage `json:"fire_payload"`
	FireAt             time.Time       `json:"fire_at"`
	Recurring          bool            `json:"recurring"`
	RecurrenceCron     string          `json:"recurrence_cron"`
	RecurrenceInterval string          `json:"recurrence_interval"`
	OwnerNode          string          `json:"owner_node"`
	OwnerAgent         string          `json:"owner_agent"`
	TaskType           string          `json:"task_type"`
	Status             string          `json:"status"`
	FiredAt            *time.Time      `json:"fired_at"`
	CreatedAt          time.Time       `json:"created_at"`
}

type runForkRevisionSession struct {
	runForkRevisionedFact
	SessionID    string     `json:"session_id"`
	Status       string     `json:"status"`
	CreatedAt    time.Time  `json:"created_at"`
	TerminatedAt *time.Time `json:"terminated_at"`
}

type runForkRevisionTurn struct {
	runForkRevisionedFact
	TurnID    string    `json:"turn_id"`
	SessionID string    `json:"session_id"`
	CreatedAt time.Time `json:"created_at"`
}

type runForkRevisionConversationAudit struct {
	runForkRevisionedFact
	SessionID string    `json:"session_id"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type runForkRevisionReplyContext struct {
	runForkRevisionedFact
	ReplyContextID string     `json:"reply_context_id"`
	RequestEventID string     `json:"request_event_id"`
	State          string     `json:"state"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	TerminalAt     *time.Time `json:"terminal_at"`
}

type runForkRevisionSnapshot struct {
	RunID              string
	Revision           int64
	Events             []runForkRevisionEvent
	EntityMutations    []runForkRevisionEntityMutation
	EntityMetadata     []runForkRevisionEntityMetadata
	Deliveries         []runForkRevisionDelivery
	Receipts           []runForkRevisionReceipt
	DeadLetters        []runForkRevisionDeadLetter
	Timers             []runForkRevisionTimer
	Sessions           []runForkRevisionSession
	Turns              []runForkRevisionTurn
	ConversationAudits []runForkRevisionConversationAudit
	ReplyContexts      []runForkRevisionReplyContext
}

func resolveRunForkRevisionPoint(ctx context.Context, tx *sql.Tx, runID, at string) (runForkEventCursor, error) {
	if tx == nil {
		return runForkEventCursor{}, fmt.Errorf("run fork revision point requires a database snapshot")
	}
	at = strings.TrimSpace(at)
	where := ""
	args := []any{runID}
	if at != "" {
		if _, err := uuid.Parse(at); err != nil {
			return runForkEventCursor{}, fmt.Errorf("run fork selector must be an event UUID: %w", err)
		}
		where = "AND fact_key = $2"
		args = append(args, at)
	}
	row := tx.QueryRowContext(ctx, `
		WITH first_events AS (
			SELECT DISTINCT ON (fact_key) fact_key, revision, fact
			FROM run_fork_fact_revisions
			WHERE run_id = $1::uuid AND family = 'events' `+where+`
			ORDER BY fact_key, revision ASC
		)
		SELECT
			fact_key,
			COALESCE(fact->>'event_name', ''),
			COALESCE(fact->>'source_event_id', ''),
			COALESCE(fact->>'produced_by', ''),
			COALESCE(fact->>'produced_by_type', ''),
			COALESCE((fact->>'created_at')::timestamptz, 'epoch'::timestamptz),
			revision
		FROM first_events
		ORDER BY revision DESC, fact_key DESC
		LIMIT 1
	`, args...)
	var cursor runForkEventCursor
	if err := row.Scan(&cursor.EventID, &cursor.EventName, &cursor.SourceEventID, &cursor.ProducedBy, &cursor.ProducedByType, &cursor.CreatedAt, &cursor.Revision); err != nil {
		if err == sql.ErrNoRows {
			if at == "" {
				return runForkEventCursor{}, fmt.Errorf("no revisioned source-run event exists for fork source run %s", runID)
			}
			return runForkEventCursor{}, fmt.Errorf("fork point event %s not found in revisioned source run %s", at, runID)
		}
		return runForkEventCursor{}, fmt.Errorf("resolve fork event revision: %w", err)
	}
	return cursor, nil
}

func loadRunForkRevisionSnapshot(ctx context.Context, tx *sql.Tx, runID string, revision int64) (*runForkRevisionSnapshot, error) {
	if tx == nil {
		return nil, fmt.Errorf("run fork revision snapshot requires a database transaction")
	}
	if revision <= 0 {
		return nil, fmt.Errorf("run fork revision must be positive")
	}
	rows, err := tx.QueryContext(ctx, `
		WITH bounded AS (
			SELECT family, fact_key, revision, fact, present,
			       MIN(revision) OVER (PARTITION BY family, fact_key) AS first_revision,
			       ROW_NUMBER() OVER (PARTITION BY family, fact_key ORDER BY revision DESC) AS latest_rank
			FROM run_fork_fact_revisions
			WHERE run_id = $1::uuid AND revision <= $2
		)
		SELECT family, first_revision, revision, fact
		FROM bounded
		WHERE latest_rank = 1 AND present
		ORDER BY family, first_revision, fact_key
	`, runID, revision)
	if err != nil {
		return nil, fmt.Errorf("load run fork revision snapshot: %w", err)
	}
	defer rows.Close()
	snapshot := &runForkRevisionSnapshot{RunID: runID, Revision: revision}
	for rows.Next() {
		var family string
		var firstRevision, factRevision int64
		var raw []byte
		if err := rows.Scan(&family, &firstRevision, &factRevision, &raw); err != nil {
			return nil, fmt.Errorf("scan run fork revision fact: %w", err)
		}
		stamp := runForkRevisionedFact{FirstRevision: firstRevision, Revision: factRevision}
		if err := appendRunForkRevisionFact(snapshot, runforkrevision.Family(family), stamp, raw); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read run fork revision snapshot: %w", err)
	}
	if len(snapshot.Events) == 0 {
		return nil, fmt.Errorf("run fork revision %d has no revisioned events", revision)
	}
	snapshot.sort()
	return snapshot, nil
}

func appendRunForkRevisionFact(snapshot *runForkRevisionSnapshot, family runforkrevision.Family, stamp runForkRevisionedFact, raw []byte) error {
	decode := func(target any) error {
		if err := json.Unmarshal(raw, target); err != nil {
			return fmt.Errorf("decode run fork %s revision fact: %w", family, err)
		}
		return nil
	}
	switch family {
	case runforkrevision.FamilyEvents:
		var fact runForkRevisionEvent
		if err := decode(&fact); err != nil {
			return err
		}
		fact.runForkRevisionedFact = stamp
		snapshot.Events = append(snapshot.Events, fact)
	case runforkrevision.FamilyEntityMutations:
		var fact runForkRevisionEntityMutation
		if err := decode(&fact); err != nil {
			return err
		}
		fact.runForkRevisionedFact = stamp
		snapshot.EntityMutations = append(snapshot.EntityMutations, fact)
	case runforkrevision.FamilyEntityMetadata:
		var fact runForkRevisionEntityMetadata
		if err := decode(&fact); err != nil {
			return err
		}
		fact.runForkRevisionedFact = stamp
		snapshot.EntityMetadata = append(snapshot.EntityMetadata, fact)
	case runforkrevision.FamilyEventDeliveries:
		var fact runForkRevisionDelivery
		if err := decode(&fact); err != nil {
			return err
		}
		fact.runForkRevisionedFact = stamp
		snapshot.Deliveries = append(snapshot.Deliveries, fact)
	case runforkrevision.FamilyEventReceipts:
		var fact runForkRevisionReceipt
		if err := decode(&fact); err != nil {
			return err
		}
		fact.runForkRevisionedFact = stamp
		snapshot.Receipts = append(snapshot.Receipts, fact)
	case runforkrevision.FamilyDeadLetters:
		var fact runForkRevisionDeadLetter
		if err := decode(&fact); err != nil {
			return err
		}
		fact.runForkRevisionedFact = stamp
		snapshot.DeadLetters = append(snapshot.DeadLetters, fact)
	case runforkrevision.FamilyTimers:
		var fact runForkRevisionTimer
		if err := decode(&fact); err != nil {
			return err
		}
		fact.runForkRevisionedFact = stamp
		snapshot.Timers = append(snapshot.Timers, fact)
	case runforkrevision.FamilyAgentSessions:
		var fact runForkRevisionSession
		if err := decode(&fact); err != nil {
			return err
		}
		fact.runForkRevisionedFact = stamp
		snapshot.Sessions = append(snapshot.Sessions, fact)
	case runforkrevision.FamilyAgentTurns:
		var fact runForkRevisionTurn
		if err := decode(&fact); err != nil {
			return err
		}
		fact.runForkRevisionedFact = stamp
		snapshot.Turns = append(snapshot.Turns, fact)
	case runforkrevision.FamilyAgentConversationAudits:
		var fact runForkRevisionConversationAudit
		if err := decode(&fact); err != nil {
			return err
		}
		fact.runForkRevisionedFact = stamp
		snapshot.ConversationAudits = append(snapshot.ConversationAudits, fact)
	case runforkrevision.FamilyReplyContexts:
		var fact runForkRevisionReplyContext
		if err := decode(&fact); err != nil {
			return err
		}
		fact.runForkRevisionedFact = stamp
		snapshot.ReplyContexts = append(snapshot.ReplyContexts, fact)
	default:
		return fmt.Errorf("run fork revision snapshot contains unsupported family %q", family)
	}
	return nil
}

func (s *runForkRevisionSnapshot) sort() {
	sort.Slice(s.Events, func(i, j int) bool {
		return revisionFactLess(s.Events[i].FirstRevision, s.Events[i].EventID, s.Events[j].FirstRevision, s.Events[j].EventID)
	})
	sort.Slice(s.EntityMutations, func(i, j int) bool {
		return revisionFactLess(s.EntityMutations[i].FirstRevision, s.EntityMutations[i].MutationID, s.EntityMutations[j].FirstRevision, s.EntityMutations[j].MutationID)
	})
	sort.Slice(s.Deliveries, func(i, j int) bool {
		return revisionFactLess(s.Deliveries[i].FirstRevision, s.Deliveries[i].DeliveryID, s.Deliveries[j].FirstRevision, s.Deliveries[j].DeliveryID)
	})
}

func revisionFactLess(leftRevision int64, leftID string, rightRevision int64, rightID string) bool {
	if leftRevision != rightRevision {
		return leftRevision < rightRevision
	}
	return strings.TrimSpace(leftID) < strings.TrimSpace(rightID)
}
