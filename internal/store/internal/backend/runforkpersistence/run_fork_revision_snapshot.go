package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

type runForkRevisionedFact struct {
	FirstRevision int64
	Revision      int64
}

type RunForkRevisionedFact = runForkRevisionedFact

type runForkRevisionEvent struct {
	runForkRevisionedFact
	EventID        string               `json:"event_id"`
	EventName      string               `json:"event_name"`
	EntityID       string               `json:"entity_id"`
	FlowInstance   string               `json:"flow_instance"`
	RoutingSource  events.RoutingSource `json:"routing_source"`
	TargetRoute    json.RawMessage      `json:"target_route"`
	TargetSet      json.RawMessage      `json:"target_set"`
	Scope          string               `json:"scope"`
	Payload        json.RawMessage      `json:"-"`
	ChainDepth     int                  `json:"chain_depth"`
	ProducedBy     string               `json:"produced_by"`
	ProducedByType string               `json:"produced_by_type"`
	HandlerNode    string               `json:"handler_node"`
	IdempotencyKey string               `json:"idempotency_key"`
	SourceEventID  string               `json:"source_event_id"`
	CreatedAt      time.Time            `json:"created_at"`
}

type RunForkRevisionEvent = runForkRevisionEvent

type runForkRevisionEntityMutation struct {
	runForkRevisionedFact
	MutationID    string          `json:"mutation_id"`
	EntityID      string          `json:"entity_id"`
	Domain        string          `json:"domain"`
	Path          string          `json:"path"`
	NewValue      json.RawMessage `json:"new_value"`
	CausedByEvent string          `json:"caused_by_event"`
	CreatedAt     time.Time       `json:"created_at"`
}

type runForkRevisionEntityMetadata struct {
	runForkRevisionedFact
	EntityID     string    `json:"entity_id"`
	FlowInstance string    `json:"flow_instance"`
	EntityType   string    `json:"entity_type"`
	Slug         string    `json:"slug"`
	Name         string    `json:"name"`
	CreatedAt    time.Time `json:"created_at"`
}

type runForkRevisionDelivery struct {
	runForkRevisionedFact
	Snapshot runtimedelivery.Snapshot
}

type RunForkRevisionDelivery = runForkRevisionDelivery

type runForkRevisionCommittedReplayScope struct {
	runForkRevisionedFact
	EventID   string    `json:"event_id"`
	RunID     string    `json:"run_id"`
	Scope     string    `json:"scope"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
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
	DeliveryID      string    `json:"delivery_id"`
	HandlerNode     string    `json:"handler_node"`
	CreatedAt       time.Time `json:"created_at"`
}

type runForkRevisionTimer struct {
	runForkRevisionedFact
	TimerID              string          `json:"timer_id"`
	TimerName            string          `json:"timer_name"`
	ScheduleScope        string          `json:"schedule_scope"`
	ScheduleKey          string          `json:"schedule_key"`
	ImmutableHash        string          `json:"immutable_hash"`
	RunID                string          `json:"run_id"`
	SourceTimerID        string          `json:"source_timer_id"`
	ForkedFromRunID      string          `json:"forked_from_run_id"`
	ForkedFromEventID    string          `json:"forked_from_event_id"`
	ReconstructionOwner  string          `json:"reconstruction_owner"`
	EntityID             string          `json:"entity_id"`
	FlowScopeKey         string          `json:"flow_scope_key"`
	FlowInstanceID       string          `json:"flow_instance_id"`
	FlowInstance         string          `json:"flow_instance"`
	FireEvent            string          `json:"fire_event"`
	FirePayload          json.RawMessage `json:"fire_payload"`
	RoutingSource        json.RawMessage `json:"routing_source"`
	ExecutionMode        string          `json:"execution_mode"`
	FireAt               time.Time       `json:"fire_at"`
	InitialFireAt        *time.Time      `json:"initial_fire_at"`
	Recurring            bool            `json:"recurring"`
	RecurrenceInterval   string          `json:"recurrence_interval"`
	OwnerNode            string          `json:"owner_node"`
	OwnerAgent           string          `json:"owner_agent"`
	OwnerKind            string          `json:"owner_kind"`
	AgentNameOwner       string          `json:"agent_name_owner"`
	AgentNameSource      string          `json:"agent_name_source"`
	AgentRoutePresence   string          `json:"agent_route_presence"`
	AgentFlowScopeKey    string          `json:"agent_flow_scope_key"`
	AgentFlowInstanceID  string          `json:"agent_flow_instance_id"`
	ReplyContextID       string          `json:"reply_context_id"`
	TaskID               string          `json:"task_id"`
	DueBasisKind         string          `json:"due_basis_kind"`
	DueBasisAbsolute     *time.Time      `json:"due_basis_absolute"`
	DueBasisDuration     string          `json:"due_basis_duration"`
	DueBasisCron         string          `json:"due_basis_cron"`
	OccurrenceEventID    string          `json:"occurrence_event_id"`
	OccurrenceAdmittedAt *time.Time      `json:"occurrence_admitted_at"`
	AcceptedAt           *time.Time      `json:"accepted_at"`
	CancelCause          string          `json:"cancel_cause"`
	CancelledAt          *time.Time      `json:"cancelled_at"`
	FailureCode          string          `json:"failure_code"`
	FailureMessage       string          `json:"failure_message"`
	FailedAt             *time.Time      `json:"failed_at"`
	TaskType             string          `json:"task_type"`
	Status               string          `json:"status"`
	FiredAt              *time.Time      `json:"fired_at"`
	CreatedAt            time.Time       `json:"created_at"`
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

type runForkRevisionFanOutFact struct {
	runForkRevisionedFact
	FactKind                 string          `json:"fact_kind"`
	TriggeringDeliveryID     string          `json:"triggering_delivery_id"`
	PackageKey               string          `json:"package_key"`
	ElementID                string          `json:"element_id"`
	BundleHash               string          `json:"bundle_hash"`
	SemanticDigest           string          `json:"semantic_digest"`
	SourceKind               string          `json:"source_kind"`
	SourceEventID            string          `json:"source_event_id"`
	SourceRunID              string          `json:"source_run_id"`
	SourceEntityID           string          `json:"source_entity_id"`
	SourceField              string          `json:"source_field"`
	SourceMutationID         string          `json:"source_mutation_id"`
	SourceResourcePackageKey string          `json:"source_resource_package_key"`
	SourceResourceEventName  string          `json:"source_resource_event_name"`
	SourceResourceVersionID  string          `json:"source_resource_version_id"`
	Cardinality              int             `json:"cardinality"`
	Cursor                   int             `json:"cursor"`
	Status                   string          `json:"status"`
	Capsule                  json.RawMessage `json:"capsule"`
	BlockedReason            string          `json:"blocked_reason"`
	CreatedAt                time.Time       `json:"created_at"`
	Ordinal                  *int            `json:"ordinal"`
	OutcomeKind              string          `json:"outcome_kind"`
	EventID                  string          `json:"event_id"`
	SourceOutcomeEventID     string          `json:"outcome_source_event_id"`
	Failure                  json.RawMessage `json:"failure"`
}

type runForkRevisionSnapshot struct {
	RunID                 string
	Revision              int64
	Events                []runForkRevisionEvent
	EntityMutations       []runForkRevisionEntityMutation
	EntityMetadata        []runForkRevisionEntityMetadata
	Deliveries            []runForkRevisionDelivery
	CommittedReplayScopes []runForkRevisionCommittedReplayScope
	Receipts              []runForkRevisionReceipt
	DeadLetters           []runForkRevisionDeadLetter
	Timers                []runForkRevisionTimer
	Sessions              []runForkRevisionSession
	Turns                 []runForkRevisionTurn
	ConversationAudits    []runForkRevisionConversationAudit
	ReplyContexts         []runForkRevisionReplyContext
	FanOutFacts           []runForkRevisionFanOutFact
}

type RunForkRevisionSnapshot = runForkRevisionSnapshot

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

func resolveSQLiteRunForkRevisionPoint(ctx context.Context, tx *sql.Tx, runID, at string) (runForkEventCursor, error) {
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
		WITH ranked_events AS (
			SELECT fact_key, revision, fact,
			       ROW_NUMBER() OVER (PARTITION BY fact_key ORDER BY revision ASC) AS first_rank
			FROM run_fork_fact_revisions
			WHERE run_id = $1 AND family = 'events' `+where+`
		)
		SELECT
			fact_key,
			COALESCE(CAST(json_extract(fact, '$.event_name') AS TEXT), ''),
			COALESCE(CAST(json_extract(fact, '$.source_event_id') AS TEXT), ''),
			COALESCE(CAST(json_extract(fact, '$.produced_by') AS TEXT), ''),
			COALESCE(CAST(json_extract(fact, '$.produced_by_type') AS TEXT), ''),
			COALESCE(CAST(json_extract(fact, '$.created_at') AS TEXT), ''),
			revision
		FROM ranked_events
		WHERE first_rank = 1
		ORDER BY revision DESC, fact_key DESC
		LIMIT 1
	`, args...)
	var cursor runForkEventCursor
	var createdAt string
	if err := row.Scan(&cursor.EventID, &cursor.EventName, &cursor.SourceEventID, &cursor.ProducedBy, &cursor.ProducedByType, &createdAt, &cursor.Revision); err != nil {
		if err == sql.ErrNoRows {
			if at == "" {
				return runForkEventCursor{}, fmt.Errorf("no revisioned source-run event exists for fork source run %s", runID)
			}
			return runForkEventCursor{}, fmt.Errorf("fork point event %s not found in revisioned source run %s", at, runID)
		}
		return runForkEventCursor{}, fmt.Errorf("resolve fork event revision: %w", err)
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(createdAt))
	if err != nil {
		return runForkEventCursor{}, fmt.Errorf("decode fork event revision timestamp: %w", err)
	}
	cursor.CreatedAt = parsed.UTC()
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
			WHERE run_id = $1 AND revision <= $2
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
		var persisted struct {
			runForkRevisionEvent
			PayloadBase64 string `json:"payload_base64"`
		}
		if err := decode(&persisted); err != nil {
			return err
		}
		payload, err := base64.StdEncoding.DecodeString(persisted.PayloadBase64)
		if err != nil {
			return fmt.Errorf("decode run fork events revision payload bytes: %w", err)
		}
		if !json.Valid(payload) {
			return fmt.Errorf("decode run fork events revision payload bytes: payload is not valid JSON")
		}
		fact := persisted.runForkRevisionEvent
		fact.Payload = json.RawMessage(payload)
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
		delivery, err := runtimedelivery.DecodeHistoricalSnapshot(raw)
		if err != nil {
			return err
		}
		fact := runForkRevisionDelivery{Snapshot: delivery}
		fact.runForkRevisionedFact = stamp
		snapshot.Deliveries = append(snapshot.Deliveries, fact)
	case runforkrevision.FamilyCommittedReplayScopes:
		var fact runForkRevisionCommittedReplayScope
		if err := decode(&fact); err != nil {
			return err
		}
		fact.runForkRevisionedFact = stamp
		snapshot.CommittedReplayScopes = append(snapshot.CommittedReplayScopes, fact)
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
	case runforkrevision.FamilyFanOutObligations:
		var fact runForkRevisionFanOutFact
		if err := decode(&fact); err != nil {
			return err
		}
		fact.runForkRevisionedFact = stamp
		snapshot.FanOutFacts = append(snapshot.FanOutFacts, fact)
	default:
		return fmt.Errorf("run fork revision snapshot contains unsupported family %q", family)
	}
	return nil
}

func AppendRunForkRevisionFact(snapshot *RunForkRevisionSnapshot, family runforkrevision.Family, stamp RunForkRevisionedFact, raw []byte) error {
	return appendRunForkRevisionFact(snapshot, family, stamp, raw)
}

func (s *runForkRevisionSnapshot) sort() {
	sort.Slice(s.Events, func(i, j int) bool {
		return revisionFactLess(s.Events[i].FirstRevision, s.Events[i].EventID, s.Events[j].FirstRevision, s.Events[j].EventID)
	})
	sort.Slice(s.EntityMutations, func(i, j int) bool {
		return revisionFactLess(s.EntityMutations[i].FirstRevision, s.EntityMutations[i].MutationID, s.EntityMutations[j].FirstRevision, s.EntityMutations[j].MutationID)
	})
	sort.Slice(s.Deliveries, func(i, j int) bool {
		return revisionFactLess(s.Deliveries[i].FirstRevision, s.Deliveries[i].Snapshot.DeliveryID, s.Deliveries[j].FirstRevision, s.Deliveries[j].Snapshot.DeliveryID)
	})
	sort.Slice(s.CommittedReplayScopes, func(i, j int) bool {
		return revisionFactLess(s.CommittedReplayScopes[i].FirstRevision, s.CommittedReplayScopes[i].EventID, s.CommittedReplayScopes[j].FirstRevision, s.CommittedReplayScopes[j].EventID)
	})
	sort.Slice(s.FanOutFacts, func(i, j int) bool {
		left := strings.Join([]string{s.FanOutFacts[i].TriggeringDeliveryID, s.FanOutFacts[i].PackageKey, s.FanOutFacts[i].ElementID, fmt.Sprint(outcomeOrdinal(s.FanOutFacts[i]))}, "|")
		right := strings.Join([]string{s.FanOutFacts[j].TriggeringDeliveryID, s.FanOutFacts[j].PackageKey, s.FanOutFacts[j].ElementID, fmt.Sprint(outcomeOrdinal(s.FanOutFacts[j]))}, "|")
		return revisionFactLess(s.FanOutFacts[i].FirstRevision, left, s.FanOutFacts[j].FirstRevision, right)
	})
}

func revisionFactLess(leftRevision int64, leftID string, rightRevision int64, rightID string) bool {
	if leftRevision != rightRevision {
		return leftRevision < rightRevision
	}
	return strings.TrimSpace(leftID) < strings.TrimSpace(rightID)
}
