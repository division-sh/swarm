package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	"github.com/google/uuid"
)

const (
	RunForkPendingClassificationDeliveredCompleted = "delivered_completed"
	RunForkPendingClassificationPending            = "pending"
	RunForkPendingClassificationInProgress         = "in_progress"
	RunForkPendingClassificationFailedRetryable    = "failed_retryable"
	RunForkPendingClassificationFailedTerminal     = "failed_terminal"
	RunForkPendingClassificationDeadLetter         = "dead_letter"
	RunForkPendingClassificationCommittedReplay    = "committed_replay_scope"
)

type RunForkPlanRequest struct {
	SourceRunID string
	At          string
}

type RunForkPlan struct {
	SourceRunID               string                            `json:"source_run_id"`
	SourceRunStatus           string                            `json:"source_run_status,omitempty"`
	SourceRunStartedAt        *time.Time                        `json:"source_run_started_at,omitempty"`
	SourceRunEndedAt          *time.Time                        `json:"source_run_ended_at,omitempty"`
	ForkPoint                 RunForkPoint                      `json:"fork_point"`
	EventCountAtFork          int                               `json:"event_count_at_fork"`
	ReconstructedEntityCount  int                               `json:"reconstructed_entity_count"`
	PendingWorkCount          int                               `json:"pending_work_count"`
	UnsupportedBlockerCount   int                               `json:"unsupported_blocker_count"`
	ExecutionReady            bool                              `json:"execution_ready"`
	ReplayResumeAdmission     RunForkReplayResumeAdmission      `json:"replay_resume_admission"`
	ContractFrontierAdmission *RunForkContractFrontierAdmission `json:"contract_frontier_admission,omitempty"`
	SelectedContractExecution *RunForkSelectedContractExecution `json:"selected_contract_execution,omitempty"`
	SelectedContractReadiness *RunForkSelectedContractReadiness `json:"selected_contract_readiness,omitempty"`
	Entities                  []RunForkEntityState              `json:"entities,omitempty"`
	PendingWork               []RunForkPendingWork              `json:"pending_work,omitempty"`
	UnsupportedBlockers       []RunForkUnsupportedBlocker       `json:"unsupported_blockers,omitempty"`
	RouteHistory              RunForkRouteHistoryProjection     `json:"route_history"`
	historicalSnapshot        *runForkRevisionSnapshot
}

type RunForkPoint struct {
	Input          string    `json:"input"`
	EventID        string    `json:"event_id"`
	EventName      string    `json:"event_name,omitempty"`
	SourceEventID  string    `json:"source_event_id,omitempty"`
	ProducedBy     string    `json:"produced_by,omitempty"`
	ProducedByType string    `json:"produced_by_type,omitempty"`
	Timestamp      time.Time `json:"timestamp"`
	Revision       int64     `json:"revision"`
}

const (
	RunForkRouteHistoryNotApplicable      = "not_applicable"
	RunForkRouteHistoryUnknownUnversioned = "unknown_unversioned"
)

type RunForkRouteHistoryProjection struct {
	State string `json:"state"`
}

type RunForkEntityState struct {
	EntityID                string                                     `json:"entity_id"`
	CurrentState            string                                     `json:"current_state,omitempty"`
	EnteredStateAt          *time.Time                                 `json:"entered_state_at,omitempty"`
	Fields                  map[string]any                             `json:"fields,omitempty"`
	Gates                   map[string]any                             `json:"gates,omitempty"`
	Accumulator             map[string]any                             `json:"accumulator,omitempty"`
	MaterializationMetadata *RunForkMaterializedEntitySnapshotMetadata `json:"materialization_metadata,omitempty"`
}

type RunForkPendingWork struct {
	EventID         string               `json:"event_id"`
	EventName       string               `json:"event_name"`
	FlowInstance    string               `json:"flow_instance,omitempty"`
	SourceRoute     events.RouteIdentity `json:"source_route,omitempty"`
	DeliveryID      string               `json:"delivery_id,omitempty"`
	SubscriberType  string               `json:"subscriber_type,omitempty"`
	SubscriberID    string               `json:"subscriber_id,omitempty"`
	Classification  string               `json:"classification"`
	Status          string               `json:"status,omitempty"`
	RetryCount      int                  `json:"retry_count,omitempty"`
	ReasonCode      string               `json:"reason_code,omitempty"`
	ActiveSessionID string               `json:"active_session_id,omitempty"`
	CreatedAt       time.Time            `json:"created_at"`
	StartedAt       *time.Time           `json:"started_at,omitempty"`
	DeliveredAt     *time.Time           `json:"delivered_at,omitempty"`
	ReceiptOutcome  string               `json:"receipt_outcome,omitempty"`
	ReceiptAt       *time.Time           `json:"receipt_at,omitempty"`
}

type RunForkUnsupportedBlocker struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type runForkEventCursor struct {
	EventID        string
	EventName      string
	SourceEventID  string
	ProducedBy     string
	ProducedByType string
	CreatedAt      time.Time
	Revision       int64
}

type runForkAdmissionEvidence struct {
	Pending                 []RunForkPendingWork
	RelevantTimer           bool
	RelevantRoute           bool
	RouteHistory            RunForkRouteHistoryProjection
	ActiveSession           bool
	ActiveConversationAudit bool
	ActiveTurn              bool
	OpenReplyContext        bool
}

type runForkSourceFacts struct {
	EntityIDs     []string
	FlowInstances []string
	SourceFlows   []string
}

func RequireCanonicalRunForkPlannerCapabilities(caps StoreSchemaCapabilities, catalog schemaColumnCatalog) error {
	switch {
	case !caps.Events.HasRuns:
		return fmt.Errorf("run fork planner requires canonical runs table")
	case caps.Events.Log != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("events", caps.Events.Log)
	case !caps.Events.LogRunID:
		return fmt.Errorf("run fork planner requires canonical events.run_id")
	case caps.Events.Deliveries != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_deliveries", caps.Events.Deliveries)
	case !caps.Events.DeliveryRunID:
		return fmt.Errorf("run fork planner requires canonical event_deliveries.run_id")
	case caps.Events.Receipts != SchemaFlavorCanonical:
		return unsupportedSchemaCapability("event_receipts", caps.Events.Receipts)
	}
	required := map[string][]string{
		"runs":             {"run_id", "status"},
		"events":           {"event_id", "run_id", "event_name", "source_event_id", "produced_by", "produced_by_type", "created_at"},
		"event_deliveries": {"delivery_id", "run_id", "event_id", "subscriber_type", "subscriber_id", "status", "retry_count", "reason_code", "active_session_id", "started_at", "delivered_at", "created_at"},
		"event_receipts":   {"event_id", "subscriber_type", "subscriber_id", "outcome", "reason_code", "processed_at"},
		"dead_letters":     {"original_event_id", "handler_node", "created_at"},
		"entity_mutations": {"mutation_id", "run_id", "entity_id", "field", "new_value", "caused_by_event", "created_at"},
		"reply_contexts":   {"reply_context_id", "run_id", "request_event_id", "state"},
	}
	for tableName, columns := range required {
		if catalog.hasColumns(tableName, columns...) {
			continue
		}
		return fmt.Errorf("run fork planner requires %s columns %v", tableName, columns)
	}
	return nil
}

func (s *PostgresStore) requireRunForkPlannerCapabilities(ctx context.Context) (schemaColumnCatalog, error) {
	caps, err := s.schemaCapabilities(ctx)
	if err != nil {
		return schemaColumnCatalog{}, err
	}
	catalog, err := loadSchemaColumnCatalog(ctx, s.DB)
	if err != nil {
		return schemaColumnCatalog{}, err
	}
	return catalog, RequireCanonicalRunForkPlannerCapabilities(caps, catalog)
}

func (s *PostgresStore) PlanRunFork(ctx context.Context, req RunForkPlanRequest) (RunForkPlan, error) {
	if s == nil || s.DB == nil {
		return RunForkPlan{}, fmt.Errorf("postgres store is required")
	}
	_, err := s.requireRunForkPlannerCapabilities(ctx)
	if err != nil {
		return RunForkPlan{}, err
	}
	runID := strings.TrimSpace(req.SourceRunID)
	if runID == "" {
		return RunForkPlan{}, fmt.Errorf("source run_id is required")
	}
	if _, err := uuid.Parse(runID); err != nil {
		return RunForkPlan{}, fmt.Errorf("source run_id must be a UUID: %w", err)
	}
	at := strings.TrimSpace(req.At)

	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return RunForkPlan{}, fmt.Errorf("begin run fork revision snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	plan := RunForkPlan{SourceRunID: runID}
	if err := loadRunForkSourceSummary(ctx, tx, &plan); err != nil {
		return RunForkPlan{}, err
	}
	if err := runforkrevision.ValidateComplete(ctx, tx, runID); err != nil {
		return RunForkPlan{}, err
	}
	if at != "" {
		if _, err := uuid.Parse(at); err != nil {
			return RunForkPlan{}, fmt.Errorf("fork point --at must be an event UUID: %w", err)
		}
	}
	cursor, err := resolveRunForkRevisionPoint(ctx, tx, runID, at)
	if err != nil {
		return RunForkPlan{}, err
	}
	snapshot, err := loadRunForkRevisionSnapshot(ctx, tx, runID, cursor.Revision)
	if err != nil {
		return RunForkPlan{}, err
	}
	plan.historicalSnapshot = snapshot
	plan.ForkPoint = RunForkPoint{
		Input:          at,
		EventID:        cursor.EventID,
		EventName:      cursor.EventName,
		SourceEventID:  cursor.SourceEventID,
		ProducedBy:     cursor.ProducedBy,
		ProducedByType: cursor.ProducedByType,
		Timestamp:      cursor.CreatedAt,
		Revision:       cursor.Revision,
	}
	plan.EventCountAtFork = len(snapshot.Events)

	entities, err := loadRunForkEntityStates(snapshot)
	if err != nil {
		return RunForkPlan{}, err
	}
	entities, entitySnapshotMetadataAdmission, err := attachRunForkMaterializedEntitySnapshotMetadata(snapshot, entities)
	if err != nil {
		return RunForkPlan{}, err
	}
	plan.Entities = entities
	plan.ReconstructedEntityCount = len(entities)

	pending, err := loadRunForkPendingWorkFromRevision(snapshot)
	if err != nil {
		return RunForkPlan{}, err
	}
	plan.PendingWork = pending
	plan.PendingWorkCount = len(pending)
	evidence, err := loadRunForkAdmissionEvidenceFromRevision(snapshot, entities, pending)
	if err != nil {
		return RunForkPlan{}, err
	}
	plan.ReplayResumeAdmission = runForkReplayResumeAdmission(evidence)
	plan.RouteHistory = evidence.RouteHistory
	plan.ReplayResumeAdmission = runForkReplayResumeAdmissionWithMaterializedEntitySnapshotMetadata(plan.ReplayResumeAdmission, entitySnapshotMetadataAdmission)
	plan.UnsupportedBlockers = plan.ReplayResumeAdmission.UnsupportedBlockers
	plan.UnsupportedBlockerCount = len(plan.UnsupportedBlockers)
	plan.ExecutionReady = plan.ReplayResumeAdmission.StateOnlyExecutionReady || plan.ReplayResumeAdmission.DeliveryEventReplayReady
	return plan, nil
}

func loadRunForkSourceSummary(ctx context.Context, q rowQueryer, plan *RunForkPlan) error {
	var started, ended sql.NullTime
	if err := q.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), started_at, ended_at
		FROM runs
		WHERE run_id = $1::uuid
	`, plan.SourceRunID).Scan(&plan.SourceRunStatus, &started, &ended); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("source run %s not found", plan.SourceRunID)
		}
		return fmt.Errorf("load source run: %w", err)
	}
	if started.Valid {
		tm := started.Time
		plan.SourceRunStartedAt = &tm
	}
	if ended.Valid {
		tm := ended.Time
		plan.SourceRunEndedAt = &tm
	}
	return nil
}

func loadRunForkEntityStates(snapshot *runForkRevisionSnapshot) ([]RunForkEntityState, error) {
	type timedProjectionMutation struct {
		mutationlog.ProjectionMutation
		CreatedAt time.Time
	}
	grouped := map[string][]timedProjectionMutation{}
	entityOrder := []string{}
	seen := map[string]struct{}{}
	for _, fact := range snapshot.EntityMutations {
		entityID := strings.TrimSpace(fact.EntityID)
		field := strings.TrimSpace(fact.Field)
		var value any
		if err := json.Unmarshal(fact.NewValue, &value); err != nil {
			return nil, fmt.Errorf("decode fork entity mutation %s/%s: %w", entityID, field, err)
		}
		if _, ok := seen[entityID]; !ok {
			seen[entityID] = struct{}{}
			entityOrder = append(entityOrder, entityID)
		}
		grouped[entityID] = append(grouped[entityID], timedProjectionMutation{
			ProjectionMutation: mutationlog.ProjectionMutation{
				Field:    field,
				NewValue: value,
			},
			CreatedAt: fact.CreatedAt,
		})
	}

	out := make([]RunForkEntityState, 0, len(entityOrder))
	for _, entityID := range entityOrder {
		mutations := grouped[entityID]
		projectionMutations := make([]mutationlog.ProjectionMutation, 0, len(mutations))
		var enteredStateAt *time.Time
		for _, mutation := range mutations {
			projectionMutations = append(projectionMutations, mutation.ProjectionMutation)
			if strings.TrimSpace(mutation.Field) == "current_state" {
				tm := mutation.CreatedAt
				enteredStateAt = &tm
			}
		}
		projection, err := mutationlog.ReconstructEntityStateProjection(projectionMutations)
		if err != nil {
			return nil, fmt.Errorf("reconstruct entity %s at fork point: %w", entityID, err)
		}
		out = append(out, RunForkEntityState{
			EntityID:       entityID,
			CurrentState:   projection.CurrentState,
			EnteredStateAt: enteredStateAt,
			Fields:         projection.Fields,
			Gates:          projection.Gates,
			Accumulator:    projection.Accumulator,
		})
	}
	return out, nil
}

func classifyRunForkPendingWork(item RunForkPendingWork, deadLetter bool) string {
	if item.SubscriberType == replayScopeMarkerSubscriberType && item.SubscriberID == replayScopeMarkerSubscriberID {
		if _, ok := committedReplayScopeFromReasonCode(item.ReasonCode); ok {
			return RunForkPendingClassificationCommittedReplay
		}
	}
	if deadLetter || strings.TrimSpace(item.Status) == "dead_letter" || strings.TrimSpace(item.ReceiptOutcome) == "dead_letter" {
		return RunForkPendingClassificationDeadLetter
	}
	switch strings.TrimSpace(item.Status) {
	case "pending":
		return RunForkPendingClassificationPending
	case "in_progress":
		return RunForkPendingClassificationInProgress
	case "failed":
		if item.RetryCount < 2 {
			return RunForkPendingClassificationFailedRetryable
		}
		return RunForkPendingClassificationFailedTerminal
	case "delivered":
		return RunForkPendingClassificationDeliveredCompleted
	default:
		if item.ReceiptAt != nil {
			return RunForkPendingClassificationDeliveredCompleted
		}
		return RunForkPendingClassificationPending
	}
}

func runForkPendingReferencesActiveSession(pending []RunForkPendingWork) bool {
	for _, item := range pending {
		if strings.TrimSpace(item.ActiveSessionID) != "" {
			return true
		}
	}
	return false
}

func stringSetValues(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	return out
}
