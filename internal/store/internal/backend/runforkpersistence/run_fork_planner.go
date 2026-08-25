package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

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
	Pending                 []runfork.RunForkPendingWork
	RelevantTimer           bool
	RelevantRoute           bool
	RouteHistory            runfork.RunForkRouteHistoryProjection
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

type RunForkSourceFacts = runForkSourceFacts

func (s *RunForkPostgresOwner) PlanRunFork(ctx context.Context, req runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkPlan{}, fmt.Errorf("postgres store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runfork.RunForkPlan{}, err
	}
	runID := strings.TrimSpace(req.SourceRunID)
	if runID == "" {
		return runfork.RunForkPlan{}, fmt.Errorf("source run_id is required")
	}
	if _, err := uuid.Parse(runID); err != nil {
		return runfork.RunForkPlan{}, fmt.Errorf("source run_id must be a UUID: %w", err)
	}
	at := strings.TrimSpace(req.At)

	tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return runfork.RunForkPlan{}, fmt.Errorf("begin run fork revision snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	plan := runfork.RunForkPlan{SourceRunID: runID}
	if err := loadRunForkSourceSummary(ctx, tx, &plan); err != nil {
		return runfork.RunForkPlan{}, err
	}
	if err := runforkrevision.ValidateCompletePostgres(ctx, tx, runID); err != nil {
		return runfork.RunForkPlan{}, err
	}
	if at != "" {
		if _, err := uuid.Parse(at); err != nil {
			return runfork.RunForkPlan{}, fmt.Errorf("fork point --at must be an event UUID: %w", err)
		}
	}
	cursor, err := resolveRunForkRevisionPoint(ctx, tx, runID, at)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	snapshot, err := loadRunForkRevisionSnapshot(ctx, tx, runID, cursor.Revision)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	forkEvent, err := runForkPointRevisionEvent(snapshot, cursor)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	historicalEventIDs := make([]string, 0, len(snapshot.Events))
	for _, historicalEvent := range snapshot.Events {
		historicalEventIDs = append(historicalEventIDs, strings.TrimSpace(historicalEvent.EventID))
	}
	plan = plan.WithHistoricalEvents(snapshot.Revision, historicalEventIDs)
	plan.ForkPoint = runfork.RunForkPoint{
		Input:          at,
		EventID:        cursor.EventID,
		EventName:      cursor.EventName,
		SourceEventID:  cursor.SourceEventID,
		ProducedBy:     cursor.ProducedBy,
		ProducedByType: cursor.ProducedByType,
		RoutingSource:  forkEvent.RoutingSource,
		Timestamp:      cursor.CreatedAt,
		Revision:       cursor.Revision,
	}
	plan.EventCountAtFork = len(snapshot.Events)

	entities, err := loadRunForkEntityStates(snapshot)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	entities, entitySnapshotMetadataAdmission, err := attachRunForkMaterializedEntitySnapshotMetadata(snapshot, entities)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	plan.Entities = entities
	plan.ReconstructedEntityCount = len(entities)

	pending, err := loadRunForkPendingWorkFromRevision(snapshot)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	plan.PendingWork = pending
	plan.PendingWorkCount = len(pending)
	evidence, err := loadRunForkAdmissionEvidenceFromRevision(snapshot, entities, pending)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	plan.ReplayResumeAdmission = runForkReplayResumeAdmission(evidence)
	plan.RouteHistory = evidence.RouteHistory
	plan.ReplayResumeAdmission = runForkReplayResumeAdmissionWithMaterializedEntitySnapshotMetadata(plan.ReplayResumeAdmission, entitySnapshotMetadataAdmission)
	plan.UnsupportedBlockers = plan.ReplayResumeAdmission.UnsupportedBlockers
	plan.UnsupportedBlockerCount = len(plan.UnsupportedBlockers)
	plan.ExecutionReady = plan.ReplayResumeAdmission.StateOnlyExecutionReady || plan.ReplayResumeAdmission.DeliveryEventReplayReady
	return plan, nil
}

func runForkPointRevisionEvent(snapshot *runForkRevisionSnapshot, cursor runForkEventCursor) (runForkRevisionEvent, error) {
	if snapshot == nil || snapshot.Revision != cursor.Revision {
		return runForkRevisionEvent{}, fmt.Errorf("fork point event %s is not backed by fixed revision %d", strings.TrimSpace(cursor.EventID), cursor.Revision)
	}
	eventID := strings.TrimSpace(cursor.EventID)
	for _, event := range snapshot.Events {
		if strings.TrimSpace(event.EventID) != eventID {
			continue
		}
		if event.FirstRevision != cursor.Revision {
			return runForkRevisionEvent{}, fmt.Errorf("fork point event %s first revision is %d, want %d", eventID, event.FirstRevision, cursor.Revision)
		}
		return event, nil
	}
	return runForkRevisionEvent{}, fmt.Errorf("fork point event %s is absent from fixed revision %d", eventID, cursor.Revision)
}

func loadRunForkSourceSummary(ctx context.Context, q rowQueryer, plan *runfork.RunForkPlan) error {
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

func loadRunForkEntityStates(snapshot *runForkRevisionSnapshot) ([]runfork.RunForkEntityState, error) {
	type timedProjectionMutation struct {
		mutationlog.ProjectionMutation
		CreatedAt time.Time
	}
	grouped := map[string][]timedProjectionMutation{}
	entityOrder := []string{}
	seen := map[string]struct{}{}
	for _, fact := range snapshot.EntityMutations {
		entityID := strings.TrimSpace(fact.EntityID)
		domain := mutationlog.Domain(strings.TrimSpace(fact.Domain))
		path := strings.TrimSpace(fact.Path)
		var value any
		if err := json.Unmarshal(fact.NewValue, &value); err != nil {
			return nil, fmt.Errorf("decode fork entity mutation %s/%s/%s: %w", entityID, domain, path, err)
		}
		if _, ok := seen[entityID]; !ok {
			seen[entityID] = struct{}{}
			entityOrder = append(entityOrder, entityID)
		}
		grouped[entityID] = append(grouped[entityID], timedProjectionMutation{
			ProjectionMutation: mutationlog.ProjectionMutation{
				Domain:   domain,
				Path:     path,
				NewValue: value,
			},
			CreatedAt: fact.CreatedAt,
		})
	}

	out := make([]runfork.RunForkEntityState, 0, len(entityOrder))
	for _, entityID := range entityOrder {
		mutations := grouped[entityID]
		projectionMutations := make([]mutationlog.ProjectionMutation, 0, len(mutations))
		var enteredStateAt *time.Time
		for _, mutation := range mutations {
			projectionMutations = append(projectionMutations, mutation.ProjectionMutation)
			if mutation.Domain == mutationlog.DomainLifecycleState {
				tm := mutation.CreatedAt
				enteredStateAt = &tm
			}
		}
		projection, err := mutationlog.ReconstructEntityStateProjection(projectionMutations)
		if err != nil {
			return nil, fmt.Errorf("reconstruct entity %s at fork point: %w", entityID, err)
		}
		out = append(out, runfork.RunForkEntityState{
			EntityID:       entityID,
			CurrentState:   projection.CurrentState,
			EnteredStateAt: enteredStateAt,
			Fields:         projection.Fields,
			Bookkeeping:    projection.Bookkeeping,
			Gates:          projection.Gates,
			Accumulator:    projection.Accumulator,
		})
	}
	return out, nil
}

func runForkPendingReferencesActiveSession(pending []runfork.RunForkPendingWork) bool {
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
