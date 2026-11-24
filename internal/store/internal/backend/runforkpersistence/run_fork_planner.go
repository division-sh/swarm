package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	lifecycleowner "github.com/division-sh/swarm/internal/store/internal/backend/runlifecycle"
	storedurabledata "github.com/division-sh/swarm/internal/store/internal/durabledata"
	"github.com/google/uuid"
)

type runForkEventCursor struct {
	Kind           runfork.RunForkPointKind
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
	TimerHistory            runForkTimerHistoryInventory
	JoinSchedules           []genericschedule.Activation
	OneShotSchedules        []genericschedule.Activation
	TransferredJoins        []genericschedule.TransferredJoinOccurrence
	RelevantRoute           bool
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
	tx, err := s.backend.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return runfork.RunForkPlan{}, fmt.Errorf("begin run fork revision snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	return planRunForkSnapshotWithActivityOrigins(ctx, tx, req, runforkrevision.ValidateCompletePostgres, resolveRunForkRevisionPoint, s.ReadPresentSourceTx, true, s.durableData)
}

func (s *RunForkSQLiteOwner) PlanRunFork(ctx context.Context, req runfork.RunForkPlanRequest) (runfork.RunForkPlan, error) {
	if s == nil || s.backend == nil {
		return runfork.RunForkPlan{}, fmt.Errorf("sqlite store is required")
	}
	if err := s.requireCurrentSchema(); err != nil {
		return runfork.RunForkPlan{}, err
	}
	var plan runfork.RunForkPlan
	err := s.backend.RunReadTransaction(ctx, func(txctx context.Context, tx *sql.Tx) error {
		var err error
		plan, err = planRunForkSnapshotWithActivityOrigins(txctx, tx, req, runforkrevision.ValidateCompleteSQLite, resolveSQLiteRunForkRevisionPoint, s.ReadPresentSourceTx, false, s.durableData)
		return err
	})
	return plan, err
}

type runForkRevisionValidator func(context.Context, *sql.Tx, string) error
type runForkRevisionPointResolver func(context.Context, *sql.Tx, string, string) (runForkEventCursor, error)

func planRunForkSnapshot(
	ctx context.Context,
	tx *sql.Tx,
	req runfork.RunForkPlanRequest,
	validate runForkRevisionValidator,
	resolve runForkRevisionPointResolver,
	postgres bool,
	data ...*storedurabledata.Owner,
) (runfork.RunForkPlan, error) {
	return planRunForkSnapshotWithActivityOrigins(ctx, tx, req, validate, resolve, nil, postgres, data...)
}

// Readback-only callers retain their ordinary plan. Preparation additionally
// requires native source references; absent references never admit an activity.
func planRunForkSnapshotWithActivityOrigins(
	ctx context.Context,
	tx *sql.Tx,
	req runfork.RunForkPlanRequest,
	validate runForkRevisionValidator,
	resolve runForkRevisionPointResolver,
	sourceAt func(context.Context, *sql.Tx, string) (correlation.SourceArtifactFact, error),
	postgres bool,
	data ...*storedurabledata.Owner,
) (runfork.RunForkPlan, error) {
	runID := strings.TrimSpace(req.SourceRunID)
	if runID == "" {
		return runfork.RunForkPlan{}, fmt.Errorf("source run_id is required")
	}
	if _, err := uuid.Parse(runID); err != nil {
		return runfork.RunForkPlan{}, fmt.Errorf("source run_id must be a UUID: %w", err)
	}
	at := strings.TrimSpace(req.At)
	if req.AtStart && at != "" {
		return runfork.RunForkPlan{}, fmt.Errorf("start and event fork selectors are mutually exclusive")
	}
	if req.PublicationID != "" {
		if req.AtStart || at != "" && at != req.PublicationID || len(data) != 1 {
			return runfork.RunForkPlan{}, fmt.Errorf("publication cut requires its exact data owner and no other selector")
		}
		point, err := resolveRunForkPublicationPoint(ctx, tx, data[0], runID, req.PublicationID, resolve)
		if err != nil {
			return runfork.RunForkPlan{}, err
		}
		if req.ResolvedPoint != nil && !req.ResolvedPoint.SameIdentity(point) {
			return runfork.RunForkPlan{}, fmt.Errorf("publication differs from its retained fork cut")
		}
		req.ResolvedPoint = &point
		at = point.Input
	}
	if req.ResolvedPoint != nil {
		if err := req.ResolvedPoint.Validate(); err != nil {
			return runfork.RunForkPlan{}, fmt.Errorf("fixed fork point: %w", err)
		}
		if at != "" && at != req.ResolvedPoint.Input {
			return runfork.RunForkPlan{}, fmt.Errorf("fork selector differs from fixed revision point")
		}
		if req.AtStart != (req.ResolvedPoint.Kind == runfork.RunForkPointRunStart) {
			return runfork.RunForkPlan{}, fmt.Errorf("fork start selector differs from fixed revision point")
		}
		at = req.ResolvedPoint.Input
	}

	plan := runfork.RunForkPlan{SourceRunID: runID}
	if err := loadRunForkSourceSummary(ctx, tx, &plan); err != nil {
		return runfork.RunForkPlan{}, err
	}
	if validate == nil {
		return runfork.RunForkPlan{}, fmt.Errorf("run fork revision validator is required")
	}
	if err := validate(ctx, tx, runID); err != nil {
		return runfork.RunForkPlan{}, err
	}
	if at != "" {
		if _, err := uuid.Parse(at); err != nil {
			return runfork.RunForkPlan{}, fmt.Errorf("fork point --at must be an event UUID: %w", err)
		}
	}
	if resolve == nil {
		return runfork.RunForkPlan{}, fmt.Errorf("run fork revision point resolver is required")
	}
	var cursor runForkEventCursor
	var err error
	if req.ResolvedPoint != nil {
		cursor, err = resolveFixedRunForkRevisionPoint(ctx, tx, runID, *req.ResolvedPoint, resolve)
	} else if req.AtStart {
		cursor, err = resolveRunForkStartPoint(ctx, tx, runID)
	} else {
		cursor, err = resolve(ctx, tx, runID, at)
	}
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	snapshot, err := loadRunForkPointSnapshot(ctx, tx, runID, runfork.RunForkPoint{
		Kind: cursor.Kind, EventID: cursor.EventID, Revision: cursor.Revision,
	})
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	var forkEvent runForkRevisionEvent
	if cursor.Kind == runfork.RunForkPointEvent {
		forkEvent, err = runForkPointRevisionEvent(snapshot, cursor)
		if err != nil {
			return runfork.RunForkPlan{}, err
		}
	} else if cursor.Kind == runfork.RunForkPointDeploymentRevision {
		found := false
		for _, fact := range snapshot.FanOutFacts {
			if fact.FactKind == "intent" && fact.OriginKind == "deployment" {
				found = true
				break
			}
		}
		if !found {
			return runfork.RunForkPlan{}, fmt.Errorf("deployment fork revision %d has no admitted deployment feed", cursor.Revision)
		}
	} else if cursor.Kind == runfork.RunForkPointRunStart {
		plan.StartFirstTurn, err = loadRunForkStartFirstTurn(ctx, tx, snapshot)
		if err != nil {
			return runfork.RunForkPlan{}, err
		}
	} else if cursor.Kind != runfork.RunForkPointStateRevision {
		return runfork.RunForkPlan{}, fmt.Errorf("unsupported fork revision point kind %q", cursor.Kind)
	}
	historicalEventIDs := make([]string, 0, len(snapshot.Events))
	for _, historicalEvent := range snapshot.Events {
		historicalEventIDs = append(historicalEventIDs, strings.TrimSpace(historicalEvent.EventID))
	}
	plan = plan.WithHistoricalEvents(snapshot.Revision, historicalEventIDs)
	inputs, err := historicalInputPublications(snapshot)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	plan, err = plan.WithHistoricalInputPublications(snapshot.Revision, inputs)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	activityAdmission := runForkSourceStateAdmission{snapshot: snapshot, sourceAt: sourceAt, postgres: postgres}
	origins, err := activityAdmission.activityRequestOriginsAtCut(ctx, tx)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	plan, err = plan.WithActivityRequestOrigins(snapshot.Revision, origins)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	plan.ForkPoint = runfork.RunForkPoint{
		Kind:           cursor.Kind,
		Input:          at,
		EventID:        cursor.EventID,
		EventName:      cursor.EventName,
		SourceEventID:  cursor.SourceEventID,
		ProducedBy:     cursor.ProducedBy,
		ProducedByType: cursor.ProducedByType,
		RoutingSource:  forkEvent.RoutingSource,
		Timestamp:      cursor.CreatedAt.UTC(),
		Revision:       cursor.Revision,
	}
	if err := plan.ForkPoint.Validate(); err != nil {
		return runfork.RunForkPlan{}, err
	}
	if req.ResolvedPoint != nil && plan.ForkPoint != *req.ResolvedPoint {
		return runfork.RunForkPlan{}, fmt.Errorf("fixed fork point contradicts its historical revision")
	}
	selectors, err := historicalRouteSelectorPublications(snapshot)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	plan, err = plan.WithHistoricalRouteSelectors(snapshot.Revision, selectors)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	plan.EventCountAtFork = len(snapshot.Events)
	for _, reply := range snapshot.ReplyContexts {
		plan.ReplyContexts = append(plan.ReplyContexts, reply.Record)
	}
	returns, err := historicalReplyReturnPublications(ctx, tx, snapshot)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	plan, err = plan.WithHistoricalReplyReturns(snapshot.Revision, returns)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	for _, timer := range snapshot.Timers {
		if timer.TaskType != "workflow_timer" {
			continue
		}
		activation, err := workflowTimerActivationFromSnapshot(timer.TimerSnapshot)
		if err != nil {
			return runfork.RunForkPlan{}, err
		}
		if activation.RunID != runID {
			return runfork.RunForkPlan{}, fmt.Errorf("workflow timer disagrees with fixed-cut run")
		}
		plan.WorkflowTimers = append(plan.WorkflowTimers, activation.PersistenceRecord())
	}

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
	plan, err = retainInheritedCreatingWitness(plan, snapshot)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}

	pending, err := loadRunForkPendingWorkFromRevision(snapshot)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	plan.PendingWork = pending
	plan.PendingWorkCount = len(pending)
	plan = retainPendingProducerWitnessReaders(plan, snapshot)
	requests, err := requestPendingPublications(ctx, tx, snapshot)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	// Accepted returns narrow to their captured unfinished audience too. The
	// selected contract must not invent an ordinary observer at that old cut.
	for _, captured := range snapshot.Deliveries {
		delivery := captured.Snapshot
		if _, returned := returns[delivery.EventID]; returned && !delivery.Terminal() {
			requests[delivery.EventID] = append(requests[delivery.EventID], delivery.DeliveryID)
		}
	}
	plan, err = plan.WithHistoricalRequestPublications(snapshot.Revision, requests)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	fanOutObligations, err := loadRunForkFanOutObligationsFromRevision(snapshot, pending)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	plan.FanOutObligations = fanOutObligations
	plan.FanOutObligationCount = len(fanOutObligations)
	if err := retainFanOutExecutionSources(ctx, tx, snapshot, plan, postgres); err != nil {
		return runfork.RunForkPlan{}, err
	}
	if err := runfork.ValidateFanOutPendingReplayAdmission(plan); err != nil {
		return runfork.RunForkPlan{}, fmt.Errorf("fixed-revision fan-out admission: %w", err)
	}
	evidence, err := loadRunForkAdmissionEvidenceFromRevision(snapshot, entities, pending, fanOutObligations)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	evidence.TimerHistory.Point = plan.ForkPoint
	plan.JoinSchedules = evidence.JoinSchedules
	plan.OneShotSchedules = evidence.OneShotSchedules
	plan.TransferredJoins = evidence.TransferredJoins
	publications, err := runForkAllJoinPublications(snapshot, plan)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	plan, err = plan.WithHistoricalJoinPublications(snapshot.Revision, publications)
	if err != nil {
		return runfork.RunForkPlan{}, err
	}
	if err := requireRunForkTransferredJoinCauses(ctx, tx, plan); err != nil {
		return runfork.RunForkPlan{}, err
	}
	plan.ReplayResumeAdmission = runForkReplayResumeAdmission(evidence)
	plan.ReplayResumeAdmission = runForkReplayResumeAdmissionWithMaterializedEntitySnapshotMetadata(plan.ReplayResumeAdmission, entitySnapshotMetadataAdmission)
	plan.UnsupportedBlockers = plan.ReplayResumeAdmission.UnsupportedBlockers
	plan.UnsupportedBlockerCount = len(plan.UnsupportedBlockers)
	plan.ExecutionReady = plan.ReplayResumeAdmission.StateOnlyExecutionReady || plan.ReplayResumeAdmission.DeliveryEventReplayReady
	return retainOrdinaryReplayAncestorCompletions(ctx, tx, plan, snapshot), nil
}

func retainOrdinaryReplayAncestorCompletions(ctx context.Context, tx *sql.Tx, plan runfork.RunForkPlan, snapshot *runForkRevisionSnapshot) runfork.RunForkPlan {
	selected := make(map[string]bool)
	for _, row := range snapshot.Events {
		if row.EventClass == string(events.EventAdmissionSelectedForkReplay) {
			selected[row.EventID] = true
		}
	}
	if len(selected) == 0 {
		return plan
	}
	readers := make(map[string]func() ([]runfork.AncestorDeliveryCompletion, error))
	cut, cutErr := newRunForkReplayCut(snapshot)
	parents := make(map[string]ordinaryReplayParent)
	for _, work := range plan.PendingWork {
		id := work.EventID
		if !selected[id] || readers[id] != nil || work.DeliveryID == "" || !ordinaryReplayCompletionCandidate(plan, work) {
			continue
		}
		rows, err := readOrdinaryReplayAncestorCompletions(ctx, tx, cut, cutErr, id, parents)
		// The admission consumer, not the public plan, owns use of this read.
		readers[id] = func() ([]runfork.AncestorDeliveryCompletion, error) { return rows, err }
	}
	return plan.WithOrdinaryReplayAncestorCompletions(readers)
}

func ordinaryReplayCompletionCandidate(plan runfork.RunForkPlan, work runfork.RunForkPendingWork) bool {
	switch work.Classification {
	case runfork.RunForkPendingClassificationPending, runfork.RunForkPendingClassificationInProgress, runfork.RunForkPendingClassificationFailedRetryable:
	default:
		return false
	}
	id := work.EventID
	if _, named := runfork.RunForkSelectedContractPlatformRuntimeOwner(work.EventName); named || work.RetainsTerminalBarrierHistory() {
		return false
	}
	if _, named := plan.ExecutionInputPublication(id); named {
		return false
	}
	if _, named := plan.HistoricalJoinPublication(id); named {
		return false
	}
	if _, named, err := plan.PendingJoinOccurrence(id); named || err != nil {
		return false
	}
	if _, named, err := plan.FanOutPendingRecipientPartition(id); named || err != nil {
		return false
	}
	if _, named, err := plan.RequestPendingRecipientPartition(id); named || err != nil {
		return false
	}
	reply, err := plan.HistoricalReplyReturn(id)
	return reply == nil && err == nil
}

type ordinaryReplayParent struct {
	binding runfork.RunForkSelectedContractBinding
	cut     runForkReplayCut
	pending []runfork.RunForkPendingWork
}

func loadOrdinaryReplayParent(ctx context.Context, tx *sql.Tx, child string) (ordinaryReplayParent, error) {
	var out ordinaryReplayParent
	binding, err := loadRunForkSelectedContractBinding(ctx, tx, child)
	if err != nil {
		return out, err
	}
	if err := binding.ValidateExecutionBinding(child); err != nil {
		return out, err
	}
	out.binding = binding
	if binding.ForkPoint.Kind == runfork.RunForkPointRunStart {
		return out, nil // The exclusive-start first input is a named path.
	}
	origin, err := lifecycleowner.ReadImmutableRunOriginTx(ctx, tx, child)
	if err != nil {
		return out, err
	}
	point := binding.ForkPoint
	if origin.Kind() != runlifecycle.OriginForkMaterialization || origin.SourceRunID() != binding.SourceRunID ||
		origin.ForkRevision() != point.Revision || origin.SourceEventID() != point.EventID || origin.ForkPointKind() != point.Kind {
		return out, fmt.Errorf("selected input lineage differs from exact source/child/event binding")
	}
	parent, err := loadRunForkPointSnapshot(ctx, tx, binding.SourceRunID, point)
	if err != nil {
		return out, err
	}
	if err := validateCapturedOriginBoundary(origin, parent); err != nil {
		return out, err
	}
	out.cut, err = newRunForkReplayCut(parent)
	if err != nil {
		return out, err
	}
	out.pending, err = loadRunForkPendingWorkFromRevision(parent)
	return out, err
}

func readOrdinaryReplayAncestorCompletions(ctx context.Context, tx *sql.Tx, cut runForkReplayCut, cutErr error, id string, parents map[string]ordinaryReplayParent) ([]runfork.AncestorDeliveryCompletion, error) {
	if cutErr != nil {
		return nil, cutErr
	}
	event, err := cut.event(id)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	var rows []runfork.AncestorDeliveryCompletion
	for {
		lineage, selected := event.SelectedForkLineage()
		if !selected {
			return rows, nil
		}
		child := event.RunID()
		if seen[child] {
			return nil, fmt.Errorf("captured construction has cyclic immutable lineage")
		}
		seen[child] = true
		parent, ok := parents[child]
		if !ok {
			parent, err = loadOrdinaryReplayParent(ctx, tx, child)
			if err != nil {
				return nil, err
			}
			parents[child] = parent
		}
		if parent.binding.ForkPoint.Kind == runfork.RunForkPointRunStart {
			return nil, nil
		}
		previous, err := parent.cut.event(lineage.SourceEventID())
		if err != nil {
			return nil, err
		}
		input, err := runfork.InputPublicationFromEvent(previous)
		if _, named := input.Event(); err != nil || named {
			return nil, err
		}
		if err := requireSelectedPublicationCutLineage(ctx, tx, parent.binding, event, previous, nil); err != nil {
			return nil, err
		}
		for _, work := range parent.pending {
			if work.EventID == previous.ID() && work.DeliveryID != "" && work.Classification == runfork.RunForkPendingClassificationDeliveredCompleted {
				rows = append(rows, runfork.AncestorDeliveryCompletion{SourceRunID: parent.cut.runID, Point: parent.binding.ForkPoint, EventID: work.EventID, DeliveryID: work.DeliveryID, Route: work.DeliveryRoute})
			}
		}
		event = previous
	}
}

func (a runForkSourceStateAdmission) activityRequestOriginsAtCut(ctx context.Context, tx *sql.Tx) ([]semanticview.ActivityRequestOrigin, error) {
	origins := make([]semanticview.ActivityRequestOrigin, 0)
	for _, event := range a.snapshot.Events {
		if a.sourceAt == nil || event.EventName != runForkActivityRequestEvent {
			continue
		}
		origin, err := a.activityRequestOriginAtCut(ctx, tx, event.EventID, make(map[string]bool))
		if err != nil {
			return nil, fmt.Errorf("admit original activity request %s: %w", event.EventID, err)
		}
		origins = append(origins, origin)
	}
	return origins, nil
}

func resolveFixedRunForkRevisionPoint(ctx context.Context, tx *sql.Tx, runID string, point runfork.RunForkPoint, resolve runForkRevisionPointResolver) (runForkEventCursor, error) {
	if err := point.Validate(); err != nil {
		return runForkEventCursor{}, err
	}
	if point.Kind == runfork.RunForkPointEvent {
		cursor, err := resolve(ctx, tx, runID, point.EventID)
		if err != nil {
			return runForkEventCursor{}, err
		}
		if cursor.Kind != point.Kind || cursor.Revision != point.Revision {
			return runForkEventCursor{}, fmt.Errorf("fixed event point is not its first committed revision")
		}
		return cursor, nil
	}
	if point.Kind == runfork.RunForkPointRunStart {
		cursor, err := resolveRunForkStartPoint(ctx, tx, runID)
		if err != nil {
			return runForkEventCursor{}, err
		}
		if cursor.Revision != point.Revision {
			return runForkEventCursor{}, fmt.Errorf("fixed start point is not original creation revision")
		}
		return cursor, nil
	}
	if point.Kind == runfork.RunForkPointStateRevision {
		var revision int64
		if err := tx.QueryRowContext(ctx, `SELECT revision FROM run_fork_revisions WHERE run_id=$1 AND revision=$2`, runID, point.Revision).Scan(&revision); err != nil {
			return runForkEventCursor{}, fmt.Errorf("fixed state revision is not committed: %w", err)
		}
		return runForkEventCursor{Kind: point.Kind, Revision: revision}, nil
	}
	var originKind string
	if err := tx.QueryRowContext(ctx, `SELECT origin_kind FROM runs WHERE run_id=$1`, runID).Scan(&originKind); err != nil {
		return runForkEventCursor{}, fmt.Errorf("read fixed deployment source origin: %w", err)
	}
	if originKind != "deployment" {
		return runForkEventCursor{}, fmt.Errorf("deployment revision point requires a deployment-origin source run")
	}
	var revision int64
	if err := tx.QueryRowContext(ctx, `SELECT revision FROM run_fork_revisions WHERE run_id=$1 AND revision=$2`, runID, point.Revision).Scan(&revision); err != nil {
		return runForkEventCursor{}, fmt.Errorf("fixed deployment revision is not committed: %w", err)
	}
	return runForkEventCursor{Kind: runfork.RunForkPointDeploymentRevision, Revision: revision}, nil
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
	var startedRaw, endedRaw any
	if err := q.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), started_at, ended_at
		FROM runs
		WHERE run_id = $1
	`, plan.SourceRunID).Scan(&plan.SourceRunStatus, &startedRaw, &endedRaw); err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("source run %s not found", plan.SourceRunID)
		}
		return fmt.Errorf("load source run: %w", err)
	}
	started, present, err := sqliteTimeValue(startedRaw)
	if err != nil {
		return fmt.Errorf("decode source run started_at: %w", err)
	}
	if present {
		tm := started
		plan.SourceRunStartedAt = &tm
	}
	ended, present, err := sqliteTimeValue(endedRaw)
	if err != nil {
		return fmt.Errorf("decode source run ended_at: %w", err)
	}
	if present {
		tm := ended
		plan.SourceRunEndedAt = &tm
	}
	return nil
}

func loadRunForkEntityStates(snapshot *runForkRevisionSnapshot) ([]runfork.RunForkEntityState, error) {
	if snapshot == nil {
		return nil, fmt.Errorf("entity reconstruction requires a fixed snapshot")
	}
	if err := validateRunForkEntityMetadataOwners(snapshot); err != nil {
		return nil, err
	}
	type timedProjectionMutation struct {
		mutationlog.ProjectionMutation
		CreatedAt time.Time
	}
	grouped := map[string][]timedProjectionMutation{}
	entityOrder := []string{}
	seen := map[string]struct{}{}
	// Empty authored-data imports have a real immutable birth, even when their
	// empty fields produce no diff row. Live inventory cannot prove that birth.
	for _, metadata := range snapshot.EntityMetadata {
		if metadata.ConstructionKind != "imported_state" {
			continue
		}
		if metadata.EntityType == "" || metadata.FlowInstance == "" || metadata.DataCreatedAt == nil || metadata.DataUpdatedAt == nil || metadata.DataCreatedAt.IsZero() || metadata.DataUpdatedAt.IsZero() || metadata.DataUpdatedAt.Before(*metadata.DataCreatedAt) {
			return nil, fmt.Errorf("data-only historical birth requires admitted identity and data clocks")
		}
		seen[metadata.EntityID] = struct{}{}
		entityOrder = append(entityOrder, metadata.EntityID)
	}
	for _, fact := range snapshot.EntityMutations {
		entityID := strings.TrimSpace(fact.EntityID)
		domain := mutationlog.Domain(strings.TrimSpace(fact.Domain))
		path := strings.TrimSpace(fact.Path)
		value, err := decodeRunForkMutationValue(fact.NewValue)
		if err != nil {
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
			Accumulator:    projection.Accumulator,
		})
	}
	return out, nil
}

func decodeRunForkMutationValue(raw json.RawMessage) (any, error) {
	var value any
	if err := canonicaljson.DecodePreservingNumberLexemes(raw, &value); err != nil {
		return nil, err
	}
	return canonicaljson.CloneRuntimeValue(value)
}

func runForkPendingReferencesActiveSession(pending []runfork.RunForkPendingWork) bool {
	for _, item := range pending {
		if item.SourceExecution.SessionBound {
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
