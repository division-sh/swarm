package pipeline

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/store/testutil/deliveryfixture"
	"github.com/division-sh/swarm/internal/store/testutil/mutationlogfixture"
	"github.com/google/uuid"
)

type pipelineTestPublicationPlan struct {
	intent     runtimeengine.EmitIntent
	commitHook func(context.Context, events.Event) error
}

func (p pipelineTestPublicationPlan) DurablePublicationEventID() string {
	return strings.TrimSpace(p.intent.Event.ID())
}

func (p pipelineTestPublicationPlan) ValidateDurablePublicationPlan() error {
	if p.DurablePublicationEventID() == "" || strings.TrimSpace(string(p.intent.Event.Type())) == "" {
		return fmt.Errorf("pipeline test publication requires exact event identity")
	}
	return nil
}

type pipelineTestCommittedPublication struct {
	eventID string
	intent  runtimeengine.EmitIntent
}

func (p pipelineTestCommittedPublication) CommittedDurablePublicationEventID() string {
	return strings.TrimSpace(p.eventID)
}

func (p pipelineTestCommittedPublication) CommittedDurablePublicationIntent() runtimeengine.EmitIntent {
	return p.intent
}

func (p pipelineTestCommittedPublication) ValidateCommittedDurablePublication() error {
	if p.CommittedDurablePublicationEventID() == "" {
		return fmt.Errorf("pipeline test committed publication requires event identity")
	}
	return nil
}

func (b *recordingPipelineBus) PrepareEnginePublications(_ context.Context, intents []runtimeengine.EmitIntent) ([]runtimeengine.DurablePublicationPlan, error) {
	if b != nil && b.publishErr != nil {
		return nil, b.publishErr
	}
	if b != nil && b.outboxErr != nil {
		return nil, b.outboxErr
	}
	plans := make([]runtimeengine.DurablePublicationPlan, 0, len(intents))
	for _, intent := range intents {
		if strings.TrimSpace(string(intent.Event.Type())) == "" {
			continue
		}
		admitted, err := events.AdmitForPublish(intent.Event, events.AdmissionOptions{})
		if err != nil {
			return nil, err
		}
		intent.Event = admitted.Event()
		plan := pipelineTestPublicationPlan{intent: intent}
		if b != nil {
			plan.commitHook = b.publishInMutationHook
		}
		if err := plan.ValidateDurablePublicationPlan(); err != nil {
			return nil, err
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

func (*recordingPipelineBus) ReleaseEnginePublications(context.Context, []runtimeengine.DurablePublicationPlan) error {
	return nil
}

func (b *recordingPipelineBus) FinalizeEnginePublications(_ context.Context, evidence []runtimeengine.CommittedDurablePublication) error {
	intents := make([]runtimeengine.EmitIntent, 0, len(evidence))
	for _, committed := range evidence {
		if committed == nil {
			return fmt.Errorf("pipeline test publication evidence is required")
		}
		if err := committed.ValidateCommittedDurablePublication(); err != nil {
			return err
		}
		value, ok := committed.(pipelineTestCommittedPublication)
		if !ok {
			return fmt.Errorf("pipeline test committed publication has unexpected type %T", committed)
		}
		intents = append(intents, value.intent)
	}
	if b != nil {
		b.mu.Lock()
		b.outboxIntents = append(b.outboxIntents, cloneEmitIntents(intents)...)
		b.mu.Unlock()
	}
	return nil
}

func (r *recordingRuntimeMutationRunner) CommitWorkflowEngineMutation(ctx context.Context, command WorkflowEngineMutationCommand) (CommittedWorkflowEngineMutation, error) {
	if err := command.Validate(); err != nil {
		return CommittedWorkflowEngineMutation{}, err
	}
	result := CommittedWorkflowEngineMutation{}
	err := r.RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
		store := workflowStoreForRecordingRunner(r)
		tx, ok := sqlTxFromContext(txctx)
		if !ok || tx == nil {
			return fmt.Errorf("pipeline test engine commit requires its private transaction")
		}
		if command.EntitylessTarget.Empty() {
			if err := commitPipelineTestWorkflowState(txctx, store, command.State); err != nil {
				return err
			}
			var err error
			result.Lifecycle, err = commitPipelineTestWorkflowLifecycle(txctx, store, command.Lifecycle)
			if err != nil {
				return err
			}
		}
		if len(command.ProposedEffects) != 0 {
			return fmt.Errorf("pipeline test closed engine owner does not support proposed effects")
		}
		for _, value := range command.Publications {
			plan, ok := value.(pipelineTestPublicationPlan)
			if !ok {
				return fmt.Errorf("pipeline test publication has unexpected type %T", value)
			}
			dialect := authoractivityfixture.DialectSQLite
			if r.dialect == workflowStoreDialectPostgres {
				dialect = authoractivityfixture.DialectPostgres
			}
			if plan.commitHook != nil {
				if err := plan.commitHook(txctx, plan.intent.Event); err != nil {
					return err
				}
			}
			if err := eventfixture.Insert(txctx, tx, dialect, plan.intent.Event); err != nil {
				return err
			}
			result.Publications = append(result.Publications, pipelineTestCommittedPublication{eventID: plan.intent.Event.ID(), intent: plan.intent})
		}
		if success := command.DeliverySuccess; success != nil {
			dialect := deliveryfixture.DialectSQLite
			if r.dialect == workflowStoreDialectPostgres {
				dialect = deliveryfixture.DialectPostgres
			}
			adapter, err := deliveryfixture.NewAdapter(dialect)
			if err != nil {
				return err
			}
			story, ok := authoractivityfixture.Mutation(txctx)
			if !ok {
				return fmt.Errorf("pipeline test engine delivery settlement requires its author activity mutation")
			}
			if _, err := adapter.Adapter.SettleSuccess(txctx, tx, story, success.Claim, success.SideEffects, success.Duration); err != nil {
				return err
			}
			claim := success.Claim
			result.DeliverySuccess = &claim
		}
		return nil
	})
	if err != nil {
		return CommittedWorkflowEngineMutation{}, err
	}
	if err := result.Validate(); err != nil {
		return CommittedWorkflowEngineMutation{}, err
	}
	r.mu.Lock()
	r.committedGenericScheduleActivations = append(r.committedGenericScheduleActivations, result.Lifecycle.GenericScheduleActivations...)
	r.committedGenericScheduleCancellations = append(r.committedGenericScheduleCancellations, result.Lifecycle.GenericScheduleCancellations...)
	r.mu.Unlock()
	return result, nil
}

func (r *recordingRuntimeMutationRunner) CommitHumanTaskDeferredRoute(ctx context.Context, command HumanTaskDeferredRouteCommand) (CommittedHumanTaskRoute, error) {
	if err := command.Validate(); err != nil {
		return CommittedHumanTaskRoute{}, err
	}
	return r.commitHumanTaskRouteForTest(ctx, command.Publication, "", "", time.Time{})
}

func (r *recordingRuntimeMutationRunner) CommitProposedEffectRoute(ctx context.Context, command ProposedEffectRouteCommand) (CommittedProposedEffectRoute, error) {
	if err := command.Validate(); err != nil {
		return CommittedProposedEffectRoute{}, err
	}
	committed, err := r.commitHumanTaskRouteForTest(ctx, command.Publication, "", "", time.Time{})
	if err != nil {
		return CommittedProposedEffectRoute{}, err
	}
	return CommittedProposedEffectRoute{Publication: committed.Publication}, nil
}

func (r *recordingRuntimeMutationRunner) CommitHumanTaskOutcomeRoute(ctx context.Context, command HumanTaskOutcomeRouteCommand) (CommittedHumanTaskRoute, error) {
	if err := command.Validate(); err != nil {
		return CommittedHumanTaskRoute{}, err
	}
	return r.commitHumanTaskRouteForTest(ctx, command.Publication, command.CardID, command.RouteEventID, command.OccurredAt)
}

func (r *recordingRuntimeMutationRunner) commitHumanTaskRouteForTest(
	ctx context.Context,
	publication runtimeengine.DurablePublicationPlan,
	cardID string,
	routeEventID string,
	occurredAt time.Time,
) (CommittedHumanTaskRoute, error) {
	plan, ok := publication.(pipelineTestPublicationPlan)
	if !ok {
		return CommittedHumanTaskRoute{}, fmt.Errorf("pipeline test human-task publication has unexpected type %T", publication)
	}
	var result CommittedHumanTaskRoute
	err := r.RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
		tx, ok := sqlTxFromContext(txctx)
		if !ok || tx == nil {
			return fmt.Errorf("pipeline test human-task route requires its private transaction")
		}
		if err := eventfixture.Insert(txctx, tx, authoractivityfixture.Dialect(r.dialect), plan.intent.Event); err != nil {
			return err
		}
		if cardID != "" {
			store, ok := r.decisionCards.(decisioncard.HumanTaskStore)
			if !ok {
				return fmt.Errorf("pipeline test human-task route requires continuation store")
			}
			if _, err := store.CompleteHumanTaskOutcome(txctx, cardID, routeEventID, occurredAt); err != nil {
				return err
			}
		}
		result.Publication = pipelineTestCommittedPublication{eventID: plan.intent.Event.ID(), intent: plan.intent}
		return nil
	})
	if err != nil {
		return CommittedHumanTaskRoute{}, err
	}
	return result, result.Validate()
}

var _ WorkflowDecisionRouteOwner = (*recordingRuntimeMutationRunner)(nil)

func (r *recordingRuntimeMutationRunner) CommitWorkflowInitialMaterialization(ctx context.Context, command WorkflowInitialMaterializationCommand) (CommittedWorkflowInitialMaterialization, error) {
	if err := command.Validate(); err != nil {
		return CommittedWorkflowInitialMaterialization{}, err
	}
	result := CommittedWorkflowInitialMaterialization{Result: WorkflowInitialMaterializationAlreadyExists}
	err := r.RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
		tx, ok := sqlTxFromContext(txctx)
		if !ok || tx == nil {
			return fmt.Errorf("pipeline test initial materialization requires its private transaction")
		}
		record := command.Record
		if r.dialect == workflowStoreDialectPostgres {
			lockIdentity := fmt.Sprintf("%d:%s%s", len(record.State.RunID), record.State.RunID, record.State.Route.InstancePath)
			if _, err := tx.ExecContext(txctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lockIdentity); err != nil {
				return err
			}
		}
		query := `SELECT projection_version, projection, occurred_at FROM workflow_instance_initial_materializations WHERE run_id = ? AND entity_id = ? AND instance_id = ?`
		if r.dialect == workflowStoreDialectPostgres {
			query = `SELECT projection_version, projection, occurred_at FROM workflow_instance_initial_materializations WHERE run_id = $1::uuid AND entity_id = $2::uuid AND instance_id = $3`
		}
		var version int
		var projection []byte
		var occurredAt time.Time
		var occurredAtRaw any
		destination := any(&occurredAt)
		if r.dialect != workflowStoreDialectPostgres {
			destination = &occurredAtRaw
		}
		err := tx.QueryRowContext(txctx, query, record.State.RunID, record.State.EntityID, record.State.Route.InstancePath).Scan(&version, &projection, destination)
		if err == nil {
			if r.dialect != workflowStoreDialectPostgres {
				var present bool
				occurredAt, present, err = sqliteWorkflowTimeValue(occurredAtRaw)
				if err != nil || !present {
					return fmt.Errorf("decode pipeline test initial occurrence")
				}
			}
			readinessEqual, err := pipelineTestInitialReadinessEqual(txctx, r.dialect, record)
			if err != nil {
				return err
			}
			if version != record.ProjectionVersion || !pipelineTestJSONEqual(projection, record.Projection) ||
				!canonicalWorkflowInstancePersistedTime(occurredAt).Equal(canonicalWorkflowInstancePersistedTime(record.OccurredAt)) || !readinessEqual {
				return pipelineTestInitialConflict(record.State.Route.InstancePath)
			}
			snapshotQuery := `SELECT EXISTS (SELECT 1 FROM flow_instances WHERE instance_id = ?), EXISTS (SELECT 1 FROM entity_state WHERE run_id = ? AND entity_id = ? AND flow_instance = ?)`
			snapshotArgs := []any{record.State.Route.InstancePath, record.State.RunID, record.State.EntityID, record.State.Route.InstancePath}
			if r.dialect == workflowStoreDialectPostgres {
				snapshotQuery = `SELECT EXISTS (SELECT 1 FROM flow_instances WHERE instance_id = $1), EXISTS (SELECT 1 FROM entity_state WHERE run_id = $2::uuid AND entity_id = $3::uuid AND flow_instance = $1)`
				snapshotArgs = []any{record.State.Route.InstancePath, record.State.RunID, record.State.EntityID}
			}
			var flow, entity bool
			if err := tx.QueryRowContext(txctx, snapshotQuery, snapshotArgs...).Scan(&flow, &entity); err != nil {
				return err
			}
			if !flow || !entity {
				return pipelineTestInitialConflict(record.State.Route.InstancePath)
			}
			return nil
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		occupiedQuery := `SELECT EXISTS (SELECT 1 FROM flow_instances WHERE instance_id = ?), EXISTS (SELECT 1 FROM entity_state WHERE run_id = ? AND entity_id = ?), EXISTS (SELECT 1 FROM workflow_instance_initial_materializations WHERE run_id = ? AND entity_id = ?), EXISTS (SELECT 1 FROM flow_instance_runtime_readiness WHERE run_id = ? AND instance_id = ?)`
		occupiedArgs := []any{record.State.Route.InstancePath, record.State.RunID, record.State.EntityID, record.State.RunID, record.State.EntityID, record.State.RunID, record.State.Route.InstancePath}
		if r.dialect == workflowStoreDialectPostgres {
			occupiedQuery = `SELECT EXISTS (SELECT 1 FROM flow_instances WHERE instance_id = $1), EXISTS (SELECT 1 FROM entity_state WHERE run_id = $2::uuid AND entity_id = $3::uuid), EXISTS (SELECT 1 FROM workflow_instance_initial_materializations WHERE run_id = $2::uuid AND entity_id = $3::uuid), EXISTS (SELECT 1 FROM flow_instance_runtime_readiness WHERE run_id = $2::uuid AND instance_id = $1)`
			occupiedArgs = []any{record.State.Route.InstancePath, record.State.RunID, record.State.EntityID}
		}
		var flow, entity, initial, readiness bool
		if err := tx.QueryRowContext(txctx, occupiedQuery, occupiedArgs...).Scan(&flow, &entity, &initial, &readiness); err != nil {
			return err
		}
		if entity || initial || readiness {
			return pipelineTestInitialConflict(record.State.Route.InstancePath)
		}
		if flow {
			allowed, err := pipelineTestInitialRouteRebindAllowed(txctx, tx, r.dialect, record)
			if err != nil {
				return err
			}
			if !allowed {
				return pipelineTestInitialConflict(record.State.Route.InstancePath)
			}
		}
		store := workflowStoreForRecordingRunner(r)
		if err := commitPipelineTestWorkflowStateWithRouteRebind(txctx, store, record.State, flow); err != nil {
			return err
		}
		insertInitial := `INSERT INTO workflow_instance_initial_materializations (run_id, entity_id, instance_id, projection_version, projection, occurred_at) VALUES (?, ?, ?, ?, ?, ?)`
		if r.dialect == workflowStoreDialectPostgres {
			insertInitial = `INSERT INTO workflow_instance_initial_materializations (run_id, entity_id, instance_id, projection_version, projection, occurred_at) VALUES ($1::uuid, $2::uuid, $3, $4, $5::jsonb, $6)`
		}
		if _, err := tx.ExecContext(txctx, insertInitial, record.State.RunID, record.State.EntityID, record.State.Route.InstancePath, record.ProjectionVersion, record.Projection, record.OccurredAt); err != nil {
			return err
		}
		if len(record.Readiness) > 0 {
			insertReadiness := `INSERT INTO flow_instance_runtime_readiness (run_id, instance_id, plan, topology_ready_at, creation_event_emitted_at, created_at, updated_at) VALUES (?, ?, ?, NULL, NULL, ?, ?)`
			args := []any{record.State.RunID, record.State.Route.InstancePath, record.Readiness, record.OccurredAt, record.OccurredAt}
			if r.dialect == workflowStoreDialectPostgres {
				insertReadiness = `INSERT INTO flow_instance_runtime_readiness (run_id, instance_id, plan, topology_ready_at, creation_event_emitted_at, created_at, updated_at) VALUES ($1::uuid, $2, $3::jsonb, NULL, NULL, $4, $4)`
				args = []any{record.State.RunID, record.State.Route.InstancePath, record.Readiness, record.OccurredAt}
			}
			if _, err := tx.ExecContext(txctx, insertReadiness, args...); err != nil {
				return err
			}
		}
		result.Lifecycle, err = commitPipelineTestWorkflowLifecycle(txctx, store, command.Lifecycle)
		if err != nil {
			return err
		}
		result.Result = WorkflowInitialMaterializationCreated
		return nil
	})
	if err != nil {
		return CommittedWorkflowInitialMaterialization{}, err
	}
	return result, result.Validate()
}

func commitPipelineTestWorkflowLifecycle(ctx context.Context, store *workflowInstanceStore, plan WorkflowLifecycleMutationPlan) (CommittedWorkflowLifecycleMutation, error) {
	result := CommittedWorkflowLifecycleMutation{}
	for _, mutation := range plan.Timers {
		switch mutation.Kind {
		case WorkflowTimerMutationInsert:
			persisted, _, err := store.insertWorkflowTimerActivation(ctx, mutation.Activation)
			if err != nil {
				return CommittedWorkflowLifecycleMutation{}, err
			}
			result.Wakeups = append(result.Wakeups, persisted.Ref)
		case WorkflowTimerMutationCancel:
			cancelled, changed, err := store.cancelWorkflowTimerActivation(ctx, mutation.Activation.Ref)
			if err != nil {
				return CommittedWorkflowLifecycleMutation{}, err
			}
			if changed {
				result.Cancellations = append(result.Cancellations, cancelled.Ref)
			}
		default:
			return CommittedWorkflowLifecycleMutation{}, fmt.Errorf("pipeline test workflow timer mutation %q is unsupported", mutation.Kind)
		}
	}
	for _, mutation := range plan.Schedules {
		activation, err := pipelineTestGenericScheduleActivation(mutation.Command)
		if err != nil {
			return CommittedWorkflowLifecycleMutation{}, err
		}
		switch mutation.Kind {
		case WorkflowScheduleMutationUpsert:
			result.GenericScheduleActivations = append(result.GenericScheduleActivations, activation)
		case WorkflowScheduleMutationCancel:
			activation.Status = runtimegenericschedule.StatusCancelled
			activation.CancelCause = mutation.CancelCause
			activation.CancelledAt = mutation.CancelledAt
			if err := activation.Validate(); err != nil {
				return CommittedWorkflowLifecycleMutation{}, err
			}
			result.GenericScheduleCancellations = append(result.GenericScheduleCancellations, activation)
		default:
			return CommittedWorkflowLifecycleMutation{}, fmt.Errorf("pipeline test workflow schedule mutation %q is unsupported", mutation.Kind)
		}
	}
	for _, mutation := range plan.GateCards {
		if store.decisionCards == nil {
			return CommittedWorkflowLifecycleMutation{}, fmt.Errorf("pipeline test gate card mutation requires decision card owner")
		}
		switch mutation.Kind {
		case WorkflowGateCardMutationCreate:
			if err := store.decisionCards.CreateDecisionCard(ctx, mutation.Card); err != nil {
				return CommittedWorkflowLifecycleMutation{}, err
			}
		case WorkflowGateCardMutationSupersede:
			if err := store.decisionCards.SupersedeDecisionCardsForStage(
				ctx,
				mutation.Card.RunID,
				mutation.EntityID,
				mutation.ActivationID,
				mutation.Reason,
				mutation.OccurredAt,
			); err != nil {
				return CommittedWorkflowLifecycleMutation{}, err
			}
		default:
			return CommittedWorkflowLifecycleMutation{}, fmt.Errorf("pipeline test workflow gate card mutation %q is unsupported", mutation.Kind)
		}
	}
	return result, nil
}

func pipelineTestGenericScheduleActivation(command runtimegenericschedule.AdmissionCommand) (runtimegenericschedule.Activation, error) {
	command = command.Canonical()
	if err := command.Validate(); err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	scope, err := command.ScopeKey()
	if err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	hash, err := command.ImmutableHash()
	if err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	admittedAt := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	dueAt, err := command.Due.FirstDue(admittedAt)
	if err != nil {
		return runtimegenericschedule.Activation{}, err
	}
	activation := runtimegenericschedule.Activation{
		ID:      uuid.NewSHA1(uuid.NameSpaceOID, []byte(scope+"\x1f"+command.ScheduleKey)).String(),
		Command: command, ImmutableHash: hash, AdmittedAt: admittedAt,
		InitialDueAt: dueAt, CurrentDueAt: dueAt, Status: runtimegenericschedule.StatusActive,
	}
	return activation, activation.Validate()
}

func pipelineTestInitialReadinessEqual(ctx context.Context, dialect workflowStoreDialect, record WorkflowInitialMaterializationRecord) (bool, error) {
	tx, ok := sqlTxFromContext(ctx)
	if !ok || tx == nil {
		return false, fmt.Errorf("pipeline test readiness comparison requires transaction")
	}
	query := `SELECT plan FROM flow_instance_runtime_readiness WHERE run_id = ? AND instance_id = ?`
	if dialect == workflowStoreDialectPostgres {
		query = `SELECT plan FROM flow_instance_runtime_readiness WHERE run_id = $1::uuid AND instance_id = $2`
	}
	var plan []byte
	err := tx.QueryRowContext(ctx, query, record.State.RunID, record.State.Route.InstancePath).Scan(&plan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return len(record.Readiness) == 0, nil
		}
		return false, err
	}
	return len(record.Readiness) > 0 && pipelineTestJSONEqual(plan, record.Readiness), nil
}

func pipelineTestJSONEqual(actual, expected []byte) bool {
	actualValue, actualErr := canonicaljson.Decode(actual)
	expectedValue, expectedErr := canonicaljson.Decode(expected)
	if actualErr != nil || expectedErr != nil {
		return false
	}
	actualCanonical, actualErr := canonicaljson.Encode(actualValue)
	expectedCanonical, expectedErr := canonicaljson.Encode(expectedValue)
	return actualErr == nil && expectedErr == nil && bytes.Equal(actualCanonical, expectedCanonical)
}

func pipelineTestInitialConflict(instancePath string) error {
	return runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "flow_instance_already_exists", "workflow-instance-lifecycle", "materialize_initial_entry", map[string]any{"flow_instance": instancePath})
}

func pipelineTestInitialRouteRebindAllowed(ctx context.Context, tx *sql.Tx, dialect workflowStoreDialect, record WorkflowInitialMaterializationRecord) (bool, error) {
	query := `
		SELECT flow_template,
		       EXISTS (
			   SELECT 1 FROM entity_state AS state WHERE state.flow_instance = ?
		       ),
		       EXISTS (
			   SELECT 1
			   FROM entity_state AS state
			   JOIN runs AS run ON run.run_id = state.run_id
			   WHERE state.flow_instance = ?
			     AND LOWER(TRIM(run.status)) IN ('running', 'paused')
		       )
		FROM flow_instances
		WHERE instance_id = ?
	`
	args := []any{record.State.Route.InstancePath, record.State.Route.InstancePath, record.State.Route.InstancePath}
	if dialect == workflowStoreDialectPostgres {
		query = `
			SELECT flow_template,
			       EXISTS (
				   SELECT 1 FROM entity_state AS state WHERE state.flow_instance = $1
			       ),
			       EXISTS (
				   SELECT 1
				   FROM entity_state AS state
				   JOIN runs AS run ON run.run_id = state.run_id
				   WHERE state.flow_instance = $1
				     AND LOWER(BTRIM(run.status)) IN ('running', 'paused')
			       )
			FROM flow_instances
			WHERE instance_id = $1
			FOR UPDATE
		`
		args = []any{record.State.Route.InstancePath}
	}
	var workflowName string
	var priorReference bool
	var activeReference bool
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&workflowName, &priorReference, &activeReference); err != nil {
		return false, err
	}
	return strings.TrimSpace(workflowName) == strings.TrimSpace(record.State.WorkflowName) && priorReference && !activeReference, nil
}

func (r *recordingRuntimeMutationRunner) CommitWorkflowTimerOccurrence(ctx context.Context, command WorkflowTimerOccurrenceCommand) (CommittedWorkflowTimerOccurrence, error) {
	if err := command.Validate(); err != nil {
		return CommittedWorkflowTimerOccurrence{}, err
	}
	plan, ok := command.Publication.(pipelineTestPublicationPlan)
	if !ok {
		return CommittedWorkflowTimerOccurrence{}, fmt.Errorf("pipeline test timer publication has unexpected type %T", command.Publication)
	}
	result := CommittedWorkflowTimerOccurrence{Outcome: WorkflowTimerOccurrenceTerminal}
	err := r.RunRuntimeMutationContext(ctx, func(txctx context.Context) error {
		store := workflowStoreForRecordingRunner(r)
		activation, found, err := store.loadWorkflowTimerActivation(txctx, command.Activation.Ref.ActivationID, true)
		if err != nil {
			return err
		}
		if !found || activation.Status != workflowTimerStatusActive || !activation.FireAt.Equal(command.Occurrence.DueAt) {
			return nil
		}
		if err := requireSameWorkflowTimerActivationFacts(activation, command.Activation); err != nil {
			return err
		}
		tx, ok := sqlTxFromContext(txctx)
		if !ok || tx == nil {
			return fmt.Errorf("pipeline test timer commit requires its private transaction")
		}
		dialect := authoractivityfixture.DialectSQLite
		if r.dialect == workflowStoreDialectPostgres {
			dialect = authoractivityfixture.DialectPostgres
		}
		if plan.commitHook != nil {
			if err := plan.commitHook(txctx, plan.intent.Event); err != nil {
				return err
			}
		}
		if err := eventfixture.Insert(txctx, tx, dialect, plan.intent.Event); err != nil {
			return err
		}
		next := activation.normalized()
		next.FiredAt = canonicalWorkflowTimerTime(command.FiredAt)
		next.Status = workflowTimerStatusFired
		if next.Recurring {
			next.Status = workflowTimerStatusActive
			next.FireAt = canonicalWorkflowTimerTime(next.FireAt.Add(next.RecurrenceInterval))
		}
		query := `UPDATE timers SET status = ?, fired_at = ?, fire_at = ? WHERE timer_id = ? AND status = 'active' AND fire_at = ?`
		args := []any{next.Status, next.FiredAt, next.FireAt, activation.Ref.ActivationID, activation.FireAt}
		if r.dialect == workflowStoreDialectPostgres {
			query = `UPDATE timers SET status = $1, fired_at = $2, fire_at = $3 WHERE timer_id = $4::uuid AND status = 'active' AND fire_at = $5`
		}
		updated, err := tx.ExecContext(txctx, query, args...)
		if err != nil {
			return err
		}
		rows, err := updated.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return fmt.Errorf("pipeline test timer occurrence advanced %d rows", rows)
		}
		result = CommittedWorkflowTimerOccurrence{
			Outcome: WorkflowTimerOccurrenceCommitted,
			Next:    next,
			Publication: pipelineTestCommittedPublication{
				eventID: plan.intent.Event.ID(), intent: plan.intent,
			},
		}
		return nil
	})
	if err != nil {
		return CommittedWorkflowTimerOccurrence{}, err
	}
	if err := result.Validate(); err != nil {
		return CommittedWorkflowTimerOccurrence{}, err
	}
	return result, nil
}

func commitPipelineTestWorkflowState(ctx context.Context, store *workflowInstanceStore, record WorkflowEngineStateRecord) error {
	return commitPipelineTestWorkflowStateWithRouteRebind(ctx, store, record, false)
}

func commitPipelineTestWorkflowStateWithRouteRebind(ctx context.Context, store *workflowInstanceStore, record WorkflowEngineStateRecord, rebindExistingRoute bool) error {
	if err := record.Validate(); err != nil {
		return err
	}
	tx, ok := sqlTxFromContext(ctx)
	if !ok || tx == nil {
		return fmt.Errorf("pipeline test workflow state commit requires its private transaction")
	}
	if store == nil || store.runLifecycle == nil {
		return fmt.Errorf("pipeline test workflow state commit requires run lifecycle owner")
	}
	if err := store.runLifecycle.RequireActiveRun(ctx, record.RunID); err != nil {
		return err
	}
	if store.testDialect() == workflowStoreDialectPostgres {
		if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, record.RunID+":"+record.Route.InstancePath); err != nil {
			return err
		}
	}
	var before runtimemutationlog.EntityStateProjection
	var err error
	if store.testDialect() == workflowStoreDialectPostgres {
		before, err = loadTrackedEntityStateProjection(ctx, tx, record.RunID, record.EntityID)
	} else {
		before, err = store.loadTrackedEntityStateProjectionSQLite(ctx, tx, record.RunID, record.EntityID)
	}
	if err != nil {
		return err
	}
	if record.Transition.CreatesState() {
		flowQuery := `INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at) VALUES (?, ?, ?, ?, ?, ?)`
		entityQuery := `
			INSERT INTO entity_state (
				run_id, entity_id, flow_instance, entity_type, slug, name,
				current_state, gates, fields, accumulator, revision,
				entered_state_at, created_at, updated_at
			) VALUES (?, ?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?, ?, 1, ?, ?, ?)
		`
		if store.testDialect() == workflowStoreDialectPostgres {
			flowQuery = `INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, created_at) VALUES ($1, $2, $3, $4::jsonb, $5, $6)`
			entityQuery = `
				INSERT INTO entity_state (
					run_id, entity_id, flow_instance, entity_type, slug, name,
					current_state, gates, fields, accumulator, revision,
					entered_state_at, created_at, updated_at
				) VALUES ($1::uuid, $2::uuid, $3, $4, NULLIF($5, ''), NULLIF($6, ''), $7, $8::jsonb, $9::jsonb, $10::jsonb, 1, $11, $12, $12)
			`
		}
		if rebindExistingRoute {
			if store.testDialect() == workflowStoreDialectPostgres {
				flowQuery += ` ON CONFLICT (instance_id) DO UPDATE SET flow_template = EXCLUDED.flow_template, mode = EXCLUDED.mode, config = EXCLUDED.config, status = EXCLUDED.status, terminated_at = NULL`
			} else {
				flowQuery += ` ON CONFLICT(instance_id) DO UPDATE SET flow_template = excluded.flow_template, mode = excluded.mode, config = excluded.config, status = excluded.status, terminated_at = NULL`
			}
		} else if store.testDialect() == workflowStoreDialectPostgres {
			flowQuery += ` ON CONFLICT (instance_id) DO NOTHING`
		} else {
			flowQuery += ` ON CONFLICT(instance_id) DO NOTHING`
		}
		result, err := tx.ExecContext(ctx, flowQuery, record.Route.InstancePath, record.WorkflowName, record.Mode, string(record.Config), record.Status, record.CreatedAt)
		if err != nil {
			return fmt.Errorf("insert pipeline test workflow flow instance: %w", err)
		}
		if !rebindExistingRoute {
			rows, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if rows == 0 {
				query := `SELECT flow_template, mode, config, status FROM flow_instances WHERE instance_id = ?`
				if store.testDialect() == workflowStoreDialectPostgres {
					query = `SELECT flow_template, mode, config, status FROM flow_instances WHERE instance_id = $1`
				}
				var workflowName, mode, status string
				var config any
				if err := tx.QueryRowContext(ctx, query, record.Route.InstancePath).Scan(&workflowName, &mode, &config, &status); err != nil {
					return err
				}
				configBytes := pipelineTestJSONBytes(config)
				if workflowName != record.WorkflowName || mode != record.Mode || status != record.Status || !pipelineTestJSONEqual(configBytes, record.Config) {
					return fmt.Errorf("pipeline test workflow flow descriptor conflicts at route %s", record.Route.InstancePath)
				}
			}
		}
		entityArgs := []any{
			record.RunID, record.EntityID, record.Route.InstancePath, record.EntityType, record.Slug, record.Name,
			record.CurrentState, string(record.Gates), string(record.Fields), string(record.Accumulator),
			record.EnteredStageAt, record.CreatedAt,
		}
		if store.testDialect() == workflowStoreDialectSQLite {
			entityArgs = append(entityArgs, record.CreatedAt)
		}
		if _, err := tx.ExecContext(ctx, entityQuery, entityArgs...); err != nil {
			return fmt.Errorf("insert pipeline test workflow entity state with %d arguments: %w", len(entityArgs), err)
		}
		return commitPipelineTestWorkflowMutationLog(ctx, tx, store, record, before)
	}
	stateQuery := `
		UPDATE entity_state
		SET entity_type = ?, slug = NULLIF(?, ''), name = NULLIF(?, ''), current_state = ?,
		    gates = ?, fields = ?, accumulator = ?, revision = revision + 1,
		    entered_state_at = ?, updated_at = ?
		WHERE run_id = ? AND entity_id = ? AND flow_instance = ? AND revision = ? AND current_state = ?
	`
	flowQuery := `UPDATE flow_instances SET flow_template = ?, mode = ?, config = ?, status = ?, terminated_at = CASE WHEN ? = 'terminated' THEN COALESCE(terminated_at, ?) ELSE NULL END WHERE instance_id = ?`
	if store.testDialect() == workflowStoreDialectPostgres {
		stateQuery = `
			UPDATE entity_state
			SET entity_type = $1, slug = NULLIF($2, ''), name = NULLIF($3, ''), current_state = $4,
			    gates = $5::jsonb, fields = $6::jsonb, accumulator = $7::jsonb, revision = revision + 1,
			    entered_state_at = $8, updated_at = $9
			WHERE run_id = $10::uuid AND entity_id = $11::uuid AND flow_instance = $12 AND revision = $13 AND current_state = $14
		`
		flowQuery = `UPDATE flow_instances SET flow_template = $1, mode = $2, config = $3::jsonb, status = $4, terminated_at = CASE WHEN $4 = 'terminated' THEN COALESCE(terminated_at, $5) ELSE NULL END WHERE instance_id = $6`
	}
	result, err := tx.ExecContext(ctx, stateQuery,
		record.EntityType, record.Slug, record.Name, record.CurrentState,
		string(record.Gates), string(record.Fields), string(record.Accumulator),
		record.EnteredStageAt, record.UpdatedAt, record.RunID, record.EntityID,
		record.Route.InstancePath, record.ExpectedRevision, record.ExpectedState,
	)
	if err != nil {
		return fmt.Errorf("update pipeline test workflow entity state: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		if err != nil {
			return err
		}
		return fmt.Errorf("pipeline test workflow state changed before commit")
	}
	if record.Transition == WorkflowEngineStateTransitionUpdateStateCreateCompanion {
		insert := `INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, terminated_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`
		args := []any{record.Route.InstancePath, record.WorkflowName, record.Mode, string(record.Config), record.Status, nullablePipelineTestWorkflowTerminationTime(record.TerminatedAt), record.CreatedAt}
		if store.testDialect() == workflowStoreDialectPostgres {
			insert = `INSERT INTO flow_instances (instance_id, flow_template, mode, config, status, terminated_at, created_at) VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7)`
		}
		if _, err := tx.ExecContext(ctx, insert, args...); err != nil {
			return fmt.Errorf("create pipeline test workflow lifecycle companion for existing state: %w", err)
		}
		return commitPipelineTestWorkflowMutationLog(ctx, tx, store, record, before)
	}
	flowArgs := []any{record.WorkflowName, record.Mode, string(record.Config), record.Status}
	if store.testDialect() == workflowStoreDialectSQLite {
		flowArgs = append(flowArgs, record.Status)
	}
	flowArgs = append(flowArgs, nullablePipelineTestWorkflowTerminationTime(record.TerminatedAt), record.Route.InstancePath)
	result, err = tx.ExecContext(ctx, flowQuery, flowArgs...)
	if err != nil {
		return fmt.Errorf("update pipeline test workflow flow instance with %d arguments: %w", len(flowArgs), err)
	}
	rows, err = result.RowsAffected()
	if err != nil {
		return err
	}
	if rows != 1 {
		return fmt.Errorf("pipeline test workflow flow instance is missing")
	}
	if err := commitPipelineTestWorkflowMutationLog(ctx, tx, store, record, before); err != nil {
		return fmt.Errorf("commit pipeline test workflow mutation log: %w", err)
	}
	return nil
}

func nullablePipelineTestWorkflowTerminationTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value.UTC()
}

func commitPipelineTestWorkflowMutationLog(
	ctx context.Context,
	tx *sql.Tx,
	store *workflowInstanceStore,
	record WorkflowEngineStateRecord,
	before runtimemutationlog.EntityStateProjection,
) error {
	decode := func(name string, raw json.RawMessage) (map[string]any, error) {
		value := map[string]any{}
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, fmt.Errorf("decode pipeline test workflow %s projection: %w", name, err)
		}
		return value, nil
	}
	fields, err := decode("fields", record.Fields)
	if err != nil {
		return err
	}
	gates, err := decode("gates", record.Gates)
	if err != nil {
		return err
	}
	accumulator, err := decode("accumulator", record.Accumulator)
	if err != nil {
		return err
	}
	after := runtimemutationlog.EntityStateProjection{
		CurrentState: record.CurrentState,
		Fields:       fields,
		Gates:        gates,
		Accumulator:  accumulator,
	}
	initial := map[string]any{}
	if err := json.Unmarshal(record.InitialFields, &initial); err != nil {
		return fmt.Errorf("decode pipeline test workflow initial fields: %w", err)
	}
	if record.Transition.CreatesState() && len(initial) > 0 {
		if store.testDialect() == workflowStoreDialectPostgres {
			before, err = insertWorkflowCreateEntityInitialValueMutations(ctx, tx, store.runLifecycle, record.EntityID, before, after, initial)
		} else {
			before, err = insertSQLiteWorkflowCreateEntityInitialValueMutations(ctx, tx, store.runLifecycle, record.EntityID, before, after, initial)
		}
		if err != nil {
			return err
		}
	}
	handlerStep := "mutate"
	if record.Transition.CreatesState() {
		handlerStep = "create"
	}
	writer := runtimemutationlog.Writer{
		Type:        "platform",
		ID:          "workflow_engine",
		HandlerStep: handlerStep,
	}
	if store.testDialect() == workflowStoreDialectPostgres {
		return mutationlogfixture.InsertEntityStateDiff(ctx, tx, store.runLifecycle, record.EntityID, before, after, writer)
	}
	return insertSQLiteEntityStateDiff(ctx, tx, store.runLifecycle, record.EntityID, before, after, writer)
}

var _ EnginePublicationPlanner = (*recordingPipelineBus)(nil)
var _ WorkflowEngineMutationOwner = (*recordingRuntimeMutationRunner)(nil)
var _ WorkflowInitialMaterializationCommitOwner = (*recordingRuntimeMutationRunner)(nil)
var _ WorkflowTimerOccurrenceOwner = (*recordingRuntimeMutationRunner)(nil)
