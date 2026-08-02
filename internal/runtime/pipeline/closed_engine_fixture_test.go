package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/events"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/store/eventfixture"
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
		store := &workflowInstanceStore{db: r.db, dialect: r.dialect, runtimeMutation: r, runLifecycle: r}
		if err := commitPipelineTestWorkflowState(txctx, store, command.State); err != nil {
			return err
		}
		for _, mutation := range command.Lifecycle.Timers {
			switch mutation.Kind {
			case WorkflowTimerMutationInsert:
				persisted, _, err := store.insertWorkflowTimerActivation(txctx, mutation.Activation)
				if err != nil {
					return err
				}
				result.Lifecycle.Wakeups = append(result.Lifecycle.Wakeups, persisted.Ref)
			case WorkflowTimerMutationCancel:
				cancelled, changed, err := store.cancelWorkflowTimerActivation(txctx, mutation.Activation.Ref)
				if err != nil {
					return err
				}
				if changed {
					result.Lifecycle.Cancellations = append(result.Lifecycle.Cancellations, cancelled.Ref)
				}
			default:
				return fmt.Errorf("pipeline test workflow timer mutation %q is unsupported", mutation.Kind)
			}
		}
		for _, mutation := range command.Lifecycle.Schedules {
			switch mutation.Kind {
			case WorkflowScheduleMutationUpsert:
				result.Lifecycle.ScheduleUpserts = append(result.Lifecycle.ScheduleUpserts, mutation.Schedule)
			case WorkflowScheduleMutationCancel:
				result.Lifecycle.ScheduleCancellations = append(result.Lifecycle.ScheduleCancellations, mutation.Schedule)
			default:
				return fmt.Errorf("pipeline test workflow schedule mutation %q is unsupported", mutation.Kind)
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
			dialect := runtimeauthoractivity.DialectSQLite
			if r.dialect == workflowStoreDialectPostgres {
				dialect = runtimeauthoractivity.DialectPostgres
			}
			tx, ok := sqlTxFromContext(txctx)
			if !ok || tx == nil {
				return fmt.Errorf("pipeline test engine commit requires its private transaction")
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
		return nil
	})
	if err != nil {
		return CommittedWorkflowEngineMutation{}, err
	}
	if err := result.Validate(); err != nil {
		return CommittedWorkflowEngineMutation{}, err
	}
	r.mu.Lock()
	r.committedScheduleUpserts = append(r.committedScheduleUpserts, result.Lifecycle.ScheduleUpserts...)
	r.committedScheduleCancellations = append(r.committedScheduleCancellations, result.Lifecycle.ScheduleCancellations...)
	r.mu.Unlock()
	return result, nil
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
		store := &workflowInstanceStore{db: r.db, dialect: r.dialect, runtimeMutation: r, runLifecycle: r}
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
		dialect := runtimeauthoractivity.DialectSQLite
		if r.dialect == workflowStoreDialectPostgres {
			dialect = runtimeauthoractivity.DialectPostgres
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
	control := workflowInstancePersistedControl{
		StorageRef: record.Route.InstancePath,
		EntityType: record.EntityType,
		InstanceID: record.Route.InstanceID,
		FlowPath:   record.Route.InstancePath,
		Slug:       record.Slug,
		Name:       record.Name,
		Status:     record.Status,
	}
	projection, err := decodeWorkflowInstancePersistedProjection(record.Fields, record.Gates, record.Accumulator, record.Config, control)
	if err != nil {
		return err
	}
	instance := WorkflowInstance{
		InstanceID: record.Route.InstanceID, StorageRef: record.Route.InstancePath,
		WorkflowName: record.WorkflowName, WorkflowVersion: record.WorkflowVersion,
		Status: record.Status, CurrentState: record.CurrentState,
		Metadata: projection.Metadata(), StateBuckets: projection.Accumulator, Config: projection.Config,
		TransitionHistory: append([]WorkflowTransitionRecord(nil), projection.Control.TransitionHistory...),
		EnteredStageAt:    record.EnteredStageAt, CreatedAt: record.CreatedAt, UpdatedAt: record.UpdatedAt,
	}
	if err := json.Unmarshal(record.InitialFields, &instance.InitialFieldValues); err != nil {
		return err
	}
	instance.Metadata["entity_id"] = record.EntityID
	if record.Create {
		return store.create(ctx, instance)
	}
	return store.mutateE(ctx, record.Route, func(current *WorkflowInstance) error {
		if current.Revision != record.ExpectedRevision || current.CurrentState != record.ExpectedState {
			return fmt.Errorf("pipeline test workflow state changed before commit")
		}
		instance.Revision = current.Revision
		*current = instance
		return nil
	})
}

var _ EnginePublicationPlanner = (*recordingPipelineBus)(nil)
var _ WorkflowEngineMutationOwner = (*recordingRuntimeMutationRunner)(nil)
var _ WorkflowTimerOccurrenceOwner = (*recordingRuntimeMutationRunner)(nil)
