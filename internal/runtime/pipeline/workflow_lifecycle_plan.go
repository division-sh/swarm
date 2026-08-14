package pipeline

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/attemptgeneration"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	decisioncard "github.com/division-sh/swarm/internal/runtime/decisioncard"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/gateruntime"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	runtimeworkflowlifecycle "github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
)

type WorkflowTimerMutationKind string

const (
	WorkflowTimerMutationInsert WorkflowTimerMutationKind = "insert"
	WorkflowTimerMutationCancel WorkflowTimerMutationKind = "cancel"
)

type WorkflowTimerMutation struct {
	Kind       WorkflowTimerMutationKind
	Activation WorkflowTimerActivation
}

func (m WorkflowTimerMutation) Validate(runID string, route runtimeflowidentity.Route, entityID string) error {
	m.Activation = m.Activation.normalized()
	if err := m.Activation.validate(); err != nil {
		return err
	}
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if m.Activation.RunID != strings.TrimSpace(runID) || m.Activation.EntityID != strings.TrimSpace(entityID) || m.Activation.Route != route {
		return fmt.Errorf("workflow timer mutation scope disagrees with engine state")
	}
	switch m.Kind {
	case WorkflowTimerMutationInsert:
		if m.Activation.Status != workflowTimerStatusActive {
			return fmt.Errorf("workflow timer insertion requires active status")
		}
	case WorkflowTimerMutationCancel:
		if m.Activation.Status != workflowTimerStatusActive {
			return fmt.Errorf("workflow timer cancellation requires the exact active record")
		}
	default:
		return fmt.Errorf("workflow timer mutation kind %q is unsupported", m.Kind)
	}
	return nil
}

type WorkflowLifecycleMutationPlan struct {
	Timers                     []WorkflowTimerMutation
	Schedules                  []WorkflowScheduleMutation
	GateCards                  []WorkflowGateCardMutation
	RequestCompletionCandidate bool
}

type WorkflowScheduleMutationKind string

const (
	WorkflowScheduleMutationUpsert WorkflowScheduleMutationKind = "upsert"
	WorkflowScheduleMutationCancel WorkflowScheduleMutationKind = "cancel"
)

type WorkflowScheduleMutation struct {
	Kind        WorkflowScheduleMutationKind
	Command     runtimegenericschedule.AdmissionCommand
	CancelCause string
	CancelledAt time.Time
}

func (m WorkflowScheduleMutation) Validate(runID string) error {
	m.Command = m.Command.Canonical()
	if strings.TrimSpace(m.Command.RunID) != strings.TrimSpace(runID) {
		return fmt.Errorf("workflow schedule mutation run disagrees with engine state")
	}
	if err := m.Command.Validate(); err != nil {
		return err
	}
	switch m.Kind {
	case WorkflowScheduleMutationUpsert:
		if strings.TrimSpace(m.CancelCause) != "" || !m.CancelledAt.IsZero() {
			return fmt.Errorf("workflow schedule admission cannot carry cancellation facts")
		}
		return nil
	case WorkflowScheduleMutationCancel:
		if strings.TrimSpace(m.CancelCause) == "" || m.CancelledAt.IsZero() {
			return fmt.Errorf("workflow schedule cancellation requires typed cause and time")
		}
		return nil
	default:
		return fmt.Errorf("workflow schedule mutation kind %q is unsupported", m.Kind)
	}
}

type WorkflowGateCardMutationKind string

const (
	WorkflowGateCardMutationCreate    WorkflowGateCardMutationKind = "create"
	WorkflowGateCardMutationSupersede WorkflowGateCardMutationKind = "supersede"
)

type WorkflowGateCardMutation struct {
	Kind         WorkflowGateCardMutationKind
	Card         decisioncard.Card
	EntityID     string
	ActivationID string
	Reason       string
	OccurredAt   time.Time
}

func (m WorkflowGateCardMutation) Validate(runID, entityID string) error {
	if err := m.Card.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(m.Card.RunID) != strings.TrimSpace(runID) || strings.TrimSpace(m.EntityID) != strings.TrimSpace(entityID) {
		return fmt.Errorf("workflow gate card mutation scope disagrees with engine state")
	}
	switch m.Kind {
	case WorkflowGateCardMutationCreate:
		if strings.TrimSpace(m.ActivationID) != "" || strings.TrimSpace(m.Reason) != "" || !m.OccurredAt.IsZero() {
			return fmt.Errorf("workflow gate card creation cannot carry supersession facts")
		}
	case WorkflowGateCardMutationSupersede:
		if strings.TrimSpace(m.ActivationID) == "" || strings.TrimSpace(m.Reason) == "" || m.OccurredAt.IsZero() {
			return fmt.Errorf("workflow gate card supersession requires exact activation, reason, and time")
		}
	default:
		return fmt.Errorf("workflow gate card mutation kind %q is unsupported", m.Kind)
	}
	return nil
}

type PreparedWorkflowLifecycleMutation struct {
	Commit    WorkflowLifecycleMutationPlan
	Emissions []runtimeengine.EmitIntent
}

func (p WorkflowLifecycleMutationPlan) Validate(runID string, route runtimeflowidentity.Route, entityID string) error {
	route = runtimeflowidentity.StoredRoute(route.ScopeKey, route.InstanceID, route.InstancePath)
	if strings.TrimSpace(runID) == "" || !route.Valid() || strings.TrimSpace(entityID) == "" {
		return fmt.Errorf("workflow lifecycle plan requires exact engine scope")
	}
	seen := make(map[string]WorkflowTimerMutationKind, len(p.Timers))
	for index, mutation := range p.Timers {
		if err := mutation.Validate(runID, route, entityID); err != nil {
			return fmt.Errorf("timer mutation %d: %w", index, err)
		}
		id := mutation.Activation.Ref.ActivationID
		if previous, exists := seen[id]; exists {
			return fmt.Errorf("workflow timer %s has both %s and %s mutations", id, previous, mutation.Kind)
		}
		seen[id] = mutation.Kind
	}
	for index, mutation := range p.Schedules {
		if err := mutation.Validate(runID); err != nil {
			return fmt.Errorf("schedule mutation %d: %w", index, err)
		}
	}
	for index, mutation := range p.GateCards {
		if err := mutation.Validate(runID, entityID); err != nil {
			return fmt.Errorf("gate card mutation %d: %w", index, err)
		}
	}
	return nil
}

type CommittedWorkflowLifecycleMutation struct {
	Wakeups                      []timeridentity.WorkflowTimerActivationRef
	Cancellations                []timeridentity.WorkflowTimerActivationRef
	GenericScheduleActivations   []runtimegenericschedule.Activation
	GenericScheduleCancellations []runtimegenericschedule.Activation
}

func (r CommittedWorkflowLifecycleMutation) Validate() error {
	seen := make(map[string]string, len(r.Wakeups)+len(r.Cancellations))
	for _, item := range []struct {
		name string
		refs []timeridentity.WorkflowTimerActivationRef
	}{
		{name: "wakeup", refs: r.Wakeups},
		{name: "cancellation", refs: r.Cancellations},
	} {
		for index, ref := range item.refs {
			ref = ref.Normalize()
			if !ref.Valid() {
				return fmt.Errorf("%s evidence %d is invalid", item.name, index)
			}
			if previous, exists := seen[ref.ActivationID]; exists {
				return fmt.Errorf("workflow timer %s repeats as %s and %s evidence", ref.ActivationID, previous, item.name)
			}
			seen[ref.ActivationID] = item.name
		}
	}
	for index, activation := range append(append([]runtimegenericschedule.Activation(nil), r.GenericScheduleActivations...), r.GenericScheduleCancellations...) {
		if err := activation.Validate(); err != nil {
			return fmt.Errorf("generic schedule evidence %d: %w", index, err)
		}
	}
	return nil
}

func (pc *PipelineCoordinator) prepareWorkflowLifecycleMutation(ctx context.Context, instance *WorkflowInstance, effects []runtimeworkflowlifecycle.Effect, reconcileGenerations bool) (PreparedWorkflowLifecycleMutation, error) {
	var prepared PreparedWorkflowLifecycleMutation
	if len(effects) == 0 && !reconcileGenerations {
		return prepared, nil
	}
	if pc == nil || instance == nil || pc.workflowStore == nil || !pc.workflowStore.enabled() || pc.SemanticSource() == nil {
		return prepared, fmt.Errorf("workflow lifecycle planning requires selected workflow persistence and semantic source")
	}
	mode, err := workflowLifecycleExecutionMode(effects)
	if err != nil {
		return PreparedWorkflowLifecycleMutation{}, err
	}
	for index, effect := range effects {
		if err := pc.planWorkflowLifecycleEffect(ctx, instance, effect, &prepared); err != nil {
			return PreparedWorkflowLifecycleMutation{}, fmt.Errorf("workflow lifecycle effect %d: %w", index, err)
		}
	}
	if reconcileGenerations {
		if err := pc.planSupersededWorkflowArtifacts(ctx, instance, effects[0].Route(), effects[0].EntityID(), mode, &prepared.Commit); err != nil {
			return PreparedWorkflowLifecycleMutation{}, err
		}
	}
	return prepared, nil
}

func workflowLifecycleExecutionMode(effects []runtimeworkflowlifecycle.Effect) (executionmode.Mode, error) {
	if len(effects) == 0 {
		return "", fmt.Errorf("workflow lifecycle planning requires an execution-mode-bearing effect")
	}
	mode := effects[0].ExecutionMode()
	if !mode.Valid() {
		return "", fmt.Errorf("workflow lifecycle effect 0 requires typed execution mode authority")
	}
	for index := 1; index < len(effects); index++ {
		candidate := effects[index].ExecutionMode()
		if !candidate.Valid() || candidate != mode {
			return "", fmt.Errorf("workflow lifecycle effect %d execution mode conflicts with the mutation", index)
		}
	}
	return mode, nil
}

func (pc *PipelineCoordinator) planSupersededWorkflowArtifacts(ctx context.Context, instance *WorkflowInstance, route runtimeflowidentity.Route, entityID identity.EntityID, mode executionmode.Mode, plan *WorkflowLifecycleMutationPlan) error {
	if instance == nil || plan == nil {
		return nil
	}
	carrier, err := workflowInstanceStateCarrier(*instance)
	if err != nil {
		return fmt.Errorf("decode current loop state: %w", err)
	}
	loops, err := loopruntime.List(carrier.StateBuckets)
	if err != nil {
		return fmt.Errorf("list current loop state: %w", err)
	}
	current := make([]attemptgeneration.Generation, 0, len(loops))
	for _, activation := range loops {
		if generation := activation.Generation(); generation.Valid() && activation.Status == loopruntime.StatusOpen {
			current = append(current, generation)
		}
	}
	if _, err := requireWorkflowInstanceIdentity(route, entityID, *instance); err != nil {
		return fmt.Errorf("validate loop artifact owner: %w", err)
	}
	runID := workflowTimerRunID(ctx, *instance)
	if runID == "" || entityID.IsZero() {
		return fmt.Errorf("loop artifact reconciliation requires exact run and entity identity")
	}
	active, err := pc.workflowStore.listPersistedWorkflowTimerActivations(ctx, runID, entityID.String(), true)
	if err != nil {
		return err
	}
	for _, activation := range active {
		if !activation.Ref.Generation.Valid() || workflowTimerGenerationPresent(current, activation.Ref.Generation) || workflowLifecycleHasTimerMutation(plan.Timers, activation.Ref.ActivationID) {
			continue
		}
		plan.Timers = append(plan.Timers, WorkflowTimerMutation{Kind: WorkflowTimerMutationCancel, Activation: activation})
		plan.RequestCompletionCandidate = true
	}
	joins, err := joinruntime.List(carrier.StateBuckets)
	if err != nil {
		return fmt.Errorf("list current joins: %w", err)
	}
	joinStateChanged := false
	for _, activation := range joins {
		if activation.Status != joinruntime.StatusClosed || activation.TimerCancelled || activation.TimerTaskID() == "" {
			continue
		}
		if activation.TimerEventType() == joinCompleteEvent && activation.OutcomePending && !activation.OutcomeFired {
			continue
		}
		command, err := joinSchedule(pc.SemanticSource(), entityID.String(), route, activation, mode)
		if err != nil {
			return err
		}
		command.RunID = workflowTimerRunID(ctx, *instance)
		if workflowLifecycleHasScheduleMutation(plan.Schedules, command.ScheduleKey) {
			continue
		}
		activation.TimerCancelled = true
		if err := joinruntime.Store(carrier.StateBuckets, activation); err != nil {
			return fmt.Errorf("persist closed join timer cancellation %s: %w", activation.Key(), err)
		}
		joinStateChanged = true
		plan.Schedules = append(plan.Schedules, WorkflowScheduleMutation{Kind: WorkflowScheduleMutationCancel, Command: command, CancelCause: "join_closed", CancelledAt: time.Now().UTC()})
		plan.RequestCompletionCandidate = true
	}
	if joinStateChanged {
		instance.StateBuckets = carrier.PersistedStateBuckets()
	}
	return nil
}

func workflowLifecycleHasTimerMutation(items []WorkflowTimerMutation, activationID string) bool {
	for _, item := range items {
		if item.Activation.Ref.ActivationID == activationID {
			return true
		}
	}
	return false
}

func workflowLifecycleHasScheduleMutation(items []WorkflowScheduleMutation, scheduleKey string) bool {
	for _, item := range items {
		if strings.TrimSpace(item.Command.ScheduleKey) == strings.TrimSpace(scheduleKey) {
			return true
		}
	}
	return false
}

func (pc *PipelineCoordinator) planWorkflowLifecycleEffect(ctx context.Context, instance *WorkflowInstance, effect runtimeworkflowlifecycle.Effect, prepared *PreparedWorkflowLifecycleMutation) error {
	entityID := effect.EntityID()
	route := runtimeflowidentity.StoredRoute(effect.Route().ScopeKey, effect.Route().InstanceID, effect.Route().InstancePath)
	if effect.OccurredAt().IsZero() {
		return fmt.Errorf("workflow lifecycle effect disagrees with the prepared instance scope")
	}
	if _, err := requireWorkflowInstanceIdentity(route, entityID, *instance); err != nil {
		return fmt.Errorf("workflow lifecycle effect disagrees with the prepared instance scope: %w", err)
	}
	fromState, toState := "", ""
	cause := workflowTimerCause{OccurredAt: effect.OccurredAt(), ExecutionMode: effect.ExecutionMode()}
	switch effect.Kind() {
	case runtimeworkflowlifecycle.KindInitialEntry:
		toState = strings.TrimSpace(effect.InitialStage())
		cause.Kind = workflowTimerCauseInitial
		cause.EventType = "state:" + toState
		cause.ToState = toState
	case runtimeworkflowlifecycle.KindAcceptedEvent:
		cause.Kind = workflowTimerCauseEvent
		cause.EventID = effect.EventID()
		cause.EventType = effect.EventType()
		if transition, ok := effect.Transition(); ok {
			fromState, toState = transition.From(), transition.To()
			cause.Kind = workflowTimerCauseTransition
			cause.TransitionID = transition.ID()
			cause.FromState, cause.ToState = fromState, toState
		}
	default:
		return fmt.Errorf("workflow lifecycle effect kind is unsupported")
	}
	if err := pc.planWorkflowTimerEffect(ctx, *instance, route, entityID, fromState, toState, cause, &prepared.Commit); err != nil {
		return err
	}
	if err := pc.planWorkflowJoinEffect(ctx, instance, route, entityID, fromState, toState, effect.ExecutionMode(), effect.OccurredAt(), &prepared.Commit); err != nil {
		return err
	}
	if err := pc.planWorkflowGateEffect(ctx, instance, route, entityID, fromState, toState, cause.EventType, effect.OccurredAt(), prepared); err != nil {
		return err
	}
	return nil
}

func (pc *PipelineCoordinator) planWorkflowTimerEffect(ctx context.Context, instance WorkflowInstance, route runtimeflowidentity.Route, entityID identity.EntityID, currentState, nextState string, cause workflowTimerCause, plan *WorkflowLifecycleMutationPlan) error {
	source := pc.SemanticSource()
	runID := workflowTimerRunID(ctx, instance)
	if runID == "" {
		return fmt.Errorf("workflow timer lifecycle requires run identity")
	}
	active, err := pc.workflowStore.listPersistedWorkflowTimerActivations(ctx, runID, entityID.String(), true)
	if err != nil {
		return err
	}
	activeByDeclaration := make(map[string]WorkflowTimerActivation, len(active))
	for _, activation := range active {
		declaration, found := workflowTimerDeclarationForInstance(source, instance, activation.Ref.Declaration)
		if !found {
			return fmt.Errorf("active workflow timer %s references unknown declaration %s", activation.Ref.ActivationID, activation.Ref.Declaration)
		}
		if err := validateWorkflowTimerTopology(source, declaration); err != nil {
			return err
		}
		if workflowTimerShouldCancelOnTransition(declaration, currentState, nextState, cause.EventType) {
			plan.Timers = append(plan.Timers, WorkflowTimerMutation{Kind: WorkflowTimerMutationCancel, Activation: activation})
			plan.RequestCompletionCandidate = true
			continue
		}
		activeByDeclaration[workflowTimerGenerationKey(activation.Ref.Declaration, activation.Ref.Generation)] = activation
	}

	generationStage := firstNonEmptyString(nextState, currentState, instance.CurrentState)
	generation, _, err := workflowLoopGenerationForStage(source, &instance, generationStage)
	if err != nil {
		return err
	}
	for _, declaration := range workflowTimerDeclarationsForInstance(source, instance) {
		if !workflowTimerShouldStartOnTransition(declaration, currentState, nextState, cause.EventType) {
			continue
		}
		if err := validateWorkflowTimerTopology(source, declaration); err != nil {
			return err
		}
		if err := cause.validateForActivation(); err != nil {
			return err
		}
		interval := workflowTimerDuration(declaration, workflowTimerPolicy(source, declaration.FlowID))
		if interval <= 0 {
			return fmt.Errorf("workflow timer %s has no executable positive delay", declaration.ID)
		}
		activation, err := workflowTimerActivationForCause(source, runID, entityID.String(), route, declaration, generation, cause, interval)
		if err != nil {
			return err
		}
		key := workflowTimerGenerationKey(declaration.ID, generation)
		if existing, found := activeByDeclaration[key]; found {
			if existing.Ref != activation.Ref {
				return fmt.Errorf("active workflow timer %s conflicts with the exact causal activation %s", existing.Ref.ActivationID, activation.Ref.ActivationID)
			}
			continue
		}
		plan.Timers = append(plan.Timers, WorkflowTimerMutation{Kind: WorkflowTimerMutationInsert, Activation: activation})
	}
	return nil
}

func (pc *PipelineCoordinator) planWorkflowJoinEffect(ctx context.Context, instance *WorkflowInstance, route runtimeflowidentity.Route, entityID identity.EntityID, currentStage, nextStage string, mode executionmode.Mode, occurredAt time.Time, plan *WorkflowLifecycleMutationPlan) error {
	if !mode.Valid() {
		return fmt.Errorf("workflow join lifecycle requires an exact causal execution mode")
	}
	currentStage = strings.TrimSpace(currentStage)
	nextStage = strings.TrimSpace(nextStage)
	if instance == nil || entityID.IsZero() || nextStage == "" {
		return nil
	}
	carrier, err := workflowInstanceStateCarrier(*instance)
	if err != nil {
		return fmt.Errorf("decode join state: %w", err)
	}
	activations, err := joinruntime.List(carrier.StateBuckets)
	if err != nil {
		return fmt.Errorf("list join state: %w", err)
	}
	for _, activation := range activations {
		if activation.Stage() != currentStage || activation.Stage() == nextStage || !activation.CloseForStageExit() {
			continue
		}
		activation.TimerCancelled = true
		if err := joinruntime.Store(carrier.StateBuckets, activation); err != nil {
			return fmt.Errorf("close join %s on stage exit: %w", activation.Key(), err)
		}
		command, err := joinSchedule(pc.SemanticSource(), entityID.String(), route, activation, mode)
		if err != nil {
			return err
		}
		command.RunID = workflowTimerRunID(ctx, *instance)
		plan.Schedules = append(plan.Schedules, WorkflowScheduleMutation{Kind: WorkflowScheduleMutationCancel, Command: command, CancelCause: "join_stage_exit", CancelledAt: occurredAt})
		plan.RequestCompletionCandidate = true
	}
	now := occurredAt.UTC()
	if now.IsZero() {
		return fmt.Errorf("workflow join lifecycle requires an exact occurrence time")
	}
	for _, joinPlan := range workflowJoinPlansForStage(pc.SemanticSource(), route, nextStage) {
		if joinPlan.ResultType.Empty() {
			return fmt.Errorf("join %s has no resolved output type in the semantic plan", joinPlan.Spec.EffectiveID())
		}
		members, ok := joinMemberSnapshot(instance.Fields, joinPlan.Spec.Members.From)
		if !ok {
			return fmt.Errorf("join %s members source %s is not a unique list of non-empty text", joinPlan.Spec.EffectiveID(), joinPlan.Spec.Members.From)
		}
		window := ""
		if joinPlan.Spec.Window != nil {
			window = strings.TrimSpace(asString(instance.Fields[joinTopLevelField(joinPlan.Spec.Window.From, "entity")]))
			if window == "" {
				return fmt.Errorf("join %s window source %s resolved empty", joinPlan.Spec.EffectiveID(), joinPlan.Spec.Window.From)
			}
		}
		generation, _, err := workflowLoopGenerationForStage(pc.SemanticSource(), instance, nextStage)
		if err != nil {
			return err
		}
		key := joinruntime.ActivationKeyForGeneration(joinPlan.Spec.Stage, joinPlan.Spec.EffectiveID(), window, generation)
		if _, found, err := joinruntime.Load(carrier.StateBuckets, joinPlan.NodeID, key); err != nil {
			return fmt.Errorf("load join %s: %w", key, err)
		} else if found {
			continue
		}
		delay := workflowTimerRenderedDelay(joinPlan.Spec.Timeout.After, workflowTimerPolicy(pc.SemanticSource(), joinPlan.FlowID))
		interval, ok := timeridentity.ParseDelayDuration(delay)
		if !ok {
			return fmt.Errorf("join %s timeout.after %q did not resolve to a positive duration", joinPlan.Spec.EffectiveID(), joinPlan.Spec.Timeout.After)
		}
		ref, err := timeridentity.NewJoinRefForGeneration(joinPlan.FlowID, joinPlan.NodeID, joinPlan.HandlerEvent, joinPlan.Spec.Stage, joinPlan.Spec.EffectiveID(), window, generation)
		if err != nil {
			return fmt.Errorf("arm join %s identity: %w", joinPlan.Spec.EffectiveID(), err)
		}
		handle, err := timeridentity.JoinTimeoutHandle(ref)
		if err != nil {
			return fmt.Errorf("arm join %s timer: %w", joinPlan.Spec.EffectiveID(), err)
		}
		activation, err := joinruntime.NewActivation(handle, members, now, now.Add(interval))
		if err != nil {
			return fmt.Errorf("arm join %s: %w", joinPlan.Spec.EffectiveID(), err)
		}
		complete, err := joinruntime.CompletionSatisfied(activation, joinPlan.Spec.CompleteWhen, func(expression string, joinContext map[string]any) (bool, error) {
			return workflowexpr.EvalJoinBool(expression, joinContext, joinPlan.ResultType)
		})
		if err != nil {
			return fmt.Errorf("evaluate join %s completion at arm: %w", joinPlan.Spec.EffectiveID(), err)
		}
		if complete {
			activation.Close(joinruntime.CloseReasonComplete, true, false)
			completionHandle, handleErr := timeridentity.JoinCompleteHandle(ref)
			if handleErr != nil {
				return fmt.Errorf("arm join %s completion timer: %w", joinPlan.Spec.EffectiveID(), handleErr)
			}
			activation, err = activation.WithTimerHandle(completionHandle, now)
			if err != nil {
				return fmt.Errorf("arm join %s completion identity: %w", joinPlan.Spec.EffectiveID(), err)
			}
		}
		if err := joinruntime.Store(carrier.StateBuckets, activation); err != nil {
			return fmt.Errorf("persist join %s: %w", activation.Key(), err)
		}
		command, err := joinSchedule(pc.SemanticSource(), entityID.String(), route, activation, mode)
		if err != nil {
			return err
		}
		command.RunID = workflowTimerRunID(ctx, *instance)
		plan.Schedules = append(plan.Schedules, WorkflowScheduleMutation{Kind: WorkflowScheduleMutationUpsert, Command: command})
	}
	instance.StateBuckets = carrier.PersistedStateBuckets()
	return nil
}

func (pc *PipelineCoordinator) planWorkflowGateEffect(ctx context.Context, instance *WorkflowInstance, route runtimeflowidentity.Route, entityID identity.EntityID, currentStage, nextStage, sourceEvent string, occurredAt time.Time, prepared *PreparedWorkflowLifecycleMutation) error {
	currentStage = strings.TrimSpace(currentStage)
	nextStage = strings.TrimSpace(nextStage)
	if instance == nil || entityID.IsZero() || nextStage == "" || currentStage == nextStage {
		return nil
	}
	now := occurredAt.UTC()
	if now.IsZero() {
		return fmt.Errorf("workflow gate lifecycle requires an exact occurrence time")
	}
	carrier, err := workflowInstanceStateCarrier(*instance)
	if err != nil {
		return fmt.Errorf("decode gate state: %w", err)
	}
	activations, err := gateruntime.List(carrier.StateBuckets)
	if err != nil {
		return fmt.Errorf("list gate activations: %w", err)
	}
	for _, activation := range activations {
		if activation.Stage != currentStage || activation.Stage == nextStage {
			continue
		}
		if activation.Status == gateruntime.StatusDecisionCommitted {
			return fmt.Errorf("stage %s cannot exit while decision card %s has a committed verdict awaiting its frozen route", currentStage, activation.CardID)
		}
		if !activation.Supersede(firstNonEmptyString(sourceEvent, "stage_exited"), now) {
			continue
		}
		if err := gateruntime.Store(carrier.StateBuckets, activation); err != nil {
			return err
		}
		if pc.decisionCards == nil {
			return fmt.Errorf("decision card store is required to supersede gate activation %s", activation.ActivationID)
		}
		card, err := pc.decisionCards.GetDecisionCard(ctx, activation.CardID)
		if err != nil {
			return fmt.Errorf("load decision card %s for supersession: %w", activation.CardID, err)
		}
		evt, err := workflowGateSupersededEvent(card, activation, *instance, now)
		if err != nil {
			return err
		}
		prepared.Commit.GateCards = append(prepared.Commit.GateCards, WorkflowGateCardMutation{
			Kind: WorkflowGateCardMutationSupersede, Card: card, EntityID: entityID.String(),
			ActivationID: activation.ActivationID, Reason: activation.SupersededReason, OccurredAt: now,
		})
		prepared.Commit.RequestCompletionCandidate = true
		prepared.Emissions = append(prepared.Emissions, runtimeengine.EmitIntent{Event: evt})
	}
	flowID, gatePlan, ok := workflowGatePlanForInstance(pc, *instance, nextStage)
	if ok {
		if pc.decisionCards == nil {
			return fmt.Errorf("decision card store is required before entering gated stage %s", nextStage)
		}
		if existing, found, err := gateruntime.Load(carrier.StateBuckets, flowID, gatePlan.Decision); err != nil {
			return err
		} else if !found || existing.Stage != nextStage || existing.Status != gateruntime.StatusOpen && existing.Status != gateruntime.StatusDecisionCommitted {
			runID := workflowTimerRunID(ctx, *instance)
			bundleHash := workflowGateBundleHash(ctx, pc)
			if runID == "" || bundleHash == "" {
				return fmt.Errorf("run and bundle identity are required before entering gated stage %s", nextStage)
			}
			enteredAt := instance.EnteredStageAt.UTC()
			if enteredAt.IsZero() {
				enteredAt = now
			}
			frozenOutcomes, err := pc.resolvedWorkflowGateOutcomes(gatePlan)
			if err != nil {
				return err
			}
			routesJSON, err := gateruntime.FreezeRoutes(frozenOutcomes)
			if err != nil {
				return fmt.Errorf("freeze gate %s continuation routes: %w", gatePlan.Decision, err)
			}
			activation, err := gateruntime.New(runID, route.InstancePath, entityID.String(), flowID, nextStage, gatePlan.Decision, bundleHash, routesJSON, sourceEvent, enteredAt)
			if err != nil {
				return err
			}
			if err := gateruntime.Store(carrier.StateBuckets, activation); err != nil {
				return err
			}
			card, err := pc.buildWorkflowDecisionCard(ctx, route, entityID, *instance, activation, gatePlan, frozenOutcomes)
			if err != nil {
				return err
			}
			prepared.Commit.GateCards = append(prepared.Commit.GateCards, WorkflowGateCardMutation{Kind: WorkflowGateCardMutationCreate, Card: card, EntityID: entityID.String()})
		}
	}
	instance.StateBuckets = carrier.PersistedStateBuckets()
	return nil
}

func (pc *PipelineCoordinator) finalizeWorkflowLifecycleMutation(ctx context.Context, committed CommittedWorkflowLifecycleMutation) error {
	if err := committed.Validate(); err != nil {
		return err
	}
	if len(committed.Wakeups)+len(committed.Cancellations)+len(committed.GenericScheduleActivations)+len(committed.GenericScheduleCancellations) == 0 {
		return nil
	}
	if pc == nil {
		return fmt.Errorf("committed workflow lifecycle evidence requires the pipeline coordinator")
	}
	if len(committed.Wakeups)+len(committed.Cancellations) > 0 && pc.workflowTimers == nil && pc.timerScheduler != nil {
		return fmt.Errorf("committed workflow timer evidence requires the lifecycle owner")
	}
	if len(committed.GenericScheduleActivations)+len(committed.GenericScheduleCancellations) > 0 && pc.genericSchedules == nil {
		return fmt.Errorf("committed generic schedule evidence requires the lifecycle owner")
	}
	for _, activation := range append(append([]runtimegenericschedule.Activation(nil), committed.GenericScheduleCancellations...), committed.GenericScheduleActivations...) {
		if _, err := pc.genericSchedules.ReconcileWakeupWithRecovery(ctx, activation.ID); err != nil {
			return err
		}
	}
	if pc.workflowTimers != nil && pc.timerScheduler != nil {
		for _, ref := range committed.Cancellations {
			if err := pc.workflowTimers.queueWakeupReconcile(ctx, ref); err != nil {
				return err
			}
		}
		for _, ref := range committed.Wakeups {
			if err := pc.workflowTimers.queueWakeupReconcile(ctx, ref); err != nil {
				return err
			}
		}
	}
	return nil
}
