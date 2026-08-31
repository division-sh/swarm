package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/handlerselection"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	runtimeregistry "github.com/division-sh/swarm/internal/runtime/core/registry"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimeeventpayload "github.com/division-sh/swarm/internal/runtime/eventpayload"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
)

type pipelineEngineEvaluator struct {
	evaluator   *workflowExpressionEvaluator
	coordinator *PipelineCoordinator
}

func (e pipelineEngineEvaluator) EvalBool(expression string, ctx runtimeengine.BaseContext) (bool, error) {
	if e.evaluator == nil {
		return false, runtimeengine.ErrNotImplemented
	}
	queryCtx := workflowExpressionContext{
		Entity:         cloneStringAnyMap(ctx.Entity.Raw()),
		PlatformEntity: cloneStringAnyMap(ctx.PlatformEntity.Raw()),
		Event:          cloneStringAnyMap(ctx.Event.Raw()),
		Payload:        cloneStringAnyMap(ctx.Payload.Raw()),
		Policy:         cloneStringAnyMap(ctx.Policy.Raw()),
		Computed:       cloneStringAnyMap(ctx.Computed.Raw()),
		Accumulated:    accumulatedItemsForCEL(ctx.Accumulated.Raw()),
		FanOut:         cloneStringAnyMap(ctx.FanOut.Raw()),
		Join:           cloneStringAnyMap(ctx.Join.Raw()),
		Loop:           cloneStringAnyMap(ctx.Loop.Raw()),
		WorkflowName:   firstNonEmptyString(strings.TrimSpace(ctx.FlowID), e.workflowName()),
	}
	queryCtx.QueryEntityCount = func(predicate string) (int, error) {
		return e.queryEntityCount(queryCtx, predicate)
	}
	options := workflowexpr.ValueExpressionOptions{AllowAccumulated: true}
	if workflowexpr.ExpressionReferencesRoot(expression, "payload") {
		eventType := strings.TrimSpace(asString(queryCtx.Event["trigger_event_type"]))
		resolution := semanticview.ResolveEventSchema(e.coordinator.SemanticSource(), ctx.FlowID, eventType)
		if !resolution.HasStructural {
			return false, fmt.Errorf("workflow payload expression for %s has no exact structural schema", eventType)
		}
		payloadType := resolution.StructuralType.Clone()
		options.PayloadType = &payloadType
	}
	return e.evaluator.EvalBoolWithOptions(expression, queryCtx, options)
}

func (e pipelineEngineEvaluator) workflowName() string {
	if e.coordinator == nil || e.coordinator.module == nil || e.coordinator.module.WorkflowDefinition() == nil {
		return ""
	}
	return strings.TrimSpace(e.coordinator.module.WorkflowDefinition().Name)
}

func accumulatedItemsForCEL(raw map[string]any) any {
	if len(raw) == 0 {
		return []any{}
	}
	if items, ok := raw["items"].([]any); ok {
		return cloneAccumulatedItems(items)
	}
	if items, ok := raw["items"].([]map[string]any); ok {
		out := make([]any, 0, len(items))
		for _, item := range items {
			out = append(out, cloneStringAnyMap(item))
		}
		return out
	}
	return []any{}
}

func (e pipelineEngineEvaluator) EvalValue(string, runtimeengine.BaseContext) (any, error) {
	return nil, runtimeengine.ErrNotImplemented
}

type pipelineEngineMutationOwner struct {
	store       *workflowInstanceStore
	state       pipelineEngineStateRepo
	publication EnginePublicationPlanner
	verifier    runtimeengine.EmitPersistenceVerifier
	lifecycle   runtimeengine.WorkflowLifecycleEffectOwner
	activities  runtimeengine.ActivityIntentWriter
}

func beginWorkflowEngineDeliverySuccess(ctx context.Context, selection handlerselection.HandlerRuleSelectionFact) (*WorkflowEngineDeliverySuccess, *runtimedelivery.ClaimSettlementGuard, error) {
	claim, hasClaim := runtimedelivery.ClaimFromContext(ctx)
	heartbeat, hasHeartbeat := runtimedelivery.ClaimHeartbeatFromContext(ctx)
	if !hasClaim && !hasHeartbeat {
		return nil, nil, nil
	}
	if !hasClaim || !hasHeartbeat {
		return nil, nil, fmt.Errorf("workflow engine delivery settlement requires the exact claim and heartbeat")
	}
	if !heartbeat.Owns(claim) {
		return nil, nil, fmt.Errorf("workflow engine delivery heartbeat disagrees with the inbound claim")
	}
	guard, err := heartbeat.BeginSettlement()
	if err != nil {
		return nil, nil, err
	}
	return &WorkflowEngineDeliverySuccess{
		Claim: claim, SideEffects: []string{"handler_completed"}, Duration: heartbeat.ExecutionDuration(),
		RuleSelection: admittedHandlerRuleSelection(selection),
	}, guard, nil
}

func finishWorkflowEngineDeliverySuccess(
	plan *WorkflowEngineDeliverySuccess,
	guard *runtimedelivery.ClaimSettlementGuard,
	committed *runtimedelivery.Claim,
) (*runtimedelivery.Claim, error) {
	if plan == nil {
		if guard != nil || committed != nil {
			return nil, fmt.Errorf("workflow engine returned undeclared delivery settlement evidence")
		}
		return nil, nil
	}
	if guard == nil {
		return nil, fmt.Errorf("workflow engine delivery settlement guard is required")
	}
	exact := committed != nil && committed.Same(plan.Claim)
	var result *runtimedelivery.Claim
	var evidenceErr error
	if committed == nil {
		evidenceErr = fmt.Errorf("workflow engine did not return declared delivery settlement evidence")
	} else if !exact {
		evidenceErr = fmt.Errorf("workflow engine committed a different delivery claim")
	}
	if exact {
		claim := *committed
		result = &claim
	}
	var finishErr error
	if exact {
		finishErr = guard.MarkCommitted()
	} else {
		guard.Abort()
	}
	return result, errors.Join(evidenceErr, finishErr)
}

func (o pipelineEngineMutationOwner) CommitEngineMutation(ctx context.Context, mutation runtimeengine.EngineMutation) (runtimeengine.CommittedEngineMutation, error) {
	if o.store != nil && o.store.enabled() {
		if o.store.engineMutations == nil {
			return runtimeengine.CommittedEngineMutation{}, fmt.Errorf("selected workflow engine mutation owner is required")
		}
		application, hasApplication := deliveryTargetApplicationFromContext(ctx)
		if hasApplication {
			if err := application.Validate(); err != nil {
				return runtimeengine.CommittedEngineMutation{}, err
			}
			if mutation.Address.Route != application.Route() || mutation.Address.EntityID.String() != application.EntityID() {
				return runtimeengine.CommittedEngineMutation{}, fmt.Errorf("engine mutation address disagrees with admitted delivery target application")
			}
			if application.Owner().EntitylessReceiver() {
				return o.commitEntitylessEngineMutation(ctx, mutation, application.Owner())
			}
		} else if _, stamped := stampedDeliveryTargetOwnership(ctx); stamped {
			return runtimeengine.CommittedEngineMutation{}, fmt.Errorf("stamped engine mutation requires delivery target application")
		}
		var targetApplications []DeliveryTargetApplication
		if hasApplication {
			targetApplications = append(targetApplications, application)
		}
		preparedState, err := o.state.prepareMutation(ctx, mutation.Address, mutation.State, targetApplications...)
		if err != nil {
			return runtimeengine.CommittedEngineMutation{}, err
		}
		lifecycle, err := o.state.coordinator.prepareWorkflowLifecycleMutation(
			ctx,
			&preparedState.instance,
			mutation.LifecycleEffects,
			len(mutation.LifecycleEffects) > 0,
		)
		if err != nil {
			return runtimeengine.CommittedEngineMutation{}, err
		}
		state, err := preparedState.record()
		if err != nil {
			return runtimeengine.CommittedEngineMutation{}, err
		}
		var publications []runtimeengine.DurablePublicationPlan
		emissionIntents := append([]runtimeengine.EmitIntent(nil), mutation.EmitIntents...)
		emissionIntents = append(emissionIntents, lifecycle.Emissions...)
		if len(emissionIntents) > 0 && len(mutation.EmitPrerequisites.Fields) > 0 {
			if err := verifyPreparedWorkflowEmitPersistence(preparedState.instance, mutation.EmitPrerequisites); err != nil {
				return runtimeengine.CommittedEngineMutation{}, err
			}
		}
		proposedEffects := make([]WorkflowEngineProposedEffect, 0, len(mutation.ActivityIntents))
		immediateActivities := make([]runtimeengine.ActivityIntent, 0, len(mutation.ActivityIntents))
		for _, value := range mutation.ActivityIntents {
			intent := value.Normalized()
			if intent.ApprovalDecision == "" {
				immediateActivities = append(immediateActivities, intent)
				continue
			}
			if o.state.coordinator == nil {
				return runtimeengine.CommittedEngineMutation{}, fmt.Errorf("approved activity requires pipeline coordinator")
			}
			card, continuation, err := o.state.coordinator.buildProposedEffectCard(ctx, intent)
			if err != nil {
				return runtimeengine.CommittedEngineMutation{}, err
			}
			proposedEffects = append(proposedEffects, WorkflowEngineProposedEffect{Card: card, Continuation: continuation})
		}
		activityPublications, err := activityRequestEmitIntents(immediateActivities)
		if err != nil {
			return runtimeengine.CommittedEngineMutation{}, err
		}
		emissionIntents = append(emissionIntents, activityPublications...)
		if len(emissionIntents) > 0 {
			if o.publication == nil {
				return runtimeengine.CommittedEngineMutation{}, fmt.Errorf("engine publication planner is required")
			}
			publications, err = o.publication.PrepareEnginePublications(ctx, emissionIntents)
			if err != nil {
				return runtimeengine.CommittedEngineMutation{}, err
			}
		}
		postCommit := WorkflowEnginePostCommitPlan{FlowDeactivation: &WorkflowEngineFlowDeactivation{
			Route: mutation.Address.Route, EntityID: mutation.Address.EntityID.String(), NextState: state.CurrentState,
		}}
		deliverySuccess, settlementGuard, err := beginWorkflowEngineDeliverySuccess(ctx, mutation.HandlerRuleSelection)
		if err != nil {
			if o.publication != nil {
				err = errors.Join(err, o.publication.ReleaseEnginePublications(context.WithoutCancel(ctx), publications))
			}
			return runtimeengine.CommittedEngineMutation{}, err
		}
		committed, commitErr := o.store.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{
			State: state, Lifecycle: lifecycle.Commit,
			ProposedEffects: proposedEffects, Publications: publications, DeliverySuccess: deliverySuccess, PostCommit: postCommit,
			FanOutIntent:            mutation.FanOutIntent,
			FanOutBarrier:           mutation.FanOutBarrier,
			FanOutBarrierCompletion: mutation.FanOutBarrierCompletion,
		})
		if commitErr == nil && mutation.FanOutIntent != nil {
			o.state.coordinator.signalFanOutWork()
		}
		settledClaim, settlementErr := finishWorkflowEngineDeliverySuccess(deliverySuccess, settlementGuard, committed.DeliverySuccess)
		commitErr = errors.Join(commitErr, settlementErr)
		if commitErr != nil && settledClaim == nil {
			if o.publication != nil {
				commitErr = errors.Join(commitErr, o.publication.ReleaseEnginePublications(context.WithoutCancel(ctx), publications))
			}
			return runtimeengine.CommittedEngineMutation{}, commitErr
		}
		engineCommit := runtimeengine.CommittedEngineMutation{SettledDeliveryClaim: settledClaim}
		var postCommitErr error
		if o.publication != nil {
			if err := o.publication.FinalizeEnginePublications(ctx, committed.Publications); err != nil {
				postCommitErr = errors.Join(postCommitErr, err)
			}
		}
		if err := o.state.coordinator.finalizeWorkflowLifecycleMutation(ctx, committed.Lifecycle); err != nil {
			postCommitErr = errors.Join(postCommitErr, err)
		}
		if deactivation := committed.PostCommit.FlowDeactivation; deactivation != nil {
			if err := o.state.coordinator.maybeDeactivateTerminalFlowInstance(ctx, deactivation.Route, identity.NormalizeEntityID(deactivation.EntityID), deactivation.NextState); err != nil {
				postCommitErr = errors.Join(postCommitErr, err)
			}
		}
		if len(committed.Publications) < len(mutation.EmitIntents) {
			postCommitErr = errors.Join(postCommitErr, fmt.Errorf("committed engine publications = %d, want at least %d emitted events", len(committed.Publications), len(mutation.EmitIntents)))
			return engineCommit, errors.Join(commitErr, postCommitErr)
		}
		committedIntents := make([]runtimeengine.EmitIntent, 0, len(mutation.EmitIntents))
		for index, publication := range committed.Publications[:len(mutation.EmitIntents)] {
			if publication == nil {
				return engineCommit, errors.Join(commitErr, postCommitErr, fmt.Errorf("committed engine publication %d is required", index))
			}
			intent := publication.CommittedDurablePublicationIntent()
			if strings.TrimSpace(intent.Event.ID()) != strings.TrimSpace(publication.CommittedDurablePublicationEventID()) {
				return engineCommit, errors.Join(commitErr, postCommitErr, fmt.Errorf("committed engine publication %d intent identity is inconsistent", index))
			}
			committedIntents = append(committedIntents, intent)
		}
		engineCommit.ActivityIntents = append([]runtimeengine.ActivityIntent(nil), mutation.ActivityIntents...)
		engineCommit.EmitIntents = committedIntents
		if err := errors.Join(commitErr, postCommitErr); err != nil {
			return engineCommit, err
		}
		return engineCommit, nil
	}
	commit := func(txctx context.Context) error {
		if mutation.FanOutIntent != nil {
			return fmt.Errorf("durable fan-out intent requires selected workflow persistence")
		}
		if o.state.coordinator == nil {
			return runtimeengine.ErrMissingStateRepo
		}
		if err := o.state.SaveState(txctx, mutation.Address, mutation.State); err != nil {
			return err
		}
		if len(mutation.LifecycleEffects) > 0 {
			if o.lifecycle == nil {
				return fmt.Errorf("workflow lifecycle owner is required for engine mutation")
			}
			if err := o.lifecycle.ApplyWorkflowLifecycleEffects(txctx, mutation.LifecycleEffects); err != nil {
				return err
			}
		}
		if len(mutation.ActivityIntents) > 0 {
			if o.activities == nil {
				return fmt.Errorf("activity intent owner is required for engine mutation")
			}
			if err := o.activities.WriteActivityIntents(txctx, mutation.ActivityIntents); err != nil {
				return err
			}
		}
		if len(mutation.EmitIntents) > 0 && o.verifier != nil && len(mutation.EmitPrerequisites.Fields) > 0 {
			if err := o.verifier.VerifyEmitPersistence(txctx, mutation.Address, mutation.EmitPrerequisites); err != nil {
				return err
			}
		}
		return nil
	}
	if err := commit(ctx); err != nil {
		return runtimeengine.CommittedEngineMutation{}, err
	}
	return runtimeengine.CommittedEngineMutation{
		ActivityIntents: append([]runtimeengine.ActivityIntent(nil), mutation.ActivityIntents...),
		EmitIntents:     append([]runtimeengine.EmitIntent(nil), mutation.EmitIntents...),
	}, nil
}

func (o pipelineEngineMutationOwner) commitEntitylessEngineMutation(ctx context.Context, mutation runtimeengine.EngineMutation, target events.DeliveryTargetOwnership) (runtimeengine.CommittedEngineMutation, error) {
	if entityID := mutation.Address.EntityID.String(); entityID != "" {
		return runtimeengine.CommittedEngineMutation{}, fmt.Errorf("entityless engine mutation carries entity identity %q", entityID)
	}
	if instancePath := mutation.Address.Route.InstancePath; instancePath != target.Route().FlowInstance {
		return runtimeengine.CommittedEngineMutation{}, fmt.Errorf("entityless engine mutation route %q disagrees with stamped receiver route %q", instancePath, target.Route().FlowInstance)
	}
	if len(mutation.LifecycleEffects) > 0 || len(mutation.EmitPrerequisites.Fields) > 0 {
		return runtimeengine.CommittedEngineMutation{}, fmt.Errorf("entityless engine mutation cannot carry state or lifecycle prerequisites")
	}
	emissionIntents := append([]runtimeengine.EmitIntent(nil), mutation.EmitIntents...)
	for _, value := range mutation.ActivityIntents {
		intent := value.Normalized()
		if intent.ApprovalDecision != "" {
			return runtimeengine.CommittedEngineMutation{}, fmt.Errorf("entityless engine mutation cannot carry approval-bound activity")
		}
	}
	activityPublications, err := activityRequestEmitIntents(mutation.ActivityIntents)
	if err != nil {
		return runtimeengine.CommittedEngineMutation{}, err
	}
	emissionIntents = append(emissionIntents, activityPublications...)
	var publications []runtimeengine.DurablePublicationPlan
	if len(emissionIntents) > 0 {
		if o.publication == nil {
			return runtimeengine.CommittedEngineMutation{}, fmt.Errorf("engine publication planner is required")
		}
		publications, err = o.publication.PrepareEnginePublications(ctx, emissionIntents)
		if err != nil {
			return runtimeengine.CommittedEngineMutation{}, err
		}
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return runtimeengine.CommittedEngineMutation{}, err
	}
	deliverySuccess, settlementGuard, err := beginWorkflowEngineDeliverySuccess(ctx, mutation.HandlerRuleSelection)
	if err != nil {
		if o.publication != nil {
			err = errors.Join(err, o.publication.ReleaseEnginePublications(context.WithoutCancel(ctx), publications))
		}
		return runtimeengine.CommittedEngineMutation{}, err
	}
	committed, commitErr := o.store.engineMutations.CommitWorkflowEngineMutation(ctx, WorkflowEngineMutationCommand{
		EntitylessTarget:        target,
		EntitylessRunID:         runID,
		Publications:            publications,
		DeliverySuccess:         deliverySuccess,
		FanOutIntent:            mutation.FanOutIntent,
		FanOutBarrier:           mutation.FanOutBarrier,
		FanOutBarrierCompletion: mutation.FanOutBarrierCompletion,
	})
	if commitErr == nil && mutation.FanOutIntent != nil {
		o.state.coordinator.signalFanOutWork()
	}
	settledClaim, settlementErr := finishWorkflowEngineDeliverySuccess(deliverySuccess, settlementGuard, committed.DeliverySuccess)
	commitErr = errors.Join(commitErr, settlementErr)
	if commitErr != nil && settledClaim == nil {
		if o.publication != nil {
			commitErr = errors.Join(commitErr, o.publication.ReleaseEnginePublications(context.WithoutCancel(ctx), publications))
		}
		return runtimeengine.CommittedEngineMutation{}, commitErr
	}
	engineCommit := runtimeengine.CommittedEngineMutation{SettledDeliveryClaim: settledClaim}
	var postCommitErr error
	if o.publication != nil {
		if err := o.publication.FinalizeEnginePublications(ctx, committed.Publications); err != nil {
			postCommitErr = errors.Join(postCommitErr, err)
		}
	}
	if len(committed.Publications) < len(mutation.EmitIntents) {
		return engineCommit, errors.Join(commitErr, postCommitErr, fmt.Errorf("committed engine publications = %d, want at least %d emitted events", len(committed.Publications), len(mutation.EmitIntents)))
	}
	committedIntents := make([]runtimeengine.EmitIntent, 0, len(mutation.EmitIntents))
	for index, publication := range committed.Publications[:len(mutation.EmitIntents)] {
		if publication == nil {
			return engineCommit, errors.Join(commitErr, postCommitErr, fmt.Errorf("committed engine publication %d is required", index))
		}
		intent := publication.CommittedDurablePublicationIntent()
		if strings.TrimSpace(intent.Event.ID()) != strings.TrimSpace(publication.CommittedDurablePublicationEventID()) {
			return engineCommit, errors.Join(commitErr, postCommitErr, fmt.Errorf("committed engine publication %d intent identity is inconsistent", index))
		}
		committedIntents = append(committedIntents, intent)
	}
	engineCommit.ActivityIntents = append([]runtimeengine.ActivityIntent(nil), mutation.ActivityIntents...)
	engineCommit.EmitIntents = committedIntents
	return engineCommit, errors.Join(commitErr, postCommitErr)
}

func verifyPreparedWorkflowEmitPersistence(instance WorkflowInstance, prerequisites runtimeengine.EmitPersistencePrerequisites) error {
	missingExpected := make([]string, 0, len(prerequisites.Fields))
	missingPrepared := make([]string, 0, len(prerequisites.Fields))
	mismatched := make([]string, 0, len(prerequisites.Fields))
	metadata := workflowInstancePersistedProjectionMetadata(instance)
	for _, prerequisite := range prerequisites.Fields {
		field := strings.TrimSpace(prerequisite.Field)
		if field == "" {
			continue
		}
		if !prerequisite.HasExpected {
			missingExpected = append(missingExpected, field)
			continue
		}
		actual, ok := workflowMetadataValue(metadata, field)
		if !ok {
			missingPrepared = append(missingPrepared, field)
			continue
		}
		if !workflowJSONValuesEqual(prerequisite.Expected, actual) {
			mismatched = append(mismatched, field)
		}
	}
	if len(missingExpected)+len(missingPrepared)+len(mismatched) == 0 {
		return nil
	}
	details := make([]string, 0, 3)
	if len(missingExpected) > 0 {
		details = append(details, "missing handler writes="+strings.Join(missingExpected, ","))
	}
	if len(missingPrepared) > 0 {
		details = append(details, "missing prepared fields="+strings.Join(missingPrepared, ","))
	}
	if len(mismatched) > 0 {
		details = append(details, "mismatched prepared fields="+strings.Join(mismatched, ","))
	}
	return fmt.Errorf("%w: %s", runtimeengine.ErrEmitPersistencePrerequisite, strings.Join(details, "; "))
}

func workflowInstancePersistedProjectionMetadata(instance WorkflowInstance) map[string]any {
	return cloneStringAnyMap(instance.Fields)
}

type pipelineEngineLocker struct {
	coordinator *PipelineCoordinator
}

func (l pipelineEngineLocker) WithEntityLock(ctx context.Context, entityID identity.EntityID, fn func(context.Context) error) error {
	if l.coordinator == nil {
		return fn(ctx)
	}
	unlock := l.coordinator.lockWorkflowEntity(entityID.String())
	defer unlock()
	return fn(ctx)
}

type pipelineEngineStateRepo struct {
	coordinator *PipelineCoordinator
}

type pipelineEngineEntityCollectionReader struct {
	coordinator *PipelineCoordinator
}

func (r pipelineEngineEntityCollectionReader) QueryEntityCollection(ctx context.Context, flowID, entityType string) ([]map[string]any, error) {
	flowID = strings.TrimSpace(flowID)
	entityType = strings.TrimSpace(entityType)
	if r.coordinator == nil || r.coordinator.workflowStore == nil || !r.coordinator.workflowStore.enabled() {
		return nil, fmt.Errorf("workflow entity collection reader is required")
	}
	instances, err := r.coordinator.workflowStore.list(ctx)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]any, 0, len(instances))
	for _, instance := range instances {
		if strings.TrimSpace(instance.WorkflowName) != flowID || strings.TrimSpace(instance.EntityType) != entityType {
			continue
		}
		rows = append(rows, cloneStringAnyMap(instance.Fields))
	}
	return rows, nil
}

func cloneWorkflowInstanceForEngineMutation(instance WorkflowInstance) WorkflowInstance {
	instance.Config = cloneStringAnyMap(instance.Config)
	instance.Fields = cloneStringAnyMap(instance.Fields)
	instance.Bookkeeping = cloneStringAnyMap(instance.Bookkeeping)
	instance.Gates = cloneWorkflowGates(instance.Gates)
	instance.StateBuckets = cloneStringAnyMap(instance.StateBuckets)
	instance.InitialFieldValues = cloneStringAnyMap(instance.InitialFieldValues)
	instance.TransitionHistory = append([]WorkflowTransitionRecord(nil), instance.TransitionHistory...)
	if instance.RuntimeReadiness != nil {
		readiness := *instance.RuntimeReadiness
		readiness.Agents = append([]DynamicFlowRuntimeAgentExpectation(nil), readiness.Agents...)
		if readiness.CreationEvent != nil {
			creation := *readiness.CreationEvent
			creation.Payload = append([]byte(nil), creation.Payload...)
			readiness.CreationEvent = &creation
		}
		instance.RuntimeReadiness = &readiness
	}
	return instance
}

type preparedWorkflowEngineState struct {
	runID            string
	route            runtimeflowidentity.Route
	instance         WorkflowInstance
	expectedState    string
	expectedRevision int64
	transition       WorkflowEngineStateTransition
	updatedAt        time.Time
}

func (p preparedWorkflowEngineState) record() (WorkflowEngineStateRecord, error) {
	return workflowEngineStateRecord(p.runID, p.route, p.instance, p.expectedState, p.expectedRevision, p.transition, p.updatedAt)
}

func (r pipelineEngineStateRepo) prepareMutation(
	ctx context.Context,
	address runtimeengine.StateAddress,
	mutation runtimeengine.StateMutation,
	targetApplications ...DeliveryTargetApplication,
) (preparedWorkflowEngineState, error) {
	if r.coordinator == nil || r.coordinator.workflowStore == nil || !r.coordinator.workflowStore.enabled() {
		return preparedWorkflowEngineState{}, fmt.Errorf("workflow engine mutation requires selected workflow persistence")
	}
	entityID := identity.NormalizeEntityID(address.EntityID.String())
	if entityID.IsZero() || !address.Route.Valid() {
		return preparedWorkflowEngineState{}, fmt.Errorf("workflow engine mutation requires exact entity and instance route")
	}
	runID, err := runtimecurrentstate.RequireRunID(ctx)
	if err != nil {
		return preparedWorkflowEngineState{}, err
	}
	flowID := strings.TrimSpace(address.FlowID.String())
	if flowID == "" {
		flowID = semanticview.RootExecutionFlowID(r.coordinator.SemanticSource())
	}
	semanticSource := r.coordinator.SemanticSource()
	if semanticSource != nil && flowID == strings.TrimSpace(semanticview.RootExecutionFlowID(semanticSource)) {
		coordinate, err := semanticview.AdmitRootExecutionCoordinate(semanticSource, runID)
		if err != nil {
			return preparedWorkflowEngineState{}, err
		}
		if !coordinate.Matches(flowID, address.Route.InstancePath) {
			return preparedWorkflowEngineState{}, fmt.Errorf("workflow engine root route (%q, %q) disagrees with current root coordinate (%q, %q)", flowID, address.Route.InstancePath, coordinate.FlowID(), coordinate.RunID())
		}
	}
	if err := r.ensureFlowOwnsEntity(ctx, address, flowID, runID); err != nil {
		return preparedWorkflowEngineState{}, err
	}
	if len(targetApplications) > 1 {
		return preparedWorkflowEngineState{}, fmt.Errorf("workflow engine mutation accepts at most one delivery target application")
	}
	var (
		current  WorkflowInstance
		presence WorkflowTargetPersistencePresence
	)
	if len(targetApplications) == 1 {
		application := targetApplications[0]
		if err := application.Validate(); err != nil {
			return preparedWorkflowEngineState{}, err
		}
		if address.Route != application.Route() || entityID.String() != application.EntityID() {
			return preparedWorkflowEngineState{}, fmt.Errorf("workflow engine mutation state disagrees with admitted delivery target application")
		}
		if application.previewOnly() {
			return preparedWorkflowEngineState{}, fmt.Errorf("workflow engine mutation rejects preview-only delivery target application")
		}
		current, presence, err = r.coordinator.loadCurrentDeliveryTargetState(ctx, application)
		if err != nil {
			return preparedWorkflowEngineState{}, err
		}
	} else {
		target, err := r.coordinator.workflowStore.LoadTargetPersistence(ctx, address.Route, entityID)
		if err != nil {
			return preparedWorkflowEngineState{}, err
		}
		presence = target.Presence
		switch presence {
		case WorkflowTargetPersistenceComplete:
			current, err = target.DecodeComplete(address.Route, entityID)
		case WorkflowTargetPersistenceStateOnly:
			current, err = decodeDeliveryTargetWorkflowEntityState(r.coordinator.SemanticSource(), flowID, runID, target.State)
		case WorkflowTargetPersistenceAbsent:
		case WorkflowTargetPersistenceLifecycleOnly:
			return preparedWorkflowEngineState{}, fmt.Errorf("workflow engine mutation rejects lifecycle companion without state")
		default:
			return preparedWorkflowEngineState{}, fmt.Errorf("workflow engine mutation requires closed target persistence presence")
		}
		if err != nil {
			return preparedWorkflowEngineState{}, err
		}
	}
	transition, err := WorkflowEngineStateTransitionForPresence(presence)
	if err != nil {
		return preparedWorkflowEngineState{}, err
	}
	if presence.HasState() {
		if err := validateWorkflowEntityType(semanticSource, flowID, current.EntityType); err != nil {
			return preparedWorkflowEngineState{}, fmt.Errorf("workflow engine persisted entity contract: %w", err)
		}
	}
	expectedState := ""
	expectedRevision := int64(0)
	if transition.CreatesState() {
		if mutation.TriggeredAt.IsZero() {
			return preparedWorkflowEngineState{}, fmt.Errorf("workflow initial materialization requires exact accepted event time")
		}
		source := r.coordinator.SemanticSource()
		workflowName := flowID
		workflowVersion := ""
		if source != nil {
			workflowName = firstNonEmptyString(workflowName, semanticview.RootExecutionFlowID(source))
			workflowVersion = source.WorkflowVersion()
		}
		initialState := strings.TrimSpace(firstNonEmptyString(workflowInitialStateForFlow(source, flowID), "pending"))
		mode := workflowPersistedFlowMode(source, flowID)
		if mode == "" {
			return preparedWorkflowEngineState{}, fmt.Errorf("workflow initial materialization rejects unsupported persistence mode for flow %s", flowID)
		}
		entityType, err := requireWorkflowEntityType(source, flowID)
		if err != nil {
			return preparedWorkflowEngineState{}, fmt.Errorf("workflow initial materialization entity contract: %w", err)
		}
		if carried := strings.TrimSpace(mutation.StateCarrier.Control.EntityType); carried != entityType {
			return preparedWorkflowEngineState{}, fmt.Errorf("workflow initial materialization carried entity_type %q disagrees with canonical contract %q", carried, entityType)
		}
		current = WorkflowInstance{
			InstanceID: address.Route.InstanceID, StorageRef: address.Route.InstancePath, EntityID: entityID.String(),
			WorkflowName: workflowName, WorkflowVersion: workflowVersion, Mode: mode, Status: "active", CurrentState: initialState,
			EntityType:   entityType,
			InstanceKind: mutation.StateCarrier.Control.InstanceKind, TemplateVersion: mutation.StateCarrier.Control.TemplateVersion,
			ParentFlowID: mutation.StateCarrier.Control.ParentFlowID, ParentFlowInstance: mutation.StateCarrier.Control.ParentFlowInstance,
			ParentEntityID: mutation.StateCarrier.Control.ParentEntityID,
			Fields:         workflowMaterializeEntityFields(source, flowID, mutation.StateCarrier.PersistedFields()),
			Bookkeeping:    mutation.StateCarrier.PersistedBookkeeping(), Gates: cloneWorkflowGates(mutation.StateCarrier.Gates),
			StateBuckets: mutation.StateCarrier.PersistedStateBuckets(), InitialFieldValues: cloneStringAnyMap(mutation.InitialFieldValues),
			EnteredStageAt: mutation.TriggeredAt.UTC(), CreatedAt: mutation.TriggeredAt.UTC(), UpdatedAt: mutation.TriggeredAt.UTC(),
		}
	} else {
		current = cloneWorkflowInstanceForEngineMutation(current)
		expectedState = strings.TrimSpace(current.CurrentState)
		expectedRevision = current.Revision
		if expectedRevision <= 0 {
			return preparedWorkflowEngineState{}, fmt.Errorf("workflow engine mutation requires persisted revision")
		}
	}

	fromState := strings.TrimSpace(current.CurrentState)
	if err := applyEngineStateMutation(&current, mutation, workflowEntitySchemaFields(r.coordinator.SemanticSource(), flowID), r.coordinator.SemanticSource(), flowID); err != nil {
		return preparedWorkflowEngineState{}, err
	}
	nextState := strings.TrimSpace(mutation.NextState)
	if nextState != "" && nextState != fromState {
		if strings.TrimSpace(mutation.TriggerEventID) == "" || strings.TrimSpace(mutation.TriggerEventType) == "" || mutation.TriggeredAt.IsZero() {
			return preparedWorkflowEngineState{}, fmt.Errorf("workflow state transition requires exact trigger event identity")
		}
		current.CurrentState = nextState
		current.EnteredStageAt = mutation.TriggeredAt.UTC()
		current.TransitionHistory = append(current.TransitionHistory, workflowTransitionRecord(
			r.coordinator.WorkflowDefinition(), fromState, nextState,
			mutation.TriggerEventID, mutation.TriggerEventType, mutation.TriggeredAt,
		))
	}
	if current.CreatedAt.IsZero() {
		return preparedWorkflowEngineState{}, fmt.Errorf("workflow engine mutation requires persisted creation time")
	}
	updatedAt := mutation.TriggeredAt.UTC()
	if transition.UpdatesState() {
		// The trigger is a causal fact and may legitimately predate materialization
		// during replay. The row revision time records this commit instead.
		updatedAt = time.Now().UTC()
		if updatedAt.Before(current.CreatedAt) {
			updatedAt = current.CreatedAt
		}
	}
	return preparedWorkflowEngineState{
		runID: runID, route: address.Route, instance: current,
		expectedState: expectedState, expectedRevision: expectedRevision,
		transition: transition, updatedAt: updatedAt,
	}, nil
}

func (r pipelineEngineStateRepo) LoadState(ctx context.Context, address runtimeengine.StateAddress) (runtimeengine.StateSnapshot, bool, error) {
	if r.coordinator == nil {
		return runtimeengine.StateSnapshot{}, false, nil
	}
	entityID := identity.NormalizeEntityID(address.EntityID.String())
	if entityID.IsZero() {
		return runtimeengine.StateSnapshot{}, false, nil
	}
	flowID := strings.TrimSpace(address.FlowID.String())
	if application, ok := deliveryTargetApplicationFromContext(ctx); ok {
		if err := application.Validate(); err != nil {
			return runtimeengine.StateSnapshot{}, false, err
		}
		if application.Owner().EntitylessReceiver() {
			return runtimeengine.StateSnapshot{}, false, nil
		}
		if address.Route != application.Route() || entityID.String() != application.EntityID() {
			return runtimeengine.StateSnapshot{}, false, fmt.Errorf("engine state lookup disagrees with admitted delivery target application")
		}
		if application.previewOnly() {
			return runtimeengine.StateSnapshot{}, false, nil
		}
		instance, presence, err := r.coordinator.loadCurrentDeliveryTargetState(ctx, application)
		if err != nil {
			return runtimeengine.StateSnapshot{}, false, err
		}
		if !presence.HasState() {
			return runtimeengine.StateSnapshot{}, false, nil
		}
		return workflowInstanceEngineStateSnapshot(r.coordinator.SemanticSource(), flowID, entityID, instance)
	}
	if r.coordinator.workflowStore != nil && r.coordinator.workflowStore.enabled() {
		if !address.Route.Valid() {
			return runtimeengine.StateSnapshot{}, false, fmt.Errorf("engine state lookup requires an exact workflow instance route")
		}
		instance, ok, err := r.coordinator.workflowStore.Load(ctx, address.Route)
		if err != nil {
			return runtimeengine.StateSnapshot{}, false, err
		}
		if ok {
			if _, err := requireWorkflowInstanceIdentity(address.Route, entityID, instance); err != nil {
				return runtimeengine.StateSnapshot{}, false, fmt.Errorf("validate engine state identity: %w", err)
			}
			return workflowInstanceEngineStateSnapshot(r.coordinator.SemanticSource(), flowID, entityID, instance)
		}
		return runtimeengine.StateSnapshot{}, false, nil
	}
	state, err := r.coordinator.currentWorkflowState(ctx, address.Route, entityID)
	if err != nil {
		return runtimeengine.StateSnapshot{}, false, err
	}
	if strings.TrimSpace(string(state.Stage)) == "" && len(state.Metadata) == 0 {
		return runtimeengine.StateSnapshot{}, false, nil
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(workflowMaterializeEntityFields(r.coordinator.SemanticSource(), flowID, state.Metadata), nil, nil, nil)
	if err != nil {
		return runtimeengine.StateSnapshot{}, false, err
	}
	carrier.Control = state.Control
	return runtimeengine.StateSnapshot{
		EntityID:     entityID,
		CurrentState: strings.TrimSpace(string(state.Stage)),
		StateCarrier: carrier,
	}, true, nil
}

func workflowInstanceEngineStateSnapshot(
	source semanticview.Source,
	flowID string,
	entityID identity.EntityID,
	instance WorkflowInstance,
) (runtimeengine.StateSnapshot, bool, error) {
	carrier, err := workflowInstanceStateCarrier(instance)
	if err != nil {
		return runtimeengine.StateSnapshot{}, false, err
	}
	carrier.Gates = workflowStateGatesForScope(source, flowID, carrier.Gates)
	return runtimeengine.StateSnapshot{
		EntityID:        entityID,
		WorkflowName:    strings.TrimSpace(instance.WorkflowName),
		WorkflowVersion: strings.TrimSpace(instance.WorkflowVersion),
		CurrentState:    strings.TrimSpace(instance.CurrentState),
		StateCarrier:    carrier,
		EnteredStateAt:  instance.EnteredStageAt,
	}, true, nil
}

func (r pipelineEngineStateRepo) SaveState(ctx context.Context, address runtimeengine.StateAddress, mutation runtimeengine.StateMutation) error {
	if r.coordinator == nil || r.coordinator.workflowStore == nil || !r.coordinator.workflowStore.enabled() {
		return nil
	}
	return fmt.Errorf("direct workflow state persistence is unsupported; use the workflow engine mutation owner")
}

func (r pipelineEngineStateRepo) VerifyEmitPersistence(ctx context.Context, address runtimeengine.StateAddress, prerequisites runtimeengine.EmitPersistencePrerequisites) error {
	if r.coordinator == nil || r.coordinator.workflowStore == nil || !r.coordinator.workflowStore.enabled() {
		return nil
	}
	entityID := identity.NormalizeEntityID(address.EntityID.String())
	if entityID.IsZero() || len(prerequisites.Fields) == 0 {
		return nil
	}
	persisted, ok, err := r.LoadState(ctx, address)
	if err != nil {
		return fmt.Errorf("%w: load persisted entity state: %v", runtimeengine.ErrEmitPersistencePrerequisite, err)
	}
	if !ok {
		return fmt.Errorf("%w: entity_state row missing for %s", runtimeengine.ErrEmitPersistencePrerequisite, entityID.String())
	}
	missingExpected := make([]string, 0, len(prerequisites.Fields))
	missingPersisted := make([]string, 0, len(prerequisites.Fields))
	mismatched := make([]string, 0, len(prerequisites.Fields))
	for _, prerequisite := range prerequisites.Fields {
		field := strings.TrimSpace(prerequisite.Field)
		if field == "" {
			continue
		}
		if !prerequisite.HasExpected {
			missingExpected = append(missingExpected, field)
			continue
		}
		actual, ok := workflowMetadataValue(persisted.StateCarrier.Fields, field)
		if !ok {
			missingPersisted = append(missingPersisted, field)
			continue
		}
		if !workflowJSONValuesEqual(prerequisite.Expected, actual) {
			mismatched = append(mismatched, field)
		}
	}
	if len(missingExpected) == 0 && len(missingPersisted) == 0 && len(mismatched) == 0 {
		return nil
	}
	details := make([]string, 0, 3)
	if len(missingExpected) > 0 {
		details = append(details, "missing handler writes="+strings.Join(missingExpected, ","))
	}
	if len(missingPersisted) > 0 {
		details = append(details, "missing persisted fields="+strings.Join(missingPersisted, ","))
	}
	if len(mismatched) > 0 {
		details = append(details, "mismatched persisted fields="+strings.Join(mismatched, ","))
	}
	return fmt.Errorf("%w: %s", runtimeengine.ErrEmitPersistencePrerequisite, strings.Join(details, "; "))
}

func (r pipelineEngineStateRepo) ensureFlowOwnsEntity(ctx context.Context, address runtimeengine.StateAddress, flowID, runID string) error {
	if r.coordinator == nil || r.coordinator.workflowStore == nil || !r.coordinator.workflowStore.enabled() {
		return nil
	}
	if flowID == "" {
		return nil
	}
	if !address.Route.Valid() {
		return fmt.Errorf("flow ownership check requires an exact workflow instance route")
	}
	instance, ok, err := r.coordinator.workflowStore.Load(ctx, address.Route)
	if err != nil || !ok {
		return err
	}
	if workflowInstanceOwnedByFlow(r.coordinator.SemanticSource(), instance, flowID, runID) {
		return nil
	}
	return runtimefailures.New(runtimefailures.ClassAuthorizationDenied, "cross_flow_write_forbidden", "pipeline-engine", "write_entity", map[string]any{
		"action":         "cross_flow_entity_write",
		"flow_id":        flowID,
		"entity_id":      address.EntityID.String(),
		"owner_workflow": strings.TrimSpace(instance.WorkflowName),
	})
}

func newCoordinatorEngineEvaluator(pc *PipelineCoordinator) runtimeengine.Evaluator {
	if pc == nil {
		return nil
	}
	return pipelineEngineEvaluator{evaluator: pc.expressionEval, coordinator: pc}
}

func (e pipelineEngineEvaluator) queryEntityCount(ctx workflowExpressionContext, predicate string) (int, error) {
	if e.coordinator == nil || e.coordinator.workflowStore == nil || !e.coordinator.workflowStore.enabled() {
		return 0, nil
	}
	parsed, err := parseWorkflowEntityQueryPredicate(predicate, ctx)
	if err != nil {
		return 0, err
	}
	runID := strings.TrimSpace(asString(ctx.Event["run_id"]))
	if runID == "" {
		return 0, fmt.Errorf("query_entities requires event.run_id in expression context")
	}
	flowID := strings.TrimSpace(ctx.WorkflowName)
	contract, ok := entityruntime.ResolveForFlow(e.coordinator.SemanticSource(), flowID)
	if !ok {
		flowLabel := flowID
		if flowLabel == "" {
			flowLabel = "<root>"
		}
		return 0, fmt.Errorf("flow-owned entity contract is not available for workflow %s", flowLabel)
	}
	if strings.TrimSpace(parsed.Field) != "current_state" {
		if _, err := entityruntime.ResolveLeafField(contract, parsed.Field); err != nil {
			return 0, err
		}
	}
	return e.coordinator.workflowStore.QueryEntityCount(context.Background(), runID, e.coordinator.SemanticSource(), contract, parsed)
}

func coordinatorEngineDependencies(pc *PipelineCoordinator) runtimeengine.RuntimeDependencies {
	if pc == nil {
		return runtimeengine.RuntimeDependencies{}
	}
	source := pc.SemanticSource()
	if source == nil {
		source = semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	}
	var dispatcher runtimeengine.PostCommitDispatcher
	if pc.bus != nil {
		dispatcher = pc.bus.EngineDispatcher()
	}
	var lifecycleOwner runtimeengine.WorkflowLifecycleEffectOwner
	if pc.workflowStore != nil && pc.workflowStore.enabled() {
		lifecycleOwner = pipelineWorkflowLifecycleOwner{coordinator: pc}
	}
	stateRepo := pipelineEngineStateRepo{coordinator: pc}
	activityWriter := pipelineActivityIntentWriter{coordinator: pc}
	publicationPlanner, _ := pc.bus.(EnginePublicationPlanner)
	return runtimeengine.RuntimeDependencies{
		Source:              source,
		StateRepo:           stateRepo,
		EntityCollections:   pipelineEngineEntityCollectionReader{coordinator: pc},
		MutationOwner:       pipelineEngineMutationOwner{store: pc.workflowStore, state: stateRepo, publication: publicationPlanner, verifier: stateRepo, lifecycle: lifecycleOwner, activities: activityWriter},
		Locker:              pipelineEngineLocker{coordinator: pc},
		WorkflowLifecycle:   lifecycleOwner,
		Dispatcher:          dispatcher,
		ActivityDispatcher:  pipelineActivityDispatcher{coordinator: pc},
		GuardRegistry:       pipelineEngineGuardRegistry{registry: pc.GuardRegistry()},
		GuardRunner:         pipelineEngineGuardRunner{coordinator: pc},
		ActionRegistry:      pipelineEngineActionRegistry{registry: pc.ActionRegistry()},
		ActionRunner:        pipelineEngineActionRunner{coordinator: pc},
		PayloadShaper:       pipelineEnginePayloadShaper{coordinator: pc},
		TransitionValidator: pipelineEngineTransitionValidator{coordinator: pc},
		EmitNow:             pc.testEngineEmitNow,
		MaxChainDepth:       workflowMaxChainDepthPolicy(source),
	}
}

func workflowMetadataValue(metadata map[string]any, target string) (any, bool) {
	target = strings.TrimSpace(target)
	if target == "" {
		return nil, false
	}
	parsed := paths.Parse(target)
	if parsed.HasExplicitRoot() {
		parsed = paths.Path{Segments: parsed.Segments}
	}
	if len(parsed.Segments) == 0 {
		return nil, false
	}
	current := any(metadata)
	for _, segment := range parsed.Segments {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok := object[strings.TrimSpace(segment)]
		if !ok {
			return nil, false
		}
		current = value
	}
	return current, true
}

func WorkflowMetadataValue(metadata map[string]any, target string) (any, bool) {
	return workflowMetadataValue(metadata, target)
}

func workflowJSONValuesEqual(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}

func WorkflowJSONValuesEqual(left, right any) bool {
	return workflowJSONValuesEqual(left, right)
}

func workflowMaxChainDepthPolicy(source semanticview.Source) int {
	if source == nil {
		return runtimeengine.DefaultMaxChainDepth
	}
	if value, ok := semanticview.PolicyValueForFlow(source, "", "max_chain_depth"); ok {
		if parsed := asInt(value.Value); parsed > 0 {
			return parsed
		}
	}
	return runtimeengine.DefaultMaxChainDepth
}

type pipelineEngineTransitionValidator struct {
	coordinator *PipelineCoordinator
}

func (v pipelineEngineTransitionValidator) ValidateTransition(currentState, nextState string) error {
	pc := v.coordinator
	if pc == nil {
		return nil
	}
	workflow := pc.WorkflowDefinition()
	if workflow == nil {
		return nil
	}
	current := NormalizeWorkflowStateID(currentState)
	next := NormalizeWorkflowStateID(nextState)
	if workflow.CanTransition(WorkflowState{Stage: current}, next) {
		return nil
	}
	return fmt.Errorf("%w: %s -> %s", runtimeengine.ErrInvalidTransition, strings.TrimSpace(string(current)), strings.TrimSpace(string(next)))
}

type pipelineEngineGuardRegistry struct{ registry GuardRegistry }

func (r pipelineEngineGuardRegistry) HasGuard(id identity.GuardKey) bool {
	return r.registry != nil && r.registry.HasGuard(id)
}
func (r pipelineEngineGuardRegistry) IsExecutable(id identity.GuardKey) bool {
	return r.registry != nil && r.registry.IsExecutable(id)
}
func (r pipelineEngineGuardRegistry) Guard(id identity.GuardKey) (runtimeregistry.GuardInstruction, bool) {
	if r.registry == nil {
		return runtimeregistry.GuardInstruction{}, false
	}
	return r.registry.Guard(id)
}

type pipelineEngineActionRegistry struct{ registry ActionRegistry }

func (r pipelineEngineActionRegistry) HasAction(id identity.ActionKey) bool {
	if r.registry != nil && r.registry.HasAction(id) {
		return true
	}
	return runtimecontracts.IsSupportedHandlerActionID(id.String())
}
func (r pipelineEngineActionRegistry) IsExecutable(id identity.ActionKey) bool {
	if r.registry != nil && r.registry.IsExecutable(id) {
		return true
	}
	return runtimecontracts.IsSupportedHandlerActionID(id.String())
}
func (r pipelineEngineActionRegistry) Action(id identity.ActionKey) (runtimeregistry.ActionInstruction, bool) {
	if r.registry != nil {
		if instruction, ok := r.registry.Action(id); ok {
			return instruction, true
		}
	}
	if !runtimecontracts.IsSupportedHandlerActionID(id.String()) {
		return runtimeregistry.ActionInstruction{}, false
	}
	return runtimeregistry.ActionInstruction{
		Key:     id,
		Builtin: id.String(),
	}, true
}

type pipelineEngineGuardRunner struct {
	coordinator *PipelineCoordinator
}

func (r pipelineEngineGuardRunner) EvaluateGuard(ctx context.Context, id identity.GuardKey, entry runtimeregistry.GuardInstruction, execCtx runtimeengine.ExecutionContext) (bool, bool, error) {
	pc := r.coordinator
	if pc == nil {
		return false, false, nil
	}
	builtin := strings.TrimSpace(firstNonEmptyString(entry.Builtin, id.String()))
	state := workflowStateFromEngine(execCtx.Request.State)
	payload := parsePayloadMap(execCtx.Request.Event.Payload())
	switch builtin {
	case "has_entity_id":
		return strings.TrimSpace(execCtx.Request.EntityID.String()) != "", true, nil
	case "has_human_decision":
		event := execCtx.Request.Event
		if hasHumanDecisionProducer(event) {
			return true, true, nil
		}
		if strings.EqualFold(strings.TrimSpace(asString(payload["decision_path"])), "mailbox") {
			return true, true, nil
		}
		return strings.TrimSpace(asString(payload["mailbox_decision_id"])) != "", true, nil
	case "not_in_terminal_state", "not_in_terminal_stage":
		source := pc.SemanticSource()
		if source == nil {
			return true, true, nil
		}
		currentState := strings.TrimSpace(string(state.Stage))
		if currentState == "" {
			return true, true, nil
		}
		flowID := execCtx.Request.Node.FlowID()
		for _, candidateFlowID := range terminalStateFlowCandidates(source, flowID, *state) {
			if terminalStageContains(source.FlowTerminalStages(candidateFlowID), currentState) {
				return false, true, nil
			}
			if stageSetContains(source.FlowStates(candidateFlowID), currentState) {
				return true, true, nil
			}
		}
		workflow := pc.WorkflowDefinition()
		if workflow != nil {
			if stage, ok := workflow.Stage(state.Stage); ok {
				return !stage.Terminal, true, nil
			}
		}
		return true, true, nil
	case "state_in_phase":
		if pc.WorkflowDefinition() == nil {
			return false, true, nil
		}
		stage, ok := pc.WorkflowDefinition().Stage(state.Stage)
		if !ok {
			return false, true, nil
		}
		required := strings.TrimSpace(entry.PolicyRef)
		if required != "" {
			if value, ok := workflowExpressionLookupPath(execCtx.Base.Policy.Raw(), required); ok {
				required = strings.TrimSpace(asString(value))
			}
		}
		if required == "" {
			required = strings.TrimSpace(asString(execCtx.Base.Policy.Raw()["required_phase"]))
		}
		if required == "" {
			return false, true, runtimeengine.ErrInvalidConfig
		}
		return strings.EqualFold(strings.TrimSpace(stage.Phase), required), true, nil
	default:
		return false, false, nil
	}
}

func hasHumanDecisionProducer(event events.Event) bool {
	return events.ProducerIs(event, events.EventProducerExternal, "human") || events.ProducerIs(event, events.EventProducerExternal, "mailbox")
}

type pipelineEngineActionRunner struct {
	coordinator        *PipelineCoordinator
	artifactRepoCommit func(context.Context, runtimecontracts.ActionSpec, runtimeengine.ExecutionContext) (runtimeengine.ActionExecution, error)
}

func (r pipelineEngineActionRunner) ExecuteAction(ctx context.Context, action runtimecontracts.ActionSpec, entry runtimeregistry.ActionInstruction, execCtx runtimeengine.ExecutionContext) (runtimeengine.ActionExecution, error) {
	pc := r.coordinator
	if pc == nil {
		return runtimeengine.ActionExecution{}, nil
	}
	actionID := runtimecontracts.NormalizeHandlerActionID(firstNonEmptyString(entry.Builtin, entry.Key.String(), action.ID))
	if actionID == "" {
		return runtimeengine.ActionExecution{}, nil
	}
	switch actionID {
	case "record_evidence":
		payload := parsePayloadMap(execCtx.Request.Event.Payload())
		bucketID := recordEvidenceTarget(execCtx.Request)
		if bucketID == "" {
			return runtimeengine.ActionExecution{Handled: true}, fmt.Errorf("node %s handler %s record_evidence is missing evidence_target", execCtx.Request.Node.Key(), recordEvidenceHandlerLabel(execCtx.Request))
		}
		mutation, err := pc.projectWorkflowEvidence(execCtx, bucketID, payload)
		if err != nil {
			return runtimeengine.ActionExecution{Handled: true}, err
		}
		return runtimeengine.ActionExecution{Handled: true, State: mutation}, nil
	case "create_flow_instance":
		plan := handlerExecutionPlan{
			Node:           execCtx.Request.Node,
			EventType:      strings.TrimSpace(string(execCtx.Request.Event.Type())),
			Action:         actionID,
			Template:       strings.TrimSpace(action.Template),
			InstanceIDFrom: strings.TrimSpace(action.InstanceIDFrom),
			InstanceIDPath: action.InstanceIDPath,
			ConfigFrom:     action.ConfigFrom,
		}
		if err := pc.createFlowInstance(ctx, engineTriggerContext(execCtx.Request), plan, execCtx.Base); err != nil {
			return runtimeengine.ActionExecution{Handled: true}, err
		}
		return runtimeengine.ActionExecution{Handled: true}, nil
	case "mailbox_write":
		if err := pc.materializeMailboxItem(ctx, action, execCtx); err != nil {
			return runtimeengine.ActionExecution{Handled: true}, err
		}
		return runtimeengine.ActionExecution{Handled: true}, nil
	case "artifact_repo_commit":
		mode, err := pipelineActionExecutionMode(ctx, execCtx)
		if err != nil {
			return runtimeengine.ActionExecution{Handled: true}, err
		}
		if mode == runtimeeffects.ExecutionModeMock {
			return runtimeengine.ActionExecution{Handled: true}, runtimefailures.New(runtimefailures.ClassSchemaInvalid, "mock_artifact_repo_commit_forbidden", "pipeline-action-runtime", "admit_artifact_repo_commit", map[string]any{
				"action": "artifact_repo_commit", "execution_mode": string(mode),
			})
		}
		commit := r.artifactRepoCommit
		if commit == nil {
			commit = pc.commitArtifactRepo
		}
		execution, err := commit(ctx, action, execCtx)
		execution.Handled = true
		return execution, err
	default:
		return runtimeengine.ActionExecution{}, nil
	}
}

func pipelineActionExecutionMode(ctx context.Context, execCtx runtimeengine.ExecutionContext) (runtimeeffects.ExecutionMode, error) {
	eventMode := execCtx.Request.Event.ExecutionMode()
	if !eventMode.Valid() {
		return "", fmt.Errorf("pipeline action requires typed causal execution mode")
	}
	if contextMode, ok := runtimeeffects.ExecutionModeFromContext(ctx); ok && contextMode != eventMode {
		return "", fmt.Errorf("pipeline action execution mode conflicts with source event")
	}
	return eventMode, nil
}

func recordEvidenceTarget(req runtimeengine.ExecutionRequest) string {
	return strings.TrimSpace(req.Handler.EvidenceTarget)
}

func recordEvidenceHandlerLabel(req runtimeengine.ExecutionRequest) string {
	if handlerKey := strings.TrimSpace(req.HandlerEventKey); handlerKey != "" {
		return handlerKey
	}
	return strings.TrimSpace(string(req.Event.Type()))
}

type pipelineEnginePayloadShaper struct {
	coordinator *PipelineCoordinator
}

func (s pipelineEnginePayloadShaper) ShapeEmitPayload(ctx context.Context, req runtimeengine.ExecutionRequest, eventType string, payload map[string]any) (map[string]any, error) {
	pc := s.coordinator
	if pc == nil {
		return cloneStringAnyMap(payload), nil
	}
	out := cloneStringAnyMap(payload)
	if out == nil {
		out = map[string]any{}
	}
	envelope := pc.handlerEmitEnvelope(ctx, engineTriggerContext(req), strings.TrimSpace(eventType))
	if emitSurface := runtimeengine.EmitSurfaceFromContext(ctx); emitSurface == runtimeengine.EmitSurfaceDeclarative {
		if err := rejectAuthoredEnvelopeFields(out); err != nil {
			return nil, err
		}
	}
	if err := validatePipelineEmitPayload(pc.SemanticSource(), req.Node.FlowID(), eventType, out, envelope, runtimeengine.EmitSurfaceFromContext(ctx)); err != nil {
		return nil, err
	}
	return out, nil
}

func validatePipelineEmitPayload(source semanticview.Source, flowID, eventType string, payload, envelope map[string]any, surface runtimeengine.EmitSurface) error {
	proof := semanticview.ResolveFlowEventProof(source, strings.TrimSpace(flowID), strings.TrimSpace(eventType))
	if !proof.HasSchema {
		return nil
	}
	resolution := semanticview.ResolveEventSchema(source, flowID, eventType)
	if !resolution.HasSchema {
		return nil
	}
	if err := resolution.UnresolvedTypeError(); err != nil {
		return fmt.Errorf("%w: event %s payload schema is unresolved: %v", runtimeengine.ErrEmitPayloadContractViolation, proof.EventKey(), err)
	}
	schema := resolution.Schema
	allowed := eventPayloadProperties(proof.Entry)
	validationPayload := cloneStringAnyMap(payload)
	if surface != runtimeengine.EmitSurfaceDeclarative {
		validationPayload = runtimeeventpayload.StripUndeclaredRuntimeOwnedCanonicalContext(validationPayload, allowed)
	}
	if err := runtimeeventschema.ValidatePayloadAgainstSchema(schema.Schema, validationPayload); err != nil {
		return fmt.Errorf("%w: event %s payload violates schema: %v", runtimeengine.ErrEmitPayloadContractViolation, proof.EventKey(), err)
	}
	return nil
}

func rejectAuthoredEnvelopeFields(payload map[string]any) error {
	fields := runtimeeventpayload.RuntimeOwnedCanonicalContextFields(payload)
	if len(fields) == 0 {
		return nil
	}
	sort.Strings(fields)
	return fmt.Errorf("%w: authored emit payload must not include platform-owned envelope field(s): %s", runtimeengine.ErrEmitPayloadContractViolation, strings.Join(fields, ", "))
}

func pipelineEmitPayloadProperties(source semanticview.Source, flowID, eventType string) map[string]struct{} {
	if source == nil {
		return nil
	}
	proof := semanticview.ResolveFlowEventProof(source, strings.TrimSpace(flowID), strings.TrimSpace(eventType))
	if !proof.HasSchema {
		return nil
	}
	allowed := eventPayloadProperties(proof.Entry)
	if len(allowed) > 0 {
		return allowed
	}
	return map[string]struct{}{}
}

func eventPayloadProperties(entry runtimecontracts.EventCatalogEntry) map[string]struct{} {
	allowed := make(map[string]struct{}, len(entry.Payload.Properties))
	for key := range entry.Payload.Properties {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		allowed[key] = struct{}{}
	}
	return allowed
}

func applyEngineStateMutation(instance *WorkflowInstance, mutation runtimeengine.StateMutation, allowedFields map[string]struct{}, source semanticview.Source, flowID string) error {
	if instance == nil {
		return nil
	}
	previousBuckets := cloneStringAnyMap(instance.StateBuckets)
	if instance.Fields == nil {
		instance.Fields = map[string]any{}
	}
	if instance.Bookkeeping == nil {
		instance.Bookkeeping = map[string]any{}
	}
	if strings.TrimSpace(instance.WorkflowName) == "" {
		defaultWorkflowName := strings.TrimSpace(flowID)
		if defaultWorkflowName == "" && source != nil {
			defaultWorkflowName = strings.TrimSpace(semanticview.RootExecutionFlowID(source))
		}
		instance.WorkflowName = defaultWorkflowName
	}
	if strings.TrimSpace(instance.WorkflowVersion) == "" && source != nil {
		instance.WorkflowVersion = strings.TrimSpace(source.WorkflowVersion())
	}
	if strings.TrimSpace(instance.CurrentState) == "" {
		instance.CurrentState = strings.TrimSpace(firstNonEmptyString(workflowInitialStateForFlow(source, flowID), "pending"))
	}
	if instance.EnteredStageAt.IsZero() {
		return fmt.Errorf("workflow mutation requires materialized entry time")
	}
	existingGates := cloneWorkflowGates(instance.Gates)
	if len(mutation.StateCarrier.Gates) > 0 || len(mutation.ClearGates) > 0 || strings.TrimSpace(mutation.SetGate) != "" {
		if mutation.StateCarrier.Fields == nil {
			mutation.StateCarrier.Fields = cloneStringAnyMap(instance.Fields)
		}
		gates := workflowCloneBoolMap(existingGates)
		for key, value := range mutation.StateCarrier.Gates {
			key = workflowScopedGateKey(source, flowID, key)
			if key != "" {
				gates[key] = value
			}
		}
		for _, gate := range mutation.ClearGates {
			gate = workflowScopedGateKey(source, flowID, gate)
			if gate != "" {
				gates[gate] = false
			}
		}
		if gate := workflowScopedGateKey(source, flowID, mutation.SetGate); gate != "" {
			gates[gate] = true
		}
		mutation.StateCarrier.Gates = gates
	}
	if mutation.StateCarrier.Fields != nil && len(mutation.StateCarrier.Gates) == 0 && len(existingGates) > 0 {
		mutation.StateCarrier.Gates = workflowCloneBoolMap(existingGates)
	}
	if mutation.StateCarrier.Fields != nil || len(mutation.StateCarrier.Gates) > 0 {
		instance.Fields = mutation.StateCarrier.PersistedFields()
		instance.Bookkeeping = mutation.StateCarrier.PersistedBookkeeping()
		instance.Gates = cloneWorkflowGates(mutation.StateCarrier.Gates)
	}
	if mutation.StateCarrier.StateBuckets != nil {
		if err := supersedePriorLoopGenerationArtifacts(instance, previousBuckets, &mutation.StateCarrier); err != nil {
			return err
		}
		instance.StateBuckets = mutation.StateCarrier.PersistedStateBuckets()
	}
	if len(allowedFields) == 0 {
		return nil
	}
	entityProjection := workflowMutableStateBucket(instance, workflowStateBucketEntityProjection)
	if instance.Fields == nil {
		return nil
	}
	for targetField := range allowedFields {
		targetField = strings.TrimSpace(targetField)
		if targetField == "" {
			continue
		}
		value, ok := instance.Fields[targetField]
		if !ok {
			continue
		}
		entityProjection[targetField] = value
	}
	if len(entityProjection) > 0 {
		workflowSetStateBucket(instance, workflowStateBucketEntityProjection, entityProjection)
	}
	return nil
}

func (pc *PipelineCoordinator) maybeDeactivateTerminalFlowInstance(ctx context.Context, route runtimeflowidentity.Route, entityID identity.EntityID, nextState string) error {
	if pc == nil || pc.instanceDeactivator == nil || pc.workflowStore == nil || !pc.workflowStore.enabled() {
		return nil
	}
	nextState = strings.TrimSpace(nextState)
	entityID = identity.NormalizeEntityID(entityID.String())
	if nextState == "" || entityID.IsZero() {
		return nil
	}
	if !route.Valid() {
		return fmt.Errorf("flow deactivation requires an exact workflow instance route")
	}
	instance, ok, err := pc.workflowStore.Load(ctx, route)
	if err != nil || !ok {
		return err
	}
	templateID := strings.TrimSpace(instance.WorkflowName)
	if templateID == "" || !pc.isTerminalFlowState(templateID, nextState) {
		return nil
	}
	instanceIdentity, err := requireWorkflowInstanceIdentity(route, entityID, instance)
	if err != nil {
		return fmt.Errorf("validate terminal workflow instance owner: %w", err)
	}
	source := pc.SemanticSource()
	if source != nil {
		schema, ok := source.FlowSchemaByID(templateID)
		if !ok || !strings.EqualFold(strings.TrimSpace(schema.Mode), "template") {
			return nil
		}
	}
	return pc.instanceDeactivator(ctx, FlowInstanceDeactivationRequest{
		ContractBundle: source,
		Instance:       instanceIdentity,
		FinalState:     nextState,
	})
}

func (pc *PipelineCoordinator) isTerminalFlowState(flowID, state string) bool {
	if pc == nil {
		return false
	}
	state = strings.TrimSpace(state)
	if state == "" {
		return false
	}
	source := pc.SemanticSource()
	if source != nil {
		for _, terminal := range source.FlowTerminalStages(flowID) {
			if strings.EqualFold(strings.TrimSpace(terminal), state) {
				return true
			}
		}
		if len(source.FlowStates(flowID)) > 0 {
			return false
		}
	}
	workflow := pc.WorkflowDefinition()
	if workflow == nil {
		return false
	}
	stage, ok := workflow.Stage(NormalizeWorkflowStateID(state))
	return ok && stage.Terminal
}

func cloneEvent(evt events.Event) events.Event {
	return evt.Clone()
}

func workflowStateFromEngine(snapshot runtimeengine.StateSnapshot) *WorkflowState {
	state := &WorkflowState{
		EntityID: snapshot.EntityID.String(),
		Stage:    NormalizeWorkflowStateID(snapshot.CurrentState),
		Metadata: snapshot.StateCarrier.PersistedFields(),
		Control:  snapshot.StateCarrier.Control,
	}
	if state.Metadata == nil {
		state.Metadata = map[string]any{}
	}
	return state
}

func workflowInstanceOwnedByFlow(source semanticview.Source, instance WorkflowInstance, flowID, runID string) bool {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return true
	}
	if flowID == strings.TrimSpace(semanticview.RootExecutionFlowID(source)) {
		coordinate, err := semanticview.AdmitRootExecutionCoordinate(source, runID)
		return err == nil && strings.TrimSpace(instance.WorkflowName) == coordinate.FlowID() &&
			coordinate.Matches(flowID, strings.TrimSpace(instance.StorageRef))
	}
	ownerScope := runtimeflowidentity.ScopeKey(source, flowID)
	targetRoute, err := workflowInstanceRouteForPersisted(source, instance)
	if err != nil {
		return false
	}
	targetScope := targetRoute.ScopeKey
	if ownerScope == "" || targetScope == "" {
		return false
	}
	return ownerScope == targetScope
}

func workflowStateGatesForScope(source semanticview.Source, flowID string, persisted map[string]bool) map[string]bool {
	gates := cloneWorkflowGates(persisted)
	scopeKey := workflowScopeKey(source, flowID)
	if scopeKey == "" {
		return gates
	}
	prefix := scopeKey + "/"
	for key, value := range workflowCloneBoolMap(gates) {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		localKey := strings.TrimPrefix(key, prefix)
		localKey = strings.TrimSpace(localKey)
		if localKey == "" {
			continue
		}
		if _, exists := gates[localKey]; !exists {
			gates[localKey] = value
		}
	}
	return gates
}

func workflowCloneBoolMap(in map[string]bool) map[string]bool {
	if len(in) == 0 {
		return map[string]bool{}
	}
	out := make(map[string]bool, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func workflowScopedGateKey(source semanticview.Source, flowID, gate string) string {
	gate = strings.TrimSpace(gate)
	if gate == "" || strings.Contains(gate, "/") {
		return gate
	}
	scopeKey := workflowScopeKey(source, flowID)
	if scopeKey == "" {
		return gate
	}
	return strings.Trim(scopeKey+"/"+gate, "/")
}

func workflowInitialStateForFlow(source semanticview.Source, flowID string) string {
	flowID = strings.TrimSpace(flowID)
	if source == nil {
		return ""
	}
	if flowID == "" {
		return strings.TrimSpace(source.WorkflowInitialStage())
	}
	return strings.TrimSpace(source.FlowInitialStage(flowID))
}

func workflowScopeKey(source semanticview.Source, flowID string) string {
	return runtimeflowidentity.ScopeKey(source, flowID)
}

func workflowBoolGatesAsMap(gates map[string]bool) map[string]any {
	out := make(map[string]any, len(gates))
	for key, value := range gates {
		key = strings.TrimSpace(key)
		if key != "" {
			out[key] = value
		}
	}
	return out
}

func engineTriggerContext(req runtimeengine.ExecutionRequest) workflowTriggerContext {
	return workflowTriggerContext{
		Event: req.Event,
		State: WorkflowState{
			EntityID: req.EntityID.String(),
			Stage:    NormalizeWorkflowStateID(req.State.CurrentState),
			Metadata: req.State.StateCarrier.PersistedFields(),
			Control:  req.State.StateCarrier.Control,
		},
	}
}
