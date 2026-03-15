package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path"
	"strings"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
	"empireai/internal/runtime/core/identity"
	runtimeregistry "empireai/internal/runtime/core/registry"
	runtimeengine "empireai/internal/runtime/engine"
	"empireai/internal/runtime/semanticview"
)

type pipelineEngineEvaluator struct {
	evaluator *workflowExpressionEvaluator
}

func (e pipelineEngineEvaluator) EvalBool(expression string, ctx runtimeengine.BaseContext) (bool, error) {
	if e.evaluator == nil {
		return false, runtimeengine.ErrNotImplemented
	}
	return e.evaluator.EvalBool(expression, workflowExpressionContext{
		Entity:  cloneStringAnyMap(ctx.Entity.Raw()),
		Payload: cloneStringAnyMap(ctx.Payload.Raw()),
		Policy:  cloneStringAnyMap(ctx.Policy.Raw()),
		Vars: map[string]any{
			"metadata":    cloneStringAnyMap(ctx.Metadata.Raw()),
			"accumulated": cloneStringAnyMap(ctx.Accumulated.Raw()),
			"fan_out":     cloneStringAnyMap(ctx.FanOut.Raw()),
		},
	})
}

func (e pipelineEngineEvaluator) EvalValue(string, runtimeengine.BaseContext) (any, error) {
	return nil, runtimeengine.ErrNotImplemented
}

type pipelineEngineTx struct {
	ctx context.Context
	tx  *sql.Tx
}

func (t pipelineEngineTx) Context() context.Context { return t.ctx }

type pipelineEngineTxRunner struct {
	db *sql.DB
}

func (r pipelineEngineTxRunner) Run(ctx context.Context, fn func(runtimeengine.Tx) error) error {
	if r.db == nil {
		return fn(pipelineEngineTx{ctx: ctx})
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	postCommit := make([]func(), 0, 4)
	txctx := withPipelinePostCommitActions(withSQLTxContext(ctx, tx), &postCommit)
	if err := fn(pipelineEngineTx{ctx: txctx, tx: tx}); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	flushPipelinePostCommitActions(postCommit)
	return nil
}

type pipelineEngineLocker struct {
	coordinator *FactoryPipelineCoordinator
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
	coordinator *FactoryPipelineCoordinator
}

func (r pipelineEngineStateRepo) LoadState(ctx context.Context, entityID identity.EntityID) (runtimeengine.StateSnapshot, bool, error) {
	if r.coordinator == nil {
		return runtimeengine.StateSnapshot{}, false, nil
	}
	entityID = identity.NormalizeEntityID(entityID.String())
	if entityID.IsZero() {
		return runtimeengine.StateSnapshot{}, false, nil
	}
	state := r.coordinator.currentWorkflowState(ctx, entityID.String())
	out := runtimeengine.StateSnapshot{
		EntityID:     entityID,
		CurrentState: strings.TrimSpace(string(state.Stage)),
		Metadata:     cloneStringAnyMap(state.Metadata),
		Gates:        workflowStateGatesAsBools(state.Metadata),
		StateBuckets: map[string]any{},
	}
	if r.coordinator.workflowStore != nil && r.coordinator.workflowStore.Enabled() {
		instance, ok, err := r.coordinator.workflowStore.Load(ctx, entityID.String())
		if err != nil {
			return runtimeengine.StateSnapshot{}, false, err
		}
		if ok {
			out.WorkflowName = strings.TrimSpace(instance.WorkflowName)
			out.WorkflowVersion = strings.TrimSpace(instance.WorkflowVersion)
			out.CurrentState = strings.TrimSpace(instance.CurrentState)
			out.Metadata = cloneStringAnyMap(instance.Metadata)
			out.Gates = workflowStateGatesAsBools(instance.Metadata)
			out.StateBuckets = cloneStringAnyMap(instance.StateBuckets)
			out.TimerState = make([]runtimeengine.TimerState, 0, len(instance.TimerState))
			for _, timer := range instance.TimerState {
				out.TimerState = append(out.TimerState, runtimeengine.TimerState{
					TimerID:   strings.TrimSpace(timer.TimerID),
					EventType: strings.TrimSpace(timer.EventType),
					CreatedAt: timer.CreatedAt,
					FiresAt:   timer.FiresAt,
					StartedBy: strings.TrimSpace(timer.StartedBy),
					Recurring: timer.Recurring,
					Cancelled: timer.Cancelled,
				})
			}
		}
	}
	return out, true, nil
}

func (r pipelineEngineStateRepo) SaveState(ctx context.Context, entityID identity.EntityID, mutation runtimeengine.StateMutation) error {
	if r.coordinator == nil {
		return nil
	}
	entityID = identity.NormalizeEntityID(entityID.String())
	if entityID.IsZero() {
		return nil
	}
	if r.coordinator.workflowStore != nil && r.coordinator.workflowStore.Enabled() {
		allowedFields := workflowEntitySchemaFields(r.coordinator.SemanticSource())
		if err := r.coordinator.workflowStore.Mutate(ctx, entityID.String(), func(instance *WorkflowInstance) {
			applyEngineStateMutation(instance, mutation, allowedFields)
		}); err != nil {
			return err
		}
	}
	if next := strings.TrimSpace(mutation.NextState); next != "" {
		r.coordinator.updateEntityState(ctx, entityID.String(), next, "")
		if err := r.coordinator.maybeDeactivateTerminalFlowInstance(ctx, entityID.String(), next); err != nil {
			return err
		}
	}
	if len(mutation.ClearGates) > 0 || strings.TrimSpace(mutation.SetGate) != "" {
		r.coordinator.applyWorkflowGateMutation(ctx, entityID.String(), "", strings.TrimSpace(mutation.SetGate), len(mutation.ClearGates) > 0)
	}
	return nil
}

type pipelineEngineTimerApplier struct {
	coordinator *FactoryPipelineCoordinator
}

func (a pipelineEngineTimerApplier) ApplyTimerIntents(ctx context.Context, entityID identity.EntityID, intents []runtimeengine.TimerIntent) error {
	pc := a.coordinator
	if pc == nil || len(intents) == 0 {
		return nil
	}
	entityID = identity.NormalizeEntityID(entityID.String())
	if entityID.IsZero() {
		return nil
	}
	type transitionKey struct {
		from    string
		to      string
		trigger string
	}
	seen := map[transitionKey]struct{}{}
	for _, intent := range intents {
		key := transitionKey{
			from:    strings.TrimSpace(intent.FromState),
			to:      strings.TrimSpace(intent.ToState),
			trigger: strings.TrimSpace(intent.TriggerEvent),
		}
		if key.to == "" || key.from == "" || key.from == key.to {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if err := pc.applyWorkflowTimerIntents(ctx, entityID.String(), key.from, key.to, key.trigger); err != nil {
			return err
		}
	}
	return nil
}

func newCoordinatorEngineEvaluator(pc *FactoryPipelineCoordinator) runtimeengine.Evaluator {
	if pc == nil {
		return nil
	}
	return pipelineEngineEvaluator{evaluator: pc.expressionEval}
}

func coordinatorEngineDependencies(pc *FactoryPipelineCoordinator) runtimeengine.RuntimeDependencies {
	if pc == nil {
		return runtimeengine.RuntimeDependencies{}
	}
	source := pc.SemanticSource()
	if source == nil {
		source = semanticview.Wrap(&runtimecontracts.WorkflowContractBundle{})
	}
	var outbox runtimeengine.OutboxWriter
	var dispatcher runtimeengine.PostCommitDispatcher
	if pc.bus != nil {
		outbox = pc.bus.EngineOutbox()
		dispatcher = pc.bus.EngineDispatcher()
	}
	return runtimeengine.RuntimeDependencies{
		Source:              source,
		StateRepo:           pipelineEngineStateRepo{coordinator: pc},
		TxRunner:            pipelineEngineTxRunner{db: pc.db},
		Locker:              pipelineEngineLocker{coordinator: pc},
		Outbox:              outbox,
		TimerApplier:        pipelineEngineTimerApplier{coordinator: pc},
		Dispatcher:          dispatcher,
		GuardRegistry:       pipelineEngineGuardRegistry{registry: pc.GuardRegistry()},
		GuardRunner:         pipelineEngineGuardRunner{coordinator: pc},
		ActionRegistry:      pipelineEngineActionRegistry{registry: pc.ActionRegistry()},
		ActionRunner:        pipelineEngineActionRunner{coordinator: pc},
		PayloadShaper:       pipelineEnginePayloadShaper{coordinator: pc},
		TransitionValidator: pipelineEngineTransitionValidator{coordinator: pc},
		MaxChainDepth:       workflowMaxChainDepthPolicy(source),
	}
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
	coordinator *FactoryPipelineCoordinator
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
	return r.registry != nil && r.registry.HasAction(id)
}
func (r pipelineEngineActionRegistry) IsExecutable(id identity.ActionKey) bool {
	return r.registry != nil && r.registry.IsExecutable(id)
}
func (r pipelineEngineActionRegistry) Action(id identity.ActionKey) (runtimeregistry.ActionInstruction, bool) {
	if r.registry == nil {
		return runtimeregistry.ActionInstruction{}, false
	}
	return r.registry.Action(id)
}

type pipelineEngineGuardRunner struct {
	coordinator *FactoryPipelineCoordinator
}

func (r pipelineEngineGuardRunner) EvaluateGuard(ctx context.Context, id identity.GuardKey, entry runtimeregistry.GuardInstruction, execCtx runtimeengine.ExecutionContext) (bool, bool, error) {
	pc := r.coordinator
	if pc == nil {
		return false, false, nil
	}
	builtin := strings.TrimSpace(firstNonEmptyString(entry.Builtin, id.String()))
	state := workflowStateFromEngine(execCtx.Request.State)
	payload := parsePayloadMap(execCtx.Request.Event.Payload)
	switch builtin {
	case "has_entity_id":
		return strings.TrimSpace(execCtx.Request.EntityID.String()) != "", true, nil
	case "has_human_decision":
		source := strings.TrimSpace(execCtx.Request.Event.SourceAgent)
		if strings.EqualFold(source, "human") || strings.EqualFold(source, "mailbox") {
			return true, true, nil
		}
		if strings.EqualFold(strings.TrimSpace(asString(payload["decision_path"])), "mailbox") {
			return true, true, nil
		}
		return strings.TrimSpace(asString(payload["mailbox_decision_id"])) != "", true, nil
	case "not_in_terminal_state", "not_in_terminal_stage":
		if pc.SemanticSource() == nil {
			return true, true, nil
		}
		currentState := strings.TrimSpace(string(state.Stage))
		if currentState == "" {
			return true, true, nil
		}
		workflow := pc.WorkflowDefinition()
		if workflow != nil {
			if stage, ok := workflow.Stage(state.Stage); ok {
				return !stage.Terminal, true, nil
			}
		}
		for _, terminal := range pc.SemanticSource().WorkflowTerminalStages() {
			if strings.EqualFold(strings.TrimSpace(terminal), currentState) {
				return false, true, nil
			}
		}
		return true, true, nil
	case "revision_count_below_limit", "inner_revision_count_below_limit":
		limit := 3
		for _, key := range []string{strings.TrimSpace(entry.PolicyRef), "max_revisions"} {
			if key == "" {
				continue
			}
			if value, ok := workflowExpressionLookupPath(execCtx.Base.Policy.Raw(), key); ok {
				if parsed := asInt(value); parsed > 0 {
					limit = parsed
					break
				}
			}
			if parsed := asInt(execCtx.Base.Policy.Raw()[key]); parsed > 0 {
				limit = parsed
				break
			}
		}
		return asInt(state.Metadata["revision_count"]) < limit, true, nil
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

type pipelineEngineActionRunner struct {
	coordinator *FactoryPipelineCoordinator
}

func (r pipelineEngineActionRunner) ExecuteAction(ctx context.Context, action runtimecontracts.ActionSpec, entry runtimeregistry.ActionInstruction, execCtx runtimeengine.ExecutionContext) (bool, error) {
	pc := r.coordinator
	if pc == nil {
		return false, nil
	}
	actionID := strings.TrimSpace(firstNonEmptyString(entry.Builtin, entry.Key.String(), action.ID))
	if actionID == "" {
		return false, nil
	}
	switch strings.TrimSpace(action.ID) {
	case "increment_revision_count":
		if pc.workflowStore != nil && pc.workflowStore.Enabled() {
			_ = pc.workflowStore.Mutate(ctx, execCtx.Request.EntityID.String(), func(instance *WorkflowInstance) {
				metadata := workflowMutableMetadata(instance)
				metadata["revision_count"] = asInt(metadata["revision_count"]) + 1
			})
		}
		return true, nil
	case identity.ActionRecordStateChange.String(),
		identity.ActionUpdateState.String(),
		identity.ActionCancelStateTimers.String(),
		identity.ActionStartStateTimers.String():
		return true, nil
	case "record_evidence":
		payload := parsePayloadMap(execCtx.Request.Event.Payload)
		return pc.recordWorkflowEvidence(ctx, execCtx.Request.EntityID.String(), execCtx.Request.NodeID.String(), payload), nil
	case "create_flow_instance":
		plan := handlerExecutionPlan{
			NodeID:         execCtx.Request.NodeID.String(),
			EventType:      strings.TrimSpace(string(execCtx.Request.Event.Type)),
			Action:         strings.TrimSpace(action.ID),
			Template:       strings.TrimSpace(action.Template),
			InstanceIDFrom: strings.TrimSpace(action.InstanceIDFrom),
			InstanceIDPath: action.InstanceIDPath,
			ConfigFrom:     action.ConfigFrom,
		}
		return pc.createFlowInstance(ctx, engineTriggerContext(execCtx.Request), plan), nil
	default:
		return false, nil
	}
}

type pipelineEnginePayloadShaper struct {
	coordinator *FactoryPipelineCoordinator
}

func (s pipelineEnginePayloadShaper) ShapeEmitPayload(ctx context.Context, req runtimeengine.ExecutionRequest, eventType string, payload map[string]any) (map[string]any, error) {
	pc := s.coordinator
	if pc == nil {
		return cloneStringAnyMap(payload), nil
	}
	base := pc.handlerEmitPayload(ctx, engineTriggerContext(req), strings.TrimSpace(eventType))
	out := cloneStringAnyMap(base)
	if out == nil {
		out = map[string]any{}
	}
	for key, value := range payload {
		out[key] = value
	}
	return out, nil
}

func applyEngineStateMutation(instance *WorkflowInstance, mutation runtimeengine.StateMutation, allowedFields map[string]struct{}) {
	if instance == nil {
		return
	}
	if len(mutation.Gates) > 0 {
		if mutation.Metadata == nil {
			mutation.Metadata = cloneStringAnyMap(instance.Metadata)
		}
		mutation.Metadata["gates"] = workflowBoolGatesAsMap(mutation.Gates)
	}
	if next := strings.TrimSpace(mutation.NextState); next != "" {
		instance.CurrentState = next
	}
	if mutation.Metadata != nil {
		instance.Metadata = cloneStringAnyMap(mutation.Metadata)
	}
	if mutation.StateBuckets != nil {
		instance.StateBuckets = cloneStringAnyMap(mutation.StateBuckets)
	}
	if !mutation.DataAccumulation.HasWrites() {
		return
	}
	if len(allowedFields) == 0 {
		return
	}
	entityProjection := workflowMutableStateBucket(instance, workflowStateBucketEntityProjection)
	for _, write := range mutation.DataAccumulation.Writes {
		targetField := strings.TrimSpace(write.Target())
		switch {
		case strings.HasPrefix(targetField, "entity."):
			targetField = strings.TrimSpace(strings.TrimPrefix(targetField, "entity."))
		case strings.HasPrefix(targetField, "metadata."):
			targetField = strings.TrimSpace(strings.TrimPrefix(targetField, "metadata."))
		}
		if targetField == "" {
			continue
		}
		if _, ok := allowedFields[targetField]; !ok {
			continue
		}
		if instance.Metadata == nil {
			continue
		}
		value, ok := instance.Metadata[targetField]
		if !ok {
			continue
		}
		entityProjection[targetField] = value
	}
	if len(entityProjection) > 0 {
		workflowSetStateBucket(instance, workflowStateBucketEntityProjection, entityProjection)
	}
}

func (pc *FactoryPipelineCoordinator) maybeDeactivateTerminalFlowInstance(ctx context.Context, entityID, nextState string) error {
	if pc == nil || pc.instanceDeactivator == nil || pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return nil
	}
	nextState = strings.TrimSpace(nextState)
	entityID = strings.TrimSpace(entityID)
	if nextState == "" || entityID == "" {
		return nil
	}
	instance, ok, err := pc.workflowStore.Load(ctx, entityID)
	if err != nil || !ok {
		return err
	}
	templateID := strings.TrimSpace(instance.WorkflowName)
	if templateID == "" || !pc.isTerminalFlowState(templateID, nextState) {
		return nil
	}
	flowPath := strings.Trim(strings.TrimSpace(asString(instance.Metadata["flow_path"])), "/")
	instanceID := strings.TrimSpace(asString(instance.Metadata["instance_id"]))
	if instanceID == "" && flowPath != "" {
		instanceID = strings.TrimSpace(path.Base(flowPath))
	}
	if instanceID == "" {
		return nil
	}
	if flowPath == "" {
		flowPath = DeriveFlowInstancePath(pc.SemanticSource(), templateID, instanceID)
	}
	return pc.instanceDeactivator(ctx, FlowInstanceDeactivationRequest{
		ContractBundle: pc.SemanticSource(),
		TemplateID:     templateID,
		InstanceID:     instanceID,
		EntityID:       entityID,
		FlowPath:       flowPath,
		FinalState:     nextState,
	})
}

func (pc *FactoryPipelineCoordinator) isTerminalFlowState(flowID, state string) bool {
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
	}
	workflow := pc.WorkflowDefinition()
	if workflow == nil {
		return false
	}
	stage, ok := workflow.Stage(NormalizeWorkflowStateID(state))
	return ok && stage.Terminal
}

func cloneEvent(evt events.Event) events.Event {
	cloned := evt
	if len(evt.Payload) > 0 {
		cloned.Payload = append([]byte(nil), evt.Payload...)
	}
	return cloned
}

func workflowStateFromEngine(snapshot runtimeengine.StateSnapshot) *WorkflowState {
	state := &WorkflowState{
		EntityID: snapshot.EntityID.String(),
		Stage:    NormalizeWorkflowStateID(snapshot.CurrentState),
		Metadata: cloneStringAnyMap(snapshot.Metadata),
	}
	if state.Metadata == nil {
		state.Metadata = map[string]any{}
	}
	return state
}

func workflowStateGatesAsBools(metadata map[string]any) map[string]bool {
	raw, _ := metadata["gates"].(map[string]any)
	out := make(map[string]bool, len(raw))
	for key, value := range raw {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		if b, ok := value.(bool); ok {
			out[key] = b
		}
	}
	return out
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
	payload := parsePayloadMap(req.Event.Payload)
	if len(payload) == 0 {
		payload = map[string]any{}
		if !req.EntityID.IsZero() {
			payload["entity_id"] = req.EntityID.String()
			if encoded, err := json.Marshal(payload); err == nil {
				req.Event.Payload = encoded
			}
		}
	}
	return workflowTriggerContext{
		Event: req.Event,
		State: WorkflowState{
			EntityID: req.EntityID.String(),
			Stage:    NormalizeWorkflowStateID(req.State.CurrentState),
			Metadata: cloneStringAnyMap(req.State.Metadata),
		},
	}
}
