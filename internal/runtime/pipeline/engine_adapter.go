package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"swarm/internal/events"
	runtimecontracts "swarm/internal/runtime/contracts"
	runtimeflowidentity "swarm/internal/runtime/core/flowidentity"
	"swarm/internal/runtime/core/identity"
	"swarm/internal/runtime/core/paths"
	runtimeregistry "swarm/internal/runtime/core/registry"
	runtimeengine "swarm/internal/runtime/engine"
	"swarm/internal/runtime/semanticview"
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
		Entity:      cloneStringAnyMap(ctx.Entity.Raw()),
		Payload:     cloneStringAnyMap(ctx.Payload.Raw()),
		Policy:      cloneStringAnyMap(ctx.Policy.Raw()),
		Accumulated: accumulatedItemsForCEL(ctx.Accumulated.Raw()),
		FanOut:      cloneStringAnyMap(ctx.FanOut.Raw()),
	}
	queryCtx.QueryEntityCount = func(predicate string) (int, error) {
		return e.queryEntityCount(queryCtx, predicate)
	}
	return e.evaluator.EvalBool(expression, queryCtx)
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
	if tx, ok := sqlTxFromContext(ctx); ok && tx != nil {
		return fn(pipelineEngineTx{ctx: ctx, tx: tx})
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
			out.Metadata = cloneStringAnyMap(instance.Metadata)
			if out.Metadata == nil {
				out.Metadata = map[string]any{}
			}
			if strings.TrimSpace(instance.SubjectID) != "" {
				out.Metadata["subject_id"] = strings.TrimSpace(instance.SubjectID)
			}
			out.CurrentState = strings.TrimSpace(instance.CurrentState)
			out.Gates = workflowStateGatesForScope(
				r.coordinator.SemanticSource(),
				pipelineFlowScope(ctx),
				out.Metadata,
			)
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
		if err := r.ensureFlowOwnsEntity(ctx, entityID.String()); err != nil {
			return err
		}
		allowedFields := workflowEntitySchemaFields(r.coordinator.SemanticSource())
		if err := r.coordinator.workflowStore.Mutate(ctx, entityID.String(), func(instance *WorkflowInstance) {
			applyEngineStateMutation(instance, mutation, allowedFields, r.coordinator.SemanticSource(), pipelineFlowScope(ctx))
		}); err != nil {
			return err
		}
		if len(mutation.Gates) > 0 || len(mutation.ClearGates) > 0 || strings.TrimSpace(mutation.SetGate) != "" {
			if err := r.coordinator.projectWorkflowSubjectGates(ctx, entityID.String()); err != nil {
				return err
			}
		}
	}
	if next := strings.TrimSpace(mutation.NextState); next != "" {
		if err := r.coordinator.updateEntityState(ctx, entityID.String(), next, ""); err != nil {
			return err
		}
		if err := r.coordinator.maybeDeactivateTerminalFlowInstance(ctx, entityID.String(), next); err != nil {
			return err
		}
	}
	return nil
}

func (r pipelineEngineStateRepo) VerifyEmitPersistence(ctx context.Context, entityID identity.EntityID, prerequisites runtimeengine.EmitPersistencePrerequisites) error {
	if r.coordinator == nil || r.coordinator.workflowStore == nil || !r.coordinator.workflowStore.Enabled() {
		return nil
	}
	entityID = identity.NormalizeEntityID(entityID.String())
	if entityID.IsZero() || len(prerequisites.Fields) == 0 {
		return nil
	}
	persisted, ok, err := r.LoadState(ctx, entityID)
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
		actual, ok := workflowMetadataValue(persisted.Metadata, field)
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

func (r pipelineEngineStateRepo) ensureFlowOwnsEntity(ctx context.Context, entityID string) error {
	if r.coordinator == nil || r.coordinator.workflowStore == nil || !r.coordinator.workflowStore.Enabled() {
		return nil
	}
	flowID := strings.TrimSpace(pipelineFlowScope(ctx))
	if flowID == "" {
		return nil
	}
	instance, ok, err := r.coordinator.workflowStore.Load(ctx, entityID)
	if err != nil || !ok {
		return err
	}
	if workflowInstanceOwnedByFlow(r.coordinator.SemanticSource(), instance, flowID) {
		return nil
	}
	return fmt.Errorf("cross_flow_write_forbidden: flow %s cannot write entity %s owned by workflow %s", flowID, entityID, strings.TrimSpace(instance.WorkflowName))
}

type pipelineEngineTimerApplier struct {
	coordinator *PipelineCoordinator
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

func newCoordinatorEngineEvaluator(pc *PipelineCoordinator) runtimeengine.Evaluator {
	if pc == nil {
		return nil
	}
	return pipelineEngineEvaluator{evaluator: pc.expressionEval, coordinator: pc}
}

func (e pipelineEngineEvaluator) queryEntityCount(ctx workflowExpressionContext, predicate string) (int, error) {
	if e.coordinator == nil || e.coordinator.db == nil {
		return 0, nil
	}
	parsed, err := parseWorkflowEntityQueryPredicate(predicate, ctx)
	if err != nil {
		return 0, err
	}
	return queryEntityStateCount(e.coordinator.db, parsed)
}

func queryEntityStateCount(db *sql.DB, predicate workflowEntityQueryPredicate) (int, error) {
	if db == nil {
		return 0, nil
	}
	var (
		query string
		args  []any
	)
	op, err := sqlComparisonOperator(predicate.Op)
	if err != nil {
		return 0, err
	}
	value := fmt.Sprintf("%v", predicate.Value)
	switch strings.TrimSpace(predicate.Field) {
	case "current_state", "name", "slug", "entity_type":
		query = fmt.Sprintf(`SELECT COUNT(*) FROM entity_state WHERE %s %s $1`, predicate.Field, op)
		args = []any{value}
	default:
		query = fmt.Sprintf(`SELECT COUNT(*) FROM entity_state WHERE fields ->> $1 %s $2`, op)
		args = []any{strings.TrimSpace(predicate.Field), value}
	}
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func sqlComparisonOperator(op string) (string, error) {
	switch strings.TrimSpace(op) {
	case "==":
		return "=", nil
	case "!=", ">=", "<=", ">", "<":
		return strings.TrimSpace(op), nil
	default:
		return "", fmt.Errorf("unsupported query_entities operator %q", op)
	}
}

func coordinatorEngineDependencies(pc *PipelineCoordinator) runtimeengine.RuntimeDependencies {
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
		EmitVerifier:        pipelineEngineStateRepo{coordinator: pc},
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

func workflowJSONValuesEqual(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
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
	return isSupportedWorkflowHandlerActionID(id.String())
}
func (r pipelineEngineActionRegistry) IsExecutable(id identity.ActionKey) bool {
	if r.registry != nil && r.registry.IsExecutable(id) {
		return true
	}
	return isSupportedWorkflowHandlerActionID(id.String())
}
func (r pipelineEngineActionRegistry) Action(id identity.ActionKey) (runtimeregistry.ActionInstruction, bool) {
	if r.registry != nil {
		if instruction, ok := r.registry.Action(id); ok {
			return instruction, true
		}
	}
	if !isSupportedWorkflowHandlerActionID(id.String()) {
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
	coordinator *PipelineCoordinator
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
		bucketID := pc.evidenceTargetForHandler(execCtx.Request.NodeID.String(), string(execCtx.Request.Event.Type))
		if bucketID == "" {
			return true, fmt.Errorf("node %s handler %s record_evidence is missing evidence_target", execCtx.Request.NodeID.String(), strings.TrimSpace(string(execCtx.Request.Event.Type)))
		}
		if err := pc.recordWorkflowEvidence(ctx, execCtx.Request.EntityID.String(), bucketID, payload); err != nil {
			return true, err
		}
		return true, nil
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
		if err := pc.createFlowInstance(ctx, engineTriggerContext(execCtx.Request), plan); err != nil {
			return true, err
		}
		return true, nil
	default:
		return false, nil
	}
}

func (pc *PipelineCoordinator) evidenceTargetForHandler(nodeID, eventType string) string {
	if pc == nil {
		return ""
	}
	source := pc.SemanticSource()
	if source == nil {
		return ""
	}
	handler, ok := source.NodeEventHandler(strings.TrimSpace(nodeID), strings.TrimSpace(eventType))
	if !ok {
		return ""
	}
	return strings.TrimSpace(handler.EvidenceTarget)
}

type pipelineEnginePayloadShaper struct {
	coordinator *PipelineCoordinator
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
		if strings.TrimSpace(key) == "entity_id" {
			continue
		}
		out[key] = value
	}
	state := workflowStateFromEngine(req.State)
	entityID := resolveEmittedEntityID(
		pc.SemanticSource(),
		req.FlowID.String(),
		eventType,
		*state,
		req.Event,
		req.EntityID.String(),
		asString(base["entity_id"]),
	)
	if entityID == "" && !req.EntityID.IsZero() {
		entityID = strings.TrimSpace(req.EntityID.String())
	}
	if entityID != "" {
		out["entity_id"] = entityID
	}
	return out, nil
}

func applyEngineStateMutation(instance *WorkflowInstance, mutation runtimeengine.StateMutation, allowedFields map[string]struct{}, source semanticview.Source, flowID string) {
	if instance == nil {
		return
	}
	if instance.Metadata == nil {
		instance.Metadata = map[string]any{}
	}
	if strings.TrimSpace(instance.WorkflowName) == "" {
		defaultWorkflowName := strings.TrimSpace(flowID)
		if defaultWorkflowName == "" && source != nil {
			defaultWorkflowName = strings.TrimSpace(source.WorkflowName())
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
		instance.EnteredStageAt = time.Now().UTC()
	}
	if len(mutation.Gates) > 0 || len(mutation.ClearGates) > 0 || strings.TrimSpace(mutation.SetGate) != "" {
		if mutation.Metadata == nil {
			mutation.Metadata = cloneStringAnyMap(instance.Metadata)
		}
		gates := workflowStateGatesAsBools(instance.Metadata)
		for key, value := range mutation.Gates {
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
		mutation.Metadata["gates"] = workflowBoolGatesAsMap(gates)
	}
	if mutation.Metadata != nil {
		instance.Metadata = cloneStringAnyMap(mutation.Metadata)
	}
	if instance.Metadata != nil && strings.TrimSpace(instance.SubjectID) == "" {
		instance.SubjectID = workflowInstanceIdentity(source, *instance).SubjectID
	}
	if mutation.StateBuckets != nil {
		instance.StateBuckets = cloneStringAnyMap(mutation.StateBuckets)
	}
	if len(allowedFields) == 0 {
		return
	}
	entityProjection := workflowMutableStateBucket(instance, workflowStateBucketEntityProjection)
	if instance.Metadata == nil {
		return
	}
	for targetField := range allowedFields {
		targetField = strings.TrimSpace(targetField)
		if targetField == "" {
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

func (pc *PipelineCoordinator) maybeDeactivateTerminalFlowInstance(ctx context.Context, entityID, nextState string) error {
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
	source := pc.SemanticSource()
	if source != nil {
		schema, ok := source.FlowSchemaByID(templateID)
		if !ok || !strings.EqualFold(strings.TrimSpace(schema.Mode), "template") {
			return nil
		}
	}
	instanceIdentity := workflowInstanceIdentity(source, instance)
	if !instanceIdentity.HasStoredPath {
		return nil
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

func workflowInstanceOwnedByFlow(source semanticview.Source, instance WorkflowInstance, flowID string) bool {
	flowID = strings.TrimSpace(flowID)
	if flowID == "" {
		return true
	}
	ownerScope := runtimeflowidentity.ScopeKey(source, flowID)
	targetScope := workflowInstanceScopeKey(source, instance)
	if ownerScope == "" || targetScope == "" {
		return false
	}
	return ownerScope == targetScope
}

func workflowStateGatesForScope(source semanticview.Source, flowID string, metadata map[string]any) map[string]bool {
	gates := workflowStateGatesAsBools(metadata)
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
