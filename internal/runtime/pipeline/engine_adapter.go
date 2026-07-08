package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	runtimepinrouting "github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	runtimeregistry "github.com/division-sh/swarm/internal/runtime/core/registry"
	runtimecurrentstate "github.com/division-sh/swarm/internal/runtime/currentstate"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	runtimeeventpayload "github.com/division-sh/swarm/internal/runtime/eventpayload"
	runtimeeventschema "github.com/division-sh/swarm/internal/runtime/eventschema"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
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
		WorkflowName:   firstNonEmptyString(strings.TrimSpace(ctx.FlowID), e.workflowName()),
	}
	queryCtx.QueryEntityCount = func(predicate string) (int, error) {
		return e.queryEntityCount(queryCtx, predicate)
	}
	return e.evaluator.EvalBool(expression, queryCtx)
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

type pipelineEngineTx struct {
	ctx context.Context
}

func (t pipelineEngineTx) Context() context.Context { return t.ctx }

type pipelineEngineTxRunner struct {
	store Store
}

func (r pipelineEngineTxRunner) Run(ctx context.Context, fn func(runtimeengine.Tx) error) error {
	if r.store == nil || !r.store.Enabled() {
		return fn(pipelineEngineTx{ctx: ctx})
	}
	return r.store.RunPipelineMutation(ctx, func(txctx context.Context) error {
		return fn(pipelineEngineTx{ctx: txctx})
	})
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
	flowID := strings.TrimSpace(pipelineFlowScope(ctx))
	if r.coordinator.workflowStore != nil && r.coordinator.workflowStore.Enabled() {
		instance, ok, err := r.coordinator.workflowStore.Load(ctx, entityID.String())
		if err != nil {
			return runtimeengine.StateSnapshot{}, false, err
		}
		if ok {
			carrier, err := runtimeengine.StateCarrierFromPersisted(workflowMaterializeEntityMetadata(r.coordinator.SemanticSource(), strings.TrimSpace(instance.WorkflowName), instance.Metadata), instance.StateBuckets)
			if err != nil {
				return runtimeengine.StateSnapshot{}, false, err
			}
			out := runtimeengine.StateSnapshot{
				EntityID:        entityID,
				WorkflowName:    strings.TrimSpace(instance.WorkflowName),
				WorkflowVersion: strings.TrimSpace(instance.WorkflowVersion),
				CurrentState:    strings.TrimSpace(instance.CurrentState),
				StateCarrier:    carrier,
				EnteredStateAt:  instance.EnteredStageAt,
			}
			out.StateCarrier.Gates = workflowStateGatesForScope(
				r.coordinator.SemanticSource(),
				pipelineFlowScope(ctx),
				out.StateCarrier.PersistedMetadata(),
			)
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
			return out, true, nil
		}
		return runtimeengine.StateSnapshot{}, false, nil
	}
	state := r.coordinator.currentWorkflowState(ctx, entityID.String())
	if strings.TrimSpace(string(state.Stage)) == "" && len(state.Metadata) == 0 {
		return runtimeengine.StateSnapshot{}, false, nil
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(workflowMaterializeEntityMetadata(r.coordinator.SemanticSource(), flowID, state.Metadata), nil)
	if err != nil {
		return runtimeengine.StateSnapshot{}, false, err
	}
	return runtimeengine.StateSnapshot{
		EntityID:     entityID,
		CurrentState: strings.TrimSpace(string(state.Stage)),
		StateCarrier: carrier,
	}, true, nil
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
		allowedFields := workflowEntitySchemaFields(r.coordinator.SemanticSource(), pipelineFlowScope(ctx))
		if err := r.coordinator.workflowStore.Mutate(ctx, entityID.String(), func(instance *WorkflowInstance) {
			applyEngineStateMutation(instance, mutation, allowedFields, r.coordinator.SemanticSource(), pipelineFlowScope(ctx))
		}); err != nil {
			return err
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
		actual, ok := workflowMetadataValue(persisted.StateCarrier.Metadata, field)
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
	if e.coordinator == nil || e.coordinator.workflowStore == nil || !e.coordinator.workflowStore.Enabled() {
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

func queryEntityStateCount(runID string, db *sql.DB, source semanticview.Source, contract entityruntime.Contract, predicate workflowEntityQueryPredicate) (int, error) {
	if db == nil {
		return 0, nil
	}
	runID, err := runtimecurrentstate.ValidateRunID(runID)
	if err != nil {
		return 0, err
	}
	flowRoot := runtimeflowidentity.ScopeKey(source, contract.FlowID)
	where := " WHERE run_id = $1::uuid"
	args := []any{runID}
	if flowRoot != "" {
		args = append(args, flowRoot, flowRoot+"/%")
		where += " AND (flow_instance = $2 OR flow_instance LIKE $3)"
	}
	rows, err := db.QueryContext(context.Background(), `
		SELECT COALESCE(fields, '{}'::jsonb), current_state
		FROM entity_state`+where, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var fieldsRaw []byte
		var currentState string
		if err := rows.Scan(&fieldsRaw, &currentState); err != nil {
			return 0, err
		}
		fields := map[string]any{}
		if len(fieldsRaw) > 0 {
			if err := json.Unmarshal(fieldsRaw, &fields); err != nil {
				return 0, err
			}
		}
		materialized, err := entityruntime.Materialize(contract, entityruntime.DeclaredValues(contract, fields))
		if err != nil {
			return 0, err
		}
		if workflowQueryPredicateMatches(map[string]any{
			"fields":         materialized,
			"current_state":  strings.TrimSpace(currentState),
			"entity_type":    contract.EntityType,
			"flow_instance":  flowRoot,
			"workflow_name":  contract.FlowID,
			"workflow_state": strings.TrimSpace(currentState),
		}, predicate) {
			count++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return count, nil
}

func workflowQueryPredicateMatches(row map[string]any, predicate workflowEntityQueryPredicate) bool {
	left := workflowQuerySelectorValue(row, predicate.Field)
	switch predicate.Op {
	case "==":
		return workflowJSONValuesEqual(left, predicate.Value)
	case "!=":
		return !workflowJSONValuesEqual(left, predicate.Value)
	}
	leftNum, leftNumOK := workflowNumericEntityValue(left)
	rightNum, rightNumOK := workflowNumericEntityValue(predicate.Value)
	if leftNumOK && rightNumOK {
		switch predicate.Op {
		case ">=":
			return leftNum >= rightNum
		case "<=":
			return leftNum <= rightNum
		case ">":
			return leftNum > rightNum
		case "<":
			return leftNum < rightNum
		}
	}
	leftText := fmt.Sprintf("%v", left)
	rightText := fmt.Sprintf("%v", predicate.Value)
	switch predicate.Op {
	case ">=":
		return leftText >= rightText
	case "<=":
		return leftText <= rightText
	case ">":
		return leftText > rightText
	case "<":
		return leftText < rightText
	default:
		return false
	}
}

func workflowNumericEntityValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	default:
		return 0, false
	}
}

func workflowQuerySelectorValue(row map[string]any, field string) any {
	field = strings.TrimSpace(field)
	if field == "" {
		return nil
	}
	if value, ok := row[field]; ok {
		return value
	}
	fields, _ := row["fields"].(map[string]any)
	if value, ok := workflowMetadataValue(fields, field); ok {
		return value
	}
	return nil
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
		TxRunner:            pipelineEngineTxRunner{store: pc.workflowStore},
		Locker:              pipelineEngineLocker{coordinator: pc},
		Outbox:              outbox,
		TimerApplier:        pipelineEngineTimerApplier{coordinator: pc},
		Dispatcher:          dispatcher,
		ActivityIntents:     pipelineActivityIntentWriter{coordinator: pc},
		ActivityDispatcher:  pipelineActivityDispatcher{coordinator: pc},
		GuardRegistry:       pipelineEngineGuardRegistry{registry: pc.GuardRegistry()},
		GuardRunner:         pipelineEngineGuardRunner{coordinator: pc},
		ActionRegistry:      pipelineEngineActionRegistry{registry: pc.ActionRegistry()},
		ActionRunner:        pipelineEngineActionRunner{coordinator: pc},
		PayloadShaper:       pipelineEnginePayloadShaper{coordinator: pc},
		TargetDescriptors:   pipelineEngineTargetDescriptorLoader(pc),
		TransitionValidator: pipelineEngineTransitionValidator{coordinator: pc},
		MaxChainDepth:       workflowMaxChainDepthPolicy(source),
	}
}

type pinRoutingDescriptorSource interface {
	PinRoutingDescriptors(context.Context) ([]runtimepinrouting.Descriptor, error)
}

func pipelineEngineTargetDescriptorLoader(pc *PipelineCoordinator) runtimeengine.TargetDescriptorLoader {
	if pc == nil || pc.bus == nil {
		return nil
	}
	source, ok := pc.bus.(pinRoutingDescriptorSource)
	if !ok || source == nil {
		return nil
	}
	return source.PinRoutingDescriptors
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
		source := strings.TrimSpace(execCtx.Request.Event.SourceAgent())
		if strings.EqualFold(source, "human") || strings.EqualFold(source, "mailbox") {
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
		flowID := strings.TrimSpace(execCtx.Request.FlowID.String())
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
	actionID := runtimecontracts.NormalizeHandlerActionID(firstNonEmptyString(entry.Builtin, entry.Key.String(), action.ID))
	if actionID == "" {
		return false, nil
	}
	switch actionID {
	case "record_evidence":
		payload := parsePayloadMap(execCtx.Request.Event.Payload())
		bucketID := recordEvidenceTarget(execCtx.Request)
		if bucketID == "" {
			return true, fmt.Errorf("node %s handler %s record_evidence is missing evidence_target", execCtx.Request.NodeID.String(), recordEvidenceHandlerLabel(execCtx.Request))
		}
		if err := pc.recordWorkflowEvidence(ctx, execCtx.Request.EntityID.String(), execCtx.Request.FlowID.String(), bucketID, payload); err != nil {
			return true, err
		}
		return true, nil
	case "create_flow_instance":
		plan := handlerExecutionPlan{
			NodeID:         execCtx.Request.NodeID.String(),
			EventType:      strings.TrimSpace(string(execCtx.Request.Event.Type())),
			Action:         actionID,
			Template:       strings.TrimSpace(action.Template),
			InstanceIDFrom: strings.TrimSpace(action.InstanceIDFrom),
			InstanceIDPath: action.InstanceIDPath,
			ConfigFrom:     action.ConfigFrom,
		}
		if err := pc.createFlowInstance(ctx, engineTriggerContext(execCtx.Request), plan, execCtx.Base); err != nil {
			return true, err
		}
		return true, nil
	case "mailbox_write":
		if err := pc.materializeMailboxItem(ctx, action, execCtx); err != nil {
			return true, err
		}
		return true, nil
	case "artifact_repo_commit":
		if err := pc.commitArtifactRepo(ctx, action, execCtx); err != nil {
			return true, err
		}
		return true, nil
	default:
		return false, nil
	}
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
	if err := validatePipelineEmitPayload(pc.SemanticSource(), req.FlowID.String(), eventType, out, envelope, runtimeengine.EmitSurfaceFromContext(ctx)); err != nil {
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

func applyEngineStateMutation(instance *WorkflowInstance, mutation runtimeengine.StateMutation, allowedFields map[string]struct{}, source semanticview.Source, flowID string) {
	if instance == nil {
		return
	}
	if instance.Metadata == nil {
		instance.Metadata = map[string]any{}
	}
	controlMetadata := workflowRuntimeControlMetadata(instance.Metadata)
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
	existingGates := workflowStateGatesAsBools(instance.Metadata)
	if len(mutation.StateCarrier.Gates) > 0 || len(mutation.ClearGates) > 0 || strings.TrimSpace(mutation.SetGate) != "" {
		if mutation.StateCarrier.Metadata == nil {
			mutation.StateCarrier.Metadata = cloneStringAnyMap(instance.Metadata)
			delete(mutation.StateCarrier.Metadata, "gates")
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
	if mutation.StateCarrier.Metadata != nil && len(mutation.StateCarrier.Gates) == 0 && len(existingGates) > 0 {
		mutation.StateCarrier.Gates = workflowCloneBoolMap(existingGates)
	}
	if mutation.StateCarrier.Metadata != nil || len(mutation.StateCarrier.Gates) > 0 {
		instance.Metadata = mutation.StateCarrier.PersistedMetadata()
		restoreWorkflowRuntimeControlMetadata(instance.Metadata, controlMetadata)
	}
	if mutation.StateCarrier.StateBuckets != nil {
		instance.StateBuckets = mutation.StateCarrier.PersistedStateBuckets()
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

var workflowRuntimeControlMetadataKeys = []string{
	"storage_ref",
	"instance_id",
	"flow_path",
	"entity_id",
	"workflow_version",
	"template_version",
	"instance_kind",
	"parent_flow_id",
	"parent_flow_instance",
	"parent_entity_id",
}

var workflowRuntimeParentRouteMetadataKeys = []string{
	"parent_flow_id",
	"parent_flow_instance",
	"parent_entity_id",
}

func workflowRuntimeControlMetadata(metadata map[string]any) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	control := make(map[string]any, len(workflowRuntimeControlMetadataKeys))
	for _, key := range workflowRuntimeControlMetadataKeys {
		value, ok := metadata[key]
		if ok {
			control[key] = value
		}
	}
	return control
}

func restoreWorkflowRuntimeControlMetadata(metadata map[string]any, control map[string]any) {
	if metadata == nil {
		return
	}
	for _, key := range workflowRuntimeControlMetadataKeys {
		value, ok := control[key]
		if ok {
			metadata[key] = value
		}
	}
	for _, key := range workflowRuntimeParentRouteMetadataKeys {
		if _, ok := control[key]; ok {
			continue
		}
		delete(metadata, key)
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
	return events.NewProjectionEvent(
		evt.ID(),
		evt.Type(),
		evt.SourceAgent(),
		evt.TaskID(),
		evt.Payload(),
		evt.ChainDepth(),
		evt.RunID(),
		evt.ParentEventID(),
		evt.NormalizedEnvelope(),
		evt.CreatedAt(),
	)
}

func workflowStateFromEngine(snapshot runtimeengine.StateSnapshot) *WorkflowState {
	state := &WorkflowState{
		EntityID: snapshot.EntityID.String(),
		Stage:    NormalizeWorkflowStateID(snapshot.CurrentState),
		Metadata: snapshot.StateCarrier.PersistedMetadata(),
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
	return workflowTriggerContext{
		Event: req.Event,
		State: WorkflowState{
			EntityID: req.EntityID.String(),
			Stage:    NormalizeWorkflowStateID(req.State.CurrentState),
			Metadata: req.State.StateCarrier.PersistedMetadata(),
		},
	}
}
