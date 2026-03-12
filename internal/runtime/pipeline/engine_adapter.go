package pipeline

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"empireai/internal/events"
	runtimecontracts "empireai/internal/runtime/contracts"
	runtimeengine "empireai/internal/runtime/engine"
	"empireai/internal/runtime/identity"
	"empireai/internal/runtime/semanticview"
	"github.com/google/uuid"
)

type pipelineEngineEvaluator struct {
	evaluator *workflowExpressionEvaluator
}

func (e pipelineEngineEvaluator) EvalBool(expression string, ctx runtimeengine.BaseContext) (bool, error) {
	if e.evaluator == nil {
		return false, runtimeengine.ErrNotImplemented
	}
	return e.evaluator.EvalBool(expression, workflowExpressionContext{
		Entity:  cloneStringAnyMap(ctx.Entity),
		Payload: cloneStringAnyMap(ctx.Payload),
		Policy:  cloneStringAnyMap(ctx.Policy),
		Vars: map[string]any{
			"metadata":    cloneStringAnyMap(ctx.Metadata),
			"accumulated": cloneStringAnyMap(ctx.Accumulated),
			"fan_out":     cloneStringAnyMap(ctx.FanOut),
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
		r.coordinator.updateVerticalStage(ctx, entityID.String(), next, "")
	}
	if len(mutation.ClearGates) > 0 || strings.TrimSpace(mutation.SetGate) != "" {
		r.coordinator.applyWorkflowGateMutation(ctx, entityID.String(), "", strings.TrimSpace(mutation.SetGate), len(mutation.ClearGates) > 0)
	}
	return nil
}

type pipelineEngineOutbox struct{}

func (o pipelineEngineOutbox) WriteOutbox(ctx context.Context, intents []runtimeengine.EmitIntent) error {
	tx, ok := sqlTxFromContext(ctx)
	if !ok || tx == nil || len(intents) == 0 {
		return nil
	}
	for _, intent := range intents {
		evt := intent.Event
		if strings.TrimSpace(string(evt.Type)) == "" {
			continue
		}
		if strings.TrimSpace(evt.ID) == "" {
			evt.ID = uuid.NewString()
		}
		if evt.CreatedAt.IsZero() {
			evt.CreatedAt = time.Now().UTC()
		}
		if err := appendEventTx(ctx, tx, evt); err != nil {
			return err
		}
		if err := insertEventDeliveriesTx(ctx, tx, evt.ID, intent.Recipients); err != nil {
			return err
		}
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

type pipelineEngineDispatcher struct {
	coordinator *FactoryPipelineCoordinator
}

func (d pipelineEngineDispatcher) DispatchPostCommit(ctx context.Context, intents []runtimeengine.EmitIntent) error {
	if d.coordinator == nil || len(intents) == 0 {
		return nil
	}
	var collected bool
	if collector, ok := ctx.Value(pipelineEmitIntentCollectorKey{}).(*[]runtimeengine.EmitIntent); ok && collector != nil {
		*collector = append(*collector, cloneEmitIntents(intents)...)
		collected = true
	}
	if collector, ok := ctx.Value(pipelineEmitCollectorKey{}).(*[]events.Event); ok && collector != nil {
		for _, intent := range intents {
			*collector = append(*collector, intent.Event)
		}
		collected = true
	}
	if collected {
		return nil
	}
	for _, intent := range intents {
		if len(intent.Recipients) > 0 {
			if err := d.coordinator.bus.PublishDirect(ctx, intent.Event, intent.Recipients); err != nil {
				return err
			}
			continue
		}
		if err := d.coordinator.bus.Publish(ctx, intent.Event); err != nil {
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
	return runtimeengine.RuntimeDependencies{
		Source:         source,
		StateRepo:      pipelineEngineStateRepo{coordinator: pc},
		TxRunner:       pipelineEngineTxRunner{db: pc.db},
		Locker:         pipelineEngineLocker{coordinator: pc},
		Outbox:         pipelineEngineOutbox{},
		TimerApplier:   pipelineEngineTimerApplier{coordinator: pc},
		Dispatcher:     pipelineEngineDispatcher{coordinator: pc},
		GuardRegistry:  pipelineEngineGuardRegistry{registry: pc.GuardRegistry()},
		GuardRunner:    pipelineEngineGuardRunner{coordinator: pc},
		ActionRegistry: pipelineEngineActionRegistry{registry: pc.ActionRegistry()},
		ActionRunner:   pipelineEngineActionRunner{coordinator: pc},
		PayloadShaper:  pipelineEnginePayloadShaper{coordinator: pc},
		MaxChainDepth:  runtimeengine.DefaultMaxChainDepth,
	}
}

type pipelineEngineGuardRegistry struct{ registry GuardRegistry }

func (r pipelineEngineGuardRegistry) HasGuard(id string) bool {
	return r.registry != nil && r.registry.HasGuard(id)
}
func (r pipelineEngineGuardRegistry) IsExecutable(id string) bool {
	return r.registry != nil && r.registry.IsExecutable(id)
}
func (r pipelineEngineGuardRegistry) Guard(id string) (runtimecontracts.GuardActionEntry, bool) {
	if r.registry == nil {
		return runtimecontracts.GuardActionEntry{}, false
	}
	return r.registry.Guard(id)
}

type pipelineEngineActionRegistry struct{ registry ActionRegistry }

func (r pipelineEngineActionRegistry) HasAction(id string) bool {
	return r.registry != nil && r.registry.HasAction(id)
}
func (r pipelineEngineActionRegistry) IsExecutable(id string) bool {
	return r.registry != nil && r.registry.IsExecutable(id)
}
func (r pipelineEngineActionRegistry) Action(id string) (runtimecontracts.GuardActionEntry, bool) {
	if r.registry == nil {
		return runtimecontracts.GuardActionEntry{}, false
	}
	return r.registry.Action(id)
}

type pipelineEngineGuardRunner struct {
	coordinator *FactoryPipelineCoordinator
}

func (r pipelineEngineGuardRunner) EvaluateGuard(ctx context.Context, id string, entry runtimecontracts.GuardActionEntry, execCtx runtimeengine.ExecutionContext) (bool, bool, error) {
	pc := r.coordinator
	if pc == nil {
		return false, false, nil
	}
	handler, ok := lookupWorkflowBuiltinGuard(firstNonEmptyString(entry.PlatformBuiltin, id))
	if !ok {
		return false, false, nil
	}
	exec := &handlerEngineExecution{
		ctx:         ctx,
		scope:       &handlerEngineContext{coordinator: pc, nodeID: execCtx.Request.NodeID.String()},
		state:       workflowStateFromEngine(execCtx.Request.State),
		handler:     execCtx.Request.Handler,
		event:       execCtx.Request.Event,
		payload:     parsePayloadMap(execCtx.Request.Event.Payload),
		entityID:    execCtx.Request.EntityID.String(),
		policy:      cloneStringAnyMap(execCtx.Base.Policy),
		accumulated: cloneStringAnyMap(execCtx.Base.Accumulated),
		fanOut:      cloneStringAnyMap(execCtx.Base.FanOut),
	}
	return handler(exec, strings.TrimSpace(entry.PolicyRef))
}

type pipelineEngineActionRunner struct {
	coordinator *FactoryPipelineCoordinator
}

func (r pipelineEngineActionRunner) ExecuteAction(ctx context.Context, action runtimecontracts.ActionSpec, entry runtimecontracts.GuardActionEntry, execCtx runtimeengine.ExecutionContext) (bool, error) {
	pc := r.coordinator
	if pc == nil {
		return false, nil
	}
	actionID := strings.TrimSpace(firstNonEmptyString(entry.PlatformBuiltin, entry.ID, action.ID))
	if actionID == "" {
		return false, nil
	}
	if handler, ok := lookupWorkflowBuiltinAction(actionID); ok {
		hookCtx := workflowHookContextFromTrigger(engineTriggerContext(execCtx.Request))
		return handler(ctx, pc, hookCtx, strings.TrimSpace(entry.PolicyRef))
	}
	switch strings.TrimSpace(action.ID) {
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
		targetField := normalizeHandlerStateField(write.Target())
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

func cloneEmitIntents(intents []runtimeengine.EmitIntent) []runtimeengine.EmitIntent {
	if len(intents) == 0 {
		return nil
	}
	out := make([]runtimeengine.EmitIntent, 0, len(intents))
	for _, intent := range intents {
		cloned := intent
		cloned.Event = cloneEvent(intent.Event)
		cloned.Recipients = append([]string{}, intent.Recipients...)
		out = append(out, cloned)
	}
	return out
}

func cloneEvent(evt events.Event) events.Event {
	cloned := evt
	if len(evt.Payload) > 0 {
		cloned.Payload = append([]byte(nil), evt.Payload...)
	}
	return cloned
}

func appendEventTx(ctx context.Context, tx *sql.Tx, evt events.Event) error {
	if tx == nil {
		return nil
	}
	const q = `
		INSERT INTO events (id, type, source_agent, task_id, vertical_id, payload, created_at)
		VALUES ($1::uuid, $2, $3, NULLIF($4,'')::uuid, NULLIF($5,'')::uuid, $6, $7)
		ON CONFLICT (id) DO NOTHING
	`
	_, err := tx.ExecContext(
		ctx,
		q,
		strings.TrimSpace(evt.ID),
		strings.TrimSpace(string(evt.Type)),
		strings.TrimSpace(evt.SourceAgent),
		sanitizeOptionalUUID(strings.TrimSpace(evt.TaskID)),
		sanitizeOptionalUUID(strings.TrimSpace(evt.EntityID())),
		evt.Payload,
		evt.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("append event: %w", err)
	}
	return nil
}

func insertEventDeliveriesTx(ctx context.Context, tx *sql.Tx, eventID string, recipients []string) error {
	if tx == nil || strings.TrimSpace(eventID) == "" {
		return nil
	}
	recipients = uniqueStrings(recipients)
	if len(recipients) == 0 {
		return nil
	}
	const q = `
		INSERT INTO event_deliveries (event_id, agent_id, created_at)
		VALUES ($1::uuid, $2, now())
		ON CONFLICT (event_id, agent_id) DO NOTHING
	`
	for _, recipient := range recipients {
		recipient = strings.TrimSpace(recipient)
		if recipient == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, q, eventID, recipient); err != nil {
			return fmt.Errorf("insert event delivery (agent=%s): %w", recipient, err)
		}
	}
	return nil
}

func sanitizeOptionalUUID(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if _, err := uuid.Parse(raw); err != nil {
		return ""
	}
	return raw
}

func workflowStateFromEngine(snapshot runtimeengine.StateSnapshot) *WorkflowState {
	state := &WorkflowState{
		VerticalID: snapshot.EntityID.String(),
		Stage:      NormalizePipelineStage(snapshot.CurrentState),
		Metadata:   cloneStringAnyMap(snapshot.Metadata),
	}
	if state.Metadata == nil {
		state.Metadata = map[string]any{}
	}
	return state
}

func engineTriggerContext(req runtimeengine.ExecutionRequest) workflowTriggerContext {
	payload := parsePayloadMap(req.Event.Payload)
	if len(payload) == 0 {
		payload = map[string]any{}
		if !req.EntityID.IsZero() {
			payload["vertical_id"] = req.EntityID.String()
			if encoded, err := json.Marshal(payload); err == nil {
				req.Event.Payload = encoded
			}
		}
	}
	return workflowTriggerContext{
		Event: req.Event,
		State: WorkflowState{
			VerticalID: req.EntityID.String(),
			Stage:      NormalizePipelineStage(req.State.CurrentState),
			Metadata:   cloneStringAnyMap(req.State.Metadata),
		},
	}
}
