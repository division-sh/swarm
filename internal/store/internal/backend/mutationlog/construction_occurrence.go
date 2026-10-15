package mutationlog

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/workflowlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

// ConstructionOccurrence carries only copied semantic evidence, never an attempt
// or transaction. Birth facts are consumed once inside its one live attempt.
type ConstructionOccurrence struct {
	attemptID uuid.UUID
	runID     string
	source    correlation.SourceArtifactFact
	at        time.Time
	states    map[string][32]byte
	births    map[[32]byte]bool
}

// FixedForkConstructionOccurrenceForTest is the sole mint for the closed fork
// witnesses below. It does not accept arbitrary historical writes.
func FixedForkConstructionOccurrenceForTest(ctx context.Context, attempt *mutationprotocol.Attempt, parent *pipeline.FlowInstanceActivationPlan, child pipeline.FlowInstanceActivationPlan, creatingEvent events.Event) (*ConstructionOccurrence, error) {
	plans, err := fixedForkConstructionPlans(parent, child, creatingEvent)
	if err != nil {
		return nil, err
	}
	if creatingEvent.ID() == "" || creatingEvent.ID() != child.CreatingInput.EventID || creatingEvent.RunID() != child.Readiness.RunID ||
		!creatingEvent.CreatedAt().Equal(child.OccurredAt) {
		return nil, fmt.Errorf("construction occurrence requires its exact prepared creating event/time")
	}
	id, err := attempt.Identity(ctx)
	if err != nil {
		return nil, err
	}
	source, ok := correlation.SourceArtifactFactFromContext(ctx)
	if !ok || source.Validate() != nil || source.BundleHash() != child.Readiness.BundleHash || correlation.RunIDFromContext(ctx) != child.Readiness.RunID {
		return nil, fmt.Errorf("construction occurrence requires its exact run/source")
	}
	value := &ConstructionOccurrence{attemptID: id, runID: child.Readiness.RunID, source: source, at: child.OccurredAt.UTC().Truncate(time.Microsecond), states: map[string][32]byte{}, births: map[[32]byte]bool{}}
	for _, plan := range plans {
		if err := value.addConstructionBirth(plan); err != nil {
			return nil, err
		}
	}
	return value, nil
}

func fixedForkConstructionPlans(parent *pipeline.FlowInstanceActivationPlan, child pipeline.FlowInstanceActivationPlan, event events.Event) ([]pipeline.FlowInstanceActivationPlan, error) {
	if child.OccurredAt.IsZero() || !child.OccurredAt.UTC().Truncate(time.Microsecond).Equal(child.Instance.CreatedAt) || !child.OccurredAt.UTC().Truncate(time.Microsecond).Equal(child.Instance.EnteredStageAt) {
		return nil, fmt.Errorf("conversation construction requires its exact historical occurrence")
	}
	if !fixedForkConstructionWitness(child, event) || len(child.Children) != 0 {
		return nil, fmt.Errorf("historical construction requires a complete enumerated fork witness")
	}
	if string(event.Type()) == "fork.state" && parent != nil {
		return nil, fmt.Errorf("route/timer witness requires exactly one parentless component")
	}
	plans := []pipeline.FlowInstanceActivationPlan{child}
	if parent != nil {
		if parent.Identity.TemplateID != "." || parent.Identity.ParentRoute != (flowidentity.ParentRoute{}) || parent.Identity.ParentEntityID != "" ||
			parent.Instance.Mode != "static" || parent.Instance.InstanceKey != "" || parent.Instance.EntityType != "" ||
			parent.Instance.ParentFlowID != "" || parent.Instance.ParentFlowInstance != "" || parent.Instance.ParentEntityID != "" ||
			parent.Identity != parent.Readiness.Identity || parent.Instance.EntityID != parent.Identity.EntityID || parent.Instance.StorageRef != parent.Identity.InstancePath ||
			parent.Identity.InstancePath != child.Identity.ParentRoute.FlowInstance || parent.Identity.EntityID != child.Readiness.RunID || parent.Readiness.RunID != child.Readiness.RunID || parent.Readiness.BundleHash != child.Readiness.BundleHash || !parent.OccurredAt.Equal(child.OccurredAt) || len(parent.Instance.Fields) != 0 || len(parent.Children) != 0 {
			return nil, fmt.Errorf("conversation construction requires its exact fieldless parent")
		}
		plans = append(plans, *parent)
	}
	return plans, nil
}

// Each tuple is one complete witness, not a cross-product of permitted fields.
// Run/entity/source and the immutable birth record are bound by the mint below.
func fixedForkConstructionWitness(plan pipeline.FlowInstanceActivationPlan, event events.Event) bool {
	var fields map[string]any
	var key, input, eventType string
	component := false
	switch plan.Identity.TemplateID + "|" + plan.Identity.InstancePath + "|" + plan.Instance.EntityType + "|" + plan.Instance.CurrentState {
	case "flow|flow/forkchat|default|draft":
		fields, key = map[string]any{"name": "Before"}, "Before"
		input, eventType = "conversation.created", "flow/conversation.created"
	case "flow-a|flow-a/1|fork_entity|ready":
		fields, key = map[string]any{"name": "Snapshot Entity"}, "Snapshot Entity"
		input, eventType = "matrix.event", "matrix.event"
	case "flow-a|flow-a/1|historical_entity|queued":
		fields, key = map[string]any{
			"title": "before-title", "slug": "before-slug", "name": "Before Name",
			"gates": map[string]any{"review": "authored-review"}, "accumulator": map[string]any{"total": "authored-total"},
			"bookkeeping": map[string]any{"activation": "manual"},
		}, "Before Name"
		input, eventType = "fork.before", "fork.before"
	case "flow-a|flow-a/1|default|ready":
		if string(event.Type()) == "fork.state" {
			fields = map[string]any{}
			input, eventType, component = "fork.state", "fork.state", true
		} else {
			fields, key = map[string]any{"fixture_key": "1"}, "1"
			input, eventType = "fork.pending", "fork.pending"
		}
	case "state-flow|state-flow/at-T|validation_case|pending":
		fields, key = map[string]any{"name": "Fork Point Name"}, "Fork Point Name"
		input, eventType = "fork.no_event_flow", "fork.no_event_flow"
	case "state-flow|state-flow/at-T|source_case|pending":
		fields, key = map[string]any{"entity_type": "field_case"}, "field_case"
		input, eventType = "fork.conflicting_entity_type", "fork.conflicting_entity_type"
	default:
		return false
	}
	mode, parent := "template", (flowidentity.ParentRoute{FlowID: ".", FlowInstance: plan.Readiness.RunID, EntityID: plan.Readiness.RunID})
	if component {
		mode, parent = "static", flowidentity.ParentRoute{}
		if !plan.OccurredAt.Equal(time.Unix(1700000210, 0).UTC()) || len(plan.Instance.Bookkeeping) != 0 || len(plan.Instance.Gates) != 0 || len(plan.Instance.StateBuckets) != 0 {
			return false
		}
	}
	if !fixedForkWitnessIdentity(plan, mode, key, parent) || plan.CreatingInput.EventID == "" || plan.CreatingInput.Input != input || string(event.Type()) != eventType {
		return false
	}
	if eventType != "flow/conversation.created" && !fixedForkWitnessBuckets(plan, component) {
		return false
	}
	var payload map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(event.Payload(), &payload); err != nil {
		return false
	}
	for field, expected := range fields {
		if value, supplied := payload[field]; supplied && !reflect.DeepEqual(value, expected) {
			return false
		}
	}
	wantPayload := map[string]any{}
	wantProducerType, wantProducer := events.EventProducerPlatform, "test"
	switch eventType {
	case "flow/conversation.created":
		wantPayload = map[string]any{"name": "Before"}
		wantProducerType, wantProducer = events.EventProducerExternal, "conversation-construction-fixture"
	case "matrix.event":
		wantPayload = map[string]any{"matrix": true, "items": []any{"matrix"}}
		wantProducerType, wantProducer = events.EventProducerExternal, "revision-matrix"
	}
	if !reflect.DeepEqual(payload, wantPayload) || event.ProducerType() != wantProducerType || event.Producer().ID() != wantProducer {
		return false
	}
	return reflect.DeepEqual(plan.Instance.Fields, fields)
}

func fixedForkWitnessIdentity(plan pipeline.FlowInstanceActivationPlan, mode, key string, parent flowidentity.ParentRoute) bool {
	return plan.Instance.Mode == mode && plan.Instance.InstanceKey == key && (plan.Instance.Status == "" || plan.Instance.Status == "active") &&
		plan.Identity == plan.Readiness.Identity && plan.Identity.EntityID != "" && plan.Instance.EntityID == plan.Identity.EntityID &&
		plan.Instance.InstanceID == plan.Identity.InstanceID && plan.Instance.StorageRef == plan.Identity.InstancePath && plan.Instance.WorkflowName == plan.Identity.TemplateID &&
		plan.Identity.ParentRoute == parent && plan.Identity.ParentEntityID == parent.EntityID && plan.Instance.ParentEntityID == parent.EntityID &&
		plan.Instance.ParentFlowID == parent.FlowID && plan.Instance.ParentFlowInstance == parent.FlowInstance
}

func fixedForkWitnessBuckets(plan pipeline.FlowInstanceActivationPlan, component bool) bool {
	if len(plan.Instance.InitialFieldValues) != 0 || len(plan.Instance.TransitionHistory) != 0 || !plan.Instance.TerminatedAt.IsZero() || !plan.Instance.StageDefined ||
		len(plan.Lifecycle.Timers)+len(plan.Lifecycle.Schedules)+len(plan.Lifecycle.GateCards) != 0 || plan.Lifecycle.TurnTermination != nil || plan.Lifecycle.RequestCompletionCandidate {
		return false
	}
	if component {
		return plan.Lifecycle.StageEntry == nil
	}
	entry, present, err := workflowlifecycle.LoadStageEntry(plan.Instance.Bookkeeping)
	want := timeridentity.StageEntryRef{RunID: plan.Readiness.RunID, FlowScope: plan.Identity.ScopeKey, InstanceID: plan.Identity.InstanceID,
		InstancePath: plan.Identity.InstancePath, EntityID: plan.Identity.EntityID, Stage: plan.Instance.CurrentState, Cause: "construction"}
	if err != nil || !present || entry != want || plan.Lifecycle.StageEntry == nil || *plan.Lifecycle.StageEntry != want {
		return false
	}
	if plan.Instance.EntityType == "historical_entity" {
		return len(plan.Instance.Bookkeeping) == 2 && plan.Instance.Bookkeeping["activation"] == "standing" &&
			reflect.DeepEqual(plan.Instance.Gates, map[string]bool{"review": true}) && reflect.DeepEqual(plan.Instance.StateBuckets, map[string]any{"total": int64(7)})
	}
	return len(plan.Instance.Bookkeeping) == 1 && len(plan.Instance.Gates) == 0 && len(plan.Instance.StateBuckets) == 0
}

func (o *ConstructionOccurrence) addConstructionBirth(plan pipeline.FlowInstanceActivationPlan) error {
	record, err := plan.PersistenceRecord()
	if err != nil {
		return err
	}
	state := record.State
	if state.Identity.RunID != o.runID || !state.UpdatedAt.Equal(o.at) || !state.Transition.CreatesState() {
		return fmt.Errorf("construction occurrence requires its exact birth operation")
	}
	if _, duplicate := o.states[state.EntityID]; duplicate {
		return fmt.Errorf("construction occurrence repeats an entity")
	}
	identity, err := constructionFingerprint(state)
	if err != nil {
		return err
	}
	o.states[state.EntityID] = identity
	projection := runtimemutationlog.EntityStateProjection{CurrentState: runtimemutationlog.PresentLifecycleState(state.CurrentState)}
	for _, bucket := range []struct {
		raw json.RawMessage
		dst *map[string]any
	}{{state.Fields, &projection.Fields}, {state.Bookkeeping, &projection.Bookkeeping}, {state.Gates, &projection.Gates}, {state.Accumulator, &projection.Accumulator}} {
		if err := canonicaljson.DecodePreservingNumberLexemes(bucket.raw, bucket.dst); err != nil {
			return err
		}
	}
	initial := runtimemutationlog.EntityStateProjection{}
	if err := canonicaljson.DecodePreservingNumberLexemes(state.InitialFields, &initial.Fields); err != nil {
		return err
	}
	records, err := runtimemutationlog.BuildEntityStateDiffRecords(state.EntityID, runtimemutationlog.EntityStateProjection{}, initial, runtimemutationlog.Writer{Type: "platform", ID: "entity_initial_value", HandlerStep: "create_entity"})
	if err != nil {
		return err
	}
	remaining, err := runtimemutationlog.BuildEntityStateDiffRecords(state.EntityID, initial, projection, runtimemutationlog.Writer{Type: "platform", ID: "workflow_engine", HandlerStep: "create"})
	if err != nil {
		return err
	}
	records = append(records, remaining...)
	for _, birth := range records {
		key, err := constructionFingerprint(birth)
		if err != nil {
			return err
		}
		o.births[key] = false
	}
	return nil
}

func constructionFingerprint(value any) ([32]byte, error) {
	raw, err := json.Marshal(value)
	return sha256.Sum256(raw), err
}

func (o *ConstructionOccurrence) ValidateBirthOccurrence(ctx context.Context, attempt *mutationprotocol.Attempt, state pipeline.WorkflowEngineStateRecord) error {
	if o == nil {
		return fmt.Errorf("construction occurrence evidence is required")
	}
	id, err := attempt.Identity(ctx)
	if err != nil {
		return err
	}
	source, ok := correlation.SourceArtifactFactFromContext(ctx)
	if id != o.attemptID || correlation.RunIDFromContext(ctx) != o.runID || state.Identity.RunID != o.runID || !ok || !o.source.Matches(source) {
		return fmt.Errorf("construction occurrence belongs to another attempt/run/source")
	}
	if !state.Transition.CreatesState() || !state.UpdatedAt.Equal(o.at) {
		return fmt.Errorf("construction occurrence requires its exact birth operation/time")
	}
	key, err := constructionFingerprint(state)
	want, found := o.states[state.EntityID]
	if err != nil || !found || key != want {
		return fmt.Errorf("construction occurrence does not match its admitted entity/birth")
	}
	return nil
}

// InsertConstructionEntityStateDiff consumes a previously admitted construction;
// it cannot mint evidence or fall back to the ordinary PostgreSQL wall clock.
func InsertConstructionEntityStateDiff(ctx context.Context, attempt *mutationprotocol.Attempt, owner ActiveRunSourceOwner, state pipeline.WorkflowEngineStateRecord, before, after runtimemutationlog.EntityStateProjection, writer runtimemutationlog.Writer, occurrence *ConstructionOccurrence, postgres bool) error {
	if err := occurrence.ValidateBirthOccurrence(ctx, attempt, state); err != nil {
		return err
	}
	records, err := runtimemutationlog.BuildEntityStateDiffRecords(state.EntityID, before, after, writer)
	if err != nil {
		return err
	}
	keys := make([][32]byte, len(records))
	for index, record := range records {
		key, err := constructionFingerprint(record)
		used, found := occurrence.births[key]
		if err != nil || !found || used {
			return fmt.Errorf("construction occurrence refuses a non-birth or repeated mutation")
		}
		keys[index] = key
	}
	for index, record := range records {
		if postgres {
			err = insertPostgresAt(ctx, attempt, owner, record, occurrence.at)
		} else {
			err = insertSQLiteAt(ctx, attempt, owner, record, occurrence.at)
		}
		if err != nil {
			return err
		}
		occurrence.births[keys[index]] = true
	}
	return nil
}
