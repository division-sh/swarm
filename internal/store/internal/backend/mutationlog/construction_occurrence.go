package mutationlog

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
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

// ConversationForkConstructionOccurrenceForTest is the sole mint, confined to
// the fixed admitted witness. It does not accept arbitrary historical writes.
func ConversationForkConstructionOccurrenceForTest(ctx context.Context, attempt *mutationprotocol.Attempt, parent *pipeline.FlowInstanceActivationPlan, child pipeline.FlowInstanceActivationPlan) (*ConstructionOccurrence, error) {
	if child.OccurredAt.IsZero() || !child.OccurredAt.UTC().Truncate(time.Microsecond).Equal(child.Instance.CreatedAt) || !child.OccurredAt.UTC().Truncate(time.Microsecond).Equal(child.Instance.EnteredStageAt) {
		return nil, fmt.Errorf("conversation construction requires its exact historical occurrence")
	}
	if child.Identity.TemplateID != "flow" || child.Identity.InstancePath != "flow/forkchat" || child.Instance.CurrentState != "draft" || child.Instance.Fields["name"] != "Before" || len(child.Children) != 0 {
		return nil, fmt.Errorf("conversation construction requires the fixed admitted forkchat witness")
	}
	plans := []pipeline.FlowInstanceActivationPlan{child}
	if parent != nil {
		if parent.Identity.InstancePath != child.Identity.ParentRoute.FlowInstance || parent.Identity.EntityID != child.Readiness.RunID || parent.Readiness.RunID != child.Readiness.RunID || parent.Readiness.BundleHash != child.Readiness.BundleHash || !parent.OccurredAt.Equal(child.OccurredAt) || len(parent.Instance.Fields) != 0 || len(parent.Children) != 0 {
			return nil, fmt.Errorf("conversation construction requires its exact fieldless parent")
		}
		plans = append(plans, *parent)
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
		record, err := plan.PersistenceRecord()
		if err != nil {
			return nil, err
		}
		state := record.State
		if state.Identity.RunID != value.runID || !state.UpdatedAt.Equal(value.at) || !state.Transition.CreatesState() {
			return nil, fmt.Errorf("construction occurrence requires its exact birth operation")
		}
		if _, duplicate := value.states[state.EntityID]; duplicate {
			return nil, fmt.Errorf("construction occurrence repeats an entity")
		}
		identity, err := constructionFingerprint(state)
		if err != nil {
			return nil, err
		}
		value.states[state.EntityID] = identity
		projection := runtimemutationlog.EntityStateProjection{CurrentState: runtimemutationlog.PresentLifecycleState(state.CurrentState)}
		for _, bucket := range []struct {
			raw json.RawMessage
			dst *map[string]any
		}{{state.Fields, &projection.Fields}, {state.Bookkeeping, &projection.Bookkeeping}, {state.Gates, &projection.Gates}, {state.Accumulator, &projection.Accumulator}} {
			if err := canonicaljson.DecodePreservingNumberLexemes(bucket.raw, bucket.dst); err != nil {
				return nil, err
			}
		}
		initial := runtimemutationlog.EntityStateProjection{}
		if err := canonicaljson.DecodePreservingNumberLexemes(state.InitialFields, &initial.Fields); err != nil {
			return nil, err
		}
		records, err := runtimemutationlog.BuildEntityStateDiffRecords(state.EntityID, runtimemutationlog.EntityStateProjection{}, initial, runtimemutationlog.Writer{Type: "platform", ID: "entity_initial_value", HandlerStep: "create_entity"})
		if err != nil {
			return nil, err
		}
		remaining, err := runtimemutationlog.BuildEntityStateDiffRecords(state.EntityID, initial, projection, runtimemutationlog.Writer{Type: "platform", ID: "workflow_engine", HandlerStep: "create"})
		if err != nil {
			return nil, err
		}
		records = append(records, remaining...)
		for _, birth := range records {
			key, err := constructionFingerprint(birth)
			if err != nil {
				return nil, err
			}
			value.births[key] = false
		}
	}
	return value, nil
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
