package pipeline

import (
	"context"
	"strings"

	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/eventfixture"
	"github.com/division-sh/swarm/internal/store/testutil/mutationlogfixture"
)

func insertWorkflowCreateEntityInitialValueMutations(
	ctx context.Context,
	attempt *eventfixture.Attempt,
	runLifecycle runtimerunlifecycle.OperationOwner,
	entityID string,
	before, after runtimemutationlog.EntityStateProjection,
	initialValues map[string]any,
) (runtimemutationlog.EntityStateProjection, error) {
	if len(initialValues) == 0 {
		return before, nil
	}
	adjusted := runtimemutationlog.EntityStateProjection{
		CurrentState: before.CurrentState,
		Fields:       cloneStringAnyMap(before.Fields),
		Bookkeeping:  cloneStringAnyMap(before.Bookkeeping),
		Gates:        cloneStringAnyMap(before.Gates),
		Accumulator:  cloneStringAnyMap(before.Accumulator),
	}
	if adjusted.Fields == nil {
		adjusted.Fields = map[string]any{}
	}
	for field, declared := range initialValues {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		finalValue, ok := after.Fields[field]
		oldValue, hadOld := adjusted.Fields[field]
		if hadOld {
			continue
		}
		if err := mutationlogfixture.Insert(ctx, attempt, runLifecycle, runtimemutationlog.Record{
			EntityID:    entityID,
			Domain:      runtimemutationlog.DomainAuthoredField,
			Path:        field,
			OldValue:    oldValueOrNil(oldValue, hadOld),
			NewValue:    declared,
			WriterType:  "platform",
			WriterID:    "entity_initial_value",
			HandlerStep: "create_entity",
		}); err != nil {
			return runtimemutationlog.EntityStateProjection{}, err
		}
		if ok && workflowJSONValuesEqual(finalValue, declared) {
			adjusted.Fields[field] = declared
			continue
		}
		adjusted.Fields[field] = declared
	}
	return adjusted, nil
}

func oldValueOrNil(value any, ok bool) any {
	if !ok {
		return nil
	}
	return value
}

func trackedWorkflowTargetProjectionForTest(record WorkflowTargetPersistenceRecord) (runtimemutationlog.EntityStateProjection, error) {
	projection := runtimemutationlog.EntityStateProjection{Fields: map[string]any{}}
	if record.Presence.HasState() {
		fields, err := decodeWorkflowInstanceJSONMap("entity_state.fields", record.State.Fields)
		if err != nil {
			return runtimemutationlog.EntityStateProjection{}, err
		}
		projection.Fields = fields
	}
	if !record.Presence.Constructed() {
		return projection, nil
	}
	header := record.Lifecycle.State
	if record.Lifecycle.StageDefined {
		projection.CurrentState = runtimemutationlog.PresentLifecycleState(header.CurrentState)
	}
	var err error
	if projection.Bookkeeping, err = decodeWorkflowInstanceJSONMap("flow_instances.bookkeeping", header.Bookkeeping); err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	gates, err := decodeWorkflowInstanceJSONBoolMap("flow_instances.gates", header.Gates)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	projection.Gates = workflowBoolGatesAsMap(gates)
	projection.Accumulator, err = decodeWorkflowInstanceJSONMap("flow_instances.accumulator", header.Accumulator)
	return projection, err
}
