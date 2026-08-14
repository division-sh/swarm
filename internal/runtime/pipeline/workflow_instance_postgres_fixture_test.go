package pipeline

import (
	"context"
	"database/sql"
	"strings"

	runtimemutationlog "github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimerunlifecycle "github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/testutil/mutationlogfixture"
)

func insertWorkflowCreateEntityInitialValueMutations(
	ctx context.Context,
	tx *sql.Tx,
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
		if err := mutationlogfixture.Insert(ctx, tx, runLifecycle, runtimemutationlog.Record{
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

func loadTrackedEntityStateProjection(ctx context.Context, tx *sql.Tx, runID, entityID string) (runtimemutationlog.EntityStateProjection, error) {
	if tx == nil || strings.TrimSpace(entityID) == "" {
		return runtimemutationlog.EntityStateProjection{}, nil
	}
	var (
		currentState   sql.NullString
		fieldsRaw      []byte
		bookkeepingRaw []byte
		gatesRaw       []byte
		accRaw         []byte
	)
	err := tx.QueryRowContext(ctx, `
		SELECT
			current_state,
			COALESCE(fields, '{}'::jsonb),
			COALESCE(bookkeeping, '{}'::jsonb),
			COALESCE(gates, '{}'::jsonb),
			COALESCE(accumulator, '{}'::jsonb)
		FROM entity_state
		WHERE run_id = $1::uuid
		  AND entity_id = $2::uuid
		FOR UPDATE
	`, runID, entityID).Scan(&currentState, &fieldsRaw, &bookkeepingRaw, &gatesRaw, &accRaw)
	if err == sql.ErrNoRows {
		return runtimemutationlog.EntityStateProjection{}, nil
	}
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	fields, err := decodeWorkflowInstanceJSONMap("entity_state.fields", fieldsRaw)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	bookkeeping, err := decodeWorkflowInstanceJSONMap("entity_state.bookkeeping", bookkeepingRaw)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	gates, err := decodeWorkflowInstanceJSONBoolMap("entity_state.gates", gatesRaw)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	accumulator, err := decodeWorkflowInstanceJSONMap("entity_state.accumulator", accRaw)
	if err != nil {
		return runtimemutationlog.EntityStateProjection{}, err
	}
	return runtimemutationlog.EntityStateProjection{
		CurrentState: strings.TrimSpace(currentState.String),
		Fields:       fields,
		Bookkeeping:  bookkeeping,
		Gates:        workflowBoolGatesAsMap(gates),
		Accumulator:  accumulator,
	}, nil
}
