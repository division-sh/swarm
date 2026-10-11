package storetest

import (
	"context"
	"encoding/json"
	"time"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

func SetWorkflowProjectionFieldsArray(ctx context.Context, selected any, run, key string) (int64, error) {
	return private.SetWorkflowProjectionFieldsArrayForTest(ctx, selected, run, key)
}

func SetWorkflowProjectionNumericGate(ctx context.Context, selected any, run, key string) (int64, error) {
	return private.SetWorkflowProjectionNumericGateForTest(ctx, selected, run, key)
}

func SetWorkflowProjectionAccumulatorArray(ctx context.Context, selected any, run, key string) (int64, error) {
	return private.SetWorkflowProjectionAccumulatorArrayForTest(ctx, selected, run, key)
}

func SetWorkflowProjectionMalformedTransitionHistory(ctx context.Context, selected any, run, key string) (int64, error) {
	return private.SetWorkflowProjectionMalformedTransitionHistoryForTest(ctx, selected, run, key)
}

func SetWorkflowProjectionConflictingInstanceID(ctx context.Context, selected any, run, key string) (int64, error) {
	return private.SetWorkflowProjectionConflictingInstanceIDForTest(ctx, selected, run, key)
}

func SetWorkflowProjectionSlashOnlyFlowPath(ctx context.Context, selected any, run, key string) (int64, error) {
	return private.SetWorkflowProjectionSlashOnlyFlowPathForTest(ctx, selected, run, key)
}

func SetWorkflowProjectionObsoleteFieldRows(ctx context.Context, selected any, run string) (int64, error) {
	return private.SetWorkflowProjectionObsoleteFieldRowsForTest(ctx, selected, run)
}

func SetWorkflowProjectionPlatformBookkeeping(ctx context.Context, selected any, run, path string) (int64, error) {
	return private.SetWorkflowProjectionPlatformBookkeepingForTest(ctx, selected, run, path)
}

func SetEntityProjectionPrivateBookkeeping(ctx context.Context, selected any, run, entity string) (int64, error) {
	return private.SetEntityProjectionPrivateBookkeepingForTest(ctx, selected, run, entity)
}

func SetWorkflowProjectionConflictingEntityType(ctx context.Context, selected any, run, entity string) (int64, error) {
	return private.SetWorkflowProjectionConflictingEntityTypeForTest(ctx, selected, run, entity)
}

func RemoveWorkflowProjectionHeader(ctx context.Context, selected any, run, path string) (int64, error) {
	return private.RemoveWorkflowProjectionHeaderForTest(ctx, selected, run, path)
}

func RemoveWorkflowProjectionFields(ctx context.Context, selected any, run, entity string) (int64, error) {
	return private.RemoveWorkflowProjectionFieldsForTest(ctx, selected, run, entity)
}

func SetWorkflowProjectionDraining(ctx context.Context, selected any, run, path string) (int64, error) {
	return private.SetWorkflowProjectionDrainingForTest(ctx, selected, run, path)
}

func SetWorkflowProjectionTerminated(ctx context.Context, selected any, run, path string, at time.Time) (int64, error) {
	return private.SetWorkflowProjectionTerminatedForTest(ctx, selected, run, path, at)
}

func SetWorkflowProjectionActiveTerminatedTimestamp(ctx context.Context, selected any, run, path string, at time.Time) (int64, error) {
	return private.SetWorkflowProjectionActiveTerminatedTimestampForTest(ctx, selected, run, path, at)
}

func SetWorkflowProjectionNumericFlowPath(ctx context.Context, selected any, run, path string) (int64, error) {
	return private.SetWorkflowProjectionNumericFlowPathForTest(ctx, selected, run, path)
}

func AddAmbiguousWorkflowProjectionFields(ctx context.Context, selected any, run, entity string) (int64, error) {
	return private.AddAmbiguousWorkflowProjectionFieldsForTest(ctx, selected, run, entity)
}

func SetWorkflowTransitionEvidenceWire(ctx context.Context, selected any, run, path string, wire json.RawMessage) (int64, error) {
	return private.SetWorkflowTransitionEvidenceWireForTest(ctx, selected, run, path, wire)
}
