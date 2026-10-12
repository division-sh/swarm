package storetest

import (
	"context"
	"encoding/json"
	"time"

	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type WorkflowControlProjectionStorage = private.WorkflowControlProjectionStorage
type WorkflowDuplicateProjectionStorage = private.WorkflowDuplicateProjectionStorage
type WorkflowStateObservationRow = private.WorkflowStateObservationRow

type WorkflowRunFieldsRevisionStorage = private.WorkflowRunFieldsRevisionStorage
type WorkflowAccumulatorStorage = private.WorkflowAccumulatorStorage

func ReadRootFlowAccumulatorStorage(ctx context.Context, selected any, runID string) ([]byte, error) {
	return private.ReadRootFlowAccumulatorStorageForTest(ctx, selected, runID)
}

func ReadWorkflowAccumulatorStorage(ctx context.Context, selected any, runID, path string) (WorkflowAccumulatorStorage, error) {
	return private.ReadWorkflowAccumulatorStorageForTest(ctx, selected, runID, path)
}

func ReadBoundReplySourceStorage(ctx context.Context, selected any, runID, path string) (map[string][][]any, error) {
	return private.ReadBoundReplySourceStorageForTest(ctx, selected, runID, path)
}

func ReadWorkflowRunFieldsRevisionStorage(ctx context.Context, selected any, runID string) (WorkflowRunFieldsRevisionStorage, error) {
	return private.ReadWorkflowRunFieldsRevisionStorageForTest(ctx, selected, runID)
}

func ReadWorkflowRunHeaderRevisionStorage(ctx context.Context, selected any, runID string) (int64, error) {
	return private.ReadWorkflowRunHeaderRevisionStorageForTest(ctx, selected, runID)
}

func ReadWorkflowRunFieldsStorage(ctx context.Context, selected any, runID string) (string, error) {
	return private.ReadWorkflowRunFieldsStorageForTest(ctx, selected, runID)
}

func CountWorkflowRunFieldRows(ctx context.Context, selected any, runID string) (int, error) {
	return private.CountWorkflowRunFieldRowsForTest(ctx, selected, runID)
}

func CountWorkflowHeadersCreatedSince(ctx context.Context, selected any, since time.Time) (int, error) {
	return private.CountWorkflowHeadersCreatedSinceForTest(ctx, selected, since)
}

func CountWorkflowHeadersForPathCreatedSince(ctx context.Context, selected any, path string, since time.Time) (int, error) {
	return private.CountWorkflowHeadersForPathCreatedSinceForTest(ctx, selected, path, since)
}

func ReadLatestWorkflowFieldsForPath(ctx context.Context, selected any, path string) (json.RawMessage, bool, error) {
	return private.ReadLatestWorkflowFieldsForPathForTest(ctx, selected, path)
}

func ReadWorkflowStateObservationRows(ctx context.Context, selected any) ([]WorkflowStateObservationRow, error) {
	return private.ReadWorkflowStateObservationRowsForTest(ctx, selected)
}

func ReadWorkflowControlProjectionStorage(ctx context.Context, selected any, run, entity string) (WorkflowControlProjectionStorage, error) {
	return private.ReadWorkflowControlProjectionStorageForTest(ctx, selected, run, entity)
}

func ReadWorkflowTransitionEvidenceWire(ctx context.Context, selected any, run, path string) (json.RawMessage, error) {
	return private.ReadWorkflowTransitionEvidenceWireForTest(ctx, selected, run, path)
}

func ReadWorkflowDuplicateProjectionStorage(ctx context.Context, selected any, run, entity string) (WorkflowDuplicateProjectionStorage, error) {
	return private.ReadWorkflowDuplicateProjectionStorageForTest(ctx, selected, run, entity)
}

func CountWorkflowInstanceHeaders(ctx context.Context, selected any) (int64, error) {
	return private.CountWorkflowInstanceHeadersForTest(ctx, selected)
}

type WorkflowEnginePhysicalCounts = private.WorkflowEnginePhysicalCounts

func ReadWorkflowEnginePhysicalCounts(ctx context.Context, selected any) (WorkflowEnginePhysicalCounts, error) {
	return private.ReadWorkflowEnginePhysicalCountsForTest(ctx, selected)
}
