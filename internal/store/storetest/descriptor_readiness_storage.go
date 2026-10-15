package storetest

import (
	"context"
	"encoding/json"
	"time"

	"github.com/division-sh/swarm/internal/runtime/pipeline"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type DescriptorReadinessStampStorage = private.DescriptorReadinessStampStorage

func ReadDescriptorReadinessStampStorage(ctx context.Context, selected any, run, path string) (DescriptorReadinessStampStorage, error) {
	return private.ReadDescriptorReadinessStampStorageForTest(ctx, selected, run, path)
}

func SetDescriptorReadinessMalformedPlan(ctx context.Context, selected any, run, path string, wire json.RawMessage) (int64, error) {
	return private.SetDescriptorReadinessMalformedPlanForTest(ctx, selected, run, path, wire)
}

func SetDescriptorReadinessConcurrentPlan(ctx context.Context, selected any, plan pipeline.DynamicFlowRuntimeReadinessPlan) (int64, error) {
	return private.SetDescriptorReadinessConcurrentPlanForTest(ctx, selected, plan)
}

func RemoveDescriptorReadiness(ctx context.Context, selected any, run, path string) (int64, error) {
	return private.RemoveDescriptorReadinessForTest(ctx, selected, run, path)
}

func SetDescriptorReadinessCreationClock(ctx context.Context, selected any, run, path string, at time.Time) (int64, error) {
	return private.SetDescriptorReadinessCreationClockForTest(ctx, selected, run, path, at)
}

func InstallDescriptorReadinessBatchUpdateFailure(ctx context.Context, selected any, run, path string) (func(context.Context) error, error) {
	return private.InstallDescriptorReadinessBatchUpdateFailureForTest(ctx, selected, run, path)
}

func InsertDescriptorEntityStateWithoutType(ctx context.Context, selected any, run string) error {
	return private.InsertDescriptorEntityStateWithoutTypeForTest(ctx, selected, run)
}

func InsertDescriptorEntityStateWithBlankType(ctx context.Context, selected any, run string) error {
	return private.InsertDescriptorEntityStateWithBlankTypeForTest(ctx, selected, run)
}
