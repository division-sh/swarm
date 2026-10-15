package storetest

import (
	"context"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type FanOutSourceProgressStorage = private.FanOutSourceProgressStorage
type FanOutSemanticOutcomeStorage = private.FanOutSemanticOutcomeStorage
type FanOutBlockedSourceStorage = private.FanOutBlockedSourceStorage
type FanOutTriggerDeliveryStorage = private.FanOutTriggerDeliveryStorage
type FanOutRunProgressStorage = private.FanOutRunProgressStorage

func ReadFanOutSourceProgressStorage(ctx context.Context, selected any, run, event string) (FanOutSourceProgressStorage, error) {
	return private.ReadFanOutSourceProgressStorageForTest(ctx, selected, run, event)
}
func ReadFanOutSourceOutcomesStorage(ctx context.Context, selected any, run, event string) ([]FanOutSemanticOutcomeStorage, error) {
	return private.ReadFanOutSourceOutcomesStorageForTest(ctx, selected, run, event)
}
func CountFanOutSourceOutcomesStorage(ctx context.Context, selected any, run, event string) (int, error) {
	return private.CountFanOutSourceOutcomesStorageForTest(ctx, selected, run, event)
}
func ReadFanOutBlockedSourceStorage(ctx context.Context, selected any, run, event string) (FanOutBlockedSourceStorage, error) {
	return private.ReadFanOutBlockedSourceStorageForTest(ctx, selected, run, event)
}
func ReadFanOutTriggerDeliveryDiagnosticStorage(ctx context.Context, selected any, run, event string) (FanOutTriggerDeliveryStorage, error) {
	return private.ReadFanOutTriggerDeliveryDiagnosticStorageForTest(ctx, selected, run, event)
}
func CountFanOutDeliveredTriggerStorage(ctx context.Context, selected any, run, event string) (int, error) {
	return private.CountFanOutDeliveredTriggerStorageForTest(ctx, selected, run, event)
}
func ReadRunFanOutProgressStorage(ctx context.Context, selected any, run string) (FanOutRunProgressStorage, error) {
	return private.ReadRunFanOutProgressStorageForTest(ctx, selected, run)
}
func CountLeasedRunFanOutStorage(ctx context.Context, selected any, run string) (int, error) {
	return private.CountLeasedRunFanOutStorageForTest(ctx, selected, run)
}
func CountFanOutAttemptResidueStorage(ctx context.Context, selected any, command pipeline.FanOutChunkCommand) (int, error) {
	return private.CountFanOutAttemptResidueStorageForTest(ctx, selected, command)
}
func ReadLastThreeFanOutRuntimeLogPayloads(ctx context.Context, selected any, run string) ([][]byte, error) {
	return private.ReadLastThreeFanOutRuntimeLogPayloadsForTest(ctx, selected, run)
}
func ReadFanOutPortfolioFieldsStorage(ctx context.Context, selected any, run string) ([]byte, error) {
	return private.ReadFanOutPortfolioFieldsStorageForTest(ctx, selected, run)
}
func ReadFanOutAccountIDsMutationSourceStorage(ctx context.Context, selected any, run, mutation, entity string) (string, error) {
	return private.ReadFanOutAccountIDsMutationSourceStorageForTest(ctx, selected, run, mutation, entity)
}
