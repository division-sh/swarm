package storetest

import (
	"context"

	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type NestedFanOutBarrierStorage = private.NestedFanOutBarrierStorage
type NestedFanOutCursorClaimStorage = private.NestedFanOutCursorClaimStorage
type NestedFanOutOwedStorage = private.NestedFanOutOwedStorage
type NestedFanOutEntityStorage = private.NestedFanOutEntityStorage

type RepeatedFanOutIntentStorage = private.RepeatedFanOutIntentStorage

func ReadNestedRunEntityStateFields(ctx context.Context, selected any, run string) ([]NestedFanOutEntityStorage, error) {
	return private.ReadNestedRunEntityStateFieldsForTest(ctx, selected, run)
}

func ReadRepeatedFanOutIntent(ctx context.Context, selected any, key fanoutobligation.IntentKey) (RepeatedFanOutIntentStorage, error) {
	return private.ReadRepeatedFanOutIntentForTest(ctx, selected, key)
}
func CountRepeatedFanOutTriggerOutcomes(ctx context.Context, selected any, run, trigger string) (int, error) {
	return private.CountRepeatedFanOutTriggerOutcomesForTest(ctx, selected, run, trigger)
}
func ReadRepeatedFanOutSnapshot(ctx context.Context, selected any, run string) ([8]int64, error) {
	return private.ReadRepeatedFanOutSnapshotForTest(ctx, selected, run)
}
func ReadRepeatedFanOutCompletionEventIDs(ctx context.Context, selected any, run string) ([]string, error) {
	return private.ReadRepeatedFanOutCompletionEventIDsForTest(ctx, selected, run)
}

func ReadNestedSourceEventIDs(ctx context.Context, selected any, run, name, parent string) ([]string, error) {
	return private.ReadNestedSourceEventIDsForTest(ctx, selected, run, name, parent)
}
func ReadNestedRunNamedEventIDs(ctx context.Context, selected any, run, name string) ([]string, error) {
	return private.ReadNestedRunNamedEventIDsForTest(ctx, selected, run, name)
}
func ReadNestedSiblingSourceEventID(ctx context.Context, selected any, run, event string) (string, error) {
	return private.ReadNestedSiblingSourceEventIDForTest(ctx, selected, run, event)
}
func CountNestedRunEventIdentity(ctx context.Context, selected any, run, event string) (int, error) {
	return private.CountNestedRunEventIdentityForTest(ctx, selected, run, event)
}
func ReadNestedFanOutBarrierStatus(ctx context.Context, selected any, key fanoutobligation.IntentKey) (string, error) {
	return private.ReadNestedFanOutBarrierStatusForTest(ctx, selected, key)
}
func ReadNestedFanOutBarrier(ctx context.Context, selected any, key fanoutobligation.IntentKey) (NestedFanOutBarrierStorage, error) {
	return private.ReadNestedFanOutBarrierForTest(ctx, selected, key)
}
func ReadNestedPipelineReceiptOutcome(ctx context.Context, selected any, event string) (string, error) {
	return private.ReadNestedPipelineReceiptOutcomeForTest(ctx, selected, event)
}
func CountNestedSuccessfulPipelineReceipt(ctx context.Context, selected any, event string) (int, error) {
	return private.CountNestedSuccessfulPipelineReceiptForTest(ctx, selected, event)
}
func ReadNestedFanOutCursorClaim(ctx context.Context, selected any, key fanoutobligation.IntentKey) (NestedFanOutCursorClaimStorage, error) {
	return private.ReadNestedFanOutCursorClaimForTest(ctx, selected, key)
}
func CountNestedRunFanOutOutcomes(ctx context.Context, selected any, run string) (int, error) {
	return private.CountNestedRunFanOutOutcomesForTest(ctx, selected, run)
}

func CountRunFanOutIntentStorage(ctx context.Context, selected any, run string) (int, error) {
	return private.CountRunFanOutIntentStorageForTest(ctx, selected, run)
}

func CountRunFanOutBarrierStorage(ctx context.Context, selected any, run string) (int, error) {
	return private.CountRunFanOutBarrierStorageForTest(ctx, selected, run)
}
func ReadNestedRunFanOutOwed(ctx context.Context, selected any, run string) (NestedFanOutOwedStorage, error) {
	return private.ReadNestedRunFanOutOwedForTest(ctx, selected, run)
}
func ReadNestedCommittedOutcomeEventIDs(ctx context.Context, selected any, run string) ([]string, error) {
	return private.ReadNestedCommittedOutcomeEventIDsForTest(ctx, selected, run)
}
func ReadNestedAccountIntentKeys(ctx context.Context, selected any, run string) ([]fanoutobligation.IntentKey, error) {
	return private.ReadNestedAccountIntentKeysForTest(ctx, selected, run)
}
func CountNestedContinuationHandoffs(ctx context.Context, selected any, event string) (int, error) {
	return private.CountNestedContinuationHandoffsForTest(ctx, selected, event)
}
func ReadNestedRunEntityStates(ctx context.Context, selected any, run string) ([]NestedFanOutEntityStorage, error) {
	return private.ReadNestedRunEntityStatesForTest(ctx, selected, run)
}
func ReadNestedEntityFieldsRevision(ctx context.Context, selected any, run, path string) (NestedFanOutEntityStorage, error) {
	return private.ReadNestedEntityFieldsRevisionForTest(ctx, selected, run, path)
}
