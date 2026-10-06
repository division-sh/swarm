package storetest

import (
	"context"
	"encoding/json"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/fanoutobligation"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ReplyReturnStorageEvidence = private.ReplyReturnStorageEvidence

func ReadPinnedAuthoredMutationStorage(ctx context.Context, selected any, source fanoutobligation.SourceRef) (json.RawMessage, error) {
	return private.ReadPinnedAuthoredMutationStorageForTest(ctx, selected, source)
}

type FanOutPublishedOutcomeStorageEvidence = private.FanOutPublishedOutcomeStorageEvidence
type FanOutRunProgressStorageEvidence = private.FanOutRunProgressStorageEvidence
type FanOutTriggeredIntentStorageEvidence = private.FanOutTriggeredIntentStorageEvidence
type FanOutIntentCompletionStorageEvidence = private.FanOutIntentCompletionStorageEvidence

func ReadReplyReturnStorage(ctx context.Context, selected any, run string) (ReplyReturnStorageEvidence, error) {
	return private.ReadReplyReturnStorageForTest(ctx, selected, run)
}
func ReadFanOutPublishedOutcomeStorage(ctx context.Context, selected any, run string) ([]FanOutPublishedOutcomeStorageEvidence, error) {
	return private.ReadFanOutPublishedOutcomeStorageForTest(ctx, selected, run)
}
func ReadFanOutRunProgressStorage(ctx context.Context, selected any, run string) (FanOutRunProgressStorageEvidence, error) {
	return private.ReadFanOutRunProgressStorageForTest(ctx, selected, run)
}
func ReadFanOutTriggeredIntentStorage(ctx context.Context, selected any, run, event string, node identity.ExecutableNode, ref contracts.FanOutElementRef) ([]FanOutTriggeredIntentStorageEvidence, error) {
	return private.ReadFanOutTriggeredIntentStorageForTest(ctx, selected, run, event, node, ref)
}
func ReadFanOutIntentCompletionStorage(ctx context.Context, selected any, key fanoutobligation.IntentKey) (FanOutIntentCompletionStorageEvidence, error) {
	return private.ReadFanOutIntentCompletionStorageForTest(ctx, selected, key)
}
