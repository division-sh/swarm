package storetest

import (
	"context"
	"github.com/division-sh/swarm/internal/runtime/engine"
	private "github.com/division-sh/swarm/internal/store/internal/runtimepersistence"
)

type ForeignOutboxDispatchObservation = private.ForeignOutboxDispatchObservation

func ObserveOutboxDispatchWithForeignTransaction(ctx context.Context, selected any, dispatcher engine.PostCommitDispatcher, intents []engine.EmitIntent) (ForeignOutboxDispatchObservation, error) {
	return private.ObserveOutboxDispatchWithForeignTransactionForTest(ctx, selected, dispatcher, intents)
}
