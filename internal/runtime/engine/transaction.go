package engine

import "context"

type PersistedEffects struct {
	StateMutation StateMutation
	TimerIntents  []TimerIntent
	EmitIntents   []EmitIntent
}

func ClassifyFailure(err error) FailureClass {
	switch err {
	case nil:
		return FailureNone
	case ErrChainDepthExceeded:
		return FailureDeadLetter
	case ErrMissingSemanticSource,
		ErrMissingStateRepo,
		ErrMissingTransaction,
		ErrMissingEntityLocker,
		ErrMissingOutbox,
		ErrMissingDispatcher,
		ErrMissingNodeID,
		ErrMissingNodeHandler:
		return FailureLogic
	default:
		return FailureTransient
	}
}

func ShouldRetry(class FailureClass) bool {
	return class == FailureTransient
}

func PersistAndDispatch(ctx context.Context, runner TransactionRunner, outbox OutboxWriter, dispatcher PostCommitDispatcher, intents []EmitIntent) error {
	if runner == nil {
		return ErrMissingTransaction
	}
	if outbox == nil {
		return ErrMissingOutbox
	}
	if dispatcher == nil {
		return ErrMissingDispatcher
	}
	if err := runner.Run(ctx, func(tx Tx) error {
		return outbox.WriteOutbox(tx.Context(), intents)
	}); err != nil {
		return err
	}
	return dispatcher.DispatchPostCommit(ctx, intents)
}
