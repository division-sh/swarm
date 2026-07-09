package engine

import (
	"context"
	"errors"
	"testing"
)

type recordingRunner struct {
	calls []string
	err   error
}

func (r *recordingRunner) Run(ctx context.Context, fn func(Tx) error) error {
	r.calls = append(r.calls, "run")
	if r.err != nil {
		return r.err
	}
	return fn(stubTx{ctx: ctx})
}

type recordingOutbox struct {
	calls   []string
	intents []EmitIntent
	err     error
}

func (o *recordingOutbox) WriteOutbox(_ context.Context, intents []EmitIntent) error {
	o.calls = append(o.calls, "outbox")
	o.intents = append([]EmitIntent(nil), intents...)
	return o.err
}

type recordingDispatcher struct {
	calls   []string
	intents []EmitIntent
	err     error
}

func (d *recordingDispatcher) DispatchPostCommit(_ context.Context, intents []EmitIntent) error {
	d.calls = append(d.calls, "dispatch")
	d.intents = append([]EmitIntent(nil), intents...)
	return d.err
}

func TestPersistAndDispatch_WritesBeforeDispatch(t *testing.T) {
	runner := &recordingRunner{}
	outbox := &recordingOutbox{}
	dispatcher := &recordingDispatcher{}
	intents := []EmitIntent{{ChainDepth: 2}}

	if err := PersistAndDispatch(context.Background(), runner, outbox, dispatcher, intents); err != nil {
		t.Fatalf("PersistAndDispatch error: %v", err)
	}
	if len(runner.calls) != 1 || runner.calls[0] != "run" {
		t.Fatalf("unexpected runner calls: %v", runner.calls)
	}
	if len(outbox.calls) != 1 || outbox.calls[0] != "outbox" {
		t.Fatalf("unexpected outbox calls: %v", outbox.calls)
	}
	if len(dispatcher.calls) != 1 || dispatcher.calls[0] != "dispatch" {
		t.Fatalf("unexpected dispatcher calls: %v", dispatcher.calls)
	}
	if outbox.intents[0].ChainDepth != 2 || dispatcher.intents[0].ChainDepth != 2 {
		t.Fatalf("chain depth not preserved through outbox/dispatch: outbox=%v dispatch=%v", outbox.intents, dispatcher.intents)
	}
}

func TestPersistAndDispatch_DoesNotDispatchOnTransactionFailure(t *testing.T) {
	runner := &recordingRunner{err: errors.New("tx failed")}
	outbox := &recordingOutbox{}
	dispatcher := &recordingDispatcher{}

	if err := PersistAndDispatch(context.Background(), runner, outbox, dispatcher, []EmitIntent{{}}); err == nil {
		t.Fatal("expected error")
	}
	if len(dispatcher.calls) != 0 {
		t.Fatalf("dispatch should not run on tx failure: %v", dispatcher.calls)
	}
}

func TestPersistAndDispatch_DoesNotDispatchWhenOutboxFails(t *testing.T) {
	runner := &recordingRunner{}
	outbox := &recordingOutbox{err: errors.New("save failed")}
	dispatcher := &recordingDispatcher{}
	intents := []EmitIntent{{ChainDepth: 2}}

	if err := PersistAndDispatch(context.Background(), runner, outbox, dispatcher, intents); err == nil {
		t.Fatal("expected error")
	}
	if len(outbox.calls) != 1 || outbox.calls[0] != "outbox" {
		t.Fatalf("unexpected outbox calls: %v", outbox.calls)
	}
	if len(dispatcher.calls) != 0 {
		t.Fatalf("dispatch should not run on outbox failure: %v", dispatcher.calls)
	}
}

func TestClassifyFailure(t *testing.T) {
	if got := ClassifyFailure(nil); got != FailureNone {
		t.Fatalf("nil failure class = %v", got)
	}
	if got := ClassifyFailure(ErrChainDepthExceeded); got != FailureDeadLetter {
		t.Fatalf("chain-depth failure class = %v", got)
	}
	if got := ClassifyFailure(ErrMissingStateRepo); got != FailureLogic {
		t.Fatalf("missing-state-repo failure class = %v", got)
	}
	if got := ClassifyFailure(ErrFanOutBoundExceeded); got != FailureLogic {
		t.Fatalf("fan-out-bound failure class = %v", got)
	}
	if got := ClassifyFailure(errors.New("temporary")); got != FailureTransient {
		t.Fatalf("generic failure class = %v", got)
	}
}
