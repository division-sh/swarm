package runforkexecution

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSelectedDecisionCallerPreservesOwnedContextAndCancellation(t *testing.T) {
	type key struct{}
	owned, retire := context.WithCancelCause(context.WithValue(context.Background(), key{}, "fork-owned"))
	defer retire(nil)
	caller, cancel := context.WithTimeout(context.WithValue(context.Background(), key{}, "source-caller"), time.Second)
	defer cancel()
	ctx, release := bindSelectedDecisionCaller(caller, owned)
	defer release()
	if ctx.Value(key{}) != "fork-owned" {
		t.Fatal("operator context replaced the child's execution authority")
	}
	deadline, bounded := ctx.Deadline()
	want, _ := caller.Deadline()
	if !bounded || deadline != want {
		t.Fatal("operator deadline was removed or extended")
	}
	cancel()
	select {
	case <-ctx.Done():
		if !errors.Is(context.Cause(ctx), context.Canceled) {
			t.Fatalf("caller cancellation changed: %v", context.Cause(ctx))
		}
	case <-time.After(time.Second):
		t.Fatal("caller cancellation did not reach the selected decision turn")
	}
	if owned.Err() != nil {
		t.Fatal("one canceled operator turn retired the selected executor")
	}
	other, otherRelease := bindSelectedDecisionCaller(context.Background(), owned)
	defer otherRelease()
	cause := errors.New("selected execution retired")
	retire(cause)
	if !errors.Is(context.Cause(other), cause) {
		t.Fatalf("retained execution cancellation was lost: %v", context.Cause(other))
	}
}
