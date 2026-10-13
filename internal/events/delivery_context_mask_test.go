package events

import (
	"context"
	"errors"
	"testing"
)

func TestWithoutDeliveryContextMasksOnlyDeliveryFacts(t *testing.T) {
	type retainedKey struct{}
	caller, cancel := context.WithCancelCause(context.WithValue(context.Background(), retainedKey{}, "exact-owner"))
	defer cancel(nil)
	incoming := DeliveryContext{Reply: &ReplyContextRef{ID: "original"}}
	parent := WithDeliveryContext(caller, incoming)
	masked := WithoutDeliveryContext(parent)
	if !DeliveryContextFromContext(masked).Empty() || masked.Value(retainedKey{}) != "exact-owner" || DeliveryContextFromContext(parent).ReplyContextID() != "original" {
		t.Fatal("clearing delivery facts lost another binding or changed the parent")
	}
	// The ordinary overlay's lawful empty-is-inherit rule remains unchanged.
	if DeliveryContextFromContext(WithDeliveryContext(parent, DeliveryContext{})).ReplyContextID() != "original" {
		t.Fatal("explicit masking changed ordinary context overlay semantics")
	}
	cause := errors.New("original caller stopped")
	cancel(cause)
	if !errors.Is(masked.Err(), context.Canceled) || context.Cause(masked) != cause {
		t.Fatal("masking delivery facts detached original cancellation")
	}
	if !DeliveryContextFromContext(WithoutDeliveryContext(nil)).Empty() {
		t.Fatal("nil caller invented delivery facts")
	}
}
