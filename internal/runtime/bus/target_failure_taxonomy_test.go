package bus

import (
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/failures"
)

func TestTargetFailureReceiptConsumesCanonicalDeadLetterTaxonomy(t *testing.T) {
	if targetDeliveryFailureEnvelope(0) != nil {
		t.Fatal("an absent failure invented a receipt failure")
	}
	for _, test := range []struct {
		failure pinrouting.TargetFailure
		class   failures.Class
		code    string
	}{
		{pinrouting.FailureTargetRequiredMissing, failures.ClassTargetUnreachable, "target_required_missing"},
		{pinrouting.FailureTargetNotSubscribed, failures.ClassTargetUnreachable, "target_not_subscribed"},
		{pinrouting.FailureTargetUnreachableTerminated, failures.ClassTargetUnreachable, "target_unreachable_terminated"},
		{pinrouting.FailureReplyAlreadyTerminal, failures.ClassReplyAlreadyTerminal, "reply_already_terminal"},
		{pinrouting.FailureStaleArrival, failures.ClassStaleArrival, "stale_arrival"},
	} {
		t.Run(test.failure.Code(), func(t *testing.T) {
			receipt := targetDeliveryFailureEnvelope(test.failure)
			canonical := canonicalTargetDeliveryFailure(test.failure, nil).Failure
			if receipt == nil || receipt.Class != test.class || receipt.Detail.Code != test.code || !reflect.DeepEqual(*receipt, canonical) {
				t.Fatalf("receipt changed the canonical terminal refusal: got=%+v want=%+v", receipt, canonical)
			}
		})
	}
}
