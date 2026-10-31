package bus

import (
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

func TestPublishRecipientPlanTargetFailureErrorKeepsCallerAndStrictVocabulary(t *testing.T) {
	if err := (PublishRecipientPlan{}).TargetFailureError("tool-executor", "emit", nil); err != nil {
		t.Fatalf("empty plan invented refusal: %v", err)
	}
	refusal := &pipeline.NoHandlerError{FlowID: "worker", FlowInstance: "worker/one", Stage: "done"}
	view := (&EventBus{}).publishRecipientPlan(events.Event{}, RoutePlan{
		TargetFailure: pinrouting.FailureTargetUnreachableTerminated, unavailableReceiverError: refusal,
	})
	err := view.TargetFailureError("tool-executor", "emit", nil)
	if !errors.Is(err, refusal) || failures.Normalize(err, "test", "observe").Class != failures.ClassUnexpectedArrival {
		t.Fatalf("pinned preflight changed an ordinary receiver refusal: %v", err)
	}
	cleanup := errors.New("preflight cleanup failed")
	view.targetFailureCause = errors.Join(refusal, cleanup)
	err = view.TargetFailureError("tool-executor", "emit", nil)
	if !errors.Is(err, cleanup) || !errors.Is(err, refusal) || failures.Normalize(err, "test", "observe").Class == failures.ClassUnexpectedArrival {
		t.Fatalf("ordinary refusal hid an independent preflight failure: %v", err)
	}
	for _, refusal := range []pinrouting.TargetFailure{
		pinrouting.FailureTargetRequiredMissing, pinrouting.FailureReplyAlreadyTerminal, pinrouting.FailureStaleArrival,
		pinrouting.TargetFailureFromConnect(pinrouting.ConnectFailureTargetUnresolved),
	} {
		t.Run(refusal.Code(), func(t *testing.T) {
			attributes := map[string]any{"tool": "emit_work_done", "event": "worker/work.done"}
			got := (PublishRecipientPlan{TargetFailure: refusal.Code()}).TargetFailureError("tool-executor", "emit", attributes)
			want := canonicalTargetDeliveryFailure(refusal, "tool-executor", "emit", attributes).Failure
			if got == nil || !reflect.DeepEqual(failures.Normalize(got, "test", "observe"), want) {
				t.Fatalf("view conversion changed caller or taxonomy: got=%v want=%+v", got, want)
			}
			if len(attributes) != 2 || attributes["tool"] != "emit_work_done" || attributes["event"] != "worker/work.done" {
				t.Fatal("view conversion changed caller attributes")
			}
		})
	}
	for _, invalid := range []string{" ", " platform.stale_arrival", "PLATFORM.STALE_ARRIVAL", "stale_arrival", "platform.unknown"} {
		t.Run("invalid/"+invalid, func(t *testing.T) {
			err := (PublishRecipientPlan{TargetFailure: invalid}).TargetFailureError("tool-executor", "emit", nil)
			if err == nil {
				t.Fatal("noncanonical failure escaped refusal")
			}
			failure := failures.Normalize(err, "test", "observe")
			if failure.Component != "tool-executor" || failure.Operation != "emit" || failure.Detail.Code != "route_plan_preflight_failed" {
				t.Fatalf("noncanonical plan lost fail-closed diagnostics: %+v", failure)
			}
		})
	}
	for _, code := range []string{"platform.reply_already_terminal", "platform.stale_arrival"} {
		failure := failures.Normalize(failures.NewTarget(code, "test", "strict_target", nil), "test", "observe")
		if failure.Class != failures.ClassInternalFailure || failure.Detail.Code != "invalid_failure_construction" {
			t.Fatal("generic target validation was weakened to accept reply classes")
		}
	}
}

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
			canonical := canonicalTargetDeliveryFailure(test.failure, "eventbus", "resolve_delivery_target", nil).Failure
			if receipt == nil || receipt.Class != test.class || receipt.Detail.Code != test.code || !reflect.DeepEqual(*receipt, canonical) {
				t.Fatalf("receipt changed the canonical terminal refusal: got=%+v want=%+v", receipt, canonical)
			}
		})
	}
}
