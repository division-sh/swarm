package runfork

import (
	"reflect"
	"testing"

	"github.com/google/uuid"
)

func TestIssue642RequestPartitionIsFixedAndDoesNotAlias(t *testing.T) {
	event, delivery := uuid.NewString(), uuid.NewString()
	p := RunForkPlan{ForkPoint: RunForkPoint{Revision: 5}, PendingWork: []RunForkPendingWork{{EventID: event, DeliveryID: delivery}}}.
		WithHistoricalEvents(5, []string{event})
	input := map[string][]string{event: {delivery}}
	bound, err := p.WithHistoricalRequestPublications(5, input)
	if err != nil {
		t.Fatal(err)
	}
	input[event][0] = "caller-changed"
	got, present, err := bound.RequestPendingRecipientPartition(event)
	if err != nil || !present || !reflect.DeepEqual(got, []string{delivery}) {
		t.Fatalf("fixed request partition: %v present=%t error=%v", got, present, err)
	}
	got[0] = "reader-changed"
	if again, _, err := bound.RequestPendingRecipientPartition(event); err != nil || !reflect.DeepEqual(again, []string{delivery}) {
		t.Fatalf("reader changed retained request evidence: %v error=%v", again, err)
	}
	for _, fault := range []string{"revision", "event", "empty", "delivery", "duplicate"} {
		t.Run(fault, func(t *testing.T) {
			revision := int64(5)
			input := map[string][]string{event: {delivery}}
			switch fault {
			case "revision":
				revision++
			case "event":
				input = map[string][]string{uuid.NewString(): {delivery}}
			case "empty":
				input[event] = nil
			case "delivery":
				input[event] = []string{uuid.NewString()}
			case "duplicate":
				input[event] = []string{delivery, delivery}
			}
			if _, err := bound.WithHistoricalRequestPublications(revision, input); err == nil {
				t.Fatal("inexact request partition admitted")
			}
			if after, _, err := bound.RequestPendingRecipientPartition(event); err != nil || !reflect.DeepEqual(after, []string{delivery}) {
				t.Fatalf("refusal changed prior partition: %v error=%v", after, err)
			}
		})
	}
	bound.ForkPoint.Revision++
	if _, present, err := bound.RequestPendingRecipientPartition(event); err == nil || present {
		t.Fatal("request partition survived a changed cut")
	}
	if _, present, err := p.RequestPendingRecipientPartition(uuid.NewString()); err != nil || present {
		t.Fatalf("absence invented request history: present=%t error=%v", present, err)
	}
}
