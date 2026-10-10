package main

import (
	"encoding/json"
	"strings"
	"testing"
)

const publisherValueCommitShape = `func (s *retainedConnectionCommitStore) CommitPublication(ctx context.Context,command runtimebus.PublicationCommand)(runtimebus.CommittedPublication,error) {
s.sawPublisherValue = ctx.Value(publisherDispatchValueKey{}) == "publisher-value"
return s.InMemoryEventStore.CommitPublication(ctx,command)
}`

const emptyReceiverObserverShape = `func (o *dispatchContextObserver) NotifyLifecycle(ctx context.Context,signal runtimelifecycleprobe.Signal) {
if signal.Kind != runtimelifecycleprobe.PostCommitDispatchStarted { return }
o.called = true
if ctx.Value(publisherDispatchValueKey{}) != nil { o.t.Error("post-commit dispatch retained publisher-owned context values") }
}`

func TestNativeBusReceiverContextKeepsCommitAndExactDispatchBoundary(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-bus-empty-receiver-context" {
			continue
		}
		matched++
		switch row.Function {
		case "CommitPublication", "NotifyLifecycle":
			shape := publisherValueCommitShape
			if row.Function == "NotifyLifecycle" {
				shape = emptyReceiverObserverShape
			}
			want, err := canonicalFunction(shape)
			actual, actualErr := canonicalFunction(row.After)
			if err != nil || actualErr != nil || want != actual {
				t.Fatal("input-value witness, actual commit, exact dispatch signal or leak refusal changed")
			}
		case "TestCommittedPublishDispatchDropsTransactionConnectionCapability":
			for _, required := range []string{
				"context.WithValue(testAuthorActivityContext(context.Background()), publisherDispatchValueKey{}, \"publisher-value\")",
				"bus.Publish(ctx, event)", "if !store.sawPublisherValue", "if !observer.called",
			} {
				if !strings.Contains(row.After, required) {
					t.Fatalf("receiver proof lost a nonvacuous boundary: %s", required)
				}
			}
			if strings.Contains(row.After, "sql.") || strings.Contains(row.After, "runtimepipelinefixture.") {
				t.Fatal("receiver value proof regained fake SQL capabilities")
			}
		default:
			t.Fatalf("unknown receiver snapshot: %s", row.Function)
		}
	}
	if matched != 3 {
		t.Fatalf("receiver snapshots=%d, want3 plus original native-root snapshot update", matched)
	}
	actual := selectedCausalObservationBody(t, "internal/runtime/bus/eventbus_publish_test.go", "TestCommittedPublishDispatchDoesNotExposePublicationClaimConnection")
	if !strings.Contains(actual, "ctx = context.WithValue(ctx, publisherDispatchValueKey{}, \"publisher-value\")") || !strings.Contains(actual, "TestLifecycleProbe: observer") || !strings.Contains(actual, "bus.Publish(ctx,") {
		t.Fatal("native PostgreSQL handoff lost the real publisher marker or observer")
	}
}
