package pipeline

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/google/uuid"
)

func TestActivityExecutionIdentityKeepsOriginalAttribution(t *testing.T) {
	expected := ActivityAttemptStartForTest(NonIdempotentActivityIntentForTest(uuid.NewString(), uuid.NewString(), uuid.NewString()))
	expected.SourceEventID = uuid.NewString()
	expected.ParentEventID = uuid.NewString()
	expected.ReplyContextID = "reply-original"
	expected.ExecutionBinding = processbinding.ActivityExecutionBinding{
		Version: processbinding.ActivityExecutionBindingVersion, GrantID: uuid.NewString(), StateVersion: 3,
	}
	if err := ValidateActivityAttemptExecutionIdentity(expected, expected); err != nil {
		t.Fatalf("exact original identity: %v", err)
	}
	for _, change := range []struct {
		name string
		edit func(*ActivityAttemptRecord)
	}{
		{"request", func(r *ActivityAttemptRecord) { r.RequestEventID = uuid.NewString() }},
		{"run", func(r *ActivityAttemptRecord) { r.RunID = uuid.NewString() }},
		{"source", func(r *ActivityAttemptRecord) { r.SourceEventID = uuid.NewString() }},
		{"parent", func(r *ActivityAttemptRecord) { r.ParentEventID = uuid.NewString() }},
		{"reply", func(r *ActivityAttemptRecord) { r.ReplyContextID = "reply-other" }},
		{"attempt", func(r *ActivityAttemptRecord) { r.Attempt++ }},
		{"zero_attempt", func(r *ActivityAttemptRecord) { r.Attempt = 0 }},
		{"mode", func(r *ActivityAttemptRecord) { r.ExecutionMode = "mock" }},
		{"input", func(r *ActivityAttemptRecord) { r.InputHash = "different" }},
		{"grant", func(r *ActivityAttemptRecord) { r.ExecutionBinding.GrantID = uuid.NewString() }},
		{"version", func(r *ActivityAttemptRecord) { r.ExecutionBinding.StateVersion++ }},
		{"missing", func(r *ActivityAttemptRecord) { r.ExecutionBinding = processbinding.ActivityExecutionBinding{} }},
	} {
		t.Run(change.name, func(t *testing.T) {
			actual := expected
			change.edit(&actual)
			if err := ValidateActivityAttemptExecutionIdentity(actual, expected); err == nil {
				t.Fatalf("changed %s was accepted: %+v", change.name, actual)
			}
			if err := ValidateActivityAttemptExecutionIdentity(expected, actual); err == nil {
				t.Fatalf("changed callback %s was accepted", change.name)
			}
		})
	}
}
