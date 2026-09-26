package forkpoint

import (
	"testing"

	"github.com/google/uuid"
)

func TestValidateIdentityClosedForkPoints(t *testing.T) {
	eventID := uuid.NewString()
	for _, tc := range []struct {
		kind     Kind
		revision int64
		eventID  string
		valid    bool
	}{
		{Event, 1, eventID, true},
		{DeploymentRevision, 2, "", true},
		{Event, 0, eventID, false},
		{Event, 1, "", false},
		{Event, 1, "not-a-uuid", false},
		{DeploymentRevision, 0, "", false},
		{DeploymentRevision, 2, eventID, false},
		{Kind("unknown"), 1, "", false},
	} {
		err := ValidateIdentity(tc.kind, tc.revision, tc.eventID)
		if (err == nil) != tc.valid {
			t.Fatalf("kind=%q revision=%d event=%q: err=%v, want valid=%t", tc.kind, tc.revision, tc.eventID, err, tc.valid)
		}
	}
}
