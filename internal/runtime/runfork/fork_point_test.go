package runfork

import (
	"testing"

	"github.com/google/uuid"
)

func TestRunForkPointClosedArms(t *testing.T) {
	event := RunForkPoint{Kind: RunForkPointEvent, Revision: 3, EventID: uuid.NewString()}
	deployment := RunForkPoint{Kind: RunForkPointDeploymentRevision, Revision: 1}
	for _, point := range []RunForkPoint{event, deployment} {
		if err := point.Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, point := range []RunForkPoint{
		{Kind: RunForkPointEvent, Revision: 1},
		{Kind: RunForkPointEvent, Revision: 0, EventID: event.EventID},
		{Kind: RunForkPointDeploymentRevision, Revision: 0},
		{Kind: RunForkPointDeploymentRevision, Revision: 1, EventID: event.EventID},
		{Kind: RunForkPointDeploymentRevision, Revision: 1, EventName: "items.ready"},
		{Kind: "unknown", Revision: 1},
	} {
		if err := point.Validate(); err == nil {
			t.Fatalf("mixed or invalid point accepted: %+v", point)
		}
	}
}
