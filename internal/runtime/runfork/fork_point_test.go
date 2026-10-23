package runfork

import (
	"testing"

	"github.com/google/uuid"
)

func TestRunForkPointClosedArms(t *testing.T) {
	event := RunForkPoint{Kind: RunForkPointEvent, Revision: 3, EventID: uuid.NewString()}
	deployment := RunForkPoint{Kind: RunForkPointDeploymentRevision, Revision: 1}
	state := RunForkPoint{Kind: RunForkPointStateRevision, Revision: 2}
	for _, point := range []RunForkPoint{event, deployment, state} {
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
		{Kind: RunForkPointStateRevision, Revision: 0},
		{Kind: RunForkPointStateRevision, Revision: 1, EventID: event.EventID},
		{Kind: RunForkPointStateRevision, Revision: 1, Input: event.EventID},
		{Kind: RunForkPointStateRevision, Revision: 1, EventName: "items.ready"},
		{Kind: "unknown", Revision: 1},
	} {
		if err := point.Validate(); err == nil {
			t.Fatalf("mixed or invalid point accepted: %+v", point)
		}
	}
}
