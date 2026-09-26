package runcontrol

import (
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func TestSelectedStopValidatesExactForkPointUnion(t *testing.T) {
	process, err := startupownership.NewColdAuthority(startupownership.AcquireRequest{
		OwnerID: "selected-stop-test", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	}, "sqlite")
	if err != nil {
		t.Fatal(err)
	}
	base := SelectedStopRequest{
		Transition: TransitionRequest{RunID: uuid.NewString()},
		Binding: runfork.RunForkSelectedContractBinding{
			Owner: runfork.RunForkSelectedContractBindingOwner, BindingID: uuid.NewString(),
			SourceRunID: uuid.NewString(), ContractSelection: runfork.RunForkContractSelection{
				Mode: runfork.RunForkContractSelectionModeSelectedContracts,
			}, CreatedAt: time.Now().UTC(),
		},
		Process: process,
	}
	base.Binding.ForkRunID = base.Transition.RunID
	eventID := uuid.NewString()
	for _, tc := range []struct {
		name    string
		point   runfork.RunForkPoint
		alias   string
		wantErr bool
	}{
		{name: "event", point: runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, Revision: 1, EventID: eventID}, alias: eventID},
		{name: "deployment", point: runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 1}},
		{name: "event missing ID", point: runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, Revision: 1}, wantErr: true},
		{name: "deployment carries event", point: runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 1, EventID: uuid.NewString()}, wantErr: true},
		{name: "deployment missing revision", point: runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision}, wantErr: true},
		{name: "event alias mismatch", point: runfork.RunForkPoint{Kind: runfork.RunForkPointEvent, Revision: 1, EventID: uuid.NewString()}, alias: uuid.NewString(), wantErr: true},
		{name: "deployment alias mismatch", point: runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 1}, alias: uuid.NewString(), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := base
			req.Binding.ForkPoint = tc.point
			req.Binding.ForkEventID = tc.alias
			if err := req.Validate(); (err != nil) != tc.wantErr {
				t.Fatalf("selected stop point=%+v alias=%q: error=%v wantErr=%t", tc.point, tc.alias, err, tc.wantErr)
			}
		})
	}
}
