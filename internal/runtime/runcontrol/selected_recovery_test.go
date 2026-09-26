package runcontrol

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func selectedRecoveryRequestForPoint(t *testing.T, point runfork.RunForkPoint) SelectedForkRecoveryRequest {
	t.Helper()
	process, err := startupownership.NewColdAuthority(startupownership.AcquireRequest{
		OwnerID: "selected-recovery", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
	}, "sqlite")
	if err != nil {
		t.Fatal(err)
	}
	return SelectedForkRecoveryRequest{
		Entry: runfork.SelectedForkRecoveryEntry{
			BundleHash: "bundle-v2:sha256:" + strings.Repeat("a", 64),
			Binding: runfork.RunForkSelectedContractBinding{
				Owner:     runfork.RunForkSelectedContractBindingOwner,
				BindingID: uuid.NewString(), ForkRunID: uuid.NewString(), SourceRunID: uuid.NewString(),
				ForkPoint: point, ForkEventID: point.EventID,
				ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
				CreatedAt:         time.Now().UTC(),
			},
		},
		Process: process,
		Effects: effects.NewRecoveryRequest(time.Now().UTC(), executionposture.MockOnly),
	}
}

func TestSelectedRecoveryRequestAdmitsExactTypedForkPoint(t *testing.T) {
	event := selectedRecoveryRequestForPoint(t, runfork.RunForkPoint{
		Kind: runfork.RunForkPointEvent, Revision: 2, EventID: uuid.NewString(),
	})
	deployment := selectedRecoveryRequestForPoint(t, runfork.RunForkPoint{
		Kind: runfork.RunForkPointDeploymentRevision, Revision: 2,
	})
	for _, tc := range []struct {
		name string
		req  SelectedForkRecoveryRequest
	}{
		{"event", event},
		{"deployment_revision_without_event", deployment},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.req.Validate(); err != nil {
				t.Fatalf("valid selected recovery point rejected: %v", err)
			}
		})
	}
}

func TestSelectedRecoveryRequestRejectsContradictoryForkPoint(t *testing.T) {
	event := selectedRecoveryRequestForPoint(t, runfork.RunForkPoint{
		Kind: runfork.RunForkPointEvent, Revision: 2, EventID: uuid.NewString(),
	})
	deployment := selectedRecoveryRequestForPoint(t, runfork.RunForkPoint{
		Kind: runfork.RunForkPointDeploymentRevision, Revision: 2,
	})
	for _, tc := range []struct {
		name   string
		base   SelectedForkRecoveryRequest
		mutate func(*SelectedForkRecoveryRequest)
	}{
		{"event_alias_mismatch", event, func(r *SelectedForkRecoveryRequest) { r.Entry.Binding.ForkEventID = uuid.NewString() }},
		{"event_alias_missing", event, func(r *SelectedForkRecoveryRequest) { r.Entry.Binding.ForkEventID = "" }},
		{"event_point_missing", event, func(r *SelectedForkRecoveryRequest) { r.Entry.Binding.ForkPoint.EventID = "" }},
		{"event_noncanonical", event, func(r *SelectedForkRecoveryRequest) {
			r.Entry.Binding.ForkPoint.EventID = strings.ToUpper(r.Entry.Binding.ForkPoint.EventID)
			r.Entry.Binding.ForkEventID = r.Entry.Binding.ForkPoint.EventID
		}},
		{"deployment_alias_invented", deployment, func(r *SelectedForkRecoveryRequest) { r.Entry.Binding.ForkEventID = uuid.NewString() }},
		{"deployment_point_event_invented", deployment, func(r *SelectedForkRecoveryRequest) { r.Entry.Binding.ForkPoint.EventID = uuid.NewString() }},
		{"deployment_event_evidence_invented", deployment, func(r *SelectedForkRecoveryRequest) { r.Entry.Binding.ForkPoint.EventName = "root.ready" }},
		{"zero_revision", deployment, func(r *SelectedForkRecoveryRequest) { r.Entry.Binding.ForkPoint.Revision = 0 }},
		{"unknown_kind", deployment, func(r *SelectedForkRecoveryRequest) { r.Entry.Binding.ForkPoint.Kind = "unknown" }},
		{"crossed_run", deployment, func(r *SelectedForkRecoveryRequest) { r.Entry.Binding.ForkRunID = r.Entry.Binding.SourceRunID }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := tc.base
			tc.mutate(&req)
			if err := req.Validate(); err == nil {
				t.Fatal("contradictory selected recovery point admitted")
			}
		})
	}
}
