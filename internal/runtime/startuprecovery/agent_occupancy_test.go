package startuprecovery

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
)

func TestAgentRecoveryOccupancyCountsOnlyExactInProgressMembers(t *testing.T) {
	f := newAgentImageReaderFixture(t)
	original := f.active[0]
	second := agentImageSnapshot(t, original.RunID, original.Route, original.Authority, deliverylifecycle.StatusInProgress, 4)
	profile := f.profiles[original.DeliveryID]
	profile.DeliveryID, profile.EventID = second.DeliveryID, second.EventID
	f.profiles[second.DeliveryID] = profile
	f.active = append(f.active, second)
	for _, due := range []time.Time{time.Now().Add(-time.Hour), time.Now().Add(time.Hour)} {
		failed := agentImageSnapshot(t, original.RunID, original.Route, original.Authority, deliverylifecycle.StatusFailed, 2)
		failed.NextEligibleAt = due
		capture := profile
		capture.DeliveryID, capture.EventID = failed.DeliveryID, failed.EventID
		f.profiles[failed.DeliveryID] = capture
		f.active = append(f.active, failed)
	}
	pending := agentImageSnapshot(t, original.RunID, original.Route, original.Authority, deliverylifecycle.StatusPending, 0)
	f.active = append(f.active, pending)
	selected := appendSelectedAgentImageFixture(t, f, deliverylifecycle.StatusInProgress, 2)
	prepared := f.selected[selected.DeliveryID]
	prepared.Backend = selection.BackendOpenAIResponses
	f.selected[selected.DeliveryID] = prepared
	appendSelectedAgentImageFixture(t, f, deliverylifecycle.StatusPending, 0)
	appendSelectedAgentImageFixture(t, f, deliverylifecycle.StatusFailed, 1)
	node, err := identity.AdmitExecutableNodeDeclaration(".", "occupancy-node")
	if err != nil {
		t.Fatal(err)
	}
	f.active = append(f.active, agentImageSnapshot(t, original.RunID, events.DeliveryRoute{
		Recipient: events.MustNodeDeliveryRecipient(node),
		Target:    events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: ".", FlowInstance: "."}),
	}, original.Authority, deliverylifecycle.StatusInProgress, 1))
	before := append([]deliverylifecycle.Snapshot(nil), f.active...)
	candidate, err := InspectAgentRecoveryOccupancyCandidate(t.Context(), f)
	want := map[string]int{selection.BackendAnthropic: 2, selection.BackendOpenAIResponses: 1}
	if err != nil || !reflect.DeepEqual(candidate.InProgressByProfile, want) || len(candidate.Image.Members) != 8 ||
		f.calls["inspection"] != 1 || f.calls["profile"] != 4 || f.calls["selected"] != 2 {
		t.Fatalf("durable occupancy classification: %+v calls=%v err=%v", candidate, f.calls, err)
	}
	if !reflect.DeepEqual(before, f.active) || candidate.Image.Process.Possession != nil {
		t.Fatal("occupancy observation changed membership or inferred possession")
	}
}

func TestAgentRecoveryOccupancyUsesCanonicalProfileIdentity(t *testing.T) {
	for _, backend := range []string{selection.BackendAnthropic, selection.BackendClaudeCLI, selection.BackendOpenAICompatible, selection.BackendOpenAIResponses, selection.BackendMock} {
		t.Run(backend, func(t *testing.T) {
			f := newAgentImageReaderFixture(t)
			ordinary := f.active[0]
			profile := f.profiles[ordinary.DeliveryID]
			profile.ProfileID = backend
			f.profiles[ordinary.DeliveryID] = profile
			selected := appendSelectedAgentImageFixture(t, f, deliverylifecycle.StatusInProgress, 1)
			prepared := f.selected[selected.DeliveryID]
			prepared.Backend = backend
			resolved, err := selection.ResolveActiveBackend(backend)
			if err != nil {
				t.Fatal(err)
			}
			prepared.Mode, err = selection.ExecutionModeForProfile(resolved)
			if err != nil {
				t.Fatal(err)
			}
			f.selected[selected.DeliveryID] = prepared
			candidate, err := InspectAgentRecoveryOccupancyCandidate(t.Context(), f)
			if err != nil || !reflect.DeepEqual(candidate.InProgressByProfile, map[string]int{backend: 2}) {
				t.Fatalf("ordinary/selected profiles diverged: %+v err=%v", candidate, err)
			}
		})
	}
}

func TestAgentRecoveryOccupancyRefusesPartialTotals(t *testing.T) {
	for _, fault := range []string{"duplicate", "foreign_capture", "corrupt_excluded_retry", "unknown_selected_profile", "noncanonical_selected_profile", "reserved_selected_profile", "profile_read", "selected_read", "close"} {
		t.Run(fault, func(t *testing.T) {
			f := newAgentImageReaderFixture(t)
			ordinary := f.active[0]
			selected := appendSelectedAgentImageFixture(t, f, deliverylifecycle.StatusInProgress, 1)
			prepared := f.selected[selected.DeliveryID]
			switch fault {
			case "duplicate":
				f.active = append(f.active, ordinary)
			case "foreign_capture":
				profile := f.profiles[ordinary.DeliveryID]
				profile.EventID = selected.EventID
				f.profiles[ordinary.DeliveryID] = profile
			case "corrupt_excluded_retry":
				failed := agentImageSnapshot(t, ordinary.RunID, ordinary.Route, ordinary.Authority, deliverylifecycle.StatusFailed, 1)
				f.active = append(f.active, failed)
			case "unknown_selected_profile":
				prepared.Backend = "unknown-profile"
			case "noncanonical_selected_profile":
				prepared.Backend = " " + prepared.Backend
			case "reserved_selected_profile":
				prepared.Backend = selection.BackendLocal
			case "profile_read":
				f.fail = "profile"
			case "selected_read":
				f.fail = "selected"
			case "close":
				f.lateFailure = true
			}
			f.selected[selected.DeliveryID] = prepared
			candidate, err := InspectAgentRecoveryOccupancyCandidate(t.Context(), f)
			if err == nil || !reflect.DeepEqual(candidate, AgentRecoveryOccupancyCandidate{}) {
				t.Fatalf("%s exposed partial totals/image: %+v err=%v", fault, candidate, err)
			}
			if (fault == "profile_read" || fault == "selected_read" || fault == "close") && !errors.Is(err, f.failure) {
				t.Fatalf("lost original reader/transaction failure: %v", err)
			}
		})
	}
}

func TestAgentRecoveryOccupancyEmptyAndCancellationAreExplicit(t *testing.T) {
	f := newAgentImageReaderFixture(t)
	f.active = []deliverylifecycle.Snapshot{}
	candidate, err := InspectAgentRecoveryOccupancyCandidate(t.Context(), f)
	if err != nil || candidate.InProgressByProfile == nil || len(candidate.InProgressByProfile) != 0 || candidate.Image.Members == nil {
		t.Fatalf("valid empty observation differs from failure: %+v err=%v", candidate, err)
	}
	for _, late := range []bool{false, true} {
		f := newAgentImageReaderFixture(t)
		ctx, cancel := context.WithCancel(t.Context())
		if late {
			f.cancel = cancel
		} else {
			cancel()
		}
		candidate, err := InspectAgentRecoveryOccupancyCandidate(ctx, f)
		cancel()
		if !errors.Is(err, context.Canceled) || !reflect.DeepEqual(candidate, AgentRecoveryOccupancyCandidate{}) {
			t.Fatalf("canceled occupancy exposed totals: %+v err=%v", candidate, err)
		}
	}
	for _, ctx := range []context.Context{nil, t.Context()} {
		if candidate, err := InspectAgentRecoveryOccupancyCandidate(ctx, nil); err == nil || !reflect.DeepEqual(candidate, AgentRecoveryOccupancyCandidate{}) {
			t.Fatalf("absent occupancy readers/context admitted: %+v err=%v", candidate, err)
		}
	}
}
