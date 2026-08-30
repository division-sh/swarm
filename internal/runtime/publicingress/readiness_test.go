package publicingress

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func TestPublicIngressReadinessUsesReadTimeFreshnessAndStartupAuthority(t *testing.T) {
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	owner := NewReadinessOwner(true)
	owner.SetRuntimeReady(true)
	owner.SetExposure(ExposureEvidence{
		GenerationID: "exposure-1", StartupAuthorityID: "startup-1",
		ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL),
	})
	installReadinessRegistration(t, owner, RegistrationEvidence{
		BindingID: "binding", Target: "target", IntentID: "intent", SlotID: "provider:slot:1",
		StartupAuthorityID: "startup-1", Applied: true, CallbackMatched: true,
		ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL),
	})
	owner.SetCurrentnessChecks(func(_ context.Context, authorityID string) (bool, error) {
		return authorityID == "startup-1", nil
	}, nil, nil)
	if snapshot := owner.Snapshot(now.Add(EvidenceTTL - time.Nanosecond)); !snapshot.Ready {
		t.Fatalf("fresh snapshot = %#v, want ready", snapshot)
	}
	if snapshot := owner.Snapshot(now.Add(EvidenceTTL)); snapshot.Ready || snapshot.PublicIngressReady {
		t.Fatalf("boundary-expired snapshot = %#v, want not ready", snapshot)
	}

	owner.SetExposure(ExposureEvidence{
		GenerationID: "exposure-1", StartupAuthorityID: "startup-1",
		ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL),
	})
	owner.SetCurrentnessChecks(func(context.Context, string) (bool, error) { return false, nil }, nil, nil)
	if snapshot := owner.Snapshot(now.Add(time.Second)); snapshot.Ready || !strings.Contains(snapshot.Failure, "no longer current") {
		t.Fatalf("superseded snapshot = %#v, want fenced failure", snapshot)
	}
	owner.SetCurrentnessChecks(func(context.Context, string) (bool, error) { return false, errors.New("selected store unavailable") }, nil, nil)
	if snapshot := owner.Snapshot(now.Add(time.Second)); snapshot.Ready || snapshot.Failure != "selected store unavailable" {
		t.Fatalf("store-failed snapshot = %#v, want typed failure", snapshot)
	}
}

func TestPublicIngressReadinessRevocationAndRegistrationReplacement(t *testing.T) {
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	owner := NewReadinessOwner(true)
	owner.SetRuntimeReady(true)
	owner.SetExposure(ExposureEvidence{GenerationID: "exposure-1", ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL)})
	installReadinessRegistration(t, owner, RegistrationEvidence{BindingID: "old", Target: "old", Applied: true, CallbackMatched: true, ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL)})
	installReadinessRegistration(t, owner, RegistrationEvidence{BindingID: "current", Target: "current", Applied: true, CallbackMatched: true, ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL)})
	currentPair := RegistrationPair{BindingID: "current", Target: RegistrationTarget{Selector: "current"}}
	current, ok := owner.registration.state(registrationSelectionKey(currentPair, "provider:slot:current"))
	if !ok {
		t.Fatal("current registration state is missing")
	}
	owner.registration.replaceSelected([]admittedPair{{pair: current.Pair, slotID: current.SelectionSlotID}})
	snapshot := owner.Snapshot(now)
	if len(snapshot.Registrations) != 1 || snapshot.Registrations[0].BindingID != "current" {
		t.Fatalf("registrations = %#v, want current only", snapshot.Registrations)
	}
	owner.RevokeExposure("tunnel unavailable")
	snapshot = owner.Snapshot(now)
	if snapshot.Ready || snapshot.Exposure != nil || snapshot.Failure != "tunnel unavailable" || snapshot.Registrations[0].CallbackMatched {
		t.Fatalf("revoked snapshot = %#v", snapshot)
	}
}

func TestPublicIngressReadinessRejectsMixedStartupAuthorityEvidence(t *testing.T) {
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	owner := NewReadinessOwner(true)
	owner.SetRuntimeReady(true)
	owner.SetExposure(ExposureEvidence{
		GenerationID: "exposure-successor", StartupAuthorityID: "startup-successor",
		ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL),
	})
	installReadinessRegistration(t, owner, RegistrationEvidence{
		BindingID: "binding", StartupAuthorityID: "startup-predecessor",
		Target: "target", Applied: true, CallbackMatched: true, ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL),
	})
	owner.SetCurrentnessChecks(func(_ context.Context, authorityID string) (bool, error) {
		return authorityID == "startup-successor", nil
	}, nil, nil)

	if snapshot := owner.Snapshot(now.Add(time.Second)); snapshot.Ready || snapshot.PublicIngressReady {
		t.Fatalf("mixed startup evidence = %#v, want not ready", snapshot)
	}
}

func TestRegistrationSnapshotSelectionReplacesRoutesAndStatesAtomically(t *testing.T) {
	owner := newRegistrationSnapshotOwner()
	left := RegistrationPair{BindingID: "left", Target: RegistrationTarget{Selector: "ingress:left:telegram", Alias: "left", Provider: "telegram"}}
	right := RegistrationPair{BindingID: "right", Target: RegistrationTarget{Selector: "ingress:right:telegram", Alias: "right", Provider: "telegram"}}
	leftSlot, rightSlot := "provider:slot:left", "provider:slot:right"
	owner.replaceSelected([]admittedPair{{pair: left, slotID: leftSlot}})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for index := 0; index < 2000; index++ {
			if index%2 == 0 {
				owner.replaceSelected([]admittedPair{{pair: right, slotID: rightSlot}})
			} else {
				owner.replaceSelected([]admittedPair{{pair: left, slotID: leftSlot}})
			}
		}
	}()
	for {
		snapshot := owner.capture()
		if len(snapshot.registrations) != 1 || len(snapshot.routes) != 1 {
			t.Fatalf("partial selected snapshot = %#v", snapshot)
		}
		_, hasLeftState := snapshot.registrations[registrationSelectionKey(left, leftSlot)]
		_, hasLeftRoute := snapshot.routes[registrationRouteKey("left", "telegram")]
		_, hasRightState := snapshot.registrations[registrationSelectionKey(right, rightSlot)]
		_, hasRightRoute := snapshot.routes[registrationRouteKey("right", "telegram")]
		if hasLeftState != hasLeftRoute || hasRightState != hasRightRoute || hasLeftState == hasRightState {
			t.Fatalf("mixed route/state selected snapshot = %#v", snapshot)
		}
		select {
		case <-done:
			return
		default:
		}
	}
}

func TestRegistrationSnapshotInvalidatesChangedSameKeyBeforeReconcile(t *testing.T) {
	owner := newRegistrationSnapshotOwner()
	pair := RegistrationPair{
		BindingID: "binding",
		Target:    RegistrationTarget{Selector: "ingress:support:telegram", Alias: "support", Provider: "telegram", Generation: 1},
	}
	const slotID = "provider:slot:binding"
	owner.replaceSelected([]admittedPair{{pair: pair, slotID: slotID}})
	key := registrationSelectionKey(pair, slotID)
	state, ok := owner.state(key)
	if !ok {
		t.Fatal("selected registration state is missing")
	}
	state.SelectedBase = "base-v1"
	state.LastVerified = &registrationIntent{BaseFingerprint: "base-v1"}
	state.Phase = registrationPhaseVerified
	owner.publishState(key, state)

	changed := pair
	changed.Target.Generation = 2
	owner.replaceSelected([]admittedPair{{pair: changed, slotID: slotID}})
	invalidated, ok := owner.state(registrationSelectionKey(changed, slotID))
	if !ok || invalidated.SelectedBase != "" || invalidated.Failure == "" || invalidated.LastVerified == nil {
		t.Fatalf("changed same-key selection state = %#v", invalidated)
	}
}

func installReadinessRegistration(t *testing.T, owner *ReadinessOwner, evidence RegistrationEvidence) {
	t.Helper()
	target := strings.TrimSpace(evidence.Target)
	if target == "" {
		target = strings.TrimSpace(evidence.BindingID)
	}
	pair := RegistrationPair{
		BindingID: evidence.BindingID,
		Target: RegistrationTarget{
			Selector: target,
			Alias:    evidence.Alias,
			Provider: evidence.Provider,
		},
	}
	exposure := owner.registration.capture().exposure
	exposureGeneration := ""
	if exposure != nil {
		exposureGeneration = exposure.GenerationID
	}
	slotID := strings.TrimSpace(evidence.SlotID)
	if slotID == "" {
		slotID = "provider:slot:" + evidence.BindingID
	}
	base := "base:" + evidence.BindingID
	state := registrationState{
		Pair:            pair,
		SelectionSlotID: slotID,
		SelectedBase:    base,
		Phase:           registrationPhaseVerified,
		LastVerified: &registrationIntent{
			BaseFingerprint:      base,
			ExposureGenerationID: exposureGeneration,
			IntentID:             evidence.IntentID,
			SlotID:               slotID,
			CallbackURL:          evidence.CallbackURL,
			Applied:              true,
			Matched:              true,
			ObservedAt:           evidence.ObservedAt,
			ExpiresAt:            evidence.ExpiresAt,
			Authority: runtimeeffects.Authority{ServeRegistration: runtimeeffects.ServeRegistrationAuthority{
				StartupAuthorityID: evidence.StartupAuthorityID,
			}},
		},
	}
	prior := owner.registration.capture()
	pairs := make([]admittedPair, 0, len(prior.registrations)+1)
	for _, existing := range prior.registrations {
		pairs = append(pairs, admittedPair{pair: existing.Pair, slotID: existing.SelectionSlotID})
	}
	pairs = append(pairs, admittedPair{pair: pair, slotID: slotID})
	owner.registration.replaceSelected(pairs)
	owner.registration.publishState(registrationSelectionKey(pair, slotID), state)
}
