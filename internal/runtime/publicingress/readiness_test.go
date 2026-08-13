package publicingress

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestPublicIngressReadinessUsesReadTimeFreshnessAndStartupAuthority(t *testing.T) {
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	owner := NewReadinessOwner(true)
	owner.SetRuntimeReady(true)
	owner.SetExposure(ExposureEvidence{
		GenerationID: "exposure-1", StartupAuthorityID: "startup-1",
		ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL),
	})
	owner.SetRegistration("binding\x00target", RegistrationEvidence{
		BindingID: "binding", Target: "target", IntentID: "intent", SlotID: "provider:slot:1",
		StartupAuthorityID: "startup-1", Applied: true, CallbackMatched: true,
		ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL),
	})
	owner.SetStartupCurrent(func(authorityID string) (bool, error) {
		return authorityID == "startup-1", nil
	})
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
	owner.SetStartupCurrent(func(string) (bool, error) { return false, nil })
	if snapshot := owner.Snapshot(now.Add(time.Second)); snapshot.Ready || !strings.Contains(snapshot.Failure, "no longer current") {
		t.Fatalf("superseded snapshot = %#v, want fenced failure", snapshot)
	}
	owner.SetStartupCurrent(func(string) (bool, error) { return false, errors.New("selected store unavailable") })
	if snapshot := owner.Snapshot(now.Add(time.Second)); snapshot.Ready || snapshot.Failure != "selected store unavailable" {
		t.Fatalf("store-failed snapshot = %#v, want typed failure", snapshot)
	}
}

func TestPublicIngressReadinessRevocationAndRegistrationReplacement(t *testing.T) {
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	owner := NewReadinessOwner(true)
	owner.SetRuntimeReady(true)
	owner.SetExposure(ExposureEvidence{GenerationID: "exposure-1", ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL)})
	owner.SetRegistration("old", RegistrationEvidence{BindingID: "old", Applied: true, CallbackMatched: true, ExpiresAt: now.Add(EvidenceTTL)})
	owner.SetRegistration("current", RegistrationEvidence{BindingID: "current", Applied: true, CallbackMatched: true, ExpiresAt: now.Add(EvidenceTTL)})
	owner.ReplaceRegistrationKeys([]string{"current"})
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
	owner.SetRegistration("binding", RegistrationEvidence{
		BindingID: "binding", StartupAuthorityID: "startup-predecessor",
		Applied: true, CallbackMatched: true, ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL),
	})
	owner.SetStartupCurrent(func(authorityID string) (bool, error) {
		return authorityID == "startup-successor", nil
	})

	if snapshot := owner.Snapshot(now.Add(time.Second)); snapshot.Ready || snapshot.PublicIngressReady {
		t.Fatalf("mixed startup evidence = %#v, want not ready", snapshot)
	}
}
