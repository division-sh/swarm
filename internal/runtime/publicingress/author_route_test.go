package publicingress

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAuthorRouteExposureUsesCanonicalReadinessCurrentness(t *testing.T) {
	for _, disposition := range []string{"current", "unrelated_registration_failed", "missing", "expired", "revoked", "failed", "foreign_authority", "authority_read_error"} {
		t.Run(disposition, func(t *testing.T) {
			now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
			readiness := NewReadinessOwner(true)
			readiness.SetExposure(ExposureEvidence{GenerationID: "exposure", StartupAuthorityID: "startup", ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL)})
			readiness.SetCurrentnessChecks(func(context.Context, string) (bool, error) {
				if disposition == "authority_read_error" {
					return false, errors.New("startup unavailable")
				}
				return disposition != "foreign_authority", nil
			}, nil, nil)
			controller := &ProviderRegistrationController{snapshot: readiness.registration, opts: RegistrationControllerOptions{
				Readiness: readiness, Now: func() time.Time { return now },
			}}
			switch disposition {
			case "missing":
				readiness = NewReadinessOwner(true)
				controller.snapshot, controller.opts.Readiness = readiness.registration, readiness
			case "expired":
				now = now.Add(EvidenceTTL)
			case "revoked":
				readiness.RevokeExposure("withdrawn")
			case "failed":
				readiness.SetExposure(ExposureEvidence{GenerationID: "exposure", StartupAuthorityID: "startup", ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL), Failure: "nonce failed"})
			case "unrelated_registration_failed":
				readiness.registration.recordAdmissionFailure(RegistrationPair{BindingID: "other", Target: RegistrationTarget{Alias: "other", Provider: "partner", Selector: "ingress:other:partner"}}, "provider unavailable")
			}
			want := disposition == "current" || disposition == "unrelated_registration_failed"
			if got := controller.ExposureCurrent(t.Context()); got != want {
				t.Fatalf("exposure=%t want=%t snapshot=%+v", got, want, readiness.Snapshot(now))
			}
		})
	}
}

func TestManagedRouteOwnershipIncludesFailedAndUnresolvedWithdrawal(t *testing.T) {
	for _, disposition := range []string{"selected", "failed", "pending_withdrawal", "uncertain_withdrawal", "rebound"} {
		t.Run(disposition, func(t *testing.T) {
			now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
			readiness := NewReadinessOwner(true)
			controller := &ProviderRegistrationController{snapshot: readiness.registration, opts: RegistrationControllerOptions{
				Readiness: readiness, Now: func() time.Time { return now },
			}}
			pair := RegistrationPair{BindingID: "managed", Target: RegistrationTarget{Alias: "managed", Provider: "partner", Selector: "ingress:.:partner", Generation: 1}}
			const slot = "provider:slot:managed"
			readiness.registration.replaceSelected([]admittedPair{{pair: pair, slotID: slot}})
			key := registrationSelectionKey(pair, slot)
			state, found := readiness.registration.state(key)
			if !found {
				t.Fatal("selected owner missing")
			}
			switch disposition {
			case "failed":
				readiness.registration.recordAdmissionFailure(pair, "credentials unavailable")
			case "pending_withdrawal", "uncertain_withdrawal":
				state.Phase = registrationPhasePendingSettlement
				if disposition == "uncertain_withdrawal" {
					state.Phase = registrationPhaseOutcomeUncertain
				}
				readiness.registration.publishState(key, state)
				readiness.registration.replaceSelected(nil)
				if readiness.registration.routeSelected("managed", "partner") {
					t.Fatal("withdrawn owner retained an executable route")
				}
			case "rebound":
				pair.Target.Generation++
				readiness.registration.replaceSelected([]admittedPair{{pair: pair, slotID: slot}})
			}
			if !controller.OwnsRoute("managed", "partner") || controller.OwnsRoute("author", "partner") {
				t.Fatal("managed and independent author route ownership collapsed")
			}
			response := httptest.NewRecorder()
			controller.Handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Fatal("incomplete managed callback escaped its strict owner")
			})).ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/webhooks/managed/partner?swarm_callback_generation=foreign", nil))
			if response.Code != http.StatusNotFound {
				t.Fatalf("incomplete managed callback status=%d", response.Code)
			}
		})
	}
}
