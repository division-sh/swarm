package publicingress

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func TestAuthorRouteExposureUsesCanonicalReadinessCurrentness(t *testing.T) {
	for _, disposition := range []string{"current", "unrelated_registration_failed", "unrelated_registration_current", "unrelated_registration_update", "missing", "expired", "revoked", "failed", "foreign_authority", "authority_read_error", "revoked_during_evaluation", "replaced_during_evaluation"} {
		t.Run(disposition, func(t *testing.T) {
			now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
			readiness := NewReadinessOwner(true)
			readiness.SetExposure(ExposureEvidence{GenerationID: "exposure", StartupAuthorityID: "startup", ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL)})
			var credentials, effects, selections int
			readiness.SetCurrentnessChecks(func(context.Context, string) (bool, error) {
				if disposition == "authority_read_error" {
					return false, errors.New("startup unavailable")
				}
				switch disposition {
				case "unrelated_registration_update":
					readiness.registration.recordAdmissionFailure(RegistrationPair{BindingID: "other", Target: RegistrationTarget{Alias: "other", Provider: "partner", Selector: "ingress:other:partner"}}, "changed during exposure check")
				case "revoked_during_evaluation":
					readiness.RevokeExposure("withdrawn during evaluation")
				case "replaced_during_evaluation":
					readiness.SetExposure(ExposureEvidence{GenerationID: "replacement", StartupAuthorityID: "startup", ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL)})
				}
				return disposition != "foreign_authority", nil
			}, func(context.Context, map[string]string) (bool, error) { credentials++; return true, nil },
				func(context.Context, runtimeeffects.Authority) (bool, error) { effects++; return true, nil })
			readiness.selectionCurrent = func(context.Context, RegistrationPair) (bool, error) { selections++; return true, nil }
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
			case "unrelated_registration_current", "unrelated_registration_update":
				installReadinessRegistration(t, readiness, RegistrationEvidence{BindingID: "other", Alias: "other", Provider: "partner", StartupAuthorityID: "startup", ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL)})
			}
			want := disposition == "current" || disposition == "unrelated_registration_failed" || disposition == "unrelated_registration_current" || disposition == "unrelated_registration_update"
			if got := controller.ExposureCurrent(t.Context()); got != want {
				t.Fatalf("exposure=%t want=%t snapshot=%+v", got, want, readiness.Snapshot(now))
			}
			if credentials != 0 || effects != 0 || selections != 0 {
				t.Fatalf("author exposure observed unrelated registration: credential=%d effect=%d selection=%d", credentials, effects, selections)
			}
		})
	}
	t.Run("concurrent_exposure_revocation", func(t *testing.T) {
		now := time.Date(2026, 10, 18, 18, 0, 0, 0, time.UTC)
		readiness := NewReadinessOwner(true)
		readiness.SetExposure(ExposureEvidence{GenerationID: "exposure", StartupAuthorityID: "startup", ObservedAt: now, ExpiresAt: now.Add(EvidenceTTL)})
		entered, release := make(chan struct{}), make(chan struct{})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		readiness.SetCurrentnessChecks(func(context.Context, string) (bool, error) {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return true, nil
		}, nil, nil)
		controller := &ProviderRegistrationController{snapshot: readiness.registration, opts: RegistrationControllerOptions{Readiness: readiness, Now: func() time.Time { return now }}}
		done := make(chan bool, 1)
		go func() { done <- controller.ExposureCurrent(ctx) }()
		<-entered
		readiness.RevokeExposure("revoked at the startup observation boundary")
		close(release)
		if <-done {
			t.Fatal("concurrently revoked exposure survived its exact exposure revision fence")
		}
	})
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
