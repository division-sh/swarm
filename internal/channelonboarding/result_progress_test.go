package channelonboarding

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
)

func TestOnboardingSelectedProviderRequirementSurvivesReadback(t *testing.T) {
	for _, role := range []string{"", "session_token"} {
		c := testCandidate(strings.Repeat("a", 64), "support")
		c.Posture, c.ProviderCredentialRole, c.SigningCredentialRole, c.Target.SigningCredentialKey = ActivationSessionConnection, role, "", ""
		c.ConnectionHealth = "provider_connection"
		c.Coordinate.TargetGeneration, c.Target.Generation, c.Target.PublicationSequence = 0, 0, 0
		r := Result{Candidate: &c, Operation: Operation{OperationID: "selected-operation", Revision: 1,
			Provider: c.Provider, Interface: c.Interface, Coordinate: c.Coordinate, TargetSelector: c.Target.Selector,
			Posture: c.Posture, Ceremony: c.Ceremony, Phase: PhaseActivatingProvider}, Pairing: &PairingReadback{Status: PairingStatusPreparing}}
		body, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		var readback Result
		if err := json.Unmarshal(body, &readback); err != nil {
			t.Fatal(err)
		}
		if got, err := readback.ProviderCredentialRequirement(); err != nil || got != role {
			t.Fatal("selected role changed across readback", got, err)
		}
		if pending, err := readback.SessionPreparationPending(); err != nil || !pending {
			t.Fatal("non-executable preparation was lost", pending, err)
		}
		readback.Operation.Coordinate.ContextPublicationGeneration++
		if _, err := readback.ProviderCredentialRequirement(); err == nil {
			t.Fatal("foreign operation borrowed the selected requirement")
		}
		if pending, err := readback.SessionPreparationPending(); err == nil || pending {
			t.Fatal("foreign operation borrowed pending preparation")
		}
	}
}

func TestInitialSessionPreparationProjectionIsExact(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Result)
	}{
		{"initial", func(*Result) {}},
		{"absent_pairing", func(r *Result) { r.Pairing = nil }},
		{"already_preparing", func(r *Result) { r.Pairing.Status = PairingStatusPreparing }},
		{"current_qr", func(r *Result) { r.Pairing.Code, r.Pairing.ExpiresAt = "controlled-qr", time.Now().Add(time.Minute) }},
		{"code_only", func(r *Result) { r.Pairing.Code = "controlled-qr" }},
		{"expiry_only", func(r *Result) { r.Pairing.ExpiresAt = time.Now().Add(time.Minute) }},
		{"malformed_code", func(r *Result) { r.Pairing.Code = "\x00" }},
		{"expired", func(r *Result) { r.Pairing.Status = "qr_expired" }},
		{"failed_observation", func(r *Result) { r.Pairing.Status = "failed" }},
		{"stopped_observation", func(r *Result) { r.Pairing.Status = "stopped" }},
		{"recovery_required", func(r *Result) { r.Pairing.Status = "recovery_required" }},
		{"paired", func(r *Result) { r.Pairing.Paired = true }},
		{"connected", func(r *Result) { r.Pairing.Connected = true }},
		{"missing_candidate", func(r *Result) { r.Candidate = nil }},
		{"missing_operation", func(r *Result) { r.Operation.OperationID = "" }},
		{"missing_revision", func(r *Result) { r.Operation.Revision = 0 }},
		{"invalid_candidate", func(r *Result) { r.Candidate.Provider = "" }},
		{"foreign_provider", func(r *Result) { r.Operation.Provider = "other" }},
		{"foreign_interface", func(r *Result) { r.Operation.Interface.Selector = "other" }},
		{"foreign_coordinate", func(r *Result) { r.Operation.Coordinate.ContextPublicationGeneration++ }},
		{"foreign_target", func(r *Result) { r.Operation.TargetSelector = "other" }},
		{"foreign_ceremony", func(r *Result) { r.Operation.Ceremony = "other" }},
		{"non_session", func(r *Result) { r.Operation.Posture = ActivationWebhookRegistration }},
		{"retired", func(r *Result) { r.Operation.Phase = PhaseRetired }},
		{"failed_phase", func(r *Result) { r.Operation.Phase = PhaseFailed }},
		{"succeeded", func(r *Result) { r.Operation.Phase = PhaseSucceeded }},
		{"failure_code", func(r *Result) { r.Operation.FailureCode = "controlled" }},
		{"failure_message", func(r *Result) { r.Operation.FailureMessage = "controlled" }},
		{"identity_reference", func(r *Result) { r.Operation.IdentityOperationID = "controlled" }},
		{"account", func(r *Result) { r.Operation.SessionAccount.Provider = "controlled" }},
		{"identity_observation", func(r *Result) { r.IdentityOperation = &operatorchannel.Operation{} }},
		{"activation", func(r *Result) { r.Operation.ActivationRevision = 1 }},
		{"binding", func(r *Result) { r.Binding = &operatorchannel.Binding{} }},
		{"logout", func(r *Result) { r.Logout = &SessionLogoutReadback{} }},
		{"ready", func(r *Result) { r.Readiness = &ConnectedChannelReadiness{Ready: true} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testCandidate(strings.Repeat("a", 64), "support")
			c.Posture, c.ProviderCredentialRole, c.SigningCredentialRole, c.Target.SigningCredentialKey = ActivationSessionConnection, "", "", ""
			c.ConnectionHealth = "provider_connection"
			c.Coordinate.TargetGeneration, c.Target.Generation, c.Target.PublicationSequence = 0, 0, 0
			r := Result{Candidate: &c, Operation: Operation{OperationID: "selected-operation", Revision: 3,
				Provider: c.Provider, Interface: c.Interface, Coordinate: c.Coordinate, TargetSelector: c.Target.Selector,
				Posture: c.Posture, Ceremony: c.Ceremony, Phase: PhaseActivatingProvider}, Pairing: &PairingReadback{Status: "awaiting_qr"}}
			tc.edit(&r)
			before := r
			var original PairingReadback
			if r.Pairing != nil {
				original = *r.Pairing
			}
			r.projectInitialSessionPreparation()
			if tc.name != "initial" {
				if !reflect.DeepEqual(r, before) || before.Pairing != nil && *before.Pairing != original {
					t.Fatal("non-initial evidence was projected as preparation")
				}
				return
			}
			if pending, err := r.SessionPreparationPending(); err != nil || !pending {
				t.Fatal("owned initial snapshot did not project as non-executable preparation", pending, err)
			}
			if *before.Pairing != original || r.Pairing == before.Pairing || !reflect.DeepEqual(r.Operation, before.Operation) {
				t.Fatal("projection mutated private evidence or durable responsibility")
			}
			if pending, err := before.SessionPairingPending(time.Now()); pending || err == nil {
				t.Fatal("awaiting QR without current material no longer refuses")
			}
		})
	}
}
