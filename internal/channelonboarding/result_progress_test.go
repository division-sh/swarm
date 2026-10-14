package channelonboarding

import (
	"encoding/json"
	"strings"
	"testing"
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
