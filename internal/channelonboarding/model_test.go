package channelonboarding

import (
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
)

func TestChannelRuntimeContextCoordinateRequiresEveryExactGeneration(t *testing.T) {
	valid := testCoordinate()
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid coordinate: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*ChannelRuntimeContextCoordinate)
	}{
		{name: "source", mutate: func(c *ChannelRuntimeContextCoordinate) { c.BundleHash = "" }},
		{name: "bundle identity", mutate: func(c *ChannelRuntimeContextCoordinate) { c.BundleIdentity = "" }},
		{name: "inventory", mutate: func(c *ChannelRuntimeContextCoordinate) { c.PackInventoryGeneration = "" }},
		{name: "runtime instance", mutate: func(c *ChannelRuntimeContextCoordinate) { c.RuntimeInstanceID = "" }},
		{name: "publication", mutate: func(c *ChannelRuntimeContextCoordinate) { c.ContextPublicationGeneration = 0 }},
		{name: "plan", mutate: func(c *ChannelRuntimeContextCoordinate) { c.PlanGeneration = plangeneration.Generation{} }},
		{name: "target", mutate: func(c *ChannelRuntimeContextCoordinate) { c.TargetGeneration = 0 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			candidate := valid
			tc.mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Fatal("incomplete coordinate was accepted")
			}
		})
	}
}

func TestChannelRuntimeContextCoordinateSeparatesDurableIdentityFromLiveOccurrence(t *testing.T) {
	original := testCoordinate()
	abaSuccessor := original
	abaSuccessor.RuntimeInstanceID = "22222222-2222-4222-8222-222222222222"
	if !original.MatchesDurableIdentity(abaSuccessor) || original.LiveOccurrence().Matches(abaSuccessor.LiveOccurrence()) || original.Matches(abaSuccessor) {
		t.Fatal("process restart reused numeric generations as live occurrence authority")
	}
	successor := original
	successor.RuntimeInstanceID = abaSuccessor.RuntimeInstanceID
	successor.ContextPublicationGeneration++
	successor.TargetGeneration++
	if !original.MatchesDurableIdentity(successor) {
		t.Fatal("process-local successor changed durable channel context identity")
	}
	if original.LiveOccurrence().Matches(successor.LiveOccurrence()) || original.Matches(successor) {
		t.Fatal("successor live occurrence was accepted as the predecessor occurrence")
	}

	tests := []struct {
		name   string
		mutate func(*ChannelRuntimeContextCoordinate)
	}{
		{name: "bundle hash", mutate: func(c *ChannelRuntimeContextCoordinate) { c.BundleHash = "bundle-v2:sha256:" + strings.Repeat("b", 64) }},
		{name: "bundle identity", mutate: func(c *ChannelRuntimeContextCoordinate) { c.BundleIdentity = "bundle:changed@sha256:identity" }},
		{name: "pack inventory", mutate: func(c *ChannelRuntimeContextCoordinate) { c.PackInventoryGeneration = "sha256:changed" }},
		{name: "plan generation", mutate: func(c *ChannelRuntimeContextCoordinate) { c.PlanGeneration = testPlanGeneration("changed") }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			changed := successor
			tc.mutate(&changed)
			if original.MatchesDurableIdentity(changed) {
				t.Fatal("changed source semantics retained durable channel identity")
			}
		})
	}
}

func TestChannelRuntimeContextCoordinateMatchesContextOccurrenceWithoutStandingTarget(t *testing.T) {
	coordinate := testCoordinate()
	coordinate.TargetGeneration = 0
	if !coordinate.MatchesContextOccurrence("  "+coordinate.RuntimeInstanceID+"  ", coordinate.ContextPublicationGeneration) {
		t.Fatal("declared activation context occurrence required a standing target")
	}
	if coordinate.MatchesContextOccurrence("22222222-2222-4222-8222-222222222222", coordinate.ContextPublicationGeneration) {
		t.Fatal("successor runtime instance matched predecessor context occurrence")
	}
	if coordinate.MatchesContextOccurrence(coordinate.RuntimeInstanceID, coordinate.ContextPublicationGeneration+1) {
		t.Fatal("successor publication matched predecessor context occurrence")
	}
}

func TestVerbAdmissionMatrix(t *testing.T) {
	states := []SlotState{SlotAbsent, SlotIdentityCurrentActivationAbsent, SlotOperationPending, SlotReady, SlotActivationStale, SlotUncertain, SlotRetired}
	wants := map[Verb]map[SlotState]AdmissionDecision{
		VerbConnect: {
			SlotAbsent: AdmissionStart, SlotIdentityCurrentActivationAbsent: AdmissionTeachReconnect,
			SlotOperationPending: AdmissionConflict, SlotReady: AdmissionAlreadyConnected,
			SlotActivationStale: AdmissionTeachReconnect, SlotUncertain: AdmissionConflict, SlotRetired: AdmissionStart,
		},
		VerbReconnect: {
			SlotAbsent: AdmissionNothingToReconnect, SlotIdentityCurrentActivationAbsent: AdmissionStartPreservingIdentity,
			SlotOperationPending: AdmissionConflict, SlotReady: AdmissionStartPreservingIdentity,
			SlotActivationStale: AdmissionStartPreservingIdentity, SlotUncertain: AdmissionConflict, SlotRetired: AdmissionNothingToReconnect,
		},
		VerbRebind: {
			SlotAbsent: AdmissionNothingToRebind, SlotIdentityCurrentActivationAbsent: AdmissionStartReplacingIdentity,
			SlotOperationPending: AdmissionConflict, SlotReady: AdmissionStartReplacingIdentity,
			SlotActivationStale: AdmissionStartReplacingIdentity, SlotUncertain: AdmissionConflict, SlotRetired: AdmissionNothingToRebind,
		},
	}
	for _, verb := range []Verb{VerbConnect, VerbReconnect, VerbRebind} {
		for _, state := range states {
			t.Run(string(verb)+"/"+string(state), func(t *testing.T) {
				if got, want := AdmitVerb(verb, state), wants[verb][state]; got != want {
					t.Fatalf("AdmitVerb(%s, %s) = %s, want %s", verb, state, got, want)
				}
			})
		}
	}
	for _, invalid := range []Verb{"", "replace", "auto"} {
		t.Run("invalid/"+string(invalid), func(t *testing.T) {
			if got := AdmitVerb(invalid, SlotAbsent); got != AdmissionConflict {
				t.Fatalf("AdmitVerb(%q, absent) = %s, want conflict", invalid, got)
			}
		})
	}
}

func TestConnectedReadinessRejectsEveryMixedOrMissingFact(t *testing.T) {
	facts := testReadinessFacts()
	ready := ProjectReadiness(facts)
	if !ready.Ready || ready.Reason != ReadinessReady {
		t.Fatalf("complete readiness = %#v", ready)
	}
	tests := []struct {
		name   string
		mutate func(*ReadinessFacts)
		want   ReadinessReason
	}{
		{name: "coordinate", mutate: func(f *ReadinessFacts) { f.Coordinate.BundleHash = "" }, want: ReadinessCoordinateInvalid},
		{name: "plan", mutate: func(f *ReadinessFacts) { f.PlanGeneration = testPlanGeneration("stale") }, want: ReadinessPlanUnavailable},
		{name: "activation publication", mutate: func(f *ReadinessFacts) { f.ActivationGeneration = ChannelActivationGeneration{} }, want: ReadinessPlanUnavailable},
		{name: "activation", mutate: func(f *ReadinessFacts) { f.ActivationCurrent = false }, want: ReadinessActivationUnavailable},
		{name: "binding", mutate: func(f *ReadinessFacts) { f.BindingRevision++ }, want: ReadinessBindingUnavailable},
		{name: "proof", mutate: func(f *ReadinessFacts) { f.ProofCurrent = false }, want: ReadinessProofUnavailable},
		{name: "credentials", mutate: func(f *ReadinessFacts) { f.CredentialsCurrent = false }, want: ReadinessCredentialsUnavailable},
		{name: "confirmation", mutate: func(f *ReadinessFacts) { f.ConfirmationActivationRevision-- }, want: ReadinessConfirmationUnavailable},
		{name: "target", mutate: func(f *ReadinessFacts) { f.TargetGeneration++ }, want: ReadinessTargetUnavailable},
		{name: "exposure", mutate: func(f *ReadinessFacts) { f.ExposureGeneration = "exposure-2" }, want: ReadinessExposureUnavailable},
		{name: "registration", mutate: func(f *ReadinessFacts) { f.RegistrationCurrent = false }, want: ReadinessRegistrationUnavailable},
		{name: "registration activation publication", mutate: func(f *ReadinessFacts) {
			f.RegistrationActivationGeneration = ChannelActivationGeneration{generation: testPlanGeneration("stale-publication")}
		}, want: ReadinessRegistrationUnavailable},
		{name: "session", mutate: func(f *ReadinessFacts) {
			f.Posture = ActivationSessionConnection
			f.ServiceFulfillmentGeneration = "service-1"
			f.ExpectedServiceGeneration = "service-1"
			f.SessionCurrent = false
		}, want: ReadinessSessionUnavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			candidate := facts
			tc.mutate(&candidate)
			if got := ProjectReadiness(candidate); got.Ready || got.Reason != tc.want {
				t.Fatalf("mixed readiness = %#v, want reason %s", got, tc.want)
			}
		})
	}

	proofless := facts
	proofless.ProofID = ""
	proofless.ProofRevision = 0
	proofless.ProofCurrent = false
	if got := ProjectReadiness(proofless); !got.Ready {
		t.Fatalf("current proofless binding is not ready: %#v", got)
	}

	session := facts
	session.Posture = ActivationSessionConnection
	session.ServiceFulfillmentGeneration = "service-1"
	session.ExpectedServiceGeneration = "service-1"
	session.SessionCurrent = true
	if got := ProjectReadiness(session); !got.Ready {
		t.Fatalf("current session-backed channel is not ready: %#v", got)
	}
}

func testCoordinate() ChannelRuntimeContextCoordinate {
	return ChannelRuntimeContextCoordinate{
		BundleHash:                   "bundle-v2:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		BundleIdentity:               "bundle:customer-support@sha256:identity",
		PackInventoryGeneration:      "sha256:inventory",
		RuntimeInstanceID:            "11111111-1111-4111-8111-111111111111",
		ContextPublicationGeneration: 7,
		PlanGeneration:               testPlanGeneration("current"),
		TargetGeneration:             11,
	}
}

func testReadinessFacts() ReadinessFacts {
	identity := operatorchannel.InterfaceIdentity{
		InterfaceRef: operatorchannel.InterfaceHITLChannelV2, ChannelPackID: "provider.telegram.hitl_channel",
		ChannelPackVersion: "0.1.0", ChannelManifestHash: "sha256:manifest", SemanticGeneration: "sha256:plan",
	}.Normalized()
	activationGeneration := ChannelActivationGeneration{generation: testPlanGeneration("activation-current")}
	return ReadinessFacts{
		Coordinate:                       testCoordinate(),
		Interface:                        identity,
		ActivationRevision:               3,
		PlanGeneration:                   testPlanGeneration("current"),
		ActivationGeneration:             activationGeneration,
		RegistrationActivationGeneration: activationGeneration,
		ActivationCurrent:                true,
		BindingRevision:                  4,
		ExpectedBindingRevision:          4,
		ProofID:                          "proof-a",
		ProofRevision:                    2,
		ExpectedProofRevision:            2,
		ProofCurrent:                     true,
		CredentialsCurrent:               true,
		ConfirmationActivationRevision:   3,
		ConfirmationBindingRevision:      4,
		ConfirmationTerminalSuccess:      true,
		Posture:                          ActivationWebhookRegistration,
		TargetGeneration:                 11,
		ExpectedTargetGeneration:         11,
		ExposureGeneration:               "exposure-1",
		ExpectedExposureGeneration:       "exposure-1",
		RegistrationCurrent:              true,
		ObservedAt:                       time.Now().UTC(),
	}
}

func testPlanGeneration(label string) plangeneration.Generation {
	generation, err := plangeneration.FromCanonicalValue(map[string]string{"test": label})
	if err != nil {
		panic(err)
	}
	return generation
}
