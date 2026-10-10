package channelonboarding

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/google/uuid"
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

func TestChannelRuntimeCoordinatesFollowClosedPhaseProduct(t *testing.T) {
	rows := []struct {
		phase            Phase
		webhook, session bool
	}{
		{PhasePreparing, false, false}, {PhaseCredentialsAdmitted, false, false},
		{PhaseActivatingProvider, true, false}, {PhaseAwaitingExternalIdentity, true, false},
		{PhaseAwaitingOperatorConfirmation, true, false}, {PhasePublishingActivation, true, true},
		{PhasePublishingProcessActivation, true, true}, {PhasePromotingRegistration, true, true},
		{PhaseRetiringPredecessor, true, true}, {PhaseDeliveringConfirmation, true, true},
		{PhaseSucceeded, true, true}, {PhaseFailed, false, false}, {PhaseRetired, false, false},
	}
	if len(rows) != len(ValidPhases()) {
		t.Fatal("phase product census is incomplete")
	}
	for _, row := range rows {
		for _, posture := range []ActivationPosture{ActivationWebhookRegistration, ActivationSessionConnection, "unknown"} {
			for _, executable := range []bool{false, true} {
				t.Run(string(row.phase)+"/"+string(posture)+"/"+fmt.Sprint(executable), func(t *testing.T) {
					coordinate := testCoordinate()
					if !executable {
						coordinate.TargetGeneration = 0
					}
					requires := row.webhook
					if posture == ActivationSessionConnection {
						requires = row.session
					}
					want := posture.Valid() && (executable || !requires)
					if err := coordinate.ValidateForPhase(row.phase, posture); (err == nil) != want {
						t.Fatalf("coordinate phase product = %v, want valid=%t", err, want)
					}
				})
			}
		}
	}
	if err := testCoordinate().ValidateForPhase("unknown", ActivationSessionConnection); err == nil {
		t.Fatal("unknown phase admitted")
	}
}

func TestRetainedSessionAccountIsProvenanceNotExecutableAuthority(t *testing.T) {
	account := operatorchannel.SessionAccountAdmission{Provider: "whatsapp", ConnectionID: uuid.NewString(),
		AccountRef: "paired-account", AdmissionID: uuid.NewString(), Revision: 1}
	for _, row := range []struct {
		phase           Phase
		absent, present bool
	}{
		{PhasePreparing, true, false}, {PhaseCredentialsAdmitted, true, false}, {PhaseActivatingProvider, true, true},
		{PhaseAwaitingExternalIdentity, false, true}, {PhaseAwaitingOperatorConfirmation, false, true},
		{PhasePublishingActivation, false, true}, {PhasePublishingProcessActivation, false, true},
		{PhasePromotingRegistration, false, true}, {PhaseRetiringPredecessor, false, true},
		{PhaseDeliveringConfirmation, false, true}, {PhaseSucceeded, false, true}, {PhaseFailed, true, true}, {PhaseRetired, true, true},
	} {
		for _, present := range []bool{false, true} {
			op := Operation{Posture: ActivationSessionConnection, Provider: "whatsapp", Phase: row.phase, SessionConnectionID: account.ConnectionID}
			want := row.absent
			if present {
				op.SessionAccount, want = account, row.present
			}
			if err := op.ValidateSessionAccount(); (err == nil) != want {
				t.Fatalf("phase %s present=%t: %v, want valid=%t", row.phase, present, err, want)
			}
		}
	}
	for _, mutate := range []func(*Operation){
		func(o *Operation) { o.Posture = ActivationWebhookRegistration },
		func(o *Operation) { o.SessionAccount.Provider = "other" },
		func(o *Operation) { o.SessionAccount.Revision = 0 },
		func(o *Operation) { o.SessionAccount.ConnectionID = "" },
		func(o *Operation) { o.SessionConnectionID = "" },
		func(o *Operation) { o.SessionConnectionID = uuid.NewString() },
	} {
		op := Operation{Posture: ActivationSessionConnection, Provider: "whatsapp", Phase: PhaseAwaitingExternalIdentity, SessionConnectionID: account.ConnectionID, SessionAccount: account}
		mutate(&op)
		if err := op.ValidateSessionAccount(); err == nil {
			t.Fatal("contradictory retained account admitted")
		}
	}
	authority := operatorchannel.ProviderAuthority{Kind: operatorchannel.ProviderAuthoritySession, Session: account}
	if err := authority.RequireExecutable(); err == nil {
		t.Fatal("retained DTO acquired executable authority")
	}
	for _, value := range []any{Operation{Coordinate: testCoordinate(), SessionConnectionID: account.ConnectionID, SessionAccount: account}, ConnectedChannelActivation{Coordinate: testCoordinate(), SessionAccount: account}} {
		encoded, err := json.Marshal(value)
		if err != nil || strings.Contains(string(encoded), account.AccountRef) || strings.Contains(string(encoded), account.ConnectionID) {
			t.Fatalf("private account leaked through public record: %s %v", encoded, err)
		}
	}
}

func TestCredentialRecoveryCommandPreservesTerminalResponsibility(t *testing.T) {
	for _, phase := range ValidPhases() {
		t.Run(string(phase), func(t *testing.T) {
			operation := Operation{OperationID: "exact-operation", Provider: "telegram", Phase: phase,
				Coordinate: testCoordinate(), Interface: operatorchannel.InterfaceIdentity{Selector: "exact-interface"}, TargetSelector: "ingress:exact-flow:telegram"}
			command := operation.CredentialRecoveryCommand()
			if phase.Terminal() {
				for _, exact := range []string{"swarm channel reconnect telegram", "--bundle " + operation.Coordinate.BundleHash, "--interface exact-interface", "--target ingress:exact-flow:telegram", "--credential-stdin"} {
					if !strings.Contains(command, exact) {
						t.Fatalf("terminal remedy lacks exact selector %q: %s", exact, command)
					}
				}
				if strings.Contains(command, "resume") || strings.Contains(command, operation.OperationID) {
					t.Fatalf("terminal remedy reopens historical responsibility: %s", command)
				}
				return
			}
			want := "swarm channel resume exact-operation"
			if phase == PhasePreparing {
				want += " --credential-stdin"
			}
			if command != want {
				t.Fatalf("nonterminal remedy = %q, want %q", command, want)
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
	session.SessionAuthority = operatorchannel.ProviderAuthority{Kind: operatorchannel.ProviderAuthoritySession,
		Session: operatorchannel.SessionAccountAdmission{Provider: "whatsapp", ConnectionID: uuid.NewString(),
			AccountRef: "provider-account", AdmissionID: uuid.NewString(), Revision: 1}}
	session.SessionObservation = &operatorchannel.SessionConnectionObservation{Admission: session.SessionAuthority.Session,
		OccurrenceID: uuid.NewString(), Connected: true, ObservedAt: session.ObservedAt}
	if got := ProjectReadiness(session); got.Ready || got.Reason != ReadinessSessionUnavailable {
		t.Fatalf("uninstalled session fabricated readiness: %#v", got)
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
