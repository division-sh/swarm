package runlifecycle

import (
	"strings"
	"testing"
)

func TestStandingLearnedAuthorityRecoveryRequired(t *testing.T) {
	for _, override := range []string{"none", "suspended"} {
		fact := StandingRestartFact{
			ExactCurrent: true, ServiceID: "11111111-1111-4111-8111-111111111111",
			RunID: "22222222-2222-4222-8222-222222222222", Generation: 3,
			DeclarationPresent: true, BindingEnabled: false,
			EffectiveState: "recovery_required", OperatorOverride: override, RunState: "paused",
		}
		got, err := ClassifyStandingRestart(fact)
		if err != nil {
			t.Fatal(err)
		}
		if got.Kind != "recovery_required" || got.Executable() || got.UsesGenericRecovery() ||
			!got.DeclarationPresent || got.OperatorOverride != override || got.Generation != 3 ||
			!strings.Contains(got.RunControlGuidance(), "channel resume") {
			t.Fatalf("stale learned authority disposition = %+v", got)
		}
		fact.RunState = "running"
		if got, err := ClassifyStandingRestart(fact); err != nil || got.Kind != StandingRestartInvalidCurrent {
			t.Fatalf("unquiesced recovery-required generation = %+v, %v", got, err)
		}
		fact.RunState = "completed"
		if got, err := ClassifyStandingRestart(fact); err != nil || got.Kind != StandingRestartTerminalDeclared {
			t.Fatalf("recovery-required terminal precedence = %+v, %v", got, err)
		}
		fact.RunState, fact.BindingEnabled = "running", true
		if _, err := ClassifyStandingRestart(fact); err == nil {
			t.Fatal("recovery-required binding admitted as enabled")
		}
	}
}

func TestStandingCredentialDormancyPreservesDeclarationAndOverride(t *testing.T) {
	for _, override := range []string{"none", "suspended"} {
		fact := StandingRestartFact{
			ExactCurrent: true, ServiceID: "11111111-1111-4111-8111-111111111111",
			RunID: "22222222-2222-4222-8222-222222222222", Generation: 3,
			DeclarationPresent: true, BindingEnabled: false,
			EffectiveState: "dormant", OperatorOverride: override, RunState: "paused",
		}
		disposition, err := ClassifyStandingRestart(fact)
		if err != nil {
			t.Fatal(err)
		}
		if disposition.Kind != StandingRestartCredentialDormant || disposition.Executable() || disposition.UsesGenericRecovery() {
			t.Fatalf("dormant execution authority: %+v", disposition)
		}
		if !disposition.DeclarationPresent || disposition.OperatorOverride != override || disposition.Generation != 3 {
			t.Fatalf("dormancy rewrote history or override: %+v", disposition)
		}
		fact.RunState = "running"
		if invalid, err := ClassifyStandingRestart(fact); err != nil || invalid.Kind != StandingRestartInvalidCurrent {
			t.Fatalf("unquiesced dormant generation = %+v, err=%v", invalid, err)
		}
		fact.RunState = "completed"
		if terminal, err := ClassifyStandingRestart(fact); err != nil || terminal.Kind != StandingRestartTerminalDeclared {
			t.Fatalf("terminal remediation lost = %+v, err=%v", terminal, err)
		}
	}
}

func TestStandingRestartRejectsEligibilityStateContradictions(t *testing.T) {
	for _, state := range []string{"active", "suspended"} {
		fact := StandingRestartFact{
			ExactCurrent: true, ServiceID: "11111111-1111-4111-8111-111111111111",
			RunID: "22222222-2222-4222-8222-222222222222", Generation: 1,
			DeclarationPresent: true, BindingEnabled: false, EffectiveState: state,
			OperatorOverride: "none", RunState: "paused",
		}
		if state == "suspended" {
			fact.OperatorOverride = "suspended"
		}
		if _, err := ClassifyStandingRestart(fact); err == nil {
			t.Fatalf("disabled binding accepted as %s", state)
		}
		fact.BindingEnabled, fact.EffectiveState = true, "dormant"
		if _, err := ClassifyStandingRestart(fact); err == nil {
			t.Fatal("enabled binding accepted as dormant")
		}
	}
}
