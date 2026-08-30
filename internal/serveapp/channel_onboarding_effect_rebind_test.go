package serveapp

import (
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
	"github.com/google/uuid"
)

func TestChannelEffectRebindDispositionMatrix(t *testing.T) {
	generation, err := plangeneration.FromCanonicalValue(map[string]any{"test": "channel-effect-rebind"})
	if err != nil {
		t.Fatal(err)
	}
	coordinate := channelonboarding.ChannelRuntimeContextCoordinate{
		BundleHash:     "bundle-v2:sha256:" + strings.Repeat("a", 64),
		BundleIdentity: "bundle:test@sha256:effect-rebind", PackInventoryGeneration: "sha256:effect-rebind-inventory",
		RuntimeInstanceID: uuid.NewString(), ContextPublicationGeneration: 7,
		PlanGeneration: generation, TargetGeneration: 11,
	}
	op := channelonboarding.Operation{
		OperationID: uuid.NewString(), Revision: 9, Coordinate: coordinate,
		ConfirmationOperationID: uuid.NewString(),
	}
	base := runtimeeffects.ChannelOnboardingEffectOutcome{
		OperationOutcome: runtimeeffects.OperationOutcome{
			OperationID: uuid.NewString(), Kind: runtimeeffects.KindServeRegistration,
			AuthorityKind: runtimeeffects.AuthorityServeRegistration, AuthorityID: uuid.NewString(),
		},
		OnboardingOperationID: op.OperationID, OnboardingRevision: 4,
		BundleHash: coordinate.BundleHash, BundleIdentity: coordinate.BundleIdentity,
		PackInventoryGeneration: coordinate.PackInventoryGeneration, RuntimeInstanceID: coordinate.RuntimeInstanceID,
		ContextPublicationGeneration: coordinate.ContextPublicationGeneration,
		PlanGeneration:               coordinate.PlanGeneration, TargetGeneration: coordinate.TargetGeneration,
	}

	tests := []struct {
		name       string
		outcome    runtimeeffects.ChannelOnboardingEffectOutcome
		wantRetry  bool
		wantRemint bool
		wantErr    bool
	}{
		{name: "registration prelaunch terminal", outcome: channelEffectOutcomeWithState(base, runtimeeffects.StateTerminalFailure, false), wantRetry: true, wantRemint: true},
		{name: "registration launched", outcome: channelEffectOutcomeWithState(base, runtimeeffects.StateLaunched, true)},
		{name: "registration response observed", outcome: channelEffectOutcomeWithState(base, runtimeeffects.StateResponseObserved, true)},
		{name: "registration outcome uncertain", outcome: channelEffectOutcomeWithState(base, runtimeeffects.StateOutcomeUncertain, true)},
		{name: "registration terminal success", outcome: channelEffectOutcomeWithState(base, runtimeeffects.StateSettled, true), wantRetry: true, wantRemint: true},
		{name: "confirmation prelaunch terminal", outcome: channelConfirmationOutcome(base, op.ConfirmationOperationID, runtimeeffects.StateTerminalFailure, false), wantRetry: true, wantRemint: true},
		{name: "confirmation outcome uncertain", outcome: channelConfirmationOutcome(base, op.ConfirmationOperationID, runtimeeffects.StateOutcomeUncertain, true)},
		{name: "confirmation terminal success", outcome: channelConfirmationOutcome(base, op.ConfirmationOperationID, runtimeeffects.StateSettled, true), wantRetry: true},
		{name: "contradictory missing provenance", outcome: func() runtimeeffects.ChannelOnboardingEffectOutcome {
			changed := channelEffectOutcomeWithState(base, runtimeeffects.StateSettled, true)
			changed.TargetGeneration = 0
			return changed
		}(), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			disposition, err := classifyChannelEffectsBeforeRebind(op, []runtimeeffects.ChannelOnboardingEffectOutcome{test.outcome})
			if (err != nil) != test.wantErr {
				t.Fatalf("classification error=%v, wantErr=%v", err, test.wantErr)
			}
			if err == nil && (disposition.RetryAllowed != test.wantRetry || disposition.RemintConfirmationOperation != test.wantRemint) {
				t.Fatalf("disposition=%#v, want retry=%v remint=%v", disposition, test.wantRetry, test.wantRemint)
			}
		})
	}
}

func channelEffectOutcomeWithState(
	outcome runtimeeffects.ChannelOnboardingEffectOutcome,
	state runtimeeffects.State,
	launched bool,
) runtimeeffects.ChannelOnboardingEffectOutcome {
	outcome.State, outcome.AttemptState, outcome.Launched = state, state, launched
	return outcome
}

func channelConfirmationOutcome(
	outcome runtimeeffects.ChannelOnboardingEffectOutcome,
	operationID string,
	state runtimeeffects.State,
	launched bool,
) runtimeeffects.ChannelOnboardingEffectOutcome {
	outcome.OperationID, outcome.AuthorityID = operationID, operationID
	outcome.Kind = runtimeeffects.KindChannelConfirmation
	outcome.AuthorityKind = runtimeeffects.AuthorityChannelConfirmation
	return channelEffectOutcomeWithState(outcome, state, launched)
}
