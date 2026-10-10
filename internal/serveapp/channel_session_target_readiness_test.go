//go:build linux || darwin

package serveapp

import (
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
)

func TestReviewerSessionReadinessRequiresCurrentTargetBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, op, observer := pairedServeSessionObserver(t, backend)
			provider, observation, current, err := observer.ObserveSession(f.ctx, op)
			defer provider.CloseExecution()
			if err != nil || !current {
				t.Fatal(current, err)
			}
			publication, err := channelonboarding.NewChannelActivationPublication(nil)
			if err != nil {
				t.Fatal(err)
			}
			// Only the SDK/account/selected parent are real here. As in the new
			// component proof, all other gates are supplied projection facts.
			facts := channelonboarding.ReadinessFacts{Coordinate: op.Coordinate, Interface: op.Interface,
				PlanGeneration: op.Coordinate.PlanGeneration, ActivationGeneration: publication.Generation(),
				ActivationRevision: 1, ActivationCurrent: true, BindingRevision: 1, ExpectedBindingRevision: 1,
				CredentialsCurrent: true, ConfirmationTerminalSuccess: true, ConfirmationActivationRevision: 1, ConfirmationBindingRevision: 1,
				Posture: channelonboarding.ActivationSessionConnection, SessionAuthority: provider, SessionObservation: &observation,
				TargetGeneration: op.Coordinate.TargetGeneration, ExpectedTargetGeneration: op.Coordinate.TargetGeneration,
				ObservedAt: observation.ObservedAt}
			if got := channelonboarding.ProjectReadiness(facts); !got.Ready {
				t.Fatal("positive control", got)
			}
			for _, tc := range []struct {
				name   string
				mutate func(*channelonboarding.ReadinessFacts)
			}{
				{"absent", func(f *channelonboarding.ReadinessFacts) { f.ExpectedTargetGeneration = 0 }},
				{"replaced", func(f *channelonboarding.ReadinessFacts) { f.ExpectedTargetGeneration++ }},
				{"missing_admission", func(f *channelonboarding.ReadinessFacts) { f.TargetGeneration = 0 }},
				{"wrong_admission", func(f *channelonboarding.ReadinessFacts) { f.TargetGeneration++ }},
			} {
				t.Run(tc.name, func(t *testing.T) {
					changed := facts
					tc.mutate(&changed)
					if got := channelonboarding.ProjectReadiness(changed); got.Ready || got.Reason != channelonboarding.ReadinessTargetUnavailable {
						t.Errorf("live session hid unavailable/replaced target: %+v", got)
					}
				})
			}
		})
	}
}
