package runtime

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/credentials"
)

func (rt *Runtime) validateChannelActivationPublication(ctx context.Context, publication channelonboarding.ChannelActivationPublication) error {
	var owner *credentials.SnapshotOwner
	var err error
	if rt.Options.ProviderCredentials != nil {
		owner, err = credentials.NewSnapshotOwner(rt.Options.ProviderCredentials)
		if err != nil {
			return err
		}
	}
	projection := owner.BeginSecretBindingProjection()
	allCurrent := true
	for _, activation := range publication.Activations() {
		if err := activation.Validate(); err != nil {
			return err
		}
		if activation.Source == channelonboarding.ActivationSourceDeclared && activation.Plan.RegistrationTarget() != "" {
			if err := rt.ValidateStandingIngressCredentials(ctx); err != nil {
				return err
			}
		}
		if activation.Source == channelonboarding.ActivationSourceLearned {
			profile, ok := activation.Plan.OnboardingProfile()
			if !ok {
				return fmt.Errorf("learned channel publication requires its onboarding profile")
			}
			current, err := channelonboarding.AdmissionResponsibilityCurrent(ctx, rt.Options.ChannelOnboardingStore, channelonboarding.AdmissionResponsibility{
				OperationID: activation.OnboardingOperationID, OperationRevision: activation.OnboardingRevision,
				ActivationRevision: activation.ActivationRevision, Coordinate: activation.Coordinate,
				TargetSelector: activation.Plan.RegistrationTarget(), Provider: profile.Provider(), Credentials: activation.CredentialAdmissions,
			}, true)
			if err != nil {
				return err
			}
			if !current {
				return fmt.Errorf("%w: learned channel publication lost its exact admission responsibility", channelonboarding.ErrRevisionConflict)
			}
		}
		for _, admission := range activation.CredentialAdmissions {
			_, current, err := projection.ObserveAdmittedActivationCredential(ctx,
				credentials.ValueEvidence{Key: admission.StoreKey, Seal: admission.ValueSeal}, admission.Receipt)
			if err != nil {
				return err
			}
			allCurrent = allCurrent && current
		}
	}
	if !allCurrent {
		return fmt.Errorf("%w: channel publication credential admission is no longer current", channelonboarding.ErrRevisionConflict)
	}
	return projection.ValidateCurrent(ctx)
}
