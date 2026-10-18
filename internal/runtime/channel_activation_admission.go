package runtime

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	sessionexecution "github.com/division-sh/swarm/internal/sessionprovider/execution"
)

type channelSessionAdmission struct {
	owner operatorchannel.SessionAdmissionOwner
}

func (rt *Runtime) nativeChannelExecution(ctx context.Context, activation channelonboarding.CompiledActivation) (sessionexecution.Channel, error) {
	var absent sessionexecution.Channel
	if err := rt.validateLearnedChannelPublication(ctx, activation); err != nil {
		return absent, err
	}
	binding := rt.channelSessions.Load()
	if binding == nil {
		return absent, &operatorchannel.SessionProviderUnavailableError{Provider: activation.SessionAccount.Provider}
	}
	owner, ok := binding.owner.(interface {
		ChannelExecution(context.Context, channelonboarding.Operation) (sessionexecution.Channel, error)
	})
	if !ok {
		return absent, &operatorchannel.SessionProviderUnavailableError{Provider: activation.SessionAccount.Provider}
	}
	op, err := rt.Options.ChannelOnboardingStore.GetChannelOnboarding(ctx, activation.OnboardingOperationID)
	if err != nil {
		return absent, err
	}
	return owner.ChannelExecution(ctx, op)
}

func (rt *Runtime) bindChannelSessionAdmission(binding *channelSessionAdmission) error {
	if rt == nil || binding == nil || binding.owner == nil {
		return fmt.Errorf("channel session admission requires its original owner")
	}
	if rt.channelSessions.CompareAndSwap(nil, binding) || rt.channelSessions.Load() == binding {
		return nil
	}
	return fmt.Errorf("channel session admission owner cannot be replaced")
}

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
			if err := rt.validateLearnedChannelPublication(ctx, activation); err != nil {
				return err
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

func (rt *Runtime) validateLearnedChannelPublication(ctx context.Context, activation channelonboarding.CompiledActivation) error {
	responsibility, err := activation.AdmissionResponsibility()
	if err != nil {
		return err
	}
	current, err := channelonboarding.AdmissionResponsibilityCurrent(ctx, rt.Options.ChannelOnboardingStore, responsibility, true)
	if err != nil {
		return err
	}
	if !current {
		return fmt.Errorf("%w: learned channel publication lost its exact admission responsibility", channelonboarding.ErrRevisionConflict)
	}
	if activation.Plan.Transport() == packs.ChannelTransportSession {
		return rt.validateNativeChannelPublication(ctx, activation)
	}
	return nil
}

func (rt *Runtime) validateNativeChannelPublication(ctx context.Context, activation channelonboarding.CompiledActivation) error {
	binding := rt.channelSessions.Load()
	if binding == nil {
		return &operatorchannel.SessionProviderUnavailableError{Provider: activation.SessionAccount.Provider}
	}
	native, err := binding.owner.AdmitSessionAccount(ctx, activation.SessionAccount)
	defer native.Close()
	if err != nil {
		return err
	}
	parent, revision := native.Parent()
	if parent != activation.OnboardingOperationID || revision < activation.OnboardingRevision || native.Validate(ctx, activation.SessionAccount) != nil {
		return fmt.Errorf("%w: native channel publication lost its original SDK admission", channelonboarding.ErrRevisionConflict)
	}
	return nil
}
