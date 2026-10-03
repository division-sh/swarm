package channelonboarding

import (
	"context"
	"slices"
)

// AdmissionResponsibility carries the selected-store occurrence, not a lookup
// instruction that can adopt a later operation or credential value.
type AdmissionResponsibility struct {
	OperationID        string
	OperationRevision  int64
	ActivationRevision int64
	Coordinate         ChannelRuntimeContextCoordinate
	TargetSelector     string
	Provider           string
	Credentials        []CredentialAdmission
}

func (p Phase) SupportsPrebindingRegistration() bool {
	return p == PhaseActivatingProvider || p == PhaseAwaitingExternalIdentity ||
		p == PhaseAwaitingOperatorConfirmation || p == PhasePublishingActivation
}

func AdmissionResponsibilityCurrent(ctx context.Context, store Store, expected AdmissionResponsibility, exactPendingRevision bool) (bool, error) {
	if store == nil || expected.OperationID == "" || expected.OperationRevision < 1 {
		return false, nil
	}
	op, err := store.GetChannelOnboarding(ctx, expected.OperationID)
	if err != nil {
		return false, err
	}
	if !op.Coordinate.Matches(expected.Coordinate) || op.TargetSelector != expected.TargetSelector ||
		op.Provider != expected.Provider || op.Revision < expected.OperationRevision ||
		!slices.Equal(op.CredentialAdmissions, expected.Credentials) {
		return false, nil
	}
	if expected.ActivationRevision == 0 {
		return (op.Phase.SupportsPrebindingRegistration() || op.Phase == PhaseCredentialsAdmitted) &&
			(!exactPendingRevision || op.Revision == expected.OperationRevision), nil
	}
	if !op.Phase.RequiresExecutableTarget() {
		return false, nil
	}
	activation, err := store.GetConnectedChannelActivation(ctx, op.SlotKey)
	if err != nil {
		return false, err
	}
	return activation.Status == ActivationCurrent && activation.OperationID == expected.OperationID &&
		activation.Revision == expected.ActivationRevision && activation.OperationRevision == expected.OperationRevision &&
		activation.Coordinate.Matches(expected.Coordinate) && activation.TargetSelector == expected.TargetSelector &&
		activation.Provider == expected.Provider && slices.Equal(activation.CredentialAdmissions, expected.Credentials), nil
}
