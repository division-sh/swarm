package channelonboarding

import (
	"context"
	"errors"
	"slices"

	"github.com/division-sh/swarm/internal/operatorchannel"
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
	SessionAccount     operatorchannel.SessionAccountAdmission
}

// RetainedSessionCurrent admits restoration for a completed native operation's
// own current slot. Historical/superseded slots grant no reconnect instruction.
func RetainedSessionCurrent(ctx context.Context, store Store, op Operation) (bool, error) {
	activation, err := store.GetConnectedChannelActivation(ctx, op.SlotKey)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if activation.OperationID != op.OperationID || activation.Status != ActivationCurrent {
		return false, nil
	}
	expected := AdmissionResponsibility{OperationID: op.OperationID, OperationRevision: activation.OperationRevision,
		ActivationRevision: op.ActivationRevision, Coordinate: op.Coordinate, TargetSelector: op.TargetSelector,
		Provider: op.Provider, Credentials: op.CredentialAdmissions, SessionAccount: op.SessionAccount}
	if activation.ValidateSessionAccount() != nil || activation.PrincipalID != op.PrincipalID ||
		activation.Interface.Normalized() != op.Interface.Normalized() || !expected.MatchesActivation(op, activation) {
		return false, ErrRevisionConflict
	}
	return true, nil
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
	if !expected.MatchesOperation(op) {
		return false, nil
	}
	if expected.ActivationRevision == 0 {
		return (op.Phase.SupportsPrebindingRegistration() || op.Phase == PhaseCredentialsAdmitted) &&
			(!exactPendingRevision || op.Revision == expected.OperationRevision), nil
	}
	activation, err := store.GetConnectedChannelActivation(ctx, op.SlotKey)
	if err != nil {
		return false, err
	}
	return expected.MatchesActivation(op, activation), nil
}

// The root-store reader and transaction-local admission use the same semantic
// projection; neither can adopt a newer responsibility from a lookup.
func (expected AdmissionResponsibility) MatchesOperation(op Operation) bool {
	coordinateCurrent := op.Coordinate.Matches(expected.Coordinate)
	if op.Posture == ActivationSessionConnection && expected.ActivationRevision == 0 && !op.Phase.RequiresExecutableTarget(op.Posture) {
		coordinateCurrent = op.Coordinate.MatchesDeclaration(expected.Coordinate)
	}
	return expected.OperationID != "" && expected.OperationRevision > 0 && op.OperationID == expected.OperationID && coordinateCurrent && op.TargetSelector == expected.TargetSelector &&
		op.Provider == expected.Provider && op.Revision >= expected.OperationRevision &&
		slices.Equal(op.CredentialAdmissions, expected.Credentials) && op.SessionAccount == expected.SessionAccount
}

func (expected AdmissionResponsibility) MatchesActivation(op Operation, activation ConnectedChannelActivation) bool {
	return expected.MatchesOperation(op) && expected.ActivationRevision > 0 && op.Phase.RequiresExecutableTarget(op.Posture) &&
		activation.Status == ActivationCurrent && activation.OperationID == expected.OperationID &&
		activation.Revision == expected.ActivationRevision && activation.OperationRevision == expected.OperationRevision &&
		activation.Coordinate.Matches(expected.Coordinate) && activation.TargetSelector == expected.TargetSelector &&
		activation.Provider == expected.Provider && activation.SessionAccount == expected.SessionAccount && slices.Equal(activation.CredentialAdmissions, expected.Credentials)
}

func (expected AdmissionResponsibility) MatchesBusinessBinding(op Operation, activation ConnectedChannelActivation, binding operatorchannel.Binding, revision int64) bool {
	return expected.MatchesActivation(op, activation) && revision > 0 && op.BindingRevision == revision && activation.BindingRevision == revision &&
		binding.Interface.Key() == op.Interface.Key() && binding.PrincipalID == op.PrincipalID && binding.Status == operatorchannel.BindingCurrent &&
		binding.Revision == revision && binding.ConversationRef == activation.ConversationRef && binding.ProofID == activation.ProofID &&
		binding.ProofRevision == activation.ProofRevision && binding.ProviderAuthority.Kind == operatorchannel.ProviderAuthoritySession &&
		binding.ProviderAuthority.Session == expected.SessionAccount
}

// ResumeSessionBusiness projects only an existing activation's process-coordinate
// refresh. Its publication operation, account, binding and target stay original.
func (expected AdmissionResponsibility) ResumeSessionBusiness(op Operation, activation ConnectedChannelActivation, bindingRevision int64, activationID string) (AdmissionResponsibility, bool) {
	if op.Posture != ActivationSessionConnection || expected.ActivationRevision < 1 ||
		bindingRevision < 1 || op.BindingRevision != bindingRevision ||
		!expected.Coordinate.MatchesDurableIdentity(op.Coordinate) ||
		expected.Coordinate.TargetGeneration != op.Coordinate.TargetGeneration ||
		activationID == "" || activation.ActivationID != activationID ||
		activation.OperationRevision < expected.OperationRevision || activation.Revision < expected.ActivationRevision {
		return AdmissionResponsibility{}, false
	}
	if expected.Coordinate.Matches(op.Coordinate) &&
		(activation.OperationRevision != expected.OperationRevision || activation.Revision != expected.ActivationRevision) {
		return AdmissionResponsibility{}, false
	}
	resumed := expected
	resumed.Coordinate = op.Coordinate
	resumed.ActivationRevision = op.ActivationRevision
	resumed.OperationRevision = activation.OperationRevision
	if !resumed.MatchesActivation(op, activation) {
		return AdmissionResponsibility{}, false
	}
	return resumed, true
}
