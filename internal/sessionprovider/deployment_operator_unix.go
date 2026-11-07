//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"fmt"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"go.mau.fi/whatsmeow/types/events"
)

type deploymentOperatorStore interface {
	AdmitDeploymentChannelInput(context.Context, render.DeploymentInput) (bool, error)
	LoadDeploymentChannelInputReceipt(context.Context, string, string) (render.DeploymentInputReceipt, bool, error)
}

func (i *runtimeIncoming) deploymentOperatorBinding(ctx context.Context, op channelonboarding.Operation) (channelnative.TransportParent, operatorchannel.Binding, bool, error) {
	var absent channelnative.TransportParent
	if !op.DeploymentRegistration || op.Phase != channelonboarding.PhaseSucceeded || !op.AccountConfirmed || op.LogoutCompleted ||
		op.IdentityOperationID == "" || op.FailureCode != "" || i.connection.operatorAccount != (channelonboarding.AccountReference{Provider: op.Provider, Account: op.AccountReference}) {
		return absent, operatorchannel.Binding{}, false, nil
	}
	parent, binding, err := readDeploymentOperator(ctx, op, i.connection.operatorBinding)
	return parent, binding, err == nil, err
}

func readDeploymentOperator(ctx context.Context, op channelonboarding.Operation, read func(context.Context, channelonboarding.Operation) (channelonboarding.Result, error)) (channelnative.TransportParent, operatorchannel.Binding, error) {
	if read == nil {
		return channelnative.TransportParent{}, operatorchannel.Binding{}, errCaptureScopeChanged
	}
	selected, err := read(ctx, op)
	if err != nil {
		return channelnative.TransportParent{}, operatorchannel.Binding{}, err
	}
	b, child := selected.Binding, selected.IdentityOperation
	if selected.Operation.OperationID != op.OperationID || selected.Operation.Revision != op.Revision || b == nil || child == nil ||
		b.OperationID != op.IdentityOperationID || b.Revision != op.BindingRevision || b.Status != operatorchannel.BindingCurrent ||
		child.OperationID != b.OperationID || child.State != operatorchannel.StateBound || child.OnboardingOperationID != op.OperationID ||
		b.PrincipalID != op.PrincipalID || b.Interface.Normalized() != op.Interface.Normalized() {
		return channelnative.TransportParent{}, operatorchannel.Binding{}, errCaptureScopeChanged
	}
	parent := channelnative.TransportParent{Kind: channelnative.TransportDeploymentRegistration, ID: op.OperationID, Revision: op.Revision,
		BindingOperationID: child.OperationID, BindingOperationRevision: child.Revision, ProofID: b.ProofID, ProofRevision: b.ProofRevision}
	return parent, *b, parent.Validate()
}

func (i *runtimeIncoming) completedOperatorConversation(ctx context.Context, op channelonboarding.Operation, message *events.Message) (bool, bool, error) {
	_, b, found, err := i.deploymentOperatorBinding(ctx, op)
	if err != nil || !found {
		return false, false, err
	}
	conversation, _, err := operatorchannel.OpaqueReference(message.Info.Chat.ToNonAD().String())
	if err != nil || conversation != b.ConversationRef {
		return false, false, err
	}
	sender, _, err := operatorchannel.OpaqueReference(message.Info.Sender.ToNonAD().String())
	return true, sender == b.ExternalAccountRef, err
}

func (i *runtimeIncoming) settleDeploymentOperator(ctx context.Context, event capturedEvent) (bool, error) {
	store, ok := i.store.(deploymentOperatorStore)
	if !ok {
		return false, fmt.Errorf("deployment input requires its selected intent owner")
	}
	settled, err := i.connection.captures.owner.ReconcileDeploymentOperator(ctx, event, store)
	if err != nil || settled {
		return settled, err
	}
	op, err := i.connection.currentOperation(ctx)
	if err != nil {
		return false, err
	}
	if event.Scope.OperationRevision != op.Revision || event.Scope.OnboardingOperation != op.OperationID {
		return false, nil
	}
	parent, binding, found, err := i.deploymentOperatorBinding(ctx, op)
	if err != nil || !found {
		return false, err
	}
	admitted, err := i.handoff.input.recoverClaim(ctx, SessionInputReference{ConnectionID: event.Scope.Session.ConnectionID,
		OccurrenceID: event.OccurrenceID, Conversation: event.Conversation, EventID: event.EventID, Kind: event.Kind})
	if err != nil {
		return false, err
	}
	defer admitted.Close()
	request, err := i.handoff.trigger.AdmitSessionInput(ctx, admitted)
	if err != nil {
		return false, err
	}
	consumed, err := store.AdmitDeploymentChannelInput(ctx, render.DeploymentInput{Parent: parent, Account: i.connection.operatorAccount,
		PrincipalID: binding.PrincipalID, BindingRevision: binding.Revision, Session: op.SessionAccount,
		Channel: i.connection.plan, Trigger: i.handoff.trigger, Input: request, ReceivedAt: admitted.ReceivedAt()})
	if err != nil || !consumed {
		return false, errors.Join(err, errCapturePublicationPending)
	}
	settled, err = i.connection.captures.owner.ReconcileDeploymentOperator(ctx, event, store)
	if !settled && err == nil {
		err = errCapturePublicationPending
	}
	return settled, err
}
