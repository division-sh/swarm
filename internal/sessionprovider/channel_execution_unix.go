//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/registration"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/sessionprovider/execution"
	"github.com/division-sh/swarm/internal/sessionprovider/internal/executionfact"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

type sessionChannelExecutor struct {
	owner      *sessionAuthorityOwner
	state      retainedChannelWriteState
	plan       packs.SatisfactionPlan
	deployment func(context.Context, channelonboarding.Operation) (channelonboarding.Result, error)
}

type retainedChannelWriteState interface {
	retainedSessionAccountReader
	prepareChannelSend(map[string]any) (preparedChannelSend, error)
	prepareChannelActivity(channelonboarding.Operation, semanticvalue.Value) (preparedChannelSend, error)
	channelWriteTargetError() error
}

type preparedChannelSend interface {
	destination() string
	current(context.Context) bool
	send(context.Context, func(context.Context) error, *runtimeeffects.Handle, string) (channelSendReceipt, bool, error, error)
	sendActivity(context.Context, string) (channelSendReceipt, error)
}

type channelSendReceipt struct {
	output, evidence map[string]any
	validationErr    error
}

func (e sessionChannelExecutor) channelExecution() execution.Channel {
	if !e.ownsSessionState() {
		return execution.Channel{}
	}
	return executionfact.SealOwnedChannel(e.owner.operation.OperationID, e)
}

func (e sessionChannelExecutor) ownsSessionState() bool {
	return e.owner != nil && e.state != nil && e.owner.state == e.state
}

func (e sessionChannelExecutor) ExecuteChannelWrite(ctx context.Context, operation, toolID string, tool runtimecontracts.ToolSchemaEntry, input map[string]any, lineage map[string]string, kind runtimeeffects.AuthorityKind) (registration.DeliveryResult, error) {
	if kind != runtimeeffects.AuthorityChannelConfirmation && kind != runtimeeffects.AuthorityChannelDelivery {
		return registration.DeliveryResult{}, fmt.Errorf("native channel write has an unsupported effect authority")
	}
	ownedID, owned, err := e.plan.ConnectorOperation(operation)
	if err != nil {
		return registration.DeliveryResult{}, err
	}
	ownedHash, err := owned.CanonicalHash()
	if err != nil {
		return registration.DeliveryResult{}, err
	}
	selectedHash, err := tool.CanonicalHash()
	if err != nil || ownedID != toolID || ownedHash != selectedHash {
		return registration.DeliveryResult{}, fmt.Errorf("native channel write contradicts its compiled operation")
	}
	return e.deliver(ctx, operation, input, lineage, kind)
}

func (e sessionChannelExecutor) DeliverChannelConfirmation(ctx context.Context, operation string, input map[string]any, lineage map[string]string) (registration.DeliveryResult, error) {
	if e.owner == nil {
		return registration.DeliveryResult{}, fmt.Errorf("native channel confirmation owner is unavailable")
	}
	id, tool, err := e.plan.ConnectorOperation(operation)
	if err != nil {
		return registration.DeliveryResult{}, err
	}
	return e.channelExecution().DeliverChannelConfirmation(ctx, e.owner.operation.OperationID, operation, id, tool, input, lineage)
}

func (e sessionChannelExecutor) DeliverChannelMessage(ctx context.Context, operation string, input map[string]any, lineage map[string]string) (registration.DeliveryResult, error) {
	if e.owner == nil {
		return registration.DeliveryResult{}, fmt.Errorf("native channel delivery owner is unavailable")
	}
	id, tool, err := e.plan.ConnectorOperation(operation)
	if err != nil {
		return registration.DeliveryResult{}, err
	}
	return e.channelExecution().DeliverChannelMessage(ctx, e.owner.operation.OperationID, operation, id, tool, input, lineage)
}

func (e sessionChannelExecutor) deliver(ctx context.Context, operation string, input map[string]any, lineage map[string]string, kind runtimeeffects.AuthorityKind) (registration.DeliveryResult, error) {
	toolID, tool, err := e.plan.ConnectorOperation(operation)
	if err != nil {
		return registration.DeliveryResult{}, err
	}
	target, native := tool.InProcess()
	if !native || !e.plan.ProviderFacts().NativeWrite(target.String()) || !e.ownsSessionState() {
		if e.state == nil {
			return registration.DeliveryResult{}, fmt.Errorf("native channel write requires its owned WhatsApp send target")
		}
		return registration.DeliveryResult{}, e.state.channelWriteTargetError()
	}
	if kind == runtimeeffects.AuthorityChannelDelivery {
		if _, compiled := tool.CompiledResultExecution(); !compiled {
			return registration.DeliveryResult{}, fmt.Errorf("native channel delivery requires the compiled result projection")
		}
	}
	fingerprint, err := registration.ChannelWriteFingerprint(toolID, tool, input)
	if err != nil {
		return registration.DeliveryResult{}, err
	}
	prepared, err := e.state.prepareChannelSend(input)
	if err != nil {
		return registration.DeliveryResult{}, err
	}
	selected, ok := runtimeeffects.AuthorityFromContext(ctx)
	if !ok || selected.Kind != kind || !selected.Valid() {
		return registration.DeliveryResult{}, fmt.Errorf("native channel write requires its exact selected effect authority")
	}
	account, err := e.owner.AdmitSessionAccount(ctx, e.owner.operation.SessionAccount)
	if err != nil {
		return registration.DeliveryResult{}, err
	}
	defer account.Close()
	preflight := func(ctx context.Context) error {
		if err := account.Validate(ctx, e.owner.operation.SessionAccount); err != nil {
			return err
		}
		if !prepared.current(ctx) {
			return errClientOccurrenceFenced
		}
		return e.requireChannelDestination(ctx, selected, prepared.destination())
	}
	if err := preflight(ctx); err != nil {
		return registration.DeliveryResult{}, err
	}
	var handle *runtimeeffects.Handle
	if kind == runtimeeffects.AuthorityChannelConfirmation {
		handle, err = runtimeeffects.BeginInProcessChannelConfirmation(ctx, fingerprint, lineage)
	} else {
		handle, err = runtimeeffects.BeginInProcessChannelDelivery(ctx, fingerprint, lineage)
	}
	if err != nil {
		return registration.DeliveryResult{}, err
	}
	// The journal, not the SDK's retry cache, owns this exact first send.
	id := strings.ReplaceAll(handle.Attempt().OperationID, "-", "")
	receipt, launched, launchErr, err := prepared.send(ctx, preflight, handle, id)
	if err != nil {
		return registration.FailChannelWrite(ctx, handle, toolID, launched, launchErr, err)
	}
	raw, err := canonicaljson.Bytes(receipt.output)
	if err == nil {
		err = receipt.validationErr
	}
	return registration.CompleteChannelWrite(ctx, handle, toolID, tool, receipt.output, receipt.evidence, raw, launchErr, err)
}

func (e sessionChannelExecutor) requireChannelDestination(ctx context.Context, selected runtimeeffects.Authority, destination string) error {
	if e.owner.operation.DeploymentRegistration {
		return e.requireDeploymentDestination(ctx, selected, destination)
	}
	op, err := e.owner.store.GetChannelOnboarding(ctx, e.owner.operation.OperationID)
	if err != nil {
		return err
	}
	generation, err := e.plan.Generation()
	if err != nil || !generation.Equal(op.Coordinate.PlanGeneration) || e.plan.Provider() != op.Provider {
		return errCaptureScopeChanged
	}
	activation, err := e.owner.store.GetConnectedChannelActivation(ctx, op.SlotKey)
	if err != nil {
		return err
	}
	expected := channelonboarding.AdmissionResponsibility{OperationID: e.owner.operation.OperationID,
		OperationRevision: activation.OperationRevision, ActivationRevision: activation.Revision,
		Coordinate: e.owner.operation.Coordinate, TargetSelector: e.owner.operation.TargetSelector,
		Provider: e.owner.operation.Provider, SessionAccount: e.owner.operation.SessionAccount}
	if !expected.MatchesActivation(op, activation, op.ProviderFacts()) || destination != activation.ConversationRef {
		return errCaptureScopeChanged
	}
	principal, bindingRevision := "", int64(0)
	var coordinate channelonboarding.ChannelRuntimeContextCoordinate
	activationID, activationRevision := "", int64(0)
	switch selected.Kind {
	case runtimeeffects.AuthorityChannelConfirmation:
		a := selected.ChannelConfirmation
		if a.OnboardingOperationID != op.OperationID || a.OnboardingRevision != op.Revision ||
			a.EffectOperationID != op.ConfirmationOperationID || op.Phase != channelonboarding.PhaseDeliveringConfirmation {
			return errCaptureScopeChanged
		}
		principal, bindingRevision, activationID, activationRevision = a.PrincipalID, a.BindingRevision, a.ActivationID, a.ActivationRevision
		coordinate = channelonboarding.ChannelRuntimeContextCoordinate{BundleHash: a.BundleHash, BundleIdentity: a.BundleIdentity,
			PackInventoryGeneration: a.PackInventoryGeneration, RuntimeInstanceID: a.RuntimeInstanceID,
			ContextPublicationGeneration: a.ContextPublicationGeneration, PlanGeneration: a.PlanGeneration}
	case runtimeeffects.AuthorityChannelDelivery:
		a := selected.ChannelDelivery
		if a.InterfaceKey != op.Interface.Key() || a.ConversationRef != destination {
			return errCaptureScopeChanged
		}
		principal, bindingRevision, activationID, activationRevision = a.PrincipalID, a.BindingRevision, a.ActivationID, a.ActivationRevision
		coordinate = channelonboarding.ChannelRuntimeContextCoordinate{BundleHash: a.BundleHash, BundleIdentity: a.BundleIdentity,
			PackInventoryGeneration: a.PackInventoryGeneration, RuntimeInstanceID: a.RuntimeInstanceID,
			ContextPublicationGeneration: a.ContextPublicationGeneration, PlanGeneration: a.PlanGeneration}
	default:
		return errCaptureScopeChanged
	}
	if !coordinate.Matches(op.Coordinate) || principal != op.PrincipalID || activationID != activation.ActivationID || activationRevision != activation.Revision {
		return errCaptureScopeChanged
	}
	bindings, ok := e.owner.store.(interface {
		ListOperatorChannelBindings(context.Context, string) ([]operatorchannel.Binding, error)
	})
	if !ok {
		return fmt.Errorf("native channel destination requires the selected operator binding owner")
	}
	rows, err := bindings.ListOperatorChannelBindings(ctx, principal)
	if err != nil {
		return err
	}
	matched := 0
	for _, binding := range rows {
		if expected.MatchesBusinessBinding(op, activation, binding, bindingRevision, op.ProviderFacts()) {
			if selected.Kind == runtimeeffects.AuthorityChannelDelivery && selected.ChannelDelivery.ExternalAccountRef != binding.ExternalAccountRef {
				return errCaptureScopeChanged
			}
			matched++
		}
	}
	if matched != 1 || ctx.Err() != nil {
		return errCaptureScopeChanged
	}
	return nil
}

func (e sessionChannelExecutor) requireDeploymentDestination(ctx context.Context, selected runtimeeffects.Authority, destination string) error {
	if selected.Kind != runtimeeffects.AuthorityChannelDelivery || e.deployment == nil {
		return errCaptureScopeChanged
	}
	a := selected.ChannelDelivery
	parent := a.Parent
	op, err := e.owner.operations.GetChannelOnboarding(ctx, e.owner.operation.OperationID)
	if err != nil {
		return err
	}
	generation, err := e.plan.Generation()
	if err != nil || parent.Kind != channelnative.TransportDeploymentRegistration || parent.Validate() != nil ||
		op.OperationID != parent.ID || op.Revision != parent.Revision || op.PrincipalID != a.PrincipalID ||
		op.Phase != channelonboarding.PhaseSucceeded || !op.AccountConfirmed || op.LogoutCompleted || op.FailureCode != "" ||
		op.IdentityOperationID != parent.BindingOperationID || op.BindingRevision != a.BindingRevision ||
		op.SessionAccount != e.owner.operation.SessionAccount || a.InterfaceKey != op.Interface.Key() ||
		a.ConversationRef != destination || generation.Diagnostic() != op.Interface.SemanticGeneration {
		return errCaptureScopeChanged
	}
	current, binding, err := readDeploymentOperator(ctx, op, e.deployment)
	if err != nil {
		return err
	}
	if current != parent || binding.ExternalAccountRef != a.ExternalAccountRef || binding.ConversationRef != destination || ctx.Err() != nil {
		return errCaptureScopeChanged
	}
	return nil
}

type whatsAppChannelSend struct {
	state      *sessionState
	occurrence *clientOccurrence
	to         types.JID
	message    *waE2E.Message
	account    string
}

func (*sessionState) channelWriteTargetError() error {
	return fmt.Errorf("native channel write requires its owned WhatsApp send target")
}

func (s *sessionState) prepareChannelSend(input map[string]any) (preparedChannelSend, error) {
	destination, destinationOK := input["destination"].(string)
	text, textOK := input["text"].(string)
	if !destinationOK || !textOK || len(input) != 2 {
		return nil, errOutboundTarget
	}
	to, message, err := encodeOutboundText(destination, text, nil)
	if err != nil {
		return nil, err
	}
	return whatsAppChannelSend{state: s, occurrence: s.currentOccurrence(), to: to, message: message, account: destination}, nil
}

func (s whatsAppChannelSend) destination() string { return s.account }
func (s whatsAppChannelSend) current(ctx context.Context) bool {
	return s.state.ownsConnectedOccurrence(ctx, s.occurrence)
}
func (s whatsAppChannelSend) send(ctx context.Context, preflight func(context.Context) error, handle *runtimeeffects.Handle, id string) (channelSendReceipt, bool, error, error) {
	response, launched, launchErr, err := executeChannelSend(ctx, s.occurrence, preflight, handle, s.to, s.message, id)
	return whatsAppSendReceipt(response, id), launched, launchErr, err
}

func (s whatsAppChannelSend) sendActivity(ctx context.Context, id string) (channelSendReceipt, error) {
	response, err := s.occurrence.send(ctx, s.to, s.message, id)
	return whatsAppSendReceipt(response, id), err
}

func whatsAppSendReceipt(response whatsmeow.SendResponse, id string) channelSendReceipt {
	var invalid error
	if response.ID != id || response.Timestamp.IsZero() {
		invalid = fmt.Errorf("native send acknowledgment contradicts the launched message")
	}
	return channelSendReceipt{output: map[string]any{"id": string(response.ID)}, evidence: map[string]any{"provider": "whatsapp", "message_id": string(response.ID)}, validationErr: invalid}
}

func executeChannelSend(ctx context.Context, occurrence *clientOccurrence, preflight func(context.Context) error, handle *runtimeeffects.Handle,
	to types.JID, message *waE2E.Message, id types.MessageID,
) (whatsmeow.SendResponse, bool, error, error) {
	if err := preflight(ctx); err != nil {
		return whatsmeow.SendResponse{}, false, nil, err
	}
	launchErr := handle.MarkLaunched(ctx)
	if launchErr != nil && !runtimeeffects.CommittedMutationPhase(launchErr, runtimeeffects.MutationLaunch, handle.Attempt()) {
		return whatsmeow.SendResponse{}, false, nil, launchErr
	}
	current, err := runtimeeffects.ProjectionCurrent(ctx)
	if err != nil || !current {
		if err == nil {
			err = errCaptureScopeChanged
		}
		return whatsmeow.SendResponse{}, false, errors.Join(launchErr, err), err
	}
	if err := preflight(ctx); err != nil {
		return whatsmeow.SendResponse{}, false, errors.Join(launchErr, err), err
	}
	response, err := occurrence.send(ctx, to, message, id)
	return response, true, launchErr, err
}
