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
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/registration"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

type sessionChannelExecutor struct {
	owner *sessionAuthorityOwner
	plan  packs.SatisfactionPlan
}

func (e sessionChannelExecutor) DeliverChannelConfirmation(ctx context.Context, operation string, input map[string]any, lineage map[string]string) (registration.DeliveryResult, error) {
	return e.deliver(ctx, operation, input, lineage, runtimeeffects.AuthorityChannelConfirmation)
}

func (e sessionChannelExecutor) DeliverChannelMessage(ctx context.Context, operation string, input map[string]any, lineage map[string]string) (registration.DeliveryResult, error) {
	return e.deliver(ctx, operation, input, lineage, runtimeeffects.AuthorityChannelDelivery)
}

func (e sessionChannelExecutor) deliver(ctx context.Context, operation string, input map[string]any, lineage map[string]string, kind runtimeeffects.AuthorityKind) (registration.DeliveryResult, error) {
	toolID, tool, err := e.plan.ConnectorOperation(operation)
	if err != nil {
		return registration.DeliveryResult{}, err
	}
	target, native := tool.InProcess()
	if !native || target != runtimecontracts.ToolInProcessWhatsAppSendText || e.owner == nil {
		return registration.DeliveryResult{}, fmt.Errorf("native channel write requires its owned WhatsApp send target")
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
	destination, destinationOK := input["destination"].(string)
	text, textOK := input["text"].(string)
	if !destinationOK || !textOK || len(input) != 2 {
		return registration.DeliveryResult{}, errOutboundTarget
	}
	to, message, err := encodeOutboundText(destination, text, nil)
	if err != nil {
		return registration.DeliveryResult{}, err
	}
	selected, ok := runtimeeffects.AuthorityFromContext(ctx)
	if !ok || selected.Kind != kind || !selected.Valid() {
		return registration.DeliveryResult{}, fmt.Errorf("native channel write requires its exact selected effect authority")
	}
	occurrence := e.owner.state.currentOccurrence()
	account, err := e.owner.AdmitSessionAccount(ctx, e.owner.operation.SessionAccount)
	if err != nil {
		return registration.DeliveryResult{}, err
	}
	defer account.Close()
	preflight := func(ctx context.Context) error {
		if err := account.Validate(ctx, e.owner.operation.SessionAccount); err != nil {
			return err
		}
		if !e.owner.state.ownsConnectedOccurrence(ctx, occurrence) {
			return errClientOccurrenceFenced
		}
		return e.requireChannelDestination(ctx, selected, destination)
	}
	if err := preflight(ctx); err != nil {
		return registration.DeliveryResult{}, err
	}
	var handle *runtimeeffects.Handle
	if kind == runtimeeffects.AuthorityChannelConfirmation {
		handle, err = runtimeeffects.BeginInProcessChannelConfirmation(ctx, target, fingerprint, lineage)
	} else {
		handle, err = runtimeeffects.BeginInProcessChannelDelivery(ctx, target, fingerprint, lineage)
	}
	if err != nil {
		return registration.DeliveryResult{}, err
	}
	// The journal, not the SDK's retry cache, owns this exact first send.
	id := strings.ReplaceAll(handle.Attempt().OperationID, "-", "")
	response, launched, launchErr, err := executeChannelSend(ctx, occurrence, preflight, handle, to, message, id)
	if err != nil {
		return registration.FailChannelWrite(ctx, handle, toolID, launched, launchErr, err)
	}
	output := map[string]any{"id": string(response.ID)}
	raw, err := canonicaljson.Bytes(output)
	if err == nil && (response.ID != id || response.Timestamp.IsZero()) {
		err = fmt.Errorf("native send acknowledgment contradicts the launched message")
	}
	return registration.CompleteChannelWrite(ctx, handle, toolID, tool, output,
		map[string]any{"provider": "whatsapp", "message_id": string(response.ID)}, raw, launchErr, err)
}

func (e sessionChannelExecutor) requireChannelDestination(ctx context.Context, selected runtimeeffects.Authority, destination string) error {
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
		Provider: "whatsapp", SessionAccount: e.owner.operation.SessionAccount}
	if !expected.MatchesActivation(op, activation) || destination != activation.ConversationRef {
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
			ContextPublicationGeneration: a.ContextPublicationGeneration, PlanGeneration: a.PlanGeneration, TargetGeneration: a.TargetGeneration}
	case runtimeeffects.AuthorityChannelDelivery:
		a := selected.ChannelDelivery
		if a.InterfaceKey != op.Interface.Key() || a.ConversationRef != destination {
			return errCaptureScopeChanged
		}
		principal, bindingRevision, activationID, activationRevision = a.PrincipalID, a.BindingRevision, a.ActivationID, a.ActivationRevision
		coordinate = channelonboarding.ChannelRuntimeContextCoordinate{BundleHash: a.BundleHash, BundleIdentity: a.BundleIdentity,
			PackInventoryGeneration: a.PackInventoryGeneration, RuntimeInstanceID: a.RuntimeInstanceID,
			ContextPublicationGeneration: a.ContextPublicationGeneration, PlanGeneration: a.PlanGeneration, TargetGeneration: a.TargetGeneration}
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
		if expected.MatchesBusinessBinding(op, activation, binding, bindingRevision) {
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
