//go:build linux || darwin

package sessionprovider

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	contracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
)

// The existing activity journal owns this send, not operator delivery's journal.
func (e sessionChannelExecutor) ExecuteChannelActivity(ctx context.Context, operation, toolID string, tool contracts.ToolSchemaEntry, input map[string]any) (any, error) {
	if e.owner == nil {
		return nil, errRuntimeConnection
	}
	permit, err := pipeline.ConsumeNativeChannelActivity(ctx, e.owner.operation.OperationID, e.owner.operation.SessionAccount, tool, input)
	if err != nil {
		return nil, err
	}
	if err := permit.ValidateConnector(ctx, operation, toolID, tool); err != nil {
		return nil, err
	}
	values, object := permit.Input().ObjectMap()
	destination, destinationOK := values["destination"].String()
	text, textOK := values["text"].String()
	expectedFields := 2
	accountValue, supplied := values["account"]
	if e.owner.operation.DeploymentRegistration || supplied {
		account, valid := accountValue.String()
		ref := channelonboarding.AccountReference{Provider: "whatsapp", Account: account}
		if !valid || ref.Validate() != nil || strings.TrimPrefix(account, "+")+"@s.whatsapp.net" != e.owner.operation.SessionAccount.AccountRef ||
			e.owner.operation.DeploymentRegistration && account != e.owner.operation.AccountReference {
			return nil, errOutboundTarget
		}
		expectedFields = 3
	}
	if !object || !destinationOK || !textOK || len(values) != expectedFields {
		return nil, errOutboundTarget
	}
	to, message, err := encodeOutboundText(destination, text, nil)
	if err != nil {
		return nil, err
	}
	account, err := e.owner.AdmitSessionAccount(ctx, e.owner.operation.SessionAccount)
	if err != nil {
		return nil, err
	}
	defer account.Close()
	occurrence := e.owner.state.currentOccurrence()
	if err := permit.Validate(ctx); err != nil {
		return nil, err
	}
	if err := account.Validate(ctx, e.owner.operation.SessionAccount); err != nil {
		return nil, err
	}
	if !e.owner.state.ownsConnectedOccurrence(ctx, occurrence) {
		return nil, errClientOccurrenceFenced
	}
	if err := e.requireNativeActivityBinding(ctx, permit); err != nil {
		return nil, err
	}
	if err := account.Validate(ctx, e.owner.operation.SessionAccount); err != nil {
		return nil, err
	}
	if err := permit.Validate(ctx); err != nil {
		return nil, err
	}
	response, err := occurrence.send(ctx, to, message, permit.MessageID())
	if err != nil {
		return nil, failures.Wrap(failures.ClassOutcomeUncertain, "native_activity_send_uncertain", "activity-runtime", "dispatch_native_channel", nil, err)
	}
	if string(response.ID) != permit.MessageID() || response.Timestamp.IsZero() {
		return nil, failures.New(failures.ClassOutcomeUncertain, "native_activity_acknowledgment_invalid", "activity-runtime", "dispatch_native_channel", nil)
	}
	output := map[string]any{"id": string(response.ID)}
	if err := tool.OutputSchema().Validate(output); err != nil {
		return nil, failures.Wrap(failures.ClassOutcomeUncertain, "native_activity_output_invalid", "activity-runtime", "dispatch_native_channel", nil, err)
	}
	return output, nil
}

func (e sessionChannelExecutor) requireNativeActivityBinding(ctx context.Context, permit pipeline.NativeChannelActivityPermit) error {
	op, err := e.owner.operations.GetChannelOnboarding(ctx, e.owner.operation.OperationID)
	if err != nil {
		return err
	}
	if op.DeploymentRegistration {
		return permit.ValidateBusinessTarget(ctx, op, channelonboarding.ConnectedChannelActivation{}, operatorchannel.Binding{})
	}
	activation, err := e.owner.store.GetConnectedChannelActivation(ctx, op.SlotKey)
	if err != nil {
		return err
	}
	if op.AccountConnection() {
		return permit.ValidateBusinessTarget(ctx, op, activation, operatorchannel.Binding{})
	}
	bindings, ok := e.owner.store.(interface {
		ListOperatorChannelBindings(context.Context, string) ([]operatorchannel.Binding, error)
	})
	if !ok {
		return fmt.Errorf("native activity requires its selected operator binding owner")
	}
	rows, err := bindings.ListOperatorChannelBindings(ctx, op.PrincipalID)
	if err != nil {
		return err
	}
	matched := 0
	for _, binding := range rows {
		if permit.ValidateBusinessTarget(ctx, op, activation, binding) == nil {
			matched++
		}
	}
	if matched != 1 || ctx.Err() != nil {
		return errCaptureScopeChanged
	}
	return nil
}
