package executionfact

import (
	"context"
	"fmt"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/registration"
)

type channelOwner interface {
	ExecuteChannelWrite(context.Context, string, string, runtimecontracts.ToolSchemaEntry, map[string]any, map[string]string, runtimeeffects.AuthorityKind) (registration.DeliveryResult, error)
}

// Channel carries a private session owner's execution, never a caller's SDK or callback.
type Channel struct {
	operationID string
	owner       channelOwner
}

// SealOwnedChannel is accessible only within the native session subtree.
func SealOwnedChannel(operationID string, owner channelOwner) Channel {
	return Channel{operationID: operationID, owner: owner}
}

func (c Channel) DeliverChannelConfirmation(ctx context.Context, operationID, operation, toolID string, tool runtimecontracts.ToolSchemaEntry, input map[string]any, lineage map[string]string) (registration.DeliveryResult, error) {
	return c.write(ctx, operationID, operation, toolID, tool, input, lineage, runtimeeffects.AuthorityChannelConfirmation)
}

func (c Channel) DeliverChannelMessage(ctx context.Context, operationID, operation, toolID string, tool runtimecontracts.ToolSchemaEntry, input map[string]any, lineage map[string]string) (registration.DeliveryResult, error) {
	return c.write(ctx, operationID, operation, toolID, tool, input, lineage, runtimeeffects.AuthorityChannelDelivery)
}

func (c Channel) write(ctx context.Context, operationID, operation, toolID string, tool runtimecontracts.ToolSchemaEntry, input map[string]any, lineage map[string]string, kind runtimeeffects.AuthorityKind) (registration.DeliveryResult, error) {
	if ctx == nil || ctx.Err() != nil || c.owner == nil || c.operationID == "" || c.operationID != operationID {
		return registration.DeliveryResult{}, fmt.Errorf("native channel write requires its exact owned connection operation")
	}
	return c.owner.ExecuteChannelWrite(ctx, operation, toolID, tool, input, lineage, kind)
}
