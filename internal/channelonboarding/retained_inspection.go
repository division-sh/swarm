package channelonboarding

import (
	"context"
	"fmt"
)

type RetainedActivationReader interface {
	ListChannelOnboardingOperations(context.Context) ([]Operation, error)
	ListCurrentConnectedChannelActivations(context.Context) ([]ConnectedChannelActivation, error)
}

type RetainedActivationInventory struct {
	Operations  []Operation
	Activations []ConnectedChannelActivation
}

// InspectRetainedActivations checks durable ownership only. It neither binds a
// successor runtime coordinate nor admits executable activation or credentials.
func InspectRetainedActivations(ctx context.Context, reader RetainedActivationReader) (RetainedActivationInventory, error) {
	if reader == nil {
		return RetainedActivationInventory{}, fmt.Errorf("retained channel activation reader is required")
	}
	operations, err := reader.ListChannelOnboardingOperations(ctx)
	if err != nil {
		return RetainedActivationInventory{}, err
	}
	byID := make(map[string]Operation, len(operations))
	for _, operation := range operations {
		if _, duplicate := byID[operation.OperationID]; duplicate {
			return RetainedActivationInventory{}, fmt.Errorf("%w: duplicate connected channel onboarding operation %s during context retirement reconciliation", ErrConflict, operation.OperationID)
		}
		byID[operation.OperationID] = operation
	}
	activations, err := reader.ListCurrentConnectedChannelActivations(ctx)
	if err != nil {
		return RetainedActivationInventory{}, err
	}
	for _, activation := range activations {
		operation, found := byID[activation.OperationID]
		if !found || operation.SlotKey != activation.SlotKey || !operation.Coordinate.Matches(activation.Coordinate) {
			return RetainedActivationInventory{}, fmt.Errorf("%w: current connected channel activation %s has no exact owning onboarding operation", ErrConflict, activation.ActivationID)
		}
	}
	return RetainedActivationInventory{Operations: operations, Activations: activations}, nil
}

type PendingTeardownReader interface {
	ListChannelTeardowns(context.Context) ([]TeardownOperation, error)
}

func InspectPendingTeardowns(ctx context.Context, reader PendingTeardownReader) ([]TeardownOperation, error) {
	if reader == nil {
		return nil, fmt.Errorf("channel teardown reader is required")
	}
	operations, err := reader.ListChannelTeardowns(ctx)
	if err != nil {
		return nil, err
	}
	var pending []TeardownOperation
	for _, operation := range operations {
		if operation.Phase.Terminal() {
			continue
		}
		if !operation.Kind.Valid() {
			return nil, fmt.Errorf("%w: unsupported teardown kind %q", ErrConflict, operation.Kind)
		}
		pending = append(pending, operation)
	}
	return pending, nil
}
