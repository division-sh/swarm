package runforkexecution

import (
	"context"
	"errors"

	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

type selectedFlowRoutePublisher interface {
	StageFlowInstanceRouteContext(context.Context, runtimebus.FlowInstanceRouteMaterializationRequest) (runtimebus.FlowInstanceRouteTopologyResult, error)
	PublishPersistedFlowInstanceRoute(runtimebus.FlowInstanceRouteMaterializationRequest) error
	VerifyFlowInstanceRoute(context.Context, runtimeflowidentity.RunScopedFlowInstance) error
}

func publishSelectedContractFlowRoute(ctx context.Context, bus selectedFlowRoutePublisher, route runtimebus.FlowInstanceRouteMaterializationRequest, diagnostics *selectedForkCommitDiagnostics, published *[]runtimeflowidentity.RunScopedFlowInstance) error {
	staged, stageErr := bus.StageFlowInstanceRouteContext(ctx, route)
	if !staged.Acknowledged {
		return errors.Join(stageErr, errors.New("selected-contract flow route stage was not acknowledged"))
	}
	if stageErr != nil {
		if diagnostics == nil {
			return stageErr
		}
		diagnostics.add(stageErr)
	}
	*published = append(*published, route.Identity)
	if err := bus.PublishPersistedFlowInstanceRoute(route); err != nil {
		return err
	}
	return bus.VerifyFlowInstanceRoute(ctx, route.Identity)
}
