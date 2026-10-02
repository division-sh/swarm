package serveapp

import (
	"context"
	"fmt"

	runtimeagentcontrol "github.com/division-sh/swarm/internal/runtime/agentcontrol"
)

type processDynamicAgentControl struct {
	supervisor *processLifecycleSupervisor
}

func (c processDynamicAgentControl) Restart(ctx context.Context, req runtimeagentcontrol.RestartRequest) (runtimeagentcontrol.RestartResult, error) {
	use, err := c.supervisor.acquireCurrentRuntime(ctx)
	if err != nil {
		return runtimeagentcontrol.RestartResult{}, err
	}
	defer func() { _ = use.Done() }()
	rt := use.Runtime()
	if rt == nil || rt.Manager == nil {
		return runtimeagentcontrol.RestartResult{}, fmt.Errorf("runtime manager unavailable")
	}
	return rt.Manager.Restart(use.WorkContext(), req)
}

func (c processDynamicAgentControl) SendDirective(ctx context.Context, req runtimeagentcontrol.SendDirectiveRequest) (runtimeagentcontrol.SendDirectiveResult, error) {
	use, err := c.supervisor.acquireCurrentRuntime(ctx)
	if err != nil {
		return runtimeagentcontrol.SendDirectiveResult{}, err
	}
	defer func() { _ = use.Done() }()
	rt := use.Runtime()
	if rt == nil || rt.Manager == nil {
		return runtimeagentcontrol.SendDirectiveResult{}, fmt.Errorf("runtime manager unavailable")
	}
	return rt.Manager.SendDirective(use.WorkContext(), req)
}
