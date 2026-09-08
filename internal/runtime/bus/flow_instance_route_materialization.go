package bus

import (
	"strings"

	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

type FlowInstanceRouteMaterializationRequest struct {
	Identity            runtimeflowidentity.RunScopedFlowInstance
	ActivationVariables map[string]string
}

func (r FlowInstanceRouteMaterializationRequest) Normalized() FlowInstanceRouteMaterializationRequest {
	r.Identity = r.Identity.Normalize()
	r.ActivationVariables = cloneRouteActivationVariables(r.ActivationVariables)
	return r
}

func cloneRouteActivationVariables(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = value
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
