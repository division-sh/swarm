package bus

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeflowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
)

type FlowInstanceRouteMaterializationRequest struct {
	Template            runtimecontracts.SystemNodeContract
	Identity            runtimeflowidentity.Route
	ActivationVariables map[string]string
}

func (r FlowInstanceRouteMaterializationRequest) Normalized() FlowInstanceRouteMaterializationRequest {
	r.Identity = runtimeflowidentity.StoredRoute(r.Identity.ScopeKey, r.Identity.InstanceID, r.Identity.InstancePath)
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

func flowInstanceRouteMaterializationVars(req FlowInstanceRouteMaterializationRequest, templateFlowID string) map[string]string {
	identity := req.Identity
	out := cloneRouteActivationVariables(req.ActivationVariables)
	if out == nil {
		out = map[string]string{}
	}
	setRouteBuiltin(out, "flow_instance_path", identity.InstancePath)
	setRouteBuiltin(out, "instance_id", identity.InstanceID)
	setRouteBuiltin(out, "template_id", templateFlowID)
	setRouteBuiltin(out, "flow_scope_key", identity.ScopeKey)
	return out
}

func setRouteBuiltin(vars map[string]string, key, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}
	vars[key] = value
}
