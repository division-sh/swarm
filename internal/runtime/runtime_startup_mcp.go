package runtime

import (
	"net/http"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

// AcquireStartupMCPRequest admits only the current grant's preflight transport.
// It does not make a prepared runtime selectable for application execution.
func (rt *Runtime) AcquireStartupMCPRequest(r *http.Request) (http.Handler, *worklifetime.Lease, error) {
	if rt == nil || r == nil || rt.ToolGateway == nil || rt.workOccurrence == nil {
		return nil, nil, nil
	}
	if err := r.Context().Err(); err != nil {
		return nil, nil, err
	}
	authority, ok := rt.ToolGateway.StartupProbeRequestAuthority(r)
	if !ok {
		return nil, nil, nil
	}
	grant, err := rt.CurrentStartupGrantEvidence()
	if err != nil || grant.State != startupownership.GrantPrepared ||
		authority.StartupProbe.StartupAuthorityID != grant.GrantID ||
		authority.StartupProbe.StartupStateVersion != grant.StateVersion ||
		authority.ExecutionOwner != grant.ProcessOwnerID || authority.FenceGeneration != grant.RuntimeGeneration {
		return nil, nil, nil
	}
	lease, err := rt.workOccurrence.Begin(r.Context())
	if err != nil {
		return nil, nil, err
	}
	return rt.ToolGateway.Handler(), lease, nil
}
