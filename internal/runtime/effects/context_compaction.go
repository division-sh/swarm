package effects

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
)

const ContextCompactionPurpose = "context_compaction"

// BeginContextCompaction retains the admitted turn's exact frame, capability,
// process and session grant. Only its model-maintenance purpose and logical
// operation differ; it cannot produce a business-turn completion.
func BeginContextCompaction(ctx context.Context, adapter, operationID string, frame agentframe.Frame, payload []byte) (*Handle, error) {
	authority, ok := completionAuthorityFromContext(ctx)
	controller, controlled := ControllerFromContext(ctx)
	request, requested := ctx.Value(completionRequestContextKey{}).(CompletionRequest)
	if !ok || !controlled || !requested || !authority.Target.Memory.Enabled || !authority.Target.Valid() {
		return nil, fmt.Errorf("context compaction requires the exact admitted memory turn")
	}
	if err := admitExecutionMode(ctx, adapter); err != nil {
		return nil, err
	}
	if _, different := DifferentOwnerFromContext(ctx); different || controller.completionHeartbeatStore == nil {
		return nil, fmt.Errorf("context compaction requires the managed completion owner and heartbeat")
	}
	request.Purpose = ContextCompactionPurpose
	authority.CompletionRequest = request
	ctx = WithAuthority(WithCompletionRequest(ctx, request), authority)
	surface, found := managedcapabilities.FromContext(ctx)
	if !found {
		return nil, fmt.Errorf("context compaction requires its admitted capability surface")
	}
	grant := sessionGrantFromContext(ctx)
	attempt, err := controller.Authorize(ctx, AuthorizeRequest{
		OperationID: operationID, Adapter: adapter, RequestFingerprint: Fingerprint(payload),
		CapabilitySurface: &surface, AgentFrame: &frame, CompletionRequest: request,
		SessionGrantID: grant.GrantID, SessionLockOwner: grant.LockOwner,
	})
	return authorizedHandle(controller, attempt, err)
}
