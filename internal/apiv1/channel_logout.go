package apiv1

import (
	"context"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
)

func operatorChannelLogoutHandler(opts OperatorChannelHandlerOptions, now func() time.Time) MethodHandler {
	return func(ctx context.Context, req Request) (any, error) {
		if err := requireOperatorPrincipal(req, opts.Channels); err != nil {
			return nil, err
		}
		operationID, err := requiredStringParam(req.Params, "operation_id")
		if err != nil {
			return nil, err
		}
		revision, err := channelRevisionParam(req.Params, "expected_revision", false)
		if err != nil {
			return nil, err
		}
		idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
		if err != nil {
			return nil, err
		}
		requestKey, requestHash := operatorchannel.RequestIdentity(req.Method, req.OperatorPrincipalID, idempotencyKey, req.RequestHash)
		return executeOperatorChannelIdempotent(ctx, req, opts, operationID, idempotencyKey, now().UTC(), func(ctx context.Context) (any, error) {
			result, err := opts.Destructive.Logout(ctx, operationID, revision, requestKey, requestHash)
			if err != nil {
				return nil, channelDestructiveError(err)
			}
			if err := result.Validate(); err != nil || result.OperationID != operationID || result.ExpectedRevision != revision || result.Teardown.PrincipalID != req.OperatorPrincipalID {
				return nil, channelDestructiveError(channelonboarding.ErrConflict)
			}
			return result, nil
		})
	}
}
