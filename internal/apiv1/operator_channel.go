package apiv1

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
)

const operatorChannelIdempotencyTTL = 24 * time.Hour

type OperatorChannelHandlerOptions struct {
	Channels     *operatorchannel.Service
	Confirmation ChannelIdentityConfirmationLifecycle
	Destructive  ChannelDestructiveLifecycle
	Readback     ConnectedChannelReadbackLifecycle
	Idempotency  APIIdempotencyStore
	Now          func() time.Time
}

type ChannelIdentityConfirmationLifecycle interface {
	ConfirmIdentity(context.Context, string, int64, bool, time.Time) (operatorchannel.Operation, operatorchannel.Binding, error)
}

type ConnectedChannelReadbackLifecycle interface {
	ReadbackConnectedChannels(context.Context) ([]channelonboarding.ConnectedChannelReadback, error)
}

type ChannelDestructiveLifecycle interface {
	Unbind(context.Context, string, int64, string, string) (operatorchannel.Operation, operatorchannel.Binding, error)
	RevokeProof(context.Context, string, int64, string, string) (operatorchannel.VerifiedProof, error)
}

func OperatorChannelHandlers(opts OperatorChannelHandlerOptions) map[string]MethodHandler {
	if opts.Channels == nil || opts.Idempotency == nil {
		return nil
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	handlers := map[string]MethodHandler{}
	if opts.Confirmation != nil {
		handlers["channel.confirm"] = func(ctx context.Context, req Request) (any, error) {
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
			approveValue, present := req.Params["approve"]
			approve, valid := approveValue.(bool)
			if !present || !valid {
				return nil, NewInvalidParamsError(map[string]any{"field": "approve", "reason": "is required and must be a boolean"})
			}
			idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
			if err != nil {
				return nil, err
			}
			return executeOperatorChannelIdempotent(ctx, req, opts, operationID, idempotencyKey, now().UTC(), func(ctx context.Context) (any, error) {
				op, binding, err := opts.Confirmation.ConfirmIdentity(ctx, operationID, revision, approve, now().UTC())
				if err != nil {
					return nil, operatorChannelError(err)
				}
				return operatorChannelOperationResult(op, binding), nil
			})
		}
	}
	if opts.Destructive != nil {
		handlers["channel.unbind"] = func(ctx context.Context, req Request) (any, error) {
			if err := requireOperatorPrincipal(req, opts.Channels); err != nil {
				return nil, err
			}
			selector, err := requiredStringParam(req.Params, "interface")
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
			return executeOperatorChannelIdempotent(ctx, req, opts, selector, idempotencyKey, now().UTC(), func(ctx context.Context) (any, error) {
				op, binding, err := opts.Destructive.Unbind(ctx, selector, revision, requestKey, requestHash)
				if err != nil {
					return nil, channelDestructiveError(err)
				}
				return operatorChannelOperationResult(op, binding), nil
			})
		}
		handlers["channel.proof_revoke"] = func(ctx context.Context, req Request) (any, error) {
			if err := requireOperatorPrincipal(req, opts.Channels); err != nil {
				return nil, err
			}
			selector, err := requiredStringParam(req.Params, "interface")
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
			return executeOperatorChannelIdempotent(ctx, req, opts, selector, idempotencyKey, now().UTC(), func(ctx context.Context) (any, error) {
				proof, err := opts.Destructive.RevokeProof(ctx, selector, revision, requestKey, requestHash)
				if err != nil {
					return nil, channelDestructiveError(err)
				}
				return map[string]any{"proof": proof}, nil
			})
		}
	}
	if opts.Readback != nil {
		handlers["channel.list"] = func(ctx context.Context, req Request) (any, error) {
			if err := requireOperatorPrincipal(req, opts.Channels); err != nil {
				return nil, err
			}
			principal, err := opts.Channels.Principal()
			if err != nil {
				return nil, err
			}
			channels, err := opts.Readback.ReadbackConnectedChannels(ctx)
			if err != nil {
				return nil, operatorChannelError(err)
			}
			return map[string]any{"principal_id": principal.ID, "channels": channels}, nil
		}
	}
	return handlers
}

func operatorChannelOperationResult(operation operatorchannel.Operation, binding operatorchannel.Binding) map[string]any {
	result := map[string]any{"operation": operation}
	if binding.PrincipalID != "" {
		result["binding"] = binding
	}
	return result
}

func executeOperatorChannelIdempotent(ctx context.Context, req Request, opts OperatorChannelHandlerOptions, resourceID, idempotencyKey string, now time.Time, execute func(context.Context) (any, error)) (any, error) {
	completion, _, err := opts.Idempotency.WithAPIIdempotency(ctx, apiidempotency.Request{
		Method: req.Method, ActorTokenID: req.ActorTokenID, IdempotencyKey: idempotencyKey,
		RequestHash: req.RequestHash, ResourceID: strings.TrimSpace(resourceID), TTL: operatorChannelIdempotencyTTL, Now: now,
	}, func(ctx context.Context) (apiidempotency.Completion, error) {
		result, err := execute(ctx)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		response, err := json.Marshal(result)
		if err != nil {
			return apiidempotency.Completion{}, err
		}
		return apiidempotency.Completion{ResourceID: strings.TrimSpace(resourceID), Response: response}, nil
	})
	if err != nil {
		return nil, runStartIdempotencyError(err)
	}
	var result map[string]any
	if err := json.Unmarshal(completion.Response, &result); err != nil {
		return nil, fmt.Errorf("decode %s idempotency response: %w", req.Method, err)
	}
	return result, nil
}

func requireOperatorPrincipal(req Request, service *operatorchannel.Service) error {
	principal, err := service.Principal()
	if err != nil {
		return err
	}
	if strings.TrimSpace(req.OperatorPrincipalID) == "" || req.OperatorPrincipalID != principal.ID {
		return fmt.Errorf("authenticated API request did not resolve the selected-store operator principal")
	}
	return nil
}

func channelRevisionParam(params map[string]any, name string, allowZero bool) (int64, error) {
	minimum := 1
	if allowZero {
		minimum = 0
	}
	value, err := requiredBoundedIntegerParam(params, name, minimum, int(^uint(0)>>1))
	return int64(value), err
}

func operatorChannelError(err error) error {
	details := map[string]any{"reason": err.Error()}
	var credentialRequired *channelonboarding.CredentialRequiredError
	switch {
	case errors.As(err, &credentialRequired):
		return NewApplicationError(ChannelCredentialRequiredCode, false, map[string]any{
			"reason": err.Error(), "operation_id": credentialRequired.OperationID,
			"role": credentialRequired.Role, "store_key": credentialRequired.StoreKey,
			"remediation": credentialRequired.ResumeCommand(),
		})
	case errors.Is(err, operatorchannel.ErrNotFound):
		if strings.Contains(err.Error(), "interface") {
			return NewApplicationError(ChannelInterfaceNotFoundCode, false, details)
		}
		return NewApplicationError(ChannelOperationNotFoundCode, false, details)
	case errors.Is(err, operatorchannel.ErrRevisionConflict):
		return NewApplicationError(ChannelRevisionConflictCode, false, details)
	case errors.Is(err, operatorchannel.ErrOperationTerminal):
		return NewApplicationError(ChannelOperationTerminalCode, false, details)
	case errors.Is(err, operatorchannel.ErrProofUnavailable):
		return NewApplicationError(ChannelProofUnavailableCode, false, details)
	case errors.Is(err, operatorchannel.ErrConflict):
		if strings.Contains(err.Error(), "ambiguous") {
			return NewApplicationError(ChannelInterfaceAmbiguousCode, false, details)
		}
		return NewApplicationError(ChannelBindingConflictCode, false, details)
	case errors.Is(err, operatorchannel.ErrInvalidRequest):
		return NewInvalidParamsError(details)
	default:
		return err
	}
}

func channelDestructiveError(err error) error {
	switch {
	case errors.Is(err, channelonboarding.ErrNotFound):
		return NewApplicationError(ChannelOperationNotFoundCode, false, map[string]any{"reason": err.Error()})
	case errors.Is(err, channelonboarding.ErrRevisionConflict):
		return NewApplicationError(ChannelRevisionConflictCode, false, map[string]any{"reason": err.Error()})
	case errors.Is(err, channelonboarding.ErrConflict):
		return NewApplicationError(ChannelBindingConflictCode, false, map[string]any{"reason": err.Error()})
	default:
		return operatorChannelError(err)
	}
}
