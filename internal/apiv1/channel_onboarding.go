package apiv1

import (
	"context"
	"errors"
	"strings"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
)

type ChannelOnboardingLifecycle interface {
	Start(context.Context, channelonboarding.StartInput) (channelonboarding.Result, error)
	Get(context.Context, string) (channelonboarding.Result, error)
	Retry(context.Context, channelonboarding.RetryInput) (channelonboarding.Result, error)
}

type ChannelOnboardingHandlerOptions struct {
	Onboarding ChannelOnboardingLifecycle
	Channels   *operatorchannel.Service
}

func ChannelOnboardingHandlers(opts ChannelOnboardingHandlerOptions) map[string]MethodHandler {
	if opts.Onboarding == nil || opts.Channels == nil {
		return nil
	}
	return map[string]MethodHandler{
		"channel.onboarding_start": func(ctx context.Context, req Request) (any, error) {
			if err := requireOperatorPrincipal(req, opts.Channels); err != nil {
				return nil, err
			}
			provider, err := requiredStringParam(req.Params, "provider")
			if err != nil {
				return nil, err
			}
			verbRaw, err := requiredStringParam(req.Params, "verb")
			if err != nil {
				return nil, err
			}
			verb := channelonboarding.Verb(verbRaw)
			if !verb.Valid() {
				return nil, NewInvalidParamsError(map[string]any{"field": "verb", "reason": "must be connect, reconnect, or rebind"})
			}
			bundleHash, _, err := optionalStringParam(req.Params, "bundle")
			if err != nil {
				return nil, err
			}
			interfaceSelector, _, err := optionalStringParam(req.Params, "interface")
			if err != nil {
				return nil, err
			}
			target, _, err := optionalStringParam(req.Params, "target")
			if err != nil {
				return nil, err
			}
			idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
			if err != nil {
				return nil, err
			}
			credential, _, err := optionalStringParam(req.Params, "provider_credential")
			if err != nil {
				return nil, err
			}
			saveProof, err := optionalBoolParam(req.Params, "save_proof", true)
			if err != nil {
				return nil, err
			}
			result, err := opts.Onboarding.Start(ctx, channelonboarding.StartInput{
				Verb: verb,
				Selection: channelonboarding.CandidateSelection{
					Provider: provider, BundleHash: bundleHash, InterfaceSelector: interfaceSelector, TargetSelector: target,
				},
				IdempotencyKey: idempotencyKey, ProviderCredential: credential, SaveProof: saveProof,
			})
			if err != nil {
				return nil, channelOnboardingError(err)
			}
			return result, nil
		},
		"channel.onboarding_get": func(ctx context.Context, req Request) (any, error) {
			if err := requireOperatorPrincipal(req, opts.Channels); err != nil {
				return nil, err
			}
			operationID, err := requiredStringParam(req.Params, "operation_id")
			if err != nil {
				return nil, err
			}
			result, err := opts.Onboarding.Get(ctx, operationID)
			if err != nil {
				return nil, channelOnboardingError(err)
			}
			return result, nil
		},
		"channel.onboarding_retry": func(ctx context.Context, req Request) (any, error) {
			if err := requireOperatorPrincipal(req, opts.Channels); err != nil {
				return nil, err
			}
			operationID, err := requiredStringParam(req.Params, "operation_id")
			if err != nil {
				return nil, err
			}
			credential, _, err := optionalStringParam(req.Params, "provider_credential")
			if err != nil {
				return nil, err
			}
			if _, _, err := optionalStringParam(req.Params, "idempotency_key"); err != nil {
				return nil, err
			}
			result, err := opts.Onboarding.Retry(ctx, channelonboarding.RetryInput{OperationID: operationID, ProviderCredential: credential})
			if err != nil {
				return nil, channelOnboardingError(err)
			}
			return result, nil
		},
	}
}

func channelOnboardingError(err error) error {
	details := map[string]any{"reason": err.Error()}
	var credentialRequired *channelonboarding.CredentialRequiredError
	switch {
	case errors.As(err, &credentialRequired):
		return NewApplicationError(ChannelCredentialRequiredCode, false, map[string]any{
			"reason": err.Error(), "role": credentialRequired.Role, "store_key": credentialRequired.StoreKey,
			"remediation": "provide the credential through hidden input or restore the exact admitted file-tier key",
		})
	case errors.Is(err, channelonboarding.ErrNotFound):
		return NewApplicationError(ChannelOperationNotFoundCode, false, details)
	case errors.Is(err, channelonboarding.ErrRevisionConflict):
		return NewApplicationError(ChannelRevisionConflictCode, false, details)
	case errors.Is(err, channelonboarding.ErrConflict):
		if strings.Contains(err.Error(), "ambiguous") {
			return NewApplicationError(ChannelInterfaceAmbiguousCode, false, details)
		}
		return NewApplicationError(ChannelBindingConflictCode, false, details)
	case errors.Is(err, channelonboarding.ErrInvalidRequest):
		return NewInvalidParamsError(details)
	default:
		return err
	}
}
