package apiv1

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/registration"
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
			language, err := channelClientLanguageParam(req.Params)
			if err != nil {
				return nil, err
			}
			result, err := opts.Onboarding.Start(ctx, channelonboarding.StartInput{
				Verb: verb,
				Selection: channelonboarding.CandidateSelection{
					Provider: provider, BundleHash: bundleHash, InterfaceSelector: interfaceSelector, TargetSelector: target,
				},
				IdempotencyKey: idempotencyKey, ProviderCredential: credential, SaveProof: saveProof, ClientLanguage: language,
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
			language, err := channelClientLanguageParam(req.Params)
			if err != nil {
				return nil, err
			}
			var localeRevision int64
			if _, supplied := req.Params["expected_locale_revision"]; supplied {
				localeRevision, err = channelRevisionParam(req.Params, "expected_locale_revision", false)
				if err != nil {
					return nil, err
				}
			}
			result, err := opts.Onboarding.Retry(ctx, channelonboarding.RetryInput{OperationID: operationID, ProviderCredential: credential,
				ClientLanguage: language, ExpectedLocaleRevision: localeRevision})
			if err != nil {
				return nil, channelOnboardingError(err)
			}
			return result, nil
		},
	}
}

func channelClientLanguageParam(params map[string]any) (string, error) {
	value, supplied := params["client_language"]
	if !supplied {
		return "", nil
	}
	language, ok := value.(string)
	if !ok || language == "" || language != strings.TrimSpace(language) {
		return "", NewInvalidParamsError(map[string]any{"field": "client_language", "reason": "must be an exact non-empty language string"})
	}
	return language, nil
}

func channelOnboardingError(err error) error {
	if errors.Is(err, credentials.ErrCredentialValueUnusable) {
		if !runtimefailures.OnlyBranches(err, func(branch error) bool { return branch == credentials.ErrCredentialValueUnusable }) {
			return err
		}
		return runtimefailures.Wrap(runtimefailures.ClassAuthenticationNeeded, "credential_value_unusable", "channel-onboarding", "credential_admission", map[string]any{"auth_kind": "channel_credential"}, err)
	}
	details := map[string]any{"reason": err.Error()}
	var credentialRequired *channelonboarding.CredentialRequiredError
	switch {
	case errors.As(err, &credentialRequired):
		if !channelCredentialRequiredBranches(err) {
			return err
		}
		return NewApplicationError(ChannelCredentialRequiredCode, false, map[string]any{
			"reason": err.Error(), "operation_id": credentialRequired.OperationID,
			"role": credentialRequired.Role, "store_key": credentialRequired.StoreKey,
			"remediation": credentialRequired.ResumeCommand(),
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

// Provider rejection owns its response cause, but no independently joined
// cleanup or revision failure can become a credential-correction outcome.
func channelCredentialRequiredBranches(err error) bool {
	switch cause := err.(type) {
	case interface{ Unwrap() []error }:
		branches := cause.Unwrap()
		if len(branches) == 0 {
			return false
		}
		for _, branch := range branches {
			if !channelCredentialRequiredBranches(branch) {
				return false
			}
		}
		return true
	case *channelonboarding.CredentialRequiredError:
		return cause != nil
	case *registration.ProviderCredentialRejectedError:
		return cause != nil && (cause.StatusCode == http.StatusUnauthorized || cause.StatusCode == http.StatusForbidden)
	case interface{ Unwrap() error }:
		return channelCredentialRequiredBranches(cause.Unwrap())
	default:
		return false
	}
}
