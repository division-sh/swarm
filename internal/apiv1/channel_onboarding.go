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
	Select(context.Context, channelonboarding.StartInput) (channelonboarding.StartSelection, error)
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
		"channel.onboarding_select": func(ctx context.Context, req Request) (any, error) {
			if err := requireOperatorPrincipal(req, opts.Channels); err != nil {
				return nil, err
			}
			input, err := channelOnboardingStartInput(req.Params)
			if err != nil {
				return nil, err
			}
			selected, err := opts.Onboarding.Select(ctx, input)
			if err != nil {
				if runtimefailures.OnlyBranches(err, func(branch error) bool { return branch == channelonboarding.ErrNotFound }) {
					return nil, NewApplicationError(ChannelInterfaceNotFoundCode, false, map[string]any{"reason": err.Error()})
				}
				return nil, channelOnboardingError(err)
			}
			return selected, nil
		},
		"channel.onboarding_start": func(ctx context.Context, req Request) (any, error) {
			if err := requireOperatorPrincipal(req, opts.Channels); err != nil {
				return nil, err
			}
			input, err := channelOnboardingStartInput(req.Params)
			if err != nil {
				return nil, err
			}
			result, err := opts.Onboarding.Start(ctx, input)
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

func channelOnboardingStartInput(params map[string]any) (channelonboarding.StartInput, error) {
	var input channelonboarding.StartInput
	provider, err := requiredStringParam(params, "provider")
	if err != nil {
		return input, err
	}
	verb, err := requiredStringParam(params, "verb")
	if err != nil {
		return input, err
	}
	input.Verb = channelonboarding.Verb(verb)
	if !input.Verb.Valid() {
		return input, NewInvalidParamsError(map[string]any{"field": "verb", "reason": "must be connect, reconnect, or rebind"})
	}
	input.Selection.Provider = provider
	for key, target := range map[string]*string{
		"bundle": &input.Selection.BundleHash, "interface": &input.Selection.InterfaceSelector,
		"target": &input.Selection.TargetSelector, "idempotency_key": &input.IdempotencyKey,
		"provider_credential": &input.ProviderCredential, "expected_selection": &input.ExpectedSelection,
	} {
		value, _, err := optionalStringParam(params, key)
		if err != nil {
			return input, err
		}
		*target = value
	}
	input.SaveProof, err = optionalBoolParam(params, "save_proof", true)
	if err != nil {
		return input, err
	}
	input.ClientLanguage, err = channelClientLanguageParam(params)
	return input, err
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
	var sessionUnavailable *operatorchannel.SessionProviderUnavailableError
	if errors.As(err, &sessionUnavailable) {
		return runtimefailures.Wrap(runtimefailures.ClassDependencyUnavailable, "channel_session_provider_unavailable", "channel-onboarding", "provider_admission", map[string]any{"provider": sessionUnavailable.Provider}, err)
	}
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
		if runtimefailures.OnlyBranches(err, func(branch error) bool {
			return branch == operatorchannel.ErrRevisionConflict || branch == operatorchannel.ErrConflict
		}) {
			return operatorChannelError(err)
		}
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
