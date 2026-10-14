package cliapp

import (
	"context"
	"errors"
	"io"

	"github.com/division-sh/swarm/internal/channelonboarding"
)

func startCLIChannelWithReplacement(ctx context.Context, client *cliAPIClient, params map[string]any, opts channelConnectOptions, errOut io.Writer) (channelOnboardingResult, error) {
	var selected channelonboarding.StartSelection
	var result channelOnboardingResult
	if err := client.call(ctx, "channel.onboarding_select", params, &selected); err != nil {
		return result, returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	role, err := selected.ProviderCredentialRequirement()
	if err != nil {
		return result, returnCLIValidationError(errOut, err)
	}
	catalog, err := channelonboarding.NewCandidateCatalog([]channelonboarding.Candidate{selected.Candidate})
	if err == nil {
		_, err = catalog.Resolve(channelonboarding.CandidateSelection{
			Provider: channelSelectionParam(params, "provider"), BundleHash: channelSelectionParam(params, "bundle"),
			InterfaceSelector: channelSelectionParam(params, "interface"), TargetSelector: channelSelectionParam(params, "target"),
		})
	}
	if err != nil {
		return result, returnCLIValidationError(errOut, errors.New("credential selection contradicts the requested declaration"))
	}
	if role == "" {
		return result, returnCLIValidationError(errOut, errors.New("the selected declaration has no provider credential to replace"))
	}
	credential, err := readSecretValue(opts.apiOptions.input, errOut, false)
	if err != nil {
		return result, returnCLIValidationError(errOut, errors.New("provider credential input could not be read"))
	}
	if err := ctx.Err(); err != nil {
		return result, returnCLIValidationError(errOut, err)
	}
	params["expected_selection"], params["provider_credential"] = selected.SelectionFence, credential
	defer delete(params, "provider_credential")
	if err := client.call(ctx, "channel.onboarding_start", params, &result); err != nil {
		return result, returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	return result, nil
}

func channelSelectionParam(params map[string]any, key string) string {
	value, _ := params[key].(string)
	return value
}
