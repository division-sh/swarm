package cliapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/division-sh/swarm/internal/channelonboarding"
)

func startCLIChannelOnboarding(ctx context.Context, client *cliAPIClient, params map[string]any, opts channelConnectOptions, errOut io.Writer) (channelOnboardingResult, error) {
	var result channelOnboardingResult
	err := client.call(ctx, "channel.onboarding_start", params, &result)
	if err != nil {
		var rpc *jsonRPCError
		if !errors.As(err, &rpc) || applicationErrorCode(rpc.Data) != "CHANNEL_CREDENTIAL_REQUIRED" ||
			(opts.verb != "connect" && !opts.credentialStdin) {
			return result, returnCLIAPIError(errOut, err, channelErrorClassifier())
		}
		result, err = selectedChannelCredentialReadback(ctx, client, rpc)
		if err != nil {
			return result, returnCLIValidationError(errOut, err)
		}
	} else if !opts.credentialStdin {
		return result, nil
	}
	credential, err := readSelectedChannelCredential(result, opts, errOut)
	if err != nil {
		return result, err
	}
	retry := map[string]any{"operation_id": result.Operation.OperationID, "provider_credential": credential}
	defer delete(retry, "provider_credential")
	var admitted channelOnboardingResult
	if err := client.call(ctx, "channel.onboarding_retry", retry, &admitted); err != nil {
		return admitted, returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	if admitted.Operation.OperationID != result.Operation.OperationID {
		return admitted, returnCLIValidationError(errOut, errors.New("credential admission changed the retained operation"))
	}
	return admitted, nil
}

func selectedChannelCredentialReadback(ctx context.Context, client *cliAPIClient, rpc *jsonRPCError) (channelOnboardingResult, error) {
	var data struct {
		Details struct {
			OperationID string `json:"operation_id"`
			Role        string `json:"role"`
			StoreKey    string `json:"store_key"`
		} `json:"details"`
	}
	var result channelOnboardingResult
	if err := json.Unmarshal(rpc.Data, &data); err != nil || data.Details.OperationID == "" ||
		data.Details.Role == "" || data.Details.StoreKey == "" {
		return result, errors.New("credential admission lacks an exact retained operation and role")
	}
	if err := client.call(ctx, "channel.onboarding_get", map[string]any{"operation_id": data.Details.OperationID}, &result); err != nil {
		return result, err
	}
	role, err := result.ProviderCredentialRequirement()
	if err != nil || result.Operation.OperationID != data.Details.OperationID || role != data.Details.Role {
		return result, errors.New("credential admission contradicts the selected provider requirement")
	}
	for _, reservation := range result.Operation.CredentialReservations {
		if reservation.Role == role && reservation.StoreKey == data.Details.StoreKey {
			return result, nil
		}
	}
	return result, errors.New("credential admission contradicts the retained credential reservation")
}

func readSelectedChannelCredential(result channelOnboardingResult, opts channelConnectOptions, errOut io.Writer) (string, error) {
	role, err := result.ProviderCredentialRequirement()
	if err != nil || result.Operation.Phase.Terminal() {
		return "", returnCLIValidationError(errOut, errors.New("provider credential replacement requires its current selected declaration"))
	}
	if role == "" {
		return "", returnCLIValidationError(errOut, errors.New("the selected declaration has no provider credential to replace"))
	}
	credential, err := readSecretValue(opts.apiOptions.input, errOut, false)
	if err != nil {
		return "", returnCLIValidationError(errOut, errors.New("provider credential input could not be read"))
	}
	return credential, nil
}

func renderChannelOnboardingResult(out, errOut io.Writer, output cliOutputOptions, result channelOnboardingResult) error {
	pending, err := result.SessionPreparationPending()
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	if !pending && (result.Operation.Phase != channelonboarding.PhaseSucceeded || result.Readiness == nil || !result.Readiness.Ready) {
		return returnCLIValidationError(errOut, errors.New("channel result has neither current READY evidence nor owned pending preparation"))
	}
	return renderCLIOutput(out, errOut, output, result, func(w io.Writer) {
		if pending {
			fmt.Fprintf(w, "Channel %s preparation is pending (not READY). Resume with `swarm channel resume %s`.\n", strings.TrimSpace(result.Operation.Provider), result.Operation.OperationID)
			return
		}
		fmt.Fprintf(w, "Connected %s channel READY at activation revision %d.\n", result.Operation.Provider, result.Operation.ActivationRevision)
		writeChannelNativeQualification(w, result)
	}, func() ([]string, error) { return []string{result.Operation.OperationID}, nil })
}

func validateCLIChannelProgress(result channelOnboardingResult) (bool, error) {
	pending, err := result.SessionPreparationPending()
	if err != nil || result.Operation.OperationID == "" || result.Operation.Revision < 1 || !result.Operation.Phase.Valid() {
		return false, errors.New("channel onboarding response has invalid progress evidence")
	}
	return pending, nil
}

func validateCLIChannelContinuation(previous, next channelOnboardingResult) (bool, error) {
	if next.Operation.OperationID != previous.Operation.OperationID {
		return false, errors.New("channel retry changed the retained operation")
	}
	pending, err := validateCLIChannelProgress(next)
	if err != nil || pending {
		return pending, err
	}
	if next.Operation.Revision == previous.Operation.Revision && next.Operation.Phase == previous.Operation.Phase {
		return false, fmt.Errorf("channel onboarding is blocked in %s; retry after resolving the reported prerequisite", next.Operation.Phase)
	}
	return false, nil
}
