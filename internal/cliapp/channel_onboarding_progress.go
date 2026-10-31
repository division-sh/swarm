package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	qrcode "github.com/skip2/go-qrcode"
)

func startCLIChannelOnboarding(ctx context.Context, client channelControlClient, params map[string]any, opts channelConnectOptions, errOut io.Writer) (channelOnboardingResult, error) {
	if opts.credentialStdin {
		return startCLIChannelWithReplacement(ctx, client, params, opts, errOut)
	}
	var result channelOnboardingResult
	err := client.call(ctx, "channel.onboarding_start", params, &result)
	if err != nil {
		var rpc *jsonRPCError
		var required *channelonboarding.CredentialRequiredError
		if errors.As(err, &required) {
			err = client.call(ctx, "channel.onboarding_get", map[string]any{"operation_id": required.OperationID}, &result)
		} else if !errors.As(err, &rpc) || applicationErrorCode(rpc.Data) != "CHANNEL_CREDENTIAL_REQUIRED" ||
			(opts.verb != "connect" && !opts.credentialStdin) {
			return result, returnCLIAPIError(errOut, err, channelErrorClassifier())
		} else {
			result, err = selectedChannelCredentialReadback(ctx, client, rpc)
		}
		if err != nil {
			return result, returnCLIValidationError(errOut, err)
		}
	} else {
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

func selectedChannelCredentialReadback(ctx context.Context, client channelControlClient, rpc *jsonRPCError) (channelOnboardingResult, error) {
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
	return result, nil
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
	if result.Operation.Phase == channelonboarding.PhaseFailed {
		return returnCLIValidationError(errOut, fmt.Errorf("channel onboarding failed (%s): %s",
			cliSanitizeOneLineValue(result.Operation.FailureCode), cliSanitizeOneLineValue(result.Operation.FailureMessage)))
	}
	now := time.Now()
	pending, err := result.SessionPairingPending(now)
	if err != nil {
		return returnCLIValidationError(errOut, err)
	}
	if result.Operation.DeploymentRegistration && result.Operation.Phase == channelonboarding.PhaseSucceeded {
		return renderDeploymentRegistration(out, errOut, output, result)
	}
	if !pending && (result.Operation.Phase != channelonboarding.PhaseSucceeded || result.Readiness == nil || !result.Readiness.Ready) {
		if result.Operation.Provider == "whatsapp" {
			return returnCLIValidationError(errOut, errors.New("Not linked: run swarm channel connect whatsapp"))
		}
		if result.Operation.Phase == channelonboarding.PhaseSucceeded {
			reason := "connected readiness evidence is unavailable"
			if result.Readiness != nil {
				reason = string(result.Readiness.Reason)
			}
			return returnCLIValidationError(errOut, fmt.Errorf("channel onboarding succeeded historically but the channel is not READY: %s", reason))
		}
		return returnCLIValidationError(errOut, errors.New("channel result has neither current READY evidence nor owned pending preparation"))
	}
	var pairingQR string
	if pending && result.Pairing.Code != "" && !output.asJSON && !output.asYAML && !output.quiet {
		code, err := qrcode.New(result.Pairing.Code, qrcode.Medium)
		if err != nil {
			return returnCLIValidationError(errOut, errors.New("cannot render the current channel pairing QR"))
		}
		pairingQR = "\x1b[37;40m" + code.ToSmallString(false) + "\x1b[0m"
	}
	return renderCLIOutput(out, errOut, output, result, func(w io.Writer) {
		if pending {
			if result.Pairing.Status != channelonboarding.PairingStatusPreparing {
				fmt.Fprintf(w, "Pairing with %s is pending.\n", result.Operation.Provider)
				if result.Pairing.Code != "" {
					fmt.Fprintf(w, "Open WhatsApp > Linked Devices > Link a device, then scan (expires %s):\n%s\n", result.Pairing.ExpiresAt.UTC().Format(time.RFC3339), pairingQR)
				}
			} else {
				fmt.Fprintf(w, "Pairing preparation for %s is pending.\n", strings.TrimSpace(result.Operation.Provider))
			}
			fmt.Fprintf(w, "Next: swarm channel connect %s\n", result.Operation.Provider)
			return
		}
		if result.Operation.Provider == "whatsapp" {
			writeWhatsAppConnection(w, result.Connection, true)
		} else {
			fmt.Fprintf(w, "Connected %s channel READY at activation revision %d.\n", result.Operation.Provider, result.Operation.ActivationRevision)
			writeChannelNativeQualification(w, result)
		}
		fmt.Fprintln(w, "Next: swarm channel list")
	}, func() ([]string, error) { return []string{result.Operation.OperationID}, nil })
}

func renewExpiredChannelPairing(ctx context.Context, client channelControlClient, current channelOnboardingResult, errOut io.Writer) (channelOnboardingResult, error) {
	var next channelOnboardingResult
	if err := client.call(ctx, "channel.onboarding_retry", map[string]any{"operation_id": current.Operation.OperationID}, &next); err != nil {
		return channelOnboardingResult{}, returnCLIAPIError(errOut, err, channelErrorClassifier())
	}
	if _, err := validateCLIChannelContinuation(current, next, false); err != nil {
		return channelOnboardingResult{}, returnCLIValidationError(errOut, err)
	}
	if expired, err := next.SessionPairingExpired(time.Now()); err != nil || expired {
		return channelOnboardingResult{}, returnCLIValidationError(errOut, errors.New("the expired pairing batch could not be renewed"))
	}
	return next, nil
}

func writeChannelPairingQR(out, errOut io.Writer, output cliOutputOptions, result channelOnboardingResult, previousLines int) (int, error) {
	var frame, diagnostic bytes.Buffer
	if err := renderChannelOnboardingResult(&frame, &diagnostic, output, result); err != nil {
		if expired, expiryErr := result.SessionPairingExpired(time.Now()); expiryErr == nil && expired {
			return previousLines, nil // Expired while rendering: let the wait owner renew it.
		}
		_, writeErr := errOut.Write(diagnostic.Bytes())
		return previousLines, errors.Join(err, writeErr)
	}
	if previousLines > 0 {
		if _, err := fmt.Fprintf(out, "\x1b[%dF\x1b[J", previousLines); err != nil {
			return previousLines, err
		}
	}
	if _, err := out.Write(frame.Bytes()); err != nil {
		return previousLines, err
	}
	return bytes.Count(frame.Bytes(), []byte{'\n'}), nil
}

func renderDeploymentRegistration(out, errOut io.Writer, output cliOutputOptions, result channelOnboardingResult) error {
	if _, err := result.ProviderCredentialRequirement(); err != nil || result.Operation.ValidateDeploymentRegistration() != nil ||
		!result.Operation.AccountConfirmed || result.Operation.ActivationRevision != 0 || result.Operation.FailureCode != "" || result.Readiness != nil {
		return returnCLIValidationError(errOut, errors.New("deployment registration readback is incomplete or carries runtime authority"))
	}
	return renderCLIOutput(out, errOut, output, result, func(w io.Writer) {
		verb := "Registered"
		if result.Operation.ExpectedAccount != "" {
			verb = "Reverified"
		}
		fmt.Fprintf(w, "%s %s account %s.\n", verb, result.Operation.Provider, result.Operation.AccountReference)
		writeChannelAccountNextSteps(w)
	}, func() ([]string, error) { return []string{result.Operation.AccountReference}, nil })
}

func writeChannelAccountNextSteps(out io.Writer) {
	fmt.Fprintln(out, "To use it, set operator_account in a flow's policy.yaml. Telegram bots connected or newly held while serve runs receive after a serve restart; WhatsApp receives live.")
	fmt.Fprintln(out, "Next: swarm channel list")
}

func writeWhatsAppConnection(out io.Writer, connection *channelonboarding.SessionConnectionReadback, ready bool) {
	if ready {
		fmt.Fprintln(out, "WhatsApp: READY")
		return
	}
	state, reason, account := "disconnected", "Not linked: run swarm channel connect whatsapp", ""
	if connection != nil {
		state, account = connection.State, connection.AccountPresentation
		switch state {
		case "connected", "connecting", "pairing":
			reason = ""
		case "disconnected":
			if connection.Reason != "" {
				reason = "WhatsApp disconnected: " + cliSanitizeOneLineValue(connection.Reason)
				if connection.Reason == "network unreachable" {
					reason += ", reconnecting..."
				}
			} else if connection.Linked {
				reason = "WhatsApp disconnected: connection unavailable"
			}
		}
	}
	fmt.Fprintf(out, "WhatsApp: %s", state)
	if account != "" {
		fmt.Fprintf(out, " (%s)", cliSanitizeOneLineValue(account))
	}
	fmt.Fprintln(out)
	if reason != "" {
		fmt.Fprintln(out, reason)
	}
}

func validateCLIChannelProgress(result channelOnboardingResult) (bool, error) {
	if result.Operation.DeploymentRegistration {
		if _, err := result.ProviderCredentialRequirement(); err != nil {
			return false, errors.New("channel registration readback changed its deployment or account")
		}
	}
	now := time.Now()
	pending, err := result.SessionPairingExpired(now)
	if !pending && err == nil {
		pending, err = result.SessionPairingPending(now)
	}
	if err != nil || result.Operation.OperationID == "" || result.Operation.Revision < 1 || !result.Operation.Phase.Valid() {
		return false, errors.New("channel onboarding response has invalid progress evidence")
	}
	return pending, nil
}

func validateCLIChannelContinuation(previous, next channelOnboardingResult, checkpointOnly bool) (bool, error) {
	if next.Operation.OperationID != previous.Operation.OperationID || next.Operation.Revision < previous.Operation.Revision {
		return false, errors.New("channel retry changed the retained operation")
	}
	if previous.Operation.DeploymentRegistration || next.Operation.DeploymentRegistration {
		if previous.Operation.DeploymentRegistration != next.Operation.DeploymentRegistration || previous.Operation.Provider != next.Operation.Provider ||
			previous.Operation.PrincipalID != next.Operation.PrincipalID || previous.Operation.ExpectedAccount != next.Operation.ExpectedAccount ||
			previous.Operation.AccountReference != "" && previous.Operation.AccountReference != next.Operation.AccountReference {
			return false, errors.New("channel continuation changed the deployment or account")
		}
	}
	pending, err := validateCLIChannelProgress(next)
	if err != nil || pending {
		return pending, err
	}
	if checkpointOnly {
		if next.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity || next.Connection == nil ||
			!next.Connection.Linked || next.Connection.State != "connected" || next.IdentityOperation != nil || next.Binding != nil ||
			next.Readiness != nil && next.Readiness.Ready {
			return false, errors.New("pairing login did not return the non-executable account preview")
		}
		return false, nil
	}
	if next.Operation.Revision == previous.Operation.Revision && next.Operation.Phase == previous.Operation.Phase {
		return false, fmt.Errorf("channel onboarding is blocked in %s; retry after resolving the reported prerequisite", next.Operation.Phase)
	}
	return false, nil
}
