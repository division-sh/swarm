package cliapp

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestChannelCLIClaimantNoAndEOFPreserveDecline(t *testing.T) {
	for _, input := range []string{"n\n", ""} {
		t.Run(input, func(t *testing.T) {
			var methods []string
			server := newOperatorChannelCLIServer(t, func(t *testing.T, request jsonRPCRequest, _ int) map[string]any {
				methods = append(methods, request.Method)
				switch request.Method {
				case "channel.onboarding_get":
					return channelOnboardingCLIResult("awaiting_operator_confirmation", "awaiting_confirmation", false)
				case "channel.confirm":
					if request.Params["approve"] != false {
						t.Fatal("no/EOF approved the claimant")
					}
					return map[string]any{"operation": operatorChannelCLIOperationResult("rejected", 3, 0)}
				case "channel.onboarding_retry":
					return channelOnboardingCLIResult("failed", "rejected", false)
				default:
					t.Fatalf("no/EOF performed unrelated work: %s", request.Method)
					return nil
				}
			})
			setCLIAPITestToken(t, operatorChannelCLIToken)
			opts := testRootCommandOptions(server)
			opts.input, opts.stdinIsTerminal = strings.NewReader(input), func() bool { return true }
			var out, errOut bytes.Buffer
			code := executeRootCommandWithOptions(context.Background(), t.TempDir(), []string{"channel", "resume", operatorChannelCLIOperation}, &out, &errOut, opts)
			if code != CLIExitValidation || !strings.Contains(errOut.String(), "claimant was rejected") ||
				strings.Join(methods, ",") != "channel.onboarding_get,channel.confirm,channel.onboarding_retry" {
				t.Fatalf("no/EOF lost its existing decline path: exit=%d methods=%v err=%s", code, methods, errOut.String())
			}
		})
	}
}
