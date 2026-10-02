package serveapp

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestChannelDuplicatePhysicalBotRejectsDurablyWithoutStealingConnection(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, credential := range []string{"duplicate-bot-token", "credential-alias"} {
			t.Run(string(backend)+"/"+credential, func(t *testing.T) {
				h := newChannelOnboardingE2EHarness(t, backend, true)
				addSecondChannelOnboardingTarget(t, h.opts.SourceRoot)
				h.start(t)
				defer h.stop(t)
				choices := requireAmbiguousChannelOnboardingChoices(t, h, 2)
				first, second := choices[0], choices[1]
				args := append(exactChannelOnboardingArgs("connect", first), "--client-language", "en")
				command := startChannelOnboardingCLICommand(t, h.opts.ConfigPath, h.endpoint,
					args, "duplicate-bot-token\n")
				challenge := waitChannelOnboardingChallenge(t, command.stdout, command.stderr, command.done)
				callbackURL, signing := waitChannelOnboardingRegistration(t, h.provider, command.stdout, command.stderr, command.done)
				requireChannelClaimDisposition(t, "first connection", submitChannelOnboardingClaimAs(t,
					callbackURL, signing, challenge, 82481, 7000, 1001, "duplicate_connection"), "consumed_by_binding")
				requireChannelOnboardingCommandSuccess(t, command)
				firstBefore := requireChannelOnboardingTargetRow(t, h, first.target)
				if firstBefore.Activation == nil || firstBefore.Readiness == nil || !firstBefore.Readiness.Ready {
					t.Fatalf("first connection is not ready: %#v", firstBefore)
				}
				initial := waitNativeQualification(t, h, firstBefore.Operation.OperationID, channelnative.QualificationQualified, time.Time{})
				var rejected channelonboarding.Result
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.onboarding_start", map[string]any{
					"provider": "telegram", "verb": "reconnect", "provider_credential": credential,
					"save_proof": true, "bundle": second.bundle, "interface": second.interfaceRef, "target": second.target,
				}, &rejected)
				if rejected.Operation.Phase != channelonboarding.PhaseFailed || rejected.Operation.FailureCode != "provider_slot_collision" || rejected.IdentityOperation != nil {
					t.Fatalf("duplicate target did not terminally reject before identity: %#v", rejected)
				}
				for _, restart := range []bool{false, true} {
					if restart {
						h.stop(t)
						h.start(t)
					}
					beforeRetry := requireChannelOnboardingTargetRow(t, h, first.target)
					registrationsBefore, _ := h.provider.Counts()
					urlBefore, signingBefore, _ := h.provider.Registration()
					if !restart && (registrationsBefore != 1 || urlBefore != callbackURL || signingBefore != signing) {
						t.Fatal("duplicate replaced the original connection")
					}
					for _, method := range []string{"channel.onboarding_get", "channel.onboarding_retry"} {
						envelope := requestServedJSONRPC(t, h.rpcEndpoint(), method, map[string]any{"operation_id": rejected.Operation.OperationID})
						if envelope.Error != nil {
							t.Fatalf("terminal %s: %#v", method, envelope.Error)
						}
						var retained channelonboarding.Result
						if err := json.Unmarshal(envelope.Result, &retained); err != nil {
							t.Fatal(err)
						}
						if retained.Operation.Phase != channelonboarding.PhaseFailed || retained.Operation.FailureCode != "provider_slot_collision" {
							t.Fatalf("restart/retry revived rejected target: %#v", retained)
						}
					}
					current := requireChannelOnboardingTargetRow(t, h, first.target)
					if current.Readiness == nil || !current.Readiness.Ready || current.Activation == nil || beforeRetry.Activation == nil ||
						current.Activation.Revision != beforeRetry.Activation.Revision ||
						current.Identity.BindingRevision != firstBefore.Identity.BindingRevision ||
						current.Operation == nil || current.Operation.OperationID != firstBefore.Operation.OperationID {
						t.Fatalf("duplicate attempt stranded or replaced first connection: %#v", current)
					}
					registered, deliveries := h.provider.Counts()
					if registered != registrationsBefore || deliveries < 1 {
						t.Fatalf("duplicate applied provider registration: %d/%d", registered, deliveries)
					}
					url, secret, _ := h.provider.Registration()
					if url != urlBefore || secret != signingBefore {
						t.Fatal("duplicate replaced surviving webhook")
					}
					qualification := waitNativeQualification(t, h, firstBefore.Operation.OperationID, channelnative.QualificationQualified, time.Time{})
					if qualification.SettingID != initial.SettingID || qualification.SettingGeneration != initial.SettingGeneration || len(h.provider.CommandWrites()) != 1 {
						t.Fatal("duplicate/alias/restart replaced the native setting or replayed installation")
					}
				}
			})
		}
	}
}
