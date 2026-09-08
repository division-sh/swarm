package serveapp

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestChannelOnboardingConfirmedChildRotationResumeE2E(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, proof := range []bool{false, true} {
			for _, restart := range []bool{false, true} {
				for _, restore := range []bool{false, true} {
					for _, publishing := range []bool{false, true} {
						t.Run(fmt.Sprintf("%s/proof=%t/restart=%t/restore=%t/publishing=%t", backend, proof, restart, restore, publishing), func(t *testing.T) {
							harness := newChannelOnboardingE2EHarness(t, backend, true)
							barrier := newChannelOnboardingTestBarrier(channelonboarding.TestAfterBindingCheckpointBeforeActivation)
							barrier.Disarm()
							t.Cleanup(barrier.Release)
							harness.opts.TestChannelOnboardingBarrier = barrier.Reach
							harness.start(t)
							var begun channelonboarding.Result
							requireServedJSONRPCResult(t, harness.rpcEndpoint(), "channel.onboarding_start", map[string]any{
								"provider": "telegram", "verb": "connect", "provider_credential": "handoff-original-token", "save_proof": proof,
							}, &begun)
							if begun.IdentityOperation == nil {
								t.Fatalf("start lacks ceremony: %#v", begun)
							}
							callback, signing, _ := harness.provider.Registration()
							requireChannelClaimDisposition(t, "original claimant", submitChannelOnboardingClaimAs(t, callback, signing, begun.IdentityOperation.Challenge, 73931, 8393, 9393, "handoff_operator"), "consumed_by_binding")
							awaiting := retryChannelOnboardingRPC(t, harness, begun.Operation.OperationID, "")
							if awaiting.Operation.Phase != channelonboarding.PhaseAwaitingOperatorConfirmation || awaiting.IdentityOperation == nil {
								t.Fatalf("awaiting confirmation: %#v", awaiting)
							}
							var confirmed map[string]any
							requireServedJSONRPCResult(t, harness.rpcEndpoint(), "channel.confirm", map[string]any{
								"operation_id": awaiting.IdentityOperation.OperationID, "expected_revision": awaiting.IdentityOperation.Revision, "approve": true,
							}, &confirmed)
							committed := getChannelOnboardingRPC(t, harness, begun.Operation.OperationID)
							if committed.Operation.BindingRevision != 0 || committed.IdentityOperation == nil || committed.IdentityOperation.State != operatorchannel.StateBound || committed.IdentityOperation.BindingRevision != 1 {
								t.Fatalf("real API confirmation must expose the uncheckpointed handoff: %#v", committed)
							}
							if publishing {
								barrier.Arm()
								checkpoint := startChannelOnboardingCLICommand(t, harness.opts.ConfigPath, harness.endpoint, []string{"channel", "resume", begun.Operation.OperationID, "--yes"}, "")
								if id := barrier.Wait(t); id != begun.Operation.OperationID {
									t.Fatalf("unexpected checkpoint owner %s", id)
								}
								staged := getChannelOnboardingRPC(t, harness, begun.Operation.OperationID)
								if staged.Operation.Phase != channelonboarding.PhasePublishingActivation || staged.Operation.BindingRevision != 1 {
									t.Fatalf("publication checkpoint missing: %#v", staged)
								}
								barrier.Release()
								select {
								case code := <-checkpoint.done:
									if code == 0 {
										t.Fatal("publication boundary did not interrupt the command")
									}
								case <-time.After(15 * time.Second):
									t.Fatal("checkpoint command did not exit")
								}
								barrier.Disarm()
							}
							file, err := credentials.NewFileStore(harness.credentialPath)
							if err != nil {
								t.Fatal(err)
							}
							keys, err := file.List(context.Background())
							if err != nil {
								t.Fatal(err)
							}
							providerKey := ""
							for _, key := range keys {
								value, found, err := file.Get(context.Background(), key)
								if err != nil {
									t.Fatal(err)
								}
								if found && value == "handoff-original-token" {
									if providerKey != "" {
										t.Fatal("ambiguous physical provider credential")
									}
									providerKey = key
								}
							}
							if providerKey == "" {
								t.Fatal("physical provider credential is missing")
							}
							if err := file.Set(context.Background(), providerKey, "handoff-intervening-token"); err != nil {
								t.Fatal(err)
							}
							if restart {
								harness.stop(t)
								harness.start(t)
							} else {
								envelope := requestServedJSONRPC(t, harness.rpcEndpoint(), "channel.onboarding_retry", map[string]any{"operation_id": begun.Operation.OperationID})
								if envelope.Error == nil {
									t.Fatal("rotation retry did not request replacement credentials")
								}
							}
							reset := getChannelOnboardingRPC(t, harness, begun.Operation.OperationID)
							if reset.Operation.Phase != channelonboarding.PhasePreparing || reset.Operation.BindingRevision != 1 || reset.Operation.IdentityOperationID != "" {
								t.Fatalf("uncheckpointed child was not reconciled: %#v", reset)
							}
							value := "handoff-replacement-token"
							if restore {
								value = "handoff-original-token"
							}
							resume := startChannelOnboardingCLICommand(t, harness.opts.ConfigPath, harness.endpoint,
								[]string{"channel", "resume", begun.Operation.OperationID, "--yes", "--credential-stdin"}, value+"\n")
							challenge := waitChannelOnboardingChallenge(t, resume.stdout, resume.stderr, resume.done)
							if challenge == begun.IdentityOperation.Challenge {
								t.Fatal("resume reused the old challenge")
							}
							fresh := getChannelOnboardingRPC(t, harness, begun.Operation.OperationID)
							if fresh.IdentityOperation == nil || fresh.IdentityOperation.Kind != operatorchannel.OperationReconnect || fresh.Operation.BindingRevision != 1 || fresh.IdentityOperation.OperationID == begun.IdentityOperation.OperationID || fresh.IdentityOperation.State != operatorchannel.StateAwaitingClaim {
								t.Fatalf("resume bypassed fresh reconnect: %#v", fresh)
							}
							callback, signing = waitChannelOnboardingRegistrationForCredential(t, harness.provider, value, 2, resume)
							requireChannelClaimDisposition(t, "fresh claimant", submitChannelOnboardingClaimAs(t, callback, signing, challenge, 73932, 8393, 9393, "handoff_operator"), "consumed_by_binding")
							requireChannelOnboardingCommandSuccess(t, resume)
							ready := getChannelOnboardingRPC(t, harness, begun.Operation.OperationID)
							if ready.Operation.Phase != channelonboarding.PhaseSucceeded || ready.Operation.BindingRevision != 2 || ready.Readiness == nil || !ready.Readiness.Ready {
								t.Fatalf("fresh ceremony did not reach READY: %#v", ready)
							}
							harness.stop(t)
						})
					}
				}
			}
		}
	}
}

func TestChannelOnboardingEarlyReconnectConfirmationRestartE2E(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, proof := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/proof=%t", backend, proof), func(t *testing.T) {
				harness := newChannelOnboardingE2EHarness(t, backend, true)
				harness.start(t)
				var begun channelonboarding.Result
				for index, verb := range []string{"connect", "reconnect"} {
					value := "early-predecessor-token"
					if index == 1 {
						value = "early-original-token"
					}
					requireServedJSONRPCResult(t, harness.rpcEndpoint(), "channel.onboarding_start", map[string]any{
						"provider": "telegram", "verb": verb, "provider_credential": value, "save_proof": proof,
					}, &begun)
					if begun.IdentityOperation == nil {
						t.Fatalf("%s lacks ceremony: %#v", verb, begun)
					}
					callback, signing, _ := harness.provider.Registration()
					requireChannelClaimDisposition(t, verb+" claimant", submitChannelOnboardingClaimAs(t, callback, signing, begun.IdentityOperation.Challenge, int64(74931+index), 8493, 9493, "early_operator"), "consumed_by_binding")
					// Public confirmation is allowed before onboarding Retry advances the parent.
					claimed := getChannelOnboardingRPC(t, harness, begun.Operation.OperationID)
					if claimed.IdentityOperation == nil || claimed.IdentityOperation.State != operatorchannel.StateAwaitingConfirmation {
						t.Fatalf("claim did not settle: %#v", claimed)
					}
					var confirmed map[string]any
					requireServedJSONRPCResult(t, harness.rpcEndpoint(), "channel.confirm", map[string]any{
						"operation_id": claimed.IdentityOperation.OperationID, "expected_revision": claimed.IdentityOperation.Revision, "approve": true,
					}, &confirmed)
					if index == 0 {
						ready := retryChannelOnboardingRPC(t, harness, begun.Operation.OperationID, "")
						if ready.Operation.Phase != channelonboarding.PhaseSucceeded {
							t.Fatalf("predecessor did not complete: %#v", ready)
						}
					}
				}
				committed := getChannelOnboardingRPC(t, harness, begun.Operation.OperationID)
				if committed.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity || committed.Operation.BindingRevision != 0 || committed.IdentityOperation == nil || committed.IdentityOperation.State != operatorchannel.StateBound || committed.IdentityOperation.BindingRevision != 2 {
					t.Fatalf("missing early confirmed-child handoff: %#v", committed)
				}
				file, err := credentials.NewFileStore(harness.credentialPath)
				if err != nil {
					t.Fatal(err)
				}
				rotated := false
				for _, admission := range committed.Operation.CredentialAdmissions {
					value, found, err := file.Get(context.Background(), admission.StoreKey)
					if err != nil {
						t.Fatal(err)
					}
					if found && value == "early-original-token" {
						if err := file.Set(context.Background(), admission.StoreKey, "early-intervening-token"); err != nil {
							t.Fatal(err)
						}
						rotated = true
					}
				}
				if !rotated {
					t.Fatal("provider credential admission missing")
				}
				harness.stop(t)
				harness.start(t)
				reset := getChannelOnboardingRPC(t, harness, begun.Operation.OperationID)
				// The later effect-recovery pass can admit the still-current predecessor
				// credential, but must start a fresh ceremony at the retained revision.
				if reset.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity || reset.Operation.BindingRevision != 2 || reset.IdentityOperation == nil || reset.IdentityOperation.OperationID == committed.IdentityOperation.OperationID || reset.IdentityOperation.State != operatorchannel.StateAwaitingClaim {
					t.Fatalf("startup skipped fresh reconnect: phase=%s binding=%d child=%+v", reset.Operation.Phase, reset.Operation.BindingRevision, reset.IdentityOperation)
				}
				resume := startChannelOnboardingCLICommand(t, harness.opts.ConfigPath, harness.endpoint,
					[]string{"channel", "resume", begun.Operation.OperationID, "--yes"}, "")
				challenge := waitChannelOnboardingChallenge(t, resume.stdout, resume.stderr, resume.done)
				fresh := getChannelOnboardingRPC(t, harness, begun.Operation.OperationID)
				if fresh.IdentityOperation == nil || fresh.IdentityOperation.OperationID == begun.IdentityOperation.OperationID || fresh.IdentityOperation.Kind != operatorchannel.OperationReconnect || fresh.IdentityOperation.State != operatorchannel.StateAwaitingClaim {
					t.Fatalf("resume skipped fresh ceremony: %#v", fresh)
				}
				callback, signing := waitChannelOnboardingRegistrationForCredential(t, harness.provider, "early-predecessor-token", 2, resume)
				requireChannelClaimDisposition(t, "fresh claimant", submitChannelOnboardingClaimAs(t, callback, signing, challenge, 74933, 8493, 9493, "early_operator"), "consumed_by_binding")
				requireChannelOnboardingCommandSuccess(t, resume)
				ready := getChannelOnboardingRPC(t, harness, begun.Operation.OperationID)
				if ready.Operation.Phase != channelonboarding.PhaseSucceeded || ready.Operation.BindingRevision != 3 || ready.Readiness == nil || !ready.Readiness.Ready {
					t.Fatalf("fresh resume did not reach READY: %#v", ready)
				}
				harness.stop(t)
			})
		}
	}
}
