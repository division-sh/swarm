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

func TestChannelOnboardingPendingResetRestartToReadyE2E(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, proof := range []bool{false, true} {
			for _, inherited := range []bool{false, true} {
				for _, boundary := range []channelonboarding.TestLifecycleBoundary{channelonboarding.TestAfterStaleIdentitySettlement, channelonboarding.TestAfterPendingResetCleanup, channelonboarding.TestAfterPendingResetCommit} {
					t.Run(fmt.Sprintf("%s/proof=%t/inherited=%t/%s", backend, proof, inherited, boundary), func(t *testing.T) {
						h := newChannelOnboardingE2EHarness(t, backend, true)
						barrier := newChannelOnboardingTestBarrier(boundary)
						barrier.Disarm()
						t.Cleanup(barrier.Release)
						h.opts.TestChannelOnboardingBarrier = barrier.Reach
						h.start(t)
						var begun channelonboarding.Result
						requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.onboarding_start", map[string]any{
							"provider": "telegram", "verb": "connect", "provider_credential": "pending-original", "save_proof": proof,
						}, &begun)
						id := begun.Operation.OperationID
						retained := int64(0)
						value := "pending-original"
						if inherited {
							claimed := claimPendingResetRPC(t, h, begun, 75930)
							var confirmed map[string]any
							requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.confirm", map[string]any{"operation_id": claimed.OperationID, "expected_revision": claimed.Revision, "approve": true}, &confirmed)
							rotatePendingResetCredential(t, h, begun, value)
							if result := requestServedJSONRPC(t, h.rpcEndpoint(), "channel.onboarding_retry", map[string]any{"operation_id": id}); result.Error == nil {
								t.Fatal("bound rotation did not require remediation")
							}
							retained = 1
							value = "pending-replacement"
							begun = retryChannelOnboardingRPC(t, h, id, value)
						}
						for cycle := 0; cycle < 2; cycle++ {
							claimed := claimPendingResetRPC(t, h, begun, int64(75931+cycle))
							before := getChannelOnboardingRPC(t, h, id)
							if before.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity || before.Operation.BindingRevision != retained {
								t.Fatalf("missing early-confirmation ordering: %#v", before)
							}
							rotatePendingResetCredential(t, h, before, value)
							barrier.Arm()
							confirmed := make(chan servedJSONRPCEnvelope, 1)
							go func() {
								confirmed <- requestServedJSONRPCWithTimeout(t, h.rpcEndpoint(), "channel.confirm", map[string]any{"operation_id": claimed.OperationID, "expected_revision": claimed.Revision, "approve": true}, 30*time.Second)
							}()
							if got := barrier.Wait(t); got != id {
								t.Fatalf("wrong reset owner: %s", got)
							}
							barrier.Release()
							select {
							case result := <-confirmed:
								if result.Error == nil {
									t.Fatal("reset interruption reported success")
								}
							case <-time.After(30 * time.Second):
								t.Fatal("interrupted confirmation did not return")
							}
							barrier.Disarm()
							h.stop(t)
							barrier = newChannelOnboardingTestBarrier(boundary)
							barrier.Disarm()
							t.Cleanup(barrier.Release)
							h.opts.TestChannelOnboardingBarrier = barrier.Reach
							h.start(t)
							reset := getChannelOnboardingRPC(t, h, id)
							if reset.Operation.Phase != channelonboarding.PhasePreparing || reset.Operation.BindingRevision != retained || reset.Operation.IdentityOperationID != "" || len(reset.Operation.CredentialAdmissions) != 0 {
								t.Fatalf("restart lost pending reset: %#v", reset)
							}
							value = "pending-replacement"
							if cycle == 0 {
								begun = retryChannelOnboardingRPC(t, h, id, value)
							}
						}
						resume := startChannelOnboardingCLICommand(t, h.opts.ConfigPath, h.endpoint, []string{"channel", "resume", id, "--yes", "--credential-stdin"}, value+"\n")
						challenge := waitChannelOnboardingChallenge(t, resume.stdout, resume.stderr, resume.done)
						fresh := getChannelOnboardingRPC(t, h, id)
						if fresh.IdentityOperation == nil || fresh.IdentityOperation.State != operatorchannel.StateAwaitingClaim {
							t.Fatalf("missing fresh ceremony: %#v", fresh)
						}
						// The child's expected predecessor is private; public readback exposes the parent's retained revision.
						if inherited && (fresh.IdentityOperation.Kind != operatorchannel.OperationReconnect || fresh.Operation.BindingRevision != retained) {
							t.Fatalf("reset forgot reconnect: %#v", fresh.IdentityOperation)
						}
						callback, signing := waitChannelOnboardingRegistrationForCredential(t, h.provider, value, 3, resume)
						requireChannelClaimDisposition(t, "fresh claimant", submitChannelOnboardingClaimAs(t, callback, signing, challenge, 75933, 8593, 9593, "pending_operator"), "consumed_by_binding")
						requireChannelOnboardingCommandSuccess(t, resume)
						ready := getChannelOnboardingRPC(t, h, id)
						if ready.Operation.Phase != channelonboarding.PhaseSucceeded || ready.Operation.BindingRevision != retained+1 || ready.Readiness == nil || !ready.Readiness.Ready {
							t.Fatalf("restart did not reach READY: %#v", ready)
						}
						h.stop(t)
					})
				}
			}
		}
	}
}

func claimPendingResetRPC(t *testing.T, h *channelOnboardingE2EHarness, begun channelonboarding.Result, updateID int64) operatorchannel.Operation {
	t.Helper()
	if begun.IdentityOperation == nil {
		t.Fatalf("missing identity: %#v", begun)
	}
	callback, signing, _ := h.provider.Registration()
	requireChannelClaimDisposition(t, "early claimant", submitChannelOnboardingClaimAs(t, callback, signing, begun.IdentityOperation.Challenge, updateID, 8593, 9593, "pending_operator"), "consumed_by_binding")
	claimed := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
	if claimed.IdentityOperation == nil || claimed.IdentityOperation.State != operatorchannel.StateAwaitingConfirmation {
		t.Fatalf("claim not settled: %#v", claimed)
	}
	return *claimed.IdentityOperation
}

func rotatePendingResetCredential(t *testing.T, h *channelOnboardingE2EHarness, current channelonboarding.Result, value string) {
	t.Helper()
	file, err := credentials.NewFileStore(h.credentialPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, admission := range current.Operation.CredentialAdmissions {
		got, found, err := file.Get(context.Background(), admission.StoreKey)
		if err != nil {
			t.Fatal(err)
		}
		if found && got == value {
			if err := file.Set(context.Background(), admission.StoreKey, "corrected-pending-value"); err != nil {
				t.Fatal(err)
			}
			return
		}
	}
	t.Fatal("missing admitted provider value")
}
