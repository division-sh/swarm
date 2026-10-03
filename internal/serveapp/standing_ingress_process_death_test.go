package serveapp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestStandingIngressPendingResetProcessDeathBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, inherited := range []bool{false, true} {
			for _, boundary := range []channelonboarding.TestLifecycleBoundary{channelonboarding.TestAfterPendingResetCleanup, channelonboarding.TestAfterPendingResetCommit} {
				t.Run(fmt.Sprintf("%s/inherited=%t/%s", backend, inherited, boundary), func(t *testing.T) {
					t.Setenv("TEST_CHANNEL_ONBOARDING_RETAIN_RUNS", "1")
					h := newChannelOnboardingE2EHarness(t, backend, true)
					h.opts.AbandonActiveRuns = false
					h.start(t)
					begun := startChannelOnboardingRPC(t, h, channelonboarding.VerbConnect, "crash-reset-original", nil)
					retained := int64(0)
					if inherited {
						claimed := claimPendingResetRPC(t, h, begun, 78930)
						var confirmed map[string]any
						requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.confirm", map[string]any{
							"operation_id": claimed.OperationID, "expected_revision": claimed.Revision, "approve": true,
						}, &confirmed)
						rotatePendingResetCredential(t, h, begun, "crash-reset-original")
						if result := requestServedJSONRPC(t, h.rpcEndpoint(), "channel.onboarding_retry", map[string]any{"operation_id": begun.Operation.OperationID}); result.Error == nil {
							t.Fatal("stale confirmed child did not require fresh admission")
						}
						retained = 1
						begun = retryChannelOnboardingRPC(t, h, begun.Operation.OperationID, "crash-reset-original")
					}
					claimed := claimPendingResetRPC(t, h, begun, 78931)
					rotatePendingResetCredential(t, h, begun, "crash-reset-original")
					h.stop(t)
					registrations, deliveries := h.provider.Counts()
					process := startChannelOnboardingCrashServeProcessAtBoundary(t, h.opts, h.telegram.URL, boundary)
					h.endpoint = process.endpoint(t)
					completed := make(chan error, 1)
					go func() {
						completed <- confirmStandingIngressAcrossProcessDeath(h.rpcEndpoint(), claimed)
					}()
					if id := waitStandingIngressProcessDeathBoundary(t, process, boundary); id != begun.Operation.OperationID {
						t.Fatalf("process stopped at another responsibility: %s", id)
					}
					if err := process.kill(); err != nil {
						t.Fatal(err)
					}
					select {
					case err := <-completed:
						if err == nil {
							t.Fatal("confirmation succeeded across process death")
						}
					case <-time.After(30 * time.Second):
						t.Fatal("confirmation did not settle after process death")
					}
					process = startChannelOnboardingCrashServeProcess(t, h.opts, h.telegram.URL)
					h.endpoint = process.endpoint(t)
					reset := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
					if reset.Operation.Phase != channelonboarding.PhasePreparing || reset.Operation.BindingRevision != retained || reset.IdentityOperation != nil || len(reset.Operation.CredentialAdmissions) != 0 {
						t.Fatalf("process death lost exact reset responsibility: %#v", reset)
					}
					if gotRegistration, gotDelivery := h.provider.Counts(); gotRegistration != registrations || gotDelivery != deliveries {
						t.Fatalf("reset recovery replayed provider effects: %d/%d -> %d/%d", registrations, deliveries, gotRegistration, gotDelivery)
					}
					finishStandingIngressProcessDeathRecovery(t, h, begun.Operation.OperationID, retained, "crash-reset-fresh", 78932)
					if err := process.stop(); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

func TestStandingIngressAdmissionProcessDeathBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, boundary := range []channelonboarding.TestLifecycleBoundary{
			channelonboarding.TestAfterCredentialWriteBeforeCheckpoint,
			channelonboarding.TestAfterStandingTargetReconciliation,
			channelonboarding.TestAfterStandingTargetPublication,
			channelonboarding.TestAfterActivationCommitBeforePublication,
			channelonboarding.TestAfterProcessPublicationBeforePromotion,
		} {
			t.Run(fmt.Sprintf("%s/%s", backend, boundary), func(t *testing.T) {
				t.Setenv("TEST_CHANNEL_ONBOARDING_RETAIN_RUNS", "1")
				h := newChannelOnboardingE2EHarness(t, backend, true)
				process := startChannelOnboardingCrashServeProcessAtBoundary(t, h.opts, h.telegram.URL, boundary)
				h.endpoint = process.endpoint(t)
				command := startChannelOnboardingCLICommand(t, h.opts.ConfigPath, h.endpoint,
					[]string{"channel", "connect", "telegram", "--yes"}, "crash-admission-token\n")
				beforeRegistration := boundary == channelonboarding.TestAfterCredentialWriteBeforeCheckpoint || boundary == channelonboarding.TestAfterStandingTargetReconciliation || boundary == channelonboarding.TestAfterStandingTargetPublication
				if !beforeRegistration {
					challenge := waitChannelOnboardingChallenge(t, command.stdout, command.stderr, command.done)
					callback, signing := waitChannelOnboardingRegistration(t, h.provider, command.stdout, command.stderr, command.done)
					requireChannelClaimDisposition(t, "pre-crash claimant", submitChannelOnboardingClaimAs(t, callback, signing, challenge, 79931, 8593, 9593, "pending_operator"), "consumed_by_binding")
				}
				id := waitStandingIngressProcessDeathBoundary(t, process, boundary, command)
				priorCallback, priorSigning, priorRegistrations := h.provider.Registration()
				if beforeRegistration {
					if registrations, deliveries := h.provider.Counts(); registrations != 0 || deliveries != 0 {
						t.Fatalf("credential-to-target handoff granted provider effects prematurely: %d/%d", registrations, deliveries)
					}
				}
				if err := process.kill(); err != nil {
					t.Fatal(err)
				}
				select {
				case code := <-command.done:
					if code == 0 {
						t.Fatal("public command succeeded across process death")
					}
				case <-time.After(30 * time.Second):
					t.Fatal("public mutation did not settle after process death")
				}
				process = startChannelOnboardingCrashServeProcess(t, h.opts, h.telegram.URL)
				h.endpoint = process.endpoint(t)
				recovered := getChannelOnboardingRPC(t, h, id)
				if beforeRegistration {
					if recovered.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity || recovered.IdentityOperation == nil {
						t.Fatalf("exact written receipts did not recover admission: %#v", recovered)
					}
					finishStandingIngressProcessDeathRecovery(t, h, id, 0, "", 79932)
				} else if recovered.Operation.Phase != channelonboarding.PhaseSucceeded || recovered.Operation.BindingRevision != 1 || recovered.Readiness == nil || !recovered.Readiness.Ready {
					t.Fatalf("activation process death did not settle the same responsibility: %#v", recovered)
				}
				wantRegistrations := 1
				if !beforeRegistration {
					// Current activation recovery renews the callback under the new
					// startup owner. Prove new authority rather than accepting replay.
					wantRegistrations = 2
					callback, signing, count := h.provider.Registration()
					oldURL, oldErr := url.Parse(priorCallback)
					newURL, newErr := url.Parse(callback)
					if priorRegistrations != 1 || count != 2 || oldErr != nil || newErr != nil || signing != priorSigning ||
						oldURL.Query().Get("swarm_callback_generation") == "" || newURL.Query().Get("swarm_callback_generation") == "" ||
						oldURL.Query().Get("swarm_callback_generation") == newURL.Query().Get("swarm_callback_generation") {
						t.Fatalf("restart did not renew exact fresh callback authority: old=%s new=%s registrations=%d->%d", priorCallback, callback, priorRegistrations, count)
					}
				}
				if registrations, confirmations := h.provider.OnboardingCounts(); registrations != wantRegistrations || confirmations != 1 {
					t.Fatalf("process recovery duplicated or lost effects: registration=%d confirmation=%d", registrations, confirmations)
				}
				if err := process.stop(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func waitStandingIngressProcessDeathBoundary(t *testing.T, process *channelOnboardingCrashServeProcess, boundary channelonboarding.TestLifecycleBoundary, commands ...channelOnboardingCLICommand) string {
	t.Helper()
	prefix := "TEST_CHANNEL_CRASH_BOUNDARY " + string(boundary) + " "
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		for _, line := range strings.Split(process.output.String(), "\n") {
			if id, found := strings.CutPrefix(line, prefix); found {
				return id
			}
		}
		for _, command := range commands {
			select {
			case code := <-command.done:
				t.Fatalf("channel command exited %d before %s:\nstdout:\n%s\nstderr:\n%s", code, boundary, command.stdout.String(), command.stderr.String())
			default:
			}
		}
		select {
		case <-process.exited:
			t.Fatalf("process exited before %s: %v\n%s", boundary, process.waitError(), process.output.String())
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("process did not reach %s:\n%s", boundary, process.output.String())
	return ""
}

func finishStandingIngressProcessDeathRecovery(t *testing.T, h *channelOnboardingE2EHarness, operationID string, retained int64, credential string, updateID int64) {
	t.Helper()
	args := []string{"channel", "resume", operationID, "--yes"}
	input := ""
	if credential != "" {
		args = append(args, "--credential-stdin")
		input = credential + "\n"
	}
	resume := startChannelOnboardingCLICommand(t, h.opts.ConfigPath, h.endpoint, args, input)
	challenge := waitChannelOnboardingChallenge(t, resume.stdout, resume.stderr, resume.done)
	fresh := getChannelOnboardingRPC(t, h, operationID)
	if fresh.Operation.BindingRevision != retained || fresh.IdentityOperation == nil || fresh.IdentityOperation.State != operatorchannel.StateAwaitingClaim || retained > 0 && fresh.IdentityOperation.Kind != operatorchannel.OperationReconnect {
		t.Fatalf("process recovery lost the exact fresh ceremony: %#v", fresh)
	}
	callback, signing, _ := h.provider.Registration()
	requireChannelClaimDisposition(t, "post-crash claimant", submitChannelOnboardingClaimAs(t, callback, signing, challenge, updateID, 8593, 9593, "pending_operator"), "consumed_by_binding")
	requireChannelOnboardingCommandSuccess(t, resume)
	ready := getChannelOnboardingRPC(t, h, operationID)
	if ready.Operation.Phase != channelonboarding.PhaseSucceeded || ready.Operation.BindingRevision != retained+1 || ready.Readiness == nil || !ready.Readiness.Ready {
		t.Fatalf("process recovery did not complete: %#v", ready)
	}
}

func confirmStandingIngressAcrossProcessDeath(endpoint string, claimed operatorchannel.Operation) error {
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": "process-death-confirm", "method": "channel.confirm",
		"params": map[string]any{"operation_id": claimed.OperationID, "expected_revision": claimed.Revision, "approve": true}})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var envelope servedJSONRPCEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		return err
	}
	if envelope.Error != nil {
		return errors.New(envelope.Error.Message)
	}
	return nil
}
