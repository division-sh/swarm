package serveapp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestStaleLearnedIngressRecoveryRequiredBothStores(t *testing.T) {
	testStaleLearnedIngressRecoveryRequiredBothStores(t, false)
}

func TestCompletedLearnedIngressRequiresFreshReconnectBothStores(t *testing.T) {
	testStaleLearnedIngressRecoveryRequiredBothStores(t, true)
}

func testStaleLearnedIngressRecoveryRequiredBothStores(t *testing.T, completed bool) {
	for _, backend := range servedparity.RequiredBackends {
		for _, role := range []string{"provider", "signing"} {
			for _, change := range []string{"changed_value", "same_value_new_receipt", "deleted"} {
				t.Run(fmt.Sprintf("%s/%s/%s", backend, role, change), func(t *testing.T) {
					h := newChannelOnboardingE2EHarness(t, backend, true)
					var rt *runtime.Runtime
					h.opts.TestRuntimeReadyHook = func(ready *runtime.Runtime) { rt = ready }
					h.start(t)
					var begun channelonboarding.Result
					requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.onboarding_start", map[string]any{
						"provider": "telegram", "verb": "connect", "provider_credential": "recovery-original-token",
					}, &begun)
					claimed := claimPendingResetRPC(t, h, begun, 77931)
					var confirmed map[string]any
					requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.confirm", map[string]any{
						"operation_id": claimed.OperationID, "expected_revision": claimed.Revision, "approve": true,
					}, &confirmed)
					if completed {
						ready := retryChannelOnboardingRPC(t, h, begun.Operation.OperationID, "")
						if ready.Operation.Phase != channelonboarding.PhaseSucceeded || ready.Readiness == nil || !ready.Readiness.Ready {
							t.Fatalf("initial ceremony did not complete: %#v", ready)
						}
					}
					file, err := credentials.NewFileStore(h.credentialPath)
					if err != nil {
						t.Fatal(err)
					}
					var selected channelonboarding.CredentialAdmission
					var original string
					for _, admission := range begun.Operation.CredentialAdmissions {
						value, found, err := file.Get(context.Background(), admission.StoreKey)
						if err != nil || !found {
							t.Fatalf("admitted role observation = %t, %v", found, err)
						}
						if (value == "recovery-original-token") == (role == "provider") {
							selected, original = admission, value
						}
					}
					if selected.StoreKey == "" {
						t.Fatal("selected role has no learned admission")
					}
					h.stop(t)
					switch change {
					case "changed_value":
						err = file.Set(context.Background(), selected.StoreKey, "unadmitted-replacement")
					case "same_value_new_receipt":
						_, err = file.AdmitWithReceipt(context.Background(), selected.StoreKey, original, "unrelated-owner-receipt")
					case "deleted":
						err = file.Delete(context.Background(), selected.StoreKey)
					}
					if err != nil {
						t.Fatal(err)
					}
					// Populated source defaults cannot replace the retained learned obligation.
					for _, reservation := range begun.Operation.CredentialReservations {
						if err := file.Set(context.Background(), reservation.StoreKey, "unrelated-source-default"); err != nil {
							t.Fatal(err)
						}
					}
					registrations, deliveries := h.provider.Counts()
					h.start(t)
					reset := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
					resetValid := reset.Operation.Phase == channelonboarding.PhasePreparing && reset.Operation.IdentityOperationID == "" && len(reset.Operation.CredentialAdmissions) == 0
					if completed {
						resetValid = reset.Operation.Phase == channelonboarding.PhaseSucceeded && reset.Operation.IdentityOperationID == claimed.OperationID
					}
					if !resetValid || reset.Operation.BindingRevision != 1 || reset.Readiness != nil && reset.Readiness.Ready {
						t.Fatalf("recovery did not retain the exact non-executable responsibility: %#v", reset)
					}
					if rt == nil {
						t.Fatal("served runtime observation is unavailable")
					}
					if targets, err := rt.PlanStandingTargets(); err != nil || len(targets) != 0 {
						t.Fatalf("recovery published executable targets = %#v, %v", targets, err)
					}
					output := h.process.outputString()
					remedy := "swarm channel resume " + begun.Operation.OperationID
					if completed {
						remedy = "swarm channel reconnect telegram"
					}
					for _, want := range []string{"RECOVERY REQUIRED ingress", "NOT READY", "recovery required", remedy} {
						if !strings.Contains(output, want) {
							t.Fatalf("recovery readback missing %q:\n%s", want, output)
						}
					}
					req, err := http.NewRequest(http.MethodPost, h.endpoint+"/webhooks/chat/telegram", strings.NewReader(`{"update_id":77932,"message":{"message_id":77932,"from":{"id":8593},"chat":{"id":9593,"type":"private"},"text":"not executable"}}`))
					if err != nil {
						t.Fatal(err)
					}
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("X-Telegram-Bot-Api-Secret-Token", original)
					resp, err := http.DefaultClient.Do(req)
					if err != nil {
						t.Fatal(err)
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
					if resp.StatusCode != http.StatusNotFound {
						t.Fatalf("stale executable ingress remained reachable: %d", resp.StatusCode)
					}
					if gotRegistration, gotDelivery := h.provider.Counts(); gotRegistration != registrations || gotDelivery != deliveries {
						t.Fatalf("recovery replayed provider effects: registration %d->%d delivery %d->%d", registrations, gotRegistration, deliveries, gotDelivery)
					}
					h.stop(t)
					// Restart after cleanup must not adopt the populated declaration defaults.
					h.start(t)
					if targets, err := rt.PlanStandingTargets(); err != nil || len(targets) != 0 {
						t.Fatalf("post-reset restart adopted defaults = %#v, %v", targets, err)
					}
					if gotRegistration, gotDelivery := h.provider.Counts(); gotRegistration != registrations || gotDelivery != deliveries {
						t.Fatalf("post-reset restart replayed effects: registration %d->%d delivery %d->%d", registrations, gotRegistration, deliveries, gotDelivery)
					}
					args := []string{"channel", "resume", begun.Operation.OperationID, "--yes", "--credential-stdin"}
					if completed {
						args = []string{"channel", "reconnect", "telegram", "--yes", "--credential-stdin"}
					}
					if completed && role == "signing" && change != "same_value_new_receipt" {
						blocked := startChannelOnboardingCLICommand(t, h.opts.ConfigPath, h.endpoint, args, "recovery-fresh-token\n")
						select {
						case code := <-blocked.done:
							if code == 0 || !strings.Contains(blocked.stderr.String(), "credential") {
								t.Fatalf("reconnect adopted a changed/missing retained signing key: exit=%d stderr=%s", code, blocked.stderr.String())
							}
						case <-time.After(20 * time.Second):
							t.Fatal("reconnect did not refuse the stale retained signing key")
						}
						if gotRegistration, gotDelivery := h.provider.Counts(); gotRegistration != registrations || gotDelivery != deliveries {
							t.Fatalf("blocked reconnect granted effects: registration %d->%d delivery %d->%d", registrations, gotRegistration, deliveries, gotDelivery)
						}
						pendingID := ""
						for _, operation := range readChannelOnboardingRows(t, h.opts.ConfigPath, h.endpoint) {
							if operation.Operation != nil && operation.Operation.OperationID != begun.Operation.OperationID && operation.Operation.Phase == string(channelonboarding.PhasePreparing) {
								pendingID = operation.Operation.OperationID
							}
						}
						if pendingID == "" {
							t.Fatal("blocked reconnect lost its fresh responsibility")
						}
						// The operator restores the exact retained key through the public
						// secret command. Restart still cannot adopt its new receipt.
						restoreInput := installChannelOnboardingCLIInput(t, original+"\n")
						var out, errOut bytes.Buffer
						code := executeCLIFrom(context.Background(), repoRootForTest(), []string{"secrets", "set", selected.StoreKey, "--stdin"}, &out, &errOut, nil)
						restoreInput()
						if code != 0 {
							t.Fatalf("explicit retained signing-key repair failed: exit=%d stderr=%s", code, errOut.String())
						}
						h.stop(t)
						h.start(t)
						waitForInboundAdmissionServeOutput(t, h.process, "swarm channel resume "+pendingID)
						if targets, err := rt.PlanStandingTargets(); err != nil || len(targets) != 0 {
							t.Fatalf("restart adopted an explicitly repaired but unadmitted key: %#v, %v", targets, err)
						}
						if gotRegistration, gotDelivery := h.provider.Counts(); gotRegistration != registrations || gotDelivery != deliveries {
							t.Fatal("restart replayed effects before fresh admission")
						}
						args = []string{"channel", "resume", pendingID, "--yes", "--credential-stdin"}
					}
					resume := startChannelOnboardingCLICommand(t, h.opts.ConfigPath, h.endpoint, args, "recovery-fresh-token\n")
					challenge := waitChannelOnboardingChallenge(t, resume.stdout, resume.stderr, resume.done)
					operationID := begun.Operation.OperationID
					if completed {
						operations := readChannelOnboardingRows(t, h.opts.ConfigPath, h.endpoint)
						for _, operation := range operations {
							if operation.Operation != nil && operation.Operation.OperationID != operationID && operation.Operation.Phase != string(channelonboarding.PhaseSucceeded) {
								operationID = operation.Operation.OperationID
								break
							}
						}
						if operationID == begun.Operation.OperationID {
							t.Fatal("reconnect did not create a fresh responsibility")
						}
					}
					fresh := getChannelOnboardingRPC(t, h, operationID)
					wantRevision := int64(1)
					if completed {
						wantRevision = 0
						historical := getChannelOnboardingRPC(t, h, begun.Operation.OperationID)
						if historical.Operation.Phase != channelonboarding.PhaseSucceeded || historical.Operation.BindingRevision != 1 || historical.Operation.IdentityOperationID != claimed.OperationID {
							t.Fatalf("reconnect rewrote completed history: %#v", historical)
						}
					}
					if fresh.Operation.BindingRevision != wantRevision || fresh.IdentityOperation == nil || fresh.IdentityOperation.Kind != operatorchannel.OperationReconnect || fresh.IdentityOperation.OperationID == claimed.OperationID {
						t.Fatalf("explicit admission lost the retained reconnect obligation: %#v", fresh)
					}
					callback, signing := waitChannelOnboardingRegistrationForCredential(t, h.provider, "recovery-fresh-token", registrations+1, resume)
					requireChannelClaimDisposition(t, "fresh recovery claimant", submitChannelOnboardingClaimAs(t, callback, signing, challenge, 77933, 8593, 9593, "pending_operator"), "consumed_by_binding")
					requireChannelOnboardingCommandSuccess(t, resume)
					ready := getChannelOnboardingRPC(t, h, operationID)
					if ready.Operation.Phase != channelonboarding.PhaseSucceeded || ready.Operation.BindingRevision != 2 || ready.Readiness == nil || !ready.Readiness.Ready {
						t.Fatalf("fresh ceremony did not complete recovery: %#v", ready)
					}
					h.stop(t)
					h.start(t)
					ready = getChannelOnboardingRPC(t, h, operationID)
					if ready.Operation.Phase != channelonboarding.PhaseSucceeded || ready.Operation.BindingRevision != 2 || ready.Readiness == nil || !ready.Readiness.Ready {
						t.Fatalf("completed recovery did not survive restart: %#v", ready)
					}
					h.stop(t)
				})
			}
		}
	}
}

func TestChannelCredentialRotationBeforeTargetPublicationBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			h := newChannelOnboardingE2EHarness(t, backend, true)
			var rt *runtime.Runtime
			h.opts.TestRuntimeReadyHook = func(ready *runtime.Runtime) { rt = ready }
			rotated := false
			h.opts.TestChannelOnboardingBarrier = func(boundary channelonboarding.TestLifecycleBoundary, operationID string) error {
				if boundary != channelonboarding.TestAfterStandingTargetReconciliation {
					return nil
				}
				op := getChannelOnboardingRPC(t, h, operationID)
				file, err := credentials.NewFileStore(h.credentialPath)
				if err != nil {
					return err
				}
				for _, admission := range op.Operation.CredentialAdmissions {
					if err := file.Set(context.Background(), admission.StoreKey, "unadmitted-after-reconciliation"); err != nil {
						return err
					}
				}
				rotated = true
				return nil
			}
			h.start(t)
			result := requestServedJSONRPC(t, h.rpcEndpoint(), "channel.onboarding_start", map[string]any{
				"provider": "telegram", "verb": "connect", "provider_credential": "publication-original-token",
			})
			if result.Error == nil || !rotated {
				t.Fatalf("changed credential was not refused at publication: %#v, rotated=%t", result, rotated)
			}
			if err := rt.ValidateStandingIngressCredentials(context.Background()); err == nil {
				t.Fatal("stale admission remained current")
			}
			if registrations, deliveries := h.provider.Counts(); registrations != 0 || deliveries != 0 {
				t.Fatalf("stale target publication granted provider effects: %d/%d", registrations, deliveries)
			}
			resp, err := http.Post(h.endpoint+"/webhooks/chat/telegram", "application/json", strings.NewReader(`{"update_id":77934}`))
			if err != nil {
				t.Fatal(err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("stale target route was published: %d", resp.StatusCode)
			}
			h.stop(t)
		})
	}
}
