package serveapp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/servedparity"
)

func TestStaleLearnedRegistrationFreshReconnectBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		for _, change := range []string{"deleted", "rotated", "same_value_new_receipt"} {
			t.Run(string(backend)+"/"+change, func(t *testing.T) {
				h := newChannelOnboardingE2EHarness(t, backend, true)
				var rt *runtime.Runtime
				h.opts.TestRuntimeReadyHook = func(ready *runtime.Runtime) { rt = ready }
				h.start(t)
				var begun channelonboarding.Result
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.onboarding_start", map[string]any{
					"provider": "telegram", "verb": "connect", "provider_credential": "original-token",
				}, &begun)
				claimed := claimPendingResetRPC(t, h, begun, 78101)
				var confirmed map[string]any
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.confirm", map[string]any{
					"operation_id": claimed.OperationID, "expected_revision": claimed.Revision, "approve": true,
				}, &confirmed)
				predecessor := retryChannelOnboardingRPC(t, h, begun.Operation.OperationID, "")
				if predecessor.Operation.Phase != channelonboarding.PhaseSucceeded || predecessor.Operation.BindingRevision != 1 {
					t.Fatalf("predecessor did not complete: %#v", predecessor)
				}
				file, err := credentials.NewFileStore(h.credentialPath)
				if err != nil {
					t.Fatal(err)
				}
				var provider channelonboarding.CredentialAdmission
				for _, admission := range predecessor.Operation.CredentialAdmissions {
					if admission.Role == predecessor.Candidate.ProviderCredentialRole {
						provider = admission
					}
				}
				if provider.StoreKey == "" {
					t.Fatal("predecessor has no exact provider admission")
				}
				if err := file.Delete(context.Background(), provider.StoreKey); err != nil {
					t.Fatal(err)
				}
				response := requestServedJSONRPC(t, h.rpcEndpoint(), "channel.onboarding_start", map[string]any{
					"provider": "telegram", "verb": "reconnect",
				})
				if response.Error == nil {
					t.Fatal("missing credential advanced reconnect")
				}
				pendingID := ""
				for _, row := range readChannelOnboardingRows(t, h.opts.ConfigPath, h.endpoint) {
					if row.Operation != nil && row.Operation.Phase == string(channelonboarding.PhasePreparing) {
						if pendingID != "" {
							t.Fatal("competing pending reconnect responsibilities")
						}
						pendingID = row.Operation.OperationID
					}
				}
				if pendingID == "" {
					t.Fatal("reconnect did not preserve recovery responsibility")
				}
				if change == "rotated" {
					err = file.Set(context.Background(), provider.StoreKey, "unadmitted-rotation")
				} else if change == "same_value_new_receipt" {
					_, err = file.AdmitWithReceipt(context.Background(), provider.StoreKey, "original-token", "foreign-receipt")
				}
				if err != nil {
					t.Fatal(err)
				}
				registrations, deliveries := h.provider.Counts()
				response = requestServedJSONRPC(t, h.rpcEndpoint(), "channel.onboarding_retry", map[string]any{
					"operation_id": pendingID, "provider_credential": "replacement-token",
				})
				if response.Error != nil {
					t.Fatalf("fresh admitted reconnect blocked by stale registration: %#v", response.Error)
				}
				var reconnect channelonboarding.Result
				if err := json.Unmarshal(response.Result, &reconnect); err != nil {
					t.Fatal(err)
				}
				if reconnect.Operation.OperationID != pendingID || reconnect.IdentityOperation == nil || reconnect.IdentityOperation.Kind != operatorchannel.OperationReconnect {
					t.Fatalf("reconnect lost inherited identity: %#v", reconnect)
				}
				lease, current := rt.ChannelActivations.AcquirePresentation()
				if !current {
					t.Fatal("complete activation publication unavailable")
				}
				activations := lease.Activations()
				lease.Release()
				if len(activations) != 0 {
					t.Fatalf("stale predecessor still grants tools/native/learned authority: %#v", activations)
				}
				if gotRegistration, gotDelivery := h.provider.Counts(); gotRegistration != registrations+1 || gotDelivery != deliveries {
					t.Fatalf("fresh prebinding duplicated provider work: %d/%d -> %d/%d", registrations, deliveries, gotRegistration, gotDelivery)
				}
				freshClaim := claimPendingResetRPC(t, h, reconnect, 78102)
				if freshClaim.OperationID == claimed.OperationID {
					t.Fatal("reconnect reused the predecessor ceremony")
				}
				requireServedJSONRPCResult(t, h.rpcEndpoint(), "channel.confirm", map[string]any{
					"operation_id": freshClaim.OperationID, "expected_revision": freshClaim.Revision, "approve": true,
				}, &confirmed)
				ready := retryChannelOnboardingRPC(t, h, pendingID, "")
				if ready.Operation.Phase != channelonboarding.PhaseSucceeded || ready.Operation.BindingRevision != 2 || ready.Readiness == nil || !ready.Readiness.Ready {
					t.Fatalf("fresh ceremony did not complete exact handoff: %#v", ready)
				}
				history := getChannelOnboardingRPC(t, h, predecessor.Operation.OperationID)
				if history.Operation.Phase != channelonboarding.PhaseSucceeded || history.Operation.BindingRevision != 1 || history.Operation.IdentityOperationID != claimed.OperationID {
					t.Fatalf("reconnect rewrote predecessor history: %#v", history)
				}
				repeated := retryChannelOnboardingRPC(t, h, pendingID, "")
				if repeated.Operation.Revision != ready.Operation.Revision {
					t.Fatal("completed retry mutated the responsibility")
				}
				h.stop(t)
				registrations, deliveries = h.provider.Counts()
				h.start(t)
				restarted := getChannelOnboardingRPC(t, h, pendingID)
				if restarted.Operation.Phase != channelonboarding.PhaseSucceeded || restarted.Operation.BindingRevision != 2 || restarted.Readiness == nil || !restarted.Readiness.Ready {
					t.Fatalf("restart lost successor authority: %#v", restarted)
				}
				if gotRegistration, gotDelivery := h.provider.Counts(); gotRegistration != registrations+1 || gotDelivery != deliveries {
					t.Fatalf("restart effect counts = %d/%d, want %d/%d", gotRegistration, gotDelivery, registrations+1, deliveries)
				}
				h.stop(t)
			})
		}
	}
}
