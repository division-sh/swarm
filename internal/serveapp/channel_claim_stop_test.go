//go:build linux || darwin

package serveapp

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	"github.com/google/uuid"
)

type heldStopClaimStore struct {
	operatorchannel.Store
	entered chan<- struct{}
	release <-chan struct{}
}

func (s heldStopClaimStore) SettleSessionChannelClaim(ctx context.Context, claim authority.Claim) (operatorchannel.ClaimSettlement, error) {
	s.entered <- struct{}{}
	<-s.release
	return s.Store.SettleSessionChannelClaim(ctx, claim)
}

func TestGracefulStopJoinsAdmittedDeploymentWebhookClaimBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			h := newChannelOnboardingE2EHarness(t, backend, false)
			configureChannelFixtureHITL(t, h.opts.ConfigPath, "420079")
			ctx := t.Context()
			var stores *selectedStoreOwner
			if backend == servedparity.BackendDefaultSQLite {
				stores = openSelectedSQLiteOwner(t, h.storeDSN, &config.Config{})
				if _, err := initializeServePlatformStateStores(ctx, stores.Schema()); err != nil {
					t.Fatal(err)
				}
			} else {
				db, err := sql.Open("postgres", h.storeDSN)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				stores = openSelectedPostgresOwner(t, h.storeDSN, db, &config.Config{})
			}
			t.Cleanup(func() { closeUnactivatedSelectedStore(t, stores) })
			process := worklifetime.NewProcess()
			capability, err := stores.StartupOwnership().AcquireProcessCapability(ctx, startupownership.AcquireRequest{
				OwnerID: "stop-claim-proof", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(),
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := closeSelectedStoreTestProcess(process, capability); err != nil {
					t.Error(err)
				}
			})
			original, err := capability.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			if err := process.BindOriginalProcess(original.LeaseStamp()); err != nil {
				t.Fatal(err)
			}
			ctx, err = startupownership.WithProcessMutationAuthority(ctx, capability)
			if err != nil {
				t.Fatal(err)
			}
			root, err := cliapp.NewInvocationRoot(repoRootForTest())
			if err != nil {
				t.Fatal(err)
			}
			directory := t.TempDir()
			var secrets *credentials.SnapshotOwner
			entered, release := make(chan struct{}, 1), make(chan struct{})
			var once sync.Once
			resume := func() { once.Do(func() { close(release) }) }
			defer resume()
			factory := func(ctx context.Context, opts cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
				secrets = opts.Credentials
				opts.Identities = heldStopClaimStore{opts.Identities, entered, release}
				return OpenChannelRegistration(ctx, opts)
			}
			client, err := cliapp.UseDeploymentChannelControl(ctx, root, h.opts.ConfigPath, directory, "telegram", stores, process, factory)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close(context.WithoutCancel(ctx)) })
			var result channelonboarding.Result
			if err := client.Dispatch(ctx, "channel.onboarding_start", map[string]any{"provider": "telegram", "verb": "connect", "account": "420079", "client_language": "en", "provider_credential": "synthetic-stop-claim", "idempotency_key": "stop-claim"}, &result); err != nil {
				t.Fatal(err)
			}
			if result.IdentityOperation == nil {
				if err := client.Dispatch(ctx, "channel.onboarding_retry", map[string]any{"operation_id": result.Operation.OperationID, "approve_account": true}, &result); err != nil {
					t.Fatal(err)
				}
			}
			child := result.IdentityOperation
			if child == nil || child.State != operatorchannel.StateAwaitingClaim || !result.Operation.AccountConfirmed {
				t.Fatal("fixture did not reach a native pending claim")
			}
			manager, err := runtime.NewRuntimeContextManager(nil)
			if err != nil {
				t.Fatal(err)
			}
			s := newProcessLifecycleSupervisor(nil, nil)
			s.runtimeContexts, s.processCapability = manager, capability
			s.shutdownOptions = runtime.ShutdownOptions{Grace: 5 * time.Second}
			// Keep the original factory and handler. Only the injected native store waits.
			handler := deploymentPendingClaimHandler(root, h.opts.ConfigPath, directory, stores, process, capability, factory, deploymentRuntimeOptions{manager: manager})
			claimed := make(chan error, 1)
			go func() {
				payload := map[string]any{"update_id": 91, "message": map[string]any{"message_id": 91, "text": child.Challenge,
					"from": map[string]any{"id": 7000}, "chat": map[string]any{"id": 1001, "type": "private"}}}
				consumed, err := handler(ctx, client.DeploymentOperatorAccount(), func(admission channelonboarding.CredentialAdmission) bool {
					current, err := secrets.CurrentValueMatchesSeal(ctx, credentials.ValueEvidence{Key: admission.StoreKey, Seal: admission.ValueSeal})
					return current && err == nil
				}, nil, func(plan providertriggers.InboundAdmissionPlan) (providertriggers.Delivery, error) {
					body, err := json.Marshal(payload)
					if err != nil {
						return providertriggers.Delivery{}, err
					}
					return plan.Accept(providertriggers.Request{Provider: "telegram", Method: http.MethodPost, Body: body, Payload: payload, ContentType: "application/json",
						Headers: http.Header{"X-Telegram-Bot-Api-Secret-Token": {"fixture-secret"}}, Target: providertriggers.Target{WebhookSecret: "fixture-secret"}})
				}, nil, func() bool { return ctx.Err() == nil })
				if !consumed && err == nil {
					err = fmt.Errorf("exact webhook claim was not consumed")
				}
				claimed <- err
			}()
			select {
			case <-entered:
			case err := <-claimed:
				t.Fatalf("real settlement entrance not reached: %v", err)
			case <-time.After(5 * time.Second):
				t.Fatal("real settlement entrance not reached before bound")
			}
			stopCtx := s.prepareInboundStop()
			defer s.inboundStopCancel()
			prepared := make(chan *serveInboundStop, 1)
			go func() { prepared <- s.beginInboundStop(nil) }()
			probe, cancel := context.WithTimeout(ctx, time.Second)
			use, _, openErr := manager.AcquireInboundPublication(probe, "stop-claim-proof", "telegram")
			cancel()
			if use != nil {
				_ = use.Done()
			}
			if openErr != context.DeadlineExceeded {
				t.Errorf("stored intake left PREPARING with matched native settlement still held: %v", openErr)
			}
			resume()
			select {
			case err := <-claimed:
				if err != nil {
					t.Error(err)
				}
			case <-stopCtx.Done():
				t.Error("admitted claim did not settle within original stop budget")
			}
			select {
			case stop := <-prepared:
				if err := manager.CloseInboundRequests(stopCtx); err != nil {
					t.Error(err)
				}
				if err := stop.Finish(); err != nil {
					t.Error(err)
				}
			case <-stopCtx.Done():
				t.Fatal("stop did not join released native claim")
			}
			receipt, found, err := stores.OperatorChannels().LoadOperatorChannelClaimReceipt(ctx, operatorchannel.SessionClaimReceiptID(result.Operation.OperationID, "91"))
			if err != nil || !found || receipt.OperationID != child.OperationID || receipt.ProviderEventID != "91" || receipt.Challenge != child.Challenge {
				t.Fatalf("released matched claim lost its native receipt: found=%t err=%v", found, err)
			}
			rows, err := stores.OperatorChannels().ListOperatorChannelOperations(ctx, child.PrincipalID)
			if err != nil || len(rows) != 1 || rows[0].OperationID != child.OperationID || rows[0].State != operatorchannel.StateAwaitingConfirmation {
				t.Fatalf("released matched claim did not settle the exact operation: %v", err)
			}
		})
	}
}
