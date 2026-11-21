//go:build linux || darwin

package serveapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/providertriggers"
	"github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/sessionprovider/authority"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type heldStopClaimStore struct {
	operatorchannel.Store
	entered chan<- struct{}
	release <-chan struct{}
}

func TestGracefulStopRefusesOnlyMatchedDeploymentWebhookClaimBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			const token = "synthetic-stop-claim"
			managers := make(chan *runtime.RuntimeContextManager, 1)
			h := prepareChannelAnchorPublicJourney(t, backend, token, true, 0, func(opts *cliapp.ServeOptions) {
				opts.TestRuntimeContextsReadyHook = func(m *runtime.RuntimeContextManager) { managers <- m }
			})
			h.opts.SourceRoot = writeStandingTelegramServeFixture(t, h.telegram.URL)
			nodesPath := filepath.Join(h.opts.SourceRoot, "telegram-chat", "nodes.yaml")
			nodes, err := os.ReadFile(nodesPath)
			if err != nil {
				t.Fatal(err)
			}
			writeStandingCandidateFile(t, nodesPath, string(nodes)+"\nstop-input-receiver:\n  inbound.telegram.text_message:\n    guard: true\n")
			var stores *selectedStoreOwner
			project := projectRuntimePersistenceForServe
			projectRuntimePersistenceForServe = func(owner *selectedStoreOwner) serveRuntimePersistence {
				stores = owner
				return project(owner)
			}
			t.Cleanup(func() { projectRuntimePersistenceForServe = project })
			h.start(t)
			defer h.stop(t)
			manager := <-managers
			ctx, cancel := context.WithCancel(t.Context())
			out, errOut, done, progress := &lockedBuffer{}, &lockedBuffer{}, make(chan int, 1), make(chan int, 1)
			restore := installChannelOnboardingCLIInput(t, token+"\n")
			defer restore()
			args := channelFixtureArguments(h.opts.ConfigPath, "channel", "connect", "telegram", "420079", "--yes", "--client-language", "en", "--api-server", h.endpoint)
			go func() {
				code := cliapp.Execute(ctx, args, out, errOut, nil, nil, ChannelRegistrations())
				done <- code
				progress <- code
			}()
			defer func() { cancel(); <-done }()
			challenge := waitChannelOnboardingChallenge(t, out, errOut, progress)
			callback, signing := waitChannelOnboardingRegistrationForCredential(t, h.provider, token, 1, channelOnboardingCLICommand{
				stdout: out, stderr: errOut, done: progress,
			})
			var listed struct{ Accounts []channelonboarding.Result }
			var listOut, listErr lockedBuffer
			if code := cliapp.Execute(ctx, channelFixtureArguments(h.opts.ConfigPath, "channel", "list", "--api-server", h.endpoint, "--json"), &listOut, &listErr, nil, nil, ChannelRegistrations()); code != 0 || json.Unmarshal([]byte(listOut.String()), &listed) != nil {
				t.Fatalf("cannot read exact deployment claim: exit=%d err=%s", code, listErr.String())
			}
			if stores == nil || len(listed.Accounts) != 1 || listed.Accounts[0].IdentityOperation == nil || listed.Accounts[0].IdentityOperation.Challenge != challenge {
				t.Fatal("no exact pending native webhook claim at the cut")
			}
			parent, child := listed.Accounts[0].Operation, listed.Accounts[0].IdentityOperation
			identities := stores.OperatorChannels()
			before, err := identities.ListOperatorChannelOperations(ctx, child.PrincipalID)
			if err != nil || len(before) != 1 || before[0].OperationID != child.OperationID || before[0].Challenge != challenge || before[0].State != operatorchannel.StateAwaitingClaim {
				t.Fatalf("native claim before stop is not awaiting its exact challenge: %v", err)
			}
			beforeParent, err := stores.DeploymentChannels().GetChannelOnboarding(ctx, parent.OperationID)
			if err != nil || !beforeParent.PendingDeploymentClaim(channelonboarding.AccountReference{Provider: "telegram", Account: "420079"}) {
				t.Fatalf("native parent is not the exact pending deployment claim: %v", err)
			}
			live := submitChannelOnboardingClaimAs(t, callback, signing, "ordinary customer input before stop", 90, 7001, 1002, "customer")
			if live.StatusCode != http.StatusAccepted || len(live.EventIDs) == 0 {
				t.Fatalf("credential-bound callback did not work before stop: status=%d events=%d body=%s", live.StatusCode, len(live.EventIDs), live.Body)
			}
			liveChild, childErr := identities.ListOperatorChannelOperations(ctx, child.PrincipalID)
			liveParent, parentErr := stores.DeploymentChannels().GetChannelOnboarding(ctx, parent.OperationID)
			_, claimed, receiptErr := identities.LoadOperatorChannelClaimReceipt(ctx, operatorchannel.SessionClaimReceiptID(parent.OperationID, "90"))
			if err := errors.Join(childErr, parentErr, receiptErr); err != nil || claimed || !reflect.DeepEqual(liveChild, before) || !reflect.DeepEqual(liveParent, beforeParent) {
				t.Fatalf("live unmatched callback changed pending native claim: receipt=%t err=%v", claimed, err)
			}
			t.Logf("credential-bound callback accepted before stop: events=%d, exact pending claim unchanged", len(live.EventIDs))
			ref := channelonboarding.AccountReference{Provider: parent.Provider, Account: parent.AccountReference}
			facts, installed := installedChannelFactsFor(ref.Provider)
			if !installed {
				t.Fatal("pending claim has no installed provider facts")
			}
			holders, err := manager.AccountHolders(ctx, ref, facts)
			if err != nil || len(holders) != 1 {
				t.Fatalf("ordinary holder discovery did not find the exact current holder: count=%d err=%v", len(holders), err)
			}
			accepted, _, err := manager.AcquireBundleHash(ctx, channelAnchorBundleHash(t, h.rpcEndpoint()))
			if err != nil || accepted == nil {
				t.Fatalf("retain exact stop fixture: %v", err)
			}
			defer func() { _ = accepted.Done() }()
			h.process.cancel()
			cut, stop := context.WithTimeout(ctx, 5*time.Second)
			defer stop()
			for !manager.InboundDraining() {
				req, err := http.NewRequestWithContext(cut, http.MethodGet, h.endpoint+"/readyz", nil)
				if err != nil {
					t.Fatal(err)
				}
				res, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				_ = res.Body.Close()
			}
			ordinary := submitChannelOnboardingClaimAs(t, callback, signing, "ordinary customer input", 92, 7001, 1002, "customer")
			if ordinary.StatusCode != http.StatusAccepted || len(ordinary.EventIDs) == 0 {
				t.Errorf("unmatched authenticated input did not store pending: status=%d events=%d body=%s", ordinary.StatusCode, len(ordinary.EventIDs), ordinary.Body)
			}
			pending := 0
			for _, id := range ordinary.EventIDs {
				rows, err := storetest.ReadEventDeliveryDiagnosticRows(cut, h.runtime.Bus.Store(), id)
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range rows {
					if row.SubscriberType == "node" {
						if row.Status != "pending" {
							t.Errorf("ordinary callback selected a node during stop: %v", row.Status)
						}
						pending++
					}
				}
			}
			if pending == 0 {
				t.Error("ordinary callback has no native pending receiver")
			}
			matched := submitChannelOnboardingClaimAs(t, callback, signing, challenge, 93, 7000, 1001, "operator")
			if matched.StatusCode != http.StatusServiceUnavailable || !strings.Contains(matched.Body, "deployment claim admission refused") || len(matched.EventIDs) != 0 {
				t.Errorf("stop-wins matched callback escaped claim refusal: status=%d events=%d body=%s", matched.StatusCode, len(matched.EventIDs), matched.Body)
			}
			rows, err := identities.ListOperatorChannelOperations(cut, child.PrincipalID)
			if err != nil || !reflect.DeepEqual(rows, before) {
				t.Errorf("stopped callback mutated the native pending claim: rows=%d err=%v", len(rows), err)
			}
			afterParent, err := stores.DeploymentChannels().GetChannelOnboarding(cut, parent.OperationID)
			if err != nil || !reflect.DeepEqual(afterParent, beforeParent) {
				t.Errorf("stopped callback mutated the native claim parent: %v", err)
			}
			for _, event := range []string{"92", "93"} {
				if _, found, err := identities.LoadOperatorChannelClaimReceipt(cut, operatorchannel.SessionClaimReceiptID(parent.OperationID, event)); err != nil || found {
					t.Errorf("unexpected claim receipt for event %s: found=%t err=%v", event, found, err)
				}
			}
			retained, err := manager.RegistrationAccountHolders(cut, ref, facts)
			if err != nil || !reflect.DeepEqual(retained, holders) {
				t.Errorf("registration observation lost the original holder during stop: count=%d err=%v", len(retained), err)
			}
			use, _, _, err := manager.AcquireRun(cut, holders[0].RunID)
			if use != nil {
				_ = use.Done()
			}
			if use != nil || !errors.Is(err, worklifetime.ErrAdmissionFenced) {
				t.Errorf("retained observation reopened ordinary run admission: %v", err)
			}
			foreignFact, err := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("0", 64))
			if err != nil {
				t.Fatal(err)
			}
			foreign := runtimecorrelation.WithSourceArtifactFact(cut, foreignFact)
			if rows, err := manager.RegistrationAccountHolders(foreign, ref, facts); len(rows) != 0 || err == nil {
				t.Errorf("registration observation accepted a foreign source: count=%d err=%v", len(rows), err)
			}
			if err := accepted.Done(); err != nil {
				t.Fatal(err)
			}
			h.stop(t)
			if rows, err := manager.RegistrationAccountHolders(ctx, ref, facts); len(rows) != 0 {
				t.Errorf("registration observation survived cutoff: count=%d err=%v", len(rows), err)
			}
		})
	}
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
			} else {
				var err error
				stores, err = storeselected.OpenRuntime(ctx, storeselected.RuntimeRequest{
					Selection:     storebackend.Selection{Backend: storebackend.BackendPostgres},
					PostgresDSN:   h.storeDSN,
					ProviderFacts: installedChannelFactsFor,
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() { closeUnactivatedSelectedStore(t, stores) })
			if _, err := initializeServePlatformStateStores(ctx, stores.Schema()); err != nil {
				t.Fatal(err)
			}
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
			var runtimeOptions deploymentRuntimeOptions
			factory := func(ctx context.Context, opts cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
				secrets = opts.Credentials
				opts.Identities = heldStopClaimStore{opts.Identities, entered, release}
				session, err := OpenChannelRegistration(ctx, opts)
				if err == nil {
					configureDeploymentRuntimeSession(session, runtimeOptions)
				}
				return session, err
			}
			client, err := cliapp.UseDeploymentChannelControl(ctx, root, h.opts.ConfigPath, directory, "telegram", stores, process, fixtureRegistration(factory))
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
			runtimeOptions = deploymentRuntimeOptions{manager: manager, beginWebhookClaim: s.beginWebhookClaim}
			handler := deploymentPendingClaimHandler(root, h.opts.ConfigPath, directory, stores, process, capability, fixtureRegistration(factory), runtimeOptions)
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
					var decoded any
					if err := canonicaljson.DecodePreservingNumberLexemes(body, &decoded); err != nil {
						return providertriggers.Delivery{}, err
					}
					return plan.Accept(providertriggers.Request{Provider: "telegram", Method: http.MethodPost, Body: body, Payload: decoded, ContentType: "application/json",
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
