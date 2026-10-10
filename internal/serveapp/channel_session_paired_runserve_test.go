//go:build linux || darwin

package serveapp

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/store/sessionstate"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
)

func TestRunServeWhatsAppSignedPairingBothStores(t *testing.T) {
	testRunServeWhatsAppSignedPairing(t, false)
}

func TestRunServeWhatsAppQuotedCardDecisionBothStores(t *testing.T) {
	testRunServeWhatsAppSignedPairing(t, true)
}

func testRunServeWhatsAppSignedPairing(t *testing.T, quotedRetirement bool) {
	t.Helper()
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
			peer := newServeNativeProtocolPeer(t)
			opts := cliapp.ServeOptions{SourceRoot: filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session"),
				PlatformSpecPath: defaultPlatformSpecPath, APIListenAddr: "127.0.0.1:0", MCPListenAddr: "127.0.0.1:0",
				SwarmDir: t.TempDir(), SwarmDirSet: true, SelfCheck: true,
				WorkspaceBackend: "host", WorkspaceBackendSet: true, StoreMode: backend, StoreModeSet: true}
			if err := os.Chmod(opts.SwarmDir, 0o700); err != nil {
				t.Fatal(err)
			}
			var activationCommitted atomic.Bool
			opts.TestChannelOnboardingBarrier = func(boundary channelonboarding.TestLifecycleBoundary, _ string) error {
				if boundary == channelonboarding.TestAfterActivationCommitBeforePublication {
					activationCommitted.Store(true)
					return errors.New("interrupt served native activation before process publication")
				}
				return nil
			}
			storeDSN := ""
			if backend == "sqlite" {
				storeDSN = filepath.Join(t.TempDir(), "paired-serve.sqlite")
				opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend,
					storeDSN, channelOnboardingHostWorkspaceFields())
			} else {
				storeDSN = testutil.StartEmptyPostgresDSN(t)
				opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, storeDSN)
			}
			process := startServeRuntimeTestProcess(t, opts)
			process.waitForReadyLine()
			endpoint := "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			var result channelonboarding.Result
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_start", map[string]any{
				"provider": "whatsapp", "verb": "connect", "save_proof": false, "idempotency_key": "native-pairing"}, &result)
			id := result.Operation.OperationID
			deadline := time.Now().Add(5 * time.Second)
			for result.Pairing == nil || result.Pairing.Code == "" {
				if time.Now().After(deadline) {
					t.Fatal("served owner did not disclose authorized QR", result.Operation.Phase, result.Pairing)
				}
				time.Sleep(10 * time.Millisecond)
				requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
			}
			peer.pair(result.Pairing.Code)
			_, initialPrekeyUploads := peer.awaitPostLogin()
			result = channelonboarding.Result{}
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id}, &result)
			if result.Operation.Phase != channelonboarding.PhaseAwaitingExternalIdentity || result.IdentityOperation == nil ||
				result.Pairing != nil || result.Operation.Coordinate.TargetGeneration != 0 {
				t.Fatal("signed pairing did not reach the original runless operator claim", result)
			}
			identityID := result.IdentityOperation.OperationID
			from := peer.claim(result.IdentityOperation.Challenge)
			previous := result.Operation.Coordinate
			if code := process.stop(); code != 0 {
				t.Fatal("paired RunServe did not join before recovery", code, process.outputString())
			}
			process = startServeRuntimeTestProcess(t, opts)
			process.waitForReadyLine()
			endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			postLogins, uploads := peer.awaitPostLogin()
			if postLogins < 2 || uploads != initialPrekeyUploads {
				t.Fatal("normal restart lost provider prekey state or skipped post-login", postLogins, uploads, initialPrekeyUploads)
			}
			result = channelonboarding.Result{}
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id}, &result)
			if result.Operation.Phase != channelonboarding.PhaseAwaitingOperatorConfirmation || result.IdentityOperation == nil ||
				result.IdentityOperation.OperationID != identityID ||
				result.IdentityOperation.State != operatorchannel.StateAwaitingConfirmation ||
				result.IdentityOperation.ExternalAccountRef != operatorchannel.MaskPresentation(from.String()) || result.Operation.Coordinate.TargetGeneration != 0 ||
				result.Operation.Coordinate.RuntimeInstanceID == previous.RuntimeInstanceID || result.Operation.Coordinate.BundleHash != previous.BundleHash {
				t.Fatal("encrypted claim did not reach authenticated confirmation without a business target", result.Operation, result.IdentityOperation, "want sender", from.String())
			}
			peer.mu.Lock()
			connections := peer.connections
			peer.mu.Unlock()
			if connections < 2 {
				t.Fatal("ordinary restart did not restore a genuine SDK connection", connections)
			}
			var confirmed struct {
				Operation operatorchannel.Operation `json:"operation"`
				Binding   operatorchannel.Binding   `json:"binding"`
			}
			requireServedJSONRPCResult(t, endpoint, "channel.confirm", map[string]any{
				"operation_id": result.IdentityOperation.OperationID, "expected_revision": result.IdentityOperation.Revision, "approve": true}, &confirmed)
			if confirmed.Operation.State != operatorchannel.StateBound || confirmed.Binding.Status != operatorchannel.BindingCurrent {
				t.Fatal("public confirmation did not commit the exact encrypted claimant", confirmed)
			}
			if confirmed.Binding.ExternalAccountRef != from.String() || confirmed.Binding.ConversationRef != from.String() {
				t.Fatal("confirmed binding differs from the genuine encrypted claimant", confirmed.Binding)
			}
			interrupted := requestServedJSONRPC(t, endpoint, "channel.onboarding_retry", map[string]any{"operation_id": id})
			if interrupted.Error == nil || !activationCommitted.Load() {
				t.Fatal("native target/activation did not reach the deliberate publication interruption", interrupted)
			}
			result = channelonboarding.Result{}
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
			if result.Operation.Phase != channelonboarding.PhasePublishingProcessActivation || result.Operation.Coordinate.TargetGeneration < 1 ||
				result.Operation.BindingRevision != confirmed.Binding.Revision || result.Operation.ActivationRevision < 1 {
				t.Fatal("served native target and activation were not committed through their canonical owners", result.Operation)
			}
			if code := process.stop(); code != 0 {
				t.Fatal("paired RunServe shutdown failed", code, process.outputString())
			}
			opts.TestChannelOnboardingBarrier = nil
			process = startServeRuntimeTestProcess(t, opts)
			process.waitForReadyLine()
			endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			result = channelonboarding.Result{}
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
			if result.Operation.Phase != channelonboarding.PhaseSucceeded || result.Readiness == nil || !result.Readiness.Ready {
				t.Fatal("interrupted native activation did not recover to executable readiness", result)
			}
			select {
			case sent := <-peer.sent:
				if sent.ID != strings.ReplaceAll(result.Operation.ConfirmationOperationID, "-", "") || sent.Body.GetConversation() != "Swarm channel connected." {
					t.Fatal("served confirmation journal differs from decrypted provider output", sent)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("ready native channel had no genuine decrypted confirmation send")
			}
			if code := process.stop(); code != 0 {
				t.Fatal("enabled RunServe shutdown failed", code, process.outputString())
			}
			completed := result.Operation
			process = startServeRuntimeTestProcess(t, opts)
			process.waitForReadyLine()
			endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			result = channelonboarding.Result{}
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
			if result.Operation.Phase != channelonboarding.PhaseSucceeded || result.Readiness == nil || !result.Readiness.Ready ||
				result.Operation.IdentityOperationID != identityID || result.Operation.BindingRevision != completed.BindingRevision ||
				result.Operation.Coordinate.TargetGeneration != completed.Coordinate.TargetGeneration ||
				result.Operation.Coordinate.RuntimeInstanceID == completed.Coordinate.RuntimeInstanceID {
				t.Fatal("completed native restart lost its original responsibility", result.Operation, result.Readiness)
			}
			select {
			case sent := <-peer.sent:
				t.Fatal("completed confirmation was resent during ordinary restart", sent.ID)
			default:
			}
			reader, err := storetest.OpenChannelObservation(backend, storeDSN)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := reader.Close(); err != nil {
					t.Error(err)
				}
			})
			if quotedRetirement {
				peer.text("Please review the service", "SERVED_RETIRE_TRIGGER")
				waitServedNativePendingCard(t, endpoint)
			}
			peer.text("/inbox", "SERVED_INBOX")
			observation := render.IntentObservationQuery{Kind: render.IntentText, Provider: "whatsapp",
				MessageReference: "SERVED_INBOX", ConversationReference: from.String(), InterfaceKey: result.Operation.Interface.Key()}
			waitTextReplyIntent(t, reader, observation, "entry")
			requireNativeIntentObservationScope(t, reader, observation)
			cards := requireServedNativeInboxReply(t, endpoint, peer, reader)
			if quotedRetirement {
				requireServedNativeQuotedRetirement(t, endpoint, peer, reader, observation, cards)
				if code := process.stop(); code != 0 {
					t.Fatal("quoted retirement RunServe did not join", code, process.outputString())
				}
				return
			}
			pairingFiles, err := filepath.Glob(filepath.Join(opts.SwarmDir, "*", "session.json"))
			if err != nil || len(pairingFiles) != 1 {
				t.Fatal("single actual pairing owner has no exact retained header", pairingFiles, err)
			}
			pairingPath := pairingFiles[0]
			pairingHeader, err := os.ReadFile(pairingPath)
			if err != nil {
				t.Fatal(err)
			}
			var unbound struct {
				Binding operatorchannel.Binding `json:"binding"`
			}
			for range 2 {
				requireServedJSONRPCResult(t, endpoint, "channel.unbind", map[string]any{
					"interface": result.Operation.Interface.Selector, "expected_revision": confirmed.Binding.Revision,
					"idempotency_key": "served-native-unbind"}, &unbound)
				if unbound.Binding.Status == operatorchannel.BindingCurrent || unbound.Binding.Revision <= confirmed.Binding.Revision {
					t.Fatal("public unbind retained executable principal authority", unbound.Binding)
				}
			}
			result = channelonboarding.Result{}
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
			if result.Operation.Phase != channelonboarding.PhaseSucceeded || result.Operation.ConfirmationOperationID != completed.ConfirmationOperationID ||
				result.Readiness == nil || result.Readiness.Ready || result.Pairing != nil {
				t.Fatal("public native unbind lost historical success or retained execution/pairing disclosure", result)
			}
			connected := requireServedNativeDisconnect(t, peer)
			if code := process.stop(); code != 0 {
				t.Fatal("retained enabled RunServe shutdown failed", code, process.outputString())
			}
			process = startServeRuntimeTestProcess(t, opts)
			process.waitForReadyLine()
			endpoint = "http://" + serveRuntimeAPIListenerFromOutput(t, process.outputString()) + "/v1/rpc"
			result = channelonboarding.Result{}
			requireServedJSONRPCResult(t, endpoint, "channel.onboarding_get", map[string]any{"operation_id": id}, &result)
			if result.Operation.Phase != channelonboarding.PhaseSucceeded || result.Operation.ConfirmationOperationID != completed.ConfirmationOperationID ||
				result.Readiness == nil || result.Readiness.Ready {
				t.Fatal("ordinary restart lost history or adopted the unbound native responsibility", result)
			}
			peer.mu.Lock()
			unchanged := peer.connections == connected
			peer.mu.Unlock()
			if !unchanged {
				t.Fatal("ordinary restart reconnected an explicitly retired parent")
			}
			if code := process.stop(); code != 0 {
				t.Fatal("post-unbind dormant RunServe shutdown failed", code, process.outputString())
			}
			after, err := os.ReadFile(pairingPath)
			if err != nil || !bytes.Equal(pairingHeader, after) {
				t.Fatal("unbind/restart deleted or rewrote retained v1 pairing identity", err)
			}
			fixture, providerState, err := sessionstate.OpenSDKFixture(context.Background(), filepath.Join(filepath.Dir(pairingPath), "provider.db"))
			if err != nil {
				t.Fatal(err)
			}
			accountRef := peer.account.ToNonAD().String()
			device, err := providerState.CurrentDevice(context.Background(), accountRef)
			closeErr := fixture.Close()
			if err != nil || closeErr != nil || device == nil || device.ID == nil || device.ID.ToNonAD().String() != accountRef {
				t.Fatal("public unbind deleted or adopted the SDK-owned paired account", err, closeErr)
			}
			select {
			case sent := <-peer.sent:
				t.Fatal("served Inbox or confirmation was resent without another request", sent.ID)
			default:
			}
		})
	}
}

func requireNativeIntentObservationScope(t *testing.T, reader storetest.ChannelObservation, exact render.IntentObservationQuery) {
	t.Helper()
	for name, mutate := range map[string]func(*render.IntentObservationQuery){
		"provider":     func(q *render.IntentObservationQuery) { q.Provider = "telegram" },
		"message":      func(q *render.IntentObservationQuery) { q.MessageReference = "FOREIGN_MESSAGE" },
		"conversation": func(q *render.IntentObservationQuery) { q.ConversationReference = "foreign@s.whatsapp.net" },
		"interface":    func(q *render.IntentObservationQuery) { q.InterfaceKey = "foreign" },
	} {
		changed := exact
		mutate(&changed)
		if _, found, err := reader.ObserveChannelIntent(context.Background(), changed); err != nil || found {
			t.Fatal("read-only native intent lookup crossed its exact scope", name, found, err)
		}
	}
	for name, mutate := range map[string]func(*render.IntentObservationQuery){
		"dual selector":        func(q *render.IntentObservationQuery) { q.ProviderEventID = "invented-publication-identity" },
		"missing conversation": func(q *render.IntentObservationQuery) { q.ConversationReference = "" },
		"wrong kind":           func(q *render.IntentObservationQuery) { q.Kind = render.IntentAction },
	} {
		changed := exact
		mutate(&changed)
		if _, found, err := reader.ObserveChannelIntent(context.Background(), changed); err == nil || found {
			t.Fatal("read-only native intent lookup accepted an ambiguous selector", name, found, err)
		}
	}
}

func requireServedNativeDisconnect(t *testing.T, peer *serveNativeProtocolPeer) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		peer.mu.Lock()
		connected, disconnected := peer.connections, peer.disconnected
		peer.mu.Unlock()
		if connected == disconnected {
			return connected
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("public unbind did not close its original SDK wire")
	return 0
}

func requireServedNativeInboxReply(t *testing.T, endpoint string, peer *serveNativeProtocolPeer, reader storetest.ChannelObservation) []servedNativeMessage {
	t.Helper()
	var sent servedNativeMessage
	var cards []servedNativeMessage
	deadline := time.After(5 * time.Second)
waiting:
	for {
		select {
		case sent = <-peer.sent:
			if strings.HasPrefix(sent.Body.GetConversation(), "Inbox\n") {
				break waiting
			}
			cards = append(cards, sent)
		case <-deadline:
			t.Fatal("served encrypted Inbox request did not produce a decrypted reply", cards)
		}
	}
	var page struct {
		Items []struct {
			Kind string `json:"kind"`
			Card struct {
				ID    string `json:"card_id"`
				Title string `json:"title"`
			} `json:"decision_card"`
			Notice struct {
				Type string `json:"type"`
			} `json:"notice"`
		} `json:"items"`
		Unread     int    `json:"unread_informational_notices"`
		NextCursor string `json:"next_cursor"`
	}
	requireServedJSONRPCResult(t, endpoint, "mailbox.list", map[string]any{"status": "pending", "limit": 200}, &page)
	if page.NextCursor != "" || page.Unread != 0 {
		t.Fatal("served baseline Inbox has unexpected notices or pages", page)
	}
	lines := []string{"Inbox", "Unread notices: 0", "No open items"}
	if len(page.Items) > 0 {
		lines[2] = "Open items:"
		for _, item := range page.Items {
			label := item.Card.Title
			if item.Kind == "notice" {
				label = item.Notice.Type
			}
			if label == "" {
				t.Fatal("public Inbox entry lost its canonical label", item)
			}
			lines = append(lines, "- "+label)
		}
	}
	for _, card := range cards {
		matched := false
		for _, item := range page.Items {
			if item.Kind == "decision_card" && strings.HasPrefix(card.Body.GetConversation(), item.Card.Title+"\n") &&
				strings.Contains(card.Body.GetConversation(), "\nReference: "+item.Card.ID) {
				matched = true
			}
		}
		if !matched {
			t.Fatal("served peer emitted output outside the public pending cards/Inbox", card.Body.GetConversation())
		}
	}
	settlementDeadline := time.Now().Add(5 * time.Second)
	var last []render.Candidate
	for time.Now().Before(settlementDeadline) {
		plans, err := reader.ListCurrentChannelDeliveryPlans(context.Background(), "", 200)
		last = plans
		if err != nil {
			t.Fatal(err)
		}
		for _, plan := range plans {
			if plan.SourceKind != "response" || plan.State != "sent" {
				continue
			}
			receipt, found, err := reader.GetCurrentChannelSentReceipt(context.Background(), plan.DeliveryID, plan.CurrentReceiptID)
			if err != nil {
				t.Fatal(err)
			}
			if !found || receipt.DeliveryReference != sent.ID {
				continue
			}
			want := strings.Join(lines, "\n") + "\nReference: " + plan.SourceID
			if sent.Body.GetConversation() != want || sent.ID != strings.ReplaceAll(receipt.OperationID, "-", "") {
				t.Fatal("decrypted native Inbox differs from canonical public list/exact journal", sent.Body.GetConversation(), want, receipt)
			}
			return cards
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("decrypted native Inbox had no exact settled delivery receipt", sent.ID, sent.Body.GetConversation(), last)
	return nil
}

func requireServedNativeQuotedRetirement(t *testing.T, endpoint string, peer *serveNativeProtocolPeer,
	reader storetest.ChannelObservation, observation render.IntentObservationQuery, cards []servedNativeMessage,
) {
	t.Helper()
	cardID := waitServedNativePendingCard(t, endpoint)
	var selected servedNativeMessage
	for _, card := range cards {
		if strings.Contains(card.Body.GetConversation(), "\nReference: "+cardID+"\n") {
			if selected.ID != "" {
				t.Fatal("served baseline has multiple retirement cards")
			}
			selected = card
		}
	}
	if selected.ID == "" {
		select {
		case selected = <-peer.sent:
		case <-time.After(5 * time.Second):
			plans, err := reader.ListCurrentChannelDeliveryPlans(context.Background(), "", 200)
			t.Fatal("served native retirement card was not delivered", plans, err)
		}
	}
	retirementChoice := false
	for _, line := range strings.Split(selected.Body.GetConversation(), "\n") {
		if label, action := strings.CutPrefix(line, "Action: "); action && render.MatchesTextControl(label, "Retire") {
			retirementChoice = true
		}
	}
	if !strings.Contains(selected.Body.GetConversation(), "\nReference: "+cardID+"\n") || !retirementChoice {
		t.Fatal("served peer did not receive the actual retirement choice", selected.Body.GetConversation())
	}
	var detail struct {
		Card struct {
			ID      string `json:"card_id"`
			Status  string `json:"status"`
			Verdict string `json:"verdict"`
		} `json:"decision_card"`
	}
	requireServedJSONRPCResult(t, endpoint, "mailbox.get", map[string]any{"mailbox_id": cardID}, &detail)
	if detail.Card.ID != cardID || detail.Card.Status == "decided" {
		t.Fatal("delivered retirement card was not publicly pending", detail)
	}
	peer.reply("Retire", "SERVED_QUOTED_RETIRE", selected)
	observation.MessageReference = "SERVED_QUOTED_RETIRE"
	waitTextReplyIntent(t, reader, observation, "control")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		requireServedJSONRPCResult(t, endpoint, "mailbox.get", map[string]any{"mailbox_id": cardID}, &detail)
		if detail.Card.ID == cardID && detail.Card.Status == "decided" && detail.Card.Verdict == "retire" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("durable encrypted reply did not decide the exact public retirement card", detail)
}

func waitServedNativePendingCard(t *testing.T, endpoint string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var page struct {
			Items []struct {
				Kind string `json:"kind"`
				Card struct {
					ID string `json:"card_id"`
				} `json:"decision_card"`
			} `json:"items"`
		}
		requireServedJSONRPCResult(t, endpoint, "mailbox.list", map[string]any{"status": "pending", "limit": 200}, &page)
		if len(page.Items) == 1 && page.Items[0].Kind == "decision_card" && page.Items[0].Card.ID != "" {
			return page.Items[0].Card.ID
		}
		if len(page.Items) > 1 {
			t.Fatal("native quoted-action proof has ambiguous pending cards", page)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("genuine encrypted business input did not reach its authored stage gate")
	return ""
}
