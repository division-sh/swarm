//go:build linux || darwin

package serveapp

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/store/storetest"
	"gopkg.in/yaml.v3"
)

// The existing native ordered-input root also reaches the deployment parent.
// Only the encrypted external service is mocked; setup and actions stay public.
func runDeploymentWhatsAppOrderedInput(t *testing.T, backend string, held bool) {
	t.Helper()
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
	peer := newServeNativeProtocolPeer(t)
	peer.loginHandoff = true
	opts, location := servedNativeProcessOptions(t, backend)
	root := t.TempDir()
	copyReleaseFixtureTree(t, filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session-input"), root)
	opts.SourceRoot = root
	// An ordinary independent static flow contributes a real boot-stage card.
	// No public input is forced into a private child or a fabricated run.
	other := filepath.Join(root, "inbox-review")
	copyReleaseFixtureTree(t, filepath.Join(root, "reviews"), other)
	otherSchema, err := os.ReadFile(filepath.Join(other, "schema.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	otherText := strings.Replace(string(otherSchema), "  waiting: {}\n", "", 1)
	otherText = strings.Replace(otherText, "decision: review_decision", "decision: other_review_decision", 1)
	otherText = strings.Replace(otherText, "inputs: [work.requested]\n", "", 1)
	writeStandingCandidateFile(t, filepath.Join(other, "schema.yaml"), otherText)
	writeStandingCandidateFile(t, filepath.Join(other, "state.yaml"), "detail: \"Independent authorized review\"\n")
	if err := os.Remove(filepath.Join(other, "nodes.yaml")); err != nil {
		t.Fatal(err)
	}
	if held {
		connectorPath := filepath.Join(root, "whatsapp", "connector.yaml")
		connector, err := os.ReadFile(connectorPath)
		if err != nil {
			t.Fatal(err)
		}
		withAccount := strings.Replace(string(connector), "      properties:\n", "      properties:\n        account: {type: string, pattern: '^\\+[1-9][0-9]{1,14}$'}\n", 1)
		if withAccount == string(connector) {
			t.Fatal("temporary holder fixture lacks its declared connector input")
		}
		writeStandingCandidateFile(t, connectorPath, withAccount)
		writeStandingCandidateFile(t, filepath.Join(root, "nodes.yaml"), `customer:
  inbound.whatsapp.message:
    activity:
      id: customer_reply
      tool: whatsapp.send_text
      input:
        account: policy.operator_account
        destination: payload.conversation_reference
        text: payload.text
`)
	} else {
		if err := os.Remove(filepath.Join(root, "policy.yaml")); err != nil {
			t.Fatal(err)
		}
		rootSchema, err := os.ReadFile(filepath.Join(root, "schema.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		withoutCustomerIngress := strings.ReplaceAll(string(rootSchema), "  - inbound.whatsapp.raw\n", "")
		withoutCustomerIngress = strings.ReplaceAll(withoutCustomerIngress, "  - inbound.whatsapp.message\n", "")
		writeStandingCandidateFile(t, filepath.Join(root, "schema.yaml"), withoutCustomerIngress)
	}
	raw, err := os.ReadFile(opts.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]any
	if err := yaml.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	config["paths"] = map[string]any{"swarm_dir": opts.SwarmDir}
	config["channels"] = map[string]any{"hitl": map[string]any{"provider": "whatsapp", "account": "+15551234567"}}
	raw, err = yaml.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	writeStandingCandidateFile(t, opts.ConfigPath, string(raw))
	enableChannelOnboardingRecoveryOnStartup(t, opts.ConfigPath)
	qr := make(chan channelonboarding.Result, 1)
	factory := func(ctx context.Context, options cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
		session, err := OpenChannelRegistration(ctx, options)
		return firstConnectPairingObserver{ChannelRegistrationSession: session, principal: options.Principal,
			observe: func(result channelonboarding.Result, _ bool) {
				if result.Pairing != nil && result.Pairing.Code != "" {
					select {
					case qr <- result:
					default:
					}
				}
			}}, err
	}
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	out, errOut := &lockedBuffer{}, &lockedBuffer{}
	done := make(chan int, 1)
	progress := make(chan int, 1)
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-done
		}
	}()
	go func() {
		code := cliapp.Execute(ctx, []string{"--config", opts.ConfigPath, "channel", "connect", "whatsapp", "--yes"}, out, errOut, nil, nil, fixtureRegistration(factory))
		done <- code
		progress <- code
	}()
	select {
	case result := <-qr:
		peer.pair(result.Pairing.Code)
	case code := <-done:
		joined = true
		t.Fatalf("deployment connect before QR exit=%d err=%s", code, errOut.String())
	case <-ctx.Done():
		t.Fatal("deployment connect did not disclose its actual QR")
	}
	challenge := waitChannelOnboardingChallenge(t, out, errOut, progress)
	peer.claimMessage(challenge, "DEPLOYMENT_OPERATOR_CLAIM")
	select {
	case code := <-done:
		joined = true
		if code != 0 {
			t.Fatalf("deployment claim exit=%d out=%s err=%s", code, out.String(), errOut.String())
		}
	case <-ctx.Done():
		t.Fatalf("deployment claim did not finish: %s", errOut.String())
	}
	if !strings.Contains(out.String(), "is this deployment's HITL channel") {
		t.Fatal("public connect omitted deployment operator ceremony", out.String())
	}
	process := startServedNativeSDKProcess(t, opts, peer)
	endpoint := process.endpoint(t) + "/v1/rpc"
	peer.awaitPostLogin()
	var identity apiv1.RuntimeIdentityResult
	requireServedJSONRPCResult(t, endpoint, "runtime.identity", map[string]any{}, &identity)
	if len(identity.SourceArtifacts) != 1 {
		t.Fatal("deployment input requires one actual authored source")
	}
	bundleHash := identity.SourceArtifacts[0].BundleHash
	runID := requireServedNativePairingRun(t, endpoint, bundleHash)
	var listed, listErr lockedBuffer
	if code := cliapp.Execute(t.Context(), []string{"--config", opts.ConfigPath, "channel", "list", "--api-server", process.endpoint(t), "--json"}, &listed, &listErr, nil, nil, fixtureRegistration(factory)); code != 0 {
		t.Fatalf("deployment account list exit=%d err=%s", code, listErr.String())
	}
	var listing struct {
		Accounts []channelonboarding.Result `json:"accounts"`
	}
	if json.Unmarshal([]byte(listed.String()), &listing) != nil || len(listing.Accounts) != 1 || listing.Accounts[0].IdentityOperation == nil ||
		listing.Accounts[0].IdentityOperation.State != operatorchannel.StateBound || !listing.Accounts[0].Operation.DeploymentRegistration {
		t.Fatal("public account list lost its actual deployment claim", listed.String())
	}
	wantHolders := 0
	if held {
		wantHolders = 1
	}
	status := listing.Accounts[0].Status
	if status == nil || status.Provider != "whatsapp" || status.Account != listing.Accounts[0].Operation.DeploymentAccount() ||
		len(status.Holders) != wantHolders || held && status.Holders[0].Flow != "." {
		t.Fatal("public account row did not establish the exact held/unheld case", status)
	}
	reader, err := storetest.OpenChannelObservation(backend, location)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	seed := requireServedEventPublishRPCResult(t, endpoint, map[string]any{"run_id": runID, "bundle_hash": bundleHash,
		"event_name": "work.requested", "payload": map[string]any{"detail": "Deployment ordered input"}, "idempotency_key": "deployment-native-input"})
	var runs struct {
		Runs       []operatorread.RunHeader `json:"runs"`
		NextCursor string                   `json:"next_cursor"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.list", map[string]any{"limit": 100}, &runs)
	otherRunID := ""
	for _, run := range runs.Runs {
		if run.FlowPath == "inbox-review" && run.Status == "live" && run.BundleHash == bundleHash {
			if otherRunID != "" {
				t.Fatal("independent review has multiple live owners")
			}
			otherRunID = run.RunID
		}
	}
	if otherRunID == "" || runs.NextCursor != "" {
		t.Fatal("independent authored review has no exact live run")
	}
	firstCard := waitDeploymentReviewCard(t, endpoint, seed.RunID, "review_decision")
	otherCard := waitDeploymentReviewCard(t, endpoint, otherRunID, "other_review_decision")
	cardID, secondCardID := firstCard.ID, otherCard.ID
	if firstCard.RunID != seed.RunID || otherCard.RunID == firstCard.RunID || cardID == secondCardID {
		t.Fatal("independent authored reviews lack their own exact runs and cards", firstCard, otherCard)
	}
	messages := map[string]servedNativeMessage{}
	until := time.NewTimer(5 * time.Second)
	defer until.Stop()
	for len(messages) != 2 {
		select {
		case sent := <-peer.sent:
			for _, id := range []string{cardID, secondCardID} {
				if strings.Contains(sent.Body.GetConversation(), "\nReference: "+id+"\n") {
					messages[id] = sent
				}
			}
		case <-until.C:
			t.Fatal("both authored runs lacked genuine provider card messages")
		}
	}
	card, secondCard := messages[cardID], messages[secondCardID]
	peer.reply("reject", "DEPLOYMENT_INPUT_BEGIN", card)
	first := awaitServedNativeInputMessage(t, peer, "Input: Reason (text) required")
	peer.reply("private-native-reason", "DEPLOYMENT_INPUT_REASON", first)
	second := awaitServedNativeInputMessage(t, peer, "Input: Count (integer) required")
	requireServedNativeInputCard(t, endpoint, cardID, false)
	if err := process.stop(); err != nil {
		t.Fatal("deployment input stop did not join", err, process.output.String())
	}
	requireServedNativeDisconnect(t, peer)
	process = startServedNativeSDKProcess(t, opts, peer)
	endpoint = process.endpoint(t) + "/v1/rpc"
	peer.awaitPostLogin()
	requireServedNativeInputCard(t, endpoint, cardID, false)
	peer.reply("27", "DEPLOYMENT_INPUT_COUNT", second)
	optional := awaitServedNativeInputMessage(t, peer, "Input: Note (text)")
	peer.reply("Skip field", "DEPLOYMENT_INPUT_SKIP", optional)
	decided := waitServedNativeInputDecision(t, endpoint, cardID)
	if decided == "" {
		t.Fatal("deployment input lacked its canonical decision")
	}
	var decisions operatorread.OperatorEventListResult
	requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{"filter": map[string]any{
		"run_id": seed.RunID, "event_name": "mailbox.card_decided"}, "limit": 10}, &decisions)
	if len(decisions.Events) != 1 || decisions.Events[0].EventID != decided {
		t.Fatal("deployment input duplicated or crossed card run authority")
	}
	peer.text("/inbox", "DEPLOYMENT_ONE_INBOX")
	requireServedNativeInboxReply(t, endpoint, peer, reader, root)
	if pending := waitDeploymentReviewCard(t, endpoint, otherRunID, "other_review_decision"); pending.ID != secondCardID {
		t.Fatal("the shared deployment Inbox replaced the second run's card")
	}
	peer.reply("approve", "DEPLOYMENT_SECOND_RUN_APPROVE", secondCard)
	deadline := time.Now().Add(5 * time.Second)
	for {
		var current struct {
			Card struct {
				RunID   string `json:"run_id"`
				Status  string `json:"status"`
				Verdict string `json:"verdict"`
			} `json:"decision_card"`
		}
		// The public card owns its run; the transport account never supplies it.
		requireServedJSONRPCResult(t, endpoint, "mailbox.get", map[string]any{"mailbox_id": secondCardID}, &current)
		detail := current.Card
		if detail.Status == "decided" {
			if detail.RunID != otherCard.RunID || detail.Verdict != "approve" || requireServedNativeInputCard(t, endpoint, cardID, true) != decided {
				t.Fatal("deployment action crossed the two cards' run authority")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("second run's real card did not accept its own quoted action")
		}
		time.Sleep(10 * time.Millisecond)
	}
	requireDeploymentEffectActivity(t, reader, "channel_delivery")
	var loggedOut, logoutErr lockedBuffer
	if code := cliapp.Execute(t.Context(), []string{"--config", opts.ConfigPath, "channel", "logout", "whatsapp",
		listing.Accounts[0].Operation.DeploymentAccount(), "--api-server", process.endpoint(t), "--json"},
		&loggedOut, &logoutErr, nil, nil, fixtureRegistration(factory)); code != 0 {
		t.Fatalf("deployment logout exit=%d err=%s", code, logoutErr.String())
	}
	var retired channelonboarding.Result
	if json.Unmarshal([]byte(loggedOut.String()), &retired) != nil ||
		retired.Operation.OperationID != listing.Accounts[0].Operation.OperationID || !retired.Operation.LogoutCompleted {
		t.Fatal("deployment logout did not retire the exact original registration")
	}
	requireServedNativeDisconnect(t, peer)
	plans, err := reader.ListCurrentChannelDeliveryPlans(t.Context(), "", 200)
	if err != nil || len(plans) != 0 {
		t.Fatal("logged-out deployment parent retained executable delivery plans", err, plans)
	}
	if err := process.stop(); err != nil {
		t.Fatal("deployment input shutdown", err)
	}
	requireServedNativeDisconnect(t, peer)
}

type deploymentReviewCard struct {
	ID       string `json:"card_id"`
	RunID    string `json:"run_id"`
	Decision string `json:"decision"`
}

func waitDeploymentReviewCard(t *testing.T, endpoint, runID, decision string) deploymentReviewCard {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var page struct {
			Items []struct {
				Card deploymentReviewCard `json:"decision_card"`
			} `json:"items"`
		}
		requireServedJSONRPCResult(t, endpoint, "mailbox.list", map[string]any{"status": "pending", "run_id": runID, "limit": 200}, &page)
		var selected deploymentReviewCard
		for _, item := range page.Items {
			if item.Card.Decision == decision && item.Card.RunID == runID {
				if selected.ID != "" {
					t.Fatal("authored decision selector is ambiguous", decision)
				}
				selected = item.Card
			}
		}
		if selected.ID != "" && selected.RunID != "" {
			return selected
		}
		if time.Now().After(deadline) {
			t.Fatal("authored decision has no public pending card", decision)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func requireDeploymentEffectActivity(t *testing.T, reader storetest.ChannelObservation, kinds ...string) {
	t.Helper()
	wanted := make(map[string]bool, len(kinds))
	for _, kind := range kinds {
		wanted[kind] = false
	}
	cursor := int64(0)
	for {
		page, err := reader.ListAuthorActivity(t.Context(), authoractivity.ListOptions{AfterSequence: cursor, Limit: 500})
		if err != nil {
			t.Fatal("deployment effect typed readback", err)
		}
		for _, occurrence := range page.Occurrences {
			if occurrence.Kind != authoractivity.KindEffectLifecycle {
				continue
			}
			if _, selected := wanted[occurrence.Projection.AuthorityKind]; !selected {
				continue
			}
			if occurrence.Scope.Kind != authoractivity.ScopeRuntime || occurrence.Scope.BundleHash != "" {
				t.Fatal("deployment lifecycle borrowed an author bundle")
			}
			raw, err := json.Marshal(occurrence)
			if err != nil || strings.Contains(string(raw), "bundle_hash") || strings.Contains(string(raw), "EffectAuthorityEvidence") {
				t.Fatal("typed deployment activity invented bundle or public provenance", err)
			}
			if _, selected := wanted[occurrence.Projection.AuthorityKind]; selected {
				wanted[occurrence.Projection.AuthorityKind] = true
			}
		}
		if len(page.Occurrences) < 500 {
			break
		}
		if page.NextCursor <= cursor {
			t.Fatal("deployment activity cursor did not advance")
		}
		cursor = page.NextCursor
	}
	for kind, observed := range wanted {
		if !observed {
			t.Fatalf("deployment %s has no exact bundle-free lifecycle readback", kind)
		}
	}
}
