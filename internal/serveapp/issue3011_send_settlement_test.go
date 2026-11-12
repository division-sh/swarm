//go:build linux || darwin

package serveapp

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/tests/support/testutil"
	"gopkg.in/yaml.v3"
)

// Diagnostic overlay only. Public authored input, no row setup or admission hook.
func TestRegisteredWhatsAppPublicInputDownLinkSettlesBeforeActivityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		if !t.Run(backend, func(t *testing.T) { issue3011SendSettlement(t, backend) }) {
			t.Fatal("first diagnostic/setup failure; later backend not run")
		}
	}
}

func issue3011SendSettlement(t *testing.T, backend string) {
	isolateCLIAPIConfigEnv(t)
	t.Setenv("SWARM_CREDENTIALS_FILE", filepath.Join(t.TempDir(), "credentials.json"))
	peer := newServeNativeProtocolPeer(t)
	opts := cliapp.DefaultServeOptions()
	opts.SourceRoot = filepath.Join(t.TempDir(), "source")
	copyReleaseFixtureTree(t, filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session-reply"), opts.SourceRoot)
	issue3011NoticeSource(t, opts.SourceRoot)
	opts.SwarmDir, opts.SwarmDirSet = t.TempDir(), true
	if err := os.Chmod(opts.SwarmDir, 0o700); err != nil {
		t.Fatal(err)
	}
	opts.StoreMode, opts.StoreModeSet = backend, true
	if backend == "sqlite" {
		opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, filepath.Join(t.TempDir(), "diagnostic.sqlite"), channelOnboardingHostWorkspaceFields())
	} else {
		opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, testutil.StartEmptyPostgresDSN(t))
	}
	config, err := os.ReadFile(opts.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	writeStandingCandidateFile(t, opts.ConfigPath, string(config)+fmt.Sprintf("\npaths:\n  swarm_dir: %q\n", opts.SwarmDir))
	enableChannelOnboardingRecoveryOnStartup(t, opts.ConfigPath)
	root := filepath.Join(os.Getenv("TEST_ISSUE3011_OUT"), backend)
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	record := map[string]any{"backend": backend, "source": os.Getenv("TEST_ISSUE3011_PIN"),
		"surface": "exact-pin Go test binary plus test-only authored-source overlay; public Go CLI registration and public RPC input; existing peer-trust RunServe subprocess, NOT published CLI/provider handshake",
		"started": time.Now().UTC(), "complete": false, "stage": "setup"}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Open(self)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.New()
	_, readErr := io.Copy(hash, binary)
	closeErr := binary.Close()
	if readErr != nil || closeErr != nil {
		t.Fatal(readErr, closeErr)
	}
	record["test_binary_sha256"] = fmt.Sprintf("%x", hash.Sum(nil))
	for _, file := range []string{"schema.yaml", "nodes.yaml", "policy.yaml", "events.yaml"} {
		raw, err := os.ReadFile(filepath.Join(opts.SourceRoot, file))
		if err != nil {
			t.Fatal(err)
		}
		record[file+"_sha256"] = fmt.Sprintf("%x", sha256.Sum256(raw))
		if err := os.WriteFile(filepath.Join(root, file), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var process *channelOnboardingCrashServeProcess
	var logs []string
	stop := func() {
		t.Helper()
		if process == nil {
			return
		}
		joined := process.stop()
		logs = append(logs, process.output.String())
		process = nil
		joins, _ := record["serve_joins"].([]string)
		record["serve_joins"] = append(joins, fmt.Sprint(joined))
		if joined != nil {
			t.Error("owned serve join", joined)
		}
	}
	defer func() {
		stop()
		record["ended"] = time.Now().UTC()
		raw, err := json.MarshalIndent(record, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(root, "receipt.json"), append(raw, '\n'), 0o600)
		}
		if err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(filepath.Join(root, "serve.log"), []byte(strings.Join(logs, "\n--- owned process boundary ---\n")), 0o600); err != nil {
			t.Error(err)
		}
		t.Logf("backend=%s stage=%s complete=%v", backend, record["stage"], record["complete"])
	}()

	// The negative is a fresh unregistered store, not a fabricated missing row.
	record["stage"] = "unlinked public-input control"
	process = startServedNativeSDKProcess(t, opts, peer)
	endpoint := process.endpoint(t) + "/v1/rpc"
	unlinked := issue3011PublishAndSettle(t, endpoint, "unlinked", record)
	record["unlinked"] = unlinked
	if len(unlinked.Deliveries) != 1 || unlinked.Deliveries[0].Failure == nil {
		t.Fatal("unlinked input did not reach one failed delivery")
	}
	failure := unlinked.Deliveries[0].Failure
	if failure.Detail.Code != "channel_account_not_linked" || !strings.HasSuffix(failure.Message, " is not linked: run swarm channel connect whatsapp and scan it") {
		t.Fatal("unlinked input reached a different refusal", failure)
	}
	stop()
	if t.Failed() {
		return
	}

	record["stage"] = "public CLI registration"
	op := issue3011RegisterForSend(t, opts, peer)
	record["operation_id"], record["account"] = op.OperationID, op.AccountReference
	process = startServedNativeSDKProcess(t, opts, peer)
	endpoint = process.endpoint(t) + "/v1/rpc"
	peer.awaitPostLogin()
	before := issue3011RegisteredStatus(t, opts, endpoint, op)
	record["before"] = before
	if before.Status.State != "live" || before.Status.Health != "connected" || len(before.Status.Holders) != 1 || !before.Status.Linked {
		t.Fatal("registered setup is not healthy and single-holder")
	}
	record["stage"] = "provider-side socket close"
	record["socket_close_at"] = time.Now().UTC()
	peer.disconnect()
	var down channelonboarding.Result
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		down = issue3011RegisteredStatus(t, opts, endpoint, op)
		if down.Status.Health == "disconnected" && down.Status.Reason != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	record["down"] = down
	if down.Status.Health != "disconnected" || down.Status.Reason == "" {
		t.Fatal("socket cut did not produce the required disconnected state")
	}
	peer.mu.Lock()
	record["peer_after_cut"] = map[string]int{"accepts": peer.connections, "closed": peer.disconnected, "post_logins": peer.postLogins}
	peer.mu.Unlock()
	record["stage"] = "down-link public-input settlement"
	settled := issue3011PublishAndSettle(t, endpoint, "down", record)
	record["down_input"] = settled
	if len(settled.Deliveries) != 1 || settled.Deliveries[0].Failure == nil || settled.Deliveries[0].Failure.Detail.Code != "unclassified_runtime_error" {
		t.Fatal("down-link input reached a different cut than the untyped pre-activity failure")
	}
	if t.Failed() {
		return
	}
	stop()
	if t.Failed() {
		return
	}
	record["stage"] = "explicit existing startup restore"
	process = startServedNativeSDKProcess(t, opts, peer)
	endpoint = process.endpoint(t) + "/v1/rpc"
	peer.awaitPostLogin()
	restored := issue3011RegisteredStatus(t, opts, endpoint, op)
	record["restored"] = restored
	if restored.Status.State != "live" || restored.Status.Health != "connected" || len(restored.Status.Holders) != 1 {
		t.Fatal("existing startup path did not restore the exact registered account")
	}
	var after operatorread.OperatorEventFull
	requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": settled.EventID}, &after)
	record["down_input_after_restore"] = after
	if len(after.Deliveries) != 1 || after.Deliveries[0].DeliveryID != settled.Deliveries[0].DeliveryID || after.Deliveries[0].Status != settled.Deliveries[0].Status {
		t.Fatal("terminal input changed during explicit restoration")
	}
	issue3011NoActivity(t, endpoint, settled.RunID, record, "after_restore_activity_requests")
	peer.mu.Lock()
	record["peer_after_restore"] = map[string]int{"accepts": peer.connections, "closed": peer.disconnected, "post_logins": peer.postLogins}
	peer.mu.Unlock()
	record["stage"], record["complete"] = "complete", true
}

func issue3011NoticeSource(t *testing.T, root string) {
	t.Helper()
	for _, name := range []string{"schema.yaml", "nodes.yaml"} {
		path := filepath.Join(root, name)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var document map[string]any
		if err := yaml.Unmarshal(raw, &document); err != nil {
			t.Fatal(err)
		}
		if name == "schema.yaml" {
			inputs, ok := document["inputs"].([]any)
			if !ok {
				t.Fatal("source inputs are not a declared list")
			}
			document["inputs"] = append(inputs, "notice.requested")
		} else {
			node := document["customer-reply"].(map[string]any)
			node["notice.requested"] = map[string]any{"activity": map[string]any{"id": "notice_delivery", "tool": "whatsapp.send_text",
				"input": map[string]any{"account": "policy.operator_account", "destination": "payload.destination", "text": "payload.text"}}}
		}
		raw, err = yaml.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "events.yaml"), []byte("notice.requested:\n  destination: string\n  text: string\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func issue3011PublishAndSettle(t *testing.T, endpoint, key string, record map[string]any) operatorread.OperatorEventFull {
	t.Helper()
	node, err := identity.ParseExecutableNode(".", "customer-reply")
	if err != nil {
		t.Fatal(err)
	}
	var runs struct {
		Runs []operatorread.RunHeader `json:"runs"`
	}
	requireServedJSONRPCResult(t, endpoint, "run.list", map[string]any{"limit": 100}, &runs)
	var selected []operatorread.RunHeader
	for _, run := range runs.Runs {
		if run.FlowPath == "." && run.Status == "live" {
			selected = append(selected, run)
		}
	}
	if len(selected) != 1 {
		record[key+"_runs"] = runs
		t.Fatal("public input requires one exact live root")
	}
	run := selected[0]
	params := map[string]any{"event_name": "notice.requested", "run_id": run.RunID, "bundle_hash": run.BundleHash,
		"payload": map[string]any{"destination": "15557654321@s.whatsapp.net", "text": "synthetic diagnostic notice"}, "idempotency_key": "issue3011-" + key}
	record[key+"_public_input_params"] = params
	publication := requireServedEventPublishRPCResult(t, endpoint, params)
	record[key+"_public_input_result"] = publication
	if publication.EventID == "" || publication.RunID != run.RunID {
		t.Fatal("public input has no exact committed event/run identity")
	}
	var event operatorread.OperatorEventFull
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": publication.EventID}, &event)
		record[key+"_last_event"] = event
		if len(event.Deliveries) == 1 && event.Deliveries[0].Terminal && event.Deliveries[0].Failure != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if event.EventID != publication.EventID || event.RunID != run.RunID || event.EventName != "notice.requested" || len(event.Deliveries) != 1 || event.Deliveries[0].SubscriberType != "node" || event.Deliveries[0].SubscriberID != node.Key() || !event.Deliveries[0].Terminal || event.Deliveries[0].Failure == nil {
		t.Fatal("public input did not reach the exact existing terminal node settlement")
	}
	issue3011NoActivity(t, endpoint, run.RunID, record, key+"_activity_requests")
	return event
}

func issue3011NoActivity(t *testing.T, endpoint, run string, record map[string]any, key string) {
	t.Helper()
	var listed operatorread.OperatorEventListResult
	requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": run, "event_name": "platform.activity_requested"}, "limit": 100}, &listed)
	record[key] = listed
	if len(listed.Events) != 0 || listed.NextCursor != "" {
		t.Fatal("pre-activity control unexpectedly created an activity request")
	}
}

func issue3011RegisterForSend(t *testing.T, opts cliapp.ServeOptions, peer *serveNativeProtocolPeer) channelonboarding.Operation {
	t.Helper()
	qr := make(chan channelonboarding.Result, 1)
	factory := func(ctx context.Context, options cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
		session, err := OpenChannelRegistration(ctx, options)
		if err != nil {
			return nil, err
		}
		return firstConnectPairingObserver{ChannelRegistrationSession: session, principal: options.Principal, observe: func(result channelonboarding.Result, _ bool) {
			if result.Pairing != nil && result.Pairing.Code != "" {
				select {
				case qr <- result:
				default:
				}
			}
		}}, nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	out, errOut := &lockedBuffer{}, &lockedBuffer{}
	done := make(chan int, 1)
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-done
		}
	}()
	go func() {
		done <- cliapp.Execute(ctx, []string{"--config", opts.ConfigPath, "channel", "connect", "whatsapp", "--yes"}, out, errOut, nil, nil, factory)
	}()
	select {
	case result := <-qr:
		if !result.Operation.DeploymentRegistration || result.Operation.AccountConfirmed {
			t.Fatal("not a fresh deployment pairing")
		}
		peer.pair(result.Pairing.Code)
	case code := <-done:
		joined = true
		t.Fatalf("registration exited before pairing: %d %s", code, errOut.String())
	case <-ctx.Done():
		t.Fatal("registration pairing timeout")
	}
	select {
	case code := <-done:
		joined = true
		if code != 0 {
			t.Fatalf("registration failed: %d %s", code, errOut.String())
		}
	case <-ctx.Done():
		t.Fatal("registration completion timeout")
	}
	var outList, errList bytes.Buffer
	if code := cliapp.Execute(ctx, []string{"--config", opts.ConfigPath, "channel", "list", "--json"}, &outList, &errList, nil, nil, factory); code != 0 {
		t.Fatal("registration list failed", code)
	}
	var listed struct {
		Accounts []channelonboarding.Result `json:"accounts"`
	}
	if err := json.Unmarshal(outList.Bytes(), &listed); err != nil || len(listed.Accounts) != 1 {
		t.Fatal("registration list is not unique", err)
	}
	op := listed.Accounts[0].Operation
	if !op.DeploymentRegistration || !op.AccountConfirmed || op.Phase != channelonboarding.PhaseSucceeded {
		t.Fatal("registration did not complete")
	}
	return op
}

func issue3011RegisteredStatus(t *testing.T, opts cliapp.ServeOptions, endpoint string, op channelonboarding.Operation) channelonboarding.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var out, errOut bytes.Buffer
	code := cliapp.Execute(ctx, []string{"--config", opts.ConfigPath, "channel", "list", "--api-server", strings.TrimSuffix(endpoint, "/v1/rpc"), "--json"}, &out, &errOut, nil, nil)
	t.Logf("registration-status readback exit=%d stdout=%q stderr=%q", code, out.String(), errOut.String())
	if code != 0 {
		t.Fatal("public registered readback failed", code, errOut.String())
	}
	var listed struct {
		Accounts []channelonboarding.Result `json:"accounts"`
	}
	if err := json.Unmarshal(out.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	var exact []channelonboarding.Result
	for _, row := range listed.Accounts {
		if row.Operation.OperationID == op.OperationID && row.Operation.AccountReference == op.AccountReference && row.Operation.Provider == op.Provider {
			exact = append(exact, row)
		}
	}
	if len(exact) != 1 || exact[0].Status == nil || exact[0].Status.Account != op.AccountReference {
		t.Fatal("public status lost exact registration")
	}
	return exact[0]
}
