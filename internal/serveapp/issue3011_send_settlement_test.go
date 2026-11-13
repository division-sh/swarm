//go:build linux || darwin

package serveapp

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/tests/support/testutil"
	"go.mau.fi/whatsmeow/types"
	"gopkg.in/yaml.v3"
)

// Invariant: a down-link send reaches its ordinary failed activity, never a handler retry or later resend.
func TestRegisteredWhatsAppPublicInputDownLinkFailsActivityBothStores(t *testing.T) {
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
	opts.WorkspaceBackend, opts.WorkspaceBackendSet = "host", true
	opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
	opts.SelfCheck = true
	opts.SourceRoot = filepath.Join(t.TempDir(), "source")
	copyReleaseFixtureTree(t, filepath.Join(repoRootForTest(), "internal/serveapp/testdata/whatsapp-session-reply"), opts.SourceRoot)
	issue3011NoticeSource(t, opts.SourceRoot)
	opts.SwarmDir, opts.SwarmDirSet = t.TempDir(), true
	if err := os.Chmod(opts.SwarmDir, 0o700); err != nil {
		t.Fatal(err)
	}
	opts.StoreMode, opts.StoreModeSet = backend, true
	location := filepath.Join(t.TempDir(), "diagnostic.sqlite")
	if backend == "sqlite" {
		opts.ConfigPath = writeStoreBackendRuntimeConfigWithWorkspaceFields(t, backend, location, channelOnboardingHostWorkspaceFields())
	} else {
		location = testutil.StartEmptyPostgresDSN(t)
		opts.ConfigPath = writeChannelOnboardingPostgresRuntimeConfig(t, location)
	}
	config, err := os.ReadFile(opts.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	writeStandingCandidateFile(t, opts.ConfigPath, string(config)+fmt.Sprintf("\npaths:\n  swarm_dir: %q\n", opts.SwarmDir))
	enableChannelOnboardingRecoveryOnStartup(t, opts.ConfigPath)
	root := filepath.Join(cmp.Or(os.Getenv("TEST_ISSUE3011_OUT"), t.TempDir()), backend)
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	record := map[string]any{"backend": backend, "source": os.Getenv("TEST_ISSUE3011_PIN"),
		"surface": "exact-pin Go test binary plus test-only authored-source overlay; public Go CLI registration and public RPC input; existing peer-trust IN-PROCESS RunServe, NOT subprocess/published CLI/provider handshake",
		"started": time.Now().UTC(), "complete": false, "stage": "setup"}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(self)
	if err != nil {
		t.Fatal(err)
	}
	record["test_binary_sha256"] = fmt.Sprintf("%x", sha256.Sum256(binary))
	for _, file := range []string{"schema.yaml", "nodes.yaml", "policy.yaml", "events.yaml"} {
		raw, err := os.ReadFile(filepath.Join(opts.SourceRoot, file))
		if err != nil {
			t.Fatal(err)
		}
		record[file+"_sha256"] = fmt.Sprintf("%x", sha256.Sum256(raw))
		writeStandingCandidateFile(t, filepath.Join(root, file), string(raw))
	}
	var selected atomic.Value
	captureSelectedRuntimePersistence(t, func(p serveRuntimePersistence) { selected.Store(p) })
	start := func() (*serveRuntimeTestProcess, string) {
		p := startServeRuntimeTestProcess(t, opts)
		p.waitForReadyLine()
		return p, "http://" + serveRuntimeAPIListenerFromOutput(t, p.outputString()) + "/v1/rpc"
	}
	var process *serveRuntimeTestProcess
	var endpoint string
	var logs []string
	stop := func() {
		t.Helper()
		if process == nil {
			return
		}
		joined := process.stop()
		logs = append(logs, process.outputString())
		process = nil
		joins, _ := record["serve_joins"].([]string)
		record["serve_joins"] = append(joins, fmt.Sprint(joined))
		if joined != 0 {
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
		writeStandingCandidateFile(t, filepath.Join(root, "serve.log"), strings.Join(logs, "\n--- owned process boundary ---\n"))
		t.Logf("backend=%s stage=%s complete=%v", backend, record["stage"], record["complete"])
	}()

	// The negative is a fresh unregistered store, not a fabricated missing row.
	record["stage"] = "unlinked public-input control"
	process, endpoint = start()
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
	_, customer, customerLID := peer.senderFrom(types.NewJID("15557654321", types.DefaultUserServer), types.NewJID("100000000090", types.HiddenUserServer))
	record["operation_id"], record["account"] = op.OperationID, op.AccountReference
	process, endpoint = start()
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
		if down.Status.Health == "error" && down.Status.Reason != "" && down.Status.Cause != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	record["down"] = down
	if down.Status.Health != "error" || down.Status.Reason == "" || down.Status.Cause == "" {
		t.Fatal("socket cut did not produce the required error state with reason and cause")
	}
	peer.mu.Lock()
	record["peer_after_cut"] = map[string]int{"accepts": peer.connections, "closed": peer.disconnected, "post_logins": peer.postLogins}
	peer.mu.Unlock()
	record["stage"] = "down-link public-input settlement"
	settled := issue3011PublishAndSettle(t, endpoint, "down", record)
	record["down_input"] = settled
	failed := issue3011ActivityOutcome(t, endpoint, settled, backend, location, "")
	record["failed_activity"] = failed
	startBoth := make(chan struct{})
	concurrent := make(chan lifecycleContenderResult, 2)
	var inputs sync.WaitGroup
	for _, key := range []string{"parallel-a", "parallel-b"} {
		params := maps.Clone(record["down_public_input_params"].(map[string]any))
		payload := maps.Clone(params["payload"].(map[string]any))
		payload["text"], params["payload"], params["idempotency_key"] = key, payload, "issue3011-"+key
		inputs.Add(1)
		go func() {
			defer inputs.Done()
			<-startBoth
			concurrent <- lifecycleContenderRPC(endpoint, "event.publish", params)
		}()
	}
	close(startBoth)
	responses := []lifecycleContenderResult{<-concurrent, <-concurrent}
	inputs.Wait()
	var parallel []operatorread.OperatorEventFull
	for _, response := range responses {
		var published servedEventPublishRPCResult
		if response.err != nil || response.reply.Error != nil {
			t.Fatal("concurrent public input failed", response.err, response.reply.Error)
		}
		if err := json.Unmarshal(response.reply.Result, &published); err != nil || published.RunID != settled.RunID {
			t.Fatal("concurrent input lost original run", err)
		}
		input := operatorread.OperatorEventFull{EventID: published.EventID, RunID: published.RunID}
		parallel = append(parallel, issue3011ActivityOutcome(t, endpoint, input, backend, location, ""))
	}
	if parallel[0].EventID == parallel[1].EventID || parallel[0].SourceEventID == parallel[1].SourceEventID || parallel[0].SourceEventID == failed.SourceEventID || parallel[1].SourceEventID == failed.SourceEventID {
		t.Fatal("concurrent inputs shared one failed result")
	}
	record["parallel_failed_activities"] = parallel
	if replay := issue3011PublishAndSettle(t, endpoint, "down", record); replay.EventID != settled.EventID {
		t.Fatal("input replay reminted the request")
	}
	stop()
	if t.Failed() {
		return
	}
	record["stage"] = "explicit existing startup restore"
	process, endpoint = start()
	peer.awaitPostLogin()
	restored := issue3011RegisteredStatus(t, opts, endpoint, op)
	record["restored"] = restored
	if restored.Status.State != "live" || restored.Status.Health != "connected" || len(restored.Status.Holders) != 1 {
		t.Fatal("existing startup path did not restore the exact registered account")
	}
	waitServedRunDeliveryQuiescence(t, selected.Load().(serveRuntimePersistence).deps.DeliveryStore.SummarizeRun, settled.RunID)
	if after := issue3011ActivityOutcome(t, endpoint, settled, backend, location, ""); !reflect.DeepEqual(after, failed) {
		t.Fatal("restart changed the original failed result")
	}
	select {
	case extra := <-peer.sent:
		t.Fatal("duplicate or unrelated application send", extra.ID)
	default:
	}
	healthy := issue3011PublishAndSettle(t, endpoint, "healthy", record)
	select {
	case sent := <-peer.sent:
		record["healthy_sent"] = map[string]any{"id": sent.ID, "to": sent.To.String(), "body": sent.Body.String()}
		if sent.To.ToNonAD() != customer && sent.To.ToNonAD() != customerLID || sent.Body.GetConversation() != "synthetic diagnostic notice" {
			t.Fatal("fresh authored send changed its customer/input")
		}
		record["healthy_activity"] = issue3011ActivityOutcome(t, endpoint, healthy, backend, location, sent.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("new authored request did not send after recovery")
	}
	waitServedRunDeliveryQuiescence(t, selected.Load().(serveRuntimePersistence).deps.DeliveryStore.SummarizeRun, settled.RunID)
	select {
	case extra := <-peer.sent:
		t.Fatal("duplicate fresh send", extra.ID)
	default:
	}
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
			node["notice_delivery.failed"] = map[string]any{}
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
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": publication.EventID}, &event)
		record[key+"_last_event"] = event
		if len(event.Deliveries) == 1 && event.Deliveries[0].Terminal {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if event.EventID != publication.EventID || event.RunID != run.RunID || event.EventName != "notice.requested" || len(event.Deliveries) != 1 || event.Deliveries[0].SubscriberType != "node" || event.Deliveries[0].SubscriberID != node.Key() || !event.Deliveries[0].Terminal {
		t.Fatal("public input did not reach the exact existing node settlement")
	}
	if key == "unlinked" {
		issue3011NoActivity(t, endpoint, run.RunID, record, key+"_activity_requests")
	} else if event.Deliveries[0].Status != "delivered" || event.Deliveries[0].Failure != nil || event.Deliveries[0].RetryCount != 0 {
		t.Fatal("send refusal escaped the activity into handler retry", event)
	}
	return event
}

func issue3011ActivityOutcome(t *testing.T, endpoint string, input operatorread.OperatorEventFull, backend, location, messageID string) operatorread.OperatorEventFull {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		var requests operatorread.OperatorEventListResult
		requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": input.RunID, "event_name": "platform.activity_requested"}, "limit": 100}, &requests)
		var matching []operatorread.OperatorEventFull
		for _, request := range requests.Events {
			if request.Payload["source_event_id"] == input.EventID {
				matching = append(matching, request)
			}
		}
		if len(matching) > 1 || requests.NextCursor != "" {
			t.Fatal("ambiguous or truncated original activity requests")
		}
		if len(matching) == 1 {
			request := matching[0]
			outcome, status := "notice_delivery.failed", pipeline.ActivityAttemptStatusFailed
			if messageID != "" {
				outcome, status = "notice_delivery.succeeded", pipeline.ActivityAttemptStatusSucceeded
			}
			var results operatorread.OperatorEventListResult
			requireServedJSONRPCResult(t, endpoint, "event.list", map[string]any{"filter": map[string]any{"run_id": input.RunID, "event_name": outcome}, "limit": 100}, &results)
			for _, row := range results.Events {
				if row.SourceEventID != input.EventID {
					continue
				}
				var result operatorread.OperatorEventFull
				requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": row.EventID}, &result)
				if messageID == "" && (len(result.Deliveries) != 1 || result.Deliveries[0].Status != "delivered") {
					continue
				}
				if len(result.Payload) != 3 || result.Payload["activity_id"] != request.Payload["activity_id"] || result.Payload["tool"] != "whatsapp.send_text" || result.RunID != input.RunID {
					t.Fatal("failure result lost exact ordinary activity/causal input", result)
				}
				if messageID == "" {
					cause := failures.Normalize(failures.New(failures.ClassDependencyUnavailable, "dependency_unavailable", "whatsapp", "admit_activity", nil), "whatsapp", "admit_activity")
					raw, err := json.Marshal(cause)
					if err != nil {
						t.Fatal(err)
					}
					var expected map[string]any
					if err := json.Unmarshal(raw, &expected); err != nil || !reflect.DeepEqual(expected, result.Payload["failure"]) {
						t.Fatal("down-link failed-event envelope changed", result.Payload)
					}
				} else if value, ok := result.Payload["result"].(map[string]any); !ok || len(value) != 1 || value["id"] != messageID || strings.ReplaceAll(request.EventID, "-", "") != messageID {
					t.Fatal("fresh send result adopted another provider acknowledgement")
				}
				attempt := readServedNativeActivityAttempt(t, backend, location, request.EventID)
				var original operatorread.OperatorEventFull
				requireServedJSONRPCResult(t, endpoint, "event.get", map[string]any{"event_id": input.EventID}, &original)
				if len(original.Deliveries) != 1 || original.Deliveries[0].Status != "delivered" || original.Deliveries[0].Failure != nil || original.Deliveries[0].RetryCount != 0 {
					t.Fatal("activity refusal leaked into input settlement", original)
				}
				value, err := canonicaljson.FromGo(request.Payload["input"])
				if err != nil {
					t.Fatal(err)
				}
				hash, err := canonicaljson.HashValue(value)
				if err != nil || attempt.Status != status || attempt.RequestEventID != request.EventID || attempt.ResultEventID != result.EventID || attempt.RunID != input.RunID || attempt.SourceEventID != input.EventID || attempt.InputHash != hash {
					t.Fatal("ordinary outcome has no exact terminal journal", attempt)
				}
				return result
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("original request did not settle its exact activity result")
	return operatorread.OperatorEventFull{}
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

func issue3011RegisterForSend(t *testing.T, opts cliapp.ServeOptions, peer *serveNativeProtocolPeer, priorAccounts ...int) channelonboarding.Operation {
	t.Helper()
	var paired sync.Once
	factory := func(ctx context.Context, options cliapp.ChannelRegistrationOptions) (cliapp.ChannelRegistrationSession, error) {
		session, err := OpenChannelRegistration(ctx, options)
		if err != nil {
			return nil, err
		}
		return firstConnectPairingObserver{ChannelRegistrationSession: session, principal: options.Principal, observe: func(result channelonboarding.Result, _ bool) {
			if result.Pairing != nil && result.Pairing.Code != "" {
				if !result.Operation.DeploymentRegistration || result.Operation.AccountConfirmed {
					t.Fatal("not a fresh deployment pairing")
				}
				paired.Do(func() { peer.pair(result.Pairing.Code) })
			}
		}}, nil
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	var out, errOut bytes.Buffer
	if code := cliapp.Execute(ctx, []string{"--config", opts.ConfigPath, "channel", "connect", "whatsapp", "--yes"}, &out, &errOut, nil, nil, factory); code != 0 {
		t.Fatalf("registration failed: %d %s", code, errOut.String())
	}
	var outList, errList bytes.Buffer
	if code := cliapp.Execute(ctx, []string{"--config", opts.ConfigPath, "channel", "list", "--json"}, &outList, &errList, nil, nil, factory); code != 0 {
		t.Fatal("registration list failed", code)
	}
	var listed struct {
		Accounts []channelonboarding.Result `json:"accounts"`
	}
	expected := 1
	if len(priorAccounts) != 0 {
		expected += priorAccounts[0]
	}
	if err := json.Unmarshal(outList.Bytes(), &listed); err != nil || len(listed.Accounts) != expected {
		t.Fatal("registration list is not unique", err)
	}
	op := listed.Accounts[0].Operation
	for _, row := range listed.Accounts {
		if row.Operation.Provider == "whatsapp" && row.Operation.AccountReference == "+"+peer.account.User {
			op = row.Operation
		}
	}
	if !op.DeploymentRegistration || !op.AccountConfirmed || op.Phase != channelonboarding.PhaseSucceeded || op.AccountReference != "+"+peer.account.User {
		t.Fatal("registration did not complete")
	}
	return op
}

func issue3011RegisteredStatus(t *testing.T, opts cliapp.ServeOptions, endpoint string, op channelonboarding.Operation) channelonboarding.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var out, errOut bytes.Buffer
	code := cliapp.Execute(ctx, []string{"--config", opts.ConfigPath, "channel", "list", "--api-server", strings.TrimSuffix(endpoint, "/v1/rpc"), "--json"}, &out, &errOut, nil, nil, OpenChannelRegistration)
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
