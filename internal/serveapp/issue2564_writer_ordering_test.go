package serveapp

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
)

func TestIssue2564ServedH3WriterOrderingCorpora(t *testing.T) {
	for _, corpus := range []struct {
		name     string
		deadline bool
		checksum string
	}{
		{"h3", false, "2489a1223a4d5795464cf6afcccc75281800b642be01e66ebebb36ad9db792d3"},
		{"m33", true, "9b64daf78814fa85ca2c444cef053b3ead47a6b171b9be9d5ce457e3cd3cd20e"},
	} {
		t.Run(corpus.name, func(t *testing.T) {
			root := writeIssue2564Fixture(t, corpus.deadline)
			var paths []string
			if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
				if err != nil || entry.IsDir() {
					return err
				}
				relative, err := filepath.Rel(root, path)
				if err != nil {
					return err
				}
				paths = append(paths, filepath.ToSlash(relative))
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			slices.Sort(paths)
			manifest := sha256.New()
			for _, path := range paths {
				contents, err := os.ReadFile(filepath.Join(root, path))
				if err != nil {
					t.Fatal(err)
				}
				checksum := sha256.Sum256(contents)
				fmt.Fprintf(manifest, "%x  %s\n", checksum, path)
				t.Logf("ISSUE2564_%s_SHA256 %x  %s", corpus.name, checksum, path)
			}
			checksum := fmt.Sprintf("%x", manifest.Sum(nil))
			if len(paths) != 7 || checksum != corpus.checksum {
				t.Fatalf("%s corpus drift: files=%d sha256=%s pinned=%s", corpus.name, len(paths), checksum, corpus.checksum)
			}
			t.Logf("ISSUE2564_%s_CORPUS_SHA256 %s admitted_bundle=%s", corpus.name, checksum, servedEventPublishFixtureBundleHash(t, root))
		})
	}
}

// These are NEW served H3/M33 fixtures, not the unavailable archived H1/H2
// workloads. All writes use generated tools on real managed provider turns.
func TestIssue2564ServedH3CollectionOperationsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, order := range []string{"a_then_b", "b_then_a"} {
			for _, indices := range []string{"distinct", "same"} {
				t.Run(backend+"/"+order+"/"+indices, func(t *testing.T) {
					p := newIssue2564Provider(t, "a", "b")
					for _, actor := range []string{"a", "b"} {
						index := 0
						if actor == "b" && indices == "distinct" {
							index = 1
						}
						p.scripts[actor] = []*issue2564ProviderStep{
							newIssue2564Step(issue2564Call{"save_hub_items", map[string]any{"op": "append", "value": actor}}, issue2564Call{"save_hub_labels", map[string]any{"op": "set", "key": actor, "value": actor}}),
							newIssue2564Step(issue2564Call{"save_hub_items", map[string]any{"op": "append", "value": "equal"}}),
							newIssue2564Step(issue2564Call{"save_hub_labels", map[string]any{"op": "set", "key": "shared", "value": actor}}),
							newIssue2564Step(issue2564Call{"save_hub_positions", map[string]any{"op": "update", "index": index, "value": actor}}),
							newIssue2564Step(),
						}
					}
					_, rt := startIssue2564Served(t, backend, writeIssue2564Fixture(t, false), p, nil, nil)
					seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "hub.start", "bundle_hash": rt.BundleHash, "payload": map[string]any{"hub_id": "h3"}, "idempotency_key": "h3-start"})
					entityID := rt.waitEntityStage(t, seed.RunID, "", "idle")
					requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "hub.begin", "run_id": seed.RunID, "payload": map[string]any{"hub_id": "h3"}, "idempotency_key": "h3-begin"})
					// Both sessions have captured literal operations before either
					// response is released. No precomputed collection is supplied.
					for _, actor := range []string{"a", "b"} {
						p.wait(t, actor, 0)
					}
					for _, actor := range []string{"a", "b"} {
						p.release(actor, 0)
					}
					for _, actor := range []string{"a", "b"} {
						p.wait(t, actor, 1)
					}
					for _, actor := range []string{"a", "b"} {
						p.release(actor, 1)
					}
					for _, actor := range []string{"a", "b"} {
						p.wait(t, actor, 2)
					}
					first, last := "a", "b"
					if order == "b_then_a" {
						first, last = last, first
					}
					p.release(first, 2)
					p.wait(t, first, 3) // The first same-key commit is acknowledged.
					p.release(last, 2)
					p.wait(t, last, 3)
					p.release(first, 3)
					p.wait(t, first, 4)
					p.release(last, 3)
					p.wait(t, last, 4)
					for _, actor := range []string{"a", "b"} {
						p.release(actor, 4)
					}
					rt.waitDeliveries(t, seed.RunID)
					entity := issue2564Entity(t, rt, seed.RunID, entityID)
					items, ok := entity.Fields["items"].([]any)
					if !ok || len(items) != 4 {
						t.Fatalf("acknowledged appends lost: %#v", entity.Fields)
					}
					got := make([]string, len(items))
					for i, item := range items {
						got[i] = fmt.Sprint(item)
					}
					slices.Sort(got)
					if !reflect.DeepEqual(got, []string{"a", "b", "equal", "equal"}) {
						t.Fatalf("append multiplicity = %v", got)
					}
					if !reflect.DeepEqual(entity.Fields["labels"], map[string]any{"a": "a", "b": "b", "shared": last}) {
						t.Fatalf("map lost keys or last-commit ordering: %#v", entity.Fields)
					}
					positions := []any{"a", "b"}
					if indices == "same" {
						positions = []any{last, "initial-1"}
					}
					if !reflect.DeepEqual(entity.Fields["positions"], positions) {
						t.Fatalf("index semantics = %#v, want %#v", entity.Fields["positions"], positions)
					}
					requireIssue2564MutationReceipts(t, rt, seed.RunID, entityID, map[string]int{"items": 4, "labels": 4, "positions": 2}, 2)
					requireIssue2564Settled(t, rt, seed.RunID, 2)
				})
			}
		}
	}
}

func TestIssue2564ServedM33NonterminalDeadlineLateResultBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, order := range []string{"timer_then_save", "save_then_timer"} {
			t.Run(backend+"/"+order, func(t *testing.T) {
				p := newIssue2564Provider(t, "a")
				p.scripts["a"] = []*issue2564ProviderStep{
					newIssue2564Step(issue2564Call{"save_hub_marker", map[string]any{"value": "retained"}}),
					newIssue2564Step(issue2564Call{"emit_hub_result", map[string]any{"hub_id": "m33", "value": "retained"}}),
				}
				resultEntered, releaseResult := make(chan struct{}, 1), make(chan struct{})
				var resultOnce sync.Once
				unblockResult := func() { resultOnce.Do(func() { close(releaseResult) }) }
				defer unblockResult()
				releaseTimer := make(chan struct{})
				var timerOnce sync.Once
				unblockTimer := func() { timerOnce.Do(func() { close(releaseTimer) }) }
				defer unblockTimer()
				_, rt := startIssue2564Served(t, backend, writeIssue2564Fixture(t, true), p, func(ctx context.Context, _ string, evt events.Event) error {
					if string(evt.Type()) != evt.FlowInstance()+"/hub.result" {
						return nil
					}
					resultEntered <- struct{}{}
					select {
					case <-releaseResult:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}, issue2564StageTimerPublicationGate{release: releaseTimer})
				seed := requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "hub.start", "bundle_hash": rt.BundleHash, "payload": map[string]any{"hub_id": "m33"}, "idempotency_key": "m33-start"})
				entityID := rt.waitEntityStage(t, seed.RunID, "", "idle")
				requireServedEventPublishRPCResult(t, rt.Endpoint, map[string]any{"event_name": "hub.begin", "run_id": seed.RunID, "payload": map[string]any{"hub_id": "m33"}, "idempotency_key": "m33-begin"})
				p.wait(t, "a", 0)
				rt.waitEntityStage(t, seed.RunID, entityID, "working")
				if order == "save_then_timer" {
					p.release("a", 0)
					p.wait(t, "a", 1)
					entity := issue2564Entity(t, rt, seed.RunID, entityID)
					if entity.Fields["marker"] != "retained" || entity.Entity.CurrentState != "working" {
						t.Fatalf("reverse ordering did not commit before timer: %#v", entity)
					}
				}
				unblockTimer()
				rt.waitEntityStage(t, seed.RunID, entityID, "review")
				requireIssue2564TimerReceipt(t, rt, seed.RunID, entityID)
				requireIssue2564Pending(t, rt, seed.RunID, "agent", "hub.work")
				if order == "timer_then_save" {
					if marker := issue2564Entity(t, rt, seed.RunID, entityID).Fields["marker"]; marker != "initial" {
						t.Fatalf("save escaped provider barrier: %v", marker)
					}
					p.release("a", 0)
					p.wait(t, "a", 1)
				}
				if marker := issue2564Entity(t, rt, seed.RunID, entityID).Fields["marker"]; marker != "retained" {
					t.Fatalf("stage movement revoked or reverted acknowledged save: %v", marker)
				}
				p.release("a", 1)
				select {
				case <-resultEntered:
				case <-time.After(servedProofPollDeadline):
					t.Fatalf("late result never reached authored handler in review\n%s", rt.debug(t, seed.RunID))
				}
				// Now the emitted result itself is durable but not handled. It
				// must remain a completion obligation, not be canceled at deadline.
				requireIssue2564Pending(t, rt, seed.RunID, "node", "hub.result")
				unblockResult()
				// A successful generated output event ends the managed turn;
				// it does not require an invented extra provider continuation.
				rt.waitDeliveries(t, seed.RunID)
				entity := issue2564Entity(t, rt, seed.RunID, entityID)
				if entity.Entity.CurrentState != "review" || entity.Fields["marker"] != "retained" || entity.Fields["result"] != "retained" || entity.Fields["result_stage"] != "review" || entity.Fields["results"] != float64(1) {
					t.Fatalf("deadline/late-result effects not retained in current stage: %#v", entity)
				}
				requireIssue2564MutationReceipts(t, rt, seed.RunID, entityID, map[string]int{"marker": 1}, 1)
				requireIssue2564Settled(t, rt, seed.RunID, 1)
				if count := len(rt.events(t, seed.RunID, issue2564EventName(t, rt, seed.RunID, "hub.result"))); count != 1 {
					t.Fatalf("hub.result event count=%d want=1", count)
				}
				t.Log("D4 target: working -> review, both NONTERMINAL. Terminal implicit retirement remains #2269; no terminate/timeout/canceled semantics tested or changed.")
			})
		}
	}
}

type issue2564Call struct {
	name  string
	input map[string]any
}

type issue2564StageTimerPublicationGate struct {
	release <-chan struct{}
}

func (g issue2564StageTimerPublicationGate) NotifyLifecycle(ctx context.Context, signal runtimelifecycleprobe.Signal) {
	if signal.Kind != runtimelifecycleprobe.EventPersisted || signal.EventType != "platform.stage_timer" {
		return
	}
	// Observe the committed occurrence before engine-outbox dispatch. Neither a
	// SQL transaction nor the timer's entity-transition lock is held here.
	select {
	case <-g.release:
	case <-ctx.Done():
	}
}

type issue2564ProviderStep struct {
	calls   []issue2564Call
	entered chan error
	gate    chan struct{}
	once    sync.Once
}

func newIssue2564Step(calls ...issue2564Call) *issue2564ProviderStep {
	return &issue2564ProviderStep{calls: calls, entered: make(chan error, 1), gate: make(chan struct{})}
}

type issue2564Provider struct {
	t       *testing.T
	mu      sync.Mutex
	scripts map[string][]*issue2564ProviderStep
	rounds  map[string]int
}

func newIssue2564Provider(t *testing.T, actors ...string) *issue2564Provider {
	p := &issue2564Provider{t: t, scripts: map[string][]*issue2564ProviderStep{}, rounds: map[string]int{}}
	for _, actor := range actors {
		p.scripts[actor] = nil
	}
	return p
}

func (p *issue2564Provider) release(actor string, round int) {
	s := p.scripts[actor][round]
	s.once.Do(func() { close(s.gate) })
}

func (p *issue2564Provider) wait(t *testing.T, actor string, round int) {
	t.Helper()
	select {
	case err := <-p.scripts[actor][round].entered:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(servedProofPollDeadline):
		t.Fatalf("real provider %s round %d not reached", actor, round)
	}
}

func (p *issue2564Provider) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var request struct {
		System   json.RawMessage `json:"system"`
		Messages any             `json:"messages"`
		Tools    []struct {
			Name   string         `json:"name"`
			Schema map[string]any `json:"input_schema"`
		} `json:"tools"`
	}
	if err := json.NewDecoder(req.Body).Decode(&request); err != nil {
		p.t.Error(err)
		http.Error(w, "bad provider request", http.StatusBadRequest)
		return
	}
	actor := ""
	for candidate := range p.scripts {
		if strings.Contains(string(request.System), "ISSUE2564_"+candidate) {
			actor = candidate
		}
	}
	p.mu.Lock()
	round := p.rounds[actor]
	p.rounds[actor]++
	p.mu.Unlock()
	if actor == "" || round >= len(p.scripts[actor]) {
		p.t.Errorf("unexpected provider actor/round %q/%d", actor, round)
		http.Error(w, "unexpected managed turn", http.StatusBadRequest)
		return
	}
	s := p.scripts[actor][round]
	var admissionErr error
	if req.URL.Path != "/v1/messages" || req.Header.Get("x-api-key") != "issue2564-proof-key" {
		admissionErr = fmt.Errorf("real provider route/credential mismatch: %s", req.URL.Path)
	}
	for _, call := range s.calls {
		var schema map[string]any
		for _, tool := range request.Tools {
			if tool.Name == call.name {
				schema = tool.Schema
			}
		}
		if schema == nil {
			admissionErr = fmt.Errorf("generated tool %s absent from real provider turn", call.name)
		} else if _, operation := call.input["op"]; operation && !issue2564SchemaProperty(schema, "op") {
			admissionErr = fmt.Errorf("generated %s lacks pending op contract (no whole-value fallback): %v", call.name, schema)
		}
	}
	if round > 0 {
		var results []any
		if messages, ok := request.Messages.([]any); ok && len(messages) > 0 {
			results = issue2564ToolResults(messages[len(messages)-1])
		}
		if len(results) != len(p.scripts[actor][round-1].calls) {
			admissionErr = fmt.Errorf("canonical result batch has %d entries, want %d", len(results), len(p.scripts[actor][round-1].calls))
		}
		for i, call := range p.scripts[actor][round-1].calls {
			id := fmt.Sprintf("%s-%d-%d", actor, round-1, i)
			var result map[string]any
			if i < len(results) {
				result, _ = results[i].(map[string]any)
			}
			if result == nil || result["name"] != call.name || result["ok"] != true {
				admissionErr = fmt.Errorf("generated %s call %s not acknowledged in current result batch: %#v", call.name, id, result)
			} else if strings.HasPrefix(call.name, "save_") {
				value, _ := result["result"].(map[string]any)
				if revision, ok := value["revision"].(float64); !ok || revision < 1 || value["entity_id"] == "" {
					admissionErr = fmt.Errorf("save %s omitted real mutation receipt: %#v", id, result)
				}
			}
		}
	}
	s.entered <- admissionErr
	if admissionErr != nil {
		http.Error(w, admissionErr.Error(), http.StatusBadRequest)
		return
	}
	select {
	case <-s.gate:
	case <-req.Context().Done():
		return
	}
	content := []any{}
	for i, call := range s.calls {
		content = append(content, map[string]any{"type": "tool_use", "id": fmt.Sprintf("%s-%d-%d", actor, round, i), "name": call.name, "input": call.input})
	}
	if len(content) == 0 {
		content = append(content, map[string]any{"type": "text", "text": "Retained work complete."})
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"model": "claude-test", "usage": map[string]any{"input_tokens": 8, "output_tokens": 2}, "content": content}); err != nil {
		p.t.Error(err)
	}
}

func issue2564SchemaProperty(value any, name string) bool {
	switch v := value.(type) {
	case map[string]any:
		if properties, ok := v["properties"].(map[string]any); ok && properties[name] != nil {
			return true
		}
		for _, child := range v {
			if issue2564SchemaProperty(child, name) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if issue2564SchemaProperty(child, name) {
				return true
			}
		}
	}
	return false
}

// This adapter wraps the latest canonical continuation in "Tool result:" and
// retains tool names/order, not provider call IDs. Do not accept older batches.
func issue2564ToolResults(value any) []any {
	switch v := value.(type) {
	case map[string]any:
		if v["kind"] == "tool_continuation" {
			results, _ := v["tool_result"].([]any)
			return results
		}
		for _, child := range v {
			if found := issue2564ToolResults(child); found != nil {
				return found
			}
		}
	case []any:
		for i := len(v) - 1; i >= 0; i-- {
			if found := issue2564ToolResults(v[i]); found != nil {
				return found
			}
		}
	case string:
		var decoded any
		content := strings.TrimPrefix(v, "Tool result:\n")
		if json.Unmarshal([]byte(content), &decoded) == nil {
			if _, isString := decoded.(string); !isString {
				return issue2564ToolResults(decoded)
			}
		}
	}
	return nil
}

func startIssue2564Served(t *testing.T, backend, root string, p *issue2564Provider, hook func(context.Context, string, events.Event) error, probe runtimelifecycleprobe.Observer) (*serveRuntimeTestProcess, issue2564ServedFixture) {
	t.Helper()
	opts, start := issue2564ServeHarness(t, backend, root, false)
	opts.TestWorkflowNodeHandlerStartHook = hook
	opts.TestLifecycleProbe = probe
	server := httptest.NewServer(p)
	t.Cleanup(server.Close)
	redirectExternalHosts(t, map[string]string{"api.anthropic.com": server.URL})
	path := filepath.Join(t.TempDir(), "credentials.json")
	t.Setenv("SWARM_CREDENTIALS_FILE", path)
	t.Setenv("ANTHROPIC_API_KEY", "")
	credentials, err := runtimecredentials.NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := credentials.Set(context.Background(), "ANTHROPIC_API_KEY", "issue2564-proof-key"); err != nil {
		t.Fatal(err)
	}
	process, rt := start()
	// Cleanup order releases HTTP barriers before joining the real serve owner.
	t.Cleanup(func() {
		if code := process.stop(); code != 0 {
			t.Errorf("serve stop=%d", code)
		}
	})
	t.Cleanup(func() {
		for actor, script := range p.scripts {
			for round := range script {
				p.release(actor, round)
			}
		}
	})
	return process, rt
}

func issue2564Entity(t *testing.T, rt issue2564ServedFixture, runID, entityID string) operatorread.OperatorEntityFull {
	t.Helper()
	var entity operatorread.OperatorEntityFull
	requireServedJSONRPCResult(t, rt.Endpoint, "entity.get", map[string]any{"run_id": runID, "entity_id": entityID}, &entity)
	return entity
}

func requireIssue2564Pending(t *testing.T, rt issue2564ServedFixture, runID, kind, event string) {
	t.Helper()
	var pending int
	event = issue2564EventName(t, rt, runID, event)
	var deliveryID string
	for _, e := range rt.events(t, runID, event) {
		for _, row := range storetest.ObserveDeliveryEventEvidence(t, context.Background(), rt.selected, e.EventID).Deliveries {
			if row.RunID == runID && row.SubscriberType == kind && (row.Status == "pending" || row.Status == "in_progress") {
				if row.RetryCount == 0 {
					pending++
				}
				if deliveryID == "" {
					deliveryID = row.DeliveryID
				}
			}
		}
	}
	header, err := rt.selected.LoadRunHeader(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	status, ended := header.Status, header.EndedAt != nil
	if pending != 1 || status != "running" || ended {
		t.Fatalf("premature completion/lost obligation: %s %s pending=%d run=%s ended=%v", kind, event, pending, status, ended)
	}
	if deliveryID == "" {
		t.Fatal("exact pending delivery absent")
	}
	summary := issue2564DeliverySummary(t, rt, runID)
	if summary.Settled() || summary.Pending+summary.InProgress < 1 {
		t.Fatalf("native completion accounting lost exact pending delivery %s: %+v", deliveryID, summary)
	}
	var diagnosis struct {
		Run            operatorread.RunHeader         `json:"run"`
		TestQuiescence operatorread.RunTestQuiescence `json:"test_quiescence"`
	}
	requireServedJSONRPCResult(t, rt.Endpoint, "run.diagnose", map[string]any{"run_id": runID}, &diagnosis)
	if diagnosis.Run.Status != "running" || diagnosis.TestQuiescence.Ready || diagnosis.TestQuiescence.ActiveDeliveries < 1 {
		t.Fatalf("public diagnosis falsely completed pending result: %+v", diagnosis)
	}
}

func issue2564EventName(t *testing.T, rt issue2564ServedFixture, runID, local string) string {
	t.Helper()
	return storetest.ObserveWriterFlow(t, context.Background(), rt.selected, runID, "").InstancePath + "/" + local
}

func issue2564DeliverySummary(t *testing.T, rt issue2564ServedFixture, runID string) runtimedelivery.RunSummary {
	t.Helper()
	summary, err := rt.selected.SummarizeRun(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	if err := summary.Validate(); err != nil {
		t.Fatal(err)
	}
	return summary
}

func requireIssue2564Settled(t *testing.T, rt issue2564ServedFixture, runID string, agents int) {
	t.Helper()
	evidence := storetest.ObserveWriterRunDelivery(t, context.Background(), rt.selected, runID)
	delivered, failed, retries, badClaims, total, settled := evidence.DeliveredAgents, evidence.DeadLetters, evidence.Retries, evidence.BadClaims, evidence.Total, evidence.SettledDelivered
	live := storetest.ObserveLiveWriterCount(t, context.Background(), rt.selected, runID)
	if delivered != agents || live != agents || failed != 0 || retries != 0 || badClaims != 0 || settled != total || !issue2564DeliverySummary(t, rt, runID).Settled() {
		t.Fatalf("real turn/delivery accounting: delivered=%d live=%d failed=%d retries=%d bad_claims=%d settled=%d/%d\n%s", delivered, live, failed, retries, badClaims, settled, total, rt.debug(t, runID))
	}
}

func requireIssue2564MutationReceipts(t *testing.T, rt issue2564ServedFixture, runID, entityID string, want map[string]int, actors int) {
	t.Helper()
	rows := storetest.ObserveEntityMutationHistory(t, context.Background(), rt.selected, runID)
	eventName := issue2564EventName(t, rt, runID, "hub.work")
	slices.Reverse(rows)
	got, writers := map[string]int{}, map[string]bool{}
	type change struct{ before, after any }
	changes := map[string][]change{}
	for _, row := range rows {
		if row.EntityID != entityID || row.WriterType != "agent" || row.Domain != "authored_field" || row.EventName != eventName || !row.RegisteredAgent {
			continue
		}
		path, writer, step, cause, before, after := row.Path, row.WriterID, row.HandlerStep, row.CausedByEvent, string(row.OldValue), string(row.NewValue)
		if writer == "" || step == "" || cause == "" {
			t.Fatalf("lost agent attribution: %s %s %s %s", path, writer, step, cause)
		}
		got[path]++
		writers[writer] = true
		var c change
		if err := json.Unmarshal([]byte(before), &c.before); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(after), &c.after); err != nil {
			t.Fatal(err)
		}
		changes[path] = append(changes[path], c)
	}
	if !reflect.DeepEqual(got, want) || len(writers) != actors {
		t.Fatalf("acknowledged operation mutation receipts=%v writers=%v, want %v/%d", got, writers, want, actors)
	}
	initial := map[string]any{"items": []any{}, "labels": map[string]any{}, "positions": []any{"initial-0", "initial-1"}, "marker": "initial"}
	final := issue2564Entity(t, rt, runID, entityID).Fields
	// Derive commit order from adjacent old/new values, not wall-clock ordering.
	for path, chain := range changes {
		current := initial[path]
		for len(chain) > 0 {
			next := -1
			for i, candidate := range chain {
				if reflect.DeepEqual(candidate.before, current) {
					next = i
					break
				}
			}
			if next < 0 {
				t.Fatalf("%s mutation history not a fresh-state chain from %#v: %#v", path, current, chain)
			}
			current = chain[next].after
			chain = slices.Delete(chain, next, next+1)
		}
		if !reflect.DeepEqual(current, final[path]) {
			t.Fatalf("%s acknowledged history ends at %#v but public state is %#v", path, current, final[path])
		}
	}
}

func requireIssue2564TimerReceipt(t *testing.T, rt issue2564ServedFixture, runID, entityID string) {
	t.Helper()
	flow := storetest.ObserveWriterFlow(t, context.Background(), rt.selected, runID, entityID)
	var config struct {
		History []pipeline.WorkflowTransitionRecord `json:"transition_history"`
	}
	if err := json.Unmarshal(flow.Config, &config); err != nil {
		t.Fatal(err)
	}
	history := config.History
	for _, record := range history {
		if err := record.Evidence.Validate(); err != nil {
			t.Fatal(err)
		}
		if record.TransitionID != record.Evidence.ID() || record.From != record.Evidence.From() || record.To != record.Evidence.To() {
			t.Fatalf("contradictory record: %#v", record)
		}
	}
	var timers int
	for _, entry := range history {
		compiled, ok := entry.Evidence.Compiled()
		if !ok || compiled.Edge().Source != "timer" {
			continue
		}
		timers++
		if entry.From != "working" || entry.To != "review" {
			t.Fatalf("M33 substituted terminal deadline: %#v", entry)
		}
		var accepted int
		for _, event := range rt.events(t, runID, "platform.stage_timer") {
			if event.EventID == entry.TriggerEventID {
				accepted++
			}
		}
		if accepted != 1 {
			t.Fatalf("transition lacks exact accepted timer event: %#v", entry)
		}
	}
	if timers != 1 {
		t.Fatalf("real nonterminal timer transitions=%d, history=%#v", timers, history)
	}
}

func writeIssue2564Fixture(t *testing.T, deadline bool) string {
	t.Helper()
	if deadline {
		return canonicalrouting.CopyIssue2564M33NonterminalDeadline(t)
	}
	return canonicalrouting.CopyIssue2564H3CollectionOperations(t)
}
