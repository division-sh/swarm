package serveapp

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
)

// Reconstructed equivalent H1, NOT the unchanged/unavailable original archive.
// Sources: https://github.com/division-sh/swarm/issues/2564 and approval
// https://github.com/division-sh/swarm/issues/2564#issuecomment-6001398406.
// Comparison: the published fieldless lu-h1 root and template hub, four inputs,
// explicit count=0 creation workaround, two roles with 60 disjoint integer
// fields each, six hubs/twelve real sessions, seven <=9-tool rounds after a
// 1.5s first-response delay, value=1, plain stop, and 600 bumps/eight publishers
// are preserved. Differences: expanded abridged field lists; current admitted
// expression syntax (payload/entity, without legacy ${}); a marker in the intent
// identifies the scripted role. Fresh selected stores, fake file credentials,
// and the normal served test launch replace the unavailable archived driver.
// No provider prewarming, first-turn serialization, writer injection or budgets
// are introduced. Per-file/corpus SHA256 and the admitted bundle hash are logged.
const (
	issue2564H1Hubs       = 6
	issue2564H1Fields     = 60
	issue2564H1Bumps      = 600
	issue2564H1Publishers = 8
	issue2564H1Batch      = 9
	issue2564H1CorpusSHA  = "5fd534a663d9e0c39b0e646b0825897325354da5e129714f88a48d0a7a65bb91"
)

func TestIssue2564H1EquivalentCorpus(t *testing.T) {
	files := issue2564H1Corpus()
	if len(files) != 7 || files["entities.yaml"] != "" || strings.Contains(files["hub/nodes.yaml"], "has(") {
		t.Fatal("published fieldless root / explicit-counter corpus drift")
	}
	for _, side := range []string{"a", "b"} {
		for n := 1; n <= issue2564H1Fields; n++ {
			field := fmt.Sprintf("%s%02d", side, n)
			if strings.Count(files["hub/entities.yaml"], "  "+field+": {type: integer, initial: 0}\n") != 1 || strings.Count(files["hub/agents.yaml"], field) != 1 {
				t.Fatalf("published distinct field assignment drift: %s", field)
			}
		}
	}
	root := issue2564H1WriteCorpus(t, files)
	t.Logf("reconstructed equivalent H1 admitted bundle=%s", servedEventPublishFixtureBundleHash(t, root))
}

func TestIssue2564ServedH1ReconstructedEquivalentBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []string{"overlap", "no_overlap"} {
			t.Run(backend+"/"+mode, func(t *testing.T) {
				deadline, bounded := t.Deadline()
				if !bounded {
					t.Fatal("reconstructed H1 requires a bounded qualification deadline")
				}
				ctx, cancel := context.WithDeadline(t.Context(), deadline)
				defer cancel()
				provider := &issue2564H1Provider{t: t, sessions: map[string]*issue2564H1Session{}}
				rt := issue2564H1StartServed(t, backend, issue2564H1WriteCorpus(t, issue2564H1Corpus()), provider)
				seed := issue2564H1Publish(t, ctx, rt.Endpoint, map[string]any{
					"event_name": "hub.start", "bundle_hash": rt.BundleHash,
					"payload": map[string]any{"hub_id": "h01"}, "idempotency_key": "h1-start-h01",
				})
				if !seed.NewRunCreated || seed.RunID == "" {
					t.Fatalf("normal served run/construction missing: %+v", seed)
				}
				// Start requests, and hence actual first provider turns, are concurrent.
				var starts sync.WaitGroup
				for hub := 2; hub <= issue2564H1Hubs; hub++ {
					starts.Add(1)
					go func(hub int) {
						defer starts.Done()
						key := fmt.Sprintf("h%02d", hub)
						result := issue2564H1Publish(t, ctx, rt.Endpoint, map[string]any{
							"event_name": "hub.start", "run_id": seed.RunID, "source_event_id": seed.EventID,
							"payload": map[string]any{"hub_id": key}, "idempotency_key": "h1-start-" + key,
						})
						if result.RunID != seed.RunID || result.NewRunCreated {
							t.Errorf("template hub escaped the original served run: %+v", result)
						}
					}(hub)
				}
				starts.Wait()
				if t.Failed() {
					return
				}
				if mode == "no_overlap" {
					issue2564H1WaitQuiescence(t, ctx, rt, seed.RunID, provider)
					provider.assert(t, issue2564H1Hubs*2*issue2564H1Fields)
				}
				issue2564H1PublishBumps(t, ctx, rt, seed.RunID, seed.EventID)
				issue2564H1WaitQuiescence(t, ctx, rt, seed.RunID, provider)
				acks := provider.assert(t, issue2564H1Hubs*2*issue2564H1Fields)
				issue2564H1AssertStore(t, rt, seed.RunID, mode, acks)
				if t.Failed() {
					return
				}
				t.Logf("RECONSTRUCTED_EQUIVALENT_H1 store=%s mode=%s run=%s bundle=%s hubs=6 sessions=12 rounds=7+stop ack=720 mutations=720 bumps=600 count=600 lost=0 reverts=0 retry_count=0 dead_letters=0", backend, mode, seed.RunID, rt.BundleHash)
			})
		}
	}
}

func issue2564H1Corpus() map[string]string {
	files := map[string]string{
		"schema.yaml": `name: lu-h1
pins: {inputs: [hub.start, hub.bump, hub.close, hub.again]}
connect:
  - {event: hub.start, from: ., to: hub, resolution: select-or-create}
  - {event: hub.bump, from: ., to: hub, resolution: select}
  - {event: hub.close, from: ., to: hub, resolution: select}
  - {event: hub.again, from: ., to: hub, resolution: select}
`,
		"events.yaml": `hub.start: {key: hub_id, hub_id: text}
hub.bump: {key: hub_id, hub_id: text, n: integer}
hub.close: {key: hub_id, hub_id: text}
hub.again: {key: hub_id, hub_id: text, wave: integer}
`,
		"hub/schema.yaml": `name: hub
instance: hub_id
stages: {open: {initial: true}, active: {}, closed: {terminal: true}}
pins: {inputs: [hub.start, hub.bump, hub.close, hub.again]}
`,
		"hub/events.yaml": "hub.work: {hub_id: text, wave: integer}\n",
		"hub/nodes.yaml": `hub-node:
  execution_type: system_node
  subscribes_to: [hub.start, hub.bump, hub.close, hub.again]
  event_handlers:
    hub.start:
      data_accumulation:
        writes:
          - {source_field: hub_id, target_field: hub_id}
          - {target_field: count, value: 0}
      advances_to: active
      emit: {event: hub.work, fields: {hub_id: payload.hub_id, wave: 1}}
    hub.again:
      emit: {event: hub.work, fields: {hub_id: payload.hub_id, wave: payload.wave}}
    hub.bump:
      data_accumulation:
        writes:
          - {target_field: count, value: entity.count + 1}
    hub.close: {advances_to: closed}
`,
	}
	var entities, agents strings.Builder
	entities.WriteString("hub:\n  hub_id: text\n  count: {type: integer, initial: 0}\n")
	for _, side := range []string{"a", "b"} {
		fields := make([]string, issue2564H1Fields)
		for n := 1; n <= issue2564H1Fields; n++ {
			fields[n-1] = fmt.Sprintf("%s%02d", side, n)
			fmt.Fprintf(&entities, "  %s: {type: integer, initial: 0}\n", fields[n-1])
		}
		fmt.Fprintf(&agents, "marker-%s:\n  role: marker_%s\n  intent: {inline: 'H1_MARKER_%s: write every assigned field to 1 then stop'}\n  model: regular\n  subscriptions: [hub.work]\n  entity_writes: {hub: {save: [%s]}}\n", side, side, side, strings.Join(fields, ", "))
	}
	files["hub/entities.yaml"], files["hub/agents.yaml"] = entities.String(), agents.String()
	return files
}

func issue2564H1WriteCorpus(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	slices.Sort(paths)
	manifest := sha256.New()
	for _, path := range paths {
		contents := files[path]
		checksum := fmt.Sprintf("%x", sha256.Sum256([]byte(contents)))
		fmt.Fprintf(manifest, "%s  %s\n", checksum, path)
		t.Logf("H1_EQUIVALENT_SHA256 %s  %s", checksum, path)
		writeWorkflowValidationFixtureFile(t, filepath.Join(root, path), contents)
	}
	checksum := fmt.Sprintf("%x", manifest.Sum(nil))
	if checksum != issue2564H1CorpusSHA {
		t.Fatalf("reconstructed equivalent H1 corpus drift: sha256=%s pinned=%s", checksum, issue2564H1CorpusSHA)
	}
	t.Logf("H1_EQUIVALENT_CORPUS_SHA256 %s", checksum)
	return root
}

func issue2564H1StartServed(t *testing.T, backend, root string, provider *issue2564H1Provider) servedControlProofRuntime {
	t.Helper()
	opts, start := lifecycleRestartHarness(t, backend, root)
	server := httptest.NewServer(provider)
	t.Cleanup(server.Close)
	setDoctorProviderSecret(t, "OPENAI_COMPATIBLE_API_KEY", "issue2564-h1-proof-key")
	t.Setenv("OPENAI_COMPATIBLE_API_KEY", "")
	config, err := os.ReadFile(opts.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	text := strings.Replace(string(config), "  backend: anthropic\n", "  backend: openai_compatible\n  openai_compatible:\n    base_url: "+server.URL+"\n", 1)
	if text == string(config) {
		t.Fatal("normal served launch no longer exposes the expected provider config")
	}
	writeWorkflowValidationFixtureFile(t, opts.ConfigPath, text)
	process, rt := start()
	t.Cleanup(func() {
		if code := process.stop(); code != 0 {
			t.Errorf("H1 normal served shutdown=%d", code)
		}
		if t.Failed() {
			t.Logf("H1 served output:\n%s", process.outputString())
		}
	})
	return rt
}

type issue2564H1Request struct {
	Messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
	Tools []struct {
		Function struct {
			Name       string         `json:"name"`
			Parameters map[string]any `json:"parameters"`
		} `json:"function"`
	} `json:"tools"`
}

type issue2564H1Ack struct {
	entity, field string
	revision      int
}

type issue2564H1Session struct {
	round int
	acks  map[string]issue2564H1Ack
}

type issue2564H1Provider struct {
	t                     *testing.T
	mu                    sync.Mutex
	sessions              map[string]*issue2564H1Session
	firstActive, firstMax int
}

var issue2564H1HubPattern = regexp.MustCompile(`\bh0[1-6]\b`)

func (p *issue2564H1Provider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var request issue2564H1Request
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		p.refuse(w, fmt.Errorf("decode real OpenAI-compatible request: %w", err))
		return
	}
	if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer issue2564-h1-proof-key" {
		p.refuse(w, fmt.Errorf("real provider route/credential mismatch: %s", r.URL.Path))
		return
	}
	side, hub := "", ""
	for _, message := range request.Messages {
		if message.Role == "system" {
			for _, candidate := range []string{"a", "b"} {
				if strings.Contains(message.Content, "H1_MARKER_"+candidate) {
					side = candidate
				}
			}
		}
		if hub == "" && message.Role == "user" {
			hub = issue2564H1HubPattern.FindString(message.Content)
		}
	}
	if side == "" || hub == "" {
		p.refuse(w, fmt.Errorf("real provider request omitted authored role/hub: side=%q hub=%q messages=%+v", side, hub, request.Messages))
		return
	}
	key := hub + "/" + side
	p.mu.Lock()
	session := p.sessions[key]
	if session == nil {
		session = &issue2564H1Session{acks: map[string]issue2564H1Ack{}}
		p.sessions[key] = session
	}
	round := session.round
	session.round++
	if round == 0 {
		p.firstActive++
		p.firstMax = max(p.firstMax, p.firstActive)
	}
	p.mu.Unlock()
	if round == 0 {
		defer func() {
			p.mu.Lock()
			p.firstActive--
			p.mu.Unlock()
		}()
		timer := time.NewTimer(1500 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-r.Context().Done():
			return
		}
	}
	if round > 7 {
		p.refuse(w, fmt.Errorf("H1 exceeded published seven tool rounds + stop: %s/%d", key, round))
		return
	}
	for n := 1; n <= issue2564H1Fields; n++ {
		name := fmt.Sprintf("save_hub_%s%02d", side, n)
		found := false
		for _, tool := range request.Tools {
			if tool.Function.Name == name && issue2564H1SchemaProperty(tool.Function.Parameters, "value") {
				found = true
			}
		}
		if !found {
			p.refuse(w, fmt.Errorf("generated role-scoped scalar tool missing: %s", name))
			return
		}
	}
	if round > 0 {
		var results []any
		for i := len(request.Messages) - 1; i >= 0; i-- {
			if results = issue2564H1ToolResults(request.Messages[i].Content); results != nil {
				break
			}
		}
		first := (round-1)*issue2564H1Batch + 1
		last := min(first+issue2564H1Batch-1, issue2564H1Fields)
		if len(results) != last-first+1 {
			p.refuse(w, fmt.Errorf("canonical current H1 result batch %s/%d has %d entries, want %d", key, round, len(results), last-first+1))
			return
		}
		for i, raw := range results {
			field := fmt.Sprintf("%s%02d", side, first+i)
			entry, _ := raw.(map[string]any)
			result, _ := entry["result"].(map[string]any)
			entity, _ := result["entity_id"].(string)
			revision, _ := result["revision"].(float64)
			if entry["name"] != "save_hub_"+field || entry["ok"] != true || result["field"] != field || entity == "" || revision < 1 || revision != float64(int(revision)) {
				p.refuse(w, fmt.Errorf("unacknowledged H1 scalar %s/%s: %#v", key, field, entry))
				return
			}
			p.mu.Lock()
			_, duplicate := session.acks[field]
			session.acks[field] = issue2564H1Ack{entity: entity, field: field, revision: int(revision)}
			p.mu.Unlock()
			if duplicate {
				p.refuse(w, fmt.Errorf("H1 repeated an acknowledged distinct save: %s/%s", key, field))
				return
			}
		}
	}
	message := map[string]any{"role": "assistant", "content": "All assigned fields written; stop."}
	finish := "stop"
	if round < 7 {
		calls := []any{}
		first := round*issue2564H1Batch + 1
		for n := first; n <= min(first+issue2564H1Batch-1, issue2564H1Fields); n++ {
			field := fmt.Sprintf("%s%02d", side, n)
			calls = append(calls, map[string]any{"id": hub + "-" + field, "type": "function", "function": map[string]any{"name": "save_hub_" + field, "arguments": `{"value":1}`}})
		}
		message["content"], message["tool_calls"], finish = "", calls, "tool_calls"
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"model": "gpt-compatible", "choices": []any{map[string]any{"index": 0, "message": message, "finish_reason": finish}}, "usage": map[string]int{"prompt_tokens": 8, "completion_tokens": 2, "total_tokens": 10}}); err != nil && r.Context().Err() == nil {
		p.t.Error(err)
	}
}

func (p *issue2564H1Provider) refuse(w http.ResponseWriter, err error) {
	p.t.Error(err)
	http.Error(w, err.Error(), http.StatusBadRequest)
}

func (p *issue2564H1Provider) assert(t *testing.T, want int) map[string]issue2564H1Ack {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	acks := map[string]issue2564H1Ack{}
	for key, session := range p.sessions {
		if session.round != 8 || len(session.acks) != issue2564H1Fields {
			t.Fatalf("published session not complete: %s rounds=%d acks=%d", key, session.round, len(session.acks))
		}
		for field, ack := range session.acks {
			acks[ack.entity+"/"+field] = ack
		}
	}
	if len(p.sessions) != issue2564H1Hubs*2 || len(acks) != want || p.firstMax < 2 {
		t.Fatalf("H1 concurrent provider/session acknowledgments: sessions=%d acks=%d want=%d overlapping_first_requests=%d", len(p.sessions), len(acks), want, p.firstMax)
	}
	t.Logf("H1_PROVIDER_RECEIPT sessions=%d acknowledgments=%d requests=%d overlapping_first_requests=%d first_round_delay=1.5s", len(p.sessions), len(acks), len(p.sessions)*8, p.firstMax)
	return acks
}

// The published H1 has no per-request or per-phase stopwatch. Keep its single
// qualification deadline instead of borrowing short budgets from small fixtures.
func issue2564H1Publish(t *testing.T, ctx context.Context, endpoint string, params map[string]any) servedEventPublishRPCResult {
	t.Helper()
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": params["idempotency_key"], "method": "event.publish", "params": params})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiv1.DefaultLoopbackAPIToken)
	started := time.Now()
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("H1 public event.publish: %v", err)
	}
	defer response.Body.Close()
	var envelope servedJSONRPCEnvelope
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || envelope.Error != nil {
		t.Fatalf("H1 publication HTTP=%d error=%+v", response.StatusCode, envelope.Error)
	}
	var result servedEventPublishRPCResult
	if err := json.Unmarshal(envelope.Result, &result); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Logf("H1_PUBLICATION_LATENCY key=%v elapsed=%s event=%s", params["idempotency_key"], elapsed, result.EventID)
	}
	return result
}

func issue2564H1WaitQuiescence(t *testing.T, ctx context.Context, rt servedControlProofRuntime, run string, provider *issue2564H1Provider) {
	t.Helper()
	stable, nextLog := 0, time.Now()
	for {
		var active int
		if err := rt.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status IN ('pending','in_progress')`, run).Scan(&active); err != nil {
			t.Fatalf("H1 durable delivery progress: %v", err)
		}
		if active == 0 {
			stable++
			if stable == 4 {
				return
			}
		} else {
			stable = 0
		}
		if time.Now().After(nextLog) {
			provider.mu.Lock()
			acks := 0
			for _, session := range provider.sessions {
				acks += len(session.acks)
			}
			t.Logf("H1_PROGRESS active_deliveries=%d provider_sessions=%d acknowledged_saves=%d", active, len(provider.sessions), acks)
			provider.mu.Unlock()
			nextLog = time.Now().Add(5 * time.Second)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("H1 qualification deadline before quiescence: %v", ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func issue2564H1PublishBumps(t *testing.T, ctx context.Context, rt servedControlProofRuntime, runID, sourceEvent string) {
	t.Helper()
	var workers sync.WaitGroup
	start := make(chan struct{})
	accepted := make(chan string, issue2564H1Bumps)
	for publisher := 0; publisher < issue2564H1Publishers; publisher++ {
		workers.Add(1)
		go func(publisher int) {
			defer workers.Done()
			<-start
			for n := publisher; n < issue2564H1Bumps; n += issue2564H1Publishers {
				result := issue2564H1Publish(t, ctx, rt.Endpoint, map[string]any{
					"event_name": "hub.bump", "run_id": runID, "source_event_id": sourceEvent,
					"payload":         map[string]any{"hub_id": fmt.Sprintf("h%02d", n%issue2564H1Hubs+1), "n": n},
					"idempotency_key": fmt.Sprintf("h1-bump-%03d", n),
				})
				if result.RunID != runID || result.EventID == "" || result.NewRunCreated {
					t.Errorf("H1 publisher %d bump %d escaped run: %+v", publisher, n, result)
					return
				}
				accepted <- result.EventID
			}
		}(publisher)
	}
	close(start)
	workers.Wait()
	close(accepted)
	ids := map[string]bool{}
	for id := range accepted {
		ids[id] = true
	}
	if len(ids) != issue2564H1Bumps {
		t.Fatalf("eight actual publishers accepted %d distinct bumps, want 600", len(ids))
	}
}

func issue2564H1Count(t *testing.T, db *sql.DB, query, runID string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(query, runID).Scan(&n); err != nil {
		t.Fatalf("H1 count: %v query=%s", err, query)
	}
	return n
}

func issue2564H1AssertStore(t *testing.T, rt servedControlProofRuntime, runID, mode string, acks map[string]issue2564H1Ack) {
	t.Helper()
	issue2564H1LogFailureWitnesses(t, rt, runID, acks)
	issue2564H1AssertSameEntityOverlap(t, rt, runID, mode)
	for query, want := range map[string]int{
		`SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND flow_template='.' AND entity_type IS NULL`:                                                                                                                                             1,
		`SELECT COUNT(*) FROM flow_instances f JOIN entity_state e ON e.run_id=f.run_id AND e.entity_id=f.entity_id WHERE f.run_id=$1 AND f.flow_template='.'`:                                                                                          0,
		`SELECT COUNT(*) FROM flow_instances WHERE run_id=$1 AND flow_template='hub' AND mode='template' AND current_state='active' AND status='active'`:                                                                                                6,
		`SELECT COUNT(*) FROM (SELECT DISTINCT agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,flow_instance FROM agent_turns WHERE run_id=$1 AND execution_mode='live' AND parse_ok=TRUE) identities`: 12,
		`SELECT COUNT(*) FROM agent_turns WHERE run_id=$1 AND execution_mode='live' AND parse_ok=TRUE`:                                                                                                                                                  96,
		`SELECT COUNT(DISTINCT session_id) FROM agent_turns WHERE run_id=$1 AND execution_mode='live' AND parse_ok=TRUE`:                                                                                                                                12,
		`SELECT COUNT(*) FROM agent_turns WHERE run_id=$1 AND (execution_mode<>'live' OR parse_ok=FALSE OR retry_count<>0 OR failure IS NOT NULL)`:                                                                                                      0,
		`SELECT COUNT(*) FROM events WHERE run_id=$1 AND event_name='hub.bump'`:                                                                                                                                                                         600,
		`SELECT COUNT(*) FROM event_deliveries d JOIN events e ON e.event_id=d.event_id WHERE d.run_id=$1 AND d.subscriber_type='node' AND e.event_name='hub.bump' AND d.status='delivered'`:                                                            600,
		`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND subscriber_type='agent' AND status='delivered'`:                                                                                                                                      12,
		`SELECT COUNT(*) FROM event_deliveries WHERE run_id=$1 AND status<>'delivered'`:                                                                                                                                                                 0,
		`SELECT COALESCE(SUM(retry_count),0) FROM event_deliveries WHERE run_id=$1`:                                                                                                                                                                     0,
		`SELECT COUNT(*) FROM dead_letters l JOIN events e ON e.event_id=l.original_event_id WHERE e.run_id=$1`:                                                                                                                                         0,
		`SELECT COUNT(*) FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id WHERE d.run_id=$1 AND (a.claim_version<>1 OR a.closure_kind<>'settled' OR a.outcome<>'delivered')`:                                       0,
	} {
		if got := issue2564H1Count(t, rt.DB, query, runID); got != want {
			t.Errorf("H1 store accounting=%d want=%d query=%s", got, want, query)
		}
	}
	rows, err := rt.DB.Query(`SELECT f.entity_id,f.revision,CAST(e.fields AS TEXT) FROM flow_instances f JOIN entity_state e ON e.run_id=f.run_id AND e.entity_id=f.entity_id WHERE f.run_id=$1 AND f.flow_template='hub'`, runID)
	if err != nil {
		t.Fatal(err)
	}
	counts, lost := 0, 0
	losses := []string{}
	for rows.Next() {
		var entity, raw string
		var revision int
		if err := rows.Scan(&entity, &revision, &raw); err != nil {
			t.Fatal(err)
		}
		var fields map[string]any
		if err := json.Unmarshal([]byte(raw), &fields); err != nil {
			t.Fatal(err)
		}
		if fields["count"] != float64(100) {
			t.Errorf("H1 count not exact per-hub delivered bumps: entity=%s count=%v want=100", entity, fields["count"])
		}
		count, _ := fields["count"].(float64)
		counts += int(count)
		for _, side := range []string{"a", "b"} {
			for n := 1; n <= issue2564H1Fields; n++ {
				field := fmt.Sprintf("%s%02d", side, n)
				ack, ok := acks[entity+"/"+field]
				if fields[field] != float64(1) || !ok {
					lost++
					losses = append(losses, fmt.Sprintf("entity=%s field=%s value=%v receipt=%+v header=%d", entity, field, fields[field], ack, revision))
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	t.Logf("H1_FIELD_RECEIPT acknowledgments=%d lost=%d count=%d planned_bumps=600", len(acks), lost, counts)
	if lost != 0 {
		t.Errorf("lost/reverted H1 acknowledged scalars=%d: %s", lost, strings.Join(losses, "; "))
	}
	if counts != issue2564H1Bumps {
		t.Errorf("H1 total count=%d, want exact600", counts)
	}
	issue2564H1AssertBumpHistory(t, rt, runID)
	rows, err = rt.DB.Query(`SELECT m.entity_id,m.path,m.writer_type,m.writer_id,m.handler_step,CAST(m.old_value AS TEXT),CAST(m.new_value AS TEXT),e.event_name,COALESCE(a.agent_name_owner,'') FROM entity_mutations m JOIN events e ON e.event_id=m.caused_by_event LEFT JOIN agents a ON a.run_id=m.run_id AND a.agent_id=m.writer_id AND a.entity_id=m.entity_id AND a.flow_scope_key='hub' AND a.agent_name_source='declared' AND a.agent_route_presence='present' AND a.lifecycle_bundle_hash=$2 WHERE m.run_id=$1 AND m.domain='authored_field' AND (m.path LIKE 'a%' OR m.path LIKE 'b%')`, runID, rt.BundleHash)
	if err != nil {
		t.Fatal(err)
	}
	mutations, writers := map[string]bool{}, map[string]int{}
	initials := map[string]bool{}
	for rows.Next() {
		var entity, field, kind, writer, step, before, after, event, owner string
		if err := rows.Scan(&entity, &field, &kind, &writer, &step, &before, &after, &event, &owner); err != nil {
			t.Fatal(err)
		}
		key := entity + "/" + field
		var old, value any
		if err := json.Unmarshal([]byte(before), &old); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(after), &value); err != nil {
			t.Fatal(err)
		}
		if kind == "platform" && writer == "entity_initial_value" && step == "create_entity" && old == nil && value == float64(0) && event == "hub.start" {
			if initials[key] {
				t.Fatalf("H1 constructor repeated an initial value: %s", key)
			}
			initials[key] = true
			continue
		}
		if _, ok := acks[key]; !ok || mutations[key] || kind != "agent" || writer == "" || owner != "swarm://hub/marker-"+field[:1] || step != "save_entity_field" || !strings.HasSuffix(event, "/hub.work") || value != float64(1) || old != nil && old != float64(0) {
			t.Fatalf("H1 missing attribution/repeated save or platform reversion: %s kind=%s writer=%s step=%s old=%s new=%s event=%s", key, kind, writer, step, before, after, event)
		}
		mutations[key] = true
		writers[entity+"/"+writer]++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if len(mutations) != 720 || len(writers) != 12 {
		t.Fatalf("H1 exact agent mutation corpus=%d writers=%v", len(mutations), writers)
	}
	for writer, n := range writers {
		if n != 60 {
			t.Fatalf("H1 writer %s mutations=%d want60", writer, n)
		}
	}
	// Keep the run-wide control too; the same-entity witness is checked above.
	var overlap int
	if err := rt.DB.QueryRow(`SELECT COUNT(*) FROM entity_mutations m JOIN events e ON e.event_id=m.caused_by_event WHERE m.run_id=$1 AND m.path='count' AND e.event_name='hub.bump' AND m.created_at>=(SELECT MIN(created_at) FROM entity_mutations WHERE run_id=$1 AND writer_type='agent') AND m.created_at<=(SELECT MAX(created_at) FROM entity_mutations WHERE run_id=$1 AND writer_type='agent')`, runID).Scan(&overlap); err != nil {
		t.Fatal(err)
	}
	if mode == "overlap" && overlap == 0 || mode == "no_overlap" && overlap != 0 {
		t.Fatalf("H1 actual committed overlap=%d mode=%s", overlap, mode)
	}
	t.Logf("H1 run-wide mutation-time overlap bumps=%d; constructor initial receipts=%d; per-agent mutation counts=%v", overlap, len(initials), writers)
}

func issue2564H1AssertSameEntityOverlap(t *testing.T, rt servedControlProofRuntime, runID, mode string) {
	t.Helper()
	rows, err := rt.DB.Query(`SELECT f.entity_id,f.instance_path,CAST(s.fields AS TEXT),
		(SELECT COUNT(*) FROM entity_mutations m JOIN events e ON e.event_id=m.caused_by_event
		 WHERE m.run_id=f.run_id AND m.entity_id=f.entity_id AND m.domain='authored_field' AND m.path='count' AND e.event_name='hub.bump'
		 AND m.created_at>=(SELECT MIN(a.created_at) FROM entity_mutations a WHERE a.run_id=f.run_id AND a.entity_id=f.entity_id AND a.domain='authored_field' AND a.writer_type='agent')
		 AND m.created_at<=(SELECT MAX(a.created_at) FROM entity_mutations a WHERE a.run_id=f.run_id AND a.entity_id=f.entity_id AND a.domain='authored_field' AND a.writer_type='agent'))
		FROM flow_instances f JOIN entity_state s ON s.run_id=f.run_id AND s.entity_id=f.entity_id
		WHERE f.run_id=$1 AND f.flow_template='hub' ORDER BY f.instance_path`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	hubs, overlappingHubs, bumps := 0, 0, 0
	for rows.Next() {
		var entity, route, raw string
		var count int
		if err := rows.Scan(&entity, &route, &raw, &count); err != nil {
			t.Fatal(err)
		}
		var fields map[string]any
		if err := json.Unmarshal([]byte(raw), &fields); err != nil {
			t.Fatal(err)
		}
		hubs++
		bumps += count
		if count > 0 {
			overlappingHubs++
		}
		t.Logf("H1_SAME_ENTITY_OVERLAP hub=%v entity=%s route=%s count_mutations_between_agent_times=%d", fields["hub_id"], entity, route, count)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Logf("H1_SAME_ENTITY_OVERLAP_TOTAL mode=%s hubs=%d overlapping_hubs=%d count_mutations=%d", mode, hubs, overlappingHubs, bumps)
	if hubs != issue2564H1Hubs || mode == "overlap" && overlappingHubs == 0 || mode == "no_overlap" && overlappingHubs != 0 {
		t.Errorf("H1 same-entity mutation-time overlap mode=%s hubs=%d overlapping_hubs=%d count_mutations=%d", mode, hubs, overlappingHubs, bumps)
	}
}

type issue2564H1MutationEvidence struct {
	id, entity, field, kind, writer, step, before, after, event, eventName, recordedAt string
}

// Read all failure witnesses before any fatal acceptance assertion can hide a
// baseline agent acknowledgment followed by its real platform overwrite.
func issue2564H1LogFailureWitnesses(t *testing.T, rt servedControlProofRuntime, runID string, acks map[string]issue2564H1Ack) {
	t.Helper()
	rows, err := rt.DB.Query(`SELECT m.mutation_id,m.entity_id,m.path,m.writer_type,m.writer_id,COALESCE(m.handler_step,''),
		COALESCE(CAST(m.old_value AS TEXT),'null'),COALESCE(CAST(m.new_value AS TEXT),'null'),
		COALESCE(CAST(m.caused_by_event AS TEXT),''),COALESCE(e.event_name,''),CAST(m.created_at AS TEXT)
		FROM entity_mutations m LEFT JOIN events e ON e.event_id=m.caused_by_event
		WHERE m.run_id=$1 AND m.domain='authored_field' AND (m.path LIKE 'a%' OR m.path LIKE 'b%')
		ORDER BY m.entity_id,m.path,m.created_at,m.mutation_id`, runID)
	if err != nil {
		t.Fatal(err)
	}
	agents := map[string][]issue2564H1MutationEvidence{}
	var reversions []issue2564H1MutationEvidence
	agentMutations := 0
	for rows.Next() {
		var m issue2564H1MutationEvidence
		if err := rows.Scan(&m.id, &m.entity, &m.field, &m.kind, &m.writer, &m.step, &m.before, &m.after, &m.event, &m.eventName, &m.recordedAt); err != nil {
			t.Fatal(err)
		}
		var before, after any
		if err := json.Unmarshal([]byte(m.before), &before); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(m.after), &after); err != nil {
			t.Fatal(err)
		}
		key := m.entity + "/" + m.field
		if _, acknowledged := acks[key]; !acknowledged {
			continue
		}
		if m.kind == "agent" && after == float64(1) {
			agents[key] = append(agents[key], m)
			agentMutations++
		}
		if m.kind == "platform" && before == float64(1) && (after == nil || after == float64(0)) {
			reversions = append(reversions, m)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	for _, m := range reversions {
		key := m.entity + "/" + m.field
		t.Logf("H1_PLATFORM_REVERSION ack=%+v agent_mutations=%+v overwrite=%+v", acks[key], agents[key], m)
	}
	t.Logf("H1_EARLY_MUTATION_RECEIPT acknowledgments=%d agent_mutations=%d platform_reversions=%d", len(acks), agentMutations, len(reversions))
	rows, err = rt.DB.Query(`SELECT d.delivery_id,e.event_id,e.event_name,d.status,d.retry_count,COALESCE(d.reason_code,''),COALESCE(CAST(d.failure AS TEXT),'null')
		FROM event_deliveries d JOIN events e ON e.event_id=d.event_id
		WHERE d.run_id=$1 AND d.subscriber_type='node' AND (d.retry_count<>0 OR d.status='dead_letter' OR d.failure IS NOT NULL)
		ORDER BY d.created_at,d.delivery_id`, runID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var delivery, event, name, status, reason, failure string
		var retries int
		if err := rows.Scan(&delivery, &event, &name, &status, &retries, &reason, &failure); err != nil {
			t.Fatal(err)
		}
		t.Logf("H1_NODE_FAILURE delivery=%s event=%s name=%s status=%s retry_count=%d reason=%s failure=%s", delivery, event, name, status, retries, reason, failure)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	rows, err = rt.DB.Query(`SELECT d.delivery_id,e.event_id,e.event_name,a.claim_version,a.closure_kind,COALESCE(a.outcome,''),COALESCE(a.reason_code,''),COALESCE(CAST(a.failure AS TEXT),'null')
		FROM event_delivery_attempts a JOIN event_deliveries d ON d.delivery_id=a.delivery_id JOIN events e ON e.event_id=d.event_id
		WHERE d.run_id=$1 AND d.subscriber_type='node' AND (a.outcome IN ('retry_scheduled','dead_letter') OR a.failure IS NOT NULL)
		ORDER BY d.delivery_id,a.claim_version`, runID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var delivery, event, name, closure, outcome, reason, failure string
		var version int
		if err := rows.Scan(&delivery, &event, &name, &version, &closure, &outcome, &reason, &failure); err != nil {
			t.Fatal(err)
		}
		t.Logf("H1_NODE_ATTEMPT_FAILURE delivery=%s event=%s name=%s claim_version=%d closure=%s outcome=%s reason=%s failure=%s", delivery, event, name, version, closure, outcome, reason, failure)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	rows, err = rt.DB.Query(`SELECT l.dead_letter_id,e.event_id,e.event_name,COALESCE(CAST(l.delivery_id AS TEXT),''),COALESCE(l.claim_version,0),l.retry_count,COALESCE(l.handler_node,''),CAST(l.failure AS TEXT)
		FROM dead_letters l JOIN events e ON e.event_id=l.original_event_id WHERE e.run_id=$1 ORDER BY l.created_at,l.dead_letter_id`, runID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id, event, name, delivery, node, failure string
		var version, retries int
		if err := rows.Scan(&id, &event, &name, &delivery, &version, &retries, &node, &failure); err != nil {
			t.Fatal(err)
		}
		t.Logf("H1_DEAD_LETTER id=%s event=%s name=%s delivery=%s claim_version=%d retry_count=%d node=%s failure=%s", id, event, name, delivery, version, retries, node, failure)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
}

func issue2564H1AssertBumpHistory(t *testing.T, rt servedControlProofRuntime, runID string) {
	t.Helper()
	rows, err := rt.DB.Query(`SELECT m.entity_id,m.caused_by_event,CAST(m.old_value AS TEXT),CAST(m.new_value AS TEXT) FROM entity_mutations m JOIN events e ON e.event_id=m.caused_by_event JOIN event_deliveries d ON d.event_id=e.event_id AND d.run_id=m.run_id AND d.subscriber_type='node' WHERE m.run_id=$1 AND m.domain='authored_field' AND m.path='count' AND e.event_name='hub.bump' AND d.status='delivered'`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	chains, events := map[string]map[int]bool{}, map[string]bool{}
	for rows.Next() {
		var entity, event, before, after string
		if err := rows.Scan(&entity, &event, &before, &after); err != nil {
			t.Fatal(err)
		}
		var old, next float64
		if err := json.Unmarshal([]byte(before), &old); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(after), &next); err != nil {
			t.Fatal(err)
		}
		if chains[entity] == nil {
			chains[entity] = map[int]bool{}
		}
		if next != old+1 || old != float64(int(old)) || old < 0 || old >= 100 || events[event] || chains[entity][int(old)] {
			t.Fatalf("H1 counter reversion/repeated effect: entity=%s event=%s %s -> %s", entity, event, before, after)
		}
		chains[entity][int(old)], events[event] = true, true
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(events) != issue2564H1Bumps || len(chains) != issue2564H1Hubs {
		t.Fatalf("H1 exact retained bump history=%d hubs=%d", len(events), len(chains))
	}
	for entity, chain := range chains {
		if len(chain) != 100 {
			t.Fatalf("H1 retained count chain for %s has %d steps, want100", entity, len(chain))
		}
	}
}

// Same canonical-continuation parsing as the read-only H3 helper, scoped here
// so this exact source can execute on the unmodified baseline production tree.
func issue2564H1ToolResults(value any) []any {
	switch v := value.(type) {
	case map[string]any:
		if v["kind"] == "tool_continuation" {
			results, _ := v["tool_result"].([]any)
			return results
		}
		for _, child := range v {
			if found := issue2564H1ToolResults(child); found != nil {
				return found
			}
		}
	case []any:
		for i := len(v) - 1; i >= 0; i-- {
			if found := issue2564H1ToolResults(v[i]); found != nil {
				return found
			}
		}
	case string:
		var decoded any
		if json.Unmarshal([]byte(strings.TrimPrefix(v, "Tool result:\n")), &decoded) == nil {
			if _, isString := decoded.(string); !isString {
				return issue2564H1ToolResults(decoded)
			}
		}
	}
	return nil
}

func issue2564H1SchemaProperty(value any, name string) bool {
	switch v := value.(type) {
	case map[string]any:
		if properties, ok := v["properties"].(map[string]any); ok && properties[name] != nil {
			return true
		}
		for _, child := range v {
			if issue2564H1SchemaProperty(child, name) {
				return true
			}
		}
	case []any:
		for _, child := range v {
			if issue2564H1SchemaProperty(child, name) {
				return true
			}
		}
	}
	return false
}
