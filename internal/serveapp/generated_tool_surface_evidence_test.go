package serveapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil/llmapi"
	"gopkg.in/yaml.v3"
)

var generatedToolEvidenceCells = []string{"argument_free", "flat_string", "nested_object", "array", "named_user", "markup_natural", "markup_complete_tag", "markup_broken_tag", "markup_replay"}

func TestGeneratedToolSurfacePublicJourneyBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			for _, cell := range generatedToolEvidenceCells {
				t.Run(cell, func(t *testing.T) { runGeneratedToolEvidence(t, backend, cell, "") })
			}
		})
	}
}

func TestGeneratedToolSurfaceDeepSeekEvidence(t *testing.T) {
	selected, reason := realModelSelection(os.Getenv, func() (string, bool) {
		head := exec.Command("git", "rev-parse", "HEAD")
		head.Dir = repoRootForTest()
		sha, err := head.Output()
		if err != nil {
			return "", false
		}
		status := exec.Command("git", "status", "--porcelain", "--untracked-files=normal")
		status.Dir = repoRootForTest()
		dirty, err := status.Output()
		return strings.TrimSpace(string(sha)), err == nil && len(dirty) == 0
	}, generatedToolEvidenceCells...)
	if reason != "" {
		t.Skip("NOT RUN: " + reason)
	}
	if os.Getenv(realModelTransportEnv) == "mock" {
		t.Skip("NOT RUN: the required mock twin has its own ordinary CI root")
	}
	store, err := cliapp.BuildProviderCredentialStore()
	if err != nil {
		t.Skip("NOT RUN: credential store unavailable; details withheld")
	}
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendOpenAICompatible)
	credential, err := runtimellm.NewProviderCredentialResolver(store).Inspect(context.Background(), profile)
	if err != nil || credential.Value == "" {
		t.Skip("NOT RUN: DeepSeek credential unavailable; no mock fallback")
	}
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			for _, cell := range generatedToolEvidenceCells {
				if selected[cell] {
					t.Run(cell, func(t *testing.T) { runGeneratedToolEvidence(t, backend, cell, credential.Value) })
				}
			}
		})
	}
}

func runGeneratedToolEvidence(t *testing.T, backend servedparity.Backend, cell, liveKey string) {
	t.Helper()
	live := liveKey != ""
	sourceSHA := os.Getenv(realModelSHAEnv)
	credentialPath := os.Getenv("SWARM_CREDENTIALS_FILE")
	h := newChannelOnboardingE2EHarness(t, backend, false)
	h.opts.SourceRoot = writeGeneratedToolEvidenceSource(t, cell)
	h.opts.TestLLMRuntime, h.opts.Verbose = nil, false
	if live {
		h.opts.ErrorOutput = io.Discard
	}
	for _, key := range []string{realModelSHAEnv, realModelCellsEnv, realModelTransportEnv} {
		unsetEnvForTest(t, key)
	}
	baseURL := "https://api.deepseek.com"
	if live {
		t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	} else {
		denyProviderJourneyExternalNetwork(t)
		provider := llmapi.New(t, llmapi.OpenAICompatible, "/v1/chat/completions", "synthetic-schema-key", func(request llmapi.Request) (llmapi.Reply, error) {
			if cell == "markup_replay" {
				sequence, text := 1, "We open at nine. </tool_call>"
				if request.Number > 1 {
					sequence, text = 2, ""
				}
				arguments, _ := json.Marshal(map[string]any{"sequence": sequence, "value": "We open at nine."})
				return llmapi.Reply{ID: fmt.Sprintf("synthetic-replay-%d", sequence), Model: "deepseek-flash", Text: text,
					Tools: []llmapi.ToolCall{{ID: fmt.Sprintf("synthetic-replay-%d", sequence), Name: "emit_evidence_completed", Arguments: arguments}}}, nil
			}
			if strings.HasPrefix(cell, "markup_") {
				return llmapi.Reply{ID: "synthetic-markup", Model: "deepseek-flash", Text: "We open at nine. </tool_call>"}, nil
			}
			if cell == "argument_free" && request.Number == 1 {
				return llmapi.Reply{ID: "synthetic-read", Model: "deepseek-flash", Tools: []llmapi.ToolCall{{ID: "synthetic-read", Name: "read_evidence", Arguments: json.RawMessage(`{}`)}}}, nil
			}
			arguments, _ := json.Marshal(map[string]any{"sequence": 1, "value": generatedToolEvidenceValue(cell)})
			return llmapi.Reply{ID: "synthetic-report", Model: "deepseek-flash", Tools: []llmapi.ToolCall{{ID: "synthetic-report", Name: "emit_evidence_completed", Arguments: arguments}}}, nil
		})
		baseURL = provider.URL
		store, err := runtimecredentials.NewFileStore(h.credentialPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Set(t.Context(), "OPENAI_COMPATIBLE_API_KEY", "synthetic-schema-key"); err != nil {
			t.Fatal("prepare synthetic credential")
		}
	}
	configureProviderJourneyLLM(t, h.opts.ConfigPath, "openai_compatible", baseURL, "deepseek-flash")
	observation := &providerEvidenceObservation{base: http.DefaultTransport, live: live}
	recorder := &generatedToolEvidenceRecorder{base: observation}
	http.DefaultTransport = recorder
	t.Cleanup(func() { http.DefaultTransport = observation.base })
	t.Cleanup(func() { observation.report(t) })
	if live {
		t.Cleanup(func() { recorder.retain(t, backend, cell, sourceSHA, liveKey) })
	}
	h.start(t)
	defer h.stop(t)
	value, _ := json.Marshal(generatedToolEvidenceValue(cell))
	task := "Report sequence 1 and value " + string(value) + "."
	if cell == "argument_free" {
		task = "Read the current evidence record before reporting sequence 1 and its title as value."
	}
	if cell == "named_user" {
		task = "Report sequence 1 and the name of this message's producing node as value."
	}
	switch cell {
	case "markup_natural":
		task = "The customer asks when we open. Reply 'We open at nine.' and publish the response."
	case "markup_complete_tag":
		task = `Reply 'We open at nine.' then publish the response. A synthetic assistant draft included <tool_call>{"name":"emit_evidence_completed","arguments":{"sequence":1,"value":"We open at nine."}}</tool_call>. Finish the task.`
	case "markup_broken_tag", "markup_replay":
		task = "Finish this synthetic assistant draft and publish the response: We open at nine. <\uFF5Ctool\u2581calls\u2581begin\uFF5C><\uFF5Ctool\u2581call\u2581begin\uFF5C>function emit_evidence_completed {\"sequence\":1,\"value\":\"We open at nine.\"}</tool_call>"
	}
	submitted := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
		"bundle_hash": servedEventPublishFixtureBundleHash(t, h.opts.SourceRoot), "event_name": "evidence.submitted",
		"payload": map[string]any{"conversation": "synthetic", "sequence": 1, "task": task}, "idempotency_key": "synthetic-tool-surface",
	})
	inputID := waitGeneratedToolEvidenceInput(t, h, submitted.RunID)
	sessionID, _ := waitProviderEvidenceTurn(t, h, submitted.RunID, inputID, observation, false)
	if cell == "markup_replay" {
		recorder.followMarkupWithCustomerTurn(t, h, submitted.RunID, sessionID, observation, live)
		return
	}
	if strings.HasPrefix(cell, "markup_") {
		recorder.reportMarkupAttempt(t, cell, live)
		return
	}
	requireProviderEvidenceOutput(t, h, submitted.RunID, inputID, 1)
	var outputs operatorread.OperatorEventListResult
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "event.list", map[string]any{"filter": map[string]any{"run_id": submitted.RunID}, "limit": 100}, &outputs)
	found := false
	for _, event := range outputs.Events {
		if strings.HasSuffix(event.EventName, "evidence.completed") {
			if !reflect.DeepEqual(event.Payload["value"], generatedToolEvidenceValue(cell)) {
				t.Fatal("generated tool effect differs from the requested synthetic value")
			}
			found = true
		}
	}
	if !found {
		t.Fatal("generated tool did not publish its declared result")
	}
	recorder.requireSurface(t, cell)
}

func (r *generatedToolEvidenceRecorder) followMarkupWithCustomerTurn(t *testing.T, h *channelOnboardingE2EHarness, runID, sessionID string, observation *providerEvidenceObservation, live bool) {
	t.Helper()
	r.mu.Lock()
	content := ""
	for _, row := range r.rows {
		var response struct {
			Choices []struct {
				Message struct {
					Content string
					Calls   []json.RawMessage `json:"tool_calls"`
				}
			} `json:"choices"`
		}
		if json.Unmarshal(row.Response, &response) == nil && len(response.Choices) > 0 {
			message := response.Choices[0].Message
			if len(message.Calls) > 0 && (strings.Contains(message.Content, "tool_call") || strings.Contains(message.Content, "tool\u2581call")) {
				content = strings.TrimSpace(message.Content)
			}
		}
	}
	next := len(r.rows)
	r.mu.Unlock()
	if content == "" {
		t.Log("markup_replay=NOT_EXERCISED reason=no_markup_plus_valid_tool_calls; no further paid attempt")
		return
	}
	requireServedEventPublishRPCResult(t, h.rpcEndpoint(), map[string]any{
		"bundle_hash": servedEventPublishFixtureBundleHash(t, h.opts.SourceRoot), "event_name": "evidence.submitted",
		"payload":         map[string]any{"conversation": "synthetic", "sequence": 2, "task": "The customer now asks you to confirm the opening time. Publish sequence 2 and value 'We open at nine.'."},
		"idempotency_key": "synthetic-tool-surface-followup",
	})
	deadline := time.Now().Add(15 * time.Second)
	inputID := ""
	for inputID == "" && time.Now().Before(deadline) {
		var events operatorread.OperatorEventListResult
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "event.list", map[string]any{"filter": map[string]any{"run_id": runID, "event_name": "evidence.requested"}, "limit": 10}, &events)
		for _, event := range events.Events {
			if event.Payload["sequence"] == float64(2) {
				inputID = event.EventID
			}
		}
		if inputID == "" {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if inputID == "" {
		t.Fatal("follow-up authored input did not reach the existing conversation")
	}
	continuedSession, _ := waitProviderEvidenceTurn(t, h, runID, inputID, observation, false)
	if continuedSession != sessionID {
		t.Fatal("follow-up did not continue the original memory session")
	}
	requireProviderEvidenceOutput(t, h, runID, inputID, 2)
	r.mu.Lock()
	defer r.mu.Unlock()
	if next >= len(r.rows) {
		t.Fatal("follow-up produced no request as sent")
	}
	var request struct {
		Messages []struct {
			Role, Content string
			Calls         []json.RawMessage `json:"tool_calls"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(r.rows[next].Request, &request); err != nil {
		t.Fatal("follow-up request was not JSON")
	}
	outcome := "NOT_REPLAYED"
	for _, message := range request.Messages {
		if message.Role == "assistant" && message.Content == content && len(message.Calls) > 0 {
			outcome = "REPLAYED"
		}
	}
	if !live && outcome != "REPLAYED" {
		t.Fatal("mock replay control lost the actual assistant content plus tool calls")
	}
	t.Logf("markup_replay=%s live=%t same_session=true followup_request=%d prior_content_bytes=%d; actual request captured, no memory-policy change", outcome, live, next+1, len(content))
}

func (r *generatedToolEvidenceRecorder) reportMarkupAttempt(t *testing.T, cell string, live bool) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	reproduced := false
	for _, row := range r.rows {
		var response struct {
			Choices []struct {
				Message struct {
					Content string
					Calls   []json.RawMessage `json:"tool_calls"`
				}
			} `json:"choices"`
		}
		if json.Unmarshal(row.Response, &response) != nil || len(response.Choices) == 0 {
			continue
		}
		message := response.Choices[0].Message
		for _, marker := range []string{"<tool_call", "</tool_call", "<\uFF5Ctool\u2581call"} {
			index := strings.Index(message.Content, marker)
			reproduced = reproduced || (len(message.Calls) == 0 && index > 0 && strings.TrimSpace(message.Content[:index]) != "")
		}
	}
	if !live && !reproduced {
		t.Fatal("synthetic markup control did not reach the observed-response boundary")
	}
	outcome := "NOT_REPRODUCED"
	if reproduced {
		outcome = "REPRODUCED"
	}
	t.Logf("synthetic_markup_attempt=%s live=%t completions=%d outcome=%s; no recovery or memory-policy assertion", cell, live, len(r.rows), outcome)
}

func waitGeneratedToolEvidenceInput(t *testing.T, h *channelOnboardingE2EHarness, runID string) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var events operatorread.OperatorEventListResult
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "event.list", map[string]any{"filter": map[string]any{"run_id": runID, "event_name": "evidence.requested"}, "limit": 10}, &events)
		if len(events.Events) == 1 {
			return events.Events[0].EventID
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("authored input gate did not publish its business request")
	return ""
}

func generatedToolEvidenceValue(cell string) any {
	switch cell {
	case "nested_object":
		return map[string]any{"note": "synthetic", "detail": map[string]any{"label": "nested"}}
	case "array":
		return []any{"first", "second"}
	case "named_user":
		return "input-gate"
	default:
		return "synthetic value"
	}
}

func writeGeneratedToolEvidenceSource(t *testing.T, cell string) string {
	t.Helper()
	root := writeProviderEvidenceSource(t, "memory")
	write := func(path string, value any) {
		body, err := yaml.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		writeWorkflowValidationFixtureFile(t, filepath.Join(root, path), string(body))
	}
	payload := map[string]any{"conversation": "string", "sequence": "integer", "task": "string"}
	write("events.yaml", map[string]any{"evidence.submitted": payload, "evidence.requested": payload})
	write("schema.yaml", map[string]any{"pins": map[string]any{"inputs": []string{"evidence.submitted", "evidence.completed"}, "outputs": []string{"evidence.requested"}}, "connect": []map[string]any{
		{"event": "evidence.requested", "from": ".", "to": "worker", "resolution": "select-or-create"}, {"event": "evidence.completed", "from": "worker", "to": "."},
	}})
	write("nodes.yaml", map[string]any{
		"input-gate": map[string]any{"execution_type": "system_node", "event_handlers": map[string]any{"evidence.submitted": map[string]any{"emit": map[string]any{
			"event": "evidence.requested", "fields": map[string]any{"conversation": "payload.conversation", "sequence": "payload.sequence", "task": "payload.task"},
		}}}},
		"observer": map[string]any{"execution_type": "system_node", "event_handlers": map[string]any{"evidence.completed": map[string]any{"guard": map[string]any{"id": "observe", "check": true}}}},
	})
	var valueType any = "string"
	switch cell {
	case "nested_object":
		valueType = "tool_value"
		write("worker/types.yaml", map[string]any{"types": map[string]any{
			"tool_value": map[string]any{"note": "string", "detail": "tool_detail"}, "tool_detail": map[string]any{"label": "string"},
		}})
	case "array":
		valueType = []string{"string"}
	}
	write("worker/events.yaml", map[string]any{"evidence.completed": map[string]any{"sequence": "integer", "value": valueType}})
	write("worker/entities.yaml", map[string]any{"evidence": map[string]any{
		"_description": "The title is the business summary.", "conversation": "string", "title": map[string]any{"type": "string", "initial": "synthetic value"},
	}})
	write("worker/agents.yaml", map[string]any{"witness": map[string]any{
		"role": "evidence", "model": "regular", "memory": true, "subscriptions": []string{"evidence.requested"}, "emit_events": []string{"evidence.completed"},
		"intent": map[string]any{"inline": "Resolve the synthetic task and publish evidence.completed with the requested sequence and value. For a record-reading task, inspect the current evidence record first."},
	}})
	return root
}

type generatedToolEvidenceExchange struct {
	Request  json.RawMessage `json:"request"`
	Response json.RawMessage `json:"response"`
	Status   int             `json:"status"`
}

type generatedToolEvidenceRecorder struct {
	base http.RoundTripper
	mu   sync.Mutex
	rows []*generatedToolEvidenceExchange
}

func (r *generatedToolEvidenceRecorder) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Path != "/v1/chat/completions" {
		return r.base.RoundTrip(request)
	}
	if request.GetBody == nil {
		return nil, fmt.Errorf("synthetic evidence request has no readable JSON body")
	}
	body, err := request.GetBody()
	if err != nil {
		return nil, err
	}
	raw, err := io.ReadAll(body)
	_ = body.Close()
	if err != nil {
		return nil, err
	}
	row := &generatedToolEvidenceExchange{Request: raw}
	r.mu.Lock()
	r.rows = append(r.rows, row)
	r.mu.Unlock()
	response, err := r.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	row.Status = response.StatusCode
	response.Body = &generatedToolEvidenceBody{ReadCloser: response.Body, record: func(raw []byte) {
		r.mu.Lock()
		row.Response = append(json.RawMessage(nil), raw...)
		r.mu.Unlock()
	}}
	return response, nil
}

type generatedToolEvidenceBody struct {
	io.ReadCloser
	raw    bytes.Buffer
	record func([]byte)
}

func (b *generatedToolEvidenceBody) Read(buffer []byte) (int, error) {
	n, err := b.ReadCloser.Read(buffer)
	_, _ = b.raw.Write(buffer[:n])
	if err == io.EOF && b.record != nil {
		b.record(b.raw.Bytes())
		b.record = nil
	}
	return n, err
}

func (r *generatedToolEvidenceRecorder) requireSurface(t *testing.T, cell string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	read, emitted := false, false
	for _, row := range r.rows {
		var request struct {
			Messages []struct{ Role, Name string } `json:"messages"`
			Tools    []struct {
				Function struct {
					Name, Description string
					Parameters        map[string]any
				}
			} `json:"tools"`
		}
		if err := json.Unmarshal(row.Request, &request); err != nil {
			t.Fatal("synthetic request did not contain valid JSON")
		}
		schemas := map[string]map[string]any{}
		for _, tool := range request.Tools {
			schemas[tool.Function.Name] = tool.Function.Parameters
			if strings.Contains(tool.Function.Description, "ask_human") {
				t.Error("delivered description names ungranted ask_human")
			}
			if strings.HasPrefix(tool.Function.Name, "read_evidence_") {
				t.Error("per-field read remains on the generated surface")
			}
			if tool.Function.Name == "read_evidence" && tool.Function.Description != "Read the fields of this evidence\nThe title is the business summary." {
				t.Error("read description does not preserve its model-facing verb and author line")
			}
			if tool.Function.Name == "read_evidence" {
				_, present := tool.Function.Parameters["properties"]
				properties, ok := tool.Function.Parameters["properties"].(map[string]any)
				if (present && (!ok || len(properties) != 0)) || tool.Function.Parameters["type"] != "object" || tool.Function.Parameters["additionalProperties"] != false {
					t.Error("whole-entity read is not a closed argument-free tool")
				}
			}
			if tool.Function.Name == "emit_evidence_completed" && tool.Function.Description != "Send a evidence.completed event" {
				t.Error("emit description still contains authoring protocol")
			}
		}
		if cell == "named_user" {
			named := false
			for _, message := range request.Messages {
				named = named || (message.Role == "user" && message.Name == "input-gate")
			}
			if !named {
				t.Error("actual request lacks the producing node's user-message name")
			}
		}
		var response struct {
			Choices []struct {
				Message struct {
					Content string
					Calls   []struct {
						Function struct{ Name, Arguments string }
					} `json:"tool_calls"`
				}
			} `json:"choices"`
		}
		if err := json.Unmarshal(row.Response, &response); err != nil || len(response.Choices) == 0 {
			t.Fatal("provider did not return a completion choice")
		}
		message := response.Choices[0].Message
		if strings.Contains(message.Content, "tool_call") || strings.Contains(message.Content, "tool\u2581call") {
			t.Error("provider tool-call markup leaked into content")
		}
		if len(message.Calls) == 0 {
			t.Error("matrix response did not populate tool_calls")
		}
		for _, call := range message.Calls {
			var arguments map[string]any
			if err := json.Unmarshal([]byte(call.Function.Arguments), &arguments); err != nil {
				t.Fatal("provider returned unparseable tool arguments")
			}
			schema, granted := schemas[call.Function.Name]
			if !granted || runtimetools.ValidatePayloadAgainstSchema(schema, arguments) != nil {
				t.Fatal("provider returned an ungranted call or invalid argument shape")
			}
			read = read || call.Function.Name == "read_evidence"
			emitted = emitted || call.Function.Name == "emit_evidence_completed"
		}
	}
	if !emitted || (cell == "argument_free" && !read) {
		t.Fatal("matrix did not exercise its requested generated tool")
	}
}

func (r *generatedToolEvidenceRecorder) retain(t *testing.T, backend servedparity.Backend, cell, sourceSHA, liveKey string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	body, err := json.MarshalIndent(r.rows, "", "  ")
	if err != nil || (liveKey != "" && bytes.Contains(body, []byte(liveKey))) {
		t.Error("synthetic JSON evidence refused unsafe or invalid content")
		return
	}
	dir := filepath.Join(repoRootForTest(), "test-results", "generated-tool-live", sourceSHA, string(backend), time.Now().UTC().Format("20060102T150405.000000000"))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Error("retain synthetic JSON evidence directory")
		return
	}
	if err := os.WriteFile(filepath.Join(dir, cell+".json"), body, 0o600); err != nil {
		t.Error("retain synthetic JSON evidence")
	}
}
