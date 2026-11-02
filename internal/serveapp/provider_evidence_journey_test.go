package serveapp

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	"github.com/division-sh/swarm/internal/runtime/core/identitytest"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil/llmapi"
	"gopkg.in/yaml.v3"
)

func runProviderEvidenceJourney(t *testing.T, backend servedparity.Backend, cell string, live bool) {
	t.Helper()
	credentialPath := os.Getenv("SWARM_CREDENTIALS_FILE")
	if live && credentialPath == "" {
		var err error
		credentialPath, err = runtimecredentials.DefaultFilePath()
		if err != nil {
			t.Skip("NOT RUN: credential location unavailable")
		}
	}
	h := newChannelOnboardingE2EHarness(t, backend, false)
	h.opts.SourceRoot = writeProviderEvidenceSource(t, cell)
	h.opts.TestLLMRuntime = nil
	h.opts.Verbose = false
	if live {
		h.opts.ErrorOutput = io.Discard
	}
	unsetEnvForTest(t, realModelSHAEnv)
	unsetEnvForTest(t, realModelCellsEnv)
	unsetEnvForTest(t, realModelTransportEnv)
	baseURL := "https://api.deepseek.com"
	if live {
		t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
	} else {
		denyProviderJourneyExternalNetwork(t)
		server := llmapi.New(t, llmapi.OpenAICompatible, "/v1/chat/completions", "synthetic-evidence-key", func(request llmapi.Request) (llmapi.Reply, error) {
			return providerEvidenceReply(request, cell)
		})
		baseURL = server.URL
		credentials, err := runtimecredentials.NewFileStore(h.credentialPath)
		if err != nil {
			t.Fatal("prepare synthetic credential store")
		}
		if err := credentials.Set(t.Context(), "OPENAI_COMPATIBLE_API_KEY", "synthetic-evidence-key"); err != nil {
			t.Fatal("prepare synthetic credential")
		}
	}
	configureProviderJourneyLLM(t, h.opts.ConfigPath, "openai_compatible", baseURL, "deepseek-flash")
	observation := &providerEvidenceObservation{base: http.DefaultTransport, live: live}
	http.DefaultTransport = observation
	t.Cleanup(func() { http.DefaultTransport = observation.base })
	t.Cleanup(func() { observation.report(t) })
	h.start(t)
	defer h.stop(t)
	turns := 1
	if cell == "memory" || cell == "empty_content" {
		turns = 2
	}
	var runID, sessionID string
	for sequence := 1; sequence <= turns; sequence++ {
		input := map[string]any{"bundle_hash": servedEventPublishFixtureBundleHash(t, h.opts.SourceRoot), "event_name": "evidence.requested",
			"payload": map[string]any{"conversation": "synthetic", "sequence": sequence}, "idempotency_key": fmt.Sprintf("evidence-%d", sequence)}
		if runID != "" {
			input["run_id"] = runID
		}
		published := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), input)
		if runID != "" && published.RunID != runID {
			t.Fatal("evidence conversation changed its run")
		}
		runID = published.RunID
		session, refusal := waitProviderEvidenceTurn(t, h, runID, published.EventID, observation, cell == "refusal")
		if sessionID != "" && sessionID != session {
			t.Fatal("evidence conversation changed its session")
		}
		sessionID = session
		if refusal {
			return
		}
		requireProviderEvidenceOutput(t, h, runID, published.EventID, sequence)
	}
	if cell == "refusal" {
		t.Log("boundary=request_refusal outcome=NOT_EXERCISED (provider accepted the journey)")
	}
	observation.requireShape(t, cell)
}

func writeProviderEvidenceSource(t *testing.T, cell string) string {
	t.Helper()
	root := t.TempDir()
	write := func(path string, value any) {
		body, err := yaml.Marshal(value)
		if err != nil {
			t.Fatal("encode synthetic evidence source")
		}
		writeWorkflowValidationFixtureFile(t, filepath.Join(root, path), string(body))
	}
	manifest := map[string]any{"version": "1.0.0", "platform_version": "*"}
	write("manifest.yaml", manifest)
	write("events.yaml", map[string]any{"evidence.requested": map[string]any{"conversation": "string", "sequence": "integer"}})
	write("schema.yaml", map[string]any{"pins": map[string]any{"inputs": []string{"evidence.requested", "evidence.completed"}}, "connect": []map[string]any{
		{"event": "evidence.requested", "from": ".", "to": "worker", "resolution": "select-or-create"}, {"event": "evidence.completed", "from": "worker", "to": "."},
	}})
	write("nodes.yaml", map[string]any{"observer": map[string]any{"execution_type": "system_node", "event_handlers": map[string]any{
		"evidence.completed": map[string]any{"guard": map[string]any{"id": "observe", "check": true}},
	}}})
	write("worker/manifest.yaml", manifest)
	write("worker/schema.yaml", map[string]any{"instance": "conversation", "pins": map[string]any{"inputs": []string{"evidence.requested"}, "outputs": []string{"evidence.completed"}}})
	write("worker/state.yaml", map[string]any{"conversation": "text"})
	write("worker/events.yaml", map[string]any{"evidence.completed": map[string]any{"sequence": "integer"}})
	agent := map[string]any{"role": "evidence", "model": "regular", "memory": true, "subscriptions": []string{"evidence.requested"}, "emit_events": []string{"evidence.completed"},
		"intent": map[string]any{"inline": "For each event call emit_evidence_completed exactly once, copying sequence from the current event. Use only that tool and produce no prose."}}
	if cell == "tool_round" || cell == "refusal" {
		agent["flow_data_access"] = []string{"evidence.txt"}
		agent["intent"] = map[string]any{"inline": "First read evidence.txt using read_flow_data, then call emit_evidence_completed once with sequence from the current event. You must read the file before emitting. Produce no prose."}
		writeWorkflowValidationFixtureFile(t, filepath.Join(root, "worker/data/evidence.txt"), "synthetic evidence only\n")
	}
	write("worker/agents.yaml", map[string]any{"witness": agent})
	return root
}

func providerEvidenceReply(request llmapi.Request, cell string) (llmapi.Reply, error) {
	const reasoning = "synthetic evidence reasoning"
	if cell == "refusal" {
		return llmapi.Reply{StatusCode: http.StatusBadRequest, ErrorBody: `{"error":{"message":"synthetic request shape refused"}}`}, nil
	}
	var body struct {
		Messages []struct {
			Role      string `json:"role"`
			Content   string `json:"content"`
			CallID    string `json:"tool_call_id"`
			Reasoning string `json:"reasoning_content"`
			Calls     []struct {
				ID string `json:"id"`
			} `json:"tool_calls"`
		} `json:"messages"`
		Tools []struct {
			Function struct {
				Name       string `json:"name"`
				Parameters struct {
					Properties map[string]struct {
						Enum []string `json:"enum"`
					} `json:"properties"`
				} `json:"parameters"`
			} `json:"function"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(request.Body, &body); err != nil {
		return llmapi.Reply{}, fmt.Errorf("invalid synthetic request")
	}
	var wire struct {
		Messages []map[string]json.RawMessage `json:"messages"`
	}
	_ = json.Unmarshal(request.Body, &wire)
	for _, message := range wire.Messages {
		if string(message["role"]) == `"assistant"` && len(message["content"]) == 0 && len(message["tool_calls"]) == 0 {
			return llmapi.Reply{StatusCode: http.StatusBadRequest, ErrorBody: `{"error":{"message":"synthetic strict provider: content or tool_calls required"}}`}, nil
		}
	}
	sequence := 0
	linked := map[string]bool{}
	for _, message := range body.Messages {
		if message.Role == "assistant" {
			linked = map[string]bool{}
			for _, call := range message.Calls {
				linked[call.ID] = true
				if call.ID == "synthetic-read" && message.Reasoning != reasoning {
					return llmapi.Reply{StatusCode: http.StatusBadRequest, ErrorBody: `{"error":{"message":"synthetic strict provider: thinking tool round requires reasoning_content"}}`}, nil
				}
			}
		}
		if message.Role == "tool" && !linked[message.CallID] {
			return llmapi.Reply{StatusCode: http.StatusBadRequest, ErrorBody: `{"error":{"message":"synthetic strict provider: orphaned tool message"}}`}, nil
		}
		var payload struct {
			Sequence int `json:"sequence"`
		}
		if message.Role == "user" && json.Unmarshal([]byte(message.Content), &payload) == nil && payload.Sequence > 0 {
			sequence = payload.Sequence
		}
	}
	if cell == "tool_round" && request.Number == 1 {
		for _, tool := range body.Tools {
			if ids := tool.Function.Parameters.Properties["static_id"].Enum; tool.Function.Name == "read_flow_data" && len(ids) == 1 {
				args, _ := json.Marshal(map[string]any{"kind": "static_file", "static_id": ids[0]})
				return llmapi.Reply{ID: "synthetic-read", Model: "deepseek-flash", Reasoning: reasoning, InputTokens: 1, OutputTokens: 1, Tools: []llmapi.ToolCall{{ID: "synthetic-read", Name: "read_flow_data", Arguments: args}}}, nil
			}
		}
		return llmapi.Reply{}, fmt.Errorf("read-only evidence tool absent")
	}
	if sequence == 0 {
		return llmapi.Reply{}, fmt.Errorf("synthetic initial frame lost its sequence")
	}
	found := false
	for _, tool := range body.Tools {
		found = found || tool.Function.Name == "emit_evidence_completed"
	}
	if !found {
		return llmapi.Reply{}, fmt.Errorf("authored evidence emit tool absent")
	}
	args, _ := json.Marshal(map[string]any{"sequence": sequence})
	return llmapi.Reply{ID: fmt.Sprintf("synthetic-%d", request.Number), Model: "deepseek-flash", InputTokens: 1, OutputTokens: 1,
		Tools: []llmapi.ToolCall{{ID: fmt.Sprintf("emit-%d", request.Number), Name: "emit_evidence_completed", Arguments: args}}}, nil
}

func waitProviderEvidenceTurn(t *testing.T, h *channelOnboardingE2EHarness, runID, inputID string, observation *providerEvidenceObservation, refusal bool) (string, bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		observation.mu.Lock()
		inconclusive, body := observation.inconclusive, observation.refusal
		observation.mu.Unlock()
		if inconclusive {
			t.Skip("INCONCLUSIVE: provider unavailable or rate limited; no paid retry")
		}
		var sessions operatorread.OperatorConversationListResult
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "conversation.list", map[string]any{"run_id": runID}, &sessions)
		if len(sessions.Conversations) == 1 {
			session := sessions.Conversations[0].SessionID
			var turns operatorread.OperatorConversationTurnListResult
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "conversation.list_turns", map[string]any{"session_id": session}, &turns)
			matched := 0
			for _, turn := range turns.Turns {
				if turn.TriggerEventID != inputID {
					continue
				}
				matched++
				if turn.Failure != nil {
					if refusal && body != "" && turn.Failure.Class == runtimefailures.ClassConnectorFailure && !turn.Failure.Retryable && turn.Failure.Detail.Attributes["response_body"] == body {
						t.Log("boundary=request_refusal outcome=EXERCISED permanent=true body_preserved=true")
						return session, true
					}
					t.Fatalf("provider journey failed: class=%s code=%s (provider body withheld)", turn.Failure.Class, turn.Failure.Detail.Code)
				}
				if !turn.ParseOK {
					t.Fatal("provider journey did not parse")
				}
			}
			var input operatorread.OperatorEventFull
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "event.get", map[string]any{"event_id": inputID}, &input)
			if matched > 0 && len(input.Deliveries) == 1 && input.Deliveries[0].Status == "delivered" {
				return session, false
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("provider journey did not settle before its execution deadline")
	return "", false
}

func requireProviderEvidenceOutput(t *testing.T, h *channelOnboardingE2EHarness, runID, inputID string, sequence int) {
	t.Helper()
	var input operatorread.OperatorEventFull
	requireServedJSONRPCResult(t, h.rpcEndpoint(), "event.get", map[string]any{"event_id": inputID}, &input)
	if input.EventID != inputID || input.RunID != runID || input.EventName != "evidence.requested" || len(input.Deliveries) != 1 || input.Deliveries[0].Target.FlowID != "worker" || input.Deliveries[0].Target.FlowInstance == "" {
		t.Fatal("evidence input lost its exact constructed receiver")
	}
	eventName := eventidentity.ExternalizeForFlow(input.Deliveries[0].Target.FlowInstance, []string{"evidence.completed"}, "evidence.completed")
	deadline := time.Now().Add(15 * time.Second)
	var last operatorread.OperatorEventListResult
	observer := identitytest.RootNode(t, "observer").Key()
	for time.Now().Before(deadline) {
		var page operatorread.OperatorEventListResult
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "event.list", map[string]any{"filter": map[string]any{"run_id": runID, "event_name": eventName}, "limit": 100}, &page)
		last = page
		matches := 0
		settled := true
		for _, event := range page.Events {
			if event.Payload["sequence"] == float64(sequence) {
				matches++
				if len(event.Deliveries) != 1 || event.Deliveries[0].SubscriberID != observer || event.Deliveries[0].Status != "delivered" || len(event.DeadLetters) != 0 {
					settled = false
				}
			}
		}
		if matches > 1 {
			t.Fatal("evidence output was emitted more than once")
		}
		if matches == 1 && settled {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	for _, event := range last.Events {
		statuses := []string{}
		for _, delivery := range event.Deliveries {
			statuses = append(statuses, delivery.Status)
		}
		t.Logf("evidence output observation: event=%s sequence_type=%T deliveries=%v dead_letters=%d", event.EventName, event.Payload["sequence"], statuses, len(event.DeadLetters))
	}
	t.Fatal("authored evidence output did not settle at its observer")
}

type providerEvidenceObservation struct {
	mu                        sync.Mutex
	base                      http.RoundTripper
	live                      bool
	requests                  int
	toolMessages, orphanTools int
	input, output             int64
	usageResponses            int
	empty, tools, toolAck     bool
	emptyPending              bool
	emptyAssistantIndex       int
	assistantMessages         int
	emptyReplayChecked        bool
	emptyReplayLost           bool
	reasoning                 bool
	readReasoning             bool
	reasoningAck              bool
	readCalls                 map[string][32]byte
	cost                      map[string]float64
	refusal                   string
	inconclusive              bool
}

func (o *providerEvidenceObservation) RoundTrip(request *http.Request) (*http.Response, error) {
	host := request.URL.Hostname()
	ip := net.ParseIP(host)
	local := host == "localhost" || (ip != nil && ip.IsLoopback())
	provider := request.URL.Path == "/v1/chat/completions"
	if !local && !(o.live && request.URL.Scheme == "https" && host == "api.deepseek.com" && provider) {
		return nil, fmt.Errorf("evidence transport refused nonselected destination")
	}
	if !provider {
		return o.base.RoundTrip(request)
	}
	o.mu.Lock()
	if o.inconclusive || o.requests >= 6 {
		o.mu.Unlock()
		return nil, fmt.Errorf("evidence execution finished; no redispatch")
	}
	o.requests++
	o.mu.Unlock()
	if request.GetBody != nil {
		body, err := request.GetBody()
		if err != nil {
			return nil, fmt.Errorf("evidence request shape unavailable")
		}
		o.observeRequest(body)
		_ = body.Close()
	}
	response, err := o.base.RoundTrip(request)
	if err != nil {
		o.mu.Lock()
		o.inconclusive = true
		o.mu.Unlock()
		return nil, err
	}
	response.Body = &providerEvidenceBody{ReadCloser: response.Body, observation: o, status: response.StatusCode}
	return response, nil
}

// Observe the adapter's own read without moving read failures into send_request.
type providerEvidenceBody struct {
	io.ReadCloser
	observation *providerEvidenceObservation
	status      int
	raw         bytes.Buffer
	finished    bool
}

func (b *providerEvidenceBody) Read(buffer []byte) (int, error) {
	n, err := b.ReadCloser.Read(buffer)
	_, _ = b.raw.Write(buffer[:n])
	if err != nil && !b.finished {
		b.finished = true
		if err == io.EOF {
			b.observation.observe(b.status, b.raw.Bytes())
		} else {
			b.observation.mu.Lock()
			b.observation.inconclusive = true
			b.observation.mu.Unlock()
		}
	}
	return n, err
}

func (o *providerEvidenceObservation) observeRequest(body io.Reader) {
	var request struct {
		Messages []struct {
			Role      string  `json:"role"`
			CallID    string  `json:"tool_call_id"`
			Reasoning string  `json:"reasoning_content"`
			Content   *string `json:"content"`
			Calls     []struct {
				ID string `json:"id"`
			} `json:"tool_calls"`
		} `json:"messages"`
	}
	if json.NewDecoder(body).Decode(&request) != nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	linked := map[string]bool{}
	reasoning := map[string][32]byte{}
	assistantCount, emptyPreserved := 0, false
	for _, message := range request.Messages {
		if message.Role == "assistant" {
			if o.emptyPending && assistantCount == o.emptyAssistantIndex {
				emptyPreserved = message.Content != nil && *message.Content == ""
			}
			assistantCount++
			linked = map[string]bool{}
			for _, call := range message.Calls {
				linked[call.ID] = true
				reasoning[call.ID] = sha256.Sum256([]byte(message.Reasoning))
			}
		}
		if message.Role == "tool" {
			o.toolMessages++
			if !linked[message.CallID] {
				o.orphanTools++
			}
			if expected, found := o.readCalls[message.CallID]; found && linked[message.CallID] {
				o.toolAck = true
				o.reasoningAck = o.reasoningAck || (o.readReasoning && expected == reasoning[message.CallID])
			}
		}
	}
	if o.emptyPending {
		o.emptyReplayChecked = true
		o.emptyReplayLost = o.emptyReplayLost || !emptyPreserved
		o.emptyPending = false
	}
	o.assistantMessages = assistantCount
}

func (o *providerEvidenceObservation) observe(status int, body []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if status == 408 || status == 429 || status >= 500 {
		o.inconclusive = true
	}
	if status >= 400 && status < 500 {
		o.refusal = string(body)
	}
	var parsed struct {
		Choices []struct {
			Message map[string]json.RawMessage `json:"message"`
		} `json:"choices"`
		Usage map[string]json.RawMessage `json:"usage"`
	}
	if json.Unmarshal(body, &parsed) != nil {
		return
	}
	var input, output *int64
	inputErr := json.Unmarshal(parsed.Usage["prompt_tokens"], &input)
	outputErr := json.Unmarshal(parsed.Usage["completion_tokens"], &output)
	if inputErr == nil && outputErr == nil && input != nil && output != nil && *input >= 0 && *output >= 0 {
		o.input += *input
		o.output += *output
		o.usageResponses++
	}
	for _, field := range []string{"cost", "total_cost", "cost_usd"} {
		var cost float64
		if raw := parsed.Usage[field]; len(raw) != 0 && string(raw) != "null" && json.Unmarshal(raw, &cost) == nil && cost >= 0 {
			if o.cost == nil {
				o.cost = map[string]float64{}
			}
			o.cost[field] += cost
		}
	}
	for index, choice := range parsed.Choices {
		empty := string(choice.Message["content"]) == `""`
		o.empty = o.empty || empty
		var calls []struct {
			ID       string `json:"id"`
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		}
		var reasoning string
		_ = json.Unmarshal(choice.Message["reasoning_content"], &reasoning)
		_ = json.Unmarshal(choice.Message["tool_calls"], &calls)
		// The adapter replays the first choice in the next assistant slot.
		if index == 0 && empty {
			o.emptyPending = true
			o.emptyAssistantIndex = o.assistantMessages
		}
		o.reasoning = o.reasoning || reasoning != ""
		for _, call := range calls {
			o.tools = o.tools || (call.ID != "" && call.Function.Name != "")
			if call.Function.Name == "read_flow_data" {
				o.readReasoning = o.readReasoning || reasoning != ""
				if o.readCalls == nil {
					o.readCalls = map[string][32]byte{}
				}
				o.readCalls[call.ID] = sha256.Sum256([]byte(reasoning))
			}
		}
	}
}

func (o *providerEvidenceObservation) emptyReplayOutcome() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.emptyReplayLost {
		return "FAIL"
	}
	if o.emptyReplayChecked {
		return "EXERCISED"
	}
	return "NOT_EXERCISED"
}

func (o *providerEvidenceObservation) requireShape(t *testing.T, cell string) {
	t.Helper()
	if cell == "empty_content" {
		outcome := o.emptyReplayOutcome()
		if outcome == "FAIL" {
			t.Fatal("observed empty-content response lost its message or content on the next replay")
		}
		t.Log("boundary=empty_content outcome=" + outcome)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if cell == "tool_round" && !o.readReasoning {
		t.Log("boundary=reasoning_content outcome=NOT_EXERCISED")
	}
	if cell == "tool_round" && len(o.readCalls) == 0 {
		t.Log("boundary=tool_round outcome=NOT_EXERCISED")
	}
	if cell == "tool_round" && len(o.readCalls) > 0 && (!o.toolAck || (o.readReasoning && !o.reasoningAck)) {
		t.Fatal("read-only tool continuation lost its exact call identity or observed reasoning")
	}
	if !o.tools {
		t.Fatal("provider journey did not exercise a tool call")
	}
}

func (o *providerEvidenceObservation) report(t *testing.T) {
	t.Log(o.summary())
}

func (o *providerEvidenceObservation) summary() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	cost := "NOT_REPORTED"
	if len(o.cost) > 0 {
		encoded, _ := json.Marshal(o.cost)
		cost = string(encoded)
	}
	usage := "NOT_REPORTED"
	if o.usageResponses > 0 {
		usage = fmt.Sprintf("responses=%d input_tokens=%d output_tokens=%d", o.usageResponses, o.input, o.output)
	}
	return fmt.Sprintf("provider_evidence live=%t requests=%d usage=%s provider_usage_cost=%s tool_messages=%d orphan_tool_messages=%d", o.live, o.requests, usage, cost, o.toolMessages, o.orphanTools)
}
