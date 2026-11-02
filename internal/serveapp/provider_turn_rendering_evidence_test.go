package serveapp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	"github.com/division-sh/swarm/internal/operatorread"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	runtimellm "github.com/division-sh/swarm/internal/runtime/llm"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/division-sh/swarm/internal/testutil/llmapi"
)

var turnRenderingCells = []string{"payload", "string", "object", "results", "read", "long", "environment", "emit_refusal", "producer", "leak_reply", "leak_error", "leak_ok"}

var renderingEvidenceKeyPattern = regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{16,}`)

func TestProviderTurnRenderingJourneyBothStores(t *testing.T) {
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			for _, cell := range turnRenderingCells {
				if strings.HasPrefix(cell, "leak_") {
					continue
				}
				t.Run(cell, func(t *testing.T) { runTurnRenderingJourney(t, backend, cell, false) })
			}
		})
	}
}

func TestProviderTurnRenderingDeepSeekEvidence(t *testing.T) {
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
	}, turnRenderingCells...)
	if reason != "" {
		t.Skip("NOT RUN: " + reason)
	}
	store, err := cliapp.BuildProviderCredentialStore()
	if err != nil {
		t.Skip("NOT RUN: credential store unavailable (details withheld)")
	}
	profile, _ := llmselection.ResolveActiveBackend(llmselection.BackendOpenAICompatible)
	credential, err := runtimellm.NewProviderCredentialResolver(store).Inspect(t.Context(), profile)
	if err != nil || credential.Value == "" {
		t.Skip("NOT RUN: DeepSeek credential unavailable; no mock fallback")
	}
	for _, backend := range servedparity.RequiredBackends {
		t.Run(string(backend), func(t *testing.T) {
			for _, cell := range turnRenderingCells {
				if selected[cell] {
					t.Run(cell, func(t *testing.T) { runTurnRenderingJourney(t, backend, cell, true) })
				}
			}
		})
	}
}

func runTurnRenderingJourney(t *testing.T, backend servedparity.Backend, cell string, live bool) {
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
	h.opts.SourceRoot = writeTurnRenderingSource(t, cell)
	h.opts.TestLLMRuntime = nil
	h.opts.Verbose = false
	baseURL := "https://api.deepseek.com"
	if live {
		t.Setenv("SWARM_CREDENTIALS_FILE", credentialPath)
		h.opts.ErrorOutput = io.Discard
	} else {
		denyProviderJourneyExternalNetwork(t)
		provider := llmapi.New(t, llmapi.OpenAICompatible, "/v1/chat/completions", "synthetic-rendering-key", func(request llmapi.Request) (llmapi.Reply, error) {
			return turnRenderingReply(request, cell)
		})
		baseURL = provider.URL
		credentials, err := runtimecredentials.NewFileStore(h.credentialPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := credentials.Set(t.Context(), "OPENAI_COMPATIBLE_API_KEY", "synthetic-rendering-key"); err != nil {
			t.Fatal(err)
		}
	}
	configureProviderJourneyLLM(t, h.opts.ConfigPath, "openai_compatible", baseURL, "deepseek-flash")
	observation := &providerEvidenceObservation{base: http.DefaultTransport, live: live}
	recorder := &turnRenderingRecorder{base: observation, sourceSHA: os.Getenv(realModelSHAEnv), evidenceDir: os.Getenv("SWARM_RENDERING_EVIDENCE_DIR")}
	for _, key := range []string{realModelSHAEnv, realModelCellsEnv, realModelTransportEnv, "SWARM_RENDERING_EVIDENCE_DIR"} {
		unsetEnvForTest(t, key)
	}
	http.DefaultTransport = recorder
	t.Cleanup(func() { http.DefaultTransport = observation.base })
	t.Cleanup(func() {
		observation.report(t)
		if strings.HasPrefix(cell, "leak_") {
			recorder.reportMarkup(t)
		}
		if live {
			recorder.save(t, backend, cell)
		}
	})
	h.start(t)
	defer h.stop(t)
	var runID, sessionID string
	for sequence := 1; sequence <= 2; sequence++ {
		params := map[string]any{"bundle_hash": servedEventPublishFixtureBundleHash(t, h.opts.SourceRoot), "event_name": "evidence.started",
			"payload": map[string]any{"conversation": "synthetic", "sequence": sequence, "text": fmt.Sprintf("synthetic turn %d", sequence)}, "idempotency_key": fmt.Sprintf("rendering-%d", sequence)}
		if runID != "" {
			params["run_id"] = runID
		}
		published := requireServedEventPublishRPCResult(t, h.rpcEndpoint(), params)
		if runID != "" && published.RunID != runID {
			t.Fatal("rendering conversation changed run")
		}
		runID = published.RunID
		inputID := waitRenderingInput(t, h, runID, published.EventID)
		var input operatorread.OperatorEventFull
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "event.get", map[string]any{"event_id": inputID}, &input)
		recorder.mu.Lock()
		if recorder.eventTimes == nil {
			recorder.eventTimes = map[string]string{}
		}
		recorder.eventTimes[fmt.Sprintf("synthetic turn %d", sequence)] = input.CreatedAt.UTC().Truncate(time.Second).Format(time.RFC3339)
		recorder.mu.Unlock()
		current := waitTurnRenderingCompletion(t, h, runID, inputID, observation)
		if sessionID != "" && current != sessionID {
			t.Fatal("rendering memory rotated")
		}
		sessionID = current
		requireProviderEvidenceOutput(t, h, runID, inputID, sequence)
		if cell == "producer" {
			var output operatorread.OperatorEventListResult
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "event.list", map[string]any{"filter": map[string]any{"run_id": runID}, "limit": 100}, &output)
			matches := 0
			for _, event := range output.Events {
				if event.SourceEventID == inputID && event.Payload["sequence"] == float64(sequence) {
					matches++
					if event.Payload["speaker"] != "speaker" {
						t.Fatalf("provider did not return the projected producing node: %v", event.Payload["speaker"])
					}
				}
			}
			if matches != 1 {
				t.Fatalf("projected-speaker outputs = %d, want 1", matches)
			}
		}
		var turns operatorread.OperatorConversationTurnListResult
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "conversation.list_turns", map[string]any{"session_id": sessionID}, &turns)
		for _, turn := range turns.Turns {
			if turn.TriggerEventID != inputID {
				continue
			}
			var detail operatorread.OperatorPublicConversationTurnDetail
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "conversation.get_turn", map[string]any{"session_id": sessionID, "turn_id": turn.TurnID}, &detail)
			if detail.Frame.FrameID == "" || detail.Frame.ContentHash == "" || detail.ProviderRendering == nil || detail.ProviderRendering.Status != "recorded" || !recorder.containsRendering(*detail.ProviderRendering) {
				t.Fatal("public turn inspection lost frame identity or exact recorded provider rendering")
			}
		}
	}
	recorder.requireRendering(t, cell)
}

func waitTurnRenderingCompletion(t *testing.T, h *channelOnboardingE2EHarness, runID, inputID string, observation *providerEvidenceObservation) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	var input operatorread.OperatorEventFull
	for time.Now().Before(deadline) {
		observation.mu.Lock()
		inconclusive := observation.inconclusive
		observation.mu.Unlock()
		if inconclusive {
			t.Skip("INCONCLUSIVE: provider unavailable or rate limited; no paid retry")
		}
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "event.get", map[string]any{"event_id": inputID}, &input)
		for _, delivery := range input.Deliveries {
			if delivery.Failure != nil {
				raw, _ := json.Marshal(delivery.Failure)
				t.Fatalf("rendering delivery refused before completion: %s", raw)
			}
		}
		var sessions operatorread.OperatorConversationListResult
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "conversation.list", map[string]any{"run_id": runID}, &sessions)
		if len(sessions.Conversations) == 1 {
			session := sessions.Conversations[0].SessionID
			var turns operatorread.OperatorConversationTurnListResult
			requireServedJSONRPCResult(t, h.rpcEndpoint(), "conversation.list_turns", map[string]any{"session_id": session}, &turns)
			matched := false
			for _, turn := range turns.Turns {
				if turn.TriggerEventID == inputID {
					if turn.Failure != nil || !turn.ParseOK {
						raw, _ := json.Marshal(turn.Failure)
						t.Fatalf("rendering provider turn refused: %s", raw)
					}
					matched = true
				}
			}
			if matched && len(input.Deliveries) == 1 && input.Deliveries[0].Status == "delivered" {
				return session
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	raw, _ := json.Marshal(input.Deliveries)
	t.Fatalf("rendering input did not settle; exact delivery readback: %s", raw)
	return ""
}

func writeTurnRenderingSource(t *testing.T, cell string) string {
	t.Helper()
	root := writeProviderEvidenceSource(t, "memory")
	files := map[string]string{
		"events.yaml":       "evidence.started: {conversation: string, sequence: integer, text: string}\nevidence.requested: {conversation: string, sequence: integer, text: string}\n",
		"schema.yaml":       "pins:\n  inputs: [evidence.started, evidence.requested, evidence.completed]\nconnect:\n  - {event: evidence.requested, from: '.', to: worker, resolution: select-or-create}\n  - {event: evidence.completed, from: worker, to: '.'}\n",
		"nodes.yaml":        "speaker:\n  execution_type: system_node\n  event_handlers:\n    evidence.started:\n      emit:\n        event: evidence.requested\n        fields: {conversation: payload.conversation, sequence: payload.sequence, text: payload.text}\nobserver:\n  execution_type: system_node\n  event_handlers:\n    evidence.completed:\n      guard: {id: observe, check: true}\n",
		"worker/state.yaml": "conversation: text\nnote: 'synthetic declared note'\n",
	}
	intent := "For each user input call emit_evidence_completed once with sequence 1 for synthetic turn 1 and sequence 2 for synthetic turn 2. Produce no prose."
	extra := ""
	switch cell {
	case "string":
		extra = "  content: payload.text\n"
	case "object":
		extra = "  content: |-\n    {\"text\": payload.text, \"at\": event.at}\n"
	case "producer":
		extra = "  content: |-\n    {\"text\": payload.text, \"speaker\": event.producer}\n"
		intent = "For each input call emit_evidence_completed once with sequence 1 for synthetic turn 1 or sequence 2 for synthetic turn 2, and copy the input's speaker exactly into the speaker argument. Produce no prose."
		files["worker/events.yaml"] = "evidence.completed: {sequence: integer, speaker: string}\n"
	case "results", "leak_error":
		extra = "  flow_data_access: [evidence.txt]\n"
		intent = "For synthetic turn 1, first try read_flow_data with kind static_file and static_id missing, then read the allowed evidence.txt using its exact static_id from the tool schema, then emit_evidence_completed with sequence 1. For synthetic turn 2 emit sequence 2. Produce no prose."
	case "read", "long":
		intent = "Read the declared state with read_state before each emit_evidence_completed. Its explicitly declared fields are conversation and note; use no row metadata. Copy the current sequence into the emission. Produce no prose."
		if cell == "long" {
			files["worker/state.yaml"] = "conversation: text\nnote: " + fmt.Sprintf("%q", "HEAD"+strings.Repeat("synthetic declared output ", 900)+"TAIL") + "\n"
		}
	case "environment":
		extra = "  native_tools: {file_io: true}\n"
		intent = "Read the runtime environment facts without executing a tool. For each input use emit_evidence_completed with its current sequence. Produce no prose."
	case "emit_refusal":
		intent = "This is a synthetic refusal exercise. On synthetic turn 1 first call emit_evidence_completed with an empty argument object, observe its required-field refusal, then correct the call with sequence 1. On synthetic turn 2 emit sequence 2. Produce no prose."
	case "leak_reply":
		intent = "Write a brief friendly acknowledgment of the current synthetic input, then call emit_evidence_completed with the current sequence. Do not imitate tool markup."
	case "leak_ok":
		intent = "For each synthetic input briefly acknowledge the prior accepted tool result if present, then call emit_evidence_completed with the current sequence. Do not imitate tool markup."
	}
	files["worker/agents.yaml"] = "witness:\n  role: evidence\n  model: regular\n  memory: true\n  subscriptions: [evidence.requested]\n  emit_events: [evidence.completed]\n" + extra + "  intent:\n    inline: " + intent + "\n"
	if cell == "results" || cell == "leak_error" {
		files["worker/data/evidence.txt"] = "synthetic business output\n"
	}
	for path, body := range files {
		writeWorkflowValidationFixtureFile(t, filepath.Join(root, path), body)
	}
	return root
}

func waitRenderingInput(t *testing.T, h *channelOnboardingE2EHarness, runID, parentID string) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		var page operatorread.OperatorEventListResult
		requireServedJSONRPCResult(t, h.rpcEndpoint(), "event.list", map[string]any{"filter": map[string]any{"run_id": runID, "event_name": "evidence.requested"}, "limit": 100}, &page)
		for _, event := range page.Events {
			if event.SourceEventID == parentID {
				return event.EventID
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("authored speaker did not publish the exact input")
	return ""
}

type turnRenderingWire struct {
	Request      json.RawMessage `json:"request"`
	Status       int             `json:"status"`
	Response     json.RawMessage `json:"response"`
	ResponseText string          `json:"response_text,omitempty"`
}

// Capture bodies only, after ordinary opt-in admission. Headers (including
// authorization) and transport addresses never enter an evidence receipt.
type turnRenderingRecorder struct {
	base        *providerEvidenceObservation
	mu          sync.Mutex
	pairs       []turnRenderingWire
	eventTimes  map[string]string
	sourceSHA   string
	evidenceDir string
}

func (r *turnRenderingRecorder) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Path != "/v1/chat/completions" {
		return r.base.RoundTrip(request)
	}
	var raw []byte
	if request.GetBody != nil {
		body, err := request.GetBody()
		if err != nil {
			return nil, err
		}
		raw, err = io.ReadAll(body)
		_ = body.Close()
		if err != nil {
			return nil, err
		}
	}
	response, err := r.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	index := len(r.pairs)
	r.pairs = append(r.pairs, turnRenderingWire{Request: raw, Status: response.StatusCode})
	r.mu.Unlock()
	response.Body = &turnRenderingBody{ReadCloser: response.Body, record: func(raw []byte) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if json.Valid(raw) {
			r.pairs[index].Response = append([]byte(nil), raw...)
		} else {
			r.pairs[index].ResponseText = string(raw)
		}
	}}
	return response, nil
}

type turnRenderingBody struct {
	io.ReadCloser
	raw      bytes.Buffer
	record   func([]byte)
	finished bool
}

func (b *turnRenderingBody) Read(out []byte) (int, error) {
	n, err := b.ReadCloser.Read(out)
	_, _ = b.raw.Write(out[:n])
	if err != nil && !b.finished {
		b.finished = true
		b.record(b.raw.Bytes())
	}
	return n, err
}

func turnRenderingReply(request llmapi.Request, cell string) (llmapi.Reply, error) {
	var body struct {
		Messages []struct {
			Role, Content string
			ToolCalls     []struct {
				ID string `json:"id"`
			} `json:"tool_calls"`
			CallID string `json:"tool_call_id"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(request.Body, &body); err != nil {
		return llmapi.Reply{}, err
	}
	sequence := 0
	speaker := ""
	results := 0
	for _, message := range body.Messages {
		if message.Role == "user" && strings.Contains(message.Content, "synthetic turn 1") {
			sequence = 1
			results = 0
		}
		if message.Role == "user" && strings.Contains(message.Content, "synthetic turn 2") {
			sequence = 2
			results = 0
		}
		if message.Role == "tool" {
			results++
		}
		if cell == "producer" && message.Role == "user" {
			var projected struct{ Speaker string }
			if json.Unmarshal([]byte(message.Content), &projected) == nil {
				speaker = projected.Speaker
			}
		}
	}
	if sequence == 0 {
		return llmapi.Reply{}, fmt.Errorf("synthetic input lost its projected content")
	}
	name := "emit_evidence_completed"
	args := map[string]any{"sequence": sequence}
	if cell == "producer" {
		args["speaker"] = speaker
	}
	if cell == "emit_refusal" && sequence == 1 && results == 0 {
		args = map[string]any{}
	}
	if (cell == "read" || cell == "long") && results == 0 {
		name = "read_state"
		args = map[string]any{}
	}
	if cell == "results" && sequence == 1 {
		if results == 0 {
			name = "read_flow_data"
			args = map[string]any{"kind": "static_file", "static_id": "missing"}
		}
		if results == 1 {
			name = "read_flow_data"
			var schema struct {
				Tools []struct {
					Function struct {
						Name       string
						Parameters struct {
							Properties map[string]struct{ Enum []string }
						}
					}
				}
			}
			if err := json.Unmarshal(request.Body, &schema); err != nil {
				return llmapi.Reply{}, err
			}
			for _, tool := range schema.Tools {
				if tool.Function.Name == name {
					ids := tool.Function.Parameters.Properties["static_id"].Enum
					if len(ids) == 1 {
						args = map[string]any{"kind": "static_file", "static_id": ids[0]}
					}
				}
			}
		}
	}
	raw, _ := json.Marshal(args)
	return llmapi.Reply{ID: fmt.Sprintf("rendering-response-%d", request.Number), Model: "deepseek-flash", InputTokens: 1, OutputTokens: 1, Tools: []llmapi.ToolCall{{ID: fmt.Sprintf("rendering-call-%d", request.Number), Name: name, Arguments: raw}}}, nil
}

func (r *turnRenderingRecorder) requireRendering(t *testing.T, cell string) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	seenError, seenRead, seenLong, seenOK := false, false, false, false
	for _, pair := range r.pairs {
		var request struct {
			User     json.RawMessage `json:"user"`
			Messages []struct {
				Role, Content string
				Name          json.RawMessage `json:"name"`
			} `json:"messages"`
		}
		if err := json.Unmarshal(pair.Request, &request); err != nil {
			t.Fatal("invalid recorded synthetic request")
		}
		if len(request.User) != 0 {
			t.Fatal("request-level user was sent")
		}
		environments := 0
		for _, message := range request.Messages {
			if message.Role == "system" && strings.Contains(message.Content, "[swarm:environment]") {
				t.Fatal("environment remains in authored system prompt")
			}
			if message.Role == "user" && strings.HasPrefix(message.Content, "[swarm:environment]") {
				environments++
				continue
			}
			if message.Role == "user" {
				if len(message.Name) != 0 {
					t.Fatal("user-frame name was sent")
				}
				if strings.Contains(message.Content, `"routing_source"`) || strings.Contains(message.Content, `"payload_bytes_base64"`) {
					t.Fatal("provider saw internal frame")
				}
				if cell == "string" && !strings.HasPrefix(message.Content, "synthetic turn ") {
					t.Fatal("projected string not verbatim")
				}
				if cell == "object" {
					var content map[string]any
					if json.Unmarshal([]byte(message.Content), &content) != nil || len(content) != 2 || content["at"] != r.eventTimes[fmt.Sprint(content["text"])] {
						t.Fatal("object/time projection lost exact authored fields")
					}
				}
				if cell == "producer" {
					var content map[string]any
					if json.Unmarshal([]byte(message.Content), &content) != nil || len(content) != 2 || content["speaker"] != "speaker" {
						t.Fatal("content projection lost the exact producing-node name")
					}
				}
			}
			if message.Role == "tool" {
				seenOK = seenOK || message.Content == "ok"
				seenError = seenError || strings.HasPrefix(message.Content, "error: ")
				seenLong = seenLong || (strings.Contains(message.Content, "HEAD") && strings.Contains(message.Content, "TAIL") && strings.Contains(message.Content, "[... tool output truncated ...]"))
				seenRead = seenRead || message.Content == `{"conversation":"synthetic","note":"synthetic declared note"}`
				if strings.Contains(message.Content, `"provider_content"`) || strings.Contains(message.Content, `"entity_id"`) || strings.Contains(message.Content, `"current_state"`) {
					t.Fatal("tool result leaked wrapper or row metadata")
				}
			}
		}
		if (cell == "environment" && environments != 1) || (cell != "environment" && environments != 0) {
			t.Fatalf("runtime environment count=%d cell=%s", environments, cell)
		}
	}
	if !seenOK {
		t.Error("accepted emit result not replayed as ok")
	}
	if (cell == "results" || cell == "emit_refusal") && !seenError {
		t.Error("typed error result was not observed")
	}
	if cell == "long" && !seenLong {
		t.Error("long output did not retain head, tail and marker")
	}
	if cell == "read" && !seenRead {
		t.Error("declared-field-only read was not observed")
	}
}

func (r *turnRenderingRecorder) containsRendering(rendering operatorread.OperatorProviderRendering) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	var got []any
	if json.Unmarshal(rendering.Input, &got) != nil || len(got) == 0 {
		return false
	}
	var tools any
	if json.Unmarshal(rendering.Tools, &tools) != nil {
		return false
	}
	for _, pair := range r.pairs {
		var request struct {
			Messages []any `json:"messages"`
			Tools    any   `json:"tools"`
		}
		if json.Unmarshal(pair.Request, &request) != nil || len(request.Messages) < len(got) {
			continue
		}
		if reflect.DeepEqual(got, request.Messages[len(request.Messages)-len(got):]) && reflect.DeepEqual(tools, request.Tools) {
			return true
		}
	}
	return false
}

func (r *turnRenderingRecorder) reportMarkup(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pairs) == 0 {
		t.Log("synthetic_markup=NOT_EXERCISED requests=0; setup failed before provider")
		return
	}
	leaked := false
	for _, pair := range r.pairs {
		var response struct {
			Choices []struct {
				Message struct {
					Content string
					Calls   []json.RawMessage `json:"tool_calls"`
				} `json:"message"`
			} `json:"choices"`
		}
		if json.Unmarshal(pair.Response, &response) == nil {
			for _, choice := range response.Choices {
				leaked = leaked || (len(choice.Message.Calls) == 0 && (strings.Contains(choice.Message.Content, "tool_call") || strings.Contains(choice.Message.Content, "｜tool") || strings.Contains(choice.Message.Content, "<function")))
			}
		}
	}
	if leaked {
		t.Logf("synthetic_markup=REPRODUCED requests=%d (runtime policy unchanged)", len(r.pairs))
	} else {
		t.Logf("synthetic_markup=NOT_REPRODUCED requests=%d; one two-input synthetic conversation, all attempts retained", len(r.pairs))
	}
}

func (r *turnRenderingRecorder) save(t *testing.T, backend servedparity.Backend, cell string) {
	t.Helper()
	dir := r.evidenceDir
	if dir == "" {
		t.Error("live synthetic raw evidence directory not selected")
		return
	}
	r.mu.Lock()
	pairs := append([]turnRenderingWire(nil), r.pairs...)
	r.mu.Unlock()
	raw, err := json.MarshalIndent(struct {
		Source  string               `json:"source_sha"`
		Cell    string               `json:"cell"`
		Backend servedparity.Backend `json:"backend"`
		Pairs   []turnRenderingWire  `json:"pairs"`
	}{r.sourceSHA, cell, backend, pairs}, "", "  ")
	if err != nil {
		t.Error("synthetic raw evidence is incomplete")
		return
	}
	// These fixtures author no physical account, channel, phone, private path or
	// credential in any body. Refuse publication material if one appears.
	for _, forbidden := range []string{"/home/", "/Users/", "Authorization", "Bearer ", "telegram_bot_token", "channel_reference", "phone_number"} {
		if bytes.Contains(raw, []byte(forbidden)) {
			t.Error("synthetic evidence requires private review/scrubbing; not written")
			return
		}
	}
	if renderingEvidenceKeyPattern.Match(raw) {
		t.Error("synthetic evidence contains a possible credential; not written")
		return
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Error("cannot retain synthetic evidence")
		return
	}
	file, err := os.OpenFile(filepath.Join(dir, string(backend)+"-"+cell+".json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Error("cannot retain synthetic evidence")
		return
	}
	_, writeErr := file.Write(raw)
	if closeErr := file.Close(); writeErr != nil || closeErr != nil {
		t.Error("cannot complete synthetic evidence")
	}
}
