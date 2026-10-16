package llm

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/core/toolcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/effects/effecttest"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

func TestProviderRequestJournalRebuildsEachHTTPRenderer(t *testing.T) {
	for _, adapter := range []string{"anthropic_api", "openai_compatible", "openai_responses"} {
		for _, memory := range []bool{false, true} {
			for _, withTools := range []bool{false, true} {
				name := adapter + "/memory_" + map[bool]string{false: "off", true: "on"}[memory] + "/tools_" + map[bool]string{false: "off", true: "on"}[withTools]
				t.Run(name, func(t *testing.T) {
					var tools []ToolDefinition
					if withTools {
						tools = []ToolDefinition{{Name: "inspect", Description: "Inspect an admitted value.", Usage: "Preserve the exact argument.",
							Schema: map[string]any{"type": "object", "properties": map[string]any{"value": map[string]any{"type": "number", "minimum": json.Number("1.0"), "maximum": json.Number("1e2")}}}}}
					}
					frame, surface, session, input, request := providerRequestJournalFixture(t, adapter, tools...)
					session.Memory = agentmemory.Plan{Enabled: memory}
					body := renderProviderJournalFixture(t, adapter, session, input, frame.Session.Provider.Model)
					journal, err := newProviderRequestJournal(adapter, frame, surface, session, input, "exact-provider-turn", request, body)
					if err != nil {
						t.Fatal(err)
					}
					encoded, err := runtimeeffects.EncodeProviderRequestJournal(journal)
					if err != nil {
						t.Fatal(err)
					}
					if bytes.Contains(encoded, []byte("private-credential")) {
						t.Fatal("journal persisted HTTP credentials")
					}
					journal, err = runtimeeffects.DecodeProviderRequestJournal(encoded)
					if err != nil {
						t.Fatal(err)
					}
					before, _ := json.Marshal(journal)
					// The rebuild may not consult the live session or configuration.
					session.Messages[0].Content = "later conversation"
					session.SystemPrompt = "later prompt"
					rebuilt, err := RebuildProviderRequestJournal(journal, frame, surface, runtimeeffects.Fingerprint(body))
					if err != nil || !bytes.Equal(rebuilt, body) {
						t.Fatalf("frozen request rebuild differs: error=%v", err)
					}
					after, _ := json.Marshal(journal)
					if !bytes.Equal(before, after) {
						t.Fatal("rebuild mutated the retained journal")
					}
				})
			}
		}
	}
}

func TestProviderRequestJournalRefusesChangedRenderingAndAuthorityCoordinates(t *testing.T) {
	for _, scenario := range []string{"adapter", "frame", "frame_hash", "surface", "surface_plan", "session", "turn", "logical_operation", "messages", "input", "prompt", "model", "tools", "unknown_field", "duplicate_field", "body", "destination", "header"} {
		t.Run(scenario, func(t *testing.T) {
			frame, surface, session, input, request := providerRequestJournalFixture(t, "anthropic_api")
			body := renderProviderJournalFixture(t, "anthropic_api", session, input, frame.Session.Provider.Model)
			journal, err := newProviderRequestJournal("anthropic_api", frame, surface, session, input, "exact-provider-turn", request, body)
			if err != nil {
				t.Fatal(err)
			}
			var projection providerRequestRendering
			if err := decodeProviderRequestRendering(journal.Rendering, &projection); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "adapter":
				journal.Adapter = "openai_responses"
			case "frame":
				journal.FrameID = "foreign-frame"
			case "frame_hash":
				journal.FrameContentHash = "foreign-content"
			case "surface":
				journal.SurfaceID = "foreign-surface"
			case "surface_plan":
				journal.SurfaceFingerprint = "foreign-plan"
			case "session":
				journal.SessionID = "foreign-session"
			case "turn":
				journal.TurnOrdinal++
			case "logical_operation":
				journal.LogicalOperationIdentity = ""
			case "messages":
				projection.Messages[0].Content = "changed"
			case "input":
				projection.Input.Content = "changed"
			case "prompt":
				projection.SystemPrompt = "changed"
			case "model":
				projection.Model = "changed"
			case "tools":
				projection.Tools = []ToolDefinition{{Name: "unadmitted_tool"}}
			case "body":
				body = []byte(`{"model":"changed"}`)
			case "destination":
				journal.Destination += "/changed"
			case "header":
				journal.SemanticHeaders["anthropic-version"] = "changed"
			}
			journal.Rendering, err = json.Marshal(projection)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "unknown_field" {
				journal.Rendering = append([]byte(`{"unrecognized":true,`), journal.Rendering[1:]...)
			} else if scenario == "duplicate_field" {
				journal.Rendering = append([]byte(`{"model":"duplicate",`), journal.Rendering[1:]...)
			}
			if _, err := RebuildProviderRequestJournal(journal, frame, surface, runtimeeffects.Fingerprint(body)); err == nil {
				t.Fatal("contradictory journal was admitted")
			}
		})
	}
}

func TestProviderRequestJournalCaptureRequiresTheActualManagedRequest(t *testing.T) {
	for _, scenario := range []string{"session", "turn", "agent", "prompt", "input", "body", "credential_url"} {
		t.Run(scenario, func(t *testing.T) {
			frame, surface, session, input, request := providerRequestJournalFixture(t, "anthropic_api")
			body := renderProviderJournalFixture(t, "anthropic_api", session, input, frame.Session.Provider.Model)
			switch scenario {
			case "session":
				session.ID = "foreign"
			case "turn":
				session.TurnCount++
			case "agent":
				session.AgentID = "foreign"
			case "prompt":
				session.SystemPrompt = "changed"
			case "input":
				input.Content = "changed"
			case "body":
				body = []byte(`{"changed":true}`)
			case "credential_url":
				request.URL.User = nil
				request.URL.RawQuery = "api_key=private-credential"
			}
			if _, err := newProviderRequestJournal("anthropic_api", frame, surface, session, input, "exact-provider-turn", request, body); err == nil {
				t.Fatal("inexact managed physical request was admitted")
			}
		})
	}
}

func providerRequestJournalFixture(t *testing.T, adapter string, tools ...ToolDefinition) (agentframe.Frame, managedcapabilities.Surface, *Session, Message, *http.Request) {
	t.Helper()
	backend := adapter
	if adapter == "anthropic_api" {
		backend = llmselection.BackendAnthropic
	}
	profile, err := llmselection.ResolveActiveBackend(backend)
	if err != nil {
		t.Fatal(err)
	}
	harness := effecttest.New()
	ctx := managedEffectHarnessContext(t, harness, t.Name())
	ctx = withEffectTestRequest(t, ctx, profile, llmselection.ResolvedModel{
		ModelAlias: "regular", ConcreteModel: "frozen-model", Backend: profile.ID,
		Provider: profile.Provider, Transport: profile.Transport, RuntimeMode: profile.RuntimeMode,
	})
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		t.Fatal("missing actual admitted surface")
	}
	if len(tools) > 0 {
		planned := make([]managedcapabilities.PlannedTool, 0, len(tools))
		for _, tool := range tools {
			planned = append(planned, managedcapabilities.PlannedTool{
				Name: tool.Name, DefinitionHash: ToolDefinitionIdentity(tool),
				Capability: toolcapabilities.Capability{Name: tool.Name, Visible: true, Callable: true},
				Bindings:   []managedcapabilities.DeliveryBinding{{Kind: managedcapabilities.BindingAPIDefinition, ExactName: tool.Name, RequiredEvidenceKind: "api_request_delivered"}},
			})
		}
		surface, err = managedcapabilities.New(managedcapabilities.Plan{
			ActorIdentity: surface.ActorIdentity, ActorPlan: surface.ActorPlan, Authority: surface.Authority,
			RuntimeMode: surface.RuntimeMode, Provider: surface.Provider, Transport: surface.Transport,
			ProviderContract: surface.ProviderContract, Tools: planned, CreatedAt: surface.CreatedAt,
		})
		if err != nil {
			t.Fatal(err)
		}
		ctx = managedcapabilities.WithContext(ctx, surface)
	}
	frame := managedProviderCallForEffectTest(t, ctx).frame
	role, content, err := frame.ProviderInput()
	if err != nil {
		t.Fatal(err)
	}
	session := &Session{
		ID: surface.Authority.SessionID, AgentID: frame.Session.AgentIdentity.AgentID(),
		TurnCount: surface.Authority.TurnOrdinal - 1, SystemPrompt: frame.Session.ProviderPrompt,
		Tools: tools,
		Messages: []Message{
			{Role: "user", Content: "prior request"},
			{Role: "assistant", Content: "prior tool call", ToolCalls: []ToolCall{{ID: "settled-call", Name: "inspect", Arguments: map[string]any{"value": json.Number("1.0")}}}},
			{Role: "tool", Content: `[{"tool_call_id":"settled-call","value":1.0}]`},
		},
	}
	request, err := http.NewRequest(http.MethodPost, "http://127.0.0.1:1234/v1/provider", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("content-type", "application/json")
	request.Header.Set("authorization", "Bearer private-credential")
	request.Header.Set("x-api-key", "private-credential")
	if adapter == "anthropic_api" {
		request.Header.Set("anthropic-version", "2023-06-01")
	}
	return frame, surface, session, Message{Role: role, Content: content}, request
}

func renderProviderJournalFixture(t *testing.T, adapter string, session *Session, input Message, model string) []byte {
	t.Helper()
	before := session.TurnCount
	var request any
	var err error
	switch adapter {
	case "anthropic_api":
		request, err = (&AnthropicAPIRuntime{}).buildRequest(session, input, model)
	case "openai_compatible":
		request, err = (&OpenAICompatibleRuntime{}).buildRequest(session, input, model)
	case "openai_responses":
		request, err = (&OpenAIResponsesRuntime{}).buildRequest(session, input, model)
	}
	if err != nil {
		t.Fatal(err)
	}
	if session.TurnCount != before {
		t.Fatal("rendering changed session turn")
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return body
}
