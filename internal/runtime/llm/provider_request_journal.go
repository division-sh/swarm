package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
)

type providerRequestRendering struct {
	Version      string           `json:"version"`
	Model        string           `json:"model"`
	SystemPrompt string           `json:"system_prompt"`
	Messages     []Message        `json:"messages"`
	Tools        []ToolDefinition `json:"tools"`
	Input        Message          `json:"input"`
}

func newProviderRequestJournal(adapter string, frame agentframe.Frame, surface managedcapabilities.Surface, session *Session, input Message, logicalOperation string, request *http.Request, body []byte) (runtimeeffects.ProviderRequestJournal, error) {
	if request == nil || request.URL == nil {
		return runtimeeffects.ProviderRequestJournal{}, fmt.Errorf("provider request journal requires its physical request")
	}
	draft, err := providerRequestJournalDraft(adapter, frame, surface, session, input, logicalOperation)
	if err != nil {
		return runtimeeffects.ProviderRequestJournal{}, err
	}
	headers, err := providerRequestJournalHeaders(adapter, request)
	if err != nil {
		return runtimeeffects.ProviderRequestJournal{}, err
	}
	draft.Method, draft.Destination, draft.SemanticHeaders = request.Method, request.URL.String(), headers
	journal, err := runtimeeffects.NewProviderRequestJournal(draft, body)
	if err != nil {
		return runtimeeffects.ProviderRequestJournal{}, err
	}
	if _, err := RebuildProviderRequestJournal(journal, frame, surface, runtimeeffects.Fingerprint(body)); err != nil {
		return runtimeeffects.ProviderRequestJournal{}, err
	}
	return journal, nil
}

func providerRequestJournalDraft(adapter string, frame agentframe.Frame, surface managedcapabilities.Surface, session *Session, input Message, logicalOperation string) (runtimeeffects.ProviderRequestJournal, error) {
	if session == nil ||
		session.ID != surface.Authority.SessionID || session.AgentID != frame.Session.AgentIdentity.AgentID() ||
		session.TurnCount+1 != surface.Authority.TurnOrdinal {
		return runtimeeffects.ProviderRequestJournal{}, fmt.Errorf("provider request journal requires the exact managed session and request")
	}
	tools := append([]ToolDefinition(nil), session.Tools...)
	for index := range tools {
		tools[index].Description = DeliveredToolDescription(tools[index])
		tools[index].Usage = ""
	}
	rendering, err := json.Marshal(providerRequestRendering{
		Version: runtimeeffects.ProviderRequestJournalVersion,
		Model:   frame.Session.Provider.Model, SystemPrompt: session.SystemPrompt,
		Messages: session.Messages, Tools: tools, Input: input,
	})
	if err != nil {
		return runtimeeffects.ProviderRequestJournal{}, fmt.Errorf("freeze provider rendering inputs: %w", err)
	}
	fingerprint, err := surface.ContinuationFingerprint()
	if err != nil {
		return runtimeeffects.ProviderRequestJournal{}, err
	}
	memory := session.Memory
	return runtimeeffects.ProviderRequestJournal{
		Version: runtimeeffects.ProviderRequestJournalVersion, Adapter: adapter,
		FrameID: frame.FrameID, FrameContentHash: frame.ContentHash,
		SurfaceID: surface.ID, SurfaceFingerprint: fingerprint,
		SessionID: session.ID, TurnOrdinal: session.TurnCount + 1, LogicalOperationIdentity: logicalOperation,
		Memory: &memory, Rendering: rendering,
	}, nil
}

func providerRequestJournalHeaders(adapter string, request *http.Request) (map[string]string, error) {
	headers := map[string]string{"content-type": request.Header.Get("content-type")}
	if len(request.Header.Values("content-type")) != 1 {
		return nil, fmt.Errorf("provider request journal requires one exact content type")
	}
	if adapter == "anthropic_api" {
		if len(request.Header.Values("anthropic-version")) != 1 {
			return nil, fmt.Errorf("provider request journal requires one exact version header")
		}
		headers["anthropic-version"] = request.Header.Get("anthropic-version")
	}
	return headers, nil
}

func prepareManagedProviderRequestJournal(ctx context.Context, adapter, destination string, session *Session, input Message, body []byte, managed *managedProviderCall) (*managedProviderCall, error) {
	if managed == nil {
		return nil, nil
	}
	surface, ok := managedcapabilities.FromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("provider request journal requires its admitted capability surface")
	}
	logicalOperation, ok := runtimeeffects.LogicalOperationIdentityFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("provider request journal requires its logical operation")
	}
	draft, err := providerRequestJournalDraft(adapter, managed.frame, surface, session, input, logicalOperation)
	if err != nil {
		return nil, err
	}
	rebuilt, err := rebuildProviderRequestJournalBody(draft, managed.frame, surface)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(rebuilt, body) {
		return nil, fmt.Errorf("provider request differs from its independently frozen rendering")
	}
	captured := *managed
	captured.requestJournal = nil
	// Unsupported credential-bearing destinations keep original-call semantics,
	// but have no persisted journal and cannot enter startup resume admission.
	if !runtimeeffects.ProviderRequestJournalSupportsDestination(destination) {
		return &captured, nil
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, destination, nil)
	if err != nil {
		return nil, fmt.Errorf("prepare provider request journal metadata: %w", err)
	}
	request.Header.Set("content-type", "application/json")
	if adapter == "anthropic_api" {
		request.Header.Set("anthropic-version", "2023-06-01")
	}
	headers, err := providerRequestJournalHeaders(adapter, request)
	if err != nil {
		return nil, err
	}
	draft.Method, draft.Destination, draft.SemanticHeaders = request.Method, request.URL.String(), headers
	journal, err := runtimeeffects.NewProviderRequestJournal(draft, body)
	if err != nil {
		return nil, err
	}
	captured.requestJournal = &journal
	return &captured, nil
}

func validateHTTPProviderRequestJournal(adapter string, request *http.Request, managed *managedProviderCall) error {
	if managed == nil || managed.requestJournal == nil {
		return nil
	}
	journal := managed.requestJournal
	headers, err := providerRequestJournalHeaders(adapter, request)
	if err != nil {
		return err
	}
	if adapter != journal.Adapter || request.Method != journal.Method || request.URL.String() != journal.Destination ||
		len(headers) != len(journal.SemanticHeaders) {
		return fmt.Errorf("provider request differs from its frozen physical metadata")
	}
	for name, value := range headers {
		if journal.SemanticHeaders[name] != value {
			return fmt.Errorf("provider request differs from its frozen semantic headers")
		}
	}
	return nil
}

// RebuildProviderRequestJournal uses only frozen evidence and the original
// renderers. A successful rebuild is not permission to retry or dispatch.
func RebuildProviderRequestJournal(journal runtimeeffects.ProviderRequestJournal, frame agentframe.Frame, surface managedcapabilities.Surface, operationFingerprint string) ([]byte, error) {
	body, err := rebuildProviderRequestJournalBody(journal, frame, surface)
	if err != nil {
		return nil, err
	}
	if err := journal.ValidateRequest(body, operationFingerprint); err != nil {
		return nil, err
	}
	return body, nil
}

func rebuildProviderRequestJournalBody(journal runtimeeffects.ProviderRequestJournal, frame agentframe.Frame, surface managedcapabilities.Surface) ([]byte, error) {
	if err := frame.Validate(); err != nil {
		return nil, fmt.Errorf("provider request journal execution frame: %w", err)
	}
	if err := surface.Validate(); err != nil {
		return nil, fmt.Errorf("provider request journal capability surface: %w", err)
	}
	fingerprint, err := surface.ContinuationFingerprint()
	if err != nil {
		return nil, err
	}
	if !frame.MatchesSurface(surface) || journal.FrameID != frame.FrameID || journal.FrameContentHash != frame.ContentHash ||
		journal.SurfaceID != surface.ID || journal.SurfaceFingerprint != fingerprint ||
		journal.SessionID != surface.Authority.SessionID || journal.TurnOrdinal != surface.Authority.TurnOrdinal {
		return nil, fmt.Errorf("provider request journal differs from the exact frame, surface or turn")
	}
	var rendering providerRequestRendering
	if err := decodeProviderRequestRendering(journal.Rendering, &rendering); err != nil {
		return nil, err
	}
	role, content, err := frame.ProviderInput()
	if err != nil {
		return nil, err
	}
	if rendering.Version != runtimeeffects.ProviderRequestJournalVersion || journal.Memory == nil ||
		rendering.SystemPrompt != frame.Session.ProviderPrompt || rendering.Model != frame.Session.Provider.Model ||
		rendering.Input.Role != role || rendering.Input.Content != content || len(rendering.Input.ToolCalls) != 0 {
		return nil, fmt.Errorf("provider request journal rendering differs from its execution frame")
	}
	if _, err := ObserveAPIRequestCapabilitySurface(surface, rendering.Tools); err != nil {
		return nil, fmt.Errorf("provider request journal tool definitions: %w", err)
	}
	provider := frame.Session.Provider
	profile, err := llmselection.ResolveProviderContractProfile(provider.RuntimeMode, provider.Provider, provider.Transport)
	if err != nil {
		return nil, fmt.Errorf("provider request journal provider contract: %w", err)
	}
	backend := journal.Adapter
	if backend == "anthropic_api" {
		backend = llmselection.BackendAnthropic
	}
	if profile.ID != backend {
		return nil, fmt.Errorf("provider request journal adapter differs from its frame")
	}
	session := &Session{
		ID: journal.SessionID, AgentID: frame.Session.AgentIdentity.AgentID(), Memory: *journal.Memory,
		SystemPrompt: rendering.SystemPrompt, Messages: rendering.Messages, Tools: rendering.Tools,
		TurnCount: journal.TurnOrdinal - 1,
	}
	var request any
	switch journal.Adapter {
	case "anthropic_api":
		request, err = (&AnthropicAPIRuntime{}).buildRequest(session, rendering.Input, rendering.Model)
	case "openai_compatible":
		request, err = (&OpenAICompatibleRuntime{}).buildRequest(session, rendering.Input, rendering.Model)
	case "openai_responses":
		request, err = (&OpenAIResponsesRuntime{}).buildRequest(session, rendering.Input, rendering.Model)
	default:
		return nil, fmt.Errorf("provider request journal adapter is not an HTTP provider")
	}
	if err != nil {
		return nil, fmt.Errorf("rebuild frozen provider request: %w", err)
	}
	body, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("encode frozen provider request: %w", err)
	}
	return body, nil
}

func decodeProviderRequestRendering(raw []byte, rendering *providerRequestRendering) error {
	if _, err := canonicaljson.Decode(raw); err != nil {
		return fmt.Errorf("invalid provider request rendering JSON: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(rendering); err != nil {
		return fmt.Errorf("decode provider request rendering: %w", err)
	}
	return nil
}
