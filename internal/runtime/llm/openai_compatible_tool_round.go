package llm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
)

// The admitted predecessor retains the provider's round even when the durable
// conversation deliberately projects only role/content. Never hydrate memory
// from this request-only reconstruction.
func replayOpenAICompatibleToolRound(request *openAICompatibleRequest, session *Session, call *managedProviderCall) error {
	if call.toolRound == nil {
		return fmt.Errorf("openai-compatible tool continuation requires its admitted predecessor")
	}
	round := call.toolRound
	prior, err := completionContinuationForPayload(round.Payload, "openai_compatible")
	if err != nil {
		return err
	}
	projection := prior.Projection
	if err := requireOpenAICompatibleRoundSession(session, call, projection); err != nil {
		return err
	}
	if successor, ok := round.ToolContinuation(); ok &&
		(successor.ParentFrameID() != call.frame.Turn.ParentFrameID || !bytes.Equal(successor.ToolResult(), call.frame.Turn.ToolResult)) {
		return fmt.Errorf("openai-compatible recovered tool continuation differs from admitted successor")
	}
	var previousRequest openAICompatibleRequest
	if err := json.Unmarshal(round.Request, &previousRequest); err != nil {
		return fmt.Errorf("decode admitted openai-compatible round request: %w", err)
	}
	if previousRequest.Model != request.Model || len(previousRequest.Messages) == 0 {
		return fmt.Errorf("openai-compatible tool continuation predecessor model/messages differ")
	}
	input := toOpenAICompatibleMessages(projection.Messages[len(projection.Messages)-2])
	if len(input) == 0 || len(input) > len(previousRequest.Messages) ||
		!reflect.DeepEqual(input, previousRequest.Messages[len(previousRequest.Messages)-len(input):]) {
		return fmt.Errorf("openai-compatible predecessor request differs from its admitted input frame")
	}
	var previousResponse openAICompatibleResponse
	if err := json.Unmarshal(prior.Response.Raw, &previousResponse); err != nil {
		return fmt.Errorf("decode admitted openai-compatible round response: %w", err)
	}
	converted, err := convertOpenAICompatibleResponse(previousResponse)
	if err != nil || !reflect.DeepEqual(converted.Message, prior.Response.Message) || len(converted.ToolCalls) == 0 {
		return fmt.Errorf("openai-compatible tool continuation predecessor response differs from admitted message")
	}
	// Reuse only the fingerprint-bound request and its admitted response. This
	// also retains earlier rounds on recovery without a new transcript store.
	assistant := previousResponse.Choices[0].Message
	assistant.Role = "assistant"
	request.Messages = append(previousRequest.Messages, assistant)
	request.Messages = append(request.Messages, toOpenAICompatibleMessages(call.message)...)
	return nil
}

func requireOpenAICompatibleRoundSession(session *Session, call *managedProviderCall, projection completionProjection) error {
	surface := call.toolRound.Surface
	if call.frame.Turn.Kind != agentframe.TurnToolContinuation ||
		(projection.Memory.Enabled && projection.SessionID != session.ID) || projection.Identity.Normalize() != session.MemoryIdentity.Normalize() ||
		projection.Memory != session.Memory || projection.TurnCount != session.TurnCount ||
		projection.FrameID == "" || projection.FrameID != call.frame.Turn.ParentFrameID ||
		len(session.Messages) != len(projection.Messages) ||
		!surface.MatchesActor(session.MemoryIdentity) || surface.Authority.SessionID != session.ID ||
		surface.RuntimeMode != call.provider.RuntimeMode || surface.Provider != call.provider.Provider || surface.Transport != call.provider.Transport {
		return fmt.Errorf("openai-compatible tool continuation predecessor differs from acquired session/frame")
	}
	return nil
}
