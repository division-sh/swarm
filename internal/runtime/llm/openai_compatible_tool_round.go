package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

// The admitted predecessor retains the provider's round even when durable
// memory redacts its content or arguments. Never hydrate memory from this
// request-only reconstruction.
func replayOpenAICompatibleToolRound(ctx context.Context, request *openAICompatibleRequest, session *Session, call *managedProviderCall) error {
	if call.selectedToolRound != nil {
		if call.toolRound != nil {
			return fmt.Errorf("openai-compatible tool continuation has conflicting predecessors")
		}
		dispatch, err := selectedOpenAICompatibleToolRound(ctx, session, call)
		if err != nil {
			return err
		}
		return appendOpenAICompatibleToolRound(request, call, dispatch.request, *call.selectedToolRound, dispatch.projection.Messages[len(dispatch.projection.Messages)-1])
	}
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
	return appendOpenAICompatibleToolRound(request, call, round.Request, prior.Response, projection.Messages[len(projection.Messages)-2])
}

func appendOpenAICompatibleToolRound(request *openAICompatibleRequest, call *managedProviderCall, rawRequest []byte, prior Response, inputMessage Message) error {
	var previousRequest openAICompatibleRequest
	if err := json.Unmarshal(rawRequest, &previousRequest); err != nil {
		return fmt.Errorf("decode admitted openai-compatible round request: %w", err)
	}
	if previousRequest.Model != request.Model || len(previousRequest.Messages) == 0 {
		return fmt.Errorf("openai-compatible tool continuation predecessor model/messages differ")
	}
	input := toOpenAICompatibleMessages(inputMessage)
	if len(input) == 0 || len(input) > len(previousRequest.Messages) ||
		!reflect.DeepEqual(input, previousRequest.Messages[len(previousRequest.Messages)-len(input):]) {
		return fmt.Errorf("openai-compatible predecessor request differs from its admitted input frame")
	}
	var previousResponse openAICompatibleResponse
	if err := json.Unmarshal(prior.Raw, &previousResponse); err != nil {
		return fmt.Errorf("decode admitted openai-compatible round response: %w", err)
	}
	converted, err := convertOpenAICompatibleResponse(previousResponse)
	if err != nil || !reflect.DeepEqual(converted.Message, prior.Message) || len(converted.ToolCalls) == 0 {
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

// Selected execution owns only this invocation's acknowledged response. It does
// not acquire a normal-delivery continuation or cross-invocation recovery grant.
func selectedOpenAICompatibleToolRound(ctx context.Context, session *Session, call *managedProviderCall) (*completionDispatch, error) {
	previous := call.selectedToolRound
	dispatch := previous.completionDispatch
	if dispatch == nil || dispatch.handle == nil || dispatch.projection == nil || previous.completionAttempt == nil || previous.ToolOutputAuthority == nil {
		return nil, fmt.Errorf("selected openai-compatible predecessor lacks acknowledged completion")
	}
	if err := requireSelectedRoundCompletion(ctx, previous, dispatch); err != nil {
		return nil, err
	}
	if err := requireSelectedRoundSession(session, call, dispatch.projection, previous.CapabilitySurface); err != nil {
		return nil, err
	}
	return dispatch, nil
}

func requireSelectedRoundCompletion(ctx context.Context, previous *Response, dispatch *completionDispatch) error {
	attempt := dispatch.handle.Attempt()
	current, ok := runtimeeffects.AuthorityFromContext(ctx)
	if !ok || !current.Valid() || current.Kind != runtimeeffects.AuthoritySelectedContractFork ||
		!attempt.Authority.Valid() || attempt.Authority.Kind != current.Kind || attempt.Authority.SelectedFork != current.SelectedFork ||
		attempt.Authority.ExecutionOwner != current.ExecutionOwner || attempt.Authority.FenceGeneration != current.FenceGeneration {
		return fmt.Errorf("selected openai-compatible predecessor differs from current execution")
	}
	if !attempt.AuthorizationAcknowledged || attempt.Kind != runtimeeffects.KindProviderTurn || attempt.Adapter != "openai_compatible" ||
		dispatch.state != runtimeeffects.StateSettled || dispatch.invocation != completionProviderInvocationStarted ||
		previous.completionAttempt.OperationID != attempt.OperationID || previous.completionAttempt.AttemptID != attempt.AttemptID ||
		previous.completionAttempt.Authority.Kind != attempt.Authority.Kind || previous.completionAttempt.Authority.SelectedFork != attempt.Authority.SelectedFork ||
		previous.ToolOutputAuthority.Validate() != nil || previous.ToolOutputAuthority.ProviderOperationID != attempt.OperationID ||
		dispatch.requestFingerprint == "" || runtimeeffects.Fingerprint(dispatch.request) != dispatch.requestFingerprint ||
		dispatch.responseFingerprint == "" || runtimeeffects.Fingerprint(previous.Raw) != dispatch.responseFingerprint {
		return fmt.Errorf("selected openai-compatible predecessor lacks exact committed request/response")
	}
	return nil
}

func requireSelectedRoundSession(session *Session, call *managedProviderCall, projection *completionProjection, surface *managedcapabilities.Surface) error {
	selected := call.selectedToolRound.completionAttempt.Authority.SelectedFork
	if call.frame.Turn.Kind != agentframe.TurnToolContinuation || projection.FrameID == "" || projection.FrameID != call.frame.Turn.ParentFrameID ||
		projection.SessionID != session.ID || projection.Identity.Normalize() != session.MemoryIdentity.Normalize() || projection.Memory != session.Memory ||
		projection.TurnCount != session.TurnCount || len(projection.Messages) == 0 || len(projection.Messages)+1 != len(session.Messages) {
		return fmt.Errorf("selected openai-compatible predecessor differs from acquired session/frame")
	}
	if surface == nil || surface.Validate() != nil || !surface.MatchesActor(session.MemoryIdentity) ||
		projection.FrameID != "agent-frame:v1:"+surface.Authority.ID ||
		surface.Authority.ExecutionKind != managedcapabilities.ExecutionSelectedContractFork || surface.Authority.ExecutionAuthorityID != selected.ExecutionID ||
		surface.Authority.RunID != selected.ForkRunID || surface.Authority.SessionID != session.ID || surface.Authority.TurnOrdinal != session.TurnCount ||
		surface.RuntimeMode != call.provider.RuntimeMode || surface.Provider != call.provider.Provider || surface.Transport != call.provider.Transport {
		return fmt.Errorf("selected openai-compatible predecessor differs from provider authority")
	}
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
