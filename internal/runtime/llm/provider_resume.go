package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/agentframe"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
)

func freezeManagedProviderInput(session *Session, call *managedProviderCall, contract ProviderContract) error {
	if call == nil {
		return nil
	}
	messages, err := json.Marshal(append(append([]Message(nil), session.Messages...), call.message))
	if err != nil {
		return err
	}
	rawContract, err := json.Marshal(contract)
	if err != nil {
		return err
	}
	call.frozen = &runtimeeffects.ProviderCall{SessionID: session.ID, TurnCount: session.TurnCount,
		Memory: session.Memory, Messages: messages, ProviderContract: rawContract}
	if call.resume != nil {
		old := call.resume.Journal.Call
		if !bytes.Equal(messages, old.Messages) || !bytes.Equal(rawContract, old.ProviderContract) || session.TurnCount != old.TurnCount {
			return fmt.Errorf("provider resume input differs from its recorded call")
		}
		call.frozen.PredecessorAttemptID = call.resume.Journal.AttemptID
	}
	return nil
}

func beginManagedHTTPProviderCall(ctx context.Context, adapter string, payload []byte, request *http.Request, managed *managedProviderCall) (*runtimeeffects.Handle, error) {
	if managed == nil {
		return runtimeeffects.BeginCompletion(ctx, adapter, payload, nil)
	}
	if managed.frozen == nil {
		return nil, fmt.Errorf("managed HTTP provider call requires frozen input")
	}
	call := *managed.frozen
	headers := make(map[string]string)
	for name, values := range request.Header {
		name = strings.ToLower(name)
		if name == "authorization" || name == "x-api-key" {
			continue
		}
		if len(values) != 1 {
			return nil, fmt.Errorf("provider call semantic header must have one exact value")
		}
		headers[name] = values[0]
	}
	call.Request = runtimeeffects.ProviderCallRequest{Method: request.Method, Destination: request.URL.String(), Headers: headers, Body: append([]byte(nil), payload...)}
	if managed.resume != nil {
		fingerprint, err := call.Request.Fingerprint()
		if err != nil {
			return nil, err
		}
		previous, err := managed.resume.Journal.Call.Request.Fingerprint()
		if err != nil || previous != fingerprint {
			return nil, fmt.Errorf("provider resume rendering differs from its recorded request")
		}
	}
	return runtimeeffects.BeginManagedProviderCall(ctx, adapter, managed.frame, call)
}

func (c *Conversation) recoverInterruptedProviderCall(ctx context.Context, previous *Response) (*Response, bool, error) {
	controller, ok := runtimeeffects.ControllerFromContext(ctx)
	if !ok {
		return nil, false, nil
	}
	recovery, found, err := controller.LoadProviderCallResume(ctx, c.Session.ID, c.Session.Memory)
	if err != nil || !found {
		return nil, found, err
	}
	var messages []Message
	if err := json.Unmarshal(recovery.Journal.Call.Messages, &messages); err != nil || len(messages) == 0 {
		return nil, true, fmt.Errorf("provider resume conversation input is invalid")
	}
	role, content, err := recovery.Frame.ProviderInput()
	last := messages[len(messages)-1]
	if err != nil || last.Role != role || last.Content != content || len(last.ToolCalls) != 0 {
		return nil, true, fmt.Errorf("provider resume input differs from its frozen frame")
	}
	c.Session.Messages = append([]Message(nil), messages[:len(messages)-1]...)
	c.Session.TurnCount = recovery.Journal.Call.TurnCount
	c.Messages, c.TurnCount = append([]Message(nil), c.Session.Messages...), c.Session.TurnCount
	if c.causalEvent == nil || recovery.Frame.Turn.Event.ID != c.causalEvent.ID() {
		return nil, true, fmt.Errorf("provider resume causal event differs from its delivery")
	}
	draft := agentframe.TurnDraft{Kind: recovery.Frame.Turn.Kind, Event: *c.causalEvent,
		ParentFrameID: recovery.Frame.Turn.ParentFrameID, InputRole: role, InputContent: content}
	if draft.Kind == agentframe.TurnToolContinuation {
		continuation, err := agentframe.NewToolContinuation(draft.ParentFrameID, recovery.Frame.Turn.ToolResult)
		if err != nil {
			return nil, true, err
		}
		draft = continuation.Draft(*c.causalEvent)
	}
	response, err := c.continueManagedProviderCall(ctx, draft, previous, &recovery)
	return response, true, err
}
