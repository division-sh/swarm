package llm

import (
	"bytes"
	"encoding/json"
	"strings"
)

type cliStreamAccumulator struct {
	raw        bytes.Buffer
	message    Message
	toolCalls  []ToolCall
	sessionID  string
	resultText string
}

func newCLIStreamAccumulator() *cliStreamAccumulator {
	return &cliStreamAccumulator{
		message: Message{Role: "assistant"},
	}
}

func (a *cliStreamAccumulator) AddLine(line []byte) {
	if a == nil {
		return
	}
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return
	}
	a.raw.Write(line)
	a.raw.WriteByte('\n')

	var obj map[string]any
	if err := json.Unmarshal(line, &obj); err != nil {
		return
	}
	if sid := strings.TrimSpace(coalesce(asString(obj["session_id"]), asString(obj["sessionId"]))); sid != "" {
		a.sessionID = sid
	}

	switch strings.ToLower(strings.TrimSpace(asString(obj["type"]))) {
	case "assistant":
		a.mergeAssistantObject(obj)
	case "result":
		if text := strings.TrimSpace(asString(obj["result"])); text != "" {
			a.resultText = text
		}
	default:
		// Ignore other stream events for final-response assembly.
	}
}

func (a *cliStreamAccumulator) mergeAssistantObject(obj map[string]any) {
	payload := obj["message"]
	if payload == nil {
		payload = obj
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return
	}
	resp := parseCLIResponse(b)
	if resp == nil {
		return
	}
	if sid := strings.TrimSpace(resp.SessionID); sid != "" {
		a.sessionID = sid
	}
	if text := strings.TrimSpace(resp.Message.Content); text != "" {
		if strings.TrimSpace(a.message.Content) == "" {
			a.message.Content = text
		} else if !strings.Contains(a.message.Content, text) {
			a.message.Content = strings.TrimSpace(a.message.Content + "\n" + text)
		}
	}
	if len(resp.ToolCalls) > 0 {
		a.toolCalls = dedupeToolCalls(append(a.toolCalls, resp.ToolCalls...))
	}
}

func (a *cliStreamAccumulator) Response() *Response {
	if a == nil {
		return &Response{Message: Message{Role: "assistant"}}
	}
	message := a.message
	if strings.TrimSpace(message.Content) == "" && strings.TrimSpace(a.resultText) != "" {
		message.Content = strings.TrimSpace(a.resultText)
	}
	return &Response{
		Message:   message,
		ToolCalls: dedupeToolCalls(a.toolCalls),
		SessionID: strings.TrimSpace(a.sessionID),
		Raw:       bytes.TrimSpace(a.raw.Bytes()),
	}
}
