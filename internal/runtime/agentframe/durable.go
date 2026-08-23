package agentframe

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
)

const DurableEncodingVersion = "agent-execution-frame-durable.v1"

type durableEnvelope struct {
	Version           string `json:"version"`
	Frame             Frame  `json:"frame"`
	EventPayloadBytes string `json:"event_payload_bytes_base64"`
	ToolResultBytes   string `json:"tool_result_bytes_base64,omitempty"`
}

// EncodeDurable is the sole durable AgentExecutionFrame encoding owner. Raw
// JSON members are carried as bytes so an outer JSON codec cannot normalize
// provider-visible evidence.
func EncodeDurable(frame Frame) ([]byte, error) {
	if err := frame.Validate(); err != nil {
		return nil, fmt.Errorf("validate durable execution frame: %w", err)
	}
	stored := frame
	stored.Turn.Event.Payload = nil
	stored.Turn.Event.PayloadBytesBase64 = ""
	stored.Turn.ToolResult = nil
	envelope := durableEnvelope{
		Version:           DurableEncodingVersion,
		Frame:             stored,
		EventPayloadBytes: base64.StdEncoding.EncodeToString(frame.Turn.Event.Payload),
	}
	if len(frame.Turn.ToolResult) > 0 {
		envelope.ToolResultBytes = base64.StdEncoding.EncodeToString(frame.Turn.ToolResult)
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encode durable execution frame: %w", err)
	}
	return raw, nil
}

// DecodeDurable validates both the canonical durable envelope and the restored
// frame. Non-canonical bytes are rejected rather than becoming a second frame
// representation.
func DecodeDurable(raw []byte) (Frame, error) {
	if len(raw) == 0 {
		return Frame{}, fmt.Errorf("durable execution frame is required")
	}
	var envelope durableEnvelope
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&envelope); err != nil {
		return Frame{}, fmt.Errorf("decode durable execution frame: %w", err)
	}
	if err := requireJSONEOF(decoder); err != nil {
		return Frame{}, err
	}
	if envelope.Version != DurableEncodingVersion {
		return Frame{}, fmt.Errorf("durable execution frame version %q is invalid", envelope.Version)
	}
	payload, err := base64.StdEncoding.DecodeString(envelope.EventPayloadBytes)
	if err != nil {
		return Frame{}, fmt.Errorf("decode durable execution frame event payload: %w", err)
	}
	toolResult, err := base64.StdEncoding.DecodeString(envelope.ToolResultBytes)
	if err != nil {
		return Frame{}, fmt.Errorf("decode durable execution frame tool result: %w", err)
	}
	frame := envelope.Frame
	frame.Turn.Event.Payload = append(json.RawMessage(nil), payload...)
	frame.Turn.Event.PayloadBytesBase64 = envelope.EventPayloadBytes
	frame.Turn.ToolResult = append(json.RawMessage(nil), toolResult...)
	if err := frame.Validate(); err != nil {
		return Frame{}, fmt.Errorf("validate hydrated execution frame: %w", err)
	}
	canonical, err := EncodeDurable(frame)
	if err != nil {
		return Frame{}, err
	}
	if !bytes.Equal(canonical, raw) {
		return Frame{}, fmt.Errorf("durable execution frame encoding is not canonical")
	}
	return frame, nil
}

func requireJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("durable execution frame has trailing JSON")
		}
		return fmt.Errorf("decode durable execution frame trailing content: %w", err)
	}
	return nil
}
