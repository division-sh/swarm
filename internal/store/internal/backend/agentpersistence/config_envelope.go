package agentpersistence

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

// The agents.config column has one internal wire shape. Receiver values are
// inert business data, never inputs to the opaque agent authority guards.
type agentConfigEnvelope struct {
	Config         json.RawMessage `json:"config"`
	ReceiverConfig json.RawMessage `json:"receiver_config"`
}

func encodeAgentConfigEnvelope(config, receiver json.RawMessage) ([]byte, error) {
	if len(config) == 0 {
		config = json.RawMessage(`{}`)
	}
	raw, err := json.Marshal(agentConfigEnvelope{Config: config, ReceiverConfig: receiver})
	if err != nil {
		return nil, err
	}
	if _, _, err := decodeAgentConfigEnvelope(raw); err != nil {
		return nil, err
	}
	return raw, nil
}

func decodeAgentConfigEnvelope(raw []byte) (json.RawMessage, json.RawMessage, error) {
	// Validate before decoding RawMessages: encoding/json alone accepts duplicate
	// keys. Keep the original numeric tokens rather than re-encoding semantics.
	if _, err := canonicaljson.Decode(raw); err != nil {
		return nil, nil, fmt.Errorf("invalid agent config envelope: %w", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, nil, fmt.Errorf("agent config envelope must be a JSON object: %w", err)
	}
	config, hasConfig := fields["config"]
	receiver, hasReceiver := fields["receiver_config"]
	if len(fields) != 2 || !hasConfig || !hasReceiver {
		return nil, nil, fmt.Errorf("agent config envelope requires exactly config and receiver_config")
	}
	if err := validateOpaqueAgentConfig(config); err != nil {
		return nil, nil, err
	}
	if bytes.Equal(bytes.TrimSpace(receiver), []byte("null")) {
		receiver = nil
	} else if trimmed := bytes.TrimSpace(receiver); len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, nil, fmt.Errorf("receiver_config must be a JSON object or null")
	}
	return append(json.RawMessage(nil), config...), append(json.RawMessage(nil), receiver...), nil
}
