package agentpersistence

import (
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
)

func encodeAgentConfig(config json.RawMessage) ([]byte, error) {
	if len(config) == 0 {
		config = json.RawMessage(`{}`)
	}
	if _, err := decodeAgentConfig(config); err != nil {
		return nil, err
	}
	var value map[string]any
	if err := canonicaljson.DecodePreservingNumberLexemes(config, &value); err != nil {
		return nil, err
	}
	return canonicaljson.MarshalPreservingNumberKinds(value)
}

func decodeAgentConfig(raw []byte) (json.RawMessage, error) {
	if _, err := canonicaljson.Decode(raw); err != nil {
		return nil, fmt.Errorf("invalid agent config: %w", err)
	}
	if err := validateOpaqueAgentConfig(raw); err != nil {
		return nil, err
	}
	return append(json.RawMessage(nil), raw...), nil
}
