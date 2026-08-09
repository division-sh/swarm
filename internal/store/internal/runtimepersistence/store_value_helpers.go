package runtimepersistence

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/google/uuid"
)

func jsonRawMessageValue(raw any) json.RawMessage {
	switch value := raw.(type) {
	case nil:
		return nil
	case json.RawMessage:
		return append(json.RawMessage(nil), value...)
	case []byte:
		return json.RawMessage(append([]byte(nil), value...))
	case string:
		return json.RawMessage([]byte(value))
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil
		}
		return encoded
	}
}

func nullUUIDString(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if _, err := uuid.Parse(raw); err != nil {
		return ""
	}
	return raw
}

func firstNonNilError(err, fallback error) error {
	if err != nil {
		return err
	}
	if fallback == nil {
		return errors.New("operation failed")
	}
	return fallback
}
