package runtime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

type CanonicalRuntimeLogPayload struct {
	LogLevel    string
	Message     string
	StackTrace  string
	Detail      map[string]any
	Correlation map[string]string

	Component     string
	Action        string
	EventID       string
	EventType     string
	ParentEventID string
	HandlerID     string
	Error         string
	ErrorCode     string
	AgentID       string
	EntityID      string
	SessionID     string
	DurationUS    int

	DeliveryState string
	PreviousState string
	Transition    string
	Reason        string
	Terminal      string
	RetryCount    int
}

func DecodeCanonicalRuntimeLogPayload(raw []byte) (CanonicalRuntimeLogPayload, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return CanonicalRuntimeLogPayload{}, fmt.Errorf("runtime log payload is empty")
	}

	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return CanonicalRuntimeLogPayload{}, fmt.Errorf("decode runtime log payload: %w", err)
	}

	level, err := requiredRuntimeLogString(payload, "log_level")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	message, err := requiredRuntimeLogString(payload, "message")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	stackTrace, err := optionalRuntimeLogString(payload, "stack_trace")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}

	detail, err := requiredRuntimeLogObject(payload, "details")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	component, err := requiredRuntimeLogString(detail, "component")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	action, err := requiredRuntimeLogString(detail, "action")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}

	eventName, err := optionalRuntimeLogString(detail, "event_name")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	eventType, err := optionalRuntimeLogString(detail, "event_type")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	if eventName != "" && eventType != "" && eventName != eventType {
		return CanonicalRuntimeLogPayload{}, fmt.Errorf("runtime log details.event_name and details.event_type disagree")
	}
	if eventName == "" {
		eventName = eventType
	}

	correlation := map[string]string{}
	if rawCorrelation, ok := detail["correlation"]; ok {
		correlation, err = runtimeLogStringMap(rawCorrelation, "details.correlation")
		if err != nil {
			return CanonicalRuntimeLogPayload{}, err
		}
	}

	durationUS, err := optionalRuntimeLogInt(detail, "duration_us")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	retryCount, err := optionalRuntimeLogInt(detail, "retry_count")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}

	parentEventID, err := optionalRuntimeLogString(detail, "parent_event_id")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	handlerID, err := optionalRuntimeLogString(detail, "handler_id")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	errorText, err := optionalRuntimeLogString(detail, "error")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	errorCode, err := optionalRuntimeLogString(detail, "error_code")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	agentID, err := optionalRuntimeLogString(detail, "agent_id")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	entityID, err := optionalRuntimeLogString(detail, "entity_id")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	sessionID, err := optionalRuntimeLogString(detail, "session_id")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	eventID, err := optionalRuntimeLogString(detail, "event_id")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	deliveryState, err := optionalRuntimeLogString(detail, "delivery_state")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	previousState, err := optionalRuntimeLogString(detail, "delivery_previous_state")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	transition, err := optionalRuntimeLogString(detail, "delivery_transition")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	reason, err := optionalRuntimeLogString(detail, "delivery_reason")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}
	terminal, err := optionalRuntimeLogString(detail, "delivery_terminal_outcome")
	if err != nil {
		return CanonicalRuntimeLogPayload{}, err
	}

	return CanonicalRuntimeLogPayload{
		LogLevel:      level,
		Message:       message,
		StackTrace:    stackTrace,
		Detail:        detail,
		Correlation:   correlation,
		Component:     component,
		Action:        action,
		EventID:       eventID,
		EventType:     eventName,
		ParentEventID: parentEventID,
		HandlerID:     handlerID,
		Error:         errorText,
		ErrorCode:     errorCode,
		AgentID:       agentID,
		EntityID:      entityID,
		SessionID:     sessionID,
		DurationUS:    durationUS,
		DeliveryState: deliveryState,
		PreviousState: previousState,
		Transition:    transition,
		Reason:        reason,
		Terminal:      terminal,
		RetryCount:    retryCount,
	}, nil
}

func requiredRuntimeLogString(raw map[string]any, key string) (string, error) {
	value, ok := raw[key]
	if !ok {
		return "", fmt.Errorf("runtime log %s is required", key)
	}
	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("runtime log %s must be a string", key)
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return "", fmt.Errorf("runtime log %s is required", key)
	}
	return text, nil
}

func optionalRuntimeLogString(raw map[string]any, key string) (string, error) {
	value, ok := raw[key]
	if !ok || value == nil {
		return "", nil
	}
	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("runtime log %s must be a string", key)
	}
	return strings.TrimSpace(text), nil
}

func requiredRuntimeLogObject(raw map[string]any, key string) (map[string]any, error) {
	value, ok := raw[key]
	if !ok {
		return nil, fmt.Errorf("runtime log %s is required", key)
	}
	obj, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("runtime log %s must be an object", key)
	}
	return obj, nil
}

func runtimeLogStringMap(raw any, field string) (map[string]string, error) {
	if raw == nil {
		return map[string]string{}, nil
	}
	obj, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("runtime log %s must be an object", field)
	}
	out := make(map[string]string, len(obj))
	for key, value := range obj {
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, fmt.Errorf("runtime log %s contains an empty key", field)
		}
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("runtime log %s.%s must be a string", field, key)
		}
		out[key] = strings.TrimSpace(text)
	}
	return out, nil
}

func optionalRuntimeLogInt(raw map[string]any, key string) (int, error) {
	value, ok := raw[key]
	if !ok || value == nil {
		return 0, nil
	}
	number, ok := value.(float64)
	if !ok {
		return 0, fmt.Errorf("runtime log %s must be an integer", key)
	}
	if math.Trunc(number) != number {
		return 0, fmt.Errorf("runtime log %s must be an integer", key)
	}
	return int(number), nil
}
