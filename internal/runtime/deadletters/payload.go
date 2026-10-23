package deadletters

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
)

// ValidatePayload implements engine.error_model.dead_letter_schema. Storage
// identity and claim fields belong to Record, not this event payload.
func ValidatePayload(raw []byte) error {
	var payload struct {
		OriginalEvent   string                 `json:"original_event"`
		OriginalPayload json.RawMessage        `json:"original_payload"`
		Source          *events.RouteIdentity  `json:"source"`
		Target          *events.RouteIdentity  `json:"target"`
		TargetSet       []events.RouteIdentity `json:"target_set"`
		DeliveredTo     []events.DeliveryRoute `json:"delivered_to"`
		EntityID        string                 `json:"entity_id"`
		FlowInstance    string                 `json:"flow_instance"`
		Failure         json.RawMessage        `json:"failure"`
		RetryCount      int                    `json:"retry_count"`
		ChainDepth      int                    `json:"chain_depth"`
		HandlerNode     string                 `json:"handler_node"`
		Timestamp       string                 `json:"timestamp"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		return fmt.Errorf("decode dead-letter payload: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("dead-letter payload must contain one JSON object")
	}
	if strings.TrimSpace(payload.OriginalEvent) == "" {
		return fmt.Errorf("dead-letter original_event is required")
	}
	var original map[string]json.RawMessage
	if err := json.Unmarshal(payload.OriginalPayload, &original); err != nil || original == nil {
		return fmt.Errorf("dead-letter original_payload must be a JSON object")
	}
	if _, err := runtimefailures.UnmarshalEnvelope(payload.Failure); err != nil {
		return fmt.Errorf("dead-letter failure: %w", err)
	}
	if payload.RetryCount < 0 || payload.ChainDepth < 0 {
		return fmt.Errorf("dead-letter retry_count and chain_depth must be nonnegative integers")
	}
	if _, err := time.Parse(time.RFC3339Nano, payload.Timestamp); err != nil {
		return fmt.Errorf("dead-letter timestamp must be RFC3339: %w", err)
	}
	for _, route := range []*events.RouteIdentity{payload.Source, payload.Target} {
		if route != nil && (route.Empty() || *route != route.Normalized()) {
			return fmt.Errorf("dead-letter route must be a nonempty canonical route identity")
		}
	}
	for _, route := range payload.TargetSet {
		if route.Empty() || route != route.Normalized() {
			return fmt.Errorf("dead-letter target_set requires canonical route identities")
		}
	}
	return nil
}
