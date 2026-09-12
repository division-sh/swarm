package runforkrevision

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/google/uuid"
)

// FactKey derives the canonical writer's existing key bytes from a fact body.
// It validates key coordinates only; owning-run, revision, body semantics and
// reference admission belong to the historical fact reader and family owners.
func FactKey(family Family, raw []byte) (string, error) {
	if family == FamilyFanOutObligations {
		return fanOutFactKey(raw)
	}
	var field string
	switch family {
	case FamilyEvents, FamilyCommittedReplayScopes:
		field = "event_id"
	case FamilyEntityMutations:
		field = "mutation_id"
	case FamilyEntityMetadata:
		field = "entity_id"
	case FamilyEventDeliveries:
		field = "delivery_id"
	case FamilyEventReceipts:
		field = "receipt_id"
	case FamilyDeadLetters:
		field = "dead_letter_id"
	case FamilyTimers:
		field = "timer_id"
	case FamilyAgentSessions, FamilyAgentConversationAudits:
		field = "session_id"
	case FamilyAgentTurns:
		field = "turn_id"
	case FamilyReplyContexts:
		field = "reply_context_id"
	default:
		return "", fmt.Errorf("unsupported run fork revision fact family %q", family)
	}
	body, err := decodeFactKeyFields(raw, field)
	if err != nil {
		return "", fmt.Errorf("decode %s fact key: %w", family, err)
	}
	var key string
	if err := json.Unmarshal(body[field], &key); err != nil {
		return "", fmt.Errorf("decode %s.%s fact key: %w", family, field, err)
	}
	if strings.TrimSpace(key) == "" {
		return "", fmt.Errorf("%s fact key requires %s", family, field)
	}
	// reply_context_id is opaque TEXT; all other scalar primary identities
	// are UUIDs. Validation must not rewrite their persisted key spelling.
	if family != FamilyReplyContexts {
		if _, err := uuid.Parse(strings.TrimSpace(key)); err != nil {
			return "", fmt.Errorf("%s fact key requires UUID %s: %w", family, field, err)
		}
	}
	return key, nil
}

func fanOutFactKey(raw []byte) (string, error) {
	if _, err := decodeFactKeyFields(raw, "fact_kind", "triggering_delivery_id", "flow_path", "declaration_family", "semantic_path", "ordinal"); err != nil {
		return "", fmt.Errorf("decode fan-out fact key: %w", err)
	}
	var body struct {
		Kind                 string `json:"fact_kind"`
		TriggeringDeliveryID string `json:"triggering_delivery_id"`
		FlowPath             string `json:"flow_path"`
		DeclarationFamily    string `json:"declaration_family"`
		SemanticPath         string `json:"semantic_path"`
		Ordinal              *int64 `json:"ordinal"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return "", fmt.Errorf("decode fan-out fact key: %w", err)
	}
	switch body.Kind {
	case "intent", "outcome", "barrier":
	default:
		return "", fmt.Errorf("unsupported fan-out fact kind %q", body.Kind)
	}
	if _, err := uuid.Parse(strings.TrimSpace(body.TriggeringDeliveryID)); err != nil {
		return "", fmt.Errorf("fan-out fact key requires UUID triggering_delivery_id: %w", err)
	}
	declaration, err := identity.AdmitDeclarationIdentity(body.FlowPath, body.DeclarationFamily, body.SemanticPath)
	if err != nil {
		return "", fmt.Errorf("fan-out fact key declaration: %w", err)
	}
	// This is the existing ledger serialization, not DeclarationIdentity.Key.
	// SemanticPath may contain delimiters; consume coordinates, never split keys.
	key := strings.Join([]string{body.Kind, body.TriggeringDeliveryID, declaration.Flow().String(), declaration.Family(), declaration.SemanticPath()}, "|")
	if body.Kind == "outcome" {
		if body.Ordinal == nil || *body.Ordinal < 0 {
			return "", fmt.Errorf("fan-out outcome fact key requires a non-negative ordinal")
		}
		key += "|" + strconv.FormatInt(*body.Ordinal, 10)
	}
	return key, nil
}

// Check only key coordinates, not arbitrary payload JSON. In particular,
// encoding/json's case-insensitive struct matching must not let the typed
// reader consume a different primary identity from this key owner.
func decodeFactKeyFields(raw []byte, fields ...string) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	opening, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if opening != json.Delim('{') {
		return nil, fmt.Errorf("fact key requires a JSON object")
	}
	values := make(map[string]json.RawMessage, len(fields))
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		name, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("fact field name must be a string")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		for _, field := range fields {
			if !strings.EqualFold(name, field) {
				continue
			}
			if name != field {
				return nil, fmt.Errorf("fact key field %q must use exact spelling %q", name, field)
			}
			if _, exists := values[field]; exists {
				return nil, fmt.Errorf("duplicate JSON fact key field %q", field)
			}
			values[field] = value
		}
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("fact key has trailing JSON content")
	}
	return values, nil
}
