package attemptgeneration

import (
	"encoding/base64"
	"fmt"
	"strings"
)

const PayloadKey = "loop_generation"

// Generation is an immutable workflow-attempt fence. Consumers compare it;
// only loopruntime advances or closes the lifecycle it identifies.
type Generation struct {
	FlowID        string `json:"flow_id,omitempty"`
	LoopID        string `json:"loop_id"`
	ActivationID  string `json:"activation_id"`
	RevisionField string `json:"revision_field"`
	RevisionID    string `json:"revision_id"`
	Attempt       int    `json:"attempt"`
}

func (g Generation) Normalize() Generation {
	g.FlowID = strings.TrimSpace(g.FlowID)
	g.LoopID = strings.TrimSpace(g.LoopID)
	g.ActivationID = strings.TrimSpace(g.ActivationID)
	g.RevisionField = strings.TrimSpace(g.RevisionField)
	g.RevisionID = strings.TrimSpace(g.RevisionID)
	return g
}

func (g Generation) Valid() bool {
	g = g.Normalize()
	return g.LoopID != "" && g.ActivationID != "" && g.RevisionField != "" && g.RevisionID != "" && g.Attempt > 0
}

func (g Generation) Equal(other Generation) bool {
	g, other = g.Normalize(), other.Normalize()
	return g.Valid() && other.Valid() && g == other
}

func (g Generation) KeySuffix() string {
	g = g.Normalize()
	if !g.Valid() {
		return ""
	}
	parts := []string{g.FlowID, g.LoopID, g.ActivationID, g.RevisionID, fmt.Sprintf("%d", g.Attempt)}
	for i := range parts {
		parts[i] = base64.RawURLEncoding.EncodeToString([]byte(parts[i]))
	}
	return strings.Join(parts, ".")
}

func ParseKeySuffix(raw string) (Generation, bool) {
	parts := strings.Split(strings.TrimSpace(raw), ".")
	if len(parts) != 5 {
		return Generation{}, false
	}
	decoded := make([]string, len(parts))
	for i, part := range parts {
		value, err := base64.RawURLEncoding.DecodeString(part)
		if err != nil {
			return Generation{}, false
		}
		decoded[i] = string(value)
	}
	attempt := 0
	if _, err := fmt.Sscanf(decoded[4], "%d", &attempt); err != nil {
		return Generation{}, false
	}
	g := Generation{FlowID: decoded[0], LoopID: decoded[1], ActivationID: decoded[2], RevisionID: decoded[3], Attempt: attempt}
	// revision_field is carried in payload and persisted records. Key parsing is
	// used only to isolate generations, so it intentionally cannot reconstruct it.
	return g.Normalize(), g.LoopID != "" && g.ActivationID != "" && g.RevisionID != "" && g.Attempt > 0
}

func (g Generation) PayloadValue() map[string]any {
	g = g.Normalize()
	if !g.Valid() {
		return nil
	}
	return map[string]any{
		"flow_id": g.FlowID, "loop_id": g.LoopID, "activation_id": g.ActivationID,
		"revision_field": g.RevisionField, "revision_id": g.RevisionID, "attempt": g.Attempt,
	}
}

func FromPayload(payload map[string]any) (Generation, bool) {
	if payload == nil {
		return Generation{}, false
	}
	raw, ok := payload[PayloadKey].(map[string]any)
	if !ok {
		return Generation{}, false
	}
	g := Generation{
		FlowID: asString(raw["flow_id"]), LoopID: asString(raw["loop_id"]), ActivationID: asString(raw["activation_id"]),
		RevisionField: asString(raw["revision_field"]), RevisionID: asString(raw["revision_id"]), Attempt: asInt(raw["attempt"]),
	}.Normalize()
	return g, g.Valid()
}

func FromLoopContext(loop map[string]any) (Generation, bool) {
	g := Generation{
		FlowID:        asString(loop["flow_id"]),
		LoopID:        asString(loop["id"]),
		ActivationID:  asString(loop["activation_id"]),
		RevisionField: asString(loop["revision_field"]),
		RevisionID:    asString(loop["revision_id"]),
		Attempt:       asInt(loop["attempt"]),
	}.Normalize()
	return g, g.Valid()
}

func asString(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func asInt(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	default:
		return 0
	}
}
