package runstalled

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/division-sh/swarm/internal/operatorread"
)

// StalledInstance is detached canonical diagnosis evidence, not work authority.
type StalledInstance struct {
	FlowInstance   string                          `json:"flow_instance"`
	BlockingLayer  string                          `json:"blocking_layer"`
	BlockingReason string                          `json:"blocking_reason"`
	Attribution    operatorread.RunTurnAttribution `json:"attribution"`
}

// Canonicalize only this alert's attribution set. Neither input order nor an
// identical repeated member constitutes a new stalled-run alert.
func canonicalStalledInstances(instances []StalledInstance) ([]StalledInstance, string, error) {
	type row struct {
		instance StalledInstance
		encoded  string
	}
	rows := make([]row, 0, len(instances))
	for _, instance := range instances {
		if instance.BlockingLayer == "" || instance.BlockingReason == "" || instance.Attribution.Historical || instance.Attribution.FlowInstance != instance.FlowInstance {
			return nil, "", fmt.Errorf("stalled-instance attribution contradicts current instance %q", instance.FlowInstance)
		}
		switch instance.Attribution.Emission {
		case operatorread.RunEmissionUnknown, operatorread.RunEmissionAccepted, operatorread.RunEmissionNone:
		default:
			return nil, "", fmt.Errorf("stalled-instance %q has invalid emission evidence %q", instance.FlowInstance, instance.Attribution.Emission)
		}
		encoded, err := json.Marshal(instance)
		if err != nil {
			return nil, "", err
		}
		rows = append(rows, row{instance, string(encoded)})
	}
	slices.SortFunc(rows, func(a, b row) int { return strings.Compare(a.encoded, b.encoded) })
	ordered := make([]StalledInstance, 0, len(rows))
	for i, row := range rows {
		if i == 0 || rows[i-1].encoded != row.encoded {
			ordered = append(ordered, row.instance)
		}
	}
	encoded, err := json.Marshal(ordered)
	return ordered, string(encoded), err
}

// MatchesPayload shares the producer's attribution-set owner with the retained
// event reader. A timestamp or blocking headline alone cannot suppress an alert.
func (key EscalationKey) MatchesPayload(payload map[string]any) (bool, error) {
	if _, retiredScope := payload["flow_instance"]; retiredScope {
		return false, nil
	}
	if payload["run_id"] != key.RunID || payload["last_progress_at"] != LastProgressAtString(key.LastProgressAt) || payload["stalled_instances"] == nil {
		return false, nil
	}
	encoded, err := json.Marshal(payload["stalled_instances"])
	if err != nil {
		return false, fmt.Errorf("encode retained stalled-instance evidence: %w", err)
	}
	var instances []StalledInstance
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&instances); err != nil {
		return false, fmt.Errorf("decode retained stalled-instance evidence: %w", err)
	}
	_, identity, err := canonicalStalledInstances(instances)
	if err != nil {
		return false, err
	}
	return len(instances) != 0 && key.AttributionSet == identity, nil
}
