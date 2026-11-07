package authoractivity

import (
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/channelnative"
)

// DeploymentEffectParent is the closed shared discriminator. Bundle absence is
// not evidence; callers must supply the original journal authority evidence.
func DeploymentEffectParent(kind, id string, evidence []byte) (*channelnative.TransportParent, error) {
	if len(evidence) == 0 {
		return nil, nil
	}
	if _, err := canonicaljson.Decode(evidence); err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(evidence, &fields); err != nil {
		return nil, err
	}
	raw, present := fields["transport_parent"]
	if !present {
		return nil, nil
	}
	parent, err := channelnative.DecodeTransportParent(raw)
	if err != nil {
		return nil, err
	}
	if parent.Kind == channelnative.TransportRuntimeActivation {
		return nil, nil
	}
	switch kind {
	case "channel_confirmation":
	case "serve_registration", "channel_delivery", "channel_action_ack", "channel_native_setting":
		var storedKind, storedID string
		if json.Unmarshal(fields["authority_kind"], &storedKind) != nil || storedKind != kind ||
			json.Unmarshal(fields["authority_id"], &storedID) != nil || storedID != id {
			return nil, fmt.Errorf("deployment effect contradicts original authority")
		}
		for _, field := range []string{"activation_id", "activation_revision", "bundle_hash", "bundle_identity", "pack_inventory_generation", "runtime_instance_id", "context_publication_generation"} {
			if _, exists := fields[field]; exists {
				return nil, fmt.Errorf("deployment effect contains authored coordinate %s", field)
			}
		}
		return &parent, nil
	default:
		return nil, fmt.Errorf("deployment effect kind %q is not admitted", kind)
	}
	var operationID, parentID string
	var storedKind, storedID string
	var revision int64
	if json.Unmarshal(fields["authority_kind"], &storedKind) != nil || storedKind != kind ||
		json.Unmarshal(fields["authority_id"], &storedID) != nil || storedID != id ||
		json.Unmarshal(fields["effect_operation_id"], &operationID) != nil || operationID != id ||
		json.Unmarshal(fields["onboarding_operation_id"], &parentID) != nil || parentID != parent.ID ||
		json.Unmarshal(fields["onboarding_revision"], &revision) != nil || revision != parent.Revision {
		return nil, fmt.Errorf("deployment effect contradicts its exact parent")
	}
	for _, field := range []string{"activation_id", "activation_revision", "bundle_hash", "bundle_identity", "pack_inventory_generation", "runtime_instance_id", "context_publication_generation", "plan_generation"} {
		if _, exists := fields[field]; exists {
			return nil, fmt.Errorf("deployment effect contains authored coordinate %s", field)
		}
	}
	return &parent, nil
}
