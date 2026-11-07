package effects

import "github.com/division-sh/swarm/internal/runtime/channelnative"

// The zero tag preserves the existing runtime evidence shape, never an
// implicit deployment case. Currentness is established by the store owners.
func channelTransportValid(parent channelnative.TransportParent, activationID string, activationRevision int64,
	bundleHash, bundleIdentity, inventory, runtimeID string, generation uint64) bool {
	if parent == (channelnative.TransportParent{}) {
		return validUUIDs(activationID, runtimeID) && activationRevision > 0 && generation > 0 && nonEmpty(bundleHash, bundleIdentity, inventory)
	}
	if parent.Validate() != nil {
		return false
	}
	if parent.Kind == channelnative.TransportRuntimeActivation {
		return parent.ID == activationID && parent.Revision == activationRevision &&
			validUUIDs(runtimeID) && generation > 0 && nonEmpty(bundleHash, bundleIdentity, inventory)
	}
	return activationID == "" && activationRevision == 0 && runtimeID == "" && generation == 0 &&
		bundleHash == "" && bundleIdentity == "" && inventory == ""
}

func channelTransportFence(parent channelnative.TransportParent, generation uint64) uint64 {
	if parent.Kind == channelnative.TransportDeploymentRegistration && parent.Validate() == nil {
		return uint64(parent.Revision)
	}
	return generation
}

func retainChannelTransportEvidence(evidence map[string]any, parent channelnative.TransportParent) {
	if parent != (channelnative.TransportParent{}) {
		evidence["transport_parent"] = parent
	}
	if parent.Kind == channelnative.TransportDeploymentRegistration {
		for _, key := range []string{"activation_id", "activation_revision", "bundle_hash", "bundle_identity",
			"pack_inventory_generation", "runtime_instance_id", "context_publication_generation"} {
			delete(evidence, key)
		}
	}
}
