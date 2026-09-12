package semanticview

import (
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

// ResolvePolicyForFlow returns the effective policy owned by the filesystem
// flow. Package requires/bind overlays no longer exist.
func ResolvePolicyForFlow(source Source, flowPath string) runtimecontracts.PolicyDocument {
	if source == nil {
		return runtimecontracts.PolicyDocument{}
	}
	return source.ResolvedPolicyForFlow(strings.TrimSpace(flowPath))
}

// ResolvePolicyForExecutableNode resolves policy through the node's owning
// flow. Authored declaration identity is the only scope input.
func ResolvePolicyForExecutableNode(source Source, node runtimeidentity.ExecutableNode) runtimecontracts.PolicyDocument {
	if source == nil || !node.Valid() {
		return runtimecontracts.PolicyDocument{}
	}
	return source.ResolvedPolicyForExecutableNode(node)
}

// CredentialStoreKeyForFlow preserves the authored logical credential key.
// Package-local remapping was retired with package.yaml.
func CredentialStoreKeyForFlow(_ Source, _ string, key string) (string, bool) {
	return strings.TrimSpace(key), false
}

func CredentialStoreKeyForActor(source Source, actorID, key string) (string, bool) {
	return CredentialStoreKeyForActorFlow(source, actorID, "", key)
}

func CredentialStoreKeyForActorFlow(_ Source, _, _, key string) (string, bool) {
	return strings.TrimSpace(key), false
}
