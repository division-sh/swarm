package semanticview

import (
	"strings"
)

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
