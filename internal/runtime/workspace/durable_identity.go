package workspace

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"strings"

	"github.com/division-sh/swarm/internal/sourceartifact"
)

type durableWorkspaceKind string

type DurableBackingKind = durableWorkspaceKind

const (
	durableWorkspaceScaffold      durableWorkspaceKind = "scaffold"
	durableWorkspaceSystem        durableWorkspaceKind = "system"
	durableWorkspaceSystemEntity  durableWorkspaceKind = "system-entities"
	durableWorkspaceSystemNginx   durableWorkspaceKind = "system-nginx"
	durableWorkspaceSystemSystemd durableWorkspaceKind = "system-systemd"
	durableWorkspaceAgent         durableWorkspaceKind = "agent"
	durableWorkspaceFlow          durableWorkspaceKind = "flow"
)

const (
	DurableBackingScaffold      = durableWorkspaceScaffold
	DurableBackingSystem        = durableWorkspaceSystem
	DurableBackingSystemEntity  = durableWorkspaceSystemEntity
	DurableBackingSystemNginx   = durableWorkspaceSystemNginx
	DurableBackingSystemSystemd = durableWorkspaceSystemSystemd
	DurableBackingAgent         = durableWorkspaceAgent
	DurableBackingFlow          = durableWorkspaceFlow
)

const durableWorkspaceKeyDomain = "swarm-durable-workspace-v1"

func durableBundleScopeKey(bundleHash string) (string, error) {
	if err := sourceartifact.ValidateHash(bundleHash); err != nil {
		return "", fmt.Errorf("durable workspace bundle identity: %w", err)
	}
	return "bundle-" + strings.TrimPrefix(bundleHash, sourceartifact.HashPrefix), nil
}

func durableWorkspaceBackingKey(bundleHash string, kind durableWorkspaceKind, semanticIdentity string) (string, error) {
	if err := sourceartifact.ValidateHash(bundleHash); err != nil {
		return "", fmt.Errorf("durable workspace backing identity: %w", err)
	}
	requiresIdentity, err := validateDurableWorkspaceKind(kind)
	if err != nil {
		return "", err
	}
	if requiresIdentity && semanticIdentity == "" {
		return "", fmt.Errorf("durable %s workspace semantic identity is required", kind)
	}
	if !requiresIdentity && semanticIdentity != "" {
		return "", fmt.Errorf("durable %s workspace does not accept a semantic identity", kind)
	}

	digest := sha256.New()
	writeDurableIdentityField(digest, durableWorkspaceKeyDomain)
	writeDurableIdentityField(digest, bundleHash)
	writeDurableIdentityField(digest, string(kind))
	writeDurableIdentityField(digest, semanticIdentity)
	return durableWorkspaceKeyDomain + "-" + string(kind) + "-" + hex.EncodeToString(digest.Sum(nil)), nil
}

func DurableBackingKey(bundleHash string, kind DurableBackingKind, semanticIdentity string) (string, error) {
	return durableWorkspaceBackingKey(bundleHash, kind, semanticIdentity)
}

func validateDurableWorkspaceKind(kind durableWorkspaceKind) (bool, error) {
	switch kind {
	case durableWorkspaceAgent, durableWorkspaceFlow:
		return true, nil
	case durableWorkspaceScaffold, durableWorkspaceSystem, durableWorkspaceSystemEntity, durableWorkspaceSystemNginx, durableWorkspaceSystemSystemd:
		return false, nil
	default:
		return false, fmt.Errorf("durable workspace kind %q is not supported", kind)
	}
}

func writeDurableIdentityField(dst hash.Hash, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = dst.Write(length[:])
	_, _ = dst.Write([]byte(value))
}

func processBundleScopeKey(bundleHash string) (string, error) {
	if err := sourceartifact.ValidateHash(bundleHash); err != nil {
		return "", fmt.Errorf("workspace process bundle identity: %w", err)
	}
	digest := strings.TrimPrefix(bundleHash, sourceartifact.HashPrefix)
	return "bundle-" + digest[:12], nil
}

func projectionScopeKey(bundleHash, projectionID string) (string, error) {
	bundle, err := processBundleScopeKey(bundleHash)
	if err != nil {
		return "", err
	}
	projectionID = strings.TrimPrefix(strings.TrimSpace(projectionID), "runtime-projection-v1:")
	if len(projectionID) < 12 {
		return "", fmt.Errorf("runtime source projection identity is invalid")
	}
	return bundle + "-projection-" + projectionID[:12], nil
}
