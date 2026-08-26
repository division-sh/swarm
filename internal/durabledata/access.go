package durabledata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
)

type ResourceAccessStore interface {
	LoadRunResourceAccess(context.Context, string, []DeclarationRef) ([]ResourceAccessItem, error)
}

const AccessSchemaVersion = "swarm.data.access.v1"

type StaticDataRef struct {
	BundleHash          string `json:"bundle_hash"`
	CanonicalInputLabel string `json:"canonical_input_label"`
}

func NewStaticDataID(ref StaticDataRef) (StaticDataID, error) {
	if strings.TrimSpace(ref.BundleHash) == "" || strings.TrimSpace(ref.CanonicalInputLabel) == "" {
		return "", fmt.Errorf("static data identity requires bundle_hash and canonical_input_label")
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("swarm.data.static.identity.v1"))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(ref.BundleHash))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(ref.CanonicalInputLabel))
	return StaticDataID("static-data-v1:sha256:" + hex.EncodeToString(hash.Sum(nil))), nil
}

func StaticContentDigest(content []byte) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("swarm.data.static.content.v1"))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(content)
	return "static-content-v1:sha256:" + hex.EncodeToString(hash.Sum(nil))
}

func StaticMountPath(id StaticDataID) (string, error) {
	if err := id.Validate(); err != nil {
		return "", err
	}
	hexPart := strings.TrimPrefix(string(id), "static-data-v1:sha256:")
	return "/data/.swarm/static/s_" + hexPart + ".data", nil
}

func ResourceMountPath(declaration DeclarationRef) (string, error) {
	if err := declaration.Validate(); err != nil {
		return "", err
	}
	coordinate, err := canonicaljson.Bytes(declaration)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("swarm.data.mount.resource.v1"))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(coordinate)
	segment := "r_" + hex.EncodeToString(hash.Sum(nil))
	return "/data/.swarm/resources/" + segment + ".jsonl", nil
}

type StaticAccessItem struct {
	Kind          string        `json:"kind"`
	StaticID      StaticDataID  `json:"static_id"`
	StaticRef     StaticDataRef `json:"static_ref"`
	PackageKey    string        `json:"package_key"`
	OwnerFlowID   string        `json:"owner_flow_id"`
	RelativePath  string        `json:"relative_path"`
	ContentDigest string        `json:"content_digest"`
	SizeBytes     int           `json:"size_bytes"`
	ContentType   string        `json:"content_type"`
	MountPath     string        `json:"mount_path"`
	Content       []byte        `json:"-"`
}

type ResourceAccessItem struct {
	Kind         string         `json:"kind"`
	Declaration  DeclarationRef `json:"declaration"`
	VersionID    VersionID      `json:"version_id"`
	SchemaDigest SchemaDigest   `json:"schema_digest"`
	RowCount     uint64         `json:"row_count"`
	MountPath    string         `json:"mount_path"`
	BusinessKey  string         `json:"-"`
	Schema       []byte         `json:"-"`
	Content      []byte         `json:"-"`
}

type AccessItem struct {
	Kind     string              `json:"kind"`
	Static   *StaticAccessItem   `json:"static_file,omitempty"`
	Resource *ResourceAccessItem `json:"resource,omitempty"`
}

type AccessList struct {
	SchemaVersion string                 `json:"schema_version"`
	RunID         string                 `json:"run_id"`
	AgentIdentity agentidentity.Identity `json:"agent_identity"`
	Items         []AccessItem           `json:"items"`
}
