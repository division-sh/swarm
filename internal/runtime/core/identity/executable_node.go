package identity

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strings"
)

const RootPackageKey = "."

// ExecutableNode identifies one authored node declaration. Node IDs are local
// to their package and owning flow; none of the three coordinates may be
// interpreted independently after admission.
type ExecutableNode struct {
	packageKey string
	flowID     string
	nodeID     string
}

type executableNodeWire struct {
	PackageKey string `json:"package_key"`
	FlowID     string `json:"flow_id"`
	NodeID     string `json:"node_id"`
}

// AdmitExecutableNodeDeclaration is the declaration-origin constructor. The
// contracts owner calls it after preserving the exact authored scope.
func AdmitExecutableNodeDeclaration(packageKey, flowID, nodeID string) (ExecutableNode, error) {
	packageKey, err := normalizePackageKey(packageKey)
	if err != nil {
		return ExecutableNode{}, err
	}
	ref := ExecutableNode{
		packageKey: packageKey,
		flowID:     strings.TrimSpace(flowID),
		nodeID:     strings.TrimSpace(nodeID),
	}
	if !ref.Valid() {
		return ExecutableNode{}, fmt.Errorf("executable node declaration requires package and local node identity")
	}
	return ref, nil
}

// ParseExecutableNode is the strict hydration boundary for persisted and wire
// identity. It rejects noncanonical input rather than silently normalizing it.
func ParseExecutableNode(packageKey, flowID, nodeID string) (ExecutableNode, error) {
	ref, err := AdmitExecutableNodeDeclaration(packageKey, flowID, nodeID)
	if err != nil {
		return ExecutableNode{}, err
	}
	if packageKey != ref.packageKey || flowID != ref.flowID || nodeID != ref.nodeID {
		return ExecutableNode{}, fmt.Errorf("executable node identity is not canonical")
	}
	return ref, nil
}

func normalizePackageKey(raw string) (string, error) {
	cleaned := strings.Trim(path.Clean(strings.ReplaceAll(strings.TrimSpace(raw), "\\", "/")), "/")
	if cleaned == "" || cleaned == "." {
		return RootPackageKey, nil
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("executable node package key %q escapes the package root", raw)
	}
	return cleaned, nil
}

func (r ExecutableNode) Valid() bool {
	packageKey, err := normalizePackageKey(r.packageKey)
	return err == nil && packageKey == r.packageKey &&
		strings.TrimSpace(r.flowID) == r.flowID &&
		strings.TrimSpace(r.nodeID) == r.nodeID && r.nodeID != ""
}

func (r ExecutableNode) Empty() bool { return r == (ExecutableNode{}) }

func (r ExecutableNode) PackageKey() string { return r.packageKey }
func (r ExecutableNode) FlowID() string     { return r.flowID }
func (r ExecutableNode) NodeID() string     { return r.nodeID }

func (r ExecutableNode) Equal(other ExecutableNode) bool { return r == other }

func (r ExecutableNode) Key() string {
	if !r.Valid() {
		return ""
	}
	parts := []string{r.packageKey, r.flowID, r.nodeID}
	for i := range parts {
		parts[i] = base64.RawURLEncoding.EncodeToString([]byte(parts[i]))
	}
	return strings.Join(parts, ".")
}

// ParseExecutableNodeKey hydrates the canonical compact form used by durable
// indexes. It rejects every alternate spelling and partial coordinate.
func ParseExecutableNodeKey(raw string) (ExecutableNode, error) {
	if raw != strings.TrimSpace(raw) {
		return ExecutableNode{}, fmt.Errorf("executable node key is not canonical")
	}
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return ExecutableNode{}, fmt.Errorf("executable node key requires package, flow, and node coordinates")
	}
	decoded := make([]string, len(parts))
	for i, part := range parts {
		value, err := base64.RawURLEncoding.DecodeString(part)
		if err != nil {
			return ExecutableNode{}, fmt.Errorf("decode executable node key coordinate %d: %w", i, err)
		}
		decoded[i] = string(value)
	}
	ref, err := ParseExecutableNode(decoded[0], decoded[1], decoded[2])
	if err != nil {
		return ExecutableNode{}, err
	}
	if ref.Key() != raw {
		return ExecutableNode{}, fmt.Errorf("executable node key is not canonical")
	}
	return ref, nil
}

func (r ExecutableNode) MarshalJSON() ([]byte, error) {
	if !r.Valid() {
		return nil, fmt.Errorf("cannot encode invalid executable node identity")
	}
	return json.Marshal(executableNodeWire{PackageKey: r.packageKey, FlowID: r.flowID, NodeID: r.nodeID})
}

func (r *ExecutableNode) UnmarshalJSON(raw []byte) error {
	if r == nil {
		return fmt.Errorf("cannot decode executable node identity into nil target")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return fmt.Errorf("executable node identity must be an object")
	}
	for _, required := range []string{"package_key", "flow_id", "node_id"} {
		if _, ok := fields[required]; !ok {
			return fmt.Errorf("executable node identity requires %s", required)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var wire executableNodeWire
	if err := decoder.Decode(&wire); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("executable node identity has trailing JSON")
	}
	parsed, err := ParseExecutableNode(wire.PackageKey, wire.FlowID, wire.NodeID)
	if err != nil {
		return err
	}
	*r = parsed
	return nil
}
