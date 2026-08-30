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

// FlowIdentity is the canonical identity of one authored filesystem flow.
// The selected root is "."; descendants use their exact admitted relative path.
type FlowIdentity struct {
	value string
}

func AdmitFlowIdentity(raw string) (FlowIdentity, error) {
	if raw != strings.TrimSpace(raw) {
		return FlowIdentity{}, fmt.Errorf("flow path is not canonical")
	}
	value := strings.ReplaceAll(raw, "\\", "/")
	if value == "" || value == "." {
		value = "."
	} else if path.Clean(value) != value || strings.HasPrefix(value, "/") || value == ".." || strings.HasPrefix(value, "../") {
		return FlowIdentity{}, fmt.Errorf("flow path %q is not canonical", raw)
	}
	segments := strings.Split(value, "/")
	if len(segments) > 32 {
		return FlowIdentity{}, fmt.Errorf("flow path has more than 32 segments")
	}
	for _, segment := range segments {
		if segment == "." && value == "." {
			continue
		}
		if !validFlowPathSegment(segment) {
			return FlowIdentity{}, fmt.Errorf("flow path segment %q is not canonical", segment)
		}
	}
	return FlowIdentity{value: value}, nil
}

func validFlowPathSegment(value string) bool {
	if len(value) == 0 || len(value) > 100 {
		return false
	}
	first := value[0]
	if !((first >= 'a' && first <= 'z') || (first >= '0' && first <= '9')) {
		return false
	}
	for _, ch := range []byte(value) {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-' {
			continue
		}
		return false
	}
	return true
}

func (f FlowIdentity) Valid() bool {
	parsed, err := AdmitFlowIdentity(f.value)
	return err == nil && parsed.value == f.value
}

func (f FlowIdentity) String() string {
	if !f.Valid() {
		return ""
	}
	return f.value
}

func (f FlowIdentity) Equal(other FlowIdentity) bool { return f == other }

// DeclarationIdentity is the sole authored declaration identity. SemanticPath
// is family-owned and position-derived; moves and renames are delete plus add.
type DeclarationIdentity struct {
	flow         FlowIdentity
	family       string
	semanticPath string
}

type declarationIdentityWire struct {
	FlowPath     string `json:"flow_path"`
	Family       string `json:"family"`
	SemanticPath string `json:"semantic_path"`
}

func AdmitDeclarationIdentity(flowPath, family, semanticPath string) (DeclarationIdentity, error) {
	flow, err := AdmitFlowIdentity(flowPath)
	if err != nil {
		return DeclarationIdentity{}, err
	}
	if family != strings.TrimSpace(family) || !validIdentityToken(family) {
		return DeclarationIdentity{}, fmt.Errorf("declaration family %q is not canonical", family)
	}
	if semanticPath != strings.TrimSpace(semanticPath) || semanticPath == "" || strings.ContainsAny(semanticPath, "\x00\r\n") {
		return DeclarationIdentity{}, fmt.Errorf("declaration semantic path is not canonical")
	}
	return DeclarationIdentity{flow: flow, family: family, semanticPath: semanticPath}, nil
}

func validIdentityToken(value string) bool {
	if value == "" || value[0] < 'a' || value[0] > 'z' {
		return false
	}
	for _, ch := range []byte(value) {
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' {
			continue
		}
		return false
	}
	return true
}

func (d DeclarationIdentity) Valid() bool {
	parsed, err := AdmitDeclarationIdentity(d.flow.String(), d.family, d.semanticPath)
	return err == nil && parsed == d
}

func (d DeclarationIdentity) Flow() FlowIdentity                   { return d.flow }
func (d DeclarationIdentity) Family() string                       { return d.family }
func (d DeclarationIdentity) SemanticPath() string                 { return d.semanticPath }
func (d DeclarationIdentity) Equal(other DeclarationIdentity) bool { return d == other }

func (d DeclarationIdentity) Key() string {
	if !d.Valid() {
		return ""
	}
	parts := []string{d.flow.String(), d.family, d.semanticPath}
	for index := range parts {
		parts[index] = base64.RawURLEncoding.EncodeToString([]byte(parts[index]))
	}
	return strings.Join(parts, ".")
}

func ParseDeclarationIdentityKey(raw string) (DeclarationIdentity, error) {
	if raw != strings.TrimSpace(raw) {
		return DeclarationIdentity{}, fmt.Errorf("declaration identity key is not canonical")
	}
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return DeclarationIdentity{}, fmt.Errorf("declaration identity key requires flow, family, and semantic path")
	}
	decoded := make([]string, 3)
	for index, part := range parts {
		value, err := base64.RawURLEncoding.DecodeString(part)
		if err != nil {
			return DeclarationIdentity{}, fmt.Errorf("decode declaration identity coordinate %d: %w", index, err)
		}
		decoded[index] = string(value)
	}
	identity, err := AdmitDeclarationIdentity(decoded[0], decoded[1], decoded[2])
	if err != nil {
		return DeclarationIdentity{}, err
	}
	if identity.Key() != raw {
		return DeclarationIdentity{}, fmt.Errorf("declaration identity key is not canonical")
	}
	return identity, nil
}

func (d DeclarationIdentity) MarshalJSON() ([]byte, error) {
	if !d.Valid() {
		return nil, fmt.Errorf("cannot encode invalid declaration identity")
	}
	return json.Marshal(declarationIdentityWire{FlowPath: d.flow.String(), Family: d.family, SemanticPath: d.semanticPath})
}

func (d *DeclarationIdentity) UnmarshalJSON(raw []byte) error {
	if d == nil {
		return fmt.Errorf("cannot decode declaration identity into nil target")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var wire declarationIdentityWire
	if err := decoder.Decode(&wire); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("declaration identity has trailing JSON")
	}
	parsed, err := AdmitDeclarationIdentity(wire.FlowPath, wire.Family, wire.SemanticPath)
	if err != nil {
		return err
	}
	*d = parsed
	return nil
}

// ExecutableNode identifies one authored node declaration by filesystem flow
// path and local node declaration name.
type ExecutableNode struct {
	declaration DeclarationIdentity
}

func AdmitExecutableNodeDeclaration(flowPath, nodeID string) (ExecutableNode, error) {
	declaration, err := AdmitDeclarationIdentity(flowPath, "node", nodeID)
	if err != nil {
		return ExecutableNode{}, err
	}
	return ExecutableNode{declaration: declaration}, nil
}

func ParseExecutableNode(flowPath, nodeID string) (ExecutableNode, error) {
	return AdmitExecutableNodeDeclaration(flowPath, nodeID)
}

func (r ExecutableNode) Valid() bool {
	return r.declaration.Valid() && r.declaration.Family() == "node"
}
func (r ExecutableNode) Empty() bool { return r == (ExecutableNode{}) }
func (r ExecutableNode) FlowPath() string {
	if !r.Valid() {
		return ""
	}
	return r.declaration.Flow().String()
}
func (r ExecutableNode) NodeID() string {
	if !r.Valid() {
		return ""
	}
	return r.declaration.SemanticPath()
}
func (r ExecutableNode) DeclarationIdentity() DeclarationIdentity { return r.declaration }
func (r ExecutableNode) Equal(other ExecutableNode) bool          { return r == other }
func (r ExecutableNode) Key() string {
	if !r.Valid() {
		return ""
	}
	parts := []string{r.FlowPath(), r.NodeID()}
	for index := range parts {
		parts[index] = base64.RawURLEncoding.EncodeToString([]byte(parts[index]))
	}
	return strings.Join(parts, ".")
}

func ParseExecutableNodeKey(raw string) (ExecutableNode, error) {
	if raw != strings.TrimSpace(raw) {
		return ExecutableNode{}, fmt.Errorf("executable node key is not canonical")
	}
	parts := strings.Split(raw, ".")
	if len(parts) != 2 {
		return ExecutableNode{}, fmt.Errorf("executable node key requires flow and node coordinates")
	}
	decoded := make([]string, 2)
	for index, part := range parts {
		value, err := base64.RawURLEncoding.DecodeString(part)
		if err != nil {
			return ExecutableNode{}, fmt.Errorf("decode executable node key coordinate %d: %w", index, err)
		}
		decoded[index] = string(value)
	}
	ref, err := ParseExecutableNode(decoded[0], decoded[1])
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
	return json.Marshal(struct {
		FlowPath string `json:"flow_path"`
		NodeID   string `json:"node_id"`
	}{FlowPath: r.FlowPath(), NodeID: r.NodeID()})
}

func (r *ExecutableNode) UnmarshalJSON(raw []byte) error {
	if r == nil {
		return fmt.Errorf("cannot decode executable node identity into nil target")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var wire struct {
		FlowPath string `json:"flow_path"`
		NodeID   string `json:"node_id"`
	}
	if err := decoder.Decode(&wire); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("executable node identity has trailing JSON")
	}
	parsed, err := ParseExecutableNode(wire.FlowPath, wire.NodeID)
	if err != nil {
		return err
	}
	*r = parsed
	return nil
}
