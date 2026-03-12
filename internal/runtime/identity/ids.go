package identity

import "strings"

type EntityID string
type NodeID string
type FlowID string
type ActionKey string
type GuardKey string
type SchemaRegistryID string

func NormalizeEntityID(raw string) EntityID { return EntityID(strings.TrimSpace(raw)) }
func NormalizeNodeID(raw string) NodeID     { return NodeID(strings.TrimSpace(raw)) }
func NormalizeFlowID(raw string) FlowID     { return FlowID(strings.TrimSpace(raw)) }
func NormalizeActionKey(raw string) ActionKey {
	return ActionKey(strings.TrimSpace(raw))
}
func NormalizeGuardKey(raw string) GuardKey {
	return GuardKey(strings.TrimSpace(raw))
}
func NormalizeSchemaRegistryID(raw string) SchemaRegistryID {
	return SchemaRegistryID(strings.TrimSpace(raw))
}

func (id EntityID) String() string        { return strings.TrimSpace(string(id)) }
func (id NodeID) String() string          { return strings.TrimSpace(string(id)) }
func (id FlowID) String() string          { return strings.TrimSpace(string(id)) }
func (id ActionKey) String() string       { return strings.TrimSpace(string(id)) }
func (id GuardKey) String() string        { return strings.TrimSpace(string(id)) }
func (id SchemaRegistryID) String() string { return strings.TrimSpace(string(id)) }

func (id EntityID) IsZero() bool        { return id.String() == "" }
func (id NodeID) IsZero() bool          { return id.String() == "" }
func (id FlowID) IsZero() bool          { return id.String() == "" }
func (id ActionKey) IsZero() bool       { return id.String() == "" }
func (id GuardKey) IsZero() bool        { return id.String() == "" }
func (id SchemaRegistryID) IsZero() bool { return id.String() == "" }
