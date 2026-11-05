package identity

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type ActivityOwner struct {
	node    ExecutableNode
	agentID string
}

func NewNodeActivityOwner(node ExecutableNode) (ActivityOwner, error) {
	if !node.Valid() {
		return ActivityOwner{}, fmt.Errorf("activity node owner requires exact executable node identity")
	}
	return ActivityOwner{node: node}, nil
}

func MustNodeActivityOwner(node ExecutableNode) ActivityOwner {
	owner, err := NewNodeActivityOwner(node)
	if err != nil {
		panic(err)
	}
	return owner
}

func NewAgentActivityOwner(agentID string) (ActivityOwner, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return ActivityOwner{}, fmt.Errorf("activity agent owner requires agent identity")
	}
	return ActivityOwner{agentID: agentID}, nil
}

func MustAgentActivityOwner(agentID string) ActivityOwner {
	owner, err := NewAgentActivityOwner(agentID)
	if err != nil {
		panic(err)
	}
	return owner
}

func ParseActivityOwnerKey(raw string) (ActivityOwner, error) {
	if raw != strings.TrimSpace(raw) {
		return ActivityOwner{}, fmt.Errorf("activity owner key is not canonical")
	}
	kind, encoded, ok := strings.Cut(raw, ":")
	if !ok || encoded == "" {
		return ActivityOwner{}, fmt.Errorf("activity owner key requires kind and identity")
	}
	switch kind {
	case "node":
		node, err := ParseExecutableNodeKey(encoded)
		if err != nil {
			return ActivityOwner{}, err
		}
		return NewNodeActivityOwner(node)
	case "agent":
		decoded, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil {
			return ActivityOwner{}, fmt.Errorf("decode activity agent owner: %w", err)
		}
		owner, err := NewAgentActivityOwner(string(decoded))
		if err != nil {
			return ActivityOwner{}, err
		}
		if owner.Key() != raw {
			return ActivityOwner{}, fmt.Errorf("activity owner key is not canonical")
		}
		return owner, nil
	default:
		return ActivityOwner{}, fmt.Errorf("activity owner kind %q is unsupported", kind)
	}
}

func (o ActivityOwner) Empty() bool                  { return o.node.Empty() && o.agentID == "" }
func (o ActivityOwner) IsNode() bool                 { return o.node.Valid() && o.agentID == "" }
func (o ActivityOwner) IsAgent() bool                { return o.node.Empty() && o.agentID != "" }
func (o ActivityOwner) Node() (ExecutableNode, bool) { return o.node, o.IsNode() }
func (o ActivityOwner) AgentID() string {
	if o.IsAgent() {
		return o.agentID
	}
	return ""
}
func (o ActivityOwner) Key() string {
	if o.IsNode() {
		return "node:" + o.node.Key()
	}
	if o.IsAgent() {
		return "agent:" + base64.RawURLEncoding.EncodeToString([]byte(o.agentID))
	}
	return ""
}

type ActivityFact struct {
	RunID           string
	SourceEventID   string
	ParentEventID   string
	EntityID        string
	Owner           ActivityOwner
	ExecutionFlowID string
	HandlerEventKey string
	ActivityID      string
	Tool            string
	Attempt         int
	RevisionID      string
}

func ActivityRequestEventID(fact ActivityFact) string {
	parts := []string{
		fact.RunID, fact.SourceEventID, fact.ParentEventID, fact.EntityID,
		fact.ExecutionFlowID, fact.Owner.Key(), fact.HandlerEventKey, fact.ActivityID,
		fmt.Sprintf("%d", fact.Attempt), fact.RevisionID,
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("swarm:activity-request:"+strings.Join(parts, "\x00"))).String()
}

func ActivityResultEventID(fact ActivityFact, eventType string) string {
	parts := []string{
		fact.RunID, fact.SourceEventID, fact.ParentEventID, fact.EntityID,
		fact.ExecutionFlowID, fact.Owner.Key(), fact.HandlerEventKey, fact.ActivityID,
		fact.Tool, NormalizeEventName(eventType), fact.RevisionID,
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("swarm:activity-result:"+strings.Join(parts, "\x00"))).String()
}

func ForkLineageEventID(forkRunID, sourceEventID string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("swarm:fork-event-lineage:"+strings.TrimSpace(forkRunID)+"\x00"+strings.TrimSpace(sourceEventID))).String()
}
