package activityidentity

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/google/uuid"
)

type Owner struct {
	node    runtimeidentity.ExecutableNode
	agentID string
}

func NewNodeOwner(node runtimeidentity.ExecutableNode) (Owner, error) {
	if !node.Valid() {
		return Owner{}, fmt.Errorf("activity node owner requires exact executable node identity")
	}
	return Owner{node: node}, nil
}

func MustNodeOwner(node runtimeidentity.ExecutableNode) Owner {
	owner, err := NewNodeOwner(node)
	if err != nil {
		panic(err)
	}
	return owner
}

func NewAgentOwner(agentID string) (Owner, error) {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return Owner{}, fmt.Errorf("activity agent owner requires agent identity")
	}
	return Owner{agentID: agentID}, nil
}

func MustAgentOwner(agentID string) Owner {
	owner, err := NewAgentOwner(agentID)
	if err != nil {
		panic(err)
	}
	return owner
}

func ParseOwnerKey(raw string) (Owner, error) {
	if raw != strings.TrimSpace(raw) {
		return Owner{}, fmt.Errorf("activity owner key is not canonical")
	}
	kind, encoded, ok := strings.Cut(raw, ":")
	if !ok || encoded == "" {
		return Owner{}, fmt.Errorf("activity owner key requires kind and identity")
	}
	switch kind {
	case "node":
		node, err := runtimeidentity.ParseExecutableNodeKey(encoded)
		if err != nil {
			return Owner{}, err
		}
		return NewNodeOwner(node)
	case "agent":
		decoded, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil {
			return Owner{}, fmt.Errorf("decode activity agent owner: %w", err)
		}
		owner, err := NewAgentOwner(string(decoded))
		if err != nil {
			return Owner{}, err
		}
		if owner.Key() != raw {
			return Owner{}, fmt.Errorf("activity owner key is not canonical")
		}
		return owner, nil
	default:
		return Owner{}, fmt.Errorf("activity owner kind %q is unsupported", kind)
	}
}

func (o Owner) Empty() bool                                  { return o.node.Empty() && o.agentID == "" }
func (o Owner) IsNode() bool                                 { return o.node.Valid() && o.agentID == "" }
func (o Owner) IsAgent() bool                                { return o.node.Empty() && o.agentID != "" }
func (o Owner) Node() (runtimeidentity.ExecutableNode, bool) { return o.node, o.IsNode() }
func (o Owner) AgentID() string {
	if o.IsAgent() {
		return o.agentID
	}
	return ""
}
func (o Owner) Key() string {
	if o.IsNode() {
		return "node:" + o.node.Key()
	}
	if o.IsAgent() {
		return "agent:" + base64.RawURLEncoding.EncodeToString([]byte(o.agentID))
	}
	return ""
}

type Fact struct {
	RunID           string
	SourceEventID   string
	ParentEventID   string
	EntityID        string
	Owner           Owner
	ExecutionFlowID string
	HandlerEventKey string
	ActivityID      string
	Tool            string
	Attempt         int
	RevisionID      string
}

func RequestEventID(fact Fact) string {
	parts := []string{
		fact.RunID, fact.SourceEventID, fact.ParentEventID, fact.EntityID,
		fact.ExecutionFlowID, fact.Owner.Key(), fact.HandlerEventKey, fact.ActivityID,
		fmt.Sprintf("%d", fact.Attempt), fact.RevisionID,
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("swarm:activity-request:"+strings.Join(parts, "\x00"))).String()
}

func ResultEventID(fact Fact, eventType string) string {
	parts := []string{
		fact.RunID, fact.SourceEventID, fact.ParentEventID, fact.EntityID,
		fact.ExecutionFlowID, fact.Owner.Key(), fact.HandlerEventKey, fact.ActivityID,
		fact.Tool, eventidentity.Normalize(eventType), fact.RevisionID,
	}
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("swarm:activity-result:"+strings.Join(parts, "\x00"))).String()
}

func ForkLineageEventID(forkRunID, sourceEventID string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("swarm:fork-event-lineage:"+strings.TrimSpace(forkRunID)+"\x00"+strings.TrimSpace(sourceEventID))).String()
}
