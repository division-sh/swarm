package containeridentity

import (
	"fmt"
	"sort"
	"strings"
)

const (
	LabelOwner          = "dev.swarm.owner"
	LabelKind           = "dev.swarm.container.kind"
	LabelResetEligible  = "dev.swarm.reset.eligible"
	LabelCreationSource = "dev.swarm.creation_source"
	LabelContainerName  = "dev.swarm.container.name"
	LabelWorkspaceScope = "dev.swarm.workspace.scope"
	LabelRunID          = "dev.swarm.run_id"
	LabelEntityID       = "dev.swarm.entity_id"
	LabelAgentID        = "dev.swarm.agent_id"
	LabelFlowInstance   = "dev.swarm.flow_instance"

	OwnerRuntime = "runtime"

	KindScaffold = "scaffold"
	KindSystem   = "system"
	KindEntity   = "entity"
	KindAgent    = "agent"
	KindFlow     = "flow"
)

type Identity struct {
	Owner          string
	Kind           string
	ResetEligible  bool
	CreationSource string
	ContainerName  string
	WorkspaceScope string
	RunID          string
	EntityID       string
	AgentID        string
	FlowInstance   string
}

func (i Identity) Normalized() Identity {
	i.Owner = strings.TrimSpace(i.Owner)
	i.Kind = strings.TrimSpace(i.Kind)
	i.CreationSource = strings.TrimSpace(i.CreationSource)
	i.ContainerName = strings.TrimSpace(i.ContainerName)
	i.WorkspaceScope = strings.TrimSpace(i.WorkspaceScope)
	i.RunID = strings.TrimSpace(i.RunID)
	i.EntityID = strings.TrimSpace(i.EntityID)
	i.AgentID = strings.TrimSpace(i.AgentID)
	i.FlowInstance = strings.Trim(strings.TrimSpace(i.FlowInstance), "/")
	return i
}

func (i Identity) Labels() map[string]string {
	i = i.Normalized()
	labels := map[string]string{
		LabelOwner:          i.Owner,
		LabelKind:           i.Kind,
		LabelResetEligible:  boolLabel(i.ResetEligible),
		LabelCreationSource: i.CreationSource,
		LabelContainerName:  i.ContainerName,
		LabelWorkspaceScope: i.WorkspaceScope,
	}
	addOptionalLabel(labels, LabelRunID, i.RunID)
	addOptionalLabel(labels, LabelEntityID, i.EntityID)
	addOptionalLabel(labels, LabelAgentID, i.AgentID)
	addOptionalLabel(labels, LabelFlowInstance, i.FlowInstance)
	for key, value := range labels {
		if strings.TrimSpace(value) == "" {
			delete(labels, key)
		}
	}
	return labels
}

func (i Identity) Validate() error {
	i = i.Normalized()
	if i.Owner != OwnerRuntime {
		return fmt.Errorf("container identity owner = %q, want %q", i.Owner, OwnerRuntime)
	}
	if !validKind(i.Kind) {
		return fmt.Errorf("container identity kind = %q, want scaffold, system, entity, agent, or flow", i.Kind)
	}
	if i.ContainerName == "" {
		return fmt.Errorf("container identity container name is required")
	}
	if i.ResetEligible && !ResetEligibleKind(i.Kind) {
		return fmt.Errorf("container kind %q cannot be reset eligible", i.Kind)
	}
	switch i.Kind {
	case KindEntity:
		if i.EntityID == "" {
			return fmt.Errorf("entity container identity requires entity_id")
		}
	case KindAgent:
		if i.AgentID == "" {
			return fmt.Errorf("agent container identity requires agent_id")
		}
	case KindFlow:
		if i.FlowInstance == "" {
			return fmt.Errorf("flow container identity requires flow_instance")
		}
	}
	return nil
}

func (i Identity) ResetEligibleManaged() bool {
	i = i.Normalized()
	return i.Owner == OwnerRuntime && i.ResetEligible && ResetEligibleKind(i.Kind)
}

func ResetEligibleKind(kind string) bool {
	switch strings.TrimSpace(kind) {
	case KindEntity, KindAgent, KindFlow:
		return true
	default:
		return false
	}
}

func FromLabels(labels map[string]string) (Identity, bool, error) {
	if len(labels) == 0 {
		return Identity{}, false, nil
	}
	owner := strings.TrimSpace(labels[LabelOwner])
	if owner == "" {
		return Identity{}, false, nil
	}
	resetEligible, err := parseBoolLabel(labels[LabelResetEligible])
	if err != nil {
		return Identity{}, true, err
	}
	identity := Identity{
		Owner:          owner,
		Kind:           labels[LabelKind],
		ResetEligible:  resetEligible,
		CreationSource: labels[LabelCreationSource],
		ContainerName:  labels[LabelContainerName],
		WorkspaceScope: labels[LabelWorkspaceScope],
		RunID:          labels[LabelRunID],
		EntityID:       labels[LabelEntityID],
		AgentID:        labels[LabelAgentID],
		FlowInstance:   labels[LabelFlowInstance],
	}.Normalized()
	if err := identity.Validate(); err != nil {
		return Identity{}, true, err
	}
	return identity, true, nil
}

func DockerCreateLabelArgs(labels map[string]string) []string {
	keys := make([]string, 0, len(labels))
	for key, value := range labels {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	args := make([]string, 0, len(keys)*2)
	for _, key := range keys {
		args = append(args, "--label", key+"="+strings.TrimSpace(labels[key]))
	}
	return args
}

func boolLabel(value bool) string {
	if value {
		return "true"
	}
	return "false"
}

func parseBoolLabel(value string) (bool, error) {
	switch strings.TrimSpace(value) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("container identity reset eligibility label must be true or false")
	}
}

func validKind(kind string) bool {
	switch strings.TrimSpace(kind) {
	case KindScaffold, KindSystem, KindEntity, KindAgent, KindFlow:
		return true
	default:
		return false
	}
}

func addOptionalLabel(labels map[string]string, key, value string) {
	value = strings.TrimSpace(value)
	if value != "" {
		labels[key] = value
	}
}
