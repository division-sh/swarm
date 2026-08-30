package agenttopology

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type SourceSetOperation string

const (
	OperationInstallCompleteSourceSet      SourceSetOperation = "install_complete_source_set"
	OperationRestoreSourceSet              SourceSetOperation = "restore_source_set"
	OperationRemoveBundleSource            SourceSetOperation = "remove_bundle_source"
	OperationApplyDestructiveResetTopology SourceSetOperation = "apply_destructive_reset_topology"
)

type SourceSetCommitRequest struct {
	Operation        SourceSetOperation `json:"operation"`
	OperationID      string             `json:"operation_id"`
	ExpectedRevision string             `json:"expected_revision,omitempty"`
	Plan             SourceSetPlan      `json:"plan"`
	RemovedSource    *SourceCoordinate  `json:"removed_source,omitempty"`
}

func (r SourceSetCommitRequest) Validate() error {
	switch r.Operation {
	case OperationInstallCompleteSourceSet, OperationRestoreSourceSet,
		OperationRemoveBundleSource, OperationApplyDestructiveResetTopology:
	default:
		return fmt.Errorf("agent topology source-set operation %q is invalid", r.Operation)
	}
	if _, err := uuid.Parse(strings.TrimSpace(r.OperationID)); err != nil {
		return fmt.Errorf("agent topology source-set operation_id is invalid: %w", err)
	}
	if err := r.Plan.Validate(); err != nil {
		return err
	}
	switch r.Operation {
	case OperationInstallCompleteSourceSet:
		if strings.TrimSpace(r.ExpectedRevision) != "" || r.RemovedSource != nil {
			return errors.New("initial source-set installation cannot carry predecessor or removed-source facts")
		}
	case OperationRemoveBundleSource:
		if strings.TrimSpace(r.ExpectedRevision) == "" || r.RemovedSource == nil {
			return errors.New("bundle-source removal requires expected revision and exact removed source")
		}
		if err := r.RemovedSource.Validate(); err != nil {
			return err
		}
	default:
		if strings.TrimSpace(r.ExpectedRevision) == "" || r.RemovedSource != nil {
			return fmt.Errorf("source-set operation %q requires one expected revision and no removed-source fact", r.Operation)
		}
	}
	return nil
}

type AgentChangeKind string

const (
	AgentAdded   AgentChangeKind = "added"
	AgentChanged AgentChangeKind = "changed"
	AgentRemoved AgentChangeKind = "removed"
)

type DesiredAgentChange struct {
	Kind     AgentChangeKind `json:"kind"`
	Previous *DesiredAgent   `json:"previous,omitempty"`
	Current  *DesiredAgent   `json:"current,omitempty"`
}

type SourceSetCommitResult struct {
	Operation        SourceSetOperation   `json:"operation"`
	OperationID      string               `json:"operation_id"`
	PreviousRevision string               `json:"previous_revision,omitempty"`
	CurrentRevision  string               `json:"current_revision"`
	Changes          []DesiredAgentChange `json:"changes"`
	Replayed         bool                 `json:"replayed"`
}

func Diff(previous, current SourceSetPlan) ([]DesiredAgentChange, error) {
	if err := previous.Validate(); err != nil {
		return nil, fmt.Errorf("previous source-set plan: %w", err)
	}
	if err := current.Validate(); err != nil {
		return nil, fmt.Errorf("current source-set plan: %w", err)
	}
	before := make(map[string]DesiredAgent, len(previous.Agents))
	after := make(map[string]DesiredAgent, len(current.Agents))
	for _, agent := range previous.Agents {
		key, err := agent.Key()
		if err != nil {
			return nil, err
		}
		before[key] = agent
	}
	for _, agent := range current.Agents {
		key, err := agent.Key()
		if err != nil {
			return nil, err
		}
		after[key] = agent
	}
	changes := make([]DesiredAgentChange, 0)
	for _, agent := range previous.Agents {
		key, _ := agent.Key()
		next, ok := after[key]
		if !ok {
			prior := agent
			changes = append(changes, DesiredAgentChange{Kind: AgentRemoved, Previous: &prior})
			continue
		}
		if agent.ConfigRevision != next.ConfigRevision || agent.Source != next.Source {
			prior, current := agent, next
			changes = append(changes, DesiredAgentChange{Kind: AgentChanged, Previous: &prior, Current: &current})
		}
	}
	for _, agent := range current.Agents {
		key, _ := agent.Key()
		if _, ok := before[key]; ok {
			continue
		}
		added := agent
		changes = append(changes, DesiredAgentChange{Kind: AgentAdded, Current: &added})
	}
	return changes, nil
}
