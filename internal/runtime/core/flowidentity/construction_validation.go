package flowidentity

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

func (i Instance) ValidateAgentExecution(source semanticview.Source, agent agentidentity.Identity, flowID, entityID string) error {
	if err := agent.Validate(); err != nil {
		return err
	}
	if err := i.ValidateConstruction(source, agent.RunID); err != nil {
		return err
	}
	route, err := i.Route().AgentIdentityRoute()
	if err != nil || agent.Route != route || flowID != i.TemplateID {
		return fmt.Errorf("agent execution route conflicts with its exact constructed flow")
	}
	if entityID != "" && entityID != i.EntityID {
		return fmt.Errorf("agent execution entity conflicts with its exact constructed flow")
	}
	return nil
}

// ValidateConstruction consumes the constructor's identity, including its
// parent context. Parent context is never connection or recipient authority.
func (i Instance) ValidateConstruction(source semanticview.Source, runID string) error {
	if source == nil || runID == "" || !i.HasStoredPath || !i.Route().Valid() || i.EntityID == "" {
		return fmt.Errorf("construction identity requires its exact source, run and instance")
	}
	schema, found := source.FlowSchemaByID(i.TemplateID)
	if !found || i.ScopeKey != ScopeKey(source, i.TemplateID) {
		return fmt.Errorf("construction identity has an unknown or inconsistent authored owner")
	}
	expected := Derive(source, i.TemplateID, i.InstanceID)
	if i.TemplateID == semanticview.RootExecutionFlowID(source) {
		expected = Stored(source, i.TemplateID, runID, runID, EntityID(runID), "")
	} else {
		parent := i.ParentRoute
		if !parent.Complete() || parent != parent.Normalized() || parent.EntityID != i.ParentEntityID || parent.EntityID != EntityID(parent.FlowInstance) {
			return fmt.Errorf("construction identity requires its exact structural parent")
		}
		if parent.FlowID == semanticview.RootExecutionFlowID(source) && parent.FlowInstance != runID {
			return fmt.Errorf("construction identity has a foreign run root")
		}
		parentInstance := Stored(source, parent.FlowID, parent.FlowInstance, LogicalInstanceID(parent.FlowInstance), parent.EntityID, "")
		var err error
		if schema.Instance.Empty() {
			expected, err = KeylessChild(source, parentInstance, i.TemplateID)
		} else {
			expected, err = KeyedChild(source, parentInstance, i.TemplateID, i.InstanceID)
		}
		if err != nil {
			return err
		}
		// Keyed admission owns the stored entity fact. It is not derived again
		// from a route during restoration or publication.
		if !schema.Instance.Empty() {
			expected.EntityID = i.EntityID
		}
	}
	if expected != i {
		return fmt.Errorf("construction identity disagrees with its canonical constructor")
	}
	return nil
}
