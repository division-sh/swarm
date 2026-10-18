package bootverify

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Error presentation looks up positions only in the exact admitted source.
// Programmatic contracts may have no retained source.
func (c *checkerContext) authoredExpressionError(record contracts.ScopedNodeRecord, event string, reader expressionReference, err error) error {
	slot := reader.SourceSlot
	if strings.HasPrefix(slot, "guard.checks[") && !strings.HasSuffix(slot, ".check") {
		slot += ".check"
	}
	if bundle, ok := semanticview.Bundle(c.source); ok && bundle != nil && slot != "" {
		node, _ := record.Identity()
		path := contracts.NodeProvenanceMapPath("", node.NodeID()) + "." + contracts.NodeProvenanceMapPath("event_handlers", event) + "." + slot
		if source, found := bundle.IntroductionLocation(record.Source.File, path, false); found {
			return fmt.Errorf("expression slot %s at %s:%d:%d: %w", slot, source.File, source.Line, source.Column, err)
		}
	}
	if slot == "" {
		slot = reader.Kind
	}
	return fmt.Errorf("expression slot %s in %s: %w", slot, record.Source.File, err)
}

func (c *checkerContext) authoredGateContextError(plan contracts.WorkflowGatePlan, field string, err error) error {
	slot := "stages." + plan.Stage + ".gate.context." + field
	if bundle, ok := semanticview.Bundle(c.source); ok && bundle != nil {
		if view, ok := bundle.FlowViewByID(plan.FlowID); ok {
			path := contracts.NodeProvenanceMapPath("stages", plan.Stage) + ".gate." + contracts.NodeProvenanceMapPath("context", field)
			if source, found := bundle.IntroductionLocation(view.Paths.SchemaFile, path, false); found {
				return fmt.Errorf("expression slot %s at %s:%d:%d: %w", slot, source.File, source.Line, source.Column, err)
			}
		}
	}
	return fmt.Errorf("expression slot %s: %w", slot, err)
}
