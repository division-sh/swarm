package bootverify

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

// Error presentation consumes the admitted provenance ledger, not source bytes
// or another expression parser. Programmatic contracts may have no source fact.
func (c *checkerContext) authoredExpressionError(record contracts.ScopedNodeRecord, event string, reader expressionReference, err error) error {
	slot := strings.TrimSuffix(strings.TrimSuffix(reader.SourceSlot, ".cel"), ".ref")
	if strings.HasPrefix(slot, "guard.checks[") && !strings.HasSuffix(slot, ".check") {
		slot += ".check"
	}
	if reader.HasRuleIndex && reader.RuleCollection == "rules" && strings.HasSuffix(slot, ".condition") {
		slot = strings.TrimSuffix(slot, ".condition") + ".when"
	}
	if bundle, ok := semanticview.Bundle(c.source); ok && bundle != nil && slot != "" {
		node, _ := record.Identity()
		if source, found := bundle.HandlerValueProvenance(node, event, slot); found {
			return fmt.Errorf("expression slot %s at %s:%d:%d: %w", slot, source.SourceFile, source.SourceLine, source.SourceColumn, err)
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
		if source, found := bundle.GateContextProvenance(plan.FlowID, plan.Stage, field); found {
			return fmt.Errorf("expression slot %s at %s:%d:%d: %w", slot, source.SourceFile, source.SourceLine, source.SourceColumn, err)
		}
	}
	return fmt.Errorf("expression slot %s: %w", slot, err)
}
