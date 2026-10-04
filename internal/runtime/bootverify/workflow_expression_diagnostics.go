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
	slot := strings.TrimSuffix(strings.TrimSuffix(reader.Kind, ".cel"), ".ref")
	if reader.EmitSite != nil {
		parts := strings.SplitN(reader.Kind, " emit field ", 2)
		if len(parts) == 2 {
			slot = strings.TrimPrefix(reader.EmitSite.SiteKey, "handler.") + ".fields." + parts[1]
			slot = strings.ReplaceAll(slot, ".emit_template.", ".emit.")
		}
	} else if parts := strings.SplitN(slot, " emit field ", 2); len(parts) == 2 && parts[0] == "guard escalation" {
		slot = "guard.on_fail.escalate.fields." + parts[1]
	}
	if strings.HasPrefix(slot, "guard.checks[") && !strings.HasSuffix(slot, ".check") {
		slot += ".check"
	}
	if reader.HasRuleIndex && reader.RuleCollection == "rules" && strings.HasSuffix(slot, ".condition") {
		slot = strings.TrimSuffix(slot, ".condition") + ".when"
	}
	if bundle, ok := semanticview.Bundle(c.source); ok && bundle != nil {
		node, _ := record.Identity()
		if source, found := bundle.HandlerValueProvenance(node, event, slot); found {
			return fmt.Errorf("expression slot %s at %s:%d:%d: %w", slot, source.SourceFile, source.SourceLine, source.SourceColumn, err)
		}
	}
	return fmt.Errorf("expression slot %s in %s: %w", slot, record.Source.File, err)
}
