package contracts

import (
	"fmt"
	"maps"
	"slices"

	"github.com/division-sh/swarm/internal/runtime/core/toolidentity"
	"github.com/division-sh/swarm/internal/runtime/flowmodel"
)

// Declaration IDs belong to the binding, not the keyless tool value.
func (e ToolSchemaEntry) ValidateDeclarationName(name string) error {
	if !e.AgentExposable() && (name == "" || toolidentity.CanonicalName(name) != name || toolidentity.IsEmitToolName(name)) {
		return fmt.Errorf("private tool declaration %q must use a canonical, non-emit ID; choose a stable private ID instead of an alias, runtime-tools wrapper or reserved emit route", name)
	}
	return nil
}

func ValidateToolDeclarationNames(entries map[string]ToolSchemaEntry) error {
	for _, name := range slices.Sorted(maps.Keys(entries)) {
		if err := entries[name].ValidateDeclarationName(name); err != nil {
			return err
		}
	}
	return nil
}

func (b *WorkflowContractBundle) ValidateToolDeclarationNames() error {
	if b == nil {
		return nil
	}
	if err := ValidateToolDeclarationNames(b.Tools); err != nil {
		return err
	}
	var invalid error
	flowmodel.Walk(b.FlowTree.Root, flowViewChildren, func(view *FlowContractView) {
		if invalid == nil {
			if err := ValidateToolDeclarationNames(view.Tools); err != nil {
				invalid = fmt.Errorf("tool declarations in package %q, flow %q: %w", view.Path, view.Paths.FlowPath, err)
			}
		}
	})
	if invalid != nil {
		return invalid
	}
	for _, flow := range slices.Sorted(maps.Keys(b.activityToolsByFlow)) {
		entries := b.activityToolsByFlow[flow]
		if err := ValidateToolDeclarationNames(entries); err != nil {
			return fmt.Errorf("compiled activity tools in flow %q: %w", flow, err)
		}
	}
	return nil
}
