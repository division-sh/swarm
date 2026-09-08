package engine

import (
	"fmt"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"strings"
)

// This isolated fixture is not a runtime writer; production uses Executor.applyDataAccumulation.
func applyDataAccumulationToState(base BaseContext, state ExecutionState, snapshot *StateSnapshot, spec runtimecontracts.WorkflowDataAccumulation, options workflowexpr.ValueExpressionOptions) error {
	if snapshot == nil || len(spec.Writes) == 0 {
		return nil
	}
	if snapshot.StateCarrier.Fields == nil {
		snapshot.StateCarrier.Fields = map[string]any{}
	}
	for _, write := range spec.Writes {
		if write.IsContainedOperation() {
			return fmt.Errorf("data_accumulation target %s: contained operations require semantic source validation", strings.TrimSpace(write.Target()))
		}
		target := strings.TrimSpace(write.Target())
		if target == "" {
			continue
		}
		parsed := paths.Parse(target)
		switch parsed.Root {
		case paths.RootEntity, paths.RootMetadata:
			parsed = paths.Path{Segments: parsed.Segments}
		case paths.RootUnknown:
		default:
			return fmt.Errorf("data_accumulation target %s: unsupported target scope", strings.TrimSpace(write.Target()))
		}
		switch {
		case write.Value.HasLiteralValue():
			setParsedValuePath(snapshot.StateCarrier.Fields, parsed, write.Value.Literal)
		case write.Value.HasCELValue():
			value, err := evalWorkflowValueExpression(base, state, write.Value.CEL, options)
			if err != nil {
				return fmt.Errorf("data_accumulation target %s: %w", strings.TrimSpace(write.Target()), err)
			}
			setParsedValuePath(snapshot.StateCarrier.Fields, parsed, value)
		default:
			source := strings.TrimSpace(write.Source())
			if source == "" {
				continue
			}
			if value, ok := lookupPath(cloneStringAnyMap(base.Payload.Raw()), source); ok {
				setParsedValuePath(snapshot.StateCarrier.Fields, parsed, value)
			}
		}
	}
	if sourceEvent := strings.TrimSpace(spec.SourceEvent); sourceEvent != "" {
		snapshot.SetBookkeeping("last_data_accumulation_source", sourceEvent)
	}
	return nil
}
