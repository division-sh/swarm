package contracts

import (
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
)

func validateExplicitScopedPath(raw, context string) error {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parsed := paths.Parse(raw)
	if !parsed.HasExplicitRoot() {
		return fmt.Errorf("%s %q must use an explicit scope", context, raw)
	}
	return nil
}

func validateExpressionValue(expr ExpressionValue) error {
	if expr.IsZero() {
		return nil
	}
	expr.hydrate()
	switch expr.Kind {
	case ExpressionKindLiteral:
		return nil
	case ExpressionKindRef:
		if strings.TrimSpace(expr.Ref) == "" {
			return fmt.Errorf("ref expression requires a ref value")
		}
		return validateExplicitScopedPath(expr.Ref, "ref expression")
	case ExpressionKindCEL:
		if strings.TrimSpace(expr.CEL) == "" {
			return fmt.Errorf("cel expression requires a cel value")
		}
		return nil
	default:
		return fmt.Errorf("unsupported expression kind %q", expr.Kind)
	}
}

func hydrateWorkflowDataWrite(w *WorkflowDataWrite) error {
	if w == nil {
		return nil
	}
	w.Field = strings.TrimSpace(w.Field)
	w.SourceField = strings.TrimSpace(w.SourceField)
	w.Operation = WorkflowDataOperation(strings.TrimSpace(string(w.Operation)))
	w.TargetRef = strings.TrimSpace(w.TargetRef)
	w.TargetField = strings.TrimSpace(w.TargetField)
	w.TargetPathRef = strings.TrimSpace(w.TargetPathRef)
	w.Value.hydrate()
	w.Key.hydrate()
	w.Index.hydrate()
	if err := validateExpressionValue(w.Value); err != nil {
		return err
	}
	if err := validateExpressionValue(w.Key); err != nil {
		return err
	}
	if err := validateExpressionValue(w.Index); err != nil {
		return err
	}
	if w.Operation != "" {
		if err := hydrateWorkflowDataOperation(w); err != nil {
			return err
		}
		w.SourcePath = paths.Parse(w.Source())
		w.TargetPath = paths.Parse(w.Target())
		return nil
	}
	if w.TargetRef != "" || !w.Key.IsZero() || !w.Index.IsZero() {
		return fmt.Errorf("workflow data write target/key/index forms require op")
	}
	if w.TargetField != "" && w.TargetPathRef != "" {
		return fmt.Errorf("workflow data write must not declare both target_field and target_path")
	}
	switch {
	case w.Field != "":
		if w.SourceField != "" || w.TargetField != "" || w.TargetPathRef != "" || !w.Value.IsZero() {
			return fmt.Errorf("workflow data write %q must use bare direct form only", w.Field)
		}
	case w.SourceField != "":
		if (w.TargetField == "" && w.TargetPathRef == "") || !w.Value.IsZero() {
			return fmt.Errorf("workflow data write with source_field %q must also declare target_field or target_path and no value/expression", w.SourceField)
		}
		if paths.Parse(w.SourceField).HasExplicitRoot() {
			return fmt.Errorf("workflow data source_field %q must be payload-local, not scoped", w.SourceField)
		}
	case !w.Value.IsZero():
		if w.TargetField == "" && w.TargetPathRef == "" {
			return fmt.Errorf("workflow data write with value/expression must declare target_field or target_path")
		}
		if w.Value.HasRefValue() {
			return fmt.Errorf("workflow data write %q cannot use ref expressions", w.Target())
		}
	default:
		return fmt.Errorf("workflow data write must use one canonical form")
	}
	w.SourcePath = paths.Parse(w.Source())
	w.TargetPath = paths.Parse(w.Target())
	return nil
}

func hydrateWorkflowDataOperation(w *WorkflowDataWrite) error {
	switch w.Operation {
	case WorkflowDataOperationSet, WorkflowDataOperationMerge, WorkflowDataOperationDelete, WorkflowDataOperationAppend, WorkflowDataOperationUpdate, WorkflowDataOperationClear:
	default:
		return fmt.Errorf("unsupported workflow data write op %q", strings.TrimSpace(string(w.Operation)))
	}
	if w.TargetRef == "" {
		return fmt.Errorf("workflow data write op %q requires target", w.Operation)
	}
	if w.Field != "" || w.SourceField != "" || w.TargetField != "" || w.TargetPathRef != "" {
		return fmt.Errorf("workflow data write op %q must use target, not field/source_field/target_field/target_path", w.Operation)
	}
	if strings.TrimSpace(w.TargetRef) != "" && paths.Parse(w.TargetRef).Root != paths.RootEntity {
		return fmt.Errorf("workflow data write op %q target %q must use entity scope", w.Operation, w.TargetRef)
	}
	switch w.Operation {
	case WorkflowDataOperationClear:
		if !w.Value.IsZero() || !w.Key.IsZero() || !w.Index.IsZero() {
			return fmt.Errorf("workflow data write op clear must not declare value, key or index")
		}
	case WorkflowDataOperationSet, WorkflowDataOperationMerge, WorkflowDataOperationAppend:
		if w.Value.IsZero() {
			return fmt.Errorf("workflow data write op %q requires value", w.Operation)
		}
	case WorkflowDataOperationDelete:
		if w.Key.IsZero() {
			return fmt.Errorf("workflow data write op delete requires key")
		}
		if !w.Value.IsZero() || !w.Index.IsZero() {
			return fmt.Errorf("workflow data write op delete must not declare value or index")
		}
	case WorkflowDataOperationUpdate:
		if w.Value.IsZero() || w.Index.IsZero() {
			return fmt.Errorf("workflow data write op update requires value and index")
		}
	}
	switch w.Operation {
	case WorkflowDataOperationSet, WorkflowDataOperationMerge:
		if w.Key.IsZero() {
			return fmt.Errorf("workflow data write op %q requires key", w.Operation)
		}
		if !w.Index.IsZero() {
			return fmt.Errorf("workflow data write op %q must not declare index", w.Operation)
		}
	case WorkflowDataOperationAppend:
		if !w.Index.IsZero() {
			return fmt.Errorf("workflow data write op append must not declare index")
		}
	}
	return nil
}
