package workflowexpr

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
)

func validateAuthoredContextRoots(expression string, opts ValueExpressionOptions) error {
	if ExpressionReferencesRoot(expression, "accumulated") {
		return fmt.Errorf("accumulated is retired; read the declared state list with state.<field>.size()")
	}
	for _, root := range []string{"entity", "_entity", "gates"} {
		if ExpressionReferencesRoot(expression, root) {
			return fmt.Errorf("%s is retired; use state.<declared field> or instance.id, instance.stage, instance.path", root)
		}
	}
	if ExpressionReferencesRoot(expression, "_loop") {
		return fmt.Errorf("_loop is internal; use the authored loop.* members")
	}
	if opts.AllowJoin {
		return contracts.ValidateJoinOutcomeExpressionScope(expression)
	}
	return nil
}

// The persisted loop context has additional carriage coordinates. They are not
// authored expression members, even when the runtime map contains them.
func validateLoopAccesses(compiled *cel.Ast, opts ValueExpressionOptions) error {
	if compiled == nil || compiled.NativeRep() == nil {
		return fmt.Errorf("workflow expression AST is unavailable")
	}
	var visit func(celast.NavigableExpr) error
	visit = func(expr celast.NavigableExpr) error {
		if expr.Kind() == celast.IdentKind && expr.AsIdent() == "_loop" {
			parent, ok := expr.Parent()
			if !ok || parent.Kind() != celast.SelectKind || parent.AsSelect().Operand().ID() != expr.ID() {
				return fmt.Errorf("loop must be accessed as loop.<defined member>")
			}
			switch field := parent.AsSelect().FieldName(); field {
			case "id", "activation_id", "revision_id", "attempt", "max_attempts":
			default:
				return fmt.Errorf("unsupported loop.%s", field)
			}
		}
		for _, child := range expr.Children() {
			if err := visit(child); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(celast.NavigateAST(compiled.NativeRep()))
}
