package workflowexpr

import (
	"sort"
	"strings"

	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
)

type entityExpressionAccess struct {
	path     string
	required bool
}

// Entity lifecycle checks consume CEL's stock AST, including expanded macro
// bindings. A nested optional selection never invents a trailing-dot field.
func entityExpressionAccesses(expression string) []entityExpressionAccess {
	return entityExpressionAccessesWithFacts(expression, nil)
}

func entityExpressionAccessesWithFacts(expression string, initialFacts workflowPresenceFacts) []entityExpressionAccess {
	return entityExpressionAccessesAtStage(expression, initialFacts, nil)
}

func entityExpressionAccessesAtStage(expression string, initialFacts workflowPresenceFacts, stage *string) []entityExpressionAccess {
	env, err := cel.NewEnv(cel.OptionalTypes())
	if err != nil {
		return nil
	}
	parsed, issues := env.Parse(RewriteLoopRoot(expression))
	if issues != nil && issues.Err() != nil {
		return nil
	}
	if parsed == nil || parsed.NativeRep() == nil {
		return nil
	}
	var accesses []entityExpressionAccess
	var visit func(celast.Expr, workflowPresenceFacts, bool)
	visit = func(expr celast.Expr, facts workflowPresenceFacts, shadowed bool) {
		if expr == nil {
			return
		}
		if !shadowed {
			if expr.Kind() == celast.IdentKind && expr.AsIdent() == "entity" {
				accesses = append(accesses, entityExpressionAccess{})
			}
			if path, ok := workflowSelectionPath(expr); ok && strings.HasPrefix(path, "entity.") {
				required := true
				if expr.Kind() == celast.SelectKind && expr.AsSelect().IsTestOnly() {
					required = false
				}
				if expr.Kind() == celast.CallKind && expr.AsCall().FunctionName() == "_?._" {
					required = false
				}
				if _, present := facts[path]; present {
					required = false
				}
				accesses = append(accesses, entityExpressionAccess{strings.TrimPrefix(path, "entity."), required})
			}
		}
		a := workflowOptionalReadAnalyzer{}
		switch expr.Kind() {
		case celast.SelectKind:
			visit(expr.AsSelect().Operand(), facts, shadowed)
		case celast.CallKind:
			call := expr.AsCall()
			args := call.Args()
			switch call.FunctionName() {
			case "_&&_", "_||_":
				if len(args) == 2 {
					visit(args[0], facts, shadowed)
					truth := call.FunctionName() == "_&&_"
					if stage == nil || stageConditionOutcomes(args[0], *stage)&stageConditionBool(truth) != 0 {
						visit(args[1], mergeWorkflowPresenceFacts(facts, a.factsWhen(args[0], truth)), shadowed)
					}
					return
				}
			case "_?_:_":
				if len(args) == 3 {
					visit(args[0], facts, shadowed)
					if stage == nil || stageConditionOutcomes(args[0], *stage)&stageConditionTrue != 0 {
						visit(args[1], mergeWorkflowPresenceFacts(facts, a.factsWhen(args[0], true)), shadowed)
					}
					if stage == nil || stageConditionOutcomes(args[0], *stage)&stageConditionFalse != 0 {
						visit(args[2], mergeWorkflowPresenceFacts(facts, a.factsWhen(args[0], false)), shadowed)
					}
					return
				}
			}
			if call.IsMemberFunction() {
				visit(call.Target(), facts, shadowed)
			}
			for _, arg := range args {
				visit(arg, facts, shadowed)
			}
		case celast.ComprehensionKind:
			c := expr.AsComprehension()
			visit(c.IterRange(), facts, shadowed)
			visit(c.AccuInit(), facts, shadowed)
			bodyFacts := withoutWorkflowShadowedFacts(facts, c.IterVar(), c.IterVar2(), c.AccuVar())
			bodyShadow := shadowed || c.IterVar() == "entity" || c.IterVar2() == "entity" || c.AccuVar() == "entity"
			visit(c.LoopCondition(), bodyFacts, bodyShadow)
			visit(c.LoopStep(), bodyFacts, bodyShadow)
			visit(c.Result(), withoutWorkflowShadowedFacts(facts, c.AccuVar()), shadowed || c.AccuVar() == "entity")
		case celast.ListKind:
			for _, e := range expr.AsList().Elements() {
				visit(e, facts, shadowed)
			}
		case celast.MapKind:
			for _, e := range expr.AsMap().Entries() {
				visit(e.AsMapEntry().Key(), facts, shadowed)
				visit(e.AsMapEntry().Value(), facts, shadowed)
			}
		case celast.StructKind:
			for _, e := range expr.AsStruct().Fields() {
				visit(e.AsStructField().Value(), facts, shadowed)
			}
		}
	}
	visit(parsed.NativeRep().Expr(), initialFacts, false)
	return accesses
}

// RequiredEntityReferences shares the same stock-CEL scope/presence walk as
// runtime missing-value admission. Facts are exact paths at this program point.
func RequiredEntityReferences(expression string, present []string) []string {
	return requiredEntityReferencesAtStage(expression, present, nil)
}

// RequiredEntityReferencesAtStage is for checked guards/conditions before any
// transition. Later readers must not reuse the handler-entry stage as current.
func RequiredEntityReferencesAtStage(expression string, present []string, stage string) []string {
	return requiredEntityReferencesAtStage(expression, present, &stage)
}

func requiredEntityReferencesAtStage(expression string, present []string, stage *string) []string {
	facts := workflowPresenceFacts{}
	for _, path := range present {
		facts[path] = struct{}{}
	}
	missing := map[string]struct{}{}
	for _, access := range entityExpressionAccessesAtStage(expression, facts, stage) {
		if access.required && access.path != "" {
			missing[access.path] = struct{}{}
		}
	}
	out := make([]string, 0, len(missing))
	for path := range missing {
		out = append(out, path)
	}
	sort.Strings(out)
	return out
}

// ConditionPresenceFacts exposes only the stock presence analyzer's bounded
// boolean facts; it does not infer values from arbitrary business predicates.
func ConditionPresenceFacts(expression string, truth bool) []string {
	env, err := cel.NewEnv(cel.OptionalTypes())
	if err != nil {
		return nil
	}
	parsed, issues := env.Parse(RewriteLoopRoot(expression))
	if issues != nil && issues.Err() != nil || parsed == nil || parsed.NativeRep() == nil {
		return nil
	}
	facts := (workflowOptionalReadAnalyzer{}).factsWhen(parsed.NativeRep().Expr(), truth)
	out := make([]string, 0, len(facts))
	for path := range facts {
		out = append(out, path)
	}
	sort.Strings(out)
	return out
}

func EntityReferences(expression string) []string {
	accesses := entityExpressionAccesses(expression)
	var out []string
	seen := map[string]bool{}
	for _, access := range accesses {
		if access.path == "" {
			continue
		}
		if seen[access.path] {
			continue
		}
		seen[access.path] = true
		prefix := false
		for _, other := range accesses {
			if strings.HasPrefix(other.path, access.path+".") {
				prefix = true
				break
			}
		}
		if !prefix {
			out = append(out, access.path)
		}
	}
	return out
}

func MissingEntityReferences(expression string, entity map[string]any) []string {
	missing := map[string]struct{}{}
	for _, access := range entityExpressionAccesses(expression) {
		if !access.required {
			continue
		}
		if _, ok := lookupPath(entity, access.path); !ok {
			missing["entity."+access.path] = struct{}{}
		}
	}
	out := make([]string, 0, len(missing))
	for path := range missing {
		out = append(out, path)
	}
	sort.Strings(out)
	return out
}
