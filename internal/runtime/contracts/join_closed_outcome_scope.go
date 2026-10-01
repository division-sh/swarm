package contracts

import (
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/parser/gen"
)

// ValidateJoinClosedOutcomeScope excludes transport context from both outcomes.
// The arrival output selector is deliberately outside this closed scope.
func ValidateJoinClosedOutcomeScope(handler SystemNodeEventHandler) error {
	if handler.Join == nil {
		return nil
	}
	for _, outcome := range []struct {
		name string
		rule HandlerRuleEntry
	}{{"on_complete", handler.Join.OnComplete}, {"on_deadline", handler.Join.OnDeadline}} {
		if err := validateJoinClosedRule(outcome.name, outcome.rule); err != nil {
			return err
		}
	}
	return nil
}

func validateJoinClosedRule(name string, rule HandlerRuleEntry) error {
	if rule.Emit.From != "" && strings.TrimSpace(rule.Emit.From) != EmitFromEntity {
		return fmt.Errorf("join.%s.emit.from may only select typed entity state", name)
	}
	for _, field := range sortedContractKeys(rule.Emit.Fields) {
		if err := validateJoinOutcomeValue(name, "emit.fields."+field, rule.Emit.Fields[field]); err != nil {
			return err
		}
	}
	for index, write := range rule.DataAccumulation.Writes {
		if err := validateJoinClosedWrite(name, index, write); err != nil {
			return err
		}
	}
	return nil
}

func validateJoinClosedWrite(name string, index int, write WorkflowDataWrite) error {
	prefix := fmt.Sprintf("data_accumulation.writes[%d]", index)
	if write.Value.IsZero() && write.Operation != WorkflowDataOperationClear {
		if source := strings.TrimSpace(write.Source()); source != "" {
			if !paths.Parse(source).HasExplicitRoot() {
				source = "payload." + source
			}
			if err := validateJoinOutcomeValue(name, prefix+".source", RefExpression(source)); err != nil {
				return err
			}
		}
	}
	for _, field := range []struct {
		name  string
		value ExpressionValue
	}{{"value", write.Value}, {"key", write.Key}, {"index", write.Index}} {
		if err := validateJoinOutcomeValue(name, prefix+"."+field.name, field.value); err != nil {
			return err
		}
	}
	return nil
}

func validateJoinOutcomeValue(outcome, name string, value ExpressionValue) error {
	var source string
	switch value.Kind {
	case ExpressionKindRef:
		source = value.Ref
	case ExpressionKindCEL:
		source = value.CEL
	default:
		return nil
	}
	if err := ValidateJoinOutcomeExpressionScope(source); err != nil {
		return fmt.Errorf("join.%s.%s: %w", outcome, name, err)
	}
	return nil
}

// ValidateJoinOutcomeExpressionScope checks free context identifiers, including
// whole-object reads. CEL locals and field names are not transport roots.
func ValidateJoinOutcomeExpressionScope(expression string) error {
	env, err := cel.NewEnv(cel.OptionalTypes())
	if err != nil {
		return err
	}
	parsed, issues := env.Parse(joinScopeLoopSource(expression))
	if issues != nil && issues.Err() != nil {
		return issues.Err()
	}
	return validateJoinOutcomeExpr(celast.NavigateAST(parsed.NativeRep()), nil)
}

func validateJoinOutcomeExpr(expr celast.NavigableExpr, bound map[string]bool) error {
	if expr.Kind() == celast.IdentKind && !bound[expr.AsIdent()] {
		switch root := expr.AsIdent(); root {
		case "payload", "event", "policy", "computed", "fan_out", "accumulated", "_entity", "metadata", "gates", "_loop":
			return fmt.Errorf("closed join outcome may not reference %s; only typed state, join, and captured loop context are available", root)
		}
	}
	if expr.Kind() == celast.ComprehensionKind {
		return validateJoinOutcomeComprehension(expr, bound)
	}
	for _, child := range expr.Children() {
		if err := validateJoinOutcomeExpr(child, bound); err != nil {
			return err
		}
	}
	return nil
}

func validateJoinOutcomeComprehension(expr celast.NavigableExpr, bound map[string]bool) error {
	c := expr.AsComprehension()
	children := expr.Children()
	locals := make(map[string]bool, len(bound)+3)
	for name, value := range bound {
		locals[name] = value
	}
	locals[c.IterVar()], locals[c.IterVar2()], locals[c.AccuVar()] = true, true, true
	for _, child := range children {
		scope := bound
		if child.ID() == c.LoopCondition().ID() || child.ID() == c.LoopStep().ID() {
			scope = locals
		} else if child.ID() == c.Result().ID() {
			scope = make(map[string]bool, len(bound)+1)
			for name, value := range bound {
				scope[name] = value
			}
			scope[c.AccuVar()] = true
		}
		if err := validateJoinOutcomeExpr(child, scope); err != nil {
			return err
		}
	}
	return nil
}

// CEL reserves loop. Rewrite tokens for this scope-only parse without exposing
// the engine's internal _loop spelling or changing strings/field names.
func joinScopeLoopSource(source string) string {
	runes := []rune(source)
	lexer := gen.NewCELLexer(antlr.NewInputStream(source))
	var out strings.Builder
	offset, previous := 0, 0
	for token := lexer.NextToken(); token.GetTokenType() != antlr.TokenEOF; token = lexer.NextToken() {
		if token.GetChannel() != antlr.TokenDefaultChannel {
			continue
		}
		if token.GetText() == "loop" && previous != gen.CELLexerDOT {
			out.WriteString(string(runes[offset:token.GetStart()]))
			out.WriteString("__swarm_join_scope_loop")
			offset = token.GetStop() + 1
		}
		previous = token.GetTokenType()
	}
	out.WriteString(string(runes[offset:]))
	return out.String()
}
