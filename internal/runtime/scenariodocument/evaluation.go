package scenariodocument

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/workflowexpr"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/uuid"
)

// Materialized values are execution data: Evaluate never scans their contents.
// The private projection preserves CEL numeric kinds; every exit is defensive.
type Materialized struct{ value any }

func Materialize(value any) (Materialized, error) {
	projected, err := workflowexpr.ProjectCELValue(value)
	if err != nil {
		return Materialized{}, err
	}
	closed, err := canonicaljson.CloneRuntimeValue(projected)
	if err != nil {
		return Materialized{}, err
	}
	return Materialized{value: closed}, nil
}

func (v Materialized) Interface() (any, error) { return canonicaljson.CloneRuntimeValue(v.value) }

type Evaluator struct {
	env  *cel.Env
	seed string
	vars map[string]any
}

func Seed(label, name, seed string) string {
	return "scenario-v1\x00path=" + label + "\x00name=" + strings.TrimSpace(name) + "\x00seed=" + strings.TrimSpace(seed)
}

func NewEvaluator(seed string, rawVars map[string]any) (*Evaluator, error) {
	e := &Evaluator{seed: seed, vars: map[string]any{}}
	env, err := cel.NewEnv(
		cel.Variable("vars", cel.DynType),
		cel.Function("scenario.sha40",
			cel.Overload("scenario_sha40_string", []*cel.Type{cel.StringType}, cel.StringType,
				cel.UnaryBinding(func(value ref.Val) ref.Val { return types.String(SHA40(fmt.Sprint(value.Value()))) }))),
		cel.Function("scenario.uuid",
			cel.Overload("scenario_uuid_string", []*cel.Type{cel.StringType}, cel.StringType,
				cel.UnaryBinding(func(value ref.Val) ref.Val { return types.String(UUID(seed, fmt.Sprint(value.Value()))) }))),
	)
	if err != nil {
		return nil, err
	}
	e.env = env
	for _, key := range sortedKeys(rawVars) {
		value, err := e.Evaluate(rawVars[key])
		if err != nil {
			return nil, fmt.Errorf("vars.%s: %w", key, err)
		}
		e.vars[key] = value
	}
	return e, nil
}

func (e *Evaluator) Seed() string { return e.seed }

func (e *Evaluator) Evaluate(value any) (any, error) {
	value, err := e.evaluate(value)
	if err != nil {
		return nil, err
	}
	data, err := Materialize(value)
	if err != nil {
		return nil, err
	}
	return data.Interface()
}

func (e *Evaluator) evaluate(value any) (any, error) {
	switch typed := value.(type) {
	case Materialized:
		return typed.Interface()
	case string:
		if IsExpression(typed) {
			return e.EvaluateExpression(strings.TrimSpace(typed[2 : len(typed)-1]))
		}
		return typed, nil
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			var err error
			out[i], err = e.evaluate(item)
			if err != nil {
				return nil, fmt.Errorf("[%d]: %w", i, err)
			}
		}
		return out, nil
	case map[string]any:
		out := make(map[string]any, len(typed))
		for _, key := range sortedKeys(typed) {
			item, err := e.evaluate(typed[key])
			if err != nil {
				return nil, fmt.Errorf("%s: %w", key, err)
			}
			out[key] = item
		}
		return out, nil
	default:
		return workflowexpr.ProjectCELValue(value)
	}
}

func (e *Evaluator) EvaluateExpression(expression string) (any, error) {
	if expression == "" {
		return nil, fmt.Errorf("CEL expression is empty")
	}
	ast, issues := e.env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	program, err := e.env.Program(ast)
	if err != nil {
		return nil, err
	}
	out, _, err := program.Eval(map[string]any{"vars": e.vars})
	if err != nil {
		return nil, err
	}
	data, err := Materialize(out)
	if err != nil {
		return nil, err
	}
	return data.Interface()
}

func SHA40(value string) string {
	sum := sha1.Sum([]byte(value))
	return hex.EncodeToString(sum[:])
}
func UUID(seed, label string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(seed+"\x00"+label)).String()
}

func sortedKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
