package entityquery

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/core/paths"
	"github.com/division-sh/swarm/internal/runtime/entityruntime"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

type Reader interface {
	CountWorkflowEntities(context.Context, Request) (int, error)
}

type Predicate struct {
	Field string
	Op    string
	Value any
}

type Request struct {
	RunID     string
	Source    semanticview.Source
	Contract  entityruntime.Contract
	Predicate Predicate
}

func (r Request) Validate() error {
	if strings.TrimSpace(r.RunID) == "" {
		return fmt.Errorf("workflow entity query requires run_id")
	}
	if r.Source == nil {
		return fmt.Errorf("workflow entity query requires semantic source")
	}
	if strings.TrimSpace(r.Contract.FlowID) == "" {
		return fmt.Errorf("workflow entity query requires flow contract")
	}
	if strings.TrimSpace(r.Predicate.Field) == "" {
		return fmt.Errorf("workflow entity query requires predicate field")
	}
	if r.Predicate.Value == nil {
		return fmt.Errorf("workflow entity query requires a non-null predicate value")
	}
	switch strings.TrimSpace(r.Predicate.Op) {
	case "==", "!=", ">=", "<=", ">", "<":
		return nil
	default:
		return fmt.Errorf("unsupported query_entities operator %q", r.Predicate.Op)
	}
}

func Matches(row map[string]any, predicate Predicate) bool {
	left, present := selectorValue(row, predicate.Field)
	if !present || predicate.Value == nil {
		return false
	}
	switch predicate.Op {
	case "==":
		return jsonValuesEqual(left, predicate.Value)
	case "!=":
		return !jsonValuesEqual(left, predicate.Value)
	}
	leftNumber, leftIsNumber := numericValue(left)
	rightNumber, rightIsNumber := numericValue(predicate.Value)
	if leftIsNumber && rightIsNumber {
		switch predicate.Op {
		case ">=":
			return leftNumber >= rightNumber
		case "<=":
			return leftNumber <= rightNumber
		case ">":
			return leftNumber > rightNumber
		case "<":
			return leftNumber < rightNumber
		}
	}
	leftText := fmt.Sprintf("%v", left)
	rightText := fmt.Sprintf("%v", predicate.Value)
	switch predicate.Op {
	case ">=":
		return leftText >= rightText
	case "<=":
		return leftText <= rightText
	case ">":
		return leftText > rightText
	case "<":
		return leftText < rightText
	default:
		return false
	}
}

func selectorValue(row map[string]any, field string) (any, bool) {
	field = strings.TrimSpace(field)
	if field == "" {
		return nil, false
	}
	if value, ok := row[field]; ok {
		return value, true
	}
	fields, _ := row["fields"].(map[string]any)
	parsed := paths.Parse(field)
	if parsed.HasExplicitRoot() {
		parsed = paths.Path{Segments: parsed.Segments}
	}
	if len(parsed.Segments) == 0 {
		return nil, false
	}
	current := any(fields)
	for _, segment := range parsed.Segments {
		object, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		value, ok := object[strings.TrimSpace(segment)]
		if !ok {
			return nil, false
		}
		current = value
	}
	return current, true
}

func numericValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float32:
		return float64(typed), true
	case float64:
		return typed, true
	default:
		return 0, false
	}
}

func jsonValuesEqual(left, right any) bool {
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && string(leftJSON) == string(rightJSON)
}
