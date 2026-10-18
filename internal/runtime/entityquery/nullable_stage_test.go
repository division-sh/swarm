package entityquery

import (
	"errors"
	"testing"
)

func TestQ10HandlerEntityQueryNullableStage(t *testing.T) {
	for _, op := range []string{"==", "!=", ">", ">=", "<", "<="} {
		t.Run(op, func(t *testing.T) {
			matched, err := Matches(map[string]any{"current_state": nil, "fields": map[string]any{"current_state": "unknown"}}, Predicate{Field: "current_state", Op: op, Value: "unknown"})
			if op == "==" || op == "!=" {
				if err != nil || matched != (op == "!=") {
					t.Fatalf("nullable comparison: match=%t err=%v", matched, err)
				}
				return
			}
			var comparison *NullComparisonError
			if matched || !errors.As(err, &comparison) || comparison.Field != "current_state" || comparison.Op != op {
				t.Fatalf("ordered null comparison was stringified or swallowed: match=%t err=%v", matched, err)
			}
		})
	}
	row := map[string]any{"current_state": "unknown", "fields": map[string]any{"current_state": "business"}}
	for field, value := range map[string]string{"current_state": "unknown", "entity.current_state": "business"} {
		matched, err := Matches(row, Predicate{Field: field, Op: "==", Value: value})
		if err != nil || !matched {
			t.Fatalf("selector category %s lost: match=%t err=%v", field, matched, err)
		}
	}
}
