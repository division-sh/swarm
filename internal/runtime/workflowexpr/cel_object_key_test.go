package workflowexpr

import (
	"errors"
	"reflect"
	"testing"

	celtypes "github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

func TestProjectCELValueRejectsNonTextObjectKeys(t *testing.T) {
	for _, key := range []ref.Val{celtypes.Int(1), celtypes.Uint(1), celtypes.Double(1), celtypes.Bool(true)} {
		t.Run(key.Type().TypeName(), func(t *testing.T) {
			value := map[string]any{"items": []any{map[ref.Val]ref.Val{key: celtypes.String("non-text"), celtypes.String("1"): celtypes.String("text"), celtypes.String("true"): celtypes.String("boolean text")}}}
			_, err := ProjectCELValue(value)
			var projection *CELProjectionError
			if !errors.As(err, &projection) || projection.Path != "$.items[0].<key>" {
				t.Fatalf("non-text key %v: %v", key, err)
			}
		})
	}
}

func TestProjectCELValuePreservesExactTextObjectKeys(t *testing.T) {
	value := map[ref.Val]ref.Val{
		celtypes.String("1"): celtypes.Int(7), celtypes.String("true"): celtypes.Double(7),
		celtypes.String(" spaced "): celtypes.String("${1+1}"), celtypes.String(""): celtypes.NullValue,
	}
	got, err := ProjectCELValue(value)
	want := map[string]any{"1": int64(7), "true": float64(7), " spaced ": "${1+1}", "": nil}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("exact text keys: %#v, %v", got, err)
	}
}
