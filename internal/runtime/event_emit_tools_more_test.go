package runtime

import "testing"

func TestSchemaHelperPrimitives(t *testing.T) {
	if !isNumeric(1) || !isNumeric(1.5) || !isNumeric(uint32(3)) {
		t.Fatal("expected numeric primitives to be accepted")
	}
	if isNumeric("1") {
		t.Fatal("string should not be treated as numeric primitive")
	}

	if !isInteger(1) || !isInteger(int64(2)) || !isInteger(2.0) {
		t.Fatal("expected integer values to pass integer check")
	}
	if isInteger(2.5) || isInteger("2") {
		t.Fatal("expected non-integer values to fail integer check")
	}

	if got := requiredList([]any{"a", " ", "b"}); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("requiredList []any mismatch: %+v", got)
	}
	if got := requiredList([]string{"x", "y"}); len(got) != 2 {
		t.Fatalf("requiredList []string mismatch: %+v", got)
	}
	if got := requiredList("x"); got != nil {
		t.Fatalf("requiredList default should return nil, got %+v", got)
	}
}
