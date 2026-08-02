package bus

import (
	"fmt"
	"reflect"
	"testing"
)

func TestTemplateInstanceLifecycleActionRemainsClosedAndNonString(t *testing.T) {
	if kind := reflect.TypeOf(TemplateInstanceLifecycleAction(0)).Kind(); kind == reflect.String {
		t.Fatal("TemplateInstanceLifecycleAction regressed to free string")
	}
	want := []string{"", "created", "would_create", "reused", "selected_existing"}
	for value, expected := range want {
		if got := templateInstanceLifecycleActionCode(TemplateInstanceLifecycleAction(value)); got != expected {
			t.Fatalf("templateInstanceLifecycleActionCode(%d) = %q, want %q", value, got, expected)
		}
	}
	if got := templateInstanceLifecycleActionCode(TemplateInstanceLifecycleAction(len(want))); got != "" {
		t.Fatalf("out-of-range lifecycle action rendered %q, want fail-closed empty", got)
	}
	if _, ok := any(TemplateInstanceLifecycleAction(0)).(fmt.Stringer); ok {
		t.Fatal("TemplateInstanceLifecycleAction must not implement fmt.Stringer")
	}
}
