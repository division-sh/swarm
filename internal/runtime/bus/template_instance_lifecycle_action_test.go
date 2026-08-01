package bus

import (
	"reflect"
	"testing"
)

func TestTemplateInstanceLifecycleActionRemainsClosedAndNonString(t *testing.T) {
	if kind := reflect.TypeOf(TemplateInstanceLifecycleAction(0)).Kind(); kind == reflect.String {
		t.Fatal("TemplateInstanceLifecycleAction regressed to free string")
	}
	want := []string{"", "created", "would_create", "reused", "selected_existing"}
	for value, expected := range want {
		if got := TemplateInstanceLifecycleAction(value).String(); got != expected {
			t.Fatalf("TemplateInstanceLifecycleAction(%d).String() = %q, want %q", value, got, expected)
		}
	}
	if got := TemplateInstanceLifecycleAction(len(want)).String(); got != "" {
		t.Fatalf("out-of-range lifecycle action rendered %q, want fail-closed empty", got)
	}
}
