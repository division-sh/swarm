package tools

import (
	"context"
	"testing"
)

func TestRegistry_RegisterGetExecute(t *testing.T) {
	r := NewRegistry()
	r.Register("echo", func(ctx context.Context, input any) (any, error) {
		_ = ctx
		return input, nil
	})
	if _, ok := r.Get("missing"); ok {
		t.Fatal("expected missing tool")
	}
	out, err := r.Execute(context.Background(), "echo", map[string]any{"a": 1})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	m, ok := out.(map[string]any)
	if !ok || m["a"].(int) != 1 {
		t.Fatalf("unexpected output: %#v", out)
	}
	if _, err := r.Execute(context.Background(), "missing", nil); err == nil {
		t.Fatal("expected error for missing tool")
	}
}

