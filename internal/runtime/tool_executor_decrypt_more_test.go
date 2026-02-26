package runtime

import (
	"context"
	"testing"
)

func TestRuntimeToolExecutor_DecryptCredentialValue_DBNilBranches(t *testing.T) {
	ex := NewRuntimeToolExecutor(NewEventBus(InMemoryEventStore{}), nil, nil)
	ctx := context.Background()

	// Non-encrypted values pass through.
	if got := ex.decryptCredentialValue(ctx, "plain"); got.(string) != "plain" {
		t.Fatalf("expected pass-through, got %#v", got)
	}

	// Encrypted values with key but without DB should remain unchanged.
	t.Setenv("EMPIREAI_CREDENTIALS_KEY", "k")
	if got := ex.decryptCredentialValue(ctx, "enc::AAAA"); got.(string) != "enc::AAAA" {
		t.Fatalf("expected encrypted passthrough when db missing, got %#v", got)
	}

	// Recursive forms should not panic and should preserve values.
	m := map[string]any{"a": "enc::AAAA", "b": []any{"enc::AAAA", "x"}, "c": 1}
	out := ex.decryptCredentialValue(ctx, m).(map[string]any)
	if out["a"].(string) != "enc::AAAA" {
		t.Fatalf("unexpected map decrypt: %#v", out)
	}
	if out["c"].(int) != 1 {
		t.Fatalf("unexpected passthrough: %#v", out)
	}
}

