package testsql

import (
	"strings"
	"testing"
)

type EventCorruptionClaim struct {
	Invariant string
	Reason    string
}

func RequireEventCorruptionClaim(t testing.TB, claim EventCorruptionClaim) {
	t.Helper()
	if strings.TrimSpace(claim.Invariant) == "" || strings.TrimSpace(claim.Reason) == "" {
		t.Fatal("unsafe event corruption requires an invariant and reason")
	}
}

func RequireEventCorruptionResult(t testing.TB, claim EventCorruptionClaim, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("corrupt event store for %s (%s): %v", claim.Invariant, claim.Reason, err)
	}
}

// Inspect the real native insertion error; this assertion executes no SQL.
func RequireEventStoreCorruptionCategory(
	t testing.TB,
	claim EventCorruptionClaim,
	category string,
	err error,
) {
	t.Helper()
	if err == nil {
		t.Fatalf("event store accepted corruption for %s (%s)", claim.Invariant, claim.Reason)
	}
	message := strings.ToLower(err.Error())
	switch category {
	case "not_null":
		if !strings.Contains(message, "not null") && !strings.Contains(message, "not-null") {
			t.Fatalf("event store rejected %s for unrelated category: %v", claim.Reason, err)
		}
	case "check":
		if !strings.Contains(message, "check constraint") && !strings.Contains(message, "constraint failed") {
			t.Fatalf("event store rejected %s for unrelated category: %v", claim.Reason, err)
		}
	default:
		t.Fatalf("unknown event corruption error category %q", category)
	}
}
