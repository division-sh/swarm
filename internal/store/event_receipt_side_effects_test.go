package store

import (
	"strings"
	"testing"

	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
)

func TestAgentReceiptSideEffects_RoundTrip(t *testing.T) {
	raw, err := marshalAgentReceiptSideEffects(newAgentReceiptSideEffects(
		runtimemanager.ReceiptStatusDeadLetter,
		"cancelled_by_kill_previous",
		2,
		"boom",
	))
	if err != nil {
		t.Fatalf("marshalAgentReceiptSideEffects: %v", err)
	}
	got, err := decodeAgentReceiptSideEffects(raw)
	if err != nil {
		t.Fatalf("decodeAgentReceiptSideEffects: %v", err)
	}
	if got.ManagerStatus != runtimemanager.ReceiptStatusDeadLetter {
		t.Fatalf("ManagerStatus = %q, want dead_letter", got.ManagerStatus)
	}
	if got.ReasonCode != "cancelled_by_kill_previous" {
		t.Fatalf("ReasonCode = %q", got.ReasonCode)
	}
	if got.RetryCount != 2 {
		t.Fatalf("RetryCount = %d, want 2", got.RetryCount)
	}
	if got.Error != "boom" {
		t.Fatalf("Error = %q, want boom", got.Error)
	}
}

func TestAgentReceiptSideEffects_FailsClosedOnMalformedPayload(t *testing.T) {
	_, err := decodeAgentReceiptSideEffects([]byte(`{"retry_count":"bad"}`))
	if err == nil {
		t.Fatal("expected malformed agent receipt side effects to fail")
	}
	if !strings.Contains(err.Error(), "cannot unmarshal") {
		t.Fatalf("unexpected error: %v", err)
	}
}
