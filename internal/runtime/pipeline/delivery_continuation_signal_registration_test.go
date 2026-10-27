package pipeline

import (
	"strings"
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestDeliveryContinuationSignalRegistrationRejectsDuplicateAuthority(t *testing.T) {
	store := &workflowInstanceStore{}
	authority := deliveryContinuationSignalTestAuthority(t, 1)
	registration, err := store.RegisterDeliveryContinuationSignal(authority, func() {})
	if err != nil {
		t.Fatalf("register first signal owner: %v", err)
	}
	defer registration.Release()

	if _, err := store.RegisterDeliveryContinuationSignal(authority, func() {}); err == nil {
		t.Fatal("duplicate signal authority registration succeeded")
	}
}

func deliveryContinuationSignalTestAuthority(t *testing.T, generation uint64) runtimedelivery.ExecutionAuthority {
	t.Helper()
	source, err := runtimecorrelation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatalf("build signal test source: %v", err)
	}
	authority, err := runtimedelivery.NewNormalExecutionAuthority(source, "signal-test-authority", generation)
	if err != nil {
		t.Fatalf("build signal test authority: %v", err)
	}
	return authority
}
