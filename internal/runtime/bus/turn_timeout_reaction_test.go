package bus

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

func TestRecoveredTimeoutPreparationStampDoesNotAdmitBusExecution(t *testing.T) {
	source, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(source, uuid.NewString(), 2)
	if err != nil {
		t.Fatal(err)
	}
	parent := context.Background()
	ctx, err := WithTurnTimeoutRecoveryDeliveryAuthority(parent, authority)
	if err != nil {
		t.Fatal(err)
	}
	if stamp, ok := ctx.Value(turnTimeoutRecoveryDeliveryAuthorityKey{}).(deliverylifecycle.ExecutionAuthority); !ok || !stamp.Equal(authority) {
		t.Fatal("preparation changed the exact issued generation stamp")
	}
	if parent.Value(turnTimeoutRecoveryDeliveryAuthorityKey{}) != nil {
		t.Fatal("preparation contaminated the parent")
	}
	if _, err := (&EventBus{}).DeliveryAuthority(); err == nil {
		t.Fatal("preparation admitted executable bus authority")
	}
	if ctx, err := WithTurnTimeoutRecoveryDeliveryAuthority(parent, deliverylifecycle.ExecutionAuthority{}); err == nil || ctx != nil {
		t.Fatal("missing authority was accepted")
	}
	selected, err := deliverylifecycle.NewSelectedExecutionAuthority(source, uuid.NewString(), uuid.NewString(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if ctx, err := WithTurnTimeoutRecoveryDeliveryAuthority(parent, selected); err == nil || ctx != nil {
		t.Fatal("normal preparation accepted a selected-fork stamp")
	}
}
