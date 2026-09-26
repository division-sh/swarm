package bus

import (
	"context"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/google/uuid"
)

func TestSelectedPipelineRecoveryRefusesForeignRunBeforeScan(t *testing.T) {
	source, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	authority, err := runtimedelivery.NewSelectedExecutionAuthority(source, uuid.NewString(), runID, 2)
	if err != nil {
		t.Fatal(err)
	}
	bus := &EventBus{deliveryAuthority: authority}
	if err := bus.RecoverSelectedRunPipelineToExhaustion(context.Background(), uuid.NewString()); err == nil {
		t.Fatal("foreign selected run reached pipeline scan")
	}
	bus.deliveryAuthority = runtimedelivery.ExecutionAuthority{}
	if err := bus.RecoverSelectedRunPipelineToExhaustion(context.Background(), runID); err == nil {
		t.Fatal("missing selected authority reached pipeline scan")
	}
}
