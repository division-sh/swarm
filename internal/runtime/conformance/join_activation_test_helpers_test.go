package conformance

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/timeridentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	"github.com/division-sh/swarm/internal/runtime/joinruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
)

func findConformanceJoinActivation(t *testing.T, ctx context.Context, instance runtimepipeline.WorkflowInstance, node identity.ExecutableNode, event, stage, joinID string) (joinruntime.Activation, bool) {
	t.Helper()
	declaration, err := timeridentity.NewJoinRef(node, event, stage, joinID)
	if err != nil {
		t.Fatal(err)
	}
	carrier, err := runtimeengine.StateCarrierFromPersisted(instance.Fields, instance.Bookkeeping, instance.Gates, instance.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	activations, err := joinruntime.List(carrier.StateBuckets)
	if err != nil {
		t.Fatal(err)
	}
	var selected joinruntime.Activation
	found := false
	for _, activation := range activations {
		ref := activation.JoinRef()
		if !ref.Declaration().Equal(declaration) {
			continue
		}
		if err := ref.StageEntry().RequireOwner(runtimecorrelation.RunIDFromContext(ctx), node.FlowPath(), instance.InstanceID, instance.StorageRef, instance.EntityID, stage); err != nil {
			t.Fatalf("join arm contradicts its exact lifecycle owner: %v", err)
		}
		if found {
			t.Fatal("fixture requires one exact arm; historical arms need an explicit retained reference")
		}
		selected, found, err = joinruntime.Load(carrier.StateBuckets, node, joinruntime.ActivationKey(ref))
		if err != nil || !found {
			t.Fatalf("load retained join arm: found=%v err=%v", found, err)
		}
	}
	return selected, found
}
