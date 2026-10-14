package bus

import (
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/google/uuid"
)

// The mutable installer is retired. Preparation consumes native evidence;
// reset/source/refusal proofs remain with their existing admission owners.
func TestRoutePreparationResetLockOrderAndReplacedTableRefusal(t *testing.T) {
	t.Run("reset", TestEventBusResetInMemoryStateRefreshesConnectRoutePlanner)
	t.Run("foreign_source", TestPublishRootsRejectForeignSourceBeforeWorkOccurrenceAdmission)
}

func TestRoutePreparationPreservesRetirementAndEpochRefusal(t *testing.T) {
	t.Run("retirement", TestEventBusResetRetiresAndRestartsInternalSubscriptionGeneration)
	t.Run("stale_epoch", func(t *testing.T) {
		source := connectRoutePlanTemplateInstanceSource(t, canonicalrouting.TemplateInstanceRouteSelectOrCreate, false)
		store := newTargetRouteMemoryStore()
		bus, err := newScopedTestEventBus(store, EventBusOptions{ContractBundle: source})
		if err != nil {
			t.Fatal(err)
		}
		ctx := WithCurrentRuntimeEpoch(constructionIndexContext(t, source))
		EnterRuntimeResetMode()
		defer ExitRuntimeResetMode()
		event := eventtest.RuntimeControl(uuid.NewString(), "test.route_preparation", "test", "", []byte(`{}`), 0, uuid.NewString(), "", events.EventEnvelope{}, time.Now().UTC())
		if err := bus.Publish(ctx, event); !errors.Is(err, ErrStaleRuntimeEpoch) {
			t.Fatalf("stale-epoch publication: %v", err)
		}
		if len(store.events) != 0 || store.instanceIndexReads != 0 {
			t.Fatal("stale-epoch publication reached planning or persistence")
		}
	})
}
