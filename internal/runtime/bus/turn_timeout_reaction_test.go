package bus

import (
	"context"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentitytest"
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
	t.Run("exact_timeout_publication_owner", func(t *testing.T) {
		runID := uuid.NewString()
		actor := agentidentitytest.RootRuntimeForRun(t, runID, "timeout-owner", "timeout-owner-proof")
		event := eventtest.RuntimeControlWithRoutingSource(uuid.NewString(), "work.expired", "agent-turn-timeout", "", []byte(`{}`), 0, runID, "", events.EventEnvelope{}, events.NewPlatformControlRoutingSource(), time.Now().UTC())
		ctx := correlation.WithSourceArtifactFact(context.Background(), source)
		bound := context.WithValue(ctx, turnTimeoutPublicationOwnerKey{}, turnTimeoutPublicationOwner{event: event, actor: actor, source: source})
		if actual, found, err := preparedTurnTimeoutActor(bound, event); err != nil || !found || actual != actor {
			t.Fatalf("exact timeout owner=%+v found=%t err=%v", actual, found, err)
		}
		if _, found, err := preparedTurnTimeoutActor(ctx, event); err != nil || found {
			t.Fatal("platform producer invented acknowledged timeout ownership")
		}
		foreign := eventtest.RuntimeControlWithRoutingSource(event.ID(), event.Type(), event.SourceAgent(), "", []byte(`{"foreign":true}`), 0, runID, "", events.EventEnvelope{}, event.RoutingSource(), event.CreatedAt())
		if _, found, err := preparedTurnTimeoutActor(bound, foreign); !found || err == nil {
			t.Fatal("changed timeout event borrowed acknowledged ownership")
		}
		foreignSource, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := preparedTurnTimeoutActor(correlation.WithSourceArtifactFact(bound, foreignSource), event); err == nil {
			t.Fatal("timeout owner crossed its admitted source")
		}
		canceled, cancel := context.WithCancel(bound)
		cancel()
		if _, _, err := preparedTurnTimeoutActor(canceled, event); err != context.Canceled {
			t.Fatalf("timeout owner lost cancellation: %v", err)
		}
	})
}
