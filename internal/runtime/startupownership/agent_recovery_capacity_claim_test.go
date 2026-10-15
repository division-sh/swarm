package startupownership

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/core/identity/identitytest"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

func TestAgentRecoveryCapacityClaimPreservesFixedOwnerOutcomeUnit(t *testing.T) {
	for _, state := range []string{"native_ack", "wait_ack", "native_ack_cleanup_error", "wait_ack_cleanup_error", "lost_ack"} {
		t.Run(state, func(t *testing.T) {
			p, session, authority, event, route := newAgentRecoveryClaimCapability(t)
			if err := BindAgentRecoveryProfilePolicy(t.Context(), p, agentRecoveryCapacityConfig()); err != nil {
				t.Fatal(err)
			}
			fault := errors.New("fixed role cleanup or native acknowledgement error")
			session.outcome = &deliverylifecycle.ClaimResult{Acknowledged: true, Disposition: deliverylifecycle.ClaimTerminal}
			switch state {
			case "wait_ack", "wait_ack_cleanup_error":
				session.capacityWaiting = true
				session.outcome.Disposition = ""
			case "lost_ack":
				session.outcome = &deliverylifecycle.ClaimResult{}
			}
			if state == "native_ack_cleanup_error" || state == "wait_ack_cleanup_error" || state == "lost_ack" {
				session.err = fault
			}
			cleanup := false
			session.cleanup = func() {
				cleanup = true
				if session.writes.active != 1 {
					t.Error("outer observation ended before fixed owner cleanup")
				}
			}
			result, err := TryClaimAgentRecoveryDelivery(t.Context(), p, authority, event, route)
			claim, native := result.NativeClaim()
			wantNative := state == "native_ack" || state == "native_ack_cleanup_error"
			if !cleanup || session.writes.active != 0 || result.Waiting() != (state == "wait_ack") ||
				result.Acknowledged() != session.outcome.Acknowledged || native != wantNative ||
				wantNative && !reflect.DeepEqual(claim, *session.outcome) || !errors.Is(err, session.err) {
				t.Fatalf("fixed role outcome changed: %+v native=%t err=%v", result, native, err)
			}
			if err := SeedAgentRecovery(t.Context(), p); err != nil {
				t.Fatalf("fixed owner returned before cleanup: %v", err)
			}
		})
	}
}

func TestAgentRecoveryCapacityClaimRequiresInitializedPolicyBeforeEntryUnit(t *testing.T) {
	for _, fault := range []string{"missing_policy", "nested", "canceled", "foreign_event", "node", "selected"} {
		t.Run(fault, func(t *testing.T) {
			p, session, authority, event, route := newAgentRecoveryClaimCapability(t)
			ctx := t.Context()
			if fault != "missing_policy" {
				if err := BindAgentRecoveryProfilePolicy(ctx, p, agentRecoveryCapacityConfig()); err != nil {
					t.Fatal(err)
				}
			}
			switch fault {
			case "nested":
				ctx = context.WithValue(ctx, agentRecoveryClaimContextKey{}, &agentRecoveryClaimIntent{})
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "foreign_event":
				route.AgentIdentity.RunID = "foreign"
			case "node":
				route.Recipient = events.MustNodeDeliveryRecipient(identitytest.RootNode(t, "capacity-node"))
			case "selected":
				var err error
				authority, err = deliverylifecycle.NewSelectedExecutionAuthority(authority.SourceArtifact(), authority.ExecutionID(), event.RunID(), authority.Generation())
				if err != nil {
					t.Fatal(err)
				}
			}
			epoch := session.writes.epoch
			result, err := TryClaimAgentRecoveryDelivery(ctx, p, authority, event, route)
			_, native := result.NativeClaim()
			if err == nil || result.Waiting() || result.Acknowledged() || native || session.entries != 0 || session.writes.epoch != epoch || session.writes.active != 0 {
				t.Fatalf("invalid capacity input reached native write: %+v err=%v", result, err)
			}
		})
	}
}
