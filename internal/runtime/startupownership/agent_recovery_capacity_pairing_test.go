package startupownership

import (
	"context"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
)

func TestAgentRecoveryCapacitySQLitePairIsExactAndOneShotUnit(t *testing.T) {
	for _, fault := range []string{"none", "extra_ended", "extra_open", "outer_ended", "foreign_observer", "foreign_inner", "inner_ended", "cancel"} {
		t.Run(fault, func(t *testing.T) {
			intent, ctx, event, route, profile := agentRecoveryCapacityDecisionBackendUnit(t, processbinding.SQLiteLeaseBackend, map[string]int{}, nil)
			writes := intent.observation.writes
			if wait, err := DecideAgentRecoveryCapacityClaim(ctx, intent.authority, event, route, profile); err == nil || wait {
				t.Fatal("SQLite decision accepted the outer observation without its exact inner")
			}
			inner, err := writes.ObserveSQLiteCapacityInnerStartContext(ctx)
			if err != nil || inner == nil || intent.capacity.inner != inner || writes.active != 2 || writes.epoch != intent.observation.epoch+2 {
				t.Fatalf("exact SQLite inner was not paired once: inner=%p err=%v", inner, err)
			}
			t.Cleanup(inner.End)
			switch fault {
			case "extra_ended", "extra_open":
				extra := writes.ObserveWriteStart()
				t.Cleanup(extra.End)
				if fault == "extra_ended" {
					extra.End()
				}
			case "outer_ended":
				intent.capacity.write.End()
			case "foreign_observer":
				intent.capacity.inner = &AgentRecoveryWriteObservation{observer: &AgentRecoveryWriteObserver{}}
			case "foreign_inner":
				intent.capacity.inner = &AgentRecoveryWriteObservation{observer: writes}
			case "inner_ended":
				inner.End()
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			wait, err := DecideAgentRecoveryCapacityClaim(ctx, intent.authority, event, route, profile)
			if wait || (err != nil) != (fault != "none") {
				t.Fatalf("owner identities were reduced to an active count: fault=%s wait=%t err=%v", fault, wait, err)
			}
			inner.End()
			if fault == "none" {
				if writes.active != 1 || intent.capacity.write.ended.Load() {
					t.Fatal("inner End consumed the outer cleanup scope")
				}
				intent.capacity.write.End()
				if writes.active != 0 {
					t.Fatal("reverse End did not join both original scopes")
				}
			}
			epoch, active := writes.epoch, writes.active
			if retry, err := writes.ObserveSQLiteCapacityInnerStartContext(ctx); err == nil || retry != nil || writes.epoch != epoch || writes.active != active {
				t.Fatal("SQLite inner retry entered a second physical observation")
			}
		})
	}
}

func TestAgentRecoveryCapacitySQLitePairRefusesBeforeStartUnit(t *testing.T) {
	for _, fault := range []string{"postgres", "extra_ended", "outer_ended", "foreign_observer", "plain_intent"} {
		t.Run(fault, func(t *testing.T) {
			backend := processbinding.SQLiteLeaseBackend
			if fault == "postgres" {
				backend = processbinding.PostgresLeaseBackend
			}
			intent, ctx, _, _, _ := agentRecoveryCapacityDecisionBackendUnit(t, backend, map[string]int{}, nil)
			writes := intent.observation.writes
			switch fault {
			case "extra_ended":
				writes.ObserveWriteStart().End()
			case "outer_ended":
				intent.capacity.write.End()
			case "foreign_observer":
				writes = &AgentRecoveryWriteObserver{}
			case "plain_intent":
				ctx = context.WithValue(ctx, agentRecoveryClaimContextKey{}, intent)
			}
			epoch, active := writes.epoch, writes.active
			if inner, err := writes.ObserveSQLiteCapacityInnerStartContext(ctx); err == nil || inner != nil || writes.epoch != epoch || writes.active != active {
				t.Fatal("invalid SQLite pairing created a physical observation")
			}
		})
	}
}
