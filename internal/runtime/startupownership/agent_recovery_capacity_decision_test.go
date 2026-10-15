package startupownership

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
)

// This fixture models only the private post-entry state. It does not acquire
// a database fence, claim a delivery or provide native COMMIT evidence.
func agentRecoveryCapacityDecisionUnit(t *testing.T, counts map[string]int, limits map[string]config.LLMProviderLimitPolicy) (*agentRecoveryClaimIntent, context.Context, events.Event, events.DeliveryRoute, deliverylifecycle.AgentClaimProfile) {
	t.Helper()
	p, session, authority, event, route := newAgentRecoveryClaimCapability(t)
	cfg := agentRecoveryCapacityConfig()
	cfg.LLM.ProviderLimits = limits
	if err := BindAgentRecoveryProfilePolicy(t.Context(), p, cfg); err != nil {
		t.Fatal(err)
	}
	read, err := session.read(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	read.InProgressByProfile = counts
	session.read = func(context.Context) (AgentRecoverySeedRead, error) { return read, nil }
	prior := session.writes.ObserveWriteStart()
	prior.End()
	if err := SeedAgentRecovery(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	observed, err := p.captureAgentRecoverySeedCheck(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	identity, err := ordinaryAgentClaimIdentity(authority, event, route)
	if err != nil {
		t.Fatal(err)
	}
	life, closeScope := context.WithCancel(t.Context())
	t.Cleanup(closeScope)
	intent := &agentRecoveryClaimIntent{process: p, observation: observed, life: life,
		authority: authority, eventID: event.ID(), runID: event.RunID(), routeIdentity: identity,
		capacity: &agentRecoveryCapacityAttempt{policy: p.agentRecoveryPolicy}}
	bound, err := BindAgentRecoveryClaimIntent(context.WithValue(t.Context(), agentRecoveryClaimContextKey{}, intent), session.writes, authority, event, route)
	if err != nil {
		t.Fatal(err)
	}
	write, err := session.writes.ObserveWriteStartContext(bound)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(write.End)
	// UNIT state setup only. The native owner must later record these exact
	// coordinates at its original single start hook, not at a second observer.
	session.writes.mu.Lock()
	p.mu.Lock()
	intent.capacity.write, intent.capacity.epoch = write, session.writes.epoch
	p.mu.Unlock()
	session.writes.mu.Unlock()
	id, err := deliverylifecycle.DeliveryID(event.ID(), route)
	if err != nil {
		t.Fatal(err)
	}
	profile := deliverylifecycle.AgentClaimProfile{DeliveryID: id, EventID: event.ID(), Identity: route.AgentIdentity,
		ProfileID: "anthropic", ConfigurationRevision: strings.Repeat("a", 64), BundleHash: authority.SourceArtifact().BundleHash(),
		ExecutionID: authority.ExecutionID(), ExecutionGeneration: authority.Generation(), AgentRuntimeEpoch: 1, AgentGeneration: 1}
	return intent, bound, event, route, profile
}

func TestAgentRecoveryCapacityDecisionUsesExactPrivateInputUnit(t *testing.T) {
	for _, state := range []string{"empty", "room", "full", "overfull", "other_profile", "unconfigured"} {
		t.Run(state, func(t *testing.T) {
			counts := map[string]int{}
			limits := map[string]config.LLMProviderLimitPolicy{"anthropic": {MaxConcurrency: 2}}
			switch state {
			case "room":
				counts["anthropic"] = 1
			case "full":
				counts["anthropic"] = 2
			case "overfull":
				counts["anthropic"] = 3
			case "other_profile":
				counts["claude_cli"] = 2
			case "unconfigured":
				limits = nil
				counts["anthropic"] = 10
			}
			intent, ctx, event, route, profile := agentRecoveryCapacityDecisionUnit(t, counts, limits)
			before := intent.observation.seed.inProgressByProfile
			expected := make(map[string]int, len(before))
			for key, value := range before {
				expected[key] = value
			}
			wait, err := DecideAgentRecoveryCapacityClaim(ctx, intent.authority, event, route, profile)
			if err != nil || wait != (state == "full" || state == "overfull") {
				t.Fatalf("private policy decision changed: wait=%t err=%v", wait, err)
			}
			if !reflect.DeepEqual(before, expected) || intent.process.agentRecoverySeed != nil || intent.observation.writes.active != 1 {
				t.Fatal("decision patched occupancy, restored the seed or ended native cleanup")
			}
			if _, err := DecideAgentRecoveryCapacityClaim(ctx, intent.authority, event, route, profile); err == nil {
				t.Fatal("one native entry made a second capacity decision")
			}
		})
	}
}

func TestAgentRecoveryCapacityDecisionRefusesChangedLifetimeUnit(t *testing.T) {
	for _, fault := range []string{"unbound", "plain_intent", "foreign_event", "foreign_profile", "foreign_observer", "missing_policy", "policy_swap",
		"write", "active", "cleanup", "source", "stamp", "nil_counts", "negative_count", "resurrected_seed", "cancel", "expired_scope", "retired"} {
		t.Run(fault, func(t *testing.T) {
			intent, ctx, event, route, profile := agentRecoveryCapacityDecisionUnit(t, map[string]int{}, map[string]config.LLMProviderLimitPolicy{"anthropic": {MaxConcurrency: 1}})
			writes, p := intent.observation.writes, intent.process
			switch fault {
			case "unbound":
				ctx = t.Context()
			case "plain_intent":
				intent.capacity = nil
			case "foreign_event":
				intent.eventID = "foreign"
			case "foreign_profile":
				profile.ExecutionGeneration++
			case "foreign_observer":
				p.agentRecoveryWrites = &AgentRecoveryWriteObserver{process: p}
			case "missing_policy":
				p.agentRecoveryPolicy = nil
			case "policy_swap":
				copy := *p.agentRecoveryPolicy
				p.agentRecoveryPolicy = &copy
			case "write", "active":
				competing := writes.ObserveWriteStart()
				t.Cleanup(competing.End)
				if fault == "write" {
					competing.End()
				}
			case "cleanup":
				intent.capacity.write.End()
			case "source":
				intent.observation.seed.sourceHead = "foreign"
			case "stamp":
				intent.observation.seed.process.AuthorityGeneration++
			case "nil_counts":
				intent.observation.seed.inProgressByProfile = nil
			case "negative_count":
				intent.observation.seed.inProgressByProfile["anthropic"] = -1
			case "resurrected_seed":
				p.agentRecoverySeed = intent.observation.seed
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "expired_scope":
				life, closeScope := context.WithCancel(t.Context())
				closeScope()
				intent.life = life
			case "retired":
				if err := p.Release(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			epoch, active := writes.epoch, writes.active
			if wait, err := DecideAgentRecoveryCapacityClaim(ctx, intent.authority, event, route, profile); err == nil || wait || fault == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatalf("changed native lifetime produced capacity: wait=%t err=%v", wait, err)
			}
			if writes.epoch != epoch || writes.active != active {
				t.Fatal("refused decision changed the original observer lifetime")
			}
		})
	}
}

func TestAgentRecoveryCapacityDecisionCannotRecycleEntryUnit(t *testing.T) {
	for _, full := range []bool{false, true} {
		t.Run(map[bool]string{false: "room", true: "full"}[full], func(t *testing.T) {
			counts := map[string]int{}
			if full {
				counts["anthropic"] = 1
			}
			intent, ctx, event, route, profile := agentRecoveryCapacityDecisionUnit(t, counts, map[string]config.LLMProviderLimitPolicy{"anthropic": {MaxConcurrency: 1}})
			wait, err := DecideAgentRecoveryCapacityClaim(ctx, intent.authority, event, route, profile)
			if err != nil || wait != full {
				t.Fatalf("first UNIT decision failed: wait=%t err=%v", wait, err)
			}
			intent.capacity.write.End()
			epoch := intent.observation.writes.epoch
			for _, reseed := range []bool{false, true} {
				if reseed {
					if err := SeedAgentRecovery(t.Context(), intent.process); err != nil {
						t.Fatal(err)
					}
				}
				if write, err := intent.observation.writes.ObserveWriteStartContext(ctx); err == nil || write != nil {
					t.Fatalf("consumed invocation re-entered its original write: reseed=%t err=%v", reseed, err)
				}
				if wait, err := DecideAgentRecoveryCapacityClaim(ctx, intent.authority, event, route, profile); err == nil || wait {
					t.Fatalf("consumed decision survived cleanup/reconstruction: reseed=%t wait=%t err=%v", reseed, wait, err)
				}
				if intent.observation.writes.active != 0 || intent.observation.writes.epoch != epoch {
					t.Fatal("refused recycled scope restarted the original write")
				}
			}
		})
	}
}

func TestAgentRecoveryCapacityDecisionKeepsLocksLocalUnit(t *testing.T) {
	intent, ctx, event, route, profile := agentRecoveryCapacityDecisionUnit(t, map[string]int{}, nil)
	var proofs atomic.Int32
	intent.process.session.(*agentRecoveryClaimSessionProbe).monitorProve = func(context.Context, time.Duration) error {
		proofs.Add(1)
		return errors.New("native proof is not allowed in the capacity critical section")
	}
	intent.process.opMu.Lock()
	var once sync.Once
	unlock := func() { once.Do(intent.process.opMu.Unlock) }
	t.Cleanup(unlock)
	finished := make(chan error, 1)
	go func() {
		_, err := DecideAgentRecoveryCapacityClaim(ctx, intent.authority, event, route, profile)
		finished <- err
	}()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		unlock()
		<-finished
		t.Fatal("capacity decision waited for the process operation lock")
	}
	unlock()
	if proofs.Load() != 0 {
		t.Fatal("capacity decision invoked a native possession proof")
	}
	if !intent.observation.writes.mu.TryLock() {
		t.Fatal("capacity decision retained its observer lock")
	}
	intent.observation.writes.mu.Unlock()
	if !intent.process.mu.TryLock() {
		t.Fatal("capacity decision retained its process lock")
	}
	intent.process.mu.Unlock()
}
