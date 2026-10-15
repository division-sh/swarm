package startupownership

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
)

type agentRecoveryClaimSessionProbe struct {
	*agentRecoverySeedSessionProbe
	invoke          func(context.Context, deliverylifecycle.ExecutionAuthority, events.Event, events.DeliveryRoute) (deliverylifecycle.ClaimResult, error)
	entries         int
	entered         context.Context
	outcome         *deliverylifecycle.ClaimResult
	err             error
	cleanup         func()
	capacityWaiting bool
}

func (s *agentRecoveryClaimSessionProbe) ClaimAgentRecoveryDelivery(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, error) {
	if s.invoke != nil {
		return s.invoke(ctx, authority, event, route)
	}
	return s.enter(ctx, authority, event, route)
}

func (s *agentRecoveryClaimSessionProbe) TryClaimAgentRecoveryDelivery(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, bool, error) {
	claim, err := s.ClaimAgentRecoveryDelivery(ctx, authority, event, route)
	return claim, s.capacityWaiting, err
}

func (s *agentRecoveryClaimSessionProbe) enter(ctx context.Context, authority deliverylifecycle.ExecutionAuthority, event events.Event, route events.DeliveryRoute) (deliverylifecycle.ClaimResult, error) {
	bound, err := BindAgentRecoveryClaimIntent(ctx, s.writes, authority, event, route)
	if err != nil {
		return deliverylifecycle.ClaimResult{}, err
	}
	write, err := s.writes.ObserveWriteStartContext(bound)
	if err != nil {
		return deliverylifecycle.ClaimResult{}, err
	}
	defer write.End()
	if s.cleanup != nil {
		defer s.cleanup()
	}
	s.entries++
	s.entered = bound
	if s.writes.active != 1 || s.writes.process.agentRecoverySeed != nil {
		return deliverylifecycle.ClaimResult{}, errors.New("native entry did not perform normal conservative revocation exactly once")
	}
	if duplicate, err := s.writes.ObserveWriteStartContext(bound); err == nil || duplicate != nil {
		return deliverylifecycle.ClaimResult{}, errors.New("one exact native entry could be consumed twice")
	}
	if s.outcome != nil {
		return *s.outcome, s.err
	}
	return deliverylifecycle.ClaimResult{Acknowledged: true, Disposition: deliverylifecycle.ClaimTerminal}, nil
}

func newAgentRecoveryClaimCapability(t *testing.T) (*processCapability, *agentRecoveryClaimSessionProbe, deliverylifecycle.ExecutionAuthority, events.Event, events.DeliveryRoute) {
	return newAgentRecoveryClaimCapabilityForBackend(t, processbinding.SQLiteLeaseBackend)
}

func newAgentRecoveryClaimCapabilityForBackend(t *testing.T, backend string) (*processCapability, *agentRecoveryClaimSessionProbe, deliverylifecycle.ExecutionAuthority, events.Event, events.DeliveryRoute) {
	t.Helper()
	base, plan := testRetainedSession(t)
	base.authority.Backend = backend
	reader := &agentRecoverySeedSessionProbe{retainedSessionProbe: base, writes: &AgentRecoveryWriteObserver{}}
	reader.read = func(context.Context) (AgentRecoverySeedRead, error) {
		return AgentRecoverySeedRead{Process: base.authority, SourceHead: plan, InProgressByProfile: map[string]int{}}, nil
	}
	session := &agentRecoveryClaimSessionProbe{agentRecoverySeedSessionProbe: reader}
	capability, err := newProcessCapability(session, time.Hour, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	p := capability.(*processCapability)
	t.Cleanup(func() {
		if err := p.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	name, err := agentidentity.DeclaredName("seed-worker", "seed-proof")
	if err != nil {
		t.Fatal(err)
	}
	runID := uuid.NewString()
	instance, err := agentidentity.PresentRoute("global", uuid.NewString(), "global")
	if err != nil {
		t.Fatal(err)
	}
	identity, err := agentidentity.New(runID, name, instance)
	if err != nil {
		t.Fatal(err)
	}
	route := events.DeliveryRoute{Recipient: events.MustAgentDeliveryRecipient(name.AgentID), AgentIdentity: identity,
		Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "global", FlowInstance: identity.FlowInstance()})}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "test.seed_handoff", "operator", "", []byte(`{}`), 0, runID, events.EventEnvelope{}, time.Now().UTC())
	authority, err := deliverylifecycle.NewNormalExecutionAuthority(sourceartifactfixture.Fact(), uuid.NewString(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := SeedAgentRecovery(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	return p, session, authority, event, route
}

func TestAgentRecoveryClaimHandoffConsumesExactEntryOnce(t *testing.T) {
	p, session, authority, event, route := newAgentRecoveryClaimCapability(t)
	writes := session.writes
	before := writes.epoch
	result, err := ClaimAgentRecoveryDelivery(t.Context(), p, authority, event, route)
	if err != nil || !result.Acknowledged || result.Disposition != deliverylifecycle.ClaimTerminal || session.entries != 1 || writes.epoch != before+1 || writes.active != 0 || p.agentRecoverySeed != nil {
		t.Fatalf("exact unit native entry changed outcome/observer lifetime: %+v err=%v entries=%d epoch=%d active=%d", result, err, session.entries, writes.epoch, writes.active)
	}
	if write, err := writes.ObserveWriteStartContext(context.WithoutCancel(session.entered)); err == nil || write != nil || writes.epoch != before+1 || writes.active != 0 {
		t.Fatalf("expired native scope reopened a write: err=%v epoch=%d active=%d", err, writes.epoch, writes.active)
	}
	if err := CheckAgentRecoverySeed(t.Context(), p); err == nil {
		t.Fatal("own write implicitly reinstalled the seed")
	}
	if err := SeedAgentRecovery(t.Context(), p); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRecoveryClaimHandoffRejectsInterveningOwnerChange(t *testing.T) {
	for _, change := range []string{"write", "reseed", "equal_value_seed", "active", "cancel", "release"} {
		t.Run(change, func(t *testing.T) {
			p, session, authority, event, route := newAgentRecoveryClaimCapability(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var active *AgentRecoveryWriteObservation
			probe := &AgentRecoverySeedFaultProbeForTest{AfterCheckProof: func() {
				if !p.opMu.TryLock() {
					t.Fatal("handoff kept capture serialization over native entry cut")
				}
				p.opMu.Unlock()
				switch change {
				case "write", "reseed", "active":
					active = session.writes.ObserveWriteStart()
					if change != "active" {
						active.End()
					}
					if change == "reseed" {
						if err := SeedAgentRecovery(t.Context(), p); err != nil {
							t.Fatal(err)
						}
					}
				case "equal_value_seed":
					p.mu.Lock()
					copy := *p.agentRecoverySeed
					p.agentRecoverySeed = &copy
					p.mu.Unlock()
				case "cancel":
					cancel()
				case "release":
					if err := p.Release(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
			}}
			remove, err := InstallAgentRecoverySeedFaultProbeForTest(p, probe)
			if err != nil {
				t.Fatal(err)
			}
			defer remove()
			defer func() { active.End() }()
			_, err = ClaimAgentRecoveryDelivery(ctx, p, authority, event, route)
			if err == nil || session.entries != 0 {
				t.Fatalf("changed observation entered native write: err=%v entries=%d", err, session.entries)
			}
			if change != "release" {
				if _, terminal := p.TerminalResult(); terminal {
					t.Fatal("entry-domain/caller refusal terminalized healthy possession")
				}
			}
		})
	}
}

func TestAgentRecoveryClaimIntentRefusesSubstitutionBeforeNativeWrite(t *testing.T) {
	for _, change := range []string{"context", "backend", "authority", "event", "route"} {
		t.Run(change, func(t *testing.T) {
			p, session, authority, event, route := newAgentRecoveryClaimCapability(t)
			seed, epoch := p.agentRecoverySeed, session.writes.epoch
			session.invoke = func(ctx context.Context, actual deliverylifecycle.ExecutionAuthority, evt events.Event, target events.DeliveryRoute) (deliverylifecycle.ClaimResult, error) {
				writes := session.writes
				switch change {
				case "context":
					ctx = context.WithValue(context.Background(), "agent_recovery_claim", true)
				case "backend":
					writes = &AgentRecoveryWriteObserver{}
				case "authority":
					actual, _ = deliverylifecycle.NewNormalExecutionAuthority(actual.SourceArtifact(), uuid.NewString(), actual.Generation())
				case "event":
					evt = eventtest.ExistingRunRootIngress(uuid.NewString(), evt.Type(), "operator", "", evt.Payload(), 0, evt.RunID(), events.EventEnvelope{}, time.Now().UTC())
				case "route":
					target = events.DeliveryRoute{}
				}
				_, err := BindAgentRecoveryClaimIntent(ctx, writes, actual, evt, target)
				return deliverylifecycle.ClaimResult{}, err
			}
			_, err := ClaimAgentRecoveryDelivery(t.Context(), p, authority, event, route)
			if err == nil || session.entries != 0 || session.writes.epoch != epoch || p.agentRecoverySeed != seed || !reflect.DeepEqual(seed.inProgressByProfile, map[string]int{}) {
				t.Fatalf("forged command acquired or changed observation: %v", err)
			}
		})
	}
}

// These are fixed-role UNIT outcomes, not evidence of a physical native
// cleanup fault. Real physical outcomes are covered by the both-store cuts.
func TestAgentRecoveryClaimHandoffPreservesFixedOwnerOutcomeUnit(t *testing.T) {
	for _, outcome := range []string{"ack", "rollback", "lost_ack", "ack_cleanup_error"} {
		t.Run(outcome, func(t *testing.T) {
			p, session, authority, event, route := newAgentRecoveryClaimCapability(t)
			expected := deliverylifecycle.ClaimResult{}
			if outcome == "ack" || outcome == "ack_cleanup_error" {
				expected.Acknowledged = true
				expected.Disposition = deliverylifecycle.ClaimTerminal
			}
			var expectedErr error
			if outcome != "ack" {
				expectedErr = errors.New(outcome + " fixed-owner unit result")
			}
			session.outcome, session.err = &expected, expectedErr
			epoch := session.writes.epoch
			entered, resume := make(chan struct{}), make(chan struct{})
			if outcome == "ack_cleanup_error" {
				session.cleanup = func() { close(entered); <-resume }
			}
			type result struct {
				claim deliverylifecycle.ClaimResult
				err   error
			}
			finished := make(chan result, 1)
			go func() {
				claim, err := ClaimAgentRecoveryDelivery(t.Context(), p, authority, event, route)
				finished <- result{claim, err}
			}()
			joined, released := false, false
			t.Cleanup(func() {
				if !released {
					close(resume)
				}
				if !joined {
					<-finished
				}
			})
			if outcome == "ack_cleanup_error" {
				select {
				case <-entered:
				case <-time.After(2 * time.Second):
					t.Fatal("unit owner did not reach its exact cleanup cut")
				}
				if session.writes.active != 1 || session.writes.epoch != epoch+1 || p.agentRecoverySeed != nil {
					t.Fatal("checked observation ended or reseeded before unit owner cleanup")
				}
				if err := SeedAgentRecovery(t.Context(), p); err == nil {
					t.Fatal("held unit cleanup allowed implicit/explicit early reseed")
				}
				select {
				case <-finished:
					joined = true
					t.Fatal("checked entry returned before the unit owner cleanup joined")
				default:
				}
			}
			close(resume)
			released = true
			got := <-finished
			joined = true
			actual, err := got.claim, got.err
			if !reflect.DeepEqual(actual, expected) || err != expectedErr || session.entries != 1 ||
				session.writes.epoch != epoch+1 || session.writes.active != 0 || p.agentRecoverySeed != nil {
				t.Fatalf("fixed owner outcome/lifetime changed: actual=%+v err=%v entries=%d active=%d epoch=%d", actual, err, session.entries, session.writes.active, session.writes.epoch)
			}
			if err := SeedAgentRecovery(t.Context(), p); err != nil {
				t.Fatalf("unit owner returned before its write end: %v", err)
			}
		})
	}
}

func TestAgentRecoveryClaimEntryKeepsUnboundWritesAndRefusesOverflow(t *testing.T) {
	p, session, authority, event, route := newAgentRecoveryClaimCapability(t)
	epoch := session.writes.epoch
	write, err := session.writes.ObserveWriteStartContext(context.Background())
	if err != nil || write == nil || session.writes.active != 1 || session.writes.epoch != epoch+1 || p.agentRecoverySeed != nil {
		t.Fatalf("unbound native writer behavior changed: %v", err)
	}
	write.End()
	write.End()
	if session.writes.active != 0 {
		t.Fatal("original End stopped being idempotent")
	}
	if err := SeedAgentRecovery(t.Context(), p); err != nil {
		t.Fatal(err)
	}
	session.writes.epoch = math.MaxUint64
	seed := p.agentRecoverySeed
	_, err = ClaimAgentRecoveryDelivery(t.Context(), p, authority, event, route)
	if err == nil || session.entries != 0 || session.writes.active != 0 || session.writes.epoch != math.MaxUint64 || p.agentRecoverySeed != seed {
		t.Fatalf("checked entry overflow mutated or entered native owner: %v", err)
	}
}
