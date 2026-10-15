package startupownership

import (
	"context"
	"errors"
	"maps"
	"math"
	"reflect"
	"testing"
)

func TestAgentRecoverySeedObservedWritesRequireCompleteReseed(t *testing.T) {
	for _, mutation := range []string{"claim", "settlement"} {
		for _, cut := range []string{"read", "publication", "installed"} {
			t.Run(mutation+"/"+cut, func(t *testing.T) {
				p, session, read := newAgentRecoverySeedCapability(t)
				before, after := 1, 2
				if mutation == "settlement" {
					before, after = 2, 1
				}
				read.InProgressByProfile = map[string]int{"anthropic": before}
				reads := 0
				session.read = func(ctx context.Context) (AgentRecoverySeedRead, error) {
					reads++
					value := read
					value.InProgressByProfile = maps.Clone(read.InProgressByProfile)
					ObserveAgentRecoverySeedRead(ctx)
					return value, nil
				}
				var result chan error
				release := make(chan struct{})
				remove := func() {}
				if cut == "installed" {
					if err := SeedAgentRecovery(context.Background(), p); err != nil {
						t.Fatal(err)
					}
				} else {
					entered := make(chan struct{})
					hold := func() { close(entered); <-release }
					probe := &AgentRecoverySeedFaultProbeForTest{}
					if cut == "read" {
						probe.InsideRead = hold
					} else {
						probe.BeforePublication = hold
					}
					var err error
					remove, err = InstallAgentRecoverySeedFaultProbeForTest(p, probe)
					if err != nil {
						t.Fatal(err)
					}
					defer remove()
					result = make(chan error, 1)
					go func() { result <- SeedAgentRecovery(context.Background(), p) }()
					<-entered
				}
				write := session.writes.ObserveWriteStart()
				p.mu.Lock()
				retained := p.agentRecoverySeed != nil
				p.mu.Unlock()
				if retained {
					t.Error("observed write retained an installed seed")
				}
				read.InProgressByProfile = map[string]int{"anthropic": after}
				if cut != "installed" {
					close(release)
					if err := <-result; !errors.Is(err, errAgentRecoverySeedChanged) {
						t.Fatalf("intervening write did not refuse stale publication: %v", err)
					}
					remove()
				}
				previousReads := reads
				if err := SeedAgentRecovery(context.Background(), p); !errors.Is(err, errAgentRecoverySeedChanged) || p.agentRecoverySeed != nil || reads != previousReads {
					t.Fatalf("unjoined native write permitted reading/publication: %v reads=%d", err, reads)
				}
				write.End()
				if err := SeedAgentRecovery(context.Background(), p); err != nil || p.agentRecoverySeed == nil || reads != previousReads+1 ||
					!reflect.DeepEqual(p.agentRecoverySeed.inProgressByProfile, map[string]int{"anthropic": after}) {
					t.Fatalf("explicit complete reconstruction lost current counts: %v reads=%d seed=%#v", err, reads, p.agentRecoverySeed)
				}
			})
		}
	}
}

func TestAgentRecoverySeedObserverRetainsPreBindAndOverlappingWrites(t *testing.T) {
	writes := &AgentRecoveryWriteObserver{}
	first := writes.ObserveWriteStart()
	p, _, _ := newAgentRecoverySeedCapabilityWithWrites(t, writes)
	if err := SeedAgentRecovery(context.Background(), p); !errors.Is(err, errAgentRecoverySeedChanged) {
		t.Fatalf("acquisition forgot a pre-bind write: %v", err)
	}
	second := writes.ObserveWriteStart()
	first.End()
	first.End()
	if err := SeedAgentRecovery(context.Background(), p); !errors.Is(err, errAgentRecoverySeedChanged) {
		t.Fatalf("repeated end lost the second active write: %v", err)
	}
	second.End()
	if err := SeedAgentRecovery(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	seed := p.agentRecoverySeed
	first.End()
	second.End()
	if p.agentRecoverySeed != seed {
		t.Fatal("completed scopes revoked a later complete observation")
	}
}

func TestAgentRecoverySeedObserverDoesNotRebindLiveOrForeignProcess(t *testing.T) {
	p, session, _ := newAgentRecoverySeedCapability(t)
	foreign, other, _ := newAgentRecoverySeedCapability(t)
	if err := SeedAgentRecovery(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	seed := p.agentRecoverySeed
	if err := session.writes.bindProcess(foreign); err == nil || session.writes.process != p || foreign.agentRecoveryWrites != other.writes {
		t.Fatal("observer replaced a live or foreign original-process binding")
	}
	write := other.writes.ObserveWriteStart()
	write.End()
	if p.agentRecoverySeed != seed {
		t.Fatal("foreign backend write revoked this process's private seed")
	}
	if err := p.ProveCurrent(context.Background()); err != nil || p.agentRecoverySeed != seed {
		t.Fatalf("read-only possession proof invalidated the seed: %v", err)
	}
	if err := p.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := session.writes.bindProcess(foreign); err == nil {
		t.Fatal("a released observer stole a process bound to another backend")
	}
}

func TestAgentRecoverySeedObserverTransfersWithoutForgettingEarlierWrite(t *testing.T) {
	writes := &AgentRecoveryWriteObserver{}
	previous, _, _ := newAgentRecoverySeedCapabilityWithWrites(t, writes)
	if err := previous.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	write := writes.ObserveWriteStart()
	next, _, _ := newAgentRecoverySeedCapabilityWithWrites(t, writes)
	if err := SeedAgentRecovery(context.Background(), next); !errors.Is(err, errAgentRecoverySeedChanged) {
		t.Fatalf("new original process forgot old backend's in-flight write: %v", err)
	}
	write.End()
	if err := SeedAgentRecovery(context.Background(), next); err != nil || previous.agentRecoverySeed != nil {
		t.Fatalf("joined write did not permit independent reconstruction: %v", err)
	}
}

func TestAgentRecoverySeedObserverEpochExhaustionFailsClosed(t *testing.T) {
	p, session, _ := newAgentRecoverySeedCapability(t)
	session.writes.epoch = math.MaxUint64
	write := session.writes.ObserveWriteStart()
	write.End()
	if err := SeedAgentRecovery(context.Background(), p); !errors.Is(err, errAgentRecoverySeedChanged) || p.agentRecoverySeed != nil {
		t.Fatalf("exhausted freshness epoch became an empty/usable seed: %v", err)
	}
	if err := p.ProveCurrent(context.Background()); err != nil {
		t.Fatalf("private observation exhaustion changed possession: %v", err)
	}
}
