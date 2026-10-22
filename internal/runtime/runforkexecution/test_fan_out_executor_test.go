package runforkexecution

import (
	"testing"

	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

type selectedFanOutExecutorProbe struct {
	startupownership.FanOutExecutor
}

func TestSelectedFanOutExecutorDecoratorIsIsolatedAndOptIn(t *testing.T) {
	owner := SelectedContractExecutionOwner{ports: &selectedContractExecutionPorts{contexts: new(selectedForkContexts)}}
	real := &selectedFanOutExecutorProbe{}
	got, err := owner.ports.fanOutExecutor(real)
	if err != nil || got != real {
		t.Fatalf("default executor=%v err=%v, want original", got, err)
	}
	first, second := &selectedFanOutExecutorProbe{}, &selectedFanOutExecutorProbe{}
	for _, want := range []*selectedFanOutExecutorProbe{first, second} {
		calls := 0
		copy, err := WithFanOutExecutorForTest(owner, func(executor startupownership.FanOutExecutor) startupownership.FanOutExecutor {
			calls++
			if executor != real {
				t.Fatal("decorator did not receive the original coordinator")
			}
			return want
		})
		if err != nil {
			t.Fatal(err)
		}
		if copy.ports == owner.ports || copy.ports.contexts != owner.ports.contexts || calls != 0 {
			t.Fatal("configuration mutated the owner, replaced its contexts or invoked the decorator early")
		}
		got, err := copy.ports.fanOutExecutor(real)
		if err != nil || got != want || calls != 1 {
			t.Fatalf("decorated executor=%v calls=%d err=%v", got, calls, err)
		}
		if _, err := WithFanOutExecutorForTest(copy, func(e startupownership.FanOutExecutor) startupownership.FanOutExecutor { return e }); err == nil {
			t.Fatal("accepted a second decorator on the same owner")
		}
	}
	got, err = owner.ports.fanOutExecutor(real)
	if err != nil || got != real || owner.ports.testFanOutExecutor != nil {
		t.Fatal("a decorated copy changed the original owner")
	}
}

func TestSelectedFanOutExecutorDecoratorRejectsInvalidInputs(t *testing.T) {
	identity := func(e startupownership.FanOutExecutor) startupownership.FanOutExecutor { return e }
	if _, err := WithFanOutExecutorForTest(SelectedContractExecutionOwner{}, identity); err == nil {
		t.Fatal("accepted an absent owner")
	}
	owner := SelectedContractExecutionOwner{ports: new(selectedContractExecutionPorts)}
	if _, err := WithFanOutExecutorForTest(owner, nil); err == nil {
		t.Fatal("accepted a nil decorator")
	}
	for _, nilExecutor := range []startupownership.FanOutExecutor{nil, (*selectedFanOutExecutorProbe)(nil)} {
		copy, err := WithFanOutExecutorForTest(owner, func(startupownership.FanOutExecutor) startupownership.FanOutExecutor { return nilExecutor })
		if err != nil {
			t.Fatal(err)
		}
		got, err := copy.ports.fanOutExecutor(&selectedFanOutExecutorProbe{})
		if err == nil || got != nil {
			t.Fatalf("nil decorated executor fell back: executor=%v err=%v", got, err)
		}
	}
}
