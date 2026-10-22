package startupownership

import (
	"context"
	"testing"
)

func TestFanOutLastRegistrationJoinsObserverBeforeReplacement(t *testing.T) {
	release := make(chan struct{})
	close(release)
	f := newServingFixture(t, 1, release)
	p := f.process.(*processCapability)
	p.mu.Lock()
	original := p.fanOutCapacity.service
	p.mu.Unlock()
	baseline := f.work.ActiveCount()
	f.registrations[0].Close()
	p.mu.Lock()
	current := p.fanOutCapacity.service
	p.mu.Unlock()
	if current != original || original.ctx.Err() != nil {
		t.Fatal("one closing source retired another source's live shared observer")
	}
	f.registrations[1].Close()
	select {
	case <-original.done:
	default:
		t.Fatal("last registration escaped the observer join")
	}
	p.mu.Lock()
	current = p.fanOutCapacity.service
	p.mu.Unlock()
	if current != nil || f.work.ActiveCount()+1 != baseline {
		t.Fatalf("last registration retained idle process work: service=%p work=%d baseline=%d", current, f.work.ActiveCount(), baseline)
	}
	replacement, err := StartFanOutServing(context.Background(), f.grants[0], f.occurrences[0], fanOutWorkers(1), f.registrations[0].executor)
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	p.mu.Lock()
	current = p.fanOutCapacity.service
	p.mu.Unlock()
	if current == nil || current == original || current.ctx.Err() != nil {
		t.Fatal("replacement reused a canceled observer")
	}
	f.registrations[1].Close()
	if current.ctx.Err() != nil {
		t.Fatal("old close retired the new serving epoch")
	}
	replacement.Close()
	if f.work.ActiveCount()+1 != baseline {
		t.Fatal("replacement leaked its shared observer")
	}
}
