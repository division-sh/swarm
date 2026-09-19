package startupownership

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestFanOutClosingRegistrationReplacementRequiresJoinAndIgnoresStaleClose(t *testing.T) {
	release, cleanup := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(cleanup) }) }
	f := newServingFixture(t, 1, release, servingProbeControls{beforeCleanup: cleanup})
	t.Cleanup(unblock)
	old := f.registrations[0]
	f.append(t, 0, uuid.NewString())
	old.Wake()
	f.waitStarted(t, time.Second)
	closed := make(chan struct{})
	go func() { old.Close(); close(closed) }()
	select {
	case <-old.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("old registration was not canceled")
	}
	if replacement, err := StartFanOutServing(context.Background(), f.grants[0], f.occurrences[0], fanOutWorkers(1), old.executor); err == nil {
		replacement.Close()
		t.Fatal("same-grant replacement bypassed old finite-turn join")
	}
	unblock()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("old registration did not join")
	}
	if f.occurrences[0].ActiveCount() != 0 {
		t.Fatal("closing evidence retained an occurrence lease")
	}
	p := old.grant.owner
	evidence, err := old.grant.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	retained := p.fanOutCapacity.registrations[evidence.GrantID]
	projection, _ := p.fanOutRegistrationsLocked()
	p.mu.Unlock()
	if retained != old || len(projection) != 2 {
		t.Fatal("closing registration left the complete acknowledged census")
	}
	// The canceled probe candidate is not a replacement-admission assertion.
	// Remove its synthetic backlog so it cannot race the explicit permit below.
	f.store.mu.Lock()
	f.store.queue = nil
	f.store.mu.Unlock()
	replacement, err := StartFanOutServing(context.Background(), f.grants[0], f.occurrences[0], fanOutWorkers(1), old.executor)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(replacement.Close)
	old.Close()
	p.mu.Lock()
	current := p.fanOutCapacity.registrations[evidence.GrantID]
	p.mu.Unlock()
	if current != replacement {
		t.Fatal("stale Close removed the same-grant replacement")
	}
	permit, found, err := replacement.BeginTurn(context.Background())
	if err != nil || !found {
		t.Fatalf("replacement lost admission: found=%v err=%v", found, err)
	}
	permit.Done()
	replacement.Close()
	if err := f.grants[0].Retire(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	current = p.fanOutCapacity.registrations[evidence.GrantID]
	p.mu.Unlock()
	if current != nil {
		t.Fatal("actual grant retirement retained the closing registration")
	}
}
