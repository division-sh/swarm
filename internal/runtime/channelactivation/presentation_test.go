package channelactivation

import (
	"context"
	"testing"
	"time"
)

func TestPresentationDescendantsJoinPredecessorUnderFence(t *testing.T) {
	o, err := NewOwner(testEmptyPublication(t))
	if err != nil {
		t.Fatal(err)
	}
	scope := PresentationScope{Actor: "actor", Source: "source", Input: "input"}
	ctx, root, err := o.AcquirePresentationForContext(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	binding := BindPresentation(ctx, time.Now().Add(time.Hour))
	defer binding.Close()
	replaced := make(chan error, 1)
	go func() { replaced <- o.Replace(testEmptyPublication(t)) }()
	waitPresentationFence(t, o)
	childCtx, childRelease, err := binding.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, nested, err := o.AcquirePresentationForContext(childCtx, scope)
	if err != nil {
		t.Fatal(err)
	}
	if nested.snapshot != root.snapshot {
		t.Fatal("child substituted successor")
	}
	joined := make(chan struct{})
	go func() { root.Release(); close(joined) }()
	select {
	case <-childCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("parent completion did not cancel request")
	}
	if _, _, err := binding.Acquire(context.Background()); err == nil {
		t.Fatal("completed parent admitted another request")
	}
	select {
	case <-joined:
		t.Fatal("root released before children joined")
	default:
	}
	select {
	case <-replaced:
		t.Fatal("replacement overtook child")
	default:
	}
	nested.Release()
	childRelease()
	childRelease()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("root did not join")
	}
	if err := <-replaced; err != nil {
		t.Fatal(err)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.current.leases != 0 {
		t.Fatal("lease leaked")
	}
}

func TestPresentationRejectsForeignAndRevokedWithoutCurrentAdmission(t *testing.T) {
	o, _ := NewOwner(testEmptyPublication(t))
	other, _ := NewOwner(testEmptyPublication(t))
	scope := PresentationScope{Source: "s", Actor: "a", Flow: "f", Run: "r", Entity: "e", Input: "i"}
	ctx, root, err := o.AcquirePresentationForContext(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Release()
	for _, field := range []string{"source", "actor", "flow", "run", "entity", "input", "identity", "owner"} {
		t.Run(field, func(t *testing.T) {
			changed := scope
			owner := o
			switch field {
			case "source":
				changed.Source += "x"
			case "actor":
				changed.Actor += "x"
			case "flow":
				changed.Flow += "x"
			case "run":
				changed.Run += "x"
			case "entity":
				changed.Entity += "x"
			case "input":
				changed.Input += "x"
			case "identity":
				changed.Identity.RunID = "foreign-run"
			case "owner":
				owner = other
			}
			if _, lease, err := owner.AcquirePresentationForContext(ctx, changed); err == nil {
				lease.Release()
				t.Fatal("foreign authority admitted")
			}
		})
	}
	b := BindPresentation(ctx, time.Now().Add(time.Hour))
	b.Close()
	if _, _, err := b.Acquire(context.Background()); err == nil {
		t.Fatal("revoked binding admitted")
	}
	root.Release()
	if _, lease, err := o.AcquirePresentationForContext(ctx, scope); err == nil {
		lease.Release()
		t.Fatal("released pin became fresh admission")
	}
}

func TestPresentationExpiryRefusesAlreadyResolvedBindingWithoutTimer(t *testing.T) {
	o, _ := NewOwner(testEmptyPublication(t))
	ctx, root, err := o.AcquirePresentationForContext(context.Background(), PresentationScope{Actor: "a"})
	if err != nil {
		t.Fatal(err)
	}
	defer root.Release()
	binding := BindPresentation(ctx, time.Now().Add(-time.Second))
	defer binding.Close()
	if _, release, err := binding.Acquire(context.Background()); err == nil {
		release()
		t.Fatal("expired resolved binding admitted a child without timer revocation")
	}
}

func TestPresentationUnrelatedAdmissionAndCancelledReplacement(t *testing.T) {
	o, _ := NewOwner(testEmptyPublication(t))
	scope := PresentationScope{Actor: "a"}
	ctx, root, err := o.AcquirePresentationForContext(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Release()
	replaceCtx, cancel := context.WithCancel(context.Background())
	replaced := make(chan error, 1)
	go func() { replaced <- o.ReplaceContext(replaceCtx, testEmptyPublication(t)) }()
	waitPresentationFence(t, o)
	newCtx, stop := context.WithCancel(context.Background())
	stop()
	if _, lease, err := o.AcquirePresentationForContext(newCtx, scope); err == nil {
		lease.Release()
		t.Fatal("unrelated request passed fence")
	}
	_, child, err := o.AcquirePresentationForContext(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	child.Release()
	cancel()
	if err := <-replaced; err == nil {
		t.Fatal("cancelled replacement succeeded")
	}
	fresh, ok := o.AcquirePresentation()
	if !ok {
		t.Fatal("cancelled replacement stranded owner")
	}
	fresh.Release()
}

func waitPresentationFence(t *testing.T, o *Owner) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		o.mu.Lock()
		fenced := !o.accepting
		o.mu.Unlock()
		if fenced {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("replacement failed to fence")
		}
		time.Sleep(time.Millisecond)
	}
}
