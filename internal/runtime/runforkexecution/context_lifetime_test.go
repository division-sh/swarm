package runforkexecution

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/google/uuid"
)

func contextLifetimeOwner(t *testing.T) (SelectedContractExecutionOwner, *worklifetime.Process, context.Context) {
	t.Helper()
	process := worklifetime.NewProcess()
	owner := SelectedContractExecutionOwner{ports: &selectedContractExecutionPorts{contexts: &selectedForkContexts{process: process, recovered: true}}}
	t.Cleanup(func() {
		if err := owner.RetireSelectedContexts(context.Background()); err != nil {
			t.Error(err)
		}
		process.Retire()
		if _, err := process.Join(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return owner, process, worklifetime.WithProcess(context.Background(), process)
}

func contextLifetimePreparation(t *testing.T, owner SelectedContractExecutionOwner, ctx context.Context) *PreparedSelectedFork {
	t.Helper()
	op, err := owner.beginPreparation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p := &PreparedSelectedFork{owner: owner, operation: op}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	return p
}

func bindContextLifetimePreparation(t *testing.T, owner SelectedContractExecutionOwner, p *PreparedSelectedFork) {
	t.Helper()
	identity := worklifetime.SelectedForkIdentity{RunID: uuid.NewString(), ExecutionID: uuid.NewString(), Generation: 1}
	if err := p.operation.Bind(identity); err != nil {
		t.Fatal(err)
	}
	owner.ports.contexts.mu.Lock()
	owner.ports.contexts.entries[p.operation].binding = runfork.RunForkSelectedContractBinding{BindingID: uuid.NewString(), ForkRunID: identity.RunID}
	owner.ports.contexts.mu.Unlock()
}

func TestSelectedForkContextHandoffAndSiblingLifetime(t *testing.T) {
	owner, process, ctx := contextLifetimeOwner(t)
	caller, cancel := context.WithCancel(ctx)
	p := contextLifetimePreparation(t, owner, caller)
	bindContextLifetimePreparation(t, owner, p)
	selected := p.operation.selected
	if err := owner.retainPrepared(p); err != nil {
		t.Fatal(err)
	}
	if err := owner.completePreparation(p); err != nil {
		t.Fatal(err)
	}
	cancel()
	if process.ActiveCount() != 2 {
		t.Fatalf("request return lost retained process ownership: %d", process.ActiveCount())
	}
	sibling, err := process.NewSelectedFork(ctx, worklifetime.SelectedForkIdentity{RunID: uuid.NewString(), ExecutionID: uuid.NewString(), Generation: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer sibling.RetireAndWait(context.Background())
	if err := owner.RetireSelectedContexts(ctx); err != nil {
		t.Fatal(err)
	}
	if lease, err := selected.Begin(ctx); !errors.Is(err, worklifetime.ErrRetired) {
		if lease != nil {
			_ = lease.Done()
		}
		t.Fatalf("retained context admitted after retirement: %v", err)
	}
	lease, err := sibling.Begin(ctx)
	if err != nil {
		t.Fatalf("retirement affected another exact occurrence: %v", err)
	}
	if err := lease.Done(); err != nil {
		t.Fatal(err)
	}
	if process.ActiveCount() != 1 {
		t.Fatalf("retired owner retained %d leases", process.ActiveCount())
	}
}

func TestSelectedForkContextRetirementJoinsAcceptedDisposition(t *testing.T) {
	owner, process, ctx := contextLifetimeOwner(t)
	p := contextLifetimePreparation(t, owner, ctx)
	bindContextLifetimePreparation(t, owner, p)
	accepted, err := p.operation.selected.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	joined := make(chan error, 1)
	go func() { joined <- owner.RetireSelectedContexts(ctx) }()
	<-accepted.Context().Done()
	select {
	case err := <-joined:
		t.Fatalf("retirement skipped accepted work/disposition: %v", err)
	default:
	}
	if process.ActiveCount() == 0 {
		t.Fatal("accepted work lost process possession")
	}
	if err := accepted.Done(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-joined:
		t.Fatalf("retirement skipped owning operation's cleanup: %v", err)
	default:
	}
	if err := owner.completePreparation(p); err != nil {
		t.Fatal(err)
	}
	if err := <-joined; err != nil {
		t.Fatal(err)
	}
	if process.ActiveCount() != 0 {
		t.Fatal("completed disposition leaked process ownership")
	}
}

func TestSelectedForkContextRetirementTimeoutFailsClosed(t *testing.T) {
	owner, process, ctx := contextLifetimeOwner(t)
	p := contextLifetimePreparation(t, owner, ctx)
	expired, cancel := context.WithDeadline(ctx, time.Unix(1, 0))
	defer cancel()
	if err := owner.RetireSelectedContexts(expired); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retirement = %v", err)
	}
	if _, err := process.Join(expired); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("store join issued early: %v", err)
	}
	if _, err := owner.beginPreparation(ctx); !errors.Is(err, worklifetime.ErrRetired) {
		t.Fatalf("post-retirement admission = %v", err)
	}
	if err := p.operation.Bind(worklifetime.SelectedForkIdentity{RunID: "fork", ExecutionID: "execution", Generation: 1}); !errors.Is(err, worklifetime.ErrRetired) {
		t.Fatalf("late binding = %v", err)
	}
	if err := owner.completePreparation(p); err != nil {
		t.Fatal(err)
	}
	if err := owner.RetireSelectedContexts(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSelectedForkOperationRetireVersusBind(t *testing.T) {
	for _, first := range []string{"retire", "bind"} {
		t.Run(first, func(t *testing.T) {
			owner, _, ctx := contextLifetimeOwner(t)
			p := contextLifetimePreparation(t, owner, ctx)
			identity := worklifetime.SelectedForkIdentity{RunID: "fork", ExecutionID: "execution", Generation: 1}
			var wg sync.WaitGroup
			wg.Add(1)
			start, finished := make(chan struct{}), make(chan struct{})
			go func() { defer wg.Done(); <-start; p.operation.Retire(); close(finished) }()
			if first == "retire" {
				close(start)
				<-finished
				if err := p.operation.Bind(identity); !errors.Is(err, worklifetime.ErrRetired) {
					t.Fatalf("late binding admitted: %v", err)
				}
			} else {
				if err := p.operation.Bind(identity); err != nil {
					t.Fatal(err)
				}
				selected := p.operation.selected
				close(start)
				<-finished
				if lease, err := selected.Begin(ctx); !errors.Is(err, worklifetime.ErrRetired) {
					if lease != nil {
						_ = lease.Done()
					}
					t.Fatalf("retirement did not cancel bound generation: %v", err)
				}
			}
			wg.Wait()
		})
	}
}

func TestSelectedForkContextCleanupPanicRetainsPossession(t *testing.T) {
	process := worklifetime.NewProcess()
	owner := SelectedContractExecutionOwner{ports: &selectedContractExecutionPorts{contexts: &selectedForkContexts{process: process, recovered: true}}}
	op, err := owner.beginPreparation(worklifetime.WithProcess(context.Background(), process))
	if err != nil {
		t.Fatal(err)
	}
	cleanupCalls := 0
	p := &PreparedSelectedFork{owner: owner, operation: op, loadedSource: LoadedSelectedContractSource{
		Cleanup: func() error {
			cleanupCalls++
			if cleanupCalls == 1 {
				panic("source release failure")
			}
			return nil
		},
	}}
	func() {
		defer func() {
			if got := recover(); got != "source release failure" {
				t.Errorf("cleanup panic = %v", got)
			}
		}()
		_ = p.Close()
	}()
	if process.ActiveCount() != 1 || p.cleanupComplete {
		t.Fatalf("panic lost unsettled process possession: %d", process.ActiveCount())
	}
	if cleanupCalls != 1 || owner.ports.contexts.entries[op].retained != p || p.closeErr == nil {
		t.Fatal("panic did not retain exact cleanup responsibility and evidence")
	}
	if err := owner.RetireSelectedContexts(context.Background()); err != nil || cleanupCalls != 2 {
		t.Fatalf("retirement could not retry source cleanup: calls=%d err=%v", cleanupCalls, err)
	}
	if err := p.Close(); err != nil || cleanupCalls != 2 {
		t.Fatalf("successful source cleanup repeated: calls=%d err=%v", cleanupCalls, err)
	}
	process.Retire()
	if _, err := process.Join(context.Background()); err != nil {
		t.Fatal(err)
	}
}
