package startupownership

import (
	"context"
	"testing"
	"time"

	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

type terminalReadbackObserver struct {
	results chan runtimestartupownership.TerminalResult
}

func (o terminalReadbackObserver) SelectedStoreSessionTerminal(result runtimestartupownership.TerminalResult) {
	o.results <- result
}

func TestTerminalFencePrecedesStalledReadbackAndJoins(t *testing.T) {
	observer := terminalReadbackObserver{results: make(chan runtimestartupownership.TerminalResult, 2)}
	entered, resume, joined := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(resume)
	go func() {
		defer close(joined)
		reportTerminalWithReadback(observer, 20*time.Millisecond, func(ctx context.Context) runtimestartupownership.TerminalResult {
			close(entered)
			<-ctx.Done()
			<-resume // Model I/O that does not promptly service its context.
			return runtimestartupownership.TerminalResult{Cause: runtimestartupownership.TerminalOwnershipSuperseded, SuccessorAuthorityID: "late"}
		})
	}()
	<-entered
	select {
	case result := <-observer.results:
		if result.Cause != runtimestartupownership.TerminalOwnershipUnprovable {
			t.Fatalf("initial safety decision=%#v", result)
		}
	default:
		t.Fatal("local safety decision waited for lineage")
	}
	select {
	case <-joined:
		t.Fatal("outstanding lineage worker was abandoned")
	case <-time.After(40 * time.Millisecond):
	}
	resume <- struct{}{}
	<-joined
	if result := <-observer.results; result.Cause != runtimestartupownership.TerminalOwnershipUnprovable {
		t.Fatalf("late expired readback attributed takeover: %#v", result)
	}
}
