package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
)

// Like the existing freshness unit probes, this observes real native ownership
// and cleanup, not native process possession, recovery-image or capacity proof.
func TestAgentSnapshotCorruptionObservationCoversQueuedPreBindWriter(t *testing.T) {
	for _, kind := range []string{"status", "route"} {
		for _, exit := range []string{"commit", "cancel", "panic"} {
			t.Run(kind+"/"+exit, func(t *testing.T) {
				proveAgentSnapshotQueuedObservation(t, kind, exit)
			})
		}
	}
}

func proveAgentSnapshotQueuedObservation(t *testing.T, kind, exit string) {
	t.Helper()
	b := newAgentSnapshotConnectionProbe(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := b.acquireMutation(ctx, mutationOperation{label: "original snapshot admission holder"}); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	unblock := func() { once.Do(b.releaseMutation) }
	type result struct {
		ack     bool
		err     error
		panic   any
		entered bool
	}
	done := make(chan result, 1)
	joined := false
	t.Cleanup(func() {
		unblock()
		if !joined {
			<-done
		}
	})
	primary := errors.New("original snapshot callback panic")
	run := b.RunAgentSnapshotStatusCorruptionForTest
	if kind == "route" {
		run = b.RunAgentSnapshotRouteCorruptionForTest
	}
	go func() {
		var outcome result
		defer func() {
			outcome.panic = recover()
			done <- outcome
		}()
		outcome.ack, outcome.err = run(ctx, func(context.Context, *sql.Tx) error {
			outcome.entered = true
			if exit == "panic" {
				panic(primary)
			}
			return nil
		})
	}()
	waitForQueuedMutations(t, b, 1)
	process := sqliteFreshnessUnitProcess(t, b)
	if err := runtimeownership.SeedAgentRecovery(ctx, process); err == nil || !strings.Contains(err.Error(), "complete fresh reconstruction") {
		t.Fatalf("queued original %s fault escaped the pre-bind freshness fence: %v", kind, err)
	}
	if exit == "cancel" {
		cancel()
	} else {
		unblock()
	}
	outcome := <-done
	joined = true
	unblock()
	switch exit {
	case "commit":
		if !outcome.ack || outcome.err != nil || outcome.panic != nil || !outcome.entered {
			t.Fatalf("passive observation changed native commit: %+v", outcome)
		}
	case "cancel":
		if outcome.ack || !errors.Is(outcome.err, context.Canceled) || outcome.panic != nil || outcome.entered {
			t.Fatalf("admission cancellation entered the callback or lost its cause: %+v", outcome)
		}
	case "panic":
		if outcome.ack || outcome.panic != primary || !outcome.entered {
			t.Fatalf("native panic did not propagate unchanged: %+v", outcome)
		}
	}
	requireAgentSnapshotProbeEnforced(t, b, 1)
	if err := runtimeownership.SeedAgentRecovery(t.Context(), process); err != nil {
		t.Fatalf("joined native cleanup/admission left an unbalanced write observation: %v", err)
	}
}
