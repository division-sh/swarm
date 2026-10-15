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

func TestSQLiteCorruptionFreshnessObservesOuterQueuedWriter(t *testing.T) {
	for _, route := range []bool{false, true} {
		name := "status"
		if route {
			name = "route"
		}
		for _, cut := range []string{"prebind", "installed", "cancel"} {
			t.Run(name+"/"+cut, func(t *testing.T) {
				backend := newAgentSnapshotConnectionProbe(t)
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				var process runtimeownership.ProcessCapability
				if cut != "prebind" {
					process = sqliteFreshnessUnitProcess(t, backend)
					if err := runtimeownership.SeedAgentRecovery(ctx, process); err != nil {
						t.Fatal(err)
					}
				}
				if err := backend.acquireMutation(ctx, mutationOperation{label: "outer corruption freshness holder"}); err != nil {
					t.Fatal(err)
				}
				var once sync.Once
				unblock := func() { once.Do(backend.releaseMutation) }
				defer unblock()
				type result struct {
					ack bool
					err error
				}
				done := make(chan result, 1)
				go func() {
					run := backend.RunAgentSnapshotStatusCorruptionForTest
					if route {
						run = backend.RunAgentSnapshotRouteCorruptionForTest
					}
					ack, err := run(ctx, func(context.Context, *sql.Tx) error { return nil })
					done <- result{ack, err}
				}()
				joined := false
				t.Cleanup(func() {
					cancel()
					unblock()
					if !joined {
						<-done
					}
				})
				waitForQueuedMutations(t, backend, 1)
				if process == nil {
					process = sqliteFreshnessUnitProcess(t, backend)
				}
				if err := runtimeownership.SeedAgentRecovery(ctx, process); err == nil || !strings.Contains(err.Error(), "complete fresh reconstruction") {
					t.Fatalf("outer corruption writer is queued before its lower transaction but private seeding was not refused: %v", err)
				}
				if cut == "cancel" {
					cancel()
				} else {
					unblock()
				}
				outcome := <-done
				joined = true
				if cut == "cancel" {
					if outcome.ack || !errors.Is(outcome.err, context.Canceled) {
						t.Fatalf("outer admission cancellation changed native disposition: %+v", outcome)
					}
				} else if !outcome.ack || outcome.err != nil {
					t.Fatalf("outer corruption owner changed native disposition: %+v", outcome)
				}
				if err := runtimeownership.SeedAgentRecovery(context.WithoutCancel(ctx), process); err != nil {
					t.Fatalf("joined outer/lower scopes did not balance: %v", err)
				}
				requireAgentSnapshotProbeEnforced(t, backend, 1)
				t.Log("original outer admission and native corruption owner; unit seed payload only, no native image or capacity claim")
			})
		}
	}
}
