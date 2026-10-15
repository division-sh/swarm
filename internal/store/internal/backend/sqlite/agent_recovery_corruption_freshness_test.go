package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
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

func TestSQLiteCorruptionFreshnessJoinsOriginalScopeCleanup(t *testing.T) {
	for _, cut := range []string{"rollback", "panic", "scoped_cleanup", "prepare_failure_close"} {
		t.Run(cut, func(t *testing.T) {
			path := "file:" + filepath.Join(t.TempDir(), "outer-corruption-cleanup.db")
			if cut != "prepare_failure_close" {
				path += "?_pragma=foreign_keys(1)"
			}
			control := &exitFaultConnector{path: path}
			db := sql.OpenDB(control)
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(0)
			backend, err := New(db)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := backend.Close(); err != nil {
					t.Error(err)
				}
			})
			process := sqliteFreshnessUnitProcess(t, backend)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if err := runtimeownership.SeedAgentRecovery(ctx, process); err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			switch cut {
			case "rollback", "panic":
				control.rollbackEntered, control.rollbackRelease = entered, release
			case "scoped_cleanup":
				control.constraintCleanupEntered, control.constraintCleanupRelease = entered, release
			case "prepare_failure_close":
				control.closeEntered, control.closeRelease = entered, release
			}
			primary := errors.New("original corruption callback failure")
			type result struct {
				ack       bool
				err       error
				recovered any
			}
			done := make(chan result, 1)
			go func() {
				var outcome result
				defer func() { outcome.recovered = recover(); done <- outcome }()
				outcome.ack, outcome.err = backend.RunAgentSnapshotRouteCorruptionForTest(ctx, func(context.Context, *sql.Tx) error {
					if cut == "panic" {
						panic(primary)
					}
					return primary
				})
			}()
			joined := false
			t.Cleanup(func() {
				unblock()
				if !joined {
					<-done
				}
			})
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("original corruption owner did not reach held cleanup")
			}
			if err := runtimeownership.SeedAgentRecovery(ctx, process); err == nil || !strings.Contains(err.Error(), "complete fresh reconstruction") {
				t.Fatalf("original %s ended freshness observation before cleanup joined: %v", cut, err)
			}
			unblock()
			outcome := <-done
			joined = true
			if outcome.ack {
				t.Fatal("failed original corruption owner reported acknowledged commit")
			}
			switch cut {
			case "panic":
				if outcome.recovered != primary {
					t.Fatalf("outer observation changed original panic: %v", outcome.recovered)
				}
			case "prepare_failure_close":
				if outcome.recovered != nil || outcome.err == nil || !strings.Contains(outcome.err.Error(), "requires enforced constraints") {
					t.Fatalf("outer observation changed original preparation refusal: %+v", outcome)
				}
			default:
				if outcome.recovered != nil || !errors.Is(outcome.err, primary) {
					t.Fatalf("outer observation changed original failure: %+v", outcome)
				}
			}
			if control.closes.Load() != 1 || backend.QueuedWritersForTest() != 0 {
				t.Fatalf("corruption cleanup left native connection/admission unjoined: closes=%d waiting=%d", control.closes.Load(), backend.QueuedWritersForTest())
			}
			if err := runtimeownership.SeedAgentRecovery(ctx, process); err != nil {
				t.Fatalf("joined outer and lower observations remained active: %v", err)
			}
			t.Log("original corruption scope/transport cleanup with unit seed payload; no native image or capacity claim")
		})
	}
}
