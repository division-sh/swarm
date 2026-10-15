//go:build darwin || linux

package startupownership

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
)

// Only the image payload is a unit probe. Binding, write/read admission,
// transaction outcomes, possession and cleanup use the original native owners.
// Complete real-image/count proofs live in runtimepersistence, not this witness.
type nativeFreshnessBoundarySession struct {
	runtimeownership.RetainedSession
	postgres *postgresSession
	sqlite   *sqliteSession
	reads    atomic.Int32
}

func (s *nativeFreshnessBoundarySession) AgentRecoveryWriteObserver() *runtimeownership.AgentRecoveryWriteObserver {
	if s.postgres != nil {
		return s.postgres.AgentRecoveryWriteObserver()
	}
	return s.sqlite.AgentRecoveryWriteObserver()
}

func (s *nativeFreshnessBoundarySession) ReadAgentRecoverySeed(context.Context) (runtimeownership.AgentRecoverySeedRead, error) {
	s.reads.Add(1)
	stamp, err := s.Authority()
	if err != nil {
		return runtimeownership.AgentRecoverySeedRead{}, err
	}
	plan, err := agenttopology.EmptySourceSetPlan()
	return runtimeownership.AgentRecoverySeedRead{Process: stamp, SourceHead: plan, InProgressByProfile: map[string]int{}}, err
}

func newNativeFreshnessBoundarySession(t *testing.T, backend string, enforceConstraints ...bool) *nativeFreshnessBoundarySession {
	t.Helper()
	if backend == "postgres" {
		session, _, _ := newPostgresGenerationMonitor(t, "")
		return &nativeFreshnessBoundarySession{RetainedSession: session, postgres: session}
	}
	session, _ := newSQLiteProofSession(t, 2, enforceConstraints...)
	return &nativeFreshnessBoundarySession{RetainedSession: session, sqlite: session}
}

func (s *nativeFreshnessBoundarySession) write(ctx context.Context, retained bool, operation func(context.Context, *sql.Tx) error) (bool, error) {
	if s.postgres != nil {
		if retained {
			return s.postgres.lease.RunTransactionOutcome(ctx, operation)
		}
		return s.postgres.owner.backend.RunTransactionOutcome(ctx, operation)
	}
	return s.sqlite.owner.backend.RunTransactionOutcome(ctx, "seed freshness boundary", operation)
}

func (s *nativeFreshnessBoundarySession) read(ctx context.Context, operation func(context.Context, *sql.Tx) error) error {
	if s.postgres != nil {
		return postgresbackend.RunAuthorityReadTransaction(ctx, s.postgres.lease.Session(), operation)
	}
	return s.sqlite.owner.backend.RunReadTransaction(ctx, operation)
}

func TestAgentRecoveryNativePreBindWriteMustJoinBothStores(t *testing.T) {
	for _, entrance := range []struct {
		backend    string
		retained   bool
		corruption string
	}{{"sqlite", false, ""}, {"postgres", false, ""}, {"postgres", true, ""}, {"sqlite", false, "route"}, {"sqlite", false, "status"}} {
		for _, rollback := range []bool{false, true} {
			name := fmt.Sprintf("%s/retained=%t/rollback=%t", entrance.backend, entrance.retained, rollback)
			if entrance.corruption != "" {
				name += "/" + entrance.corruption
			}
			t.Run(name, func(t *testing.T) {
				session := newNativeFreshnessBoundarySession(t, entrance.backend, entrance.corruption != "")
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				primary := errors.New("native freshness rollback witness")
				type result struct {
					ack bool
					err error
				}
				done := make(chan result, 1)
				go func() {
					operation := func(context.Context, *sql.Tx) error {
						close(entered)
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-release:
						}
						if rollback {
							return primary
						}
						return nil
					}
					var ack bool
					var err error
					if entrance.corruption != "" {
						run := session.sqlite.owner.backend.RunAgentSnapshotStatusCorruptionForTest
						if entrance.corruption == "route" {
							run = session.sqlite.owner.backend.RunAgentSnapshotRouteCorruptionForTest
						}
						ack, err = run(ctx, operation)
					} else {
						ack, err = session.write(ctx, entrance.retained, operation)
					}
					done <- result{ack, err}
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
				case outcome := <-done:
					joined = true
					t.Fatalf("native pre-bind owner refused before the held cut: %+v", outcome)
				case <-ctx.Done():
					t.Fatal("native pre-bind write did not enter")
				}
				process, err := runtimeownership.NewProcessCapability(session)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					unblock()
					if err := process.Release(context.WithoutCancel(ctx)); err != nil {
						t.Error(err)
					}
				})
				if err := runtimeownership.SeedAgentRecovery(ctx, process); err == nil || !strings.Contains(err.Error(), "complete fresh reconstruction") || session.reads.Load() != 0 {
					t.Fatalf("binding forgot its already-active native write: %v reads=%d", err, session.reads.Load())
				}
				unblock()
				outcome := <-done
				joined = true
				if outcome.ack != !rollback || rollback && !errors.Is(outcome.err, primary) || !rollback && outcome.err != nil {
					t.Fatalf("observer changed native acknowledgment/error: %+v", outcome)
				}
				if err := runtimeownership.SeedAgentRecovery(ctx, process); err != nil || session.reads.Load() != 1 {
					t.Fatalf("joined native write prevented explicit reconstruction: %v reads=%d", err, session.reads.Load())
				}
				if err := session.read(ctx, func(context.Context, *sql.Tx) error { return nil }); err != nil {
					t.Fatal(err)
				}
				if err := process.ProveCurrent(ctx); err != nil {
					t.Fatal(err)
				}
				if err := runtimeownership.SeedAgentRecovery(ctx, process); err == nil || !strings.Contains(err.Error(), "already installed") || session.reads.Load() != 1 {
					t.Fatalf("read-only native inspection/proof invalidated the seed: %v reads=%d", err, session.reads.Load())
				}
				t.Log("real pre-bind native write and outcome; synthetic seed payload only, no complete-image or capacity claim")
			})
		}
	}
}
