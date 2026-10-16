package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

// This seed is a unit observation of real native cleanup, not a claim of native
// possession or a canonical recovery image. Those have separate real-store cuts.
type postgresFreshnessUnitSession struct {
	runtimeownership.RetainedSession
	writes *runtimeownership.AgentRecoveryWriteObserver
	value  runtimeownership.AgentRecoverySeedRead
}

func (s *postgresFreshnessUnitSession) Authority() (runtimeownership.Authority, error) {
	return s.value.Process, nil
}
func (*postgresFreshnessUnitSession) ProveCurrent(context.Context) error { return nil }
func (*postgresFreshnessUnitSession) MonitorProveCurrent(context.Context, time.Duration) error {
	return nil
}
func (*postgresFreshnessUnitSession) InstallTerminalOwner(runtimeownership.SessionTerminalOwner, time.Duration) error {
	return nil
}
func (*postgresFreshnessUnitSession) Release(context.Context) error { return nil }
func (s *postgresFreshnessUnitSession) AgentRecoveryWriteObserver() *runtimeownership.AgentRecoveryWriteObserver {
	return s.writes
}
func (s *postgresFreshnessUnitSession) ReadAgentRecoverySeed(context.Context) (runtimeownership.AgentRecoverySeedRead, error) {
	return s.value, nil
}

func postgresFreshnessUnitProcess(t *testing.T, backend *Backend) runtimeownership.ProcessCapability {
	t.Helper()
	plan, err := agenttopology.EmptySourceSetPlan()
	if err != nil {
		t.Fatal(err)
	}
	session := &postgresFreshnessUnitSession{writes: backend.AgentRecoveryWriteObserver(), value: runtimeownership.AgentRecoverySeedRead{
		Process: runtimeownership.Authority{AuthorityID: uuid.NewString(), AuthorityGeneration: 1, OwnerID: "cleanup-unit",
			BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(), Backend: processbinding.PostgresLeaseBackend},
		SourceHead: plan, InProgressByProfile: map[string]int{},
	}}
	process, err := runtimeownership.NewProcessCapability(session)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := process.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return process
}

func TestPostgresFreshnessObservationJoinsOriginalCleanup(t *testing.T) {
	for _, retained := range []bool{false, true} {
		for _, cut := range []string{"rollback", "uncertain_commit_close"} {
			t.Run(fmt.Sprintf("retained=%t/%s", retained, cut), func(t *testing.T) {
				backend, control := newExitProbe(t)
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				var session *SessionAuthority
				if retained {
					conn, err := backend.Conn(ctx)
					if err != nil {
						t.Fatal(err)
					}
					session, err = backend.NewSessionAuthority(conn)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = session.Release() })
				}
				process := postgresFreshnessUnitProcess(t, backend)
				if err := runtimeownership.SeedAgentRecovery(ctx, process); err != nil {
					t.Fatal(err)
				}
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				primary := errors.New("original postgres freshness callback")
				uncertain := errors.New("original uncertain postgres commit")
				if cut == "rollback" {
					control.rollbackEntered, control.rollbackRelease = entered, release
				} else {
					control.commitFailure = uncertain
					control.closeEntered, control.closeRelease = entered, release
				}
				type result struct {
					ack bool
					err error
				}
				done := make(chan result, 1)
				go func() {
					operation := func(context.Context, *sql.Tx) error {
						if cut == "rollback" {
							return primary
						}
						return nil
					}
					var ack bool
					var err error
					if retained {
						ack, err = RunAuthorityTransactionOutcome(ctx, session, operation)
					} else {
						ack, err = backend.RunTransactionOutcome(ctx, operation)
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
				case <-ctx.Done():
					t.Fatal("original native cleanup did not reach held cut")
				}
				if err := runtimeownership.SeedAgentRecovery(ctx, process); err == nil || !strings.Contains(err.Error(), "complete fresh reconstruction") {
					t.Fatalf("write observation ended before original %s joined: %v", cut, err)
				}
				unblock()
				outcome := <-done
				joined = true
				wantErr := primary
				if cut != "rollback" {
					wantErr = uncertain
				}
				if outcome.ack || !errors.Is(outcome.err, wantErr) {
					t.Fatalf("freshness changed original native disposition: %+v want=%v", outcome, wantErr)
				}
				if retained && cut != "rollback" && !session.fenced.Load() {
					t.Fatal("observer erased uncertain native session fencing")
				}
				if err := runtimeownership.SeedAgentRecovery(ctx, process); err != nil {
					t.Fatalf("joined original cleanup left stale active observation: %v", err)
				}
				t.Log("real original native cleanup; unit seed only, no native image/possession/capacity claim")
			})
		}
	}
}

func TestPostgresFreshnessObservesPoolWaitBeforeProcessBind(t *testing.T) {
	backend, _ := newExitProbe(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	conn, err := backend.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	unblock := func() { once.Do(func() { _ = conn.Close() }) }
	defer unblock()
	before := backend.db.Stats().WaitCount
	type result struct {
		ack bool
		err error
	}
	done := make(chan result, 1)
	go func() {
		ack, err := backend.RunTransactionOutcome(ctx, func(context.Context, *sql.Tx) error { return nil })
		done <- result{ack, err}
	}()
	joined := false
	t.Cleanup(func() {
		unblock()
		if !joined {
			<-done
		}
	})
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for backend.db.Stats().WaitCount == before {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("native writer did not reach pool admission")
		}
	}
	process := postgresFreshnessUnitProcess(t, backend)
	if err := runtimeownership.SeedAgentRecovery(ctx, process); err == nil || !strings.Contains(err.Error(), "complete fresh reconstruction") {
		t.Fatalf("native pool admission preceded the pre-bind write observation: %v", err)
	}
	unblock()
	outcome := <-done
	joined = true
	if !outcome.ack || outcome.err != nil {
		t.Fatalf("passive observation changed queued native outcome: %+v", outcome)
	}
	if err := runtimeownership.SeedAgentRecovery(ctx, process); err != nil {
		t.Fatalf("joined pool wait left stale active state: %v", err)
	}
}
