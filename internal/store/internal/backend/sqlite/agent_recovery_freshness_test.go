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

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

// This unit seed only observes the native cleanup boundary; it is not native
// possession/image evidence. Runtimepersistence tests supply that separate proof.
type sqliteFreshnessUnitSession struct {
	runtimeownership.RetainedSession
	writes *runtimeownership.AgentRecoveryWriteObserver
	value  runtimeownership.AgentRecoverySeedRead
}

func (s *sqliteFreshnessUnitSession) Authority() (runtimeownership.Authority, error) {
	return s.value.Process, nil
}
func (*sqliteFreshnessUnitSession) ProveCurrent(context.Context) error { return nil }
func (*sqliteFreshnessUnitSession) MonitorProveCurrent(context.Context, time.Duration) error {
	return nil
}
func (*sqliteFreshnessUnitSession) InstallTerminalOwner(runtimeownership.SessionTerminalOwner, time.Duration) error {
	return nil
}
func (*sqliteFreshnessUnitSession) Release(context.Context) error { return nil }
func (s *sqliteFreshnessUnitSession) AgentRecoveryWriteObserver() *runtimeownership.AgentRecoveryWriteObserver {
	return s.writes
}
func (s *sqliteFreshnessUnitSession) ReadAgentRecoverySeed(context.Context) (runtimeownership.AgentRecoverySeedRead, error) {
	return s.value, nil
}

func sqliteFreshnessUnitProcess(t *testing.T, backend *Backend) runtimeownership.ProcessCapability {
	t.Helper()
	plan, err := agenttopology.EmptySourceSetPlan()
	if err != nil {
		t.Fatal(err)
	}
	session := &sqliteFreshnessUnitSession{writes: backend.AgentRecoveryWriteObserver(), value: runtimeownership.AgentRecoverySeedRead{
		Process: runtimeownership.Authority{AuthorityID: uuid.NewString(), AuthorityGeneration: 1, OwnerID: "cleanup-unit",
			BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(), Backend: processbinding.SQLiteLeaseBackend},
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

type freshnessCleanupScope struct {
	entered, release chan struct{}
	prepareErr       error
}

func (s *freshnessCleanupScope) prepare(context.Context, *sql.Conn) error { return s.prepareErr }
func (s *freshnessCleanupScope) cleanup(*sql.Conn) error {
	close(s.entered)
	<-s.release
	return nil
}

func TestSQLiteFreshnessObservationJoinsOriginalCleanup(t *testing.T) {
	for _, cut := range []string{"rollback", "uncertain_commit_close", "scope_prepare_failure", "scope_rollback_cleanup"} {
		t.Run(cut, func(t *testing.T) {
			control := &exitFaultConnector{path: filepath.Join(t.TempDir(), "freshness.db")}
			db := sql.OpenDB(control)
			t.Cleanup(func() { _ = db.Close() })
			backend, err := New(db)
			if err != nil {
				t.Fatal(err)
			}
			process := sqliteFreshnessUnitProcess(t, backend)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if err := runtimeownership.SeedAgentRecovery(ctx, process); err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			primary := errors.New("original sqlite freshness callback")
			var scope *freshnessCleanupScope
			switch cut {
			case "rollback":
				control.rollbackEntered, control.rollbackRelease = entered, release
			case "uncertain_commit_close":
				control.commitAfterError.Store(true)
				control.closeEntered, control.closeRelease = entered, release
			default:
				scope = &freshnessCleanupScope{entered: entered, release: release}
				if cut == "scope_prepare_failure" {
					scope.prepareErr = primary
				}
			}
			type result struct {
				ack bool
				err error
			}
			done := make(chan result, 1)
			go func() {
				operation := func(context.Context, *sql.Tx) error {
					if cut == "uncertain_commit_close" {
						return nil
					}
					return primary
				}
				var ack bool
				var err error
				if scope != nil {
					ack, err = backend.runTransactionWithConnectionScope(ctx, nil, 0, scope, operation)
				} else {
					ack, err = backend.RunTransactionOutcome(ctx, "freshness cleanup", operation)
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
			if cut == "uncertain_commit_close" {
				wantErr = errInjectedCommit
			}
			if outcome.ack || !errors.Is(outcome.err, wantErr) {
				t.Fatalf("freshness changed original native disposition: %+v want=%v", outcome, wantErr)
			}
			if err := runtimeownership.SeedAgentRecovery(ctx, process); err != nil {
				t.Fatalf("joined original cleanup left stale active observation: %v", err)
			}
			t.Log("real original native cleanup; unit seed only, no native image/possession/capacity claim")
		})
	}
}

func TestSQLiteFreshnessObservesQueuedWriteBeforeProcessBind(t *testing.T) {
	backend := newTransactionTestBackend(t, filepath.Join(t.TempDir(), "prebind-queue.db"))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := backend.acquireMutation(ctx, mutationOperation{label: "freshness queue holder"}); err != nil {
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
		ack, err := backend.RunTransactionOutcome(ctx, "freshness queued writer", func(context.Context, *sql.Tx) error { return nil })
		done <- result{ack, err}
	}()
	joined := false
	t.Cleanup(func() {
		unblock()
		if !joined {
			<-done
		}
	})
	waitForQueuedMutations(t, backend, 1)
	process := sqliteFreshnessUnitProcess(t, backend)
	if err := runtimeownership.SeedAgentRecovery(ctx, process); err == nil || !strings.Contains(err.Error(), "complete fresh reconstruction") {
		t.Fatalf("native mutation admission preceded the pre-bind write observation: %v", err)
	}
	unblock()
	outcome := <-done
	joined = true
	if !outcome.ack || outcome.err != nil {
		t.Fatalf("passive observation changed queued native writer outcome: %+v", outcome)
	}
	if err := runtimeownership.SeedAgentRecovery(ctx, process); err != nil {
		t.Fatalf("joined queue left stale active state: %v", err)
	}
}
