package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/startupownership"
	storeownership "github.com/division-sh/swarm/internal/store/internal/startupownership"
	"github.com/google/uuid"
)

func TestAgentRecoverySeedNativeInterruptionFencesLateSuccessBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, cut := range []string{"read", "publication"} {
			t.Run(backend+"/"+cut, func(t *testing.T) {
				f := newAgentClaimProfileFixture(t, backend)
				f.claim(t)
				process, grant := agentRecoveryFixtureOwners(t, f)
				stamp, err := process.Evidence()
				if err != nil {
					t.Fatal(err)
				}
				before := profileFixtureSnapshot(t, f)
				if err := interruptAgentRecoverySeedFixture(f.ctx, f.store, process, stamp); err == nil {
					t.Fatal("native interruption was available without its exact active seed cut")
				}
				entered, unblock := holdAgentRecoverySeedCut(t, process, cut)
				result := make(chan error, 1)
				go func() { result <- startupownership.SeedAgentRecovery(f.ctx, process) }()
				awaitAgentRecoverySeedCut(t, entered)
				assertAgentRecoverySeedFaultRefusals(t, f, process, stamp, backend)
				if err := interruptAgentRecoverySeedFixture(f.ctx, f.store, process, stamp); err != nil {
					t.Fatalf("interrupt exact original retained session: %v", err)
				}
				if err := interruptAgentRecoverySeedFixture(f.ctx, f.store, process, stamp); err == nil {
					t.Fatal("native interruption was reusable")
				}
				select {
				case err := <-result:
					t.Fatalf("native interruption returned before the original read/close joined: %v", err)
				default:
				}
				unblock()
				seedErr := <-result
				if seedErr == nil || (backend == "sqlite" && !errors.Is(seedErr, sql.ErrConnDone)) {
					t.Fatalf("interrupted seed returned late success or lost its native error: %v", seedErr)
				}
				select {
				case <-process.Done():
				default:
					t.Fatal("native interruption did not fence the original process")
				}
				if _, err := grant.Evidence(); err == nil {
					t.Fatal("native interruption left its original grant live")
				}
				if err := startupownership.SeedAgentRecovery(f.ctx, process); err == nil || strings.Contains(err.Error(), "already installed") {
					t.Fatalf("terminal owner exposed an installed/partial seed instead of refusing authority: %v", err)
				}
				if err := startupownership.CheckAgentRecoverySeed(f.ctx, process); err == nil {
					t.Fatal("native original-session loss left its private seed checkable")
				}
				if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
					t.Fatal("native interruption/late refusal changed complete durable storage")
				}
				assertAgentProfileProcessNoAttempts(t, f)
				if backend == "sqlite" {
					t.Log("native loss is retained proof-connection failure, not release of the OS possession lock; original release/cleanup remains required")
				}
			})
		}
	}
}

func interruptAgentRecoverySeedFixture(ctx context.Context, selected agentFixtureFlowStore, process startupownership.ProcessCapability, expected startupownership.Authority) error {
	switch store := selected.(type) {
	case *PostgresStore:
		return storeownership.InterruptPostgresAgentRecoverySeedForTest(ctx, store.startupPostgresOwner, process, expected)
	case *SQLiteRuntimeStore:
		return storeownership.InterruptSQLiteAgentRecoverySeedForTest(ctx, store.startupSQLiteOwner, process, expected)
	default:
		return errors.New("agent recovery fault requires its exact selected native owner")
	}
}

func assertAgentRecoverySeedFaultRefusals(t *testing.T, f agentClaimProfileFixture, process startupownership.ProcessCapability, stamp startupownership.Authority, backend string) {
	t.Helper()
	foreign := newAgentClaimProfileFixture(t, backend)
	before := profileFixtureSnapshot(t, foreign)
	if err := interruptAgentRecoverySeedFixture(f.ctx, foreign.store, process, stamp); err == nil {
		t.Fatal("foreign selected owner interrupted the original held process")
	}
	wrapped := &struct {
		startupownership.ProcessCapability
	}{process}
	if err := interruptAgentRecoverySeedFixture(f.ctx, f.store, wrapped, stamp); err == nil {
		t.Fatal("substituted process capability interrupted the original session")
	}
	for _, field := range []string{"lease", "generation", "owner", "boot", "runtime", "backend"} {
		wrong := stamp
		switch field {
		case "lease":
			wrong.AuthorityID = uuid.NewString()
		case "generation":
			wrong.AuthorityGeneration++
		case "owner":
			wrong.OwnerID = "foreign"
		case "boot":
			wrong.BootID = uuid.NewString()
		case "runtime":
			wrong.RuntimeInstanceID = uuid.NewString()
		case "backend":
			wrong.Backend = "foreign"
		}
		if err := interruptAgentRecoverySeedFixture(f.ctx, f.store, process, wrong); err == nil {
			t.Fatalf("wrong %s stamp interrupted the original session", field)
		}
	}
	canceled, cancel := context.WithCancel(f.ctx)
	cancel()
	if err := interruptAgentRecoverySeedFixture(canceled, f.store, process, stamp); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled fault invocation lost its original error: %v", err)
	}
	if err := interruptAgentRecoverySeedFixture(nil, f.store, process, stamp); err == nil {
		t.Fatal("nil-context fault interrupted the original session")
	}
	if after := profileFixtureSnapshot(t, foreign); !reflect.DeepEqual(before, after) {
		t.Fatal("foreign-owner fault refusal changed its complete storage")
	}
	foreignProcess, _ := agentRecoveryFixtureOwners(t, foreign)
	if err := foreignProcess.ProveCurrent(foreign.ctx); err != nil {
		t.Fatalf("foreign-owner fault refusal fenced independent possession: %v", err)
	}
	assertAgentProfileProcessNoAttempts(t, foreign)
}

func TestAgentRecoverySeedNativeInterruptionRefusesCompletedCutBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newAgentClaimProfileFixture(t, backend)
			f.claim(t)
			process, _ := agentRecoveryFixtureOwners(t, f)
			stamp, err := process.Evidence()
			if err != nil {
				t.Fatal(err)
			}
			before := profileFixtureSnapshot(t, f)
			probe := &startupownership.AgentRecoverySeedFaultProbeForTest{InsideRead: func() {}, BeforePublication: func() {}}
			remove, err := startupownership.InstallAgentRecoverySeedFaultProbeForTest(process, probe)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(remove)
			if err := interruptAgentRecoverySeedFixture(f.ctx, f.store, process, stamp); err == nil {
				t.Fatal("installed but inactive probe allowed native interruption")
			}
			if err := startupownership.SeedAgentRecovery(f.ctx, process); err != nil {
				t.Fatal(err)
			}
			if err := interruptAgentRecoverySeedFixture(f.ctx, f.store, process, stamp); err == nil {
				t.Fatal("completed publication cut retained native interruption authority")
			}
			if err := process.ProveCurrent(f.ctx); err != nil {
				t.Fatalf("late fault invocation damaged healthy possession: %v", err)
			}
			if after := profileFixtureSnapshot(t, f); !reflect.DeepEqual(before, after) {
				t.Fatal("completed-cut fault refusal changed complete storage")
			}
			assertAgentProfileProcessNoAttempts(t, f)
		})
	}
}
