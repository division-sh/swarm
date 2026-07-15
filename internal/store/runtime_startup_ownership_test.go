package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	runtimestartupownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func testStartupAcquireRequest(ownerID string) runtimestartupownership.AcquireRequest {
	return runtimestartupownership.AcquireRequest{OwnerID: ownerID, BootID: uuid.NewString(), BundleFingerprint: "test-bundle"}
}

type startupAuthorityParityStore interface {
	runtimestartupownership.Store
	runtimestartupownership.Recorder
}

func TestRuntimeStartupAuthorityTransitionsPersistWithBackendParity(t *testing.T) {
	tests := []struct {
		name  string
		store func(*testing.T) (startupAuthorityParityStore, *sql.DB)
	}{
		{
			name: "postgres",
			store: func(t *testing.T) (startupAuthorityParityStore, *sql.DB) {
				_, db, _ := testutil.StartPostgres(t)
				return &PostgresStore{DB: db}, db
			},
		},
		{
			name: "sqlite",
			store: func(t *testing.T) (startupAuthorityParityStore, *sql.DB) {
				store := newBootstrappedSQLiteRuntimeStoreForTest(t)
				return store, store.DB
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store, db := tc.store(t)
			lease, err := store.AcquireRuntimeStartupOwnership(ctx, testStartupAcquireRequest("owner-a"))
			if err != nil {
				t.Fatalf("AcquireRuntimeStartupOwnership: %v", err)
			}
			t.Cleanup(func() { _ = lease.Release(context.Background()) })
			active, err := lease.Authority()
			if err != nil {
				t.Fatalf("Authority: %v", err)
			}
			probeAuthority := runtimeeffects.Authority{
				Kind: runtimeeffects.AuthorityStartupProbe, ID: uuid.NewString(), ExecutionOwner: active.OwnerID,
				LeaseExpiresAt: time.Now().UTC().Add(time.Hour), FenceGeneration: active.Generation,
				ExecutionMode: runtimeeffects.ExecutionModeLive,
				StartupProbe: runtimeeffects.StartupProbeAuthority{
					ProbeID: uuid.NewString(), StartupAuthorityID: active.AuthorityID, StartupStateVersion: active.StateVersion,
					ActorID: "agent-a", ExecutionKind: "normal_agent", ExecutionAuthorityID: active.AuthorityID,
				},
			}
			probeAuthority.ID = probeAuthority.StartupProbe.ProbeID
			probeCurrent := func() (bool, error) {
				switch store.(type) {
				case *PostgresStore:
					return externalEffectAuthorityCurrentPostgres(ctx, db, probeAuthority)
				case *SQLiteRuntimeStore:
					return externalEffectAuthorityCurrentSQLite(ctx, db, probeAuthority)
				default:
					return false, nil
				}
			}
			if current, err := probeCurrent(); err != nil || !current {
				t.Fatalf("initial startup probe authority current=%v err=%v, want true", current, err)
			}
			if _, err := lease.MarkProbesSettled(ctx, nil); err != nil {
				t.Fatalf("MarkProbesSettled: %v", err)
			}
			if current, err := probeCurrent(); err != nil || current {
				t.Fatalf("superseded startup probe authority current=%v err=%v, want false", current, err)
			}
			if _, err := lease.AdmitExecution(ctx); err != nil {
				t.Fatalf("AdmitExecution: %v", err)
			}
			first, err := lease.PrepareHandoff(ctx, runtimestartupownership.HandoffRequest{
				CandidateOwnerID: "owner-b", CandidateBootID: uuid.NewString(), CandidateBundleFingerprint: "bundle-b",
			})
			if err != nil {
				t.Fatalf("PrepareHandoff first: %v", err)
			}
			if _, err := first.MarkProbesSettled(ctx, []string{uuid.NewString()}); err != nil {
				t.Fatalf("first MarkProbesSettled: %v", err)
			}
			committed, err := first.Commit(ctx)
			if err != nil {
				t.Fatalf("first Commit: %v", err)
			}
			finalized, err := first.Finalize(ctx)
			if err != nil {
				t.Fatalf("first Finalize: %v", err)
			}
			if err := store.RecordRuntimeStartupAuthorityTransition(ctx, &committed, finalized); err == nil || !strings.Contains(err.Error(), "compare-and-set predecessor mismatch") {
				t.Fatalf("stale transition error = %v, want exact predecessor rejection", err)
			}
			second, err := lease.PrepareHandoff(ctx, runtimestartupownership.HandoffRequest{
				CandidateOwnerID: "owner-c", CandidateBootID: uuid.NewString(), CandidateBundleFingerprint: "bundle-c",
			})
			if err != nil {
				t.Fatalf("PrepareHandoff second: %v", err)
			}
			restored, err := second.Rollback(ctx)
			if err != nil {
				t.Fatalf("second Rollback: %v", err)
			}
			var count int
			var ordinal uint64
			var state string
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*),MAX(transition_ordinal) FROM runtime_startup_authority_facts WHERE lease_authority_id=$1`, restored.LeaseAuthorityID).Scan(&count, &ordinal); err != nil {
				if err := db.QueryRowContext(ctx, `SELECT COUNT(*),MAX(transition_ordinal) FROM runtime_startup_authority_facts WHERE lease_authority_id=?`, restored.LeaseAuthorityID).Scan(&count, &ordinal); err != nil {
					t.Fatalf("query transition facts: %v", err)
				}
			}
			if err := db.QueryRowContext(ctx, `SELECT state FROM runtime_startup_authority_facts WHERE lease_authority_id=$1 ORDER BY transition_ordinal DESC LIMIT 1`, restored.LeaseAuthorityID).Scan(&state); err != nil {
				if err := db.QueryRowContext(ctx, `SELECT state FROM runtime_startup_authority_facts WHERE lease_authority_id=? ORDER BY transition_ordinal DESC LIMIT 1`, restored.LeaseAuthorityID).Scan(&state); err != nil {
					t.Fatalf("query transition head: %v", err)
				}
			}
			if count != 10 || ordinal != 10 || state != string(runtimestartupownership.StateActive) {
				t.Fatalf("transition facts count=%d ordinal=%d state=%s, want 10/10/active", count, ordinal, state)
			}
		})
	}
}

func TestPostgresStore_AcquireRuntimeStartupOwnership_DeniesCompetingOwner(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}

	lease1, err := pg.AcquireRuntimeStartupOwnership(context.Background(), testStartupAcquireRequest("runtime-1"))
	if err != nil {
		t.Fatalf("AcquireRuntimeStartupOwnership(runtime-1): %v", err)
	}
	t.Cleanup(func() { _ = lease1.Release(context.Background()) })

	lease2, err := pg.AcquireRuntimeStartupOwnership(context.Background(), testStartupAcquireRequest("runtime-2"))
	if lease2 != nil {
		t.Fatalf("AcquireRuntimeStartupOwnership(runtime-2) lease = %#v, want nil", lease2)
	}
	if err == nil || !strings.Contains(err.Error(), "shared runtime store already owned by another runtime instance") {
		t.Fatalf("AcquireRuntimeStartupOwnership(runtime-2) error = %v, want explicit ownership denial", err)
	}
}

func TestPostgresStore_AcquireRuntimeStartupOwnership_ReleaseAllowsSuccessor(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	pg := &PostgresStore{DB: db}

	lease1, err := pg.AcquireRuntimeStartupOwnership(context.Background(), testStartupAcquireRequest("runtime-1"))
	if err != nil {
		t.Fatalf("AcquireRuntimeStartupOwnership(runtime-1): %v", err)
	}
	if err := lease1.Release(context.Background()); err != nil {
		t.Fatalf("Release(runtime-1): %v", err)
	}

	lease2, err := pg.AcquireRuntimeStartupOwnership(context.Background(), testStartupAcquireRequest("runtime-2"))
	if err != nil {
		t.Fatalf("AcquireRuntimeStartupOwnership(runtime-2): %v", err)
	}
	if err := lease2.Release(context.Background()); err != nil {
		t.Fatalf("Release(runtime-2): %v", err)
	}
}
