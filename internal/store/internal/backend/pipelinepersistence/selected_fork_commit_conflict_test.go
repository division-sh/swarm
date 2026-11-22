package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	runtimeactivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimebus "github.com/division-sh/swarm/internal/runtime/bus"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/division-sh/swarm/tests/support/testutil"
	"github.com/lib/pq"
)

func TestSelectedForkPublicationReturnPreservesNativeOrderingConflict(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.RunTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		return authoractivity.FenceMutationOrder(ctx, tx, authoractivity.DialectPostgres)
	}); err != nil {
		t.Fatal(err)
	}
	lease, held, err := postgresbackend.AcquireAdvisoryLockLease(ctx, backend, "selected-fork-ordering-proof")
	if err != nil || !held {
		t.Fatalf("native lease: held=%t err=%v", held, err)
	}
	t.Cleanup(func() {
		if err := lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	probe, restore, err := backend.InstallTransactionProbeForTest(transactiontest.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restore()
	ack, nativeErr := postgresbackend.RunAuthorityTransactionOutcomeWithOptions(ctx, lease.Session(), &sql.TxOptions{Isolation: sql.LevelSerializable}, func(ctx context.Context, tx *sql.Tx) error {
		story, err := authoractivity.Begin(ctx, tx, authoractivity.DialectPostgres)
		if err != nil {
			return err
		}
		if _, _, err := story.PersistedOccurredAt(ctx, "selected-fork-ordering-proof"); err != nil {
			return err
		}
		if err := backend.RunTransaction(ctx, commitCompetingAuthorStory); err != nil {
			return err
		}
		return story.Finalize(ctx)
	})
	stats := probe.Snapshot().Retained
	var conflict *pq.Error
	if ack || !postgresbackend.IsRolledBackSerializationConflict(nativeErr) || !errors.As(nativeErr, &conflict) || conflict.Code != "40001" ||
		!strings.Contains(nativeErr.Error(), "lock author activity order") || stats.Begun != 1 || stats.RollbackAttempts != 1 || stats.CommitAttempts != 0 {
		t.Fatalf("native outcome: ack=%t stats=%+v error=%v", ack, stats, nativeErr)
	}
	result, err := finishSelectedForkPostgresCommit(runtimebus.CommittedSelectedForkEvent{}, ack, nativeErr)
	var classified *runtimefailures.RunForkPublicationConflict
	if result.AppendOutcome != 0 || !errors.As(err, &classified) || !errors.Is(err, nativeErr) {
		t.Fatalf("selected publication lost conflict classification or native cause: %+v %T %v", result, err, err)
	}
}

func commitCompetingAuthorStory(ctx context.Context, tx *sql.Tx) error {
	story, err := authoractivity.Begin(ctx, tx, authoractivity.DialectPostgres)
	if err != nil {
		return err
	}
	if err := story.Record(ctx, runtimeactivity.Draft{
		Kind: runtimeactivity.KindEntityLifecycle, Transition: "created",
		SourceOwner: "entity_mutations", SourceIdentity: "competing-story", DedupKey: "competing-story",
		OccurredAt: time.Now().UTC(),
		Scope:      runtimeactivity.BundleScope("00000000-0000-0000-0000-000000000242", "bundle-v2:sha256:"+strings.Repeat("a", 64)),
		Projection: runtimeactivity.Projection{SubjectType: "entity", SubjectID: "00000000-0000-0000-0000-000000000243"},
	}); err != nil {
		return err
	}
	return story.Finalize(ctx)
}
