package apiidempotency

import (
	"context"
	"database/sql"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	"github.com/google/uuid"
)

// Keep the original lease outside the pool whose release behavior is tested.
func statementOriginalContext(t testing.TB, dsn string) context.Context {
	t.Helper()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	backend, err := postgresbackend.New(db)
	if err != nil {
		t.Fatal(err)
	}
	lease, acquired, err := postgresbackend.AcquireAdvisoryLockLease(t.Context(), backend, "swarm:runtime:shared-store-owner")
	if err != nil || !acquired {
		t.Fatalf("retain original statement process: acquired=%v err=%v", acquired, err)
	}
	t.Cleanup(func() {
		if err := lease.Release(context.Background()); err != nil {
			t.Error(err)
		}
	})
	var original processbinding.LeaseStamp
	committed, err := lease.RunProcessAcquisitionOutcome(t.Context(), func(ctx context.Context, tx *sql.Tx) error {
		var err error
		original, err = generationauthority.AdvanceProcessGeneration(ctx, tx, processbinding.LeaseIdentity{
			LeaseID: uuid.NewString(), OwnerID: "native-statement-fixture", BootID: uuid.NewString(),
			RuntimeInstanceID: uuid.NewString(), Backend: processbinding.PostgresLeaseBackend,
		}, false)
		return err
	})
	if err != nil || !committed {
		t.Fatalf("acquire original statement process: committed=%v err=%v", committed, err)
	}
	ctx, err := generationauthority.MutationContext(t.Context(), original, false)
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}
