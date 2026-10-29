package channelonboarding

import (
	"context"
	"database/sql"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	"github.com/google/uuid"
)

func statementOriginalContext(t testing.TB, sqlite bool, acquire func(context.Context, func(context.Context, *sql.Tx) error) (bool, error)) context.Context {
	t.Helper()
	identity := processbinding.LeaseIdentity{LeaseID: uuid.NewString(), OwnerID: "native-statement-fixture", BootID: uuid.NewString(), RuntimeInstanceID: uuid.NewString(), Backend: processbinding.PostgresLeaseBackend}
	if sqlite {
		identity.Backend = processbinding.SQLiteLeaseBackend
	}
	var original processbinding.LeaseStamp
	committed, err := acquire(t.Context(), func(ctx context.Context, tx *sql.Tx) error {
		var err error
		original, err = generationauthority.AdvanceProcessGeneration(ctx, tx, identity, sqlite)
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
