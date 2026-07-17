package store

import (
	"context"
	"database/sql"
	"testing"

	runtimeownership "github.com/division-sh/swarm/internal/runtime/core/ownership"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runtimereplayclaim "github.com/division-sh/swarm/internal/runtime/replayclaim"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestPostgresPublicationClaimRetainsBorrowedConnectionAfterOriginalOwnerReturns(t *testing.T) {
	_, db, cleanup := testutil.StartPostgres(t)
	t.Cleanup(cleanup)
	store := admitTestPostgresStore(t, db)

	for _, withParentClaim := range []bool{false, true} {
		name := "transaction_owned_connection"
		if withParentClaim {
			name = "parent_claim_owned_connection"
		}
		t.Run(name, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			var parent runtimeownership.Lease
			if withParentClaim {
				var claimed bool
				var err error
				parent, claimed, err = store.ClaimPipelinePublication(ctx, uuid.NewString())
				if err != nil || !claimed || parent == nil {
					t.Fatalf("claim parent publication: claimed=%v lease=%T err=%v", claimed, parent, err)
				}
				ctx = runtimereplayclaim.BindLeaseContext(ctx, parent)
			}

			childEventID := uuid.NewString()
			var child runtimeownership.Lease
			var childCtx context.Context
			if err := store.RunEventTransaction(ctx, func(txctx context.Context, _ *sql.Tx) error {
				lease, claimed, err := store.ClaimPipelinePublication(txctx, childEventID)
				if err != nil {
					return err
				}
				if !claimed || lease == nil {
					t.Fatalf("child publication claim = claimed:%v lease:%T", claimed, lease)
				}
				child = lease
				childCtx = runtimereplayclaim.BindLeaseContext(runtimepipeline.WithoutPipelineSQLTxContext(txctx), lease)
				return nil
			}); err != nil {
				t.Fatalf("RunEventTransaction: %v", err)
			}
			if parent != nil {
				if err := parent.Release(testAuthorActivityContext()); err != nil {
					t.Fatalf("release parent publication claim: %v", err)
				}
			}

			conn, ok := runtimepipeline.PipelineSQLConnFromContext(childCtx)
			if !ok || conn == nil {
				t.Fatal("child publication claim did not bind its retained connection")
			}
			var one int
			if err := conn.QueryRowContext(childCtx, `SELECT 1`).Scan(&one); err != nil || one != 1 {
				t.Fatalf("retained child connection query = %d, %v", one, err)
			}
			if err := child.Release(testAuthorActivityContext()); err != nil {
				t.Fatalf("release child publication claim: %v", err)
			}

			replacement, claimed, err := store.ClaimPipelinePublication(testAuthorActivityContext(), childEventID)
			if err != nil || !claimed || replacement == nil {
				t.Fatalf("reclaim released child publication: claimed=%v lease=%T err=%v", claimed, replacement, err)
			}
			if err := replacement.Release(testAuthorActivityContext()); err != nil {
				t.Fatalf("release replacement publication claim: %v", err)
			}
		})
	}
}
