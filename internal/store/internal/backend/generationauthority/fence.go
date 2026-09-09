// Package generationauthority owns the transaction fence shared by generation
// grant transitions and the mutations they authorize.
package generationauthority

import (
	"context"
	"database/sql"

	"github.com/division-sh/swarm/internal/store/internal/backend/authoractivity"
)

// FenceMutation uses the already-existing selected-store mutation order, not a
// historical grant row. Acquire before domain locks and read current grant facts
// afterwards. All append-only grant successors, including bulk retirement, use
// this coordinate. Reacquisition in the same transaction is harmless.
func FenceMutation(ctx context.Context, tx *sql.Tx, sqlite bool) error {
	dialect := authoractivity.DialectPostgres
	if sqlite {
		dialect = authoractivity.DialectSQLite
	}
	return authoractivity.FenceMutationOrder(ctx, tx, dialect)
}
