package channelonboarding

import (
	"context"
	"database/sql"
	"fmt"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/google/uuid"
)

// readSessionConnectionForTest projects one retained coordinate from the same
// operation decoder, under the original read-only inspection transaction.
func readSessionConnectionForTest(ctx context.Context, owner providerFactReader, tx *sql.Tx, postgres bool, operationID string) (string, error) {
	if tx == nil || uuid.Validate(operationID) != nil {
		return "", fmt.Errorf("session connection observation requires its original transaction and exact operation")
	}
	d := dialectSQLite
	if postgres {
		d = dialectPostgres
	}
	op, found, err := loadOperation(owner, ctx, tx, d, operationID, false)
	if err != nil {
		return "", err
	}
	if !found {
		return "", domain.ErrNotFound
	}
	if op.OperationID != operationID || op.Posture != domain.ActivationSessionConnection || uuid.Validate(op.SessionConnectionID) != nil {
		return "", fmt.Errorf("session connection observation found contradictory retained identity")
	}
	return op.SessionConnectionID, ctx.Err()
}
