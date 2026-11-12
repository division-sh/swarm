package channelonboarding

import (
	"context"
	"database/sql"
	"fmt"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/google/uuid"
)

// ReadSessionConnectionForTest projects only the opaque retained coordinate.
func (s *PostgresOwner) ReadSessionConnectionForTest(ctx context.Context, operationID string) (string, error) {
	if uuid.Validate(operationID) != nil {
		return "", fmt.Errorf("session connection observation requires its original transaction and exact operation")
	}
	var connection string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		op, found, err := loadOperation(s, ctx, tx, dialectPostgres, operationID, false)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrNotFound
		}
		if op.OperationID != operationID || op.Posture != domain.ActivationSessionConnection || uuid.Validate(op.SessionConnectionID) != nil {
			return fmt.Errorf("session connection observation found contradictory retained identity")
		}
		connection = op.SessionConnectionID
		return ctx.Err()
	})
	return connection, err
}

func (s *SQLiteOwner) ReadSessionConnectionForTest(ctx context.Context, operationID string) (string, error) {
	if uuid.Validate(operationID) != nil {
		return "", fmt.Errorf("session connection observation requires its original transaction and exact operation")
	}
	var connection string
	err := s.backend.RunReadTransaction(ctx, func(ctx context.Context, tx *sql.Tx) error {
		op, found, err := loadOperation(s, ctx, tx, dialectSQLite, operationID, false)
		if err != nil {
			return err
		}
		if !found {
			return domain.ErrNotFound
		}
		if op.OperationID != operationID || op.Posture != domain.ActivationSessionConnection || uuid.Validate(op.SessionConnectionID) != nil {
			return fmt.Errorf("session connection observation found contradictory retained identity")
		}
		connection = op.SessionConnectionID
		return ctx.Err()
	})
	return connection, err
}
