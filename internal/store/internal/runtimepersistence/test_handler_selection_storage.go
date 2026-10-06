package runtimepersistence

import (
	"context"
	"database/sql"
	"fmt"

	storedelivery "github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/google/uuid"
)

// HandlerSelectionStorageEvidence retains exact stored columns rather than
// normalizing them through the live delivery or operator-trace projection.
type HandlerSelectionStorageEvidence = storedelivery.FixtureHandlerSelectionStorage

func ReadHandlerSelectionStorageForTest(ctx context.Context, selected any, eventID string) ([]HandlerSelectionStorageEvidence, error) {
	id, err := uuid.Parse(eventID)
	if err != nil || id == uuid.Nil || id.String() != eventID {
		return nil, fmt.Errorf("handler selection storage requires an exact canonical event identity")
	}
	if _, err := eventFixtureDialectForTest(selected); err != nil {
		return nil, err
	}
	var evidence []HandlerSelectionStorageEvidence
	read := func(ctx context.Context, tx *sql.Tx) error {
		var err error
		evidence, err = storedelivery.FixtureHandlerSelectionStorageTx(ctx, tx, eventID)
		return err
	}
	switch owner := selected.(type) {
	case *PostgresStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	case *SQLiteRuntimeStore:
		err = owner.backend.RunReadTransaction(ctx, read)
	}
	if err != nil {
		return nil, err
	}
	return evidence, nil
}
