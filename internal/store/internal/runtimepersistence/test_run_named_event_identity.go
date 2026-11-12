package runtimepersistence

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Read the physical identity at the caller's existing run/name cut. An
// ambiguous fixture is a refusal, never permission to choose an arbitrary row.
func ReadRunNamedEventIdentityStorageForTest(ctx context.Context, selected any, runID, eventName string) (string, error) {
	if err := validateChannelObservationOwner(selected); err != nil {
		return "", err
	}
	id, err := uuid.Parse(runID)
	if err != nil || id == uuid.Nil || id.String() != runID || eventName == "" || strings.TrimSpace(eventName) != eventName {
		return "", fmt.Errorf("run event identity read requires a canonical run and exact event name")
	}
	switch store := selected.(type) {
	case *PostgresStore:
		return store.eventPostgresOwner.ReadRunNamedEventIdentityForTest(ctx, runID, eventName)
	case *SQLiteRuntimeStore:
		return store.eventSQLiteOwner.ReadRunNamedEventIdentityForTest(ctx, runID, eventName)
	default:
		return "", fmt.Errorf("run event identity read requires the original native owner")
	}
}
