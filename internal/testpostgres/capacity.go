package testpostgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// RequiredMaxConnections is the shared test service's declared capacity floor.
const RequiredMaxConnections = 300

const capacityObservationTimeout = 2 * time.Second

// CapacityError reports a usable server that cannot support the test harness.
type CapacityError struct {
	Have int
	Need int
}

func (e *CapacityError) Error() string {
	return fmt.Sprintf("swarm testutil: host Postgres max_connections=%d, need >= %d for parallel test packages. Fix once: ask the administrator to run ALTER SYSTEM SET max_connections = %d; then restart the dedicated test server", e.Have, e.Need, e.Need)
}

type capacityObservationError struct{ cause error }

func (e *capacityObservationError) Error() string {
	return "swarm testutil: cannot establish host Postgres max_connections; inspect the test server connection and permissions"
}

func (e *capacityObservationError) Unwrap() error { return e.cause }

// ValidateServerCapacity admits a connected test server before resource writes.
// It neither changes server settings nor caches an observation across owners.
func ValidateServerCapacity(ctx context.Context, db interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) error {
	ctx, cancel := context.WithTimeout(ctx, capacityObservationTimeout)
	defer cancel()
	var actual int
	if err := db.QueryRowContext(ctx, `SHOW max_connections`).Scan(&actual); err != nil {
		// Keep the cause inspectable without printing driver errors or credentials.
		return &capacityObservationError{cause: errors.Join(err, ctx.Err())}
	}
	if actual < RequiredMaxConnections {
		return &CapacityError{Have: actual, Need: RequiredMaxConnections}
	}
	return nil
}
