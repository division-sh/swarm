package eventfixture

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/runhandoff"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
)

// Attempt exposes the canonical mutation writer to tests outside store/internal.
type Attempt = mutationprotocol.Attempt
type Result = mutationprotocol.Result[struct{}]

// RunMutation commits one fixture mutation through the native store protocol.
func RunMutation(ctx context.Context, db *sql.DB, dialect authoractivityfixture.Dialect, write func(context.Context, *Attempt) error) Result {
	if db == nil || write == nil {
		return mutationprotocol.Reject[struct{}](fmt.Errorf("fixture mutation requires a database and writer"))
	}
	apply := func(ctx context.Context, attempt *mutationprotocol.Attempt) (struct{}, error) {
		return struct{}{}, write(ctx, attempt)
	}
	candidates := runhandoff.NewCandidateCoordinator()
	switch dialect {
	case authoractivityfixture.DialectPostgres:
		backend, err := postgresbackend.New(db)
		if err != nil {
			return mutationprotocol.Reject[struct{}](err)
		}
		return mutationprotocol.RunPostgres(ctx, backend, mutationprotocol.Story, mutationprotocol.Ordinary, nil, candidates, apply)
	case authoractivityfixture.DialectSQLite:
		backend, err := sqlitebackend.New(db)
		if err != nil {
			return mutationprotocol.Reject[struct{}](err)
		}
		return mutationprotocol.RunSQLite(ctx, backend, "canonical event fixture", mutationprotocol.Story, mutationprotocol.Ordinary, nil, candidates, apply)
	default:
		return mutationprotocol.Reject[struct{}](fmt.Errorf("fixture mutation dialect %q is unsupported", dialect))
	}
}
