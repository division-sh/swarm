package pipelinepersistence

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/testutil"
	_ "modernc.org/sqlite"
)

type acknowledgementProbeExpiryOwner struct{ err error }

func (o acknowledgementProbeExpiryOwner) ExpireHumanTasksTx(context.Context, *mutationprotocol.Attempt, time.Time, int) ([]events.Event, error) {
	return nil, o.err
}

func TestEmptyHumanTaskExpiryAcknowledgementOnBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var db *sql.DB
			if backend == "postgres" {
				_, db, _ = testutil.StartEmptyPostgres(t)
			} else {
				var err error
				db, err = sql.Open("sqlite", "file:"+filepath.Join(t.TempDir(), "expiry-ack.db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
			}
			run := func(ctx context.Context, write func(context.Context, *mutationprotocol.Attempt) (runtimepipeline.CommittedHumanTaskExpiry, error)) mutationprotocol.Result[runtimepipeline.CommittedHumanTaskExpiry] {
				if backend == "postgres" {
					store, err := postgresbackend.New(db)
					if err != nil {
						t.Fatal(err)
					}
					return mutationprotocol.RunPostgres(ctx, store, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
				}
				store, err := sqlitebackend.New(db)
				if err != nil {
					t.Fatal(err)
				}
				return mutationprotocol.RunSQLite(ctx, store, "empty human-task expiry acknowledgement", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, write)
			}
			command := runtimepipeline.HumanTaskExpiryCommand{ObservedAt: time.Now().UTC(), Limit: 1}
			result, err := commitHumanTaskExpirations(context.Background(), nil, acknowledgementProbeExpiryOwner{}, run, command)
			if err != nil || !result.Acknowledged || len(result.Publications) != 0 {
				t.Fatalf("empty committed expiry = %+v, %v; want acknowledged empty result", result, err)
			}
			commitErr := errors.New("expiry transaction failed")
			result, err = commitHumanTaskExpirations(context.Background(), nil, acknowledgementProbeExpiryOwner{err: commitErr}, run, command)
			if !errors.Is(err, commitErr) || result.Acknowledged || len(result.Publications) != 0 {
				t.Fatalf("failed expiry = %+v, %v; want unacknowledged original error", result, err)
			}
		})
	}
}
