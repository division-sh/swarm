package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/google/uuid"
)

func TestForkOperationConcurrentInitialBindBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			db.SetMaxOpenConns(4)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var transact func(func(context.Context, *sql.Tx) error) (bool, error)
			if backend == "postgres" {
				owner, err := postgresbackend.New(db)
				if err != nil {
					t.Fatal(err)
				}
				transact = func(fn func(context.Context, *sql.Tx) error) (bool, error) {
					return owner.RunTransactionOutcome(ctx, fn)
				}
			} else {
				owner, err := sqlitebackend.New(db)
				if err != nil {
					t.Fatal(err)
				}
				transact = func(fn func(context.Context, *sql.Tx) error) (bool, error) {
					return owner.RunTransactionOutcome(ctx, "concurrent fork operation bind", fn)
				}
			}
			base := runfork.ForkOperationRequest{
				Actor: "bearer:operator", IdempotencyKey: "same-first-key",
				TransportHash: "sha256:same-first-transport", SourceRunID: uuid.NewString(),
				TargetBundleHash:  "sha256:bundle",
				ContractSelection: runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
				ResolvedPoint:     &runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 1},
			}
			type outcome struct {
				childID   string
				committed bool
				err       error
			}
			outcomes := make([]outcome, 2)
			start := make(chan struct{})
			var wg sync.WaitGroup
			for i := range outcomes {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					request := base
					request.OperationID = uuid.NewString()
					childID, bindingID := uuid.NewString(), uuid.NewString()
					outcomes[i].childID = childID
					<-start
					outcomes[i].committed, outcomes[i].err = transact(func(txctx context.Context, tx *sql.Tx) error {
						_, replay, err := bindForkOperationTx(txctx, tx, request, childID, bindingID, backend == "postgres")
						if replay {
							return fmt.Errorf("different child falsely replayed an operation")
						}
						return err
					})
				}(i)
			}
			close(start)
			wg.Wait()
			var winner string
			for _, outcome := range outcomes {
				if outcome.committed && outcome.err == nil {
					if winner != "" {
						t.Fatalf("two distinct children reported committed success: %+v", outcomes)
					}
					winner = outcome.childID
				} else if outcome.committed || outcome.err == nil {
					t.Fatalf("losing contender reported false success or committed error: %+v", outcomes)
				}
			}
			if winner == "" {
				t.Fatalf("neither first contender committed: %+v", outcomes)
			}
			var count int
			var durableChild string
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*), MIN(fork_run_id) FROM run_fork_operations`).Scan(&count, &durableChild); err != nil {
				t.Fatal(err)
			}
			if count != 1 || durableChild != winner {
				t.Fatalf("durable first bind count=%d child=%s, winning child=%s", count, durableChild, winner)
			}
		})
	}
}
