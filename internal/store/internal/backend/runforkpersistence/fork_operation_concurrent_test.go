package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/processbinding"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/store/internal/backend/generationauthority"
	postgresbackend "github.com/division-sh/swarm/internal/store/internal/backend/postgres"
	sqlitebackend "github.com/division-sh/swarm/internal/store/internal/backend/sqlite"
	"github.com/division-sh/swarm/internal/store/internal/schemastore"
	"github.com/division-sh/swarm/internal/store/platformschema"
	"github.com/google/uuid"
)

func TestForkOperationConcurrentInitialBindBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := forkOperationTestDatabase(t, backend)
			root := forkOperationTargetRoot(t)
			db.SetMaxOpenConns(4)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var transact func(func(context.Context, *sql.Tx) error) (bool, error)
			var acquire func(context.Context, func(context.Context, *sql.Tx) error) (bool, error)
			leaseBackend := processbinding.SQLiteLeaseBackend
			if backend == "postgres" {
				owner, err := postgresbackend.New(db)
				if err != nil {
					t.Fatal(err)
				}
				lease, err := postgresbackend.AcquireBlockingAdvisoryLockLease(ctx, owner, "concurrent-fork-original")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := lease.Release(context.Background()); err != nil {
						t.Error(err)
					}
				})
				acquire, leaseBackend = lease.RunProcessAcquisitionOutcome, processbinding.PostgresLeaseBackend
				transact = func(fn func(context.Context, *sql.Tx) error) (bool, error) {
					return owner.RunTransactionOutcome(ctx, fn)
				}
			} else {
				owner, err := sqlitebackend.New(db)
				if err != nil {
					t.Fatal(err)
				}
				acquire = owner.RunProcessAcquisitionOutcome
				transact = func(fn func(context.Context, *sql.Tx) error) (bool, error) {
					return owner.RunTransactionOutcome(ctx, "concurrent fork operation bind", fn)
				}
			}
			spec, err := contracts.LoadPlatformSpecDocument(filepath.Join("..", "..", "..", "..", "..", "platform-spec.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			plans, err := schemastore.GeneratePlatformTableDDLs(spec)
			if err != nil {
				t.Fatal(err)
			}
			for _, plan := range plans {
				if plan.TableName != "runtime_process_generation" {
					continue
				}
				statements := plan.Statements
				if backend == "sqlite" {
					statements, err = schemastore.SQLiteStatementsForPlan(plan)
					if err != nil {
						t.Fatal(err)
					}
				}
				for _, statement := range statements {
					if _, err := db.ExecContext(ctx, statement); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := platformschema.SeedFreshProcessGeneration(ctx, db, plans); err != nil {
				t.Fatal(err)
			}
			var original processbinding.LeaseStamp
			committed, err := acquire(ctx, func(txctx context.Context, tx *sql.Tx) error {
				var err error
				original, err = generationauthority.AdvanceProcessGeneration(txctx, tx, processbinding.LeaseIdentity{
					LeaseID: uuid.NewString(), OwnerID: "concurrent-fork-fixture", BootID: uuid.NewString(),
					RuntimeInstanceID: uuid.NewString(), Backend: leaseBackend,
				}, backend == "sqlite")
				return err
			})
			if err != nil || !committed {
				t.Fatalf("acquire original concurrent-fork fixture: committed=%t err=%v", committed, err)
			}
			ctx, err = generationauthority.MutationContext(ctx, original, false)
			if err != nil {
				t.Fatal(err)
			}
			base := runfork.ForkOperationRequest{
				Actor: "bearer:operator", IdempotencyKey: "same-first-key",
				TransportHash: "sha256:same-first-transport", SourceRunID: uuid.NewString(),
				TargetBundleHash:    root.BundleHash,
				TargetRootReference: root,
				ContractSelection:   runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts},
				ResolvedPoint:       &runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 1},
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
				t.Fatalf("neither first contender committed: %+v; errors: %v; %v", outcomes, outcomes[0].err, outcomes[1].err)
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
