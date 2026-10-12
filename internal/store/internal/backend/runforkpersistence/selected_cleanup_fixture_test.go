package runforkpersistence

import (
	"context"
	"database/sql"
	"testing"

	"github.com/division-sh/swarm/internal/store/internal/backend/delivery"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationlog"
	"github.com/division-sh/swarm/internal/store/internal/backend/pipelinepersistence"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

// Query-only qualification includes foreign runs and statuses, but admits no
// execution and manufactures no lifecycle, claim, receipt or fork authority.
func TestSelectedCleanupPhysicalCountsKeepEveryOriginalScope(t *testing.T) {
	_, db, cleanup := testutil.StartEmptyPostgres(t)
	t.Cleanup(cleanup)
	for _, table := range []string{"events", "event_deliveries", "entity_state", "entity_mutations"} {
		if _, err := db.Exec("CREATE TABLE " + table + " (run_id UUID, status TEXT)"); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range []string{"run_fork_selected_contract_bindings", "run_fork_selected_contract_executions", "run_fork_selected_contract_route_recoveries"} {
		if _, err := db.Exec("CREATE TABLE " + table + " (fork_run_id UUID, status TEXT)"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`CREATE TABLE event_receipts (run_id UUID,status TEXT)`); err != nil {
		t.Fatal(err)
	}
	run, sibling := uuid.NewString(), uuid.NewString()
	for _, table := range []string{"events", "event_deliveries", "entity_state", "entity_mutations", "event_receipts", "run_fork_selected_contract_bindings", "run_fork_selected_contract_executions", "run_fork_selected_contract_route_recoveries"} {
		if _, err := db.Exec("INSERT INTO "+table+" VALUES ($1,'pending'),($1,'cancelled'),($2,'delivered')", run, sibling); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, read := range []struct {
		name string
		read func(context.Context, *sql.Tx, string) (int, error)
	}{
		{"events", eventrecord.CountRunEventStorageTx}, {"deliveries", delivery.CountSelectedForkDeliveryStorageTx},
		{"state", pipelinepersistence.CountWorkflowFieldRowsForRun}, {"mutations", mutationlog.CountRunMutationStorageTx},
		{"bindings", CountSelectedForkBindingStorageTx}, {"lineage", CountSelectedForkExecutionStorageTx}, {"route recovery", CountSelectedForkRouteRecoveryStorageTx},
	} {
		for _, test := range []struct {
			run  string
			want int
		}{{run, 2}, {sibling, 1}, {uuid.NewString(), 0}} {
			if count, err := read.read(context.Background(), tx, test.run); count != test.want || err != nil {
				t.Fatalf("%s narrowed statuses or changed run membership: %d want%d err=%v", read.name, count, test.want, err)
			}
		}
	}
	if count, err := pipelinepersistence.CountGlobalPipelineReceiptStorageTx(context.Background(), tx); count != 3 || err != nil {
		t.Fatalf("receipt witness narrowed the global scope: %d want3 err=%v", count, err)
	}
}
