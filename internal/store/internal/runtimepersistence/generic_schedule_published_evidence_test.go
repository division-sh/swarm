package runtimepersistence

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/eventrecord"
	eventrecordpostgres "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/postgres"
	eventrecordsqlite "github.com/division-sh/swarm/internal/store/internal/backend/eventrecord/sqlite"
	privategenericschedule "github.com/division-sh/swarm/internal/store/internal/backend/genericschedule"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

// Actual canonical publication and evidence readback, not historical fork
// serving or permission to remove the selected-join capability blocker.
func TestGenericSchedulePublishedEvidenceUsesCommittedNativeEventBothStores(t *testing.T) {
	for _, backend := range selectedScheduleStoreCases() {
		t.Run(backend.name, func(t *testing.T) {
			selected, _, ctx := backend.open(t)
			registerTestAuthorActivityCatalogForContext(t, selected.(testAuthorActivityCatalogRegistrar), testAuthorActivityContext())
			bus, err := newStoreTestEventBus(t, selected.(storeTestDurableEventBusStore))
			if err != nil {
				t.Fatal(err)
			}
			scheduler := &selectedStoreLifecycleScheduler{}
			dispatcher := &terminalScheduleDispatcherProbe{}
			lifecycle, err := runtimegenericschedule.NewLifecycle(selected, scheduler, bus, dispatcher, nil, executionposture.Live)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := lifecycle.Stop(context.Background()); err != nil {
					t.Error(err)
				}
			}()
			runID := runtimecorrelation.RunIDFromContext(ctx)
			command := testRootGenericScheduleCommand(t, runID, uuid.NewString(), "publication-evidence", runtimegenericschedule.AbsoluteDue(time.Now().UTC().Add(-time.Second)))
			command.EventType = "test.node_emitted"
			command.Payload, err = canonicaljson.Decode([]byte(`{"integer":7.0,"fraction":7.5}`))
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := lifecycle.Admit(ctx, command)
			if err != nil || len(scheduler.registered) != 1 {
				t.Fatalf("admit real schedule: wakeups=%v err=%v", scheduler.registered, err)
			}
			if rows := readPublishedScheduleEvidence(t, ctx, selected, runID); len(rows) != 0 {
				t.Fatal("unpublished arm became accepted publication evidence")
			}
			scheduler.callback(ctx, scheduler.registered[0])
			activation, found, err := selected.LoadGenericScheduleActivation(ctx, admitted.Activation.ID)
			if err != nil || !found || activation.Status != runtimegenericschedule.StatusFired || dispatcher.calls != 1 {
				t.Fatalf("real publication: activation=%+v found=%t dispatch=%d err=%v", activation, found, dispatcher.calls, err)
			}
			rows := readPublishedScheduleEvidence(t, ctx, selected, runID)
			if len(rows) != 1 || !reflect.DeepEqual(rows[0].Canonical(), activation.Canonical()) {
				t.Fatalf("native accepted occurrence census=%v", rows)
			}
			var record eventrecord.Record
			switch selected := selected.(type) {
			case *PostgresStore:
				record, found, err = eventrecordpostgres.Load(ctx, selected.backend, activation.CurrentEventID)
			case *SQLiteRuntimeStore:
				record, found, err = eventrecordsqlite.Load(ctx, selected.backend, activation.CurrentEventID)
			}
			if err != nil || !found {
				t.Fatalf("read exact committed event: found=%t err=%v", found, err)
			}
			event, err := record.Decode()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := activation.ValidatePublishedOccurrence(event.Event()); err != nil {
				t.Fatalf("actual native publication failed canonical evidence: %v", err)
			}
			if err := lifecycle.ReconcileRunWakeups(ctx, runID); err != nil || len(scheduler.registered) != 1 {
				t.Fatalf("accepted occurrence was rearmed: wakeups=%v err=%v", scheduler.registered, err)
			}
			scheduler.callback(ctx, scheduler.registered[0])
			if dispatcher.calls != 1 {
				t.Fatal("repeated wakeup resent committed publication")
			}
		})
	}
}

func readPublishedScheduleEvidence(t *testing.T, ctx context.Context, selected selectedScheduleStore, runID string) []runtimegenericschedule.Activation {
	t.Helper()
	var result mutationprotocol.Result[[]runtimegenericschedule.Activation]
	read := func(ctx context.Context, attempt *mutationprotocol.Attempt) ([]runtimegenericschedule.Activation, error) {
		var rows []runtimegenericschedule.Activation
		err := attempt.WithSQL(ctx, func(ctx context.Context, tx *sql.Tx) error {
			var err error
			_, postgres := selected.(*PostgresStore)
			rows, err = privategenericschedule.ReadPublishedOccurrencesTx(ctx, tx, postgres, runID)
			return err
		})
		return rows, err
	}
	switch selected := selected.(type) {
	case *PostgresStore:
		result = mutationprotocol.RunPostgres(ctx, selected.backend, mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, read)
	case *SQLiteRuntimeStore:
		result = mutationprotocol.RunSQLite(ctx, selected.backend, "published schedule evidence test", mutationprotocol.RevisionOnly, mutationprotocol.Ordinary, nil, nil, read)
	}
	rows, acknowledged := result.Value()
	if err := result.Err(); err != nil || !acknowledged {
		t.Fatalf("native evidence read: acknowledged=%t err=%v", acknowledged, err)
	}
	return rows
}
