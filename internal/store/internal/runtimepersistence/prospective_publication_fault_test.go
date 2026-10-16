package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestProspectivePublicationFaultKeepsExactEventAndAtomicRollbackBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, state := newCompetingRevisionPairFixture(t, backend)
			t.Cleanup(func() {
				if err := RemoveProspectivePublicationCreationFailureForTest(f.ctx, f.store); err != nil {
					t.Error(err)
				}
			})
			before := competingRevisionPairSnapshot(t, f)
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			cancelled, cancel := context.WithCancel(f.ctx)
			cancel()
			if err := InstallProspectivePublicationCreationFailureForTest(cancelled, f.store); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancelled event fault: %v", err)
			}
			if err := InstallProspectivePublicationCreationFailureForTest(f.ctx, f.store); err != nil {
				t.Fatal(err)
			}
			if counts := probe.Snapshot(); counts.Total.Begun != 1 || counts.Total.WriteCommits != 1 || counts.Total.ReadCommits != 0 || counts.Active != 0 {
				t.Fatalf("fault installation escaped original coordinator: %+v", counts)
			}
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("event-fault installation changed application history or clocks")
			}
			producer, err := events.NewProducerIdentity(events.EventProducerExternal, "prospective-fault-proof")
			if err != nil {
				t.Fatal(err)
			}
			at := time.Now().UTC()
			id := uuid.NewString()
			if _, err := InsertNativeExistingRunRootEventForTest(f.ctx, f.store, id, state.Identity.RunID, "work.ready", producer, []byte(`{}`), events.EventEnvelope{}, at); err == nil || !strings.Contains(err.Error(), "prospective_publication_fault_cut") {
				t.Fatalf("did not hit real named event insert: %v", err)
			}
			if !reflect.DeepEqual(before, competingRevisionPairSnapshot(t, f)) {
				t.Fatal("failed event insert advanced data, revision, journal or lifecycle clocks")
			}
			// This is physical fault correspondence. The public pipeline journey
			// separately proves admitted execution and rollback of its whole mutation.
			if event, err := InsertNativeExistingRunRootEventForTest(f.ctx, f.store, uuid.NewString(), state.Identity.RunID, "review/finish.requested", producer, []byte(`{}`), events.EventEnvelope{}, at); err != nil || event.ID() == "" {
				t.Fatalf("named fault rejected unrelated native event: %v", err)
			}
			if err := RemoveProspectivePublicationCreationFailureForTest(f.ctx, f.store); err != nil {
				t.Fatal(err)
			}
			if event, err := InsertNativeExistingRunRootEventForTest(f.ctx, f.store, id, state.Identity.RunID, "work.ready", producer, []byte(`{}`), events.EventEnvelope{}, at); err != nil || event.ID() != id {
				t.Fatalf("fixed cleanup did not restore the exact event insertion: %v", err)
			}
		})
	}
}

func TestProspectivePublicationFaultRefusesRawAndClosedOwners(t *testing.T) {
	var postgres *PostgresStore
	var sqlite *SQLiteRuntimeStore
	for _, selected := range []any{nil, postgres, sqlite, &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if err := InstallProspectivePublicationCreationFailureForTest(context.Background(), selected); err == nil {
			t.Fatalf("event fault accepted invalid owner %T", selected)
		}
		if err := RemoveProspectivePublicationCreationFailureForTest(context.Background(), selected); err == nil {
			t.Fatalf("event fault cleanup accepted invalid owner %T", selected)
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f, _ := newCompetingRevisionPairFixture(t, backend)
			if err := f.manager.Shutdown(); err != nil {
				t.Fatal(err)
			}
			if err := f.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if err := InstallProspectivePublicationCreationFailureForTest(f.ctx, f.store); err == nil {
				t.Fatal("event fault accepted closed native owner")
			}
		})
	}
}
