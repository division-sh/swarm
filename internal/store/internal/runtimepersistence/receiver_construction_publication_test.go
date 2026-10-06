package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestReceiverConstructionPublicationFieldsUsesExactOriginalOwnerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithAgents(t, backend, false)
			req := f.request("receipt-key", "ti-receipt", "initial")
			plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil {
				t.Fatal(err)
			}
			owner := flowidentity.RunScopedFlowInstance{RunID: plan.Readiness.RunID, Route: plan.Identity.Route()}
			probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer restore()
			read := func() map[string]any {
				fields, err := ReadReceiverConstructionPublicationFieldsForTest(f.ctx, f.store, owner, plan.Identity.EntityID, plan.CreatingInput.EventID)
				if err != nil || !reflect.DeepEqual(fields, plan.Instance.Fields) {
					t.Fatalf("immutable precise receipt: fields=%#v want=%#v err=%v", fields, plan.Instance.Fields, err)
				}
				return fields
			}
			fields := read()
			fields["label"] = "not persisted"
			fields["nested"].([]any)[0] = "not persisted"
			read()
			if counts := probe.Snapshot(); counts.Total.Begun != 2 || counts.Total.ReadCommits != 2 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("receipt escaped original read owner: %+v", counts)
			}
			for _, which := range []string{"run", "route", "entity", "event"} {
				wrong, entity, event := owner, plan.Identity.EntityID, plan.CreatingInput.EventID
				switch which {
				case "run":
					wrong.RunID = uuid.NewString()
				case "route":
					wrong.Route.InstancePath = "review/ti-foreign"
				case "entity":
					entity = uuid.NewString()
				case "event":
					event = uuid.NewString()
				}
				if fields, err := ReadReceiverConstructionPublicationFieldsForTest(f.ctx, f.store, wrong, entity, event); err == nil || fields != nil {
					t.Fatalf("foreign %s became receipt evidence: %#v %v", which, fields, err)
				}
			}
		})
	}
}

func TestReceiverConstructionPublicationFieldsRefusesInvalidCancelledAndClosedOwnersBothStores(t *testing.T) {
	owner := flowidentity.RunScopedFlowInstance{RunID: uuid.NewString(), Route: flowidentity.Route{ScopeKey: "review", InstancePath: "review/ti-receipt"}}
	entity, event := uuid.NewString(), uuid.NewString()
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if fields, err := ReadReceiverConstructionPublicationFieldsForTest(context.Background(), selected, owner, entity, event); err == nil || errors.Is(err, sql.ErrNoRows) || fields != nil {
			t.Fatalf("invalid owner became missing/successful evidence: %#v %v", fields, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if fields, err := ReadReceiverConstructionPublicationFieldsForTest(ctx, fixture.store, owner, entity, event); !errors.Is(err, context.Canceled) || fields != nil {
				t.Fatalf("cancelled read lost its error or returned evidence: %#v %v", fields, err)
			}
			if fields, err := ReadReceiverConstructionPublicationFieldsForTest(context.Background(), fixture.store, owner, entity, event); !errors.Is(err, sql.ErrNoRows) || fields != nil {
				t.Fatalf("missing exact receipt: %#v %v", fields, err)
			}
			if fields, err := ReadReceiverConstructionPublicationFieldsForTest(context.Background(), fixture.store, owner, "not-an-entity", event); err == nil || fields != nil {
				t.Fatalf("invalid identity returned evidence: %#v %v", fields, err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if fields, err := ReadReceiverConstructionPublicationFieldsForTest(context.Background(), fixture.store, owner, entity, event); err == nil || errors.Is(err, sql.ErrNoRows) || fields != nil {
				t.Fatalf("closed owner became missing/successful evidence: %#v %v", fields, err)
			}
		})
	}
}
