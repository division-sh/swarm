package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestReceiverConstructionStoragePreservesExactPhysicalWitnessBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, shape := range []string{"fieldless", "fields"} {
			t.Run(backend+"/"+shape, func(t *testing.T) {
				documents := map[string]string{"schema.yaml": "name: physical-receiver\n", "review/schema.yaml": "name: review\nstages:\n  queued: {initial: true}\n"}
				if shape == "fields" {
					documents["review/entities.yaml"] = "test_entity:\n  amount: {type: json, initial: 75.0}\n  literal: {type: text, initial: '${x}'}\n"
				}
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, documents, nil)
				runID := correlation.RunIDFromContext(f.ctx)
				req := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
				req.Instance = flowidentity.Stored(req.ContractBundle, ".", runID, runID, runID, "")
				plan, err := f.manager.PrepareFlowInstanceActivation(f.ctx, req)
				if err != nil || len(plan.Children) != 1 {
					t.Fatalf("prepare real physical receiver: %+v %v", plan, err)
				}
				if _, err := (agentFixtureFlowActivationCommitter{store: f.store}).CommitFlowInstanceActivation(f.ctx, plan); err != nil {
					t.Fatal(err)
				}
				child := plan.Children[0]
				probe, restore, err := InstallTransactionProbeForTest(f.store, transactiontest.Options{})
				if err != nil {
					t.Fatal(err)
				}
				defer restore()
				got, err := ReadReceiverConstructionStorageForTest(f.ctx, f.store, runID, child.Identity.InstancePath, child.Identity.EntityID, child.Instance.EntityType)
				if err != nil || got.EntityID != child.Identity.EntityID || got.Template != "review" || got.State != "queued" || got.Revision != 1 || got.EntityTypePresent != (shape == "fields") || got.EntityType != child.Instance.EntityType || !got.OrderedClocks || got.CreatedAt == "" || got.CreatedAt != got.UpdatedAt || !got.ReadinessPresent || got.Phase != string(pipeline.FlowAttachmentPlanned) {
					t.Fatalf("physical construction/clock/readiness witness changed: %+v err=%v", got, err)
				}
				decoded, err := pipeline.DecodeFlowReadinessPlan(got.Plan, got.PlanHash)
				if err != nil || !reflect.DeepEqual(decoded, child.Readiness) {
					t.Fatalf("physical readiness bytes lost exact owner: %+v err=%v", decoded, err)
				}
				if shape == "fieldless" {
					if got.FieldRows != 0 || got.Fields != nil {
						t.Fatalf("fieldless physical header acquired fields: %+v", got)
					}
				} else {
					var fields map[string]any
					if err := canonicaljson.DecodePreservingNumberLexemes(got.Fields, &fields); err != nil {
						t.Fatal(err)
					}
					value, err := canonicaljson.CloneRuntimeValue(fields)
					if err != nil || got.FieldRows != 1 || !reflect.DeepEqual(value, child.Instance.Fields) || value.(map[string]any)["amount"] != float64(75) || value.(map[string]any)["literal"] != "${x}" {
						t.Fatalf("physical native fields changed: fields=%#v raw=%s err=%v", value, got.Fields, err)
					}
				}
				if _, err := ReadReceiverConstructionStorageForTest(f.ctx, f.store, uuid.NewString(), child.Identity.InstancePath, child.Identity.EntityID, child.Instance.EntityType); !errors.Is(err, sql.ErrNoRows) {
					t.Fatalf("foreign run became physical receiver evidence: %v", err)
				}
				if counts := probe.Snapshot(); counts.Total.Begun != 2 || counts.Total.ReadCommits != 1 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
					t.Fatalf("physical observation escaped original read owner: %+v", counts)
				}
			})
		}
	}
}

func TestReceiverConstructionStorageRefusesRawCancelledClosedAndUnavailableOwnersBothStores(t *testing.T) {
	refuse := func(t *testing.T, ctx context.Context, selected any) {
		t.Helper()
		if got, err := ReadReceiverConstructionStorageForTest(ctx, selected, uuid.NewString(), "review", uuid.NewString(), "test_entity"); err == nil || errors.Is(err, sql.ErrNoRows) || !reflect.DeepEqual(got, ReceiverConstructionStorage{}) {
			t.Fatalf("invalid physical owner became absence or evidence: %+v %v", got, err)
		}
	}
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		refuse(t, context.Background(), selected)
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			refuse(t, ctx, fixture.store)
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			refuse(t, context.Background(), fixture.store)
		})
	}
}
