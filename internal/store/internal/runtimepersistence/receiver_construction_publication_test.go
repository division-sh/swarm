package runtimepersistence

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/transactiontest"
	"github.com/google/uuid"
)

func TestReceiverConstructionPublicationFieldsUsesExactOriginalOwnerBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newReceiverConfigActivationFixtureWithAgents(t, backend, false)
			req := f.request("receipt-key", "ti-receipt", "initial")
			raw, err := canonicaljson.MarshalPreservingNumberKinds(map[string]any{
				"request_id": "receipt-key",
				"label":      "initial",
				"nested":     []any{int64(7), float64(7), nil},
			})
			if err != nil {
				t.Fatal(err)
			}
			event := req.TriggerEvent
			req.TriggerEvent = eventtest.ExistingRunRootIngress(event.ID(), "task.create", event.SourceAgent(), event.TaskID(), raw, event.ChainDepth(), event.RunID(), event.Envelope(), event.CreatedAt())
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
			read := func() pipeline.FlowConstructionPublicationEvidence {
				evidence, err := ReadReceiverConstructionPublicationForTest(f.ctx, f.store, owner, plan.Identity.EntityID)
				if err != nil || evidence.CreatingInput != plan.CreatingInput || !reflect.DeepEqual(evidence.Fields, plan.Instance.Fields) {
					t.Fatalf("immutable precise receipt: evidence=%#v want=%#v/%#v err=%v", evidence, plan.CreatingInput, plan.Instance.Fields, err)
				}
				return evidence
			}
			evidence := read()
			evidence.CreatingInput.EventID = uuid.NewString()
			evidence.CreatingInput.Input = "not.persisted"
			evidence.Fields["label"] = "not persisted"
			evidence.Fields["nested"].([]any)[0] = "not persisted"
			read()
			if counts := probe.Snapshot(); counts.Total.Begun != 2 || counts.Total.ReadCommits != 2 || counts.Total.WriteCommits != 0 || counts.Active != 0 {
				t.Fatalf("receipt escaped original read owner: %+v", counts)
			}
			later := req
			later.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "task.create", event.SourceAgent(), event.TaskID(), raw, event.ChainDepth(), event.RunID(), event.Envelope(), event.CreatedAt())
			if created, err := f.manager.EnsureFlowInstance(f.ctx, later); err != nil || created {
				t.Fatalf("later ingress must reuse the receiver: created=%t %v", created, err)
			}
			if observed := read(); observed.CreatingInput.EventID == later.TriggerEvent.ID() {
				t.Fatal("later ingress replaced the immutable creating-event coordinate")
			}
			for _, which := range []string{"run", "route", "entity"} {
				wrong, entity := owner, plan.Identity.EntityID
				switch which {
				case "run":
					wrong.RunID = uuid.NewString()
				case "route":
					wrong.Route.InstancePath = "review/ti-foreign"
				case "entity":
					entity = uuid.NewString()
				}
				if evidence, err := ReadReceiverConstructionPublicationForTest(f.ctx, f.store, wrong, entity); err == nil || !reflect.DeepEqual(evidence, pipeline.FlowConstructionPublicationEvidence{}) {
					t.Fatalf("foreign %s became receipt evidence: %#v %v", which, evidence, err)
				}
			}
		})
	}
}

func TestReceiverConstructionPublicationFieldsRefusesInvalidCancelledAndClosedOwnersBothStores(t *testing.T) {
	owner := flowidentity.RunScopedFlowInstance{RunID: uuid.NewString(), Route: flowidentity.Route{ScopeKey: "review", InstancePath: "review/ti-receipt"}}
	entity := uuid.NewString()
	for _, selected := range []any{nil, (*PostgresStore)(nil), (*SQLiteRuntimeStore)(nil), &PostgresStore{}, &SQLiteRuntimeStore{}, &sql.DB{}, &sql.Tx{}} {
		if evidence, err := ReadReceiverConstructionPublicationForTest(context.Background(), selected, owner, entity); err == nil || errors.Is(err, sql.ErrNoRows) || !reflect.DeepEqual(evidence, pipeline.FlowConstructionPublicationEvidence{}) {
			t.Fatalf("invalid owner became missing/successful evidence: %#v %v", evidence, err)
		}
	}
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if evidence, err := ReadReceiverConstructionPublicationForTest(ctx, fixture.store, owner, entity); !errors.Is(err, context.Canceled) || !reflect.DeepEqual(evidence, pipeline.FlowConstructionPublicationEvidence{}) {
				t.Fatalf("cancelled read lost its error or returned evidence: %#v %v", evidence, err)
			}
			if evidence, err := ReadReceiverConstructionPublicationForTest(context.Background(), fixture.store, owner, entity); !errors.Is(err, sql.ErrNoRows) || !reflect.DeepEqual(evidence, pipeline.FlowConstructionPublicationEvidence{}) {
				t.Fatalf("missing exact receipt: %#v %v", evidence, err)
			}
			if evidence, err := ReadReceiverConstructionPublicationForTest(context.Background(), fixture.store, owner, "not-an-entity"); err == nil || !reflect.DeepEqual(evidence, pipeline.FlowConstructionPublicationEvidence{}) {
				t.Fatalf("invalid identity returned evidence: %#v %v", evidence, err)
			}
			if err := fixture.store.(interface{ Close() error }).Close(); err != nil {
				t.Fatal(err)
			}
			if evidence, err := ReadReceiverConstructionPublicationForTest(context.Background(), fixture.store, owner, entity); err == nil || errors.Is(err, sql.ErrNoRows) || !reflect.DeepEqual(evidence, pipeline.FlowConstructionPublicationEvidence{}) {
				t.Fatalf("closed owner became missing/successful evidence: %#v %v", evidence, err)
			}
		})
	}
}

func TestReceiverConstructionPublicationObservesStandingTypedAbsenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fielded := range []bool{false, true} {
			name := "fieldless"
			if fielded {
				name = "fielded"
			}
			t.Run(backend+"/"+name, func(t *testing.T) {
				documents := map[string]string{
					"schema.yaml":         "name: standing-absence\n",
					"service/schema.yaml": "name: service\n",
				}
				if fielded {
					documents["entities.yaml"] = "receipt:\n  marker: {type: text, initial: unchanged}\n"
					documents["service/entities.yaml"] = documents["entities.yaml"]
				}
				f := newReceiverConfigActivationFixtureWithDocuments(t, backend, false, documents, nil)
				runID := correlation.RunIDFromContext(f.ctx)
				req := sqliteFlowActivationRequest(f.bundle, ".", runID, "", runID)
				var err error
				req.Instance, err = flowidentity.StandingForGeneration(req.ContractBundle, ".", runID)
				if err != nil {
					t.Fatal(err)
				}
				created, finish, err := f.manager.PrepareStandingFlowInstance(f.ctx, req)
				if err != nil || !created || finish == nil {
					t.Fatalf("no-argument standing construction: created=%t completion=%t %v", created, finish != nil, err)
				}
				if err := finish(); err != nil {
					t.Fatal(err)
				}
				before, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
				if err != nil {
					t.Fatal(err)
				}
				for _, flowID := range []string{".", "service"} {
					instance, err := flowidentity.StandingForGeneration(req.ContractBundle, flowID, runID)
					if err != nil {
						t.Fatal(err)
					}
					owner := flowidentity.RunScopedFlowInstance{RunID: runID, Route: instance.Route()}
					evidence, err := ReadReceiverConstructionPublicationForTest(f.ctx, f.store, owner, instance.EntityID)
					if err != nil || evidence.CreatingInput != (pipeline.FlowConstructionInput{}) {
						t.Fatalf("%s typed absence became missing or forged provenance: %#v %v", flowID, evidence, err)
					}
					if fielded && evidence.Fields["marker"] != "unchanged" || !fielded && len(evidence.Fields) != 0 {
						t.Fatalf("%s no-publication receipt lost initial fields: %#v", flowID, evidence.Fields)
					}
				}
				after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
				if err != nil || !reflect.DeepEqual(before, after) {
					t.Fatalf("typed-absence observation mutated or could not re-observe the selected store: %v", err)
				}
			})
		}
	}
}
