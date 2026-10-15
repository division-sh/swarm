package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

type snapshotOwnershipStore interface {
	runForkSelectedLifecycleStore
	runtimepipeline.WorkflowEngineMutationOwner
	SetupScenarioEntities(context.Context, runtimepipeline.ScenarioSetupRequest) (runtimepipeline.ScenarioSetupResult, error)
}

type snapshotOwnershipFixture struct {
	store                    snapshotOwnershipStore
	db                       *sql.DB
	ctx                      context.Context
	runID, entityID, eventID string
	state                    runtimepipeline.WorkflowEngineStateRecord
	initialMaterialization   json.RawMessage
}

func newSnapshotOwnershipFixture(t *testing.T, backend eventRecordContractBackend, eventContext, reverse bool) snapshotOwnershipFixture {
	t.Helper()
	opened := backend.open(t)
	construction := newReceiverConfigActivationFixtureForStore(t, opened.store.(agentFixtureFlowStore), false, map[string]string{
		"schema.yaml":         "name: snapshot-ownership\n",
		"owner/schema.yaml":   "name: owner\ninstance: subject_id\nstages:\n  active: {}\n  ready: {}\n  later: {}\npins:\n  inputs:\n    - construct.requested\n",
		"owner/entities.yaml": "review_item:\n  subject_id: text\n  entity_type: {type: text, initial: authored-not-owner}\n  value: text?\n",
		"owner/events.yaml":   "construct.requested:\n",
	}, nil, ownStoreTestAgentManager, nil)
	f := snapshotOwnershipFixture{store: opened.store.(snapshotOwnershipStore), db: opened.db, ctx: construction.ctx, runID: runtimecorrelation.RunIDFromContext(construction.ctx)}
	at := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	req := sqliteFlowActivationRequest(construction.bundle, "owner", "one", "", "owner/one")
	parent := flowidentity.Stored(req.ContractBundle, semanticview.RootExecutionFlowID(req.ContractBundle), f.runID, f.runID, f.runID, "")
	rootRequest := sqliteFlowActivationRequest(construction.bundle, ".", f.runID, "", f.runID)
	rootRequest.Instance, rootRequest.OccurredAt = parent, at
	constructHistoricalSourceFixture(t, f.ctx, construction.store, rootRequest)
	child, err := flowidentity.KeyedChild(req.ContractBundle, parent, "owner", "one")
	if err != nil {
		t.Fatal(err)
	}
	req.Instance = child
	req.OccurredAt = at
	req.ConstructorInput, req.ResolvedKey = "construct.requested", "one"
	req.TriggerEvent = eventtest.ExistingRunRootIngress(uuid.NewString(), "construct.requested", "constructor-fixture", "", []byte(`{}`), 0, f.runID, events.EventEnvelope{}, at)
	activation, err := construction.manager.PrepareFlowInstanceActivation(f.ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := (agentFixtureFlowActivationCommitter{store: construction.store}).CommitFlowInstanceActivation(f.ctx, activation)
	if err != nil || !committed.Acknowledged || !committed.Created {
		t.Fatalf("construct snapshot source: %+v %v", committed, err)
	}
	persisted, err := activation.PersistenceRecord()
	if err != nil {
		t.Fatal(err)
	}
	f.entityID = persisted.State.EntityID
	f.initialMaterialization = append(json.RawMessage(nil), persisted.InitialMaterialization...)
	f.state = persisted.State
	f.state.Transition = runtimepipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
	f.state.ExpectedState, f.state.ExpectedRevision = "active", 1
	f.state.CurrentState = "ready"
	f.state.Slug, f.state.Name = "snapshot-slug", "Snapshot Name"
	f.state.Fields = json.RawMessage(`{"subject_id":"one","entity_type":"authored-not-owner","value":"at-R"}`)
	f.state.Gates = json.RawMessage(`{"review":true}`)
	f.state.Bookkeeping = json.RawMessage(`{"activation":"snapshot"}`)
	f.state.Accumulator = json.RawMessage(`{"total":7}`)
	if _, err := f.store.CommitWorkflowEngineMutation(f.ctx, runtimepipeline.WorkflowEngineMutationCommand{State: f.state}); err != nil {
		t.Fatal(err)
	}
	flows := []string{"causal/one", "causal/two"}
	if !eventContext {
		flows = []string{""}
	}
	if reverse {
		flows[0], flows[1] = flows[1], flows[0]
	}
	for _, flow := range flows {
		f.eventID = uuid.NewString()
		event := eventtest.ExistingRunRootIngress(f.eventID, "fork.metadata_context", "metadata-proof", "", json.RawMessage(`{"entity_type":"event-not-owner"}`), 0,
			f.runID, events.EventEnvelope{EntityID: f.entityID, FlowInstance: flow}, time.Now().UTC())
		if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, event); err != nil {
			t.Fatal(err)
		}
	}
	return f
}

func (f snapshotOwnershipFixture) plan(t *testing.T) runfork.RunForkPlan {
	t.Helper()
	plan, err := f.store.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: f.runID, At: f.eventID})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func (f snapshotOwnershipFixture) advance(t *testing.T) {
	t.Helper()
	state := f.state
	state.Transition = runtimepipeline.WorkflowEngineStateTransitionUpdateStateAndCompanion
	state.ExpectedRevision, state.ExpectedState = 2, "ready"
	state.CurrentState, state.Name, state.Slug = "later", "Later Name", "later-slug"
	state.Fields, state.Gates = json.RawMessage(`{"subject_id":"one","entity_type":"later-authored","value":"after-R"}`), json.RawMessage(`{"review":false}`)
	state.Bookkeeping, state.Accumulator = json.RawMessage(`{"activation":"later"}`), json.RawMessage(`{"total":99}`)
	state.UpdatedAt, state.EnteredStageAt = time.Now().UTC(), time.Now().UTC()
	if _, err := f.store.CommitWorkflowEngineMutation(f.ctx, runtimepipeline.WorkflowEngineMutationCommand{State: state}); err != nil {
		t.Fatal(err)
	}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "fork.after_metadata", "metadata-proof", "", json.RawMessage(`{}`), 0, f.runID, events.EventEnvelope{}, time.Now().UTC())
	if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, event); err != nil {
		t.Fatal(err)
	}
}

func TestRunForkSnapshotOwnershipMetadataBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, order := range []string{"ordinary_materialization", "forward", "reverse"} {
				t.Run(order, func(t *testing.T) {
					f := newSnapshotOwnershipFixture(t, backend, order != "ordinary_materialization", order == "reverse")
					plan := f.plan(t)
					if len(plan.Entities) != 2 {
						t.Fatalf("plan = %#v", plan)
					}
					var entity runfork.RunForkEntityState
					for _, candidate := range plan.Entities {
						if candidate.EntityID == f.entityID {
							entity = candidate
						}
					}
					wantMetadata := runfork.RunForkMaterializedEntitySnapshotMetadata{
						InitialMaterialization: canonicalSnapshotOwnershipJSON(t, f.initialMaterialization),
						FlowConfig:             canonicalSnapshotOwnershipJSON(t, f.state.Config),
						Owner:                  runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance,
						FlowInstance: "owner/one", EntityType: "review_item", Slug: "snapshot-slug", Name: "Snapshot Name",
						ParentInstance: f.runID, InstanceKey: "one",
						StageDefined: true, FlowTemplate: "owner", Mode: "template", Status: "active",
						CreatedAt: f.state.CreatedAt, UpdatedAt: f.state.UpdatedAt, EnteredStateAt: f.state.EnteredStageAt,
						DataCreatedAt: &f.state.CreatedAt, DataUpdatedAt: &f.state.UpdatedAt,
					}
					if entity.MaterializationMetadata != nil {
						gotMetadata := *entity.MaterializationMetadata
						gotMetadata.FlowConfig = canonicalSnapshotOwnershipJSON(t, gotMetadata.FlowConfig)
						gotMetadata.InitialMaterialization = canonicalSnapshotOwnershipJSON(t, gotMetadata.InitialMaterialization)
						if !reflect.DeepEqual(gotMetadata, wantMetadata) {
							t.Fatalf("metadata = %#v, want %#v", gotMetadata, wantMetadata)
						}
					} else {
						t.Fatal("fixed-cut entity is missing ownership metadata")
					}
					if entity.Fields["entity_type"] != "authored-not-owner" {
						t.Fatalf("authored type field lost: %#v", entity.Fields)
					}
					f.advance(t)
					if later := f.plan(t); !reflect.DeepEqual(later.Entities, plan.Entities) || later.ForkPoint != plan.ForkPoint {
						t.Fatalf("fixed revision changed: before=%#v after=%#v", plan, later)
					}
					if !plan.ExecutionReady || !plan.ReplayResumeAdmission.StateOnlyExecutionReady || len(plan.UnsupportedBlockers) != 0 {
						t.Fatalf("fixed snapshot acquired authority from unrelated event context: %#v", plan.UnsupportedBlockers)
					}
					req := runfork.RunForkMaterializeRequest{SourceRunID: f.runID, At: f.eventID}
					materialized, err := f.store.MaterializeRunFork(f.ctx, req)
					if err != nil {
						t.Fatal(err)
					}
					assertSnapshotOwnershipEntity(t, f, materialized.ForkRunID, entity, 1)
					before := snapshotOwnershipCounts(t, f, materialized.ForkRunID)
					replayed, err := f.store.MaterializeRunFork(f.ctx, req)
					if err != nil || replayed.ForkRunID != materialized.ForkRunID {
						t.Fatalf("exact replay = %#v, %v", replayed, err)
					}
					assertSnapshotOwnershipEntity(t, f, materialized.ForkRunID, entity, 1)
					if after := snapshotOwnershipCounts(t, f, materialized.ForkRunID); after != before {
						t.Fatalf("exact replay wrote facts: before=%v after=%v", before, after)
					}
					if _, err := f.store.ActivateRunFork(f.ctx, runfork.RunForkActivateRequest{ForkRunID: materialized.ForkRunID, AllowSourceFreeze: true}); err == nil {
						t.Fatal("state-only activation accepted source advancement")
					} else if _, fact, ok := runForkReplayResumeBlockerFromError(err); !ok || fact != runfork.RunForkReplayResumeFactSourceAdvanced {
						t.Fatalf("activation refused for wrong reason: %v", err)
					}
					if after := snapshotOwnershipCounts(t, f, materialized.ForkRunID); after != before {
						t.Fatalf("refused activation wrote facts: before=%v after=%v", before, after)
					}
					var sourceState, sourceName string
					if err := f.db.QueryRowContext(f.ctx, `SELECT fi.current_state, es.name
FROM entity_state es JOIN flow_instances fi
ON fi.run_id=es.run_id AND fi.entity_id=es.entity_id AND fi.instance_path=es.flow_instance
WHERE es.run_id=$1 AND es.entity_id=$2`, f.runID, f.entityID).Scan(&sourceState, &sourceName); err != nil {
						t.Fatal(err)
					}
					if sourceState != "later" || sourceName != "Later Name" {
						t.Fatalf("fork changed source: %q %q", sourceState, sourceName)
					}
				})
			}
		})
	}
}

func canonicalSnapshotOwnershipJSON(t *testing.T, raw json.RawMessage) json.RawMessage {
	t.Helper()
	var value any
	if err := canonicaljson.DecodePreservingNumberLexemes(raw, &value); err != nil {
		t.Fatal(err)
	}
	encoded, err := canonicaljson.MarshalPreservingNumberKinds(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func assertSnapshotOwnershipEntity(t *testing.T, f snapshotOwnershipFixture, forkRunID string, want runfork.RunForkEntityState, wantRevision int64) {
	t.Helper()
	got := readSnapshotOwnershipEntity(t, f, forkRunID)
	meta := want.MaterializationMetadata
	if got.Flow != meta.FlowInstance || got.EntityType != meta.EntityType || got.Slug != meta.Slug || got.Name != meta.Name || want.CurrentState == nil || got.State != *want.CurrentState || got.Revision != wantRevision || want.EnteredStateAt == nil || !got.Entered.Equal(*want.EnteredStateAt) {
		t.Fatalf("fork row differs: got=%#v want=%#v revision=%d", got, want, wantRevision)
	}
	for i, bucket := range []map[string]any{want.Fields, want.Gates, want.Bookkeeping, want.Accumulator} {
		if !reflect.DeepEqual(got.Buckets[i], bucket) {
			t.Fatalf("fork bucket = %#v, want %#v", got.Buckets[i], bucket)
		}
	}
}

type snapshotOwnershipEntityRow struct {
	Flow, EntityType, Slug, Name, State string
	Revision                            int64
	Entered                             time.Time
	Buckets                             [4]map[string]any
}

func readSnapshotOwnershipEntity(t *testing.T, f snapshotOwnershipFixture, runID string) snapshotOwnershipEntityRow {
	t.Helper()
	var flow, entityType, slug, name, state string
	var fields, gates, bookkeeping, accumulator []byte
	var revision int64
	var rawEntered any
	if err := f.db.QueryRowContext(f.ctx, `SELECT es.flow_instance, es.entity_type, es.slug, es.name, fi.current_state,
es.fields, fi.gates, fi.bookkeeping, fi.accumulator, fi.revision, fi.entered_state_at
FROM entity_state es JOIN flow_instances fi
ON fi.run_id=es.run_id AND fi.entity_id=es.entity_id AND fi.instance_path=es.flow_instance
WHERE es.run_id=$1 AND es.entity_id=$2`, runID, f.entityID).Scan(&flow, &entityType, &slug, &name, &state, &fields, &gates, &bookkeeping, &accumulator, &revision, &rawEntered); err != nil {
		t.Fatal(err)
	}
	entered, present, err := sqliteTimeValue(rawEntered)
	if err != nil || !present {
		t.Fatalf("decode committed entered_state_at: present=%t err=%v raw=%#v", present, err, rawEntered)
	}
	row := snapshotOwnershipEntityRow{Flow: flow, EntityType: entityType, Slug: slug, Name: name, State: state, Revision: revision, Entered: entered}
	for i, raw := range [][]byte{fields, gates, bookkeeping, accumulator} {
		if err := canonicaljson.DecodePreservingNumberLexemes(raw, &row.Buckets[i]); err != nil {
			t.Fatal(err)
		}
		for key, value := range row.Buckets[i] {
			cloned, err := canonicaljson.CloneRuntimeValue(value)
			if err != nil {
				t.Fatal(err)
			}
			row.Buckets[i][key] = cloned
		}
	}
	return row
}

func snapshotOwnershipCounts(t *testing.T, f snapshotOwnershipFixture, runID string) [5]int {
	t.Helper()
	var counts [5]int
	for i, table := range []string{"entity_state", "entity_mutations", "run_fork_fact_revisions", "events", "flow_instances"} {
		if err := f.db.QueryRowContext(f.ctx, "SELECT COUNT(*) FROM "+table+" WHERE run_id=$1", runID).Scan(&counts[i]); err != nil {
			t.Fatal(err)
		}
	}
	return counts
}

func TestRunForkSnapshotOwnershipInvalidMetadataBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			for _, phase := range []string{"initial", "exact_replay"} {
				t.Run(phase, func(t *testing.T) {
					for _, invalid := range []string{"missing", "blank_flow", "blank_type", "duplicate_identical", "duplicate_flow", "duplicate_type", "empty_import_missing_birth", "empty_import_forged_kind", "empty_import_duplicate_birth", "empty_import_missing_creation_clock", "empty_import_missing_update_clock", "empty_import_omitted_creation_clock", "empty_import_omitted_update_clock", "empty_import_zero_creation_clock", "empty_import_zero_update_clock", "empty_import_reversed_clocks", "empty_import_lifecycle_history", "empty_import_gate_history", "empty_import_bookkeeping_history", "empty_import_accumulator_history"} {
						t.Run(invalid, func(t *testing.T) {
							f := newSnapshotOwnershipFixture(t, backend, false, false)
							emptyImport := strings.HasPrefix(invalid, "empty_import_")
							duplicateImport := invalid == "empty_import_duplicate_birth"
							metadataEntityID, wantEntities := f.entityID, 2
							importedAt := f.state.CreatedAt.Add(time.Second)
							if emptyImport {
								metadataEntityID, wantEntities = uuid.NewString(), 3
								if _, err := f.store.SetupScenarioEntities(f.ctx, runtimepipeline.ScenarioSetupRequest{
									RunID: f.runID, CreatedAt: importedAt,
									Entities: []runtimepipeline.ScenarioSetupEntityRequest{{Alias: "empty", EntityID: metadataEntityID, FlowInstance: "owner/import", EntityType: "review_item", Fields: map[string]any{}}},
								}); err != nil {
									t.Fatal(err)
								}
								f.eventID = uuid.NewString()
								marker := eventtest.ExistingRunRootIngress(f.eventID, "fork.metadata_context", "metadata-proof", "", []byte(`{}`), 0, f.runID, events.EventEnvelope{}, time.Now().UTC())
								if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, marker); err != nil {
									t.Fatal(err)
								}
							}
							plan := f.plan(t)
							if len(plan.Entities) != wantEntities {
								t.Fatalf("constructed parent and child headers: %#v", plan.Entities)
							}
							var child runfork.RunForkEntityState
							for _, entity := range plan.Entities {
								if entity.EntityID == f.entityID {
									child = entity
								}
								if emptyImport && entity.EntityID == metadataEntityID {
									meta := entity.MaterializationMetadata
									if !reflect.DeepEqual(entity.Fields, map[string]any{}) || entity.CurrentState != nil || entity.EnteredStateAt != nil || entity.Gates != nil || entity.Bookkeeping != nil || entity.Accumulator != nil || meta == nil || meta.Source != runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState || meta.DataCreatedAt == nil || meta.DataUpdatedAt == nil || !meta.DataCreatedAt.Equal(importedAt) || !meta.DataUpdatedAt.Equal(importedAt) {
										t.Fatalf("genuine empty import lost birth or invented lifecycle: %+v", entity)
									}
								}
							}
							if child.EntityID != f.entityID {
								t.Fatal("fixed snapshot lost its exact child header")
							}
							var forkRunID string
							var forkBefore [5]int
							var forkEntityBefore snapshotOwnershipEntityRow
							if phase == "exact_replay" {
								materialized, err := f.store.MaterializeRunFork(f.ctx, runfork.RunForkMaterializeRequest{SourceRunID: f.runID, At: f.eventID})
								if err != nil {
									t.Fatal(err)
								}
								forkRunID = materialized.ForkRunID
								forkBefore = snapshotOwnershipCounts(t, f, forkRunID)
								assertSnapshotOwnershipEntity(t, f, forkRunID, child, 1)
								forkEntityBefore = readSnapshotOwnershipEntity(t, f, forkRunID)
							}
							f.advance(t)
							sourceEntityBefore := readSnapshotOwnershipEntity(t, f, f.runID)
							latest, err := f.store.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: f.runID})
							if err != nil || len(latest.Entities) != wantEntities {
								t.Fatalf("latest source plan: %#v, %v", latest, err)
							}
							inspector := f.store.(interface {
								InspectRunMutationDrift(context.Context, string) (mutationlog.DriftReport, error)
							})
							if emptyImport {
								report, err := inspector.InspectRunMutationDrift(f.ctx, f.runID)
								if err != nil || report.EntitiesChecked != wantEntities || len(report.Rows) != 0 {
									t.Fatalf("genuine empty import was not a clean native oracle member: %+v %v", report, err)
								}
							}
							if strings.HasSuffix(invalid, "_history") {
								record := mutationlog.Record{EntityID: metadataEntityID, WriterType: "platform", WriterID: "snapshot-ownership-proof", HandlerStep: "injected-import-history"}
								emptyPath := ""
								row := mutationlog.DriftRow{Kind: "value", EntityID: metadataEntityID, Path: &emptyPath, FoldedPresent: true, StoredValue: map[string]any(nil), FoldedType: "object", StoredType: "absent"}
								switch invalid {
								case "empty_import_lifecycle_history":
									record.Domain, record.NewValue = mutationlog.DomainLifecycleState, "unknown"
									row.FoldedValue, row.StoredValue, row.FoldedType = "unknown", nil, "text"
								case "empty_import_gate_history":
									record.Domain, record.Path, record.NewValue = mutationlog.DomainGate, "approved", true
									row.FoldedValue = map[string]any{"approved": true}
								case "empty_import_bookkeeping_history":
									record.Domain, record.Path, record.NewValue = mutationlog.DomainBookkeeping, "step", "retained"
									row.FoldedValue = map[string]any{"step": "retained"}
								case "empty_import_accumulator_history":
									record.Domain, record.Path, record.NewValue = mutationlog.DomainAccumulator, "total", int64(7)
									row.FoldedValue = map[string]any{"total": int64(7)}
								}
								row.Domain = record.Domain
								want := mutationlog.DriftReport{RunID: f.runID, EntitiesChecked: wantEntities, Rows: []mutationlog.DriftRow{row}}
								if record.Domain == mutationlog.DomainLifecycleState {
									for _, domain := range []mutationlog.Domain{mutationlog.DomainBookkeeping, mutationlog.DomainGate, mutationlog.DomainAccumulator} {
										want.Rows = append(want.Rows, mutationlog.DriftRow{Kind: "value", EntityID: metadataEntityID, Domain: domain, Path: &emptyPath, FoldedPresent: true, FoldedValue: map[string]any{}, StoredValue: map[string]any(nil), FoldedType: "object", StoredType: "absent"})
									}
								}
								// Bypass header mutation admission, not the native log/source/revision owner.
								if err := insertMutationLogPrivateAdapter(f.ctx, f.store, record); err != nil {
									t.Fatal(err)
								}
								poisonID := uuid.NewString()
								marker := eventtest.ExistingRunRootIngress(poisonID, "fork.after_import_history", "metadata-proof", "", []byte(`{}`), 0, f.runID, events.EventEnvelope{}, time.Now().UTC())
								if err := commitSemanticPipelineProcessedEventFixture(f.ctx, f.store, marker); err != nil {
									t.Fatal(err)
								}
								physicalBefore, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
								if err != nil {
									t.Fatal(err)
								}
								report, err := inspector.InspectRunMutationDrift(f.ctx, f.runID)
								if err != nil || !reflect.DeepEqual(report, want) {
									t.Fatalf("native imported-domain history lost exact drift: got=%+v want=%+v err=%v", report, want, err)
								}
								blocked, err := f.store.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: f.runID, At: poisonID})
								wantBlockers := []runfork.RunForkUnsupportedBlocker{{Code: runfork.RunForkBlockerEntitySnapshotMetadataUnproven, Message: fmt.Sprintf("imported entity %s cannot carry lifecycle history", metadataEntityID)}}
								if err != nil || blocked.ExecutionReady || len(blocked.Entities) != wantEntities || !reflect.DeepEqual(blocked.UnsupportedBlockers, wantBlockers) {
									t.Fatalf("native import history escaped exact fork admission: %+v %v", blocked, err)
								}
								refused, err := f.store.MaterializeRunFork(f.ctx, runfork.RunForkMaterializeRequest{SourceRunID: f.runID, At: poisonID})
								if err == nil || err.Error() != "fork materialization requires execution-ready plan; blockers: "+runfork.RunForkBlockerEntitySnapshotMetadataUnproven || refused.ForkRunID != "" || refused.ExecutionReady || !reflect.DeepEqual(refused.UnsupportedBlockers, wantBlockers) {
									t.Fatalf("native import history materialized or lost exact refusal: %+v %v", refused, err)
								}
								if after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store); err != nil || !reflect.DeepEqual(physicalBefore, after) {
									t.Fatalf("native import history refusal changed source, fork or ledger: %v", err)
								}
								prior := f.plan(t)
								if !reflect.DeepEqual(prior.Entities, plan.Entities) || prior.ForkPoint != plan.ForkPoint {
									t.Fatalf("later imported history poisoned the earlier clean cut: before=%+v after=%+v", plan, prior)
								}
								valid, err := f.store.MaterializeRunFork(f.ctx, runfork.RunForkMaterializeRequest{SourceRunID: f.runID, At: f.eventID})
								if err != nil || (phase == "exact_replay" && valid.ForkRunID != forkRunID) {
									t.Fatalf("earlier admitted import cut stopped materializing: %+v %v", valid, err)
								}
								assertSnapshotOwnershipEntity(t, f, valid.ForkRunID, child, 1)
								clean, err := inspector.InspectRunMutationDrift(f.ctx, valid.ForkRunID)
								if err != nil || !reflect.DeepEqual(clean, mutationlog.DriftReport{RunID: valid.ForkRunID, EntitiesChecked: wantEntities, Rows: []mutationlog.DriftRow{}}) {
									t.Fatalf("earlier imported member lost clean native fork oracle: %+v %v", clean, err)
								}
								if phase == "exact_replay" {
									if after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store); err != nil || !reflect.DeepEqual(physicalBefore, after) {
										t.Fatalf("earlier exact replay changed source, fork or ledger: %v", err)
									}
								}
								return
							}
							var raw []byte
							var revision int64
							if err := f.db.QueryRowContext(f.ctx, `SELECT revision, fact FROM run_fork_fact_revisions WHERE run_id=$1 AND family='entity_metadata' AND fact_key=$2 AND revision <= $3 ORDER BY revision DESC LIMIT 1`, f.runID, metadataEntityID, plan.ForkPoint.Revision).Scan(&revision, &raw); err != nil {
								t.Fatal(err)
							}
							var fact map[string]any
							if err := json.Unmarshal(raw, &fact); err != nil {
								t.Fatal(err)
							}
							if strings.HasSuffix(invalid, "flow") {
								fact["flow_instance"] = ""
							}
							if strings.HasSuffix(invalid, "type") {
								fact["entity_type"] = ""
							}
							if invalid == "duplicate_flow" {
								fact["flow_instance"] = "foreign/one"
							}
							if invalid == "duplicate_type" {
								fact["entity_type"] = "foreign_type"
							}
							switch invalid {
							case "empty_import_forged_kind":
								fact["construction_kind"] = "constructed"
							case "empty_import_missing_creation_clock":
								fact["data_created_at"] = nil
							case "empty_import_missing_update_clock":
								fact["data_updated_at"] = nil
							case "empty_import_omitted_creation_clock":
								delete(fact, "data_created_at")
							case "empty_import_omitted_update_clock":
								delete(fact, "data_updated_at")
							case "empty_import_zero_creation_clock":
								fact["data_created_at"] = time.Time{}
							case "empty_import_zero_update_clock":
								fact["data_updated_at"] = time.Time{}
							case "empty_import_reversed_clocks":
								fact["data_updated_at"] = importedAt.Add(-time.Second)
							}
							encoded, err := json.Marshal(fact)
							if err != nil {
								t.Fatal(err)
							}
							// Corrupt only historical evidence beneath a later valid live projection.
							var duplicateFactKey string
							if strings.HasPrefix(invalid, "duplicate") || duplicateImport {
								key := uuid.NewString()
								duplicateFactKey = key
								_, err = f.db.ExecContext(f.ctx, `INSERT INTO run_fork_fact_revisions (run_id,revision,family,fact_key,fact,present) VALUES ($1,$2,'entity_metadata',$3,$4,TRUE)`, f.runID, revision, key, string(encoded))
								if err == nil {
									_, err = f.db.ExecContext(f.ctx, `INSERT INTO run_fork_fact_revisions (run_id,revision,family,fact_key,fact,present) VALUES ($1,$2,'entity_metadata',$3,'{}',FALSE)`, f.runID, latest.ForkPoint.Revision, key)
								}
							} else if invalid == "missing" || invalid == "empty_import_missing_birth" {
								_, err = f.db.ExecContext(f.ctx, `UPDATE run_fork_fact_revisions SET fact='{}', present=FALSE WHERE run_id=$1 AND revision <= $2 AND family='entity_metadata' AND fact_key=$3`, f.runID, plan.ForkPoint.Revision, metadataEntityID)
							} else {
								_, err = f.db.ExecContext(f.ctx, `UPDATE run_fork_fact_revisions SET fact=$1 WHERE run_id=$2 AND revision=$3 AND family='entity_metadata' AND fact_key=$4`, string(encoded), f.runID, revision, metadataEntityID)
							}
							if err != nil {
								t.Fatal(err)
							}
							tx, err := f.db.BeginTx(f.ctx, nil)
							if err != nil {
								t.Fatal(err)
							}
							if backend.name == "postgres" {
								err = runforkrevision.ValidateCompletePostgres(f.ctx, tx, f.runID)
							} else {
								err = runforkrevision.ValidateCompleteSQLite(f.ctx, tx, f.runID)
							}
							_ = tx.Rollback()
							if emptyImport && !duplicateImport {
								want := fmt.Sprintf("run %s has unsupported unrevisioned entity_metadata facts; recreate the store and retry", f.runID)
								if err == nil || err.Error() != want {
									t.Fatalf("forged import birth escaped native revision validation: %v", err)
								}
							} else if err != nil {
								t.Fatalf("latest projection must remain valid despite historical corruption: %v", err)
							}
							before := snapshotOwnershipCounts(t, f, f.runID)
							physicalBefore, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store)
							if err != nil {
								t.Fatal(err)
							}
							if emptyImport {
								report, err := inspector.InspectRunMutationDrift(f.ctx, f.runID)
								if duplicateImport {
									want := mutationlog.DriftReport{RunID: f.runID, EntitiesChecked: wantEntities, Rows: []mutationlog.DriftRow{}}
									if err != nil || !reflect.DeepEqual(report, want) {
										t.Fatalf("historical-only duplicate changed the clean latest import: got=%+v want=%+v err=%v", report, want, err)
									}
								} else if invalid == "empty_import_missing_birth" || invalid == "empty_import_forged_kind" {
									want := mutationlog.DriftReport{RunID: f.runID, EntitiesChecked: wantEntities, Rows: []mutationlog.DriftRow{{
										Kind: "entity_presence", EntityID: metadataEntityID, FoldedPresent: false, StoredPresent: true, FoldedType: "entity", StoredType: "entity",
									}}}
									if err != nil || !reflect.DeepEqual(report, want) {
										t.Fatalf("missing empty-import birth hid membership or reported imprecise drift: got=%+v want=%+v err=%v", report, want, err)
									}
								} else {
									var historyErr *mutationlog.HistoryError
									if !errors.As(err, &historyErr) || historyErr.RunID != f.runID || historyErr.Code != "invalid_mutation_history" || historyErr.Reason != "data-only historical birth requires admitted identity and data clocks" {
										t.Fatalf("forged empty-import birth did not produce the exact oracle history refusal: %v", err)
									}
								}
							}
							got, planErr := f.store.PlanRunFork(f.ctx, runfork.RunForkPlanRequest{SourceRunID: f.runID, At: f.eventID})
							if planErr == nil && !runForkTestHasPlanBlocker(got, runfork.RunForkBlockerEntitySnapshotMetadataUnproven) {
								t.Fatalf("invalid historical metadata admitted: %#v", got)
							}
							wantImportRefusal := fmt.Sprintf("run %s has unsupported unrevisioned entity_metadata facts; recreate the store and retry", f.runID)
							if emptyImport && !duplicateImport && (planErr == nil || planErr.Error() != wantImportRefusal) {
								t.Fatalf("empty import planning refused for the wrong reason: %v", planErr)
							}
							_, materializeErr := f.store.MaterializeRunFork(f.ctx, runfork.RunForkMaterializeRequest{SourceRunID: f.runID, At: f.eventID})
							if materializeErr == nil || (emptyImport && !duplicateImport && materializeErr.Error() != wantImportRefusal) {
								t.Fatalf("invalid historical metadata materialized or refused for the wrong reason: %v", materializeErr)
							}
							if duplicateImport {
								for _, refusal := range []error{planErr, materializeErr} {
									want := fmt.Sprintf("run fork historical entity_metadata fact key %q disagrees with body identity %q", duplicateFactKey, metadataEntityID)
									if refusal == nil || refusal.Error() != want {
										t.Fatalf("duplicate empty-import birth lost exact native fact-owner refusal: got=%v want=%s", refusal, want)
									}
								}
							}
							if after, err := ReadSelectedForkApplicationStorageSnapshotForTest(f.ctx, f.store); err != nil || !reflect.DeepEqual(physicalBefore, after) {
								t.Fatalf("invalid historical admission changed source, fork or ledger evidence: %v", err)
							}
							if after := snapshotOwnershipCounts(t, f, f.runID); after != before {
								t.Fatalf("invalid admission changed source: before=%v after=%v", before, after)
							}
							if after := readSnapshotOwnershipEntity(t, f, f.runID); !reflect.DeepEqual(after, sourceEntityBefore) {
								t.Fatalf("invalid admission changed committed source snapshot: before=%#v after=%#v", sourceEntityBefore, after)
							}
							var forks int
							if err := f.db.QueryRowContext(f.ctx, `SELECT COUNT(*) FROM runs WHERE forked_from_run_id=$1`, f.runID).Scan(&forks); err != nil {
								t.Fatal(err)
							}
							wantForks := 0
							if phase == "exact_replay" {
								wantForks = 1
								if after := readSnapshotOwnershipEntity(t, f, forkRunID); !reflect.DeepEqual(after, forkEntityBefore) {
									t.Fatalf("invalid replay changed committed child snapshot: before=%#v after=%#v", forkEntityBefore, after)
								}
								if after := snapshotOwnershipCounts(t, f, forkRunID); after != forkBefore {
									t.Fatalf("invalid replay changed child: before=%v after=%v", forkBefore, after)
								}
							}
							if forks != wantForks {
								t.Fatalf("invalid metadata left %d forks, want %d", forks, wantForks)
							}
						})
					}
				})
			}
		})
	}
}
