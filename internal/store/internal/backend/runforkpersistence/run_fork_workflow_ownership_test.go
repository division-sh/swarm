package runforkpersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/runforkreadiness"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestSelectedForkWorkflowOwnerAssociations(t *testing.T) {
	source := workflowOwnershipSource(t, canonicalrouting.CopyForkReceiverNestedOwnership(t, []canonicalrouting.ForkReceiver{{Path: "left", Policy: canonicalrouting.ForkReceiverOptionalExisting}, {Path: "right", Policy: canonicalrouting.ForkReceiverOptionalExisting}}))
	for _, flow := range []string{".", "branch", "branch/producer", "branch/left", "branch/right"} {
		t.Run(flow, func(t *testing.T) {
			for _, change := range []string{"exact", "mixed_static_associations", "reordered_associations", "mock_process", "missing_process_posture", "duplicate_entity", "contradictory_entity", "wrong_entity_type", "missing_event_mode"} {
				t.Run(change, func(t *testing.T) {
					plan, planning, _, modes, _ := workflowOwnershipProjection(t, source, flow)
					switch change {
					case "mixed_static_associations":
						modes["event-b"] = executionmode.Mock
					case "reordered_associations":
						planning.RecipientPlanEvents[0], planning.RecipientPlanEvents[1] = planning.RecipientPlanEvents[1], planning.RecipientPlanEvents[0]
					case "duplicate_entity", "contradictory_entity":
						other := plan.Entities[0]
						if change == "contradictory_entity" {
							meta := *other.MaterializationMetadata
							meta.FlowInstance = "foreign"
							other.MaterializationMetadata = &meta
						}
						plan.Entities = append(plan.Entities, other)
					case "wrong_entity_type":
						plan.Entities[0].MaterializationMetadata.EntityType = "foreign"
					case "missing_event_mode":
						delete(modes, "event-b")
					}
					posture := executionposture.Live
					if change == "mock_process" {
						posture = executionposture.MockOnly
						modes["event-a"], modes["event-b"] = executionmode.Mock, executionmode.Mock
					} else if change == "missing_process_posture" {
						posture = ""
					}
					got, err := runforkreadiness.Project(plan, source, planning, modes, manager.AgentManagerOptions{ExecutionPosture: posture})
					wantOK := change == "exact" || change == "mixed_static_associations" || change == "reordered_associations" || change == "mock_process"
					if (err == nil) != wantOK {
						t.Fatalf("canonical owner projection: %v, want acceptance %t", err, wantOK)
					}
					if wantOK {
						wantCount := 1
						if flow == "branch/left" || flow == "branch/right" {
							wantCount = 2
						}
						matched := false
						for _, state := range got.States {
							matched = matched || state.FlowID == flow
							if state.ExecutionMode != posture.RootMode() || len(state.SourceEvents) != 2 ||
								state.SourceEvents[0].ExecutionMode != modes[state.SourceEvents[0].SourceEventID] || state.SourceEvents[1].ExecutionMode != modes[state.SourceEvents[1].SourceEventID] {
								t.Fatalf("owner/associations not retained: %+v", got.States)
							}
						}
						if len(got.States) != wantCount || !matched {
							t.Fatalf("complete constructed receiver set not retained: %+v", got.States)
						}
					}
				})
			}
		})
	}
}

func TestSelectedForkTemplateGenerationAssociations(t *testing.T) {
	source := workflowOwnershipSource(t, canonicalrouting.CopyReceiverConfigHistory(t))
	for _, change := range []string{"live", "mock", "conflicting_generation", "missing_generation"} {
		t.Run(change, func(t *testing.T) {
			plan, planning, _, modes, _ := workflowOwnershipProjection(t, source, "consumer")
			switch change {
			case "mock":
				modes["event-a"], modes["event-b"] = executionmode.Mock, executionmode.Mock
			case "conflicting_generation":
				modes["event-b"] = executionmode.Mock
			case "missing_generation":
				delete(modes, "event-b")
			}
			got, err := runforkreadiness.Project(plan, source, planning, modes, manager.AgentManagerOptions{ExecutionPosture: executionposture.Live})
			wantOK := change == "live" || change == "mock"
			if (err == nil) != wantOK {
				t.Fatalf("canonical template generation: %v, want acceptance %t", err, wantOK)
			}
			if wantOK && (len(got.States) != 1 || got.States[0].ExecutionMode != modes["event-a"]) {
				t.Fatalf("generation mode lost: %+v", got.States)
			}
		})
	}
}

func TestSelectedForkTemplateCompanionReadinessBothStores(t *testing.T) {
	source := workflowOwnershipSource(t, canonicalrouting.CopyReceiverConfigHistory(t))
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := workflowOwnershipCompanionDatabase(t, backend)
			for _, change := range []string{"fresh_then_exact_reuse", "missing_config", "duplicate_agents", "wrong_agent_route", "missing_readiness", "wrong_readiness", "inactive", "wrong_config", "wrong_numeric_kind", "wrong_entity_owner"} {
				t.Run(change, func(t *testing.T) {
					plan, planning, state, modes, forkID := workflowOwnershipProjection(t, source, "consumer")
					state.ExecutionMode = executionmode.Live
					config, err := pipeline.WorkflowInstanceBusinessConfigForRoute(state.Route, plan.Entities[0].MaterializationMetadata.FlowConfig)
					if err != nil {
						t.Fatal(err)
					}
					flow, err := manager.TemplateFlowMaterialization(source, state.FlowID, state.Route.InstancePath, state.EntityID, config)
					if err != nil {
						t.Fatal(err)
					}
					state.Config = flow.Config
					for _, agent := range flow.Agents {
						revision, err := manager.AgentConfigPlanRevision(agent.Config, agent.Identity)
						if err != nil {
							t.Fatal(err)
						}
						state.Agents = append(state.Agents, runfork.RunForkSelectedContractAgentExpectation{Plan: agent.Identity, ConfigRevision: revision})
					}
					if len(state.Agents) == 0 {
						t.Fatal("fixture did not produce a declaration-owned agent")
					}
					prepared, err := runforkreadiness.Project(plan, source, planning, modes, manager.AgentManagerOptions{ExecutionPosture: executionposture.Live})
					if err != nil {
						t.Fatal(err)
					}
					if len(prepared.States) != 1 {
						t.Fatalf("canonical template state missing: %+v", prepared.States)
					}
					canonical := prepared.States[0]
					candidate := selectedContractWorkflowState{SourceRunID: plan.SourceRunID, RunID: forkID, EntityID: canonical.EntityID, EntityType: canonical.EntityType, WorkflowName: canonical.FlowID, WorkflowVersion: canonical.WorkflowVersion, Mode: canonical.Mode, ExecutionMode: canonical.ExecutionMode, Route: canonical.Route.InstancePath, Config: canonical.Config, Agents: canonical.Agents, History: plan.Entities[0]}
					fact, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
					if err != nil {
						t.Fatal(err)
					}
					ctx := context.Background()
					tx, err := db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					if _, err := tx.Exec(`INSERT INTO entity_state (run_id,entity_id,flow_instance,entity_type) VALUES ($1,$2,$3,$4)`, candidate.RunID, candidate.EntityID, candidate.Route, candidate.EntityType); err != nil {
						t.Fatal(err)
					}
					switch change {
					case "missing_config":
						candidate.Config = nil
					case "duplicate_agents":
						candidate.Agents = append(candidate.Agents, candidate.Agents[0])
					case "wrong_agent_route":
						candidate.Agents[0].Plan.Route.InstancePath = "consumer/other"
					}
					topologies, err := materializeSelectedContractWorkflowState(ctx, tx, backend == "postgres", fact, candidate, time.Now().UTC())
					if change == "missing_config" || change == "duplicate_agents" || change == "wrong_agent_route" {
						if err == nil {
							t.Fatal("invalid declaration inputs admitted")
						}
						return
					}
					if err != nil || len(topologies) != len(state.Agents) {
						t.Fatalf("fresh template historical-header construction: %v", err)
					}
					var persisted []byte
					if err := tx.QueryRow(`SELECT config FROM flow_instances WHERE run_id=$1 AND instance_path=$2`, candidate.RunID, candidate.Route).Scan(&persisted); err != nil {
						t.Fatal(err)
					}
					readback, err := pipeline.WorkflowInstanceBusinessConfigForRoute(canonical.Route, persisted)
					if err != nil {
						t.Fatal(err)
					}
					wire, err := canonicaljson.MarshalPreservingNumberKinds(readback)
					if err != nil || string(wire) != `{"flow_path":["business","path"],"nested":[7,7.0,null],"status":false,"vertical_id":"recorded-business-key"}` {
						t.Fatalf("materialized business config = %s: %v", wire, err)
					}
					if change == "wrong_numeric_kind" {
						candidate.Config["nested"].([]any)[1] = int64(7)
					}
					query := ""
					switch change {
					case "missing_readiness":
						query = `DELETE FROM flow_instance_runtime_readiness`
					case "wrong_readiness":
						query = `UPDATE flow_instance_runtime_readiness SET plan = '{}'`
					case "inactive":
						query = `UPDATE flow_instances SET status = 'draining'`
					case "wrong_config":
						query = `UPDATE flow_instances SET config = '{}'`
					case "wrong_entity_owner":
						query = `UPDATE entity_state SET flow_instance = 'consumer/other'`
					}
					if query != "" {
						if _, err := tx.Exec(query); err != nil {
							t.Fatal(err)
						}
					}
					before := workflowOwnershipCompanionSnapshot(t, tx)
					got, err := requireSelectedContractWorkflowState(ctx, tx, backend == "postgres", fact, candidate)
					if (err == nil) != (change == "fresh_then_exact_reuse") {
						t.Fatalf("existing template companion reuse: %v", err)
					}
					if err == nil && !reflect.DeepEqual(topologies, got) {
						t.Fatal("exact reuse changed admitted topology")
					}
					if after := workflowOwnershipCompanionSnapshot(t, tx); !reflect.DeepEqual(before, after) {
						t.Fatal("existing companion was repaired, revived, or rehomed")
					}
				})
			}
		})
	}
}

func TestSelectedForkHistoricalFieldlessHeaderBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := workflowOwnershipCompanionDatabase(t, backend)
			for _, staged := range []bool{false, true} {
				for _, change := range []string{"exact", "imported_state", "missing_lifecycle", "spurious_fields", "spurious_state", "mismatched_type", "missing_execution_mode", "dynamic_agents", "missing_readiness", "wrong_readiness"} {
					t.Run(fmt.Sprintf("staged_%t/%s", staged, change), func(t *testing.T) {
						ctx := context.Background()
						tx, err := db.BeginTx(ctx, nil)
						if err != nil {
							t.Fatal(err)
						}
						defer tx.Rollback()
						entered := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
						metadata := &runfork.RunForkMaterializedEntitySnapshotMetadata{Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance, FlowInstance: "receiver", StageDefined: staged}
						sourceRun, childRun, sourceEntity := uuid.NewString(), uuid.NewString(), flowidentity.EntityID(metadata.FlowInstance)
						projection, err := runfork.ProjectEntityOwnership(sourceRun, childRun, sourceEntity, metadata.FlowInstance)
						if err != nil {
							t.Fatal(err)
						}
						state := selectedContractWorkflowState{SourceRunID: sourceRun, RunID: childRun, EntityID: projection.Fork.EntityID, WorkflowName: "receiver", WorkflowVersion: "fixture", Mode: "static", ExecutionMode: executionmode.Mock, Route: projection.Fork.FlowInstance, Config: map[string]any{}, History: runfork.RunForkEntityState{EntityID: sourceEntity, CurrentState: "pending", EnteredStateAt: &entered, MaterializationMetadata: metadata}}
						fact, err := correlation.NewSourceArtifactFact("bundle-v2:sha256:" + strings.Repeat("a", 64))
						if err != nil {
							t.Fatal(err)
						}
						switch change {
						case "imported_state":
							metadata.Source = runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState
						case "missing_lifecycle":
							state.History.EnteredStateAt = nil
						case "spurious_fields":
							state.History.Fields = map[string]any{"fake": "field"}
						case "spurious_state":
							if _, err := tx.Exec(`INSERT INTO entity_state (run_id,entity_id,flow_instance,entity_type) VALUES ($1,$2,$3,'fake')`, state.RunID, state.EntityID, state.Route); err != nil {
								t.Fatal(err)
							}
						case "mismatched_type":
							metadata.EntityType = "fake"
						case "missing_execution_mode":
							state.ExecutionMode = ""
						case "dynamic_agents":
							state.Agents = []runfork.RunForkSelectedContractAgentExpectation{{}}
						}
						before := workflowOwnershipCompanionSnapshot(t, tx)
						_, err = materializeSelectedContractWorkflowState(ctx, tx, backend == "postgres", fact, state, time.Now().UTC())
						if change != "exact" && change != "missing_readiness" && change != "wrong_readiness" {
							if err == nil {
								t.Fatal("invalid historical header acquired execution permission")
							}
							if after := workflowOwnershipCompanionSnapshot(t, tx); !reflect.DeepEqual(before, after) {
								t.Fatal("refused historical construction changed selected state")
							}
							return
						}
						if err != nil {
							t.Fatal(err)
						}
						var phase string
						var attempt uint64
						var raw []byte
						var hash string
						if err := tx.QueryRow(`SELECT plan,plan_hash,phase,activation_attempt_id FROM flow_instance_runtime_readiness WHERE run_id=$1 AND instance_path=$2`, state.RunID, state.Route).Scan(&raw, &hash, &phase, &attempt); err != nil {
							t.Fatalf("static receiver has no attachment authority: %v", err)
						}
						plan, err := pipeline.DecodeFlowReadinessPlan(raw, hash)
						if err != nil || plan.ExecutionMode != state.ExecutionMode || phase != "planned" || attempt != 1 || len(plan.Agents) != 0 {
							t.Fatalf("static attachment evidence: %+v %s %d %v", plan, phase, attempt, err)
						}
						var count int
						if err := tx.QueryRow(`SELECT count(*) FROM entity_state`).Scan(&count); err != nil || count != 0 {
							t.Fatalf("fieldless history created entity fields: %d %v", count, err)
						}
						var stageDefined bool
						var current, entityID string
						if err := tx.QueryRow(`SELECT stage_defined,current_state,entity_id FROM flow_instances`).Scan(&stageDefined, &current, &entityID); err != nil || stageDefined != staged || current != "pending" || entityID != state.EntityID {
							t.Fatalf("historical header identity/stage changed: %t %s %s %v", stageDefined, current, entityID, err)
						}
						if change == "missing_readiness" {
							if _, err := tx.Exec(`DELETE FROM flow_instance_runtime_readiness`); err != nil {
								t.Fatal(err)
							}
						} else if change == "wrong_readiness" {
							if _, err := tx.Exec(`UPDATE flow_instance_runtime_readiness SET plan='{}'`); err != nil {
								t.Fatal(err)
							}
						}
						snapshot := workflowOwnershipCompanionSnapshot(t, tx)
						if _, err := requireSelectedContractWorkflowState(ctx, tx, backend == "postgres", fact, state); (err == nil) != (change == "exact") {
							t.Fatalf("read-only static reuse: %v", err)
						}
						if after := workflowOwnershipCompanionSnapshot(t, tx); !reflect.DeepEqual(snapshot, after) {
							t.Fatal("historical header replay repeated construction")
						}
					})
				}
			}
		})
	}
}

func workflowOwnershipSource(t *testing.T, dir string) semanticview.Source {
	t.Helper()
	repo := canonicalrouting.RepoRoot(t)
	bundle, err := contracts.LoadWorkflowContractBundleWithOverrides(repo, dir, contracts.DefaultPlatformSpecFile(repo))
	if err != nil {
		t.Fatal(err)
	}
	return semanticview.Wrap(bundle)
}

func workflowOwnershipProjection(t *testing.T, source semanticview.Source, flow string) (runfork.RunForkPlan, runfork.RunForkSelectedContractRecipientPlanning, runfork.RunForkSelectedContractWorkflowState, map[string]executionmode.Mode, string) {
	t.Helper()
	runID, forkID, entityID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	path, entityType, mode := flow, "receipt", "static"
	schema, ok := source.FlowSchemaByID(flow)
	if !ok {
		t.Fatalf("fixture has no authored flow %s", flow)
	}
	if flow == "." {
		path, entityID, entityType = runID, runID, "root"
	} else if flow == "branch" {
		entityType = "root"
	} else if flow == "branch/producer" {
		entityType = "work"
	} else if schema.EffectiveMode() == "template" {
		path, entityType, mode = flow+"/item", "deployment", "template"
	}
	graph, found := semanticview.WorkflowStageTopology(source, flow)
	if !found {
		t.Fatal("fixture flow has no compiled lifecycle")
	}
	initial, err := graph.InitialStoredStage()
	if err != nil {
		t.Fatal(err)
	}
	enteredAt := time.Now().UTC().Add(-time.Hour)
	configPayload, err := pipeline.WorkflowInstanceConfigPayloadForRoute(flowidentity.RouteForInstancePath(path), source.WorkflowVersion(), nil)
	if err != nil {
		t.Fatal(err)
	}
	config, err := json.Marshal(configPayload)
	if err != nil {
		t.Fatal(err)
	}
	plan := runfork.RunForkPlan{SourceRunID: runID, ForkPoint: runfork.RunForkPoint{Revision: 7}, Entities: []runfork.RunForkEntityState{{EntityID: entityID, CurrentState: initial.ID(), EnteredStateAt: &enteredAt, MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
		Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceFlowInstance, FlowInstance: path, EntityType: entityType, StageDefined: graph.StageCount() != 0,
		FlowTemplate: flow, Mode: mode, FlowConfig: config,
	}}}}
	if flow == "branch/left" || flow == "branch/right" {
		sibling := "branch/right"
		if flow == sibling {
			sibling = "branch/left"
		}
		other := plan.Entities[0]
		other.EntityID = uuid.NewString()
		metadata := *other.MaterializationMetadata
		metadata.FlowInstance = sibling
		metadata.FlowTemplate = sibling
		payload, err := pipeline.WorkflowInstanceConfigPayloadForRoute(flowidentity.RouteForInstancePath(sibling), source.WorkflowVersion(), nil)
		if err != nil {
			t.Fatal(err)
		}
		metadata.FlowConfig, err = json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		other.MaterializationMetadata = &metadata
		plan.Entities = append(plan.Entities, other)
	}
	if mode == "template" {
		plan.Entities[0].MaterializationMetadata.FlowConfig = json.RawMessage(`{"instance_id":"item","storage_ref":"consumer/item","flow_path":"consumer/item","config":{"vertical_id":"recorded-business-key","nested":[7,7.0,null],"status":false,"flow_path":["business","path"]}}`)
	}
	plan = plan.WithHistoricalEvents(7, []string{"event-a", "event-b"})
	eventName := "branch/producer/work.ready"
	producer := eventtest.StaticFlowRoutingSource("branch/producer", "branch/producer", uuid.NewString())
	switch flow {
	case ".":
		eventName = "outer.requested"
		producer = eventtest.RootRoutingSource(runID)
	case "branch":
		eventName = "start.requested"
		producer = eventtest.RootRoutingSource(runID)
	case "branch/producer":
		eventName = "branch/work.requested"
		producer = eventtest.StaticFlowRoutingSource("branch", "branch", uuid.NewString())
	case "consumer":
		eventName = "consumer/item/deploy.done"
		producer = eventtest.ConcreteTemplateRoutingSource("consumer", "consumer/item", entityID)
	}
	for _, eventID := range []string{"event-a", "event-b"} {
		target, err := events.NewExistingEntityTarget(events.RouteIdentity{FlowID: flow, FlowInstance: path, EntityID: entityID})
		if err != nil {
			t.Fatal(err)
		}
		plan.PendingWork = append(plan.PendingWork, runfork.RunForkPendingWork{EventID: eventID, EventName: eventName, RoutingSource: producer, FlowInstance: path, DeliveryRoute: events.DeliveryRoute{Target: target}, Classification: runfork.RunForkPendingClassificationPending})
	}
	frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: source, ContractSelection: runfork.RunForkContractSelection{Mode: "selected_contracts"}})
	if err != nil {
		t.Fatal(err)
	}

	planning := runfork.RunForkSelectedContractRecipientPlanning{}
	for _, event := range frontier.FrontierEvents {
		planning.RecipientPlanEvents = append(planning.RecipientPlanEvents, runfork.RunForkSelectedContractRecipientPlanEvent{SourceEventID: event.SourceEventID, EventName: event.EventName, Recipients: event.DerivedRecipients})
	}
	state := runfork.RunForkSelectedContractWorkflowState{SourceEventID: "event-a", EntityID: entityID, EntityType: entityType, FlowID: flow, WorkflowVersion: source.WorkflowVersion(), Mode: mode,
		SourceEvents: []runfork.RunForkSelectedContractWorkflowStateSourceEvent{{SourceEventID: "event-a", ExecutionMode: executionmode.Live}, {SourceEventID: "event-b", ExecutionMode: executionmode.Live}},
		AddressKind:  runfork.RunForkSelectedContractWorkflowStateExact, Route: flowidentity.StoredRoute(flowidentity.ScopeKey(source, flow), flowidentity.LogicalInstanceID(path), path)}
	state.Config = map[string]any{}
	if flow == "." {
		state.AddressKind, state.Route = runfork.RunForkSelectedContractWorkflowStateRunScope, flowidentity.Route{}
	}
	return plan, planning, state, map[string]executionmode.Mode{"event-a": executionmode.Live, "event-b": executionmode.Live}, forkID
}

func workflowOwnershipCompanionDatabase(t *testing.T, backend string) *sql.DB {
	t.Helper()
	var db *sql.DB
	jsonType, timeType, idType := "TEXT", "TEXT", "TEXT"
	if backend == "postgres" {
		_, db, _ = testutil.StartEmptyPostgres(t)
		jsonType, timeType, idType = "JSONB", "TIMESTAMPTZ", "UUID"
	} else {
		var err error
		db, err = sql.Open("sqlite", filepath.Join(t.TempDir(), "companions.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
	}
	db.SetMaxOpenConns(1)
	// Companion-consumer SQL proof; the runtimepersistence matrix separately
	// crosses full-schema materialization, revision, and rollback ownership.
	for _, query := range []string{
		fmt.Sprintf(`CREATE TEMP TABLE entity_state (run_id %s, entity_id %s, flow_instance TEXT, entity_type TEXT, PRIMARY KEY(run_id,entity_id))`, idType, idType),
		fmt.Sprintf(`CREATE TEMP TABLE flow_instances (run_id %s, instance_path TEXT, entity_id %s NOT NULL, entity_type TEXT, slug TEXT, name TEXT, flow_template TEXT, mode TEXT, config %s, status TEXT, stage_defined BOOLEAN NOT NULL, current_state TEXT NOT NULL, gates %s NOT NULL, bookkeeping %s NOT NULL, accumulator %s NOT NULL, revision BIGINT NOT NULL, entered_state_at %s NOT NULL, created_at %s, updated_at %s NOT NULL, terminated_at %s, PRIMARY KEY(run_id,instance_path))`, idType, idType, jsonType, jsonType, jsonType, jsonType, timeType, timeType, timeType, timeType),
		fmt.Sprintf(`CREATE TEMP TABLE flow_instance_runtime_readiness (run_id %s, instance_path TEXT, plan %s, plan_hash TEXT NOT NULL, activation_attempt_id BIGINT NOT NULL DEFAULT 1, activation_attempt_grant_id %s, activation_attempt_state TEXT NOT NULL DEFAULT 'planned', phase TEXT NOT NULL DEFAULT 'planned', creation_event_emitted_at %s, created_at %s, updated_at %s, PRIMARY KEY(run_id,instance_path))`, idType, jsonType, idType, timeType, timeType, timeType),
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func workflowOwnershipCompanionSnapshot(t *testing.T, tx *sql.Tx) []string {
	t.Helper()
	var out []string
	for _, table := range []string{"entity_state", "flow_instances", "flow_instance_runtime_readiness"} {
		rows, err := tx.Query("SELECT * FROM " + table)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			values, destinations := make([]any, len(columns)), make([]any, len(columns))
			for i := range values {
				destinations[i] = &values[i]
			}
			if err := rows.Scan(destinations...); err != nil {
				t.Fatal(err)
			}
			for i, value := range values {
				if bytes, ok := value.([]byte); ok {
					values[i] = string(bytes)
				}
			}
			out = append(out, fmt.Sprintf("%s:%#v", table, values))
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
	}
	return out
}
