package runforkpersistence

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/manager"
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
			for _, change := range []string{"exact", "mixed_static_associations", "reordered_associations", "duplicate_entity", "contradictory_entity", "wrong_entity_type", "missing_event_mode"} {
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
					got, err := runforkreadiness.Project(plan, source, planning, modes, manager.AgentManagerOptions{})
					wantOK := change == "exact" || change == "mixed_static_associations" || change == "reordered_associations"
					if (err == nil) != wantOK {
						t.Fatalf("canonical owner projection: %v, want acceptance %t", err, wantOK)
					}
					if wantOK && (len(got.States) != 1 || got.States[0].FlowID != flow || got.States[0].ExecutionMode != "" || len(got.States[0].SourceEvents) != 2) {
						t.Fatalf("owner/associations not retained: %+v", got.States)
					}
				})
			}
		})
	}
}

func TestSelectedForkTemplateGenerationAssociations(t *testing.T) {
	source := workflowOwnershipSource(t, canonicalrouting.CopyTemplateInstanceRoute(t, canonicalrouting.TemplateInstanceRouteOptions{Consumer: canonicalrouting.TemplateInstanceAgentConsumer}))
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
			got, err := runforkreadiness.Project(plan, source, planning, modes, manager.AgentManagerOptions{})
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
	source := workflowOwnershipSource(t, canonicalrouting.CopyTemplateInstanceRoute(t, canonicalrouting.TemplateInstanceRouteOptions{Consumer: canonicalrouting.TemplateInstanceAgentConsumer}))
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			db := workflowOwnershipCompanionDatabase(t, backend)
			for _, change := range []string{"fresh_then_exact_reuse", "missing_config", "duplicate_agents", "wrong_agent_route", "missing_readiness", "wrong_readiness", "inactive", "wrong_config", "wrong_entity_owner"} {
				t.Run(change, func(t *testing.T) {
					plan, planning, state, modes, forkID := workflowOwnershipProjection(t, source, "consumer")
					state.ExecutionMode = executionmode.Live
					flow, err := manager.TemplateFlowMaterialization(source, state.FlowID, state.Route.InstancePath, state.EntityID)
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
					prepared, err := runforkreadiness.Project(plan, source, planning, modes, manager.AgentManagerOptions{})
					if err != nil {
						t.Fatal(err)
					}
					if len(prepared.States) != 1 {
						t.Fatalf("canonical template state missing: %+v", prepared.States)
					}
					canonical := prepared.States[0]
					candidate := selectedContractWorkflowState{RunID: forkID, EntityID: canonical.EntityID, EntityType: canonical.EntityType, WorkflowName: canonical.FlowID, WorkflowVersion: canonical.WorkflowVersion, Mode: canonical.Mode, ExecutionMode: canonical.ExecutionMode, Route: canonical.Route.InstancePath, Config: canonical.Config, Agents: canonical.Agents}
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
						t.Fatalf("fresh template state-only construction: %v", err)
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
					got, err := materializeSelectedContractWorkflowState(ctx, tx, backend == "postgres", fact, candidate, time.Now().UTC())
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
	} else if schema.Mode == "template" {
		path, entityType, mode = flow+"/item", "deployment", "template"
	}
	plan := runfork.RunForkPlan{SourceRunID: runID, ForkPoint: runfork.RunForkPoint{Revision: 7}, Entities: []runfork.RunForkEntityState{{EntityID: entityID, MaterializationMetadata: &runfork.RunForkMaterializedEntitySnapshotMetadata{
		Owner: runfork.RunForkMaterializedEntitySnapshotMetadataOwner, Source: runfork.RunForkMaterializedEntitySnapshotMetadataSourceEntityState, FlowInstance: path, EntityType: entityType,
	}}}}
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
		fmt.Sprintf(`CREATE TEMP TABLE flow_instances (run_id %s, instance_path TEXT, flow_template TEXT, mode TEXT, config %s, status TEXT, created_at %s, terminated_at %s, PRIMARY KEY(run_id,instance_path))`, idType, jsonType, timeType, timeType),
		fmt.Sprintf(`CREATE TEMP TABLE flow_instance_runtime_readiness (run_id %s, instance_path TEXT, plan %s, topology_ready_at %s, creation_event_emitted_at %s, created_at %s, updated_at %s, PRIMARY KEY(run_id,instance_path))`, idType, jsonType, timeType, timeType, timeType, timeType),
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
