package runforkexecution

import (
	"context"
	"database/sql"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/actors"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/pinrouting"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	llmselection "github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	runforkrevision "github.com/division-sh/swarm/internal/store/testutil/runforkrevisionfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
	"github.com/google/uuid"
)

type constructedInputAgent struct{ selectedContractForkTestAgent }

func (a *constructedInputAgent) OnEvent(_ context.Context, event events.Event) ([]events.Event, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.runIDs = append(a.runIDs, event.RunID())
	a.eventIDs = append(a.eventIDs, event.ID())
	return nil, nil
}

// The source headers are explicit constructor fixtures. Selected preparation,
// materialization, delivery, agent admission and settlement execute normally.
func TestSelectedConstructedAgentInputExecutesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) { proveSelectedConstructedAgentInput(t, backend, false, false) })
	}
}

func TestSelectedNestedConstructedAgentInputExecutesBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, fields := range []bool{false, true} {
			name := "fieldless"
			if fields {
				name = "field_bearing"
			}
			t.Run(backend+"/"+name, func(t *testing.T) { proveSelectedConstructedAgentInput(t, backend, true, fields) })
		}
	}
}

func proveSelectedConstructedAgentInput(t *testing.T, backend string, nested, fields bool) {
	t.Helper()
	var db *sql.DB
	var selected startupownership.Store
	var owner SelectedContractExecutionOwner
	if backend == "sqlite" {
		s := storetest.StartSQLiteRuntimeStore(t)
		selected, db, owner = s, storetest.Database(s), selectedContractSQLiteExecutionOwnerForTest(t, s)
	} else {
		_, db, _ = testutil.StartPostgres(t)
		s := storetest.AdmitPostgresRuntimeStore(t, db)
		selected, owner = s, selectedContractExecutionOwnerForTest(t, s)
	}
	ctx, repo := runForkTestContext(t), runForkExecutionRepoRoot(t)
	root := canonicalrouting.CopySelectedConstructedAgentInput(t)
	if nested {
		root = canonicalrouting.CopySelectedNestedConstructedAgentInput(t, fields)
	}
	loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo, SourceRoot: root, PlatformSpecPath: contracts.DefaultPlatformSpecFile(repo)}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
	if err != nil {
		t.Fatal(err)
	}
	runID, eventID := uuid.NewString(), uuid.NewString()
	at := time.Unix(1700002200, 0).UTC()
	bundle, _ := semanticview.Bundle(loaded.Source)
	fixture := runlifecyclefixture.Fixture{Origin: runlifecyclefixture.ScenarioSetupOrigin(), RunID: runID,
		Source: loaded.SourceArtifactFact, Artifact: bundle.SourceArtifact, StartedAt: at.Add(-time.Minute)}
	if backend == "sqlite" {
		runlifecyclefixture.RequireSQLite(t, ctx, db, fixture)
	} else {
		runlifecyclefixture.RequirePostgres(t, ctx, db, fixture)
	}
	payload := []byte(`{}`)
	anchorType := events.EventType("task.assigned")
	if nested {
		payload = []byte(`{"work_id":"one"}`)
		anchorType = "construction.started"
	}
	input := eventtest.OperatorInjectedWithRoutingSource(eventID, anchorType, "operator", "", payload, 0, runID, nil,
		events.EventEnvelope{}, eventtest.RootRoutingSource(runID), at)
	input, err = eventtest.AdmitPayload(input, ".", string(anchorType))
	if err != nil {
		t.Fatal(err)
	}
	rootIdentity := flowidentity.Stored(loaded.Source, ".", runID, runID, runID, "")
	instances := []flowidentity.Instance{rootIdentity}
	var child flowidentity.Instance
	if !nested {
		child, err = flowidentity.KeylessChild(loaded.Source, rootIdentity, "child")
		if err != nil {
			t.Fatal(err)
		}
		instances = append(instances, child)
	} else {
		for _, key := range []string{"one", "two"} {
			parent := flowidentity.Derive(loaded.Source, "templ", key)
			parent.ParentRoute = flowidentity.ParentRoute{FlowID: ".", FlowInstance: runID, EntityID: runID}
			parent.ParentEntityID = runID
			instances = append(instances, parent)
			for _, branch := range []string{"child", "audit"} {
				current := parent
				for _, flow := range []string{"templ/" + branch, "templ/" + branch + "/leaf"} {
					current, err = flowidentity.KeylessChild(loaded.Source, current, flow)
					if err != nil {
						t.Fatal(err)
					}
					instances = append(instances, current)
					if key == "one" && flow == "templ/child/leaf" {
						child = current
					}
				}
			}
		}
	}
	writeCtx := effects.WithExecutionMode(correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, runID), loaded.SourceArtifactFact), input.ExecutionMode())
	for _, identity := range instances {
		var constructorInput []string
		if nested && identity.TemplateID == "templ" {
			constructorInput = []string{string(anchorType)}
		}
		command := selectedExecutionSourceFlowCommand(t, writeCtx, loaded, input, identity, constructorInput...)
		result, err := selected.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(writeCtx, command)
		if err != nil || !result.Acknowledged || !result.Created {
			t.Fatalf("component source construction: result=%#v err=%v", result, err)
		}
	}
	if nested {
		// This fixture anchor carries lineage, not admitted subscriber work.
		acknowledged := pipelineobligation.Acknowledged("construction_fixture_anchor")
		storetest.CommitSemanticEventWithInitialFacts(t, writeCtx, selected, input, nil, pipelineobligation.ScopeDirect, &acknowledged)
		lineage := events.EventLineage{RunID: runID, ParentEventID: input.ID(), ExecutionMode: input.ExecutionMode()}
		routingSource := eventtest.StaticFlowRoutingSource(child.TemplateID, child.InstancePath, child.EntityID)
		publication, err := pinrouting.AdmitPublicationIdentity(child.TemplateID, "work.ready", routingSource)
		if err != nil {
			t.Fatal(err)
		}
		input = eventtest.ChildForProducerWithRoutingSource(uuid.NewString(), publication, eventtest.Producer(events.EventProducerPlatform, "workflow"), "", payload, 1, lineage,
			events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, child.EntityID), child.InstancePath),
			routingSource, at.Add(time.Second))
		input, err = eventtest.AdmitPayload(input, child.TemplateID, "work.ready")
		if err != nil {
			t.Fatal(err)
		}
		eventID = input.ID()
	}
	materialized, err := manager.ConstructedFlowMaterialization(loaded.Source, runID, child, map[string]any{})
	if err != nil || len(materialized.Agents) != 1 {
		t.Fatalf("exact constructed child declaration: plan=%#v err=%v", materialized, err)
	}
	record, err := materialized.Agents[0].Materialize(runID)
	if err != nil {
		t.Fatal(err)
	}
	storetest.CommitSemanticEventWithRoutes(t, writeCtx, selected, input, []events.DeliveryRoute{{
		Recipient: events.MustAgentDeliveryRecipient(record.Config.ID), AgentIdentity: record.Config.Identity,
		Target: events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: child.TemplateID, FlowInstance: child.InstancePath}),
	}}, pipelineobligation.ScopeSubscribed)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if backend == "sqlite" {
		_, err = runforkrevision.CaptureSQLite(ctx, tx, runID, runforkrevision.AllFamilies()...)
	} else {
		_, err = runforkrevision.Capture(ctx, tx, runID, runforkrevision.AllFamilies()...)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if nested {
		plan, err := selected.(interface {
			PlanRunFork(context.Context, runfork.RunForkPlanRequest) (runfork.RunForkPlan, error)
		}).PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: eventID})
		if err != nil {
			t.Fatal(err)
		}
		frontier, err := runforkadmission.AdmitContractFrontier(runforkadmission.ContractFrontierRequest{Plan: plan, Source: loaded.Source})
		if err != nil {
			t.Fatal(err)
		}
		history, err := runforkadmission.AdmitSelectedContractRouteHistory(runforkadmission.SelectedContractRouteHistoryRequest{Plan: plan, Source: loaded.Source, FrontierAdmission: frontier})
		if err != nil {
			t.Fatal(err)
		}
		topology, err := BuildSelectedContractRouteTopology(SelectedContractRouteTopologyRequest{Admission: frontier, RouteAdmission: history})
		if err != nil || !topology.DynamicTopologySupported {
			t.Fatalf("exact constructed route proof: frontier=%+v history=%+v topology=%+v err=%v", frontier, history, topology, err)
		}
	}
	before := selectedPreparationDatabaseSnapshot(t, db, backend)
	agents := map[agentidentity.Identity]*constructedInputAgent{}
	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{
		SourceRunID: runID, At: eventID, AllowSourceFreeze: true, Owner: owner, SourceLoader: loader,
		ContractSelection: runforkadmission.SelectedContractSelection(loaded.Source),
		AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live,
			ProcessCapability: selectedContractTestProcessCapability(t, ctx, selected),
			Config:            &config.Config{LLM: config.LLMConfig{Backend: llmselection.BackendAnthropic}},
			AgentFactory: func(cfg actors.AgentConfig) (manager.Agent, error) {
				identity, err := cfg.ConcreteIdentity()
				if err != nil {
					return nil, err
				}
				agent := &constructedInputAgent{}
				agent.Configure(cfg)
				agents[identity] = agent
				return agent, nil
			},
		},
	})
	if err != nil || !result.Activation.Activated || result.ExecutedEventCount != 1 {
		t.Fatalf("actual selected constructed-input execution: result=%#v err=%v", result, err)
	}
	wantActors := 1
	if nested {
		wantActors = 8
	}
	if result.AgentRuntimeMaterialization == nil || len(result.AgentRuntimeMaterialization.ConfiguredAgentIdentities) != wantActors || len(agents) != wantActors {
		t.Fatalf("selected runtime omitted known actors: proof=%+v agents=%d", result.AgentRuntimeMaterialization, len(agents))
	}
	var executions int
	for identity, agent := range agents {
		if identity.RunID != result.Materialization.ForkRunID {
			t.Fatal("source/sibling actor adopted into selected runtime")
		}
		if nested && (identity.Route.InstancePath == "templ/child" || identity.Route.InstancePath == "templ/audit") {
			t.Fatal("authored-path phantom prepared")
		}
		if seen := agent.SeenRunIDs(); len(seen) != 0 {
			if len(seen) != 1 || seen[0] != result.Materialization.ForkRunID {
				t.Fatalf("actor escaped exact selected owner: %v", seen)
			}
			executions++
		}
	}
	if executions != 1 {
		t.Fatalf("fixed frontier executed %d actors, want one", executions)
	}
	workflow := pipeline.NewWorkflowPersistence(selected.(pipeline.WorkflowPersistenceOwner))
	for _, source := range instances[1:] {
		childOwner := flowidentity.RunScopedFlowInstance{RunID: result.Materialization.ForkRunID, Route: source.Route()}
		header, found, err := workflow.LoadWorkflowInstance(ctx, childOwner)
		parent, parentErr := runfork.ProjectParentRoute(runID, childOwner.RunID, source.ParentRoute)
		if err != nil || parentErr != nil || !found || header.ParentFlowID != parent.FlowID || header.ParentFlowInstance != parent.FlowInstance || header.ParentEntityID != parent.EntityID {
			t.Fatalf("fork header lost exact constructor parent: found=%v header=%#v err=%v parentErr=%v", found, header, err, parentErr)
		}
		if fields && source.TemplateID != "templ" && header.Fields["value"] != "retained" {
			t.Fatalf("fork companion lost retained business field: %+v", header.Fields)
		}
	}
	after := selectedPreparationDatabaseSnapshot(t, db, backend)
	for table, rows := range before {
		if table == "runs" {
			continue
		}
		for _, row := range rows {
			if strings.Contains(row, runID) && !slices.Contains(after[table], row) {
				t.Fatalf("fork changed source evidence in %s: %s", table, row)
			}
		}
	}

}
