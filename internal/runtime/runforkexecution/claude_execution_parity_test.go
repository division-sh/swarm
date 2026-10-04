package runforkexecution

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	runforkrevision "github.com/division-sh/swarm/internal/store/testutil/runforkrevisionfixture"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
)

func seedSelectedClaudeExecutionSource(t *testing.T, ctx context.Context, backend string, db *sql.DB, selected startupownership.Store, loaded LoadedSelectedContractSource, runID, eventID string, at time.Time) {
	t.Helper()
	seedSelectedAgentExecutionSource(t, ctx, backend, db, selected, loaded, runID, eventID, at, executionmode.Live)
}

func seedSelectedAgentExecutionSource(t *testing.T, ctx context.Context, backend string, db *sql.DB, selected startupownership.Store, loaded LoadedSelectedContractSource, runID, eventID string, at time.Time, mode executionmode.Mode) {
	t.Helper()
	artifact := selectedExecutionSourceArtifact(t, loaded.SourceArtifactFact.BundleHash())
	fixture := runlifecyclefixture.Fixture{
		RunID: runID, Origin: runlifecyclefixture.ScenarioSetupOrigin(), Source: loaded.SourceArtifactFact,
		Artifact: artifact, StartedAt: at.Add(-time.Minute),
	}
	if backend == "sqlite" {
		if _, err := selected.(*store.SQLiteRuntimeStore).EnsureSourceArtifact(ctx, artifact); err != nil {
			t.Fatal(err)
		}
		runlifecyclefixture.RequireSQLite(t, ctx, db, fixture)
	} else {
		if _, err := selected.(*store.PostgresStore).EnsureSourceArtifact(ctx, artifact); err != nil {
			t.Fatal(err)
		}
		runlifecyclefixture.RequirePostgres(t, ctx, db, fixture)
	}
	root := flowidentity.Stored(loaded.Source, semanticview.RootExecutionFlowID(loaded.Source), runID, runID, runID, "")
	worker, err := flowidentity.KeylessChild(loaded.Source, root, "worker")
	if err != nil {
		t.Fatal(err)
	}
	entityID := worker.EntityID
	payload, err := json.Marshal(map[string]any{"entity_id": entityID})
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(eventID, "task.assigned", "source-runtime", "", payload, 0, runID,
		events.EventEnvelope{Scope: events.EventScopeGlobal}, eventtest.RootRoutingSource(runID), at, mode)
	route := selectedExecutionTestAgentRoute(t, runID, "test-agent", "worker")
	route.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "worker", FlowInstance: "worker", EntityID: entityID})
	storetest.CommitSemanticEventWithRoutes(t, ctx, selected, event, []events.DeliveryRoute{route}, pipelineobligation.ScopeSubscribed)
	ctx = effects.WithExecutionMode(correlation.WithSourceArtifactFact(correlation.WithRunID(ctx, runID), loaded.SourceArtifactFact), event.ExecutionMode())
	rootCommand := selectedExecutionSourceFlowCommand(t, ctx, loaded, event, root)
	workerCommand := selectedExecutionSourceFlowCommand(t, ctx, loaded, event, worker)
	rootCommand.Plan.Children = append(rootCommand.Plan.Children, workerCommand.Plan)
	rootCommand.RouteTopology = append(rootCommand.RouteTopology, workerCommand.RouteTopology...)
	if err := rootCommand.Validate(); err != nil {
		t.Fatal(err)
	}
	committed, err := selected.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(ctx, rootCommand)
	if err != nil || !committed.Created || !committed.Acknowledged || len(committed.Children) != 1 || !committed.Children[0].Created || !committed.Children[0].Acknowledged {
		t.Fatalf("component source tree construction: committed=%+v err=%v", committed, err)
	}
	failure := runtimefailures.Normalize(runtimefailures.New(runtimefailures.ClassConnectorFailure, "source_dead_letter", "run-fork-test", "seed", nil), "run-fork-test", "seed")
	failureRaw, err := json.Marshal(failure)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO event_receipts
		(event_id,subscriber_type,subscriber_id,entity_id,flow_instance,outcome,reason_code,side_effects,processed_at)
		VALUES ($1,'platform','old-source-node',$2,'flow-a/1','success','source_outcome_must_not_suppress_fork','{}',$3)`, eventID, entityID, at); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO dead_letters
		(original_event_id,original_event,entity_id,flow_instance,failure,handler_node,created_at)
		VALUES ($1,'item.received',$2,'flow-a/1',$3,'old-source-node',$4)`, eventID, entityID, string(failureRaw), at); err != nil {
		t.Fatal(err)
	}
	if backend == "postgres" {
		captureSelectedExecutionSourceRevision(t, db, runID)
		return
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := runforkrevision.CaptureSQLite(ctx, tx, runID, runforkrevision.AllFamilies()...); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
