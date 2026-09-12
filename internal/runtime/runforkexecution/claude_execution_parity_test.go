package runforkexecution

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipelineobligation"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
	runforkrevision "github.com/division-sh/swarm/internal/store/testutil/runforkrevisionfixture"
	"github.com/division-sh/swarm/internal/testutil/runlifecyclefixture"
)

func seedSelectedClaudeExecutionSource(t *testing.T, ctx context.Context, backend string, db *sql.DB, selected startupownership.Store, loaded LoadedSelectedContractSource, runID, entityID, eventID string, at time.Time) {
	t.Helper()
	if backend == "postgres" {
		seedSelectedExecutionRootSourceRun(t, db, runID, entityID, eventID, "task.assigned", at, "test_entity", loaded.SourceArtifactFact)
		seedSourceOutcomeThatMustNotSuppressFork(t, db, eventID, entityID, at)
		captureSelectedExecutionSourceRevision(t, db, runID)
		return
	}
	s := selected.(*store.SQLiteRuntimeStore)
	artifact := selectedExecutionSourceArtifact(t, loaded.SourceArtifactFact.BundleHash())
	if _, err := s.EnsureSourceArtifact(ctx, artifact); err != nil {
		t.Fatal(err)
	}
	runlifecyclefixture.RequireSQLite(t, ctx, db, runlifecyclefixture.Fixture{
		RunID: runID, Origin: runlifecyclefixture.ScenarioSetupOrigin(), Source: loaded.SourceArtifactFact,
		Artifact: artifact, StartedAt: at.Add(-time.Minute),
	})
	payload, err := json.Marshal(map[string]any{"entity_id": entityID})
	if err != nil {
		t.Fatal(err)
	}
	event := eventtest.ExistingRunRootIngressWithRoutingSourceAndMode(eventID, "task.assigned", "source-runtime", "", payload, 0, runID,
		events.EventEnvelope{Scope: events.EventScopeGlobal}, eventtest.RootRoutingSource(runID), at, executionmode.Live)
	route := selectedExecutionTestAgentRoute(t, runID, "test-agent", "worker")
	route.Target = events.MustExistingEntityTarget(events.RouteIdentity{FlowID: "worker", FlowInstance: "worker", EntityID: entityID})
	storetest.CommitSemanticEventWithRoutes(t, ctx, s, event, []events.DeliveryRoute{route}, pipelineobligation.ScopeSubscribed)
	if _, err := db.ExecContext(ctx, `INSERT INTO entity_mutations
		(run_id,entity_id,domain,path,old_value,new_value,caused_by_event,writer_type,writer_id,handler_step,created_at)
		VALUES ($1,$2,'lifecycle_state','','null','"pending"',$3,'platform','selected-execution-test','seed',$4),
		($1,$2,'authored_field','name','null','"Selected Execution Entity"',$3,'platform','selected-execution-test','seed',$4)`, runID, entityID, eventID, at); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO entity_state
		(run_id,entity_id,flow_instance,entity_type,name,current_state,gates,fields,accumulator,revision,entered_state_at,created_at,updated_at)
		VALUES ($1,$2,'worker','test_entity','Selected Execution Entity','pending','{}','{"name":"Selected Execution Entity"}','{}',1,$3,$3,$3)`, runID, entityID, at); err != nil {
		t.Fatal(err)
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
