package runtimepersistence

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/google/uuid"
	modernsqlite "modernc.org/sqlite"
)

func TestSQLiteWorkflowEngineMutationPreBusyAttemptUsesCallerContext(t *testing.T) {
	functionName := "swarm_block_workflow_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	functionEntered := make(chan struct{})
	functionRelease := make(chan struct{})
	var enterOnce sync.Once
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(functionRelease) }) }
	t.Cleanup(release)
	if err := modernsqlite.RegisterScalarFunction(functionName, 0, func(*modernsqlite.FunctionContext, []driver.Value) (driver.Value, error) {
		enterOnce.Do(func() { close(functionEntered) })
		<-functionRelease
		return int64(1), nil
	}); err != nil {
		t.Fatalf("register blocking SQLite function: %v", err)
	}

	flowID := "prebusy-workflow-" + uuid.NewString()
	instancePath := flowID + "/receiver"
	createdAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	fixture, record := constructWorkflowMutationFixture(t, "sqlite", flowID, createdAt)
	db, baseCtx, runID := fixture.db, fixture.ctx, correlation.RunIDFromContext(fixture.ctx)
	owner, entityID := fixture.store.(runtimepipeline.WorkflowEngineMutationOwner), record.EntityID
	node := mustPersistenceNode(flowID, "engine-prebusy")
	route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(node), Target: events.MustExistingEntityTarget(events.RouteIdentity{FlowID: flowID, FlowInstance: instancePath, EntityID: entityID})}
	event := eventtest.ExistingRunRootIngress(uuid.NewString(), "engine.prebusy.requested", "fixture", "", []byte(`{}`), 0, runID,
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), instancePath), createdAt)
	selected := fixture.store.(stateOnlyAcquisitionStore)
	if err := commitSemanticEventFixtureWithRoutes(baseCtx, selected, event, []events.DeliveryRoute{route}); err != nil {
		t.Fatal(err)
	}
	claimed, err := claimDeliveryFixture(baseCtx, selected, event, route)
	if err != nil {
		t.Fatal(err)
	}
	record = workflowMutationDeliveryEntry(t, record, node, event, claimed.Claim)
	triggerName := "swarm_prebusy_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := db.ExecContext(baseCtx, fmt.Sprintf(`
		CREATE TRIGGER %s
		BEFORE UPDATE ON entity_state
		WHEN OLD.entity_id = '%s'
		BEGIN
			SELECT %s();
		END
	`, triggerName, entityID, functionName)); err != nil {
		t.Fatalf("install blocking workflow mutation trigger: %v", err)
	}

	ctx, cancel := context.WithTimeout(baseCtx, 15*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := owner.CommitWorkflowEngineMutation(ctx, runtimepipeline.WorkflowEngineMutationCommand{State: record})
		done <- err
	}()

	select {
	case <-functionEntered:
	case <-ctx.Done():
		t.Fatalf("workflow mutation did not reach its real SQLite write: %v", ctx.Err())
	}
	timer := time.NewTimer(sqliteRuntimeMutationRetryBudget + 100*time.Millisecond)
	defer timer.Stop()
	select {
	case err := <-done:
		t.Fatalf("valid admitted workflow mutation returned before its write completed: %v", err)
	case <-timer.C:
	}

	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("commit workflow mutation after long non-busy write: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("workflow mutation did not complete after write release: %v", ctx.Err())
	}
	assertWorkflowTargetTransitionRows(t, "sqlite", db, runID, entityID, instancePath, flowID, "done", 2, 1)
}
