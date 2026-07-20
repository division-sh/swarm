package pipeline

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	runtimeauthoractivity "github.com/division-sh/swarm/internal/runtime/authoractivity"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	runtimelifecycleprobe "github.com/division-sh/swarm/internal/runtime/lifecycleprobe"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

type systemNodeCompletionBus struct {
	converged []string
}

func (*systemNodeCompletionBus) SubscribeInternal(string, ...events.EventType) <-chan events.Event {
	return make(chan events.Event)
}

func (*systemNodeCompletionBus) Publish(context.Context, events.Event) error { return nil }

func (b *systemNodeCompletionBus) ConvergeNormalRunCompletionForEvent(_ context.Context, eventID string) error {
	b.converged = append(b.converged, eventID)
	return nil
}

func TestSystemNodeRunner_MarkProcessedSettlesNodeDeliveryAndTriggersNormalRunCompletion(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext(t, context.Background())
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	seedSystemNodeCompletionRun(t, db, runID, eventID, entityID)
	bus := &systemNodeCompletionBus{}
	handlerCalled := false
	handlerObservedStatus := ""
	handlerObservedReason := ""
	runner := newSystemNodeRunner("terminal-node", bus, db, func() []events.EventType {
		return []events.EventType{"example.started"}
	}, func(ctx context.Context, evt events.Event) error {
		handlerCalled = true
		if err := db.QueryRowContext(ctx, `
			SELECT COALESCE(status, ''), COALESCE(reason_code, '')
			FROM event_deliveries
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'node'
			  AND subscriber_id = 'terminal-node'
		`, eventID).Scan(&handlerObservedStatus, &handlerObservedReason); err != nil {
			t.Fatalf("load node delivery during handler: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			UPDATE entity_state
			SET current_state = 'done',
			    updated_at = now()
			WHERE run_id = $1::uuid
			  AND entity_id = $2::uuid
		`, runID, entityID); err != nil {
			t.Fatalf("mark entity terminal: %v", err)
		}
		return nil
	})

	runner.ProcessEventForTest(ctx, eventtest.RunCreatingRootIngress(
		eventID,
		"example.started",
		"",
		"",
		[]byte(`{}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now().UTC(),
	))

	if !handlerCalled {
		t.Fatal("handler was not called")
	}
	if handlerObservedStatus != "in_progress" || handlerObservedReason != "node_processing" {
		t.Fatalf("handler observed node delivery = %s/%s, want in_progress/node_processing", handlerObservedStatus, handlerObservedReason)
	}
	if len(bus.converged) != 1 || bus.converged[0] != eventID {
		t.Fatalf("normal run convergence events = %#v, want %s", bus.converged, eventID)
	}
	var (
		status      string
		reason      string
		deliveredAt sql.NullTime
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, ''), COALESCE(reason_code, ''), delivered_at
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'terminal-node'
	`, eventID).Scan(&status, &reason, &deliveredAt); err != nil {
		t.Fatalf("load node delivery: %v", err)
	}
	if status != "delivered" || reason != "node_processed" || !deliveredAt.Valid {
		t.Fatalf("node delivery = status:%q reason:%q delivered:%v, want delivered node_processed with delivered_at", status, reason, deliveredAt.Valid)
	}
	var receiptOutcome string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(outcome, '')
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'terminal-node'
	`, eventID).Scan(&receiptOutcome); err != nil {
		t.Fatalf("load node receipt: %v", err)
	}
	if receiptOutcome != "no_op" {
		t.Fatalf("node receipt outcome = %q, want no_op", receiptOutcome)
	}
}

func TestSystemNodeRunner_TargetSetSameNodeSettlesEachTargetDelivery(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext(t, context.Background())
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	seedSystemNodeCompletionEventWithoutDelivery(t, db, runID, eventID, entityID)
	targetOne := events.RouteIdentity{FlowInstance: "worker/w-001", EntityID: "worker/w-001"}
	targetTwo := events.RouteIdentity{FlowInstance: "worker/w-002", EntityID: "worker/w-002"}
	seedSystemNodeCompletionTargetDelivery(t, db, runID, eventID, "task-handler", targetOne)
	seedSystemNodeCompletionTargetDelivery(t, db, runID, eventID, "task-handler", targetTwo)

	var handledTargets []string
	runner := newSystemNodeRunner("task-handler", &systemNodeCompletionBus{}, db, func() []events.EventType {
		return []events.EventType{"worker/work.assign"}
	}, func(ctx context.Context, evt events.Event) error {
		target := evt.TargetRoute()
		handledTargets = append(handledTargets, target.FlowInstance)
		status := loadSystemNodeCompletionTargetDeliveryStatus(t, db, eventID, "task-handler", target)
		if status != "in_progress" {
			t.Fatalf("target %s handler observed status = %q, want in_progress", target.FlowInstance, status)
		}
		return nil
	})

	eventForTarget := func(target events.RouteIdentity) events.Event {
		return eventtest.RunCreatingRootIngress(eventID,
			"worker/work.assign", "", "", []byte(`{}`), 0, runID, "", events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), time.Now().UTC())
	}

	runner.ProcessEventForTest(ctx, eventForTarget(targetOne))
	if got := loadSystemNodeCompletionTargetDeliveryStatus(t, db, eventID, "task-handler", targetOne); got != "delivered" {
		t.Fatalf("target one status = %q, want delivered", got)
	}
	if got := loadSystemNodeCompletionTargetDeliveryStatus(t, db, eventID, "task-handler", targetTwo); got != "pending" {
		t.Fatalf("target two status after first delivery = %q, want pending", got)
	}

	runner.ProcessEventForTest(ctx, eventForTarget(targetTwo))
	if got := loadSystemNodeCompletionTargetDeliveryStatus(t, db, eventID, "task-handler", targetTwo); got != "delivered" {
		t.Fatalf("target two status = %q, want delivered", got)
	}
	if len(handledTargets) != 2 || handledTargets[0] != "worker/w-001" || handledTargets[1] != "worker/w-002" {
		t.Fatalf("handled targets = %#v, want both worker targets in order", handledTargets)
	}
	var deliveredRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'task-handler'
		  AND status = 'delivered'
		  AND reason_code = 'node_processed'
	`, eventID).Scan(&deliveredRows); err != nil {
		t.Fatalf("count delivered target rows: %v", err)
	}
	if deliveredRows != 2 {
		t.Fatalf("delivered target rows = %d, want 2", deliveredRows)
	}
}

func TestSystemNodeRunner_TargetSetSameNodeFailureKeepsSiblingPending(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext(t, context.Background())
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	seedSystemNodeCompletionEventWithoutDelivery(t, db, runID, eventID, entityID)
	targetOne := events.RouteIdentity{FlowInstance: "worker/w-001", EntityID: "worker/w-001"}
	targetTwo := events.RouteIdentity{FlowInstance: "worker/w-002", EntityID: "worker/w-002"}
	seedSystemNodeCompletionTargetDelivery(t, db, runID, eventID, "task-handler", targetOne)
	seedSystemNodeCompletionTargetDelivery(t, db, runID, eventID, "task-handler", targetTwo)

	attempts := 0
	runner := newSystemNodeRunner("task-handler", &systemNodeCompletionBus{}, db, func() []events.EventType {
		return []events.EventType{"worker/work.assign"}
	}, func(context.Context, events.Event) error {
		attempts++
		if attempts == 1 {
			return runtimefailures.New(runtimefailures.ClassConnectorFailure, "temporary_target_failure", "pipeline-test", "handle", nil)
		}
		return nil
	})
	runner.SetRetryPolicyForTest(2, func(int) time.Duration {
		targetOneDelivery := loadSystemNodeCompletionTargetDelivery(t, db, eventID, "task-handler", targetOne)
		if targetOneDelivery.Status != "failed" || targetOneDelivery.Reason != "handler_failure" || targetOneDelivery.RetryCount != 1 || targetOneDelivery.Failure == nil {
			t.Fatalf("target one retry delivery = %+v, want failed/handler_error retry=1 with error", targetOneDelivery)
		}
		if got := loadSystemNodeCompletionTargetDeliveryStatus(t, db, eventID, "task-handler", targetTwo); got != "pending" {
			t.Fatalf("target two status during target one retry = %q, want pending", got)
		}
		return 0
	})

	eventForTarget := func(target events.RouteIdentity) events.Event {
		return eventtest.RunCreatingRootIngress(eventID,
			"worker/work.assign", "", "", []byte(`{}`), 0, runID, "", events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), time.Now().UTC())
	}

	runner.ProcessEventForTest(ctx, eventForTarget(targetOne))

	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	if got := loadSystemNodeCompletionTargetDeliveryStatus(t, db, eventID, "task-handler", targetOne); got != "delivered" {
		t.Fatalf("target one final status = %q, want delivered", got)
	}
	if got := loadSystemNodeCompletionTargetDeliveryStatus(t, db, eventID, "task-handler", targetTwo); got != "pending" {
		t.Fatalf("target two final status = %q, want pending", got)
	}
}

func TestSystemNodeRunner_TargetSetSameNodeDeadLetterKeepsSiblingExecutable(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext(t, context.Background())
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	seedSystemNodeCompletionEventWithoutDelivery(t, db, runID, eventID, entityID)
	targetOne := events.RouteIdentity{FlowInstance: "worker/w-001", EntityID: "worker/w-001"}
	targetTwo := events.RouteIdentity{FlowInstance: "worker/w-002", EntityID: "worker/w-002"}
	seedSystemNodeCompletionTargetDelivery(t, db, runID, eventID, "task-handler", targetOne)
	seedSystemNodeCompletionTargetDelivery(t, db, runID, eventID, "task-handler", targetTwo)

	failingRunner := newSystemNodeRunner("task-handler", &systemNodeCompletionBus{}, db, func() []events.EventType {
		return []events.EventType{"worker/work.assign"}
	}, func(context.Context, events.Event) error {
		return runtimefailures.New(runtimefailures.ClassConnectorFailure, "permanent_target_failure", "pipeline-test", "handle", nil)
	})
	failingRunner.SetRetryPolicyForTest(1, func(int) time.Duration { return 0 })

	eventForTarget := func(target events.RouteIdentity) events.Event {
		return eventtest.RunCreatingRootIngress(eventID,
			"worker/work.assign", "", "", []byte(`{}`), 0, runID, "", events.EnvelopeForTargetRoute(events.EventEnvelope{}, target), time.Now().UTC())
	}

	failingRunner.ProcessEventForTest(ctx, eventForTarget(targetOne))

	targetOneDelivery := loadSystemNodeCompletionTargetDelivery(t, db, eventID, "task-handler", targetOne)
	if targetOneDelivery.Status != "dead_letter" || targetOneDelivery.Reason != "handler_terminal_failure" || targetOneDelivery.RetryCount != 1 || targetOneDelivery.Failure == nil {
		t.Fatalf("target one dead-letter delivery = %+v, want dead_letter/retry_exhausted retry=1 with error", targetOneDelivery)
	}
	if got := loadSystemNodeCompletionTargetDeliveryStatus(t, db, eventID, "task-handler", targetTwo); got != "pending" {
		t.Fatalf("target two status after target one dead-letter = %q, want pending", got)
	}

	successRunner := newSystemNodeRunner("task-handler", &systemNodeCompletionBus{}, db, func() []events.EventType {
		return []events.EventType{"worker/work.assign"}
	}, func(context.Context, events.Event) error {
		return nil
	})
	successRunner.ProcessEventForTest(ctx, eventForTarget(targetTwo))
	if got := loadSystemNodeCompletionTargetDeliveryStatus(t, db, eventID, "task-handler", targetTwo); got != "delivered" {
		t.Fatalf("target two status after target one dead-letter = %q, want delivered", got)
	}
}

func TestSQLiteSystemNodeTargetSetSameNodeTransitionsAreTargetScoped(t *testing.T) {
	db := newSQLiteWorkflowInstanceStoreTestDB(t)
	store := newSQLiteWorkflowInstanceStoreForTest(t, db)
	ctx := testAuthorActivityContext(t, context.Background())
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	seedSQLiteSystemNodeCompletionEventWithoutDelivery(t, db, runID, eventID, entityID)
	targetOne := events.RouteIdentity{FlowInstance: "worker/w-001", EntityID: "worker/w-001"}
	targetTwo := events.RouteIdentity{FlowInstance: "worker/w-002", EntityID: "worker/w-002"}
	seedSQLiteSystemNodeCompletionTargetDelivery(t, db, runID, eventID, "task-handler", targetOne)
	seedSQLiteSystemNodeCompletionTargetDelivery(t, db, runID, eventID, "task-handler", targetTwo)

	authorized, err := store.SystemNodeDeliveryAuthorizedForTarget(ctx, "task-handler", eventID, targetOne, DefaultSystemNodeRetryLimit)
	if err != nil {
		t.Fatalf("SystemNodeDeliveryAuthorizedForTarget target one: %v", err)
	}
	if !authorized {
		t.Fatal("target one authorized = false, want true")
	}
	if err := store.MarkSystemNodeDeliveryInProgressForTarget(ctx, "task-handler", eventID, targetOne, DefaultSystemNodeRetryLimit); err != nil {
		t.Fatalf("MarkSystemNodeDeliveryInProgressForTarget target one: %v", err)
	}
	if got := loadSQLiteSystemNodeCompletionTargetDeliveryStatus(t, db, eventID, "task-handler", targetOne); got != "in_progress" {
		t.Fatalf("target one sqlite status = %q, want in_progress", got)
	}
	if got := loadSQLiteSystemNodeCompletionTargetDeliveryStatus(t, db, eventID, "task-handler", targetTwo); got != "pending" {
		t.Fatalf("target two sqlite status after target one in-progress = %q, want pending", got)
	}

	failure := testPipelineFailure(runtimefailures.ClassConnectorFailure, "temporary_target_failure")
	if err := store.MarkSystemNodeDeliveryFailedForTarget(ctx, "task-handler", eventID, targetOne, "handler_error", failure, 1, DefaultSystemNodeRetryLimit); err != nil {
		t.Fatalf("MarkSystemNodeDeliveryFailedForTarget target one: %v", err)
	}
	targetOneDelivery := loadSQLiteSystemNodeCompletionTargetDelivery(t, db, eventID, "task-handler", targetOne)
	if targetOneDelivery.Status != "failed" || targetOneDelivery.Reason != "handler_error" || targetOneDelivery.RetryCount != 1 || targetOneDelivery.Failure == nil {
		t.Fatalf("target one sqlite failed delivery = %+v, want failed/handler_error retry=1 with error", targetOneDelivery)
	}
	if got := loadSQLiteSystemNodeCompletionTargetDeliveryStatus(t, db, eventID, "task-handler", targetTwo); got != "pending" {
		t.Fatalf("target two sqlite status after target one failed = %q, want pending", got)
	}

	sideEffects := systemNodeDeadLetterReceiptSideEffects("task-handler", eventID, "retry_exhausted", DefaultSystemNodeRetryLimit, targetOne)
	if err := store.MarkSystemNodeDeliveryDeadLetterForTarget(ctx, "task-handler", eventID, targetOne, "retry_exhausted", failure, DefaultSystemNodeRetryLimit, sideEffects); err != nil {
		t.Fatalf("MarkSystemNodeDeliveryDeadLetterForTarget target one: %v", err)
	}
	targetOneDelivery = loadSQLiteSystemNodeCompletionTargetDelivery(t, db, eventID, "task-handler", targetOne)
	if targetOneDelivery.Status != "dead_letter" || targetOneDelivery.Reason != "retry_exhausted" || targetOneDelivery.RetryCount != DefaultSystemNodeRetryLimit || targetOneDelivery.Failure == nil {
		t.Fatalf("target one sqlite dead-letter delivery = %+v, want dead_letter/retry_exhausted retry=%d with error", targetOneDelivery, DefaultSystemNodeRetryLimit)
	}
	targetTwoProcessed, err := store.SystemNodeProcessedForTarget(ctx, "task-handler", eventID, targetTwo)
	if err != nil {
		t.Fatalf("SystemNodeProcessedForTarget target two: %v", err)
	}
	if targetTwoProcessed {
		t.Fatal("target two processed = true after target one dead-letter, want false")
	}

	sideEffects = systemNodeProcessedReceiptSideEffects("task-handler", eventID, targetTwo)
	if err := store.MarkSystemNodeProcessedAndSettleDeliveryForTarget(ctx, "task-handler", eventID, targetTwo, sideEffects); err != nil {
		t.Fatalf("MarkSystemNodeProcessedAndSettleDeliveryForTarget target two: %v", err)
	}
	if got := loadSQLiteSystemNodeCompletionTargetDeliveryStatus(t, db, eventID, "task-handler", targetTwo); got != "delivered" {
		t.Fatalf("target two sqlite status after target one dead-letter = %q, want delivered", got)
	}
}

func TestSystemNodeRunnerLifecycleProbeEmitsHandlerBoundaries(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext(t, context.Background())
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	seedSystemNodeCompletionRun(t, db, runID, eventID, entityID)
	bus := &systemNodeCompletionBus{}
	probe := runtimelifecycleprobe.New()
	handlerObservedStatus := ""
	handlerObservedReason := ""
	runner := newSystemNodeRunner("terminal-node", bus, db, func() []events.EventType {
		return []events.EventType{"example.started"}
	}, func(ctx context.Context, evt events.Event) error {
		startedCtx, cancelStarted := context.WithTimeout(ctx, time.Second)
		defer cancelStarted()
		if _, err := probe.WaitForHandlerStarted(startedCtx, eventID, "terminal-node"); err != nil {
			t.Fatalf("handler started lifecycle signal: %v", err)
		}
		if err := db.QueryRowContext(ctx, `
			SELECT COALESCE(status, ''), COALESCE(reason_code, '')
			FROM event_deliveries
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'node'
			  AND subscriber_id = 'terminal-node'
		`, eventID).Scan(&handlerObservedStatus, &handlerObservedReason); err != nil {
			t.Fatalf("load node delivery during handler: %v", err)
		}
		if _, err := db.ExecContext(ctx, `
			UPDATE entity_state
			SET current_state = 'done',
			    updated_at = now()
			WHERE run_id = $1::uuid
			  AND entity_id = $2::uuid
		`, runID, entityID); err != nil {
			t.Fatalf("mark entity terminal: %v", err)
		}
		return nil
	})
	runner.SetTestLifecycleProbe(probe)

	runner.ProcessEventForTest(ctx, eventtest.RunCreatingRootIngress(
		eventID,
		"example.started",
		"",
		"",
		[]byte(`{}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now().UTC(),
	))

	waitCtx, cancelWait := context.WithTimeout(ctx, time.Second)
	defer cancelWait()
	inProgress, err := probe.WaitForDeliveryStatus(waitCtx, eventID, "node", "terminal-node", "in_progress")
	if err != nil {
		t.Fatalf("node in_progress lifecycle signal: %v", err)
	}
	started, err := probe.WaitForHandlerStarted(waitCtx, eventID, "terminal-node")
	if err != nil {
		t.Fatalf("handler started lifecycle signal after process: %v", err)
	}
	completed, err := probe.WaitForHandlerCompleted(waitCtx, eventID, "terminal-node")
	if err != nil {
		t.Fatalf("handler completed lifecycle signal: %v", err)
	}
	if completed.Status != "completed" {
		t.Fatalf("handler completed status = %q, want completed", completed.Status)
	}
	delivered, err := probe.WaitForDeliveryStatus(waitCtx, eventID, "node", "terminal-node", "delivered")
	if err != nil {
		t.Fatalf("node delivered lifecycle signal: %v", err)
	}
	if started.At.Before(inProgress.At) || completed.At.Before(started.At) || delivered.At.Before(completed.At) {
		t.Fatalf("lifecycle signal order = in_progress:%s started:%s completed:%s delivered:%s, want in-progress before handler start before completion before delivered",
			inProgress.At.Format(time.RFC3339Nano),
			started.At.Format(time.RFC3339Nano),
			completed.At.Format(time.RFC3339Nano),
			delivered.At.Format(time.RFC3339Nano))
	}
	if handlerObservedStatus != "in_progress" || handlerObservedReason != "node_processing" {
		t.Fatalf("handler observed node delivery = %s/%s, want in_progress/node_processing", handlerObservedStatus, handlerObservedReason)
	}
	var status string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'terminal-node'
	`, eventID).Scan(&status); err != nil {
		t.Fatalf("load final node delivery: %v", err)
	}
	if status != "delivered" {
		t.Fatalf("final node delivery status = %q, want delivered", status)
	}
}

func TestSystemNodeRunner_RetryableFailureWritesFailedBeforeRetry(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext(t, context.Background())
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	seedSystemNodeCompletionRun(t, db, runID, eventID, entityID)
	bus := &systemNodeCompletionBus{}
	attempts := 0
	var (
		backoffStatus     string
		backoffReason     string
		backoffFailureRaw []byte
		backoffRetry      int
	)
	runner := newSystemNodeRunner("terminal-node", bus, db, func() []events.EventType {
		return []events.EventType{"example.started"}
	}, func(context.Context, events.Event) error {
		attempts++
		if attempts == 1 {
			return runtimefailures.New(runtimefailures.ClassConnectorFailure, "temporary_node_failure", "pipeline-test", "handle", nil)
		}
		return nil
	})
	runner.SetRetryPolicyForTest(2, func(int) time.Duration {
		if err := db.QueryRowContext(ctx, `
			SELECT COALESCE(status, ''), COALESCE(reason_code, ''), failure, COALESCE(retry_count, 0)
			FROM event_deliveries
			WHERE event_id = $1::uuid
			  AND subscriber_type = 'node'
			  AND subscriber_id = 'terminal-node'
		`, eventID).Scan(&backoffStatus, &backoffReason, &backoffFailureRaw, &backoffRetry); err != nil {
			t.Fatalf("load node delivery during retry backoff: %v", err)
		}
		return 0
	})

	runner.ProcessEventForTest(ctx, eventtest.RunCreatingRootIngress(
		eventID,
		"example.started",
		"",
		"",
		[]byte(`{}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now().UTC(),
	))

	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	backoffFailure := decodeTestPipelineFailure(t, backoffFailureRaw)
	if backoffStatus != "failed" || backoffReason != "handler_failure" || backoffRetry != 1 || backoffFailure == nil || backoffFailure.Detail.Code != "temporary_node_failure" {
		t.Fatalf("retry backoff delivery = %s/%s retry=%d failure=%#v, want failed/handler_failure retry=1 with failure", backoffStatus, backoffReason, backoffRetry, backoffFailure)
	}
	var finalStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'terminal-node'
	`, eventID).Scan(&finalStatus); err != nil {
		t.Fatalf("load final node delivery: %v", err)
	}
	if finalStatus != "delivered" {
		t.Fatalf("final node delivery status = %q, want delivered", finalStatus)
	}
}

func TestSystemNodeRunner_RetryableFailureExhaustsConfiguredRetryLimit(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext(t, context.Background())
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	seedSystemNodeCompletionRun(t, db, runID, eventID, entityID)
	bus := &systemNodeCompletionBus{}
	attempts := 0
	runner := newSystemNodeRunner("terminal-node", bus, db, func() []events.EventType {
		return []events.EventType{"example.started"}
	}, func(context.Context, events.Event) error {
		attempts++
		return runtimefailures.New(runtimefailures.ClassConnectorFailure, "temporary_node_failure", "pipeline-test", "handle", nil)
	})
	runner.SetRetryPolicyForTest(DefaultSystemNodeRetryLimit, func(int) time.Duration { return 0 })

	runner.ProcessEventForTest(ctx, eventtest.RunCreatingRootIngress(
		eventID,
		"example.started",
		"",
		"",
		[]byte(`{}`),
		0,
		runID,
		"",
		events.EnvelopeForEntityID(events.EventEnvelope{}, entityID),
		time.Now().UTC(),
	))

	if attempts != DefaultSystemNodeRetryLimit {
		t.Fatalf("attempts = %d, want configured retry limit %d", attempts, DefaultSystemNodeRetryLimit)
	}
	var (
		deliveryStatus     string
		deliveryReason     string
		deliveryRetry      int
		deliveryFailureRaw []byte
		receiptOutcome     string
	)
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(d.status, ''), COALESCE(d.reason_code, ''), COALESCE(d.retry_count, 0), d.failure, COALESCE(r.outcome, '')
		FROM event_deliveries d
		LEFT JOIN event_receipts r
		  ON r.event_id = d.event_id
		 AND r.subscriber_type = d.subscriber_type
		 AND r.subscriber_id = d.subscriber_id
		WHERE d.event_id = $1::uuid
		  AND d.subscriber_type = 'node'
		  AND d.subscriber_id = 'terminal-node'
	`, eventID).Scan(&deliveryStatus, &deliveryReason, &deliveryRetry, &deliveryFailureRaw, &receiptOutcome); err != nil {
		t.Fatalf("load exhausted node delivery: %v", err)
	}
	deliveryFailure := decodeTestPipelineFailure(t, deliveryFailureRaw)
	if deliveryStatus != "dead_letter" || deliveryReason != "handler_terminal_failure" || deliveryRetry != DefaultSystemNodeRetryLimit || deliveryFailure.Detail.Code != "delivery_retry_exhausted" || receiptOutcome != "dead_letter" {
		t.Fatalf("exhausted node delivery = %s/%s retry=%d failure=%#v receipt=%q, want terminal delivery with retry_exhausted failure", deliveryStatus, deliveryReason, deliveryRetry, deliveryFailure, receiptOutcome)
	}
}

func TestSystemNodeRunner_PipelineNamedNodeDoesNotMaskPlatformReceipt(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext(t, context.Background())
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	seedSystemNodeCompletionRun(t, db, runID, eventID, entityID, "pipeline")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO event_receipts (
			event_id, subscriber_type, subscriber_id, outcome, reason_code, side_effects, processed_at
		) VALUES (
			$1::uuid, 'platform', 'pipeline', 'success', 'processed', '{}'::jsonb, now()
		)
	`, eventID); err != nil {
		t.Fatalf("seed platform pipeline receipt: %v", err)
	}

	sideEffects := systemNodeProcessedReceiptSideEffects("pipeline", eventID)
	if err := persistSystemNodeProcessedReceiptAndSettleDelivery(ctx, db, "pipeline", eventID, sideEffects); err != nil {
		t.Fatalf("persistSystemNodeProcessedReceiptAndSettleDelivery: %v", err)
	}

	var platformReceipts int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'platform'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&platformReceipts); err != nil {
		t.Fatalf("count platform receipt: %v", err)
	}
	if platformReceipts != 1 {
		t.Fatalf("platform pipeline receipts = %d, want 1", platformReceipts)
	}
	var nodeReceipts int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&nodeReceipts); err != nil {
		t.Fatalf("count node receipt: %v", err)
	}
	if nodeReceipts != 1 {
		t.Fatalf("node pipeline receipts = %d, want 1", nodeReceipts)
	}
	var deliveryStatus string
	if err := db.QueryRowContext(ctx, `
		SELECT COALESCE(status, '')
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'pipeline'
	`, eventID).Scan(&deliveryStatus); err != nil {
		t.Fatalf("load pipeline node delivery: %v", err)
	}
	if deliveryStatus != "delivered" {
		t.Fatalf("pipeline node delivery status = %q, want delivered", deliveryStatus)
	}
}

func TestSystemNodeProcessedSettlementFailsWithoutNodeDeliveryAuthority(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext(t, context.Background())
	runID := uuid.NewString()
	eventID := uuid.NewString()
	entityID := uuid.NewString()
	seedSystemNodeCompletionEventWithoutDelivery(t, db, runID, eventID, entityID)

	sideEffects := systemNodeProcessedReceiptSideEffects("terminal-node", eventID)
	err := persistSystemNodeProcessedReceiptAndSettleDelivery(ctx, db, "terminal-node", eventID, sideEffects)
	if !errors.Is(err, ErrSystemNodeDeliveryAuthorityMissing) {
		t.Fatalf("persistSystemNodeProcessedReceiptAndSettleDelivery error = %v, want ErrSystemNodeDeliveryAuthorityMissing", err)
	}
	var deliveries int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'terminal-node'
	`, eventID).Scan(&deliveries); err != nil {
		t.Fatalf("count node deliveries: %v", err)
	}
	if deliveries != 0 {
		t.Fatalf("node deliveries = %d, want 0", deliveries)
	}
	var receipts int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM event_receipts
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = 'terminal-node'
	`, eventID).Scan(&receipts); err != nil {
		t.Fatalf("count node receipts: %v", err)
	}
	if receipts != 0 {
		t.Fatalf("node receipts = %d, want 0", receipts)
	}
}

func TestSystemNodeProcessedSettlementFailsWithTerminalNodeDeliveryAuthority(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     string
		retryCount int
	}{
		{name: "dead_letter", status: "dead_letter", retryCount: 2},
		{name: "retry_exhausted_failed", status: "failed", retryCount: DefaultSystemNodeRetryLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, db, _ := testutil.StartPostgres(t)
			ctx := testAuthorActivityContext(t, context.Background())
			runID := uuid.NewString()
			eventID := uuid.NewString()
			entityID := uuid.NewString()
			seedSystemNodeCompletionEventWithoutDelivery(t, db, runID, eventID, entityID)
			if _, err := db.ExecContext(ctx, `
				INSERT INTO event_deliveries (
					run_id, event_id, subscriber_type, subscriber_id, status, retry_count, reason_code, created_at
				) VALUES (
					$1::uuid, $2::uuid, 'node', 'terminal-node', $3, $4, 'terminal_test', now()
				)
			`, runID, eventID, tc.status, tc.retryCount); err != nil {
				t.Fatalf("seed terminal node delivery: %v", err)
			}

			sideEffects := systemNodeProcessedReceiptSideEffects("terminal-node", eventID)
			err := persistSystemNodeProcessedReceiptAndSettleDelivery(ctx, db, "terminal-node", eventID, sideEffects)
			if !errors.Is(err, ErrSystemNodeDeliveryAuthorityMissing) {
				t.Fatalf("persistSystemNodeProcessedReceiptAndSettleDelivery error = %v, want ErrSystemNodeDeliveryAuthorityMissing", err)
			}
			var status string
			var retryCount int
			if err := db.QueryRowContext(ctx, `
				SELECT COALESCE(status, ''), COALESCE(retry_count, 0)
				FROM event_deliveries
				WHERE event_id = $1::uuid
				  AND subscriber_type = 'node'
				  AND subscriber_id = 'terminal-node'
			`, eventID).Scan(&status, &retryCount); err != nil {
				t.Fatalf("load terminal delivery: %v", err)
			}
			if status != tc.status || retryCount != tc.retryCount {
				t.Fatalf("terminal delivery = %s/%d, want %s/%d", status, retryCount, tc.status, tc.retryCount)
			}
			var receipts int
			if err := db.QueryRowContext(ctx, `
				SELECT COUNT(*)
				FROM event_receipts
				WHERE event_id = $1::uuid
				  AND subscriber_type = 'node'
				  AND subscriber_id = 'terminal-node'
			`, eventID).Scan(&receipts); err != nil {
				t.Fatalf("count node receipts: %v", err)
			}
			if receipts != 0 {
				t.Fatalf("node receipts = %d, want 0", receipts)
			}
		})
	}
}

type systemNodeCompletionTargetDelivery struct {
	Status     string
	Reason     string
	RetryCount int
	Failure    *runtimefailures.Envelope
}

func seedSystemNodeCompletionRun(t *testing.T, db *sql.DB, runID, eventID, entityID string, nodeIDs ...string) {
	t.Helper()
	nodeID := "terminal-node"
	if len(nodeIDs) > 0 && nodeIDs[0] != "" {
		nodeID = nodeIDs[0]
	}
	seedSystemNodeCompletionEventWithoutDelivery(t, db, runID, eventID, entityID)
	if _, err := db.ExecContext(testAuthorActivityContext(t, context.Background()), `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, status, reason_code, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'node', $3, 'pending', 'matched_node_subscription', now()
		)
	`, runID, eventID, nodeID); err != nil {
		t.Fatalf("seed node delivery: %v", err)
	}
}

func seedSQLiteSystemNodeCompletionEventWithoutDelivery(t *testing.T, db *sql.DB, runID, eventID, entityID string) {
	t.Helper()
	ctx := testAuthorActivityContext(t, context.Background())
	now := time.Now().UTC()
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES (?, 'running', ?)
	`, runID, now); err != nil {
		t.Fatalf("seed sqlite run: %v", err)
	}
	event := eventtest.RunCreatingRootIngress(eventID, "worker/work.assign", "test", "", []byte(`{}`), 0, runID, "",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "example"), now)
	seedPipelineEventRecordForDialect(t, ctx, db, runtimeauthoractivity.DialectSQLite, event)
}

func seedSystemNodeCompletionTargetDelivery(t *testing.T, db *sql.DB, runID, eventID, nodeID string, target events.RouteIdentity) {
	t.Helper()
	if _, err := db.ExecContext(testAuthorActivityContext(t, context.Background()), `
		INSERT INTO event_deliveries (
			run_id, event_id, subscriber_type, subscriber_id, delivery_target_route, status, reason_code, created_at
		) VALUES (
			$1::uuid, $2::uuid, 'node', $3, $4::jsonb, 'pending', 'matched_node_subscription', now()
		)
	`, runID, eventID, nodeID, systemNodeRouteIdentityJSON(target)); err != nil {
		t.Fatalf("seed targeted node delivery: %v", err)
	}
}

func seedSQLiteSystemNodeCompletionTargetDelivery(t *testing.T, db *sql.DB, runID, eventID, nodeID string, target events.RouteIdentity) {
	t.Helper()
	if _, err := db.ExecContext(testAuthorActivityContext(t, context.Background()), `
		INSERT INTO event_deliveries (
			delivery_id, run_id, event_id, subscriber_type, subscriber_id, delivery_target_route, status, retry_count, reason_code, created_at
		) VALUES (
			?, ?, ?, 'node', ?, ?, 'pending', 0, 'matched_node_subscription', ?
		)
	`, uuid.NewString(), runID, eventID, nodeID, systemNodeRouteIdentityJSON(target), time.Now().UTC()); err != nil {
		t.Fatalf("seed sqlite targeted node delivery: %v", err)
	}
}

func loadSystemNodeCompletionTargetDeliveryStatus(t *testing.T, db *sql.DB, eventID, nodeID string, target events.RouteIdentity) string {
	t.Helper()
	return loadSystemNodeCompletionTargetDelivery(t, db, eventID, nodeID, target).Status
}

func loadSystemNodeCompletionTargetDelivery(t *testing.T, db *sql.DB, eventID, nodeID string, target events.RouteIdentity) systemNodeCompletionTargetDelivery {
	t.Helper()
	var delivery systemNodeCompletionTargetDelivery
	var failureRaw []byte
	if err := db.QueryRowContext(testAuthorActivityContext(t, context.Background()), `
		SELECT COALESCE(status, ''), COALESCE(reason_code, ''), COALESCE(retry_count, 0), failure
		FROM event_deliveries
		WHERE event_id = $1::uuid
		  AND subscriber_type = 'node'
		  AND subscriber_id = $2
		  AND delivery_target_route = $3::jsonb
	`, eventID, nodeID, systemNodeRouteIdentityJSON(target)).Scan(&delivery.Status, &delivery.Reason, &delivery.RetryCount, &failureRaw); err != nil {
		t.Fatalf("load targeted node delivery: %v", err)
	}
	delivery.Failure = decodeTestPipelineFailure(t, failureRaw)
	return delivery
}

func loadSQLiteSystemNodeCompletionTargetDeliveryStatus(t *testing.T, db *sql.DB, eventID, nodeID string, target events.RouteIdentity) string {
	t.Helper()
	return loadSQLiteSystemNodeCompletionTargetDelivery(t, db, eventID, nodeID, target).Status
}

func loadSQLiteSystemNodeCompletionTargetDelivery(t *testing.T, db *sql.DB, eventID, nodeID string, target events.RouteIdentity) systemNodeCompletionTargetDelivery {
	t.Helper()
	var delivery systemNodeCompletionTargetDelivery
	var failureRaw []byte
	if err := db.QueryRowContext(testAuthorActivityContext(t, context.Background()), `
		SELECT COALESCE(status, ''), COALESCE(reason_code, ''), COALESCE(retry_count, 0), failure
		FROM event_deliveries
		WHERE event_id = ?
		  AND subscriber_type = 'node'
		  AND subscriber_id = ?
		  AND COALESCE(delivery_target_route, '{}') = ?
	`, eventID, nodeID, systemNodeRouteIdentityJSON(target)).Scan(&delivery.Status, &delivery.Reason, &delivery.RetryCount, &failureRaw); err != nil {
		t.Fatalf("load sqlite targeted node delivery: %v", err)
	}
	delivery.Failure = decodeTestPipelineFailure(t, failureRaw)
	return delivery
}

func decodeTestPipelineFailure(t testing.TB, raw []byte) *runtimefailures.Envelope {
	t.Helper()
	if len(raw) == 0 {
		return nil
	}
	failure, err := runtimefailures.UnmarshalEnvelope(raw)
	if err != nil {
		t.Fatalf("decode pipeline failure: %v", err)
	}
	return &failure
}

func seedSystemNodeCompletionEventWithoutDelivery(t *testing.T, db *sql.DB, runID, eventID, entityID string) {
	t.Helper()
	ctx := testAuthorActivityContext(t, context.Background())
	if _, err := db.ExecContext(ctx, `
		INSERT INTO runs (run_id, status, started_at)
		VALUES ($1::uuid, 'running', now())
	`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	event := eventtest.RunCreatingRootIngress(eventID, "example.started", "test", "", []byte(`{}`), 0, runID, "",
		events.EnvelopeForFlowInstance(events.EnvelopeForEntityID(events.EventEnvelope{}, entityID), "example"), time.Now().UTC())
	seedPipelineEventRecord(t, ctx, db, event)
	if _, err := db.ExecContext(ctx, `
		UPDATE runs
		SET trigger_event_id = $2::uuid,
		    trigger_event_type = 'example.started'
		WHERE run_id = $1::uuid
	`, runID, eventID); err != nil {
		t.Fatalf("seed run trigger: %v", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO entity_state (
			run_id, entity_id, flow_instance, entity_type, slug, name, current_state,
			gates, fields, accumulator, revision, entered_state_at, created_at, updated_at
		) VALUES (
			$1::uuid, $2::uuid, 'example', 'default', 'example', 'Example', 'working',
			'{}'::jsonb, '{}'::jsonb, '{}'::jsonb, 1, now(), now(), now()
		)
	`, runID, entityID); err != nil {
		t.Fatalf("seed entity state: %v", err)
	}
}
