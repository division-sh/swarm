package store

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	runforkrevision "github.com/division-sh/swarm/internal/runtime/runforkrevision"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRunForkRevisionRegistryIsClosed(t *testing.T) {
	want := []runforkrevision.Family{
		runforkrevision.FamilyAgentConversationAudits,
		runforkrevision.FamilyAgentSessions,
		runforkrevision.FamilyAgentTurns,
		runforkrevision.FamilyDeadLetters,
		runforkrevision.FamilyEntityMetadata,
		runforkrevision.FamilyEntityMutations,
		runforkrevision.FamilyEventDeliveries,
		runforkrevision.FamilyEventReceipts,
		runforkrevision.FamilyEvents,
		runforkrevision.FamilyReplyContexts,
		runforkrevision.FamilyTimers,
	}
	got := runforkrevision.AllFamilies()
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("run-fork revision registry = %q, want exact 11-family registry %q", got, want)
	}
}

func TestRunForkRevisionCaptureReusesTransactionRevisionAndRollbackPublishesNothing(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO events (run_id, event_id, event_name, scope, produced_by_type)
		VALUES ($1::uuid, $2::uuid, 'revision.rollback', 'global', 'platform')
	`, runID, eventID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, field, new_value, caused_by_event, writer_type, writer_id
		) VALUES ($1::uuid, $2::uuid, 'current_state', '"ready"'::jsonb, $3::uuid, 'platform', 'revision-test')
	`, runID, uuid.NewString(), eventID); err != nil {
		t.Fatalf("seed mutation: %v", err)
	}
	revisions, err := runforkrevision.CaptureCurrentTransaction(ctx, tx)
	if err != nil {
		t.Fatalf("capture transaction: %v", err)
	}
	if revisions[runID] != 1 {
		t.Fatalf("captured revision = %d, want 1", revisions[runID])
	}
	reused, err := runforkrevision.Capture(ctx, tx, runID, runforkrevision.FamilyEvents)
	if err != nil {
		t.Fatalf("repeat capture: %v", err)
	}
	if reused != revisions[runID] {
		t.Fatalf("repeated capture revision = %d, want transaction revision %d", reused, revisions[runID])
	}
	var ledgerRows, factRows int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1::uuid`, runID).Scan(&ledgerRows); err != nil {
		t.Fatalf("count transaction ledger: %v", err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1::uuid`, runID).Scan(&factRows); err != nil {
		t.Fatalf("count transaction facts: %v", err)
	}
	if ledgerRows != 1 || factRows != 2 {
		t.Fatalf("transaction projection = ledger:%d facts:%d, want 1/2", ledgerRows, factRows)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback transaction: %v", err)
	}
	for table, query := range map[string]string{
		"events":                  `SELECT COUNT(*) FROM events WHERE run_id=$1::uuid`,
		"entity_mutations":        `SELECT COUNT(*) FROM entity_mutations WHERE run_id=$1::uuid`,
		"run_fork_revisions":      `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1::uuid`,
		"run_fork_fact_revisions": `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1::uuid`,
		"run_fork_revision_heads": `SELECT COUNT(*) FROM run_fork_revision_heads WHERE run_id=$1::uuid`,
	} {
		var count int
		if err := db.QueryRowContext(ctx, query, runID).Scan(&count); err != nil {
			t.Fatalf("count rolled-back %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("rolled-back %s rows = %d, want 0", table, count)
		}
	}
}

func TestRunForkRevisionCaptureSerializesSameRunCommitVisibility(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	runID := uuid.NewString()
	firstEventID := uuid.NewString()
	secondEventID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}

	first, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin first transaction: %v", err)
	}
	defer func() { _ = first.Rollback() }()
	if _, err := first.ExecContext(ctx, `INSERT INTO events (run_id,event_id,event_name,scope,produced_by_type) VALUES ($1::uuid,$2::uuid,'revision.first','global','platform')`, runID, firstEventID); err != nil {
		t.Fatalf("seed first event: %v", err)
	}
	firstRevision, err := runforkrevision.Capture(ctx, first, runID, runforkrevision.FamilyEvents)
	if err != nil {
		t.Fatalf("capture first transaction: %v", err)
	}

	second, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin second transaction: %v", err)
	}
	defer func() { _ = second.Rollback() }()
	if _, err := second.ExecContext(ctx, `INSERT INTO events (run_id,event_id,event_name,scope,produced_by_type) VALUES ($1::uuid,$2::uuid,'revision.second','global','platform')`, runID, secondEventID); err != nil {
		t.Fatalf("seed second event: %v", err)
	}
	type captureResult struct {
		revision int64
		err      error
	}
	done := make(chan captureResult, 1)
	go func() {
		revision, err := runforkrevision.Capture(ctx, second, runID, runforkrevision.FamilyEvents)
		done <- captureResult{revision: revision, err: err}
	}()
	select {
	case result := <-done:
		t.Fatalf("second capture completed before first commit: revision=%d err=%v", result.revision, result.err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := first.Commit(); err != nil {
		t.Fatalf("commit first transaction: %v", err)
	}
	result := <-done
	if result.err != nil {
		t.Fatalf("capture second transaction: %v", result.err)
	}
	if firstRevision != 1 || result.revision != 2 {
		t.Fatalf("serialized revisions = %d then %d, want 1 then 2", firstRevision, result.revision)
	}
	if err := second.Commit(); err != nil {
		t.Fatalf("commit second transaction: %v", err)
	}
	var firstCount, secondCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1::uuid AND family='events' AND revision=1 AND present`, runID).Scan(&firstCount); err != nil {
		t.Fatalf("count first revision facts: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1::uuid AND family='events' AND revision=2 AND present`, runID).Scan(&secondCount); err != nil {
		t.Fatalf("count second revision facts: %v", err)
	}
	if firstCount != 1 || secondCount != 2 {
		t.Fatalf("visible event facts = revision1:%d revision2:%d, want 1/2", firstCount, secondCount)
	}
}

func TestRunForkRevisionCaptureOrdersMultiRunLocksDeterministically(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runIDs := []string{uuid.NewString(), uuid.NewString()}
	sort.Strings(runIDs)
	for _, runID := range runIDs {
		if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
			t.Fatalf("seed run %s: %v", runID, err)
		}
	}
	type workerResult struct {
		revisions map[string]int64
		err       error
	}
	start := make(chan struct{})
	results := make(chan workerResult, 2)
	worker := func(order []string) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			results <- workerResult{err: err}
			return
		}
		defer func() { _ = tx.Rollback() }()
		for _, runID := range order {
			if _, err := tx.ExecContext(ctx, `INSERT INTO events (run_id,event_id,event_name,scope,produced_by_type) VALUES ($1::uuid,$2::uuid,'revision.multi','global','platform')`, runID, uuid.NewString()); err != nil {
				results <- workerResult{err: err}
				return
			}
		}
		<-start
		changes := []runforkrevision.Change{
			{RunID: order[0], Families: []runforkrevision.Family{runforkrevision.FamilyEvents}},
			{RunID: order[1], Families: []runforkrevision.Family{runforkrevision.FamilyEvents}},
		}
		revisions, err := runforkrevision.CaptureChanges(ctx, tx, changes...)
		if err == nil {
			err = tx.Commit()
		}
		results <- workerResult{revisions: revisions, err: err}
	}
	go worker([]string{runIDs[0], runIDs[1]})
	go worker([]string{runIDs[1], runIDs[0]})
	close(start)
	first := <-results
	second := <-results
	if first.err != nil || second.err != nil {
		t.Fatalf("opposite-order captures failed: first=%v second=%v", first.err, second.err)
	}
	for _, revisions := range []map[string]int64{first.revisions, second.revisions} {
		if revisions[runIDs[0]] != revisions[runIDs[1]] {
			t.Fatalf("one transaction received inconsistent multi-run revisions: %#v", revisions)
		}
	}
	if first.revisions[runIDs[0]]+second.revisions[runIDs[0]] != 3 {
		t.Fatalf("multi-run revision results = %#v and %#v, want one revision 1 and one revision 2", first.revisions, second.revisions)
	}
}

func TestPostgresLifecycleSessionMutationPublishesRunForkRevision(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	store := &PostgresStore{DB: db}
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	agentID := "revision-lifecycle-agent"
	agent := runtimemanager.PersistedAgent{
		Config: runtimeactors.AgentConfig{
			ID: agentID, Role: "worker", Type: "sonnet", Model: "regular", Mode: "global",
			Config: []byte(`{"system_prompt":"revision proof"}`),
		},
		Status: "active", HiredBy: "revision-proof", StartedAt: now,
	}
	spawned, err := store.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "spawn", RequestHash: "revision-spawn",
		AgentID: agentID, Trigger: "spawn", TargetEpoch: 1, TargetGeneration: 1,
		TargetPhase: runtimemanager.AgentLifecycleRegistered, ConfigRevision: "revision-1",
		RunMode: runtimemanager.AgentRunModeStopped, Agent: &agent, Now: now,
	})
	if err != nil {
		t.Fatalf("spawn lifecycle agent: %v", err)
	}
	started, err := store.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "start", RequestHash: "revision-start",
		AgentID: agentID, Trigger: "start", ExpectedEpoch: spawned.RuntimeEpoch,
		ExpectedGeneration: spawned.Generation, ExpectedPhase: spawned.Phase,
		TargetEpoch: spawned.RuntimeEpoch, TargetGeneration: spawned.Generation + 1,
		TargetPhase: runtimemanager.AgentLifecycleRunning, ConfigRevision: "revision-1",
		RunMode: runtimemanager.AgentRunModeStandard, Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("start lifecycle agent: %v", err)
	}

	runID := uuid.NewString()
	eventID := uuid.NewString()
	sessionID := uuid.NewString()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin lifecycle source revision: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed lifecycle source run: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO events (run_id,event_id,event_name,scope,produced_by_type,created_at) VALUES ($1::uuid,$2::uuid,'lifecycle.revision','global','platform',$3)`, runID, eventID, now); err != nil {
		t.Fatalf("seed lifecycle source event: %v", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, scope_key, scope, conversation, turn_count,
			runtime_mode, runtime_state, status, created_at, updated_at
		) VALUES ($1::uuid,$2::uuid,$3,'global','global','[]'::jsonb,0,'session','{}'::jsonb,'active',$4,$4)
	`, sessionID, runID, agentID, now); err != nil {
		t.Fatalf("seed lifecycle source session: %v", err)
	}
	if _, err := runforkrevision.CaptureCurrentTransaction(ctx, tx); err != nil {
		t.Fatalf("capture lifecycle source revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit lifecycle source revision: %v", err)
	}

	if _, err := store.CommitAgentLifecycleTransition(ctx, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "teardown", RequestHash: "revision-terminate",
		AgentID: agentID, Trigger: "terminate", ExpectedEpoch: started.RuntimeEpoch,
		ExpectedGeneration: started.Generation, ExpectedPhase: started.Phase,
		TargetEpoch: started.RuntimeEpoch, TargetGeneration: started.Generation + 1,
		TargetPhase: runtimemanager.AgentLifecycleTerminated, ConfigRevision: "revision-1",
		RunMode: runtimemanager.AgentRunModeStopped,
		Subordinate: runtimesessions.LifecycleMutationPlan{
			Action:            runtimesessions.LifecycleMutationTerminateCurrentSet,
			TerminationReason: runtimesessions.TerminationReasonNormal,
		},
		Now: now.Add(2 * time.Second),
	}); err != nil {
		t.Fatalf("terminate lifecycle source session: %v", err)
	}

	var head int64
	if err := db.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1::uuid`, runID).Scan(&head); err != nil {
		t.Fatalf("load lifecycle source revision head: %v", err)
	}
	if head != 2 {
		t.Fatalf("lifecycle source revision head = %d, want 2", head)
	}
	for revision, want := range map[int64]string{1: "active", 2: "terminated"} {
		var status string
		if err := db.QueryRowContext(ctx, `
			SELECT fact->>'status'
			FROM run_fork_fact_revisions
			WHERE run_id=$1::uuid AND family='agent_sessions' AND fact_key=$2 AND revision=$3 AND present
		`, runID, sessionID, revision).Scan(&status); err != nil {
			t.Fatalf("load lifecycle session revision %d: %v", revision, err)
		}
		if status != want {
			t.Fatalf("lifecycle session revision %d status = %q, want %q", revision, status, want)
		}
	}
}

func TestScheduleNoOpTerminalMutationsDoNotPublishRunForkRevision(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(context.Background(), runID)
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	store := &PostgresStore{DB: db}

	tests := []struct {
		name   string
		mutate func(context.Context, *PostgresStore, runtimepipeline.Schedule) error
	}{
		{
			name: "mark fired",
			mutate: func(ctx context.Context, store *PostgresStore, schedule runtimepipeline.Schedule) error {
				return store.MarkScheduleFired(ctx, schedule)
			},
		},
		{
			name: "mark exact fired",
			mutate: func(ctx context.Context, store *PostgresStore, schedule runtimepipeline.Schedule) error {
				return store.MarkScheduleFiredExact(ctx, schedule)
			},
		},
		{
			name: "cancel",
			mutate: func(ctx context.Context, store *PostgresStore, schedule runtimepipeline.Schedule) error {
				return store.CancelSchedule(ctx, schedule.AgentID, schedule.EventType)
			},
		},
		{
			name: "cancel exact",
			mutate: func(ctx context.Context, store *PostgresStore, schedule runtimepipeline.Schedule) error {
				return store.CancelScheduleExact(ctx, schedule)
			},
		},
	}

	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schedule := runtimepipeline.Schedule{
				RunID:        runID,
				AgentID:      fmt.Sprintf("revision-agent-%d", index),
				EventType:    fmt.Sprintf("revision.timer.%d", index),
				Mode:         "once",
				At:           time.Now().Add(time.Hour),
				EntityID:     uuid.NewString(),
				FlowInstance: fmt.Sprintf("revision-flow-%d", index),
				TaskID:       fmt.Sprintf("revision-task-%d", index),
				Payload:      []byte(`{}`),
			}
			if err := store.UpsertSchedule(ctx, schedule); err != nil {
				t.Fatalf("upsert schedule: %v", err)
			}
			if err := test.mutate(ctx, store, schedule); err != nil {
				t.Fatalf("first mutation: %v", err)
			}
			var revision int64
			if err := db.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1::uuid`, runID).Scan(&revision); err != nil {
				t.Fatalf("load revision after terminal mutation: %v", err)
			}
			if err := test.mutate(ctx, store, schedule); err != nil {
				t.Fatalf("duplicate mutation: %v", err)
			}
			var afterDuplicate int64
			if err := db.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1::uuid`, runID).Scan(&afterDuplicate); err != nil {
				t.Fatalf("load revision after duplicate mutation: %v", err)
			}
			if afterDuplicate != revision {
				t.Fatalf("revision after duplicate mutation = %d, want unchanged %d", afterDuplicate, revision)
			}
		})
	}
}

func TestRunForkRevisionDeletionPublishesTombstoneAndUnrevisionedDriftFailsClosed(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := context.Background()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	timerID := uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO runs (run_id, status) VALUES ($1::uuid, 'running')`, runID); err != nil {
		t.Fatalf("seed run: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO events (run_id,event_id,event_name,scope,produced_by_type) VALUES ($1::uuid,$2::uuid,'revision.delete','global','platform')`, runID, eventID); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO timers (timer_id,run_id,timer_name,fire_event,fire_at,owner_agent,task_type,status) VALUES ($1::uuid,$2::uuid,'revision-delete','timer.fire',NOW(),'agent-a','timer','active')`, timerID, runID); err != nil {
		t.Fatalf("seed timer: %v", err)
	}
	firstRevision := captureRunForkTestRevision(t, db, runID)
	if _, err := db.ExecContext(ctx, `DELETE FROM timers WHERE timer_id=$1::uuid`, timerID); err != nil {
		t.Fatalf("delete timer: %v", err)
	}
	secondRevision := captureRunForkTestRevision(t, db, runID, runforkrevision.FamilyTimers)
	if secondRevision <= firstRevision {
		t.Fatalf("deletion revision = %d, want after %d", secondRevision, firstRevision)
	}
	var present bool
	if err := db.QueryRowContext(ctx, `SELECT present FROM run_fork_fact_revisions WHERE run_id=$1::uuid AND family='timers' AND fact_key=$2 AND revision=$3`, runID, timerID, secondRevision).Scan(&present); err != nil {
		t.Fatalf("load timer tombstone: %v", err)
	}
	if present {
		t.Fatal("deleted timer revision remained present")
	}
	if _, err := db.ExecContext(ctx, `UPDATE events SET event_name='revision.drifted' WHERE event_id=$1::uuid`, eventID); err != nil {
		t.Fatalf("write unrevisioned drift: %v", err)
	}
	if _, err := (&PostgresStore{DB: db}).PlanRunFork(ctx, RunForkPlanRequest{SourceRunID: runID, At: eventID}); err == nil || !strings.Contains(err.Error(), "unsupported unrevisioned events facts") {
		t.Fatalf("PlanRunFork drift error = %v, want fail-closed unrevisioned events", err)
	}
}
