package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/division-sh/swarm/internal/testutil"
)

type runForkRevisionMatrixFixture struct {
	runID        string
	eventID      string
	entityID     string
	mutationID   string
	deliveryID   string
	receiptID    string
	deadLetterID string
	timerID      string
	sessionID    string
	turnID       string
	auditID      string
	replyID      string
	surfaceID    string
	operationID  string
	attemptID    string
	authorityID  string
	at           time.Time
}

type runForkRevisionMatrixFact struct {
	Family  runforkrevision.Family
	Key     string
	Fact    any
	Present bool
}

func TestRunForkRevisionTwelveFamilySelectedStoreParity(t *testing.T) {
	fixture := runForkRevisionMatrixFixture{
		runID:        "00000000-0000-0000-0000-000000002272",
		eventID:      "00000000-0000-0000-0000-000000002273",
		entityID:     "00000000-0000-0000-0000-000000002274",
		mutationID:   "00000000-0000-0000-0000-000000002275",
		deliveryID:   "00000000-0000-0000-0000-000000002276",
		receiptID:    "00000000-0000-0000-0000-000000002277",
		deadLetterID: "00000000-0000-0000-0000-000000002278",
		timerID:      "00000000-0000-0000-0000-000000002279",
		sessionID:    "00000000-0000-0000-0000-000000002280",
		turnID:       "00000000-0000-0000-0000-000000002281",
		auditID:      "00000000-0000-0000-0000-000000002282",
		replyID:      "revision-matrix-reply",
		surfaceID:    "00000000-0000-0000-0000-000000002283",
		operationID:  "00000000-0000-0000-0000-000000002284",
		attemptID:    "00000000-0000-0000-0000-000000002285",
		authorityID:  "00000000-0000-0000-0000-000000002286",
		at:           time.Date(2026, 8, 25, 19, 0, 0, 123000000, time.UTC),
	}

	results := make(map[string][]runForkRevisionMatrixFact, 2)
	t.Run("sqlite", func(t *testing.T) {
		store := newBootstrappedSQLiteRuntimeStoreForTest(t)
		db := store.backend.ConstructionHandle()
		results["sqlite"] = proveRunForkRevisionTwelveFamilyMatrix(t, db, false, fixture)
	})
	t.Run("postgres", func(t *testing.T) {
		_, db, _ := testutil.StartPostgres(t)
		results["postgres"] = proveRunForkRevisionTwelveFamilyMatrix(t, db, true, fixture)
	})
	if !reflect.DeepEqual(results["sqlite"], results["postgres"]) {
		t.Fatalf("selected-store canonical revision facts differ:\nsqlite=%#v\npostgres=%#v", results["sqlite"], results["postgres"])
	}
}

func proveRunForkRevisionTwelveFamilyMatrix(t *testing.T, db *sql.DB, postgres bool, fixture runForkRevisionMatrixFixture) []runForkRevisionMatrixFact {
	t.Helper()
	ctx := testAuthorActivityContext()
	var selected any = NewSQLiteRuntimeStoreForTest(db)
	if postgres {
		selected = newPostgresStoreWithBackend(mustPostgresBackend(db))
	}
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{
		Origin: semanticScenarioSetupRunOriginForTest(), RunID: fixture.runID, StartedAt: fixture.at,
	})
	identity := testAgentIdentity(t, "revision-matrix-agent", "")
	seedTestAgentRow(t, ctx, db, postgres, identity, "active")

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin twelve-family transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	seedRunForkRevisionMatrixFacts(t, ctx, tx, fixture, true)
	effects, err := runforkrevision.ForRun(fixture.runID, runforkrevision.AllFamilies()...)
	if err != nil {
		t.Fatalf("declare twelve-family effects: %v", err)
	}
	result, err := finalizeRunForkRevisionMatrix(ctx, tx, postgres, effects)
	if err != nil {
		t.Fatalf("finalize twelve-family revision: %v", err)
	}
	if got := result[fixture.runID]; !got.Changed || got.Revision != 1 {
		t.Fatalf("initial twelve-family result = %#v, want changed revision 1", got)
	}
	if err := validateRunForkRevisionMatrix(ctx, tx, postgres, fixture.runID); err != nil {
		t.Fatalf("validate initial twelve-family revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit twelve-family revision: %v", err)
	}

	noChangeTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin no-change transaction: %v", err)
	}
	defer func() { _ = noChangeTx.Rollback() }()
	noChange, err := finalizeRunForkRevisionMatrix(ctx, noChangeTx, postgres, effects)
	if err != nil {
		t.Fatalf("finalize exact no-change: %v", err)
	}
	if got := noChange[fixture.runID]; got.Changed || got.Revision != 1 {
		t.Fatalf("exact no-change result = %#v, want unchanged revision 1", got)
	}
	if err := noChangeTx.Commit(); err != nil {
		t.Fatalf("commit exact no-change: %v", err)
	}
	initial := loadRunForkRevisionMatrixFacts(t, ctx, db, fixture.runID, 1)
	assertRunForkRevisionMatrixShape(t, initial, true)

	proveRunForkRevisionCorruptionFailsClosed(t, ctx, db, postgres, fixture)

	deleteTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin twelve-family deletion: %v", err)
	}
	defer func() { _ = deleteTx.Rollback() }()
	deleteRunForkRevisionMatrixFacts(t, ctx, deleteTx, fixture)
	deleted, err := finalizeRunForkRevisionMatrix(ctx, deleteTx, postgres, effects)
	if err != nil {
		t.Fatalf("finalize twelve-family deletion: %v", err)
	}
	if got := deleted[fixture.runID]; !got.Changed || got.Revision != 2 {
		t.Fatalf("twelve-family deletion result = %#v, want changed revision 2", got)
	}
	if err := validateRunForkRevisionMatrix(ctx, deleteTx, postgres, fixture.runID); err != nil {
		t.Fatalf("validate twelve-family deletion: %v", err)
	}
	if err := deleteTx.Commit(); err != nil {
		t.Fatalf("commit twelve-family deletion: %v", err)
	}
	assertRunForkRevisionMatrixShape(t, loadRunForkRevisionMatrixFacts(t, ctx, db, fixture.runID, 2), false)

	rollbackRunID := "00000000-0000-0000-0000-000000002287"
	requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{
		Origin: semanticScenarioSetupRunOriginForTest(), RunID: rollbackRunID, StartedAt: fixture.at,
	})
	rollbackTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin rollback proof: %v", err)
	}
	rollbackEventID := "00000000-0000-0000-0000-000000002288"
	seedRunForkRevisionMatrixEvent(t, ctx, rollbackTx, rollbackRunID, rollbackEventID, fixture.at)
	rollbackEffects, err := runforkrevision.ForRun(rollbackRunID, runforkrevision.FamilyEvents)
	if err != nil {
		t.Fatalf("declare rollback effects: %v", err)
	}
	if _, err := finalizeRunForkRevisionMatrix(ctx, rollbackTx, postgres, rollbackEffects); err != nil {
		t.Fatalf("finalize rollback proof: %v", err)
	}
	if err := rollbackTx.Rollback(); err != nil {
		t.Fatalf("rollback domain and revision facts: %v", err)
	}
	for _, table := range []string{"events", "run_fork_revision_heads", "run_fork_revisions", "run_fork_fact_revisions"} {
		var count int
		if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE run_id=$1", table), rollbackRunID).Scan(&count); err != nil {
			t.Fatalf("count rolled-back %s: %v", table, err)
		}
		if count != 0 {
			t.Fatalf("rolled-back %s rows = %d, want 0", table, count)
		}
	}
	proveRunForkRevisionMultiRunFinalization(t, ctx, db, postgres, selected, fixture.at)
	return initial
}

func proveRunForkRevisionCorruptionFailsClosed(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, fixture runForkRevisionMatrixFixture) {
	t.Helper()
	for _, proof := range []struct {
		name   string
		mutate func(*sql.Tx) error
	}{
		{
			name: "unrevisioned_current_projection",
			mutate: func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `UPDATE entity_state SET name='Unrevisioned Matrix Entity' WHERE run_id=$1`, fixture.runID)
				return err
			},
		},
		{
			name: "corrupt_latest_revision_fact",
			mutate: func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `UPDATE run_fork_fact_revisions SET fact=$1 WHERE run_id=$2 AND revision=1 AND family='entity_metadata'`, `{"corrupt":true}`, fixture.runID)
				return err
			},
		},
	} {
		t.Run(proof.name, func(t *testing.T) {
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatalf("begin corruption transaction: %v", err)
			}
			defer func() { _ = tx.Rollback() }()
			if err := proof.mutate(tx); err != nil {
				t.Fatalf("apply corruption: %v", err)
			}
			err = validateRunForkRevisionMatrix(ctx, tx, postgres, fixture.runID)
			if err == nil || !strings.Contains(err.Error(), "unsupported unrevisioned entity_metadata facts") {
				t.Fatalf("corruption validation error = %v, want fail-closed entity_metadata mismatch", err)
			}
		})
	}
}

func proveRunForkRevisionMultiRunFinalization(t *testing.T, ctx context.Context, db *sql.DB, postgres bool, selected any, at time.Time) {
	t.Helper()
	runIDs := []string{
		"00000000-0000-0000-0000-000000002290",
		"00000000-0000-0000-0000-000000002289",
	}
	for _, runID := range runIDs {
		requireRunFixtureForTest(t, ctx, selected, semanticRunFixture{
			Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: at,
		})
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin multi-run revision transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	effects := runforkrevision.NewEffects()
	for index, runID := range runIDs {
		seedRunForkRevisionMatrixEvent(t, ctx, tx, runID, fmt.Sprintf("00000000-0000-0000-0000-00000000229%d", index+1), at.Add(time.Duration(index)*time.Second))
		if err := effects.Add(runID, runforkrevision.FamilyEvents); err != nil {
			t.Fatalf("declare multi-run revision effect: %v", err)
		}
	}
	results, err := finalizeRunForkRevisionMatrix(ctx, tx, postgres, effects)
	if err != nil {
		t.Fatalf("finalize multi-run revision: %v", err)
	}
	for _, runID := range runIDs {
		if got := results[runID]; !got.Changed || got.Revision != 1 {
			t.Fatalf("multi-run result for %s = %#v, want changed revision 1", runID, got)
		}
		if err := validateRunForkRevisionMatrix(ctx, tx, postgres, runID); err != nil {
			t.Fatalf("validate multi-run revision for %s: %v", runID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit multi-run revision: %v", err)
	}
}

func finalizeRunForkRevisionMatrix(ctx context.Context, tx *sql.Tx, postgres bool, effects *runforkrevision.Effects) (map[string]runforkrevision.Result, error) {
	if postgres {
		return runforkrevision.FinalizePostgres(ctx, tx, effects)
	}
	return runforkrevision.FinalizeSQLite(ctx, tx, effects)
}

func validateRunForkRevisionMatrix(ctx context.Context, tx *sql.Tx, postgres bool, runID string) error {
	if postgres {
		return runforkrevision.ValidateCompletePostgres(ctx, tx, runID)
	}
	return runforkrevision.ValidateCompleteSQLite(ctx, tx, runID)
}

func seedRunForkRevisionMatrixFacts(t *testing.T, ctx context.Context, tx *sql.Tx, f runForkRevisionMatrixFixture, includeEventDelivery bool) {
	t.Helper()
	if includeEventDelivery {
		target := events.MustEntitylessReceiverTarget(events.RouteIdentity{FlowID: "matrix-flow", FlowInstance: "matrix-flow/one"})
		route := events.DeliveryRoute{Recipient: events.MustNodeDeliveryRecipient(mustPersistenceRootNode("matrix-node")), Target: target}
		routeIdentity, err := route.Identity()
		if err != nil {
			t.Fatalf("construct revision matrix delivery route: %v", err)
		}
		targetJSON, err := json.Marshal(target)
		if err != nil {
			t.Fatalf("encode revision matrix delivery target: %v", err)
		}
		seedRunForkRevisionMatrixEvent(t, ctx, tx, f.runID, f.eventID, f.at)
		mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO event_deliveries (delivery_id,run_id,event_id,route_identity,subscriber_type,subscriber_id,agent_name_owner,agent_name_source,agent_route_presence,agent_flow_scope_key,agent_flow_instance_id,agent_flow_instance_path,delivery_target_route,delivery_context,delivery_payload_projection,connect_execution_claim,execution_authority_kind,authority_bundle_hash,authority_bundle_source,execution_authority_id,execution_authority_generation,status,retry_count,max_retries,next_eligible_at,claim_version,created_at,updated_at) VALUES ($1,$2,$3,$4,'node',$5,'','','','','','',$6,$7,$7,$7,'normal_runtime',$8,'persisted','revision-matrix',1,'pending',0,3,$9,0,$9,$9)`, f.deliveryID, f.runID, f.eventID, events.EncodeDeliveryRouteIdentity(routeIdentity), route.Recipient.ID(), string(targetJSON), `{}`, "bundle-v1:sha256:"+strings.Repeat("1", 64), f.at)
	}
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO entity_state (run_id,entity_id,flow_instance,entity_type,slug,name,current_state,created_at,updated_at) VALUES ($1,$2,'matrix-flow','matrix-type','matrix-slug','Matrix Entity','ready',$3,$3)`, f.runID, f.entityID, f.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO entity_mutations (mutation_id,run_id,entity_id,domain,path,new_value,caused_by_event,writer_type,writer_id,created_at) VALUES ($1,$2,$3,'authored_field','name',$4,$5,'platform','revision-matrix',$6)`, f.mutationID, f.runID, f.entityID, `"Matrix Entity"`, f.eventID, f.at)
	if includeEventDelivery {
		mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO committed_replay_scopes (event_id,run_id,scope,created_at,updated_at) VALUES ($1,$2,'direct',$3,$3)`, f.eventID, f.runID, f.at)
	}
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO event_receipts (receipt_id,event_id,subscriber_type,subscriber_id,outcome,side_effects,processed_at) VALUES ($1,$2,'platform','pipeline','success',$3,$4)`, f.receiptID, f.eventID, `{}`, f.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO dead_letters (dead_letter_id,original_event_id,original_event,original_payload,flow_instance,failure,created_at) VALUES ($1,$2,'matrix.event',$3,'matrix-flow',$4,$5)`, f.deadLetterID, f.eventID, `{}`, `{"class":"matrix"}`, f.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO timers (timer_id,timer_name,schedule_scope,schedule_key,immutable_hash,run_id,fire_event,fire_payload,routing_source,execution_mode,fire_at,initial_fire_at,recurring,owner_node,owner_kind,due_basis_kind,due_basis_absolute,task_type,status,created_at) VALUES ($1,'matrix-timer','run','matrix-key','matrix-hash',$2,'matrix.fire',$3,$4,'live',$5,$5,FALSE,'matrix-node','system','absolute',$5,'timer','active',$6)`, f.timerID, f.runID, `{}`, `{"kind":"root"}`, f.at.Add(time.Hour), f.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO agent_sessions (session_id,run_id,agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,flow_instance,memory_enabled,memory_source,conversation,turn_count,runtime_state,status,created_at,updated_at) VALUES ($1,$2,'revision-matrix-agent','store-test-fixture','runtime_created','root','','','',TRUE,'authored',$3,0,$4,'active',$5,$5)`, f.sessionID, f.runID, `[]`, `{}`, f.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO managed_agent_capability_surfaces (surface_id,integrity_hash,authority_kind,authority_id,execution_kind,execution_authority_id,run_id,actor_id,provider,transport,surface,created_at) VALUES ($1,'revision-matrix-surface','startup_probe',$2,'normal_agent','revision-matrix',$3,'revision-matrix-agent','mock','in_process',$4,$5)`, f.surfaceID, f.authorityID, f.runID, `{}`, f.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO runtime_external_effect_operations (operation_id,effect_kind,effect_class,execution_mode,bundle_hash,authority_kind,authority_id,generation,startup_authority_id,capability_plan_fingerprint,authority_evidence,lineage,request_fingerprint,state,created_at,updated_at,completed_at) VALUES ($1,'provider_turn','read_only','mock','matrix-bundle','startup_probe','revision-matrix',1,$2,'matrix-plan',$3,$3,'matrix-request','settled',$4,$4,$4)`, f.operationID, f.authorityID, `{}`, f.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO runtime_external_effect_attempts (attempt_id,operation_id,attempt_ordinal,adapter,transport,execution_mode,generation,execution_owner,lease_expires_at,fence_generation,usage_target_kind,usage_target_id,capability_surface_id,state,evidence,authorized_at,launched_at,response_observed_at,completed_at,updated_at) VALUES ($1,$2,1,'mock','in_process','mock',1,'revision-matrix',$3,1,'agent_turn',$4,$5,'settled',$6,$7,$7,$7,$7,$7)`, f.attemptID, f.operationID, f.at.Add(time.Hour), f.turnID, f.surfaceID, `{}`, f.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO agent_turns (turn_id,run_id,agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,session_id,flow_instance,memory_enabled,memory_source,capability_surface_id,tool_calls,emitted_events,turn_blocks,parse_ok,latency_ms,retry_count,agent_frame_bytes,completion_attempt_id,execution_mode,created_at) VALUES ($1,$2,'revision-matrix-agent','store-test-fixture','runtime_created','root','','',$3,'',TRUE,'authored',$4,$5,$5,$5,TRUE,1,0,$6,$7,'mock',$8)`, f.turnID, f.runID, f.sessionID, f.surfaceID, `[]`, []byte("matrix-frame"), f.attemptID, f.at)
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO agent_conversation_audits (session_id,run_id,agent_id,agent_name_owner,agent_name_source,agent_route_presence,flow_scope_key,flow_instance_id,flow_instance,memory_enabled,memory_source,conversation,turn_count,runtime_state,status,created_at,updated_at) VALUES ($1,$2,'revision-matrix-audit','store-test-fixture','runtime_created','root','','','',FALSE,'platform_default',$3,0,$4,'active',$5,$5)`, f.auditID, f.runID, `[]`, `{}`, f.at)
	if includeEventDelivery {
		mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO reply_contexts (reply_context_id,run_id,request_event_id,requester_flow_id,request_output_pin,reply_input_pin,provider_flow_id,provider_input_pin,provider_output_pin,origin_route,request_correlation_id,state,created_at,updated_at) VALUES ($1,$2,$3,'requester','out','reply','provider','in','out',$4,'matrix-correlation','open',$5,$5)`, f.replyID, f.runID, f.eventID, `{}`, f.at)
	}
}

func seedRunForkRevisionMatrixEvent(t *testing.T, ctx context.Context, tx *sql.Tx, runID, eventID string, at time.Time) {
	t.Helper()
	mustExecRunForkRevisionMatrix(t, ctx, tx, `INSERT INTO events (event_class,event_id,run_id,event_name,scope,payload,payload_bytes,execution_mode,chain_depth,produced_by,produced_by_type,created_at,routing_source_kind,source_route,target_route,target_set,route_settlement) VALUES ('selected_fork_replay',$1,$2,'matrix.event','global',$3,$4,'live',0,'revision-matrix','platform',$5,'absent',$6,$6,$7,$8)`, eventID, runID, `{"matrix":true}`, []byte(`{"matrix":true}`), at, `{}`, `[]`, `{"write_class":"historical_run_fork_replay","arm":"delivery"}`)
}

func deleteRunForkRevisionMatrixFacts(t *testing.T, ctx context.Context, tx *sql.Tx, f runForkRevisionMatrixFixture) {
	t.Helper()
	for _, statement := range []struct {
		query string
		arg   string
	}{
		{`DELETE FROM reply_contexts WHERE run_id=$1`, f.runID},
		{`DELETE FROM dead_letters WHERE original_event_id=$1`, f.eventID},
		{`DELETE FROM event_receipts WHERE event_id=$1`, f.eventID},
		{`DELETE FROM committed_replay_scopes WHERE run_id=$1`, f.runID},
		{`DELETE FROM event_deliveries WHERE run_id=$1`, f.runID},
		{`DELETE FROM timers WHERE run_id=$1`, f.runID},
		{`DELETE FROM agent_turns WHERE run_id=$1`, f.runID},
		{`DELETE FROM agent_conversation_audits WHERE run_id=$1`, f.runID},
		{`DELETE FROM agent_sessions WHERE run_id=$1`, f.runID},
		{`DELETE FROM entity_mutations WHERE run_id=$1`, f.runID},
		{`DELETE FROM entity_state WHERE run_id=$1`, f.runID},
		{`DELETE FROM events WHERE run_id=$1`, f.runID},
	} {
		mustExecRunForkRevisionMatrix(t, ctx, tx, statement.query, statement.arg)
	}
}

func mustExecRunForkRevisionMatrix(t *testing.T, ctx context.Context, tx *sql.Tx, query string, args ...any) {
	t.Helper()
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		t.Fatalf("execute revision matrix statement %q: %v", strings.Join(strings.Fields(query), " "), err)
	}
}

func loadRunForkRevisionMatrixFacts(t *testing.T, ctx context.Context, db *sql.DB, runID string, revision int64) []runForkRevisionMatrixFact {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT family,fact_key,fact,present FROM run_fork_fact_revisions WHERE run_id=$1 AND revision=$2 ORDER BY family,fact_key`, runID, revision)
	if err != nil {
		t.Fatalf("load revision matrix facts: %v", err)
	}
	defer rows.Close()
	var facts []runForkRevisionMatrixFact
	for rows.Next() {
		var family runforkrevision.Family
		var key string
		var encoded []byte
		var present bool
		if err := rows.Scan(&family, &key, &encoded, &present); err != nil {
			t.Fatalf("scan revision matrix fact: %v", err)
		}
		var fact any
		if err := json.Unmarshal(encoded, &fact); err != nil {
			t.Fatalf("decode %s/%s revision matrix fact: %v", family, key, err)
		}
		facts = append(facts, runForkRevisionMatrixFact{Family: family, Key: key, Fact: fact, Present: present})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read revision matrix facts: %v", err)
	}
	return facts
}

func assertRunForkRevisionMatrixShape(t *testing.T, facts []runForkRevisionMatrixFact, present bool) {
	t.Helper()
	if len(facts) != len(runforkrevision.AllFamilies()) {
		t.Fatalf("revision matrix fact count = %d, want %d: %#v", len(facts), len(runforkrevision.AllFamilies()), facts)
	}
	got := make([]runforkrevision.Family, 0, len(facts))
	for _, fact := range facts {
		if fact.Present != present {
			t.Fatalf("%s/%s present = %v, want %v", fact.Family, fact.Key, fact.Present, present)
		}
		got = append(got, fact.Family)
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	want := runforkrevision.AllFamilies()
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("revision matrix families = %q, want exact closed registry %q", got, want)
	}
}
