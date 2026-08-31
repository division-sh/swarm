package runtimepersistence

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/events"
	"github.com/division-sh/swarm/internal/events/eventtest"
	"github.com/division-sh/swarm/internal/runtime/agentmemory"
	runtimeactors "github.com/division-sh/swarm/internal/runtime/core/actors"
	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	runtimedelivery "github.com/division-sh/swarm/internal/runtime/deliverylifecycle"
	runtimegenericschedule "github.com/division-sh/swarm/internal/runtime/genericschedule"
	runtimemanager "github.com/division-sh/swarm/internal/runtime/manager"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimesessions "github.com/division-sh/swarm/internal/runtime/sessions"
	runforkrevision "github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	agentfixture "github.com/division-sh/swarm/internal/store/testutil/agentfixture"
	authoractivityfixture "github.com/division-sh/swarm/internal/store/testutil/authoractivityfixture"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

func TestRunForkRevisionRegistryIsClosed(t *testing.T) {
	want := []runforkrevision.Family{
		runforkrevision.FamilyAgentConversationAudits,
		runforkrevision.FamilyAgentSessions,
		runforkrevision.FamilyAgentTurns,
		runforkrevision.FamilyCommittedReplayScopes,
		runforkrevision.FamilyDeadLetters,
		runforkrevision.FamilyEntityMetadata,
		runforkrevision.FamilyEntityMutations,
		runforkrevision.FamilyEventDeliveries,
		runforkrevision.FamilyEventReceipts,
		runforkrevision.FamilyEvents,
		runforkrevision.FamilyFanOutObligations,
		runforkrevision.FamilyReplyContexts,
		runforkrevision.FamilyTimers,
	}
	got := runforkrevision.AllFamilies()
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("run-fork revision registry = %q, want exact 13-family registry %q", got, want)
	}
}

func TestRunForkRevisionCapturePreservesExactEventPayloadBytes(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	payload := []byte("{\n  \"numeric\": 1.0, \"ordered\": [2, 1]\n}")
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	event := semanticEventRecordFixture(
		eventID, runID, "revision.payload_bytes", eventtest.Producer(events.EventProducerPlatform, "revision-test"),
		payload, events.EventEnvelope{}, time.Now().UTC(),
	)
	if err := insertPostgresCanonicalEventRecordFixtureTx(ctx, tx, event); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	revision, err := finalizePostgresRunForkTestRevision(ctx, tx, runID, runforkrevision.FamilyEvents)
	if err != nil {
		t.Fatalf("capture revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit revision: %v", err)
	}

	var fact []byte
	if err := db.QueryRowContext(ctx, `
		SELECT fact
		FROM run_fork_fact_revisions
		WHERE run_id = $1::uuid AND revision = $2 AND family = 'events' AND fact_key = $3 AND present
	`, runID, revision, eventID).Scan(&fact); err != nil {
		t.Fatalf("load revision fact: %v", err)
	}
	var projection map[string]json.RawMessage
	if err := json.Unmarshal(fact, &projection); err != nil {
		t.Fatalf("decode revision fact: %v", err)
	}
	if _, exists := projection["payload"]; exists {
		t.Fatal("revision fact retained normalized payload as a second authority")
	}
	var encoded string
	if err := json.Unmarshal(projection["payload_base64"], &encoded); err != nil {
		t.Fatalf("decode payload_base64: %v", err)
	}
	restored, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode revision payload bytes: %v", err)
	}
	if !bytes.Equal(restored, payload) {
		t.Fatalf("revision payload = %q, want %q", restored, payload)
	}
}

func TestRunForkRevisionEqualityIncludesExactEventPayloadBytes(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	firstPayload := []byte(`{"numeric":1,"ordered":{"a":1,"b":2}}`)
	secondPayload := []byte("{\n  \"ordered\": {\"b\": 2, \"a\": 1}, \"numeric\": 1.0\n}")
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})

	firstTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin first revision transaction: %v", err)
	}
	defer func() { _ = firstTx.Rollback() }()
	event := semanticEventRecordFixture(
		eventID, runID, "revision.payload_identity", eventtest.Producer(events.EventProducerPlatform, "revision-test"),
		firstPayload, events.EventEnvelope{}, time.Now().UTC(),
	)
	if err := insertPostgresCanonicalEventRecordFixtureTx(ctx, firstTx, event); err != nil {
		t.Fatalf("insert first event fact: %v", err)
	}
	firstRevision, err := finalizePostgresRunForkTestRevision(ctx, firstTx, runID, runforkrevision.FamilyEvents)
	if err != nil {
		t.Fatalf("capture first event revision: %v", err)
	}
	if err := firstTx.Commit(); err != nil {
		t.Fatalf("commit first event revision: %v", err)
	}

	secondTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin second revision transaction: %v", err)
	}
	defer func() { _ = secondTx.Rollback() }()
	if _, err := secondTx.ExecContext(ctx, `UPDATE events SET payload_bytes=$2::bytea WHERE event_id=$1::uuid`, eventID, secondPayload); err != nil {
		t.Fatalf("mutate only authoritative payload bytes: %v", err)
	}
	secondRevision, err := finalizePostgresRunForkTestRevision(ctx, secondTx, runID, runforkrevision.FamilyEvents)
	if err != nil {
		t.Fatalf("capture byte-distinct event revision: %v", err)
	}
	if secondRevision <= firstRevision {
		t.Fatalf("byte-distinct payload revision = %d, want newer than %d", secondRevision, firstRevision)
	}
	if err := secondTx.Commit(); err != nil {
		t.Fatalf("commit byte-distinct event revision: %v", err)
	}

	for _, proof := range []struct {
		name     string
		revision int64
		want     []byte
	}{
		{name: "first", revision: firstRevision, want: firstPayload},
		{name: "second", revision: secondRevision, want: secondPayload},
	} {
		t.Run(proof.name, func(t *testing.T) {
			var fact []byte
			if err := db.QueryRowContext(ctx, `
				SELECT fact
				FROM run_fork_fact_revisions
				WHERE run_id=$1::uuid AND revision=$2 AND family='events' AND fact_key=$3 AND present
			`, runID, proof.revision, eventID).Scan(&fact); err != nil {
				t.Fatalf("load revision fact: %v", err)
			}
			var projection struct {
				PayloadBase64 string `json:"payload_base64"`
			}
			if err := json.Unmarshal(fact, &projection); err != nil {
				t.Fatalf("decode revision fact: %v", err)
			}
			restored, err := base64.StdEncoding.DecodeString(projection.PayloadBase64)
			if err != nil {
				t.Fatalf("decode revision payload bytes: %v", err)
			}
			if !bytes.Equal(restored, proof.want) {
				t.Fatalf("revision payload = %q, want %q", restored, proof.want)
			}
		})
	}
}

func TestRunForkRevisionStateAccessorInventoryIsClosed(t *testing.T) {
	root := repoRootForRuntimeWriterGuard(t)
	want := []string{
		"internal/runtime/destructivereset/cleanup_catalog.go",
		"internal/store/internal/adminpersistence/destructive_reset_cleanup.go",
		"internal/store/internal/backend/runforkpersistence/run_fork_activation.go",
		"internal/store/internal/backend/runforkrevision/postgres.go",
		"internal/store/internal/backend/runforkrevision/sqlite.go",
		"internal/store/platformschema/platformschema.go",
	}
	var got []string
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(body)
		if !strings.Contains(text, "run_fork_revision_heads") && !strings.Contains(text, "run_fork_revisions") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		got = append(got, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatalf("scan run-fork revision accessors: %v", err)
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("run-fork revision accessor inventory = %q, want exact classified set %q", got, want)
	}
}

func TestRunForkRevisionCaptureReusesTransactionRevisionAndRollbackPublishesNothing(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	seedPostgresSemanticEventRecordFixtureTx(t, ctx, tx, eventID, runID, "revision.rollback", events.EventProducerPlatform, "revision-test", "", "", time.Now().UTC())
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO entity_mutations (
			run_id, entity_id, domain, path, new_value, caused_by_event, writer_type, writer_id
		) VALUES ($1::uuid, $2::uuid, 'lifecycle_state', '', '"ready"'::jsonb, $3::uuid, 'platform', 'revision-test')
	`, runID, uuid.NewString(), eventID); err != nil {
		t.Fatalf("seed mutation: %v", err)
	}
	effects := runforkrevision.NewEffects()
	if err := effects.Add(runID, runforkrevision.FamilyEvents, runforkrevision.FamilyEntityMutations); err != nil {
		t.Fatalf("declare transaction revision effects: %v", err)
	}
	results, err := runforkrevision.FinalizePostgres(ctx, tx, effects)
	if err != nil {
		t.Fatalf("finalize transaction revision: %v", err)
	}
	if results[runID].Revision != 1 {
		t.Fatalf("finalized revision = %d, want 1", results[runID].Revision)
	}
	reused, err := finalizePostgresRunForkTestRevision(ctx, tx, runID, runforkrevision.FamilyEvents)
	if err != nil {
		t.Fatalf("repeat capture: %v", err)
	}
	if reused != results[runID].Revision {
		t.Fatalf("repeated finalization revision = %d, want unchanged revision %d", reused, results[runID].Revision)
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
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	firstEventID := uuid.NewString()
	secondEventID := uuid.NewString()
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})

	first, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin first transaction: %v", err)
	}
	defer func() { _ = first.Rollback() }()
	seedPostgresSemanticEventRecordFixtureTx(t, ctx, first, firstEventID, runID, "revision.first", events.EventProducerPlatform, "revision-test", "", "", time.Now().UTC())
	firstRevision, err := finalizePostgresRunForkTestRevision(ctx, first, runID, runforkrevision.FamilyEvents)
	if err != nil {
		t.Fatalf("capture first transaction: %v", err)
	}

	second, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin second transaction: %v", err)
	}
	defer func() { _ = second.Rollback() }()
	seedPostgresSemanticEventRecordFixtureTx(t, ctx, second, secondEventID, runID, "revision.second", events.EventProducerPlatform, "revision-test", "", "", time.Now().UTC())
	type captureResult struct {
		revision int64
		err      error
	}
	done := make(chan captureResult, 1)
	go func() {
		revision, err := finalizePostgresRunForkTestRevision(ctx, second, runID, runforkrevision.FamilyEvents)
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
	if firstCount != 1 || secondCount != 1 {
		t.Fatalf("changed event facts = revision1:%d revision2:%d, want one append-only delta per revision", firstCount, secondCount)
	}
}

func TestRunForkRevisionCaptureLocksParentBeforeRevisionState(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 10*time.Second)
	defer cancel()
	runID := uuid.NewString()
	seedEventID := uuid.NewString()
	publishedEventID := uuid.NewString()
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	seedEvent := seedPostgresSemanticEventRecordFixture(t, ctx, db, seedEventID, runID, "revision.delivery.seed", events.EventProducerPlatform, "revision-test", "", "", time.Now().UTC())
	route := testAgentDeliveryRoute(t, runID, "revision-agent", "fixture/revision-agent")
	deliveryID := seedDeliveryStateFixture(t, ctx, postgresDeliveryFixtureStore(db), seedEvent, route, runtimedelivery.StateQueued, nil).DeliveryID

	publishTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin event publication: %v", err)
	}
	defer func() { _ = publishTx.Rollback() }()
	var status string
	if err := publishTx.QueryRowContext(ctx, `SELECT status FROM runs WHERE run_id=$1::uuid FOR UPDATE`, runID).Scan(&status); err != nil {
		t.Fatalf("lock event publication run: %v", err)
	}
	seedPostgresSemanticEventRecordFixtureTx(t, ctx, publishTx, publishedEventID, runID, "revision.delivery.concurrent", events.EventProducerPlatform, "revision-test", "", "", time.Now().UTC())

	deliveryTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin delivery start: %v", err)
	}
	defer func() { _ = deliveryTx.Rollback() }()
	deliveryTxCtx, err := authoractivityfixture.Begin(ctx, deliveryTx, authoractivityfixture.DialectPostgres)
	if err != nil {
		t.Fatalf("begin delivery author activity: %v", err)
	}
	story, ok := authoractivityfixture.Mutation(deliveryTxCtx)
	if !ok {
		t.Fatal("delivery author activity owner is unavailable")
	}
	snapshot, err := postgresDeliveryAdapter.SnapshotExact(deliveryTxCtx, deliveryTx, seedEvent, route)
	if err != nil {
		t.Fatalf("load delivery authority: %v", err)
	}
	result, err := postgresDeliveryAdapter.ClaimExactResult(deliveryTxCtx, deliveryTx, story, snapshot.Authority, seedEvent, route, runtimedelivery.DefaultLeaseTTL)
	if err != nil {
		t.Fatalf("stage delivery start: %v", err)
	}
	if _, ok := result.Acquired(); !ok {
		t.Fatalf("stage delivery start disposition = %s, want acquired", result.Disposition)
	}
	var deliveryBackendPID int
	if err := deliveryTx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&deliveryBackendPID); err != nil {
		t.Fatalf("load delivery backend PID: %v", err)
	}
	type captureResult struct {
		revision int64
		err      error
	}
	deliveryCapture := make(chan captureResult, 1)
	go func() {
		runID, err := runforkrevision.RunIDForEvent(deliveryTxCtx, deliveryTx, seedEventID)
		if err != nil {
			deliveryCapture <- captureResult{err: err}
			return
		}
		revision, err := finalizePostgresRunForkTestRevision(deliveryTxCtx, deliveryTx, runID, runforkrevision.FamilyEventDeliveries)
		deliveryCapture <- captureResult{revision: revision, err: err}
	}()
	waitForPostgresBackendLock(t, ctx, db, deliveryBackendPID)

	publishRevision, err := finalizePostgresRunForkTestRevision(ctx, publishTx, runID, runforkrevision.FamilyEvents, runforkrevision.FamilyEventDeliveries)
	if err != nil {
		t.Fatalf("capture event publication revision: %v", err)
	}
	if err := publishTx.Commit(); err != nil {
		t.Fatalf("commit event publication: %v", err)
	}
	var delivered captureResult
	select {
	case delivered = <-deliveryCapture:
	case <-ctx.Done():
		t.Fatalf("delivery capture did not resume after parent commit: %v", ctx.Err())
	}
	if delivered.err != nil {
		t.Fatalf("capture delivery-start revision: %v", delivered.err)
	}
	if err := deliveryTx.Commit(); err != nil {
		t.Fatalf("commit delivery start: %v", err)
	}
	if publishRevision != 2 || delivered.revision != 3 {
		t.Fatalf("concurrent revisions = publish:%d delivery:%d, want 2 then 3 after initial obligation admission", publishRevision, delivered.revision)
	}

	for revision, wantStatus := range map[int64]string{1: "pending", 3: "in_progress"} {
		var gotStatus string
		if err := db.QueryRowContext(ctx, `
			SELECT fact->>'status'
			FROM run_fork_fact_revisions
			WHERE run_id=$1::uuid AND revision=$2 AND family='event_deliveries' AND fact_key=$3 AND present
		`, runID, revision, deliveryID).Scan(&gotStatus); err != nil {
			t.Fatalf("load delivery revision %d: %v", revision, err)
		}
		if gotStatus != wantStatus {
			t.Fatalf("delivery revision %d status = %q, want %q", revision, gotStatus, wantStatus)
		}
	}
	var publishedFacts, ledgerRows int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_fact_revisions WHERE run_id=$1::uuid AND revision=2 AND family='events' AND present`, runID).Scan(&publishedFacts); err != nil {
		t.Fatalf("count published event facts: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_revisions WHERE run_id=$1::uuid`, runID).Scan(&ledgerRows); err != nil {
		t.Fatalf("count revision ledger: %v", err)
	}
	if publishedFacts != 2 || ledgerRows != 3 {
		t.Fatalf("revision evidence = changed_events:%d ledger:%d, want 2/3", publishedFacts, ledgerRows)
	}
}

func waitForPostgresBackendLock(t *testing.T, ctx context.Context, db *sql.DB, backendPID int) {
	t.Helper()
	for {
		var waiting bool
		var query string
		err := db.QueryRowContext(ctx, `
			SELECT COALESCE(wait_event_type = 'Lock', false), COALESCE(query, '')
			FROM pg_stat_activity
			WHERE pid = $1
		`, backendPID).Scan(&waiting, &query)
		if err != nil {
			t.Fatalf("inspect PostgreSQL backend %d: %v", backendPID, err)
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("PostgreSQL backend %d did not reach lock barrier; last query %q: %v", backendPID, query, ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func waitForPostgresQueryLock(t *testing.T, ctx context.Context, db *sql.DB, queryFragment string) {
	t.Helper()
	for {
		var waiting bool
		err := db.QueryRowContext(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM pg_stat_activity
				WHERE datname = current_database()
				  AND pid <> pg_backend_pid()
				  AND wait_event_type = 'Lock'
				  AND query LIKE '%' || $1 || '%'
			)
		`, queryFragment).Scan(&waiting)
		if err != nil {
			t.Fatalf("inspect PostgreSQL query lock %q: %v", queryFragment, err)
		}
		if waiting {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("PostgreSQL query %q did not reach lock barrier: %v", queryFragment, ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestRunForkRevisionCaptureOrdersMultiRunLocksDeterministically(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(testAuthorActivityContext(), 5*time.Second)
	defer cancel()
	runIDs := []string{uuid.NewString(), uuid.NewString()}
	sort.Strings(runIDs)
	for _, runID := range runIDs {
		requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
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
			event := eventtest.PersistedProjectionForProducer(
				uuid.NewString(), "revision.multi", eventtest.Producer(events.EventProducerPlatform, "revision-test"), "",
				[]byte(`{}`), 0, runID, "", events.EventEnvelope{Scope: events.EventScopeGlobal}, time.Now().UTC(),
			)
			if err := insertPostgresCanonicalEventRecordFixtureTx(ctx, tx, event); err != nil {
				results <- workerResult{err: err}
				return
			}
		}
		<-start
		effects := runforkrevision.NewEffects()
		for _, runID := range order {
			if err := effects.Add(runID, runforkrevision.FamilyEvents); err != nil {
				results <- workerResult{err: err}
				return
			}
		}
		finalized, err := runforkrevision.FinalizePostgres(ctx, tx, effects)
		revisions := make(map[string]int64, len(finalized))
		for runID, result := range finalized {
			revisions[runID] = result.Revision
		}
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
	ctx := testAuthorActivityContext()
	store := admitTestPostgresStore(t, db)
	now := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	runID := uuid.NewString()
	agentID := "revision-lifecycle-agent"
	identity := mustTestAgentIdentityForRun(runID, agentID, runForkRevisionFlowInstance)
	fields := testAgentIdentityStorageFields(t, identity)
	agent := runtimemanager.PersistedAgent{
		Config: withRuntimePersistenceTestIntent(t, runtimeactors.AgentConfig{ExecutionMode: "live", ID: agentID, Identity: identity, FlowID: runForkRevisionFlowInstance, FlowPath: runForkRevisionFlowInstance, Role: "worker", Type: "sonnet", Model: "regular",
			Memory: agentmemory.Authored(true),
			Config: []byte(`{}`),
		}),
		Status: "active", HiredBy: "revision-proof", StartedAt: now,
	}
	spawned, err := agentfixture.CommitStatic(t, ctx, store, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "spawn", RequestHash: "revision-spawn",
		Identity: identity, AgentID: agentID, Trigger: "spawn", TargetEpoch: 1, TargetGeneration: 1,
		TargetPhase: runtimemanager.AgentLifecycleRegistered, ConfigRevision: "revision-1",
		RunMode: runtimemanager.AgentRunModeStopped, Agent: &agent, Now: now,
	})
	if err != nil {
		t.Fatalf("spawn lifecycle agent: %v", err)
	}
	started, err := agentfixture.CommitStatic(t, ctx, store, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "start", RequestHash: "revision-start",
		Identity: identity, AgentID: agentID, Trigger: "start", ExpectedEpoch: spawned.RuntimeEpoch,
		ExpectedGeneration: spawned.Generation, ExpectedPhase: spawned.Phase,
		TargetEpoch: spawned.RuntimeEpoch, TargetGeneration: spawned.Generation + 1,
		TargetPhase: runtimemanager.AgentLifecycleRunning, ConfigRevision: "revision-1",
		RunMode: runtimemanager.AgentRunModeStandard, Now: now.Add(time.Second),
	})
	if err != nil {
		t.Fatalf("start lifecycle agent: %v", err)
	}

	eventID := uuid.NewString()
	sessionID := uuid.NewString()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin lifecycle source revision: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	requirePostgresRunFixtureInRawTxForTest(t, ctx, tx, semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	seedPostgresSemanticEventRecordFixtureTx(t, ctx, tx, eventID, runID, "lifecycle.revision", events.EventProducerPlatform, "revision-test", "", "", now)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_sessions (
			session_id, run_id, agent_id, agent_name_owner, agent_name_source,
			agent_route_presence, flow_scope_key, flow_instance_id, flow_instance,
			memory_enabled, memory_source,
			conversation, turn_count, runtime_state, status, created_at, updated_at
		) VALUES (
			$1::uuid,$2::uuid,$3,$4,$5,$6,$7,$8,$9,TRUE,'authored',
			'[]'::jsonb,0,'{}'::jsonb,'active',$10,$10
		)
	`, sessionID, runID, fields.AgentID, fields.NameOwner, fields.NameSource,
		fields.RoutePresence, fields.FlowScopeKey, fields.FlowInstanceID, fields.FlowInstancePath, now); err != nil {
		t.Fatalf("seed lifecycle source session: %v", err)
	}
	if _, err := finalizePostgresRunForkTestRevision(ctx, tx, runID, runforkrevision.FamilyEvents, runforkrevision.FamilyAgentSessions); err != nil {
		t.Fatalf("finalize lifecycle source revision: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit lifecycle source revision: %v", err)
	}

	if _, err := agentfixture.CommitStatic(t, ctx, store, runtimemanager.AgentLifecycleTransition{
		OperationID: uuid.NewString(), OperationKind: "teardown", RequestHash: "revision-terminate",
		Identity: identity, AgentID: agentID, Trigger: "terminate", ExpectedEpoch: started.RuntimeEpoch,
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

func TestRunForkRevisionSessionProjectionIgnoresExcludedWriterChurnAndTracksStatusPresence(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	store := admitTestPostgresStore(t, db)
	runID := uuid.NewString()
	eventID := uuid.NewString()
	agentID := "revision-session-agent"
	sessionID := uuid.NewString()
	at := time.Unix(1700000850, 0).UTC()
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "session.projection", events.EventProducerPlatform, "revision-test", "", "", at)
	seedRunForkSessionProjection(t, db, runID, agentID, sessionID, "active", at)
	firstRevision := captureRunForkTestRevision(t, db, runID)

	exerciseRunForkSessionExcludedWriters(t, store, runID, agentID, sessionID)
	var afterExcluded int64
	if err := db.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1::uuid`, runID).Scan(&afterExcluded); err != nil {
		t.Fatalf("load revision after excluded writers: %v", err)
	}
	if afterExcluded != firstRevision {
		t.Fatalf("revision after lease/watchdog/turn/provider-session writers = %d, want unchanged %d", afterExcluded, firstRevision)
	}
	validationTx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("begin excluded-writer validation: %v", err)
	}
	if err := runforkrevision.ValidateCompletePostgres(ctx, validationTx, runID); err != nil {
		_ = validationTx.Rollback()
		t.Fatalf("validate excluded-writer projection: %v", err)
	}
	_ = validationTx.Rollback()

	statusTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin projected status mutation: %v", err)
	}
	defer func() { _ = statusTx.Rollback() }()
	if _, err := statusTx.ExecContext(ctx, `UPDATE agent_sessions SET status='terminated', termination_reason='normal', terminated_at=$2, updated_at=$2 WHERE session_id=$1::uuid`, sessionID, at.Add(time.Minute)); err != nil {
		t.Fatalf("update projected session status: %v", err)
	}
	statusRevision, err := finalizePostgresRunForkTestRevision(ctx, statusTx, runID, runforkrevision.FamilyAgentSessions)
	if err != nil {
		t.Fatalf("capture projected session status: %v", err)
	}
	if statusRevision <= firstRevision {
		t.Fatalf("projected status revision = %d, want after %d", statusRevision, firstRevision)
	}
	if err := statusTx.Commit(); err != nil {
		t.Fatalf("commit projected session status: %v", err)
	}

	deleteTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin projected session deletion: %v", err)
	}
	defer func() { _ = deleteTx.Rollback() }()
	if _, err := deleteTx.ExecContext(ctx, `DELETE FROM agent_sessions WHERE session_id=$1::uuid`, sessionID); err != nil {
		t.Fatalf("delete projected session: %v", err)
	}
	deleteRevision, err := finalizePostgresRunForkTestRevision(ctx, deleteTx, runID, runforkrevision.FamilyAgentSessions)
	if err != nil {
		t.Fatalf("capture projected session deletion: %v", err)
	}
	if deleteRevision <= statusRevision {
		t.Fatalf("session deletion revision = %d, want after %d", deleteRevision, statusRevision)
	}
	if err := deleteTx.Commit(); err != nil {
		t.Fatalf("commit projected session deletion: %v", err)
	}
	var present bool
	if err := db.QueryRowContext(ctx, `SELECT present FROM run_fork_fact_revisions WHERE run_id=$1::uuid AND family='agent_sessions' AND fact_key=$2 AND revision=$3`, runID, sessionID, deleteRevision).Scan(&present); err != nil {
		t.Fatalf("load projected session tombstone: %v", err)
	}
	if present {
		t.Fatal("deleted projected session remained present")
	}
}

func TestGenericScheduleDuplicateCancellationDoesNotPublishRunForkRevision(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	runID := uuid.NewString()
	ctx := runtimecorrelation.WithRunID(testAuthorActivityContext(), runID)
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	store := admitTestPostgresStore(t, db)
	command := testAgentGenericScheduleCommand(
		t, runID, "revision-agent", "revision-flow/instance", uuid.NewString(), "revision-task",
		runtimegenericschedule.AbsoluteDue(time.Now().Add(time.Hour)),
	)
	admitted, err := store.AdmitGenericSchedule(ctx, command)
	if err != nil {
		t.Fatalf("admit generic schedule: %v", err)
	}
	var admittedRevision int64
	if err := db.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1::uuid`, runID).Scan(&admittedRevision); err != nil {
		t.Fatalf("load revision after admission: %v", err)
	}
	var admittedFact []byte
	if err := db.QueryRowContext(ctx, `SELECT fact FROM run_fork_fact_revisions WHERE run_id=$1::uuid AND family='timers' AND fact_key=$2 AND revision=$3 AND present`, runID, admitted.Activation.ID, admittedRevision).Scan(&admittedFact); err != nil {
		t.Fatalf("load projected admission fact: %v", err)
	}
	var admission map[string]any
	if err := json.Unmarshal(admittedFact, &admission); err != nil {
		t.Fatalf("decode projected admission fact: %v", err)
	}
	for field, want := range map[string]string{
		"schedule_key": command.ScheduleKey, "immutable_hash": admitted.Activation.ImmutableHash,
		"due_basis_kind": string(command.Due.Kind), "owner_kind": string(command.OwnerKind),
		"owner_agent": command.OwnerID, "task_id": command.TaskID, "status": string(runtimegenericschedule.StatusActive),
		"execution_mode": string(command.ExecutionMode),
	} {
		if got, _ := admission[field].(string); got != want {
			t.Fatalf("projected admission %s = %q, want %q; fact=%s", field, got, want, admittedFact)
		}
	}
	cancel := runtimegenericschedule.CancelCommand{
		ActivationID: admitted.Activation.ID, Cause: "test_cancel", CancelledAt: time.Now(),
	}
	if _, err := store.CancelGenericSchedule(ctx, cancel); err != nil {
		t.Fatalf("first cancellation: %v", err)
	}
	var revision int64
	if err := db.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1::uuid`, runID).Scan(&revision); err != nil {
		t.Fatalf("load revision after terminal mutation: %v", err)
	}
	if revision <= admittedRevision {
		t.Fatalf("cancellation revision = %d, want after admission revision %d", revision, admittedRevision)
	}
	var cancelledFact []byte
	if err := db.QueryRowContext(ctx, `SELECT fact FROM run_fork_fact_revisions WHERE run_id=$1::uuid AND family='timers' AND fact_key=$2 AND revision=$3 AND present`, runID, admitted.Activation.ID, revision).Scan(&cancelledFact); err != nil {
		t.Fatalf("load projected cancellation fact: %v", err)
	}
	var cancellation map[string]any
	if err := json.Unmarshal(cancelledFact, &cancellation); err != nil {
		t.Fatalf("decode projected cancellation fact: %v", err)
	}
	if got, _ := cancellation["status"].(string); got != string(runtimegenericschedule.StatusCancelled) {
		t.Fatalf("projected cancellation status = %q, want cancelled; fact=%s", got, cancelledFact)
	}
	if got, _ := cancellation["cancel_cause"].(string); got != cancel.Cause {
		t.Fatalf("projected cancel cause = %q, want %q; fact=%s", got, cancel.Cause, cancelledFact)
	}
	if _, err := store.CancelGenericSchedule(ctx, cancel); err != nil {
		t.Fatalf("duplicate cancellation: %v", err)
	}
	var afterDuplicate int64
	if err := db.QueryRowContext(ctx, `SELECT last_revision FROM run_fork_revision_heads WHERE run_id=$1::uuid`, runID).Scan(&afterDuplicate); err != nil {
		t.Fatalf("load revision after duplicate mutation: %v", err)
	}
	if afterDuplicate != revision {
		t.Fatalf("revision after duplicate cancellation = %d, want unchanged %d", afterDuplicate, revision)
	}
}

func TestRunForkRevisionDeletionPublishesTombstoneAndUnrevisionedDriftFailsClosed(t *testing.T) {
	_, db, _ := testutil.StartPostgres(t)
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	eventID := uuid.NewString()
	requireRunFixtureForTest(t, ctx, newPostgresStoreWithBackend(mustPostgresBackend(db)), semanticRunFixture{Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID})
	seedPostgresSemanticEventRecordFixture(t, ctx, db, eventID, runID, "revision.delete", events.EventProducerPlatform, "revision-test", "", "", time.Now().UTC())
	timer := admitGenericScheduleFixture(t, ctx, admitTestPostgresStore(t, db), testRootGenericScheduleCommand(
		t, runID, uuid.NewString(), "revision-delete", runtimegenericschedule.DelayDue(time.Hour),
	))
	timerID := timer.ID
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
	if _, err := (admitTestPostgresStore(t, db)).PlanRunFork(ctx, runfork.RunForkPlanRequest{SourceRunID: runID, At: eventID}); err == nil || !strings.Contains(err.Error(), "unsupported unrevisioned events facts") {
		t.Fatalf("PlanRunFork drift error = %v, want fail-closed unrevisioned events", err)
	}
}
