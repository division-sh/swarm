package runtimepersistence

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/activityidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/loopruntime"
	runtimepipeline "github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	authoractivityadapter "github.com/division-sh/swarm/internal/store/internal/backend/authoractivity/readadapter"
	"github.com/google/uuid"
)

type activityStoryJournal interface {
	activityTimestampJournal
	ClaimActivityAttemptForLoopGeneration(context.Context, runtimepipeline.ActivityAttemptRecord) (runtimepipeline.ActivityAttemptRecord, bool, error)
}

type activityStorySpy struct {
	drafts []authoractivity.Draft
	err    error
}

func (s *activityStorySpy) Record(_ context.Context, draft authoractivity.Draft) error {
	s.drafts = append(s.drafts, draft)
	return s.err
}
func (*activityStorySpy) PersistedOccurredAt(context.Context, string) (time.Time, bool, error) {
	return time.Time{}, false, nil
}
func (*activityStorySpy) PersistedAuthorSafeSummary(context.Context, string) (string, bool, error) {
	return "", false, nil
}

// The kernel spy observes drafts before deduplication. Each transaction is rolled
// back; the subsequent public store operation independently proves real commit
// behavior with the actual run guard and story-aware outer transaction.
func TestActivityJournalStoryDispositionBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			postgres := backend.name == "postgres"
			for _, mode := range []executionmode.Mode{executionmode.Live, executionmode.Mock} {
				for _, status := range []string{"absent", "started", "succeeded", "failed", "uncertain"} {
					for _, op := range []string{"start", "claim", "complete", "uncertain"} {
						t.Run(string(mode)+"/"+status+"/"+op, func(t *testing.T) {
							record := seedActivityStoryAttempt(t, fixture, mode, status, op == "claim")
							request := activityStoryRequest(t, record, op)
							before := snapshotForkHistoricalExecutionTables(t, fixture.db, postgres)
							missing := status == "absent" && (op == "complete" || op == "uncertain")
							changed := (status == "absent" && (op == "start" || op == "claim")) ||
								(status == "started" && (op == "complete" || op == "uncertain"))
							spy := &activityStorySpy{}
							got, inserted, err := activityStoryKernel(t, fixture, postgres, op, request, spy)
							if missing {
								if err == nil || !strings.Contains(err.Error(), "was not found") {
									t.Fatalf("missing record error = %v", err)
								}
							} else if err != nil {
								t.Fatal(err)
							}
							wantDrafts := 0
							if changed {
								wantDrafts = 1
							}
							if len(spy.drafts) != wantDrafts {
								t.Fatalf("draft calls = %d, want %d", len(spy.drafts), wantDrafts)
							}
							if !changed && !missing && (!reflect.DeepEqual(got, record) || inserted) {
								t.Fatal("kernel no-op changed authoritative evidence")
							}
							assertActivityStoryTablesUnchanged(t, fixture, postgres, before)
							previous := activityStoryOccurrences(t, fixture, postgres, record.RunID)
							got, inserted, err = activityStoryOuter(fixture, op, request)
							if missing {
								if err == nil || !strings.Contains(err.Error(), "was not found") {
									t.Fatalf("outer missing record error = %v", err)
								}
							} else if err != nil {
								t.Fatal(err)
							}
							if !changed {
								if !missing && (!reflect.DeepEqual(got, record) || inserted) {
									t.Fatal("outer no-op changed authoritative evidence")
								}
								assertActivityStoryTablesUnchanged(t, fixture, postgres, before)
								return
							}
							after := activityStoryOccurrences(t, fixture, postgres, record.RunID)
							if len(after) != len(previous)+1 || !reflect.DeepEqual(previous, after[:len(previous)]) {
								t.Fatal("real transition did not append exactly one occurrence")
							}
							last := after[len(after)-1]
							wantStatus := "started"
							if op == "complete" {
								wantStatus = "succeeded"
							}
							if op == "uncertain" {
								wantStatus = "uncertain"
							}
							if got.Status != wantStatus || last.Transition != wantStatus || last.SourceIdentity != record.RequestEventID || last.Kind != authoractivity.KindActivityLifecycle || spy.drafts[0].Transition != wantStatus {
								t.Fatalf("wrong source/story transition: record=%#v story=%#v", got, last)
							}
							if inserted != (status == "absent") {
								t.Fatal("incorrect insert disposition")
							}
							var head int64
							if err := fixture.db.QueryRow(`SELECT last_sequence FROM author_activity_order WHERE singleton_id=1`).Scan(&head); err != nil || head != last.Sequence {
								t.Fatalf("story head = %d, err=%v, want %d", head, err, last.Sequence)
							}
							loaded, found, err := fixture.store.(activityStoryJournal).LoadActivityAttempt(testAuthorActivityContext(), record.RequestEventID)
							if err != nil || !found || !reflect.DeepEqual(loaded, got) {
								t.Fatal("committed source readback differs")
							}
						})
					}
				}
			}
		})
	}
}

func TestActivityJournalStoryValidationNoDraftBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			postgres := backend.name == "postgres"
			for _, status := range []string{"started", "succeeded", "failed", "uncertain"} {
				for _, op := range []string{"start", "claim", "complete", "uncertain"} {
					for _, fault := range []string{"mode", "identity", "stale_generation"} {
						if fault == "identity" && op != "start" && op != "claim" {
							continue
						}
						if fault == "stale_generation" && op != "claim" {
							continue
						}
						t.Run(status+"/"+op+"/"+fault, func(t *testing.T) {
							record := seedActivityStoryAttempt(t, fixture, executionmode.Live, status, op == "claim")
							request := activityStoryRequest(t, record, op)
							want := "conflict"
							switch fault {
							case "mode":
								request.ExecutionMode = executionmode.Mock
							case "identity":
								request.InputHash = "conflicting-input"
							case "stale_generation":
								request.Generation.RevisionID = uuid.NewString()
								want = "activity_loop_generation_stale"
							}
							before := snapshotForkHistoricalExecutionTables(t, fixture.db, postgres)
							spy := &activityStorySpy{}
							_, _, err := activityStoryKernel(t, fixture, postgres, op, request, spy)
							if err == nil || !strings.Contains(err.Error(), want) || len(spy.drafts) != 0 {
								t.Fatalf("kernel error=%v drafts=%d, want %s and no draft", err, len(spy.drafts), want)
							}
							_, _, err = activityStoryOuter(fixture, op, request)
							if err == nil || !strings.Contains(err.Error(), want) {
								t.Fatalf("outer error=%v, want %s", err, want)
							}
							assertActivityStoryTablesUnchanged(t, fixture, postgres, before)
						})
					}
				}
			}
		})
	}
}

func TestActivityJournalStoryFailureAtomicityBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			postgres := backend.name == "postgres"
			for _, op := range []string{"start", "claim", "complete", "uncertain"} {
				t.Run(op, func(t *testing.T) {
					status := "started"
					if op == "start" || op == "claim" {
						status = "absent"
					}
					record := seedActivityStoryAttempt(t, fixture, executionmode.Live, status, op == "claim")
					request := activityStoryRequest(t, record, op)
					before := snapshotForkHistoricalExecutionTables(t, fixture.db, postgres)
					failure := errors.New("injected activity story draft failure")
					spy := &activityStorySpy{err: failure}
					_, _, err := activityStoryKernel(t, fixture, postgres, op, request, spy)
					if !errors.Is(err, failure) || len(spy.drafts) != 1 {
						t.Fatalf("injected draft result=%v calls=%d", err, len(spy.drafts))
					}
					assertActivityStoryTablesUnchanged(t, fixture, postgres, before)
					// Fail actual occurrence insertion inside the real outer owner, not
					// just the spy, after the source mutation has been attempted.
					remove := installActivityStoryFailure(t, fixture.db, postgres)
					_, _, err = activityStoryOuter(fixture, op, request)
					remove()
					if err == nil || !strings.Contains(err.Error(), "injected activity story persistence") {
						t.Fatalf("outer injected failure=%v", err)
					}
					assertActivityStoryTablesUnchanged(t, fixture, postgres, before)
					if _, _, err := activityStoryOuter(fixture, op, request); err != nil {
						t.Fatalf("retry after rollback: %v", err)
					}
				})
			}
		})
	}
}

func seedActivityStoryAttempt(t *testing.T, fixture authorActivityReceiptFixture, mode executionmode.Mode, status string, generated bool) runtimepipeline.ActivityAttemptRecord {
	t.Helper()
	ctx := testAuthorActivityContext()
	runID := uuid.NewString()
	seedAuthorActivityReceiptRun(t, fixture, ctx, runID)
	record := runtimepipeline.ActivityAttemptRecord{
		RequestEventID: uuid.NewString(), RunID: runID, ExecutionMode: mode, EntityID: uuid.NewString(), FlowInstance: "flow/" + uuid.NewString(),
		NodeID: activityidentity.MustNodeOwner(mustPersistenceNode("flow", "writer")).Key(), HandlerEventKey: "request", ActivityID: "write", Tool: "provider.write",
		EffectClass: "non_idempotent_write", Attempt: 1, SuccessEvent: "write.succeeded", FailureEvent: "write.failed", InputHash: "exact-input",
	}
	if generated {
		at := time.Now().UTC()
		activation, err := loopruntime.New(runID, record.EntityID, "flow", "revision", "revision_id", uuid.NewString(), "review", 3, at)
		if err != nil {
			t.Fatal(err)
		}
		buckets := map[string]map[string]any{}
		if err := loopruntime.Store(buckets, activation); err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(buckets)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.db.Exec(`INSERT INTO entity_state (run_id,entity_id,flow_instance,entity_type,current_state,gates,fields,bookkeeping,accumulator,revision,entered_state_at,created_at,updated_at) VALUES ($1,$2,$3,'default','review','{}','{}','{}',$4,1,$5,$5,$5)`, runID, record.EntityID, record.FlowInstance, string(raw), at); err != nil {
			t.Fatal(err)
		}
		record.Generation, record.LoopStage = activation.Generation(), "review"
	}
	if status == "absent" {
		return record
	}
	op := "start"
	if generated {
		op = "claim"
	}
	actual, inserted, err := activityStoryOuter(fixture, op, record)
	if err != nil || !inserted {
		t.Fatalf("seed start: inserted=%v err=%v", inserted, err)
	}
	if status == "started" {
		return actual
	}
	request := activityStoryTerminal(t, actual, status)
	op = "complete"
	if status == "uncertain" {
		op = "uncertain"
	}
	actual, _, err = activityStoryOuter(fixture, op, request)
	if err != nil {
		t.Fatalf("seed terminal: %v", err)
	}
	return actual
}

func activityStoryTerminal(t *testing.T, record runtimepipeline.ActivityAttemptRecord, status string) runtimepipeline.ActivityAttemptRecord {
	t.Helper()
	record.Status, record.ResultEventID, record.ResultEventType = status, uuid.NewString(), record.SuccessEvent
	record.ResultPayload, record.Failure = map[string]any{"ok": true}, nil
	if status != "succeeded" {
		class := runtimefailures.ClassDependencyUnavailable
		if status == "uncertain" {
			class = runtimefailures.ClassOutcomeUncertain
		}
		failure, ok := runtimefailures.EnvelopeFromError(runtimefailures.New(class, "activity_story_test", "activity-runtime", "execute", nil))
		if !ok {
			t.Fatal("missing failure envelope")
		}
		record.Failure, record.ResultEventType = &failure, record.FailureEvent
	}
	return record
}

func activityStoryRequest(t *testing.T, record runtimepipeline.ActivityAttemptRecord, op string) runtimepipeline.ActivityAttemptRecord {
	if op == "uncertain" {
		return activityStoryTerminal(t, record, "uncertain")
	}
	if op == "complete" {
		if record.Status == "succeeded" || record.Status == "failed" || record.Status == "uncertain" {
			return record
		}
		return activityStoryTerminal(t, record, "succeeded")
	}
	return record
}

func activityStoryOuter(fixture authorActivityReceiptFixture, op string, record runtimepipeline.ActivityAttemptRecord) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	j, ctx := fixture.store.(activityStoryJournal), testAuthorActivityContext()
	switch op {
	case "start":
		return j.StartActivityAttempt(ctx, record)
	case "claim":
		return j.ClaimActivityAttemptForLoopGeneration(ctx, record)
	case "complete":
		out, err := j.CompleteActivityAttempt(ctx, record)
		return out, false, err
	case "uncertain":
		out, err := j.MarkActivityAttemptUncertain(ctx, record)
		return out, false, err
	default:
		panic("unknown activity test operation")
	}
}

func activityStoryKernel(t *testing.T, fixture authorActivityReceiptFixture, postgres bool, op string, record runtimepipeline.ActivityAttemptRecord, spy *activityStorySpy) (runtimepipeline.ActivityAttemptRecord, bool, error) {
	t.Helper()
	ctx := testAuthorActivityContext()
	tx, err := fixture.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	dialect := activityjournal.DialectSQLite
	if postgres {
		dialect = activityjournal.DialectPostgres
	}
	// Only this draft-count probe stubs the run guard. Real outer tests do not.
	active := func(context.Context, string) error { return nil }
	switch op {
	case "start":
		return activityjournal.Start(ctx, tx, dialect, active, spy, record)
	case "claim":
		return activityjournal.Claim(ctx, tx, dialect, active, spy, record)
	case "complete":
		out, err := activityjournal.Complete(ctx, tx, dialect, active, spy, record)
		return out, false, err
	case "uncertain":
		out, err := activityjournal.MarkUncertain(ctx, tx, dialect, active, spy, record)
		return out, false, err
	default:
		panic("unknown activity test operation")
	}
}

func assertActivityStoryTablesUnchanged(t *testing.T, fixture authorActivityReceiptFixture, postgres bool, before map[string][]string) {
	t.Helper()
	after := snapshotForkHistoricalExecutionTables(t, fixture.db, postgres)
	for table, rows := range after {
		if !reflect.DeepEqual(before[table], rows) {
			t.Errorf("no-op/rollback changed table %s", table)
		}
	}
	if len(before) != len(after) {
		t.Fatal("application table set changed")
	}
}

func activityStoryOccurrences(t *testing.T, fixture authorActivityReceiptFixture, postgres bool, runID string) []authoractivity.Occurrence {
	t.Helper()
	dialect := authoractivityadapter.DialectSQLite
	if postgres {
		dialect = authoractivityadapter.DialectPostgres
	}
	page, err := authoractivityadapter.List(testAuthorActivityContext(), fixture.db, dialect, authoractivity.ListOptions{RunID: runID, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Occurrences) >= 100 {
		t.Fatal("activity test unexpectedly reached its story page limit")
	}
	return page.Occurrences
}

func installActivityStoryFailure(t *testing.T, db *sql.DB, postgres bool) func() {
	t.Helper()
	if postgres {
		if _, err := db.Exec(`CREATE FUNCTION activity_story_test_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected activity story persistence'; END $$`); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`CREATE TRIGGER activity_story_test_fail BEFORE INSERT ON author_activity_occurrences FOR EACH ROW EXECUTE FUNCTION activity_story_test_fail()`); err != nil {
			t.Fatal(err)
		}
	} else {
		if _, err := db.Exec(`CREATE TRIGGER activity_story_test_fail BEFORE INSERT ON author_activity_occurrences BEGIN SELECT RAISE(ABORT, 'injected activity story persistence'); END`); err != nil {
			t.Fatal(err)
		}
	}
	removed := false
	remove := func() {
		if removed {
			return
		}
		removed = true
		query := `DROP TRIGGER activity_story_test_fail`
		if postgres {
			query += ` ON author_activity_occurrences`
		}
		if _, err := db.Exec(query); err != nil {
			t.Error(err)
		}
		if postgres {
			if _, err := db.Exec(`DROP FUNCTION activity_story_test_fail()`); err != nil {
				t.Error(err)
			}
		}
	}
	t.Cleanup(remove)
	return remove
}
