package runtimepersistence

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkpersistence"
	"github.com/google/uuid"
)

type selectedInstalledHistoryRows struct {
	driver.Rows
	fault    string
	closed   bool
	position int
}

func (r *selectedInstalledHistoryRows) Close() error {
	err := r.Rows.Close()
	if r.fault == "close_error" {
		return errors.Join(err, errSelectedGrantHistoryClose)
	}
	return err
}

func (r *selectedInstalledHistoryRows) Next(values []driver.Value) error {
	if r.fault == "read_error" {
		return errSelectedGrantHistoryRead
	}
	columns := make(map[string]int)
	for i, column := range r.Columns() {
		columns[column] = i
	}
	for {
		if err := r.Rows.Next(values); err != nil {
			return err
		}
		r.position++
		if r.closed {
			generation := values[columns["generation"]].(int64)
			if r.fault == "missing_first" && generation == 1 || r.fault == "missing_last" && generation == 2 {
				continue
			}
			if generation == 1 {
				switch r.fault {
				case "running":
					values[columns["state"]] = "running"
				case "missing_terminal":
					values[columns["terminal_at"]] = nil
				case "binding_frame":
					values[columns["binding_id"]] = uuid.NewString()
				case "source_frame":
					values[columns["source_run_id"]] = uuid.NewString()
				case "grant_frame":
					values[columns["execution_owner"]] = "different-closed-owner"
				case "preparation":
					values[columns["preparation_fingerprint"]] = "sha256:" + strings.Repeat("e", 64)
				case "cut_frame":
					values[columns["fork_revision"]] = values[columns["fork_revision"]].(int64) + 1
				}
			}
			return nil
		}
		switch r.fault {
		case "last_copy_payload":
			if r.position == 2 {
				values[columns["result_payload"]] = []byte(`{"value":"changed-last-copy"}`)
			}
		case "copy_payload":
			values[columns["result_payload"]] = []byte(`{"value":"changed-copy"}`)
		case "copy_clock":
			values[columns["updated_at"]] = time.Now().UTC().Add(time.Hour)
		case "journal_read_error":
			return errSelectedGrantHistoryRead
		case "foreign_run":
			values[columns["run_id"]] = uuid.NewString()
		case "pending":
			values[columns["status"]] = "started"
		case "copied_null":
			values[columns["execution_binding"]] = nil
		case "missing_completion":
			values[columns["completed_at"]] = nil
		case "prepared_reference", "retired_reference", "missing_version", "unknown_grant":
			var raw []byte
			switch value := values[columns["execution_binding"]].(type) {
			case string:
				raw = []byte(value)
			case []byte:
				raw = value
			}
			var binding map[string]any
			if err := canonicaljson.DecodeInto(raw, &binding); err != nil {
				return err
			}
			switch r.fault {
			case "prepared_reference":
				binding["state_version"] = 1
			case "retired_reference":
				binding["state_version"] = 4
			case "missing_version":
				binding["state_version"] = 99
			case "unknown_grant":
				binding["grant_id"] = uuid.NewString()
			}
			raw, err := canonicaljson.Bytes(binding)
			if err != nil {
				return err
			}
			values[columns["execution_binding"]] = raw
		}
		return nil
	}
}

func completeSelectedHistoryJournal(t *testing.T, ctx context.Context, original runtimeownership.GenerationGrant, grant runtimeownership.GrantEvidence, runID, sourceID, status string) pipeline.ActivityAttemptRecord {
	t.Helper()
	ctx, err := runtimeownership.WithRuntimeMutationAuthority(ctx, original)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := runtimeownership.CaptureActivityJournal(ctx, original, grant)
	if err != nil {
		t.Fatal(err)
	}
	record, inserted, err := journal.StartActivityAttempt(ctx, pipeline.ActivityAttemptRecord{
		RequestEventID: uuid.NewString(), RunID: runID, SourceEventID: sourceID, ActivityID: uuid.NewString(), Tool: "selected_history.fixture",
		Attempt: 1, SuccessEvent: "fixture.succeeded", FailureEvent: "fixture.failed", InputHash: "selected-history-input",

		ExecutionMode: executionmode.Live, StartedAt: time.Now().UTC(),
	})
	if err != nil || !inserted {
		t.Fatalf("start original installed selected journal: inserted=%t err=%v", inserted, err)
	}
	record.Status, record.ResultEventID, record.ResultEventType = status, uuid.NewString(), record.SuccessEvent
	record.ResultPayload = map[string]any{"value": "original output"}
	if status != pipeline.ActivityAttemptStatusSucceeded {
		class := failures.ClassConnectorFailure
		if status == pipeline.ActivityAttemptStatusUncertain {
			class = failures.ClassOutcomeUncertain
		}
		failure, ok := failures.EnvelopeFromError(failures.New(class, "selected_history_"+status, "activity-runtime", "execute", map[string]any{"status": status}))
		if !ok || failure.Class != class {
			t.Fatal("terminal history fixture has no exact canonical failure")
		}
		record.Failure, record.ResultEventType = &failure, record.FailureEvent
		record.ResultPayload = map[string]any{"value": "recorded " + status}
	}
	settle := journal.CompleteActivityAttempt
	if status == pipeline.ActivityAttemptStatusUncertain {
		settle = journal.MarkActivityAttemptUncertain
	}
	record, acknowledged, err := settle(ctx, record)
	if err != nil || !acknowledged {
		t.Fatalf("complete original installed selected journal: acknowledged=%t err=%v", acknowledged, err)
	}
	if record.Status != status || record.CompletedAt == nil {
		t.Fatalf("original terminal journal lost its physical disposition: %+v want=%s", record, status)
	}
	return record
}

// Reach: all three physical journal outcomes retain their original claim and
// failure under each of two retired generations, not just successful results.
func proveClosedSelectedInstalledJournalCohort(t *testing.T, fixture completionSettlementFixture, selected selectedCompletionFixture, firstGrant runtimeownership.GrantEvidence, firstJournals []pipeline.ActivityAttemptRecord, control *activityGrantHistoryConnector) {
	t.Helper()
	ctx := originalStoreFixtureContext(t, testAuthorActivityContext(), selected.store)
	issued, err := selected.store.IssueRunForkSelectedContractRuntimeExecution(ctx, selected.request)
	if err != nil {
		t.Fatal(err)
	}
	if issued.Generation != 2 {
		t.Fatalf("native second generation = %d, want 2", issued.Generation)
	}
	authority, err := selected.store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "second-history-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	grantBinding := *firstGrant.SelectedFork
	grantBinding.ExecutionID, grantBinding.ExecutionGeneration = issued.ExecutionID, issued.Generation
	grantBinding.ExecutionOwner, grantBinding.FenceGeneration = authority.ExecutionOwner, authority.FenceGeneration
	grant, err := selected.process.IssueSelectedForkGenerationGrant(ctx, runtimeownership.SelectedForkGrantRequest{
		RuntimeInstanceID: firstGrant.RuntimeInstanceID, BundleHash: firstGrant.BundleHash, Binding: grantBinding,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	admitted, err := grant.AdmitExecution(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[string]pipeline.ActivityAttemptRecord)
	for _, record := range firstJournals {
		want[record.RequestEventID] = record
	}
	for _, status := range []string{pipeline.ActivityAttemptStatusSucceeded, pipeline.ActivityAttemptStatusFailed, pipeline.ActivityAttemptStatusUncertain} {
		record := completeSelectedHistoryJournal(t, ctx, grant, admitted, selected.forkRun, selected.eventID, status)
		want[record.RequestEventID] = record
	}
	read := func(readCtx context.Context) (runforkpersistence.ClosedSelectedInstalledJournalCohort, error) {
		var captured runforkpersistence.ClosedSelectedInstalledJournalCohort
		err := runUnrevisionedEventFixtureTransactionForTest(ctx, fixture.store, func(txctx context.Context, tx *sql.Tx) error {
			if err := readCtx.Err(); err != nil {
				return err
			}
			var err error
			captured, err = runforkpersistence.ReadClosedSelectedInstalledJournalCohortTx(txctx, tx, selected.forkRun, fixture.sqlite, semanticview.OriginalLoopCarriage{})
			return err
		})
		if err != nil {
			return runforkpersistence.ClosedSelectedInstalledJournalCohort{}, err
		}
		return captured, nil
	}
	if got, err := read(ctx); err == nil || len(got.Generations) != 0 || !strings.Contains(err.Error(), "not closed") {
		t.Fatalf("running second generation became a closed cohort: %+v err=%v", got, err)
	}
	if err := selected.store.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err != nil {
		t.Fatal(err)
	}
	if err := grant.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if committed, err := selected.store.CloseRunForkSelectedContractRuntimeExecution(ctx, issued.ExecutionID); err != nil || !committed {
		t.Fatalf("second original execution closure: committed=%t err=%v", committed, err)
	}
	before := historicalContextDatabaseRows(t, fixture.db, !fixture.sqlite)
	for _, scenario := range []string{"complete", "missing_first", "missing_last", "running", "missing_terminal", "binding_frame", "source_frame", "grant_frame", "preparation", "cut_frame",
		"foreign_run", "pending", "copied_null", "missing_completion", "prepared_reference", "retired_reference", "missing_version", "unknown_grant",
		"query_error", "read_error", "close_error", "journal_read_error", "cancelled"} {
		t.Run("closed_installed_cohort/"+scenario, func(t *testing.T) {
			control.installedFault = scenario
			defer func() { control.installedFault = "" }()
			readCtx := ctx
			if scenario == "cancelled" {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				readCtx = cancelled
			}
			got, err := read(readCtx)
			if scenario != "complete" {
				if err == nil || got.Generations != nil || got.Journals != nil || got.CopiedJournals != nil {
					t.Fatalf("incomplete cohort returned evidence: %+v err=%v", got, err)
				}
				stages := map[string]string{
					"missing_first": "generation census", "missing_last": "generation census", "running": "not closed", "missing_terminal": "physical clock",
					"binding_frame": "retired grant history differs", "source_frame": "preparation frame", "grant_frame": "retired grant history differs",
					"preparation": "preparation frame", "cut_frame": "preparation frame", "foreign_run": "foreign or repeated request", "pending": "not terminal",
					"copied_null": "source-lineage membership owner", "missing_completion": "terminal physical clocks",
					"prepared_reference": "exact admitted historical grant version", "retired_reference": "exact admitted historical grant version",
					"missing_version": "exact admitted historical grant version", "unknown_grant": "exact admitted historical grant version",
				}
				if stage, found := stages[scenario]; found && !strings.Contains(err.Error(), stage) {
					t.Fatalf("cohort refused before the intended evidence comparison: want=%s err=%v", stage, err)
				}
				if scenario == "cancelled" && !errors.Is(err, context.Canceled) ||
					(scenario == "query_error" || scenario == "read_error" || scenario == "journal_read_error") && !errors.Is(err, errSelectedGrantHistoryRead) ||
					scenario == "close_error" && !errors.Is(err, errSelectedGrantHistoryClose) {
					t.Fatalf("cohort lost original read cause: %v", err)
				}
				return
			}
			if err != nil || len(got.Generations) != 2 || len(got.Journals) != 6 || len(want) != 6 || len(got.CopiedJournals) != 0 {
				t.Fatalf("all generations and actual installed journals: %+v err=%v", got, err)
			}
			for i, fact := range got.Generations {
				if fact.Execution.Generation != uint64(i+1) || len(fact.Grants) != 4 || fact.Execution.State != "closed" || fact.TerminalAt.IsZero() {
					t.Fatalf("cohort omitted physical generation or grant versions: %+v", fact)
				}
			}
			for _, fact := range got.Journals {
				if expected, found := want[fact.Record.RequestEventID]; !found || !reflect.DeepEqual(fact.Record, expected) || fact.GrantFactID == "" {
					t.Fatalf("installed journal changed or lost its exact admitted version: %+v want=%+v", fact, expected)
				}
				matches := 0
				for _, generation := range got.Generations {
					for _, grant := range generation.Grants {
						if grant.Evidence.GrantID == fact.Record.ExecutionBinding.GrantID && grant.Evidence.StateVersion == fact.Record.ExecutionBinding.StateVersion {
							if grant.FactID != fact.GrantFactID || grant.Evidence.State != runtimeownership.GrantAdmitted {
								t.Fatal("journal membership substituted the physical admitted grant fact")
							}
							matches++
						}
					}
				}
				if matches != 1 {
					t.Fatalf("journal membership matched %d historical versions, want exactly one", matches)
				}
			}
		})
	}
	historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, fixture.db, !fixture.sqlite))
}
