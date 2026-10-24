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
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	runtimeownership "github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/internal/backend/activityjournal"
	"github.com/google/uuid"
)

var errSelectedGrantHistoryRead = errors.New("selected grant history read unavailable")
var errSelectedGrantHistoryClose = errors.New("selected grant history close unavailable")

type selectedGrantCohortRows struct {
	driver.Rows
	fault   string
	firstID driver.Value
}

func (r *selectedGrantCohortRows) Close() error {
	err := r.Rows.Close()
	if r.fault == "close_error" {
		return errors.Join(err, errSelectedGrantHistoryClose)
	}
	return err
}

func (r *selectedGrantCohortRows) Next(values []driver.Value) error {
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
		version := values[columns["state_version"]].(int64)
		if version == 1 {
			r.firstID = values[columns["fact_id"]]
		}
		if r.fault == "first_missing" && version == 1 || r.fault == "middle_missing" && version == 2 || r.fault == "retired_missing" && version == 4 {
			continue
		}
		if version != 2 {
			return nil
		}
		if r.fault == "duplicate_fact" {
			values[columns["fact_id"]] = r.firstID
		}
		if r.fault == "zero_fact" {
			values[columns["fact_id"]] = uuid.Nil.String()
		}
		if r.fault == "missing_clock" {
			values[columns["created_at"]] = time.Time{}
		}
		if r.fault == "projection" {
			values[columns["bundle_hash"]] = "bundle-v2:sha256:" + strings.Repeat("d", 64)
		}
		if r.fault == "snapshot_only_hidden" {
			values[columns["source_set_revision"]] = "contradictory-normal-projection"
			for _, column := range []string{"selected_binding_id", "selected_fork_run_id", "selected_execution_id"} {
				values[columns[column]] = nil
			}
		}
		if r.fault == "boot" || r.fault == "probe" || r.fault == "early_retired" || r.fault == "duplicate_version" {
			var raw []byte
			switch v := values[columns["snapshot"]].(type) {
			case []byte:
				raw = v
			case string:
				raw = []byte(v)
			}
			var grant runtimeownership.GrantEvidence
			if err := canonicaljson.DecodeInto(raw, &grant); err != nil {
				return err
			}
			switch r.fault {
			case "boot":
				grant.ProcessBootID = uuid.NewString()
			case "probe":
				grant.ProbeSurfaceIDs = []string{uuid.NewString()}
			case "early_retired":
				grant.State = runtimeownership.GrantRetired
				values[columns["state"]] = string(grant.State)
			case "duplicate_version":
				grant.StateVersion = 1
				values[columns["state_version"]] = int64(1)
			}
			raw, err := canonicaljson.Bytes(grant)
			if err != nil {
				return err
			}
			values[columns["snapshot"]] = raw
		}
		return nil
	}
}

func proveRetiredSelectedGrantHistory(t *testing.T, fixture completionSettlementFixture, process runtimeownership.ProcessCapability, control *activityGrantHistoryConnector) {
	t.Helper()
	ctx := testAuthorActivityContext()
	ctx, err := runtimeownership.WithProcessMutationAuthority(ctx, process)
	if err != nil {
		t.Fatal(err)
	}
	store, ok := fixture.store.(selectedCompletionAuthorityStore)
	if !ok {
		t.Fatal("grant history fixture has no original selected execution owner")
	}
	selected := newSelectedCompletionFixtureWithProcess(t, store, fixture.db, fixture.sqlite, process)
	selected.request.ContainerPlanFingerprint = "sha256:" + strings.Repeat("1", 64)
	selected.request.ActorCensusFingerprint = "sha256:" + strings.Repeat("2", 64)
	selected.request.EffectiveConfigFingerprint = "sha256:" + strings.Repeat("3", 64)
	issued, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, selected.request)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "grant-history-owner", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	coordinates, err := process.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	bindings, ok := fixture.store.(interface {
		RequireRunForkSelectedContractBinding(context.Context, string) (runfork.RunForkSelectedContractBinding, error)
	})
	if !ok {
		t.Fatal("grant history fixture has no original selected binding owner")
	}
	binding, err := bindings.RequireRunForkSelectedContractBinding(ctx, selected.forkRun)
	if err != nil {
		t.Fatal(err)
	}
	grant, err := process.IssueSelectedForkGenerationGrant(ctx, runtimeownership.SelectedForkGrantRequest{
		RuntimeInstanceID: coordinates.RuntimeInstanceID, BundleHash: selected.request.DeclarationPlan.BundleHash,
		Binding: runtimeownership.SelectedForkGrantBinding{
			BindingID: binding.BindingID, ForkRunID: selected.forkRun, ExecutionID: issued.ExecutionID,
			ExecutionGeneration: issued.Generation, FenceGeneration: authority.FenceGeneration, ExecutionOwner: authority.ExecutionOwner,
			AdmissionFingerprint: issued.AdmissionFingerprint, ContainerPlanFingerprint: issued.ContainerPlanFingerprint,
			ActorCensusFingerprint: issued.ActorCensusFingerprint, EffectiveConfigFingerprint: issued.EffectiveConfigFingerprint,
			DeclarationPlanFingerprint: issued.DeclarationPlanFingerprint, PreparationFingerprint: issued.PreparationFingerprint,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var expected []runtimeownership.GrantEvidence
	first, err := grant.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	expected = append(expected, first)
	settled, err := grant.MarkProbesSettled(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	expected = append(expected, settled)
	admitted, err := grant.AdmitExecution(ctx)
	if err != nil {
		t.Fatal(err)
	}
	expected = append(expected, admitted)
	var firstJournals []pipeline.ActivityAttemptRecord
	for _, status := range []string{pipeline.ActivityAttemptStatusSucceeded, pipeline.ActivityAttemptStatusFailed, pipeline.ActivityAttemptStatusUncertain} {
		firstJournals = append(firstJournals, completeSelectedHistoryJournal(t, ctx, grant, admitted, selected.forkRun, selected.eventID, status))
	}
	if err := store.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err != nil {
		t.Fatal(err)
	}
	if err := grant.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if committed, err := store.CloseRunForkSelectedContractRuntimeExecution(ctx, issued.ExecutionID); err != nil || !committed {
		t.Fatalf("original selected execution did not close: committed=%t err=%v", committed, err)
	}
	tx, err := fixture.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	retired, err := activityjournal.ReadActivityExecutionGrantTx(ctx, tx, admitted.GrantID, admitted.StateVersion+1)
	if err := errors.Join(err, tx.Rollback()); err != nil {
		t.Fatal(err)
	}
	if retired.State != runtimeownership.GrantRetired {
		t.Fatal("original retirement did not record a retired historical version")
	}
	expected = append(expected, retired)
	before, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"complete", "snapshot_only", "first_missing", "middle_missing", "retired_missing", "duplicate_version", "duplicate_fact", "zero_fact", "missing_clock", "projection", "boot", "probe", "early_retired", "snapshot_only_hidden", "query_error", "read_error", "close_error", "cancelled", "noncanonical", "absent"} {
		t.Run("selected_retired_history/"+scenario, func(t *testing.T) {
			readCtx, executionID := ctx, issued.ExecutionID
			control.cohortFault = scenario
			defer func() { control.cohortFault = "" }()
			switch scenario {
			case "cancelled":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				readCtx = cancelled
			case "noncanonical":
				executionID = " " + executionID
			case "absent":
				executionID = uuid.NewString()
			}
			tx, err := fixture.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			reads := control.cohortReads
			facts, readErr := activityjournal.ReadRetiredSelectedGrantHistoryTx(readCtx, tx, executionID, fixture.sqlite)
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			wantReads := 1
			if scenario == "cancelled" || scenario == "noncanonical" {
				wantReads = 0
			}
			if control.cohortReads-reads != wantReads {
				t.Fatalf("history queries=%d want %d", control.cohortReads-reads, wantReads)
			}
			if scenario == "absent" {
				if readErr != nil || len(facts) != 0 {
					t.Fatalf("foreign execution acquired selected grant history: %+v err=%v", facts, readErr)
				}
				return
			}
			if scenario != "complete" && scenario != "snapshot_only" {
				if readErr == nil || facts != nil {
					t.Fatalf("incomplete or inconsistent selected history was returned: %+v err=%v", facts, readErr)
				}
				stages := map[string]string{
					"first_missing":        "lacks its original prepared version",
					"middle_missing":       "missing, repeated or foreign version",
					"retired_missing":      "not retired",
					"duplicate_version":    "missing, repeated or foreign version",
					"duplicate_fact":       "physical fact coordinates",
					"zero_fact":            "physical fact coordinates",
					"missing_clock":        "physical fact coordinates",
					"projection":           "snapshot differs from its exact durable projections",
					"boot":                 "changed its captured ownership",
					"probe":                "changed its captured ownership",
					"early_retired":        "contradictory recorded states",
					"snapshot_only_hidden": "selected activity grant history differs from its exact execution projections",
				}
				if stage, found := stages[scenario]; found && !strings.Contains(readErr.Error(), stage) {
					t.Fatalf("selected history refused before its intended consistency check: want=%s err=%v", stage, readErr)
				}
				if scenario == "cancelled" && !errors.Is(readErr, context.Canceled) ||
					(scenario == "query_error" || scenario == "read_error") && !errors.Is(readErr, errSelectedGrantHistoryRead) ||
					scenario == "close_error" && !errors.Is(readErr, errSelectedGrantHistoryClose) {
					t.Fatalf("native history cause was lost: %v", readErr)
				}
				return
			}
			if readErr != nil || len(facts) != len(expected) {
				t.Fatalf("whole selected history: count=%d want=%d err=%v", len(facts), len(expected), readErr)
			}
			for i, fact := range facts {
				if fact.FactID == "" || fact.CreatedAt.IsZero() || !reflect.DeepEqual(fact.Evidence, expected[i]) {
					t.Fatalf("history substituted a grant/version or omitted physical metadata: %+v want=%+v", fact, expected[i])
				}
				var snapshot runtimeownership.GrantEvidence
				if err := canonicaljson.DecodeInto(fact.Snapshot, &snapshot); err != nil || !reflect.DeepEqual(snapshot, expected[i]) {
					t.Fatalf("history omitted original snapshot bytes: err=%v", err)
				}
			}
		})
	}
	after, err := ReadSelectedForkApplicationStorageSnapshotForTest(ctx, fixture.store)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("complete selected history changed durable rows: %v", err)
	}
	if err := grant.ProveCurrent(ctx); err == nil {
		t.Fatal("historical grant evidence restored retired execution permission")
	}
	proveClosedSelectedInstalledJournalCohort(t, fixture, selected, admitted, firstJournals, control)
}
