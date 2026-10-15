package runtimepersistence

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/llm/selection"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/google/uuid"
)

func TestSelectedRecoveryCurrentPredecessorIsNotAbandonedBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, state := range []string{"prepared", "running", "quiesced"} {
			t.Run(backend+"/"+state, func(t *testing.T) {
				store, db, sqlite := selectedForkDiscardTestStore(t, backend)
				fixture := newSelectedCompletionFixture(t, store, db, sqlite)
				ctx := testAuthorActivityContext()
				issued, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, fixture.request)
				if err != nil {
					t.Fatal(err)
				}
				if state != "prepared" {
					authority, err := store.ClaimRunForkSelectedContractRuntimeExecution(ctx, issued, "current-predecessor", time.Minute)
					if err != nil {
						t.Fatal(err)
					}
					if state == "quiesced" {
						if err := store.QuiesceRunForkSelectedContractRuntimeExecution(ctx, authority); err != nil {
							t.Fatal(err)
						}
					}
				}
				process, err := fixture.process.Evidence()
				if err != nil {
					t.Fatal(err)
				}
				recovery := store.(interface {
					ListSelectedForkRecoveryEntries(context.Context) ([]runfork.SelectedForkRecoveryEntry, error)
					InspectSelectedForkRecovery(context.Context, runfork.SelectedForkRecoveryEntry) (runfork.SelectedForkRecoveryInspection, error)
					RecoverSelectedFork(context.Context, runcontrol.SelectedForkRecoveryRequest) (runfork.SelectedForkRecoveryResult, error)
				})
				entries, err := recovery.ListSelectedForkRecoveryEntries(ctx)
				if err != nil || len(entries) != 1 {
					t.Fatalf("current predecessor inventory: %+v %v", entries, err)
				}
				request := runcontrol.SelectedForkRecoveryRequest{Entry: entries[0], Process: process.LeaseStamp(), Effects: liveExternalEffectRecoveryRequest(time.Now().UTC())}
				before := snapshotForkHistoricalExecutionTables(t, db, !sqlite)
				inspection, err := recovery.InspectSelectedForkRecovery(ctx, request.Entry)
				if err != nil || inspection.ExecutionState != state || inspection.Plan.ExecutionID != issued.ExecutionID {
					t.Fatalf("current predecessor state differs: %+v %v", inspection, err)
				}
				result, err := recovery.RecoverSelectedFork(ctx, request)
				if err != nil || result.Disposition != runfork.SelectedForkRecoveryCurrent || result.ExecutionID != issued.ExecutionID ||
					result.Continuation != nil || len(result.CanceledTurns) != 0 || len(result.PendingCancellations) != 0 || result.Effects.PrelaunchTerminal != 0 || result.Effects.OutcomeUncertain != 0 {
					t.Fatalf("current predecessor gained recovery authority: %+v %v", result, err)
				}
				for _, field := range []string{"boot", "lease", "generation"} {
					bad := request
					switch field {
					case "boot":
						bad.Process.BootID = uuid.NewString()
					case "lease":
						bad.Process.LeaseID = uuid.NewString()
					case "generation":
						bad.Process.Generation++
					}
					if _, err := recovery.RecoverSelectedFork(ctx, bad); err == nil {
						t.Fatalf("crossed %s recovered current %s predecessor", field, state)
					}
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, !sqlite)) {
					t.Fatal("current predecessor inspection/refusal changed durable evidence")
				}
			})
		}
	}
}

func TestSelectedRecoveryUsesNativeAdmittedPreparationWithoutProcessJournalBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, nonProbed := range []bool{false, true} {
			t.Run(backend+map[bool]string{true: "/non_probed", false: "/actor_free"}[nonProbed], func(t *testing.T) {
				store, db, sqlite := selectedForkDiscardTestStore(t, backend)
				fixture := newSelectedCompletionFixture(t, store, db, sqlite)
				ctx := testAuthorActivityContext()
				request := fixture.request
				if nonProbed {
					name, err := agentidentity.DeclaredName("native-agent", "selected-target")
					if err != nil {
						t.Fatal(err)
					}
					plan, err := agentidentity.NewPlan(name, agentidentity.RootRoute())
					if err != nil {
						t.Fatal(err)
					}
					request.DeclarationPlan, err = agenttopology.NewSelectedDeclarationPlan(request.DeclarationPlan.BundleHash, []agenttopology.DesiredAgent{{
						Identity: plan, Source: agenttopology.SourceCoordinate{BundleHash: request.DeclarationPlan.BundleHash}, ConfigRevision: strings.Repeat("f", 64),
					}})
					if err != nil {
						t.Fatal(err)
					}
					request.Preparation.DeclarationPlanFingerprint = request.DeclarationPlan.Revision
					request.Preparation.Actors = []runfork.SelectedForkPreparedActor{{
						Plan: plan, ConfigurationRevision: strings.Repeat("f", 64), Backend: selection.BackendAnthropic, Mode: executionmode.Live,
					}}
					if request.Preparation.Actors[0].RequiresProbe() {
						t.Fatal("preservation control unexpectedly requires a receipt")
					}
				}
				for _, field := range []string{"process", "owner", "boot", "future_generation"} {
					bad := request
					switch field {
					case "process":
						bad.Preparation.Coordinates.ProcessAuthorityID = uuid.NewString()
					case "owner":
						bad.Preparation.Coordinates.ProcessOwnerID = "crossed-owner"
					case "boot":
						bad.Preparation.Coordinates.ProcessBootID = uuid.NewString()
					case "future_generation":
						bad.Preparation.ProcessGeneration++
					}
					if _, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, bad); !errors.Is(err, startupownership.ErrProcessNotCurrent) {
						t.Fatalf("%s bypassed native issuance admission: %v", field, err)
					}
					var count int
					if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_fork_selected_contract_runtime_executions`).Scan(&count); err != nil || count != 0 {
						t.Fatalf("rejected %s wrote retained execution: count=%d err=%v", field, count, err)
					}
				}
				issued, err := store.IssueRunForkSelectedContractRuntimeExecution(ctx, request)
				if err != nil {
					t.Fatal(err)
				}
				if err := fixture.process.Release(ctx); err != nil {
					t.Fatal(err)
				}
				// The discarded journal is not a compatibility source. The native
				// selected binding is the surviving historical owner in R5.3.
				query := `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN ('runtime_startup_authority_facts','runtime_startup_authority_repairs')`
				if !sqlite {
					query = `SELECT COUNT(*) FROM pg_tables WHERE schemaname='public' AND tablename IN ('runtime_startup_authority_facts','runtime_startup_authority_repairs')`
				}
				var journals int
				if err := db.QueryRowContext(ctx, query).Scan(&journals); err != nil || journals != 0 {
					t.Fatalf("fresh selected provenance still depends on process history: tables=%d err=%v", journals, err)
				}
				reader := store.(interface {
					ListSelectedForkRecoveryEntries(context.Context) ([]runfork.SelectedForkRecoveryEntry, error)
					InspectSelectedForkRecovery(context.Context, runfork.SelectedForkRecoveryEntry) (runfork.SelectedForkRecoveryInspection, error)
				})
				entries, err := reader.ListSelectedForkRecoveryEntries(ctx)
				if err != nil || len(entries) != 1 {
					t.Fatalf("retained native entry: %+v %v", entries, err)
				}
				before := snapshotForkHistoricalExecutionTables(t, db, !sqlite)
				inspection, err := reader.InspectSelectedForkRecovery(ctx, entries[0])
				if err != nil || inspection.Plan.ExecutionID != issued.ExecutionID || inspection.ExecutionState != "prepared" || inspection.Plan.Disposition != runfork.SelectedForkRecoveryControlOnly {
					t.Fatalf("retained preparation inspection required a journal: %+v %v", inspection, err)
				}
				crossed := entries[0]
				crossed.Binding.BindingID = uuid.NewString()
				if _, err := reader.InspectSelectedForkRecovery(ctx, crossed); err == nil || !strings.Contains(err.Error(), "binding changed") {
					t.Fatalf("caller binding replaced admitted native binding: %v", err)
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, !sqlite)) {
					t.Fatal("read-only recovery mutated or acquired process/receipt evidence")
				}
				var raw string
				if err := db.QueryRowContext(ctx, `SELECT preparation_binding FROM run_fork_selected_contract_runtime_executions WHERE execution_id=$1`, issued.ExecutionID).Scan(&raw); err != nil {
					t.Fatal(err)
				}
				var corrupt runfork.SelectedForkPreparationBinding
				if err := json.Unmarshal([]byte(raw), &corrupt); err != nil {
					t.Fatal(err)
				}
				corrupt.Coordinates.ProcessBootID = uuid.NewString()
				changed, err := json.Marshal(corrupt)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := db.ExecContext(ctx, `UPDATE run_fork_selected_contract_runtime_executions SET preparation_binding=$1 WHERE execution_id=$2`, string(changed), issued.ExecutionID); err != nil {
					t.Fatal(err)
				}
				before = snapshotForkHistoricalExecutionTables(t, db, !sqlite)
				if _, err := reader.InspectSelectedForkRecovery(ctx, entries[0]); err == nil || !strings.Contains(err.Error(), "preparation conflicts with binding") {
					t.Fatalf("retained fingerprint corruption admitted: %v", err)
				}
				if !reflect.DeepEqual(before, snapshotForkHistoricalExecutionTables(t, db, !sqlite)) {
					t.Fatal("refused corrupt observation changed native state")
				}
			})
		}
	}
}
