package runtimepersistence

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision"
	"github.com/google/uuid"
)

func TestRunForkFreshInvocationsAtSameCutHaveIndependentChildrenBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			ctx := testAuthorActivityContext()
			fixture := backend.open(t)
			owner, db := fixture.store.(runForkSelectedLifecycleStore), fixture.db
			runID, eventID := uuid.NewString(), uuid.NewString()
			at := time.Now().UTC().Truncate(time.Microsecond)
			requireRunFixtureForTest(t, ctx, owner, semanticRunFixture{
				Origin: semanticScenarioSetupRunOriginForTest(), RunID: runID, StartedAt: at.Add(-time.Minute),
				BundleHash: authorActivityTestBundleHash,
			})
			tx, err := db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			seedRunForkRevisionMatrixEvent(t, ctx, tx, runID, eventID, at, backend.name == "postgres")
			effects, err := runforkrevision.ForRun(runID, runforkrevision.AllFamilies()...)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := finalizeRunForkRevisionMatrix(ctx, tx, backend.name == "postgres", effects); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			request := runfork.RunForkMaterializeRequest{SourceRunID: runID, At: eventID}
			first, err := owner.MaterializeRunFork(ctx, request)
			if err != nil {
				t.Fatalf("first invocation: %v", err)
			}
			second, err := owner.MaterializeRunFork(ctx, request)
			if err != nil {
				t.Fatalf("second fresh invocation: %v", err)
			}
			if first.ForkRunID == second.ForkRunID {
				t.Fatalf("distinct fresh invocations collapsed to the same child %s at fixed cut %s", first.ForkRunID, eventID)
			}
			if first.SourceRunID != runID || second.SourceRunID != runID || first.ForkPoint != second.ForkPoint {
				t.Fatalf("independent identity changed the fixed source cut: first=%+v second=%+v", first, second)
			}
			before := historicalContextDatabaseRows(t, db, backend.name == "postgres")
			for _, child := range []runfork.RunForkMaterialization{first, second} {
				got, err := owner.RequireMaterializedRunFork(ctx, child.ForkRunID, request)
				if err != nil || !reflect.DeepEqual(got, child) {
					t.Fatalf("read explicit child with sibling present: got=%+v want=%+v err=%v", got, child, err)
				}
			}
			for _, child := range []string{"", "not-a-uuid", uuid.Nil.String(), uuid.NewString()} {
				got, err := owner.RequireMaterializedRunFork(ctx, child, request)
				if err == nil || got.ForkRunID != "" {
					t.Fatalf("missing/invalid exact child %q selected a sibling: got=%+v err=%v", child, got, err)
				}
				if parsed, parseErr := uuid.Parse(child); parseErr == nil && parsed != uuid.Nil && !errors.Is(err, runlifecycle.ErrRunNotFound) {
					t.Fatalf("absent exact child lost typed refusal: %v", err)
				}
			}
			foreignSource := request
			foreignSource.SourceRunID = uuid.NewString()
			changedCut := request
			changedCut.At = uuid.NewString()
			changedStart := request
			changedStart.At = ""
			changedStart.AtStart = true
			changedContract := request
			changedContract.ContractSelection = &runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts}
			for _, hostile := range []runfork.RunForkMaterializeRequest{foreignSource, changedCut, changedStart, changedContract} {
				if got, err := owner.RequireMaterializedRunFork(ctx, first.ForkRunID, hostile); err == nil || got.ForkRunID != "" {
					t.Fatalf("crossed child validation accepted: request=%+v got=%+v err=%v", hostile, got, err)
				}
			}
			historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, db, backend.name == "postgres"))
			historicalLineageCheckpoint(t, db, runID, backend.name == "postgres", at.Add(time.Second))
			before = historicalContextDatabaseRows(t, db, backend.name == "postgres")
			originalLatest := request
			originalLatest.At = ""
			got, err := owner.RequireMaterializedRunFork(ctx, first.ForkRunID, originalLatest)
			if err != nil || got.ForkRunID != first.ForkRunID || got.ForkPoint.EventID != first.ForkPoint.EventID || got.ForkPoint.Revision != first.ForkPoint.Revision || got.ForkPoint.Input != "" {
				t.Fatalf("required child followed latest source alias: got=%+v err=%v", got, err)
			}
			historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, db, backend.name == "postgres"))
		})
	}
}

func TestSelectedForkFreshInvocationsAtSameCutHaveIndependentChildrenBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, request := forkWorkflowOwnershipRequest(t, fixture, backend.name == "postgres")
			owner := fixture.store.(forkWorkflowOwnershipStore)
			first, err := owner.MaterializeRunForkForSelectedContractExecution(ctx, request)
			if err != nil {
				t.Fatalf("first selected invocation: %v", err)
			}
			second, err := owner.MaterializeRunForkForSelectedContractExecution(ctx, request)
			if err != nil {
				t.Fatalf("second selected invocation: %v", err)
			}
			if first.ForkRunID == second.ForkRunID {
				t.Fatalf("fresh selected invocations collapsed at one fixed cut: %s", first.ForkRunID)
			}
			if first.SourceRunID != request.SourceRunID || second.SourceRunID != request.SourceRunID || first.ForkPoint != second.ForkPoint {
				t.Fatalf("fresh selected identity changed source provenance: first=%+v second=%+v", first, second)
			}
			for _, child := range []runfork.RunForkMaterialization{first, second} {
				if child.SelectedContractBinding == nil || child.SelectedContractBinding.ForkRunID != child.ForkRunID {
					t.Fatalf("selected child lost its exact binding: %+v", child)
				}
				requireForkWorkflowOwnershipPreserved(t, fixture.db, request.SourceRunID, child.ForkRunID)
			}
		})
	}
}

func TestSelectedForkLatestPreparationRetainsCapturedSelectorBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, seeded := forkWorkflowOwnershipRequest(t, fixture, backend.name == "postgres")
			request := prepareSelectedStoreMaterializationForTest(t, ctx, fixture.store, seeded.SourceRunID, "", seeded.ContractSelection)
			point := request.Preparation.ForkPoint
			if point.EventID == "" || request.At != point.Input || point.EventID == point.Input {
				t.Fatalf("fixture did not distinguish captured latest from concrete event: point=%+v at=%q", point, request.At)
			}
			owner := fixture.store.(forkWorkflowOwnershipStore)
			child, err := owner.MaterializeRunForkForSelectedContractExecution(ctx, request)
			if err != nil || child.ForkRunID == "" || child.ForkPoint != point {
				t.Fatalf("materialization changed the already captured latest selector: child=%+v point=%+v err=%v", child, point, err)
			}
			before := historicalContextDatabaseRows(t, fixture.db, backend.name == "postgres")
			hostile := request
			hostile.At = uuid.NewString()
			if got, err := owner.MaterializeRunForkForSelectedContractExecution(ctx, hostile); err == nil || got.ForkRunID != "" ||
				!strings.Contains(err.Error(), "fork selector differs from fixed revision point") {
				t.Fatalf("foreign request selector bypassed captured point: child=%+v err=%v", got, err)
			}
			historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, fixture.db, backend.name == "postgres"))
		})
	}
}
