package runtimepersistence

import (
	"context"
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
			changedContract := request
			changedContract.ContractSelection = &runfork.RunForkContractSelection{Mode: runfork.RunForkContractSelectionModeSelectedContracts}
			for _, hostile := range []runfork.RunForkMaterializeRequest{foreignSource, changedCut, changedContract} {
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
			reader := fixture.store.(interface {
				LoadForkOperationByID(context.Context, string) (runfork.ForkOperationRecord, bool, error)
			})
			seen := map[string]bool{first.ForkRunID: true, second.ForkRunID: true}
			for _, key := range []string{"independent-first", "independent-second", "", ""} {
				operation := runfork.ForkOperationRequest{
					OperationID: uuid.NewString(), Actor: "bearer:operator", IdempotencyKey: key,
					TransportHash: "sha256:same-source-request", SourceRunID: request.SourceRunID, ForkEventID: request.At,
					TargetBundleHash: request.SourceArtifactFact.BundleHash(), ContractSelection: request.ContractSelection,
				}
				attempt := request
				attempt.ForkOperation = &operation
				child, err := owner.MaterializeRunForkForSelectedContractExecution(ctx, attempt)
				if err != nil || child.ForkRunID == "" || seen[child.ForkRunID] {
					t.Fatalf("independent operation reused a child: key=%q child=%+v err=%v", key, child, err)
				}
				seen[child.ForkRunID] = true
				if child.SourceRunID != first.SourceRunID || child.ForkPoint != first.ForkPoint {
					t.Fatalf("operation identity changed fixed provenance: child=%+v first=%+v", child, first)
				}
				record, found, err := reader.LoadForkOperationByID(ctx, operation.OperationID)
				if err != nil || !found || record.ForkRunID != child.ForkRunID || child.SelectedContractBinding == nil ||
					record.BindingID != child.SelectedContractBinding.BindingID || record.Request.OperationID != operation.OperationID ||
					record.Request.IdempotencyKey != key || record.Status != runfork.ForkOperationMaterialized {
					t.Fatalf("operation did not durably bind its acknowledged child: record=%+v child=%+v found=%t err=%v", record, child, found, err)
				}
				requireForkWorkflowOwnershipPreserved(t, fixture.db, request.SourceRunID, child.ForkRunID)
			}
		})
	}
}

func TestSelectedForkKeyElectionRefusesLosingPreparationWithoutMutationBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, request := forkWorkflowOwnershipRequest(t, fixture, backend.name == "postgres")
			owner := fixture.store.(forkWorkflowOwnershipStore)
			operation := runfork.ForkOperationRequest{
				OperationID: uuid.NewString(), Actor: "bearer:operator", IdempotencyKey: "selected-winner",
				TransportHash: "sha256:original-request", SourceRunID: request.SourceRunID, ForkEventID: request.At,
				TargetBundleHash: request.SourceArtifactFact.BundleHash(), ContractSelection: request.ContractSelection,
			}
			request.ForkOperation = &operation
			winner, err := owner.MaterializeRunForkForSelectedContractExecution(ctx, request)
			if err != nil {
				t.Fatalf("winning selected preparation: %v", err)
			}
			before := historicalContextDatabaseRows(t, fixture.db, backend.name == "postgres")
			for _, change := range []string{"same_invocation", "late_winner", "latest_bundle_changed", "different_transport"} {
				t.Run(change, func(t *testing.T) {
					candidate := operation
					if change != "same_invocation" {
						candidate.OperationID = uuid.NewString()
					}
					if change == "latest_bundle_changed" {
						candidate.TargetBundleHash = "sha256:later-source-alias"
					}
					if change == "different_transport" {
						candidate.TransportHash = "sha256:changed-request"
					}
					attempt := request
					attempt.ForkOperation = &candidate
					got, err := owner.MaterializeRunForkForSelectedContractExecution(ctx, attempt)
					var replay *runfork.ForkOperationReplayRequiredError
					if got.ForkRunID != "" || err == nil {
						t.Fatalf("losing preparation acknowledged a child: got=%+v err=%v", got, err)
					}
					if change == "different_transport" {
						var conflict *runfork.ForkOperationKeyConflictError
						if !errors.As(err, &conflict) || errors.As(err, &replay) {
							t.Fatalf("changed request became continuation: %v", err)
						}
					} else if !errors.As(err, &replay) || replay.OperationID != operation.OperationID || replay.ForkRunID != winner.ForkRunID {
						t.Fatalf("clean losing preparation lost durable winner: replay=%+v err=%v", replay, err)
					}
					historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, fixture.db, backend.name == "postgres"))
				})
			}
		})
	}
}

func TestSelectedDeploymentForkRequiresDurableInvocationBeforeMutationBothStores(t *testing.T) {
	for _, backend := range eventRecordContractBackends() {
		t.Run(backend.name, func(t *testing.T) {
			fixture := backend.open(t)
			ctx, request := forkWorkflowOwnershipRequest(t, fixture, backend.name == "postgres")
			request.Preparation.ForkPoint = runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 4}
			before := historicalContextDatabaseRows(t, fixture.db, backend.name == "postgres")
			got, err := fixture.store.(forkWorkflowOwnershipStore).MaterializeRunForkForSelectedContractExecution(ctx, request)
			if err == nil || !strings.Contains(err.Error(), "deployment revision fork requires a durable invocation") || got.ForkRunID != "" {
				t.Fatalf("deployment revision bypassed durable invocation admission: got=%+v err=%v", got, err)
			}
			historicalContextRequireUnchanged(t, before, historicalContextDatabaseRows(t, fixture.db, backend.name == "postgres"))
		})
	}
}
