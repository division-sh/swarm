package runtimepersistence

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/store/internal/backend/mutationprotocol"
	"github.com/google/uuid"
)

type rootedForkRunWriter interface {
	InsertRunForkRunTx(context.Context, *mutationprotocol.Attempt, string, string, runfork.RunForkPoint, time.Time, correlation.SourceArtifactFact, sourceartifact.RootAdmission) error
}

func TestS1ForkRunCreationKeepsTargetRootAndParentHistoryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend).(rootedRunStore)
			var writer rootedForkRunWriter
			switch selected := selected.(type) {
			case *PostgresStore:
				writer = selected.runForkPostgresOwner
			case *SQLiteRuntimeStore:
				writer = selected.runForkSQLiteOwner
			default:
				t.Fatalf("unsupported native store %T", selected)
			}
			a, pathA := admittedRootForStorage(t, "fork-parent")
			b, pathB := admittedRootForStorage(t, "fork-target")
			unretained, _ := admittedRootForStorage(t, "fork-unretained")
			ctx := testAuthorActivityContextForBundle(a.Artifact().BundleHash())
			for _, source := range []sourceartifact.RootedSource{a, b} {
				if _, err := selected.EnsureRootedSource(ctx, source); err != nil {
					t.Fatal(err)
				}
			}
			parent := rootedRunCreationRequest(a)
			if _, err := selected.CreateRun(ctx, parent); err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{pathA, pathB} {
				if err := os.RemoveAll(path); err != nil {
					t.Fatal(err)
				}
			}
			beforeParent, err := ReadWorkflowJournalStorageForTest(ctx, selected, parent.RunID)
			if err != nil {
				t.Fatal(err)
			}
			point := runfork.RunForkPoint{Kind: runfork.RunForkPointDeploymentRevision, Revision: 7}
			for _, cell := range []struct {
				name      string
				ctx       context.Context
				root      sourceartifact.RootAdmission
				rollback  bool
				wantError bool
			}{
				{"exact", ctx, b.Admission(), false, false},
				{"correct-context", correlation.WithRootAdmission(ctx, b.Admission()), b.Admission(), false, false},
				{"missing", ctx, sourceartifact.RootAdmission{}, false, true},
				{"unretained", ctx, unretained.Admission(), false, true},
				{"foreign-context", correlation.WithRootAdmission(ctx, a.Admission()), b.Admission(), false, true},
				{"cleared-context", correlation.WithRootAdmission(ctx, sourceartifact.RootAdmission{}), b.Admission(), false, true},
				{"rollback", ctx, b.Admission(), true, true},
			} {
				t.Run(cell.name, func(t *testing.T) {
					runID := uuid.NewString()
					before, err := ReadFiniteRunStartStorageCountsForTest(ctx, selected)
					if err != nil {
						t.Fatal(err)
					}
					beforeJournal, err := ReadWorkflowJournalStorageForTest(ctx, selected, runID)
					if err != nil {
						t.Fatal(err)
					}
					rollback := errors.New("rollback fork run root proof")
					err = runSelectedFixtureMutation(cell.ctx, selected, "rooted fork lifecycle creation", func(txctx context.Context, attempt *mutationprotocol.Attempt) error {
						if err := writer.InsertRunForkRunTx(txctx, attempt, runID, parent.RunID, point, time.Now(), parent.Source, cell.root); err != nil {
							return err
						}
						if cell.rollback {
							return rollback
						}
						return nil
					})
					if cell.wantError {
						if err == nil || cell.rollback && !errors.Is(err, rollback) {
							t.Fatalf("fork root refusal/rollback lost: %v", err)
						}
						after, err := ReadFiniteRunStartStorageCountsForTest(ctx, selected)
						if err != nil || after != before {
							t.Fatalf("refused fork changed durable rows: before=%+v after=%+v err=%v", before, after, err)
						}
						afterJournal, err := ReadWorkflowJournalStorageForTest(ctx, selected, runID)
						if err != nil || afterJournal != beforeJournal {
							t.Fatalf("refused fork changed journal or revisions: before=%+v after=%+v err=%v", beforeJournal, afterJournal, err)
						}
						if _, err := selected.LoadRunRootedSource(ctx, runID); !errors.Is(err, runlifecycle.ErrRunNotFound) {
							t.Fatalf("refused fork left rooted run evidence: %v", err)
						}
						return
					}
					if err != nil {
						t.Fatal(err)
					}
					requireRunRoot(t, ctx, selected, runID, b)
					snapshot, err := ReadRuntimeLogRunSnapshotForTest(ctx, selected, runID)
					wantOrigin, originErr := point.MaterializationRunOrigin(parent.RunID)
					if err != nil || originErr != nil || snapshot.State != runlifecycle.StatePaused || !snapshot.Origin.Equal(wantOrigin) {
						t.Fatalf("fork lifecycle changed source lineage or status: %+v err=%v origin=%v", snapshot, err, originErr)
					}
				})
			}
			requireRunRoot(t, ctx, selected, parent.RunID, a)
			afterParent, err := ReadWorkflowJournalStorageForTest(ctx, selected, parent.RunID)
			if err != nil || afterParent.StoryCount != beforeParent.StoryCount+2 || afterParent.StoryHead != beforeParent.StoryHead+2 ||
				afterParent.RunForkRevisions != beforeParent.RunForkRevisions || afterParent.Attempts != beforeParent.Attempts || afterParent.MockAttemptStories != beforeParent.MockAttemptStories {
				t.Fatalf("fork target changed parent history: before=%+v after=%+v err=%v", beforeParent, afterParent, err)
			}
		})
	}
}
