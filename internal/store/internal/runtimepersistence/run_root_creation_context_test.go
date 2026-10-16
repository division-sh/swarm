package runtimepersistence

import (
	"context"
	"errors"
	"strings"
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
)

func TestS1RunCreationChecksSuppliedRootContextBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend).(rootedRunStore)
			sourceA, _ := admittedRootForStorage(t, "create-a")
			sourceB, _ := admittedRootForStorage(t, "create-b")
			ctx := testAuthorActivityContextForBundle(sourceA.Artifact().BundleHash())
			for _, source := range []sourceartifact.RootedSource{sourceA, sourceB} {
				if _, err := selected.EnsureRootedSource(ctx, source); err != nil {
					t.Fatal(err)
				}
			}
			correct := runtimecorrelation.WithRootAdmission(ctx, sourceB.Admission())
			for _, replay := range []bool{false, true} {
				mode := "fresh"
				if replay {
					mode = "replay"
				}
				for _, cell := range []struct {
					name string
					ctx  context.Context
				}{
					{"foreign", runtimecorrelation.WithRootAdmission(ctx, sourceA.Admission())},
					{"cleared", runtimecorrelation.WithRootAdmission(correct, sourceartifact.RootAdmission{})},
				} {
					t.Run(mode+"/"+cell.name, func(t *testing.T) {
						request := rootedRunCreationRequest(sourceB)
						if replay {
							if _, err := selected.CreateRun(correct, request); err != nil {
								t.Fatal(err)
							}
						}
						beforeRows, err := ReadFiniteRunStartStorageCountsForTest(ctx, selected)
						if err != nil {
							t.Fatal(err)
						}
						beforeJournal, err := ReadWorkflowJournalStorageForTest(ctx, selected, request.RunID)
						if err != nil {
							t.Fatal(err)
						}
						disposition, err := selected.CreateRun(cell.ctx, request)
						if err == nil || disposition != "" {
							t.Errorf("contradictory or cleared root obtained creation/replay authority: %s, %v", disposition, err)
						} else if cell.name == "cleared" && !errors.Is(err, sourceartifact.ErrRootAdmissionMissing) || cell.name == "foreign" && !strings.Contains(err.Error(), "rooted execution context does not match") {
							t.Errorf("root refusal lost its canonical cause: %v", err)
						}
						afterRows, err := ReadFiniteRunStartStorageCountsForTest(ctx, selected)
						if err != nil || afterRows != beforeRows {
							t.Errorf("refused creation/replay changed durable rows: before=%+v after=%+v err=%v", beforeRows, afterRows, err)
						}
						afterJournal, err := ReadWorkflowJournalStorageForTest(ctx, selected, request.RunID)
						if err != nil || afterJournal != beforeJournal {
							t.Errorf("refused creation/replay changed story or revisions: before=%+v after=%+v err=%v", beforeJournal, afterJournal, err)
						}
						if replay {
							requireRunRoot(t, ctx, selected, request.RunID, sourceB)
						} else if _, err := selected.LoadRunRootedSource(ctx, request.RunID); !errors.Is(err, runlifecycle.ErrRunNotFound) {
							t.Errorf("refused creation left a bound run: %v", err)
						}
					})
				}
			}
			for _, cell := range []struct {
				name string
				ctx  context.Context
			}{{"absent", ctx}, {"correct", correct}} {
				t.Run(cell.name, func(t *testing.T) {
					request := rootedRunCreationRequest(sourceB)
					if disposition, err := selected.CreateRun(cell.ctx, request); err != nil || disposition != runlifecycle.MutationApplied {
						t.Fatalf("valid root context cannot create: %s, %v", disposition, err)
					}
					before, err := ReadWorkflowJournalStorageForTest(ctx, selected, request.RunID)
					if err != nil {
						t.Fatal(err)
					}
					if disposition, err := selected.CreateRun(cell.ctx, request); err != nil || disposition != runlifecycle.MutationExactNoop {
						t.Fatalf("valid root context cannot replay: %s, %v", disposition, err)
					}
					after, err := ReadWorkflowJournalStorageForTest(ctx, selected, request.RunID)
					if err != nil || after != before {
						t.Fatalf("exact replay appended a story or revision: before=%+v after=%+v err=%v", before, after, err)
					}
					requireRunRoot(t, ctx, selected, request.RunID, sourceB)
				})
			}
			request := rootedRunCreationRequest(sourceA)
			predecessor := runtimecorrelation.WithRootAdmission(ctx, sourceA.Admission())
			if _, err := selected.CreateRun(predecessor, request); err != nil {
				t.Fatal(err)
			}
			if disposition, err := selected.ReviseRunSource(predecessor, runlifecycle.SourceRevisionRequest{
				RunID: request.RunID, Source: request.Source, RootAdmission: sourceB.Admission(),
			}); err != nil || disposition != runlifecycle.MutationApplied {
				t.Fatalf("explicit revision refused its legitimate predecessor context: %s, %v", disposition, err)
			}
			requireRunRoot(t, ctx, selected, request.RunID, sourceB)
		})
	}
}
