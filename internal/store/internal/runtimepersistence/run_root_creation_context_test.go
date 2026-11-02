package runtimepersistence

import (
	"context"
	"errors"
	"strings"
	"testing"

	runtimecorrelation "github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/sourceartifact"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
)

func TestS1RunCreationChecksSuppliedRootContextBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			selected := selectedRootAdmissionStore(t, backend).(rootedRunStore)
			sourceA, _ := admittedRootForStorage(t, "create-a")
			sourceB, _ := admittedRootForStorage(t, "create-b")
			ctx := originalStoreFixtureContext(t, testAuthorActivityContextForBundle(sourceA.Artifact().BundleHash()), selected)
			for _, source := range []sourceartifact.RootedSource{sourceA, sourceB} {
				if _, err := selected.EnsureRootedSource(ctx, source); err != nil {
					t.Fatal(err)
				}
			}
			correct := runtimecorrelation.WithRootAdmission(ctx, sourceB.Admission())
			// The journal observation includes the allocated ordering row. Create
			// an ordinary control through its owner before observing fresh refusals.
			if _, err := selected.CreateRun(correct, rootedRunCreationRequest(sourceB)); err != nil {
				t.Fatal(err)
			}
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
			live := selected.(runlifecycle.FlowRunOwner)
			for _, cell := range []struct {
				name   string
				ctx    context.Context
				source runtimecorrelation.SourceArtifactFact
			}{
				{"absent", ctx, request.Source},
				{"cleared", runtimecorrelation.WithRootAdmission(correct, sourceartifact.RootAdmission{}), request.Source},
				{"contradictory", correct, sourceartifactfixture.Fact()},
			} {
				t.Run("live_reconcile/"+cell.name, func(t *testing.T) {
					before, err := ReadFiniteRunStartStorageCountsForTest(ctx, selected)
					if err != nil {
						t.Fatal(err)
					}
					if result, err := live.ReconcileFlowRun(cell.ctx, runlifecycle.LiveRunScope{FlowPath: "."}, cell.source); err == nil || result.RunID != "" {
						t.Fatalf("missing or contradictory caller root reconciled a live run: %+v, %v", result, err)
					}
					after, err := ReadFiniteRunStartStorageCountsForTest(ctx, selected)
					if err != nil || after != before {
						t.Fatalf("refused live reconciliation changed rows: before=%+v after=%+v err=%v", before, after, err)
					}
				})
			}
			reconciled, err := live.ReconcileFlowRun(correct, runlifecycle.LiveRunScope{FlowPath: "."}, request.Source)
			if err != nil || !reconciled.Created {
				t.Fatalf("genuine caller root could not reconcile its live run: %+v, %v", reconciled, err)
			}
			requireRunRoot(t, correct, selected, reconciled.RunID, sourceB)
			incomingArtifact, err := sourceartifact.AdmitMembers(map[string][]byte{
				"manifest.yaml": []byte("version: 1.0.0\nplatform_version: '*'\n"),
				"nodes.yaml":    []byte("incoming: {type: procedure}\n"),
			})
			if err != nil {
				t.Fatal(err)
			}
			incoming, _ := sourceartifactfixture.Rooted(t, "create-upgrade", incomingArtifact)
			incomingFact := sourceartifactfixture.FactFor(incoming.Artifact())
			incomingContext := runtimecorrelation.WithRootAdmission(runtimecorrelation.WithSourceArtifactFact(ctx, incomingFact), incoming.Admission())
			if _, err := selected.EnsureRootedSource(incomingContext, incoming); err != nil {
				t.Fatal(err)
			}
			upgraded, err := live.ReconcileFlowRun(incomingContext, runlifecycle.LiveRunScope{FlowPath: "."}, incomingFact)
			if err != nil || upgraded.Created || upgraded.RunID != reconciled.RunID || upgraded.CommittedMutation != runlifecycle.MutationApplied {
				t.Fatalf("incoming root could not revise its exact retained landing: %+v, %v", upgraded, err)
			}
			requireRunRoot(t, incomingContext, selected, reconciled.RunID, incoming)
			replayed, err := live.ReconcileFlowRun(incomingContext, runlifecycle.LiveRunScope{FlowPath: "."}, incomingFact)
			if err != nil || replayed.RunID != upgraded.RunID || replayed.Created || replayed.CommittedMutation != runlifecycle.MutationExactNoop {
				t.Fatalf("exact incoming-root replay mutated the landing: %+v, %v", replayed, err)
			}
		})
	}
}
