package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/bus"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/mutationlog"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	storeselected "github.com/division-sh/swarm/internal/store/selected"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/tests/support/testutil/flowactivationfixture"
	"github.com/google/uuid"
)

func TestVerifyRunPublicAdmissionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			ctx := context.Background()
			root := issue2567VerifySource(t)
			if err := os.WriteFile(filepath.Join(root, "state.yaml"), []byte("verdict: 'original'\n"), 0600); err != nil {
				t.Fatal(err)
			}
			config := filepath.Join(t.TempDir(), "swarm.yaml")
			_, bundle, err := NewSwarmWorkflowModule(RepoRoot(), root)
			if err != nil {
				t.Fatal(err)
			}
			plans, err := StateStoreSchemaPlans(bundle)
			if err != nil {
				t.Fatal(err)
			}
			schema, err := SchemaBootstrapRequest(bundle.Platform, plans.Platform)
			if err != nil {
				t.Fatal(err)
			}
			selected, configText := newVerifyCompositionBootStore(t, backend, root, schema)
			ctx = cliFixtureProcessContext(t, ctx, selected)
			writeRuntimeConfigText(t, config, configText)
			runID := "abcdefab-1234-4000-8000-abcdefabcdef"
			storetest.RequireRun(t, ctx, selected, storetest.RunFixture{RunID: runID, Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact})
			for _, withMutations := range []bool{false, true} {
				t.Run(fmt.Sprintf("mutations_%t", withMutations), func(t *testing.T) {
					if withMutations {
						fact, err := correlation.NewSourceArtifactFact(bundle.SourceArtifact.BundleHash())
						if err != nil {
							t.Fatal(err)
						}
						runtimeID := uuid.NewString()
						constructionContext := correlation.WithRuntimeInstanceID(ctx, runtimeID)
						constructionContext = correlation.WithSourceArtifactFact(constructionContext, fact)
						constructionContext = authoractivity.WithScope(constructionContext, authoractivity.BundleScope(runtimeID, fact.BundleHash()))
						constructionContext = effects.WithExecutionMode(correlation.WithRunID(constructionContext, runID), effects.ExecutionModeLive)
						at := time.Now().UTC()
						// Typed component construction supplies nonempty mutation history;
						// it is a UUID-admission witness, not the required S03 workload.
						command, err := flowactivationfixture.Command(constructionContext, pipeline.WorkflowInstance{
							InstanceID: runID, StorageRef: runID, EntityID: runID,
							WorkflowName: ".", WorkflowVersion: bundle.WorkflowVersion(), EntityType: "work",
							Mode:         bundle.RootSchema.EffectiveMode(),
							CurrentState: "waiting", StageDefined: true, CreatedAt: at, EnteredStageAt: at,
							Fields: map[string]any{"verdict": "original"},
						}, pipeline.WorkflowLifecycleMutationPlan{}, at)
						if err != nil {
							t.Fatal(err)
						}
						committed, err := selected.(bus.FlowInstanceActivationCommitOwner).CommitFlowInstanceActivation(constructionContext, command)
						if err != nil || !committed.Acknowledged || !committed.Created {
							t.Fatalf("construct mutation-bearing run: %+v %v", committed, err)
						}
						if history := storetest.ObserveEntityMutationHistory(t, constructionContext, selected, runID); len(history) == 0 {
							t.Fatal("UUID fixture has no mutations")
						}
						ctx = constructionContext
					}
					for _, missing := range []bool{false, true} {
						t.Run(fmt.Sprintf("missing_%t", missing), func(t *testing.T) {
							id := runID
							if missing {
								id = uuid.NewString()
							}
							cfg, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: root, ExplicitPath: config})
							if err != nil {
								t.Fatal(err)
							}
							request, _, err := prepareVerifySelectedStoreInspection(ctx, root, CLISourcePaths{SourceRoot: root}, cfg, cliSwarmDirOptions{}, semanticview.Wrap(bundle))
							if err != nil {
								t.Fatal(err)
							}
							inspection, err := storeselected.OpenAdmissionInspection(ctx, request)
							if err != nil {
								t.Fatal(err)
							}
							var report mutationlog.DriftReport
							_, err = inspection.Inspect(ctx, schema, func(snapshot *storeselected.AdmissionSnapshot) error {
								var err error
								report, err = snapshot.InspectRunMutationDrift(ctx, id)
								return err
							})
							err = errors.Join(err, inspection.Close())
							if missing {
								failure, ok := failures.As(verifyRunFailure(id, err))
								if !ok || failure.Failure.Detail.Code != "run_not_found" || failure.Failure.Retryable {
									t.Fatalf("missing native run lost typed refusal: %v", err)
								}
								return
							}
							want := 0
							if withMutations {
								want = 1
							}
							if err != nil || report.RunID != id || report.EntitiesChecked != want || len(report.Rows) != 0 {
								t.Fatalf("native drift read lost history/current reach: %+v %v", report, err)
							}
							stage, err := serveChecksForTest(t, ctx, root, root, config)
							if err != nil {
								t.Fatal(err)
							}
							observed := false
							for _, observation := range stage.Validation.BootReport.Observations {
								if observation.CheckID == "run_mutation_drift" {
									observed = observation.Status == "passed"
								}
							}
							if !observed {
								t.Fatal("boot's snapshot omitted the existing active-run mutation check")
							}
						})
					}
				})
			}
			if t.Failed() {
				return
			}
			history := storetest.ObserveEntityMutationHistory(t, ctx, selected, runID)
			if err := storetest.CorruptRegistryVerdict(ctx, selected, runID, runID, "hostile-drift"); err != nil {
				t.Fatal(err)
			}
			for _, state := range []runlifecycle.State{runlifecycle.StateRunning, runlifecycle.StatePaused, runlifecycle.StateCancelled} {
				t.Run("drift_"+string(state), func(t *testing.T) {
					if state != runlifecycle.StateRunning {
						if state == runlifecycle.StatePaused {
							if _, err := selected.TransitionActiveRun(ctx, runlifecycle.ActiveTransitionRequest{RunID: runID, State: state}); err != nil {
								t.Fatal(err)
							}
						} else if _, _, err := selected.MarkTerminalRun(ctx, runlifecycle.TerminalRequest{RunID: runID, State: state, EndedAt: time.Now().UTC()}); err != nil {
							t.Fatal(err)
						}
					}
					stage, err := serveChecksForTest(t, ctx, root, root, config)
					if err != nil {
						t.Fatal(err)
					}
					observed := false
					for _, observation := range stage.Validation.BootReport.Observations {
						if observation.CheckID != "run_mutation_drift" {
							continue
						}
						observed = true
						if state == runlifecycle.StateCancelled {
							if observation.Status != "passed" {
								t.Fatalf("terminal history entered active boot checks: %+v", observation)
							}
						} else if observation.Status != "failed" || !strings.Contains(observation.Reason, runID) || !strings.Contains(observation.Reason, "verdict") || !strings.Contains(observation.Reason, "hostile-drift") || !strings.Contains(observation.Reason, "original") {
							t.Fatalf("%s stage lost exact hostile drift refusal: %+v", state, observation)
						}
					}
					if !observed {
						t.Fatal("stage omitted active-run drift predicate")
					}
					if after := storetest.ObserveEntityMutationHistory(t, ctx, selected, runID); !reflect.DeepEqual(history, after) {
						t.Fatal("read-only stage changed mutation history")
					}
				})
			}
		})
	}
}

func TestVerifyRunPublicFlagAndAbsentStoreAdmission(t *testing.T) {
	for _, flag := range []string{"--run", "--portable"} {
		t.Run(flag, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			root := issue2567VerifySource(t)
			args := []string{"verify", root, flag}
			var out, errOut bytes.Buffer
			code := executeRootCommand(context.Background(), RepoRoot(), args, &out, &errOut)
			if code == 0 || !strings.Contains(errOut.String(), "unknown flag: "+flag) {
				t.Fatalf("retired flag remained accepted: code=%d stderr=%s", code, &errOut)
			}
		})
	}
}

func TestVerifyRunTypedFailuresRetainCoordinatesAndCancellation(t *testing.T) {
	run := uuid.NewString()
	for _, tc := range []struct {
		name  string
		cause error
		code  string
		class failures.Class
	}{
		{"missing_run", &runlifecycle.RunNotFoundError{RunID: run}, "run_not_found", failures.ClassSchemaInvalid},
		{"history", &mutationlog.HistoryError{RunID: run, EntityID: "entity", MutationID: "mutation", Code: "mutation_order_missing", Reason: "missing coordinate"}, "mutation_order_missing", failures.ClassSchemaInvalid},
		{"canceled", errors.Join(&runlifecycle.RunNotFoundError{RunID: run}, context.Canceled), "run_inspection_canceled", failures.ClassDependencyUnavailable},
		{"deadline", context.DeadlineExceeded, "run_inspection_timeout", failures.ClassTimeout},
		{"transport", fmt.Errorf("load run fork revision snapshot: %w", errors.New("driver transport failure")), "run_inspection_failed", failures.ClassDependencyUnavailable},
		{"wrapped_deadline", fmt.Errorf("load run fork revision snapshot: %w", context.DeadlineExceeded), "run_inspection_timeout", failures.ClassTimeout},
		{"history_and_cleanup", errors.Join(&mutationlog.HistoryError{RunID: run, Code: "invalid_mutation_order"}, errors.New("row cleanup failed")), "run_inspection_failed", failures.ClassDependencyUnavailable},
		{"missing_and_cleanup", errors.Join(&runlifecycle.RunNotFoundError{RunID: run}, errors.New("inspection close failed")), "run_inspection_failed", failures.ClassDependencyUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failure, ok := failures.As(verifyRunFailure(run, tc.cause))
			if !ok || failure.Failure.Class != tc.class || failure.Failure.Detail.Code != tc.code || failure.Failure.Detail.Attributes["run_id"] != run {
				t.Fatalf("wrong disposition: %v", failure)
			}
			if tc.class == failures.ClassSchemaInvalid && failure.Failure.Retryable {
				t.Fatal("deterministic missing evidence was made retryable")
			}
			if !errors.Is(failure, tc.cause) || (tc.class == failures.ClassDependencyUnavailable && !failure.Failure.Retryable) {
				t.Fatal("operational cause or retry classification lost")
			}
			if tc.name == "history" && (failure.Failure.Detail.Attributes["entity_id"] != "entity" || failure.Failure.Detail.Attributes["mutation_id"] != "mutation") {
				t.Fatal("coordinates lost")
			}
		})
	}
}

func TestVerifyRunPublicOutputParity(t *testing.T) {
	path := "profile.name"
	result := verifyRunCommandResult{DriftReport: mutationlog.DriftReport{RunID: uuid.NewString(), EntitiesChecked: 1, Rows: []mutationlog.DriftRow{{Kind: "value", EntityID: "entity", Domain: mutationlog.DomainAuthoredField, Path: &path, FoldedPresent: true, StoredPresent: true, FoldedValue: int64(7), StoredValue: float64(7), FoldedType: "integer", StoredType: "double"}}}, Status: "drift"}
	var text bytes.Buffer
	if err := writeVerifyRunResult(&text, result); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text.String(), "profile.name") || !strings.Contains(text.String(), "integer:7") || !strings.Contains(text.String(), "double:7.0") {
		t.Fatalf("lost evidence: %s", &text)
	}
	wire, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var readback verifyRunCommandResult
	if err := json.Unmarshal(wire, &readback); err != nil {
		t.Fatal(err)
	}
	if readback.Rows[0].FoldedType != "integer" || readback.Rows[0].StoredType != "double" || !readback.Rows[0].StoredPresent {
		t.Fatalf("machine evidence lost: %s", wire)
	}
}
