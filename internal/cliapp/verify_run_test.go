package cliapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/flowactivationfixture"
	"github.com/google/uuid"
)

func TestVerifyRunPublicAdmissionBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			ctx := context.Background()
			root := issue2567VerifySource(t)
			config := filepath.Join(t.TempDir(), "swarm.yaml")
			_, bundle, err := NewSwarmWorkflowModule(RepoRoot(), root, filepath.Join(RepoRoot(), "platform-spec.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			plans, err := StateStoreSchemaPlans(bundle)
			if err != nil {
				t.Fatal(err)
			}
			schema, err := SchemaBootstrapRequest(bundle.Platform, plans.Platform, plans.State)
			if err != nil {
				t.Fatal(err)
			}
			selected, configText := newVerifyCompositionBootStore(t, backend, root, schema)
			writeRuntimeConfigText(t, config, configText)
			runID := "abcdefab-1234-4000-8000-abcdefabcdef"
			storetest.RequireRun(t, ctx, selected, storetest.RunFixture{RunID: runID, Origin: storetest.ScenarioSetupOrigin(), Artifact: bundle.SourceArtifact})
			for _, withMutations := range []bool{false, true} {
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
						CurrentState: "waiting", StageDefined: true, CreatedAt: at, EnteredStageAt: at,
						Fields: map[string]any{},
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
				}
				for _, missing := range []bool{false, true} {
					id := runID
					if missing {
						id = uuid.NewString()
					}
					for _, mode := range []string{"json", "text", "quiet"} {
						for _, uppercase := range []bool{false, true} {
							t.Run(fmt.Sprintf("mutations_%t/missing_%t/%s/uppercase_%t", withMutations, missing, mode, uppercase), func(t *testing.T) {
								argument := id
								if uppercase {
									argument = strings.ToUpper(id)
								}
								args := []string{"verify", root, "--config", config, "--run", argument}
								if mode != "text" {
									args = append(args, "--"+mode)
								}
								var out, errOut bytes.Buffer
								code := executeRootCommand(ctx, RepoRoot(), args, &out, &errOut)
								wantEntities := 0
								if withMutations && !missing {
									wantEntities = 1
								}
								if (code != 0) != missing || errOut.Len() != 0 {
									t.Fatalf("wrong exit: missing=%t code=%d stdout=%s stderr=%s", missing, code, &out, &errOut)
								}
								if mode == "json" {
									var result verifyRunCommandResult
									if err := json.Unmarshal(out.Bytes(), &result); err != nil {
										t.Fatal(err)
									}
									if result.RunID != id || result.EntitiesChecked != wantEntities || len(result.Rows) != 0 || result.OK == missing {
										t.Fatalf("wrong result: %+v", result)
									}
									if missing {
										if result.Status != "failed" || len(result.Errors) != 1 || result.Errors[0].Detail.Code != "run_not_found" || result.Errors[0].Retryable {
											t.Fatalf("missing run lost typed refusal: %+v", result)
										}
									} else if result.Status != "passed" || len(result.Errors) != 0 {
										t.Fatalf("existing run refused: %+v", result)
									}
								} else if missing {
									if !strings.Contains(out.String(), "run_not_found") || strings.Contains(out.String(), "no drift") {
										t.Fatalf("missing run reported clean: %s", &out)
									}
								} else if want := fmt.Sprintf("%d entities checked, no drift", wantEntities); !strings.Contains(out.String(), want) {
									t.Fatalf("missing clean transcript: %s", &out)
								}
							})
						}
					}
				}
			}
		})
	}
}

func TestVerifyRunPublicFlagAndAbsentStoreAdmission(t *testing.T) {
	for _, tc := range []struct {
		name, run string
		portable  bool
		want      string
	}{
		{"absent", uuid.NewString(), false, "not_run"},
		{"blank", "", false, "failed"},
		{"invalid", "not-a-run", false, "failed"},
		{"portable", uuid.NewString(), true, "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolateCLIAPIConfigEnv(t)
			root := issue2567VerifySource(t)
			stateDir := filepath.Join(t.TempDir(), "never-created")
			args := []string{"verify", root, "--run", tc.run, "--swarm-dir", stateDir, "--json"}
			if tc.portable {
				args = append(args, "--portable")
			}
			var out, errOut bytes.Buffer
			code := executeRootCommand(context.Background(), RepoRoot(), args, &out, &errOut)
			var got verifyRunCommandResult
			if err := json.Unmarshal(out.Bytes(), &got); err != nil {
				t.Fatalf("%v: out=%s err=%s", err, &out, &errOut)
			}
			if got.Status != tc.want || got.OK || len(got.Rows) != 0 || got.EntitiesChecked != 0 || (code == 0) != (tc.want == "not_run") {
				t.Fatalf("result=%+v code=%d err=%s", got, code, &errOut)
			}
			if tc.want == "failed" && (len(got.Errors) != 1 || got.Errors[0].Retryable) {
				t.Fatalf("untyped/retryable refusal: %+v", got)
			}
			if tc.want == "not_run" && (!strings.Contains(got.Reason, stateDir) || strings.Contains(out.String(), "no drift")) {
				t.Fatalf("false empty verdict: %s", &out)
			}
			if _, err := os.Stat(stateDir); !os.IsNotExist(err) {
				t.Fatalf("inspection created state: %v", err)
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
