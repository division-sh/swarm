package cataloge2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/runcontrol"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	forkexecution "github.com/division-sh/swarm/internal/runtime/runforkexecution"
	"github.com/division-sh/swarm/internal/runtime/runlifecycle"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

func TestSelectedForkRecoveredReceiverReadinessBothStores(t *testing.T) {
	for _, backend := range []catalogRuntimeBackend{catalogBackendSQLite, catalogBackendPostgres} {
		for _, change := range []string{"valid", "runtime_replacement", "submission_failure", "selected_source_cleanup", "original_source_cleanup", "reconstructed_store", "terminal_run", "inactive", "termination_time", "wrong_entity", "wrong_type", "wrong_workflow", "wrong_mode", "wrong_version", "config", "missing_readiness", "readiness_run", "readiness_mode", "agent_revision"} {
			t.Run(string(backend)+"/"+change, func(t *testing.T) {
				cleanupFailure := change == "selected_source_cleanup" || change == "original_source_cleanup"
				root := selectedForkReadinessCatalogFixture(t, 1, "agent")
				h := newRuntimeHarnessForBackend(t, root, backend, true)
				selected := runScopedCatalogStore(t, h)
				const path = "worker-flow/worker-001"
				entity := materializeCatalogSelectedForkSourceFlow(t, h, catalogRuntimeRunID, path)
				ctx := worklifetime.WithOccurrence(catalogRunContext(h, catalogRuntimeRunID), h.rt.WorkOccurrence())
				if _, err := selected.PauseRunControl(ctx, runcontrol.TransitionRequest{RunID: catalogRuntimeRunID, Reason: "recovered readiness proof", ControlledBy: "cataloge2e"}); err != nil {
					t.Fatal(err)
				}
				event := catalogRunScopedWorkerReadyEvent(t, catalogRuntimeRunID, path, entity, uuid.NewString())
				if err := h.rt.Bus.PublishAndWait(ctx, event); err != nil {
					t.Fatal(err)
				}
				var sourceStore interface {
					storetest.DurableDataCatalogStore
					forkexecution.SourceArtifactSelectedContractSourceStore
				} = h.pg
				var forkStore forkexecution.SelectedContractForkLifecycle = h.pg
				if h.sqlite != nil {
					sourceStore, forkStore = h.sqlite, h.sqlite
				}
				loader, selection, loaded := selectedContractForkFixtureSelection(t, ctx, repoRootFromCatalogE2E(t), root, sourceStore)
				installCatalogSelectedSourceTopology(t, ctx, h, loaded)
				cfg := testRuntimeConfig()
				cfg.LLM.Backend = "anthropic"
				options := selectedContractAgentRuntimeOptionsForCatalogHarness(h, cfg)
				staged, err := forkexecution.ExecuteSelectedContractRunFork(ctx, forkexecution.SelectedContractExecutionRequest{
					SourceRunID: catalogRuntimeRunID, At: event.ID(), AllowSourceFreeze: true,
					Owner:        selectedContractExecutionOwnerForCatalogHarness(t, h, stopAfterSelectedForkCommit{forkStore}),
					SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
				})
				child := staged.Materialization.ForkRunID
				if !errors.Is(err, errStopAfterSelectedForkCommit) || child == "" {
					t.Fatalf("stage real child before recovery: %#v, %v", staged, err)
				}
				readiness, found, err := h.rt.Pipeline.LoadDynamicFlowRuntimeReadiness(ctx, child, flowidentity.RouteForInstancePath(path))
				if err != nil || !found || readiness.RunStatus != runfork.RunForkMaterializedStatus || !readiness.Eligible() {
					t.Fatalf("canonical paused child is not eligible for staged binding: %#v, %t, %v", readiness, found, err)
				}
				if change == "reconstructed_store" {
					if h.sqlite != nil {
						h.sqlite = h.reopenedSQLite
					} else {
						h.pg = storetest.AdmitPostgresRuntimeStore(t, h.db)
					}
					selected = runScopedCatalogStore(t, h)
				} else if change == "runtime_replacement" || change == "submission_failure" {
					if err := h.rt.Shutdown(); err != nil {
						t.Fatalf("retire loaded runtime before selected recovery: %v", err)
					}
				} else if change == "terminal_run" {
					if _, err := selected.StopRunControl(ctx, runcontrol.TransitionRequest{RunID: child, Reason: "terminal recovery refusal", ControlledBy: "cataloge2e"}); err != nil {
						t.Fatal(err)
					}
					readiness, found, err := h.rt.Pipeline.LoadDynamicFlowRuntimeReadiness(ctx, child, flowidentity.RouteForInstancePath(path))
					if err != nil || !found || readiness.Eligible() {
						t.Fatalf("canonical terminal child retained readiness eligibility: %#v, %t, %v", readiness, found, err)
					}
				} else if change != "valid" && change != "submission_failure" && !cleanupFailure {
					corruptSelectedForkRecoveredReadiness(t, ctx, h, child, path, change)
				}
				before := selectedForkRecoveredPhysicalSnapshot(t, ctx, h, child)
				executionOwner := selectedContractExecutionOwnerForCatalogHarness(t, h)
				var sourceCleanup *selectedRecoverySourceCleanup
				if cleanupFailure {
					sourceCleanup = &selectedRecoverySourceCleanup{SelectedContractSourceLoader: loader, failure: errors.New("injected recovered source release failure")}
					if change == "original_source_cleanup" {
						sourceCleanup.faultAt = 1
					}
					loader = sourceCleanup
					defer func() {
						sourceCleanup.repaired = true
						if err := executionOwner.RetireSelectedContexts(context.Background()); err != nil {
							t.Error(err)
						}
					}()
				}
				var submission *selectedRecoverySubmissionFailure
				if change == "submission_failure" {
					submission = &selectedRecoverySubmissionFailure{
						SelectedContractForkLifecycle: forkStore,
						registrar:                     forkStore.(runlifecycle.CandidateRegistrar),
						bundleHash:                    loaded.SourceArtifactFact.BundleHash(),
						failure:                       errors.New("injected recovered activation submission failure"),
					}
					executionOwner = selectedContractExecutionOwnerForCatalogHarness(t, h, submission)
				}
				activated, err := forkexecution.ActivateSelectedContractRunFork(ctx, forkexecution.SelectedContractActivationGateRequest{
					ForkRunID: child, AllowSourceFreeze: true, Store: selected,
					ExecutionOwner: executionOwner, SourceLoader: loader, AgentRuntime: options,
				})
				if change == "valid" || change == "reconstructed_store" || change == "runtime_replacement" || change == "submission_failure" || cleanupFailure {
					if submission != nil {
						if !errors.Is(err, submission.failure) || submission.submitted == 0 || submission.discards != 0 {
							t.Fatalf("recovered post-commit disposition: submitted=%d discards=%d err=%v", submission.submitted, submission.discards, err)
						}
						var status, executionState string
						if readErr := h.db.QueryRowContext(ctx, `SELECT r.status, e.state FROM runs r JOIN run_fork_selected_contract_runtime_executions e ON e.fork_run_id=r.run_id WHERE r.run_id=$1`, child).Scan(&status, &executionState); readErr != nil || status != "running" || executionState != "closed" {
							t.Fatalf("recovered committed activation/execution = %s/%s: %v", status, executionState, readErr)
						}
					} else if err != nil {
						logSelectedForkRecoveryFailure(t, ctx, h, child, err)
						t.Fatalf("lawful persisted recovery: %#v, %v", activated, err)
					}
					if !activated.Activated || activated.ExecutedEventCount != 1 {
						t.Fatalf("lost recovered commit evidence: %#v", activated)
					}
					owner, err := flowidentity.NewRunScopedFlowInstance(child, flowidentity.RouteForInstancePath(path))
					if err != nil {
						t.Fatal(err)
					}
					state, found, err := h.workflow.Load(ctx, owner)
					if err != nil || !found || state.CurrentState != "complete" || state.EntityID != entity || state.EntityType != "worker" {
						t.Fatalf("recovered agent did not reach its exact final consumer: %#v, %t, %v", state, found, err)
					}
					if sourceCleanup != nil {
						if sourceCleanup.loads != 2 || sourceCleanup.calls != [2]int{} {
							t.Fatalf("recovered context lost its two retained sources: loads=%d calls=%v", sourceCleanup.loads, sourceCleanup.calls)
						}
						for attempt := 0; attempt < 2; attempt++ {
							if err := executionOwner.RetireSelectedContexts(context.Background()); !errors.Is(err, sourceCleanup.failure) {
								t.Fatalf("retirement attempt %d lost source failure: %v", attempt, err)
							}
						}
						sourceCleanup.repaired = true
						if err := executionOwner.RetireSelectedContexts(context.Background()); err != nil {
							t.Fatal(err)
						}
						for i, root := range sourceCleanup.roots {
							want := 1
							if i == sourceCleanup.faultAt {
								want = 3
							}
							if sourceCleanup.calls[i] != want {
								t.Fatalf("source %d release calls=%d want=%d", i, sourceCleanup.calls[i], want)
							}
							if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
								t.Fatalf("source %d projection remains after retry: %v", i, err)
							}
						}
					}
					return
				}
				if err == nil || activated.Activated || activated.ExecutedEventCount != 0 || activated.ForkLocalRuntimeContainer != nil || len(activated.ForkEvents) != 0 {
					t.Fatalf("contradictory child reached publication: %#v, %v", activated, err)
				}
				if change != "terminal_run" && !strings.Contains(err.Error(), "selected-contract recovered") {
					t.Fatalf("contradiction escaped the recovered binding owner: %v", err)
				}
				t.Logf("exact recovery refusal: %v", err)
				if after := selectedForkRecoveredPhysicalSnapshot(t, ctx, h, child); after != before {
					t.Fatalf("recovery repaired or executed contradictory child:\nbefore=%s\nafter=%s", before, after)
				}
			})
		}
	}
}

type selectedRecoverySourceCleanup struct {
	forkexecution.SelectedContractSourceLoader
	faultAt, loads int
	failure        error
	repaired       bool
	calls          [2]int
	roots          [2]string
}

func (p *selectedRecoverySourceCleanup) LoadRunForkSelectedContractSourceForRequest(ctx context.Context, req forkexecution.SelectedContractSourceLoadRequest) (forkexecution.LoadedSelectedContractSource, error) {
	loaded, err := p.SelectedContractSourceLoader.LoadRunForkSelectedContractSourceForRequest(ctx, req)
	if err != nil {
		return loaded, err
	}
	i := p.loads
	p.loads++
	p.roots[i] = filepath.Dir(loaded.RuntimeProjection.PrivateRoot())
	release := loaded.Cleanup
	loaded.Cleanup = func() error {
		p.calls[i]++
		if i == p.faultAt && !p.repaired {
			return p.failure
		}
		return release()
	}
	return loaded, nil
}

type selectedRecoverySubmissionFailure struct {
	forkexecution.SelectedContractForkLifecycle
	registrar           runlifecycle.CandidateRegistrar
	bundleHash          string
	failure             error
	submitted, discards int
}

func (p *selectedRecoverySubmissionFailure) ActivateRunForkForSelectedContractExecution(ctx context.Context, req runfork.RunForkSelectedContractExecutionActivateRequest) (runfork.RunForkActivation, error) {
	registration, err := p.registrar.RegisterCompletionCandidateSink(ctx, runlifecycle.CandidateScope{BundleHash: p.bundleHash}, p)
	if err != nil {
		return runfork.RunForkActivation{}, err
	}
	defer registration.Release()
	return p.SelectedContractForkLifecycle.ActivateRunForkForSelectedContractExecution(ctx, req)
}

func (p *selectedRecoverySubmissionFailure) DiscardMaterializedSelectedContractExecutionFork(ctx context.Context, runID string) error {
	p.discards++
	return p.SelectedContractForkLifecycle.DiscardMaterializedSelectedContractExecutionFork(ctx, runID)
}

func (p *selectedRecoverySubmissionFailure) ReserveCompletionCandidate(ctx context.Context) (runlifecycle.CandidateAdmission, error) {
	owner, ok := worklifetime.OccurrenceFromContext(ctx)
	if !ok {
		return nil, errors.New("recovered candidate lost occurrence")
	}
	lease, err := owner.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &selectedRecoveryCandidateAdmission{owner: p, lease: lease}, nil
}

type selectedRecoveryCandidateAdmission struct {
	owner *selectedRecoverySubmissionFailure
	lease *worklifetime.Lease
}

func (a *selectedRecoveryCandidateAdmission) Submit(runlifecycle.Candidate) error {
	a.owner.submitted++
	return errors.Join(a.lease.Done(), a.owner.failure)
}

func (a *selectedRecoveryCandidateAdmission) Cancel() error { return a.lease.Done() }

// Capture durable facts before harness cleanup without repairing state or changing
// the timing of a successful execution. Joined errors retain every typed cause.
func logSelectedForkRecoveryFailure(t *testing.T, ctx context.Context, h *runtimeHarness, child string, failure error) {
	t.Helper()
	var logCause func(error)
	logCause = func(err error) {
		if err == nil {
			return
		}
		if typed, ok := err.(*runtimefailures.Error); ok {
			raw, marshalErr := json.Marshal(typed.Failure)
			t.Logf("recovery typed cause: %s (marshal error: %v)", raw, marshalErr)
		}
		switch wrapped := err.(type) {
		case interface{ Unwrap() []error }:
			for _, cause := range wrapped.Unwrap() {
				logCause(cause)
			}
		case interface{ Unwrap() error }:
			logCause(wrapped.Unwrap())
		}
	}
	logCause(failure)
	readCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	for _, table := range []string{"flow_instances", "flow_instance_runtime_readiness", "agents", "agent_lifecycle_transition_facts", "events", "event_deliveries"} {
		rows, err := h.db.QueryContext(readCtx, "SELECT * FROM "+table+" WHERE run_id = $1", child)
		if err != nil {
			t.Logf("recovery diagnostic %s: %v", table, err)
			continue
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Logf("recovery diagnostic %s columns: %v", table, err)
			continue
		}
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				t.Logf("recovery diagnostic %s scan: %v", table, err)
				break
			}
			record := make(map[string]any, len(columns))
			for i, name := range columns {
				if raw, ok := values[i].([]byte); ok {
					record[name] = string(raw)
				} else {
					record[name] = values[i]
				}
			}
			raw, marshalErr := json.Marshal(record)
			t.Logf("recovery diagnostic %s: %s (marshal error: %v)", table, raw, marshalErr)
		}
		if err := rows.Err(); err != nil {
			t.Logf("recovery diagnostic %s rows: %v", table, err)
		}
		rows.Close()
	}
}

// Corruption is applied only after canonical selected-fork materialization;
// these writes cannot manufacture a positive readiness or source fixture.
func corruptSelectedForkRecoveredReadiness(t *testing.T, ctx context.Context, h *runtimeHarness, runID, path, change string) {
	t.Helper()
	query := ""
	var value any
	switch change {
	case "inactive":
		query, value = `UPDATE flow_instances SET status = $3 WHERE run_id = $1 AND instance_path = $2`, "terminated"
	case "termination_time":
		query, value = `UPDATE flow_instances SET terminated_at = $3 WHERE run_id = $1 AND instance_path = $2`, time.Now().UTC()
	case "wrong_type":
		query, value = `UPDATE entity_state SET entity_type = $3 WHERE run_id = $1 AND flow_instance = $2`, "foreign"
	case "wrong_entity":
		query, value = `UPDATE entity_state SET entity_id = $3 WHERE run_id = $1 AND flow_instance = $2`, uuid.NewString()
	case "wrong_workflow":
		query, value = `UPDATE flow_instances SET flow_template = $3 WHERE run_id = $1 AND instance_path = $2`, "foreign"
	case "wrong_mode":
		query, value = `UPDATE flow_instances SET mode = $3 WHERE run_id = $1 AND instance_path = $2`, "static"
	case "missing_readiness":
		query = `DELETE FROM flow_instance_runtime_readiness WHERE run_id = $1 AND instance_path = $2`
	case "config", "wrong_version", "readiness_run", "readiness_mode", "agent_revision":
		column, table := "plan", "flow_instance_runtime_readiness"
		if change == "config" || change == "wrong_version" {
			column, table = "config", "flow_instances"
		}
		var raw []byte
		if err := h.db.QueryRowContext(ctx, fmt.Sprintf(`SELECT %s FROM %s WHERE run_id = $1 AND instance_path = $2`, column, table), runID, path).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var data map[string]any
		if err := json.Unmarshal(raw, &data); err != nil {
			t.Fatal(err)
		}
		switch change {
		case "config":
			data["worker_id"] = "foreign"
		case "wrong_version":
			data["workflow_version"] = "foreign"
		case "readiness_run":
			data["run_id"] = uuid.NewString()
		case "readiness_mode":
			data["execution_mode"] = "mock"
		case "agent_revision":
			data["agents"].([]any)[0].(map[string]any)["config_revision"] = "foreign-revision"
		}
		raw, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		query, value = fmt.Sprintf(`UPDATE %s SET %s = $3 WHERE run_id = $1 AND instance_path = $2`, table, column), string(raw)
	default:
		t.Fatalf("unknown recovered readiness corruption %q", change)
	}
	args := []any{runID, path}
	if value != nil {
		args = append(args, value)
	}
	result, err := h.db.ExecContext(ctx, query, args...)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := result.RowsAffected(); err != nil || count != 1 {
		t.Fatalf("corrupt exact child row: %d, %v", count, err)
	}
}

func selectedForkRecoveredPhysicalSnapshot(t *testing.T, ctx context.Context, h *runtimeHarness, runID string) string {
	t.Helper()
	var snapshot []string
	for _, table := range []string{"entity_state", "flow_instances", "flow_instance_runtime_readiness", "events"} {
		rows, err := h.db.QueryContext(ctx, "SELECT * FROM "+table+" WHERE run_id = $1", runID)
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			rows.Close()
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			encoded, err := json.Marshal([]any{table, values})
			if err != nil {
				rows.Close()
				t.Fatal(err)
			}
			snapshot = append(snapshot, string(encoded))
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	sort.Strings(snapshot)
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
